#!/usr/bin/env python3
"""
convert_day.py — CLI entrypoint for the Nautilus converter.

Reads raw ``trade_v2`` and ``depth_v2`` JSONL(.zst) for a given UTC date,
builds Nautilus Instrument objects from exchangeInfo, converts trades to
TradeTick, replays depth deterministically to OrderBookDeltas, and writes
everything into a ParquetDataCatalog.

OrderBookDepth10 is optional (off by default) and derived only from the
replayed deterministic book state.

Usage:
    python convert_day.py                          # yesterday UTC
    python convert_day.py --date 2026-04-17       # specific date
    python convert_day.py --date 2026-04-17 --staging
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from config import (
    NAUTILUS_CATALOG_ROOT,
    DEPTH10_INTERVAL_SEC,
    EMIT_DEPTH10_DEFAULT,
    STATE_ROOT,
)
from converter.depth_phase2 import convert_depth_v2
from converter.catalog import purge_catalog_date_range
from converter.instruments import build_instruments, load_exchange_info
from converter.trades import convert_trades
from converter.universe import resolve_universe
from time_utils import local_now_iso

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

WRITE_BATCH_SIZE: int = 5000


# ===================================================================
# Main conversion logic
# ===================================================================

def convert_date(
    date: datetime,
    catalog_root: Optional[Path] = None,
    staging: bool = False,
    *,
    emit_depth10: bool = EMIT_DEPTH10_DEFAULT,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
) -> Dict:
    """Convert one UTC day's raw data → Nautilus ParquetDataCatalog.

    Returns a report dict that is also persisted to
    ``state/convert_reports/{date}.json``.
    """
    t0 = time.time()
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"Converting data for {date_str} (deterministic native) …")

    target_root = catalog_root or NAUTILUS_CATALOG_ROOT
    if staging:
        staging_dir = Path(str(target_root) + f".staging.{os.getpid()}")
        work_root = staging_dir
    else:
        work_root = target_root

    work_root.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(work_root))

    # ── universe ──────────────────────────────────────────────────────
    universe = resolve_universe(date_str)
    if not universe:
        logger.warning(f"No raw data found for {date_str}")
        return _save_report(_empty_report(date_str, t0, status="no_data"))

    # ── exchangeInfo ──────────────────────────────────────────────────
    einfo_spot = load_exchange_info("BINANCE_SPOT", date_str)
    einfo_fut = load_exchange_info("BINANCE_USDTF", date_str)

    # ── build instruments ─────────────────────────────────────────────
    all_instruments = []
    for venue, syms in universe.items():
        einfo = einfo_fut if "USDTF" in venue else einfo_spot
        insts = build_instruments(venue, syms, einfo)
        all_instruments.extend(insts)

    # ── purge existing catalog data (date-scoped idempotency) ─────────
    if not staging:
        iid_list = [inst.id for inst in all_instruments]
        purge_catalog_date_range(work_root, iid_list, date_str)

    if all_instruments:
        catalog.write_data(all_instruments)
        logger.info(f"Wrote {len(all_instruments)} instruments")

    # ── instrument lookup ─────────────────────────────────────────────
    inst_map: Dict[Tuple[str, str], Tuple] = {}
    for inst in all_instruments:
        raw = str(inst.raw_symbol)
        vtag = "BINANCE_USDTF" if isinstance(inst, CryptoPerpetual) else "BINANCE_SPOT"
        inst_map[(vtag, raw)] = (inst.id, inst.price_precision, inst.size_precision)

    # ── per-venue / per-symbol conversion ─────────────────────────────
    total_trades = 0
    total_delta_events = 0
    total_depth10 = 0
    total_bad = 0
    total_snapshot_seeds = 0
    total_resyncs = 0
    total_desyncs = 0
    total_fenced_ranges = 0
    venue_reports: Dict[str, dict] = {}
    per_symbol_fenced_ranges: Dict[str, Dict[str, object]] = {}
    ts_ranges: Dict[str, Dict[str, Optional[int]]] = {
        "trade": {"start_ns": None, "end_ns": None},
        "order_book_deltas": {"start_ns": None, "end_ns": None},
        "order_book_depths": {"start_ns": None, "end_ns": None},
    }
    symbols_processed: Dict[str, List[str]] = {}

    # Track data presence per instrument
    instruments_with_trades: List[str] = []
    instruments_with_depth: List[str] = []
    instruments_with_no_data: List[str] = []

    for venue, symbols in sorted(universe.items()):
        v_trades = 0
        v_delta_events = 0
        v_depth10 = 0
        v_snapshot_seeds = 0
        v_resyncs = 0
        v_desyncs = 0
        v_fenced_ranges = 0
        v_symbols: List[str] = []

        for symbol in sorted(symbols):
            key = (venue, symbol)
            if key not in inst_map:
                logger.warning(f"No instrument for {venue}/{symbol}, skipping")
                continue
            iid, pp, sp = inst_map[key]
            v_symbols.append(symbol)

            # ── trades (trade_v2) ─────────────────────────────────────
            ticks, bad_t, t_first, t_last = convert_trades(
                venue, symbol, date_str, iid, pp, sp,
            )
            total_bad += bad_t
            sym_has_trades = len(ticks) > 0
            if ticks:
                ticks.sort(key=lambda t: t.ts_init)
                for i in range(0, len(ticks), WRITE_BATCH_SIZE):
                    catalog.write_data(ticks[i : i + WRITE_BATCH_SIZE])
                v_trades += len(ticks)
                _update_ts_range(ts_ranges["trade"], t_first, t_last)

            # ── depth (depth_v2 → OrderBookDeltas) ────────────────────
            deltas, depth10s, depth_metrics = convert_depth_v2(
                venue,
                symbol,
                date_str,
                iid,
                pp,
                sp,
                emit_depth10=emit_depth10,
                depth10_interval_sec=depth10_interval_sec,
            )
            total_bad += depth_metrics.bad_lines
            v_delta_events += len(deltas)
            v_depth10 += len(depth10s)
            v_snapshot_seeds += depth_metrics.snapshot_seed_count
            v_resyncs += depth_metrics.resync_count
            v_desyncs += depth_metrics.desync_events
            v_fenced_ranges += len(depth_metrics.fenced_ranges)
            sym_has_depth = len(deltas) > 0 or len(depth10s) > 0
            if depth_metrics.fenced_ranges:
                per_symbol_fenced_ranges[f"{venue}/{symbol}"] = {
                    "fenced_ranges": len(depth_metrics.fenced_ranges),
                    "examples": depth_metrics.fenced_ranges[:3],
                }
            if deltas:
                deltas.sort(key=lambda d: d.ts_init)
                for i in range(0, len(deltas), WRITE_BATCH_SIZE):
                    catalog.write_data(deltas[i : i + WRITE_BATCH_SIZE])
                _update_ts_range(
                    ts_ranges["order_book_deltas"],
                    depth_metrics.first_ts_ns,
                    depth_metrics.last_ts_ns,
                )
            if depth10s:
                depth10s.sort(key=lambda d: d.ts_init)
                for i in range(0, len(depth10s), WRITE_BATCH_SIZE):
                    catalog.write_data(depth10s[i : i + WRITE_BATCH_SIZE])
                _update_ts_range(
                    ts_ranges["order_book_depths"],
                    depth_metrics.first_ts_ns,
                    depth_metrics.last_ts_ns,
                )

            # ── track data presence ───────────────────────────────────
            iid_str = str(iid)
            if sym_has_trades:
                instruments_with_trades.append(iid_str)
            if sym_has_depth:
                instruments_with_depth.append(iid_str)
            if not sym_has_trades and not sym_has_depth:
                instruments_with_no_data.append(iid_str)

        total_trades += v_trades
        total_delta_events += v_delta_events
        total_depth10 += v_depth10
        total_snapshot_seeds += v_snapshot_seeds
        total_resyncs += v_resyncs
        total_desyncs += v_desyncs
        total_fenced_ranges += v_fenced_ranges
        symbols_processed[venue] = v_symbols
        venue_reports[venue] = {
            "symbols": v_symbols,
            "trades_written": v_trades,
            "delta_events_written": v_delta_events,
            "depth10_written": v_depth10,
            "snapshot_seed_count": v_snapshot_seeds,
            "resync_count": v_resyncs,
            "desync_events": v_desyncs,
            "fenced_ranges": v_fenced_ranges,
        }

    # ── staging → atomic rename ───────────────────────────────────────
    if staging and total_trades + total_delta_events > 0:
        if target_root.exists():
            backup = Path(str(target_root) + ".bak")
            if backup.exists():
                shutil.rmtree(backup)
            target_root.rename(backup)
        staging_dir.rename(target_root)
        logger.info(f"Staging → {target_root} (atomic rename)")
    elif staging:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)

    # ── data presence summary ─────────────────────────────────────────
    instruments_with_both = set(instruments_with_trades) & set(instruments_with_depth)
    data_presence = {
        "instruments_defined": len(all_instruments),
        "instruments_with_trades": len(instruments_with_trades),
        "instruments_with_depth": len(instruments_with_depth),
        "instruments_with_both": len(instruments_with_both),
        "instruments_with_no_data": len(instruments_with_no_data),
        "no_data_list": instruments_with_no_data[:20],
    }

    # ── report ────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    report = {
        "date": date_str,
        "timestamp": local_now_iso(),
        "runtime_sec": round(elapsed, 2),
        "status": "ok" if (total_trades + total_delta_events) > 0 else "empty",
        "architecture": "deterministic_native",
        "instruments_written": len(all_instruments),
        "total_trades_written": total_trades,
        "total_order_book_deltas_written": total_delta_events,
        "total_depth10_written": total_depth10,
        "bad_lines": total_bad,
        "snapshot_seed_count": total_snapshot_seeds,
        "resync_count": total_resyncs,
        "desync_events": total_desyncs,
        "fenced_ranges_total": total_fenced_ranges,
        "per_symbol_fenced_ranges": per_symbol_fenced_ranges,
        "data_presence": data_presence,
        "futures_enabled": "BINANCE_USDTF" in universe,
        "symbols_processed": symbols_processed,
        "venues": venue_reports,
        "ts_ranges": ts_ranges,
        "depth_settings": {
            "emit_depth10": emit_depth10,
            "depth10_interval_sec": depth10_interval_sec,
        },
        "catalog_root": str(target_root),
    }

    _save_report(report)

    logger.info(
        f"Done: {total_trades} trades, "
        f"{total_delta_events} delta_events, "
        f"{total_depth10} depth10, "
        f"{len(all_instruments)} instruments, {total_bad} bad lines, "
        f"{total_fenced_ranges} fenced ranges in {elapsed:.1f}s"
    )
    return report


# ── helpers ───────────────────────────────────────────────────────────

def _update_ts_range(
    r: Dict[str, Optional[int]],
    first: Optional[int],
    last: Optional[int],
) -> None:
    if first is not None:
        if r["start_ns"] is None or first < r["start_ns"]:
            r["start_ns"] = first
    if last is not None:
        if r["end_ns"] is None or last > r["end_ns"]:
            r["end_ns"] = last


def _empty_report(date_str: str, t0: float, **kwargs) -> dict:
    return {
        "date": date_str,
        "runtime_sec": round(time.time() - t0, 2),
        **kwargs,
    }


def _save_report(report: dict) -> dict:
    rp = STATE_ROOT / "convert_reports" / f"{report['date']}.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"Report → {rp}")
    return report


# ===================================================================
# CLI
# ===================================================================

def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Convert raw Binance JSONL → Nautilus ParquetDataCatalog (deterministic native)",
    )
    ap.add_argument(
        "--date", type=str,
        help="Date to convert (YYYY-MM-DD). Default: yesterday UTC.",
    )
    ap.add_argument(
        "--staging", action="store_true",
        help="Write to staging dir, then atomically rename on success.",
    )
    ap.add_argument(
        "--emit-depth10",
        action="store_true",
        default=EMIT_DEPTH10_DEFAULT,
        help="Derive optional OrderBookDepth10 output from replayed book state.",
    )
    ap.add_argument(
        "--depth10-interval-sec",
        type=float,
        default=DEPTH10_INTERVAL_SEC,
        help="Minimum interval between derived depth10 snapshots.",
    )
    return ap


def main(
    argv: Optional[Sequence[str]] = None,
) -> int:
    ap = _build_arg_parser()
    args = ap.parse_args(argv)

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    report = convert_date(
        date,
        staging=args.staging,
        emit_depth10=args.emit_depth10,
        depth10_interval_sec=args.depth10_interval_sec,
    )
    return 0 if report.get("status") in ("ok", "no_data") else 1


if __name__ == "__main__":
    raise SystemExit(main())
