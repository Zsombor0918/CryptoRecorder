#!/usr/bin/env python3
"""
convert_yesterday.py — CLI entry-point for the Nautilus catalog converter.

Reads raw trade and depth JSONL(.zst) for a given UTC date, builds Nautilus
Instrument objects from exchangeInfo, converts trades to TradeTick,
reconstructs approximate L2 Depth-10 snapshots from cryptofeed-normalised
deltas, and writes everything into a ParquetDataCatalog.

Usage:
    python convert_yesterday.py                          # yesterday UTC
    python convert_yesterday.py --date 2026-04-17        # specific date
    python convert_yesterday.py --date 2026-04-17 --staging
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
from typing import Dict, List, Optional, Tuple

from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from config import NAUTILUS_CATALOG_ROOT, STATE_ROOT
from converter.book import convert_depth
from converter.catalog import purge_catalog_data
from converter.instruments import build_instruments, load_exchange_info
from converter.trades import convert_trades
from converter.universe import resolve_universe

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
) -> Dict:
    """Convert one UTC day's raw data → Nautilus ParquetDataCatalog.

    Returns a report dict that is also persisted to
    ``state/convert_reports/{date}.json``.
    """
    t0 = time.time()
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"Converting data for {date_str} …")

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

    # ── purge existing catalog data (idempotency) ─────────────────────
    if not staging:
        purge_catalog_data(work_root, all_instruments)

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
    total_depth = 0
    total_bad = 0
    total_gaps = 0
    venue_reports: Dict[str, dict] = {}
    ts_ranges: Dict[str, Dict[str, Optional[int]]] = {
        "trade": {"start_ns": None, "end_ns": None},
        "depth": {"start_ns": None, "end_ns": None},
    }
    symbols_processed: Dict[str, List[str]] = {}

    for venue, symbols in sorted(universe.items()):
        v_trades = 0
        v_depth = 0
        v_gaps = 0
        v_symbols: List[str] = []

        for symbol in sorted(symbols):
            key = (venue, symbol)
            if key not in inst_map:
                logger.warning(f"No instrument for {venue}/{symbol}, skipping")
                continue
            iid, pp, sp = inst_map[key]
            v_symbols.append(symbol)

            # ── trades ────────────────────────────────────────────────
            ticks, bad_t, t_first, t_last = convert_trades(
                venue, symbol, date_str, iid, pp, sp,
            )
            total_bad += bad_t
            if ticks:
                ticks.sort(key=lambda t: t.ts_init)
                for i in range(0, len(ticks), WRITE_BATCH_SIZE):
                    catalog.write_data(ticks[i : i + WRITE_BATCH_SIZE])
                v_trades += len(ticks)
                _update_ts_range(ts_ranges["trade"], t_first, t_last)

            # ── depth ─────────────────────────────────────────────────
            snaps, bad_d, gaps, d_first, d_last = convert_depth(
                venue, symbol, date_str, iid, pp, sp,
            )
            total_bad += bad_d
            v_gaps += gaps
            if snaps:
                snaps.sort(key=lambda s: s.ts_init)
                for i in range(0, len(snaps), WRITE_BATCH_SIZE):
                    catalog.write_data(snaps[i : i + WRITE_BATCH_SIZE])
                v_depth += len(snaps)
                _update_ts_range(ts_ranges["depth"], d_first, d_last)

        total_trades += v_trades
        total_depth += v_depth
        total_gaps += v_gaps
        symbols_processed[venue] = v_symbols
        venue_reports[venue] = {
            "symbols": v_symbols,
            "trades_written": v_trades,
            "depth_snapshots_written": v_depth,
            "gaps_suspected": v_gaps,
        }

    # ── staging → atomic rename ───────────────────────────────────────
    if staging and total_trades + total_depth > 0:
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

    # ── report ────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    report = {
        "date": date_str,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "runtime_sec": round(elapsed, 2),
        "status": "ok" if (total_trades + total_depth) > 0 else "empty",
        "instruments_written": len(all_instruments),
        "total_trades_written": total_trades,
        "total_depth_snapshots_written": total_depth,
        "bad_lines": total_bad,
        "gaps_suspected": total_gaps,
        "futures_enabled": "BINANCE_USDTF" in universe,
        "symbols_processed": symbols_processed,
        "venues": venue_reports,
        "ts_ranges": {
            "trade": ts_ranges["trade"],
            "depth": ts_ranges["depth"],
        },
        "catalog_root": str(target_root),
    }

    _save_report(report)

    logger.info(
        f"Done: {total_trades} trades, {total_depth} depth, "
        f"{len(all_instruments)} instruments, {total_bad} bad lines, "
        f"{total_gaps} gaps suspected in {elapsed:.1f}s"
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

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert raw Binance JSONL → Nautilus ParquetDataCatalog",
    )
    ap.add_argument(
        "--date", type=str,
        help="Date to convert (YYYY-MM-DD). Default: yesterday UTC.",
    )
    ap.add_argument(
        "--staging", action="store_true",
        help="Write to staging dir, then atomically rename on success.",
    )
    args = ap.parse_args()

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    report = convert_date(date, staging=args.staging)
    raise SystemExit(0 if report.get("status") in ("ok", "no_data") else 1)


if __name__ == "__main__":
    main()
