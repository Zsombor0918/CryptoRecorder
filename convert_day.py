#!/usr/bin/env python3
"""
convert_day.py — CLI entrypoint for the Nautilus converter.

Reads raw ``trade_v2`` and ``depth_v2`` JSONL(.zst) for a given UTC date,
builds Nautilus Instrument objects from exchangeInfo, converts trades to
TradeTick, replays depth deterministically to OrderBookDeltas, and writes
everything into a ParquetDataCatalog.

OrderBookDepth10 is enabled by default and derived only from the replayed
deterministic book state.

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
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from config import (
    NAUTILUS_CATALOG_ROOT,
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
    EMIT_DEPTH10_DEFAULT,
    MIN_TRADE_RECORDS_FOR_FULL_READY,
    PHASE2_SNAPSHOT_LIMIT,
    STATE_ROOT,
)
from converter.depth_phase2 import canonical_fence_digest, convert_depth_v2_streaming
from converter.catalog import _parse_parquet_date_range, purge_catalog_date_range
from converter.instruments import build_instruments, load_exchange_info
from converter.readers import stream_raw_records
from converter.spool import ObjectSpool, TimestampSpool
from converter.trades import convert_trades_streaming
from converter.universe import resolve_universe
from time_utils import local_now_iso
from converter.trade_coverage import build_readiness_summary

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

WRITE_BATCH_SIZE: int = 5000
# Threshold for refusing a non-staging conversion when raw depth coverage is too low.
# Refuse only when expected_symbols_total is large enough to be meaningful.
OVERWRITE_DEPTH_REFUSE_MIN_RATIO: float = 0.80
OVERWRITE_DEPTH_REFUSE_MIN_EXPECTED_SYMBOLS: int = 50
DERIVED_DEPTH_SNAPSHOT_TYPE: str = "OrderBookDepth10"
FULL_DEPTH_SOURCE: str = "OrderBookDeltas"
DERIVED_DEPTH_CAP_WARNING: str = (
    "Nautilus catalog supports OrderBookDepth10 only; full depth is available "
    "via OrderBookDeltas."
)
DATE_SCOPED_CATALOG_TYPES: frozenset[str] = frozenset(
    {"trade_tick", "order_book_deltas", "order_book_depths"}
)
METADATA_CATALOG_TYPES: frozenset[str] = frozenset(
    {"currency_pair", "crypto_perpetual"}
)


class StagingValidationError(RuntimeError):
    """Raised when staged catalog output is not safe to publish."""


class StagingPublishError(RuntimeError):
    """Raised when staged catalog publication fails."""

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
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
    allow_partial_overwrite: bool = False,
    symbols: Optional[Sequence[str]] = None,
    venues: Optional[Sequence[str]] = None,
) -> Dict:
    """Convert one UTC day's raw data → Nautilus ParquetDataCatalog.

    Returns a report dict that is also persisted to
    ``state/convert_reports/{date}.json``.
    """
    t0 = time.time()
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"Converting data for {date_str} (deterministic native) …")

    target_root = catalog_root or NAUTILUS_CATALOG_ROOT
    requested_depth_snapshot_levels = max(0, int(derived_depth_snapshot_levels))
    applied_depth_snapshot_levels = min(requested_depth_snapshot_levels, 10)
    if applied_depth_snapshot_levels <= 0:
        applied_depth_snapshot_levels = 10
    derived_snapshot_warning = (
        DERIVED_DEPTH_CAP_WARNING
        if requested_depth_snapshot_levels != applied_depth_snapshot_levels
        else None
    )
    if staging:
        target_root.parent.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(
            tempfile.mkdtemp(
                prefix=f"{target_root.name}.staging.",
                dir=target_root.parent,
            )
        )
        work_root = staging_dir
    else:
        work_root = target_root

    work_root.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(work_root))

    # ── universe ──────────────────────────────────────────────────────
    universe = resolve_universe(date_str)
    if venues:
        venue_filter = {v.strip().upper() for v in venues if v.strip()}
        universe = {
            venue: syms
            for venue, syms in universe.items()
            if venue in venue_filter
        }
    if symbols:
        symbol_filter = {s.strip().upper() for s in symbols if s.strip()}
        universe = {
            venue: [sym for sym in syms if sym.upper() in symbol_filter]
            for venue, syms in universe.items()
        }
        universe = {venue: syms for venue, syms in universe.items() if syms}
    if not universe:
        logger.warning(f"No raw data found for {date_str}")
        return _save_report(_empty_report(
            date_str,
            t0,
            status="no_data",
            catalog_root=str(target_root),
            **_derived_snapshot_report_fields(
                requested_depth_snapshot_levels,
                applied_depth_snapshot_levels,
                derived_snapshot_warning,
            ),
        ))

    # ── exchangeInfo ──────────────────────────────────────────────────
    einfo_spot = load_exchange_info("BINANCE_SPOT", date_str)
    einfo_fut = load_exchange_info("BINANCE_USDTF", date_str)

    # ── build instruments ─────────────────────────────────────────────
    all_instruments = []
    for venue, syms in universe.items():
        einfo = einfo_fut if "USDTF" in venue else einfo_spot
        insts = build_instruments(venue, syms, einfo)
        all_instruments.extend(insts)

    # ── instrument lookup (needed before raw scan and purge guard) ────
    inst_map: Dict[Tuple[str, str], Tuple] = {}
    for inst in all_instruments:
        raw = str(inst.raw_symbol)
        vtag = "BINANCE_USDTF" if isinstance(inst, CryptoPerpetual) else "BINANCE_SPOT"
        inst_map[(vtag, raw)] = (inst.id, inst.price_precision, inst.size_precision)

    expected_symbol_keys = {
        f"{venue}/{symbol}"
        for (venue, symbol) in inst_map.keys()
    }

    # ── raw coverage scan (before any purge) ──────────────────────────
    raw_depth_symbols_set = (
        _symbols_with_raw_record_type(universe, date_str, channel="depth_v2", record_type="depth_update")
        & expected_symbol_keys
    )
    raw_trade_symbols_set = (
        _symbols_with_raw_record_type(universe, date_str, channel="trade_v2", record_type="trade")
        & expected_symbol_keys
    )
    raw_depth_symbols = sorted(raw_depth_symbols_set)
    raw_trade_symbols = sorted(raw_trade_symbols_set)

    # ── USDTF depth-without-trades raw ingest warning ─────────────────
    usdtf_depth = {k for k in raw_depth_symbols_set if k.startswith("BINANCE_USDTF/")}
    usdtf_trades = {k for k in raw_trade_symbols_set if k.startswith("BINANCE_USDTF/")}
    _usdtf_ingest_warnings: List[str] = []
    if usdtf_depth and not usdtf_trades:
        _usdtf_ingest_warnings.append(
            "futures depth is healthy but trade websocket received no messages; "
            "TradeTick missing is caused by raw ingest, not conversion"
        )

    # ── partial overwrite guard (before purge so catalog is never touched on refuse) ──
    overwrite_enabled = not staging
    integrity_warnings: List[str] = list(_usdtf_ingest_warnings)
    if (
        overwrite_enabled
        and not allow_partial_overwrite
        and len(expected_symbol_keys) >= OVERWRITE_DEPTH_REFUSE_MIN_EXPECTED_SYMBOLS
    ):
        depth_ratio = (
            len(raw_depth_symbols) / float(len(expected_symbol_keys))
            if expected_symbol_keys
            else 1.0
        )
        if depth_ratio < OVERWRITE_DEPTH_REFUSE_MIN_RATIO:
            msg = (
                "REFUSING conversion: partial raw depth coverage would overwrite catalog. "
                f"raw_depth_symbols={len(raw_depth_symbols)}/{len(expected_symbol_keys)} "
                f"({depth_ratio:.1%}). "
                "Pass --allow-partial-overwrite to force."
            )
            logger.error(msg)
            return _save_report({
                "date": date_str,
                "timestamp": local_now_iso(),
                "runtime_sec": round(time.time() - t0, 2),
                "status": "refused_partial_raw_depth",
                "architecture": "deterministic_native",
                "catalog_root": str(target_root),
                **_derived_snapshot_report_fields(
                    requested_depth_snapshot_levels,
                    applied_depth_snapshot_levels,
                    derived_snapshot_warning,
                ),
                "conversion_integrity": {
                    "date_converted": date_str,
                    "catalog_root_written": str(work_root),
                    "staging": staging,
                    "emit_depth10": emit_depth10,
                    "expected_symbols_total": len(expected_symbol_keys),
                    "raw_depth_symbols": raw_depth_symbols,
                    "raw_trade_symbols": raw_trade_symbols,
                    "warnings": [msg],
                },
            })
    elif (
        overwrite_enabled
        and len(expected_symbol_keys) >= OVERWRITE_DEPTH_REFUSE_MIN_EXPECTED_SYMBOLS
    ):
        depth_ratio = (
            len(raw_depth_symbols) / float(len(expected_symbol_keys))
            if expected_symbol_keys
            else 1.0
        )
        if depth_ratio < OVERWRITE_DEPTH_REFUSE_MIN_RATIO:
            warning = (
                "WARNING: partial raw depth overwrite allowed by --allow-partial-overwrite. "
                f"raw_depth_symbols={len(raw_depth_symbols)}/{len(expected_symbol_keys)} "
                f"({depth_ratio:.1%})"
            )
            integrity_warnings.append(warning)
            logger.warning(warning)

    # ── purge existing catalog data (date-scoped idempotency) ─────────
    if not staging:
        iid_list = [inst.id for inst in all_instruments]
        purge_catalog_date_range(work_root, iid_list, date_str)

    if all_instruments:
        catalog.write_data(all_instruments)
        logger.info(f"Wrote {len(all_instruments)} instruments")

    # ── per-venue / per-symbol conversion ─────────────────────────────
    total_trades = 0
    total_delta_events = 0
    total_depth10 = 0
    total_derived_depth_snapshots = 0
    total_bad = 0
    bad_lines_by_exception_type: Dict[str, int] = {}
    bad_lines_by_record_type: Dict[str, int] = {}
    bad_lines_by_venue_symbol: Dict[str, int] = {}
    bad_line_examples: List[Dict[str, Any]] = []
    zero_size_trade_skipped_by_venue_symbol: Dict[str, int] = {}
    zero_size_trade_examples: List[Dict[str, Any]] = []
    total_snapshot_seeds = 0
    total_resyncs = 0
    total_desyncs = 0
    total_fenced_ranges = 0
    total_fenced_ranges_low = 0
    total_fenced_ranges_medium = 0
    total_fenced_ranges_high = 0
    total_unrecovered_fences = 0
    total_bootstrap_fences = 0
    total_shutdown_fences = 0
    total_reconnect_fences = 0
    total_utc_day_rollover_fences = 0
    total_real_desync_fences = 0
    total_unrecovered_real_fences = 0
    total_depth_gap_warnings_over_60s = 0
    standalone_depth_day = True
    extra_raw_partitions_scanned: set[str] = set()
    records_imported_from_previous_folder = 0
    records_imported_from_next_folder = 0
    records_dropped_outside_target_utc = 0
    duplicate_records_suppressed = 0
    carried_seed_symbol_count = 0
    synthetic_opening_snapshot_count = 0
    venue_reports: Dict[str, dict] = {}
    per_symbol_fenced_ranges: Dict[str, Dict[str, object]] = {}
    per_symbol_trade: Dict[str, Dict[str, int]] = {}
    per_symbol_depth: Dict[str, Dict[str, int]] = {}
    per_symbol_gap_diagnostics: Dict[str, Dict[str, object]] = {}
    top_real_gap_candidates: List[Dict[str, object]] = []
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
    converted_trade_symbols: set[str] = set()
    converted_order_book_delta_symbols: set[str] = set()
    converted_order_book_depth_symbols: set[str] = set()

    for venue, symbols in sorted(universe.items()):
        v_trades = 0
        v_trade_raw_records = 0
        v_trade_raw_trade_records = 0
        v_trade_raw_lifecycle_records = 0
        v_symbols_with_trades: List[str] = []
        v_symbols_without_trades: List[str] = []
        v_symbols_with_trade_ticks: List[str] = []
        v_symbols_without_trade_ticks: List[str] = []
        v_lifecycle_only_symbols: List[str] = []
        v_delta_events = 0
        v_depth10 = 0
        v_derived_depth_snapshots = 0
        v_snapshot_seeds = 0
        v_resyncs = 0
        v_desyncs = 0
        v_fenced_ranges = 0
        v_fenced_ranges_low = 0
        v_fenced_ranges_medium = 0
        v_fenced_ranges_high = 0
        v_unrecovered_fences = 0
        v_bootstrap_fences = 0
        v_shutdown_fences = 0
        v_reconnect_fences = 0
        v_utc_day_rollover_fences = 0
        v_real_desync_fences = 0
        v_unrecovered_real_fences = 0
        v_depth_gap_warnings_over_60s = 0
        v_records_imported_from_previous_folder = 0
        v_records_imported_from_next_folder = 0
        v_records_dropped_outside_target_utc = 0
        v_duplicate_records_suppressed = 0
        v_carried_seed_symbol_count = 0
        v_synthetic_opening_snapshot_count = 0
        v_symbols: List[str] = []
        v_top_symbols_by_trade_count: List[Tuple[str, int]] = []
        v_top_real_gap_candidates: List[Dict[str, object]] = []

        for symbol in sorted(symbols):
            key = (venue, symbol)
            if key not in inst_map:
                logger.warning(f"No instrument for {venue}/{symbol}, skipping")
                continue
            iid, pp, sp = inst_map[key]
            v_symbols.append(symbol)

            # ── trades (trade_v2) ─────────────────────────────────────
            trade_ordinal = 0
            with ObjectSpool(prefix="cryptorecorder-trade-objects-") as trade_spool:
                def on_trade_batch(batch: List[object]) -> None:
                    nonlocal trade_ordinal
                    trade_ordinal = trade_spool.insert_many(
                        batch,
                        start_ordinal=trade_ordinal,
                    )

                bad_t, t_first, t_last, trade_diag = convert_trades_streaming(
                    venue,
                    symbol,
                    date_str,
                    iid,
                    pp,
                    sp,
                    on_ticks_batch=on_trade_batch,
                    batch_size=WRITE_BATCH_SIZE,
                )
                total_bad += bad_t

                # Aggregate bad_lines tracking data from trades
                sym_key = f"{venue}/{symbol}"
                if bad_t > 0:
                    bad_lines_by_venue_symbol[sym_key] = (
                        bad_lines_by_venue_symbol.get(sym_key, 0) + bad_t
                    )
                for exc_type, count in trade_diag.get(
                    "bad_lines_by_exception_type", {}
                ).items():
                    bad_lines_by_exception_type[exc_type] = (
                        bad_lines_by_exception_type.get(exc_type, 0) + count
                    )
                for rec_type, count in trade_diag.get("bad_lines_by_record_type", {}).items():
                    bad_lines_by_record_type[rec_type] = (
                        bad_lines_by_record_type.get(rec_type, 0) + count
                    )
                bad_line_examples.extend(trade_diag.get("bad_line_examples", []))
                zero_size_skipped = int(trade_diag.get("zero_size_trade_skipped", 0))
                if zero_size_skipped > 0:
                    zero_size_trade_skipped_by_venue_symbol[sym_key] = (
                        zero_size_trade_skipped_by_venue_symbol.get(sym_key, 0)
                        + zero_size_skipped
                    )
                zero_size_trade_examples.extend(trade_diag.get("zero_size_trade_examples", []))

                v_trade_raw_records += int(trade_diag.get("raw_record_count", 0))
                v_trade_raw_trade_records += int(trade_diag.get("raw_trade_record_count", 0))
                v_trade_raw_lifecycle_records += int(trade_diag.get("raw_lifecycle_record_count", 0))
                sym_has_trades = int(trade_diag.get("raw_trade_record_count", 0)) > 0
                sym_has_trade_ticks = trade_spool.count > 0
                if sym_has_trades:
                    v_symbols_with_trades.append(symbol)
                else:
                    v_symbols_without_trades.append(symbol)
                if sym_has_trade_ticks:
                    v_symbols_with_trade_ticks.append(symbol)
                else:
                    v_symbols_without_trade_ticks.append(symbol)
                if (
                    int(trade_diag.get("raw_trade_record_count", 0)) == 0
                    and int(trade_diag.get("raw_lifecycle_record_count", 0)) > 0
                ):
                    v_lifecycle_only_symbols.append(symbol)
                v_top_symbols_by_trade_count.append(
                    (symbol, int(trade_diag.get("raw_trade_record_count", 0)))
                )

                per_symbol_trade[f"{venue}/{symbol}"] = {
                    "raw_record_count": int(trade_diag.get("raw_record_count", 0)),
                    "raw_trade_record_count": int(trade_diag.get("raw_trade_record_count", 0)),
                    "raw_lifecycle_record_count": int(trade_diag.get("raw_lifecycle_record_count", 0)),
                    "ticks_written": int(trade_diag.get("ticks_written", 0)),
                    "zero_size_trade_skipped": zero_size_skipped,
                    "first_trade_ts_ns": t_first,
                    "last_trade_ts_ns": t_last,
                    "will_create_tradetick": sym_has_trade_ticks,
                }

                if trade_spool.count:
                    trades_written = _write_object_spool(catalog, trade_spool)
                    v_trades += trades_written
                    trade_diag["ticks_written"] = trades_written
                    per_symbol_trade[f"{venue}/{symbol}"]["ticks_written"] = trades_written
                    converted_trade_symbols.add(f"{venue}/{symbol}")
                    _update_ts_range(ts_ranges["trade"], t_first, t_last)

            # ── depth (depth_v2 → OrderBookDeltas) ────────────────────
            delta_ordinal = 0
            depth10_ordinal = 0
            with (
                ObjectSpool(prefix="cryptorecorder-delta-objects-") as deltas_spool,
                ObjectSpool(prefix="cryptorecorder-depth10-objects-") as depth10_spool,
                TimestampSpool(prefix="cryptorecorder-depth10-gap-") as depth10_ts_spool,
            ):
                def on_deltas_batch(batch: List[object]) -> None:
                    nonlocal delta_ordinal
                    delta_ordinal = deltas_spool.insert_many(
                        batch,
                        start_ordinal=delta_ordinal,
                    )

                def on_depth10_batch(batch: List[object]) -> None:
                    nonlocal depth10_ordinal
                    depth10_ordinal = depth10_spool.insert_many(
                        batch,
                        start_ordinal=depth10_ordinal,
                    )
                    depth10_ts_spool.insert_many([int(item.ts_event) for item in batch])

                depth_metrics = convert_depth_v2_streaming(
                    venue,
                    symbol,
                    date_str,
                    iid,
                    pp,
                    sp,
                    on_deltas_batch=on_deltas_batch,
                    on_depth10_batch=on_depth10_batch,
                    batch_size=WRITE_BATCH_SIZE,
                    emit_depth10=emit_depth10,
                    depth10_interval_sec=depth10_interval_sec,
                    derived_depth_snapshot_levels=requested_depth_snapshot_levels,
                )
                depth10_ts_spool.commit()
                total_bad += depth_metrics.bad_lines

                # Aggregate bad_lines tracking data from depth
                if depth_metrics.bad_lines > 0:
                    sym_key = f"{venue}/{symbol}"
                    bad_lines_by_venue_symbol[sym_key] = (
                        bad_lines_by_venue_symbol.get(sym_key, 0)
                        + depth_metrics.bad_lines
                    )
                for exc_type, count in depth_metrics.bad_lines_by_exception_type.items():
                    bad_lines_by_exception_type[exc_type] = (
                        bad_lines_by_exception_type.get(exc_type, 0) + count
                    )
                for rec_type, count in depth_metrics.bad_lines_by_record_type.items():
                    bad_lines_by_record_type[rec_type] = (
                        bad_lines_by_record_type.get(rec_type, 0) + count
                    )
                bad_line_examples.extend(depth_metrics.bad_line_examples)

                deltas_written = 0
                depth10_written = 0
                if deltas_spool.count:
                    deltas_written = _write_object_spool(catalog, deltas_spool)
                    converted_order_book_delta_symbols.add(f"{venue}/{symbol}")
                    _update_ts_range(
                        ts_ranges["order_book_deltas"],
                        depth_metrics.first_ts_ns,
                        depth_metrics.last_ts_ns,
                    )
                if depth10_spool.count:
                    depth10_written = _write_object_spool(catalog, depth10_spool)
                    converted_order_book_depth_symbols.add(f"{venue}/{symbol}")
                    _update_ts_range(
                        ts_ranges["order_book_depths"],
                        depth_metrics.first_ts_ns,
                        depth_metrics.last_ts_ns,
                    )

                v_delta_events += deltas_written
                v_depth10 += depth10_written
                v_derived_depth_snapshots += depth_metrics.derived_depth_snapshots_written
                v_snapshot_seeds += depth_metrics.snapshot_seed_count
                v_resyncs += depth_metrics.resync_count
                v_desyncs += depth_metrics.desync_events
                v_fenced_ranges += len(depth_metrics.fenced_ranges)
                extra_raw_partitions_scanned.update(depth_metrics.extra_raw_partitions_scanned)
                records_imported_from_previous_folder += depth_metrics.records_imported_from_previous_folder
                records_imported_from_next_folder += depth_metrics.records_imported_from_next_folder
                records_dropped_outside_target_utc += depth_metrics.records_dropped_outside_target_utc
                duplicate_records_suppressed += depth_metrics.duplicate_records_suppressed
                v_records_imported_from_previous_folder += depth_metrics.records_imported_from_previous_folder
                v_records_imported_from_next_folder += depth_metrics.records_imported_from_next_folder
                v_records_dropped_outside_target_utc += depth_metrics.records_dropped_outside_target_utc
                v_duplicate_records_suppressed += depth_metrics.duplicate_records_suppressed
                if depth_metrics.carried_seed_from_previous_day:
                    carried_seed_symbol_count += 1
                    v_carried_seed_symbol_count += 1
                if depth_metrics.synthetic_opening_snapshot_written:
                    synthetic_opening_snapshot_count += 1
                    v_synthetic_opening_snapshot_count += 1
                if (
                    depth_metrics.depth_update_record_count > 0
                    and depth_metrics.snapshot_seed_count == 0
                    and not depth_metrics.carried_seed_from_previous_day
                ):
                    standalone_depth_day = False
                fence_summary = _summarize_fences(depth_metrics.fenced_ranges)
                v_fenced_ranges_low += fence_summary["fenced_ranges_low"]
                v_fenced_ranges_medium += fence_summary["fenced_ranges_medium"]
                v_fenced_ranges_high += fence_summary["fenced_ranges_high"]
                v_unrecovered_fences += fence_summary["unrecovered_fences"]
                v_bootstrap_fences += fence_summary["bootstrap_fences"]
                v_shutdown_fences += fence_summary["shutdown_fences"]
                v_reconnect_fences += fence_summary["reconnect_fences"]
                v_utc_day_rollover_fences += fence_summary["utc_day_rollover_fences"]
                v_real_desync_fences += fence_summary["real_desync_fences"]
                v_unrecovered_real_fences += fence_summary["unrecovered_real_fences"]
                gap_diag = _build_gap_diagnostics(
                    venue,
                    symbol,
                    date_str,
                    depth10_gap_counts=depth10_ts_spool.gap_counts(),
                )
                v_depth_gap_warnings_over_60s += int(gap_diag["depth_gap_count_over_60s"])
                per_symbol_gap_diagnostics[f"{venue}/{symbol}"] = gap_diag
                gap_offender = _real_gap_offender_entry(f"{venue}/{symbol}", gap_diag)
                if gap_offender is not None:
                    top_real_gap_candidates.append(gap_offender)
                    v_top_real_gap_candidates.append(gap_offender)
                sym_has_depth = deltas_written > 0 or depth10_written > 0
                per_symbol_depth[f"{venue}/{symbol}"] = {
                    "raw_record_count": depth_metrics.raw_record_count,
                    "snapshot_seed_count": depth_metrics.snapshot_seed_count,
                    "depth_update_record_count": depth_metrics.depth_update_record_count,
                    "sync_state_record_count": depth_metrics.sync_state_record_count,
                    "stream_lifecycle_record_count": depth_metrics.stream_lifecycle_record_count,
                    "deltas_written": int(deltas_written),
                    "depth10_written": int(depth10_written),
                    "derived_depth_snapshots_written": depth_metrics.derived_depth_snapshots_written,
                    "derived_depth_snapshot_type": depth_metrics.derived_depth_snapshot_type,
                    "derived_depth_snapshot_levels": depth_metrics.derived_depth_snapshot_levels,
                    "requested_depth_snapshot_levels": depth_metrics.requested_depth_snapshot_levels,
                    "requested_depth_snapshot_levels_applied": depth_metrics.requested_depth_snapshot_levels_applied,
                    "fenced_ranges": len(depth_metrics.fenced_ranges),
                    **fence_summary,
                    **gap_diag,
                    "desync_events": depth_metrics.desync_events,
                    "resync_count": depth_metrics.resync_count,
                    "first_depth_ts_ns": depth_metrics.first_ts_ns,
                    "last_depth_ts_ns": depth_metrics.last_ts_ns,
                    "will_create_l2": deltas_written > 0,
                    "bad_lines": depth_metrics.bad_lines,
                    "carried_seed_from_previous_day": depth_metrics.carried_seed_from_previous_day,
                    "carried_seed_date": depth_metrics.carried_seed_date,
                    "carried_seed_session_id": depth_metrics.carried_seed_session_id,
                    "carried_seed_last_update_id": depth_metrics.carried_seed_last_update_id,
                    "carry_replay_record_count": depth_metrics.carry_replay_record_count,
                    "carry_recovery_failed_reason": depth_metrics.carry_recovery_failed_reason,
                    "synthetic_opening_snapshot_written": depth_metrics.synthetic_opening_snapshot_written,
                    "timestamp_repartition_enabled": depth_metrics.timestamp_repartition_enabled,
                    "extra_raw_partitions_scanned": depth_metrics.extra_raw_partitions_scanned,
                    "records_imported_from_previous_folder": depth_metrics.records_imported_from_previous_folder,
                    "records_imported_from_next_folder": depth_metrics.records_imported_from_next_folder,
                    "records_dropped_outside_target_utc": depth_metrics.records_dropped_outside_target_utc,
                    "duplicate_records_suppressed": depth_metrics.duplicate_records_suppressed,
                }
                if depth_metrics.fenced_ranges:
                    annotated_fences = _annotated_fence_examples(depth_metrics.fenced_ranges)
                    per_symbol_fenced_ranges[f"{venue}/{symbol}"] = {
                        "fenced_ranges": len(depth_metrics.fenced_ranges),
                        **fence_summary,
                        "examples": annotated_fences[:3],
                        "lifecycle_examples": [
                            fence
                            for fence in annotated_fences
                            if fence["classification"] in {"bootstrap", "shutdown", "utc_day_rollover"}
                        ][:3],
                        "real_examples": [
                            fence
                            for fence in annotated_fences
                            if fence["classification"] in {"reconnect", "real_desync"}
                        ][:3],
                        # Issue #20 Phase 1 correction: `examples` above is
                        # truncated to 3 for human readability and cannot by
                        # itself prove candidate/reference equivalence for a
                        # symbol with more than 3 fences. `canonical_count`/
                        # `canonical_digest` cover the COMPLETE
                        # `depth_metrics.fenced_ranges` list (already fully
                        # materialized in memory by this point — no new
                        # full-day materialization is introduced) and are
                        # what validation.validate_catalog_equivalence
                        # actually gates equivalence on.
                        "canonical_count": len(depth_metrics.fenced_ranges),
                        "canonical_digest": canonical_fence_digest(depth_metrics.fenced_ranges),
                    }
                else:
                    per_symbol_fenced_ranges[f"{venue}/{symbol}"] = {
                        "fenced_ranges": 0,
                        "canonical_count": 0,
                        "canonical_digest": canonical_fence_digest([]),
                    }

            # ── track data presence ───────────────────────────────────
            iid_str = str(iid)
            if sym_has_trade_ticks:
                instruments_with_trades.append(iid_str)
            if sym_has_depth:
                instruments_with_depth.append(iid_str)
            if not sym_has_trades and not sym_has_depth:
                instruments_with_no_data.append(iid_str)

        total_trades += v_trades
        total_delta_events += v_delta_events
        total_depth10 += v_depth10
        total_derived_depth_snapshots += v_derived_depth_snapshots
        total_snapshot_seeds += v_snapshot_seeds
        total_resyncs += v_resyncs
        total_desyncs += v_desyncs
        total_fenced_ranges += v_fenced_ranges
        total_fenced_ranges_low += v_fenced_ranges_low
        total_fenced_ranges_medium += v_fenced_ranges_medium
        total_fenced_ranges_high += v_fenced_ranges_high
        total_unrecovered_fences += v_unrecovered_fences
        total_bootstrap_fences += v_bootstrap_fences
        total_shutdown_fences += v_shutdown_fences
        total_reconnect_fences += v_reconnect_fences
        total_utc_day_rollover_fences += v_utc_day_rollover_fences
        total_real_desync_fences += v_real_desync_fences
        total_unrecovered_real_fences += v_unrecovered_real_fences
        total_depth_gap_warnings_over_60s += v_depth_gap_warnings_over_60s
        symbols_processed[venue] = v_symbols
        venue_reports[venue] = {
            "symbols": v_symbols,
            "trades_written": v_trades,
            "trade_raw_record_count": v_trade_raw_records,
            "trade_raw_trade_record_count": v_trade_raw_trade_records,
            "trade_raw_lifecycle_record_count": v_trade_raw_lifecycle_records,
            "symbols_with_trades": v_symbols_with_trades,
            "symbols_without_trades": v_symbols_without_trades,
            "symbols_with_trade_ticks": v_symbols_with_trade_ticks,
            "symbols_without_trade_ticks": v_symbols_without_trade_ticks,
            "lifecycle_only_symbols": v_lifecycle_only_symbols,
            "top_symbols_by_trade_count": [
                {"symbol": sym, "trade_record_count": count}
                for sym, count in sorted(
                    v_top_symbols_by_trade_count,
                    key=lambda item: (-item[1], item[0]),
                )[:10]
            ],
            "delta_events_written": v_delta_events,
            "depth10_written": v_depth10,
            "derived_depth_snapshots_written": v_derived_depth_snapshots,
            "snapshot_seed_count": v_snapshot_seeds,
            "resync_count": v_resyncs,
            "desync_events": v_desyncs,
            "fenced_ranges": v_fenced_ranges,
            "fenced_ranges_low": v_fenced_ranges_low,
            "fenced_ranges_medium": v_fenced_ranges_medium,
            "fenced_ranges_high": v_fenced_ranges_high,
            "unrecovered_fences": v_unrecovered_fences,
            "bootstrap_fences": v_bootstrap_fences,
            "shutdown_fences": v_shutdown_fences,
            "reconnect_fences": v_reconnect_fences,
            "utc_day_rollover_fences": v_utc_day_rollover_fences,
            "real_desync_fences": v_real_desync_fences,
            "unrecovered_real_fences": v_unrecovered_real_fences,
            "depth_gap_warnings_over_60s": v_depth_gap_warnings_over_60s,
            "records_imported_from_previous_folder": v_records_imported_from_previous_folder,
            "records_imported_from_next_folder": v_records_imported_from_next_folder,
            "records_dropped_outside_target_utc": v_records_dropped_outside_target_utc,
            "duplicate_records_suppressed": v_duplicate_records_suppressed,
            "carried_seed_symbol_count": v_carried_seed_symbol_count,
            "synthetic_opening_snapshot_count": v_synthetic_opening_snapshot_count,
            "top_real_gap_offenders": _top_real_gap_offenders(v_top_real_gap_candidates),
        }

    staging_publication: Dict[str, Any] | None = None
    staging_status_override: str | None = None
    staging_error: str | None = None
    if staging and total_trades + total_delta_events > 0:
        try:
            staged_files = _validate_staging_catalog(staging_dir, date_str)
            staging_publication = _publish_staged_catalog_for_date(
                staging_dir=staging_dir,
                target_root=target_root,
                target_date_str=date_str,
                staged_files=staged_files,
            )
            shutil.rmtree(staging_dir, ignore_errors=True)
        except StagingValidationError as exc:
            staging_status_override = "staging_validation_failed"
            staging_error = str(exc)
            logger.error("Staging validation failed; live catalog unchanged: %s", exc)
        except StagingPublishError as exc:
            staging_status_override = "staging_publish_failed"
            staging_error = str(exc)
            logger.error("Staging publish failed: %s", exc)
    elif staging:
        logger.info(
            "Staging conversion produced no publishable trade/depth data; "
            "live catalog unchanged."
        )
        shutil.rmtree(staging_dir, ignore_errors=True)

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
    readiness = build_readiness_summary(
        per_symbol_trade,
        per_symbol_depth,
        min_trade_records_for_full_ready=MIN_TRADE_RECORDS_FOR_FULL_READY,
    )

    # ── readiness classification (from actual conversion output) ──────
    readiness_classification: Dict[str, object] = {
        "full_ready": [],
        "l2_ready": [],
        "trade_only": [],
        "not_ready": [],
        "full_ready_count": 0,
        "l2_ready_count": 0,
        "trade_only_count": 0,
        "not_ready_count": 0,
        "by_venue": {},
    }
    for key, info in sorted(readiness["per_symbol"].items()):
        cls = info["readiness"]
        readiness_classification[cls].append(key)  # type: ignore[union-attr]
        readiness_classification[f"{cls}_count"] += 1  # type: ignore[operator]
        sym_venue = key.split("/")[0]
        bv = readiness_classification["by_venue"].setdefault(  # type: ignore[union-attr]
            sym_venue,
            {"full_ready_count": 0, "l2_ready_count": 0, "trade_only_count": 0, "not_ready_count": 0},
        )
        bv[f"{cls}_count"] += 1

    # ── by-venue sets for conversion_integrity ────────────────────────
    def _by_venue_names(key_set: set, venue_name: str) -> List[str]:
        return sorted(
            sym for key in key_set
            if (parts := key.split("/", 1)) and parts[0] == venue_name
            for sym in [parts[1]]
        )

    all_venues = sorted(universe.keys())
    conv_int_expected_by_venue = {v: sorted(universe[v]) for v in all_venues}
    conv_int_raw_trade_by_venue = {v: _by_venue_names(raw_trade_symbols_set, v) for v in all_venues}
    conv_int_raw_depth_by_venue = {v: _by_venue_names(raw_depth_symbols_set, v) for v in all_venues}
    conv_int_conv_trade_by_venue = {v: _by_venue_names(converted_trade_symbols, v) for v in all_venues}
    conv_int_conv_depth_by_venue = {v: _by_venue_names(converted_order_book_delta_symbols, v) for v in all_venues}
    conv_int_conv_depth10_by_venue = {v: _by_venue_names(converted_order_book_depth_symbols, v) for v in all_venues}
    conv_int_miss_raw_trade_by_venue = {
        v: sorted(set(universe[v]) - set(conv_int_raw_trade_by_venue[v])) for v in all_venues
    }
    conv_int_miss_raw_depth_by_venue = {
        v: sorted(set(universe[v]) - set(conv_int_raw_depth_by_venue[v])) for v in all_venues
    }
    conv_int_miss_conv_trade_by_venue = {
        v: sorted(set(universe[v]) - set(conv_int_conv_trade_by_venue[v])) for v in all_venues
    }
    conv_int_miss_conv_depth_by_venue = {
        v: sorted(set(universe[v]) - set(conv_int_conv_depth_by_venue[v])) for v in all_venues
    }
    conv_int_miss_conv_depth10_by_venue = {
        v: sorted(set(universe[v]) - set(conv_int_conv_depth10_by_venue[v])) for v in all_venues
    }

    converted_trade_symbols_sorted = sorted(converted_trade_symbols)
    converted_order_book_delta_symbols_sorted = sorted(converted_order_book_delta_symbols)
    converted_order_book_depth_symbols_sorted = sorted(converted_order_book_depth_symbols)
    conversion_integrity = {
        "date_converted": date_str,
        "catalog_root_written": str(target_root),
        "staging": staging,
        "emit_depth10": emit_depth10,
        "expected_symbols_total": len(expected_symbol_keys),
        "expected_symbols_by_venue": conv_int_expected_by_venue,
        "raw_trade_symbols_by_venue": conv_int_raw_trade_by_venue,
        "raw_depth_symbols_by_venue": conv_int_raw_depth_by_venue,
        "converted_trade_symbols_by_venue": conv_int_conv_trade_by_venue,
        "converted_depth_symbols_by_venue": conv_int_conv_depth_by_venue,
        "converted_depth10_symbols_by_venue": conv_int_conv_depth10_by_venue,
        "missing_raw_trade_symbols_by_venue": conv_int_miss_raw_trade_by_venue,
        "missing_raw_depth_symbols_by_venue": conv_int_miss_raw_depth_by_venue,
        "missing_converted_trade_symbols_by_venue": conv_int_miss_conv_trade_by_venue,
        "missing_converted_depth_symbols_by_venue": conv_int_miss_conv_depth_by_venue,
        "missing_converted_depth10_symbols_by_venue": conv_int_miss_conv_depth10_by_venue,
        # Flat lists kept for backward compatibility
        "raw_depth_symbols": raw_depth_symbols,
        "raw_trade_symbols": raw_trade_symbols,
        "converted_trade_symbols": converted_trade_symbols_sorted,
        "converted_order_book_delta_symbols": converted_order_book_delta_symbols_sorted,
        "converted_order_book_depth_symbols": converted_order_book_depth_symbols_sorted,
        "missing_depth_after_convert": sorted(expected_symbol_keys - set(converted_order_book_delta_symbols_sorted)),
        "missing_trade_after_convert": sorted(expected_symbol_keys - set(converted_trade_symbols_sorted)),
        "missing_depth10_after_convert": sorted(expected_symbol_keys - set(converted_order_book_depth_symbols_sorted)),
        "overwrite_enabled": overwrite_enabled,
        "warnings": integrity_warnings,
    }
    if derived_snapshot_warning and derived_snapshot_warning not in integrity_warnings:
        integrity_warnings.append(derived_snapshot_warning)

    # ── report ────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    gap_warning_counts = {
        "depth_gap_count_over_60s": total_depth_gap_warnings_over_60s,
    }
    top_real_gap_offenders = _top_real_gap_offenders(top_real_gap_candidates)
    gap_warning_counts["top_real_gap_offenders"] = top_real_gap_offenders
    fence_severity_counts = {
        "fenced_ranges_low": total_fenced_ranges_low,
        "fenced_ranges_medium": total_fenced_ranges_medium,
        "fenced_ranges_high": total_fenced_ranges_high,
        "unrecovered_fences": total_unrecovered_fences,
        "bootstrap_fences": total_bootstrap_fences,
        "shutdown_fences": total_shutdown_fences,
        "reconnect_fences": total_reconnect_fences,
        "utc_day_rollover_fences": total_utc_day_rollover_fences,
        "real_desync_fences": total_real_desync_fences,
        "unrecovered_real_fences": total_unrecovered_real_fences,
    }
    report = {
        "date": date_str,
        "timestamp": local_now_iso(),
        "runtime_sec": round(elapsed, 2),
        "status": staging_status_override or (
            "ok" if (total_trades + total_delta_events) > 0 else "empty"
        ),
        "architecture": "deterministic_native",
        "instruments_written": len(all_instruments),
        "total_trades_written": total_trades,
        "total_order_book_deltas_written": total_delta_events,
        "total_depth10_written": total_depth10,
        "total_derived_depth_snapshots_written": total_derived_depth_snapshots,
        **_derived_snapshot_report_fields(
            requested_depth_snapshot_levels,
            applied_depth_snapshot_levels,
            derived_snapshot_warning,
        ),
        "bad_lines": total_bad,
        "bad_lines_by_exception_type": bad_lines_by_exception_type,
        "bad_lines_by_record_type": bad_lines_by_record_type,
        "bad_lines_by_venue_symbol": bad_lines_by_venue_symbol,
        "bad_line_examples": bad_line_examples[:20],  # Keep first 20 examples
        "zero_size_trade_skipped_total": sum(
            zero_size_trade_skipped_by_venue_symbol.values()
        ),
        "zero_size_trade_skipped_by_venue_symbol": zero_size_trade_skipped_by_venue_symbol,
        "zero_size_trade_examples": zero_size_trade_examples[:20],
        "snapshot_seed_count": total_snapshot_seeds,
        "resync_count": total_resyncs,
        "desync_events": total_desyncs,
        "fenced_ranges_total": total_fenced_ranges,
        **fence_severity_counts,
        "standalone_depth_day": standalone_depth_day,
        "timestamp_repartition_enabled": True,
        "extra_raw_partitions_scanned": sorted(extra_raw_partitions_scanned),
        "records_imported_from_previous_folder": records_imported_from_previous_folder,
        "records_imported_from_next_folder": records_imported_from_next_folder,
        "records_dropped_outside_target_utc": records_dropped_outside_target_utc,
        "duplicate_records_suppressed": duplicate_records_suppressed,
        "carried_seed_symbol_count": carried_seed_symbol_count,
        "synthetic_opening_snapshot_count": synthetic_opening_snapshot_count,
        "gap_warning_counts": gap_warning_counts,
        "top_real_gap_offenders": top_real_gap_offenders,
        "per_symbol_fenced_ranges": per_symbol_fenced_ranges,
        "per_symbol_gap_diagnostics": per_symbol_gap_diagnostics,
        "per_symbol_trade": per_symbol_trade,
        "per_symbol_depth": per_symbol_depth,
        "data_presence": data_presence,
        "readiness": readiness,
        "readiness_classification": readiness_classification,
        "conversion_integrity": conversion_integrity,
        "futures_enabled": "BINANCE_USDTF" in universe,
        "symbols_processed": symbols_processed,
        "venues": venue_reports,
        "ts_ranges": ts_ranges,
        "depth_settings": {
            "emit_depth10": emit_depth10,
            "depth10_interval_sec": depth10_interval_sec,
            "emit_derived_depth_snapshots": emit_depth10,
            "derived_depth_snapshot_interval_sec": depth10_interval_sec,
            "derived_depth_snapshot_levels": applied_depth_snapshot_levels,
            "requested_depth_snapshot_levels": requested_depth_snapshot_levels,
            "requested_depth_snapshot_levels_applied": applied_depth_snapshot_levels,
            "snapshot_seed_limit": PHASE2_SNAPSHOT_LIMIT,
        },
        "catalog_root": str(target_root),
    }
    if staging_publication is not None:
        report["staging_publication"] = staging_publication
    if staging_error is not None:
        report["staging_error"] = staging_error

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


def _write_object_spool(
    catalog: ParquetDataCatalog,
    spool: ObjectSpool,
    *,
    batch_size: int = WRITE_BATCH_SIZE,
) -> int:
    """Write a per-symbol object spool in the old ts_init-sorted order."""
    spool.commit()
    written = 0
    for batch in spool.iter_batches(batch_size):
        catalog.write_data(batch)
        written += len(batch)
    return written


def _empty_report(date_str: str, t0: float, **kwargs) -> dict:
    return {
        "date": date_str,
        "runtime_sec": round(time.time() - t0, 2),
        **kwargs,
    }


def _target_date(target_date_str: str):
    return datetime.strptime(target_date_str, "%Y-%m-%d").date()


def _validate_staging_catalog(
    staging_root: Path,
    target_date_str: str,
) -> List[Path]:
    """Validate staged output before touching the live catalog.

    Date-scoped Nautilus parquet files are accepted only when their timestamp
    range overlaps the requested UTC day. That mirrors the existing date-scoped
    purge semantics and still allows legitimate midnight carry/reseed files.
    """
    if not staging_root.exists():
        raise StagingValidationError(f"staging catalog is missing: {staging_root}")

    try:
        ParquetDataCatalog(str(staging_root)).instruments()
    except Exception as exc:
        raise StagingValidationError(
            f"Nautilus could not read staged instrument metadata: {exc}"
        ) from exc

    data_root = staging_root / "data"
    if not data_root.exists():
        raise StagingValidationError(
            f"staging catalog has no data directory: {data_root}"
        )

    staged_files = sorted(path for path in data_root.rglob("*.parquet") if path.is_file())
    if not staged_files:
        raise StagingValidationError("staging catalog contains no parquet files")

    target = _target_date(target_date_str)
    metadata_files = 0
    for path in staged_files:
        rel = path.relative_to(staging_root)
        if len(rel.parts) != 4 or rel.parts[0] != "data":
            raise StagingValidationError(
                f"unexpected staged parquet layout: {rel}"
            )

        catalog_type = rel.parts[1]
        parsed = _parse_parquet_date_range(path.name)
        if parsed is None:
            raise StagingValidationError(
                f"cannot date-scope staged parquet filename: {rel}"
            )
        start_date, end_date = parsed

        if catalog_type in DATE_SCOPED_CATALOG_TYPES:
            if start_date.year < 2000 or end_date.year < 2000:
                raise StagingValidationError(
                    f"date-scoped staged parquet has metadata-like timestamp: {rel}"
                )
            if not (start_date <= target <= end_date):
                raise StagingValidationError(
                    f"staged parquet falls outside requested UTC date "
                    f"{target_date_str}: {rel}"
                )
            continue

        if catalog_type in METADATA_CATALOG_TYPES:
            metadata_files += 1
            if start_date.year >= 2000 or end_date.year >= 2000:
                raise StagingValidationError(
                    f"instrument metadata parquet is not epoch-scoped: {rel}"
                )
            continue

        raise StagingValidationError(
            f"unsupported staged catalog type {catalog_type!r}: {rel}"
        )

    if metadata_files == 0:
        raise StagingValidationError(
            "staging catalog contains no instrument metadata parquet files"
        )

    logger.info(
        "Validated staging catalog %s for UTC date %s with %d parquet files",
        staging_root,
        target_date_str,
        len(staged_files),
    )
    return staged_files


def _collect_live_catalog_replacements(
    *,
    target_root: Path,
    target_date_str: str,
    staged_files: Sequence[Path],
    staging_root: Path,
) -> List[Path]:
    """Return live files that must be backed up before staged publication."""
    target = _target_date(target_date_str)
    replacements: set[Path] = set()

    for staged_path in staged_files:
        rel = staged_path.relative_to(staging_root)
        catalog_type = rel.parts[1]
        instrument_id = rel.parts[2]
        live_dir = target_root / "data" / catalog_type / instrument_id
        if not live_dir.exists():
            continue

        if catalog_type in DATE_SCOPED_CATALOG_TYPES:
            for live_path in live_dir.glob("*.parquet"):
                parsed = _parse_parquet_date_range(live_path.name)
                if parsed is None:
                    logger.warning(
                        "Preserving unparseable live parquet during staged publish: %s",
                        live_path,
                    )
                    continue
                start_date, end_date = parsed
                if start_date <= target <= end_date:
                    replacements.add(live_path)
            continue

        if catalog_type in METADATA_CATALOG_TYPES:
            replacements.update(live_dir.glob("*.parquet"))

    replacement_list = sorted(replacements)
    for path in replacement_list:
        logger.info("Staged publish will replace live parquet: %s", path)
    if not replacement_list:
        logger.info(
            "Staged publish found no existing live target-date parquet files "
            "to replace for %s.",
            target_date_str,
        )
    return replacement_list


def _publish_staged_catalog_for_date(
    *,
    staging_dir: Path,
    target_root: Path,
    target_date_str: str,
    staged_files: Sequence[Path],
) -> Dict[str, Any]:
    """Publish staged parquet files into the live catalog with rollback."""
    if not staged_files:
        raise StagingPublishError("no staged files were provided for publication")

    target_root.parent.mkdir(parents=True, exist_ok=True)
    if target_root.exists() and not target_root.is_dir():
        raise StagingPublishError(f"live catalog root is not a directory: {target_root}")
    target_root.mkdir(parents=True, exist_ok=True)

    replacement_paths = _collect_live_catalog_replacements(
        target_root=target_root,
        target_date_str=target_date_str,
        staged_files=staged_files,
        staging_root=staging_dir,
    )
    replacement_set = set(replacement_paths)
    for staged_path in staged_files:
        dest = target_root / staged_path.relative_to(staging_dir)
        if dest.exists() and dest not in replacement_set:
            raise StagingPublishError(
                "staged publish would overwrite an unscoped live parquet "
                f"without backup: {dest}"
            )

    backup_root = Path(
        tempfile.mkdtemp(
            prefix=f"{target_root.name}.publish-backup.{target_date_str}.",
            dir=target_root.parent,
        )
    )
    moved_backups: List[Tuple[Path, Path]] = []
    published_files: List[Tuple[Path, Path]] = []

    logger.info(
        "Publishing staged UTC date %s into live catalog %s. "
        "The catalog root remains in place; only listed parquet files are replaced.",
        target_date_str,
        target_root,
    )
    try:
        for live_path in replacement_paths:
            backup_path = backup_root / live_path.relative_to(target_root)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(live_path, backup_path)
            moved_backups.append((live_path, backup_path))
            logger.info("Backed up live parquet before staged publish: %s", live_path)

        for staged_path in staged_files:
            dest = target_root / staged_path.relative_to(staging_dir)
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged_path, dest)
            published_files.append((staged_path, dest))
            logger.info("Published staged parquet into live catalog: %s", dest)
    except Exception as exc:
        rollback_errors: List[str] = []
        for staged_path, dest in reversed(published_files):
            try:
                staged_path.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    os.replace(dest, staged_path)
                    logger.warning("Rolled back published staged parquet: %s", dest)
            except Exception as rollback_exc:
                rollback_errors.append(
                    f"failed to move published file {dest} back to staging: {rollback_exc}"
                )

        for live_path, backup_path in reversed(moved_backups):
            try:
                live_path.parent.mkdir(parents=True, exist_ok=True)
                if backup_path.exists():
                    os.replace(backup_path, live_path)
                    logger.warning("Restored live parquet from staged backup: %s", live_path)
            except Exception as rollback_exc:
                rollback_errors.append(
                    f"failed to restore backup {backup_path} to {live_path}: {rollback_exc}"
                )

        if rollback_errors:
            logger.critical(
                "Staged publish rollback incomplete. Recoverable backup kept at %s. "
                "Errors: %s",
                backup_root,
                "; ".join(rollback_errors),
            )
            raise StagingPublishError(
                "staged publish failed and rollback was incomplete; "
                f"recoverable backup remains at {backup_root}: {'; '.join(rollback_errors)}"
            ) from exc

        shutil.rmtree(backup_root, ignore_errors=True)
        raise StagingPublishError(
            f"staged publish failed and live catalog was rolled back: {exc}"
        ) from exc

    shutil.rmtree(backup_root, ignore_errors=True)
    logger.info(
        "Staged publish complete for %s: %d prior live parquet files replaced, "
        "%d staged parquet files installed. Unlisted live catalog files were preserved.",
        target_date_str,
        len(replacement_paths),
        len(published_files),
    )
    return {
        "target_date": target_date_str,
        "live_catalog_root": str(target_root),
        "replaced_live_parquet_count": len(replacement_paths),
        "published_staged_parquet_count": len(published_files),
        "replaced_live_parquets": [str(path) for path in replacement_paths],
        "published_live_parquets": [str(dest) for _, dest in published_files],
        "preserved_scope": (
            "All live catalog files not listed in replaced_live_parquets were preserved."
        ),
    }


def _derived_snapshot_report_fields(
    requested_levels: int,
    applied_levels: int,
    warning: Optional[str],
) -> Dict[str, object]:
    fields: Dict[str, object] = {
        "full_depth_source": FULL_DEPTH_SOURCE,
        "derived_depth_snapshot_type": DERIVED_DEPTH_SNAPSHOT_TYPE,
        "derived_depth_snapshot_levels": applied_levels,
        "requested_depth_snapshot_levels": requested_levels,
        "requested_depth_snapshot_levels_applied": applied_levels,
        "snapshot_seed_limit": PHASE2_SNAPSHOT_LIMIT,
    }
    if warning:
        fields["derived_depth_snapshot_warning"] = warning
    return fields


def _record_ts_ns(rec: dict, *, trade: bool = False) -> Optional[int]:
    ts_ms = rec.get("ts_trade_ms") if trade else None
    ts_ms = ts_ms or rec.get("ts_event_ms") or rec.get("exchange_ts_ms")
    if ts_ms is not None:
        return int(ts_ms) * 1_000_000
    ts_recv_ns = rec.get("ts_recv_ns")
    return int(ts_recv_ns) if ts_recv_ns is not None else None


def _gap_counts(timestamps_ns: List[int]) -> Dict[str, object]:
    if len(timestamps_ns) < 2:
        return {
            "max_gap_sec": 0.0,
            "gap_count_over_1s": 0,
            "gap_count_over_5s": 0,
            "gap_count_over_60s": 0,
        }
    ordered = sorted(timestamps_ns)
    gaps = [
        (ordered[i] - ordered[i - 1]) / 1_000_000_000.0
        for i in range(1, len(ordered))
        if ordered[i] >= ordered[i - 1]
    ]
    if not gaps:
        return {
            "max_gap_sec": 0.0,
            "gap_count_over_1s": 0,
            "gap_count_over_5s": 0,
            "gap_count_over_60s": 0,
        }
    return {
        "max_gap_sec": round(max(gaps), 6),
        "gap_count_over_1s": sum(1 for gap in gaps if gap > 1.0),
        "gap_count_over_5s": sum(1 for gap in gaps if gap > 5.0),
        "gap_count_over_60s": sum(1 for gap in gaps if gap > 60.0),
    }


def _build_gap_diagnostics(
    venue: str,
    symbol: str,
    date_str: str,
    depth10s: Sequence | None = None,
    *,
    depth10_gap_counts: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    lifecycle_boundaries: List[Dict[str, object]] = []

    with (
        TimestampSpool(prefix="cryptorecorder-gap-depth-") as depth_ts_spool,
        TimestampSpool(prefix="cryptorecorder-gap-trade-") as trade_ts_spool,
    ):
        for rec in stream_raw_records(venue, symbol, "depth_v2", date_str):
            record_type = rec.get("record_type", "depth_update")
            if record_type == "depth_update":
                ts_ns = _record_ts_ns(rec)
                if ts_ns is not None:
                    depth_ts_spool.insert(ts_ns)
            elif record_type == "stream_lifecycle":
                lifecycle_boundaries.append(rec)

        for rec in stream_raw_records(venue, symbol, "trade_v2", date_str):
            if rec.get("record_type", "trade") == "trade":
                ts_ns = _record_ts_ns(rec, trade=True)
                if ts_ns is not None:
                    trade_ts_spool.insert(ts_ns)

        depth_ts_spool.commit()
        trade_ts_spool.commit()
        depth_gaps = depth_ts_spool.gap_counts()
        trade_gaps = trade_ts_spool.gap_counts()

    if depth10_gap_counts is not None:
        depth10_gaps = depth10_gap_counts
    else:
        depth10_gaps = _gap_counts([int(d.ts_event) for d in (depth10s or [])])
    boundary_counts = _classify_lifecycle_boundaries(lifecycle_boundaries)
    return {
        "max_depth_update_gap_sec": depth_gaps["max_gap_sec"],
        "depth_gap_count_over_1s": depth_gaps["gap_count_over_1s"],
        "depth_gap_count_over_5s": depth_gaps["gap_count_over_5s"],
        "depth_gap_count_over_60s": depth_gaps["gap_count_over_60s"],
        "max_trade_gap_sec": trade_gaps["max_gap_sec"],
        "trade_gap_informational": True,
        "max_depth10_gap_sec": depth10_gaps["max_gap_sec"],
        **boundary_counts,
    }


def _classify_lifecycle_boundaries(boundaries: List[Dict[str, object]]) -> Dict[str, int]:
    shutdown_boundary_gap_count = 0
    reconnect_boundary_gap_count = 0
    seen_session_start = False

    for index, rec in enumerate(boundaries):
        reason = str(rec.get("reason", "")).lower()
        event = rec.get("event")
        if event == "session_start":
            session_id = rec.get("stream_session_id")
            try:
                session_number = int(session_id) if session_id is not None else None
            except (TypeError, ValueError):
                session_number = None
            if seen_session_start or (session_number is not None and session_number > 1):
                reconnect_boundary_gap_count += 1
            seen_session_start = True
            continue

        if event != "session_end":
            continue

        later_restart = any(
            later.get("event") == "session_start"
            for later in boundaries[index + 1 :]
        )
        if "websocket_closed" in reason and not later_restart:
            shutdown_boundary_gap_count += 1
        elif "reconnect" in reason and not later_restart:
            reconnect_boundary_gap_count += 1

    return {
        "session_boundary_gap_count": len(boundaries),
        "shutdown_boundary_gap_count": shutdown_boundary_gap_count,
        "reconnect_boundary_gap_count": reconnect_boundary_gap_count,
    }


def _normalize_fence_reason(reason: object) -> str:
    value = str(reason or "unknown").lower()
    if "utc_day_rollover" in value:
        return "utc_day_rollover"
    if "bootstrap" in value:
        return "bootstrap"
    if "websocket_closed" in value:
        return "websocket_closed"
    if "shutdown" in value:
        return "shutdown"
    if "continuity" in value:
        return "continuity_break"
    if "desync" in value:
        return "desynced"
    if "snapshot" in value:
        return "no_snapshot_seed"
    if "rate" in value or "resync_limit" in value:
        return "rate_limit_resync"
    return "unknown"


def _fence_time_increased(fence: Dict[str, object]) -> bool:
    try:
        start = int(fence.get("start_ts_ns") or 0)
        end = int(fence.get("end_ts_ns") or 0)
    except (TypeError, ValueError):
        return False
    return end > start


def _fence_category(fence: Dict[str, object]) -> str:
    reason = _normalize_fence_reason(fence.get("reason"))
    if reason == "utc_day_rollover":
        return "utc_day_rollover"
    if reason == "bootstrap":
        return "bootstrap"
    if reason == "shutdown":
        return "shutdown"
    if reason == "websocket_closed":
        if (
            fence.get("closed_by_session_change")
            or bool(fence.get("recovered"))
            or _fence_time_increased(fence)
        ):
            return "reconnect"
        return "shutdown"
    if "reconnect" in str(fence.get("reason", "")).lower():
        return "reconnect"
    return "real_desync"


def _fence_severity(fence: Dict[str, object]) -> str:
    category = _fence_category(fence)
    reason = _normalize_fence_reason(fence.get("reason"))
    recovered = bool(fence.get("recovered"))
    if category in {"bootstrap", "shutdown", "utc_day_rollover"}:
        return "low"
    if category == "reconnect":
        return "medium" if recovered else "low"
    if recovered and reason in {"continuity_break", "desynced", "rate_limit_resync"}:
        return "medium"
    if reason in {"continuity_break", "desynced", "no_snapshot_seed", "rate_limit_resync"}:
        return "high"
    return "medium" if recovered else "high"


def _summarize_fences(fences: List[Dict[str, object]]) -> Dict[str, int]:
    summary = {
        "fenced_ranges_low": 0,
        "fenced_ranges_medium": 0,
        "fenced_ranges_high": 0,
        "unrecovered_fences": 0,
        "bootstrap_fences": 0,
        "shutdown_fences": 0,
        "reconnect_fences": 0,
        "utc_day_rollover_fences": 0,
        "real_desync_fences": 0,
        "unrecovered_real_fences": 0,
    }
    for fence in fences:
        severity = _fence_severity(fence)
        category = _fence_category(fence)
        summary[f"fenced_ranges_{severity}"] += 1
        summary[f"{category}_fences"] += 1
        if category == "real_desync" and not bool(fence.get("recovered")):
            summary["unrecovered_real_fences"] += 1

    # Compatibility: readiness/audit consumers should treat this as a real
    # data-quality count, not as normal end-of-run lifecycle accounting.
    summary["unrecovered_fences"] = summary["unrecovered_real_fences"]
    return summary


def _annotated_fence_examples(
    fences: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    annotated: List[Dict[str, object]] = []
    for fence in fences:
        item = dict(fence)
        item["classification"] = _fence_category(fence)
        annotated.append(item)
    return annotated


def _real_gap_offender_entry(
    key: str,
    gap_diag: Dict[str, object],
) -> Optional[Dict[str, object]]:
    max_depth_gap = float(gap_diag.get("max_depth_update_gap_sec") or 0.0)
    depth_gaps_over_1s = int(gap_diag.get("depth_gap_count_over_1s") or 0)
    if max_depth_gap <= 1.0 and depth_gaps_over_1s <= 0:
        return None
    return {
        "symbol": key,
        "max_depth_update_gap_sec": max_depth_gap,
        "depth_gap_count_over_1s": depth_gaps_over_1s,
        "depth_gap_count_over_5s": int(gap_diag.get("depth_gap_count_over_5s") or 0),
        "depth_gap_count_over_60s": int(gap_diag.get("depth_gap_count_over_60s") or 0),
        "max_depth10_gap_sec": float(gap_diag.get("max_depth10_gap_sec") or 0.0),
    }


def _top_real_gap_offenders(
    candidates: List[Dict[str, object]],
    *,
    limit: int = 10,
) -> List[Dict[str, object]]:
    return sorted(
        candidates,
        key=lambda item: (
            -int(item.get("depth_gap_count_over_60s") or 0),
            -float(item.get("max_depth_update_gap_sec") or 0.0),
            -int(item.get("depth_gap_count_over_5s") or 0),
            -int(item.get("depth_gap_count_over_1s") or 0),
            str(item.get("symbol") or ""),
        ),
    )[:limit]


def _symbols_with_raw_record_type(
    universe: Dict[str, List[str]],
    date_str: str,
    *,
    channel: str,
    record_type: str,
) -> set[str]:
    symbols: set[str] = set()
    for venue, venue_symbols in universe.items():
        for symbol in venue_symbols:
            for rec in stream_raw_records(venue, symbol, channel, date_str):
                current_record_type = rec.get("record_type", "trade")
                if current_record_type == record_type:
                    symbols.add(f"{venue}/{symbol}")
                    break
    return symbols


def _save_report(report: dict) -> dict:
    rp = STATE_ROOT / "convert_reports" / f"{report['date']}.json"
    catalog_root = Path(report.get("catalog_root", NAUTILUS_CATALOG_ROOT))
    extra_rp = catalog_root.parent / "convert_reports" / f"{report['date']}.json"
    report["catalog_root"] = str(catalog_root)
    report["convert_report_extra_path"] = str(extra_rp)
    report["report_paths"] = [str(rp), str(extra_rp)]
    if "full_depth_source" not in report:
        requested = int(report.get("requested_depth_snapshot_levels", DERIVED_DEPTH_SNAPSHOT_LEVELS))
        applied = min(max(requested, 1), 10)
        warning = DERIVED_DEPTH_CAP_WARNING if requested != applied else None
        report.update(_derived_snapshot_report_fields(requested, applied, warning))

    payload = json.dumps(report, indent=2, default=str)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(payload)
    logger.info(f"Report → {rp}")
    if extra_rp != rp:
        extra_rp.parent.mkdir(parents=True, exist_ok=True)
        extra_rp.write_text(payload)
        logger.info(f"Report → {extra_rp}")
    return report


# ===================================================================
# CLI
# ===================================================================

def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Convert raw Binance JSONL → Nautilus ParquetDataCatalog (deterministic native)",
    )
    partial_ratio_help = f"{OVERWRITE_DEPTH_REFUSE_MIN_RATIO:.0%}".replace("%", "%%")
    ap.add_argument(
        "--date", type=str,
        help="Date to convert (YYYY-MM-DD). Default: yesterday UTC.",
    )
    ap.add_argument(
        "--staging", action="store_true",
        help=(
            "Write to an isolated staging catalog, validate it, then publish "
            "only target-date parquet files into the live catalog."
        ),
    )
    ap.add_argument(
        "--catalog-root",
        type=Path,
        help="Catalog root override. Default: configured NAUTILUS_CATALOG_ROOT.",
    )
    ap.add_argument(
        "--symbols",
        type=str,
        help="Optional comma-separated raw symbols to convert. Default: all resolved symbols.",
    )
    ap.add_argument(
        "--venues",
        type=str,
        help="Optional comma-separated venues to convert. Default: all resolved venues.",
    )
    ap.add_argument(
        "--emit-depth10",
        action="store_true",
        default=EMIT_DEPTH10_DEFAULT,
        help="Derive OrderBookDepth10 output from replayed book state.",
    )
    ap.add_argument(
        "--depth10-interval-sec",
        type=float,
        default=DEPTH10_INTERVAL_SEC,
        help="Minimum interval between derived depth10 snapshots.",
    )
    ap.add_argument(
        "--derived-depth-snapshot-levels",
        type=int,
        default=DERIVED_DEPTH_SNAPSHOT_LEVELS,
        help=(
            "Requested derived snapshot levels. Nautilus catalog output is "
            "currently capped to OrderBookDepth10."
        ),
    )
    ap.add_argument(
        "--allow-partial-overwrite",
        action="store_true",
        default=False,
        help=(
            "Allow overwriting catalog even when raw depth coverage is below "
            f"{partial_ratio_help} of expected symbols. "
            "Without this flag the conversion refuses when coverage is too low."
        ),
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
        catalog_root=args.catalog_root,
        staging=args.staging,
        emit_depth10=args.emit_depth10,
        depth10_interval_sec=args.depth10_interval_sec,
        derived_depth_snapshot_levels=args.derived_depth_snapshot_levels,
        allow_partial_overwrite=args.allow_partial_overwrite,
        symbols=[s.strip() for s in args.symbols.split(",")] if args.symbols else None,
        venues=[v.strip() for v in args.venues.split(",")] if args.venues else None,
    )
    return 0 if report.get("status") in ("ok", "no_data") else 1


if __name__ == "__main__":
    raise SystemExit(main())
