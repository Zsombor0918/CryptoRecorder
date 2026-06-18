"""
pipeline.generate_catalog — On-demand Nautilus catalog generation from replay_store.

Reads replay_store data and generates temporary Nautilus ParquetDataCatalog
for specific symbols, venues, and time windows.

Critical for semantic equivalence validation:
  old: raw → convert_day.py → catalog
  new: raw → replay_store → generate_catalog → catalog
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    from nautilus_trader.model.data import OrderBookDeltas, OrderBookDepth10, TradeTick
    from nautilus_trader.model.enums import AggressorSide
    from nautilus_trader.model.identifiers import InstrumentId, TradeId
    from nautilus_trader.model.objects import Price, Quantity
    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    NAUTILUS_AVAILABLE = True
except ImportError:
    NAUTILUS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Nautilus not available; generate_catalog will not work")

from config import (
    CATALOG_JOBS_ROOT,
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
    EMIT_DEPTH10_DEFAULT,
    REPLAY_ROOT,
)
from converter.depth_phase2 import replay_records_to_depth_streaming
from converter.instruments import build_instruments
from converter.spool import ObjectSpool
from stores.replay_depth_adapter import iter_replay_depth_records
from stores.replay_reader import ReplayReader

logger = logging.getLogger(__name__)

WRITE_BATCH_SIZE = 5000

# Supported catalog profiles:
#   trades_only — instruments + TradeTick (validated equivalent path)
#   full_l2     — instruments + TradeTick + OrderBookDeltas (+ optional Depth10)
#   depth_only  — instruments + OrderBookDeltas (+ optional Depth10), no trades
#   depth10     — instruments + OrderBookDepth10 only
SUPPORTED_PROFILES = ("trades_only", "full_l2", "depth_only", "depth10")

# Documented equivalence caveats for the replay-based full_l2 path. These stem
# from replay_store v0 NOT persisting sync_state/stream_lifecycle records and
# NOT performing cross-day repartitioning/carry recovery. See
# docs/FULL_L2_REPLAY_CATALOG_PLAN.md for the full equivalence boundary.
FULL_L2_CAVEATS = [
    "sync_state-driven fenced ranges are not regenerated (replay v0 drops sync_state records)",
    "cross-day carry / synthetic opening snapshot is not reproduced (no prev/next repartitioning)",
    "UTC-boundary repartitioning of clock-skewed records is not applied",
    "duplicate depth suppression relies on the replay builder, not the converter spool",
]


def _profile_write_flags(profile: str, emit_depth10: bool) -> tuple[bool, bool, bool]:
    """Resolve (writes_trades, writes_deltas, writes_depth10) for a profile."""
    if profile == "trades_only":
        return True, False, False
    if profile == "full_l2":
        return True, True, emit_depth10
    if profile == "depth_only":
        return False, True, emit_depth10
    if profile == "depth10":
        return False, False, True
    raise ValueError(f"Unsupported profile: {profile}")


def _parse_iso_datetime(iso_str: str) -> datetime:
    """Parse ISO 8601 UTC datetime string."""
    try:
        # Try parsing with timezone
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid ISO 8601 datetime: {iso_str}")


def _date_range_from_window(start: datetime, end: datetime) -> list[str]:
    """Generate date strings touched by the half-open [start, end) window."""
    dates = []
    current = datetime.combine(start.date(), datetime.min.time(), tzinfo=timezone.utc)
    while current < end:
        dates.append(current.date().isoformat())
        current += timedelta(days=1)
    return dates


def _window_from_date(date_str: str) -> tuple[datetime, datetime]:
    """Return the UTC day window for YYYY-MM-DD."""
    try:
        start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Invalid date: {date_str}; expected YYYY-MM-DD") from exc
    return start, start + timedelta(days=1)


def _convert_trade_to_nautilus(
    trade: dict,
    instrument_id: InstrumentId,
    venue: str,
) -> Optional[TradeTick]:
    """Convert replay trade record to Nautilus TradeTick."""
    if not NAUTILUS_AVAILABLE:
        return None
    
    try:
        trade_id = trade.get("trade_id") or trade.get("agg_trade_id")
        if not trade_id:
            return None
        
        price = trade.get("price_str") or trade.get("price", 0)
        quantity = trade.get("quantity_str") or trade.get("quantity", 0)
        ts_ns = int(trade.get("ts_exchange_ns", 0))
        ts_recv_ns = int(trade.get("ts_receive_ns", ts_ns))
        
        if float(price) <= 0 or float(quantity) <= 0:
            return None
        
        side = AggressorSide.BUYER if not trade.get("buyer_maker", False) else AggressorSide.SELLER
        
        tick = TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(str(price)),
            size=Quantity.from_str(str(quantity)),
            aggressor_side=side,
            trade_id=TradeId(str(trade_id)),
            ts_event=ts_ns,
            ts_init=ts_recv_ns,
        )
        return tick
    except Exception as e:
        logger.warning(f"Error converting trade: {e}")
        return None


def _write_depth_for_partition(
    *,
    reader: ReplayReader,
    venue: str,
    symbol: str,
    date: str,
    instrument,
    catalog,
    start_ns: int,
    end_ns: int,
    writes_deltas: bool,
    writes_depth10: bool,
    depth10_interval_sec: float,
    derived_depth_snapshot_levels: int,
    time_filter: str,
    batch_size: int = WRITE_BATCH_SIZE,
):
    """Replay one partition's depth through the shared engine and write to catalog.

    Reuses the validated ``converter.depth_phase2`` engine (via the replay
    adapter) so OrderBookDeltas / Depth10 semantics match the raw path exactly.
    Objects are buffered in disk-backed :class:`ObjectSpool`s (memory-bounded)
    and written in the same ``ts_init``-ordered batches as ``convert_day.py``.

    Returns ``(metrics, deltas_written, depth10_written, deltas_skipped,
    depth10_skipped)``.
    """
    iid = instrument.id
    price_prec = instrument.price_precision
    size_prec = instrument.size_precision

    def _ts(obj) -> int:
        return int(obj.ts_init) if time_filter == "ts_init" else int(obj.ts_event)

    def _in_window(obj) -> bool:
        ts = _ts(obj)
        return start_ns <= ts < end_ns

    deltas_skipped = 0
    depth10_skipped = 0

    with (
        ObjectSpool(prefix="cryptorecorder-gc-delta-") as deltas_spool,
        ObjectSpool(prefix="cryptorecorder-gc-depth10-") as depth10_spool,
    ):
        delta_ordinal = 0
        depth10_ordinal = 0

        def on_deltas_batch(batch):
            nonlocal delta_ordinal, deltas_skipped
            if not writes_deltas:
                return
            kept = [d for d in batch if _in_window(d)]
            deltas_skipped += len(batch) - len(kept)
            if kept:
                delta_ordinal = deltas_spool.insert_many(kept, start_ordinal=delta_ordinal)

        def on_depth10_batch(batch):
            nonlocal depth10_ordinal, depth10_skipped
            if not writes_depth10:
                return
            kept = [d for d in batch if _in_window(d)]
            depth10_skipped += len(batch) - len(kept)
            if kept:
                depth10_ordinal = depth10_spool.insert_many(kept, start_ordinal=depth10_ordinal)

        records = iter_replay_depth_records(reader.iter_depths(venue, symbol, date))
        metrics = replay_records_to_depth_streaming(
            records,
            venue,
            symbol,
            iid,
            price_prec,
            size_prec,
            on_deltas_batch=on_deltas_batch,
            on_depth10_batch=on_depth10_batch,
            batch_size=batch_size,
            emit_depth10=writes_depth10,
            depth10_interval_sec=depth10_interval_sec,
            derived_depth_snapshot_levels=derived_depth_snapshot_levels,
        )

        deltas_written = 0
        depth10_written = 0
        if writes_deltas and deltas_spool.count:
            deltas_spool.commit()
            for spool_batch in deltas_spool.iter_batches(batch_size):
                catalog.write_data(spool_batch)
                deltas_written += len(spool_batch)
        if writes_depth10 and depth10_spool.count:
            depth10_spool.commit()
            for spool_batch in depth10_spool.iter_batches(batch_size):
                catalog.write_data(spool_batch)
                depth10_written += len(spool_batch)

    return metrics, deltas_written, depth10_written, deltas_skipped, depth10_skipped


def _exchange_info_from_replay_metadata(symbol: str, metadata: Optional[dict]) -> dict[str, dict]:
    """Return exchangeInfo-shaped metadata if the replay partition provides it."""
    if not metadata:
        return {}
    if isinstance(metadata.get("exchange_info"), dict):
        return {symbol: metadata["exchange_info"]}
    if isinstance(metadata.get("filters"), list):
        return {symbol: metadata}
    return {}


def generate_catalog_from_replay(
    replay_root: Path,
    catalog_root: Path,
    job_id: str,
    symbols: list[str],
    venues: list[str],
    start: datetime,
    end: datetime,
    profile: str = "trades_only",
    overwrite: bool = False,
    *,
    emit_depth10: bool = EMIT_DEPTH10_DEFAULT,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
    time_filter: str = "ts_init",
) -> dict:
    """
    Generate Nautilus catalog from replay_store.

    Args:
        replay_root: Path to replay_store
        catalog_root: Output path for catalog_jobs
        job_id: Unique job identifier
        symbols: List of symbols to include
        venues: List of venues to include
        start: Start datetime (UTC)
        end: End datetime (UTC)
        profile: Catalog profile. One of SUPPORTED_PROFILES
            (trades_only, full_l2, depth_only, depth10).
        overwrite: Delete and recreate the job dir if it exists.
        emit_depth10: Whether full_l2/depth_only also emit OrderBookDepth10.
        depth10_interval_sec: Minimum interval between derived Depth10 snapshots.
        derived_depth_snapshot_levels: Levels per derived Depth10 snapshot (<=10).
        time_filter: Window filter field for catalog reads ('ts_init' or 'ts_event').

    Returns:
        Status dict with manifest
    """
    status = {
        "job_id": job_id,
        "status": "success",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "profile": profile,
        "symbols_requested": symbols,
        "venues_requested": venues,
        "requested_symbols": symbols,
        "requested_venues": venues,
        "time_filter": time_filter,
        "symbols_processed": [],
        "found_partitions": [],
        "missing_partitions": [],
        "date_partitions_scanned": [],
        "records_read": {
            "trades": 0,
            "depth": 0,
        },
        "records_written": {
            "trade_ticks": 0,
            "order_book_deltas": 0,
            "order_book_depth10": 0,
        },
        "records_skipped": {
            "outside_window": 0,
            "invalid_trade": 0,
            "depth_outside_window": 0,
        },
        "skipped_invalid_records": 0,
        "depth_diagnostics": {
            "raw_depth_records_read": 0,
            "snapshot_seeds": 0,
            "resyncs": 0,
            "desyncs": 0,
            "fenced_range_count": 0,
            "bad_lines": 0,
            "emit_depth10": emit_depth10,
            "depth10_interval_sec": depth10_interval_sec,
            "derived_depth_snapshot_levels": derived_depth_snapshot_levels,
        },
        "fenced_ranges": [],
        "caveats": list(FULL_L2_CAVEATS) if profile in ("full_l2", "depth_only", "depth10") else [],
        "warnings": [],
        "errors": [],
    }

    if not NAUTILUS_AVAILABLE:
        status["status"] = "failed"
        status["errors"].append("Nautilus not installed")
        logger.error("Nautilus not available for catalog generation")
        return status

    if profile not in SUPPORTED_PROFILES:
        status["status"] = "failed"
        status["errors"].append(
            f"Unsupported profile: {profile}. Supported: {', '.join(SUPPORTED_PROFILES)}."
        )
        return status

    if time_filter not in ("ts_init", "ts_event"):
        status["status"] = "failed"
        status["errors"].append(
            f"Unsupported time_filter: {time_filter}. Use 'ts_init' or 'ts_event'."
        )
        return status

    writes_trades, writes_deltas, writes_depth10 = _profile_write_flags(profile, emit_depth10)

    try:
        reader = ReplayReader(replay_root)
        job_dir = catalog_root / f"job_{job_id}"
        if job_dir.exists():
            if not overwrite:
                status["status"] = "failed"
                status["errors"].append(
                    f"Catalog job already exists: {job_dir}. Use overwrite=True or --overwrite."
                )
                return status
            shutil.rmtree(job_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        catalog = ParquetDataCatalog(str(job_dir))

        # Determine date range
        dates = _date_range_from_window(start, end)
        if not dates:
            status["status"] = "failed"
            status["errors"].append("End time must be after start time")
            return status
        logger.info(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")

        start_ns = int(start.timestamp() * 1_000_000_000)
        end_ns = int(end.timestamp() * 1_000_000_000)
        instruments_written: set[str] = set()
        processed_symbols: set[str] = set()
        available_venues = set(reader.iter_venues())
        target_venues = venues or sorted(available_venues)

        for venue in target_venues:
            if venue not in available_venues:
                for symbol in symbols:
                    for date in dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "venue_missing",
                        })
                continue

            available_symbols = set(reader.iter_symbols(venue))
            target_symbols = symbols or sorted(available_symbols)
            for symbol in target_symbols:
                if symbol not in available_symbols:
                    for date in dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "symbol_missing",
                        })
                    continue

                available_dates = set(reader.iter_dates(venue, symbol))
                for date in dates:
                    if date not in available_dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "date_missing",
                        })
                        continue

                    logger.info(f"Processing {venue}/{symbol}/{date}...")
                    partition_key = f"{venue}:{symbol}:{date}"
                    status["found_partitions"].append({
                        "venue": venue,
                        "symbol": symbol,
                        "date": date,
                    })
                    status["date_partitions_scanned"].append(partition_key)
                    symbol_key = f"{venue}:{symbol}"
                    if symbol_key not in processed_symbols:
                        status["symbols_processed"].append(symbol_key)
                        processed_symbols.add(symbol_key)

                    # Load instrument metadata
                    instrument_metadata = reader.load_instrument_metadata(
                        venue, symbol, date
                    )
                    if not instrument_metadata:
                        logger.warning(
                            f"No instrument metadata for {venue}/{symbol}/{date}; "
                            "using default Nautilus instrument settings"
                        )

                    exchange_info = _exchange_info_from_replay_metadata(
                        symbol, instrument_metadata
                    )
                    instruments = build_instruments(venue, [symbol], exchange_info)
                    if not instruments:
                        status["errors"].append(f"could not build instrument for {venue}/{symbol}")
                        continue
                    instrument = instruments[0]
                    instrument_id = instrument.id
                    instrument_key = str(instrument_id)
                    if instrument_key not in instruments_written:
                        catalog.write_data([instrument])
                        instruments_written.add(instrument_key)

                    # Stream and convert trades
                    if writes_trades:
                        trade_batch = []
                        for trade in reader.iter_trades(venue, symbol, date):
                            status["records_read"]["trades"] += 1
                            ts_init_ns = int(trade.get("ts_receive_ns") or trade.get("ts_exchange_ns", 0))

                            # Nautilus catalog bounded reads are based on ts_init.
                            if ts_init_ns < start_ns:
                                status["records_skipped"]["outside_window"] += 1
                                continue
                            if ts_init_ns >= end_ns:
                                status["records_skipped"]["outside_window"] += 1
                                continue

                            trade_tick = _convert_trade_to_nautilus(
                                trade, instrument_id, venue
                            )
                            if trade_tick:
                                trade_batch.append(trade_tick)
                                status["records_written"]["trade_ticks"] += 1
                                if len(trade_batch) >= 5000:
                                    catalog.write_data(trade_batch)
                                    trade_batch = []
                            else:
                                status["records_skipped"]["invalid_trade"] += 1
                                status["skipped_invalid_records"] += 1
                        if trade_batch:
                            catalog.write_data(trade_batch)

                    # Depth records (OrderBookDeltas / OrderBookDepth10) via the
                    # shared, validated converter engine + replay adapter.
                    if writes_deltas or writes_depth10:
                        (
                            depth_metrics,
                            deltas_written,
                            depth10_written,
                            deltas_skipped,
                            depth10_skipped,
                        ) = _write_depth_for_partition(
                            reader=reader,
                            venue=venue,
                            symbol=symbol,
                            date=date,
                            instrument=instrument,
                            catalog=catalog,
                            start_ns=start_ns,
                            end_ns=end_ns,
                            writes_deltas=writes_deltas,
                            writes_depth10=writes_depth10,
                            depth10_interval_sec=depth10_interval_sec,
                            derived_depth_snapshot_levels=derived_depth_snapshot_levels,
                            time_filter=time_filter,
                        )
                        status["records_read"]["depth"] += depth_metrics.raw_record_count
                        status["records_written"]["order_book_deltas"] += deltas_written
                        status["records_written"]["order_book_depth10"] += depth10_written
                        status["records_skipped"]["depth_outside_window"] += (
                            deltas_skipped + depth10_skipped
                        )
                        diag = status["depth_diagnostics"]
                        diag["raw_depth_records_read"] += depth_metrics.raw_record_count
                        diag["snapshot_seeds"] += depth_metrics.snapshot_seed_count
                        diag["resyncs"] += depth_metrics.resync_count
                        diag["desyncs"] += depth_metrics.desync_events
                        diag["fenced_range_count"] += len(depth_metrics.fenced_ranges)
                        diag["bad_lines"] += depth_metrics.bad_lines
                        for fence in depth_metrics.fenced_ranges:
                            enriched = dict(fence)
                            enriched.setdefault("venue", venue)
                            enriched.setdefault("symbol", symbol)
                            enriched["date"] = date
                            status["fenced_ranges"].append(enriched)

                    logger.info(
                        f"Processed {venue}/{symbol}/{date}: "
                        f"trades={status['records_written']['trade_ticks']}, "
                        f"deltas={status['records_written']['order_book_deltas']}, "
                        f"depth10={status['records_written']['order_book_depth10']}"
                    )

        if writes_trades and status["records_written"]["trade_ticks"] == 0:
            status["warnings"].append(
                "No TradeTick records were written for the requested venues/symbols/window."
            )
        if writes_deltas and status["records_written"]["order_book_deltas"] == 0:
            status["warnings"].append(
                "No OrderBookDeltas records were written for the requested venues/symbols/window."
            )

        # Write manifest
        manifest = {
            "job_id": job_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "profile": profile,
            "requested_symbols": symbols,
            "requested_venues": venues,
            "symbols": status["symbols_processed"],
            "found_partitions": status["found_partitions"],
            "missing_partitions": status["missing_partitions"],
            "date_partitions_scanned": status["date_partitions_scanned"],
            "time_filter": time_filter,
            "time_window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "records_read": status["records_read"],
            "record_counts": status["records_written"],
            "records_skipped": status["records_skipped"],
            "skipped_invalid_records": status["skipped_invalid_records"],
            "instrument_count": len(instruments_written),
            "replay_source": str(replay_root),
            "depth_diagnostics": status["depth_diagnostics"],
            "fenced_ranges": status["fenced_ranges"],
            "equivalence_caveats": status["caveats"],
            "warnings": status["warnings"],
        }

        manifest_path = job_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(
            f"✓ Catalog generated: job_id={job_id}, profile={profile}, "
            f"symbols={len(status['symbols_processed'])}, "
            f"trades={status['records_written']['trade_ticks']}, "
            f"deltas={status['records_written']['order_book_deltas']}, "
            f"depth10={status['records_written']['order_book_depth10']}"
        )

    except Exception as e:
        status["status"] = "failed"
        status["errors"].append(str(e))
        logger.error(f"Failed to generate catalog: {e}")

    return status


def main():
    """CLI entry point for generate_catalog."""
    parser = argparse.ArgumentParser(
        description="Generate Nautilus catalog from replay_store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m pipeline.generate_catalog --input /path/to/replay_store --symbols BTCUSDT --venues BINANCE_SPOT --start 2026-06-15T12:00:00Z --end 2026-06-15T13:00:00Z
  python -m pipeline.generate_catalog --input /path/to/replay_store --symbols BTCUSDT --venues BINANCE_SPOT --date 2026-06-15 --job-id validation_day --overwrite
  python -m pipeline.generate_catalog --input /path/to/replay_store --symbols BTCUSDT,ETHUSDT --start 2026-06-15T00:00:00Z --end 2026-06-17T00:00:00Z --output /path/to/catalog_jobs --job-id validation_new --overwrite
        """,
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help=f"Replay store root (default: {REPLAY_ROOT})",
    )
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbols (e.g., BTCUSDT,ETHUSDT)",
    )
    parser.add_argument(
        "--venues",
        default="BINANCE_SPOT,BINANCE_USDTF",
        help="Comma-separated venues (default: BINANCE_SPOT,BINANCE_USDTF)",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="Start time (ISO 8601 UTC, e.g., 2026-06-15T12:00:00Z)",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End time (ISO 8601 UTC, e.g., 2026-06-15T13:00:00Z)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="UTC date shortcut (YYYY-MM-DD), equivalent to that full half-open UTC day.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=f"Catalog output root (default: {CATALOG_JOBS_ROOT})",
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="Deterministic job id. Output directory is job_{job_id}.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete and recreate job_{job_id} if it already exists.",
    )
    parser.add_argument(
        "--profile",
        choices=list(SUPPORTED_PROFILES),
        default="trades_only",
        help=(
            "Catalog profile (default: trades_only). "
            "full_l2 = trades + OrderBookDeltas (+ Depth10); "
            "depth_only = OrderBookDeltas (+ Depth10); "
            "depth10 = OrderBookDepth10 only."
        ),
    )
    parser.add_argument(
        "--emit-depth10",
        dest="emit_depth10",
        action="store_true",
        default=EMIT_DEPTH10_DEFAULT,
        help="Emit derived OrderBookDepth10 snapshots (full_l2/depth_only). Default: on.",
    )
    parser.add_argument(
        "--no-emit-depth10",
        dest="emit_depth10",
        action="store_false",
        help="Do not emit derived OrderBookDepth10 snapshots (full_l2/depth_only).",
    )
    parser.add_argument(
        "--depth10-interval-sec",
        type=float,
        default=DEPTH10_INTERVAL_SEC,
        help=f"Minimum seconds between derived Depth10 snapshots (default: {DEPTH10_INTERVAL_SEC}).",
    )
    parser.add_argument(
        "--derived-depth-snapshot-levels",
        type=int,
        default=DERIVED_DEPTH_SNAPSHOT_LEVELS,
        help=f"Levels per derived Depth10 snapshot, <=10 (default: {DERIVED_DEPTH_SNAPSHOT_LEVELS}).",
    )
    parser.add_argument(
        "--time-filter",
        choices=["ts_init", "ts_event"],
        default="ts_init",
        help="Window filter field for catalog reads (default: ts_init).",
    )
    args = parser.parse_args()

    replay_root = args.input or REPLAY_ROOT
    catalog_root = args.output or CATALOG_JOBS_ROOT

    if args.date and (args.start or args.end):
        parser.error("Use either --date or --start/--end, not both.")
    if args.date:
        try:
            start, end = _window_from_date(args.date)
        except ValueError as e:
            parser.error(str(e))
    else:
        if not args.start or not args.end:
            parser.error("Either --date or both --start and --end are required.")
        try:
            start = _parse_iso_datetime(args.start)
            end = _parse_iso_datetime(args.end)
        except ValueError as e:
            logger.error(f"Invalid datetime format: {e}")
            sys.exit(1)
    if end <= start:
        parser.error("--end must be after --start.")

    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    venues = [v.strip().upper() for v in args.venues.split(",")]

    job_id = args.job_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info(
        f"Generating catalog: job_id={job_id}, symbols={symbols}, "
        f"venues={venues}, start={start}, end={end}"
    )

    result = generate_catalog_from_replay(
        replay_root,
        catalog_root,
        job_id,
        symbols,
        venues,
        start,
        end,
        profile=args.profile,
        overwrite=args.overwrite,
        emit_depth10=args.emit_depth10,
        depth10_interval_sec=args.depth10_interval_sec,
        derived_depth_snapshot_levels=args.derived_depth_snapshot_levels,
        time_filter=args.time_filter,
    )

    if result["status"] != "success":
        logger.error(f"Catalog generation failed: {result['errors']}")
        sys.exit(1)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
