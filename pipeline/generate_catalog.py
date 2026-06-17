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

from config import CATALOG_JOBS_ROOT, REPLAY_ROOT
from converter.instruments import build_instruments
from stores.replay_reader import ReplayReader

logger = logging.getLogger(__name__)


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


def _convert_depth_to_nautilus(
    depth: dict,
    instrument_id: InstrumentId,
    emit_depth10: bool = True,
) -> tuple[Optional[OrderBookDeltas], Optional[OrderBookDepth10]]:
    """Convert replay depth record to Nautilus OrderBookDeltas + OrderBookDepth10."""
    if not NAUTILUS_AVAILABLE:
        return None, None
    
    # Simplified stub - full implementation would require building order book state
    # For now, return None to indicate feature is deferred
    return None, None


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
        profile: Catalog profile. Only 'trades_only' is currently implemented.

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
        "time_filter": "ts_init",
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
        },
        "skipped_invalid_records": 0,
        "warnings": [],
        "errors": [],
    }

    if not NAUTILUS_AVAILABLE:
        status["status"] = "failed"
        status["errors"].append("Nautilus not installed")
        logger.error("Nautilus not available for catalog generation")
        return status

    if profile != "trades_only":
        status["status"] = "failed"
        status["errors"].append(
            "Only trades_only catalog generation is currently implemented; "
            "depth/full_l2 generation is deferred."
        )
        return status

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

                    # Depth records (deferred for full implementation)
                    # For now, we only support trades_only profile
                    logger.info(
                        f"Processed {venue}/{symbol}: "
                        f"{status['records_written']['trade_ticks']} trades"
                    )

        if status["records_written"]["trade_ticks"] == 0:
            status["warnings"].append(
                "No TradeTick records were written for the requested venues/symbols/window."
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
            "time_filter": "ts_init",
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
            "warnings": status["warnings"],
        }

        manifest_path = job_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(
            f"✓ Catalog generated: job_id={job_id}, "
            f"symbols={len(status['symbols_processed'])}, "
            f"trades={status['records_written']['trade_ticks']}"
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
        choices=["trades_only"],
        default="trades_only",
        help="Catalog profile (default: trades_only)",
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
