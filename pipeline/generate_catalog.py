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
        
        price = trade.get("price", 0)
        quantity = trade.get("quantity", 0)
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


def generate_catalog_from_replay(
    replay_root: Path,
    catalog_root: Path,
    job_id: str,
    symbols: list[str],
    venues: list[str],
    start: datetime,
    end: datetime,
    profile: str = "trades_only",
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
        "symbols_processed": [],
        "records_written": {
            "trade_ticks": 0,
            "order_book_deltas": 0,
            "order_book_depth10": 0,
        },
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
        job_dir.mkdir(parents=True, exist_ok=True)
        catalog = ParquetDataCatalog(str(job_dir))

        # Determine date range
        dates = _date_range_from_window(start, end)
        if not dates:
            status["status"] = "failed"
            status["errors"].append("End time must be after start time")
            return status
        logger.info(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")

        instruments_written: set[str] = set()
        
        for venue in reader.iter_venues():
            if venues and venue not in venues:
                continue

            for symbol in reader.iter_symbols(venue):
                if symbols and symbol not in symbols:
                    continue

                for date in dates:
                    if date not in list(reader.iter_dates(venue, symbol)):
                        continue

                    logger.info(f"Processing {venue}/{symbol}/{date}...")
                    status["symbols_processed"].append(f"{venue}:{symbol}")

                    # Load instrument metadata
                    instrument_metadata = reader.load_instrument_metadata(
                        venue, symbol, date
                    )
                    if not instrument_metadata:
                        logger.warning(
                            f"No instrument metadata for {venue}/{symbol}/{date}; "
                            "using default Nautilus instrument settings"
                        )

                    instruments = build_instruments(venue, [symbol], {})
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
                        ts_ns = int(trade.get("ts_exchange_ns", 0))
                        
                        # Filter by time window
                        if ts_ns < start.timestamp() * 1e9:
                            continue
                        if ts_ns >= end.timestamp() * 1e9:
                            break
                        
                        trade_tick = _convert_trade_to_nautilus(
                            trade, instrument_id, venue
                        )
                        if trade_tick:
                            trade_batch.append(trade_tick)
                            status["records_written"]["trade_ticks"] += 1
                            if len(trade_batch) >= 5000:
                                catalog.write_data(trade_batch)
                                trade_batch = []
                    if trade_batch:
                        catalog.write_data(trade_batch)

                    # Depth records (deferred for full implementation)
                    # For now, we only support trades_only profile
                    logger.info(
                        f"Processed {venue}/{symbol}: "
                        f"{status['records_written']['trade_ticks']} trades"
                    )

        # Write manifest
        manifest = {
            "job_id": job_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "profile": profile,
            "symbols": status["symbols_processed"],
            "time_window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "record_counts": status["records_written"],
            "instrument_count": len(instruments_written),
            "replay_source": str(replay_root),
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
  python -m pipeline.generate_catalog --input /path/to/replay_store --symbols BTCUSDT,ETHUSDT --start 2026-06-15T00:00:00Z --end 2026-06-17T00:00:00Z --output /path/to/catalog_jobs/test_job
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
        required=True,
        help="Start time (ISO 8601 UTC, e.g., 2026-06-15T12:00:00Z)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End time (ISO 8601 UTC, e.g., 2026-06-15T13:00:00Z)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=f"Catalog output root (default: {CATALOG_JOBS_ROOT})",
    )
    parser.add_argument(
        "--profile",
        choices=["trades_only"],
        default="trades_only",
        help="Catalog profile (default: trades_only; depth/full_l2 is deferred)",
    )
    args = parser.parse_args()

    replay_root = args.input or REPLAY_ROOT
    catalog_root = args.output or CATALOG_JOBS_ROOT

    try:
        start = _parse_iso_datetime(args.start)
        end = _parse_iso_datetime(args.end)
    except ValueError as e:
        logger.error(f"Invalid datetime format: {e}")
        sys.exit(1)

    symbols = [s.strip().upper() for s in args.symbols.split(",")]
    venues = [v.strip().upper() for v in args.venues.split(",")]

    # Generate job_id from timestamp
    job_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

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
