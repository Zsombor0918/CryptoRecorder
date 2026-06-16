"""
pipeline.build_feature_store — Daily feature store builder from replay data.

Converts replay_store to feature_store with timeframe aggregation.
Implements all core v1 features; advanced features remain NULL/TODO.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from config import FEATURE_ROOT, REPLAY_ROOT
from stores.feature_calc import calculate_core_features
from stores.feature_writer import FeatureWriter
from stores.replay_reader import ReplayReader

logger = logging.getLogger(__name__)

# Suppress verbose library logs
logging.getLogger("pyarrow").setLevel(logging.WARNING)


def _timeframe_to_ms(timeframe: str) -> int:
    """Convert timeframe string to milliseconds."""
    timeframe_map = {
        "100ms": 100,
        "1s": 1000,
        "1m": 60 * 1000,
    }
    return timeframe_map.get(timeframe, 1000)


def _aggregate_window(
    venue: str,
    symbol: str,
    date: str,
    timeframe: str,
    reader: ReplayReader,
) -> list[dict]:
    """
    Aggregate depth and trade records into feature windows.

    Returns:
        List of feature records for this day/symbol/timeframe
    """
    timeframe_ms = _timeframe_to_ms(timeframe)
    timeframe_ns = timeframe_ms * 1_000_000  # Convert to nanoseconds
    
    # Collect all records
    depth_records = list(reader.iter_depths(venue, symbol, date))
    trade_records = list(reader.iter_trades(venue, symbol, date))
    
    if not depth_records and not trade_records:
        logger.warning(f"No data for {venue}/{symbol}/{date}")
        return []
    
    # Find overall time range
    all_timestamps = []
    for d in depth_records:
        all_timestamps.append(d.get("ts_exchange_ns", 0))
    for t in trade_records:
        all_timestamps.append(t.get("ts_exchange_ns", 0))
    
    if not all_timestamps:
        return []
    
    min_ts = min(all_timestamps)
    max_ts = max(all_timestamps)
    
    # Create time windows
    features_list = []
    current_window_start = (min_ts // timeframe_ns) * timeframe_ns
    
    while current_window_start <= max_ts:
        current_window_end = current_window_start + timeframe_ns
        
        # Collect records in this window
        window_depths = [
            d for d in depth_records
            if current_window_start <= d.get("ts_exchange_ns", 0) < current_window_end
        ]
        window_trades = [
            t for t in trade_records
            if current_window_start <= t.get("ts_exchange_ns", 0) < current_window_end
        ]
        
        # Only create feature if we have some data
        if window_depths or window_trades:
            # Use end-of-window timestamp for feature
            window_end_ts = current_window_end - 1
            
            feature = calculate_core_features(
                venue,
                symbol,
                window_end_ts,
                timeframe,
                window_depths,
                window_trades,
            )
            features_list.append(feature)
        
        current_window_start = current_window_end
    
    return features_list


def build_features_for_symbol(
    venue: str,
    symbol: str,
    date: str,
    timeframes: list[str],
    replay_root: Path,
    feature_root: Path,
) -> dict:
    """
    Build feature store for a single venue/symbol/date across all timeframes.

    Returns:
        Status dict with counts and errors.
    """
    status = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "status": "success",
        "timeframes_processed": {},
        "errors": [],
    }

    reader = ReplayReader(replay_root)

    try:
        for timeframe in timeframes:
            try:
                logger.info(f"Aggregating {venue}/{symbol}/{date} @ {timeframe}...")
                
                # Aggregate records into features
                features = _aggregate_window(venue, symbol, date, timeframe, reader)
                
                if not features:
                    logger.warning(
                        f"No features for {venue}/{symbol}/{date} @ {timeframe}"
                    )
                    status["timeframes_processed"][timeframe] = 0
                    continue
                
                # Write features
                writer = FeatureWriter(
                    feature_root, timeframe, venue, symbol, date
                )
                
                # Batch write
                batch_size = 5000
                for i in range(0, len(features), batch_size):
                    batch = features[i : i + batch_size]
                    writer.write_feature_batch(batch)
                
                # Finalize and publish
                manifest = writer.finalize_staging()
                writer.publish(manifest)
                
                status["timeframes_processed"][timeframe] = len(features)
                logger.info(
                    f"✓ Built features: {venue}/{symbol}/{date} @ {timeframe} "
                    f"({len(features)} records)"
                )

            except Exception as e:
                status["errors"].append(f"Failed for timeframe {timeframe}: {e}")
                logger.error(
                    f"✗ Failed to build features for {venue}/{symbol}/{date} @ {timeframe}: {e}"
                )

    except Exception as e:
        status["status"] = "failed"
        status["errors"].append(str(e))
        logger.error(f"✗ Critical error for {venue}/{symbol}/{date}: {e}")

    return status


def main():
    """CLI entry point for build_feature_store."""
    parser = argparse.ArgumentParser(
        description="Build feature_store from replay_store data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m pipeline.build_feature_store --date 2026-06-15
  python -m pipeline.build_feature_store --date 2026-06-15 --symbols BTCUSDT,ETHUSDT --timeframes 1s,1m
  python -m pipeline.build_feature_store --date 2026-06-15 --replay-root /path/to/replay --feature-root /path/to/features
        """,
    )
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols",
        default="all",
        help="Comma-separated symbols or 'all' (default: all)",
    )
    parser.add_argument(
        "--timeframes",
        default="100ms,1s,1m",
        help="Comma-separated timeframes (default: 100ms,1s,1m)",
    )
    parser.add_argument(
        "--replay-root",
        type=Path,
        default=None,
        help=f"Replay root (default: {REPLAY_ROOT})",
    )
    parser.add_argument(
        "--feature-root",
        type=Path,
        default=None,
        help=f"Feature root (default: {FEATURE_ROOT})",
    )
    args = parser.parse_args()

    replay_root = args.replay_root or REPLAY_ROOT
    feature_root = args.feature_root or FEATURE_ROOT

    date_str = args.date
    timeframes = [t.strip() for t in args.timeframes.split(",")]

    # Parse symbols
    if args.symbols.lower() == "all":
        reader = ReplayReader(replay_root)
        all_symbols = set()
        for venue in reader.iter_venues():
            for symbol in reader.iter_symbols(venue):
                all_symbols.add(symbol)
        symbols_to_process = sorted(all_symbols)
    else:
        symbols_to_process = [s.strip().upper() for s in args.symbols.split(",")]

    if not symbols_to_process:
        logger.error(f"No symbols found in replay_store for {date_str}")
        sys.exit(1)

    # Build features for each venue/symbol
    reader = ReplayReader(replay_root)
    results = []

    for venue in reader.iter_venues():
        for symbol in symbols_to_process:
            # Check if symbol exists for this venue
            if symbol not in list(reader.iter_symbols(venue)):
                continue
            
            # Check if date exists
            if date_str not in list(reader.iter_dates(venue, symbol)):
                continue
            
            result = build_features_for_symbol(
                venue, symbol, date_str, timeframes, replay_root, feature_root
            )
            results.append(result)

    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    total_features = sum(
        sum(r.get("timeframes_processed", {}).values()) for r in results
    )

    logger.info(
        f"Feature build complete: {successful} successful, {failed} failed, "
        f"{total_features} total feature records"
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
