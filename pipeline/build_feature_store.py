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


def _date_bounds_ns(date: str) -> tuple[int, int]:
    start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return (
        int(start.timestamp() * 1_000_000_000),
        int(end.timestamp() * 1_000_000_000),
    )


def _aggregate_window(
    venue: str,
    symbol: str,
    date: str,
    timeframe: str,
    reader: ReplayReader,
    window_mode: str = "utc_day",
) -> list[dict]:
    """
    Aggregate depth and trade records into feature windows.

    Returns:
        List of feature records for this day/symbol/timeframe
    """
    timeframe_ms = _timeframe_to_ms(timeframe)
    timeframe_ns = timeframe_ms * 1_000_000  # Convert to nanoseconds
    day_start_ns, day_end_ns = _date_bounds_ns(date)
    
    # Collect one symbol/date in v0, then clamp to the requested UTC day.
    depth_records = [
        record
        for record in reader.iter_depths(venue, symbol, date)
        if day_start_ns <= int(record.get("ts_exchange_ns", 0)) < day_end_ns
    ]
    trade_records = [
        record
        for record in reader.iter_trades(venue, symbol, date)
        if day_start_ns <= int(record.get("ts_exchange_ns", 0)) < day_end_ns
    ]
    
    if not depth_records and not trade_records:
        logger.warning(f"No data for {venue}/{symbol}/{date}")
        return []
    
    all_timestamps: list[int] = []
    for d in depth_records:
        all_timestamps.append(int(d.get("ts_exchange_ns", 0)))
    for t in trade_records:
        all_timestamps.append(int(t.get("ts_exchange_ns", 0)))
    
    if not all_timestamps:
        return []
    
    if window_mode not in {"observed", "utc_day"}:
        raise ValueError(f"Unsupported window_mode: {window_mode}")
    
    depth_by_window: dict[int, list[dict]] = {}
    trade_by_window: dict[int, list[dict]] = {}
    for record in depth_records:
        ts_ns = int(record.get("ts_exchange_ns", 0))
        window_start = (ts_ns // timeframe_ns) * timeframe_ns
        depth_by_window.setdefault(window_start, []).append(record)
    for record in trade_records:
        ts_ns = int(record.get("ts_exchange_ns", 0))
        window_start = (ts_ns // timeframe_ns) * timeframe_ns
        trade_by_window.setdefault(window_start, []).append(record)

    # Sparse output: create only windows containing at least one record.
    features_list = []
    for current_window_start in sorted(set(depth_by_window) | set(trade_by_window)):
        current_window_end = current_window_start + timeframe_ns
        feature = calculate_core_features(
            venue,
            symbol,
            current_window_end - 1,
            timeframe,
            depth_by_window.get(current_window_start, []),
            trade_by_window.get(current_window_start, []),
        )
        features_list.append(feature)
    
    return features_list


def build_features_for_symbol(
    venue: str,
    symbol: str,
    date: str,
    timeframes: list[str],
    replay_root: Path,
    feature_root: Path,
    window_mode: str = "utc_day",
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
                features = _aggregate_window(
                    venue,
                    symbol,
                    date,
                    timeframe,
                    reader,
                    window_mode=window_mode,
                )
                
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
    parser.add_argument(
        "--window-mode",
        choices=["utc_day", "observed"],
        default="utc_day",
        help=(
            "Window bounds policy. utc_day clamps to [date 00:00 UTC, next day); "
            "observed spans only observed timestamps. Output is sparse in both modes."
        ),
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
                venue,
                symbol,
                date_str,
                timeframes,
                replay_root,
                feature_root,
                window_mode=args.window_mode,
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
