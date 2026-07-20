"""
pipeline.daily_build — Daily build orchestrator for the replay store.

Orchestrates raw → replay_store + daily_build_report generation.
Entry point for daily scheduled builds via systemd timer.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from config import DATA_ROOT, REPLAY_ROOT, DAILY_REPORT_ROOT

logger = logging.getLogger(__name__)


def _parse_date_arg(date_str: str) -> str:
    """
    Parse date argument.
    
    Special case: 'yesterday' means previous completed UTC date.
    Otherwise expects YYYY-MM-DD format.
    """
    if date_str.lower() == "yesterday":
        # Previous completed UTC date
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    
    # Validate YYYY-MM-DD format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str} (expected YYYY-MM-DD or 'yesterday')")


def run_raw_manifest(date_str: str, data_root: Path) -> dict:
    """Run raw manifest scan."""
    logger.info(f"Scanning raw data coverage for {date_str}...")
    
    from pipeline.raw_manifest import scan_raw_coverage
    coverage = scan_raw_coverage(date_str, data_root)
    
    logger.info(
        f"Raw scan complete: {coverage['symbol_count']} symbols, "
        f"{len(coverage['venues'])} venues"
    )
    return coverage


def run_build_replay_store(
    date_str: str,
    symbols: list[str],
    data_root: Path,
    replay_root: Path,
) -> dict:
    """Run replay store builder."""
    logger.info(f"Building replay_store for {date_str}...")
    
    # Import here to avoid circular deps
    from pipeline.build_replay_store import build_replay_for_symbol
    from pipeline.raw_manifest import scan_raw_coverage
    
    coverage = scan_raw_coverage(date_str, data_root)
    
    results = []
    for venue in coverage.get("venues", []):
        for symbol in symbols:
            if symbol in coverage.get("data", {}).get(venue, {}):
                result = build_replay_for_symbol(
                    venue, symbol, date_str, data_root, replay_root
                )
                results.append(result)
    
    successful = sum(1 for r in results if r["status"] == "success")
    total_depth = sum(r.get("depth_count", 0) for r in results)
    total_trades = sum(r.get("trade_count", 0) for r in results)
    
    if not results:
        # Zero eligible venue/symbol partitions for this date (empty/missing
        # raw data). Never report this as "success" — 0 successful == 0
        # attempted is not a real successful build.
        status = "no_data"
        logger.warning(
            "Replay build found no eligible venue/symbol partitions for "
            f"{date_str} — reporting no_data (not success)."
        )
    elif successful == len(results):
        status = "success"
    elif successful == 0:
        # Partitions were attempted but every single one failed — distinct
        # from both "no_data" (nothing was eligible) and "partial" (a mix
        # of success/failure).
        status = "failed"
        logger.error(
            f"Replay build attempted {len(results)} partition(s) for "
            f"{date_str} but none succeeded — reporting failed."
        )
    else:
        status = "partial"
    
    logger.info(
        f"Replay build complete: {successful}/{len(results)} symbols, "
        f"{total_depth} depth, {total_trades} trades"
    )
    
    return {
        "status": status,
        "symbols_processed": successful,
        "symbols_total": len(results),
        "depth_records": total_depth,
        "trade_records": total_trades,
        "results": results,
    }


def generate_daily_report(
    date_str: str,
    data_root: Path,
    replay_root: Path,
    report_root: Path,
    raw_result: dict,
    replay_result: dict,
    runtime_sec: float,
) -> dict:
    """Generate daily build report."""
    report = {
        "date": date_str,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_sec": runtime_sec,
        "status": "success",
        
        # Paths
        "data_root": str(data_root),
        "replay_root": str(replay_root),
        "report_root": str(report_root),
        
        # Coverage
        "raw_coverage": {
            "venues": raw_result.get("venues", []),
            "symbol_count": raw_result.get("symbol_count", 0),
        },
        
        # Replay store stats
        "replay_build": {
            "status": replay_result.get("status", "unknown"),
            "symbols_processed": replay_result.get("symbols_processed", 0),
            "symbols_total": replay_result.get("symbols_total", 0),
            "depth_records": replay_result.get("depth_records", 0),
            "trade_records": replay_result.get("trade_records", 0),
        },
        
        # Errors
        "errors": [],
    }
    
    # Check overall status. Distinguish "no_data" (no raw partitions were
    # eligible to build — not a success), "failed" (partitions were
    # attempted but every single one failed), "partial" (some symbols
    # succeeded and some failed), and "success" (all eligible symbols
    # built). Only "success" produces a zero process exit code.
    replay_status = replay_result.get("status")
    if replay_status in ("no_data", "failed", "partial"):
        report["status"] = replay_status
    elif replay_status != "success":
        report["status"] = "partial"
    
    # Collect errors
    for result in replay_result.get("results", []):
        if result.get("errors"):
            report["errors"].extend(result.get("errors", []))
    
    # Write report
    report_root.mkdir(parents=True, exist_ok=True)
    report_path = report_root / f"daily_build_{date_str}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Daily report written: {report_path}")
    return report


def main():
    """CLI entry point for daily_build."""
    parser = argparse.ArgumentParser(
        description="Daily build orchestrator for the replay store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m pipeline.daily_build --date 2026-06-15
  python -m pipeline.daily_build --date yesterday
  python -m pipeline.daily_build --date 2026-06-15 --symbols BTCUSDT,ETHUSDT
  python -m pipeline.daily_build --date 2026-06-15 --data-root /custom/raw --replay-root /custom/replay
        """,
    )
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD or 'yesterday')")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols to process (default: all from raw)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help=f"Data root (default: {DATA_ROOT})",
    )
    parser.add_argument(
        "--replay-root",
        type=Path,
        default=None,
        help=f"Replay root (default: {REPLAY_ROOT})",
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=None,
        help=f"Report root (default: {DAILY_REPORT_ROOT})",
    )
    
    args = parser.parse_args()

    # Parse paths
    data_root = args.data_root or DATA_ROOT
    replay_root = args.replay_root or REPLAY_ROOT
    report_root = args.report_root or DAILY_REPORT_ROOT

    # Parse date
    try:
        date_str = _parse_date_arg(args.date)
    except ValueError as e:
        logger.error(f"Invalid date: {e}")
        sys.exit(1)

    logger.info(
        f"Daily build started: {date_str}, "
        f"data_root={data_root}, replay_root={replay_root}"
    )

    start_time = time.time()

    try:
        # Step 1: Raw manifest scan (always run)
        raw_result = run_raw_manifest(date_str, data_root)
        
        # Determine symbols to process
        if args.symbols:
            symbols = [s.strip().upper() for s in args.symbols.split(",")]
        else:
            all_symbols = set()
            for venue_data in raw_result.get("data", {}).values():
                all_symbols.update(venue_data.keys())
            symbols = sorted(all_symbols)
        
        logger.info(f"Processing symbols: {symbols}")

        # Step 2: Build replay store (always run)
        replay_result = run_build_replay_store(date_str, symbols, data_root, replay_root)

        # Step 3: Generate report
        runtime_sec = time.time() - start_time
        report = generate_daily_report(
            date_str,
            data_root,
            replay_root,
            report_root,
            raw_result,
            replay_result,
            runtime_sec,
        )

        logger.info(
            f"✓ Daily build complete: status={report['status']}, "
            f"runtime={runtime_sec:.1f}s"
        )
        if report["status"] != "success":
            logger.warning(
                f"Daily build for {date_str} did not succeed "
                f"(status={report['status']}); exiting with nonzero status."
            )

        return 0 if report["status"] == "success" else 1

    except Exception as e:
        logger.error(f"✗ Daily build failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
