"""Validate old convert_day catalog output against replay-generated catalogs."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import generate_catalog_from_replay
from validation.catalog_compare import (
    compare_trade_ticks_semantic,
    load_instrument_ids,
    load_trade_ticks,
    write_validation_report,
)

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _instrument_id_for(venue: str, symbol: str) -> str:
    if "USDTF" in venue:
        return f"{symbol}-PERP.BINANCE"
    return f"{symbol}.BINANCE"


def _split_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _prepare_dir(path: Path, *, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"{path} already exists; use --overwrite to replace it")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _run_old_converter(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    old_catalog_root: Path,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "convert_day.py",
        "--date",
        date,
        "--staging",
        "--catalog-root",
        str(old_catalog_root),
        "--symbols",
        ",".join(symbols),
        "--venues",
        ",".join(venues),
        "--allow-partial-overwrite",
    ]
    env = os.environ.copy()
    env["CRYPTO_RECORDER_DATA_ROOT"] = str(data_root)
    result = subprocess.run(
        cmd,
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
    }


def _run_new_pipeline(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    replay_root: Path,
    new_catalog_root: Path,
    start: datetime,
    end: datetime,
    overwrite: bool,
) -> dict[str, Any]:
    replay_results = []
    for venue in venues:
        for symbol in symbols:
            replay_results.append(
                build_replay_for_symbol(venue, symbol, date, data_root, replay_root)
            )

    catalog_result = generate_catalog_from_replay(
        replay_root=replay_root,
        catalog_root=new_catalog_root,
        job_id="validation_new",
        symbols=symbols,
        venues=venues,
        start=start,
        end=end,
        profile="trades_only",
        overwrite=overwrite,
    )
    return {
        "replay_results": replay_results,
        "catalog_result": catalog_result,
        "catalog_path": str(new_catalog_root / "job_validation_new"),
    }


def validate_catalog_equivalence(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    work_root: Path,
    old_catalog_root: Path,
    replay_root: Path,
    new_catalog_root: Path,
    profile: str,
    overwrite: bool,
) -> dict[str, Any]:
    start = _parse_date(date)
    end = start + timedelta(days=1)
    report: dict[str, Any] = {
        "date": date,
        "symbols": symbols,
        "venues": venues,
        "profile": profile,
        "status": "failed",
        "old_path": str(old_catalog_root),
        "new_path": str(new_catalog_root / "job_validation_new"),
        "comparison": {},
        "notes": [],
        "errors": [],
    }

    if profile != "trades_only":
        report["status"] = "skipped"
        report["notes"].append("generate_catalog full_l2/depth validation is deferred")
        return report

    work_root.mkdir(parents=True, exist_ok=True)
    _prepare_dir(old_catalog_root, overwrite=overwrite)
    _prepare_dir(replay_root, overwrite=overwrite)
    _prepare_dir(new_catalog_root, overwrite=overwrite)

    old_result = _run_old_converter(
        date=date,
        symbols=symbols,
        venues=venues,
        data_root=data_root,
        old_catalog_root=old_catalog_root,
    )
    report["old_run"] = old_result
    if old_result["returncode"] != 0:
        report["errors"].append("old convert_day.py run failed")
        return report

    new_result = _run_new_pipeline(
        date=date,
        symbols=symbols,
        venues=venues,
        data_root=data_root,
        replay_root=replay_root,
        new_catalog_root=new_catalog_root,
        start=start,
        end=end,
        overwrite=overwrite,
    )
    report["new_run"] = new_result
    if new_result["catalog_result"].get("status") != "success":
        report["errors"].append("new replay-generated catalog run failed")
        return report

    expected_ids = sorted(_instrument_id_for(venue, symbol) for venue in venues for symbol in symbols)
    old_ids = load_instrument_ids(old_catalog_root)
    new_catalog_path = new_catalog_root / "job_validation_new"
    new_ids = load_instrument_ids(new_catalog_path)
    old_expected_ids = sorted(instrument_id for instrument_id in old_ids if instrument_id in expected_ids)
    new_expected_ids = sorted(instrument_id for instrument_id in new_ids if instrument_id in expected_ids)

    comparison: dict[str, Any] = {
        "expected_instrument_ids": expected_ids,
        "old_instrument_ids": old_ids,
        "new_instrument_ids": new_ids,
        "instrument_ids_match": old_expected_ids == new_expected_ids == expected_ids,
        "by_instrument": {},
    }

    all_passed = comparison["instrument_ids_match"]
    for instrument_id in expected_ids:
        old_ticks = load_trade_ticks(
            old_catalog_root,
            instrument_id,
            start=int(start.timestamp() * 1_000_000_000),
            end=int(end.timestamp() * 1_000_000_000),
        )
        new_ticks = load_trade_ticks(
            new_catalog_path,
            instrument_id,
            start=int(start.timestamp() * 1_000_000_000),
            end=int(end.timestamp() * 1_000_000_000),
        )
        instrument_comparison = compare_trade_ticks_semantic(old_ticks, new_ticks)
        comparison["by_instrument"][instrument_id] = instrument_comparison
        all_passed = all_passed and instrument_comparison["passed"]

    if len(expected_ids) == 1:
        only = comparison["by_instrument"][expected_ids[0]]
        comparison.update(
            {
                "trade_count_old": only["trade_count_old"],
                "trade_count_new": only["trade_count_new"],
                "trade_count_match": only["trade_count_match"],
                "ts_min_old": only["ts_min_old"],
                "ts_min_new": only["ts_min_new"],
                "ts_max_old": only["ts_max_old"],
                "ts_max_new": only["ts_max_new"],
                "timestamp_range_match": only["timestamp_range_match"],
                "sample_mismatches": only["sample_mismatches"],
            }
        )

    report["comparison"] = comparison
    report["status"] = "passed" if all_passed else "failed"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare old convert_day.py catalog output with replay-generated trades_only output.",
    )
    parser.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    parser.add_argument("--symbols", required=True, help="Comma-separated raw symbols")
    parser.add_argument("--venues", required=True, help="Comma-separated venues")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--old-catalog-root", type=Path, required=True)
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--new-catalog-root", type=Path, required=True)
    parser.add_argument("--profile", choices=["trades_only", "full_l2"], default="trades_only")
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    report = validate_catalog_equivalence(
        date=args.date,
        symbols=_split_csv(args.symbols),
        venues=_split_csv(args.venues),
        data_root=args.data_root,
        work_root=args.work_root,
        old_catalog_root=args.old_catalog_root,
        replay_root=args.replay_root,
        new_catalog_root=args.new_catalog_root,
        profile=args.profile,
        overwrite=args.overwrite,
    )
    report_path = args.report_path or (args.work_root / f"catalog_equivalence_{args.date}.json")
    write_validation_report(report, report_path)

    print(f"Catalog equivalence status: {report['status']}")
    print(f"Report: {report_path}")
    comparison = report.get("comparison") or {}
    if "trade_count_old" in comparison:
        print(
            "Trades old/new: "
            f"{comparison['trade_count_old']} / {comparison['trade_count_new']}"
        )
        print(f"Timestamp range match: {comparison['timestamp_range_match']}")
        print(f"Sample mismatches: {len(comparison.get('sample_mismatches') or [])}")

    if report["status"] == "passed":
        return 0
    if report["status"] == "skipped":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
