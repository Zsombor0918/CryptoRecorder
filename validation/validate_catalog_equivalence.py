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

from config import (
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
    EMIT_DEPTH10_DEFAULT,
)
from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import generate_catalog_from_replay
from validation.catalog_compare import (
    compare_book_checkpoints,
    compare_depth10_semantic,
    compare_order_book_deltas_semantic,
    compare_trade_ticks_semantic,
    load_instrument_ids,
    load_order_book_deltas,
    load_order_book_depth10,
    load_trade_ticks,
    write_validation_report,
)

logger = logging.getLogger(__name__)

# Profiles that exercise the depth (OrderBookDeltas / Depth10) comparison path.
_DEPTH_PROFILES = ("full_l2", "depth_only", "depth10")


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
    profile: str,
    overwrite: bool,
    emit_depth10: bool,
    depth10_interval_sec: float,
    derived_depth_snapshot_levels: int,
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
        profile=profile,
        overwrite=overwrite,
        emit_depth10=emit_depth10,
        depth10_interval_sec=depth10_interval_sec,
        derived_depth_snapshot_levels=derived_depth_snapshot_levels,
    )
    return {
        "replay_results": replay_results,
        "catalog_result": catalog_result,
        "catalog_path": str(new_catalog_root / "job_validation_new"),
    }


def _compare_depth_for_instrument(
    old_catalog_root: Path,
    new_catalog_path: Path,
    instrument_id: str,
    start_ns: int,
    end_ns: int,
    *,
    emit_depth10: bool,
    levels: int,
) -> dict[str, Any]:
    """Compare OrderBookDeltas, Depth10 and reconstructed book checkpoints."""
    old_deltas = load_order_book_deltas(old_catalog_root, instrument_id, start=start_ns, end=end_ns)
    new_deltas = load_order_book_deltas(new_catalog_path, instrument_id, start=start_ns, end=end_ns)

    deltas_cmp = compare_order_book_deltas_semantic(old_deltas, new_deltas)
    checkpoints_cmp = compare_book_checkpoints(
        old_deltas, new_deltas, start_ns, end_ns, levels=levels
    )

    out: dict[str, Any] = {
        "order_book_deltas": deltas_cmp,
        "book_checkpoints": checkpoints_cmp,
    }
    if emit_depth10:
        old_depth10 = load_order_book_depth10(
            old_catalog_root, instrument_id, start=start_ns, end=end_ns
        )
        new_depth10 = load_order_book_depth10(
            new_catalog_path, instrument_id, start=start_ns, end=end_ns
        )
        out["order_book_depth10"] = compare_depth10_semantic(old_depth10, new_depth10)
    else:
        out["order_book_depth10"] = {"passed": True, "skipped": True}

    out["passed"] = (
        deltas_cmp["passed"]
        and checkpoints_cmp["passed"]
        and out["order_book_depth10"].get("passed", True)
    )
    return out


def _read_new_manifest(new_catalog_path: Path) -> dict[str, Any]:
    manifest_path = new_catalog_path / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        return {}


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
    emit_depth10: bool = EMIT_DEPTH10_DEFAULT,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
) -> dict[str, Any]:
    start = _parse_date(date)
    end = start + timedelta(days=1)
    new_catalog_path = new_catalog_root / "job_validation_new"
    compares_trades = profile in ("trades_only", "full_l2")
    compares_depth = profile in _DEPTH_PROFILES
    report: dict[str, Any] = {
        "date": date,
        "symbols": symbols,
        "venues": venues,
        "profile": profile,
        "status": "failed",
        "old_catalog_root": str(old_catalog_root),
        "new_catalog_root": str(new_catalog_path),
        "replay_root": str(replay_root),
        "old_path": str(old_catalog_root),
        "new_path": str(new_catalog_path),
        "comparison": {},
        "diagnostics": {},
        "notes": [],
        "errors": [],
    }

    if profile not in ("trades_only", "full_l2"):
        report["status"] = "skipped"
        report["notes"].append(
            f"validate_catalog_equivalence supports trades_only and full_l2; got profile={profile}"
        )
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
        profile=profile,
        overwrite=overwrite,
        emit_depth10=emit_depth10,
        depth10_interval_sec=depth10_interval_sec,
        derived_depth_snapshot_levels=derived_depth_snapshot_levels,
    )
    report["new_run"] = new_result
    if new_result["catalog_result"].get("status") != "success":
        report["errors"].append("new replay-generated catalog run failed")
        return report

    expected_ids = sorted(_instrument_id_for(venue, symbol) for venue in venues for symbol in symbols)
    old_ids = load_instrument_ids(old_catalog_root)
    new_ids = load_instrument_ids(new_catalog_path)
    old_expected_ids = sorted(instrument_id for instrument_id in old_ids if instrument_id in expected_ids)
    new_expected_ids = sorted(instrument_id for instrument_id in new_ids if instrument_id in expected_ids)

    start_ns = int(start.timestamp() * 1_000_000_000)
    end_ns = int(end.timestamp() * 1_000_000_000)

    comparison: dict[str, Any] = {
        "expected_instrument_ids": expected_ids,
        "old_instrument_ids": old_ids,
        "new_instrument_ids": new_ids,
        "instrument_ids_match": old_expected_ids == new_expected_ids == expected_ids,
        "by_instrument": {},
    }

    all_passed = comparison["instrument_ids_match"]
    trades_all_passed = True
    deltas_all_passed = True
    depth10_all_passed = True
    checkpoints_all_passed = True

    for instrument_id in expected_ids:
        per_instrument: dict[str, Any] = {}

        if compares_trades:
            old_ticks = load_trade_ticks(old_catalog_root, instrument_id, start=start_ns, end=end_ns)
            new_ticks = load_trade_ticks(new_catalog_path, instrument_id, start=start_ns, end=end_ns)
            trades_cmp = compare_trade_ticks_semantic(old_ticks, new_ticks)
            per_instrument["trade_ticks"] = trades_cmp
            trades_all_passed = trades_all_passed and trades_cmp["passed"]
            all_passed = all_passed and trades_cmp["passed"]

        if compares_depth:
            depth_cmp = _compare_depth_for_instrument(
                old_catalog_root,
                new_catalog_path,
                instrument_id,
                start_ns,
                end_ns,
                emit_depth10=emit_depth10,
                levels=derived_depth_snapshot_levels,
            )
            per_instrument["order_book_deltas"] = depth_cmp["order_book_deltas"]
            per_instrument["order_book_depth10"] = depth_cmp["order_book_depth10"]
            per_instrument["book_checkpoints"] = depth_cmp["book_checkpoints"]
            deltas_all_passed = deltas_all_passed and depth_cmp["order_book_deltas"]["passed"]
            depth10_all_passed = depth10_all_passed and depth_cmp["order_book_depth10"].get(
                "passed", True
            )
            checkpoints_all_passed = (
                checkpoints_all_passed and depth_cmp["book_checkpoints"]["passed"]
            )
            all_passed = all_passed and depth_cmp["passed"]

        comparison["by_instrument"][instrument_id] = per_instrument

    # Aggregate, profile-shaped comparison block.
    if compares_trades:
        comparison["trade_ticks"] = {"passed": trades_all_passed}
    if compares_depth:
        comparison["order_book_deltas"] = {"passed": deltas_all_passed}
        comparison["order_book_depth10"] = {
            "passed": depth10_all_passed,
            "emitted": emit_depth10,
        }
        comparison["book_checkpoints"] = {"passed": checkpoints_all_passed}

    # Backward-compatible flat single-instrument trade fields.
    if compares_trades and len(expected_ids) == 1:
        only = comparison["by_instrument"][expected_ids[0]].get("trade_ticks")
        if only:
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

    new_manifest = _read_new_manifest(new_catalog_path)
    report["diagnostics"] = {
        "old_report": {
            "returncode": old_result["returncode"],
            "stdout_tail": old_result.get("stdout_tail", ""),
        },
        "new_manifest": new_manifest,
        "fenced_ranges": new_manifest.get("fenced_ranges", []),
        "equivalence_caveats": new_manifest.get("equivalence_caveats", []),
        "warnings": new_manifest.get("warnings", []),
    }

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
    parser.add_argument(
        "--emit-depth10",
        dest="emit_depth10",
        action="store_true",
        default=EMIT_DEPTH10_DEFAULT,
        help="Compare derived OrderBookDepth10 (full_l2). Default: on.",
    )
    parser.add_argument(
        "--no-emit-depth10",
        dest="emit_depth10",
        action="store_false",
        help="Skip OrderBookDepth10 comparison (full_l2).",
    )
    parser.add_argument(
        "--depth10-interval-sec",
        type=float,
        default=DEPTH10_INTERVAL_SEC,
        help=f"Depth10 snapshot interval for the new pipeline (default: {DEPTH10_INTERVAL_SEC}).",
    )
    parser.add_argument(
        "--derived-depth-snapshot-levels",
        type=int,
        default=DERIVED_DEPTH_SNAPSHOT_LEVELS,
        help=f"Depth10 levels for the new pipeline (default: {DERIVED_DEPTH_SNAPSHOT_LEVELS}).",
    )
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    symbols = _split_csv(args.symbols)
    venues = _split_csv(args.venues)

    report = validate_catalog_equivalence(
        date=args.date,
        symbols=symbols,
        venues=venues,
        data_root=args.data_root,
        work_root=args.work_root,
        old_catalog_root=args.old_catalog_root,
        replay_root=args.replay_root,
        new_catalog_root=args.new_catalog_root,
        profile=args.profile,
        overwrite=args.overwrite,
        emit_depth10=args.emit_depth10,
        depth10_interval_sec=args.depth10_interval_sec,
        derived_depth_snapshot_levels=args.derived_depth_snapshot_levels,
    )

    if args.report_path is not None:
        report_path = args.report_path
    elif args.profile == "full_l2":
        repo_root = Path(__file__).resolve().parent.parent
        symbol_tag = "-".join(symbols) if symbols else "ALL"
        report_path = (
            repo_root
            / "validation_reports"
            / f"full_l2_equivalence_{args.date}_{symbol_tag}.json"
        )
    else:
        report_path = args.work_root / f"catalog_equivalence_{args.date}.json"
    write_validation_report(report, report_path)

    print(f"Catalog equivalence status: {report['status']} (profile={report['profile']})")
    print(f"Report: {report_path}")
    comparison = report.get("comparison") or {}
    if "trade_count_old" in comparison:
        print(
            "Trades old/new: "
            f"{comparison['trade_count_old']} / {comparison['trade_count_new']}"
        )
        print(f"Timestamp range match: {comparison['timestamp_range_match']}")
        print(f"Sample mismatches: {len(comparison.get('sample_mismatches') or [])}")
    if "order_book_deltas" in comparison:
        print(f"OrderBookDeltas match: {comparison['order_book_deltas']['passed']}")
        print(f"OrderBookDepth10 match: {comparison['order_book_depth10']['passed']}")
        print(f"Book checkpoints match: {comparison['book_checkpoints']['passed']}")

    if report["status"] == "passed":
        return 0
    if report["status"] == "skipped":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
