"""Bounded daily/backlog orchestration for schema-v2 replay construction."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import Counter
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from config import (
    DAILY_REPORT_ROOT,
    DATA_ROOT,
    REPLAY_BACKLOG_DAYS,
    REPLAY_MAX_BUILD_DATES,
    REPLAY_RECOVERY_MAX_ACTIONS,
    REPLAY_RECOVERY_MAX_ENTRIES,
    REPLAY_ROOT,
    REPLAY_SCHEMA_VERSION,
)
from pipeline.replay_lifecycle import (
    ReplayBuildActiveError,
    ReplayLifecycleContext,
    acquire_replay_build_lock,
    atomic_write_json,
    reconcile_replay_root,
    tree_size_bytes,
    utc_now_iso,
)

logger = logging.getLogger(__name__)

REPORT_CONTRACT_VERSION = 2
ELIGIBLE_REPLAY_CHANNELS = frozenset({"depth_v2", "trade_v2"})
SUCCESS_OUTCOMES = frozenset({"built", "skipped_valid", "recovered"})
OUTCOME_NAMES = (
    "built",
    "skipped_valid",
    "deferred_not_ready",
    "missing_required_raw",
    "source_changed_rebuild_required",
    "incompatible_schema_rebuild_required",
    "recovered",
    "failed",
)
MAX_BACKLOG_DAYS = 31
MAX_BUILD_DATES = 31


def _parse_date_arg(date_str: str) -> str:
    if date_str.lower() == "yesterday":
        return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid date format: {date_str} (expected YYYY-MM-DD or 'yesterday')"
        ) from exc


def _bounded_positive(name: str, value: int, maximum: int) -> int:
    if value < 1 or value > maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}, got {value}")
    return value


def _selected_dates(newest: str, backlog_days: int) -> list[str]:
    newest_date = date_type.fromisoformat(newest)
    return [
        (newest_date - timedelta(days=offset)).isoformat()
        for offset in reversed(range(backlog_days))
    ]


def run_raw_manifest(date_str: str, data_root: Path) -> dict:
    from pipeline.raw_manifest import scan_raw_coverage

    logger.info("Scanning raw data coverage for %s", date_str)
    return scan_raw_coverage(date_str, data_root)


def _inventory(
    coverage: dict,
    selected_symbols: set[str] | None = None,
    selected_venues: set[str] | None = None,
) -> list[dict]:
    inventory: list[dict] = []
    for venue in sorted(coverage.get("data", {})):
        if selected_venues is not None and venue not in selected_venues:
            continue
        for symbol, channels in sorted(coverage["data"][venue].items()):
            if selected_symbols is not None and symbol not in selected_symbols:
                continue
            present = sorted(set(channels) & ELIGIBLE_REPLAY_CHANNELS)
            if not present:
                continue
            inventory.append(
                {
                    "venue": venue,
                    "symbol": symbol,
                    "channels_present": present,
                    "channels_missing": sorted(ELIGIBLE_REPLAY_CHANNELS - set(present)),
                }
            )
    return inventory


def _canonical_partition(replay_root: Path, venue: str, symbol: str, date_str: str) -> Path:
    return replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date_str}"


def _partition_counts_and_size(partition: Path) -> dict:
    depth_count = trade_count = 0
    manifest_path = partition / "manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text())
            depth_count = int(
                manifest.get("depth_record_count", manifest.get("depth_count", 0))
            )
            trade_count = int(
                manifest.get("trade_record_count", manifest.get("trade_count", 0))
            )
        except Exception:
            pass
    try:
        allocated, apparent = tree_size_bytes(partition)
    except Exception:
        allocated = apparent = None
    return {
        "depth_count": depth_count,
        "trade_count": trade_count,
        "allocated_bytes": allocated,
        "apparent_bytes": apparent,
    }


def _normalize_result(result: dict, replay_root: Path) -> dict:
    from stores.replay_writer import validate_partition

    outcome = result.get("outcome")
    if outcome not in OUTCOME_NAMES:
        outcome = {
            "success": "built",
            "skipped": "skipped_valid",
            "deferred": "deferred_not_ready",
        }.get(result.get("status"), "failed")
    normalized = dict(result)
    normalized["outcome"] = outcome
    partition = _canonical_partition(
        replay_root, result["venue"], result["symbol"], result["date"]
    )
    if outcome in SUCCESS_OUTCOMES and not validate_partition(partition):
        outcome = "failed"
        normalized["outcome"] = outcome
        normalized["status"] = "failed"
        normalized.setdefault("errors", []).append(
            f"post-build/reuse routine validation failed: {partition}"
        )
    observed = _partition_counts_and_size(partition)
    for field, value in observed.items():
        if field in ("depth_count", "trade_count"):
            normalized[field] = int(normalized.get(field) or value or 0)
        else:
            normalized[field] = value
    normalized["reason"] = "; ".join(normalized.get("errors", [])) or None
    return normalized


def run_build_replay_store(
    date_str: str,
    symbols: list[str],
    data_root: Path,
    replay_root: Path,
    *,
    check_repartition_readiness: bool = True,
    schema_version: int = 0,
    lifecycle_context: ReplayLifecycleContext | None = None,
    rebuild_source_changed: bool = False,
    replace_incompatible: bool = False,
    allow_build: bool = True,
    venues: list[str] | None = None,
) -> dict:
    """Compatibility single-date API plus exact partition outcomes."""
    from pipeline.build_replay_store import build_replay_for_symbol

    coverage = run_raw_manifest(date_str, data_root)
    selected = set(symbols)
    inventory = _inventory(
        coverage,
        selected,
        set(venues) if venues is not None else None,
    )
    results: list[dict] = []
    for item in inventory:
        venue, symbol = item["venue"], item["symbol"]
        if item["channels_missing"]:
            results.append(
                {
                    "venue": venue,
                    "symbol": symbol,
                    "date": date_str,
                    "status": "failed",
                    "outcome": "missing_required_raw",
                    "depth_count": 0,
                    "trade_count": 0,
                    "errors": [
                        "missing required raw channel(s): "
                        + ", ".join(item["channels_missing"])
                    ],
                }
            )
            continue
        partition = _canonical_partition(replay_root, venue, symbol, date_str)
        if not allow_build and not partition.exists():
            results.append(
                {
                    "venue": venue,
                    "symbol": symbol,
                    "date": date_str,
                    "status": "deferred",
                    "outcome": "deferred_not_ready",
                    "depth_count": 0,
                    "trade_count": 0,
                    "errors": ["max-build-dates reached before this incomplete date"],
                }
            )
            continue
        result = build_replay_for_symbol(
            venue,
            symbol,
            date_str,
            data_root,
            replay_root,
            schema_version=schema_version,
            check_repartition_readiness=check_repartition_readiness,
            lifecycle_context=lifecycle_context,
            rebuild_source_changed=rebuild_source_changed,
            replace_incompatible=replace_incompatible,
        )
        results.append(_normalize_result(result, replay_root))

    counts = Counter(result["outcome"] for result in results)
    total = len(results)
    successful = sum(counts[name] for name in SUCCESS_OUTCOMES)
    if total == 0:
        status = "no_data"
    elif successful == total:
        status = "success"
    elif successful == 0 and counts["deferred_not_ready"] == total:
        status = "deferred"
    elif successful == 0:
        status = "failed"
    else:
        status = "partial"
    return {
        "status": status,
        "symbols_processed": counts["built"],
        "symbols_skipped": counts["skipped_valid"],
        "symbols_deferred": counts["deferred_not_ready"],
        "symbols_total": total,
        "depth_records": sum(int(r.get("depth_count", 0)) for r in results),
        "trade_records": sum(int(r.get("trade_count", 0)) for r in results),
        "outcome_counts": {name: counts[name] for name in OUTCOME_NAMES},
        "eligible_partition_inventory": inventory,
        "results": results,
    }


def _date_report(
    *,
    context: ReplayLifecycleContext,
    date_str: str,
    newest_date: str,
    backlog_days: int,
    max_build_dates: int,
    schema_version: int,
    rebuild_source_changed: bool,
    replace_incompatible: bool,
    recovery_actions: list[dict],
    raw_result: dict,
    replay_result: dict,
    started_utc: str,
    runtime_sec: float,
) -> dict:
    counts = replay_result["outcome_counts"]
    report = {
        "report_contract_version": REPORT_CONTRACT_VERSION,
        "run_id": context.run_id,
        "lock_metadata": context.metadata,
        "repository_sha": context.metadata["repository_sha"],
        "requested_schema_version": schema_version,
        "effective_schema_version": schema_version,
        "date": date_str,
        "newest_target_date": newest_date,
        "backlog_days": backlog_days,
        "max_build_dates": max_build_dates,
        "data_root": str(context.data_root.resolve()),
        "replay_root": str(context.replay_root.resolve()),
        "report_root": str(context.report_root.resolve()),
        "source_change_policy": (
            "rebuild_exact_partition" if rebuild_source_changed else "fail_closed"
        ),
        "incompatible_schema_policy": (
            "replace_exact_partition" if replace_incompatible else "fail_closed"
        ),
        "recovery_actions": recovery_actions,
        "eligible_partition_inventory": replay_result["eligible_partition_inventory"],
        "partition_results": replay_result["results"],
        "built_count": counts["built"],
        "skipped_valid_count": counts["skipped_valid"],
        "deferred_count": counts["deferred_not_ready"],
        "missing_count": counts["missing_required_raw"],
        "source_changed_count": counts["source_changed_rebuild_required"],
        "incompatible_schema_count": counts["incompatible_schema_rebuild_required"],
        "recovered_count": counts["recovered"],
        "failed_count": counts["failed"],
        "total_eligible_count": replay_result["symbols_total"],
        "depth_record_count": replay_result["depth_records"],
        "trade_record_count": replay_result["trade_records"],
        "staging_observation_count": sum(
            1 for action in recovery_actions if "staging" in action["action"]
        ),
        "start_utc": started_utc,
        "end_utc": utc_now_iso(),
        "runtime_seconds": runtime_sec,
        "final_status": replay_result["status"],
        "status": replay_result["status"],
        "process_exit_classification": (
            "success" if replay_result["status"] == "success" else "nonzero_incomplete"
        ),
        "raw_coverage": {
            "venues": raw_result.get("venues", []),
            "symbol_count": raw_result.get("symbol_count", 0),
        },
    }
    report["replay_build"] = {
        "status": replay_result["status"],
        "symbols_processed": replay_result["symbols_processed"],
        "symbols_total": replay_result["symbols_total"],
        "depth_records": replay_result["depth_records"],
        "trade_records": replay_result["trade_records"],
    }
    report["errors"] = [
        error
        for result in replay_result["results"]
        for error in result.get("errors", [])
    ]
    return report


def _failed_replay_result(
    inventory: list[dict], date_str: str, reason: str
) -> dict:
    """Represent an orchestration exception without inventing artifact success."""
    results = [
        {
            "venue": item["venue"],
            "symbol": item["symbol"],
            "date": date_str,
            "status": "failed",
            "outcome": "failed",
            "depth_count": 0,
            "trade_count": 0,
            "errors": [reason],
        }
        for item in inventory
    ]
    counts = {name: 0 for name in OUTCOME_NAMES}
    counts["failed"] = len(results)
    return {
        "status": "failed",
        "symbols_processed": 0,
        "symbols_skipped": 0,
        "symbols_deferred": 0,
        "symbols_total": len(results),
        "depth_records": 0,
        "trade_records": 0,
        "outcome_counts": counts,
        "eligible_partition_inventory": inventory,
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
    """Backward-compatible atomic single-date report helper."""
    report = {
        "report_contract_version": REPORT_CONTRACT_VERSION,
        "date": date_str,
        "created_at_utc": utc_now_iso(),
        "runtime_sec": runtime_sec,
        "status": replay_result.get("status", "failed"),
        "data_root": str(data_root),
        "replay_root": str(replay_root),
        "report_root": str(report_root),
        "raw_coverage": {
            "venues": raw_result.get("venues", []),
            "symbol_count": raw_result.get("symbol_count", 0),
        },
        "replay_build": {
            "status": replay_result.get("status", "failed"),
            "symbols_processed": replay_result.get("symbols_processed", 0),
            "symbols_total": replay_result.get("symbols_total", 0),
            "depth_records": replay_result.get("depth_records", 0),
            "trade_records": replay_result.get("trade_records", 0),
            "outcome_counts": replay_result.get("outcome_counts", {}),
        },
        "errors": [
            error
            for result in replay_result.get("results", [])
            for error in result.get("errors", [])
        ],
    }
    atomic_write_json(report_root / f"daily_build_{date_str}.json", report)
    return report


def run_backlog(
    *,
    newest_date: str,
    backlog_days: int,
    max_build_dates: int,
    schema_version: int,
    data_root: Path,
    replay_root: Path,
    report_root: Path,
    symbols: list[str] | None = None,
    venues: list[str] | None = None,
    rebuild_source_changed: bool = False,
    replace_incompatible: bool = False,
) -> tuple[dict, int]:
    backlog_days = _bounded_positive("backlog-days", backlog_days, MAX_BACKLOG_DAYS)
    max_build_dates = _bounded_positive(
        "max-build-dates", max_build_dates, MAX_BUILD_DATES
    )
    if schema_version not in (0, 1, 2):
        raise ValueError(f"unsupported schema version: {schema_version}")
    started_utc = utc_now_iso()
    started = time.monotonic()
    dates = _selected_dates(newest_date, backlog_days)

    with acquire_replay_build_lock(
        replay_root=replay_root,
        data_root=data_root,
        report_root=report_root,
    ) as context:
        try:
            recovery_actions = reconcile_replay_root(
                context,
                max_entries=REPLAY_RECOVERY_MAX_ENTRIES,
                max_actions=REPLAY_RECOVERY_MAX_ACTIONS,
            )
        except Exception as exc:
            reason = f"cross-date reconciliation failed closed: {type(exc).__name__}: {exc}"
            run_report = {
                "report_contract_version": REPORT_CONTRACT_VERSION,
                "run_id": context.run_id,
                "lock_metadata": context.metadata,
                "repository_sha": context.metadata["repository_sha"],
                "requested_schema_version": schema_version,
                "effective_schema_version": schema_version,
                "newest_target_date": newest_date,
                "backlog_days": backlog_days,
                "max_build_dates": max_build_dates,
                "data_root": str(data_root.resolve()),
                "replay_root": str(replay_root.resolve()),
                "report_root": str(report_root.resolve()),
                "source_change_policy": (
                    "rebuild_exact_partition"
                    if rebuild_source_changed
                    else "fail_closed"
                ),
                "incompatible_schema_policy": (
                    "replace_exact_partition"
                    if replace_incompatible
                    else "fail_closed"
                ),
                "recovery_actions": [],
                "dates_inspected": [],
                "dates_selected_for_build": [],
                "selected_venues": sorted(set(venues)) if venues else None,
                "selected_symbols": sorted(set(symbols)) if symbols else None,
                "date_report_paths": [],
                "date_results": [],
                **{
                    field: 0
                    for field in (
                        "built_count",
                        "skipped_valid_count",
                        "deferred_count",
                        "missing_count",
                        "source_changed_count",
                        "incompatible_schema_count",
                        "recovered_count",
                        "failed_count",
                        "total_eligible_count",
                        "depth_record_count",
                        "trade_record_count",
                    )
                },
                "errors": [reason],
                "start_utc": started_utc,
                "end_utc": utc_now_iso(),
                "runtime_seconds": time.monotonic() - started,
                "final_status": "failed",
                "process_exit_classification": "reconciliation_failure",
            }
            atomic_write_json(
                report_root / f"replay_backlog_{context.run_id}.json",
                run_report,
            )
            return run_report, 1
        recovered_keys = {
            (action.get("venue"), action.get("date"), action.get("symbol"))
            for action in recovery_actions
            if action["action"] == "backup_restored"
        }
        date_reports: list[dict] = []
        build_dates_used = 0
        selected_for_build: list[str] = []
        selected_symbols = sorted(set(symbols)) if symbols else None
        selected_venues = sorted(set(venues)) if venues else None

        for date_str in dates:
            date_started = time.monotonic()
            date_started_utc = utc_now_iso()
            try:
                raw_result = run_raw_manifest(date_str, data_root)
                inventory = _inventory(
                    raw_result,
                    set(selected_symbols) if selected_symbols else None,
                    set(selected_venues) if selected_venues else None,
                )
            except Exception as exc:
                reason = f"raw inventory failed closed: {type(exc).__name__}: {exc}"
                replay_result = _failed_replay_result([], date_str, reason)
                report = _date_report(
                    context=context,
                    date_str=date_str,
                    newest_date=newest_date,
                    backlog_days=backlog_days,
                    max_build_dates=max_build_dates,
                    schema_version=schema_version,
                    rebuild_source_changed=rebuild_source_changed,
                    replace_incompatible=replace_incompatible,
                    recovery_actions=[
                        action
                        for action in recovery_actions
                        if action.get("date") == date_str
                    ],
                    raw_result={},
                    replay_result=replay_result,
                    started_utc=date_started_utc,
                    runtime_sec=time.monotonic() - date_started,
                )
                report["orchestration_errors"] = [reason]
                atomic_write_json(
                    report_root / f"daily_build_{date_str}.json", report
                )
                date_reports.append(report)
                break
            requires_build = any(
                not _canonical_partition(
                    replay_root, item["venue"], item["symbol"], date_str
                ).exists()
                for item in inventory
                if not item["channels_missing"]
            )
            allow_build = not requires_build or build_dates_used < max_build_dates
            date_orchestration_failed = False
            try:
                replay_result = run_build_replay_store(
                    date_str,
                    [item["symbol"] for item in inventory],
                    data_root,
                    replay_root,
                    check_repartition_readiness=True,
                    schema_version=schema_version,
                    lifecycle_context=context,
                    rebuild_source_changed=(
                        rebuild_source_changed and build_dates_used < max_build_dates
                    ),
                    replace_incompatible=(
                        replace_incompatible and build_dates_used < max_build_dates
                    ),
                    allow_build=allow_build,
                    venues=selected_venues,
                )
            except Exception as exc:
                reason = f"date build orchestration failed closed: {type(exc).__name__}: {exc}"
                replay_result = _failed_replay_result(inventory, date_str, reason)
                date_orchestration_failed = True
            if replay_result["outcome_counts"]["built"]:
                build_dates_used += 1
                selected_for_build.append(date_str)
            for result in replay_result["results"]:
                if (
                    result["outcome"] == "skipped_valid"
                    and (result["venue"], date_str, result["symbol"]) in recovered_keys
                ):
                    result["outcome"] = "recovered"
            replay_result["outcome_counts"] = {
                name: sum(1 for result in replay_result["results"] if result["outcome"] == name)
                for name in OUTCOME_NAMES
            }
            report = _date_report(
                context=context,
                date_str=date_str,
                newest_date=newest_date,
                backlog_days=backlog_days,
                max_build_dates=max_build_dates,
                schema_version=schema_version,
                rebuild_source_changed=rebuild_source_changed,
                replace_incompatible=replace_incompatible,
                recovery_actions=[
                    action for action in recovery_actions if action.get("date") == date_str
                ],
                raw_result=raw_result,
                replay_result=replay_result,
                started_utc=date_started_utc,
                runtime_sec=time.monotonic() - date_started,
            )
            atomic_write_json(report_root / f"daily_build_{date_str}.json", report)
            date_reports.append(report)
            if date_orchestration_failed:
                break

        overall_success = bool(date_reports) and all(
            report["final_status"] == "success" for report in date_reports
        )
        aggregate_counts = {
            field: sum(int(report[field]) for report in date_reports)
            for field in (
                "built_count",
                "skipped_valid_count",
                "deferred_count",
                "missing_count",
                "source_changed_count",
                "incompatible_schema_count",
                "recovered_count",
                "failed_count",
                "total_eligible_count",
                "depth_record_count",
                "trade_record_count",
            )
        }
        run_report = {
            "report_contract_version": REPORT_CONTRACT_VERSION,
            "run_id": context.run_id,
            "lock_metadata": context.metadata,
            "repository_sha": context.metadata["repository_sha"],
            "requested_schema_version": schema_version,
            "effective_schema_version": schema_version,
            "newest_target_date": newest_date,
            "backlog_days": backlog_days,
            "max_build_dates": max_build_dates,
            "data_root": str(data_root.resolve()),
            "replay_root": str(replay_root.resolve()),
            "report_root": str(report_root.resolve()),
            "source_change_policy": "rebuild_exact_partition" if rebuild_source_changed else "fail_closed",
            "incompatible_schema_policy": "replace_exact_partition" if replace_incompatible else "fail_closed",
            "recovery_actions": recovery_actions,
            "dates_inspected": [report["date"] for report in date_reports],
            "dates_selected_for_build": selected_for_build,
            "selected_venues": selected_venues,
            "selected_symbols": selected_symbols,
            "date_report_paths": [
                str(report_root / f"daily_build_{report['date']}.json")
                for report in date_reports
            ],
            "date_results": [
                {
                    "date": report["date"],
                    "final_status": report["final_status"],
                    "eligible_partition_inventory": report["eligible_partition_inventory"],
                    "partition_results": report["partition_results"],
                    "built_count": report["built_count"],
                    "skipped_valid_count": report["skipped_valid_count"],
                    "deferred_count": report["deferred_count"],
                    "missing_count": report["missing_count"],
                    "source_changed_count": report["source_changed_count"],
                    "incompatible_schema_count": report["incompatible_schema_count"],
                    "recovered_count": report["recovered_count"],
                    "failed_count": report["failed_count"],
                }
                for report in date_reports
            ],
            "errors": [
                error
                for report in date_reports
                for error in report.get("errors", [])
            ]
            + [
                error
                for report in date_reports
                for error in report.get("orchestration_errors", [])
            ],
            **aggregate_counts,
            "start_utc": started_utc,
            "end_utc": utc_now_iso(),
            "runtime_seconds": time.monotonic() - started,
            "final_status": "success" if overall_success else "failed",
            "process_exit_classification": "success" if overall_success else "nonzero_incomplete",
        }
        atomic_write_json(report_root / f"replay_backlog_{context.run_id}.json", run_report)
        return run_report, 0 if overall_success else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded replay backlog orchestrator")
    parser.add_argument("--date", required=True, help="Newest target date (YYYY-MM-DD or yesterday)")
    parser.add_argument("--symbols", default=None, help="Optional comma-separated symbol restriction")
    parser.add_argument("--venues", default=None, help="Optional comma-separated venue restriction")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--replay-root", type=Path, default=REPLAY_ROOT)
    parser.add_argument("--report-root", type=Path, default=DAILY_REPORT_ROOT)
    parser.add_argument("--backlog-days", type=int, default=REPLAY_BACKLOG_DAYS)
    parser.add_argument("--max-build-dates", type=int, default=REPLAY_MAX_BUILD_DATES)
    parser.add_argument("--schema-version", type=int, choices=(0, 1, 2), default=REPLAY_SCHEMA_VERSION)
    parser.add_argument("--rebuild-source-changed", action="store_true")
    parser.add_argument("--replace-incompatible", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        newest = _parse_date_arg(args.date)
        symbols = None
        if args.symbols:
            symbols = sorted({item.strip().upper() for item in args.symbols.split(",") if item.strip()})
            if not symbols:
                raise ValueError("--symbols did not contain any symbol")
        venues = None
        if args.venues:
            venues = sorted(
                {item.strip().upper() for item in args.venues.split(",") if item.strip()}
            )
            if not venues:
                raise ValueError("--venues did not contain any venue")
        _report, exit_code = run_backlog(
            newest_date=newest,
            backlog_days=args.backlog_days,
            max_build_dates=args.max_build_dates,
            schema_version=args.schema_version,
            data_root=args.data_root,
            replay_root=args.replay_root,
            report_root=args.report_root,
            symbols=symbols,
            venues=venues,
            rebuild_source_changed=args.rebuild_source_changed,
            replace_incompatible=args.replace_incompatible,
        )
        return exit_code
    except ReplayBuildActiveError as exc:
        logger.error("build already active: %s", exc)
        return 2
    except Exception as exc:
        logger.error("daily replay backlog failed closed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
