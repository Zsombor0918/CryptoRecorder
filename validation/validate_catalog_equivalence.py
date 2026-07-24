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
from converter.readers import stream_raw_records
from pipeline.build_replay_store import build_replay_for_symbol
from stores.replay_reader import ReplayReader
from validation.replay_catalog_reconstruct import generate_catalog_from_replay
from validation.catalog_compare import (
    compare_book_checkpoints,
    compare_continuity_diagnostics_semantic,
    compare_depth10_semantic,
    compare_fenced_ranges_semantic,
    compare_instruments_semantic,
    compare_order_book_deltas_exhaustive,
    compare_quality_flags_semantic,
    compare_trade_ticks_exhaustive,
    iter_order_book_deltas_windowed,
    iter_trade_ticks_windowed,
    load_instrument_ids,
    load_instruments,
    load_order_book_deltas,
    load_order_book_depth10,
    write_validation_report,
)

logger = logging.getLogger(__name__)

# Profiles that exercise the depth (OrderBookDeltas / Depth10) comparison path.
_DEPTH_PROFILES = ("full_l2", "depth_only", "depth10")

# Default window size for the bounded-memory windowed loaders used by the
# acceptance path (see iter_trade_ticks_windowed()/iter_order_book_deltas_windowed()
# in validation.catalog_compare for the boundary-safety design and its
# explicit "not a proven strict memory ceiling from time alone" caveat).
DEFAULT_WINDOW_NS = 3_600_000_000_000  # 1 hour


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
    window_ns: int,
    emit_depth10: bool,
    levels: int,
) -> dict[str, Any]:
    """Compare OrderBookDeltas exhaustively (bounded-memory, order-preserving
    — the acceptance-gating comparison), plus Depth10 and reconstructed
    book checkpoints as non-gating diagnostics.

    `order_book_deltas` uses compare_order_book_deltas_exhaustive() fed by
    iter_order_book_deltas_windowed() (bounded-memory streaming, no
    full-day list materialization) and is the comparison that gates
    `passed` here — this is the acceptance-path wiring corrected per the
    issue #20 follow-up review.

    `book_checkpoints` and `order_book_depth10` remain informational-only
    diagnostics, NOT part of `passed`: compare_book_checkpoints() and
    load_order_book_depth10() both require a full-day list materialization
    internally (compare_book_checkpoints() calls `list(objects)` on both
    inputs; there is no windowed/streaming equivalent for either today),
    which is exactly what the bounded-memory acceptance path is designed to
    avoid for a complete production day. They remain useful, low-cost smoke
    signal on the small/local (Tier 1/2) data these currently run against,
    but must not be relied on as the pass/fail gate for a Tier-3
    representative production day. This is a deliberate, documented
    limitation, not an oversight — closing it (a windowed/streaming
    checkpoint reconstruction and Depth10 comparison) is out of scope for
    this correction and remains future work.
    """
    old_delta_stream = iter_order_book_deltas_windowed(
        old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    new_delta_stream = iter_order_book_deltas_windowed(
        new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    deltas_cmp = compare_order_book_deltas_exhaustive(old_delta_stream, new_delta_stream)

    # Diagnostic-only: full-day materialization, not gating `passed`.
    old_deltas_full = load_order_book_deltas(old_catalog_root, instrument_id, start=start_ns, end=end_ns)
    new_deltas_full = load_order_book_deltas(new_catalog_path, instrument_id, start=start_ns, end=end_ns)
    checkpoints_cmp = compare_book_checkpoints(
        old_deltas_full, new_deltas_full, start_ns, end_ns, levels=levels
    )
    checkpoints_cmp["gating"] = False

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
        depth10_cmp = compare_depth10_semantic(old_depth10, new_depth10)
        depth10_cmp["gating"] = False
        out["order_book_depth10"] = depth10_cmp
    else:
        out["order_book_depth10"] = {"passed": True, "skipped": True, "gating": False}

    # Only the exhaustive, bounded-memory delta comparison gates `passed`.
    out["passed"] = deltas_cmp["passed"]
    return out


def _load_old_convert_report(old_catalog_root: Path, date: str) -> dict[str, Any]:
    """Load convert_day.py's own report JSON (per-symbol continuity/fenced-
    range diagnostics live here — convert_day.py's Nautilus catalog output
    itself carries none of this). See convert_day.py's `_save_report()`:
    it writes to `catalog_root.parent / "convert_reports" / f"{date}.json"`,
    where `catalog_root` is exactly `old_catalog_root` as invoked by
    `_run_old_converter()` above."""
    report_path = old_catalog_root.parent / "convert_reports" / f"{date}.json"
    if not report_path.exists():
        return {}
    try:
        return json.loads(report_path.read_text())
    except (OSError, ValueError):
        return {}


def _compare_continuity_for_symbol(
    old_report: dict[str, Any], new_manifest: dict[str, Any], venue: str, symbol: str
) -> dict[str, Any]:
    """Compare snapshot-seed/resync/desync/fenced-range counts between the
    reference route's per-symbol depth report and the candidate route's
    manifest depth_diagnostics — see
    validation.catalog_compare.compare_continuity_diagnostics_semantic()
    for the field-name normalization this relies on."""
    key = f"{venue}/{symbol}"
    old_per_symbol = (old_report.get("per_symbol_depth") or {}).get(key, {})
    new_depth_diagnostics = new_manifest.get("depth_diagnostics") or {}
    if not old_per_symbol and not new_depth_diagnostics:
        return {
            "passed": True,
            "skipped": True,
            "reason": "no continuity diagnostics available on either side for this symbol",
        }
    return compare_continuity_diagnostics_semantic(old_per_symbol, new_depth_diagnostics)


def _compare_fenced_ranges_for_symbol(
    old_report: dict[str, Any], new_manifest: dict[str, Any], venue: str, symbol: str
) -> dict[str, Any]:
    """Compare individual fenced ranges by content where possible.

    The reference route (convert_day.py) only records up to 3 example
    fences per symbol in `per_symbol_fenced_ranges[...]["examples"]`, not
    the complete per-fence list — so an "extra_in_new" result is EXPECTED
    whenever the candidate legitimately has more than 3 fences for that
    symbol/day, and is not itself an equivalence failure. Only
    "missing_in_new" (every reference example must be reproduced in the
    candidate) is a meaningful signal from this truncated reference data;
    it is surfaced separately as `gating_passed` so callers can gate on the
    part of this comparison that is actually apples-to-apples, while
    `extra_in_new`/`count_match` remain visible for diagnostic context.
    """
    key = f"{venue}/{symbol}"
    old_entry = (old_report.get("per_symbol_fenced_ranges") or {}).get(key, {})
    old_examples = old_entry.get("examples") or []
    new_fences_all = new_manifest.get("fenced_ranges") or []
    new_fences_for_symbol = [
        f for f in new_fences_all if f.get("venue") == venue and f.get("symbol") == symbol
    ]
    result = compare_fenced_ranges_semantic(old_examples, new_fences_for_symbol)
    result["gating_passed"] = not result["missing_in_new"]
    result["note"] = (
        "reference side exposes only up to 3 example fences per symbol; "
        "extra_in_new is expected and non-gating, missing_in_new (via "
        "gating_passed) is the meaningful equivalence signal here"
    )
    return result


def _collect_quality_flags_from_raw(
    data_root: Path, venue: str, symbol: str, date: str
) -> list[Any]:
    flags: list[Any] = []
    for channel in ("depth_v2", "trade_v2"):
        for rec in stream_raw_records(venue, symbol, channel, date, root=data_root):
            flags.append(rec.get("quality_flags"))
    return flags


def _collect_quality_flags_from_replay(
    replay_root: Path, venue: str, symbol: str, date: str
) -> list[Any]:
    reader = ReplayReader(replay_root)
    flags: list[Any] = []
    for rec in reader.iter_depths(venue, symbol, date):
        flags.append(rec.get("quality_flags"))
    for rec in reader.iter_trades(venue, symbol, date):
        flags.append(rec.get("quality_flags"))
    return flags


def _compare_quality_flags_for_symbol(
    data_root: Path, replay_root: Path, venue: str, symbol: str, date: str
) -> dict[str, Any]:
    """Compare quality_flags content between the permanent raw source
    (data_raw, read directly) and the replay_store the candidate route
    builds from that same raw source.

    convert_day.py's Nautilus catalog output does not persist a per-event
    quality_flags field at all (Nautilus's TradeTick/OrderBookDelta objects
    have no such field) and its own report JSON does not expose a
    per-event quality_flags stream either — so there is no "old Nautilus
    catalog vs new Nautilus catalog" quality_flags comparison available.
    The one place quality_flags genuinely exists on both a reference and a
    candidate side is: the permanent raw source (`data_raw`, the ultimate
    ground truth both routes read from) versus the replay_store the
    candidate pipeline builds from it. This proves the replay pipeline
    faithfully preserves quality_flags content — a real, meaningful
    equivalence check, using a different "reference" (raw) than the rest of
    this validator (convert_day.py's catalog), documented explicitly here
    rather than silently assumed.

    Uses multiset (Counter) comparison via compare_quality_flags_semantic(),
    which does not depend on the raw stream's per-file ordering matching the
    replay's sorted ordering.
    """
    old_flags = _collect_quality_flags_from_raw(data_root, venue, symbol, date)
    new_flags = _collect_quality_flags_from_replay(replay_root, venue, symbol, date)
    return compare_quality_flags_semantic(old_flags, new_flags)


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
    window_ns: int = DEFAULT_WINDOW_NS,
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
    # Parallel (venue, symbol, instrument_id) triples — continuity/fenced-
    # range/quality-flag comparisons operate on venue+symbol, not
    # instrument_id, and need to be run alongside the per-instrument loop.
    venue_symbol_by_id: dict[str, tuple[str, str]] = {
        _instrument_id_for(venue, symbol): (venue, symbol) for venue in venues for symbol in symbols
    }

    # Instrument identity AND precision/increment comparison (issue #20
    # Phase 1 coverage-gap fix, now wired into the real acceptance path —
    # a wrong price_precision/tick-size on an otherwise-correctly-named
    # instrument was previously undetectable here).
    old_instruments = load_instruments(old_catalog_root)
    new_instruments = load_instruments(new_catalog_path)
    instrument_precision_cmp = compare_instruments_semantic(old_instruments, new_instruments)

    old_ids = load_instrument_ids(old_catalog_root)
    new_ids = load_instrument_ids(new_catalog_path)
    old_expected_ids = sorted(instrument_id for instrument_id in old_ids if instrument_id in expected_ids)
    new_expected_ids = sorted(instrument_id for instrument_id in new_ids if instrument_id in expected_ids)

    start_ns = int(start.timestamp() * 1_000_000_000)
    end_ns = int(end.timestamp() * 1_000_000_000)

    old_report = _load_old_convert_report(old_catalog_root, date)
    new_manifest = _read_new_manifest(new_catalog_path)

    comparison: dict[str, Any] = {
        "expected_instrument_ids": expected_ids,
        "old_instrument_ids": old_ids,
        "new_instrument_ids": new_ids,
        "instrument_ids_match": old_expected_ids == new_expected_ids == expected_ids,
        "instrument_precision": instrument_precision_cmp,
        "by_instrument": {},
    }

    all_passed = comparison["instrument_ids_match"] and instrument_precision_cmp["passed"]
    trades_all_passed = True
    deltas_all_passed = True
    depth10_all_passed = True
    checkpoints_all_passed = True
    continuity_all_passed = True
    fenced_ranges_all_passed = True
    quality_flags_all_passed = True

    for instrument_id in expected_ids:
        per_instrument: dict[str, Any] = {}
        venue, symbol = venue_symbol_by_id[instrument_id]

        if compares_trades:
            # Exhaustive, order-preserving, bounded-memory comparison —
            # the acceptance-gating trade comparison (issue #20 follow-up
            # correction). Fed by the windowed loader, never the full-day
            # load_trade_ticks() list loader, and never
            # compare_trade_ticks_semantic()'s sampled comparator.
            old_trade_stream = iter_trade_ticks_windowed(
                old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
            )
            new_trade_stream = iter_trade_ticks_windowed(
                new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
            )
            trades_cmp = compare_trade_ticks_exhaustive(old_trade_stream, new_trade_stream)
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
                window_ns=window_ns,
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
            # Only order_book_deltas gates `passed` here; book_checkpoints
            # and order_book_depth10 are diagnostic-only (see
            # _compare_depth_for_instrument()'s docstring).
            all_passed = all_passed and depth_cmp["passed"]

            continuity_cmp = _compare_continuity_for_symbol(old_report, new_manifest, venue, symbol)
            per_instrument["continuity_diagnostics"] = continuity_cmp
            continuity_all_passed = continuity_all_passed and continuity_cmp["passed"]
            all_passed = all_passed and continuity_cmp["passed"]

            fenced_ranges_cmp = _compare_fenced_ranges_for_symbol(old_report, new_manifest, venue, symbol)
            per_instrument["fenced_ranges"] = fenced_ranges_cmp
            fenced_ranges_all_passed = fenced_ranges_all_passed and fenced_ranges_cmp["gating_passed"]
            all_passed = all_passed and fenced_ranges_cmp["gating_passed"]

            quality_flags_cmp = _compare_quality_flags_for_symbol(data_root, replay_root, venue, symbol, date)
            per_instrument["quality_flags"] = quality_flags_cmp
            quality_flags_all_passed = quality_flags_all_passed and quality_flags_cmp["passed"]
            all_passed = all_passed and quality_flags_cmp["passed"]

        comparison["by_instrument"][instrument_id] = per_instrument

    # Aggregate, profile-shaped comparison block.
    if compares_trades:
        comparison["trade_ticks"] = {"passed": trades_all_passed}
    if compares_depth:
        comparison["order_book_deltas"] = {"passed": deltas_all_passed}
        comparison["order_book_depth10"] = {
            "passed": depth10_all_passed,
            "emitted": emit_depth10,
            "gating": False,
        }
        comparison["book_checkpoints"] = {"passed": checkpoints_all_passed, "gating": False}
        comparison["continuity_diagnostics"] = {"passed": continuity_all_passed}
        comparison["fenced_ranges"] = {"passed": fenced_ranges_all_passed}
        comparison["quality_flags"] = {"passed": quality_flags_all_passed}

    # Backward-compatible flat single-instrument trade fields. Field shape
    # intentionally changed (issue #20 follow-up correction): the exhaustive
    # comparator does not compute ts_min/ts_max/timestamp_range_match/
    # sample_mismatches the way the old sampled comparator did — those
    # fields are dropped here rather than faked.
    if compares_trades and len(expected_ids) == 1:
        only = comparison["by_instrument"][expected_ids[0]].get("trade_ticks")
        if only:
            comparison.update(
                {
                    "trade_count_old": only["trade_count_old"],
                    "trade_count_new": only["trade_count_new"],
                    "trade_count_match": only["trade_count_match"],
                    "positions_compared": only["positions_compared"],
                }
            )

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
    parser.add_argument(
        "--window-hours",
        type=float,
        default=DEFAULT_WINDOW_NS / 3_600_000_000_000,
        help=(
            "Bounded-memory time window (in hours) used by the exhaustive "
            "trade/delta comparators' windowed catalog loaders. Default: 1 "
            "hour. Tune based on measured per-window RSS for the target "
            "production day (issue #20 Tier 3) — a fixed time window bounds "
            "query result size per window but is not by itself a proven "
            "strict event-count/RSS ceiling."
        ),
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
        window_ns=int(args.window_hours * 3_600_000_000_000),
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
