"""Per-stage CLI entrypoints for the serial, process-isolated semantic-
equivalence gate. Each subcommand reads a
small JSON config file, does its ONE stage of work, and writes a small JSON
result fragment — designed to be invoked as its own child process by
`validation.serial_gate.run_stage()`, never imported and run in-process for
production use (import is fine for unit tests exercising a subcommand
function directly against small synthetic fixtures).

Subcommands:
  A. trades      -- exhaustive TradeTick comparison.
  B. deltas      -- exhaustive flattened OrderBookDelta comparison.
  C. depth10     -- exhaustive OrderBookDepth10 comparison.
  D. checkpoints -- deterministic order-book checkpoint comparison.
  E. continuity  -- reference/candidate continuity diagnostics comparison.
  F. fences      -- complete fenced-range count/digest comparison.
  G. metadata    -- exhaustive raw-to-replay logical metadata comparison plus
                   a fresh raw source-identity check.
  H. integrity   -- routine plus deep replay-partition integrity audit.
  I. report      -- aggregate an exact required set of result fragments.

Artifact construction remains in ``pipeline/``. This validation-only CLI
operates on already-produced raw/replay/catalog artifacts, and each substantial
comparison should run as its own cgroup-limited process.

Every subcommand writes ONLY small, already-summarized dicts to its
`--out` path (counts, pass/fail booleans, capped mismatch samples) — never
raw event lists. Stdout is used only for brief human-readable progress
lines; the parent process (`serial_gate.run_stage()`) redirects this
straight to a persistent log file, never captures it in memory.

Every config must name a precomputed, path-sanitized
``artifact_identity_path``, repeat its exact scope, and include the artifact
inputs/carry mode accepted by ``validation.artifact_identity``. Every stage
fully re-hashes those inputs before and after execution and records the
canonical identity-document SHA-256 plus component hashes in its fragment.
The final report independently recomputes the same identity and rejects any
fragment whose cryptographic binding differs, even when caller labels match.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

_SCOPE_FIELDS = (
    "date",
    "venue",
    "symbol",
    "instrument_id",
    "source_identity_sha256",
    "candidate_identity_sha256",
)


def _require_scope(config: dict[str, Any]) -> dict[str, str]:
    """Return the normalized artifact scope which binds stage fragments.

    A fragment without a complete source/candidate identity can be valid JSON
    while belonging to another symbol or artifact generation. Refuse that
    ambiguity before a reusable gate report is assembled.
    """
    scope = config.get("scope")
    if not isinstance(scope, dict):
        raise ValueError("scope must be an object binding this stage to its artifacts")
    normalized = {field: str(scope.get(field) or "") for field in _SCOPE_FIELDS}
    missing = [field for field, value in normalized.items() if not value]
    if missing:
        raise ValueError(f"scope is missing required fields: {', '.join(missing)}")
    return normalized


def _load_config(path: str) -> dict[str, Any]:
    from validation.artifact_identity import load_json_object

    return load_json_object(Path(path))


def _write_result(path: str, result: dict[str, Any]) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("x") as handle:
        json.dump(result, handle, indent=2, default=str)
        handle.write("\n")


def _verified_artifact_scope(
    config: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Load the required artifact identity and bind this config to it."""
    from validation.artifact_identity import (
        load_artifact_identity,
        validate_artifact_identity_document,
    )

    identity_path = config.get("artifact_identity_path")
    if (
        not isinstance(identity_path, str)
        or not identity_path
        or identity_path != identity_path.strip()
    ):
        raise ValueError("artifact_identity_path must be a non-empty string")
    identity = load_artifact_identity(Path(identity_path))
    identity_scope = validate_artifact_identity_document(identity)
    configured_scope = _require_scope(config)
    if configured_scope != identity_scope:
        raise ValueError(
            "configured scope does not exactly match the artifact identity scope"
        )

    for field in ("date", "venue", "symbol", "instrument_id"):
        configured = config.get(field)
        if configured is not None and str(configured) != identity_scope[field]:
            raise ValueError(
                f"configured {field} does not match the artifact identity scope"
            )

    instrument_ids = config.get("instrument_ids")
    if instrument_ids is not None and instrument_ids != [
        identity_scope["instrument_id"]
    ]:
        raise ValueError(
            "instrument_ids must exactly match the artifact identity instrument"
        )

    venue_symbols = config.get("venue_symbols")
    if venue_symbols is not None:
        expected = [
            {
                "venue": identity_scope["venue"],
                "symbol": identity_scope["symbol"],
                "instrument_id": identity_scope["instrument_id"],
            }
        ]
        if venue_symbols != expected:
            raise ValueError(
                "venue_symbols must exactly match the artifact identity scope"
            )
    return identity_scope, identity


def _cmd_trades(config: dict[str, Any]) -> dict[str, Any]:
    """Stage A: exhaustive TradeTick comparison for each requested
    instrument_id, streamed via the true batch-bounded reader (see
    validation.catalog_compare)."""
    from validation.catalog_compare import compare_trade_ticks_exhaustive, iter_trade_ticks_bounded

    old_catalog_root = Path(config["old_catalog_root"])
    new_catalog_path = Path(config["new_catalog_path"])
    instrument_ids = config["instrument_ids"]
    start_ns = int(config["start_ns"])
    end_ns = int(config["end_ns"])

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for instrument_id in instrument_ids:
        old_stream = iter_trade_ticks_bounded(old_catalog_root, instrument_id, start_ns, end_ns)
        new_stream = iter_trade_ticks_bounded(new_catalog_path, instrument_id, start_ns, end_ns)
        cmp_result = compare_trade_ticks_exhaustive(old_stream, new_stream)
        per_instrument[instrument_id] = cmp_result
        all_passed = all_passed and cmp_result["passed"]

    return {"stage": "trades", "by_instrument": per_instrument, "passed": all_passed}


def _cmd_deltas(config: dict[str, Any]) -> dict[str, Any]:
    """Stage B: exhaustive OrderBookDeltas comparison for each requested
    instrument_id."""
    from validation.catalog_compare import compare_order_book_deltas_exhaustive, iter_order_book_deltas_bounded

    old_catalog_root = Path(config["old_catalog_root"])
    new_catalog_path = Path(config["new_catalog_path"])
    instrument_ids = config["instrument_ids"]
    start_ns = int(config["start_ns"])
    end_ns = int(config["end_ns"])

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for instrument_id in instrument_ids:
        old_stream = iter_order_book_deltas_bounded(old_catalog_root, instrument_id, start_ns, end_ns)
        new_stream = iter_order_book_deltas_bounded(new_catalog_path, instrument_id, start_ns, end_ns)
        cmp_result = compare_order_book_deltas_exhaustive(old_stream, new_stream)
        per_instrument[instrument_id] = cmp_result
        all_passed = all_passed and cmp_result["passed"]

    return {"stage": "deltas", "by_instrument": per_instrument, "passed": all_passed}


def _comparison_catalog_config(config: dict[str, Any]) -> tuple[Path, Path, list[str], int, int]:
    old_catalog_root = Path(config["old_catalog_root"])
    new_catalog_path = Path(config["new_catalog_path"])
    instrument_ids = list(config["instrument_ids"])
    start_ns = int(config["start_ns"])
    end_ns = int(config["end_ns"])
    return old_catalog_root, new_catalog_path, instrument_ids, start_ns, end_ns


def _cmd_depth10(config: dict[str, Any]) -> dict[str, Any]:
    """Exhaustively compare Depth10 events for each requested instrument."""
    from validation.catalog_compare import (
        compare_order_book_depth10_exhaustive,
        iter_order_book_depth10_bounded,
    )

    old_catalog_root, new_catalog_path, instrument_ids, start_ns, end_ns = (
        _comparison_catalog_config(config)
    )
    emit_depth10 = bool(config.get("emit_depth10", True))

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for instrument_id in instrument_ids:
        if emit_depth10:
            old_depth10_stream = iter_order_book_depth10_bounded(old_catalog_root, instrument_id, start_ns, end_ns)
            new_depth10_stream = iter_order_book_depth10_bounded(new_catalog_path, instrument_id, start_ns, end_ns)
            depth10_cmp = compare_order_book_depth10_exhaustive(old_depth10_stream, new_depth10_stream)
        else:
            depth10_cmp = {"passed": True, "skipped": True, "reason": "emit_depth10 disabled"}
        per_instrument[instrument_id] = depth10_cmp
        all_passed = all_passed and depth10_cmp["passed"]

    return {"stage": "depth10", "by_instrument": per_instrument, "passed": all_passed}


def _cmd_checkpoints(config: dict[str, Any]) -> dict[str, Any]:
    """Compare deterministic order-book checkpoints from the delta streams."""
    from validation.catalog_compare import (
        compare_book_checkpoints_streaming,
        iter_order_book_deltas_bounded,
    )

    old_catalog_root, new_catalog_path, instrument_ids, start_ns, end_ns = (
        _comparison_catalog_config(config)
    )
    levels = int(config.get("derived_depth_snapshot_levels", 10))

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for instrument_id in instrument_ids:
        old_delta_stream = iter_order_book_deltas_bounded(
            old_catalog_root, instrument_id, start_ns, end_ns
        )
        new_delta_stream = iter_order_book_deltas_bounded(
            new_catalog_path, instrument_id, start_ns, end_ns
        )
        checkpoints_cmp = compare_book_checkpoints_streaming(
            old_delta_stream, new_delta_stream, start_ns, end_ns, levels=levels
        )
        per_instrument[instrument_id] = checkpoints_cmp
        all_passed = all_passed and checkpoints_cmp["passed"]

    return {"stage": "checkpoints", "by_instrument": per_instrument, "passed": all_passed}


def _configured_venue_symbols(config: dict[str, Any]) -> list[dict[str, str]]:
    configured = config.get("venue_symbols")
    if not isinstance(configured, list) or not configured:
        raise ValueError("venue_symbols must be a non-empty list")

    normalized: list[dict[str, str]] = []
    for item in configured:
        if not isinstance(item, dict):
            raise ValueError("each venue_symbols entry must be an object")
        venue = str(item.get("venue") or "")
        symbol = str(item.get("symbol") or "")
        instrument_id = str(item.get("instrument_id") or "")
        if not venue or not symbol or not instrument_id:
            raise ValueError(
                "each venue_symbols entry requires venue, symbol, and instrument_id"
            )
        normalized.append(
            {"venue": venue, "symbol": symbol, "instrument_id": instrument_id}
        )
    return normalized


def _load_diagnostic_inputs(
    config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, str]]]:
    from validation.validate_catalog_equivalence import (
        _load_old_convert_report,
        _read_new_manifest,
    )

    date = str(config["date"])
    old_report = _load_old_convert_report(Path(config["old_catalog_root"]), date)
    if not old_report:
        raise ValueError("reference convert report is missing or unreadable")
    new_manifest = _read_new_manifest(Path(config["new_catalog_path"]))
    if not new_manifest:
        raise ValueError("candidate reconstruction manifest is missing or unreadable")
    return old_report, new_manifest, _configured_venue_symbols(config)


def _cmd_continuity(config: dict[str, Any]) -> dict[str, Any]:
    """Compare reference and candidate continuity diagnostics per symbol."""
    from validation.validate_catalog_equivalence import _compare_continuity_for_symbol

    old_report, new_manifest, venue_symbols = _load_diagnostic_inputs(config)
    old_by_symbol = old_report.get("per_symbol_depth") or {}
    new_diagnostics = new_manifest.get("depth_diagnostics")
    if not isinstance(new_diagnostics, dict):
        raise ValueError("candidate manifest has no depth_diagnostics object")

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for item in venue_symbols:
        key = f"{item['venue']}/{item['symbol']}"
        if key not in old_by_symbol:
            raise ValueError(f"reference convert report has no continuity entry for {key}")
        comparison = _compare_continuity_for_symbol(
            old_report, new_manifest, item["venue"], item["symbol"]
        )
        if comparison.get("skipped"):
            raise ValueError(f"continuity comparison unexpectedly skipped for {key}")
        per_instrument[item["instrument_id"]] = comparison
        all_passed = all_passed and comparison["passed"]

    return {"stage": "continuity", "by_instrument": per_instrument, "passed": all_passed}


def _cmd_fences(config: dict[str, Any]) -> dict[str, Any]:
    """Compare complete reference/candidate fenced-range count and digest."""
    from validation.validate_catalog_equivalence import _compare_fenced_ranges_for_symbol

    old_report, new_manifest, venue_symbols = _load_diagnostic_inputs(config)
    old_by_symbol = old_report.get("per_symbol_fenced_ranges") or {}
    if not isinstance(new_manifest.get("fenced_ranges"), list):
        raise ValueError("candidate manifest has no fenced_ranges list")

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for item in venue_symbols:
        key = f"{item['venue']}/{item['symbol']}"
        if key not in old_by_symbol:
            raise ValueError(f"reference convert report has no fenced-range entry for {key}")
        comparison = _compare_fenced_ranges_for_symbol(
            old_report, new_manifest, item["venue"], item["symbol"]
        )
        per_instrument[item["instrument_id"]] = comparison
        all_passed = all_passed and comparison["passed"]

    return {"stage": "fences", "by_instrument": per_instrument, "passed": all_passed}


def _identity_digest(identity: Any) -> str:
    import hashlib

    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _cmd_metadata(config: dict[str, Any]) -> dict[str, Any]:
    """Exhaustively compare raw/replay logical metadata and revalidate that
    the replay manifest's recorded source identity still exactly matches the
    current raw inputs."""
    from pipeline.build_replay_store import (
        check_depth_repartition_readiness,
        compute_repartitioned_source_identity,
    )
    from stores.replay_reader import ReplayReader
    from validation.validate_catalog_equivalence import (
        _compare_raw_to_replay_metadata_for_symbol,
    )

    data_root = Path(config["data_root"])
    replay_root = Path(config["replay_root"])
    date = str(config["date"])
    expected_schema_version = int(config["expected_schema_version"])
    venue_symbols = _configured_venue_symbols(config)
    replay_reader = ReplayReader(replay_root)

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for item in venue_symbols:
        venue = item["venue"]
        symbol = item["symbol"]
        instrument_id = item["instrument_id"]
        readiness_reason = check_depth_repartition_readiness(
            venue,
            symbol,
            date,
            data_root,
            require_complete_next_day=True,
        )
        if readiness_reason is not None:
            raise ValueError(
                f"raw source scope is not closed for {venue}/{symbol}/{date}: "
                f"{readiness_reason}"
            )
        logical_cmp = _compare_raw_to_replay_metadata_for_symbol(
            data_root, replay_root, venue, symbol, date
        )

        manifest = replay_reader.load_manifest(venue, symbol, date)
        if not isinstance(manifest, dict):
            raise ValueError(f"replay manifest is missing or unreadable for {venue}/{symbol}/{date}")
        recorded_identity = manifest.get("source_identity")
        integrity_identity = (manifest.get("integrity") or {}).get("source_identity")
        live_identity = compute_repartitioned_source_identity(
            venue,
            symbol,
            date,
            data_root,
            include_record_counts=True,
        )
        status_matches = manifest.get("status") == "complete"
        schema_matches = manifest.get("schema_version") == expected_schema_version
        recorded_complete = (
            isinstance(recorded_identity, dict)
            and recorded_identity.get("complete") is True
            and not recorded_identity.get("missing_channels")
        )
        recorded_matches_live = recorded_identity == live_identity
        integrity_matches_recorded = integrity_identity == recorded_identity
        source_identity_cmp = {
            "manifest_status": manifest.get("status"),
            "status_complete": status_matches,
            "schema_version": manifest.get("schema_version"),
            "expected_schema_version": expected_schema_version,
            "schema_version_match": schema_matches,
            "recorded_complete": recorded_complete,
            "recorded_digest": _identity_digest(recorded_identity),
            "integrity_digest": _identity_digest(integrity_identity),
            "live_digest": _identity_digest(live_identity),
            "recorded_matches_live": recorded_matches_live,
            "integrity_matches_recorded": integrity_matches_recorded,
            "channel_file_counts": {
                channel: len(entries)
                for channel, entries in (live_identity.get("channels") or {}).items()
            },
        }
        source_identity_cmp["passed"] = all(
            (
                status_matches,
                schema_matches,
                recorded_complete,
                recorded_matches_live,
                integrity_matches_recorded,
            )
        )
        instrument_passed = logical_cmp["passed"] and source_identity_cmp["passed"]
        per_instrument[instrument_id] = {
            "raw_to_replay_metadata": logical_cmp,
            "source_identity": source_identity_cmp,
            "passed": instrument_passed,
        }
        all_passed = all_passed and instrument_passed

    return {"stage": "metadata", "by_instrument": per_instrument, "passed": all_passed}


def _cmd_integrity(config: dict[str, Any]) -> dict[str, Any]:
    """Run routine and deep replay integrity checks per partition."""
    from stores.replay_reader import ReplayReader
    from stores.replay_writer import audit_partition_deep, validate_partition

    replay_root = Path(config["replay_root"])
    date = str(config["date"])
    expected_schema_version = int(config["expected_schema_version"])
    venue_symbols = _configured_venue_symbols(config)
    reader = ReplayReader(replay_root)

    per_instrument: dict[str, Any] = {}
    all_passed = True
    for item in venue_symbols:
        venue = item["venue"]
        symbol = item["symbol"]
        instrument_id = item["instrument_id"]
        partition_dir = (
            replay_root
            / f"venue={venue}"
            / f"symbol={symbol}"
            / f"date={date}"
        )
        manifest = reader.load_manifest(venue, symbol, date)
        schema_matches = (
            isinstance(manifest, dict)
            and manifest.get("schema_version") == expected_schema_version
        )
        routine_valid = validate_partition(partition_dir)
        deep_problems = (
            audit_partition_deep(partition_dir)
            if routine_valid and schema_matches
            else ["routine validity or schema-version contract failed"]
        )
        passed = schema_matches and routine_valid and not deep_problems
        per_instrument[instrument_id] = {
            "schema_version": (
                manifest.get("schema_version")
                if isinstance(manifest, dict)
                else None
            ),
            "expected_schema_version": expected_schema_version,
            "schema_version_match": schema_matches,
            "routine_valid": routine_valid,
            "deep_problem_count": len(deep_problems),
            "deep_problems": deep_problems[:20],
            "passed": passed,
        }
        all_passed = all_passed and passed

    return {
        "stage": "integrity",
        "by_instrument": per_instrument,
        "passed": all_passed,
    }


def _cmd_report(config: dict[str, Any]) -> dict[str, Any]:
    """Aggregate only fragments bound to the independently verified identity."""
    from validation.artifact_identity import (
        artifact_binding_summary,
        load_artifact_identity,
        verify_artifact_inputs,
    )

    identity_path = config.get("artifact_identity_path")
    if not isinstance(identity_path, str) or not identity_path:
        raise ValueError("report requires artifact_identity_path")
    identity = load_artifact_identity(Path(identity_path))
    verify_artifact_inputs(config, identity)
    expected_binding = artifact_binding_summary(identity, config)
    expected_scope = expected_binding["scope"]
    fragment_paths = config["fragment_paths"]
    fragments = [_load_config(p) for p in fragment_paths]
    stages = [fragment.get("stage") for fragment in fragments]
    duplicate_stages = sorted(
        {stage for stage in stages if isinstance(stage, str) and stages.count(stage) > 1}
    )
    required_stages = list(config.get("required_stages") or [])
    missing_stages = sorted(set(required_stages) - set(stages))
    unexpected_stages = sorted(set(stages) - set(required_stages)) if required_stages else []
    stage_set_matches = (
        not duplicate_stages
        and not missing_stages
        and not unexpected_stages
        and all(isinstance(stage, str) and stage for stage in stages)
    )
    scope_mismatches = [
        {
            "stage": fragment.get("stage"),
            "actual_scope": fragment.get("scope"),
        }
        for fragment in fragments
        if fragment.get("scope") != expected_scope
    ]
    binding_mismatches = [
        {
            "stage": fragment.get("stage"),
            "identity_document_sha256": (
                fragment.get("artifact_binding") or {}
            ).get("identity_document_sha256"),
        }
        for fragment in fragments
        if fragment.get("artifact_binding") != expected_binding
    ]
    all_passed = (
        stage_set_matches
        and not scope_mismatches
        and not binding_mismatches
        and all(
        fragment.get("passed", False) for fragment in fragments
        )
    )
    return {
        "stage": "report",
        "fragments": fragments,
        "scope": expected_scope,
        "scope_mismatches": scope_mismatches,
        "artifact_binding": expected_binding,
        "binding_mismatches": binding_mismatches,
        "required_stages": required_stages,
        "duplicate_stages": duplicate_stages,
        "missing_stages": missing_stages,
        "unexpected_stages": unexpected_stages,
        "stage_set_matches": stage_set_matches,
        "status": "passed" if all_passed else "failed",
        "passed": all_passed,
    }


_SUBCOMMANDS = {
    "trades": _cmd_trades,
    "deltas": _cmd_deltas,
    "depth10": _cmd_depth10,
    "checkpoints": _cmd_checkpoints,
    "continuity": _cmd_continuity,
    "fences": _cmd_fences,
    "metadata": _cmd_metadata,
    "integrity": _cmd_integrity,
    "report": _cmd_report,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one isolated gate stage.")
    parser.add_argument("subcommand", choices=sorted(_SUBCOMMANDS))
    parser.add_argument("--config", required=True, help="path to a small JSON config file")
    parser.add_argument("--out", required=True, help="path to write the small JSON result fragment to")
    args = parser.parse_args(argv)

    out_path = Path(args.out)
    if out_path.exists():
        print(
            "stage output already exists; refusing to overwrite",
            file=sys.stderr,
        )
        return 2

    handler = _SUBCOMMANDS[args.subcommand]
    scope: dict[str, str] | None = None
    artifact_binding: dict[str, Any] | None = None
    try:
        config = _load_config(args.config)
        scope, artifact_identity = _verified_artifact_scope(config)
        from validation.artifact_identity import (
            artifact_binding_summary,
            verify_artifact_inputs,
        )

        # Every stage performs a complete content verification before work.
        # The report subcommand independently repeats this inside its handler
        # before accepting any fragment.
        verify_artifact_inputs(config, artifact_identity)
        artifact_binding = artifact_binding_summary(artifact_identity, config)
        result = handler(config)
        # Re-hash after the stage while its result is still in memory. A
        # time-of-check/time-of-use mutation turns the stage into a failure;
        # no successful fragment can escape with a stale identity.
        from validation.artifact_identity import load_artifact_identity

        post_identity = load_artifact_identity(Path(config["artifact_identity_path"]))
        verify_artifact_inputs(config, post_identity)
        post_binding = artifact_binding_summary(post_identity, config)
        if post_binding != artifact_binding:
            raise ValueError("artifact identity binding changed during stage execution")
        result["scope"] = scope
        result["artifact_binding"] = artifact_binding
    except Exception as exc:  # noqa: BLE001 - must report, not crash silently without a fragment
        result = {"stage": args.subcommand, "passed": False, "error": f"{type(exc).__name__}: {exc}"}
        if scope is not None:
            result["scope"] = scope
        if artifact_binding is not None:
            result["artifact_binding"] = artifact_binding
        try:
            _write_result(args.out, result)
        except FileExistsError:
            print(
                "stage output appeared during execution; refusing to overwrite",
                file=sys.stderr,
            )
            return 2
        print(f"stage {args.subcommand!r} raised: {exc}", file=sys.stderr)
        return 1

    try:
        _write_result(args.out, result)
    except FileExistsError:
        print(
            "stage output appeared during execution; refusing to overwrite",
            file=sys.stderr,
        )
        return 2
    print(f"stage {args.subcommand!r} done: passed={result.get('passed')}")
    return 0 if result.get("passed", False) else 1


if __name__ == "__main__":
    raise SystemExit(main())
