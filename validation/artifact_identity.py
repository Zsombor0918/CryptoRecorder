"""Fail-closed artifact identities for isolated semantic validation stages.

This module is validation-only: it does not build, repair, or rewrite any
catalog or replay partition.  It binds one logical venue/symbol/day gate to
the exact reference, raw, replay, and reconstructed-catalog artifacts which
the gate will read.

Directory identities use a deterministic, length-framed SHA-256 tree digest.
Only POSIX paths relative to the selected tree root enter that digest, so
moving an unchanged tree does not change its identity.  JSON components are
canonicalized after absolute machine paths are replaced with a fixed marker;
the original documents are validated against their configured roots before
that normalization.  The emitted identity contains no input paths.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from stores.replay_schema import (
    BUILDER_VERSION_V2,
    FORMAT_VERSION_V2,
    SCHEMA_VERSION_V2,
)
from stores.replay_writer import validate_v2_source_identity

IDENTITY_SCHEMA = "cryptorecorder-artifact-identity-v1"
IDENTITY_DOCUMENT_DIGEST_ALGORITHM = "sha256-canonical-identity-document-v1"
TREE_HASH_ALGORITHM = "sha256-tree-length-framed-v1"
JSON_HASH_ALGORITHM = "sha256-canonical-json-length-framed-v1"
COMPOSITE_HASH_ALGORITHM = "sha256-composite-length-framed-v1"

_SCOPE_FIELDS = (
    "date",
    "venue",
    "symbol",
    "instrument_id",
    "source_identity_sha256",
    "candidate_identity_sha256",
)
_SOURCE_COMPONENTS = {
    "reference_catalog_tree",
    "reference_report",
    "raw_target_source_identity",
}
_CANDIDATE_COMMON_COMPONENTS = {
    "candidate_catalog_tree",
    "candidate_reconstruction_manifest",
    "target_replay_manifest",
}
_CARRY_COMPONENTS = {"carry_replay_manifest", "no_carry_prelisting_marker"}
_ABSOLUTE_PATH_MARKER = "<absolute-machine-path>"
_WINDOWS_ABSOLUTE_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_JSON_BYTES = 8 * 1024 * 1024


def _json_object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _reject_non_finite_json(value: str) -> Any:
    raise ValueError(f"non-finite JSON number is not permitted: {value}")


def load_json_object(path: Path) -> dict[str, Any]:
    """Load one regular, non-symlink JSON object and reject ambiguous JSON."""
    if path.is_symlink():
        raise ValueError("JSON component must not be a symlink")
    if not path.is_file():
        raise ValueError("JSON component is missing or is not a regular file")
    try:
        with path.open("rb") as handle:
            raw = handle.read(_MAX_JSON_BYTES + 1)
        if len(raw) > _MAX_JSON_BYTES:
            raise ValueError(
                f"JSON component exceeds the {_MAX_JSON_BYTES}-byte safety limit"
            )
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_json_object_no_duplicates,
            parse_constant=_reject_non_finite_json,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("JSON component is unreadable or invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("JSON component must contain one top-level object")
    return value


def _update_frame(digest: Any, label: bytes, payload: bytes) -> None:
    """Add an unambiguous label/value frame to ``digest``."""
    digest.update(len(label).to_bytes(8, "big"))
    digest.update(label)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _stat_identity(stat_result: Any) -> tuple[int, int, int, int, int]:
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_size,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
    )


def _hash_file(path: Path) -> tuple[str, int, tuple[int, int, int, int, int]]:
    before = path.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = path.stat(follow_symlinks=False)
    before_identity = _stat_identity(before)
    after_identity = _stat_identity(after)
    if before_identity != after_identity:
        raise ValueError("artifact file changed while its identity was computed")
    return digest.hexdigest(), before.st_size, after_identity


def _tree_files(root: Path, excluded: set[Path]) -> list[tuple[str, Path]]:
    selected: list[tuple[str, Path]] = []
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("artifact trees must not contain symlinks")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError("artifact trees may contain only directories and regular files")
        resolved = path.resolve(strict=True)
        if resolved in excluded:
            continue
        relative = path.relative_to(root).as_posix()
        if not relative or relative.startswith("/") or "\\" in relative:
            raise ValueError("artifact tree produced a non-canonical relative path")
        selected.append((relative, path))
    selected.sort(key=lambda item: item[0])
    return selected


def hash_tree(
    root: Path,
    *,
    exclude_paths: Iterable[Path] = (),
) -> dict[str, Any]:
    """Return a bounded-memory, path-independent digest of a file tree.

    The root's absolute location, mtimes, ownership, and inode numbers are
    not hashed.  Relative POSIX file names, exact sizes, and exact file-byte
    SHA-256 values are length-framed in lexical path order.  Symlinks and
    concurrent tree changes fail closed.
    """
    root = Path(root)
    if root.is_symlink():
        raise ValueError("artifact tree root must not be a symlink")
    if not root.is_dir():
        raise ValueError("artifact tree root is missing or is not a directory")
    root = root.resolve(strict=True)

    excluded: set[Path] = set()
    for raw_path in exclude_paths:
        path = Path(raw_path)
        if path.is_symlink():
            raise ValueError("excluded artifact path must not be a symlink")
        if not path.is_file():
            raise ValueError("excluded artifact path is missing or is not a regular file")
        resolved = path.resolve(strict=True)
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError("excluded artifact path is outside its tree root") from exc
        excluded.add(resolved)

    files = _tree_files(root, excluded)
    if not files:
        raise ValueError("artifact tree has no selected regular files")

    digest = hashlib.sha256()
    _update_frame(digest, b"domain", TREE_HASH_ALGORITHM.encode())
    total_bytes = 0
    hashed_stats: dict[str, tuple[int, int, int, int, int]] = {}
    for relative, path in files:
        file_sha256, size_bytes, stat_identity = _hash_file(path)
        hashed_stats[relative] = stat_identity
        total_bytes += size_bytes
        _update_frame(digest, b"file-relative-path", relative.encode("utf-8"))
        _update_frame(digest, b"file-size-bytes", str(size_bytes).encode("ascii"))
        _update_frame(digest, b"file-sha256", bytes.fromhex(file_sha256))

    final_files = _tree_files(root, excluded)
    if [relative for relative, _ in final_files] != [
        relative for relative, _ in files
    ]:
        raise ValueError("artifact tree changed while its identity was computed")
    for relative, path in final_files:
        if _stat_identity(path.stat(follow_symlinks=False)) != hashed_stats[relative]:
            raise ValueError("artifact file changed while its identity was computed")
    _update_frame(digest, b"file-count", str(len(files)).encode("ascii"))
    _update_frame(digest, b"total-bytes", str(total_bytes).encode("ascii"))
    return {
        "algorithm": TREE_HASH_ALGORITHM,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "sha256": digest.hexdigest(),
    }


def _looks_like_absolute_path(value: str) -> bool:
    return (
        value.startswith("/")
        or value.startswith("\\\\")
        or value.startswith("file://")
        or _WINDOWS_ABSOLUTE_PATH.match(value) is not None
    )


def _normalize_machine_paths(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_machine_paths(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_machine_paths(item) for item in value]
    if isinstance(value, str) and _looks_like_absolute_path(value):
        return _ABSOLUTE_PATH_MARKER
    return value


def hash_json_value(value: Any) -> dict[str, Any]:
    """Hash canonical JSON after removing absolute-machine-path variance."""
    normalized = _normalize_machine_paths(value)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    digest = hashlib.sha256()
    _update_frame(digest, b"domain", JSON_HASH_ALGORITHM.encode())
    _update_frame(digest, b"canonical-json", encoded)
    return {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": len(encoded),
        "sha256": digest.hexdigest(),
    }


def canonical_identity_document_bytes(document: Mapping[str, Any]) -> bytes:
    """Return the one canonical byte representation of a validated identity.

    Identity documents are already path-sanitized by construction. Sorting
    object keys, using compact JSON separators, preserving list order, and
    rejecting non-finite numbers gives every local stage and aggregator the
    same unambiguous bytes to bind with SHA-256.
    """
    validate_artifact_identity_document(document)
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def identity_document_sha256(document: Mapping[str, Any]) -> str:
    """Return the canonical SHA-256 binding for one identity document."""
    digest = hashlib.sha256()
    _update_frame(
        digest,
        b"domain",
        IDENTITY_DOCUMENT_DIGEST_ALGORITHM.encode("ascii"),
    )
    _update_frame(
        digest,
        b"identity-document",
        canonical_identity_document_bytes(document),
    )
    return digest.hexdigest()


def artifact_binding_summary(
    document: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the compact verified binding embedded in every stage fragment."""
    scope = validate_artifact_identity_document(document)
    contracts = document["contracts"]
    target_replay = contracts["target_replay"]
    candidate_contract = contracts["candidate_reconstruction"]
    profile = config.get("profile", candidate_contract.get("profile"))
    if profile != candidate_contract.get("profile"):
        raise ValueError(
            "stage profile does not match artifact identity candidate profile"
        )

    component_hashes = {
        f"source.{name}": component["sha256"]
        for name, component in document["source"]["components"].items()
    }
    component_hashes.update(
        {
            f"candidate.{name}": component["sha256"]
            for name, component in document["candidate"]["components"].items()
        }
    )
    stage_configuration = {
        "date": scope["date"],
        "venue": scope["venue"],
        "symbol": scope["symbol"],
        "instrument_id": scope["instrument_id"],
        "profile": profile,
        "schema_version": target_replay["schema_version"],
        "format_version": target_replay["format_version"],
        "builder_version": target_replay["builder_version"],
    }
    for field in ("start_ns", "end_ns", "time_filter"):
        if field in config:
            stage_configuration[field] = config[field]
    return {
        "identity_document_algorithm": IDENTITY_DOCUMENT_DIGEST_ALGORITHM,
        "identity_document_sha256": identity_document_sha256(document),
        "identity_schema": document["identity_schema"],
        "scope": scope,
        "stage_configuration": stage_configuration,
        "input_hashes": {
            "source": document["source"]["sha256"],
            "candidate": document["candidate"]["sha256"],
            "components": dict(sorted(component_hashes.items())),
        },
    }


def _component_digest(component: Any, name: str) -> str:
    if not isinstance(component, dict):
        raise ValueError(f"artifact component {name!r} must be an object")
    digest = component.get("sha256")
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise ValueError(f"artifact component {name!r} has an invalid SHA-256")
    return digest


def composite_hash(kind: str, components: Mapping[str, Any]) -> str:
    """Hash a named set of component digests with deterministic framing."""
    digest = hashlib.sha256()
    _update_frame(digest, b"domain", COMPOSITE_HASH_ALGORITHM.encode())
    _update_frame(digest, b"composite-kind", kind.encode("utf-8"))
    for name in sorted(components):
        component_sha256 = _component_digest(components[name], name)
        _update_frame(digest, b"component-name", name.encode("utf-8"))
        _update_frame(
            digest,
            b"component-sha256",
            bytes.fromhex(component_sha256),
        )
    _update_frame(
        digest,
        b"component-count",
        str(len(components)).encode("ascii"),
    )
    return digest.hexdigest()


def _require_string(config: Mapping[str, Any], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _canonical_date(value: str, field: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be canonical YYYY-MM-DD") from exc
    if parsed.strftime("%Y-%m-%d") != value:
        raise ValueError(f"{field} must be canonical YYYY-MM-DD")
    return parsed


def _require_path(config: Mapping[str, Any], key: str) -> Path:
    return Path(_require_string(config, key))


def _partition_dir(replay_root: Path, venue: str, symbol: str, date: str) -> Path:
    return (
        replay_root
        / f"venue={venue}"
        / f"symbol={symbol}"
        / f"date={date}"
    )


def _validate_replay_manifest(
    manifest: dict[str, Any],
    *,
    venue: str,
    symbol: str,
    date: str,
) -> dict[str, Any]:
    expected = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "status": "complete",
        "schema_version": SCHEMA_VERSION_V2,
        "format_version": FORMAT_VERSION_V2,
        "builder_version": BUILDER_VERSION_V2,
    }
    mismatches = {
        key: {"expected": value, "actual": manifest.get(key)}
        for key, value in expected.items()
        if manifest.get(key) != value
    }
    if mismatches:
        fields = ", ".join(sorted(mismatches))
        raise ValueError(
            f"replay manifest contract mismatch for {venue}/{symbol}/{date}: {fields}"
        )

    source_identity = manifest.get("source_identity")
    validate_v2_source_identity(source_identity, venue, symbol, date)
    integrity = manifest.get("integrity")
    if not isinstance(integrity, dict):
        raise ValueError("schema-v2 replay manifest has no integrity object")
    if integrity.get("source_identity") != source_identity:
        raise ValueError(
            "schema-v2 replay manifest integrity.source_identity does not "
            "exactly match source_identity"
        )
    return source_identity


def _validate_reference_report(
    report: dict[str, Any],
    *,
    catalog_root: Path,
    venue: str,
    symbol: str,
    date: str,
) -> None:
    if report.get("date") != date:
        raise ValueError("reference report date does not match requested date")
    if report.get("architecture") != "deterministic_native":
        raise ValueError("reference report architecture is not deterministic_native")
    if report.get("status") != "ok":
        raise ValueError("reference report status is not ok")
    key = f"{venue}/{symbol}"
    for field in ("per_symbol_trade", "per_symbol_depth", "per_symbol_fenced_ranges"):
        value = report.get(field)
        if not isinstance(value, dict) or key not in value:
            raise ValueError(f"reference report has no {field} entry for {key}")
    processed = report.get("symbols_processed")
    if (
        not isinstance(processed, dict)
        or not isinstance(processed.get(venue), list)
        or symbol not in processed[venue]
    ):
        raise ValueError("reference report does not list the requested venue/symbol")
    recorded_root = report.get("catalog_root")
    if not isinstance(recorded_root, str) or not recorded_root:
        raise ValueError("reference report has no catalog_root provenance")
    if Path(recorded_root).resolve(strict=False) != catalog_root.resolve(strict=True):
        raise ValueError(
            "reference report catalog_root does not match the configured reference catalog"
        )


def _validate_candidate_manifest(
    manifest: dict[str, Any],
    *,
    replay_root: Path,
    venue: str,
    symbol: str,
    date: str,
) -> None:
    if manifest.get("profile") != "full_l2":
        raise ValueError("candidate reconstruction manifest profile must be full_l2")
    if manifest.get("requested_symbols") != [symbol]:
        raise ValueError(
            "candidate reconstruction manifest requested_symbols must exactly "
            "match the requested symbol"
        )
    if manifest.get("requested_venues") != [venue]:
        raise ValueError(
            "candidate reconstruction manifest requested_venues must exactly "
            "match the requested venue"
        )
    if manifest.get("symbols") != [f"{venue}:{symbol}"]:
        raise ValueError(
            "candidate reconstruction manifest symbols do not exactly match "
            "the requested venue/symbol"
        )
    expected_partition = [{"venue": venue, "symbol": symbol, "date": date}]
    if manifest.get("found_partitions") != expected_partition:
        raise ValueError(
            "candidate reconstruction manifest found_partitions do not exactly "
            "match the requested partition"
        )
    if manifest.get("missing_partitions") != []:
        raise ValueError("candidate reconstruction manifest has missing partitions")
    if manifest.get("time_filter") != "ts_init":
        raise ValueError("candidate reconstruction manifest time_filter must be ts_init")
    window = manifest.get("time_window")
    if not isinstance(window, dict):
        raise ValueError("candidate reconstruction manifest has no time_window object")
    expected_start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    expected_end = expected_start + timedelta(days=1)
    try:
        actual_start = datetime.fromisoformat(str(window.get("start")))
        actual_end = datetime.fromisoformat(str(window.get("end")))
    except ValueError as exc:
        raise ValueError(
            "candidate reconstruction manifest time_window is invalid"
        ) from exc
    if actual_start != expected_start or actual_end != expected_end:
        raise ValueError(
            "candidate reconstruction manifest time_window is not the exact UTC day"
        )
    recorded_replay_root = manifest.get("replay_source")
    if not isinstance(recorded_replay_root, str) or not recorded_replay_root:
        raise ValueError(
            "candidate reconstruction manifest has no replay_source provenance"
        )
    if (
        Path(recorded_replay_root).resolve(strict=False)
        != replay_root.resolve(strict=True)
    ):
        raise ValueError(
            "candidate reconstruction manifest replay_source does not match "
            "the configured replay root"
        )


def _contract_summary(date: str) -> dict[str, Any]:
    return {
        "date": date,
        "status": "complete",
        "schema_version": SCHEMA_VERSION_V2,
        "format_version": FORMAT_VERSION_V2,
        "builder_version": BUILDER_VERSION_V2,
        "source_identity_complete": True,
    }


def build_artifact_identity(config: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and identify one exact representative semantic-gate scope."""
    date = _require_string(config, "date")
    venue = _require_string(config, "venue")
    symbol = _require_string(config, "symbol")
    instrument_id = _require_string(config, "instrument_id")
    parsed_date = _canonical_date(date, "date")

    data_root = _require_path(config, "data_root")
    replay_root = _require_path(config, "replay_root")
    reference_catalog_root = _require_path(config, "reference_catalog_root")
    reference_report_path = _require_path(config, "reference_report_path")
    candidate_catalog_root = _require_path(config, "candidate_catalog_root")
    candidate_manifest_path = _require_path(
        config, "candidate_reconstruction_manifest_path"
    )

    if not data_root.is_dir():
        raise ValueError("data_root is missing or is not a directory")
    if not replay_root.is_dir():
        raise ValueError("replay_root is missing or is not a directory")
    expected_candidate_manifest = candidate_catalog_root / "manifest.json"
    if (
        candidate_manifest_path.resolve(strict=True)
        != expected_candidate_manifest.resolve(strict=True)
    ):
        raise ValueError(
            "candidate_reconstruction_manifest_path must be the selected "
            "candidate catalog's manifest.json"
        )

    reference_report = load_json_object(reference_report_path)
    _validate_reference_report(
        reference_report,
        catalog_root=reference_catalog_root,
        venue=venue,
        symbol=symbol,
        date=date,
    )
    candidate_manifest = load_json_object(candidate_manifest_path)
    _validate_candidate_manifest(
        candidate_manifest,
        replay_root=replay_root,
        venue=venue,
        symbol=symbol,
        date=date,
    )

    target_partition = _partition_dir(replay_root, venue, symbol, date)
    target_manifest_path = target_partition / "manifest.json"
    target_manifest = load_json_object(target_manifest_path)
    target_recorded_source = _validate_replay_manifest(
        target_manifest,
        venue=venue,
        symbol=symbol,
        date=date,
    )

    from pipeline.build_replay_store import (
        compute_repartitioned_source_identity,
        replay_partition_has_source_records,
    )

    live_raw_identity = compute_repartitioned_source_identity(
        venue,
        symbol,
        date,
        data_root,
        include_record_counts=True,
        strict=True,
    )
    validate_v2_source_identity(live_raw_identity, venue, symbol, date)
    if live_raw_identity != target_recorded_source:
        raise ValueError(
            "target replay manifest source_identity does not exactly match "
            "the current raw target source identity"
        )

    source_components = {
        "reference_catalog_tree": hash_tree(
            reference_catalog_root,
            exclude_paths=(
                [reference_report_path]
                if reference_report_path.resolve(strict=True).is_relative_to(
                    reference_catalog_root.resolve(strict=True)
                )
                else []
            ),
        ),
        "reference_report": hash_json_value(reference_report),
        "raw_target_source_identity": hash_json_value(live_raw_identity),
    }
    candidate_components: dict[str, dict[str, Any]] = {
        "candidate_catalog_tree": hash_tree(
            candidate_catalog_root,
            exclude_paths=[candidate_manifest_path],
        ),
        "candidate_reconstruction_manifest": hash_json_value(candidate_manifest),
        "target_replay_manifest": hash_json_value(target_manifest),
    }

    carry = config.get("carry")
    if not isinstance(carry, dict):
        raise ValueError(
            "carry must explicitly select replay_manifest or "
            "no_carry_prelisting"
        )
    if set(carry) != {"kind", "date"}:
        raise ValueError("carry must contain exactly kind and date")
    carry_kind = carry.get("kind")
    carry_date = carry.get("date")
    if not isinstance(carry_date, str):
        raise ValueError("carry.date must be a string")
    expected_carry_date = (parsed_date - timedelta(days=1)).strftime("%Y-%m-%d")
    if carry_date != expected_carry_date:
        raise ValueError("carry.date must be exactly the previous UTC day")
    carry_partition = _partition_dir(replay_root, venue, symbol, carry_date)

    if carry_kind == "replay_manifest":
        carry_manifest = load_json_object(carry_partition / "manifest.json")
        _validate_replay_manifest(
            carry_manifest,
            venue=venue,
            symbol=symbol,
            date=carry_date,
        )
        candidate_components["carry_replay_manifest"] = hash_json_value(
            carry_manifest
        )
        carry_contract = {
            "kind": "replay_manifest",
            **_contract_summary(carry_date),
        }
    elif carry_kind == "no_carry_prelisting":
        if carry_partition.exists():
            raise ValueError(
                "no-carry prelisting marker is invalid because a previous-day "
                "replay partition exists"
            )
        if replay_partition_has_source_records(
            venue, symbol, carry_date, data_root
        ):
            raise ValueError(
                "no-carry prelisting marker is invalid because previous-day "
                "source records exist"
            )
        marker = {
            "kind": "no_carry_prelisting",
            "result": "not_applicable_pre_listing",
            "date": carry_date,
            "venue": venue,
            "symbol": symbol,
        }
        candidate_components["no_carry_prelisting_marker"] = hash_json_value(
            marker
        )
        carry_contract = marker
    else:
        raise ValueError(
            "carry.kind must be replay_manifest or no_carry_prelisting"
        )

    source_sha256 = composite_hash("source", source_components)
    candidate_sha256 = composite_hash("candidate", candidate_components)
    document = {
        "identity_schema": IDENTITY_SCHEMA,
        "scope": {
            "date": date,
            "venue": venue,
            "symbol": symbol,
            "instrument_id": instrument_id,
            "source_identity_sha256": source_sha256,
            "candidate_identity_sha256": candidate_sha256,
        },
        "source": {
            "algorithm": COMPOSITE_HASH_ALGORITHM,
            "sha256": source_sha256,
            "components": source_components,
        },
        "candidate": {
            "algorithm": COMPOSITE_HASH_ALGORITHM,
            "sha256": candidate_sha256,
            "components": candidate_components,
        },
        "contracts": {
            "reference_report": {
                "date": date,
                "architecture": "deterministic_native",
                "status": "ok",
            },
            "candidate_reconstruction": {
                "profile": "full_l2",
                "time_filter": "ts_init",
                "date": date,
            },
            "target_replay": _contract_summary(date),
            "carry": carry_contract,
            "raw_target_source_identity": {
                "complete": True,
                "channel_file_counts": {
                    channel: len(entries)
                    for channel, entries in live_raw_identity["channels"].items()
                },
            },
        },
    }
    validate_artifact_identity_document(document)
    return document


def validate_artifact_identity_document(
    document: Any,
) -> dict[str, str]:
    """Validate an emitted identity and return its exact normalized scope."""
    if not isinstance(document, dict):
        raise ValueError("artifact identity must be a JSON object")
    required_top_keys = {
        "identity_schema",
        "scope",
        "source",
        "candidate",
        "contracts",
    }
    if set(document) != required_top_keys:
        raise ValueError(
            f"artifact identity keys must be exactly {sorted(required_top_keys)!r}"
        )
    if document.get("identity_schema") != IDENTITY_SCHEMA:
        raise ValueError(
            f"artifact identity schema must be {IDENTITY_SCHEMA!r}"
        )
    scope = document.get("scope")
    if not isinstance(scope, dict) or set(scope) != set(_SCOPE_FIELDS):
        raise ValueError(
            f"artifact identity scope keys must be exactly {sorted(_SCOPE_FIELDS)!r}"
        )
    normalized_scope: dict[str, str] = {}
    for field in _SCOPE_FIELDS:
        value = scope.get(field)
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError(f"artifact identity scope.{field} is invalid")
        normalized_scope[field] = value
    _canonical_date(normalized_scope["date"], "artifact identity scope.date")
    for field in ("source_identity_sha256", "candidate_identity_sha256"):
        if _SHA256_RE.fullmatch(normalized_scope[field]) is None:
            raise ValueError(f"artifact identity scope.{field} is not SHA-256")

    source = document.get("source")
    candidate = document.get("candidate")
    if not isinstance(source, dict) or not isinstance(candidate, dict):
        raise ValueError("artifact identity source/candidate must be objects")
    if source.get("algorithm") != COMPOSITE_HASH_ALGORITHM:
        raise ValueError("artifact identity source algorithm is unsupported")
    if candidate.get("algorithm") != COMPOSITE_HASH_ALGORITHM:
        raise ValueError("artifact identity candidate algorithm is unsupported")
    source_components = source.get("components")
    candidate_components = candidate.get("components")
    if not isinstance(source_components, dict) or set(source_components) != _SOURCE_COMPONENTS:
        raise ValueError("artifact identity has an invalid source component set")
    if not isinstance(candidate_components, dict):
        raise ValueError("artifact identity candidate components must be an object")
    candidate_names = set(candidate_components)
    carry_names = candidate_names & _CARRY_COMPONENTS
    if (
        candidate_names - _CARRY_COMPONENTS != _CANDIDATE_COMMON_COMPONENTS
        or len(carry_names) != 1
    ):
        raise ValueError("artifact identity has an invalid candidate component set")
    for name, component in {**source_components, **candidate_components}.items():
        _component_digest(component, name)
        expected_algorithm = (
            TREE_HASH_ALGORITHM
            if name.endswith("_catalog_tree")
            else JSON_HASH_ALGORITHM
        )
        if component.get("algorithm") != expected_algorithm:
            raise ValueError(
                f"artifact identity component {name!r} algorithm is unsupported"
            )
        if expected_algorithm == TREE_HASH_ALGORITHM:
            for field in ("file_count", "total_bytes"):
                value = component.get(field)
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value < 0
                ):
                    raise ValueError(
                        f"artifact identity component {name!r}.{field} is invalid"
                    )
        else:
            value = component.get("normalized_size_bytes")
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(
                    f"artifact identity component {name!r}.normalized_size_bytes "
                    "is invalid"
                )
    expected_source = composite_hash("source", source_components)
    expected_candidate = composite_hash("candidate", candidate_components)
    if source.get("sha256") != expected_source:
        raise ValueError("artifact identity source composite SHA-256 is invalid")
    if candidate.get("sha256") != expected_candidate:
        raise ValueError("artifact identity candidate composite SHA-256 is invalid")
    if normalized_scope["source_identity_sha256"] != expected_source:
        raise ValueError("artifact identity scope source SHA-256 is inconsistent")
    if normalized_scope["candidate_identity_sha256"] != expected_candidate:
        raise ValueError("artifact identity scope candidate SHA-256 is inconsistent")

    contracts = document.get("contracts")
    required_contracts = {
        "reference_report",
        "candidate_reconstruction",
        "target_replay",
        "carry",
        "raw_target_source_identity",
    }
    if not isinstance(contracts, dict) or set(contracts) != required_contracts:
        raise ValueError("artifact identity has an invalid contracts object")
    target_contract = contracts.get("target_replay")
    expected_target_contract = _contract_summary(normalized_scope["date"])
    if target_contract != expected_target_contract:
        raise ValueError("artifact identity target replay contract is invalid")
    if contracts.get("reference_report") != {
        "date": normalized_scope["date"],
        "architecture": "deterministic_native",
        "status": "ok",
    }:
        raise ValueError("artifact identity reference report contract is invalid")
    if contracts.get("candidate_reconstruction") != {
        "profile": "full_l2",
        "time_filter": "ts_init",
        "date": normalized_scope["date"],
    }:
        raise ValueError(
            "artifact identity candidate reconstruction contract is invalid"
        )
    raw_contract = contracts.get("raw_target_source_identity")
    if (
        not isinstance(raw_contract, dict)
        or raw_contract.get("complete") is not True
        or set(raw_contract) != {"complete", "channel_file_counts"}
        or not isinstance(raw_contract.get("channel_file_counts"), dict)
        or set(raw_contract["channel_file_counts"]) != {"depth_v2", "trade_v2"}
        or any(
            not isinstance(count, int)
            or isinstance(count, bool)
            or count <= 0
            for count in raw_contract["channel_file_counts"].values()
        )
    ):
        raise ValueError("artifact identity raw source contract is invalid")
    carry_contract = contracts.get("carry")
    if not isinstance(carry_contract, dict):
        raise ValueError("artifact identity carry contract must be an object")
    carry_component = next(iter(carry_names))
    target_date = datetime.strptime(normalized_scope["date"], "%Y-%m-%d")
    expected_carry_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
    if carry_component == "carry_replay_manifest":
        if carry_contract != {
            "kind": "replay_manifest",
            **_contract_summary(expected_carry_date),
        }:
            raise ValueError("artifact identity carry replay contract is invalid")
    else:
        if carry_contract != {
            "kind": "no_carry_prelisting",
            "result": "not_applicable_pre_listing",
            "date": expected_carry_date,
            "venue": normalized_scope["venue"],
            "symbol": normalized_scope["symbol"],
        }:
            raise ValueError(
                "artifact identity no-carry contract does not match its component"
            )

    def _reject_paths(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                _reject_paths(item)
        elif isinstance(value, list):
            for item in value:
                _reject_paths(item)
        elif isinstance(value, str) and _looks_like_absolute_path(value):
            raise ValueError("artifact identity document contains an absolute path")

    _reject_paths(document)
    return normalized_scope


def verify_artifact_inputs(
    config: Mapping[str, Any],
    expected_document: dict[str, Any],
) -> None:
    """Re-hash and revalidate configured inputs against an earlier identity.

    This is intended for the final ``stage_runner_cli report`` boundary.  It
    provides a second full proof after the isolated stages and detects any
    artifact mutation or path substitution which occurred after initial
    identity creation.
    """
    validate_artifact_identity_document(expected_document)
    current_document = build_artifact_identity(config)
    if current_document != expected_document:
        changed: list[str] = []
        for side in ("source", "candidate"):
            expected_side = expected_document[side]
            current_side = current_document[side]
            if expected_side.get("sha256") != current_side.get("sha256"):
                changed.append(side)
        detail = ", ".join(changed) if changed else "identity contract"
        raise ValueError(
            f"artifact inputs no longer match the recorded identity: {detail}"
        )


def load_artifact_identity(path: Path) -> dict[str, Any]:
    document = load_json_object(Path(path))
    validate_artifact_identity_document(document)
    return document


def write_identity_exclusive(path: Path, document: dict[str, Any]) -> None:
    """Write compact JSON exactly once; an existing output is never replaced."""
    validate_artifact_identity_document(document)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    with path.open("x") as handle:
        handle.write(encoded)
        handle.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create one fail-closed semantic-gate artifact identity."
    )
    parser.add_argument("--config", required=True, help="path to identity config JSON")
    parser.add_argument(
        "--out",
        required=True,
        help="new path for compact path-sanitized identity JSON",
    )
    args = parser.parse_args(argv)

    out_path = Path(args.out)
    if out_path.exists():
        print("artifact identity output already exists; refusing to overwrite", file=sys.stderr)
        return 2
    try:
        config = load_json_object(Path(args.config))
        document = build_artifact_identity(config)
        write_identity_exclusive(out_path, document)
    except Exception as exc:  # noqa: BLE001 - CLI must fail clearly at its boundary
        print(f"artifact identity failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(
        "artifact identity created: "
        f"{document['scope']['venue']}/{document['scope']['symbol']}/"
        f"{document['scope']['date']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
