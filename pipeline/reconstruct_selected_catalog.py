"""Supported selected replay -> temporary Nautilus catalog boundary.

This development-computer API/CLI deliberately wraps, rather than copies,
``validation.replay_catalog_reconstruct``.  Callers must name every venue,
symbol, UTC endpoint, output root, and job ID.  The result is an atomically
published, job-scoped temporary catalog cryptographically bound to the replay
partitions it consumed; this module is not a persistent service or an
all-history catalog builder.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from stores.replay_reader import ReplayReader
from stores.replay_writer import validate_partition
from validation.artifact_identity import load_json_object

MANIFEST_VERSION = "cryptorecorder-selected-catalog-job-v1"
INVENTORY_DIGEST_ALGORITHM = "sha256-canonical-json-v1"
CATALOG_DIGEST_ALGORITHM = "sha256-catalog-tree-v1"
SUPPORTED_PROFILES = ("full_l2", "trades_only")
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class SelectedCatalogError(RuntimeError):
    """The selected reconstruction request failed closed."""


@dataclass(frozen=True)
class SelectedCatalogRequest:
    """Explicit contract for one selected temporary-catalog job."""

    replay_root: Path
    venues: Sequence[str]
    symbols: Sequence[str]
    start: datetime | str
    end: datetime | str
    output_root: Path
    job_id: str
    profile: str
    overwrite: bool = False


@dataclass(frozen=True)
class _NormalizedRequest:
    replay_root: Path
    venues: tuple[str, ...]
    symbols: tuple[str, ...]
    start: datetime
    end: datetime
    output_root: Path
    job_id: str
    profile: str
    overwrite: bool

    def manifest_value(self) -> dict[str, Any]:
        return {
            "replay_root": str(self.replay_root),
            "output_root": str(self.output_root),
            "job_id": self.job_id,
            "venues": list(self.venues),
            "symbols": list(self.symbols),
            "start_utc": _utc_text(self.start),
            "end_utc": _utc_text(self.end),
            "interval": "[start,end)",
            "end_exclusive": True,
            "profile": self.profile,
        }


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SelectedCatalogError(f"{field} must be a valid ISO-8601 timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise SelectedCatalogError(f"{field} must be an ISO-8601 string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SelectedCatalogError(f"{field} must include an unambiguous UTC offset")
    return parsed.astimezone(timezone.utc)


def _normalize_selection(values: Sequence[str], field: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not values:
        raise SelectedCatalogError(f"at least one {field} value is required")
    normalized = tuple(str(value).strip().upper() for value in values)
    if any(not value for value in normalized):
        raise SelectedCatalogError(f"empty {field} values are not permitted")
    if len(set(normalized)) != len(normalized):
        raise SelectedCatalogError(f"duplicate {field} values are not permitted")
    if any("/" in value or "\\" in value or value in (".", "..") for value in normalized):
        raise SelectedCatalogError(f"invalid {field} selection")
    return tuple(sorted(normalized))


def _validate_job_id(job_id: str) -> str:
    if not isinstance(job_id, str) or not job_id or not _JOB_ID_RE.fullmatch(job_id):
        raise SelectedCatalogError(
            "job_id must be 1-64 ASCII letters, digits, '.', '_' or '-', "
            "start with a letter or digit, and contain no path separators"
        )
    if ".." in job_id or Path(job_id).is_absolute():
        raise SelectedCatalogError("job_id must identify one child directory")
    return job_id


def _owned_real_directory(path: Path, label: str) -> Path:
    path = Path(path)
    if path.is_symlink():
        raise SelectedCatalogError(f"{label} must not be a symlink")
    if not path.is_dir():
        raise SelectedCatalogError(f"{label} must be an existing directory")
    resolved = path.resolve(strict=True)
    if resolved.stat().st_uid != os.getuid():
        raise SelectedCatalogError(f"{label} ownership is ambiguous")
    return resolved


def _normalize_request(request: SelectedCatalogRequest) -> _NormalizedRequest:
    start = _parse_utc(request.start, "start")
    end = _parse_utc(request.end, "end")
    if start >= end:
        raise SelectedCatalogError("start must be strictly before end")
    if request.profile not in SUPPORTED_PROFILES:
        raise SelectedCatalogError(
            f"unsupported profile {request.profile!r}; supported: {', '.join(SUPPORTED_PROFILES)}"
        )
    replay_root = _owned_real_directory(Path(request.replay_root), "replay_root")
    output_root = _owned_real_directory(Path(request.output_root), "output_root")
    job_id = _validate_job_id(request.job_id)
    final_job = output_root / job_id
    if final_job == output_root or final_job.parent != output_root:
        raise SelectedCatalogError("job directory must be an immediate child of output_root")
    return _NormalizedRequest(
        replay_root=replay_root,
        venues=_normalize_selection(request.venues, "venue"),
        symbols=_normalize_selection(request.symbols, "symbol"),
        start=start,
        end=end,
        output_root=output_root,
        job_id=job_id,
        profile=request.profile,
        overwrite=bool(request.overwrite),
    )


def _canonical_digest(value: Any, algorithm: str = INVENTORY_DIGEST_ALGORITHM) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    digest = hashlib.sha256()
    digest.update(algorithm.encode("ascii"))
    digest.update(b"\0")
    digest.update(encoded)
    return digest.hexdigest()


def _hash_file(path: Path) -> tuple[str, int]:
    if path.is_symlink() or not path.is_file():
        raise SelectedCatalogError(f"required regular file is missing or unsafe: {path}")
    before = path.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = path.stat(follow_symlinks=False)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    ):
        raise SelectedCatalogError(f"artifact changed while hashing: {path}")
    return digest.hexdigest(), before.st_size


def _load_stable_json(path: Path, label: str) -> tuple[dict[str, Any], str, int]:
    """Load strict JSON only when its exact bytes stay unchanged around parsing."""
    try:
        before_sha, before_size = _hash_file(path)
        document = load_json_object(path)
        after_sha, after_size = _hash_file(path)
    except (OSError, ValueError) as exc:
        raise SelectedCatalogError(f"{label} is missing, malformed, or unsafe") from exc
    if (before_sha, before_size) != (after_sha, after_size):
        raise SelectedCatalogError(f"{label} changed while it was validated")
    return document, after_sha, after_size


def _assert_safe_descendant(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise SelectedCatalogError(f"path escapes replay_root: {path}") from exc
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise SelectedCatalogError(f"symlink traversal is not permitted: {current}")


def _target_dates(start: datetime, end: datetime) -> tuple[str, ...]:
    last = (end - timedelta(microseconds=1)).date()
    current = start.date()
    dates: list[str] = []
    while current <= last:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(dates)


def _previous_date(date: str) -> str:
    return (datetime.strptime(date, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()


def _partition_dir(root: Path, venue: str, symbol: str, date: str) -> Path:
    path = root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    _assert_safe_descendant(root, path)
    return path


def _validate_instrument(document: Mapping[str, Any], venue: str, symbol: str) -> None:
    if document.get("venue") != venue or document.get("symbol") != symbol:
        raise SelectedCatalogError(f"instrument metadata contradicts {venue}/{symbol}")
    info = document.get("exchange_info") if isinstance(document.get("exchange_info"), dict) else document
    filters = info.get("filters")
    if not isinstance(filters, list) or not filters:
        raise SelectedCatalogError(f"instrument metadata has no exchange filters for {venue}/{symbol}")
    by_type = {item.get("filterType"): item for item in filters if isinstance(item, dict)}
    price_filter = by_type.get("PRICE_FILTER", {})
    lot_filter = by_type.get("LOT_SIZE", {})
    if not price_filter.get("tickSize") or not lot_filter.get("stepSize"):
        raise SelectedCatalogError(
            f"instrument metadata lacks exact PRICE_FILTER/LOT_SIZE increments for {venue}/{symbol}"
        )


def _partition_inventory(
    request: _NormalizedRequest,
    venue: str,
    symbol: str,
    date: str,
    roles: Iterable[str],
) -> dict[str, Any]:
    partition = _partition_dir(request.replay_root, venue, symbol, date)
    if not partition.is_dir() or partition.is_symlink():
        raise SelectedCatalogError(f"required replay partition is missing: {venue}/{symbol}/{date}")
    if partition.stat().st_uid != os.getuid():
        raise SelectedCatalogError(f"replay partition ownership is ambiguous: {venue}/{symbol}/{date}")
    if not validate_partition(partition):
        raise SelectedCatalogError(f"replay partition failed routine validation: {venue}/{symbol}/{date}")

    manifest_path = partition / "manifest.json"
    depth_path = partition / "depth.parquet"
    trades_path = partition / "trades.parquet"
    instrument_path = partition / "instrument.json"
    manifest, manifest_sha, manifest_bytes = _load_stable_json(
        manifest_path, f"replay manifest for {venue}/{symbol}/{date}"
    )
    instrument, instrument_sha, instrument_bytes = _load_stable_json(
        instrument_path, f"instrument metadata for {venue}/{symbol}/{date}"
    )
    if manifest.get("status") != "complete":
        raise SelectedCatalogError(f"replay manifest is not complete: {venue}/{symbol}/{date}")
    for field, expected in (("venue", venue), ("symbol", symbol), ("date", date)):
        if manifest.get(field) != expected:
            raise SelectedCatalogError(
                f"replay manifest {field} contradicts partition path for {venue}/{symbol}/{date}"
            )
    _validate_instrument(instrument, venue, symbol)
    reader = ReplayReader(request.replay_root)
    try:
        schema_version = reader.get_schema_version(venue, symbol, date)
    except Exception as exc:
        raise SelectedCatalogError(
            f"unsupported or contradictory replay schema contract: {venue}/{symbol}/{date}"
        ) from exc

    depth_sha, depth_bytes = _hash_file(depth_path)
    trades_sha, trades_bytes = _hash_file(trades_path)
    if manifest.get("depth_checksum") != depth_sha or manifest.get("trades_checksum") != trades_sha:
        raise SelectedCatalogError(f"manifest checksum contradiction: {venue}/{symbol}/{date}")
    relative = partition.relative_to(request.replay_root).as_posix()
    return {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "roles": sorted(set(roles)),
        "relative_path": relative,
        "replay_manifest_sha256": manifest_sha,
        "schema_version": schema_version,
        "format_version": manifest.get("format_version", 0),
        "builder_version": manifest.get("builder_version", "historical-v0"),
        "files": {
            "manifest.json": {"sha256": manifest_sha, "size_bytes": manifest_bytes},
            "depth.parquet": {"sha256": depth_sha, "size_bytes": depth_bytes},
            "trades.parquet": {"sha256": trades_sha, "size_bytes": trades_bytes},
            "instrument.json": {"sha256": instrument_sha, "size_bytes": instrument_bytes},
        },
        "source_identity_digest": (
            _canonical_digest(manifest["source_identity"])
            if isinstance(manifest.get("source_identity"), dict)
            else None
        ),
        "integrity_digest": (
            _canonical_digest(manifest["integrity"])
            if isinstance(manifest.get("integrity"), dict)
            else None
        ),
    }


def _starts_from_local_snapshot(reader: ReplayReader, venue: str, symbol: str, date: str) -> bool:
    for record in reader.iter_depths(venue, symbol, date):
        kind = record.get("record_type")
        if kind == "snapshot_seed":
            return True
        if kind == "depth_update":
            return False
    return False


def _preflight(request: _NormalizedRequest) -> dict[str, Any]:
    reader = ReplayReader(request.replay_root)
    roles: dict[tuple[str, str, str], set[str]] = {}
    carry_resolution: list[dict[str, str]] = []
    dates = _target_dates(request.start, request.end)
    for venue in request.venues:
        for symbol in request.symbols:
            for date in dates:
                target_path = _partition_dir(request.replay_root, venue, symbol, date)
                if not target_path.is_dir() or target_path.is_symlink():
                    raise SelectedCatalogError(
                        f"required replay partition is missing: {venue}/{symbol}/{date}"
                    )
                roles.setdefault((venue, symbol, date), set()).add("target")
                if request.profile == "full_l2":
                    previous = _previous_date(date)
                    previous_path = _partition_dir(request.replay_root, venue, symbol, previous)
                    if previous_path.is_dir():
                        roles.setdefault((venue, symbol, previous), set()).add("preceding_carry")
                    elif not _starts_from_local_snapshot(reader, venue, symbol, date):
                        raise SelectedCatalogError(
                            f"preceding replay state is required but missing for {venue}/{symbol}/{date}: {previous}"
                        )
                    else:
                        carry_resolution.append({
                            "venue": venue,
                            "symbol": symbol,
                            "target_date": date,
                            "resolution": "target_partition_starts_from_snapshot_seed",
                        })
    inventory = [
        _partition_inventory(request, venue, symbol, date, selected_roles)
        for (venue, symbol, date), selected_roles in sorted(roles.items())
    ]
    for venue in request.venues:
        for symbol in request.symbols:
            instrument_hashes = {
                item["files"]["instrument.json"]["sha256"]
                for item in inventory
                if item["venue"] == venue
                and item["symbol"] == symbol
                and "target" in item["roles"]
            }
            if len(instrument_hashes) != 1:
                raise SelectedCatalogError(
                    f"target partitions have contradictory instrument metadata for {venue}/{symbol}"
                )
    return {
        "target_dates": list(dates),
        "partitions": inventory,
        "inventory_sha256": _canonical_digest(inventory),
        "carry_resolution": carry_resolution,
    }


def _rehash_preflight(request: _NormalizedRequest, original: Mapping[str, Any]) -> dict[str, Any]:
    refreshed = []
    for item in original["partitions"]:
        refreshed.append(
            _partition_inventory(
                request, item["venue"], item["symbol"], item["date"], item["roles"]
            )
        )
    result = {
        "target_dates": list(original["target_dates"]),
        "partitions": refreshed,
        "inventory_sha256": _canonical_digest(refreshed),
        "carry_resolution": list(original.get("carry_resolution", [])),
    }
    if result != original:
        raise SelectedCatalogError("consumed replay artifact identity changed during reconstruction")
    return result


def _catalog_inventory(root: Path) -> tuple[list[dict[str, Any]], str]:
    if root.is_symlink() or not root.is_dir():
        raise SelectedCatalogError("reconstruction did not produce a safe catalog directory")
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if path.is_symlink():
            raise SelectedCatalogError("catalog output contains a symlink")
        if path.is_dir():
            continue
        if not path.is_file():
            raise SelectedCatalogError("catalog output contains a non-regular file")
        sha256, size = _hash_file(path)
        files.append({
            "relative_path": path.relative_to(root).as_posix(),
            "size_bytes": size,
            "sha256": sha256,
        })
    if not files:
        raise SelectedCatalogError("reconstruction produced an empty catalog")
    return files, _canonical_digest(files, CATALOG_DIGEST_ALGORITHM)


def _repository_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=_REPOSITORY_ROOT,
            check=True, capture_output=True, text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SelectedCatalogError("could not determine repository commit SHA") from exc


def _implementation_file_hashes() -> dict[str, str]:
    selected = Path(__file__).resolve()
    engine = _REPOSITORY_ROOT / "validation" / "replay_catalog_reconstruct.py"
    return {
        selected.relative_to(_REPOSITORY_ROOT).as_posix(): _hash_file(selected)[0],
        engine.relative_to(_REPOSITORY_ROOT).as_posix(): _hash_file(engine)[0],
    }


def _dependency_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for distribution in ("nautilus_trader", "pyarrow", "zstandard"):
        try:
            versions[distribution] = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            versions[distribution] = "not-installed"
    return versions


def _load_engine():
    guidance = (
        "run 'uv sync --frozen --no-default-groups --extra reconstruction'"
    )
    try:
        from validation import replay_catalog_reconstruct as engine
    except ImportError as exc:
        raise SelectedCatalogError(
            "selected reconstruction dependencies are required; " + guidance
        ) from exc
    if not engine.NAUTILUS_AVAILABLE:
        raise SelectedCatalogError(
            "selected reconstruction dependencies are required; " + guidance
        )
    try:
        installed = importlib.metadata.version("nautilus_trader")
    except importlib.metadata.PackageNotFoundError as exc:
        raise SelectedCatalogError(
            "selected reconstruction dependencies are required; " + guidance
        ) from exc
    if installed != "1.225.0":
        raise SelectedCatalogError(
            "selected reconstruction requires the tested nautilus_trader==1.225.0 "
            f"compatibility boundary (installed: {installed})"
        )
    return engine


def _validate_existing_job(path: Path, job_id: str) -> None:
    if path.is_symlink() or not path.is_dir() or path.stat().st_uid != os.getuid():
        raise SelectedCatalogError("existing job is unsafe or ownership is ambiguous")
    for member in path.rglob("*"):
        if member.is_symlink() or (not member.is_dir() and not member.is_file()):
            raise SelectedCatalogError("existing job contains an unexpected file type or symlink")
        if member.stat(follow_symlinks=False).st_uid != os.getuid():
            raise SelectedCatalogError("existing job contains ambiguously owned content")
    manifest = load_json_object(path / "job_manifest.json")
    if (
        manifest.get("manifest_version") != MANIFEST_VERSION
        or manifest.get("job_id") != job_id
        or manifest.get("status") != "complete"
    ):
        raise SelectedCatalogError("existing job is not a completed selected-catalog job")


def _write_json(path: Path, document: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(document, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def reconstruct_selected_catalog(*, request: SelectedCatalogRequest) -> Path:
    """Build and atomically publish one explicitly selected temporary catalog.

    Returns the final ``<output-root>/<job-id>`` path.  Any exception is
    fail-closed: the final path is not created or replaced, and staging is
    preserved as a sibling ``.failed_*`` evidence directory when possible.
    """
    normalized = _normalize_request(request)
    final_job = normalized.output_root / normalized.job_id
    if final_job.exists():
        if not normalized.overwrite:
            raise SelectedCatalogError(f"job already exists: {final_job}")
        _validate_existing_job(final_job, normalized.job_id)
    elif final_job.is_symlink():
        raise SelectedCatalogError("final job path must not be a symlink")

    try:
        preflight = _preflight(normalized)
        engine = _load_engine()
    except SelectedCatalogError:
        raise
    except Exception as exc:
        raise SelectedCatalogError(f"selected reconstruction preflight failed: {exc}") from exc
    created = datetime.now(timezone.utc)
    nonce = uuid.uuid4().hex
    staging = normalized.output_root / f".staging_{normalized.job_id}_{nonce}"
    failed = normalized.output_root / f".failed_{normalized.job_id}_{created.strftime('%Y%m%dT%H%M%SZ')}_{nonce}"
    staging.mkdir(mode=0o700)
    try:
        status = engine.generate_catalog_from_replay(
            normalized.replay_root,
            staging,
            normalized.job_id,
            list(normalized.symbols),
            list(normalized.venues),
            normalized.start,
            normalized.end,
            profile=normalized.profile,
            overwrite=False,
        )
        if status.get("status") != "success":
            raise SelectedCatalogError(
                "reconstruction engine failed: " + "; ".join(status.get("errors") or ["unknown error"])
            )
        if status.get("missing_partitions"):
            raise SelectedCatalogError("reconstruction engine reported a missing requested partition")
        if len(status.get("found_partitions", [])) != (
            len(normalized.venues) * len(normalized.symbols) * len(preflight["target_dates"])
        ):
            raise SelectedCatalogError("reconstruction engine processed an unexpected partition set")

        catalog_source = staging / f"job_{normalized.job_id}"
        catalog = staging / "catalog"
        if not catalog_source.is_dir() or catalog_source.is_symlink():
            raise SelectedCatalogError("reconstruction engine did not create the expected catalog")
        catalog_source.rename(catalog)
        _rehash_preflight(normalized, preflight)
        catalog_files, catalog_digest = _catalog_inventory(catalog)
        completed = datetime.now(timezone.utc)
        manifest = {
            "manifest_version": MANIFEST_VERSION,
            "job_id": normalized.job_id,
            "status": "complete",
            "normalized_request": normalized.manifest_value(),
            "time_semantics": {"interval": "[start,end)", "end_exclusive": True, "filter": "ts_init"},
            "profile": normalized.profile,
            "repository_commit_sha": _repository_commit(),
            "implementation_file_sha256": _implementation_file_hashes(),
            "python_version": sys.version.split()[0],
            "dependency_versions": _dependency_versions(),
            "created_at_utc": _utc_text(created),
            "completed_at_utc": _utc_text(completed),
            "catalog_relative_path": "catalog",
            "record_counts": status["records_written"],
            "record_counts_by_partition": status.get("partition_record_counts", []),
            "target_replay_partitions": [
                item for item in preflight["partitions"] if "target" in item["roles"]
            ],
            "preceding_carry_partitions": [
                item for item in preflight["partitions"] if "preceding_carry" in item["roles"]
            ],
            "consumed_partition_inventory": preflight["partitions"],
            "consumed_partition_inventory_digest": {
                "algorithm": INVENTORY_DIGEST_ALGORITHM,
                "sha256": preflight["inventory_sha256"],
            },
            "catalog_file_inventory": catalog_files,
            "catalog_tree_digest": {
                "algorithm": CATALOG_DIGEST_ALGORITHM,
                "sha256": catalog_digest,
            },
            "carry_resolution": preflight.get("carry_resolution", []),
            "warnings": list(status.get("warnings", [])),
        }
        _write_json(staging / "job_manifest.json", manifest)
        final_catalog_files, final_catalog_digest = _catalog_inventory(catalog)
        if final_catalog_files != catalog_files or final_catalog_digest != catalog_digest:
            raise SelectedCatalogError("catalog output changed before publication")
        _rehash_preflight(normalized, preflight)

        backup: Path | None = None
        if final_job.exists():
            _validate_existing_job(final_job, normalized.job_id)
            backup = normalized.output_root / f".replaced_{normalized.job_id}_{nonce}"
            final_job.rename(backup)
        try:
            staging.rename(final_job)
        except Exception:
            if backup is not None and backup.exists() and not final_job.exists():
                backup.rename(final_job)
            raise
        if backup is not None:
            shutil.rmtree(backup)
        return final_job
    except Exception as exc:
        if staging.exists():
            try:
                _write_json(staging / "failure.json", {
                    "manifest_version": MANIFEST_VERSION,
                    "job_id": normalized.job_id,
                    "status": "failed",
                    "failed_at_utc": _utc_text(datetime.now(timezone.utc)),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                })
                staging.rename(failed)
            except Exception:
                pass
        if isinstance(exc, SelectedCatalogError):
            raise
        raise SelectedCatalogError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconstruct one explicitly selected temporary Nautilus catalog."
    )
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--venues", nargs="+", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--profile", choices=SUPPORTED_PROFILES, required=True)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        output = reconstruct_selected_catalog(request=SelectedCatalogRequest(
            replay_root=args.replay_root,
            venues=args.venues,
            symbols=args.symbols,
            start=args.start,
            end=args.end,
            output_root=args.output_root,
            job_id=args.job_id,
            profile=args.profile,
            overwrite=args.overwrite,
        ))
    except SelectedCatalogError as exc:
        print(f"selected catalog reconstruction failed: {exc}", file=sys.stderr)
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
