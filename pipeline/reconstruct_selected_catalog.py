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
import ctypes
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import uuid
import warnings
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
CLAIM_CONTRACT_VERSION = 1
SUPPORTED_PROFILES = ("full_l2", "trades_only")
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class SelectedCatalogError(RuntimeError):
    """The selected reconstruction request failed closed."""


class _PreserveClaimError(SelectedCatalogError):
    """Publication evidence is ambiguous and its exact claim must remain."""


@dataclass
class _JobClaim:
    path: Path
    document: dict[str, Any]
    released: bool = False


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


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, document: Mapping[str, Any]) -> None:
    """Write one owned JSON file durably within its existing directory."""
    if path.is_symlink() or path.parent.is_symlink() or not path.parent.is_dir():
        raise SelectedCatalogError(f"unsafe JSON publication path: {path}")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(temporary, flags, 0o600)
        try:
            encoded = (
                json.dumps(
                    document,
                    sort_keys=True,
                    indent=2,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            offset = 0
            while offset < len(encoded):
                offset += os.write(fd, encoded[offset:])
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _claim_path(request: _NormalizedRequest) -> Path:
    path = request.output_root / f".claim_{request.job_id}"
    if path.parent != request.output_root:
        raise SelectedCatalogError("selected-job claim escapes output_root")
    return path


def _validate_claim_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise SelectedCatalogError(
            f"selected-job claim is unsafe; manual inspection required: {path}"
        )
    info = path.stat(follow_symlinks=False)
    if info.st_uid != os.getuid() or (stat.S_IMODE(info.st_mode) & 0o077) != 0:
        raise SelectedCatalogError(
            f"selected-job claim ownership/mode is ambiguous: {path}"
        )


def _claim_state(claim: _JobClaim, state: str, **updates: Any) -> None:
    document = dict(claim.document)
    document.update(updates)
    document["state"] = state
    document["updated_at_utc"] = _utc_text(datetime.now(timezone.utc))
    _atomic_write_json(claim.path / "claim.json", document)
    claim.document = document


def _validate_claim_contents(claim_path: Path) -> None:
    allowed = {"claim.json"}
    actual: set[str] = set()
    for entry in claim_path.iterdir():
        if entry.is_symlink() or not entry.is_file():
            raise SelectedCatalogError(
                f"selected-job claim contains unsafe evidence: {entry}"
            )
        info = entry.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or (stat.S_IMODE(info.st_mode) & 0o022) != 0
        ):
            raise SelectedCatalogError(
                f"selected-job claim contains unsafe ownership/type/mode: {entry}"
            )
        actual.add(entry.name)
    if actual != allowed:
        raise SelectedCatalogError(
            f"selected-job claim has unknown or missing entries {sorted(actual)}; "
            "manual inspection required"
        )


def _remove_claim(claim: _JobClaim) -> None:
    if claim.released:
        return
    _validate_claim_contents(claim.path)
    (claim.path / "claim.json").unlink()
    claim.path.rmdir()
    _fsync_directory(claim.path.parent)
    claim.released = True


def _report_completed_claim_cleanup_failure(
    *,
    final_job: Path,
    claim: _JobClaim,
    error: BaseException,
) -> None:
    """Report post-publication claim cleanup failure without reversing success.

    The selected job is already complete and validated at this boundary.  The
    manifest is updated when safely possible, while the runtime warning remains
    the reporting fallback if that update cannot be made durable.  This helper
    never recreates a partially removed claim and never raises.
    """
    warning = (
        f"selected job is complete and validated, but exact-job claim cleanup/"
        f"durability failed for {claim.path}: {type(error).__name__}: {error}; "
        "manual claim inspection/cleanup is required"
    )
    warning_to_emit = warning
    try:
        manifest_path = final_job / "job_manifest.json"
        manifest, _manifest_sha, _manifest_size = _load_stable_json(
            manifest_path,
            "completed selected-job manifest",
        )
        manifest_warnings = manifest.get("warnings")
        if not isinstance(manifest_warnings, list):
            raise SelectedCatalogError(
                "completed selected-job manifest warnings must be a list"
            )
        manifest["warnings"] = [*manifest_warnings, warning]
        _atomic_write_json(manifest_path, manifest)
    except Exception as manifest_error:
        warning_to_emit = (
            f"{warning}; the warning could not be appended durably to the job "
            f"manifest: {type(manifest_error).__name__}: {manifest_error}"
        )
    try:
        warnings.warn(warning_to_emit, RuntimeWarning, stacklevel=2)
    except Exception:
        # A warnings filter configured as "error" must not reverse an already
        # completed publication. The durable manifest warning remains the
        # primary report whenever it could be written.
        pass


def _new_job_claim(request: _NormalizedRequest) -> _JobClaim:
    claim_path = _claim_path(request)
    try:
        claim_path.mkdir(mode=0o700)
    except FileExistsError:
        _validate_claim_directory(claim_path)
        _validate_claim_contents(claim_path)
        document, _claim_sha, _claim_size = _load_stable_json(
            claim_path / "claim.json", "selected-job claim metadata"
        )
        if (
            document.get("claim_contract_version") != CLAIM_CONTRACT_VERSION
            or document.get("job_id") != request.job_id
            or document.get("output_root") != str(request.output_root)
        ):
            raise SelectedCatalogError(
                f"selected-job claim identity is contradictory; manual inspection required: "
                f"{claim_path}"
            )
        raise SelectedCatalogError(
            f"selected-catalog job is already claimed by an active invocation or "
            f"preserved crash evidence; manual recovery is required before "
            f"mutation: {claim_path}"
        )
    try:
        document = {
            "claim_contract_version": CLAIM_CONTRACT_VERSION,
            "job_id": request.job_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "start_utc": _utc_text(datetime.now(timezone.utc)),
            "repository_sha": _repository_commit(),
            "output_root": str(request.output_root),
            "state": "claimed",
            "staging_name": None,
            "failed_name": None,
            "backup_name": None,
            "new_manifest_sha256": None,
            "updated_at_utc": None,
        }
        document["updated_at_utc"] = document["start_utc"]
        claim = _JobClaim(claim_path, document)
        _atomic_write_json(claim_path / "claim.json", document)
        _fsync_directory(request.output_root)
        return claim
    except Exception:
        # Only clean the directory created by this invocation, and only while
        # it contains the exact initialization files we could have created.
        try:
            for child in claim_path.iterdir():
                if child.name != "claim.json":
                    raise SelectedCatalogError(
                        f"claim initialization left ambiguous evidence: {claim_path}"
                    )
                child.unlink()
            claim_path.rmdir()
            _fsync_directory(request.output_root)
        except OSError:
            pass
        raise


def _renameat2(left: Path, right: Path, flags: int, operation: str) -> None:
    """Apply one required Linux atomic-rename operation without fallback."""
    if left.is_symlink() or right.is_symlink() or left.parent != right.parent:
        raise SelectedCatalogError(
            f"selected-job {operation} requires safe sibling paths: {left}, {right}"
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise SelectedCatalogError(
            f"selected-job publication requires Linux renameat2 for {operation}; "
            "no job was replaced"
        )
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    at_fdcwd = -100
    if renameat2(
        at_fdcwd,
        os.fsencode(left),
        at_fdcwd,
        os.fsencode(right),
        flags,
    ) != 0:
        error_number = ctypes.get_errno()
        raise OSError(
            error_number,
            os.strerror(error_number),
            f"{left} <-> {right}",
        )


def _rename_exchange(left: Path, right: Path) -> None:
    """Atomically exchange two existing same-parent directories."""
    if not left.is_dir() or not right.is_dir():
        raise SelectedCatalogError(
            f"selected-job exchange requires two directories: {left}, {right}"
        )
    _renameat2(left, right, 2, "exchange")


def _rename_noreplace(left: Path, right: Path) -> None:
    """Atomically publish a directory only when the destination is absent."""
    if not left.is_dir() or right.exists() or right.is_symlink():
        raise SelectedCatalogError(
            f"selected-job no-replace publication state is unsafe: {left}, {right}"
        )
    _renameat2(left, right, 1, "no-replace rename")


def _job_manifest_sha(path: Path) -> str:
    return _hash_file(path / "job_manifest.json")[0]


def _job_has_manifest_sha(path: Path, expected_sha256: str) -> bool:
    try:
        return path.is_dir() and not path.is_symlink() and _job_manifest_sha(path) == expected_sha256
    except (OSError, SelectedCatalogError):
        return False


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


def _preserve_failed_staging(
    *,
    claim: _JobClaim,
    staging: Path,
    failed: Path,
    error: BaseException,
) -> None:
    """Durably preserve this invocation's staging evidence, or fail closed."""
    if not staging.exists():
        return
    if staging.is_symlink() or not staging.is_dir():
        raise _PreserveClaimError(
            f"selected-job staging is unsafe; claim preserved for inspection: {staging}"
        )
    if failed.exists() or failed.is_symlink():
        raise _PreserveClaimError(
            f"selected-job failure-evidence destination is ambiguous: {failed}"
        )
    _atomic_write_json(staging / "failure.json", {
        "manifest_version": MANIFEST_VERSION,
        "job_id": claim.document["job_id"],
        "status": "failed",
        "failed_at_utc": _utc_text(datetime.now(timezone.utc)),
        "error_type": type(error).__name__,
        "error": str(error),
    })
    _rename_noreplace(staging, failed)
    _fsync_directory(failed.parent)
    _claim_state(
        claim,
        "failed_evidence_preserved",
        staging_name=None,
        failed_name=failed.name,
    )


def _rollback_selected_publication(
    *,
    claim: _JobClaim,
    final_job: Path,
    staging: Path,
    backup: Path | None,
    initial_manifest_sha256: str | None,
    new_manifest_sha256: str,
) -> None:
    """Restore the pre-publication job state after a handled failure.

    Every overwrite location is recorded in the claim before mutation.  The
    atomic exchange keeps one complete job at the canonical path throughout;
    this helper exchanges the old job back when later validation/state work
    fails.  Any contradiction preserves the claim for manual recovery.
    """
    if _job_has_manifest_sha(final_job, new_manifest_sha256):
        if initial_manifest_sha256 is None:
            if staging.exists() or staging.is_symlink():
                raise _PreserveClaimError(
                    "cannot withdraw first publication because staging reappeared"
                )
            os.replace(final_job, staging)
            _fsync_directory(final_job.parent)
        else:
            old_location: Path | None = None
            for candidate in (staging, backup):
                if candidate is not None and _job_has_manifest_sha(
                    candidate, initial_manifest_sha256
                ):
                    old_location = candidate
                    break
            if old_location is None:
                raise _PreserveClaimError(
                    "cannot locate the prior completed job for rollback; claim preserved"
                )
            _rename_exchange(old_location, final_job)
            _fsync_directory(final_job.parent)
            if old_location == backup:
                if staging.exists() or staging.is_symlink():
                    raise _PreserveClaimError(
                        "cannot preserve withdrawn candidate because staging is ambiguous"
                    )
                _rename_noreplace(backup, staging)
                _fsync_directory(final_job.parent)

    if initial_manifest_sha256 is None:
        if final_job.exists() or final_job.is_symlink():
            raise _PreserveClaimError(
                "first-publication rollback left a canonical job; claim preserved"
            )
    elif not _job_has_manifest_sha(final_job, initial_manifest_sha256):
        raise _PreserveClaimError(
            "overwrite rollback did not restore the exact prior job; claim preserved"
        )
    _claim_state(claim, "rolled_back", staging_name=staging.name)


def reconstruct_selected_catalog(*, request: SelectedCatalogRequest) -> Path:
    """Build and recoverably publish one explicitly selected temporary catalog.

    One atomic exact-job claim is held from preflight through publication.
    First publication is a no-replace rename; overwrite uses Linux atomic
    directory exchange plus a durable claim state, so the previous completed
    job remains canonical throughout and every crash location is explicit.
    """
    normalized = _normalize_request(request)
    final_job = normalized.output_root / normalized.job_id
    claim = _new_job_claim(normalized)
    preserve_claim = False
    staging: Path | None = None
    failed: Path | None = None
    backup: Path | None = None
    initial_manifest_sha256: str | None = None
    new_manifest_sha256: str | None = None
    publication_prepared = False
    publication_complete = False
    try:
        if final_job.exists():
            if not normalized.overwrite:
                raise SelectedCatalogError(f"job already exists: {final_job}")
            _validate_existing_job(final_job, normalized.job_id)
            initial_manifest_sha256 = _job_manifest_sha(final_job)
        elif final_job.is_symlink():
            raise SelectedCatalogError("final job path must not be a symlink")

        try:
            preflight = _preflight(normalized)
            engine = _load_engine()
        except SelectedCatalogError:
            raise
        except Exception as exc:
            raise SelectedCatalogError(
                f"selected reconstruction preflight failed: {exc}"
            ) from exc

        created = datetime.now(timezone.utc)
        nonce = uuid.uuid4().hex
        staging = normalized.output_root / f".staging_{normalized.job_id}_{nonce}"
        failed = normalized.output_root / (
            f".failed_{normalized.job_id}_{created.strftime('%Y%m%dT%H%M%SZ')}_{nonce}"
        )
        backup = normalized.output_root / f".replaced_{normalized.job_id}_{nonce}"
        if any(path.exists() or path.is_symlink() for path in (staging, failed, backup)):
            raise SelectedCatalogError("selected-job temporary path collision")
        staging.mkdir(mode=0o700)
        _fsync_directory(normalized.output_root)
        _claim_state(
            claim,
            "building",
            staging_name=staging.name,
            failed_name=failed.name,
            backup_name=backup.name,
        )

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
        _atomic_write_json(staging / "job_manifest.json", manifest)
        final_catalog_files, final_catalog_digest = _catalog_inventory(catalog)
        if final_catalog_files != catalog_files or final_catalog_digest != catalog_digest:
            raise SelectedCatalogError("catalog output changed before publication")
        _rehash_preflight(normalized, preflight)

        new_manifest_sha256 = _job_manifest_sha(staging)
        # Recheck both authorization and the exact original job immediately
        # before mutation while the exclusive claim remains held.
        if final_job.exists():
            if not normalized.overwrite:
                raise SelectedCatalogError(f"job already exists: {final_job}")
            _validate_existing_job(final_job, normalized.job_id)
            current_manifest_sha256 = _job_manifest_sha(final_job)
            if initial_manifest_sha256 is None:
                raise SelectedCatalogError(
                    "job appeared during reconstruction; refusing unapproved replacement"
                )
            if current_manifest_sha256 != initial_manifest_sha256:
                raise SelectedCatalogError(
                    "existing job changed during reconstruction; refusing replacement"
                )
        elif final_job.is_symlink():
            raise SelectedCatalogError("final job path must not be a symlink")
        elif initial_manifest_sha256 is not None:
            raise SelectedCatalogError(
                "existing job disappeared during reconstruction; refusing publication"
            )

        publication_prepared = True
        _claim_state(
            claim,
            "publication_prepared",
            new_manifest_sha256=new_manifest_sha256,
        )
        if initial_manifest_sha256 is None:
            _rename_noreplace(staging, final_job)
            _fsync_directory(normalized.output_root)
        else:
            _rename_exchange(staging, final_job)
            _fsync_directory(normalized.output_root)
            _claim_state(claim, "jobs_exchanged")

        if not _job_has_manifest_sha(final_job, new_manifest_sha256):
            raise SelectedCatalogError(
                "published selected job failed exact manifest validation"
            )
        _validate_existing_job(final_job, normalized.job_id)
        _claim_state(claim, "published_valid")

        if initial_manifest_sha256 is not None:
            # After atomic exchange, staging contains the old completed job.
            _rename_noreplace(staging, backup)
            _fsync_directory(normalized.output_root)
            _claim_state(claim, "obsolete_backup_preserved", staging_name=None)
            cleanup_error: Exception | None = None
            try:
                shutil.rmtree(backup)
                _fsync_directory(normalized.output_root)
            except Exception as cleanup_exc:
                cleanup_error = cleanup_exc
            if cleanup_error is None:
                _claim_state(claim, "complete", backup_name=None)
            else:
                warning = (
                    f"published job is complete, but obsolete backup cleanup/durability "
                    f"failed; inspect {backup.name}: {cleanup_error}"
                )
                manifest["warnings"].append(warning)
                _atomic_write_json(final_job / "job_manifest.json", manifest)
                new_manifest_sha256 = _job_manifest_sha(final_job)
                _claim_state(
                    claim,
                    "complete_with_backup_warning",
                    new_manifest_sha256=new_manifest_sha256,
                    cleanup_warning=warning,
                )
        else:
            _claim_state(claim, "complete", staging_name=None, backup_name=None)
        publication_complete = True
        return final_job
    except Exception as exc:
        if publication_prepared and staging is not None and new_manifest_sha256 is not None:
            try:
                _rollback_selected_publication(
                    claim=claim,
                    final_job=final_job,
                    staging=staging,
                    backup=backup,
                    initial_manifest_sha256=initial_manifest_sha256,
                    new_manifest_sha256=new_manifest_sha256,
                )
            except Exception as rollback_exc:
                preserve_claim = True
                raise _PreserveClaimError(
                    f"selected-job publication failed and automatic rollback is "
                    f"ambiguous; claim preserved at {claim.path}: {rollback_exc}"
                ) from exc
        if staging is not None and failed is not None:
            try:
                _preserve_failed_staging(
                    claim=claim,
                    staging=staging,
                    failed=failed,
                    error=exc,
                )
            except Exception as evidence_exc:
                preserve_claim = True
                raise _PreserveClaimError(
                    f"selected-job failure evidence is ambiguous; claim preserved at "
                    f"{claim.path}: {evidence_exc}"
                ) from exc
        if isinstance(exc, SelectedCatalogError):
            raise
        raise SelectedCatalogError(str(exc)) from exc
    finally:
        if not preserve_claim:
            try:
                _remove_claim(claim)
            except Exception as claim_exc:
                if publication_complete:
                    _report_completed_claim_cleanup_failure(
                        final_job=final_job,
                        claim=claim,
                        error=claim_exc,
                    )
                else:
                    raise SelectedCatalogError(
                        f"selected-job claim cleanup failed; manual inspection required: "
                        f"{claim.path}: {claim_exc}"
                    ) from claim_exc


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
