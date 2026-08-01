"""Build-wide replay mutation ownership, recovery, and atomic evidence.

This module is intentionally limited to replay *lifecycle* mechanics.  It does
not decode raw records or alter replay semantics.  Every supported mutating
entrypoint acquires :func:`acquire_replay_build_lock` and passes the resulting
context to nested operations, avoiding nested-lock deadlocks.
"""
from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence

from stores.replay_writer import validate_partition


LIFECYCLE_CONTRACT_VERSION = 1
_VENUE_RE = re.compile(r"venue=([A-Z0-9_]+)\Z")
_SYMBOL_RE = re.compile(r"symbol=([A-Z0-9_-]+)\Z")
_DATE_RE = re.compile(r"date=(\d{4}-\d{2}-\d{2})\Z")
_TRANSIENT_RE = re.compile(
    r"\.(staging|backup|quarantine)_(\d{4}-\d{2}-\d{2})_(.+)\Z"
)


class ReplayBuildActiveError(RuntimeError):
    """Another process owns the common replay mutation lock."""


class ReplayLifecycleSafetyError(RuntimeError):
    """Replay lifecycle state is unsafe or ambiguous."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def repository_sha(repository_root: Path | None = None) -> str:
    root = repository_root or Path(__file__).resolve().parents[1]
    try:
        digest = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except Exception as exc:
        raise ReplayLifecycleSafetyError(
            f"cannot determine repository commit SHA: {exc}"
        ) from exc
    if not re.fullmatch(r"[0-9a-f]{40}", digest):
        raise ReplayLifecycleSafetyError(
            f"repository commit SHA is malformed: {digest!r}"
        )
    return digest


def atomic_write_json(target: Path, payload: dict) -> None:
    """Durably publish JSON using same-directory temp + fsync + replace."""
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink() or target.parent.is_symlink():
        raise ReplayLifecycleSafetyError(f"report path may not be a symlink: {target}")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        raise


@dataclass
class ReplayLifecycleContext:
    run_id: str
    replay_root: Path
    data_root: Path
    report_root: Path
    lock_path: Path
    metadata: dict
    _fd: int
    _active: bool = True

    def assert_held(self, replay_root: Path | None = None) -> None:
        if not self._active or self._fd < 0:
            raise ReplayLifecycleSafetyError("replay lifecycle lock is not held")
        os.fstat(self._fd)
        if replay_root is not None and Path(replay_root).resolve() != self.replay_root.resolve():
            raise ReplayLifecycleSafetyError(
                f"lifecycle context owns {self.replay_root}, not {replay_root}"
            )


def _validate_lock_parent(replay_root: Path) -> Path:
    replay_root = Path(replay_root)
    if replay_root.exists() and replay_root.is_symlink():
        raise ReplayLifecycleSafetyError(f"replay root may not be a symlink: {replay_root}")
    replay_root.mkdir(parents=True, exist_ok=True)
    lifecycle_dir = replay_root / ".lifecycle"
    if lifecycle_dir.is_symlink():
        raise ReplayLifecycleSafetyError(
            f"lifecycle directory may not be a symlink: {lifecycle_dir}"
        )
    lifecycle_dir.mkdir(mode=0o700, exist_ok=True)
    info = lifecycle_dir.stat(follow_symlinks=False)
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or (stat.S_IMODE(info.st_mode) & 0o022) != 0
    ):
        raise ReplayLifecycleSafetyError(
            f"unsafe lifecycle directory ownership/type/mode: {lifecycle_dir}"
        )
    return lifecycle_dir


@contextmanager
def acquire_replay_build_lock(
    *,
    replay_root: Path,
    data_root: Path,
    report_root: Path,
    command: Sequence[str] | None = None,
    run_id: str | None = None,
    repository_root: Path | None = None,
) -> Iterator[ReplayLifecycleContext]:
    """Acquire the common Linux advisory replay mutation lock non-blocking."""
    lifecycle_dir = _validate_lock_parent(Path(replay_root))
    lock_path = lifecycle_dir / "build.lock"
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise ReplayLifecycleSafetyError(f"cannot open safe build lock {lock_path}: {exc}") from exc
    context: ReplayLifecycleContext | None = None
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or (stat.S_IMODE(info.st_mode) & 0o022) != 0
        ):
            raise ReplayLifecycleSafetyError(
                f"build lock must be a single-link, non-group/world-writable regular "
                f"file owned by uid {os.getuid()}: {lock_path}"
            )
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            existing = ""
            try:
                existing = os.read(fd, 16_384).decode("utf-8", errors="replace")
            except OSError:
                pass
            raise ReplayBuildActiveError(
                f"build already active for replay root {replay_root}; lock={lock_path}; "
                f"owner_metadata={existing.strip() or 'unavailable'}"
            ) from exc

        actual_run_id = run_id or f"replay-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:12]}"
        metadata = {
            "contract_version": LIFECYCLE_CONTRACT_VERSION,
            "run_id": actual_run_id,
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "command": list(command or sys.argv),
            "start_utc": utc_now_iso(),
            "repository_sha": repository_sha(repository_root),
            "data_root": str(Path(data_root).resolve()),
            "replay_root": str(Path(replay_root).resolve()),
            "report_root": str(Path(report_root).resolve()),
        }
        encoded = (json.dumps(metadata, sort_keys=True, indent=2) + "\n").encode()
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        offset = 0
        while offset < len(encoded):
            offset += os.write(fd, encoded[offset:])
        os.fsync(fd)
        context = ReplayLifecycleContext(
            run_id=actual_run_id,
            replay_root=Path(replay_root),
            data_root=Path(data_root),
            report_root=Path(report_root),
            lock_path=lock_path,
            metadata=metadata,
            _fd=fd,
        )
        yield context
    finally:
        if context is not None:
            context._active = False
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _iter_entries_bounded(directory: Path, counter: list[int], max_entries: int) -> list[Path]:
    if directory.is_symlink():
        raise ReplayLifecycleSafetyError(f"symlink is forbidden in replay lifecycle scan: {directory}")
    entries = sorted(directory.iterdir(), key=lambda path: path.name)
    counter[0] += len(entries)
    if counter[0] > max_entries:
        raise ReplayLifecycleSafetyError(
            f"replay lifecycle scan exceeded configured max entries {max_entries}"
        )
    return entries


def _unique_quarantine(parent: Path, date: str, symbol: str, reason: str, run_id: str) -> Path:
    base = parent / f".quarantine_{date}_{symbol}_{reason}.{run_id}"
    if base.exists() or base.is_symlink():
        raise ReplayLifecycleSafetyError(f"quarantine destination collision: {base}")
    return base


def reconcile_replay_root(
    context: ReplayLifecycleContext,
    *,
    max_entries: int = 20_000,
    max_actions: int = 2_000,
) -> list[dict]:
    """Reconcile all canonical replay partitions and transients, fail closed.

    The scan is deliberately shallow and contract-aware: replay root -> venue
    -> symbol -> canonical/transient partition.  Symlinks and unknown entries
    are never followed or ignored.
    """
    context.assert_held()
    if max_entries < 1 or max_actions < 1:
        raise ValueError("reconciliation bounds must be positive")
    root = context.replay_root
    actions: list[dict] = []
    counter = [0]

    def record(action: str, path: Path, **extra: object) -> None:
        if len(actions) >= max_actions:
            raise ReplayLifecycleSafetyError(
                f"reconciliation exceeded configured max actions {max_actions}"
            )
        actions.append({"action": action, "path": str(path), **extra})

    for venue_dir in _iter_entries_bounded(root, counter, max_entries):
        if venue_dir.name == ".lifecycle":
            continue
        venue_match = _VENUE_RE.fullmatch(venue_dir.name)
        if not venue_match or venue_dir.is_symlink() or not venue_dir.is_dir():
            raise ReplayLifecycleSafetyError(f"unknown/unsafe replay-root entry: {venue_dir}")
        venue = venue_match.group(1)
        for symbol_dir in _iter_entries_bounded(venue_dir, counter, max_entries):
            symbol_match = _SYMBOL_RE.fullmatch(symbol_dir.name)
            if not symbol_match or symbol_dir.is_symlink() or not symbol_dir.is_dir():
                raise ReplayLifecycleSafetyError(f"unknown/unsafe venue entry: {symbol_dir}")
            symbol = symbol_match.group(1)
            canonicals: dict[str, Path] = {}
            transients: dict[tuple[str, str], list[Path]] = {}
            for entry in _iter_entries_bounded(symbol_dir, counter, max_entries):
                if entry.is_symlink():
                    raise ReplayLifecycleSafetyError(f"symlink in replay tree: {entry}")
                date_match = _DATE_RE.fullmatch(entry.name)
                if date_match and entry.is_dir():
                    date = date_match.group(1)
                    if date in canonicals:
                        raise ReplayLifecycleSafetyError(f"duplicate canonical date: {entry}")
                    canonicals[date] = entry
                    continue
                transient_match = _TRANSIENT_RE.fullmatch(entry.name)
                if transient_match and entry.is_dir():
                    kind, date, remainder = transient_match.groups()
                    if remainder != symbol and not remainder.startswith(f"{symbol}_"):
                        raise ReplayLifecycleSafetyError(
                            f"transient symbol contradicts parent: {entry}"
                        )
                    transients.setdefault((kind, date), []).append(entry)
                    continue
                raise ReplayLifecycleSafetyError(f"unknown replay artifact: {entry}")

            all_dates = sorted(set(canonicals) | {date for _kind, date in transients})
            for date in all_dates:
                canonical = canonicals.get(date)
                backups = transients.get(("backup", date), [])
                stagings = transients.get(("staging", date), [])
                quarantines = transients.get(("quarantine", date), [])
                for quarantine in quarantines:
                    record("quarantine_preserved", quarantine, venue=venue, date=date, symbol=symbol)
                if len(backups) > 1:
                    raise ReplayLifecycleSafetyError(
                        f"ambiguous multiple backups for {symbol}/{date}: {backups}"
                    )
                if len(stagings) > 1:
                    raise ReplayLifecycleSafetyError(
                        f"ambiguous multiple staging artifacts for {symbol}/{date}: {stagings}"
                    )
                for staging in stagings:
                    destination = _unique_quarantine(
                        symbol_dir, date, symbol, "stale_staging", context.run_id
                    )
                    os.replace(staging, destination)
                    record(
                        "stale_staging_quarantined", staging,
                        destination=str(destination), venue=venue, date=date, symbol=symbol,
                    )

                canonical_exists = canonical is not None and canonical.exists()
                canonical_valid = bool(canonical_exists and validate_partition(canonical))
                backup = backups[0] if backups else None
                backup_valid = bool(backup and validate_partition(backup))

                if backup is not None:
                    expected_backup_name = f".backup_{date}_{symbol}"
                    if backup.name != expected_backup_name:
                        raise ReplayLifecycleSafetyError(
                            f"non-canonical backup name is ambiguous: {backup}"
                        )
                    if canonical_valid and backup_valid:
                        shutil.rmtree(backup)
                        record("obsolete_valid_backup_removed", backup, venue=venue, date=date, symbol=symbol)
                    elif canonical_valid and not backup_valid:
                        raise ReplayLifecycleSafetyError(
                            f"invalid backup beside valid canonical requires inspection: {backup}"
                        )
                    elif backup_valid:
                        if canonical_exists and canonical is not None:
                            destination = _unique_quarantine(
                                symbol_dir, date, symbol, "invalid_canonical", context.run_id
                            )
                            os.replace(canonical, destination)
                            record(
                                "invalid_canonical_quarantined", canonical,
                                destination=str(destination), venue=venue, date=date, symbol=symbol,
                            )
                        target = symbol_dir / f"date={date}"
                        os.replace(backup, target)
                        if not validate_partition(target):
                            raise ReplayLifecycleSafetyError(
                                f"restored backup failed validation: {target}"
                            )
                        record("backup_restored", backup, destination=str(target), venue=venue, date=date, symbol=symbol)
                    else:
                        raise ReplayLifecycleSafetyError(
                            f"backup is invalid and was preserved: {backup}"
                        )
                elif canonical_exists and not canonical_valid:
                    raise ReplayLifecycleSafetyError(
                        f"canonical replay partition is invalid with no valid backup: {canonical}"
                    )
    return actions


def tree_size_bytes(path: Path, *, max_entries: int = 100_000) -> tuple[int, int]:
    """Return (allocated, apparent) bytes without following symlinks."""
    allocated = apparent = entries = 0
    if not path.exists():
        return 0, 0
    for current, dirnames, filenames in os.walk(path, followlinks=False):
        current_path = Path(current)
        for name in list(dirnames):
            child = current_path / name
            if child.is_symlink():
                raise ReplayLifecycleSafetyError(f"symlink in measured tree: {child}")
        for name in filenames:
            entries += 1
            if entries > max_entries:
                raise ReplayLifecycleSafetyError(
                    f"tree measurement exceeded {max_entries} entries: {path}"
                )
            file_path = current_path / name
            info = file_path.stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode):
                raise ReplayLifecycleSafetyError(f"non-regular file in measured tree: {file_path}")
            allocated += info.st_blocks * 512
            apparent += info.st_size
    return allocated, apparent
