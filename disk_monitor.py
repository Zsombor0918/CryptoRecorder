#!/usr/bin/env python
"""
Disk usage monitoring and estimation module.

Tracks:
  - Total size of data_raw/
  - Canonical replay separately from staging, backups, and quarantine
  - Total size of meta/ and state/report evidence
  - Filesystem-level capacity (independent of recursive directory sizing)
  - Growth rate and days-to-full, computed from real sample timestamps
  - Non-destructive raw-retention proof planning (automatic deletion disabled)

Safety invariant
-----------------
A failed or unavailable directory-size measurement must never be reported or
used as numeric zero. Every measurement is represented as a
`DirectoryMeasurement` with an explicit `ok`/`status` outcome. Callers must
never infer failure from a bare numeric value. When a measurement fails, the
monitor falls back to the last-known-good value (marked `stale`), or to
`None` if no prior value exists — never to `0`. Automatic cleanup refuses to
run whenever the current `data_raw` retention measurement is not a fresh,
successful measurement (see `cleanup_old_data`).
"""
import asyncio
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Deque, Dict, List, Literal, Optional, Tuple

from time_utils import local_now_iso
from stores.replay_writer import audit_partition_deep, validate_partition

logger = logging.getLogger(__name__)

BYTES_PER_GB = 1024 ** 3

MeasurementStatus = Literal[
    "ok",
    "missing",
    "timeout",
    "command_error",
    "malformed_output",
    "error",
]

# Minimum elapsed span between the oldest and newest valid growth samples
# before a growth-rate estimate is considered meaningful. Below this, growth
# and days_to_full are reported as None rather than as a noisy estimate.
MIN_GROWTH_SPAN_SEC = 3600.0


# ============================================================================
# Structured measurement results
# ============================================================================

@dataclass(frozen=True)
class DirectoryMeasurement:
    """Result of measuring one directory's size.

    `ok=False` must never be represented to callers as `value_bytes=0`.
    A genuinely empty directory that *was* measured successfully reports
    `value_bytes=0, ok=True, status="ok"`.
    """

    path: Path
    value_bytes: Optional[int]
    ok: bool
    status: MeasurementStatus
    error: Optional[str]
    measured_at: datetime
    duration_seconds: float


@dataclass(frozen=True)
class FilesystemMeasurement:
    """Fast, independent filesystem-capacity measurement via `shutil.disk_usage`."""

    path: Path
    device: str
    total_bytes: int
    used_bytes: int
    free_bytes: int
    measured_at: datetime

    @property
    def percent_used(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return self.used_bytes / self.total_bytes * 100

    def to_dict(self) -> dict:
        return {
            "filesystem_path": str(self.path),
            "filesystem_device_or_identity": self.device,
            "filesystem_total_gb": round(self.total_bytes / BYTES_PER_GB, 2),
            "filesystem_used_gb": round(self.used_bytes / BYTES_PER_GB, 2),
            "filesystem_free_gb": round(self.free_bytes / BYTES_PER_GB, 2),
            "filesystem_percent_used": round(self.percent_used, 1),
            "measured_at": self.measured_at.isoformat(),
        }


@dataclass
class LastKnownGood:
    """Last successful measurement for one monitored directory, persisted
    across restarts so a stale fallback survives a process restart."""

    value_bytes: int
    measured_at: str  # ISO-8601
    duration_seconds: float

    def to_dict(self) -> dict:
        return {
            "value_bytes": self.value_bytes,
            "measured_at": self.measured_at,
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LastKnownGood":
        return cls(
            value_bytes=int(data["value_bytes"]),
            measured_at=str(data["measured_at"]),
            duration_seconds=float(data.get("duration_seconds", 0.0)),
        )


@dataclass
class GrowthSample:
    """One bounded-history sample used for growth-rate estimation.

    Tracks `data_raw` usage specifically (the quantity the retention hard
    limit governs), not the cross-root observability total. Only recorded
    when this cycle's `data_raw` measurement was itself fresh and
    successful, so growth is never derived from a failed/stale/fallback
    sample.
    """

    epoch: float
    timestamp: str  # ISO-8601
    data_raw_bytes: int
    replay_bytes: int = 0
    replay_transient_bytes: int = 0
    replay_measurement_ok: bool = True

    def to_dict(self) -> dict:
        return {
            "epoch": self.epoch,
            "timestamp": self.timestamp,
            "data_raw_bytes": self.data_raw_bytes,
            "replay_bytes": self.replay_bytes,
            "replay_transient_bytes": self.replay_transient_bytes,
            "replay_measurement_ok": self.replay_measurement_ok,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GrowthSample":
        return cls(
            epoch=float(data["epoch"]),
            timestamp=str(data["timestamp"]),
            data_raw_bytes=int(data["data_raw_bytes"]),
            replay_bytes=int(data.get("replay_bytes", 0)),
            replay_transient_bytes=int(data.get("replay_transient_bytes", 0)),
            # Old persisted samples predate replay-aware measurement and may
            # not be used to prove a combined capacity projection.
            replay_measurement_ok=bool(data.get("replay_measurement_ok", False)),
        )


@dataclass(frozen=True)
class ReplayStorageScan:
    """One bounded, non-symlink-following classification of replay storage."""

    path: Path
    ok: bool
    status: MeasurementStatus
    error: Optional[str]
    measured_at: datetime
    duration_seconds: float
    categories: Dict[str, int]
    transient_counts: Dict[str, int]
    transient_oldest_age_seconds: Dict[str, Optional[float]]


def scan_replay_storage(
    path: Path,
    max_entries: int = 250_000,
    timeout_sec: float = 60.0,
) -> ReplayStorageScan:
    """Classify replay bytes in one bounded traversal without following links."""
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    names = ("replay_published", "replay_staging", "replay_backups", "replay_quarantine", "replay_metadata")
    categories = {name: 0 for name in names}
    counts = {name: 0 for name in ("staging", "backups", "quarantine")}
    oldest_mtime: Dict[str, Optional[float]] = {name: None for name in counts}
    path = Path(path)
    if max_entries < 1 or timeout_sec <= 0:
        return ReplayStorageScan(
            path,
            False,
            "error",
            "replay scan bounds must be positive",
            started_at,
            time.monotonic() - started,
            categories,
            counts,
            {name: None for name in counts},
        )
    if not path.exists():
        return ReplayStorageScan(
            path, False, "missing", f"directory does not exist: {path}", started_at,
            time.monotonic() - started, categories, counts,
            {name: None for name in counts},
        )
    try:
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError(f"replay root is not a safe directory: {path}")
        seen = 0
        stack = [path]
        while stack:
            if time.monotonic() - started > timeout_sec:
                raise TimeoutError(f"replay scan exceeded {timeout_sec}s")
            current = stack.pop()
            with os.scandir(current) as iterator:
                entries = sorted(iterator, key=lambda entry: entry.name)
            for entry in entries:
                if time.monotonic() - started > timeout_sec:
                    raise TimeoutError(f"replay scan exceeded {timeout_sec}s")
                seen += 1
                if seen > max_entries:
                    raise RuntimeError(
                        f"replay scan exceeded configured entry bound {max_entries}"
                    )
                if entry.is_symlink():
                    raise RuntimeError(f"symlink in replay tree: {entry.path}")
                relative_parts = Path(entry.path).relative_to(path).parts
                transient = None
                for part in relative_parts:
                    if part.startswith(".staging_"):
                        transient = "staging"
                    elif part.startswith(".backup_"):
                        transient = "backups"
                    elif part.startswith(".quarantine_"):
                        transient = "quarantine"
                if entry.is_dir(follow_symlinks=False):
                    if transient and Path(entry.path).name.startswith(f".{transient.rstrip('s')}_"):
                        counts[transient] += 1
                        mtime = entry.stat(follow_symlinks=False).st_mtime
                        previous = oldest_mtime[transient]
                        oldest_mtime[transient] = mtime if previous is None else min(previous, mtime)
                    stack.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise RuntimeError(f"non-regular replay entry: {entry.path}")
                allocated = entry.stat(follow_symlinks=False).st_blocks * 512
                if transient == "staging":
                    categories["replay_staging"] += allocated
                elif transient == "backups":
                    categories["replay_backups"] += allocated
                elif transient == "quarantine":
                    categories["replay_quarantine"] += allocated
                elif any(part.startswith("date=") for part in relative_parts):
                    categories["replay_published"] += allocated
                else:
                    categories["replay_metadata"] += allocated
        now_epoch = datetime.now(timezone.utc).timestamp()
        ages = {
            name: (None if mtime is None else max(0.0, now_epoch - mtime))
            for name, mtime in oldest_mtime.items()
        }
        return ReplayStorageScan(
            path, True, "ok", None, started_at, time.monotonic() - started,
            categories, counts, ages,
        )
    except Exception as exc:
        status: MeasurementStatus = (
            "timeout" if isinstance(exc, TimeoutError) else "error"
        )
        return ReplayStorageScan(
            path, False, status, f"{type(exc).__name__}: {exc}", started_at,
            time.monotonic() - started, categories, counts,
            {name: None for name in counts},
        )


# ============================================================================
# Standalone measurement functions (no shared state; easy to unit test)
# ============================================================================

def measure_directory(path: Path, timeout_sec: float) -> DirectoryMeasurement:
    """Measure a directory's on-disk size using `du -s -B1` (allocated bytes).

    Allocated bytes (rather than `du -sb` apparent/logical bytes) are used
    because retention decisions must reflect actual disk consumption; for
    ordinary non-sparse JSONL/Zstandard recorder files the two are close,
    but allocated bytes is the honest answer to "how much disk is this
    tree using".

    Never returns a numeric zero for a failed measurement. Distinguishes:
      - "ok": successful measurement (value may legitimately be 0 for an
        empty directory);
      - "missing": path does not exist;
      - "timeout": subprocess exceeded `timeout_sec` (the child process is
        terminated and reaped by `subprocess.run`'s own timeout handling);
      - "command_error": `du` exited non-zero;
      - "malformed_output": `du` produced output that could not be parsed;
      - "error": any other unexpected exception.
    """
    started_at = datetime.now(timezone.utc)
    start_perf = time.monotonic()

    if not path.exists():
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="missing",
            error=f"directory does not exist: {path}",
            measured_at=started_at,
            duration_seconds=time.monotonic() - start_perf,
        )

    try:
        result = subprocess.run(
            ["du", "-s", "-B1", str(path)],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        # subprocess.run() terminates and reaps the child before raising.
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="timeout",
            error=f"du timed out after {timeout_sec}s scanning {path}",
            measured_at=started_at,
            duration_seconds=time.monotonic() - start_perf,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="error",
            error=f"{type(exc).__name__}: {exc}",
            measured_at=started_at,
            duration_seconds=time.monotonic() - start_perf,
        )

    duration = time.monotonic() - start_perf

    if result.returncode != 0:
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="command_error",
            error=f"du exited {result.returncode}: {result.stderr.strip()[:500]}",
            measured_at=started_at,
            duration_seconds=duration,
        )

    stdout = result.stdout.strip()
    parts = stdout.split()
    if not parts:
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="malformed_output",
            error=f"du produced no parsable output for {path}",
            measured_at=started_at,
            duration_seconds=duration,
        )

    try:
        value_bytes = int(parts[0])
    except ValueError:
        return DirectoryMeasurement(
            path=path,
            value_bytes=None,
            ok=False,
            status="malformed_output",
            error=f"could not parse du output {stdout!r} for {path}",
            measured_at=started_at,
            duration_seconds=duration,
        )

    return DirectoryMeasurement(
        path=path,
        value_bytes=value_bytes,
        ok=True,
        status="ok",
        error=None,
        measured_at=started_at,
        duration_seconds=duration,
    )


def measure_filesystem(path: Path) -> FilesystemMeasurement:
    """Fast, independent filesystem-capacity measurement.

    Walks up to the nearest existing ancestor so this still works when
    `path` itself has not been created yet.
    """
    probe_path = path
    while not probe_path.exists():
        parent = probe_path.parent
        if parent == probe_path:
            break
        probe_path = parent

    usage = shutil.disk_usage(probe_path)
    try:
        device = str(os.stat(probe_path).st_dev)
    except OSError:
        device = "unknown"

    return FilesystemMeasurement(
        path=probe_path,
        device=device,
        total_bytes=usage.total,
        used_bytes=usage.used,
        free_bytes=usage.free,
        measured_at=datetime.now(timezone.utc),
    )


class DiskMonitor:
    """Monitor disk usage and growth with fail-safe measurement semantics."""

    _ROOT_NAMES: Tuple[str, ...] = (
        "data_raw", "replay_published", "replay_staging", "replay_backups",
        "replay_quarantine", "replay_metadata", "metadata", "state_reports",
    )

    def __init__(self, config):
        """
        Args:
            config: Config module with paths and disk limits
        """
        self.config = config
        self.data_root = config.DATA_ROOT
        self.meta_root = config.META_ROOT
        self.state_root = config.STATE_ROOT
        self.replay_root = Path(
            getattr(config, "REPLAY_ROOT", self.data_root.parent / "replay_store")
        )

        # Retention thresholds (GB). These apply to fresh `data_raw` usage
        # only — never to the cross-root observability total (`total_gb`,
        # which may span different filesystems) and never to filesystem
        # capacity.
        self.soft_limit_gb = config.DISK_SOFT_LIMIT_GB
        self.hard_limit_gb = config.DISK_HARD_LIMIT_GB
        self.cleanup_target_gb = config.DISK_CLEANUP_TARGET_GB

        self.scan_timeout_sec = float(getattr(config, "DISK_SCAN_TIMEOUT_SEC", 60.0))
        if self.scan_timeout_sec <= 0:
            raise ValueError(
                f"DISK_SCAN_TIMEOUT_SEC must be > 0, got {self.scan_timeout_sec}"
            )
        self.stale_after_sec = float(
            getattr(config, "DISK_MEASUREMENT_STALE_AFTER_SEC", 1800.0)
        )
        self.fs_free_warn_gb = float(getattr(config, "DISK_FS_FREE_WARN_GB", 100.0))
        self.fs_free_critical_gb = float(
            getattr(config, "DISK_FS_FREE_CRITICAL_GB", 50.0)
        )
        self.history_max_samples = int(
            getattr(config, "DISK_HISTORY_MAX_SAMPLES", 288)
        )
        self.history_max_age_sec = float(
            getattr(config, "DISK_HISTORY_MAX_AGE_SEC", 172800.0)
        )
        self.replay_scan_max_entries = int(
            getattr(config, "REPLAY_MONITOR_MAX_ENTRIES", 250_000)
        )
        self.replay_transient_warn_age_sec = float(
            getattr(config, "REPLAY_TRANSIENT_WARN_AGE_SEC", 86_400.0)
        )
        self.raw_retention_enabled = bool(
            getattr(config, "RAW_RETENTION_ENABLED", False)
        )
        self.raw_retention_days = int(getattr(config, "RAW_RETENTION_DAYS", 7))
        self.raw_retention_stable_age_sec = int(
            getattr(config, "RAW_RETENTION_STABLE_AGE_SEC", 3600)
        )

        # State directory
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.usage_log_file = self.state_root / "disk_usage.json"
        # Companion file: last-known-good measurements + bounded growth
        # history, persisted separately from the (overwritten-every-cycle)
        # usage report so restarts can recover stale-but-known values.
        self.monitor_state_file = self.state_root / "disk_monitor_state.json"

        self._roots: Dict[str, Path] = {
            "data_raw": self.data_root,
            "metadata": self.meta_root,
            "state_reports": self.state_root,
        }
        self._last_replay_scan: Optional[ReplayStorageScan] = None

        self._last_known_good: Dict[str, LastKnownGood] = {}
        self._growth_history: Deque[GrowthSample] = deque(maxlen=self.history_max_samples)
        self._scan_lock = asyncio.Lock()
        self._last_report: Optional[Dict] = None

        self._load_persisted_state()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _atomic_write_json(target: Path, payload: dict) -> None:
        """Write JSON atomically: temp file in the same directory + os.replace().

        Cleans up the temp file if anything fails before the replace.
        """
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                dir=str(target.parent),
                prefix=f".{target.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp_file:
                tmp_path = Path(tmp_file.name)
                json.dump(payload, tmp_file, indent=2)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, target)
        except Exception:
            if tmp_path is not None and tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise

    def _persist_state(self) -> None:
        payload = {
            "last_known_good": {
                name: lkg.to_dict() for name, lkg in self._last_known_good.items()
            },
            "growth_history": [sample.to_dict() for sample in self._growth_history],
        }
        try:
            self._atomic_write_json(self.monitor_state_file, payload)
        except Exception as exc:
            logger.error(f"Error persisting disk-monitor state: {exc}")

    def _load_persisted_state(self) -> None:
        if not self.monitor_state_file.exists():
            return
        try:
            raw = json.loads(self.monitor_state_file.read_text())
        except Exception as exc:
            logger.warning(f"Could not load persisted disk-monitor state: {exc}")
            return

        for name, entry in raw.get("last_known_good", {}).items():
            try:
                self._last_known_good[name] = LastKnownGood.from_dict(entry)
            except Exception:
                continue

        for entry in raw.get("growth_history", []):
            try:
                self._growth_history.append(GrowthSample.from_dict(entry))
            except Exception:
                continue

        self._prune_growth_history(datetime.now(timezone.utc))

    # ------------------------------------------------------------------
    # Measurement orchestration
    # ------------------------------------------------------------------

    async def _measure_all_roots(self) -> Dict[str, DirectoryMeasurement]:
        """Measure every monitored root sequentially (never concurrently
        against the same disk)."""
        loop = asyncio.get_event_loop()
        measurements: Dict[str, DirectoryMeasurement] = {}
        for name, path in self._roots.items():
            measurements[name] = await loop.run_in_executor(
                None, measure_directory, path, self.scan_timeout_sec
            )
        replay_scan = await loop.run_in_executor(
            None,
            scan_replay_storage,
            self.replay_root,
            self.replay_scan_max_entries,
            self.scan_timeout_sec,
        )
        self._last_replay_scan = replay_scan
        for name, value in replay_scan.categories.items():
            measurements[name] = DirectoryMeasurement(
                path=self.replay_root,
                value_bytes=value if replay_scan.ok else None,
                ok=replay_scan.ok,
                status=replay_scan.status,
                error=replay_scan.error,
                measured_at=replay_scan.measured_at,
                duration_seconds=replay_scan.duration_seconds,
            )
        return measurements

    def _resolve_component(
        self,
        name: str,
        measurement: DirectoryMeasurement,
        now: datetime,
        alerts: List[str],
    ) -> Tuple[dict, Optional[int], bool]:
        """Turn one DirectoryMeasurement into a report entry.

        Returns (entry_dict, value_bytes_or_none, fresh_ok).
        `fresh_ok` is True only when this cycle's measurement itself
        succeeded (never true for a last-known-good fallback, even if the
        fallback is within the staleness window).
        """
        if measurement.ok:
            self._last_known_good[name] = LastKnownGood(
                value_bytes=measurement.value_bytes,
                measured_at=measurement.measured_at.isoformat(),
                duration_seconds=measurement.duration_seconds,
            )
            entry = {
                "value_gb": round(measurement.value_bytes / BYTES_PER_GB, 2),
                "measurement_ok": True,
                "measurement_status": "ok",
                "measurement_error": None,
                "measurement_timestamp": measurement.measured_at.isoformat(),
                "measurement_age_seconds": 0.0,
                "stale": False,
            }
            return entry, measurement.value_bytes, True

        alerts.append(
            f"ERROR: {name} measurement failed (status={measurement.status}): "
            f"{measurement.error}"
        )

        lkg = self._last_known_good.get(name)
        if lkg is None:
            entry = {
                "value_gb": None,
                "measurement_ok": False,
                "measurement_status": measurement.status,
                "measurement_error": measurement.error,
                "measurement_timestamp": None,
                "measurement_age_seconds": None,
                "stale": False,
            }
            return entry, None, False

        try:
            lkg_measured_at = datetime.fromisoformat(lkg.measured_at)
        except ValueError:
            lkg_measured_at = now
        age_seconds = max(0.0, (now - lkg_measured_at).total_seconds())

        if age_seconds > self.stale_after_sec:
            alerts.append(
                f"WARNING: {name} last-known-good value is stale "
                f"({age_seconds:.0f}s > {self.stale_after_sec:.0f}s limit)"
            )

        entry = {
            "value_gb": round(lkg.value_bytes / BYTES_PER_GB, 2),
            "measurement_ok": False,
            "measurement_status": measurement.status,
            "measurement_error": measurement.error,
            "measurement_timestamp": lkg.measured_at,
            "measurement_age_seconds": round(age_seconds, 1),
            "stale": True,
        }
        return entry, lkg.value_bytes, False

    def _record_growth_sample(
        self,
        now: datetime,
        data_raw_bytes: int,
        replay_bytes: int = 0,
        replay_transient_bytes: int = 0,
        replay_measurement_ok: bool = True,
    ) -> None:
        epoch = now.timestamp()
        if self._growth_history and epoch <= self._growth_history[-1].epoch:
            logger.warning(
                "Skipping growth-history sample with non-increasing timestamp"
            )
            return
        self._growth_history.append(
            GrowthSample(
                epoch=epoch,
                timestamp=now.isoformat(),
                data_raw_bytes=data_raw_bytes,
                replay_bytes=replay_bytes,
                replay_transient_bytes=replay_transient_bytes,
                replay_measurement_ok=replay_measurement_ok,
            )
        )
        self._prune_growth_history(now)

    def _prune_growth_history(self, now: datetime) -> None:
        cutoff = now.timestamp() - self.history_max_age_sec
        while self._growth_history and self._growth_history[0].epoch < cutoff:
            self._growth_history.popleft()

    def _compute_growth(
        self,
    ) -> Optional[Tuple[float, Optional[float], float, str, str]]:
        """Returns (growth_rate_gb_day, days_to_full, sample_interval_sec,
        oldest_timestamp, newest_timestamp) or None if there is insufficient
        valid evidence."""
        if len(self._growth_history) < 2:
            return None

        oldest = self._growth_history[0]
        newest = self._growth_history[-1]
        elapsed_sec = newest.epoch - oldest.epoch
        if elapsed_sec < MIN_GROWTH_SPAN_SEC:
            return None

        delta_gb = (newest.data_raw_bytes - oldest.data_raw_bytes) / BYTES_PER_GB
        if delta_gb < 0:
            # A real decrease (e.g. cleanup ran) — never report negative
            # growth. Only successful, non-stale samples ever reach this
            # history, so this cannot be a fake-zero artifact.
            delta_gb = 0.0

        growth_rate_gb_day = delta_gb / elapsed_sec * 86400.0

        if growth_rate_gb_day > 0:
            current_gb = newest.data_raw_bytes / BYTES_PER_GB
            available_gb = self.hard_limit_gb - current_gb
            days_to_full = available_gb / growth_rate_gb_day if available_gb > 0 else 0.0
        else:
            days_to_full = None

        return (
            growth_rate_gb_day,
            days_to_full,
            elapsed_sec,
            oldest.timestamp,
            newest.timestamp,
        )

    def _compute_capacity_growth(self) -> Optional[dict]:
        if len(self._growth_history) < 2:
            return None
        oldest, newest = self._growth_history[0], self._growth_history[-1]
        if not oldest.replay_measurement_ok or not newest.replay_measurement_ok:
            return None
        elapsed = newest.epoch - oldest.epoch
        if elapsed < MIN_GROWTH_SPAN_SEC:
            return None
        day_factor = 86400.0 / elapsed
        raw_delta = max(0, newest.data_raw_bytes - oldest.data_raw_bytes)
        replay_delta = max(0, newest.replay_bytes - oldest.replay_bytes)
        return {
            "sample_interval_seconds": elapsed,
            "raw_growth_bytes_per_day": int(raw_delta * day_factor),
            "replay_growth_bytes_per_day": int(replay_delta * day_factor),
            "combined_growth_bytes_per_day": int((raw_delta + replay_delta) * day_factor),
            "current_transient_pressure_bytes": newest.replay_transient_bytes,
        }

    async def check_disk_usage(self) -> Dict:
        """
        Check total disk usage.

        Guards against overlapping scans: if a check is already running, this
        call is skipped (not queued) and the previous report is returned with
        `skipped_duplicate=True`.

        Returns:
            Dict with usage breakdown, measurement health, and alerts.
        """
        if self._scan_lock.locked():
            logger.warning(
                "Disk check already in progress; skipping this overlapping run"
            )
            if self._last_report is not None:
                skipped = dict(self._last_report)
                skipped["skipped_duplicate"] = True
                # A skipped duplicate is never a fresh measurement, even if
                # the previous cycle's report was trustworthy — force the
                # flag closed so callers (in particular cleanup_old_data())
                # cannot treat a stale duplicate as authorization to act.
                skipped["retention_measurement_trustworthy"] = False
                skipped_alerts = list(skipped.get("alerts", []))
                skipped_alerts.append(
                    "WARNING: disk check skipped (overlapping scan in progress); "
                    "reporting the previous cycle's report, not a fresh measurement"
                )
                skipped["alerts"] = skipped_alerts
                if skipped.get("monitoring_health") == "healthy":
                    skipped["monitoring_health"] = "degraded"
                return skipped
            return {
                "timestamp": local_now_iso(),
                "skipped_duplicate": True,
                "retention_measurement_trustworthy": False,
                "monitoring_health": "unhealthy",
                "alerts": [
                    "ERROR: disk check skipped (overlapping scan in progress) "
                    "with no prior report available"
                ],
            }

        async with self._scan_lock:
            return await self._check_disk_usage_locked()

    async def _check_disk_usage_locked(self) -> Dict:
        now = datetime.now(timezone.utc)
        measurements = await self._measure_all_roots()

        alerts: List[str] = []
        components: Dict[str, dict] = {}
        resolved_values: Dict[str, Optional[int]] = {}
        # Combined size across all monitored roots. This is an OBSERVABILITY
        # aggregate only: raw, published/transient replay, metadata, and
        # reports may live on different filesystems, so this sum must never
        # drive retention
        # threshold decisions or percent-of-limit reporting — see
        # data_raw_bytes/data_raw_trustworthy below for that.
        total_bytes = 0
        total_known = True
        all_fresh_ok = True
        unhealthy = False
        data_raw_trustworthy = False
        data_raw_bytes: Optional[int] = None

        for name, measurement in measurements.items():
            entry, value_bytes, fresh_ok = self._resolve_component(
                name, measurement, now, alerts
            )
            components[name] = entry
            resolved_values[name] = value_bytes
            if value_bytes is None:
                total_known = False
            else:
                total_bytes += value_bytes
            if not fresh_ok:
                all_fresh_ok = False
            if name == "data_raw":
                data_raw_trustworthy = fresh_ok
                data_raw_bytes = value_bytes
                if not fresh_ok:
                    unhealthy = True
            elif name.startswith("replay_") and not fresh_ok:
                unhealthy = True

        total_gb = round(total_bytes / BYTES_PER_GB, 2) if total_known else None
        total_stale = total_known and not all_fresh_ok

        fs_measurement = measure_filesystem(self.data_root)
        fs_dict = fs_measurement.to_dict()
        fs_free_gb = fs_dict["filesystem_free_gb"]
        if fs_free_gb < self.fs_free_critical_gb:
            alerts.append(
                f"CRITICAL: filesystem free space {fs_free_gb}GB < "
                f"{self.fs_free_critical_gb}GB critical threshold"
            )
            unhealthy = True
        elif fs_free_gb < self.fs_free_warn_gb:
            alerts.append(
                f"WARNING: filesystem free space {fs_free_gb}GB < "
                f"{self.fs_free_warn_gb}GB warn threshold"
            )

        # Retention soft/hard/cleanup thresholds apply to fresh `data_raw`
        # usage only, never to the cross-root `total_gb` observability sum
        # (which may span different filesystems) and never to a stale
        # last-known-good fallback. A failed or stale data_raw measurement
        # this cycle means these fields are null, not a current-looking
        # estimate silently computed from old data.
        data_raw_gb_for_retention = (
            round(data_raw_bytes / BYTES_PER_GB, 2)
            if data_raw_trustworthy and data_raw_bytes is not None
            else None
        )
        percent_of_soft_limit = None
        percent_of_hard_limit = None
        if data_raw_gb_for_retention is not None:
            percent_of_soft_limit = round(
                data_raw_gb_for_retention / self.soft_limit_gb * 100, 1
            )
            percent_of_hard_limit = round(
                data_raw_gb_for_retention / self.hard_limit_gb * 100, 1
            )
            if data_raw_gb_for_retention >= self.hard_limit_gb:
                alerts.append(
                    f"CRITICAL: data_raw retention usage {data_raw_gb_for_retention}GB >= "
                    f"{self.hard_limit_gb}GB hard limit"
                )
                unhealthy = True
                logger.critical(
                    f"DISK CRITICAL: {data_raw_gb_for_retention}GB >= "
                    f"{self.hard_limit_gb}GB hard limit!"
                )
            elif data_raw_gb_for_retention >= self.soft_limit_gb:
                alerts.append(
                    f"WARNING: data_raw retention usage {data_raw_gb_for_retention}GB >= "
                    f"{self.soft_limit_gb}GB soft limit"
                )
                logger.warning(
                    f"DISK WARNING: {data_raw_gb_for_retention}GB >= "
                    f"{self.soft_limit_gb}GB soft limit"
                )

        # Growth history tracks data_raw only (the quantity the hard limit
        # actually governs) and only ever records a sample when this cycle's
        # data_raw measurement was itself fresh and successful.
        replay_component_names = (
            "replay_published", "replay_staging", "replay_backups",
            "replay_quarantine", "replay_metadata",
        )
        replay_values = [resolved_values.get(name) for name in replay_component_names]
        replay_trustworthy = all(
            components.get(name, {}).get("measurement_ok") is True
            for name in replay_component_names
        )
        replay_published_bytes = resolved_values.get("replay_published")
        replay_transient_bytes = sum(
            int(resolved_values.get(name) or 0)
            for name in ("replay_staging", "replay_backups", "replay_quarantine")
        )
        if data_raw_trustworthy and data_raw_bytes is not None:
            self._record_growth_sample(
                now,
                data_raw_bytes,
                int(replay_published_bytes or 0),
                replay_transient_bytes,
                replay_measurement_ok=replay_trustworthy,
            )

        # Never report a current-looking growth rate / days-to-full derived
        # from a stale or failed current-cycle data_raw measurement, even if
        # the historical sample window itself remains valid.
        growth = self._compute_growth() if data_raw_trustworthy else None
        if growth is not None:
            growth_rate_gb_day, days_to_full, sample_interval_sec, oldest_ts, newest_ts = growth
            if days_to_full is not None and days_to_full < 7:
                alerts.append(f"WARNING: projected full in {days_to_full:.1f} days")
        else:
            growth_rate_gb_day = None
            days_to_full = None
            sample_interval_sec = None
            oldest_ts = None
            newest_ts = None

        replay_artifacts = {
            "counts": {"staging": 0, "backups": 0, "quarantine": 0},
            "oldest_age_seconds": {"staging": None, "backups": None, "quarantine": None},
            "measurement_ok": replay_trustworthy,
        }
        if self._last_replay_scan is not None:
            replay_artifacts["counts"] = self._last_replay_scan.transient_counts
            replay_artifacts["oldest_age_seconds"] = (
                self._last_replay_scan.transient_oldest_age_seconds
            )
            if self._last_replay_scan.ok:
                for name in ("staging", "backups"):
                    age = self._last_replay_scan.transient_oldest_age_seconds.get(name)
                    if age is not None and age > self.replay_transient_warn_age_sec:
                        alerts.append(
                            f"WARNING: replay {name} oldest age {age:.0f}s exceeds "
                            f"{self.replay_transient_warn_age_sec:.0f}s"
                        )

        # Group logical roots by the actual filesystem device. Free capacity
        # is measured once per device and never summed across roots.
        root_components = {
            "data_raw": (self.data_root, ["data_raw"]),
            "replay": (self.replay_root, list(replay_component_names)),
            "metadata": (self.meta_root, ["metadata"]),
            "state_reports": (self.state_root, ["state_reports"]),
        }
        filesystem_groups: Dict[str, dict] = {}
        for root_name, (root_path, component_names) in root_components.items():
            measurement = measure_filesystem(root_path)
            group = filesystem_groups.setdefault(
                measurement.device,
                {
                    **measurement.to_dict(),
                    "roots": [],
                    "combined_allocated_bytes": 0,
                    "combined_measurement_complete": True,
                },
            )
            group["roots"].append(root_name)
            values = [resolved_values.get(name) for name in component_names]
            if any(value is None for value in values):
                group["combined_measurement_complete"] = False
                group["combined_allocated_bytes"] = None
            elif group["combined_allocated_bytes"] is not None:
                group["combined_allocated_bytes"] += sum(int(value) for value in values)
        filesystem_capacity = sorted(filesystem_groups.values(), key=lambda group: group["filesystem_device_or_identity"])

        if unhealthy:
            monitoring_health = "unhealthy"
        elif not all_fresh_ok or total_stale:
            monitoring_health = "degraded"
        else:
            monitoring_health = "healthy"

        report = {
            # Operator-facing top-level report timestamp: always the
            # configured local report timezone (Europe/Budapest), matching
            # every other top-level report timestamp (heartbeat, startup
            # coverage, and the skipped/overlap path below) — see
            # docs/OPERATIONS.md "State File Schemas". `now` (UTC) remains
            # the basis for every internal/derived calculation below
            # (measured_at, growth-history epoch ordering, measurement age,
            # staleness) and must not be replaced by this local timestamp.
            "timestamp": local_now_iso(),
            "components": components,
            # Backward-compatible top-level fields. `None` (never `0`) when
            # the underlying measurement and every fallback are unavailable.
            "data_raw_gb": components["data_raw"]["value_gb"],
            "replay_published_gb": components["replay_published"]["value_gb"],
            "replay_staging_gb": components["replay_staging"]["value_gb"],
            "replay_backups_gb": components["replay_backups"]["value_gb"],
            "replay_quarantine_gb": components["replay_quarantine"]["value_gb"],
            "replay_metadata_gb": components["replay_metadata"]["value_gb"],
            "meta_gb": components["metadata"]["value_gb"],
            "state_gb": components["state_reports"]["value_gb"],
            # Combined observability total across all roots (may span
            # different filesystems) — NOT the retention-limit basis.
            "total_gb": total_gb,
            "total_stale": total_stale,
            "percent_of_soft_limit": percent_of_soft_limit,
            "percent_of_hard_limit": percent_of_hard_limit,
            "filesystem": fs_dict,
            "filesystem_capacity": filesystem_capacity,
            "replay_artifacts": replay_artifacts,
            "capacity_projection": (
                self._compute_capacity_growth() if replay_trustworthy else None
            ),
            "growth_rate_gb_day": round(growth_rate_gb_day, 2) if growth_rate_gb_day is not None else None,
            "days_to_full": round(days_to_full, 1) if days_to_full is not None else None,
            "growth_sample_interval_sec": sample_interval_sec,
            "growth_sample_oldest_timestamp": oldest_ts,
            "growth_sample_newest_timestamp": newest_ts,
            "monitoring_health": monitoring_health,
            "alerts": alerts,
            "retention_measurement_trustworthy": data_raw_trustworthy,
            "raw_retention_enabled": self.raw_retention_enabled,
            "cleanup_required": bool(
                data_raw_gb_for_retention is not None
                and data_raw_gb_for_retention > self.soft_limit_gb
            ),
            "skipped_duplicate": False,
        }

        self._last_report = report
        self._persist_state()
        return report

    async def write_usage_report(self, usage: Dict) -> None:
        """Write usage report to disk atomically for monitoring."""
        try:
            self._atomic_write_json(self.usage_log_file, usage)
            logger.debug(
                f"Disk usage report written: monitoring_health="
                f"{usage.get('monitoring_health')}, total_gb={usage.get('total_gb')}"
            )
        except Exception as e:
            logger.error(f"Error writing usage report: {e}")

    # ------------------------------------------------------------------
    # Cleanup (fails closed on any untrusted retention measurement)
    # ------------------------------------------------------------------

    @staticmethod
    def _is_retention_measurement_trustworthy(usage: Dict) -> bool:
        """Fail-closed: absence of the flag means "not trustworthy"."""
        return bool(usage.get("retention_measurement_trustworthy", False))

    def get_dir_size_gb(self, path: Path) -> Optional[float]:
        """Best-effort single-directory size, used only for cleanup logging.

        Returns None (never 0) when the directory cannot be measured.
        """
        measurement = measure_directory(path, self.scan_timeout_sec)
        if not measurement.ok:
            logger.warning(
                f"Could not measure {path} for cleanup logging "
                f"(status={measurement.status}): {measurement.error}"
            )
            return None
        return measurement.value_bytes / BYTES_PER_GB

    @staticmethod
    def _raw_directory_proof(
        path: Path, *, stable_before_epoch: float | None = None
    ) -> tuple[bool, list[str], list[Path]]:
        reasons: list[str] = []
        files: list[Path] = []
        if not path.exists() or not path.is_dir() or path.is_symlink():
            return False, [f"missing or unsafe directory: {path}"], files
        stems: Dict[str, set[str]] = {}
        for entry in sorted(path.iterdir()):
            if entry.is_symlink() or not entry.is_file():
                reasons.append(f"unexpected non-regular entry: {entry}")
                continue
            stat_result = entry.stat()
            if stat_result.st_size <= 0:
                reasons.append(f"empty raw file: {entry}")
            if (
                stable_before_epoch is not None
                and stat_result.st_mtime > stable_before_epoch
            ):
                reasons.append(
                    f"raw file is not stable for {path}: {entry.name}"
                )
            name = entry.name
            if name.endswith(".jsonl.zst"):
                stem, variant = name[:-4], "zst"
            elif name.endswith(".jsonl.gz"):
                stem, variant = name[:-3], "gz"
            elif name.endswith(".jsonl"):
                stem, variant = name, "plain"
            else:
                reasons.append(f"unexpected raw filename: {entry}")
                continue
            stems.setdefault(stem, set()).add(variant)
            files.append(entry)
        for stem, variants in stems.items():
            if len(variants) > 1:
                reasons.append(
                    f"ambiguous compressed/uncompressed variants for {stem}: {sorted(variants)}"
                )
        if not files:
            reasons.append(f"no raw data files: {path}")
        return not reasons, reasons, files

    def plan_raw_retention(self, *, max_units: int = 10_000) -> dict:
        """Return a bounded, exact depth+trade retention proof plan.

        This is deliberately non-destructive.  The paired transactional move,
        journal, rollback, and recovery mechanism is not implemented, so even
        fully proven units remain ``cleanup_required`` rather than being moved
        or deleted.
        """
        from pipeline.raw_manifest import compute_raw_source_identity
        from pipeline.replay_lifecycle import acquire_replay_build_lock

        if max_units < 1:
            raise ValueError("max_units must be positive")
        report_root = Path(
            getattr(self.config, "DAILY_REPORT_ROOT", self.state_root / "daily_build_reports")
        )
        units: list[dict] = []
        today = datetime.now(timezone.utc).date()
        cutoff = today - timedelta(days=self.raw_retention_days)
        stable_before_epoch = time.time() - self.raw_retention_stable_age_sec
        with acquire_replay_build_lock(
            replay_root=self.replay_root,
            data_root=self.data_root,
            report_root=report_root,
            command=["disk_monitor", "raw-retention-dry-run"],
        ) as lifecycle:
            keys: set[tuple[str, str, str]] = set()
            for venue_dir in sorted(self.data_root.iterdir()) if self.data_root.exists() else []:
                if venue_dir.is_symlink() or not venue_dir.is_dir():
                    raise RuntimeError(f"unsafe raw venue entry: {venue_dir}")
                for channel in ("depth_v2", "trade_v2"):
                    channel_dir = venue_dir / channel
                    if not channel_dir.exists():
                        continue
                    if channel_dir.is_symlink() or not channel_dir.is_dir():
                        raise RuntimeError(f"unsafe raw channel entry: {channel_dir}")
                    for symbol_dir in sorted(channel_dir.iterdir()):
                        if symbol_dir.is_symlink() or not symbol_dir.is_dir():
                            raise RuntimeError(f"unsafe raw symbol entry: {symbol_dir}")
                        for date_dir in sorted(symbol_dir.iterdir()):
                            try:
                                date_type.fromisoformat(date_dir.name)
                            except ValueError:
                                raise RuntimeError(f"unexpected raw date entry: {date_dir}")
                            keys.add((venue_dir.name, symbol_dir.name, date_dir.name))
                            if len(keys) > max_units:
                                raise RuntimeError(
                                    f"raw retention scan exceeded {max_units} units"
                                )

            for venue, symbol, date_str in sorted(keys):
                raw_date = date_type.fromisoformat(date_str)
                depth_dir = self.data_root / venue / "depth_v2" / symbol / date_str
                trade_dir = self.data_root / venue / "trade_v2" / symbol / date_str
                depth_ok, depth_reasons, _depth_files = self._raw_directory_proof(
                    depth_dir, stable_before_epoch=stable_before_epoch
                )
                trade_ok, trade_reasons, _trade_files = self._raw_directory_proof(
                    trade_dir, stable_before_epoch=stable_before_epoch
                )
                reasons = list(depth_reasons) + list(trade_reasons)
                if raw_date >= cutoff:
                    reasons.append(f"inside retention grace period ending before {cutoff}")
                if raw_date == today:
                    reasons.append("recorder current/open UTC date")

                dependencies = [
                    (raw_date - timedelta(days=1)).isoformat(),
                    raw_date.isoformat(),
                    (raw_date + timedelta(days=1)).isoformat(),
                ]
                current_identity = None
                if depth_ok and trade_ok:
                    try:
                        current_identity = compute_raw_source_identity(
                            venue,
                            symbol,
                            date_str,
                            ["depth_v2", "trade_v2"],
                            self.data_root,
                            strict=True,
                        )
                    except Exception as exc:
                        reasons.append(f"raw source identity failed: {exc}")
                dependency_results: list[dict] = []
                for target_date in dependencies:
                    partition = (
                        self.replay_root / f"venue={venue}" / f"symbol={symbol}"
                        / f"date={target_date}"
                    )
                    routine = validate_partition(partition)
                    deep_problems = audit_partition_deep(partition) if routine else ["routine validation failed"]
                    identity_match = False
                    schema_ok = False
                    if routine:
                        try:
                            manifest = json.loads((partition / "manifest.json").read_text())
                            schema_ok = manifest.get("schema_version") == 2
                            manifest_entries = manifest.get("source_identity", {}).get("channels", {})
                            required_entries = []
                            if current_identity is not None:
                                required_entries.extend(current_identity["channels"]["depth_v2"])
                                if target_date == date_str:
                                    required_entries.extend(current_identity["channels"]["trade_v2"])
                            available = {
                                (entry.get("path"), entry.get("sha256"), entry.get("size_bytes"))
                                for entries in manifest_entries.values()
                                for entry in entries
                            }
                            identity_match = all(
                                (entry["path"], entry["sha256"], entry["size_bytes"]) in available
                                for entry in required_entries
                            )
                        except Exception as exc:
                            reasons.append(f"manifest identity check failed for {partition}: {exc}")
                    if not routine:
                        reasons.append(f"required adjacent replay is not routine-valid: {partition}")
                    elif deep_problems:
                        reasons.append(f"required adjacent replay deep integrity failed: {partition}")
                    elif not schema_ok:
                        reasons.append(f"required adjacent replay is not schema 2: {partition}")
                    elif not identity_match:
                        reasons.append(f"required adjacent replay source identity mismatch: {partition}")
                    dependency_results.append(
                        {
                            "date": target_date,
                            "partition": str(partition),
                            "routine_valid": routine,
                            "deep_integrity_problems": deep_problems,
                            "schema_version_2": schema_ok,
                            "source_identity_match": identity_match,
                        }
                    )
                units.append(
                    {
                        "venue": venue,
                        "symbol": symbol,
                        "source_date": date_str,
                        "depth_directory": str(depth_dir),
                        "trade_directory": str(trade_dir),
                        "replay_dependencies": dependency_results,
                        "proof_passed": not reasons,
                        "eligible_for_transactional_retirement": False,
                        "outcome": "cleanup_required" if not reasons else "blocked",
                        "reasons": reasons or [
                            "paired transactional journal/move/rollback is not implemented; no mutation performed"
                        ],
                    }
                )
            return {
                "contract_version": 1,
                "run_id": lifecycle.run_id,
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "retention_enabled_configuration": self.raw_retention_enabled,
                "mutation_performed": False,
                "units": units,
            }

    async def cleanup_old_data(self) -> bool:
        """Never delete raw data; surface cleanup-required state fail closed."""
        usage = await self.check_disk_usage()
        if usage.get("skipped_duplicate") or not self._is_retention_measurement_trustworthy(usage):
            logger.error("Raw retention refused: current measurement is not trustworthy")
            return False
        raw_gb = usage.get("data_raw_gb")
        if raw_gb is None or raw_gb <= self.soft_limit_gb:
            return False
        if not self.raw_retention_enabled:
            logger.error(
                "Raw cleanup required but automatic retention is disabled; no files were moved or deleted"
            )
            return False
        logger.error(
            "Raw cleanup required, but transactional paired depth/trade retirement is not implemented; "
            "no files were moved or deleted. Use plan_raw_retention() for proof-only evidence."
        )
        return False

    async def disk_check_task(self) -> None:
        """Background task for periodic disk checks."""
        from config import DISK_CHECK_INTERVAL_SEC

        logger.info("Disk monitor starting...")

        while True:
            try:
                usage = await self.check_disk_usage()
                await self.write_usage_report(usage)

                # Cleanup raw data only when raw storage exceeds the limit
                # AND the measurement backing that decision is trustworthy
                # (cleanup_old_data() re-verifies and fails closed itself).
                raw_gb = usage.get("data_raw_gb")
                if raw_gb is not None and raw_gb > self.soft_limit_gb:
                    await self.cleanup_old_data()

            except Exception as e:
                logger.error(f"Error in disk monitor: {e}", exc_info=True)

            await asyncio.sleep(DISK_CHECK_INTERVAL_SEC)

    async def shutdown(self) -> None:
        """Shutdown disk monitor and save final state."""
        logger.info("Disk monitor shutting down...")
        try:
            # Final usage report
            usage = await self.check_disk_usage()
            await self.write_usage_report(usage)
            logger.info(
                f"Final disk usage: total_gb={usage.get('total_gb')}, "
                f"monitoring_health={usage.get('monitoring_health')}"
            )
        except Exception as e:
            logger.error(f"Error during disk monitor shutdown: {e}")
        logger.info("Disk monitor shutdown complete")
