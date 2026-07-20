#!/usr/bin/env python
"""
Disk usage monitoring and estimation module.

Tracks:
  - Total size of data_raw/
  - Total size of catalog/ (Nautilus Parquet)
  - Total size of meta/ and state/
  - Filesystem-level capacity (independent of recursive directory sizing)
  - Growth rate and days-to-full, computed from real sample timestamps
  - Automatic raw-data cleanup, gated on a trustworthy retention measurement

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
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, List, Literal, Optional, Tuple

from time_utils import local_now_iso

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

    def to_dict(self) -> dict:
        return {
            "epoch": self.epoch,
            "timestamp": self.timestamp,
            "data_raw_bytes": self.data_raw_bytes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GrowthSample":
        return cls(
            epoch=float(data["epoch"]),
            timestamp=str(data["timestamp"]),
            data_raw_bytes=int(data["data_raw_bytes"]),
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

    _ROOT_NAMES: Tuple[str, ...] = ("data_raw", "catalog", "meta", "state")

    def __init__(self, config):
        """
        Args:
            config: Config module with paths and disk limits
        """
        self.config = config
        self.data_root = config.DATA_ROOT
        self.meta_root = config.META_ROOT
        self.state_root = config.STATE_ROOT
        self.catalog_root = config.NAUTILUS_CATALOG_ROOT

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

        # State directory
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.usage_log_file = self.state_root / "disk_usage.json"
        # Companion file: last-known-good measurements + bounded growth
        # history, persisted separately from the (overwritten-every-cycle)
        # usage report so restarts can recover stale-but-known values.
        self.monitor_state_file = self.state_root / "disk_monitor_state.json"

        self._roots: Dict[str, Path] = {
            "data_raw": self.data_root,
            "catalog": self.catalog_root,
            "meta": self.meta_root,
            "state": self.state_root,
        }

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

    def _record_growth_sample(self, now: datetime, data_raw_bytes: int) -> None:
        epoch = now.timestamp()
        if self._growth_history and epoch <= self._growth_history[-1].epoch:
            logger.warning(
                "Skipping growth-history sample with non-increasing timestamp"
            )
            return
        self._growth_history.append(
            GrowthSample(epoch=epoch, timestamp=now.isoformat(), data_raw_bytes=data_raw_bytes)
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
        # Combined size across all monitored roots. This is an OBSERVABILITY
        # aggregate only: data_raw, catalog, meta, and state may live on
        # different filesystems, so this sum must never drive retention
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
        if data_raw_trustworthy and data_raw_bytes is not None:
            self._record_growth_sample(now, data_raw_bytes)

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

        if unhealthy:
            monitoring_health = "unhealthy"
        elif not all_fresh_ok or total_stale:
            monitoring_health = "degraded"
        else:
            monitoring_health = "healthy"

        report = {
            "timestamp": now.isoformat(),
            "components": components,
            # Backward-compatible top-level fields. `None` (never `0`) when
            # the underlying measurement and every fallback are unavailable.
            "data_raw_gb": components["data_raw"]["value_gb"],
            "catalog_gb": components["catalog"]["value_gb"],
            "meta_gb": components["meta"]["value_gb"],
            "state_gb": components["state"]["value_gb"],
            # Combined observability total across all roots (may span
            # different filesystems) — NOT the retention-limit basis.
            "total_gb": total_gb,
            "total_stale": total_stale,
            "percent_of_soft_limit": percent_of_soft_limit,
            "percent_of_hard_limit": percent_of_hard_limit,
            "filesystem": fs_dict,
            "growth_rate_gb_day": round(growth_rate_gb_day, 2) if growth_rate_gb_day is not None else None,
            "days_to_full": round(days_to_full, 1) if days_to_full is not None else None,
            "growth_sample_interval_sec": sample_interval_sec,
            "growth_sample_oldest_timestamp": oldest_ts,
            "growth_sample_newest_timestamp": newest_ts,
            "monitoring_health": monitoring_health,
            "alerts": alerts,
            "retention_measurement_trustworthy": data_raw_trustworthy,
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

    async def get_oldest_date_dir(self) -> Optional[Path]:
        """Get oldest date directory in data_raw/.

        Returns the actual date directory, e.g.:
            data_raw/BINANCE_SPOT/depth/BTCUSDT/2026-04-15/
        Only targets directories whose name looks like YYYY-MM-DD.
        Never returns venue, channel, or symbol directories.
        """
        try:
            date_dirs = []
            
            for venue_dir in self.data_root.glob("*/"):
                if not venue_dir.is_dir():
                    continue
                for channel_dir in venue_dir.glob("*/"):
                    if not channel_dir.is_dir():
                        continue
                    for symbol_dir in channel_dir.glob("*/"):
                        if not symbol_dir.is_dir():
                            continue
                        for d in symbol_dir.iterdir():
                            if (d.is_dir()
                                    and len(d.name) == 10
                                    and d.name[4] == '-'
                                    and d.name[7] == '-'):
                                date_dirs.append(d)
            
            if not date_dirs:
                return None
            
            # Return the single oldest date directory
            return min(date_dirs, key=lambda x: x.name)
        except Exception as e:
            logger.warning(f"Could not find oldest date dir: {e}")
            return None

    async def cleanup_old_data(self) -> bool:
        """
        Delete oldest data directories if raw retention usage > soft limit.

        Fails closed: never runs (or continues) unless the current cycle's
        `data_raw` measurement is fresh and successful
        (`retention_measurement_trustworthy=True`). A missing, failed,
        timed-out, or merely stale (last-known-good) measurement is never
        treated as "below threshold" — cleanup is skipped and an ERROR is
        logged instead.

        Returns:
            True if cleanup performed, False otherwise
        """
        usage = await self.check_disk_usage()

        if usage.get("skipped_duplicate"):
            logger.error(
                "Cleanup skipped: this disk check was skipped due to an "
                "overlapping scan already in progress; refusing to act on a "
                "duplicate report instead of a fresh measurement"
            )
            return False

        if not self._is_retention_measurement_trustworthy(usage):
            status = (
                usage.get("components", {})
                .get("data_raw", {})
                .get("measurement_status", "unknown")
            )
            logger.error(
                "Cleanup skipped: data_raw retention measurement is unavailable, "
                f"stale, or unknown (status={status}); refusing to delete "
                "production data based on an untrusted measurement"
            )
            return False

        raw_gb = usage.get("data_raw_gb")
        if raw_gb is None or raw_gb <= self.soft_limit_gb:
            logger.debug("Disk usage within limits, no cleanup needed")
            return False

        logger.info(
            f"Raw data usage {raw_gb}GB > soft limit {self.soft_limit_gb}GB "
            f"(combined observability total: {usage.get('total_gb')}GB), "
            "cleaning up oldest raw data..."
        )

        # Delete oldest date directories until we hit cleanup target
        deleted_count = 0
        max_attempts = 10

        while (
            usage.get("data_raw_gb") is not None
            and usage["data_raw_gb"] > self.cleanup_target_gb
            and deleted_count < max_attempts
        ):
            if not self._is_retention_measurement_trustworthy(usage):
                logger.error(
                    "Cleanup aborted mid-run: data_raw retention measurement "
                    "became untrusted before the next destructive phase"
                )
                break

            oldest_dir = await self.get_oldest_date_dir()

            if not oldest_dir:
                logger.warning("Could not find old directories to delete")
                break

            try:
                dir_size_gb = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.get_dir_size_gb,
                    oldest_dir
                )

                size_desc = f"{dir_size_gb:.1f}GB" if dir_size_gb is not None else "unknown size"
                logger.info(f"Deleting oldest date dir {oldest_dir} ({size_desc})...")

                await asyncio.get_event_loop().run_in_executor(
                    None,
                    shutil.rmtree,
                    oldest_dir
                )

                deleted_count += 1

                # Re-check usage (also re-validates measurement trust) before
                # the next destructive phase.
                usage = await self.check_disk_usage()
                logger.info(
                    f"After cleanup: raw={usage.get('data_raw_gb')}GB, "
                    f"total={usage.get('total_gb')}GB"
                )
            except Exception as e:
                logger.error(f"Error deleting {oldest_dir}: {e}")
                break

        logger.info(
            f"Cleanup complete: deleted {deleted_count} date directories, "
            f"current raw size: {usage.get('data_raw_gb')}GB, "
            f"current combined observability total: {usage.get('total_gb')}GB"
        )

        return deleted_count > 0

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
