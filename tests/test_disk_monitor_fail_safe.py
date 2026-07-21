"""Fail-safe disk-measurement tests for issue #19.

Covers the safety invariant: a failed or unavailable directory-size
measurement must never be represented as numeric zero. These tests use only
temporary directories and mocks; they never inspect or touch production
paths.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import disk_monitor as disk_monitor_mod
from config import REPORT_TIMEZONE_NAME
from disk_monitor import (
    DirectoryMeasurement,
    DiskMonitor,
    GrowthSample,
    LastKnownGood,
    measure_directory,
    measure_filesystem,
)
from time_utils import local_now_iso


class FakeConfig:
    def __init__(
        self,
        root: Path,
        *,
        soft_limit_gb: float = 750,
        hard_limit_gb: float = 850,
        cleanup_target_gb: float = 700,
        scan_timeout_sec: float = 5.0,
        stale_after_sec: float = 1800.0,
        fs_free_warn_gb: float = 100.0,
        fs_free_critical_gb: float = 50.0,
        history_max_samples: int = 288,
        history_max_age_sec: float = 172800.0,
    ):
        self.DATA_ROOT = root / "data_raw"
        self.META_ROOT = root / "meta"
        self.STATE_ROOT = root / "state"
        self.NAUTILUS_CATALOG_ROOT = root / "catalog"
        self.DISK_SOFT_LIMIT_GB = soft_limit_gb
        self.DISK_HARD_LIMIT_GB = hard_limit_gb
        self.DISK_CLEANUP_TARGET_GB = cleanup_target_gb
        self.DISK_SCAN_TIMEOUT_SEC = scan_timeout_sec
        self.DISK_MEASUREMENT_STALE_AFTER_SEC = stale_after_sec
        self.DISK_FS_FREE_WARN_GB = fs_free_warn_gb
        self.DISK_FS_FREE_CRITICAL_GB = fs_free_critical_gb
        self.DISK_HISTORY_MAX_SAMPLES = history_max_samples
        self.DISK_HISTORY_MAX_AGE_SEC = history_max_age_sec


def _make_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_inline_executor(monkeypatch) -> None:
    """Route run_in_executor calls synchronously so mocked functions apply."""

    def run_inline(_executor, func, *args):
        fut = asyncio.get_event_loop().create_future()
        try:
            fut.set_result(func(*args))
        except Exception as exc:  # pragma: no cover - defensive
            fut.set_exception(exc)
        return fut

    monkeypatch.setattr(asyncio.get_event_loop(), "run_in_executor", run_inline)


# ---------------------------------------------------------------------------
# measure_directory(): standalone measurement function
# ---------------------------------------------------------------------------

def test_measure_directory_success(tmp_path) -> None:
    target = _make_dir(tmp_path / "d")
    (target / "f.txt").write_text("hello world")

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is True
    assert result.status == "ok"
    assert result.value_bytes is not None and result.value_bytes >= 0
    assert result.error is None


def test_measure_directory_empty_dir_reports_zero_with_success(tmp_path) -> None:
    """A genuinely empty directory is measured successfully. Its allocated
    size may be a small non-zero value (filesystem block/inode overhead,
    since we use allocated -B1 semantics, not apparent size) — the important
    invariant is `ok=True`/`status="ok"`, never the failure sentinel."""
    target = _make_dir(tmp_path / "empty")

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is True
    assert result.status == "ok"
    assert result.value_bytes is not None
    assert result.value_bytes >= 0


def test_measure_directory_missing(tmp_path) -> None:
    target = tmp_path / "does_not_exist"

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "missing"
    assert result.value_bytes is None
    assert result.error is not None


def test_measure_directory_timeout(tmp_path, monkeypatch) -> None:
    target = _make_dir(tmp_path / "d")

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="du", timeout=kwargs.get("timeout", 5.0))

    monkeypatch.setattr(disk_monitor_mod.subprocess, "run", fake_run)

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "timeout"
    assert result.value_bytes is None


def test_measure_directory_nonzero_exit(tmp_path, monkeypatch) -> None:
    target = _make_dir(tmp_path / "d")

    class FakeResult:
        returncode = 1
        stdout = ""
        stderr = "du: permission denied"

    monkeypatch.setattr(disk_monitor_mod.subprocess, "run", lambda *a, **k: FakeResult())

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "command_error"
    assert "permission denied" in result.error


def test_measure_directory_malformed_output(tmp_path, monkeypatch) -> None:
    target = _make_dir(tmp_path / "d")

    class FakeResult:
        returncode = 0
        stdout = "not-a-number\tsome/path\n"
        stderr = ""

    monkeypatch.setattr(disk_monitor_mod.subprocess, "run", lambda *a, **k: FakeResult())

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "malformed_output"


def test_measure_directory_empty_stdout_is_malformed(tmp_path, monkeypatch) -> None:
    target = _make_dir(tmp_path / "d")

    class FakeResult:
        returncode = 0
        stdout = "   "
        stderr = ""

    monkeypatch.setattr(disk_monitor_mod.subprocess, "run", lambda *a, **k: FakeResult())

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "malformed_output"


def test_measure_directory_unexpected_exception(tmp_path, monkeypatch) -> None:
    target = _make_dir(tmp_path / "d")

    def fake_run(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(disk_monitor_mod.subprocess, "run", fake_run)

    result = measure_directory(target, timeout_sec=5.0)

    assert result.ok is False
    assert result.status == "error"
    assert "boom" in result.error


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def test_invalid_scan_timeout_rejected(tmp_path) -> None:
    config = FakeConfig(tmp_path, scan_timeout_sec=0.0)
    with pytest.raises(ValueError):
        DiskMonitor(config)


# ---------------------------------------------------------------------------
# Last-known-good fallback + persistence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_measurement_falls_back_to_last_known_good_marked_stale(
    tmp_path, monkeypatch
) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    # First cycle succeeds normally.
    usage = await monitor.check_disk_usage()
    assert usage["components"]["data_raw"]["measurement_ok"] is True
    assert usage["components"]["data_raw"]["stale"] is False

    # Second cycle: force data_raw measurement to fail.
    def fake_measure(path, timeout_sec):
        if path == config.DATA_ROOT:
            return DirectoryMeasurement(
                path=path,
                value_bytes=None,
                ok=False,
                status="timeout",
                error="simulated timeout",
                measured_at=datetime.now(timezone.utc),
                duration_seconds=timeout_sec,
            )
        return measure_directory(path, timeout_sec)

    monkeypatch.setattr(disk_monitor_mod, "measure_directory", fake_measure)

    usage2 = await monitor.check_disk_usage()
    data_raw = usage2["components"]["data_raw"]

    assert data_raw["measurement_ok"] is False
    assert data_raw["measurement_status"] == "timeout"
    assert data_raw["stale"] is True
    # Falls back to the last-known-good value from the first cycle, not 0.
    assert data_raw["value_gb"] == usage["components"]["data_raw"]["value_gb"]
    assert usage2["monitoring_health"] in ("degraded", "unhealthy")
    assert any("data_raw" in alert for alert in usage2["alerts"])


@pytest.mark.asyncio
async def test_no_previous_valid_value_produces_null_not_zero(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    # DATA_ROOT deliberately not created -> "missing" on first ever measurement.
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    usage = await monitor.check_disk_usage()
    data_raw = usage["components"]["data_raw"]

    assert data_raw["measurement_ok"] is False
    assert data_raw["value_gb"] is None
    assert usage["total_gb"] is None
    assert usage["monitoring_health"] == "unhealthy"
    assert usage["retention_measurement_trustworthy"] is False


@pytest.mark.asyncio
async def test_restart_loads_persisted_last_known_good_state(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    await monitor.check_disk_usage()
    assert monitor.monitor_state_file.exists()

    # Simulate a process restart: build a brand new DiskMonitor instance
    # against the same state directory.
    monitor2 = DiskMonitor(config)
    assert "data_raw" in monitor2._last_known_good
    assert monitor2._last_known_good["data_raw"].value_bytes is not None


# ---------------------------------------------------------------------------
# Alerts / monitoring health
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_last_known_good_produces_staleness_alert(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path, stale_after_sec=1.0)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    await monitor.check_disk_usage()

    # Backdate the last-known-good timestamp so it looks old.
    old_lkg = monitor._last_known_good["data_raw"]
    monitor._last_known_good["data_raw"] = LastKnownGood(
        value_bytes=old_lkg.value_bytes,
        measured_at=(datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat(),
        duration_seconds=old_lkg.duration_seconds,
    )

    def fake_measure(path, timeout_sec):
        if path == config.DATA_ROOT:
            return DirectoryMeasurement(
                path=path,
                value_bytes=None,
                ok=False,
                status="timeout",
                error="simulated timeout",
                measured_at=datetime.now(timezone.utc),
                duration_seconds=timeout_sec,
            )
        return measure_directory(path, timeout_sec)

    monkeypatch.setattr(disk_monitor_mod, "measure_directory", fake_measure)

    usage = await monitor.check_disk_usage()
    assert any("stale" in alert.lower() for alert in usage["alerts"])


@pytest.mark.asyncio
async def test_misleading_percentages_and_growth_omitted_when_unknown(
    tmp_path, monkeypatch
) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    usage = await monitor.check_disk_usage()

    assert usage["total_gb"] is None
    assert usage["percent_of_soft_limit"] is None
    assert usage["percent_of_hard_limit"] is None
    assert usage["growth_rate_gb_day"] is None
    assert usage["days_to_full"] is None


# ---------------------------------------------------------------------------
# Cleanup fails closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cleanup_skipped_on_unknown_measurement(tmp_path) -> None:
    config = FakeConfig(tmp_path, soft_limit_gb=1, cleanup_target_gb=0)
    monitor = DiskMonitor(config)

    async def fake_usage() -> dict:
        return {
            "data_raw_gb": None,
            "components": {"data_raw": {"measurement_status": "timeout"}},
            "retention_measurement_trustworthy": False,
        }

    monitor.check_disk_usage = fake_usage
    cleaned = await monitor.cleanup_old_data()

    assert cleaned is False


@pytest.mark.asyncio
async def test_cleanup_skipped_on_stale_measurement(tmp_path) -> None:
    config = FakeConfig(tmp_path, soft_limit_gb=1, cleanup_target_gb=0)
    monitor = DiskMonitor(config)

    async def fake_usage() -> dict:
        return {
            # Value present (from last-known-good) but explicitly stale ->
            # must not be trusted for a destructive decision.
            "data_raw_gb": 999.0,
            "components": {
                "data_raw": {"measurement_status": "ok", "stale": True}
            },
            "retention_measurement_trustworthy": False,
        }

    monitor.check_disk_usage = fake_usage
    cleaned = await monitor.cleanup_old_data()

    assert cleaned is False


@pytest.mark.asyncio
async def test_no_destructive_call_made_on_failure_path(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path, soft_limit_gb=1, cleanup_target_gb=0)
    monitor = DiskMonitor(config)

    rmtree_calls = []
    monkeypatch.setattr(disk_monitor_mod.shutil, "rmtree", lambda p: rmtree_calls.append(p))

    async def fake_usage() -> dict:
        return {
            "data_raw_gb": None,
            "components": {"data_raw": {"measurement_status": "error"}},
            "retention_measurement_trustworthy": False,
        }

    monitor.check_disk_usage = fake_usage
    await monitor.cleanup_old_data()

    assert rmtree_calls == []


# ---------------------------------------------------------------------------
# Filesystem capacity + separate thresholds
# ---------------------------------------------------------------------------

def test_measure_filesystem_reports_expected_fields(tmp_path) -> None:
    result = measure_filesystem(tmp_path)

    assert result.total_bytes > 0
    assert result.free_bytes >= 0
    d = result.to_dict()
    assert set(d) >= {
        "filesystem_path",
        "filesystem_device_or_identity",
        "filesystem_total_gb",
        "filesystem_used_gb",
        "filesystem_free_gb",
        "filesystem_percent_used",
    }


@pytest.mark.asyncio
async def test_low_free_space_alert_independent_of_raw_scan_failure(
    tmp_path, monkeypatch
) -> None:
    config = FakeConfig(tmp_path, fs_free_warn_gb=1e12, fs_free_critical_gb=1)
    monitor = DiskMonitor(config)
    # DATA_ROOT missing -> data_raw measurement fails, but filesystem alert
    # must still fire (it uses shutil.disk_usage independently).
    _make_dir(config.NAUTILUS_CATALOG_ROOT)
    _make_dir(config.META_ROOT)

    usage = await monitor.check_disk_usage()

    assert any("filesystem free space" in alert.lower() for alert in usage["alerts"])
    assert usage["filesystem"]["filesystem_total_gb"] > 0


@pytest.mark.asyncio
async def test_retention_and_filesystem_thresholds_are_independent(tmp_path) -> None:
    """A low soft/hard retention limit must not be conflated with filesystem
    percent-used semantics."""
    config = FakeConfig(tmp_path, soft_limit_gb=1_000_000, hard_limit_gb=2_000_000)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)

    usage = await monitor.check_disk_usage()

    # Retention percentage is computed from tracked GB vs retention limits,
    # not from filesystem_percent_used.
    assert usage["percent_of_soft_limit"] != usage["filesystem"]["filesystem_percent_used"]


# ---------------------------------------------------------------------------
# Atomic report writing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_usage_report_is_atomic_and_readable(tmp_path) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)

    usage = await monitor.check_disk_usage()
    await monitor.write_usage_report(usage)

    assert monitor.usage_log_file.exists()
    on_disk = json.loads(monitor.usage_log_file.read_text())
    assert on_disk["monitoring_health"] == usage["monitoring_health"]
    # No leftover temp files.
    leftovers = list(monitor.usage_log_file.parent.glob(".disk_usage.json.*.tmp"))
    assert leftovers == []


def test_atomic_write_cleans_up_temp_file_on_failure(tmp_path) -> None:
    config = FakeConfig(tmp_path)
    monitor = DiskMonitor(config)
    target = tmp_path / "state" / "some_report.json"
    target.parent.mkdir(parents=True, exist_ok=True)

    class Unserializable:
        pass

    with pytest.raises(TypeError):
        DiskMonitor._atomic_write_json(target, {"bad": Unserializable()})

    leftovers = list(target.parent.glob(f".{target.name}.*.tmp"))
    assert leftovers == []
    assert not target.exists()


# ---------------------------------------------------------------------------
# Growth / days-to-full uses real timestamps and valid samples only
# ---------------------------------------------------------------------------

def test_growth_uses_real_timestamps_and_excludes_short_spans(tmp_path) -> None:
    config = FakeConfig(tmp_path)
    monitor = DiskMonitor(config)

    now = datetime.now(timezone.utc).timestamp()
    monitor._growth_history.append(
        GrowthSample(epoch=now - 10, timestamp="t0", data_raw_bytes=1_000_000)
    )
    monitor._growth_history.append(
        GrowthSample(epoch=now, timestamp="t1", data_raw_bytes=2_000_000)
    )

    # Span is only 10s, well under MIN_GROWTH_SPAN_SEC -> insufficient evidence.
    assert monitor._compute_growth() is None


def test_growth_computed_from_two_valid_widely_spaced_samples(tmp_path) -> None:
    config = FakeConfig(tmp_path, hard_limit_gb=1000)
    monitor = DiskMonitor(config)

    now = datetime.now(timezone.utc).timestamp()
    one_gb = 1024 ** 3
    monitor._growth_history.append(
        GrowthSample(epoch=now - 86400, timestamp="t0", data_raw_bytes=10 * one_gb)
    )
    monitor._growth_history.append(
        GrowthSample(epoch=now, timestamp="t1", data_raw_bytes=20 * one_gb)
    )

    growth = monitor._compute_growth()
    assert growth is not None
    growth_rate_gb_day, days_to_full, elapsed_sec, oldest_ts, newest_ts = growth
    assert growth_rate_gb_day == pytest.approx(10.0, rel=0.05)
    assert days_to_full is not None
    assert elapsed_sec == pytest.approx(86400, rel=0.01)
    assert oldest_ts == "t0"
    assert newest_ts == "t1"


def test_non_increasing_growth_sample_rejected(tmp_path) -> None:
    config = FakeConfig(tmp_path)
    monitor = DiskMonitor(config)

    now = datetime.now(timezone.utc)
    monitor._record_growth_sample(now, 1_000_000)
    # Same or earlier timestamp must be rejected, not appended.
    monitor._record_growth_sample(now, 2_000_000)

    assert len(monitor._growth_history) == 1


@pytest.mark.asyncio
async def test_failed_or_stale_samples_excluded_from_growth_history(
    tmp_path, monkeypatch
) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    await monitor.check_disk_usage()
    assert len(monitor._growth_history) == 1

    def fake_measure(path, timeout_sec):
        if path == config.DATA_ROOT:
            return DirectoryMeasurement(
                path=path,
                value_bytes=None,
                ok=False,
                status="timeout",
                error="simulated timeout",
                measured_at=datetime.now(timezone.utc),
                duration_seconds=timeout_sec,
            )
        return measure_directory(path, timeout_sec)

    monkeypatch.setattr(disk_monitor_mod, "measure_directory", fake_measure)
    await monitor.check_disk_usage()

    # The failed cycle must not add a new growth sample.
    assert len(monitor._growth_history) == 1


# ---------------------------------------------------------------------------
# Overlapping scan prevention
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overlapping_scans_are_skipped_not_queued(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)

    release = asyncio.Event()
    call_count = 0

    async def slow_measure_all_roots():
        nonlocal call_count
        call_count += 1
        await release.wait()
        return await DiskMonitor._measure_all_roots(monitor)

    monkeypatch.setattr(monitor, "_measure_all_roots", slow_measure_all_roots)

    first = asyncio.ensure_future(monitor.check_disk_usage())
    await asyncio.sleep(0.05)  # let the first call acquire the lock and block

    second = await monitor.check_disk_usage()
    assert second["skipped_duplicate"] is True

    release.set()
    first_result = await first
    assert first_result["skipped_duplicate"] is False
    assert call_count == 1


@pytest.mark.asyncio
async def test_lock_released_on_exception_path(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)

    async def boom():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(monitor, "_measure_all_roots", boom)

    with pytest.raises(RuntimeError):
        await monitor.check_disk_usage()

    assert monitor._scan_lock.locked() is False


@pytest.mark.asyncio
async def test_skipped_duplicate_forces_measurement_untrustworthy_and_degraded(
    tmp_path, monkeypatch
) -> None:
    """A skipped-duplicate report must never inherit a trustworthy flag from
    the previous cycle, must carry a visible alert, and must not silently
    remain 'healthy'."""
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)

    release = asyncio.Event()

    async def slow_measure_all_roots():
        await release.wait()
        return await DiskMonitor._measure_all_roots(monitor)

    monkeypatch.setattr(monitor, "_measure_all_roots", slow_measure_all_roots)

    first = asyncio.ensure_future(monitor.check_disk_usage())
    await asyncio.sleep(0.05)  # let the first call acquire the lock and block

    second = await monitor.check_disk_usage()
    assert second["skipped_duplicate"] is True
    # Even though the previous (in-flight) cycle's last report was
    # trustworthy, a skipped duplicate is not a fresh measurement and must
    # fail closed.
    assert second["retention_measurement_trustworthy"] is False
    assert second["monitoring_health"] in ("degraded", "unhealthy")
    assert any("skipped" in alert.lower() for alert in second["alerts"])

    release.set()
    await first


@pytest.mark.asyncio
async def test_no_rmtree_when_lock_held_and_previous_report_trustworthy(
    tmp_path, monkeypatch
) -> None:
    """Even when the previous cycle's report was trustworthy, cleanup must
    never delete anything if the current check was skipped due to an
    overlapping scan (skipped_duplicate=True)."""
    config = FakeConfig(tmp_path, soft_limit_gb=0.000001, cleanup_target_gb=0)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    rmtree_calls = []
    monkeypatch.setattr(disk_monitor_mod.shutil, "rmtree", lambda p: rmtree_calls.append(p))

    # First cycle: a genuine, trustworthy measurement.
    first_usage = await monitor.check_disk_usage()
    assert first_usage["retention_measurement_trustworthy"] is True

    # Simulate an overlapping scan: acquire the lock manually so the next
    # check_disk_usage() call takes the skipped_duplicate path.
    await monitor._scan_lock.acquire()
    try:
        usage = await monitor.check_disk_usage()
        assert usage["skipped_duplicate"] is True
        assert usage["retention_measurement_trustworthy"] is False

        cleaned = await monitor.cleanup_old_data()
    finally:
        monitor._scan_lock.release()

    assert cleaned is False
    assert rmtree_calls == []


@pytest.mark.asyncio
async def test_stale_last_known_good_nulls_all_derived_fields(
    tmp_path, monkeypatch
) -> None:
    """When the current data_raw measurement failed/is stale, retention
    percentages, growth rate, and days-to-full must all be null — never a
    current-looking estimate silently computed from the stale fallback."""
    config = FakeConfig(tmp_path, soft_limit_gb=1, hard_limit_gb=2)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    # First cycle: genuine successful measurement, establishes last-known-good
    # and a growth-history sample.
    await monitor.check_disk_usage()

    # Second cycle: data_raw measurement fails -> falls back to stale LKG.
    def fake_measure(path, timeout_sec):
        if path == config.DATA_ROOT:
            return DirectoryMeasurement(
                path=path,
                value_bytes=None,
                ok=False,
                status="timeout",
                error="simulated timeout",
                measured_at=datetime.now(timezone.utc),
                duration_seconds=timeout_sec,
            )
        return measure_directory(path, timeout_sec)

    monkeypatch.setattr(disk_monitor_mod, "measure_directory", fake_measure)
    usage = await monitor.check_disk_usage()

    assert usage["components"]["data_raw"]["stale"] is True
    assert usage["retention_measurement_trustworthy"] is False
    # All derived fields based on data_raw must be null, not a stale-looking
    # current estimate.
    assert usage["percent_of_soft_limit"] is None
    assert usage["percent_of_hard_limit"] is None
    assert usage["growth_rate_gb_day"] is None
    assert usage["days_to_full"] is None


# ---------------------------------------------------------------------------
# Top-level report timestamp timezone consistency (docs/OPERATIONS.md
# "Europe/Budapest" contract) — Codex review finding #2.
# ---------------------------------------------------------------------------

REPORT_TZ = ZoneInfo(REPORT_TIMEZONE_NAME)


@pytest.mark.asyncio
async def test_normal_report_timestamp_uses_configured_local_timezone(
    tmp_path, monkeypatch
) -> None:
    """The top-level `timestamp` field of a normal (non-overlapping) scan
    must carry the configured local report-timezone offset (Europe/Budapest),
    not a bare UTC offset, matching the documented contract."""
    config = FakeConfig(tmp_path)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    usage = await monitor.check_disk_usage()

    reported = datetime.fromisoformat(usage["timestamp"])
    assert reported.tzinfo is not None
    expected_offset = REPORT_TZ.utcoffset(reported.replace(tzinfo=None))
    assert reported.utcoffset() == expected_offset


@pytest.mark.asyncio
async def test_overlapping_scan_report_timestamp_uses_same_timezone_contract(
    tmp_path, monkeypatch
) -> None:
    """The skipped/overlap ('no prior report') path must use the identical
    local-timezone contract as a normal scan's top-level timestamp."""
    config = FakeConfig(tmp_path)
    monitor = DiskMonitor(config)
    # No prior report exists yet and the lock is held -> hits the
    # no-prior-report branch of the overlapping-scan guard.
    await monitor._scan_lock.acquire()
    try:
        usage = await monitor.check_disk_usage()
    finally:
        monitor._scan_lock.release()

    assert usage["skipped_duplicate"] is True
    reported = datetime.fromisoformat(usage["timestamp"])
    assert reported.tzinfo is not None
    expected_offset = REPORT_TZ.utcoffset(reported.replace(tzinfo=None))
    assert reported.utcoffset() == expected_offset


@pytest.mark.asyncio
async def test_report_timestamp_change_does_not_alter_growth_or_age_logic(
    tmp_path, monkeypatch
) -> None:
    """Switching the top-level timestamp to local time must not change
    growth-sample ordering, measurement-age computation, or staleness
    logic — those must remain driven by UTC/epoch values internally."""
    config = FakeConfig(tmp_path, stale_after_sec=1.0)
    for root in (config.DATA_ROOT, config.NAUTILUS_CATALOG_ROOT, config.META_ROOT):
        _make_dir(root)
    monitor = DiskMonitor(config)
    _run_inline_executor(monkeypatch)

    first = await monitor.check_disk_usage()
    assert first["components"]["data_raw"]["measurement_age_seconds"] == 0.0

    def fake_measure(path, timeout_sec):
        if path == config.DATA_ROOT:
            return DirectoryMeasurement(
                path=path,
                value_bytes=None,
                ok=False,
                status="timeout",
                error="simulated timeout",
                measured_at=datetime.now(timezone.utc),
                duration_seconds=timeout_sec,
            )
        return measure_directory(path, timeout_sec)

    monkeypatch.setattr(disk_monitor_mod, "measure_directory", fake_measure)
    second = await monitor.check_disk_usage()

    data_raw = second["components"]["data_raw"]
    # Age is computed from UTC/epoch internals and must remain a sane,
    # non-negative elapsed value independent of the local-time top-level
    # timestamp representation.
    assert data_raw["measurement_age_seconds"] is not None
    assert data_raw["measurement_age_seconds"] >= 0.0
    assert data_raw["stale"] is True

    # Growth-history ordering (internal epoch-based) is unaffected: a
    # second, later sample must still be accepted (no "non-increasing
    # timestamp" rejection) even though the top-level report timestamp is
    # now expressed in local time.
    assert len(monitor._growth_history) >= 1
