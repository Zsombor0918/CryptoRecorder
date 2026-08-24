"""Replay-aware disk classification and fail-closed raw retention tests."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

import disk_monitor as module
from disk_monitor import DiskMonitor, FilesystemMeasurement, GrowthSample, scan_replay_storage


class Config:
    def __init__(self, root: Path):
        self.DATA_ROOT = root / "raw"
        self.REPLAY_ROOT = root / "replay"
        self.META_ROOT = root / "meta"
        self.STATE_ROOT = root / "state"
        self.DAILY_REPORT_ROOT = root / "reports"
        self.DISK_SOFT_LIMIT_GB = 1
        self.DISK_HARD_LIMIT_GB = 2
        self.DISK_CLEANUP_TARGET_GB = 1
        self.DISK_SCAN_TIMEOUT_SEC = 5
        self.DISK_MEASUREMENT_STALE_AFTER_SEC = 60
        self.DISK_FS_FREE_WARN_GB = 0
        self.DISK_FS_FREE_CRITICAL_GB = 0
        self.DISK_HISTORY_MAX_SAMPLES = 10
        self.DISK_HISTORY_MAX_AGE_SEC = 86400
        self.REPLAY_MONITOR_MAX_ENTRIES = 1000
        self.REPLAY_TRANSIENT_WARN_AGE_SEC = 1
        self.RAW_RETENTION_ENABLED = False
        self.RAW_RETENTION_DAYS = 7
        self.RAW_RETENTION_STABLE_AGE_SEC = 3600


def _file(path: Path, content: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_one_replay_scan_classifies_canonical_and_transients(tmp_path: Path) -> None:
    root = tmp_path / "replay"
    _file(root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "date=2026-01-01" / "depth.parquet")
    _file(root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / ".staging_2026-01-02_ADAUSDT" / "depth.parquet")
    _file(root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / ".backup_2026-01-03_ADAUSDT" / "manifest.json")
    _file(root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / ".quarantine_2026-01-04_ADAUSDT_evidence" / "bad")
    _file(root / ".lifecycle" / "build.lock", "metadata")

    result = scan_replay_storage(root)

    assert result.ok is True
    assert all(result.categories[name] > 0 for name in result.categories)
    assert result.transient_counts == {"staging": 1, "backups": 1, "quarantine": 1}


def test_replay_scan_symlink_and_bound_fail_not_zero(tmp_path: Path) -> None:
    root = tmp_path / "replay"
    root.mkdir()
    (root / "unsafe").symlink_to(tmp_path)
    failed = scan_replay_storage(root)
    assert failed.ok is False
    assert failed.status == "error"
    assert failed.error and "symlink" in failed.error

    (root / "unsafe").unlink()
    _file(root / "a")
    _file(root / "b")
    bounded = scan_replay_storage(root, max_entries=1)
    assert bounded.ok is False
    assert "entry bound" in (bounded.error or "")

    timed_out = scan_replay_storage(root, timeout_sec=1e-12)
    assert timed_out.ok is False
    assert timed_out.status == "timeout"


@pytest.mark.asyncio
async def test_monitor_removes_catalog_and_groups_shared_filesystem_once(tmp_path: Path) -> None:
    config = Config(tmp_path)
    for path in (config.DATA_ROOT, config.REPLAY_ROOT, config.META_ROOT, config.STATE_ROOT):
        path.mkdir(parents=True, exist_ok=True)
    monitor = DiskMonitor(config)
    report = await monitor.check_disk_usage()

    assert "catalog" not in report["components"]
    assert "catalog_gb" not in report
    assert set(report["components"]) >= {
        "replay_published", "replay_staging", "replay_backups", "replay_quarantine"
    }
    shared = [group for group in report["filesystem_capacity"] if {"data_raw", "replay"} <= set(group["roots"])]
    assert len(shared) == 1
    assert shared[0]["filesystem_free_gb"] >= 0


@pytest.mark.asyncio
async def test_separate_filesystems_remain_separate(monkeypatch, tmp_path: Path) -> None:
    config = Config(tmp_path)
    for path in (config.DATA_ROOT, config.REPLAY_ROOT, config.META_ROOT, config.STATE_ROOT):
        path.mkdir(parents=True, exist_ok=True)

    def fake_filesystem(path: Path) -> FilesystemMeasurement:
        path = Path(path)
        device = "replay-device" if path == config.REPLAY_ROOT else "raw-device"
        return FilesystemMeasurement(
            path=path,
            device=device,
            total_bytes=1000,
            used_bytes=200,
            free_bytes=800,
            measured_at=datetime.now(timezone.utc),
        )

    monkeypatch.setattr(module, "measure_filesystem", fake_filesystem)
    report = await DiskMonitor(config).check_disk_usage()
    by_device = {
        group["filesystem_device_or_identity"]: group
        for group in report["filesystem_capacity"]
    }
    assert set(by_device) == {"raw-device", "replay-device"}
    assert by_device["replay-device"]["roots"] == ["replay"]
    assert "data_raw" in by_device["raw-device"]["roots"]


@pytest.mark.asyncio
async def test_replay_scan_failure_uses_stale_not_zero(tmp_path: Path) -> None:
    config = Config(tmp_path)
    for path in (config.DATA_ROOT, config.REPLAY_ROOT, config.META_ROOT, config.STATE_ROOT):
        path.mkdir(parents=True, exist_ok=True)
    _file(config.REPLAY_ROOT / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "date=2026-01-01" / "depth.parquet")
    monitor = DiskMonitor(config)
    first = await monitor.check_disk_usage()
    previous = first["components"]["replay_published"]["value_gb"]
    (config.REPLAY_ROOT / "unsafe").symlink_to(tmp_path)
    second = await monitor.check_disk_usage()
    component = second["components"]["replay_published"]
    assert component["measurement_ok"] is False
    assert component["stale"] is True
    assert component["value_gb"] == previous


def test_capacity_projection_includes_raw_replay_and_transient_pressure(tmp_path: Path) -> None:
    monitor = DiskMonitor(Config(tmp_path))
    monitor._growth_history.extend(
        [
            GrowthSample(0, "old", 100, replay_bytes=50, replay_transient_bytes=5),
            GrowthSample(86400, "new", 300, replay_bytes=150, replay_transient_bytes=25),
        ]
    )
    projection = monitor._compute_capacity_growth()
    assert projection == {
        "sample_interval_seconds": 86400,
        "raw_growth_bytes_per_day": 200,
        "replay_growth_bytes_per_day": 100,
        "combined_growth_bytes_per_day": 300,
        "current_transient_pressure_bytes": 25,
    }


@pytest.mark.asyncio
async def test_cleanup_disabled_even_when_pressure_is_high(tmp_path: Path, monkeypatch) -> None:
    config = Config(tmp_path)
    monitor = DiskMonitor(config)
    raw = _file(config.DATA_ROOT / "BINANCE_SPOT" / "depth_v2" / "ADAUSDT" / "2026-01-01" / "00.jsonl")
    removals: list[Path] = []
    monkeypatch.setattr(module.shutil, "rmtree", lambda path: removals.append(Path(path)))

    async def high_usage():
        return {"data_raw_gb": 99, "retention_measurement_trustworthy": True}

    monitor.check_disk_usage = high_usage
    assert await monitor.cleanup_old_data() is False
    assert raw.exists()
    assert removals == []


def test_retention_dry_run_pairs_channels_and_missing_replay_blocks(tmp_path: Path) -> None:
    config = Config(tmp_path)
    _file(config.DATA_ROOT / "BINANCE_SPOT" / "depth_v2" / "ADAUSDT" / "2026-01-01" / "00.jsonl", "{}\n")
    _file(config.DATA_ROOT / "BINANCE_SPOT" / "trade_v2" / "ADAUSDT" / "2026-01-01" / "00.jsonl", "{}\n")
    _file(config.DATA_ROOT / "BINANCE_SPOT" / "exchangeinfo" / "EXCHANGEINFO" / "2026-01-01" / "00.jsonl", "{}\n")
    monitor = DiskMonitor(config)

    plan = monitor.plan_raw_retention()

    assert plan["mutation_performed"] is False
    assert len(plan["units"]) == 1
    unit = plan["units"][0]
    assert unit["venue"] == "BINANCE_SPOT" and unit["symbol"] == "ADAUSDT"
    assert unit["outcome"] == "blocked"
    assert len(unit["replay_dependencies"]) == 3
    assert any("adjacent replay" in reason for reason in unit["reasons"])


def test_retention_source_identity_mismatch_blocks(monkeypatch, tmp_path: Path) -> None:
    config = Config(tmp_path)
    depth = _file(config.DATA_ROOT / "BINANCE_SPOT" / "depth_v2" / "ADAUSDT" / "2026-01-01" / "00.jsonl", "{}\n")
    trade = _file(config.DATA_ROOT / "BINANCE_SPOT" / "trade_v2" / "ADAUSDT" / "2026-01-01" / "00.jsonl", "{}\n")
    stable_mtime = time.time() - 7200
    os.utime(depth, (stable_mtime, stable_mtime))
    os.utime(trade, (stable_mtime, stable_mtime))
    for target_date in ("2025-12-31", "2026-01-01", "2026-01-02"):
        partition = config.REPLAY_ROOT / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / f"date={target_date}"
        partition.mkdir(parents=True)
        (partition / "manifest.json").write_text(json.dumps({
            "schema_version": 2,
            "source_identity": {"channels": {"depth_v2": [], "trade_v2": []}},
        }))
    monkeypatch.setattr(module, "validate_partition", lambda path: True)
    monkeypatch.setattr(module, "audit_partition_deep", lambda path: [])
    monkeypatch.setattr(
        "pipeline.raw_manifest.compute_raw_source_identity",
        lambda *args, **kwargs: {
            "channels": {
                "depth_v2": [{"path": str(depth), "sha256": "a", "size_bytes": depth.stat().st_size}],
                "trade_v2": [{"path": str(trade), "sha256": "b", "size_bytes": trade.stat().st_size}],
            }
        },
    )
    plan = DiskMonitor(config).plan_raw_retention()
    assert plan["units"][0]["outcome"] == "blocked"
    assert any("source identity mismatch" in reason for reason in plan["units"][0]["reasons"])
