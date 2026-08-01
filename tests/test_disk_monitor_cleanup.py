from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path

import pytest

from disk_monitor import DiskMonitor


class FakeConfig:
    def __init__(
        self,
        root: Path,
        *,
        soft_limit_gb: float = 750,
        hard_limit_gb: float = 850,
        cleanup_target_gb: float = 700,
    ):
        self.DATA_ROOT = root / "data_raw"
        self.META_ROOT = root / "meta"
        self.STATE_ROOT = root / "state"
        self.NAUTILUS_CATALOG_ROOT = root / "catalog"
        self.REPLAY_ROOT = root / "replay_store"
        self.DAILY_REPORT_ROOT = root / "reports"
        self.RAW_RETENTION_ENABLED = False
        self.RAW_RETENTION_DAYS = 7
        self.DISK_SOFT_LIMIT_GB = soft_limit_gb
        self.DISK_HARD_LIMIT_GB = hard_limit_gb
        self.DISK_CLEANUP_TARGET_GB = cleanup_target_gb


def _make_raw_day(root: Path, channel: str, symbol: str, date_str: str) -> Path:
    day_dir = root / "data_raw" / "BINANCE_SPOT" / channel / symbol / date_str
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "2026-05-16T00.jsonl").write_text("{}\n")
    return day_dir


def _raw_date_dirs(root: Path) -> list[Path]:
    return sorted(
        p
        for p in (root / "data_raw").glob("*/*/*/*")
        if p.is_dir() and len(p.name) == 10
    )


@pytest.mark.asyncio
async def test_catalog_size_does_not_trigger_raw_cleanup(tmp_path) -> None:
    config = FakeConfig(tmp_path, soft_limit_gb=750, cleanup_target_gb=700)
    monitor = DiskMonitor(config)
    old_raw = _make_raw_day(tmp_path, "trade_v2", "BTCUSDT", "2026-05-14")

    async def fake_usage() -> dict:
        return {
            "data_raw_gb": 10.0,
            "catalog_gb": 800.0,
            "meta_gb": 0.0,
            "state_gb": 0.0,
            "total_gb": 810.0,
            "retention_measurement_trustworthy": True,
        }

    monitor.check_disk_usage = fake_usage
    cleaned = await monitor.cleanup_old_data()

    assert cleaned is False
    assert old_raw.exists()


@pytest.mark.asyncio
async def test_cleanup_never_deletes_single_channel_or_date_directory(tmp_path, monkeypatch) -> None:
    config = FakeConfig(tmp_path, soft_limit_gb=3, cleanup_target_gb=1)
    monitor = DiskMonitor(config)

    for date_str in ("2026-05-14", "2026-05-15"):
        _make_raw_day(tmp_path, "depth_v2", "BTCUSDT", date_str)
        _make_raw_day(tmp_path, "trade_v2", "BTCUSDT", date_str)

    async def fake_usage() -> dict:
        raw_gb = float(len(_raw_date_dirs(tmp_path)))
        return {
            "data_raw_gb": raw_gb,
            "catalog_gb": 0.0,
            "meta_gb": 0.0,
            "state_gb": 0.0,
            "total_gb": raw_gb,
            "retention_measurement_trustworthy": True,
        }

    def run_inline(_executor, func, *args):
        fut = asyncio.get_running_loop().create_future()
        fut.set_result(func(*args))
        return fut

    monitor.check_disk_usage = fake_usage
    monkeypatch.setattr(asyncio.get_running_loop(), "run_in_executor", run_inline)
    cleaned = await monitor.cleanup_old_data()
    remaining_dates = [p.name for p in _raw_date_dirs(tmp_path)]

    assert cleaned is False
    assert Counter(remaining_dates) == Counter({"2026-05-14": 2, "2026-05-15": 2})
