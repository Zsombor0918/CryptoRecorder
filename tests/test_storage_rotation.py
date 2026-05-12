"""Storage rotation tests."""
from __future__ import annotations

from datetime import datetime
import time

import pytest

import storage as storage_mod
from storage import FileRotator


@pytest.mark.asyncio
async def test_file_rotator_rotates_when_utc_output_path_changes(monkeypatch, tmp_path):
    class FakeDateTime:
        current = datetime(2026, 5, 11, 23, 59)

        @classmethod
        def utcnow(cls):
            return cls.current

    monkeypatch.setattr(storage_mod, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(storage_mod, "datetime", FakeDateTime)

    rotator = FileRotator()
    key = rotator.get_file_key("BINANCE_SPOT", "BTCUSDT", "depth_v2")
    old_path = rotator.get_file_path("BINANCE_SPOT", "BTCUSDT", "depth_v2")
    rotator.current_files[key] = (old_path, time.time())

    assert await rotator.should_rotate(key) is False

    FakeDateTime.current = datetime(2026, 5, 12, 0, 0)
    assert await rotator.should_rotate(key) is True
