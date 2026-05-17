from __future__ import annotations

import asyncio
from datetime import datetime
import importlib
import time

import pytest

import config as config_mod
import storage as storage_mod


def test_writer_env_config_parsing(monkeypatch, tmp_path) -> None:
    with monkeypatch.context() as mp:
        mp.setenv("CRYPTO_RECORDER_DATA_ROOT", str(tmp_path / "data"))
        mp.setenv("CRYPTO_RECORDER_STATE_ROOT", str(tmp_path / "state"))
        mp.setenv("CRYPTO_RECORDER_DEPTH_WRITER_QUEUE_MAX_SIZE", "123")
        mp.setenv("CRYPTO_RECORDER_TRADE_WRITER_QUEUE_MAX_SIZE", "45")
        mp.setenv("CRYPTO_RECORDER_WRITER_QUEUE_MAX_SIZE", "67")
        mp.setenv("CRYPTO_RECORDER_WRITER_BATCH_SIZE", "89")
        mp.setenv("CRYPTO_RECORDER_WRITER_FLUSH_INTERVAL_SEC", "1.25")
        mp.setenv("CRYPTO_RECORDER_DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC", "0")
        mp.setenv("CRYPTO_RECORDER_TRADE_WRITER_ENQUEUE_TIMEOUT_SEC", "0.5")
        mp.setenv("CRYPTO_RECORDER_DEPTH_BLOCK_WARN_INTERVAL_SEC", "2.5")
        mp.setenv("CRYPTO_RECORDER_DEPTH_BLOCK_ALERT_SEC", "7.5")
        mp.setenv("CRYPTO_RECORDER_WRITER_TELEMETRY_LOG_INTERVAL_SEC", "11")
        mp.setenv("CRYPTO_RECORDER_WRITER_COMPRESSION_WORKERS", "2")
        mp.setenv("CRYPTO_RECORDER_WRITER_COMPRESSION_SHUTDOWN_TIMEOUT_SEC", "3.5")

        cfg = importlib.reload(config_mod)
        assert cfg.DATA_ROOT == tmp_path / "data"
        assert cfg.STATE_ROOT == tmp_path / "state"
        assert cfg.DEPTH_WRITER_QUEUE_MAX_SIZE == 123
        assert cfg.TRADE_WRITER_QUEUE_MAX_SIZE == 45
        assert cfg.QUEUE_MAX_SIZE == 67
        assert cfg.WRITER_BATCH_SIZE == 89
        assert cfg.WRITER_FLUSH_INTERVAL_SEC == 1.25
        assert cfg.DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC == 0
        assert cfg.TRADE_WRITER_ENQUEUE_TIMEOUT_SEC == 0.5
        assert cfg.DEPTH_BLOCK_WARN_INTERVAL_SEC == 2.5
        assert cfg.DEPTH_BLOCK_ALERT_SEC == 7.5
        assert cfg.WRITER_TELEMETRY_LOG_INTERVAL_SEC == 11
        assert cfg.WRITER_COMPRESSION_WORKERS == 2
        assert cfg.WRITER_COMPRESSION_SHUTDOWN_TIMEOUT_SEC == 3.5

    importlib.reload(config_mod)
    importlib.reload(storage_mod)


def test_writer_env_config_rejects_invalid_values(monkeypatch, tmp_path) -> None:
    with monkeypatch.context() as mp:
        mp.setenv("CRYPTO_RECORDER_DATA_ROOT", str(tmp_path / "data"))
        mp.setenv("CRYPTO_RECORDER_STATE_ROOT", str(tmp_path / "state"))
        mp.setenv("CRYPTO_RECORDER_WRITER_BATCH_SIZE", "0")
        with pytest.raises(ValueError, match="CRYPTO_RECORDER_WRITER_BATCH_SIZE"):
            importlib.reload(config_mod)

    importlib.reload(config_mod)
    importlib.reload(storage_mod)


@pytest.mark.asyncio
async def test_trade_queue_overflow_increments_drop_accounting(monkeypatch) -> None:
    monkeypatch.setattr(storage_mod, "TRADE_WRITER_QUEUE_MAX_SIZE", 1)
    monkeypatch.setattr(storage_mod, "TRADE_WRITER_ENQUEUE_TIMEOUT_SEC", 0.01)

    writer = storage_mod.AsyncWriter(
        "BINANCE_SPOT",
        "BTCUSDT",
        storage_mod.TRADE_V2_CHANNEL,
        storage_mod.FileRotator(),
    )

    await writer.enqueue({"n": 1})
    await writer.enqueue({"n": 2})

    telemetry = writer.get_telemetry()
    assert writer.drop_count == 1
    assert writer.enqueued_count == 1
    assert telemetry["queue_size"] == 1
    assert telemetry["queue_high_watermark"] == 1
    assert telemetry["drop_count"] == 1


@pytest.mark.asyncio
async def test_depth_queue_saturation_blocks_without_dropping(monkeypatch, caplog) -> None:
    monkeypatch.setattr(storage_mod, "DEPTH_WRITER_QUEUE_MAX_SIZE", 1)
    monkeypatch.setattr(storage_mod, "DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC", 0)
    monkeypatch.setattr(storage_mod, "DEPTH_BLOCK_WARN_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(storage_mod, "DEPTH_BLOCK_ALERT_SEC", 60)
    caplog.set_level("WARNING", logger="storage")

    writer = storage_mod.AsyncWriter(
        "BINANCE_SPOT",
        "BTCUSDT",
        storage_mod.DEPTH_V2_CHANNEL,
        storage_mod.FileRotator(),
    )

    await writer.enqueue({"n": 1})
    task = asyncio.create_task(writer.enqueue({"n": 2}))
    await asyncio.sleep(0.03)

    telemetry = writer.get_telemetry()
    assert not task.done()
    assert telemetry["blocked"] is True
    assert telemetry["current_block_sec"] > 0
    assert telemetry["drop_count"] == 0
    assert "Depth writer queue blocked" in caplog.text

    assert writer.queue.get_nowait() == {"n": 1}
    await asyncio.wait_for(task, timeout=0.2)

    telemetry = writer.get_telemetry()
    assert telemetry["blocked"] is False
    assert telemetry["drop_count"] == 0
    assert telemetry["enqueued_count"] == 2
    assert telemetry["max_block_sec"] > 0


@pytest.mark.asyncio
async def test_depth_blocking_enqueue_is_cancelable(monkeypatch) -> None:
    monkeypatch.setattr(storage_mod, "DEPTH_WRITER_QUEUE_MAX_SIZE", 1)
    monkeypatch.setattr(storage_mod, "DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC", 0)
    monkeypatch.setattr(storage_mod, "DEPTH_BLOCK_WARN_INTERVAL_SEC", 10)

    writer = storage_mod.AsyncWriter(
        "BINANCE_USDTF",
        "ETHUSDT",
        storage_mod.DEPTH_V2_CHANNEL,
        storage_mod.FileRotator(),
    )

    await writer.enqueue({"n": 1})
    task = asyncio.create_task(writer.enqueue({"n": 2}))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    telemetry = writer.get_telemetry()
    assert telemetry["blocked"] is False
    assert telemetry["drop_count"] == 0
    assert telemetry["max_block_sec"] > 0


@pytest.mark.asyncio
async def test_storage_manager_writer_telemetry_fields(monkeypatch) -> None:
    monkeypatch.setattr(storage_mod, "TRADE_WRITER_QUEUE_MAX_SIZE", 2)
    manager = storage_mod.StorageManager()
    writer = storage_mod.AsyncWriter(
        "BINANCE_SPOT",
        "ETHUSDT",
        storage_mod.TRADE_V2_CHANNEL,
        manager.rotator,
    )
    key = manager.rotator.get_file_key("BINANCE_SPOT", "ETHUSDT", storage_mod.TRADE_V2_CHANNEL)
    manager.writers[key] = writer

    await writer.enqueue({"n": 1})
    telemetry = manager.get_writer_telemetry()
    item = telemetry["writers"][key]

    assert item["venue"] == "BINANCE_SPOT"
    assert item["symbol"] == "ETHUSDT"
    assert item["channel"] == storage_mod.TRADE_V2_CHANNEL
    assert item["queue_size"] == 1
    assert item["queue_max_size"] == 2
    assert item["queue_high_watermark"] == 1
    assert item["drop_count"] == 0
    assert item["enqueued_count"] == 1
    assert item["write_count"] == 0
    assert telemetry["totals"]["writer_count"] == 1
    assert telemetry["totals"]["queued_records"] == 1
    assert telemetry["compression"]["queued"] == 0
    assert telemetry["top_pressure_writers"][0]["key"] == key


@pytest.mark.asyncio
async def test_rotation_schedules_compression_without_blocking_new_handle(monkeypatch, tmp_path) -> None:
    class FakeDateTime:
        current = datetime(2026, 5, 17, 0, 0)

        @classmethod
        def utcnow(cls):
            return cls.current

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_compress(self, file_path):
        started.set()
        await release.wait()

    monkeypatch.setattr(storage_mod, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(storage_mod, "datetime", FakeDateTime)
    monkeypatch.setattr(storage_mod.CompressionManager, "_compress_file", slow_compress)

    compressor = storage_mod.CompressionManager(worker_count=1)
    rotator = storage_mod.FileRotator(compression_manager=compressor)

    handle = await rotator.get_file_handle("BINANCE_SPOT", "BTCUSDT", storage_mod.DEPTH_V2_CHANNEL)
    handle.write('{"n":1}\n')
    handle.flush()

    FakeDateTime.current = datetime(2026, 5, 17, 1, 0)
    start = time.monotonic()
    new_handle = await rotator.get_file_handle("BINANCE_SPOT", "BTCUSDT", storage_mod.DEPTH_V2_CHANNEL)
    elapsed = time.monotonic() - start

    assert elapsed < 0.1
    assert new_handle is not handle
    await asyncio.wait_for(started.wait(), timeout=0.5)
    assert compressor.get_telemetry()["active"] == 1

    release.set()
    await compressor.shutdown()
    await rotator.close_all(compress=False)


@pytest.mark.asyncio
async def test_compression_shutdown_timeout_does_not_hang(monkeypatch, tmp_path) -> None:
    async def stuck_compress(self, file_path):
        await asyncio.Event().wait()

    monkeypatch.setattr(storage_mod.CompressionManager, "_compress_file", stuck_compress)

    file_path = tmp_path / "stuck.jsonl"
    file_path.write_text('{"n":1}\n')
    compressor = storage_mod.CompressionManager(worker_count=1)

    await compressor.enqueue(file_path)
    await asyncio.sleep(0.01)
    await asyncio.wait_for(compressor.shutdown(timeout_sec=0.01), timeout=0.2)

    telemetry = compressor.get_telemetry()
    assert telemetry["failed"] >= 1
    assert "compression shutdown timed out" in telemetry["last_error"]
