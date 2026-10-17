"""Storage rotation tests."""
from __future__ import annotations

from datetime import datetime
import asyncio
import copy
import json
from pathlib import Path
import threading
import time

import pytest
import zstandard as zstd

import storage as storage_mod
from storage import FileRotator


async def record_same_hour_session(raw, ledger, monkeypatch, number):
    """Small native-shaped session; useful for the CLI smoke as well."""
    import recording_coverage as coverage

    class Clock:
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 1, 6, number * 10)

    monkeypatch.setattr(storage_mod, "DATA_ROOT", raw)
    monkeypatch.setattr(storage_mod, "datetime", Clock)
    monkeypatch.setattr(coverage, "utc_now", lambda: f"2026-01-01T06:{number * 10:02d}:00.000000Z")
    raw.mkdir(parents=True, exist_ok=True)
    session = coverage.RecordingSession(ledger, raw, {"BINANCE_SPOT": ["ADAUSDT"]})
    manager = storage_mod.StorageManager()
    stamp = 1767247200000000000 + number * 600_000_000_000
    records = {
        "depth_v2": [
            {"record_type": "snapshot_seed", "stream_session_id": number, "session_seq": 1,
             "ts_recv_ns": stamp, "ts_event_ms": stamp // 1_000_000, "lastUpdateId": 100,
             "payload": {"bids": [["1.00", "2.00"]], "asks": [["1.01", "2.00"]]}},
            {"record_type": "depth_update", "stream_session_id": number, "session_seq": 2,
             "ts_recv_ns": stamp + 2_000_000_000, "ts_event_ms": stamp // 1_000_000 + 2000,
             "U": 101, "u": 101, "pu": None,
             "payload": {"bids": [["1.00", "2.01"]], "asks": [["1.01", "2.02"]]}},
        ],
        "trade_v2": [{"record_type": "trade", "market_type": "spot",
                      "trade_stream_session_id": number, "trade_session_seq": 1,
                      "ts_recv_ns": stamp, "ts_trade_ms": stamp // 1_000_000,
                      "exchange_trade_id": number, "price": "1.00", "quantity": "2.00",
                      "is_buyer_maker": False}],
    }
    try:
        for channel, rows in records.items():
            for row in rows:
                await manager.write_record("BINANCE_SPOT", "ADAUSDT", channel, row)
        result = await manager.shutdown()
        files = manager.coverage_raw_files()
        monkeypatch.setattr(coverage, "utc_now", lambda: f"2026-01-01T06:{number * 10:02d}:05.000000Z")
        final = session.finish(result, files, producers_stopped=True,
                               ingestion_stopped_at=f"2026-01-01T06:{number * 10:02d}:04.000000Z",
                               errors=[], shutdown_reason="test",
                               unowned_raw_files=manager.coverage_unowned_files())
        return final, records
    finally:
        session.release()


@pytest.mark.asyncio
async def test_same_hour_restart_preserves_records_and_immutable_coverage(monkeypatch, tmp_path):
    import recording_coverage as coverage
    from converter.readers import stream_raw_records

    raw, ledger = tmp_path / "raw", tmp_path / "coverage"
    first, first_records = await record_same_hour_session(raw, ledger, monkeypatch, 1)
    initial = dict(first["raw_files"])
    second, second_records = await record_same_hour_session(raw, ledger, monkeypatch, 2)
    current = {p: coverage.file_identity(raw / p) for p in initial}
    observed = {channel: list(stream_raw_records("BINANCE_SPOT", "ADAUSDT", channel,
                                                "2026-01-01", root=raw))
                for channel in first_records}
    print({"initial_compressed": initial, "final_compressed": current,
           "record_identities": {c: [(r.get("stream_session_id", r.get("trade_stream_session_id")),
                                     r.get("session_seq", r.get("trade_session_seq"))) for r in rows]
                                 for c, rows in observed.items()},
           "session_a_survives": all(first_records[c][0] in observed[c] for c in observed),
           "ledger_matches_disk": initial == current})
    assert current == initial, "session A compressed bytes were replaced"
    assert first["status"] == second["status"] == "closed_clean"
    assert not set(first["raw_files"]) & set(second["raw_files"])
    assert first["unowned_raw_files"] == second["unowned_raw_files"] == []
    for channel in observed:
        assert observed[channel] == first_records[channel] + second_records[channel]
    records = coverage.snapshot(ledger)
    coverage.validate_chain(records, records["head.json"]["payload"]["data_root_identity"])
    for final in (first, second):
        for path, identity in final["raw_files"].items():
            assert coverage.file_identity(raw / path) == identity
    bad = copy.deepcopy(second)
    path = next(iter(bad["raw_files"]))
    bad["raw_files"][path.replace("segment-000001", "segment-000000")] = bad["raw_files"].pop(path)
    start = records[f"{second['session_id']}.start.json"]["payload"]
    with pytest.raises(coverage.CoverageError, match="final compressed hour"):
        coverage.validate_final(start, bad)
    # Preserve the conservative retirement contract: several owners are not
    # silently reclassified as one retiring owner, even with sound bytes.
    monkeypatch.setattr(coverage, "utc_now", lambda: "2026-01-01T07:00:00.000000Z")
    successor = coverage.RecordingSession(ledger, raw, {"BINANCE_SPOT": []})
    successor.release()
    with pytest.raises(coverage.CoverageError, match="ownership"):
        coverage.retirement_proof(ledger, raw, "BINANCE_SPOT", "ADAUSDT", "2026-01-01")


@pytest.mark.asyncio
async def test_same_hour_rotations_while_compression_is_running(monkeypatch, tmp_path):
    from converter.readers import stream_raw_records

    class Clock:
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 1, 6)

    monkeypatch.setattr(storage_mod, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(storage_mod, "datetime", Clock)
    entered, release = threading.Event(), threading.Event()
    original = storage_mod.zstd.ZstdCompressor

    def paused_compressor(*args, **kwargs):
        entered.set()
        assert release.wait(5), "test did not release compressor"
        return original(*args, **kwargs)

    monkeypatch.setattr(storage_mod.zstd, "ZstdCompressor", paused_compressor)
    compressor = storage_mod.CompressionManager(worker_count=2)
    rotator = FileRotator(compressor)
    key = rotator.get_file_key("BINANCE_SPOT", "ADAUSDT", "trade_v2")
    try:
        for number in range(12):
            handle = await rotator.get_file_handle("BINANCE_SPOT", "ADAUSDT", "trade_v2")
            assert not await rotator.should_rotate(key)
            handle.write(json.dumps({"n": number}) + "\n")
            await rotator.rotate_file(key)
            if number == 0:
                assert await asyncio.to_thread(entered.wait, 5)
        assert len(rotator.touched_files) == 12
    finally:
        release.set()
        await compressor.shutdown()
    assert compressor.failed == 0
    assert list(stream_raw_records("BINANCE_SPOT", "ADAUSDT", "trade_v2", "2026-01-01", root=tmp_path)) == [{"n": n} for n in range(12)]
    assert len(list(tmp_path.rglob("*.jsonl.zst"))) == 12
    assert not list(tmp_path.rglob("*.pending"))


def test_concurrent_allocators_never_share_a_raw_path(monkeypatch, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    class Clock:
        @classmethod
        def utcnow(cls):
            return datetime(2026, 1, 1, 6)

    monkeypatch.setattr(storage_mod, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(storage_mod, "datetime", Clock)
    allocated = threading.Barrier(2, timeout=5)

    async def session(n):
        rotator = FileRotator()
        handle = await rotator.get_file_handle("BINANCE_SPOT", "ADAUSDT", "trade_v2")
        path = Path(handle.name)
        handle.write(json.dumps({"n": n}) + "\n")
        await asyncio.to_thread(allocated.wait)
        await rotator.close_all()
        assert not rotator.closure_errors
        return path

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(asyncio.run, session(n)) for n in range(2)]
        paths = [future.result(timeout=10) for future in futures]
    assert len(set(paths)) == 2
    assert all(Path(f"{p}.zst").exists() for p in paths)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["existing", "publish_race", "temp_fsync", "published_fsync"])
async def test_compression_failures_preserve_sources_and_old_archives(monkeypatch, tmp_path, fault):
    import recording_coverage as coverage
    from converter.readers import stream_raw_records
    from pipeline.raw_manifest import select_replay_raw_inventory

    directory = tmp_path / "BINANCE_SPOT/trade_v2/ADAUSDT/2026-01-01"
    directory.mkdir(parents=True)
    old = directory / "2026-01-01T06.jsonl.zst"
    old.write_bytes(zstd.ZstdCompressor().compress(b'{"session":1}\n'))
    identity = coverage.file_identity(old)
    source = directory / "2026-01-01T06.segment-000001.jsonl"
    source.write_bytes(b'{"session":2}\n')
    target = Path(f"{source}.zst")
    if fault == "existing":
        target.symlink_to(old)  # Existing destinations, including symlinks, cannot be opened.
    elif fault == "publish_race":
        real_link = storage_mod.os.link
        def collide(src, dst):
            Path(dst).write_bytes(b"unexpected-existing-target")
            real_link(src, dst)
        monkeypatch.setattr(storage_mod.os, "link", collide)
    elif fault == "temp_fsync":
        real_sync = storage_mod.os.fsync
        calls = 0
        def fail_temp(fd):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected temporary fsync failure")
            real_sync(fd)
        monkeypatch.setattr(storage_mod.os, "fsync", fail_temp)
    else:
        monkeypatch.setattr(coverage, "_fsync_dir", lambda path: (_ for _ in ()).throw(OSError("injected publication fsync failure")))
    compressor = storage_mod.CompressionManager()
    await compressor.enqueue(source)
    await compressor.shutdown()
    assert compressor.failed == 1 and compressor.last_error
    assert not compressor.completed_files
    assert source.read_bytes() == b'{"session":2}\n'
    assert coverage.file_identity(old) == identity
    if fault == "publish_race":
        assert target.read_bytes() == b"unexpected-existing-target"
    if fault == "temp_fsync":
        assert not target.exists() and not list(directory.glob("*.pending"))
        # After a handled prepublication failure the unchanged source is retryable.
        await storage_mod.CompressionManager()._compress_file(source)
        assert not source.exists()
    else:
        with pytest.raises(RuntimeError, match="unfinished|ambiguous"):
            list(stream_raw_records("BINANCE_SPOT", "ADAUSDT", "trade_v2", "2026-01-01", root=tmp_path))
        with pytest.raises(RuntimeError):
            select_replay_raw_inventory("BINANCE_SPOT", "ADAUSDT", "2026-01-01", tmp_path)


@pytest.mark.asyncio
async def test_legacy_and_same_hour_segments_build_exact_replay(monkeypatch, tmp_path):
    from pipeline.build_replay_store import build_replay_for_symbol
    from recording_coverage import file_identity
    from stores.replay_reader import ReplayReader
    from stores.replay_writer import audit_partition_deep, resolve_source_record, validate_partition

    raw, ledger = tmp_path / "raw", tmp_path / "coverage"
    for n in (1, 2):
        await record_same_hour_session(raw, ledger, monkeypatch, n)
    manifests = []
    for name in ("replay", "rebuild"):
        root = tmp_path / name
        result = build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-01-01", raw, root,
                                         schema_version=2, price_scale=2, qty_scale=2)
        assert result["outcome"] == "built", result
        assert (result["depth_count"], result["trade_count"]) == (4, 2)
        reader = ReplayReader(root)
        depths = list(reader.iter_depths("BINANCE_SPOT", "ADAUSDT", "2026-01-01"))
        trades = list(reader.iter_trades("BINANCE_SPOT", "ADAUSDT", "2026-01-01"))
        assert [(r["stream_session_id"], r["session_seq"]) for r in depths] == [(1, 1), (1, 2), (2, 1), (2, 2)]
        assert [(r["trade_stream_session_id"], r["trade_session_seq"], r["trade_id"]) for r in trades] == [(1, 1, "1"), (2, 1, "2")]
        partition = root / "venue=BINANCE_SPOT/symbol=ADAUSDT/date=2026-01-01"
        assert validate_partition(partition)
        assert audit_partition_deep(partition) == []
        manifest = json.loads((partition / "manifest.json").read_text())
        manifests.append(manifest)
        for channel, count in (("depth_v2", 2), ("trade_v2", 1)):
            entries = manifest["source_identity"]["channels"][channel]
            assert len(entries) == 2
            assert [entry["record_range"] for entry in entries] == [[0, count], [count, 2 * count]]
            for entry in entries:
                assert file_identity(raw / entry["path"]) == {k: entry[k] for k in ("sha256", "size_bytes")}
            for ordinal in range(2 * count):
                assert resolve_source_record(manifest["source_identity"], channel, ordinal)["path"] == entries[ordinal // count]["path"]
    assert manifests[0]["source_identity"] == manifests[1]["source_identity"]
    assert manifests[0]["depth_checksum"] == manifests[1]["depth_checksum"]
    assert manifests[0]["trades_checksum"] == manifests[1]["trades_checksum"]


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
