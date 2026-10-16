"""Deterministic retirement fixtures and fault injection; no production sources."""
from __future__ import annotations

import asyncio
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import zstandard as zstd

import recording_coverage as coverage
from pipeline.build_replay_store import build_replay_for_symbol
from stores.replay_writer import validate_partition

DATE = "2026-01-01"
VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"


def make_fixture(base: Path, *, depth_updates=False):
    raw, ledger, replay = base / "raw", base / "coverage", base / "replay"
    raw.mkdir(parents=True)
    with patch.object(coverage, "utc_now", return_value="2026-01-01T00:00:00.000000Z"):
        session = coverage.RecordingSession(ledger, raw, {VENUE: [SYMBOL]})
    files = {}
    for hour in range(7):
        recv_ns = int(datetime(2026, 1, 1, hour, 1, tzinfo=timezone.utc).timestamp() * 1e9)
        for channel in ("depth_v2", "trade_v2"):
            path = raw / VENUE / channel / SYMBOL / DATE / f"{DATE}T{hour:02d}.jsonl.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            record = ({"record_type": "snapshot_seed", "stream_session_id": hour + 1, "session_seq": 1, "ts_recv_ns": recv_ns, "ts_event_ms": recv_ns // 1_000_000, "lastUpdateId": 100 + hour, "payload": {"bids": [["1.00", "2.00"]], "asks": [["1.01", "2.00"]]}} if channel == "depth_v2" else {"record_type": "trade", "market_type": "spot", "trade_stream_session_id": hour + 1, "trade_session_seq": 1, "ts_recv_ns": recv_ns, "ts_trade_ms": recv_ns // 1_000_000, "exchange_trade_id": hour + 1, "price": "1.00", "quantity": "2.00", "is_buyer_maker": False})
            records = [record]
            if channel == "depth_v2" and depth_updates:
                records.append({"record_type": "depth_update", "stream_session_id": hour + 1, "session_seq": 2, "ts_recv_ns": recv_ns + 2_000_000_000, "ts_event_ms": recv_ns // 1_000_000 + 2000, "U": 101 + hour, "u": 101 + hour, "pu": None, "payload": {"bids": [["1.00", "2.01"]], "asks": [["1.01", "2.02"]]}})
            path.write_bytes(zstd.ZstdCompressor().compress(("".join(json.dumps(row) + "\n" for row in records)).encode()))
            files[path.relative_to(raw).as_posix()] = coverage.file_identity(path)
    writers = {f"{VENUE}:{SYMBOL}:{channel}": {"enqueued_count": 14 if channel == "depth_v2" and depth_updates else 7, "write_count": 14 if channel == "depth_v2" and depth_updates else 7, "drop_count": 0, "write_errors": 0, "queue_size": 0} for channel in ("depth_v2", "trade_v2")}
    storage = {"queues_drained": True, "files_closed": True, "compression_drained": True, "writers": writers, "compression": {"queued": 0, "active": 0, "failed": 0, "last_error": None}, "errors": []}
    with patch.object(coverage, "utc_now", return_value="2026-01-01T06:49:01.000000Z"):
        final = session.finish(storage, files, producers_stopped=True, ingestion_stopped_at="2026-01-01T06:49:00.000000Z", errors=[], shutdown_reason="SIGTERM")
    session.release()
    assert final["status"] == "closed_clean"
    with patch.object(coverage, "utc_now", return_value="2026-01-01T06:50:00.000000Z"):
        successor = coverage.RecordingSession(ledger, raw, {VENUE: ["BTCUSDT"]})
    successor.release()
    return SimpleNamespace(raw=raw, ledger=ledger, replay=replay, session=session, successor=successor, final=final, storage=storage, files=files, base=base)


@pytest.fixture
def fixture(tmp_path):
    return make_fixture(tmp_path)


def build(f, **kwargs):
    return build_replay_for_symbol(VENUE, SYMBOL, DATE, f.raw, f.replay, schema_version=2, price_scale=2, qty_scale=2, check_repartition_readiness=True, coverage_root=f.ledger, **kwargs)


def write_payload(path, payload):
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def final_path(f):
    return f.ledger / f"{f.session.start['session_id']}.final.json"


def test_t00_to_t06_retirement_build_audits_and_reuses(fixture):
    result = build(fixture)
    assert result["outcome"] == "built", result
    assert (result["depth_count"], result["trade_count"]) == (7, 7)
    assert result["readiness_mode"] == "clean_retirement_evidence"
    partition = fixture.replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    assert validate_partition(partition)
    manifest = json.loads((partition / "manifest.json").read_text())
    proof = manifest["readiness_evidence"]
    assert proof["sha256"] == result["readiness_evidence_sha256"]
    assert manifest["readiness_contract_version"] == proof["contract_version"] == 2
    assert manifest["integrity"]["readiness_evidence_sha256"] == proof["sha256"]
    assert (partition / "manifest.json").read_text().count('"readiness_evidence":') == 1
    assert build(fixture)["outcome"] == "skipped_valid"
    assert not list(fixture.replay.rglob(".staging*"))


def test_closed_next_day_route_does_not_read_ledger(fixture):
    day = "2026-01-02"
    path = fixture.raw / VENUE / "depth_v2" / SYMBOL / day / f"{day}T00.jsonl.zst"
    path.parent.mkdir()
    path.write_bytes(zstd.ZstdCompressor().compress(b""))
    (fixture.ledger / "bad").write_text("bad")
    result = build(fixture)
    assert result["outcome"] == "built", result
    assert result["readiness_mode"] == "next_day_t00"
    partition = fixture.replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    assert "readiness_evidence" not in json.loads((partition / "manifest.json").read_text())


def test_no_evidence_custom_root_does_not_use_defaults(fixture):
    result = build_replay_for_symbol(VENUE, SYMBOL, DATE, fixture.raw, fixture.replay, schema_version=2, check_repartition_readiness=True)
    assert result["outcome"] == "deferred_not_ready"


@pytest.mark.parametrize("field", sorted({"contract_version", "session_id", "start_sha256", "status", "ingestion_stopped_at", "finalized_at", "shutdown_reason", "producers_stopped", "storage", "raw_files", "unowned_raw_files", "errors"}))
def test_missing_final_fields_defer(fixture, field):
    payload = copy.deepcopy(fixture.final)
    del payload[field]
    write_payload(final_path(fixture), payload)
    assert build(fixture)["outcome"] == "deferred_not_ready"


@pytest.mark.parametrize("mutation", ["failed", "producer", "drop", "write_error", "counts", "queue", "close", "schedule", "compression", "late", "reversed", "root", "traversal", "version", "start_hash"])
def test_dirty_contradictory_evidence_defers(fixture, mutation):
    payload = copy.deepcopy(fixture.final)
    if mutation == "failed": payload["status"] = "failed"
    elif mutation == "producer": payload["producers_stopped"] = False
    elif mutation in {"drop", "write_error", "counts", "queue"}:
        field = {"drop": "drop_count", "write_error": "write_errors", "counts": "write_count", "queue": "queue_size"}[mutation]
        next(iter(payload["storage"]["writers"].values()))[field] = 1
    elif mutation == "close": payload["storage"]["files_closed"] = False
    elif mutation == "schedule": payload["storage"]["errors"] = ["scheduling failed"]
    elif mutation == "compression": payload["storage"]["compression"]["failed"] = 1
    elif mutation == "late": payload["finalized_at"] = "2026-01-01T23:30:00.000000Z"
    elif mutation == "reversed": payload["finalized_at"] = "2025-12-31T00:00:00.000000Z"
    elif mutation == "root":
        write_payload(fixture.ledger / "head.json", {**json.loads((fixture.ledger / "head.json").read_text()), "data_root_identity": "0"*64})
    elif mutation == "traversal": payload["raw_files"]["../escaped"] = next(iter(payload["raw_files"].values()))
    elif mutation == "version": payload["contract_version"] = 999
    elif mutation == "start_hash": payload["start_sha256"] = "0"*64
    write_payload(final_path(fixture), payload)
    assert build(fixture)["outcome"] == "deferred_not_ready"


@pytest.mark.parametrize("kind", ["open", "truncated", "symlink", "directory", "duplicate_key", "pending", "missing_start", "tail_deleted", "raw_changed", "raw_symlink", "extra_raw", "unknown_file"])
def test_unsafe_inventory_defers(fixture, kind):
    path = final_path(fixture)
    if kind == "open": path.unlink()
    elif kind == "truncated": path.write_text("{")
    elif kind == "symlink":
        copy_path = fixture.base / "copy.json"
        copy_path.write_bytes(path.read_bytes()); path.unlink(); path.symlink_to(copy_path)
    elif kind == "directory": path.unlink(); path.mkdir()
    elif kind == "duplicate_key": path.write_text('{"status":"closed_clean","status":"failed"}')
    elif kind == "pending": (fixture.ledger / ".pending-interrupted").write_text("{}")
    elif kind == "missing_start": (fixture.ledger / f"{fixture.session.start['session_id']}.start.json").unlink()
    elif kind == "tail_deleted": (fixture.ledger / f"{fixture.successor.start['session_id']}.start.json").unlink()
    elif kind == "raw_changed": (fixture.raw / next(iter(fixture.files))).write_bytes(b"changed")
    elif kind == "raw_symlink":
        raw_path = fixture.raw / next(iter(fixture.files)); raw_path.unlink(); raw_path.symlink_to(fixture.base / "missing")
    elif kind == "extra_raw":
        raw_path = fixture.raw / next(iter(fixture.files)); raw_path.with_name(f"{DATE}T07.jsonl.zst").write_bytes(raw_path.read_bytes())
    elif kind == "unknown_file": (fixture.ledger / "attestation.json").write_text("{}")
    assert build(fixture)["outcome"] == "deferred_not_ready"


def test_later_selection_blocks_and_unknown_open_handoff_blocks(fixture):
    with patch.object(coverage, "utc_now", return_value="2026-01-01T07:00:00.000000Z"):
        s = coverage.RecordingSession(fixture.ledger, fixture.raw, {VENUE: [SYMBOL]})
    s.release()
    assert build(fixture)["outcome"] == "deferred_not_ready"


def test_wrong_dataset_and_wrong_symbol(fixture, tmp_path):
    other = tmp_path / "other_raw"; other.mkdir()
    with pytest.raises(coverage.CoverageError):
        coverage.retirement_proof(fixture.ledger, other, VENUE, SYMBOL, DATE)
    with pytest.raises(coverage.CoverageError):
        coverage.retirement_proof(fixture.ledger, fixture.raw, VENUE, "ETHUSDT", DATE)


@pytest.mark.parametrize("phase", ["streaming", "after_finalize", "same_byte_replace"])
def test_mutation_aborts_without_publication(fixture, monkeypatch, phase):
    import pipeline.build_replay_store as builder
    from stores.replay_writer import ReplayWriter
    path = final_path(fixture)
    def mutate():
        if phase == "same_byte_replace":
            temp = fixture.base / "replacement"; temp.write_bytes(path.read_bytes()); temp.replace(path)
        else:
            payload = json.loads(path.read_text()); payload["shutdown_reason"] = "changed"; write_payload(path, payload)
    if phase == "streaming":
        original = builder._stream_repartitioned_depth_records
        def stream(*args, **kwargs):
            yield from original(*args, **kwargs)
            mutate()
        monkeypatch.setattr(builder, "_stream_repartitioned_depth_records", stream)
    else:
        original = ReplayWriter.finalize_staging
        def finalize(self):
            result = original(self); mutate(); return result
        monkeypatch.setattr(ReplayWriter, "finalize_staging", finalize)
    result = build(fixture)
    assert result["outcome"] == "failed", result
    assert not list(fixture.replay.rglob("date=*"))
    assert not list(fixture.replay.rglob(".staging*"))
    assert not list(fixture.replay.rglob(".backup*"))


@pytest.mark.parametrize("operation", ["start", "final"])
def test_publication_lock_wait_is_bounded(fixture, monkeypatch, operation):
    sleeps = []
    monkeypatch.setattr(coverage.time, "sleep", sleeps.append)
    monkeypatch.setattr(coverage.time, "monotonic", lambda: 0)
    with coverage.ledger_lock(fixture.ledger):
        with pytest.raises(coverage.CoverageError, match="41/41 attempts"):
            if operation == "start":
                coverage.RecordingSession(fixture.ledger, fixture.raw, {VENUE: [SYMBOL]})
            else:
                finish_successor(fixture)
    assert sleeps == [0.05] * 40
    assert not (fixture.ledger / f"{fixture.successor.start['session_id']}.final.json").exists()


@pytest.mark.parametrize("operation", ["fsync", "link", "replace"])
def test_start_publication_failures_never_authorize_ingestion(tmp_path, monkeypatch, operation):
    raw = tmp_path / "raw"; raw.mkdir()
    def fail(*args, **kwargs): raise OSError("injected publication failure")
    monkeypatch.setattr(coverage.os, operation, fail)
    with pytest.raises(OSError):
        coverage.RecordingSession(tmp_path / "ledger", raw, {VENUE: [SYMBOL]})


def test_immutable_final_cannot_be_overwritten(fixture):
    before = final_path(fixture).read_bytes()
    with pytest.raises(FileExistsError):
        fixture.session.finish(fixture.storage, fixture.files, producers_stopped=True, ingestion_stopped_at="2026-01-01T06:49:00.000000Z", errors=[], shutdown_reason="second")
    assert final_path(fixture).read_bytes() == before


def test_embedded_provenance_failure_is_not_valid_partition(fixture):
    assert build(fixture)["outcome"] == "built"
    partition = fixture.replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    path = partition / "manifest.json"
    manifest = json.loads(path.read_text()); manifest["readiness_evidence"]["horizon"] = "2030-01-01T00:00:00.000000Z"
    write_payload(path, manifest)
    assert not validate_partition(partition)


@pytest.mark.asyncio
async def test_storage_drain_close_compress_and_reject_post_shutdown(tmp_path, monkeypatch):
    import storage
    raw = tmp_path / "raw"; raw.mkdir()
    monkeypatch.setattr(storage, "DATA_ROOT", raw)
    monkeypatch.setattr(storage, "WRITER_FLUSH_INTERVAL_SEC", 0.01)
    manager = storage.StorageManager()
    await manager.write_record(VENUE, SYMBOL, "depth_v2", {"record_type": "test"})
    result = await manager.shutdown()
    assert result["queues_drained"] and result["files_closed"] and result["compression_drained"]
    assert len(manager.coverage_raw_files()) == 1
    with pytest.raises(RuntimeError):
        await manager.write_record(VENUE, SYMBOL, "depth_v2", {})


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["write", "close", "schedule", "compression", "writer_exception", "timeout", "drop"])
async def test_storage_faults_never_certify_clean(tmp_path, monkeypatch, fault):
    import storage
    raw = tmp_path / "raw"; raw.mkdir()
    monkeypatch.setattr(storage, "DATA_ROOT", raw)
    monkeypatch.setattr(storage, "WRITER_FLUSH_INTERVAL_SEC", 0.01)
    manager = storage.StorageManager()
    writer = await manager.get_writer(VENUE, SYMBOL, "depth_v2")
    if fault == "write":
        async def fail_handle(*args): raise OSError("write failure")
        monkeypatch.setattr(manager.rotator, "get_file_handle", fail_handle)
    elif fault == "close": manager.rotator.closure_errors.append("file close failed")
    elif fault == "schedule":
        async def fail_enqueue(*args): raise OSError("schedule failure")
        monkeypatch.setattr(manager.compression_manager, "enqueue", fail_enqueue)
    elif fault == "compression":
        async def fail_compress(*args): raise OSError("compression failure")
        monkeypatch.setattr(manager.compression_manager, "_compress_file", fail_compress)
    elif fault == "writer_exception":
        manager.writer_tasks[next(iter(manager.writer_tasks))].cancel()
        await asyncio.gather(*manager.writer_tasks.values(), return_exceptions=True)
    elif fault == "timeout":
        original = storage.asyncio.wait_for
        async def timed_out(awaitable, *, timeout):
            if timeout == 10 and isinstance(awaitable, asyncio.Future):
                awaitable.cancel(); raise asyncio.TimeoutError
            return await original(awaitable, timeout=timeout)
        monkeypatch.setattr(storage.asyncio, "wait_for", timed_out)
    elif fault == "drop": writer.drop_count = 1
    await manager.write_record(VENUE, SYMBOL, "depth_v2", {"record_type": "test"})
    result = await manager.shutdown()
    assert not (result["queues_drained"] and result["files_closed"] and result["compression_drained"])


@pytest.mark.asyncio
@pytest.mark.parametrize("native_fault", [None, "timeout", "exception"])
async def test_recorder_start_precedes_feeds_and_native_failure_prevents_clean(tmp_path, monkeypatch, native_fault):
    import recorder
    import storage
    from unittest.mock import AsyncMock, Mock
    raw, ledger = tmp_path / "raw", tmp_path / "ledger"
    raw.mkdir()
    monkeypatch.setattr(recorder, "DATA_ROOT", raw)
    monkeypatch.setattr(storage, "DATA_ROOT", raw)
    monkeypatch.setattr(storage, "WRITER_FLUSH_INTERVAL_SEC", 0.01)
    monkeypatch.setattr(recorder.config, "COVERAGE_ROOT", ledger)
    for name in ("native_depth_recorder", "native_trade_recorder", "recording_session"):
        monkeypatch.setattr(recorder, name, None)
    monkeypatch.setattr(recorder, "session_errors", [])
    health = Mock()
    health.write_heartbeat = AsyncMock()
    health.heartbeat_task = AsyncMock()
    async def initialize():
        recorder.storage_manager = storage.StorageManager()
        recorder.health_monitor = health
        recorder.disk_monitor = SimpleNamespace(shutdown=AsyncMock())
        recorder.shutdown_event = asyncio.Event()
        return {VENUE: [SYMBOL]}
    monkeypatch.setattr(recorder, "initialize", initialize)
    monkeypatch.setattr(recorder, "disk_check_task", AsyncMock())
    monkeypatch.setattr(recorder, "_update_drop_stats_task", AsyncMock())
    monkeypatch.setattr(recorder, "metadata_task", AsyncMock())
    monkeypatch.setattr(recorder, "_persist_startup_coverage", Mock())
    started = []
    class Feed:
        channel = "depth_v2"
        def __init__(self, **kwargs): self._tasks = []
        async def run(self, symbols):
            records = coverage.snapshot(ledger)
            assert coverage.validate_chain(records, records["head.json"]["payload"]["data_root_identity"])[-1]["universe"] == {VENUE: [SYMBOL]}
            assert not any(name.endswith(".final.json") for name in records)
            await recorder.storage_manager.write_record(VENUE, SYMBOL, self.channel, {"record_type": "fixture"})
            started.append(self.channel)
            if len(started) == 2: recorder.shutdown_event.set()
        async def shutdown(self):
            if self.channel == "depth_v2" and native_fault == "timeout": raise asyncio.TimeoutError
            if self.channel == "depth_v2" and native_fault == "exception": raise RuntimeError("native fault")
        def get_venue_diagnostics(self): return {}
    class TradeFeed(Feed): channel = "trade_v2"
    monkeypatch.setattr(recorder, "BinanceNativeDepthRecorder", Feed)
    monkeypatch.setattr(recorder, "BinanceNativeTradeRecorder", TradeFeed)
    # Keep signal handlers local to this synthetic main invocation.
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", Mock())
    await recorder.main()
    assert sorted(started) == ["depth_v2", "trade_v2"]
    finals = list(ledger.glob("*.final.json"))
    assert len(finals) == 1
    final = json.loads(finals[0].read_text())
    assert final["status"] == ("closed_clean" if native_fault is None else "failed")
    assert final["producers_stopped"] is (native_fault is None)


def test_strict_horizon_and_future_selection_are_rejected(fixture):
    proof = coverage.retirement_proof(fixture.ledger, fixture.raw, VENUE, SYMBOL, DATE, complete_next_day=True)
    assert proof["horizon"] == "2026-01-03T00:00:00.000000Z"
    with patch.object(coverage, "datetime") as clock:
        clock.strptime = datetime.strptime
        clock.now.return_value = datetime(2026, 1, 2, 0, 30, tzinfo=timezone.utc)
        with pytest.raises(coverage.CoverageError, match="horizon"):
            coverage.retirement_proof(fixture.ledger, fixture.raw, VENUE, SYMBOL, DATE)


def test_clean_excluding_successor_can_extend_chain(fixture):
    f = fixture
    with patch.object(coverage, "utc_now", return_value="2026-01-01T08:00:00.000000Z"):
        f.successor.finish({**f.storage, "writers": {}}, {}, producers_stopped=True, ingestion_stopped_at="2026-01-01T07:59:00.000000Z", errors=[], shutdown_reason="SIGTERM")
    with patch.object(coverage, "utc_now", return_value="2026-01-01T08:01:00.000000Z"):
        third = coverage.RecordingSession(f.ledger, f.raw, {VENUE: ["ETHUSDT"]})
    third.release()
    assert build(f)["outcome"] == "built"


def test_plain_compressed_ambiguity_rejected(fixture):
    compressed = fixture.raw / next(iter(fixture.files))
    compressed.with_suffix("").write_bytes(b"ambiguous")
    assert build(fixture)["outcome"] == "deferred_not_ready"


def test_scan_bound_is_fail_closed(fixture, monkeypatch):
    monkeypatch.setattr(coverage, "MAX_RECORDS", 2)
    assert build(fixture)["outcome"] == "deferred_not_ready"


def test_evidence_io_failure_is_failed_without_retry(fixture, monkeypatch):
    def fail(*args): raise OSError("injected I/O error")
    monkeypatch.setattr(coverage, "snapshot", fail)
    assert build(fixture)["outcome"] == "failed"


def test_daily_reports_include_evidence(fixture):
    from pipeline.daily_build import run_backlog
    # The CLI derives exact scales from fixture-local exchangeInfo.
    path = fixture.raw / VENUE / "exchangeinfo" / "EXCHANGEINFO" / DATE / f"{DATE}T00.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"symbols": [{"symbol": SYMBOL, "baseAsset": "ADA", "quoteAsset": "USDT", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.01"}]}]}) + "\n")
    report, code = run_backlog(newest_date=DATE, backlog_days=1, max_build_dates=1, schema_version=2, data_root=fixture.raw, replay_root=fixture.replay, report_root=fixture.base / "reports", coverage_root=fixture.ledger)
    assert code == 0, report
    daily = json.loads((fixture.base / "reports" / f"daily_build_{DATE}.json").read_text())
    assert daily["partition_results"][0]["readiness_mode"] == "clean_retirement_evidence"
    assert len(daily["partition_results"][0]["readiness_evidence_sha256"]) == 64


def test_preexisting_raw_bytes_cannot_be_certified(fixture):
    payload = copy.deepcopy(fixture.final)
    payload["unowned_raw_files"] = [next(iter(payload["raw_files"]))]
    write_payload(final_path(fixture), payload)
    assert build(fixture)["outcome"] == "deferred_not_ready"


@pytest.mark.asyncio
async def test_watchdog_forced_exit_leaves_open_record(tmp_path, monkeypatch):
    import recorder
    import threading
    raw = tmp_path / "raw"; raw.mkdir()
    session = coverage.RecordingSession(tmp_path / "coverage", raw, {VENUE: [SYMBOL]})
    monkeypatch.setattr(recorder, "recording_session", session)
    monkeypatch.setattr(recorder, "native_depth_recorder", None)
    monkeypatch.setattr(recorder, "native_trade_recorder", None)
    monkeypatch.setattr(recorder, "health_monitor", None)
    monkeypatch.setattr(recorder, "disk_monitor", None)
    monkeypatch.setattr(recorder, "shutdown_event", asyncio.Event())
    callback = []
    class Timer:
        def __init__(self, seconds, fn): callback.append(fn)
        def start(self): pass
        def cancel(self): pass
    monkeypatch.setattr(threading, "Timer", Timer)
    def exit_process(code): raise SystemExit(code)
    monkeypatch.setattr(coverage.os, "_exit", exit_process)
    async def storage_shutdown(): callback[0]()
    monkeypatch.setattr(recorder, "storage_manager", SimpleNamespace(shutdown=storage_shutdown))
    try:
        with pytest.raises(SystemExit): await recorder.shutdown([])
        assert list(session.root.glob("*.start.json"))
        assert not list(session.root.glob("*.final.json"))
    finally:
        session.release()


def test_explicit_transferred_bundle_is_portable_read_only(fixture, tmp_path):
    import shutil
    bundle = tmp_path / "transferred"
    shutil.copytree(fixture.raw, bundle / "raw")
    shutil.copytree(fixture.ledger, bundle / "ledger")
    f = SimpleNamespace(raw=bundle / "raw", ledger=bundle / "ledger", replay=bundle / "replay")
    assert build(f)["outcome"] == "built"
    # A read-only portable bundle cannot silently become a second recorder ledger.
    with pytest.raises(coverage.CoverageError, match="different local raw root"):
        coverage.RecordingSession(f.ledger, f.raw, {VENUE: [SYMBOL]})


def test_final_directory_fsync_failure_leaves_untrusted_pending(tmp_path, monkeypatch):
    raw = tmp_path / "raw"; raw.mkdir()
    session = coverage.RecordingSession(tmp_path / "ledger", raw, {VENUE: [SYMBOL]})
    storage = {"queues_drained": True, "files_closed": True, "compression_drained": True, "writers": {}, "compression": {"queued": 0, "active": 0, "failed": 0, "last_error": None}, "errors": []}
    def fail(path): raise OSError("final directory fsync failed")
    monkeypatch.setattr(coverage, "_fsync_dir", fail)
    try:
        with pytest.raises(OSError):
            session.finish(storage, {}, producers_stopped=True, ingestion_stopped_at=coverage.utc_now(), errors=[], shutdown_reason="SIGTERM")
        with pytest.raises(coverage.CoverageError): coverage.snapshot(session.root)
    finally:
        session.release()


def test_native_payload_full_l2_equivalence_through_retirement(tmp_path, monkeypatch):
    import pipeline.build_replay_store as builder
    from validation.validate_catalog_equivalence import validate_catalog_equivalence
    f = make_fixture(tmp_path / "fixture", depth_updates=True)
    monkeypatch.setattr(builder, "DATA_ROOT", f.raw)
    monkeypatch.setattr(builder, "COVERAGE_ROOT", f.ledger)
    path = f.raw / VENUE / "exchangeinfo" / "EXCHANGEINFO" / DATE / f"{DATE}T00.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"symbols": [{"symbol": SYMBOL, "baseAsset": "ADA", "quoteAsset": "USDT", "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.01"}]}]}) + "\n")
    report = validate_catalog_equivalence(date=DATE, symbols=[SYMBOL], venues=[VENUE], data_root=f.raw, work_root=tmp_path / "work", old_catalog_root=tmp_path / "old", replay_root=f.replay, new_catalog_root=tmp_path / "new", profile="full_l2", schema_version=2, overwrite=True)
    assert report["status"] == "passed", report


def test_audit_rejects_corrupt_retirement_provenance(fixture):
    from validation.audit_replay_store import audit_replay_partition
    assert build(fixture)["outcome"] == "built"
    path = fixture.replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}" / "manifest.json"
    manifest = json.loads(path.read_text())
    del manifest["readiness_evidence"]
    write_payload(path, manifest)
    report = audit_replay_partition(fixture.replay, venue=VENUE, symbol=SYMBOL, date=DATE)
    assert report["readiness_evidence_validation"]["valid"] is False


def test_retirement_and_ordinary_readiness_emit_identical_parquet(fixture):
    result = build(fixture)
    assert result['outcome'] == 'built'
    retired = fixture.replay / f'venue={VENUE}' / f'symbol={SYMBOL}' / f'date={DATE}'
    day = '2026-01-02'
    path = fixture.raw / VENUE / 'depth_v2' / SYMBOL / day / f'{day}T00.jsonl.zst'
    path.parent.mkdir()
    path.write_bytes(zstd.ZstdCompressor().compress(b''))
    other = SimpleNamespace(raw=fixture.raw, ledger=fixture.ledger, replay=fixture.base / 'ordinary_replay')
    assert build(other)['outcome'] == 'built'
    ordinary = other.replay / f'venue={VENUE}' / f'symbol={SYMBOL}' / f'date={DATE}'
    for name in ('depth.parquet', 'trades.parquet'):
        assert (retired / name).read_bytes() == (ordinary / name).read_bytes()


def test_wrong_venue_unknown_universe_and_shared_owners_defer(fixture):
    with pytest.raises(coverage.CoverageError):
        coverage.retirement_proof(fixture.ledger, fixture.raw, 'BINANCE_USDTF', SYMBOL, DATE)
    path = fixture.ledger / f"{fixture.session.start['session_id']}.start.json"
    payload = json.loads(path.read_text()); payload['universe_digest'] = '0'*64
    write_payload(path, payload)
    assert build(fixture)['outcome'] == 'deferred_not_ready'


def finish_successor(f):
    return f.successor.finish({**f.storage, "writers": {}}, {}, producers_stopped=True,
                              ingestion_stopped_at=coverage.utc_now(), errors=[], shutdown_reason="SIGTERM")


def partition_path(f, symbol=SYMBOL):
    return f.replay / f"venue={VENUE}" / f"symbol={symbol}" / f"date={DATE}"


def populated_fixture(base, *, before=80, after=80, files_per_session=96):
    """Synthetic unrelated history; no real feeds or existing raw data.

    Whole canonical marker hashes are rebuilt only for this invented ledger.
    Three symbols share an owner; no history session owns their raw inputs.
    """
    from datetime import timedelta
    import shutil
    f = make_fixture(base)
    symbols = [SYMBOL, "UNIUSDT", "ETHUSDT"]
    owner = copy.deepcopy(f.session.start)
    owner["universe"] = {VENUE: sorted(symbols)}
    owner["universe_digest"] = coverage.digest(owner["universe"])
    final = copy.deepcopy(f.final)
    for symbol in symbols[1:]:
        for channel in ("depth_v2", "trade_v2"):
            shutil.copytree(f.raw / VENUE / channel / SYMBOL, f.raw / VENUE / channel / symbol)
        final["raw_files"].update({p.replace(f"/{SYMBOL}/", f"/{symbol}/"): i for p, i in f.files.items()})
    rows = []
    def history(index, timestamp):
        start = {**copy.deepcopy(owner), "session_id": f"{index:032x}",
                 "started_at": timestamp, "universe": {VENUE: ["HISTUSDT"]}}
        start["universe_digest"] = coverage.digest(start["universe"])
        end = {**copy.deepcopy(final), "session_id": start["session_id"],
               "ingestion_stopped_at": timestamp, "finalized_at": timestamp,
               "raw_files": {}, "unowned_raw_files": [], "storage": {**f.storage, "writers": {}}}
        # Distinct canonical hours in each record, unrelated to fixture scope.
        for n in range(files_per_session):
            dt = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n)
            path = f"{VENUE}/depth_v2/HISTUSDT/{dt:%Y-%m-%d}/{dt:%Y-%m-%dT%H}.jsonl.zst"
            end["raw_files"][path] = {"sha256": coverage.digest([index, n]), "size_bytes": 128}
        return start, end
    for i in range(before):
        timestamp = (datetime(2025, 12, 1, tzinfo=timezone.utc) + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        rows.append(history(i + 1, timestamp))
    rows += [(owner, final), (copy.deepcopy(f.successor.start), None)]
    # The first start after the strict horizon proves the segment is complete.
    rows.append(history(before + 1, "2026-01-03T00:00:00.000000Z"))
    for i in range(after):
        timestamp = (datetime(2026, 1, 4, tzinfo=timezone.utc) + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        rows.append(history(before + 2 + i, timestamp))
    previous = None
    for sequence, (start, end) in enumerate(rows, 1):
        start.update(sequence=sequence, previous=previous)
        sha = coverage.digest(start)
        previous = {"session_id": start["session_id"], "sha256": sha}
        write_payload(f.ledger / f"{start['session_id']}.start.json", start)
        if end is not None:
            end["start_sha256"] = sha
            write_payload(f.ledger / f"{start['session_id']}.final.json", end)
    head = json.loads((f.ledger / "head.json").read_text())
    head.update(count=len(rows), session_id=previous["session_id"], start_sha256=previous["sha256"])
    write_payload(f.ledger / "head.json", head)
    f.symbols = symbols
    return f


def test_large_history_multiple_partitions_store_only_relevant_records(tmp_path):
    f = populated_fixture(tmp_path)
    records = coverage.snapshot(f.ledger)
    ledger_bytes = sum(e["size_bytes"] for e in records.values())
    assert len(records) > 300 and ledger_bytes > 2_000_000
    for symbol in f.symbols:
        result = build_replay_for_symbol(VENUE, symbol, DATE, f.raw, f.replay,
                                        schema_version=2, price_scale=2, qty_scale=2,
                                        check_repartition_readiness=True, coverage_root=f.ledger)
        assert result["outcome"] == "built", result
        path = partition_path(f, symbol)
        manifest_text = (path / "manifest.json").read_text()
        manifest = json.loads(manifest_text)
        proof = manifest["readiness_evidence"]
        assert len(proof["records"]) == 4  # owner start/final, excluding start, boundary start
        assert all("file_key" not in e for e in proof["records"].values())
        assert len(manifest_text) < 60_000
        assert manifest_text.count('"readiness_evidence":') == 1
        assert manifest_text.count('"shutdown_reason":') == 1
        assert validate_partition(path)
        assert (result["depth_count"], result["trade_count"]) == (7, 7)
    # Adding arbitrarily large unrelated tails cannot grow the same proof.
    proof = coverage.retirement_proof(f.ledger, f.raw, VENUE, SYMBOL, DATE)
    later = coverage.RecordingSession(f.ledger, f.raw, {VENUE: ["BTCUSDT"]})
    later.release()
    assert coverage.retirement_proof(f.ledger, f.raw, VENUE, SYMBOL, DATE) == proof
    assert build(f)["outcome"] == "skipped_valid"
    assert not list(f.replay.rglob(".staging*")) and not list(f.replay.rglob(".backup*"))


@pytest.mark.parametrize("fault", ["owner_final", "successor", "boundary", "previous_hash", "raw_hash", "handoff", "duplicate", "version"])
def test_compact_proof_rejects_missing_or_conflicting_material(tmp_path, fault):
    f = populated_fixture(tmp_path, before=2, after=2, files_per_session=1)
    proof = coverage.retirement_proof(f.ledger, f.raw, VENUE, SYMBOL, DATE)
    records = proof["records"]
    starts = sorted((n for n in records if n.endswith(".start.json")), key=lambda n: records[n]["payload"]["sequence"])
    if fault == "owner_final": del records[f"{proof['session_id']}.final.json"]
    elif fault == "successor": del records[starts[1]]
    elif fault == "boundary": del records[starts[-1]]
    elif fault == "raw_hash": next(iter(proof["raw_files"].values()))["sha256"] = "0" * 64
    elif fault in {"previous_hash", "handoff"}:
        entry = records[starts[1]] if fault == "previous_hash" else records[f"{proof['session_id']}.final.json"]
        if fault == "previous_hash": entry["payload"]["previous"]["sha256"] = "0" * 64
        else: entry["payload"]["finalized_at"] = "2026-01-01T07:00:00.000000Z"
        entry["sha256"] = coverage.digest(entry["payload"])
        entry["size_bytes"] = len(json.dumps(entry["payload"], sort_keys=True, separators=(",", ":")).encode())
    elif fault == "duplicate": records["head.json"] = {k: v for k, v in coverage.snapshot(f.ledger)["head.json"].items() if k != "file_key"}
    elif fault == "version": proof["contract_version"] = 999
    proof["sha256"] = coverage.digest({k: v for k, v in proof.items() if k != "sha256"})
    with pytest.raises(coverage.CoverageError): coverage.validate_provenance(proof, VENUE, SYMBOL, DATE)


def test_version_one_existing_manifest_remains_valid_and_reusable(fixture):
    assert build(fixture)["outcome"] == "built"
    path = partition_path(fixture) / "manifest.json"
    manifest = json.loads(path.read_text())
    proof = manifest["readiness_evidence"]
    proof["contract_version"] = 1
    proof["records"] = coverage.snapshot(fixture.ledger)
    proof["sha256"] = coverage.digest({k: v for k, v in proof.items() if k != "sha256"})
    manifest["readiness_contract_version"] = 1
    manifest["integrity"].pop("readiness_evidence_sha256")
    manifest["integrity"]["readiness_evidence"] = proof
    write_payload(path, manifest)
    original = path.read_bytes()
    assert validate_partition(path.parent)
    assert build(fixture)["outcome"] == "skipped_valid"
    assert path.read_bytes() == original


def test_read_only_bundle_and_saved_partition_reuse(fixture, tmp_path):
    import shutil
    assert build(fixture)["outcome"] == "built"
    bundle = tmp_path / "bundle"
    f = SimpleNamespace(raw=bundle / "raw", ledger=bundle / "ledger", replay=bundle / "replay")
    for source, dest in ((fixture.raw, f.raw), (fixture.ledger, f.ledger), (fixture.replay, f.replay)):
        shutil.copytree(source, dest)
    for root in (f.raw, f.ledger):
        for path in root.rglob("*"):
            path.chmod(0o500 if path.is_dir() else 0o400)
        root.chmod(0o500)
    before = {p: p.read_bytes() for p in f.ledger.iterdir()}
    try:
        assert build(f)["outcome"] == "skipped_valid"
        f.replay = bundle / "fresh_replay"
        assert build(f)["outcome"] == "built"
        assert validate_partition(partition_path(f))
        assert {p: p.read_bytes() for p in f.ledger.iterdir()} == before
    finally:
        for root in (f.raw, f.ledger):
            root.chmod(0o700)
            for path in root.rglob("*"):
                path.chmod(0o700 if path.is_dir() else 0o600)


@pytest.mark.parametrize("operation", ["start", "final"])
def test_recorder_publication_waits_for_short_guard(fixture, monkeypatch, operation):
    guard = coverage.ledger_lock(fixture.ledger)
    guard.__enter__()
    waits = []
    def release_on_wait(seconds):
        waits.append(seconds)
        guard.__exit__(None, None, None)
    monkeypatch.setattr(coverage.time, "sleep", release_on_wait)
    if operation == "start":
        successor = coverage.RecordingSession(fixture.ledger, fixture.raw, {VENUE: ["BTCUSDT"]})
        successor.release()
        assert (fixture.ledger / f"{successor.start['session_id']}.start.json").exists()
    else:
        assert finish_successor(fixture)["status"] == "closed_clean"
    assert waits == [0.05]


def test_shutdown_and_successor_during_build_preserve_clean_evidence(fixture, monkeypatch):
    import recorder
    from stores.replay_writer import ReplayWriter
    from unittest.mock import AsyncMock
    f = fixture
    monkeypatch.setattr(recorder, "recording_session", f.successor)
    for name in ("native_depth_recorder", "native_trade_recorder", "health_monitor", "disk_monitor"):
        monkeypatch.setattr(recorder, name, None)
    monkeypatch.setattr(recorder, "session_errors", [])
    monkeypatch.setattr(recorder, "shutdown_event", asyncio.Event())
    monkeypatch.setattr(recorder, "storage_manager", SimpleNamespace(
        shutdown=AsyncMock(return_value={**f.storage, "writers": {}}),
        coverage_raw_files=lambda: {}, coverage_unowned_files=lambda: []))
    # Fake watchdog checks that publication remains covered by the time limit.
    import threading
    timers = []
    class Timer:
        def __init__(self, *args): self.cancelled = False; timers.append(self)
        def start(self): pass
        def cancel(self): self.cancelled = True
    monkeypatch.setattr(threading, "Timer", Timer)
    original_finish = f.successor.finish
    def finish(*args, **kwargs):
        assert not timers[0].cancelled
        return original_finish(*args, **kwargs)
    monkeypatch.setattr(f.successor, "finish", finish)
    original = ReplayWriter.finalize_staging
    def finalize(writer):
        manifest = original(writer)
        asyncio.run(recorder.shutdown([]))
        successor = coverage.RecordingSession(f.ledger, f.raw, {VENUE: ["ETHUSDT"]})
        successor.release()
        return manifest
    with monkeypatch.context() as m:
        m.setattr(ReplayWriter, "finalize_staging", finalize)
        result = build(f)
    assert result["outcome"] == "failed" and "changed" in str(result["errors"])
    final = json.loads((f.ledger / f"{f.successor.start['session_id']}.final.json").read_text())
    assert final["status"] == "closed_clean" and timers[0].cancelled
    assert not partition_path(f).exists() and not list(f.replay.rglob(".staging*"))
    assert build(f)["outcome"] == "built"
    assert build(f)["outcome"] == "skipped_valid"


def test_raw_hashing_does_not_hold_ledger_lock(fixture, monkeypatch):
    original = coverage.file_identity
    count = 0
    def identity(path):
        nonlocal count
        if path.is_relative_to(fixture.raw):
            with coverage.ledger_lock(fixture.ledger, exclusive=True): count += 1
        return original(path)
    monkeypatch.setattr(coverage, "file_identity", identity)
    assert build(fixture)["outcome"] == "built"
    assert count > 0


def test_publication_rename_is_guarded(fixture, monkeypatch):
    from stores import replay_writer
    original = replay_writer.os.replace
    seen = []
    def replace(source, target):
        if Path(target) == partition_path(fixture):
            with pytest.raises(coverage.CoverageError, match="busy"):
                with coverage.ledger_lock(fixture.ledger, exclusive=True): pass
            seen.append(True)
        return original(source, target)
    monkeypatch.setattr(replay_writer.os, "replace", replace)
    assert build(fixture)["outcome"] == "built"
    assert seen == [True]


@pytest.mark.parametrize("kind", ["directory", "empty", "plain", "ambiguous"])
def test_present_incomplete_next_day_never_uses_retirement(fixture, kind):
    day = "2026-01-02"
    path = fixture.raw / VENUE / "depth_v2" / SYMBOL / day
    path.mkdir()
    if kind == "directory": (path / f"{day}T00.jsonl.zst").mkdir()
    elif kind in {"plain", "ambiguous"}:
        (path / f"{day}T00.jsonl").write_text("{}\n")
        if kind == "ambiguous": (path / f"{day}T00.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress(b"{}\n"))
    result = build(fixture)
    assert result["outcome"] == "deferred_not_ready", result
    assert result.get("readiness_mode") != "clean_retirement_evidence"


@pytest.mark.parametrize("version", [1, 2])
def test_reuse_refuses_changed_persisted_evidence(fixture, version):
    assert build(fixture)["outcome"] == "built"
    path = partition_path(fixture) / "manifest.json"
    manifest = json.loads(path.read_text())
    if version == 1:
        proof = manifest["readiness_evidence"]
        proof.update(contract_version=1, records=coverage.snapshot(fixture.ledger))
        proof["sha256"] = coverage.digest({k: v for k, v in proof.items() if k != "sha256"})
        manifest["readiness_contract_version"] = 1
        manifest["integrity"].pop("readiness_evidence_sha256")
        manifest["integrity"]["readiness_evidence"] = proof
        write_payload(path, manifest)
    before = path.read_bytes()
    payload = json.loads(final_path(fixture).read_text())
    payload["shutdown_reason"] = "changed after publication"
    write_payload(final_path(fixture), payload)
    result = build(fixture)
    assert result["outcome"] != "skipped_valid" and "disagrees" in str(result["errors"])
    assert path.read_bytes() == before
    assert not list(fixture.replay.rglob(".backup*"))


def test_relevant_intermediate_final_cannot_be_omitted(fixture):
    f = fixture
    with patch.object(coverage, "utc_now", return_value="2026-01-01T08:00:00.000000Z"):
        finish_successor(f)
    with patch.object(coverage, "utc_now", return_value="2026-01-01T08:01:00.000000Z"):
        last = coverage.RecordingSession(f.ledger, f.raw, {VENUE: ["ETHUSDT"]})
    last.release()
    proof = coverage.retirement_proof(f.ledger, f.raw, VENUE, SYMBOL, DATE)
    assert len(proof["records"]) == 6
    del proof["records"][f"{f.successor.start['session_id']}.final.json"]
    proof["sha256"] = coverage.digest({k: v for k, v in proof.items() if k != "sha256"})
    with pytest.raises(coverage.CoverageError): coverage.validate_provenance(proof, VENUE, SYMBOL, DATE)


def test_ledger_reading_never_holds_shared_lock(fixture, monkeypatch):
    original = coverage.snapshot
    def snapshot(root):
        with coverage.ledger_lock(root, exclusive=True): pass
        return original(root)
    monkeypatch.setattr(coverage, "snapshot", snapshot)
    assert build(fixture)["outcome"] == "built"
    assert build(fixture)["outcome"] == "skipped_valid"


def test_compact_manifest_rejects_duplicate_or_missing_digest(fixture):
    assert build(fixture)["outcome"] == "built"
    path = partition_path(fixture) / "manifest.json"
    manifest = json.loads(path.read_text())
    duplicate = copy.deepcopy(manifest)
    duplicate["integrity"]["readiness_evidence"] = duplicate["readiness_evidence"]
    write_payload(path, duplicate)
    assert not validate_partition(path.parent)
    manifest["integrity"].pop("readiness_evidence_sha256")
    write_payload(path, manifest)
    assert not validate_partition(path.parent)
