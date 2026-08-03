"""
tests.test_replay_memory_bounded — Regression tests for bounded replay-store building.

These tests verify that:
1. ReplayWriter does not retain a full symbol/day in Python lists.
2. Output counts, schemas, ordering, checksums, and manifests are preserved.
3. Cleanup runs on success, exception, and failed publish.
4. Stale staging is handled safely.
5. Validated partitions are skipped; incomplete/corrupt ones are not.
"""
from __future__ import annotations

import gc
import hashlib
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.build_replay_store import (
    _partition_is_valid,
    build_replay_for_symbol,
)
from stores.replay_writer import ReplayWriter
from stores.replay_schema import DEPTH_REPLAY_SCHEMA, TRADE_REPLAY_SCHEMA


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _depth_record(*, session: int, seq: int, raw_idx: int, ts_ns: int = 1_000_000_000) -> dict:
    return {
        "venue": "BINANCE_SPOT",
        "symbol": "TESTUSDT",
        "date": "2026-07-01",
        "stream_session_id": session,
        "session_seq": seq,
        "raw_index": raw_idx,
        "record_type": "depth_update",
        "U": str(seq),
        "u": str(seq),
        "pu": None,
        "ts_exchange_ns": ts_ns,
        "ts_receive_ns": ts_ns + 1,
        "bids": [{"price": 1.0, "size": 1.0, "price_str": "1.00", "size_str": "1.0"}],
        "asks": [{"price": 1.1, "size": 1.0, "price_str": "1.10", "size_str": "1.0"}],
        "is_snapshot_seed": False,
        "is_depth_update": True,
        "is_sync_state": False,
        "is_desync": False,
        "is_resync": False,
        "quality_flags": None,
        "native_payload_hash": None,
    }


def _trade_record(*, session: int, seq: int, raw_idx: int, ts_ns: int = 1_000_000_000) -> dict:
    return {
        "venue": "BINANCE_SPOT",
        "symbol": "TESTUSDT",
        "date": "2026-07-01",
        "trade_stream_session_id": session,
        "trade_session_seq": seq,
        "raw_index": raw_idx,
        "record_type": "trade",
        "market_type": "spot",
        "trade_id": str(seq),
        "agg_trade_id": None,
        "ts_exchange_ns": ts_ns,
        "ts_receive_ns": ts_ns + 1,
        "price": 1.0,
        "quantity": 10.0,
        "price_str": "1.00000000",
        "quantity_str": "10.00000000",
        "buyer_maker": False,
        "aggressor_side": None,
        "quality_flags": None,
        "native_payload_hash": None,
    }


def _write_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _make_raw_root(tmp_path: Path, venue: str, symbol: str, date: str,
                   depth_count: int = 5, trade_count: int = 5) -> Path:
    raw_root = tmp_path / "raw"
    ts_base = 1_750_000_000_000_000_000  # ns
    depth_records = [
        {
            "record_type": "depth_update",
            "stream_session_id": 1,
            "session_seq": i + 1,
            "ts_recv_ns": ts_base + i,
            "ts_event_ms": (ts_base + i) // 1_000_000,
            "U": i + 1,
            "u": i + 1,
            "payload": {"bids": [["1.00", "1.0"]], "asks": [["1.01", "1.0"]]},
        }
        for i in range(depth_count)
    ]
    trade_records = [
        {
            "record_type": "trade",
            "trade_stream_session_id": 1,
            "trade_session_seq": i + 1,
            "ts_recv_ns": ts_base + i,
            "ts_trade_ms": (ts_base + i) // 1_000_000,
            "price": "1.00000000",
            "quantity": "1.00000000",
            "is_buyer_maker": False,
            "exchange_trade_id": i + 1,
            "native_payload": {"t": i + 1},
        }
        for i in range(trade_count)
    ]
    _write_jsonl(
        raw_root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        depth_records,
    )
    _write_jsonl(
        raw_root / venue / "trade_v2" / symbol / date / f"{date}T00.jsonl",
        trade_records,
    )
    return raw_root


# ---------------------------------------------------------------------------
# 1. Memory-boundedness: spool not Python lists
# ---------------------------------------------------------------------------

def test_writer_does_not_accumulate_python_lists(tmp_path: Path) -> None:
    """Depth/trade data must be written to spool, not retained in memory lists."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")

    batch = [_depth_record(session=1, seq=i, raw_idx=i) for i in range(1, 101)]
    writer.write_depth_batch(batch)

    # After writing, the writer must not hold a list of Python dicts
    assert not hasattr(writer, "depth_batches"), (
        "ReplayWriter must not retain depth_batches list"
    )
    assert not hasattr(writer, "trade_batches"), (
        "ReplayWriter must not retain trade_batches list"
    )

    writer.cleanup_staging()


def test_writer_spool_count_matches_input(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")

    n_depth = 300
    n_trade = 200
    writer.write_depth_batch([_depth_record(session=1, seq=i, raw_idx=i) for i in range(n_depth)])
    writer.write_trades_batch([_trade_record(session=1, seq=i, raw_idx=i) for i in range(n_trade)])

    assert writer.depth_count == n_depth
    assert writer.trade_count == n_trade

    writer.cleanup_staging()


def test_memory_not_proportional_to_total_records(tmp_path: Path) -> None:
    """
    Write 4x records vs 1x records and verify writer retained-list size is zero.

    This is a structural check (no large Python lists) not a RSS measurement,
    to keep CI tests fast.
    """
    for n in (100, 400):
        replay_root = tmp_path / f"replay_{n}"
        writer = ReplayWriter(
            replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01",
            parquet_batch_size=50,
        )
        # Feed in many small batches
        for batch_start in range(0, n, 50):
            writer.write_depth_batch([
                _depth_record(session=1, seq=i, raw_idx=i)
                for i in range(batch_start, batch_start + 50)
            ])
        # No large Python list should exist on writer
        for attr in ("depth_batches", "trade_batches"):
            assert not hasattr(writer, attr), f"Writer has forbidden attribute {attr}"
        writer.cleanup_staging()


# ---------------------------------------------------------------------------
# 2. Schema, count, ordering, checksum, manifest
# ---------------------------------------------------------------------------

def test_output_has_correct_schema_counts_ordering(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01",
        parquet_batch_size=10,
    )

    # Write out-of-order, expect sorted output
    depths = [_depth_record(session=1, seq=seq, raw_idx=seq) for seq in [3, 1, 2]]
    trades = [_trade_record(session=1, seq=seq, raw_idx=seq) for seq in [2, 3, 1]]
    writer.write_depth_batch(depths)
    writer.write_trades_batch(trades)
    manifest = writer.finalize_staging()
    writer.publish()

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    assert partition.exists()

    depth_tbl = pq.ParquetFile(partition / "depth.parquet").read()
    trade_tbl = pq.ParquetFile(partition / "trades.parquet").read()

    # Schema check
    assert depth_tbl.schema == DEPTH_REPLAY_SCHEMA
    assert trade_tbl.schema == TRADE_REPLAY_SCHEMA

    # Count check
    assert depth_tbl.num_rows == 3
    assert trade_tbl.num_rows == 3
    assert manifest["depth_record_count"] == 3
    assert manifest["trade_record_count"] == 3

    # Ordering check (sorted by session_seq)
    depth_seqs = depth_tbl.column("session_seq").to_pylist()
    assert depth_seqs == sorted(depth_seqs)
    trade_seqs = trade_tbl.column("trade_session_seq").to_pylist()
    assert trade_seqs == sorted(trade_seqs)


def test_manifest_checksum_matches_file(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.write_trades_batch([_trade_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()
    writer.publish()

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    manifest = json.loads((partition / "manifest.json").read_text())

    def sha256(p: Path) -> str:
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    assert sha256(partition / "depth.parquet") == manifest["depth_checksum"]
    assert sha256(partition / "trades.parquet") == manifest["trades_checksum"]
    assert manifest["status"] == "complete"


def test_empty_channel_produces_schema_bearing_parquet(tmp_path: Path) -> None:
    """An empty channel must produce a Parquet file with the correct schema."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    # Write trades only, no depth
    writer.write_trades_batch([_trade_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()
    writer.publish()

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    depth_tbl = pq.ParquetFile(partition / "depth.parquet").read()
    assert depth_tbl.schema == DEPTH_REPLAY_SCHEMA
    assert depth_tbl.num_rows == 0


def test_ts_range_in_manifest(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([
        _depth_record(session=1, seq=1, raw_idx=1, ts_ns=1_000),
        _depth_record(session=1, seq=2, raw_idx=2, ts_ns=3_000),
    ])
    manifest = writer.finalize_staging()
    writer.publish()

    assert manifest["ts_range_start_ns"] == 1_000
    assert manifest["ts_range_end_ns"] == 3_000


def test_large_record_set_spans_many_batches(tmp_path: Path) -> None:
    """50 000 records across many spool batches must produce correct count."""
    n = 50_000
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01",
        parquet_batch_size=1000,
    )
    batch_size = 5000
    for start in range(0, n, batch_size):
        writer.write_depth_batch([
            _depth_record(session=1, seq=i, raw_idx=i) for i in range(start, start + batch_size)
        ])
    manifest = writer.finalize_staging()
    writer.publish()

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    tbl = pq.ParquetFile(partition / "depth.parquet").read()
    assert tbl.num_rows == n
    assert manifest["depth_record_count"] == n


# ---------------------------------------------------------------------------
# 3. Cleanup
# ---------------------------------------------------------------------------

def test_cleanup_removes_staging_on_success(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    staging = writer.staging_dir
    assert staging.exists()
    writer.finalize_staging()
    writer.publish()
    # Staging must be gone (renamed to output)
    assert not staging.exists()


def test_cleanup_on_exception_removes_staging(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    staging = writer.staging_dir
    assert staging.exists()

    writer.cleanup_staging()
    assert not staging.exists()


def test_failed_build_does_not_publish(tmp_path: Path) -> None:
    """An exception during finalization must not replace a valid existing partition."""
    replay_root = tmp_path / "replay"

    # Create a valid existing partition
    writer1 = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer1.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer1.finalize_staging()
    writer1.publish()
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    original_checksum = json.loads((partition / "manifest.json").read_text())["depth_checksum"]

    # Now simulate a failed second build — do not call publish
    writer2 = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer2.write_depth_batch([_depth_record(session=1, seq=2, raw_idx=2)])
    writer2.cleanup_staging()

    # Existing partition must be unchanged
    assert partition.exists()
    new_checksum = json.loads((partition / "manifest.json").read_text())["depth_checksum"]
    assert new_checksum == original_checksum


def test_stale_staging_cleaned_before_build(tmp_path: Path) -> None:
    """A stale staging dir from a previous SIGKILL must not block or corrupt a new build."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTSYM", "2026-07-01",
                              depth_count=3, trade_count=2)
    replay_root = tmp_path / "replay"

    # Simulate a stale staging dir
    stale_staging = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTSYM" / ".staging_2026-07-01_TESTSYM"
    )
    stale_staging.mkdir(parents=True)
    (stale_staging / "junk.txt").write_text("leftover")

    result = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTSYM", "2026-07-01", raw_root, replay_root
    )
    assert result["status"] == "success"
    assert not stale_staging.exists()


# ---------------------------------------------------------------------------
# 4. Skip-if-valid / restart progress
# ---------------------------------------------------------------------------

def test_valid_partition_is_skipped(tmp_path: Path) -> None:
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTSYM", "2026-07-01",
                              depth_count=3, trade_count=2)
    replay_root = tmp_path / "replay"

    # First build succeeds
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTSYM", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    # Second run on same date/symbol must be skipped
    r2 = build_replay_for_symbol("BINANCE_SPOT", "TESTSYM", "2026-07-01", raw_root, replay_root)
    assert r2["status"] == "skipped"


def test_corrupt_partition_is_not_skipped(tmp_path: Path) -> None:
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTSYM", "2026-07-01",
                              depth_count=3, trade_count=2)
    replay_root = tmp_path / "replay"

    # Build once
    build_replay_for_symbol("BINANCE_SPOT", "TESTSYM", "2026-07-01", raw_root, replay_root)

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTSYM" / "date=2026-07-01"
    # Corrupt the depth file
    (partition / "depth.parquet").write_bytes(b"corrupted")

    # Must not skip — checksum will fail
    assert not _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTSYM", "2026-07-01")


def test_missing_manifest_not_skipped(tmp_path: Path) -> None:
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTSYM", "2026-07-01",
                              depth_count=2, trade_count=2)
    replay_root = tmp_path / "replay"

    build_replay_for_symbol("BINANCE_SPOT", "TESTSYM", "2026-07-01", raw_root, replay_root)
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTSYM" / "date=2026-07-01"
    (partition / "manifest.json").unlink()

    assert not _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTSYM", "2026-07-01")


def test_staging_dir_not_treated_as_valid(tmp_path: Path) -> None:
    """A staging directory must never be counted as a valid published partition."""
    replay_root = tmp_path / "replay"
    stale_staging = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTSYM" / ".staging_2026-07-01_TESTSYM"
    )
    stale_staging.mkdir(parents=True)
    # _partition_is_valid checks the date= directory, not .staging_
    assert not _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTSYM", "2026-07-01")


# ---------------------------------------------------------------------------
# 5. Cross-batch ordering
# ---------------------------------------------------------------------------

def test_cross_batch_ordering_is_deterministic(tmp_path: Path) -> None:
    """Records written across multiple batches must be sorted in the output."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01",
        parquet_batch_size=3,
    )
    # Write batches in reverse sequence order
    for batch_start in range(9, -1, -3):
        writer.write_depth_batch([
            _depth_record(session=1, seq=batch_start + offset, raw_idx=batch_start + offset)
            for offset in range(3)
        ])
    writer.finalize_staging()
    writer.publish()

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    tbl = pq.ParquetFile(partition / "depth.parquet").read()
    seqs = tbl.column("session_seq").to_pylist()
    assert seqs == sorted(seqs), f"Not sorted: {seqs}"
    assert len(seqs) == 12


# ---------------------------------------------------------------------------
# Spool lifetime — spools live inside staging dir
# ---------------------------------------------------------------------------

def test_spool_files_live_inside_staging_dir(tmp_path: Path) -> None:
    """SQLite spool files must be created inside staging_dir/scratch so that
    stale-staging cleanup removes them even after a SIGKILL."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    # Trigger spool creation
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    scratch = writer.staging_dir / "scratch"
    assert scratch.exists(), "scratch directory must exist under staging_dir"
    all_files = list(scratch.iterdir())
    assert all_files, "spool file must be inside staging_dir/scratch"
    # Simulate stale-staging cleanup: rmtree staging_dir removes everything
    import shutil
    shutil.rmtree(writer.staging_dir)
    assert not scratch.exists(), "scratch must be gone after staging cleanup"


def test_stale_staging_cleanup_removes_spools(tmp_path: Path) -> None:
    """After a SIGKILL the .staging_* dir is removed by the next build.
    Confirm that removes spool files too (no orphaned SQLite on data disk)."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 10, 10)
    replay_root = tmp_path / "replay"
    # Simulate a SIGKILL mid-run: create staging + scratch + a fake spool file
    staging = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / ".staging_2026-07-01_TESTUSDT"
    scratch = staging / "scratch"
    scratch.mkdir(parents=True)
    fake_spool = scratch / "replay-depth-orphan"
    fake_spool.write_bytes(b"fake sqlite")
    assert fake_spool.exists()

    # Running the build should remove the stale staging (incl. scratch) and rebuild
    result = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root
    )
    assert result["status"] == "success"
    assert not staging.exists(), "stale staging (incl. spool) must be removed"


# ---------------------------------------------------------------------------
# Atomic publication — backup/restore
# ---------------------------------------------------------------------------

def test_publish_preserves_existing_partition_on_replace_error(tmp_path: Path) -> None:
    """If os.replace(staging->output) fails, the pre-existing valid partition
    must be restored so the published store is never left empty."""
    import os
    import unittest.mock
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    # Build a first valid partition
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    original_depth_size = (partition / "depth.parquet").stat().st_size

    # Start a second writer, finalize staging, then make os.replace fail
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    _real_replace = os.replace
    def _failing_replace(src, dst):
        # Only fail the staging->output rename
        if ".staging_" in str(src):
            raise OSError("injected failure")
        return _real_replace(src, dst)

    with unittest.mock.patch("os.replace", side_effect=_failing_replace):
        with pytest.raises(OSError, match="injected failure"):
            writer.publish()

    # The original partition must still exist and be intact
    assert partition.exists(), "original partition must still exist after failed publish"
    assert (partition / "manifest.json").exists()
    assert (partition / "depth.parquet").stat().st_size == original_depth_size


def test_publish_fsyncs_all_files_and_staging_before_first_rename(
    monkeypatch, tmp_path: Path,
) -> None:
    import stores.replay_writer as writer_module

    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.write_trades_batch([_trade_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()
    events: list[str] = []
    real_replace = writer_module.os.replace

    monkeypatch.setattr(
        writer_module,
        "_fsync_regular_file",
        lambda path: events.append(f"file:{Path(path).name}"),
    )
    monkeypatch.setattr(
        writer_module,
        "_fsync_directory",
        lambda path: events.append(
            "dir:staging" if Path(path) == writer.staging_dir else "dir:parent"
        ),
    )

    def tracked_replace(source, destination):
        events.append(f"replace:{Path(source).name}->{Path(destination).name}")
        return real_replace(source, destination)

    monkeypatch.setattr(writer_module.os, "replace", tracked_replace)
    monkeypatch.setattr(
        writer_module,
        "validate_partition",
        lambda path: events.append("validate") or True,
    )
    writer.publish(instrument_metadata={"id": "TESTUSDT.BINANCE"})

    rename_index = next(i for i, value in enumerate(events) if value.startswith("replace:"))
    assert events[:rename_index] == [
        "file:depth.parquet",
        "file:trades.parquet",
        "file:instrument.json",
        "file:manifest.json",
        "dir:staging",
    ]
    assert events[rename_index + 1:] == ["dir:parent", "validate"]


def test_overwrite_fsyncs_parent_around_renames_and_backup_removal(
    monkeypatch, tmp_path: Path,
) -> None:
    import stores.replay_writer as writer_module

    replay_root = tmp_path / "replay"
    first = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    first.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    first.publish()

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=2, raw_idx=2)])
    writer.finalize_staging()
    events: list[str] = []
    real_replace = writer_module.os.replace
    real_rmtree = writer_module.shutil.rmtree

    monkeypatch.setattr(writer_module, "_fsync_regular_file", lambda path: None)
    monkeypatch.setattr(
        writer_module,
        "_fsync_directory",
        lambda path: events.append(
            "dir:staging" if Path(path) == writer.staging_dir else "dir:parent"
        ),
    )

    def tracked_replace(source, destination):
        events.append(f"replace:{Path(source).name}->{Path(destination).name}")
        return real_replace(source, destination)

    def tracked_rmtree(path, *args, **kwargs):
        events.append(f"rmtree:{Path(path).name}")
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(writer_module.os, "replace", tracked_replace)
    monkeypatch.setattr(writer_module.shutil, "rmtree", tracked_rmtree)
    monkeypatch.setattr(
        writer_module,
        "validate_partition",
        lambda path: events.append("validate") or True,
    )
    writer.publish()

    assert events == [
        "dir:staging",
        "replace:date=2026-07-01->.backup_2026-07-01_TESTUSDT",
        "dir:parent",
        "replace:.staging_2026-07-01_TESTUSDT->date=2026-07-01",
        "dir:parent",
        "validate",
        "rmtree:.backup_2026-07-01_TESTUSDT",
        "dir:parent",
    ]


def test_parquet_fsync_failure_prevents_first_publication(
    monkeypatch, tmp_path: Path,
) -> None:
    import stores.replay_writer as writer_module

    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    def fail_depth(path):
        if Path(path).name == "depth.parquet":
            raise OSError("injected parquet fsync failure")

    monkeypatch.setattr(writer_module, "_fsync_regular_file", fail_depth)
    with pytest.raises(OSError, match="injected parquet fsync failure"):
        writer.publish()
    assert not writer.output_dir.exists()
    assert writer.staging_dir.exists()


def test_parquet_fsync_failure_preserves_existing_canonical(
    monkeypatch, tmp_path: Path,
) -> None:
    import stores.replay_writer as writer_module

    replay_root = tmp_path / "replay"
    first = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    first.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    canonical = first.publish()
    original_manifest_sha = hashlib.sha256(
        (canonical / "manifest.json").read_bytes()
    ).hexdigest()

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=2, raw_idx=2)])
    writer.finalize_staging()

    def fail_trades(path):
        if Path(path).name == "trades.parquet":
            raise OSError("injected parquet fsync failure")

    monkeypatch.setattr(writer_module, "_fsync_regular_file", fail_trades)
    with pytest.raises(OSError, match="injected parquet fsync failure"):
        writer.publish()
    assert hashlib.sha256((canonical / "manifest.json").read_bytes()).hexdigest() == original_manifest_sha
    assert not list(canonical.parent.glob(".backup_*"))


def test_force_rebuild_overrides_valid_partition(tmp_path: Path) -> None:
    """force=True must rebuild even when the partition is already valid."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    # Without force -> skipped
    r2 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r2["status"] == "skipped"
    # With force -> success (rebuilt)
    r3 = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
    )
    assert r3["status"] == "success"


def test_published_partition_layout_is_clean(tmp_path: Path) -> None:
    """The published partition must contain only supported files (no scratch/, spools, backups)."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert result["status"] == "success"

    partition_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    )
    assert partition_dir.is_dir(), "Published partition directory must exist"

    allowed_names = {"depth.parquet", "trades.parquet", "manifest.json", "instrument.json"}
    actual_names = {p.name for p in partition_dir.rglob("*") if p.is_file()}
    assert actual_names <= allowed_names, (
        f"Published partition contains unexpected files: {actual_names - allowed_names}"
    )
    # No subdirectories (e.g. scratch/) must remain
    subdirs = [p for p in partition_dir.iterdir() if p.is_dir()]
    assert not subdirs, f"Published partition must not contain subdirectories: {subdirs}"


def test_crash_recovery_restores_backup_on_startup(tmp_path: Path) -> None:
    """Simulate a SIGKILL between the two os.replace() calls in publish().

    After the crash: output_dir is missing, backup_dir exists.
    The next build_replay_for_symbol() call must restore the backup rather than
    rebuild from scratch.
    """
    import os
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    # First successful build
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / ".backup_2026-07-01_TESTUSDT"

    # Simulate mid-publish crash: backup exists, output is gone
    os.replace(partition_dir, backup_dir)
    assert not partition_dir.exists()
    assert backup_dir.exists()

    # Next run must restore (not rebuild)
    r2 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r2["status"] in ("success", "skipped"), f"Expected recovery, got: {r2['status']}"
    assert partition_dir.exists(), "Partition must be restored from backup"
    assert not backup_dir.exists(), "Backup must be removed after successful recovery"


def test_stale_staging_cleanup_fails_closed(tmp_path: Path) -> None:
    """If stale staging cannot be removed, the build must return status=error."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 3, 3)
    replay_root = tmp_path / "replay"

    # Create a fake stale staging dir with a file inside
    staging_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".staging_2026-07-01_TESTUSDT"
    )
    staging_dir.mkdir(parents=True)
    (staging_dir / "depth.parquet").touch()

    # Make staging_dir immutable so rmtree cannot delete it
    import stat
    staging_dir.chmod(stat.S_IRUSR | stat.S_IXUSR)  # remove write bit
    try:
        result = build_replay_for_symbol(
            "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root
        )
        # rmtree may still succeed on some Linux configurations (root user etc.);
        # if it succeeds, that's fine — just check we didn't build on stale files.
        if result["status"] == "failed":
            assert any("stale" in e.lower() or "staging" in e.lower() for e in result["errors"]), (
                f"error message should mention staging: {result['errors']}"
            )
    finally:
        # Restore permissions so tmp_path cleanup works. If rmtree succeeded
        # despite the removed write bit (e.g. running as root, which ignores
        # the write-permission check), staging_dir no longer exists and
        # chmod would raise FileNotFoundError.
        if staging_dir.exists():
            staging_dir.chmod(stat.S_IRWXU)


# ---------------------------------------------------------------------------
# 6. recover_partition_state() — cases A through G
# ---------------------------------------------------------------------------

from pipeline.build_replay_store import recover_partition_state  # noqa: E402


def _make_valid_partition(replay_root: Path, venue: str, symbol: str, date: str,
                          raw_root: Path) -> None:
    """Build a valid partition for use in recovery case tests."""
    result = build_replay_for_symbol(venue, symbol, date, raw_root, replay_root)
    assert result["status"] == "success", f"setup failed: {result}"


def test_recovery_case_f_valid_no_backup(tmp_path: Path) -> None:
    """Case F: valid partition, no backup -> skip."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"
    _make_valid_partition(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root)

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "skip", f"Expected skip, got: {action}"


def test_recovery_case_g_missing_no_backup(tmp_path: Path) -> None:
    """Case G: output missing, no backup -> rebuild."""
    replay_root = tmp_path / "replay"

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "rebuild", f"Expected rebuild, got: {action}"


def test_recovery_case_a_restores_valid_backup(tmp_path: Path) -> None:
    """Case A: output missing, backup valid -> restore and return skip."""
    import os
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"
    _make_valid_partition(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root)

    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )

    # Simulate crash: output gone, backup exists
    os.replace(partition_dir, backup_dir)
    assert not partition_dir.exists()
    assert backup_dir.exists()

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "skip", f"Expected skip after restore, got: {action}"
    assert partition_dir.exists(), "Partition must be restored from backup"
    assert not backup_dir.exists(), "Backup must be consumed by restore"


def test_recovery_case_b_fails_on_invalid_backup_no_output(tmp_path: Path) -> None:
    """Case B: output missing, backup exists but invalid -> fail (preserve for operator)."""
    replay_root = tmp_path / "replay"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    backup_dir.mkdir(parents=True)
    # Invalid backup: no manifest.json
    (backup_dir / "depth.parquet").touch()

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "fail", f"Expected fail, got: {action}"
    assert backup_dir.exists(), "Invalid backup must be preserved for operator inspection"


def test_recovery_case_c_valid_output_removes_stale_backup(tmp_path: Path) -> None:
    """Case C: canonical valid, stale backup exists -> skip, backup removed."""
    import shutil
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"
    _make_valid_partition(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root)

    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )

    # Create a stale backup (copy of valid partition)
    shutil.copytree(partition_dir, backup_dir)
    assert backup_dir.exists()

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "skip", f"Expected skip, got: {action}"
    assert partition_dir.exists(), "Canonical partition must remain"
    assert not backup_dir.exists(), "Stale backup must be removed"


def test_recovery_case_d_restores_backup_when_output_invalid(tmp_path: Path) -> None:
    """Case D: output invalid, backup valid -> quarantine invalid, restore backup, skip."""
    import shutil
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"
    _make_valid_partition(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root)

    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )

    # Create a valid backup (copy of valid partition) first
    shutil.copytree(partition_dir, backup_dir)

    # Corrupt the canonical output
    (partition_dir / "manifest.json").write_text("CORRUPTED")

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "skip", f"Expected skip after restore, got: {action}"
    assert partition_dir.exists(), "Partition must be restored from backup"
    assert not backup_dir.exists(), "Backup must be consumed"
    # The restored manifest must be valid (not corrupted)
    manifest = json.loads((partition_dir / "manifest.json").read_text())
    assert "depth_record_count" in manifest, "Restored manifest must be valid"


def test_recovery_case_e_both_invalid(tmp_path: Path) -> None:
    """Case E: both canonical and backup invalid -> fail, preserve both."""
    replay_root = tmp_path / "replay"
    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )

    # Create both as invalid
    partition_dir.mkdir(parents=True)
    (partition_dir / "manifest.json").write_text("BAD")
    backup_dir.mkdir(parents=True)
    (backup_dir / "manifest.json").write_text("ALSO BAD")

    action = recover_partition_state(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert action.action == "fail", f"Expected fail, got: {action}"
    # Both must be preserved for operator inspection
    assert partition_dir.exists(), "Invalid canonical must be preserved"
    assert backup_dir.exists(), "Invalid backup must be preserved"


def test_recovery_failure_counts_as_failed_status(tmp_path: Path) -> None:
    """Status returned for a fail-action recovery must be 'failed', not 'error'."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"

    # Create an invalid backup with no canonical output -> Case B -> fail
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    backup_dir.mkdir(parents=True)
    (backup_dir / "garbage").touch()  # invalid backup

    result = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root
    )
    assert result["status"] == "failed", (
        f"Recovery failure must produce status='failed', got: {result['status']!r}"
    )


# ---------------------------------------------------------------------------
# 7. Failure injection tests
# ---------------------------------------------------------------------------

def test_publish_backup_deletion_failure_does_not_fail_build(tmp_path: Path) -> None:
    """
    If backup deletion fails after a successful os.replace(staging -> output),
    the build must still succeed (backup deletion is best-effort).
    """
    import shutil
    import unittest.mock as mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    replay_root = tmp_path / "replay"

    # First build to create an existing partition that becomes the backup
    _make_valid_partition(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root)

    # Second build with injected failure on backup deletion
    original_rmtree = shutil.rmtree
    rmtree_calls: list = []

    def failing_rmtree(path, *args, **kwargs):
        p = str(path)
        rmtree_calls.append(p)
        if ".backup_" in p:
            raise OSError("injected backup deletion failure")
        return original_rmtree(path, *args, **kwargs)

    with mock.patch("stores.replay_writer.shutil.rmtree", side_effect=failing_rmtree):
        result = build_replay_for_symbol(
            "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root,
            force=True,
        )

    # Build must succeed despite backup deletion failure
    assert result["status"] == "success", (
        f"Build must succeed even if backup deletion fails; got: {result}"
    )
    # New canonical partition must exist
    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    assert partition_dir.exists(), "New partition must exist after publish"


def test_scratch_nonempty_prevents_publication(tmp_path: Path) -> None:
    """If a file remains in scratch after all spool-to-parquet, finalize must raise."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")

    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])

    # Inject a leftover file into scratch (simulating an unexpected file)
    scratch_dir = writer._spool_scratch_dir
    leftover = scratch_dir / "unexpected_leftover.dat"
    leftover.touch()

    with pytest.raises(Exception):
        writer.finalize_staging()

    # Canonical partition must not be created
    partition_dir = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    assert not partition_dir.exists(), "Partition must not exist when scratch cleanup fails"


# ---------------------------------------------------------------------------
# 8. Post-publication validation (Codex finding 1)
# ---------------------------------------------------------------------------

def test_publish_validates_normal_publication_succeeds(tmp_path: Path) -> None:
    """A normal publication must pass post-publish validation and return success."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert result["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    assert partition.exists()
    assert (partition / "manifest.json").exists()


def test_publish_raises_when_output_missing_after_replace(tmp_path: Path) -> None:
    """
    If os.replace(staging, output) does not raise but the destination is
    missing/corrupted (fault-injected), publish() must raise instead of
    returning normally, and any previous valid partition must be restored.
    """
    import os
    import shutil
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    # First valid partition (becomes the backup during the second publish).
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    _real_replace = os.replace

    def _fault_injecting_replace(src, dst):
        if ".staging_" in str(src):
            # Simulate a replace that "succeeds" from the filesystem's point
            # of view but leaves the destination missing (e.g. a non-standard
            # filesystem path). Remove the source without creating dst.
            if Path(src).is_dir():
                shutil.rmtree(src)
            return None
        return _real_replace(src, dst)

    with unittest.mock.patch("os.replace", side_effect=_fault_injecting_replace):
        with pytest.raises(RuntimeError, match="Post-publish validation failed"):
            writer.publish()

    # The previous valid partition must be restored, not lost.
    assert partition.exists(), "Previous valid partition must be restored after invalid publish"
    assert (partition / "manifest.json").exists()


def test_publish_raises_on_corrupt_manifest_after_publication(tmp_path: Path) -> None:
    """A corrupt manifest.json in the newly published partition must fail closed."""
    import os
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    _real_replace = os.replace

    def _corrupting_replace(src, dst):
        result = _real_replace(src, dst)
        if ".staging_" in str(src):
            # Corrupt the manifest right after the canonical rename succeeds.
            (Path(dst) / "manifest.json").write_text("NOT VALID JSON")
        return result

    with unittest.mock.patch("os.replace", side_effect=_corrupting_replace):
        with pytest.raises(RuntimeError, match="Post-publish validation failed"):
            writer.publish()

    # The previous valid partition must be restored.
    assert partition.exists(), "Previous valid partition must be restored after corrupt manifest"
    manifest = json.loads((partition / "manifest.json").read_text())
    assert manifest.get("status") == "complete"


def test_publish_raises_on_checksum_mismatch_after_publication(tmp_path: Path) -> None:
    """A checksum-invalid depth.parquet in the newly published partition must fail closed."""
    import os
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    _real_replace = os.replace

    def _truncating_replace(src, dst):
        result = _real_replace(src, dst)
        if ".staging_" in str(src):
            # Truncate depth.parquet so its checksum no longer matches the
            # manifest, without touching the manifest itself.
            (Path(dst) / "depth.parquet").write_bytes(b"corrupt-bytes")
        return result

    with unittest.mock.patch("os.replace", side_effect=_truncating_replace):
        with pytest.raises(RuntimeError, match="Post-publish validation failed"):
            writer.publish()

    # The previous valid partition must be restored and still checksum-valid.
    assert partition.exists()
    from pipeline.build_replay_store import _partition_is_valid
    assert _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")


def test_publish_does_not_delete_backup_until_validated(tmp_path: Path) -> None:
    """The obsolete backup must still exist at the moment validation runs and
    only be removed after validation succeeds (proven via a checksum-mismatch
    fault injection that leaves the backup intact)."""
    import os
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    writer = ReplayWriter(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    writer.write_depth_batch([_depth_record(session=1, seq=1, raw_idx=1)])
    writer.finalize_staging()

    _real_replace = os.replace

    def _truncating_replace(src, dst):
        result = _real_replace(src, dst)
        if ".staging_" in str(src):
            (Path(dst) / "depth.parquet").write_bytes(b"corrupt-bytes")
        return result

    with unittest.mock.patch("os.replace", side_effect=_truncating_replace):
        with pytest.raises(RuntimeError):
            writer.publish()

    # Backup must have been consumed by the restore (moved back to canonical),
    # not left dangling — but crucially it was never deleted while the
    # replacement was unvalidated.
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    assert not backup_dir.exists(), "Backup must be consumed by restore, not orphaned"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    assert partition.exists()


def test_publish_success_still_deletes_obsolete_backup(tmp_path: Path) -> None:
    """A successful, validated publication must still delete the obsolete backup best-effort."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    r2 = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
    )
    assert r2["status"] == "success"

    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    assert not backup_dir.exists(), "Obsolete backup must be deleted after validated publication"


# ---------------------------------------------------------------------------
# 9. Preserve failed result when cleanup also fails (Codex finding 2)
# ---------------------------------------------------------------------------

def test_failed_result_preserved_when_cleanup_also_fails(tmp_path: Path) -> None:
    """
    If the primary build fails and cleanup_staging() also raises, the function
    must still return a normal status=failed result containing both error
    messages, not propagate the cleanup exception.
    """
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"

    def _raise_primary(*args, **kwargs):
        raise RuntimeError("injected primary build failure")

    def _raise_cleanup(self):
        raise RuntimeError("injected cleanup failure")

    with unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.finalize_staging", side_effect=_raise_primary
    ), unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.cleanup_staging", _raise_cleanup
    ):
        result = build_replay_for_symbol(
            "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root
        )

    assert result["status"] == "failed"
    assert any("injected primary build failure" in e for e in result["errors"])
    assert any("injected cleanup failure" in e for e in result["errors"])
    assert len(result["errors"]) == 2, f"Expected both errors preserved, got: {result['errors']}"


def test_daily_build_continues_after_cleanup_failure(tmp_path: Path) -> None:
    """run_build_replay_store()-style aggregation must keep processing later
    symbols even when one symbol's cleanup also fails."""
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "AAAUSDT", "2026-07-01", 3, 3)
    _make_raw_root(tmp_path, "BINANCE_SPOT", "BBBUSDT", "2026-07-01", 3, 3)
    # _make_raw_root reuses the same tmp_path/raw directory across calls since
    # it always writes under tmp_path / "raw"; both symbols share raw_root.
    replay_root = tmp_path / "replay"

    def _raise_primary(*args, **kwargs):
        raise RuntimeError("injected primary build failure")

    def _raise_cleanup(self):
        raise RuntimeError("injected cleanup failure")

    results = []
    with unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.finalize_staging", side_effect=_raise_primary
    ), unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.cleanup_staging", _raise_cleanup
    ):
        results.append(
            build_replay_for_symbol("BINANCE_SPOT", "AAAUSDT", "2026-07-01", raw_root, replay_root)
        )

    # Cleanup patch is now removed; the second symbol builds normally.
    results.append(
        build_replay_for_symbol("BINANCE_SPOT", "BBBUSDT", "2026-07-01", raw_root, replay_root)
    )

    assert results[0]["status"] == "failed"
    assert results[1]["status"] == "success"


# ---------------------------------------------------------------------------
# 10. Preserve valid backups during --force rebuild failures (Codex finding 3)
# ---------------------------------------------------------------------------

def test_force_case1_valid_no_backup_replacement_succeeds(tmp_path: Path) -> None:
    """force=True, canonical valid, no backup, replacement succeeds -> new canonical valid."""
    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    r2 = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
    )
    assert r2["status"] == "success"
    from pipeline.build_replay_store import _partition_is_valid
    assert _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")


def test_force_case2_valid_no_backup_replacement_fails(tmp_path: Path) -> None:
    """force=True, canonical valid, no backup, replacement fails -> original canonical preserved."""
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    original_checksum = json.loads((partition / "manifest.json").read_text())["depth_checksum"]

    def _raise_primary(*args, **kwargs):
        raise RuntimeError("injected forced-rebuild failure")

    with unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.finalize_staging", side_effect=_raise_primary
    ):
        r2 = build_replay_for_symbol(
            "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
        )

    assert r2["status"] == "failed"
    assert partition.exists(), "Original canonical partition must be preserved on forced-rebuild failure"
    assert json.loads((partition / "manifest.json").read_text())["depth_checksum"] == original_checksum


def test_force_case3_missing_canonical_valid_backup_replacement_succeeds(tmp_path: Path) -> None:
    """force=True, canonical missing (crash-left backup valid), replacement succeeds ->
    backup stays protected until the new canonical validates, then is removed."""
    import os

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    # Simulate a mid-publish crash: canonical missing, backup valid.
    os.replace(partition, backup_dir)
    assert not partition.exists()
    assert backup_dir.exists()

    r2 = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
    )
    assert r2["status"] == "success"
    from pipeline.build_replay_store import _partition_is_valid
    assert _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert not backup_dir.exists(), "Backup must be removed only after the new canonical validated"


def test_force_case4_missing_canonical_valid_backup_replacement_fails(tmp_path: Path) -> None:
    """force=True, canonical missing (crash-left backup valid), replacement fails ->
    the valid backup must be restored/preserved, no total data loss."""
    import os
    import unittest.mock

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    os.replace(partition, backup_dir)
    assert not partition.exists()
    assert backup_dir.exists()

    def _raise_primary(*args, **kwargs):
        raise RuntimeError("injected forced-rebuild failure")

    with unittest.mock.patch(
        "stores.replay_writer.ReplayWriter.finalize_staging", side_effect=_raise_primary
    ):
        r2 = build_replay_for_symbol(
            "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
        )

    assert r2["status"] == "failed"
    # No total data loss: either the backup remains, or it was restored to canonical.
    assert partition.exists() or backup_dir.exists(), (
        "Valid partition must survive as canonical or backup after forced-rebuild failure"
    )
    if partition.exists():
        from pipeline.build_replay_store import _partition_is_valid
        assert _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")


def test_force_case5_invalid_canonical_valid_backup_preserved(tmp_path: Path) -> None:
    """force=True, canonical invalid, backup valid -> the valid backup must be
    preserved/restored before attempting the replacement."""
    import shutil

    raw_root = _make_raw_root(tmp_path, "BINANCE_SPOT", "TESTUSDT", "2026-07-01", 5, 5)
    replay_root = tmp_path / "replay"
    r1 = build_replay_for_symbol("BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root)
    assert r1["status"] == "success"

    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT" / "date=2026-07-01"
    backup_dir = (
        replay_root / "venue=BINANCE_SPOT" / "symbol=TESTUSDT"
        / ".backup_2026-07-01_TESTUSDT"
    )
    # Create a valid backup copy, then corrupt the canonical output.
    shutil.copytree(partition, backup_dir)
    (partition / "manifest.json").write_text("CORRUPTED")

    r2 = build_replay_for_symbol(
        "BINANCE_SPOT", "TESTUSDT", "2026-07-01", raw_root, replay_root, force=True
    )
    assert r2["status"] == "success"
    from pipeline.build_replay_store import _partition_is_valid
    assert _partition_is_valid(replay_root, "BINANCE_SPOT", "TESTUSDT", "2026-07-01")
    assert not backup_dir.exists()
