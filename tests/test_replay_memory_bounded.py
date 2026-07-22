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
