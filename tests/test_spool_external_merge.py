"""Focused tests for the issue #20 Phase 6 correction: replace the
scratch-heavy SQLite full-JSON RawRecordSpool with a bounded-memory
external merge sort (buffered runs sorted in memory up to a bounded run
size, flushed to disk-backed pickle files, merged via a k-way
``heapq.merge``).

These tests prove:
  - correctness is unchanged (sort order, filters, first_record,
    has_record_before, max_record semantics all match the prior
    SQLite-backed behavior exactly, including first_tie tie-breaking);
  - peak memory is bounded by the configured run size, not by the total
    number of spooled records (live-object-counter proof, same pattern
    as tests/test_streaming_gating_bounded_memory.py);
  - multiple run files are actually created once the run-size threshold
    is exceeded (proving the external-merge path, not an in-memory sort,
    is exercised for large inputs);
  - temp-file cleanup on close() still works (multiple run files, not
    just a single sqlite file).
"""
from __future__ import annotations

import gc
import os

import pytest

from converter.spool import RawRecordSpool


class _LiveCounter:
    def __init__(self) -> None:
        self.alive = 0
        self.max_alive = 0

    def inc(self) -> None:
        self.alive += 1
        self.max_alive = max(self.max_alive, self.alive)

    def dec(self) -> None:
        self.alive -= 1


class _TrackedRecord(dict):
    """A dict subclass whose __del__ decrements a shared live-object
    counter, so we can empirically measure peak simultaneously-alive
    spooled records without relying on process RSS (unreliable in CI)."""

    def __init__(self, *args, counter: _LiveCounter, **kwargs):
        super().__init__(*args, **kwargs)
        self._counter = counter
        counter.inc()

    def __del__(self):
        self._counter.dec()


def _record(*, session=1, seq, ts, record_type="depth_update", counter=None, **extra):
    base = {
        "record_type": record_type,
        "stream_session_id": session,
        "session_seq": seq,
        "ts_recv_ns": ts,
    }
    base.update(extra)
    if counter is not None:
        return _TrackedRecord(base, counter=counter)
    return base


def test_iter_records_sort_order_matches_canonical_key(tmp_path):
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        # Insert out of order; iter_records() must yield in canonical
        # (sort1, sort2, sort3/raw_index) order regardless of insertion order.
        spool.insert(_record(seq=3, ts=3), (1, 3, 0), 0)
        spool.insert(_record(seq=1, ts=1), (1, 1, 0), 1)
        spool.insert(_record(seq=2, ts=2), (1, 2, 0), 2)
        spool.commit()
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == [1, 2, 3]


def test_iter_records_filters_by_record_type_and_session_and_min_sort_key(tmp_path):
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(session=1, seq=1, ts=1, record_type="snapshot_seed"), (1, 1, 0), 0)
        spool.insert(_record(session=1, seq=2, ts=2, record_type="depth_update"), (1, 2, 0), 1)
        spool.insert(_record(session=2, seq=1, ts=3, record_type="depth_update"), (2, 1, 0), 2)
        spool.commit()

        only_depth = list(spool.iter_records(record_type="depth_update"))
        assert len(only_depth) == 2
        assert all(r["record_type"] == "depth_update" for r in only_depth)

        only_session1 = list(spool.iter_records(session_id=1))
        assert len(only_session1) == 2
        assert all(r["stream_session_id"] == 1 for r in only_session1)

        after_seq1 = list(spool.iter_records(min_sort_key=(1, 2, 0)))
        assert [r["session_seq"] for r in after_seq1 if r["stream_session_id"] == 1] == [2]


def test_first_record_respects_record_type_filter_and_sort_order(tmp_path):
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(seq=1, ts=1, record_type="depth_update"), (1, 1, 0), 0)
        spool.insert(_record(seq=2, ts=2, record_type="snapshot_seed"), (1, 2, 0), 1)
        spool.commit()
        assert spool.first_record()["session_seq"] == 1
        assert spool.first_record(record_type="snapshot_seed")["session_seq"] == 2
        assert spool.first_record(record_type="nonexistent") is None


def test_has_record_before_matches_sort_key_boundary(tmp_path):
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(seq=1, ts=1, record_type="snapshot_seed"), (1, 1, 0), 0)
        spool.insert(_record(seq=5, ts=5, record_type="depth_update"), (1, 5, 0), 1)
        spool.commit()
        assert spool.has_record_before("snapshot_seed", (1, 3, 0)) is True
        assert spool.has_record_before("snapshot_seed", (1, 1, 0)) is False
        assert spool.has_record_before("depth_update", (1, 1, 0)) is False


def test_max_record_first_tie_true_matches_prior_sql_semantics(tmp_path):
    """first_tie=True mirrors the prior 'ORDER BY sort1 DESC, sort2 DESC,
    sort3 ASC, raw_index ASC LIMIT 1' — max (sort1, sort2), then MIN
    (sort3, raw_index) among ties."""
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(seq=10, ts=1, record_type="snapshot_seed", tag="a"), (1, 10, 5), 9)
        spool.insert(_record(seq=10, ts=2, record_type="snapshot_seed", tag="b"), (1, 10, 2), 3)
        spool.insert(_record(seq=10, ts=3, record_type="snapshot_seed", tag="c"), (1, 10, 2), 1)
        spool.insert(_record(seq=5, ts=4, record_type="snapshot_seed", tag="d"), (1, 5, 0), 0)
        spool.commit()
        result = spool.max_record(record_type="snapshot_seed", session_id=1, first_tie=True)
        # max (sort1=1, sort2=10); among (sort3=5) vs (sort3=2, sort3=2):
        # min sort3 -> 2; tie on sort3=2 -> min raw_index -> raw_index=1 -> tag "c"
        assert result["tag"] == "c"


def test_max_record_first_tie_false_matches_prior_sql_semantics(tmp_path):
    """first_tie=False mirrors 'ORDER BY sort1 DESC, sort2 DESC, sort3 DESC,
    raw_index DESC LIMIT 1' — pure max of the full (sort1, sort2, sort3,
    raw_index) tuple."""
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(seq=10, ts=1, record_type="snapshot_seed", tag="a"), (1, 10, 5), 9)
        spool.insert(_record(seq=10, ts=2, record_type="snapshot_seed", tag="b"), (1, 10, 2), 3)
        spool.commit()
        result = spool.max_record(record_type="snapshot_seed", session_id=1, first_tie=False)
        assert result["tag"] == "a"


def test_max_record_filters_by_session_and_record_type(tmp_path):
    with RawRecordSpool(temp_dir=tmp_path) as spool:
        spool.insert(_record(session=1, seq=1, ts=1, record_type="snapshot_seed"), (1, 1, 0), 0)
        spool.insert(_record(session=2, seq=9, ts=2, record_type="snapshot_seed"), (2, 9, 0), 1)
        spool.commit()
        result = spool.max_record(record_type="snapshot_seed", session_id=1)
        assert result["stream_session_id"] == 1
        assert spool.max_record(record_type="snapshot_seed", session_id=999) is None


def test_close_removes_all_run_files_and_marker(tmp_path):
    spool = RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-cleanup-")
    marker_path = spool.path
    for i in range(3):
        spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
    spool.commit()
    files_before_close = list(tmp_path.iterdir())
    assert len(files_before_close) >= 1  # at least the marker (and possibly run files)
    spool.close()
    assert not marker_path.exists()
    assert list(tmp_path.iterdir()) == []


def test_run_size_env_var_produces_multiple_run_files(tmp_path, monkeypatch):
    """Forcing a small run size must produce multiple on-disk run files
    for a moderate number of records — proving the external-merge path
    (not a single in-memory sort) is genuinely exercised."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_SIZE", "10")
    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-runsize-") as spool:
        for i in range(55):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) == 6  # ceil(55/10)
        # Correctness must still hold across multiple runs.
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(55))


def test_run_size_env_var_default_is_bounded(monkeypatch):
    monkeypatch.delenv("CRYPTO_RECORDER_SPOOL_RUN_SIZE", raising=False)
    spool = RawRecordSpool()
    try:
        assert spool._run_size == 20000
    finally:
        spool.close()


def test_peak_live_records_bounded_by_run_size_not_total_count(tmp_path, monkeypatch):
    """Live-object-counter proof (same pattern as
    tests/test_streaming_gating_bounded_memory.py): insert far more
    records than the configured run size and prove the number of
    simultaneously-alive Python record objects during insertion never
    exceeds a small bound tied to the run size, not the total count."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_SIZE", "200")
    counter = _LiveCounter()
    n = 5_000

    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-boundedmem-") as spool:
        for i in range(n):
            rec = _record(seq=i, ts=i, counter=counter)
            spool.insert(rec, (1, i, 0), i)
            del rec
            if i % 500 == 0:
                gc.collect()
        spool.commit()
        gc.collect()

        # Peak alive records while inserting must stay small relative to
        # the total record count (bounded by run size, with some slack for
        # transient references during flush), never proportional to n.
        assert counter.max_alive < 1000, (
            f"peak alive records ({counter.max_alive}) not bounded — "
            f"expected well under total count {n}"
        )

        # Correctness: full sorted output must still be exactly right even
        # though it spanned many run files.
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))

    gc.collect()
    assert counter.alive == 0, "all spooled records must be released after spool.close()"
