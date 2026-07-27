"""Focused tests for the issue #20 Phase 6 correction (RawRecordSpool):
a bounded-memory external merge sort replacing the SQLite full-JSON
scratch store, corrected to satisfy the approved Phase 6 RAM/scratch
model exactly:

  - the in-memory run buffer is bounded by *serialized byte size*
    (``CRYPTO_RECORDER_SPOOL_RUN_BYTES``), not by record count, since
    raw depth records vary hugely in size;
  - the final merge is a bounded-fan-in hierarchical (multi-pass) merge
    (``CRYPTO_RECORDER_SPOOL_FAN_IN``), never a flat merge across every
    run file, so memory/file-descriptor usage during a merge pass is
    O(fan_in), never O(number of runs);
  - each intermediate merge output is written atomically (temp name,
    flush+fsync+close, then os.replace) and inputs are unlinked only
    after the replacement output is durably in place;
  - a failure while writing an intermediate merge output cleans up the
    partial output and leaves not-yet-consumed inputs untouched.

These tests prove:
  - correctness is unchanged (sort order, filters, first_record,
    has_record_before, max_record semantics all match the prior
    SQL-based behavior exactly, including first_tie tie-breaking),
    including across multiple bounded-fan-in merge passes;
  - the byte budget — not record count — governs run-file flush
    boundaries, even for highly variable/large nested payloads, and a
    single oversized record is still accepted without blocking;
  - forcing more than 100 runs never opens more than
    ``fan_in + small constant`` files at once, including across
    repeated query passes (which reuse the cached, already-merged
    single run file);
  - a failure during an intermediate merge cleans the partial output
    and does not delete inputs prematurely;
  - close() cleans up all remaining run files and the marker path.
"""
from __future__ import annotations

import builtins
import gc
import os
import pickle

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


class _OpenTracker:
    """Wraps builtins.open to count concurrently-open file handles,
    used to empirically prove the bounded-fan-in file-descriptor
    guarantee without depending on platform-specific /proc/self/fd
    inspection."""

    def __init__(self):
        self.current = 0
        self.peak = 0
        self._real_open = builtins.open

    def install(self, monkeypatch):
        monkeypatch.setattr(builtins, "open", self._tracked_open)

    def _tracked_open(self, path, mode="r", *args, **kwargs):
        fh = self._real_open(path, mode, *args, **kwargs)
        tracker = self
        real_close = fh.close

        def _close():
            if not getattr(fh, "_tracked_closed", False):
                fh._tracked_closed = True
                tracker.current -= 1
            real_close()

        fh._tracked_closed = False
        fh.close = _close
        tracker.current += 1
        tracker.peak = max(tracker.peak, tracker.current)
        return fh


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


# ---------------------------------------------------------------------------
# 1. Byte-budgeted buffering (not record-count buffering)
# ---------------------------------------------------------------------------


def test_byte_budget_flushes_based_on_serialized_size_not_record_count(tmp_path, monkeypatch):
    """The run buffer must flush once *cumulative serialized bytes*
    cross the configured budget — never once a record *count* is
    reached. Predicts exact flush boundaries by replicating the spool's
    own accounting against directly measured pickle sizes, so the test
    is not tied to a specific pickle-protocol byte count."""
    budget = 100_000
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", str(budget))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", "16")

    pad_len = 40_000
    n = 7

    def make(i):
        return _record(seq=i, ts=i, pad="x" * pad_len)

    sizes = [len(pickle.dumps(make(i), protocol=pickle.HIGHEST_PROTOCOL)) for i in range(n)]
    expected_run_sizes: list[int] = []
    running = 0
    count_in_run = 0
    for s in sizes:
        running += s
        count_in_run += 1
        if running >= budget:
            expected_run_sizes.append(count_in_run)
            running = 0
            count_in_run = 0
    if count_in_run:
        expected_run_sizes.append(count_in_run)
    # Sanity: the budget must actually be exercised (multiple runs), not
    # collapse into one giant in-memory run.
    assert len(expected_run_sizes) >= 2

    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-bytesbudget-") as spool:
        for i in range(n):
            spool.insert(make(i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) == len(expected_run_sizes)
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))


def test_byte_budget_handles_highly_variable_payload_sizes(tmp_path, monkeypatch):
    """Mixes tiny records with large nested-array payloads (mirroring
    real depth_update vs sync_state size variance) and proves the byte
    budget still bounds run sizes and correctness is preserved."""
    budget = 200_000
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", str(budget))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", "8")

    def make(i):
        if i % 5 == 0:
            # Large nested payload, mirroring a real depth_update with a
            # sizeable bids/asks array.
            book = [[str(i + j), str(j)] for j in range(5000)]
            return _record(seq=i, ts=i, bids=book, asks=book)
        # Small payload, mirroring a sync_state/stream_lifecycle record.
        return _record(seq=i, ts=i, record_type="sync_state", state="synced")

    n = 40
    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-variable-") as spool:
        for i in range(n):
            spool.insert(make(i), (1, i, 0), i)
        spool.commit()
        # Multiple runs must have been produced (budget genuinely exercised).
        assert len(spool._run_paths) >= 2
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))


def test_oversized_single_record_flushes_alone_without_blocking(tmp_path, monkeypatch):
    """A single record whose serialized size already exceeds the byte
    budget must still be accepted: it becomes its own one-record run,
    flushed immediately, rather than blocking or growing the buffer
    without bound."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1000")
    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-oversized-") as spool:
        huge = _record(seq=0, ts=0, pad="x" * 100_000)  # payload far exceeds budget
        spool.insert(huge, (1, 0, 0), 0)
        # Buffer must have been flushed immediately after this single
        # oversized insert — never retained/accumulated further.
        assert spool._buffer == []
        assert spool._buffer_bytes == 0
        assert len(spool._run_paths) == 1

        spool.insert(_record(seq=1, ts=1), (1, 1, 0), 1)
        spool.commit()
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == [0, 1]


def test_peak_live_records_bounded_not_proportional_to_total_count(tmp_path, monkeypatch):
    """Live-object-counter proof (same pattern as
    tests/test_streaming_gating_bounded_memory.py): insert far more
    records than fit in a single run buffer and prove the number of
    simultaneously-alive Python record objects during insertion never
    grows proportionally to the total record count. Records are
    serialized to bytes immediately on insert() and the live object is
    not retained beyond that call, so peak-alive should stay very small
    regardless of how many records are spooled."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "50000")
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

        assert counter.max_alive < 50, (
            f"peak alive records ({counter.max_alive}) not bounded — "
            f"expected far under total count {n}"
        )

        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))

    gc.collect()
    assert counter.alive == 0, "all spooled records must be released after spool.close()"


# ---------------------------------------------------------------------------
# 2. Bounded fan-in hierarchical (multi-pass) merge
# ---------------------------------------------------------------------------


def test_multipass_merge_preserves_correctness_across_many_small_runs(tmp_path, monkeypatch):
    """Forces a small fan_in and a tiny byte budget so many runs are
    created and several merge passes are genuinely required (not just
    one flat merge), then proves the final output is still fully and
    correctly sorted, and every existing query method still matches the
    prior SQL-based semantics."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", "2")
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run

    n = 50
    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-multipass-") as spool:
        # Insert in reverse order to prove sorting (not insertion order)
        # drives the final output, across multiple merge passes.
        for i in reversed(range(n)):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) > 20  # sanity: genuinely many runs, multi-pass required

        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))
        assert spool.first_record()["session_seq"] == 0
        assert spool.has_record_before("depth_update", (1, 10, 0)) is True
        assert spool.has_record_before("depth_update", (1, 0, 0)) is False
        best = spool.max_record(record_type="depth_update", session_id=1, first_tie=False)
        assert best["session_seq"] == n - 1


def test_fan_in_bounds_concurrent_open_files_across_many_runs(tmp_path, monkeypatch):
    """Forcing more than 100 runs must never open more than
    ``fan_in + small documented constant`` files at once during the
    merge, and repeated query passes (which reuse the cached, already
    merged single run) must stay within the same bound."""
    fan_in = 8
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", str(fan_in))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run
    n = 130

    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-faninfiles-") as spool:
        for i in range(n):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) > 100  # sanity: genuinely many runs

        tracker = _OpenTracker()
        tracker.install(monkeypatch)

        first_pass = [r["session_seq"] for r in spool.iter_records()]
        assert first_pass == list(range(n))
        peak_after_first_pass = tracker.peak

        # A small documented slack constant (2) covers the merge's own
        # +1 output-file handle plus incidental overlap; it must never
        # scale with the number of runs (130 >> fan_in + 2).
        assert tracker.peak <= fan_in + 2, (
            f"peak concurrent open files ({tracker.peak}) exceeded fan_in+2 ({fan_in + 2})"
        )

        # Repeated query passes must reuse the already-merged single run
        # file and never re-open more than the same small bound.
        second_pass = [r["session_seq"] for r in spool.iter_records()]
        assert second_pass == list(range(n))
        assert tracker.peak == peak_after_first_pass, (
            "a second query pass must not increase peak concurrent open files "
            "(the merged single run file must be cached and reused)"
        )
        assert tracker.peak <= fan_in + 2


def test_repeated_first_record_and_max_record_passes_stay_within_descriptor_bound(tmp_path, monkeypatch):
    """first_record/has_record_before/max_record are called repeatedly
    per partition build; each call must reuse the cached merged run
    rather than re-triggering a fresh bounded-fan-in merge every time."""
    fan_in = 4
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", str(fan_in))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")
    n = 40

    with RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-repeatedquery-") as spool:
        for i in range(n):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) > fan_in * 2  # sanity: multi-pass required

        tracker = _OpenTracker()
        tracker.install(monkeypatch)

        for _ in range(5):
            spool.first_record()
            spool.has_record_before("depth_update", (1, 5, 0))
            spool.max_record(record_type="depth_update", session_id=1)

        assert tracker.peak <= fan_in + 2


# ---------------------------------------------------------------------------
# 3. Atomic intermediate merge output + failure cleanup
# ---------------------------------------------------------------------------


def test_merge_failure_cleans_partial_output_and_preserves_inputs(tmp_path, monkeypatch):
    """A failure while writing an intermediate merge output must leave
    no partial ``*.run.part`` file behind, and must not delete the
    not-yet-consumed input run files for that pass (they are only
    unlinked after the replacement output is durably renamed into
    place)."""
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", "4")
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run

    spool = RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-mergefail-")
    try:
        for i in range(5):  # > fan_in (4) -> a real merge batch is required
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        pre_merge_paths = list(spool._run_paths)
        assert len(pre_merge_paths) == 5
        assert all(p.exists() for p in pre_merge_paths)

        real_fsync = os.fsync
        state = {"raised": False}

        def failing_fsync(fd):
            if not state["raised"]:
                state["raised"] = True
                raise OSError("simulated failure during merge output fsync")
            return real_fsync(fd)

        monkeypatch.setattr(os, "fsync", failing_fsync)

        with pytest.raises(OSError):
            list(spool.iter_records())

        # No partial merge output must remain.
        leftover_parts = list(tmp_path.glob("*.run.part"))
        assert leftover_parts == [], f"partial merge output(s) left behind: {leftover_parts}"

        # Input run files for the (failed) pass must be untouched —
        # deletion only happens after a successful atomic replace.
        assert spool._run_paths == pre_merge_paths
        assert all(p.exists() for p in pre_merge_paths)

        # Recovery: with the fault no longer injected, the spool must
        # still be able to complete the merge correctly on a later call —
        # no manual reset is required: `self._merged` was never set True
        # by the failed attempt, so the next query automatically retries
        # from the exact pre-failure state.
        monkeypatch.setattr(os, "fsync", real_fsync)
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(5))
    finally:
        spool.close()


def test_run_bytes_and_fan_in_env_defaults(monkeypatch):
    monkeypatch.delenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", raising=False)
    monkeypatch.delenv("CRYPTO_RECORDER_SPOOL_FAN_IN", raising=False)
    spool = RawRecordSpool()
    try:
        assert spool._run_bytes_budget == 64 * 1024 * 1024
        assert spool._fan_in == 16
    finally:
        spool.close()


# ---------------------------------------------------------------------------
# 4. Transactional ownership/state model corrections: retry safety across
#    multiple successful batches, later merge passes, os.replace failures,
#    and insertion after a query.
# ---------------------------------------------------------------------------


def test_retry_after_failure_in_second_batch_after_first_batch_succeeded(tmp_path, monkeypatch):
    """Injects a failure into the *second* merge batch, after the first
    batch has already completed successfully (its output tracked, its
    inputs unlinked). Proves ``self._run_paths`` after the caught
    failure reflects exactly that complete, valid, retryable state — not
    a stale reference to already-deleted inputs — and that a later
    retry recovers every original record exactly once, with no owned
    file leaked after ``close()``."""
    fan_in = 4
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", str(fan_in))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run
    n = 12

    spool = RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-2ndbatch-")
    try:
        for i in range(n):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) == n

        original_merge_batch = RawRecordSpool._merge_batch
        calls = {"n": 0}

        def failing_merge_batch(self, batch):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated failure in second batch")
            return original_merge_batch(self, batch)

        monkeypatch.setattr(RawRecordSpool, "_merge_batch", failing_merge_batch)

        with pytest.raises(RuntimeError, match="simulated failure in second batch"):
            list(spool.iter_records())

        assert calls["n"] == 2  # the first batch succeeded before the second failed
        assert spool._merged is False

        # Exactly one successful reduction (the first batch) must be
        # reflected: run count shrank by (fan_in - 1), every remaining
        # path (the first batch's merged output plus every untouched
        # run) actually exists on disk, and is retryable.
        assert len(spool._run_paths) == n - (fan_in - 1)
        for p in spool._run_paths:
            assert p.exists()

        # Restore the real merge and retry: must recover completely.
        monkeypatch.setattr(RawRecordSpool, "_merge_batch", original_merge_batch)
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))
    finally:
        spool.close()

    remaining = list(tmp_path.glob("cr-test-2ndbatch-*"))
    assert remaining == [], f"close() left owned file(s) behind: {remaining}"


def test_retry_after_failure_during_later_merge_pass(tmp_path, monkeypatch):
    """Injects a failure several successful batches into the reduction
    (i.e. well after the very first batch — representative of a failure
    during a later merge pass over an already-partially-reduced run
    list), and proves the same retry-safety guarantee holds regardless
    of how many prior batches already succeeded."""
    fan_in = 4
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", str(fan_in))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run
    n = 40  # forces many successful batch merges before the injected failure

    spool = RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-laterpass-")
    try:
        for i in range(n):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        assert len(spool._run_paths) == n

        original_merge_batch = RawRecordSpool._merge_batch
        calls = {"n": 0}
        fail_at = 5  # several batches succeed first

        def failing_merge_batch(self, batch):
            calls["n"] += 1
            if calls["n"] == fail_at:
                raise RuntimeError("simulated failure during a later merge pass")
            return original_merge_batch(self, batch)

        monkeypatch.setattr(RawRecordSpool, "_merge_batch", failing_merge_batch)

        with pytest.raises(RuntimeError, match="simulated failure during a later merge pass"):
            list(spool.iter_records())

        assert calls["n"] == fail_at
        assert spool._merged is False
        expected_run_count = n - (fail_at - 1) * (fan_in - 1)
        assert len(spool._run_paths) == expected_run_count
        for p in spool._run_paths:
            assert p.exists()

        monkeypatch.setattr(RawRecordSpool, "_merge_batch", original_merge_batch)
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))
    finally:
        spool.close()

    remaining = list(tmp_path.glob("cr-test-laterpass-*"))
    assert remaining == [], f"close() left owned file(s) behind: {remaining}"


def test_retry_after_os_replace_failure_cleans_partial_output(tmp_path, monkeypatch):
    """A failure raised by ``os.replace`` itself (after the merge output
    has been fully written, fsync'd, and closed under its ``.run.part``
    name) must be treated exactly like a mid-write failure: the partial
    output is removed, the not-yet-consumed inputs for that batch are
    left untouched, and a later retry (once the fault is gone) recovers
    every original record exactly once with no owned file leaked."""
    fan_in = 4
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_FAN_IN", str(fan_in))
    monkeypatch.setenv("CRYPTO_RECORDER_SPOOL_RUN_BYTES", "1")  # ~1 record per run
    n = 5  # > fan_in -> a real merge batch is required

    spool = RawRecordSpool(temp_dir=tmp_path, prefix="cr-test-replacefail-")
    try:
        for i in range(n):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()
        pre_merge_paths = list(spool._run_paths)
        assert len(pre_merge_paths) == n
        assert all(p.exists() for p in pre_merge_paths)

        real_replace = os.replace
        state = {"raised": False}

        def failing_replace(src, dst):
            if not state["raised"]:
                state["raised"] = True
                raise OSError("simulated os.replace failure")
            return real_replace(src, dst)

        monkeypatch.setattr(os, "replace", failing_replace)

        with pytest.raises(OSError):
            list(spool.iter_records())

        # No partial merge output (.run.part) must remain — an
        # os.replace failure is treated exactly like a mid-write failure.
        leftover_parts = list(tmp_path.glob("*.run.part"))
        assert leftover_parts == [], f"partial merge output(s) left behind: {leftover_parts}"

        # Inputs for the failed batch are untouched, and the logical
        # state is still the complete, valid, retryable pre-failure set.
        assert spool._run_paths == pre_merge_paths
        assert all(p.exists() for p in pre_merge_paths)
        assert spool._merged is False

        monkeypatch.setattr(os, "replace", real_replace)
        seqs = [r["session_seq"] for r in spool.iter_records()]
        assert seqs == list(range(n))
    finally:
        spool.close()

    remaining = list(tmp_path.glob("cr-test-replacefail-*"))
    assert remaining == [], f"close() left owned file(s) behind: {remaining}"


def test_insert_after_query_then_commit_then_query_sees_all_records_exactly_once(tmp_path):
    """Insertion, a query (which merges and caches), then additional
    insertion, ``commit()``, and another query — the second query must
    see every original record plus every newly inserted record, each
    exactly once. Proves inserting after a query never silently ignores
    the new records (matching the original SQLite-backed implementation,
    where every query was a live view of the current table contents),
    and that no owned file leaks after ``close()``."""
    tmp_dir = tmp_path
    spool = RawRecordSpool(temp_dir=tmp_dir, prefix="cr-test-insertafterquery-")
    try:
        for i in range(5):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        spool.commit()

        first_pass = [r["session_seq"] for r in spool.iter_records()]
        assert first_pass == list(range(5))
        assert spool._merged is True  # cached after the first query

        # Insert more records AFTER a query has already merged/cached the
        # spool — must invalidate the cache rather than silently drop
        # these records from any future query.
        for i in range(5, 8):
            spool.insert(_record(seq=i, ts=i), (1, i, 0), i)
        assert spool._merged is False  # invalidated immediately on insert

        spool.commit()

        second_pass = [r["session_seq"] for r in spool.iter_records()]
        assert second_pass == list(range(8)), "later records must not be silently ignored"
        assert sorted(second_pass) == list(range(8)), "no duplicates, no loss"
    finally:
        spool.close()

    remaining = list(tmp_dir.glob("cr-test-insertafterquery-*"))
    assert remaining == [], f"close() left owned file(s) behind: {remaining}"
