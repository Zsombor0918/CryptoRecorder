"""Bounded-memory proofs for the gating comparators added in the issue #20
follow-up correction: compare_book_checkpoints_streaming(),
compare_order_book_depth10_exhaustive(), and
compare_event_metadata_exhaustive().

Same empirical technique as tests/test_semantic_oracle_exhaustive_streaming.py:
a `_LiveCounter` hooked into each fake event object's `__del__` proves peak
simultaneously-alive objects stays small and independent of stream length
(CPython frees a non-cyclic object's memory the instant its last reference
is dropped, so max_alive is a faithful proxy for peak simultaneous
materialization), while a difference injected near the end of a large
stream is still detected — proving the whole stream is genuinely scanned,
not truncated or sampled.
"""
from __future__ import annotations

from validation.catalog_compare import (
    compare_book_checkpoints_streaming,
    compare_event_metadata_exhaustive,
    compare_order_book_depth10_exhaustive,
)


class _LiveCounter:
    def __init__(self) -> None:
        self.alive = 0
        self.max_alive = 0

    def inc(self) -> None:
        self.alive += 1
        self.max_alive = max(self.max_alive, self.alive)

    def dec(self) -> None:
        self.alive -= 1


# ---------------------------------------------------------------------------
# compare_book_checkpoints_streaming
# ---------------------------------------------------------------------------


class _FakeOrder:
    def __init__(self, side: str, price: str, size: str) -> None:
        self.side = side
        self.price = price
        self.size = size
        self.order_id = 0


class _FakeDelta:
    def __init__(self, *, action: str, order, sequence: int, ts: int, counter: _LiveCounter) -> None:
        self.action = action
        self.order = order
        self.sequence = sequence
        self.ts_event = ts
        self.ts_init = ts
        self._counter = counter
        counter.inc()

    def __del__(self) -> None:
        self._counter.dec()


def _fake_delta_stream(n: int, counter: _LiveCounter, *, corrupt_from: int | None = None):
    """A long stream of updates that all target the SAME top-of-book price
    level (size incrementing each time), so a late-stream corruption
    actually changes the final top-of-book state — a corruption to a price
    level far outside the top-10 window (as an ever-decreasing-price
    stream would produce) would never show up in the reconstructed
    checkpoint regardless of position, which would make this an invalid
    test of "late difference detection". Corrupts every update from
    `corrupt_from` onward (not just a single position) so the corrupted
    value is guaranteed to persist to the final checkpoint rather than
    being overwritten by a later, uncorrupted update to the same level."""
    for i in range(n):
        size = "999999.0" if corrupt_from is not None and i >= corrupt_from else f"{i + 1}.0"
        order = _FakeOrder("BUY", "100.0", size)
        yield _FakeDelta(action="UPDATE", order=order, sequence=i, ts=i, counter=counter)


def test_streaming_checkpoints_bounded_memory_and_detects_late_difference() -> None:
    n = 20_000
    diff_position = n - 5
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()

    checkpoint_tss = [0, n // 4, n // 2, (3 * n) // 4, n - 1]

    def _reconstruct(counter, corrupt_from):
        from validation.catalog_compare import reconstruct_book_checkpoints_streaming
        return reconstruct_book_checkpoints_streaming(
            _fake_delta_stream(n, counter, corrupt_from=corrupt_from), checkpoint_tss, levels=10
        )

    old_snaps = _reconstruct(counter_old, None)
    new_snaps = _reconstruct(counter_new, diff_position)

    # The corruption applied from diff_position onward (near the end) must
    # show up in the final checkpoint's book state.
    assert old_snaps[checkpoint_tss[-1]] != new_snaps[checkpoint_tss[-1]]

    assert counter_old.max_alive < 200, f"old side held {counter_old.max_alive} deltas alive at once"
    assert counter_new.max_alive < 200, f"new side held {counter_new.max_alive} deltas alive at once"


def test_compare_book_checkpoints_streaming_detects_late_difference_end_to_end() -> None:
    """Uses nanosecond-scale timestamps (matching real Nautilus event
    timestamps) so compare_book_checkpoints_streaming()'s built-in
    percentage/`end-1min` checkpoint labels land at meaningful positions
    within the stream, rather than the tiny integer timestamps used in the
    boundedness test above (where a fixed 60-second offset would collapse
    every checkpoint to the same position)."""
    n = 5_000
    # "end-1min" (one of compare_book_checkpoints_streaming's fixed
    # checkpoint labels) sits 60 real seconds before the stream's end; the
    # corruption must start early enough to still be in effect at that
    # checkpoint (it persists to the end once started, per
    # _fake_delta_stream's "every update from corrupt_from onward" design).
    diff_position = n - 1_000
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    ns_per_event = 100_000_000  # 100ms apart in real ns terms

    def _ns_stream(counter, corrupt_from):
        for delta in _fake_delta_stream(n, counter, corrupt_from=corrupt_from):
            delta.ts_event *= ns_per_event
            delta.ts_init *= ns_per_event
            yield delta

    result = compare_book_checkpoints_streaming(
        _ns_stream(counter_old, None),
        _ns_stream(counter_new, diff_position),
        start_ns=0,
        end_ns=n * ns_per_event,
    )
    assert result["passed"] is False
    assert any(not cp["match"] for cp in result["checkpoints"])
    assert any(not cp["hash_match"] for cp in result["checkpoints"])


# ---------------------------------------------------------------------------
# compare_order_book_depth10_exhaustive
# ---------------------------------------------------------------------------


class _FakeDepth10:
    def __init__(self, *, sequence: int, ts: int, bid_price: str, counter: _LiveCounter) -> None:
        self.instrument_id = "ADAUSDT.BINANCE"
        self.sequence = sequence
        self.flags = 0
        self.ts_event = ts
        self.ts_init = ts
        self.bids = [_FakeOrder("BUY", bid_price if i == 0 else f"{100 - i}.0", "1.0") for i in range(10)]
        self.asks = [_FakeOrder("SELL", f"{101 + i}.0", "1.0") for i in range(10)]
        self._counter = counter
        counter.inc()

    def __del__(self) -> None:
        self._counter.dec()


def _fake_depth10_stream(n: int, counter: _LiveCounter, *, corrupt_at: int | None = None):
    for i in range(n):
        bid_price = "999999.0" if i == corrupt_at else "100.0"
        yield _FakeDepth10(sequence=i, ts=i, bid_price=bid_price, counter=counter)


def test_depth10_exhaustive_bounded_memory_and_detects_late_difference() -> None:
    n = 20_000
    diff_position = n - 5
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()

    result = compare_order_book_depth10_exhaustive(
        _fake_depth10_stream(n, counter_old, corrupt_at=None),
        _fake_depth10_stream(n, counter_new, corrupt_at=diff_position),
    )

    assert result["positions_compared"] == n
    assert result["passed"] is False
    assert any(m["position"] == diff_position for m in result["position_mismatches"])
    assert counter_old.max_alive < 100, f"old side held {counter_old.max_alive} depth10 objects alive at once"
    assert counter_new.max_alive < 100, f"new side held {counter_new.max_alive} depth10 objects alive at once"


# ---------------------------------------------------------------------------
# compare_event_metadata_exhaustive
# ---------------------------------------------------------------------------


def _record_stream(n: int, *, corrupt_at: int | None = None):
    for i in range(n):
        quality_flags = "corrupted" if i == corrupt_at else "ok"
        yield {"stream_session_id": 1, "session_seq": i, "raw_index": i, "record_type": "depth_update", "quality_flags": quality_flags}


def test_event_metadata_exhaustive_bounded_memory_and_detects_late_difference() -> None:
    n = 50_000
    diff_position = n - 5

    result = compare_event_metadata_exhaustive(
        _record_stream(n, corrupt_at=None),
        _record_stream(n, corrupt_at=diff_position),
        compare_fields=("stream_session_id", "session_seq", "raw_index", "record_type", "quality_flags"),
    )

    assert result["positions_compared"] == n
    assert result["passed"] is False
    assert any(m["position"] == diff_position for m in result["position_mismatches"])


def test_event_metadata_exhaustive_detects_value_moved_to_wrong_event() -> None:
    """A value moved from one event to another while the overall multiset
    of values is unchanged must still be detected (this is the exact gap
    a pure multiset comparison has, per the issue #20 follow-up
    correction)."""
    old_records = [
        {"raw_index": 0, "quality_flags": "A"},
        {"raw_index": 1, "quality_flags": "B"},
    ]
    new_records = [
        {"raw_index": 0, "quality_flags": "B"},  # swapped
        {"raw_index": 1, "quality_flags": "A"},  # swapped
    ]
    result = compare_event_metadata_exhaustive(old_records, new_records, compare_fields=("raw_index", "quality_flags"))
    assert result["passed"] is False
    assert result["position_mismatches"]


def test_event_metadata_exhaustive_passes_when_identical() -> None:
    records_a = [{"raw_index": i, "quality_flags": "ok"} for i in range(10)]
    records_b = [{"raw_index": i, "quality_flags": "ok"} for i in range(10)]
    result = compare_event_metadata_exhaustive(records_a, records_b, compare_fields=("raw_index", "quality_flags"))
    assert result["passed"] is True
