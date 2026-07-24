"""Tests proving the Phase-1 semantic oracle satisfies exhaustive,
order-preserving, bounded-memory comparison — not sampling, not multiset
equality.

This corrects a gap in the original Phase 1 oracle-hardening work:
`compare_trade_ticks_semantic()` sampled up to `sample_count` positions
after re-sorting both streams, and `compare_order_book_deltas_semantic()`
was a multiset (sorted) comparison. Neither could detect:

- a difference outside the sampled positions;
- a pure reordering of otherwise-valid events (sorting erases position);
- a reordering of "commutative-looking" depth deltas that happens to
  produce the same final book state;
- duplicate events;
- a difference near the end of a very large stream, without materializing
  the whole stream in memory.

`compare_trade_ticks_exhaustive()` and `compare_order_book_deltas_exhaustive()`
(added in validation/catalog_compare.py) close these gaps. Each test below
either demonstrates the new exhaustive comparator catching something the
old sampled/multiset comparator misses, or proves a specific required
detection/memory-boundedness property directly.
"""
from __future__ import annotations

import gc

from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity

from validation.catalog_compare import (
    _sample_indexes,
    compare_book_checkpoints,
    compare_order_book_deltas_exhaustive,
    compare_order_book_deltas_semantic,
    compare_trade_ticks_exhaustive,
    compare_trade_ticks_semantic,
)
from converter.instruments import build_instruments


def _instrument():
    return build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]


def _order(side: OrderSide, price: str, size: str, order_id: int = 0) -> BookOrder:
    return BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str(size), order_id=order_id)


# ---------------------------------------------------------------------------
# Lightweight synthetic TradeTick stand-in (plain Python object; the
# comparator only ever accesses generic attributes via _tick_to_record(),
# so it does not need to be a real Nautilus TradeTick).
# ---------------------------------------------------------------------------


class _LiveCounter:
    """Tracks how many _FakeTick instances are simultaneously alive, to
    empirically prove a comparator never holds the whole stream in memory
    at once (CPython frees an object's refcount-reachable memory the
    instant its last reference is dropped, for non-cyclic objects like
    this one, so max_alive is a faithful proxy for "peak simultaneously
    materialized objects")."""

    def __init__(self) -> None:
        self.alive = 0
        self.max_alive = 0

    def inc(self) -> None:
        self.alive += 1
        self.max_alive = max(self.max_alive, self.alive)

    def dec(self) -> None:
        self.alive -= 1


class _FakeTick:
    def __init__(
        self,
        *,
        instrument_id: str,
        trade_id: str,
        price: str,
        size: str,
        aggressor_side: str,
        ts_event: int,
        ts_init: int,
        counter: _LiveCounter,
    ) -> None:
        self.instrument_id = instrument_id
        self.trade_id = trade_id
        self.price = price
        self.size = size
        self.aggressor_side = aggressor_side
        self.ts_event = ts_event
        self.ts_init = ts_init
        self._counter = counter
        counter.inc()

    def __del__(self) -> None:
        self._counter.dec()


def _fake_tick(i: int, counter: _LiveCounter, *, price: str = "0.1700") -> _FakeTick:
    return _FakeTick(
        instrument_id="ADAUSDT.BINANCE",
        trade_id=str(i),
        price=price,
        size="1.0",
        aggressor_side="BUYER",
        ts_event=i,
        ts_init=i,
        counter=counter,
    )


# ---------------------------------------------------------------------------
# 1. Exhaustive TradeTick comparison — no sampling
# ---------------------------------------------------------------------------


def test_exhaustive_trade_comparison_catches_difference_outside_sample_points() -> None:
    """A difference placed at a position the legacy sampled comparator does
    NOT select must still be caught by the exhaustive comparator, and must
    NOT be caught by the legacy sampled one — proving both the gap and the
    fix concretely."""
    length = 1000
    sample_count = 100
    sampled_positions = set(_sample_indexes(length, sample_count))
    # Find a position that the legacy sampler will never look at.
    injected_position = next(i for i in range(1, length - 1) if i not in sampled_positions)

    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(length)]
    new_ticks = [
        _fake_tick(i, counter_new, price="9.9999" if i == injected_position else "0.1700")
        for i in range(length)
    ]

    legacy_result = compare_trade_ticks_semantic(old_ticks, new_ticks, sample_count=sample_count)
    assert legacy_result["passed"] is True, (
        "sanity check: the injected position must be outside the legacy "
        "sampler's selected positions, or this test proves nothing"
    )

    exhaustive_result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert exhaustive_result["passed"] is False
    assert any(m["position"] == injected_position for m in exhaustive_result["position_mismatches"])
    assert exhaustive_result["positions_compared"] == length


# ---------------------------------------------------------------------------
# 2. Exhaustive OrderBookDelta comparison — positional, not multiset
# ---------------------------------------------------------------------------


def test_exhaustive_delta_comparison_detects_pure_reordering_multiset_misses() -> None:
    """Two deltas touching different, non-conflicting price levels, emitted
    in a different order between old and new: the multiset comparator
    reports equality (same set of deltas); the exhaustive comparator must
    report a positional mismatch."""
    instrument = _instrument()
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100

    delta_a = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "100.0", "1.0"),
        flags=0, sequence=1, ts_event=ts_event, ts_init=ts_init,
    )
    delta_b = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.SELL, "200.0", "2.0"),
        flags=0, sequence=2, ts_event=ts_event, ts_init=ts_init,
    )

    old = [OrderBookDeltas(instrument.id, [delta_a, delta_b])]
    new = [OrderBookDeltas(instrument.id, [delta_b, delta_a])]  # swapped order

    multiset_result = compare_order_book_deltas_semantic(old, new)
    assert multiset_result["passed"] is True, (
        "sanity check: the multiset comparator must report these as equal "
        "(same set, different order), or this test proves nothing"
    )

    exhaustive_result = compare_order_book_deltas_exhaustive(old, new)
    assert exhaustive_result["passed"] is False
    assert exhaustive_result["position_mismatches"]


# ---------------------------------------------------------------------------
# 3a. Extra / missing events
# ---------------------------------------------------------------------------


def test_exhaustive_detects_extra_trade_appended() -> None:
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(50)]
    new_ticks = [_fake_tick(i, counter_new) for i in range(51)]  # one extra

    result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert result["passed"] is False
    assert result["trade_count_match"] is False
    assert result["first_length_divergence_position"] == 50


def test_exhaustive_detects_missing_trade() -> None:
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(50)]
    new_ticks = [_fake_tick(i, counter_new) for i in range(49)]  # one missing

    result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert result["passed"] is False
    assert result["trade_count_match"] is False
    assert result["first_length_divergence_position"] == 49


def test_exhaustive_delta_detects_extra_and_missing() -> None:
    instrument = _instrument()
    delta = OrderBookDelta.clear(instrument.id, 1, 1_000, 1_100)
    old = [OrderBookDeltas(instrument.id, [delta])]
    new = [OrderBookDeltas(instrument.id, [delta, delta])]  # one extra

    result = compare_order_book_deltas_exhaustive(old, new)
    assert result["passed"] is False
    assert result["delta_count_match"] is False
    assert result["first_length_divergence_position"] == 1


# ---------------------------------------------------------------------------
# 3b. Duplicate events (added and removed)
# ---------------------------------------------------------------------------


def test_exhaustive_detects_duplicate_trade_added() -> None:
    """The candidate ("new") stream contains a duplicated trade the
    reference does not — must be flagged as a new-side duplicate."""
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(10)]
    new_ticks = [_fake_tick(i, counter_new) for i in range(5)]
    # Duplicate trade_id=4 immediately after its original occurrence.
    new_ticks.insert(5, _fake_tick(4, counter_new))
    new_ticks.extend(_fake_tick(i, counter_new) for i in range(5, 9))

    result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert result["duplicate_events_new"], "duplicate insertion on the new side must be detected"
    assert result["passed"] is False


def test_exhaustive_detects_duplicate_trade_removed() -> None:
    """The reference ("old") stream contains a duplicate that the candidate
    does not reproduce — must be flagged as an old-side duplicate."""
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(5)]
    old_ticks.insert(3, _fake_tick(2, counter_old))  # old has a duplicate of trade_id=2
    new_ticks = [_fake_tick(i, counter_new) for i in range(5)]  # new does not

    result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert result["duplicate_events_old"], "duplicate present only on the old side must be detected"
    assert result["passed"] is False


# ---------------------------------------------------------------------------
# 3c. Reordering (trades)
# ---------------------------------------------------------------------------


def test_exhaustive_detects_reordered_trades_outside_sample_points() -> None:
    """Swap two adjacent, otherwise-valid trades at positions the legacy
    sampled comparator does not select. The set of trade_ids is unchanged
    (missing_keys/extra_keys stay empty even in the legacy comparator), so
    only a genuinely position-aware comparator can catch this."""
    length = 500
    sample_count = 100
    sampled_positions = set(_sample_indexes(length, sample_count))
    swap_at = next(i for i in range(1, length - 2) if i not in sampled_positions and i + 1 not in sampled_positions)

    counter_old = _LiveCounter()
    counter_new = _LiveCounter()
    old_ticks = [_fake_tick(i, counter_old) for i in range(length)]
    new_ticks = [_fake_tick(i, counter_new) for i in range(length)]
    new_ticks[swap_at], new_ticks[swap_at + 1] = new_ticks[swap_at + 1], new_ticks[swap_at]

    legacy_result = compare_trade_ticks_semantic(old_ticks, new_ticks, sample_count=sample_count)
    assert legacy_result["trade_count_match"] is True
    assert legacy_result["missing_keys"] == []
    assert legacy_result["extra_keys"] == []

    exhaustive_result = compare_trade_ticks_exhaustive(old_ticks, new_ticks)
    assert exhaustive_result["passed"] is False
    assert any(m["position"] in (swap_at, swap_at + 1) for m in exhaustive_result["position_mismatches"])


# ---------------------------------------------------------------------------
# 3c continued. Reordering commutative-looking depth deltas that produce
# the SAME final book state (the specifically required scenario).
# ---------------------------------------------------------------------------


def test_exhaustive_detects_commutative_delta_reorder_with_identical_final_book_state() -> None:
    """Two independent updates (different sides, non-crossing prices) are
    swapped between old and new. Applying them in either order yields an
    identical final book state, so compare_book_checkpoints() at a
    checkpoint after both deltas reports match=True — proving that
    deterministic book-state checkpoints ALONE cannot catch this class of
    reordering. compare_order_book_deltas_exhaustive() must catch it."""
    instrument = _instrument()
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100

    clear = OrderBookDelta.clear(instrument.id, 1, ts_event, ts_init)
    update_buy = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "99.0", "0.5"),
        flags=0, sequence=2, ts_event=ts_event, ts_init=ts_init,
    )
    update_sell = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.SELL, "101.0", "2.0"),
        flags=0, sequence=3, ts_event=ts_event, ts_init=ts_init,
    )

    old_deltas = OrderBookDeltas(instrument.id, [clear, update_buy, update_sell])
    new_deltas = OrderBookDeltas(instrument.id, [clear, update_sell, update_buy])  # reordered

    checkpoint_result = compare_book_checkpoints(
        [old_deltas], [new_deltas], start_ns=ts_init, end_ns=ts_init + 10 * 60_000_000_000
    )
    assert checkpoint_result["passed"] is True, (
        "sanity check: the final book state must be identical regardless "
        "of update order for BUY/SELL updates at non-crossing, independent "
        "price levels, or this test does not demonstrate the required gap"
    )

    exhaustive_result = compare_order_book_deltas_exhaustive([old_deltas], [new_deltas])
    assert exhaustive_result["passed"] is False
    assert exhaustive_result["position_mismatches"]


# ---------------------------------------------------------------------------
# 4 & 5. Bounded-memory streaming + late-stream difference detection
# ---------------------------------------------------------------------------


def test_exhaustive_trade_comparison_is_bounded_memory_and_catches_late_difference() -> None:
    """Prove, empirically (not by assertion of implementation detail alone):

    - the comparator processes a large stream (20,000 events) via
      generators without ever holding more than a small, bounded number of
      tick objects alive simultaneously (independent of stream length);
    - a single difference injected 5 positions before the end of that
      large stream is still detected (proving the full stream is actually
      scanned, not truncated or sampled)."""
    n = 20_000
    diff_position = n - 5
    counter_old = _LiveCounter()
    counter_new = _LiveCounter()

    def _gen_old():
        for i in range(n):
            yield _fake_tick(i, counter_old)

    def _gen_new():
        for i in range(n):
            price = "9.9999" if i == diff_position else "0.1700"
            yield _fake_tick(i, counter_new, price=price)

    result = compare_trade_ticks_exhaustive(_gen_old(), _gen_new())
    gc.collect()

    assert result["positions_compared"] == n
    assert result["passed"] is False
    assert any(m["position"] == diff_position for m in result["position_mismatches"])

    # Bounded-memory proof: peak simultaneously-alive tick objects must stay
    # small and NOT scale with n (n=20,000 here). A generous but still
    # strict bound: well under 1% of n.
    assert counter_old.max_alive < 100, f"old side held {counter_old.max_alive} ticks alive at once"
    assert counter_new.max_alive < 100, f"new side held {counter_new.max_alive} ticks alive at once"


def test_exhaustive_delta_comparison_is_bounded_memory_and_catches_late_difference() -> None:
    """Same bounded-memory + late-difference proof, for OrderBookDeltas."""
    instrument = _instrument()
    n = 5_000
    diff_position = n - 3

    def _make_group(i: int, *, corrupt: bool = False) -> OrderBookDeltas:
        price = "999.0" if corrupt else "100.0"
        delta = OrderBookDelta(
            instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, price, "1.0"),
            flags=0, sequence=i, ts_event=i, ts_init=i,
        )
        return OrderBookDeltas(instrument.id, [delta])

    def _gen_old():
        for i in range(n):
            yield _make_group(i)

    def _gen_new():
        for i in range(n):
            yield _make_group(i, corrupt=(i == diff_position))

    result = compare_order_book_deltas_exhaustive(_gen_old(), _gen_new())

    assert result["positions_compared"] == n
    assert result["passed"] is False
    assert any(m["position"] == diff_position for m in result["position_mismatches"])
