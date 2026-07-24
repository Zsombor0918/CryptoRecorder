"""Semantic-oracle failure-injection proof (issue #20 Phase 1).

The oracle in validation/catalog_compare.py is the sole gate that will
later decide whether a compact replay schema is semantically equivalent to
the reference convert_day.py path. Per the approved plan, that oracle must
be proven to actually *detect* deliberately injected faults — for every
fault class in the issue's contract — before it is trusted to gate any
schema change.

Each test below starts from a known-good synthetic "old" (reference) and
"new" (candidate) pair that the oracle currently reports as passing, then
injects exactly one fault into the "new" side and asserts the relevant
comparator flips to failing. A test that does not fail on the injected
fault would mean the oracle is blind to that fault class — that is exactly
what this suite exists to catch.

No compact replay schema is implemented or modified by this suite.
"""
from __future__ import annotations

import copy

from nautilus_trader.model.data import (
    BookOrder,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth10,
    TradeTick,
)
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity

from validation.catalog_compare import (
    compare_book_checkpoints,
    compare_continuity_diagnostics_semantic,
    compare_depth10_semantic,
    compare_fenced_ranges_semantic,
    compare_instruments_semantic,
    compare_order_book_deltas_semantic,
    compare_quality_flags_semantic,
    compare_trade_ticks_semantic,
)
from converter.instruments import build_instruments


def _instrument():
    return build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]


def _order(side: OrderSide, price: str, size: str, order_id: int = 0) -> BookOrder:
    return BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str(size), order_id=order_id)


def _trade_tick(instrument, *, trade_id: str, price: str, size: str, aggressor, ts_event: int, ts_init: int):
    return TradeTick(
        instrument_id=instrument.id,
        price=Price.from_str(price),
        size=Quantity.from_str(size),
        aggressor_side=aggressor,
        trade_id=TradeId(trade_id),
        ts_event=ts_event,
        ts_init=ts_init,
    )


def _good_trade_ticks(instrument):
    return [
        _trade_tick(instrument, trade_id="1", price="0.1700", size="10.0", aggressor=AggressorSide.BUYER, ts_event=1_000, ts_init=1_100),
        _trade_tick(instrument, trade_id="2", price="0.1701", size="20.0", aggressor=AggressorSide.SELLER, ts_event=2_000, ts_init=2_100),
    ]


def test_oracle_detects_wrong_trade_price() -> None:
    instrument = _instrument()
    old_ticks = _good_trade_ticks(instrument)
    new_ticks = _good_trade_ticks(instrument)

    baseline = compare_trade_ticks_semantic(old_ticks, new_ticks)
    assert baseline["passed"] is True

    corrupted = list(new_ticks)
    corrupted[0] = _trade_tick(
        instrument, trade_id="1", price="9.9999", size="10.0",
        aggressor=AggressorSide.BUYER, ts_event=1_000, ts_init=1_100,
    )
    injected = compare_trade_ticks_semantic(old_ticks, corrupted)
    assert injected["passed"] is False
    assert any("price" in m["fields"] for m in injected["sample_mismatches"])


def test_oracle_detects_wrong_trade_timestamp() -> None:
    instrument = _instrument()
    old_ticks = _good_trade_ticks(instrument)
    new_ticks = _good_trade_ticks(instrument)
    assert compare_trade_ticks_semantic(old_ticks, new_ticks)["passed"] is True

    corrupted = list(new_ticks)
    corrupted[0] = _trade_tick(
        instrument, trade_id="1", price="0.1700", size="10.0",
        aggressor=AggressorSide.BUYER, ts_event=999_999, ts_init=1_100,
    )
    injected = compare_trade_ticks_semantic(old_ticks, corrupted)
    assert injected["passed"] is False


def test_oracle_detects_dropped_trade() -> None:
    instrument = _instrument()
    old_ticks = _good_trade_ticks(instrument)
    new_ticks = _good_trade_ticks(instrument)[:1]  # drop one trade
    injected = compare_trade_ticks_semantic(old_ticks, new_ticks)
    assert injected["passed"] is False
    assert injected["trade_count_match"] is False
    assert injected["missing_keys"]


def _good_deltas(instrument):
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100
    deltas = [
        OrderBookDelta.clear(instrument.id, 100, ts_event, ts_init),
        OrderBookDelta(
            instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "100.0", "1.0"),
            flags=32, sequence=100, ts_event=ts_event, ts_init=ts_init,
        ),
        OrderBookDelta(
            instrument.id, BookAction.UPDATE, _order(OrderSide.SELL, "101.0", "2.0"),
            flags=32 | 128, sequence=100, ts_event=ts_event, ts_init=ts_init,
        ),
    ]
    return OrderBookDeltas(instrument.id, deltas)


def test_oracle_detects_dropped_delta() -> None:
    instrument = _instrument()
    old = [_good_deltas(instrument)]
    new_full = [_good_deltas(instrument)]
    baseline = compare_order_book_deltas_semantic(old, new_full)
    assert baseline["passed"] is True

    truncated = OrderBookDeltas(instrument.id, list(_good_deltas(instrument).deltas)[:-1])
    injected = compare_order_book_deltas_semantic(old, [truncated])
    assert injected["passed"] is False
    assert injected["delta_count_match"] is False


def test_oracle_detects_wrong_sequence_number() -> None:
    instrument = _instrument()
    old = [_good_deltas(instrument)]
    good_deltas = list(_good_deltas(instrument).deltas)
    good_deltas[1] = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "100.0", "1.0"),
        flags=32, sequence=999, ts_event=1_000_000_000, ts_init=1_000_000_100,
    )
    corrupted = OrderBookDeltas(instrument.id, good_deltas)
    injected = compare_order_book_deltas_semantic(old, [corrupted])
    assert injected["passed"] is False
    assert any("sequence" in m["fields"] for m in injected["sample_mismatches"])


def test_oracle_detects_wrong_flag() -> None:
    instrument = _instrument()
    old = [_good_deltas(instrument)]
    good_deltas = list(_good_deltas(instrument).deltas)
    good_deltas[2] = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.SELL, "101.0", "2.0"),
        flags=0,  # was 32|128 — drop the snapshot/last flags
        sequence=100, ts_event=1_000_000_000, ts_init=1_000_000_100,
    )
    corrupted = OrderBookDeltas(instrument.id, good_deltas)
    injected = compare_order_book_deltas_semantic(old, [corrupted])
    assert injected["passed"] is False
    assert any("flags" in m["fields"] for m in injected["sample_mismatches"])


def test_oracle_detects_wrong_side() -> None:
    instrument = _instrument()
    old = [_good_deltas(instrument)]
    good_deltas = list(_good_deltas(instrument).deltas)
    good_deltas[1] = OrderBookDelta(
        instrument.id, BookAction.UPDATE, _order(OrderSide.SELL, "100.0", "1.0"),  # was BUY
        flags=32, sequence=100, ts_event=1_000_000_000, ts_init=1_000_000_100,
    )
    corrupted = OrderBookDeltas(instrument.id, good_deltas)
    injected = compare_order_book_deltas_semantic(old, [corrupted])
    assert injected["passed"] is False
    assert any("side" in m["fields"] for m in injected["sample_mismatches"])


def test_oracle_detects_missing_snapshot_seed_clear() -> None:
    """A missing CLEAR delta (snapshot seed) must be detected — this is the
    'snapshot seed' / 'clear/reset behavior' requirement from the issue."""
    instrument = _instrument()
    old = [_good_deltas(instrument)]
    good_deltas = list(_good_deltas(instrument).deltas)
    without_clear = OrderBookDeltas(instrument.id, good_deltas[1:])  # drop CLEAR
    injected = compare_order_book_deltas_semantic(old, [without_clear])
    assert injected["passed"] is False
    assert injected["delta_count_match"] is False


def _good_depth10(instrument):
    bids = [_order(OrderSide.BUY, f"{100 - i}.0", "1.0") for i in range(10)]
    asks = [_order(OrderSide.SELL, f"{101 + i}.0", "1.0") for i in range(10)]
    return OrderBookDepth10(
        instrument_id=instrument.id,
        bids=bids,
        asks=asks,
        bid_counts=[1] * 10,
        ask_counts=[1] * 10,
        flags=0,
        sequence=1,
        ts_event=1_000_000_000,
        ts_init=1_000_000_100,
    )


def test_oracle_detects_wrong_depth10_level() -> None:
    instrument = _instrument()
    old = [_good_depth10(instrument)]
    baseline_new = [_good_depth10(instrument)]
    baseline = compare_depth10_semantic(old, baseline_new)
    assert baseline["passed"] is True

    corrupted_bids = [_order(OrderSide.BUY, f"{100 - i}.0", "1.0") for i in range(10)]
    corrupted_bids[0] = _order(OrderSide.BUY, "999.0", "1.0")  # wrong top-of-book price
    corrupted = OrderBookDepth10(
        instrument_id=instrument.id,
        bids=corrupted_bids,
        asks=[_order(OrderSide.SELL, f"{101 + i}.0", "1.0") for i in range(10)],
        bid_counts=[1] * 10,
        ask_counts=[1] * 10,
        flags=0,
        sequence=1,
        ts_event=1_000_000_000,
        ts_init=1_000_000_100,
    )
    injected = compare_depth10_semantic(old, [corrupted])
    assert injected["passed"] is False
    assert injected["sample_mismatches"]


def test_oracle_detects_mismatched_checkpoint() -> None:
    """A book-state checkpoint mismatch (deterministic reconstruction from
    deltas) must be detected — this proves the checkpoint reconstruction
    itself is sensitive to a real difference, not just delta-by-delta."""
    instrument = _instrument()
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100
    old_deltas = OrderBookDeltas(
        instrument.id,
        [
            OrderBookDelta.clear(instrument.id, 1, ts_event, ts_init),
            OrderBookDelta(
                instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "100.0", "1.0"),
                flags=32, sequence=1, ts_event=ts_event, ts_init=ts_init,
            ),
        ],
    )
    # Candidate ends up with a different best bid at the checkpoint — the
    # deterministic reconstruction must catch this.
    new_deltas = OrderBookDeltas(
        instrument.id,
        [
            OrderBookDelta.clear(instrument.id, 1, ts_event, ts_init),
            OrderBookDelta(
                instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "50.0", "1.0"),
                flags=32, sequence=1, ts_event=ts_event, ts_init=ts_init,
            ),
        ],
    )
    result = compare_book_checkpoints(
        [old_deltas], [new_deltas], start_ns=ts_init, end_ns=ts_init + 10 * 60_000_000_000
    )
    assert result["passed"] is False
    assert any(not cp["match"] for cp in result["checkpoints"])


def test_oracle_baseline_checkpoints_match_when_identical() -> None:
    """Sanity check: identical deltas on both sides must pass the checkpoint
    comparison — otherwise the mismatch test above would be uninformative."""
    instrument = _instrument()
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100
    deltas = OrderBookDeltas(
        instrument.id,
        [
            OrderBookDelta.clear(instrument.id, 1, ts_event, ts_init),
            OrderBookDelta(
                instrument.id, BookAction.UPDATE, _order(OrderSide.BUY, "100.0", "1.0"),
                flags=32, sequence=1, ts_event=ts_event, ts_init=ts_init,
            ),
        ],
    )
    result = compare_book_checkpoints(
        [deltas], [deltas], start_ns=ts_init, end_ns=ts_init + 10 * 60_000_000_000
    )
    assert result["passed"] is True


def test_oracle_detects_instrument_precision_mismatch() -> None:
    """Instrument identity/precision comparison (Phase 1 coverage-gap fix):
    a wrong price_precision must be detected even though the instrument_id
    set is unchanged."""
    instrument = _instrument()
    old_instruments = {str(instrument.id): instrument}

    baseline = compare_instruments_semantic(old_instruments, old_instruments)
    assert baseline["passed"] is True

    # Build a second instrument object with a different declared precision
    # by mutating the private slot Nautilus uses is not possible (frozen
    # Cython object) — instead simulate a corrupted "new" side by comparing
    # against a record dict with a deliberately wrong precision using the
    # same instrument_id.
    class _FakeInstrument:
        id = instrument.id
        price_precision = instrument.price_precision + 1  # corrupted
        size_precision = instrument.size_precision
        price_increment = instrument.price_increment
        size_increment = instrument.size_increment

    injected = compare_instruments_semantic(old_instruments, {str(instrument.id): _FakeInstrument()})
    assert injected["passed"] is False
    assert injected["precision_mismatches"]
    assert injected["precision_mismatches"][0]["fields"].get("price_precision")


def test_oracle_detects_missing_instrument() -> None:
    instrument = _instrument()
    old_instruments = {str(instrument.id): instrument}
    injected = compare_instruments_semantic(old_instruments, {})
    assert injected["passed"] is False
    assert injected["missing_in_new"] == [str(instrument.id)]


def test_oracle_detects_continuity_count_mismatch() -> None:
    """Sync/desync/resync/fenced-range count comparison (Phase 1
    coverage-gap fix): a wrong resync count between the reference
    per-symbol report and the candidate manifest's depth_diagnostics must
    be detected."""
    old_per_symbol_depth = {
        "snapshot_seed_count": 1,
        "resync_count": 2,
        "desync_events": 1,
        "fenced_ranges": 3,
    }
    matching_new = {
        "snapshot_seeds": 1,
        "resyncs": 2,
        "desyncs": 1,
        "fenced_range_count": 3,
    }
    baseline = compare_continuity_diagnostics_semantic(old_per_symbol_depth, matching_new)
    assert baseline["passed"] is True

    corrupted_new = dict(matching_new)
    corrupted_new["resyncs"] = 0  # dropped resyncs on the candidate side
    injected = compare_continuity_diagnostics_semantic(old_per_symbol_depth, corrupted_new)
    assert injected["passed"] is False
    assert "resync_count" in injected["field_mismatches"]


def test_oracle_detects_desync_count_mismatch() -> None:
    old_per_symbol_depth = {"snapshot_seed_count": 0, "resync_count": 0, "desync_events": 2, "fenced_ranges": 0}
    new = {"snapshot_seeds": 0, "resyncs": 0, "desyncs": 5, "fenced_range_count": 0}
    injected = compare_continuity_diagnostics_semantic(old_per_symbol_depth, new)
    assert injected["passed"] is False
    assert "desync_events" in injected["field_mismatches"]


def test_oracle_detects_fenced_range_count_mismatch() -> None:
    old_per_symbol_depth = {"snapshot_seed_count": 0, "resync_count": 0, "desync_events": 0, "fenced_ranges": 3}
    new = {"snapshot_seeds": 0, "resyncs": 0, "desyncs": 0, "fenced_range_count": 1}
    injected = compare_continuity_diagnostics_semantic(old_per_symbol_depth, new)
    assert injected["passed"] is False
    assert "fenced_range_count" in injected["field_mismatches"]


def test_oracle_detects_missing_fenced_range_by_content() -> None:
    old_fences = [
        {"venue": "BINANCE_SPOT", "symbol": "ADAUSDT", "start_ts_ns": 100, "end_ts_ns": 200, "severity": "high"},
        {"venue": "BINANCE_SPOT", "symbol": "ADAUSDT", "start_ts_ns": 300, "end_ts_ns": 400, "severity": "low"},
    ]
    matching_new = copy.deepcopy(old_fences)
    baseline = compare_fenced_ranges_semantic(old_fences, matching_new)
    assert baseline["passed"] is True

    truncated_new = copy.deepcopy(old_fences)[:1]
    injected = compare_fenced_ranges_semantic(old_fences, truncated_new)
    assert injected["passed"] is False
    assert injected["missing_in_new"]


def test_oracle_detects_quality_flag_content_mismatch() -> None:
    old_flags = ['{"gap": false}', '{"gap": true, "severity": "low"}', None]
    matching_new = ['{"gap": false}', '{"gap": true, "severity": "low"}', None]
    baseline = compare_quality_flags_semantic(old_flags, matching_new)
    assert baseline["passed"] is True

    corrupted_new = ['{"gap": false}', '{"gap": false}', None]  # dropped the gap=true entry
    injected = compare_quality_flags_semantic(old_flags, corrupted_new)
    assert injected["passed"] is False
    assert injected["mismatches"]


def test_reference_and_candidate_decoders_are_independently_implemented() -> None:
    """Independence guard (Phase 1 requirement): the candidate depth-adapter
    module must not import its book-replay logic from the *catalog
    comparison* module itself, and the comparator module must not import
    replay-schema-specific decoding helpers. This is a structural proxy for
    'a shared bug in new compact-decoding logic could not silently pass both
    sides' — the only code the two paths are permitted to share is the
    already-proven-shared book-replay engine in converter/depth_phase2.py,
    not any new schema-specific decoder."""
    import ast
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent
    catalog_compare_path = repo_root / "validation" / "catalog_compare.py"
    depth_adapter_path = repo_root / "stores" / "replay_depth_adapter.py"

    def _imported_modules(path: Path) -> set[str]:
        tree = ast.parse(path.read_text())
        modules: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.add(node.module)
        return modules

    compare_imports = _imported_modules(catalog_compare_path)
    adapter_imports = _imported_modules(depth_adapter_path)

    # The comparator must not import the candidate's schema-specific decoder.
    assert "stores.replay_depth_adapter" not in compare_imports
    assert "stores.replay_reader" not in compare_imports
    assert "stores.replay_writer" not in compare_imports
    # The depth adapter must not import the comparator (which would let a
    # shared-bug-shaped dependency flow the other way).
    assert "validation.catalog_compare" not in adapter_imports
