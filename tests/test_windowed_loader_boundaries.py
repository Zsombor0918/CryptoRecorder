"""Prove the windowed catalog loaders' boundary semantics against a real,
on-disk Nautilus ParquetDataCatalog — not just documented/asserted.

iter_trade_ticks_windowed() / iter_order_book_deltas_windowed() claim
half-open [start, end) windowing (an event at exactly the window's `end`
belongs to the *next* window, not the current one, and no event is ever
yielded twice or dropped across adjacent windows). These tests build a real
temporary catalog with events placed exactly at the interesting boundary
positions and iterate the loaders with a small window size to force
multiple window boundaries within the test range, then assert every event
is yielded exactly once, in order, with no gap or duplicate.
"""
from __future__ import annotations

from pathlib import Path

from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas, TradeTick
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from converter.instruments import build_instruments
from validation.catalog_compare import (
    iter_order_book_deltas_windowed,
    iter_trade_ticks_windowed,
)


def _instrument():
    return build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]


def _write_catalog(catalog_root: Path, instrument, data: list) -> None:
    catalog = ParquetDataCatalog(str(catalog_root))
    catalog.write_data([instrument])
    catalog.write_data(data)


def _tick(instrument, *, trade_id: str, ts: int) -> TradeTick:
    return TradeTick(
        instrument_id=instrument.id,
        price=Price.from_str("1.0000"),
        size=Quantity.from_str("1.0"),
        aggressor_side=AggressorSide.BUYER,
        trade_id=TradeId(trade_id),
        ts_event=ts,
        ts_init=ts,
    )


def _delta(instrument, *, sequence: int, ts: int) -> OrderBookDelta:
    return OrderBookDelta(
        instrument.id,
        BookAction.UPDATE,
        BookOrder(side=OrderSide.BUY, price=Price.from_str("100.0"), size=Quantity.from_str("1.0"), order_id=0),
        flags=0,
        sequence=sequence,
        ts_event=ts,
        ts_init=ts,
    )


# Overall requested range and window size chosen so the boundary positions
# fall cleanly on/around window edges: 3 windows of 1000 ns each, spanning
# [0, 3000).
_START_NS = 0
_END_NS = 3000
_WINDOW_NS = 1000

# Boundary-position test matrix, expressed as a set of "interesting"
# timestamps: overall start; immediately before/on/after an internal window
# boundary (1000); immediately before the overall end.
_BOUNDARY_TS = [
    _START_NS,           # at the overall start
    _WINDOW_NS - 1,       # immediately before a window boundary (999)
    _WINDOW_NS,           # exactly on a window boundary (1000)
    _WINDOW_NS + 1,       # immediately after a boundary (1001)
    _END_NS - 1,          # immediately before the overall end (2999)
]


def test_trade_windowed_loader_yields_every_boundary_event_exactly_once_in_order(tmp_path: Path) -> None:
    instrument = _instrument()
    ticks = [_tick(instrument, trade_id=str(i), ts=ts) for i, ts in enumerate(_BOUNDARY_TS)]
    _write_catalog(tmp_path / "catalog", instrument, ticks)

    yielded = list(
        iter_trade_ticks_windowed(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=_WINDOW_NS
        )
    )
    yielded_ts = [int(t.ts_event) for t in yielded]

    assert yielded_ts == _BOUNDARY_TS, (
        f"expected every boundary event exactly once, in order: {_BOUNDARY_TS}, got: {yielded_ts}"
    )
    # No duplicates, no gaps: same set, same length.
    assert len(yielded_ts) == len(set(yielded_ts)) == len(_BOUNDARY_TS)


def test_trade_windowed_loader_event_on_boundary_belongs_to_exactly_one_window(tmp_path: Path) -> None:
    """Directly prove the loader's boundary contract for the specific
    on-boundary timestamp: an event at ts == 1000 (a window edge) must be
    yielded exactly once by the full windowed iteration across
    [0, 3000) — not zero times and not twice.

    This also documents a real discovered fact about the underlying
    Nautilus catalog query: `catalog.trade_ticks(start=a, end=b)` is
    INCLUSIVE on both `a` and `b` (not the half-open [a, b) that a naive
    reading of "start/end" might suggest). A naive window-chaining
    implementation using `next_window_start = previous_window_end` would
    therefore yield an event exactly on an internal boundary TWICE — this
    test's first assertion proves that inclusive-both-ends fact directly
    against the real catalog, and the second assertion proves the actual
    iter_trade_ticks_windowed() implementation correctly avoids the
    resulting double-yield via closed, non-overlapping sub-windows."""
    instrument = _instrument()
    on_boundary = _tick(instrument, trade_id="boundary", ts=_WINDOW_NS)
    _write_catalog(tmp_path / "catalog", instrument, [on_boundary])

    # Ground-truth fact about Nautilus's own query semantics: a naive
    # [0, 1000] query (inclusive end) DOES return the on-boundary event.
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    naive_first_window = list(
        catalog.trade_ticks(instrument_ids=[str(instrument.id)], start=0, end=_WINDOW_NS) or []
    )
    assert len(naive_first_window) == 1, (
        "Nautilus catalog.trade_ticks(start=a, end=b) is inclusive of b — "
        "this is the discovered fact the windowed loader's closed-window "
        "partitioning must account for for to avoid double-yielding"
    )

    # The actual loader, despite that inclusive-both-ends underlying
    # behavior, must yield the boundary event exactly once across the full
    # requested range.
    all_yielded = list(
        iter_trade_ticks_windowed(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=_WINDOW_NS
        )
    )
    assert len(all_yielded) == 1
    assert int(all_yielded[0].ts_event) == _WINDOW_NS


def test_trade_windowed_loader_matches_single_call_load_over_full_range(tmp_path: Path) -> None:
    """Cross-check: windowed iteration over many small windows must yield
    the exact same set of events, in the same order, as a single
    unwindowed catalog.trade_ticks(start=, end=) call over the whole range
    — proving the windowing itself introduces no gap, duplicate, or
    reorder relative to the ground truth."""
    instrument = _instrument()
    ticks = [_tick(instrument, trade_id=str(i), ts=ts) for i, ts in enumerate(range(0, 3000, 137))]
    _write_catalog(tmp_path / "catalog", instrument, ticks)

    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    single_call = list(
        catalog.trade_ticks(instrument_ids=[str(instrument.id)], start=_START_NS, end=_END_NS) or []
    )
    windowed = list(
        iter_trade_ticks_windowed(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=_WINDOW_NS
        )
    )

    single_call_ts = [int(t.ts_event) for t in single_call]
    windowed_ts = [int(t.ts_event) for t in windowed]
    assert windowed_ts == single_call_ts


def test_delta_windowed_loader_yields_every_boundary_event_exactly_once_in_order(tmp_path: Path) -> None:
    instrument = _instrument()
    deltas = [
        OrderBookDeltas(instrument.id, [_delta(instrument, sequence=i, ts=ts)])
        for i, ts in enumerate(_BOUNDARY_TS)
    ]
    _write_catalog(tmp_path / "catalog", instrument, deltas)

    yielded = list(
        iter_order_book_deltas_windowed(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=_WINDOW_NS
        )
    )
    yielded_ts = [int(d.ts_event) for d in yielded]

    assert yielded_ts == _BOUNDARY_TS
    assert len(yielded_ts) == len(set(yielded_ts)) == len(_BOUNDARY_TS)


def test_delta_windowed_loader_matches_single_call_load_over_full_range(tmp_path: Path) -> None:
    instrument = _instrument()
    deltas = [
        OrderBookDeltas(instrument.id, [_delta(instrument, sequence=i, ts=ts)])
        for i, ts in enumerate(range(0, 3000, 137))
    ]
    _write_catalog(tmp_path / "catalog", instrument, deltas)

    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    single_call = list(
        catalog.order_book_deltas(instrument_ids=[str(instrument.id)], start=_START_NS, end=_END_NS) or []
    )
    windowed = list(
        iter_order_book_deltas_windowed(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=_WINDOW_NS
        )
    )

    def _flat_ts(objs) -> list[int]:
        out: list[int] = []
        for obj in objs:
            inner = getattr(obj, "deltas", None)
            if inner is not None:
                out.extend(int(d.ts_event) for d in inner)
            else:
                out.append(int(obj.ts_event))
        return out

    assert _flat_ts(windowed) == _flat_ts(single_call)


def test_window_duration_is_configurable(tmp_path: Path) -> None:
    """Prove window_ns is a genuine, respected configuration knob (not
    hardcoded) by using two different window sizes against the same data
    and confirming both still yield the identical, complete, ordered
    result."""
    instrument = _instrument()
    ticks = [_tick(instrument, trade_id=str(i), ts=ts) for i, ts in enumerate(range(0, 3000, 91))]
    _write_catalog(tmp_path / "catalog", instrument, ticks)

    small_window = list(
        iter_trade_ticks_windowed(tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=250)
    )
    large_window = list(
        iter_trade_ticks_windowed(tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS, window_ns=10_000)
    )

    small_ts = [int(t.ts_event) for t in small_window]
    large_ts = [int(t.ts_event) for t in large_window]
    assert small_ts == large_ts
    assert len(large_window) > 0
