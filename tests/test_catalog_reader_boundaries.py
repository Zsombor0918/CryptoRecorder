"""Prove the bounded catalog readers' boundary semantics against a real,
on-disk Nautilus ``ParquetDataCatalog``.

``iter_trade_ticks_bounded()`` and
``iter_order_book_deltas_bounded()`` expose a half-open caller contract
``[start, end)`` while the underlying Nautilus selector is inclusive at both
ends. The active implementation performs one inclusive file selection and
streams bounded Arrow batches; it does not create time sub-windows.
These tests place events at the interesting endpoints and assert exact
ordering with no duplicates.
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
    iter_order_book_deltas_bounded,
    iter_trade_ticks_bounded,
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


def _delta(instrument, *, sequence: int, ts: int, is_last: bool = True) -> OrderBookDelta:
    """`is_last` defaults to True: each call represents one COMPLETE,
    standalone update group (a real depth_update always ends with the
    F_LAST flag on its final delta — see
    converter.depth_phase2/OrderBookDeltas.batch()). Grouping into
    OrderBookDeltas is defined by this flag, not by time-window
    boundaries; tests exercising multiple deltas per logical group must
    pass is_last=False for all but the final delta explicitly."""
    return OrderBookDelta(
        instrument.id,
        BookAction.UPDATE,
        BookOrder(side=OrderSide.BUY, price=Price.from_str("100.0"), size=Quantity.from_str("1.0"), order_id=0),
        flags=128 if is_last else 0,
        sequence=sequence,
        ts_event=ts,
        ts_init=ts,
    )


# Overall requested range plus an interior inclusive-query boundary.
_START_NS = 0
_END_NS = 3000
_INTERIOR_BOUNDARY_NS = 1000

# Boundary-position test matrix, expressed as a set of "interesting"
# timestamps: overall start; immediately before/on/after the selected interior
# timestamp (1000); immediately before the overall end.
_BOUNDARY_TS = [
    _START_NS,                    # at the overall start
    _INTERIOR_BOUNDARY_NS - 1,    # immediately before the interior timestamp (999)
    _INTERIOR_BOUNDARY_NS,        # exactly on the interior timestamp (1000)
    _INTERIOR_BOUNDARY_NS + 1,    # immediately after the interior timestamp (1001)
    _END_NS - 1,                  # immediately before the overall end (2999)
]


def test_trade_reader_yields_every_boundary_event_exactly_once_in_order(tmp_path: Path) -> None:
    instrument = _instrument()
    ticks = [_tick(instrument, trade_id=str(i), ts=ts) for i, ts in enumerate(_BOUNDARY_TS)]
    _write_catalog(tmp_path / "catalog", instrument, ticks)

    yielded = list(
        iter_trade_ticks_bounded(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS
        )
    )
    yielded_ts = [int(t.ts_event) for t in yielded]

    assert yielded_ts == _BOUNDARY_TS, (
        f"expected every boundary event exactly once, in order: {_BOUNDARY_TS}, got: {yielded_ts}"
    )
    # No duplicates, no gaps: same set, same length.
    assert len(yielded_ts) == len(set(yielded_ts)) == len(_BOUNDARY_TS)


def test_trade_loader_inclusive_selector_row_is_emitted_once(tmp_path: Path) -> None:
    """Prove the loader's inclusive-selector/half-open-wrapper contract.

    This also documents a real discovered fact about the underlying
    Nautilus catalog query: `catalog.trade_ticks(start=a, end=b)` is
    INCLUSIVE on both `a` and `b` (not the half-open [a, b) that a naive
    reading of "start/end" might suggest). The first assertion proves that
    fact directly; the second proves the wrapper emits the row once across
    the requested half-open range."""
    instrument = _instrument()
    on_boundary = _tick(
        instrument, trade_id="boundary", ts=_INTERIOR_BOUNDARY_NS
    )
    _write_catalog(tmp_path / "catalog", instrument, [on_boundary])

    # Ground-truth fact about Nautilus's own query semantics: a naive
    # [0, 1000] query (inclusive end) DOES return the on-boundary event.
    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    inclusive_query = list(
        catalog.trade_ticks(
            instrument_ids=[str(instrument.id)],
            start=0,
            end=_INTERIOR_BOUNDARY_NS,
        )
        or []
    )
    assert len(inclusive_query) == 1, (
        "Nautilus catalog.trade_ticks(start=a, end=b) is inclusive of b — "
        "this is the discovered fact the bounded reader's half-open wrapper "
        "must account for"
    )

    # The actual loader, despite that inclusive-both-ends underlying
    # behavior, must yield the boundary event exactly once across the full
    # requested range.
    all_yielded = list(
        iter_trade_ticks_bounded(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS
        )
    )
    assert len(all_yielded) == 1
    assert int(all_yielded[0].ts_event) == _INTERIOR_BOUNDARY_NS


def test_trade_reader_matches_single_call_load_over_full_range(tmp_path: Path) -> None:
    """Cross-check the bounded reader against one Nautilus catalog query."""
    instrument = _instrument()
    ticks = [_tick(instrument, trade_id=str(i), ts=ts) for i, ts in enumerate(range(0, 3000, 137))]
    _write_catalog(tmp_path / "catalog", instrument, ticks)

    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    single_call = list(
        catalog.trade_ticks(instrument_ids=[str(instrument.id)], start=_START_NS, end=_END_NS) or []
    )
    bounded = list(
        iter_trade_ticks_bounded(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS
        )
    )

    single_call_ts = [int(t.ts_event) for t in single_call]
    bounded_ts = [int(t.ts_event) for t in bounded]
    assert bounded_ts == single_call_ts


def test_delta_reader_yields_every_boundary_event_exactly_once_in_order(tmp_path: Path) -> None:
    instrument = _instrument()
    deltas = [
        OrderBookDeltas(instrument.id, [_delta(instrument, sequence=i, ts=ts)])
        for i, ts in enumerate(_BOUNDARY_TS)
    ]
    _write_catalog(tmp_path / "catalog", instrument, deltas)

    yielded = list(
        iter_order_book_deltas_bounded(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS
        )
    )
    yielded_ts = [int(d.ts_event) for d in yielded]

    assert yielded_ts == _BOUNDARY_TS
    assert len(yielded_ts) == len(set(yielded_ts)) == len(_BOUNDARY_TS)


def test_delta_reader_matches_single_call_load_over_full_range(tmp_path: Path) -> None:
    instrument = _instrument()
    deltas = [
        OrderBookDeltas(instrument.id, [_delta(instrument, sequence=i, ts=ts)])
        for i, ts in enumerate(range(0, 3000, 137))
    ]
    _write_catalog(tmp_path / "catalog", instrument, deltas)

    catalog = ParquetDataCatalog(str(tmp_path / "catalog"))
    single_call = list(
        catalog.order_book_deltas(instrument_ids=[str(instrument.id)], batched=True, start=_START_NS, end=_END_NS) or []
    )
    bounded = list(
        iter_order_book_deltas_bounded(
            tmp_path / "catalog", str(instrument.id), _START_NS, _END_NS
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

    assert _flat_ts(bounded) == _flat_ts(single_call)
