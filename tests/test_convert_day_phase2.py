"""
test_convert_day_phase2.py — Deterministic native convert_date tests.

Validates that convert_date produces the correct report shape and catalog
output for the deterministic native architecture (no Phase 1 / mode flag).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
from nautilus_trader.model.enums import BookAction, OrderSide
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import convert_day as convert_day_mod
from converter.depth_phase2 import Phase2ReplayMetrics


def _order(side: OrderSide, price: str, size: str) -> BookOrder:
    return BookOrder(
        side=side,
        price=Price.from_str(price),
        size=Quantity.from_str(size),
        order_id=0,
    )


def _snapshot_deltas(instrument) -> OrderBookDeltas:
    ts_event = 1_000_000_000
    ts_init = 1_000_000_100
    deltas = [
        OrderBookDelta.clear(instrument.id, 100, ts_event, ts_init),
        OrderBookDelta(
            instrument.id,
            BookAction.UPDATE,
            _order(OrderSide.BUY, "100.0", "1.0"),
            flags=32,
            sequence=100,
            ts_event=ts_event,
            ts_init=ts_init,
        ),
        OrderBookDelta(
            instrument.id,
            BookAction.UPDATE,
            _order(OrderSide.SELL, "101.0", "2.0"),
            flags=32 | 128,
            sequence=100,
            ts_event=ts_event,
            ts_init=ts_init,
        ),
    ]
    return OrderBookDeltas(instrument.id, deltas)


def _live_deltas(instrument) -> OrderBookDeltas:
    ts_event = 2_000_000_000
    ts_init = 2_000_000_100
    deltas = [
        OrderBookDelta(
            instrument.id,
            BookAction.UPDATE,
            _order(OrderSide.BUY, "100.0", "1.5"),
            flags=128,
            sequence=101,
            ts_event=ts_event,
            ts_init=ts_init,
        ),
    ]
    return OrderBookDeltas(instrument.id, deltas)


def test_convert_date_writes_order_book_deltas_without_depth10(monkeypatch, tmp_path: Path) -> None:
    """convert_date emits OrderBookDeltas and no Depth10 by default."""
    instrument = TestInstrumentProvider.btcusdt_binance()

    monkeypatch.setattr(
        convert_day_mod,
        "resolve_universe",
        lambda date_str: {"BINANCE_SPOT": ["BTCUSDT"]},
    )
    monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda venue, date_str: {})
    monkeypatch.setattr(convert_day_mod, "build_instruments", lambda venue, syms, einfo: [instrument])
    monkeypatch.setattr(
        convert_day_mod,
        "convert_trades",
        lambda *args, **kwargs: ([], 0, None, None),
    )
    monkeypatch.setattr(
        convert_day_mod,
        "convert_depth_v2",
        lambda *args, **kwargs: (
            [_snapshot_deltas(instrument), _live_deltas(instrument)],
            [],
            Phase2ReplayMetrics(
                snapshot_seed_count=1,
                delta_events_written=2,
                first_ts_ns=1_000_000_000,
                last_ts_ns=2_000_000_000,
            ),
        ),
    )

    catalog_root = tmp_path / "catalog"
    report = convert_day_mod.convert_date(
        datetime(2026, 4, 21),
        catalog_root=catalog_root,
        emit_depth10=False,
    )

    catalog = ParquetDataCatalog(str(catalog_root))
    deltas = catalog.order_book_deltas(instrument_ids=[instrument.id], batched=True)
    depth10 = catalog.order_book_depth10(instrument_ids=[instrument.id])

    assert report["architecture"] == "deterministic_native"
    assert report["total_order_book_deltas_written"] == 2
    assert report["total_depth10_written"] == 0
    assert report["total_trades_written"] == 0
    assert len(deltas) == 2
    assert depth10 == []
