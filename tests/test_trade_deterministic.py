"""Tests for deterministic Binance-native trade conversion.

All fixtures use trade_stream_session_id + trade_session_seq as the canonical
ordering.  Exchange trade IDs are diagnostic only and never affect replay order.
"""
from __future__ import annotations

from typing import Iterable

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.enums import AggressorSide

import converter.trades as trade_mod


# ── record builders ───────────────────────────────────────────────────

def _spot_trade(
    *,
    session: int,
    seq: int,
    ts_recv_ns: int,
    ts_event_ms: int | None = None,
    ts_trade_ms: int | None = None,
    price: str = "100.0",
    quantity: str = "1.0",
    is_buyer_maker: bool = False,
    exchange_trade_id: int | None = None,
    best_match_flag: bool | None = None,
) -> dict:
    rec = {
        "schema_version": 2,
        "record_type": "trade",
        "venue": "BINANCE_SPOT",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "channel": "trade_v2",
        "trade_stream_session_id": session,
        "trade_session_seq": seq,
        "ts_recv_ns": ts_recv_ns,
        "ts_event_ms": ts_event_ms,
        "ts_trade_ms": ts_trade_ms,
        "price": price,
        "quantity": quantity,
        "is_buyer_maker": is_buyer_maker,
        "exchange_trade_id": exchange_trade_id,
        "best_match_flag": best_match_flag,
        "native_payload": {},
    }
    return rec


def _futures_trade(
    *,
    session: int,
    seq: int,
    ts_recv_ns: int,
    ts_event_ms: int | None = None,
    ts_trade_ms: int | None = None,
    price: str = "100.0",
    quantity: str = "1.0",
    is_buyer_maker: bool = False,
    exchange_trade_id: int | None = None,
    first_trade_id: int | None = None,
    last_trade_id: int | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "trade",
        "venue": "BINANCE_USDTF",
        "market_type": "futures",
        "symbol": "BTCUSDT",
        "channel": "trade_v2",
        "trade_stream_session_id": session,
        "trade_session_seq": seq,
        "ts_recv_ns": ts_recv_ns,
        "ts_event_ms": ts_event_ms,
        "ts_trade_ms": ts_trade_ms,
        "price": price,
        "quantity": quantity,
        "is_buyer_maker": is_buyer_maker,
        "exchange_trade_id": exchange_trade_id,
        "first_trade_id": first_trade_id,
        "last_trade_id": last_trade_id,
        "native_payload": {},
    }


def _lifecycle(*, session: int, event: str, ts_recv_ns: int) -> dict:
    return {
        "schema_version": 2,
        "record_type": "trade_stream_lifecycle",
        "venue": "BINANCE_SPOT",
        "market_type": "spot",
        "symbol": "BTCUSDT",
        "channel": "trade_v2",
        "trade_stream_session_id": session,
        "ts_recv_ns": ts_recv_ns,
        "event": event,
        "reason": "test",
    }


# ── tests ─────────────────────────────────────────────────────────────

def test_canonical_ordering_by_session_seq() -> None:
    """Trade replay must order by (session_id, session_seq), not by timestamps."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = trade_mod.stream_raw_records

    try:
        def _fake(*a, **kw) -> Iterable[dict]:
            # Deliberately out of session_seq order
            yield _spot_trade(session=1, seq=3, ts_recv_ns=3_000_000_000, price="102.0", quantity="0.5")
            yield _spot_trade(session=1, seq=1, ts_recv_ns=1_000_000_000, price="100.0", quantity="1.0")
            yield _spot_trade(session=1, seq=2, ts_recv_ns=2_000_000_000, price="101.0", quantity="0.8")

        trade_mod.stream_raw_records = _fake
        ticks, bad, first, last = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid, 2, 3,
        )
    finally:
        trade_mod.stream_raw_records = original

    assert len(ticks) == 3
    assert bad == 0
    prices = [str(t.price) for t in ticks]
    assert prices == ["100.0", "101.0", "102.0"]


def test_is_buyer_maker_maps_to_aggressor_correctly() -> None:
    """is_buyer_maker=True → seller is aggressor; False → buyer is aggressor."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = trade_mod.stream_raw_records

    try:
        def _fake(*a, **kw):
            yield _spot_trade(session=1, seq=1, ts_recv_ns=1_000, is_buyer_maker=True, price="50.0")
            yield _spot_trade(session=1, seq=2, ts_recv_ns=2_000, is_buyer_maker=False, price="51.0")

        trade_mod.stream_raw_records = _fake
        ticks, _, _, _ = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    assert ticks[0].aggressor_side == AggressorSide.SELLER
    assert ticks[1].aggressor_side == AggressorSide.BUYER


def test_lifecycle_markers_are_skipped() -> None:
    """Lifecycle records should not produce TradeTick output."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = trade_mod.stream_raw_records

    try:
        def _fake(*a, **kw):
            yield _lifecycle(session=1, event="session_start", ts_recv_ns=1_000)
            yield _spot_trade(session=1, seq=1, ts_recv_ns=2_000, price="50.0")
            yield _lifecycle(session=1, event="session_end", ts_recv_ns=3_000)

        trade_mod.stream_raw_records = _fake
        ticks, bad, _, _ = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    assert len(ticks) == 1
    assert bad == 0


def test_spot_vs_futures_schema_decoding() -> None:
    """Spot and futures produce valid TradeTick with correct IDs."""
    original = trade_mod.stream_raw_records

    # Spot
    try:
        def _fake_spot(*a, **kw):
            yield _spot_trade(
                session=1, seq=1, ts_recv_ns=1_000,
                exchange_trade_id=12345, best_match_flag=True,
            )

        trade_mod.stream_raw_records = _fake_spot
        iid_spot = InstrumentId.from_str("BTCUSDT.BINANCE")
        spot_ticks, _, _, _ = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid_spot, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    assert len(spot_ticks) == 1
    assert str(spot_ticks[0].trade_id) == "12345"

    # Futures
    try:
        def _fake_fut(*a, **kw):
            yield _futures_trade(
                session=1, seq=1, ts_recv_ns=1_000,
                exchange_trade_id=99999, first_trade_id=100, last_trade_id=105,
            )

        trade_mod.stream_raw_records = _fake_fut
        iid_fut = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
        fut_ticks, _, _, _ = trade_mod.convert_trades(
            "BINANCE_USDTF", "BTCUSDT", "2026-04-21", iid_fut, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    assert len(fut_ticks) == 1
    assert str(fut_ticks[0].trade_id) == "99999"


def test_exchange_trade_id_does_not_affect_ordering() -> None:
    """Exchange trade IDs may be non-monotonic; ordering is still by session_seq."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = trade_mod.stream_raw_records

    try:
        def _fake(*a, **kw):
            # exchange_trade_id goes backwards but session_seq is correct
            yield _spot_trade(session=1, seq=1, ts_recv_ns=1_000, exchange_trade_id=500, price="100.0")
            yield _spot_trade(session=1, seq=2, ts_recv_ns=2_000, exchange_trade_id=300, price="101.0")
            yield _spot_trade(session=1, seq=3, ts_recv_ns=3_000, exchange_trade_id=700, price="102.0")

        trade_mod.stream_raw_records = _fake
        ticks, bad, _, _ = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    # Must be in session_seq order, not exchange_trade_id order
    prices = [str(t.price) for t in ticks]
    assert prices == ["100.0", "101.0", "102.0"]


def test_multi_session_ordering() -> None:
    """Trades across sessions are ordered session-first, seq-second."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = trade_mod.stream_raw_records

    try:
        def _fake(*a, **kw):
            yield _spot_trade(session=2, seq=1, ts_recv_ns=5_000, price="200.0")
            yield _spot_trade(session=1, seq=2, ts_recv_ns=2_000, price="101.0")
            yield _spot_trade(session=1, seq=1, ts_recv_ns=1_000, price="100.0")
            yield _spot_trade(session=2, seq=2, ts_recv_ns=6_000, price="201.0")

        trade_mod.stream_raw_records = _fake
        ticks, _, _, _ = trade_mod.convert_trades(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21", iid, 1, 1,
        )
    finally:
        trade_mod.stream_raw_records = original

    prices = [str(t.price) for t in ticks]
    assert prices == ["100.0", "101.0", "200.0", "201.0"]
