"""
converter.trades — Convert raw trade JSONL records → Nautilus TradeTick.
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity

from converter.readers import stream_raw_records

logger = logging.getLogger(__name__)


def convert_trades(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[TradeTick], int, Optional[int], Optional[int]]:
    """Stream-convert raw trade records to Nautilus TradeTick.

    Returns ``(tick_list, bad_line_count, first_ts_ns, last_ts_ns)``.
    """
    ticks: List[TradeTick] = []
    bad = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    for rec in stream_raw_records(venue, symbol, "trade", date_str):
        try:
            payload = rec.get("payload", {})
            price_str = payload.get("price", "0")
            qty_str = payload.get("quantity", "0")
            side_raw = payload.get("side", "buy")
            trade_id_raw = payload.get("trade_id")

            ts_event_ms = rec.get("ts_event_ms")
            ts_recv_ns = rec.get("ts_recv_ns", 0)
            ts_event = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            ts_init = ts_recv_ns

            aggressor = (
                AggressorSide.BUYER if side_raw == "buy" else AggressorSide.SELLER
            )
            tid = TradeId(str(trade_id_raw)) if trade_id_raw else TradeId("0")

            tick = TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(price_str),
                size=Quantity.from_str(qty_str),
                aggressor_side=aggressor,
                trade_id=tid,
                ts_event=ts_event,
                ts_init=ts_init,
            )
            ticks.append(tick)

            if first_ts is None:
                first_ts = ts_event
            last_ts = ts_event
        except Exception:
            bad += 1

    return ticks, bad, first_ts, last_ts
