"""
converter.trades — Convert raw ``trade_v2`` JSONL records → Nautilus TradeTick.

Records are sorted by ``(trade_stream_session_id, trade_session_seq)`` so
replay determinism depends only on committed canonical order, never on file
timestamp coincidence.

Exchange trade IDs are preserved as diagnostic ``TradeId`` metadata but do
not affect ordering.
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


def _trade_sort_key(rec: dict) -> Tuple[int, int]:
    """Sort by committed canonical trade order."""
    return (
        int(rec.get("trade_stream_session_id", 0)),
        int(rec.get("trade_session_seq", 0)),
    )


def convert_trades(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[TradeTick], int, Optional[int], Optional[int]]:
    """Stream-convert raw trade_v2 records to Nautilus TradeTick.

    Returns ``(tick_list, bad_line_count, first_ts_ns, last_ts_ns)``.
    """
    ticks: List[TradeTick] = []
    bad = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    # Collect then sort by committed session ordering
    raw_records = list(stream_raw_records(venue, symbol, "trade_v2", date_str))
    raw_records.sort(key=_trade_sort_key)

    for rec in raw_records:
        try:
            record_type = rec.get("record_type", "trade")
            # Skip lifecycle markers — they are metadata, not trade ticks
            if record_type != "trade":
                continue

            price_str = rec.get("price", "0")
            qty_str = rec.get("quantity", "0")
            is_buyer_maker = rec.get("is_buyer_maker", False)
            exchange_trade_id = rec.get("exchange_trade_id")

            ts_event_ms = rec.get("ts_event_ms")
            ts_trade_ms = rec.get("ts_trade_ms")
            ts_recv_ns = rec.get("ts_recv_ns", 0)

            # Prefer trade time, then event time, then recv time
            if ts_trade_ms:
                ts_event = int(ts_trade_ms) * 1_000_000
            elif ts_event_ms:
                ts_event = int(ts_event_ms) * 1_000_000
            else:
                ts_event = int(ts_recv_ns)
            ts_init = int(ts_recv_ns)

            # is_buyer_maker=True means the buyer was the maker,
            # so the taker (aggressor) is the seller
            aggressor = (
                AggressorSide.SELLER if is_buyer_maker else AggressorSide.BUYER
            )
            tid = TradeId(str(exchange_trade_id)) if exchange_trade_id else TradeId("0")

            tick = TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(str(price_str)),
                size=Quantity.from_str(str(qty_str)),
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
