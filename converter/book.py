"""
converter.book — Approximate L2 order-book reconstruction from delta updates.

Phase 1: best-effort reconstruction from cryptofeed-normalised deltas.
No deterministic Binance U/u/pu replay.

Suspected gaps (long inactivity) are counted, optionally triggering a book reset.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from nautilus_trader.model.data import BookOrder, OrderBookDepth10
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

from converter.readers import stream_raw_records

logger = logging.getLogger(__name__)

# ── padding helpers ──────────────────────────────────────────────────

_ZERO_BID = lambda: BookOrder(
    side=OrderSide.BUY, price=Price.from_str("0"),
    size=Quantity.from_str("0"), order_id=0,
)
_ZERO_ASK = lambda: BookOrder(
    side=OrderSide.SELL, price=Price.from_str("0"),
    size=Quantity.from_str("0"), order_id=0,
)

# Gap detection: if no update for this many seconds, suspect a reconnect gap.
GAP_THRESHOLD_SEC: float = 30.0


# ── BookReconstructor ────────────────────────────────────────────────

class BookReconstructor:
    """In-memory L2 book from delta updates → OrderBookDepth10 snapshots.

    Tracks ``gaps_suspected``: incremented when an inactivity gap exceeding
    ``GAP_THRESHOLD_SEC`` is detected between consecutive depth records.
    On gap detection the in-memory book is reset to empty so that stale
    levels from before the reconnect do not contaminate the new book state.
    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        price_prec: int,
        size_prec: int,
        *,
        gap_threshold_sec: float = GAP_THRESHOLD_SEC,
    ):
        self.instrument_id = instrument_id
        self.price_prec = price_prec
        self.size_prec = size_prec
        self.gap_threshold_ns = int(gap_threshold_sec * 1e9)

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.gaps_suspected: int = 0
        self.book_resets: int = 0
        self._last_ts_ns: Optional[int] = None

    def apply_delta(self, rec: dict, ts_ns: int) -> None:
        """Apply a single delta record, checking for inactivity gaps."""
        # ── gap detection ──
        if self._last_ts_ns is not None:
            gap = ts_ns - self._last_ts_ns
            if gap > self.gap_threshold_ns:
                self.gaps_suspected += 1
                self.book_resets += 1
                logger.debug(
                    f"Gap suspected for {self.instrument_id}: "
                    f"{gap / 1e9:.1f}s since last update (threshold "
                    f"{self.gap_threshold_ns / 1e9:.0f}s) — resetting book"
                )
                self.bids.clear()
                self.asks.clear()
        self._last_ts_ns = ts_ns

        payload = rec.get("payload", {})
        for price_s, size_s in payload.get("bids", []):
            price, size = float(price_s), float(size_s)
            if size == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = size
        for price_s, size_s in payload.get("asks", []):
            price, size = float(price_s), float(size_s)
            if size == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size

    def snapshot(self, ts_event: int, ts_init: int) -> Optional[OrderBookDepth10]:
        """Build an OrderBookDepth10 from current state.

        Returns None if both bids and asks are empty.
        """
        if not self.bids or not self.asks:
            return None

        top_bids = sorted(self.bids.items(), key=lambda x: -x[0])[:10]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:10]

        bid_orders = [
            BookOrder(
                side=OrderSide.BUY,
                price=Price.from_str(f"{p:.{self.price_prec}f}"),
                size=Quantity.from_str(f"{s:.{self.size_prec}f}"),
                order_id=0,
            )
            for p, s in top_bids
        ]
        ask_orders = [
            BookOrder(
                side=OrderSide.SELL,
                price=Price.from_str(f"{p:.{self.price_prec}f}"),
                size=Quantity.from_str(f"{s:.{self.size_prec}f}"),
                order_id=0,
            )
            for p, s in top_asks
        ]

        # Pad to exactly 10
        while len(bid_orders) < 10:
            bid_orders.append(_ZERO_BID())
        while len(ask_orders) < 10:
            ask_orders.append(_ZERO_ASK())

        return OrderBookDepth10(
            instrument_id=self.instrument_id,
            bids=bid_orders,
            asks=ask_orders,
            bid_counts=[1] * 10,
            ask_counts=[1] * 10,
            flags=0,
            sequence=0,
            ts_event=ts_event,
            ts_init=ts_init,
        )


# ── top-level conversion function ───────────────────────────────────

DEPTH_SNAPSHOT_INTERVAL_SEC: float = 1.0


def convert_depth(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    snapshot_interval_sec: float = DEPTH_SNAPSHOT_INTERVAL_SEC,
) -> Tuple[List[OrderBookDepth10], int, int, Optional[int], Optional[int]]:
    """Stream-convert raw depth deltas → 1-second OrderBookDepth10 snapshots.

    Returns ``(snapshot_list, bad_line_count, gaps_suspected,
               book_resets, first_ts_ns, last_ts_ns)``.
    """
    book = BookReconstructor(instrument_id, price_prec, size_prec)
    snapshots: List[OrderBookDepth10] = []
    bad = 0
    interval_ns = int(snapshot_interval_sec * 1e9)
    last_emit_ns: Optional[int] = None
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    for rec in stream_raw_records(venue, symbol, "depth", date_str):
        try:
            ts_event_ms = rec.get("ts_event_ms")
            ts_recv_ns = rec.get("ts_recv_ns", 0)
            ts_event = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            ts_init = ts_recv_ns

            if ts_event == 0:
                continue

            book.apply_delta(rec, ts_event)

            if last_emit_ns is None or (ts_event - last_emit_ns) >= interval_ns:
                snap = book.snapshot(ts_event, ts_init)
                if snap is not None:
                    snapshots.append(snap)
                    last_emit_ns = ts_event

            if first_ts is None:
                first_ts = ts_event
            last_ts = ts_event
        except Exception:
            bad += 1

    return snapshots, bad, book.gaps_suspected, book.book_resets, first_ts, last_ts
