"""
converter.book — Approximate L2 order-book reconstruction from delta updates.

Phase 1: best-effort reconstruction from cryptofeed-normalised deltas.
No deterministic Binance U/u/pu replay.

Suspected gaps (long inactivity) are counted, optionally triggering a book reset.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
        self.crossed_book_events: int = 0
        self.crossed_book_examples: List[Dict[str, Any]] = []
        self._last_ts_ns: Optional[int] = None
        self._last_snapshot_context: Optional[Dict[str, Any]] = None

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

    def _sorted_levels(self, side: str, limit: int = 10) -> List[Dict[str, float]]:
        if side == "bids":
            levels = sorted(self.bids.items(), key=lambda x: -x[0])[:limit]
        else:
            levels = sorted(self.asks.items(), key=lambda x: x[0])[:limit]
        return [{"price": price, "size": size} for price, size in levels]

    def _current_book_context(self, limit: int = 10) -> Dict[str, Any]:
        bids = self._sorted_levels("bids", limit=limit)
        asks = self._sorted_levels("asks", limit=limit)
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bids": bids,
            "asks": asks,
        }

    def _delta_context(self, rec: dict, limit: int = 10) -> Dict[str, Any]:
        payload = rec.get("payload", {})
        return {
            "sequence_number": rec.get("sequence_number"),
            "bids": payload.get("bids", [])[:limit],
            "asks": payload.get("asks", [])[:limit],
        }

    def _reset_book(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.book_resets += 1

    def handle_crossed_book(
        self,
        rec: dict,
        *,
        ts_event: int,
        ts_init: int,
        input_index: int,
        log_limit: int = 10,
    ) -> bool:
        """Log and reset when the reconstructed book becomes crossed."""
        context = self._current_book_context(limit=log_limit)
        best_bid = context["best_bid"]
        best_ask = context["best_ask"]
        if best_bid is None or best_ask is None or best_bid < best_ask:
            return False

        self.crossed_book_events += 1
        event = {
            "event": "crossed_book",
            "instrument_id": str(self.instrument_id),
            "ts_event": ts_event,
            "ts_init": ts_init,
            "input_index": input_index,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "previous_snapshot": self._last_snapshot_context,
            "current_delta": self._delta_context(rec, limit=log_limit),
            "current_book": context,
        }
        if len(self.crossed_book_examples) < 10:
            self.crossed_book_examples.append(event)
        if self.crossed_book_events <= log_limit:
            logger.warning("crossed_book %s", json.dumps(event, sort_keys=True, default=str))
        elif self.crossed_book_events == log_limit + 1:
            logger.warning(
                "crossed_book further events suppressed for %s after %s detailed logs",
                self.instrument_id,
                log_limit,
            )
        self._reset_book()
        return True

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

        snapshot = OrderBookDepth10(
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
        self._last_snapshot_context = {
            "ts_event": ts_event,
            "ts_init": ts_init,
            **self._current_book_context(),
        }
        return snapshot


# ── top-level conversion function ───────────────────────────────────

DEPTH_SNAPSHOT_INTERVAL_SEC: float = 1.0


@dataclass
class BookBuilderPolicy:
    on_crossed_book: str = "soft_reset"
    snapshot_emit_mode: str = "interval"
    snapshot_interval_sec: float = DEPTH_SNAPSHOT_INTERVAL_SEC


@dataclass
class BookBuilderMetrics:
    depth_events_in: int = 0
    depth_snapshots_out: int = 0
    crossed_book_drops: int = 0
    book_resets: int = 0
    out_of_order_events: int = 0
    gaps_suspected: int = 0
    crossed_book_examples: List[Dict[str, Any]] = field(default_factory=list)


class BookBuilder(BookReconstructor):
    """Compatibility wrapper used by tests and older callers."""

    def __init__(
        self,
        instrument_id: InstrumentId,
        price_prec: int,
        size_prec: int,
        *,
        venue: str,
        symbol: str,
        policy: Optional[BookBuilderPolicy] = None,
    ):
        super().__init__(instrument_id, price_prec, size_prec)
        self.venue = venue
        self.symbol = symbol
        self.policy = policy or BookBuilderPolicy()
        self.metrics = BookBuilderMetrics()
        self._last_emit_ns: Optional[int] = None
        self._last_ts_event: Optional[int] = None

    def process_event(
        self,
        rec: dict,
        *,
        ts_event: int,
        ts_init: int,
        input_index: int,
    ) -> Optional[OrderBookDepth10]:
        self.metrics.depth_events_in += 1
        if self._last_ts_event is not None and ts_event < self._last_ts_event:
            self.metrics.out_of_order_events += 1
        self._last_ts_event = ts_event

        gaps_before = self.gaps_suspected
        resets_before = self.book_resets
        self.apply_delta(rec, ts_init)
        self.metrics.gaps_suspected += self.gaps_suspected - gaps_before
        self.metrics.book_resets += self.book_resets - resets_before

        context = self._current_book_context()
        best_bid = context["best_bid"]
        best_ask = context["best_ask"]
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            event = {
                "event": "crossed_book",
                "instrument_id": str(self.instrument_id),
                "venue": self.venue,
                "symbol": self.symbol,
                "ts_event": ts_event,
                "ts_init": ts_init,
                "input_index": input_index,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "action": self.policy.on_crossed_book,
                "previous_snapshot": self._last_snapshot_context,
                "current_delta": self._delta_context(rec),
                "current_book": context,
            }
            self.crossed_book_events += 1
            if len(self.crossed_book_examples) < 10:
                self.crossed_book_examples.append(event)
            if len(self.metrics.crossed_book_examples) < 10:
                self.metrics.crossed_book_examples.append(event)
            self.metrics.crossed_book_drops += 1

            if self.policy.on_crossed_book == "soft_reset":
                self._reset_book()
                self.metrics.book_resets += 1
            return None

        should_emit = self.policy.snapshot_emit_mode == "every_delta"
        if not should_emit:
            interval_ns = int(self.policy.snapshot_interval_sec * 1e9)
            if self._last_emit_ns is None or (ts_init - self._last_emit_ns) >= interval_ns:
                should_emit = True

        if not should_emit:
            return None

        snapshot = self.snapshot(ts_event, ts_init)
        if snapshot is not None:
            self._last_emit_ns = ts_init
            self.metrics.depth_snapshots_out += 1
        return snapshot


def _record_ts_init_ns(rec: dict) -> int:
    """Return the converter replay timestamp for a raw depth record."""
    ts_init = rec.get("ts_recv_ns")
    if ts_init:
        return int(ts_init)
    ts_event_ms = rec.get("ts_event_ms")
    return int(ts_event_ms * 1_000_000) if ts_event_ms else 0


def _sorted_depth_records(records: Iterable[dict]) -> List[Tuple[int, dict]]:
    """Return records sorted stably by ts_init/ts_recv_ns.

    The input order is preserved when multiple records share the same ts_init.
    """
    buffered: List[Tuple[int, int, dict]] = []
    for input_index, rec in enumerate(records):
        ts_init = _record_ts_init_ns(rec)
        buffered.append((ts_init, input_index, rec))
    buffered.sort(key=lambda item: (item[0], item[1]))
    return [(input_index, rec) for _, input_index, rec in buffered]


def convert_depth(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    snapshot_interval_sec: float = DEPTH_SNAPSHOT_INTERVAL_SEC,
) -> Tuple[
    List[OrderBookDepth10],
    int,
    int,
    int,
    int,
    List[Dict[str, Any]],
    Optional[int],
    Optional[int],
]:
    """Stream-convert raw depth deltas → 1-second OrderBookDepth10 snapshots.

    Returns ``(snapshot_list, bad_line_count, gaps_suspected, book_resets,
               crossed_book_events, crossed_book_examples, first_ts_ns, last_ts_ns)``.
    """
    book = BookReconstructor(instrument_id, price_prec, size_prec)
    snapshots: List[OrderBookDepth10] = []
    bad = 0
    interval_ns = int(snapshot_interval_sec * 1e9)
    last_emit_ns: Optional[int] = None
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    ordered_records = _sorted_depth_records(
        stream_raw_records(venue, symbol, "depth", date_str)
    )

    for input_index, rec in ordered_records:
        try:
            ts_event_ms = rec.get("ts_event_ms")
            ts_recv_ns = _record_ts_init_ns(rec)
            ts_event = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            ts_init = ts_recv_ns

            if ts_event == 0:
                continue

            book.apply_delta(rec, ts_init)
            if book.handle_crossed_book(
                rec,
                ts_event=ts_event,
                ts_init=ts_init,
                input_index=input_index,
            ):
                if first_ts is None:
                    first_ts = ts_event
                last_ts = ts_event
                continue

            if last_emit_ns is None or (ts_init - last_emit_ns) >= interval_ns:
                snap = book.snapshot(ts_event, ts_init)
                if snap is not None:
                    snapshots.append(snap)
                    last_emit_ns = ts_init

            if first_ts is None:
                first_ts = ts_event
            last_ts = ts_event
        except Exception:
            bad += 1

    return (
        snapshots,
        bad,
        book.gaps_suspected,
        book.book_resets,
        book.crossed_book_events,
        book.crossed_book_examples,
        first_ts,
        last_ts,
    )


def convert_depth_records(
    records: Iterable[dict],
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    venue: str,
    symbol: str,
    policy: Optional[BookBuilderPolicy] = None,
    include_metrics: bool = False,
):
    """Compatibility helper for converting in-memory depth records."""
    builder = BookBuilder(
        instrument_id,
        price_prec,
        size_prec,
        venue=venue,
        symbol=symbol,
        policy=policy,
    )
    snapshots: List[OrderBookDepth10] = []
    bad = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    for input_index, rec in _sorted_depth_records(records):
        try:
            ts_init = _record_ts_init_ns(rec)
            ts_event_ms = rec.get("ts_event_ms")
            ts_event = int(ts_event_ms * 1_000_000) if ts_event_ms else ts_init
            if ts_event == 0:
                continue

            snapshot = builder.process_event(
                rec,
                ts_event=ts_event,
                ts_init=ts_init,
                input_index=input_index,
            )
            if snapshot is not None:
                snapshots.append(snapshot)

            if first_ts is None:
                first_ts = ts_event
            last_ts = ts_event
        except Exception:
            bad += 1

    result = (
        snapshots,
        bad,
        builder.metrics.gaps_suspected,
        builder.metrics.book_resets,
        builder.metrics.crossed_book_drops,
        builder.metrics.crossed_book_examples,
        first_ts,
        last_ts,
    )
    if include_metrics:
        return result + (builder.metrics.__dict__.copy(),)
    return result
