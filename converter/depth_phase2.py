"""
Phase 2 deterministic Binance-native depth replay.

Reads ``depth_v2`` raw records, enforces snapshot/bootstrap and continuity
rules, emits primary ``OrderBookDeltas`` data, and optionally derives
``OrderBookDepth10`` from the same replayed book state.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from nautilus_trader.model.data import (
    BookOrder,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth10,
)
from nautilus_trader.model.enums import BookAction, OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

from config import PHASE2_DEPTH10_INTERVAL_SEC
from converter.readers import stream_raw_records

logger = logging.getLogger(__name__)

F_LAST = 1 << 7
F_SNAPSHOT = 1 << 5


@dataclass
class Phase2ReplayMetrics:
    bad_lines: int = 0
    snapshot_seed_count: int = 0
    resync_count: int = 0
    desync_events: int = 0
    delta_events_written: int = 0
    depth10_written: int = 0
    first_ts_ns: Optional[int] = None
    last_ts_ns: Optional[int] = None
    fenced_ranges: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReplayState:
    instrument_id: InstrumentId
    venue: str
    symbol: str
    price_prec: int
    size_prec: int
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    current_stream_session_id: Optional[int] = None
    sync_state: str = "unsynced"
    last_snapshot_update_id: Optional[int] = None
    prev_update_id: Optional[int] = None
    fence_open: Optional[Dict[str, Any]] = None
    last_depth10_emit_ns: Optional[int] = None

    def reset_book(self) -> None:
        self.bids.clear()
        self.asks.clear()


def _ts_event_ns(rec: dict) -> int:
    ts_event_ms = rec.get("ts_event_ms") or rec.get("exchange_ts_ms")
    ts_recv_ns = int(rec.get("ts_recv_ns", 0))
    return int(ts_event_ms) * 1_000_000 if ts_event_ms else ts_recv_ns


def _sort_key(raw_index: int, rec: dict) -> Tuple[int, int, int, int, int]:
    priority = {
        "stream_lifecycle": 0,
        "sync_state": 1,
        "snapshot_seed": 2,
        "depth_update": 3,
    }.get(rec.get("record_type", "depth_update"), 9)
    return (
        int(rec.get("stream_session_id", 0)),
        int(rec.get("connection_seq", 0)),
        priority,
        int(rec.get("ts_recv_ns", 0)),
        int(rec.get("file_position", raw_index)),
    )


def _make_order(
    *,
    side: OrderSide,
    price_str: str,
    size_str: str,
) -> BookOrder:
    return BookOrder(
        side=side,
        price=Price.from_str(price_str),
        size=Quantity.from_str(size_str),
        order_id=0,
    )


def _apply_levels(book: Dict[float, float], levels: Iterable[List[str]]) -> None:
    for price_s, size_s in levels:
        price = float(price_s)
        size = float(size_s)
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size


def _snapshot_to_book(state: ReplayState, payload: dict) -> None:
    state.reset_book()
    _apply_levels(state.bids, payload.get("bids", []))
    _apply_levels(state.asks, payload.get("asks", []))


def _snapshot_deltas(
    state: ReplayState,
    payload: dict,
    *,
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> Optional[OrderBookDeltas]:
    deltas: List[OrderBookDelta] = [
        OrderBookDelta.clear(state.instrument_id, sequence, ts_event, ts_init),
    ]
    snapshot_levels: List[Tuple[OrderSide, List[str]]] = []
    for level in payload.get("bids", []):
        snapshot_levels.append((OrderSide.BUY, level))
    for level in payload.get("asks", []):
        snapshot_levels.append((OrderSide.SELL, level))
    if not snapshot_levels:
        return None
    last_index = len(snapshot_levels) - 1
    for idx, (side, level) in enumerate(snapshot_levels):
        price_s, size_s = level
        flags = F_SNAPSHOT | (F_LAST if idx == last_index else 0)
        deltas.append(
            OrderBookDelta(
                state.instrument_id,
                BookAction.UPDATE if float(size_s) > 0 else BookAction.DELETE,
                _make_order(side=side, price_str=price_s, size_str=size_s),
                flags=flags,
                sequence=sequence,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
    return OrderBookDeltas(state.instrument_id, deltas)


def _live_deltas(
    state: ReplayState,
    payload: dict,
    *,
    sequence: int,
    ts_event: int,
    ts_init: int,
) -> Optional[OrderBookDeltas]:
    items: List[OrderBookDelta] = []
    raw_levels: List[Tuple[OrderSide, List[str]]] = []
    for level in payload.get("bids", []):
        raw_levels.append((OrderSide.BUY, level))
    for level in payload.get("asks", []):
        raw_levels.append((OrderSide.SELL, level))
    if not raw_levels:
        return None
    last_index = len(raw_levels) - 1
    for idx, (side, level) in enumerate(raw_levels):
        price_s, size_s = level
        flags = F_LAST if idx == last_index else 0
        items.append(
            OrderBookDelta(
                state.instrument_id,
                BookAction.UPDATE if float(size_s) > 0 else BookAction.DELETE,
                _make_order(side=side, price_str=price_s, size_str=size_s),
                flags=flags,
                sequence=sequence,
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
    return OrderBookDeltas(state.instrument_id, items)


def _depth10_from_state(state: ReplayState, *, ts_event: int, ts_init: int) -> Optional[OrderBookDepth10]:
    if not state.bids or not state.asks:
        return None

    bid_levels = sorted(state.bids.items(), key=lambda kv: -kv[0])[:10]
    ask_levels = sorted(state.asks.items(), key=lambda kv: kv[0])[:10]

    def _orders(side: OrderSide, levels: List[Tuple[float, float]]) -> List[BookOrder]:
        out: List[BookOrder] = []
        for price, size in levels:
            out.append(
                BookOrder(
                    side=side,
                    price=Price.from_str(f"{price:.{state.price_prec}f}"),
                    size=Quantity.from_str(f"{size:.{state.size_prec}f}"),
                    order_id=0,
                )
            )
        while len(out) < 10:
            out.append(
                BookOrder(
                    side=side,
                    price=Price.from_str("0"),
                    size=Quantity.from_str("0"),
                    order_id=0,
                )
            )
        return out

    return OrderBookDepth10(
        instrument_id=state.instrument_id,
        bids=_orders(OrderSide.BUY, bid_levels),
        asks=_orders(OrderSide.SELL, ask_levels),
        bid_counts=[1] * 10,
        ask_counts=[1] * 10,
        flags=0,
        sequence=state.prev_update_id or 0,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def _open_fence(
    state: ReplayState,
    metrics: Phase2ReplayMetrics,
    *,
    reason: str,
    rec: dict,
) -> None:
    if state.fence_open is not None:
        return
    state.fence_open = {
        "venue": state.venue,
        "symbol": state.symbol,
        "stream_session_id": rec.get("stream_session_id"),
        "start_ts_ns": _ts_event_ns(rec),
        "end_ts_ns": None,
        "reason": reason,
        "triggering_ids": {
            "U": rec.get("U"),
            "u": rec.get("u"),
            "pu": rec.get("pu"),
            "last_update_id": state.prev_update_id,
        },
        "recovered": False,
    }


def _close_fence(
    state: ReplayState,
    metrics: Phase2ReplayMetrics,
    *,
    rec: dict,
    recovered: bool,
) -> None:
    if state.fence_open is None:
        return
    state.fence_open["end_ts_ns"] = _ts_event_ns(rec)
    state.fence_open["recovered"] = recovered
    metrics.fenced_ranges.append(state.fence_open)
    state.fence_open = None


def _should_accept_update(state: ReplayState, rec: dict) -> bool:
    U = rec.get("U")
    u = rec.get("u")
    pu = rec.get("pu")
    prev = state.prev_update_id
    if U is None or u is None or prev is None:
        return False
    if state.venue == "BINANCE_USDTF":
        return pu == prev and u > prev
    return U <= (prev + 1) <= u


def convert_depth_v2(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    emit_depth10: bool = False,
    depth10_interval_sec: float = PHASE2_DEPTH10_INTERVAL_SEC,
) -> Tuple[List[OrderBookDeltas], List[OrderBookDepth10], Phase2ReplayMetrics]:
    records = list(stream_raw_records(venue, symbol, "depth_v2", date_str))
    metrics = Phase2ReplayMetrics()
    deltas_out: List[OrderBookDeltas] = []
    depth10_out: List[OrderBookDepth10] = []
    state = ReplayState(
        instrument_id=instrument_id,
        venue=venue,
        symbol=symbol,
        price_prec=price_prec,
        size_prec=size_prec,
    )
    interval_ns = int(depth10_interval_sec * 1e9)

    ordered = sorted(
        enumerate(records),
        key=lambda item: _sort_key(item[0], item[1]),
    )
    for raw_index, rec in ordered:
        try:
            record_type = rec.get("record_type", "depth_update")
            ts_event = _ts_event_ns(rec)
            ts_init = int(rec.get("ts_recv_ns", ts_event))

            if metrics.first_ts_ns is None:
                metrics.first_ts_ns = ts_event
            metrics.last_ts_ns = ts_event

            session_id = rec.get("stream_session_id")
            if state.current_stream_session_id != session_id:
                if state.fence_open is not None:
                    _close_fence(state, metrics, rec=rec, recovered=False)
                state.current_stream_session_id = session_id
                state.sync_state = "unsynced"
                state.last_snapshot_update_id = None
                state.prev_update_id = None
                state.reset_book()

            if record_type == "stream_lifecycle":
                continue

            if record_type == "sync_state":
                state.sync_state = rec.get("state", state.sync_state)
                if state.sync_state == "snapshot_seeded":
                    _close_fence(state, metrics, rec=rec, recovered=True)
                elif state.sync_state == "resync_required":
                    metrics.resync_count += 1
                    _open_fence(state, metrics, reason=rec.get("reason", "resync_required"), rec=rec)
                elif state.sync_state == "desynced":
                    metrics.desync_events += 1
                    _open_fence(state, metrics, reason=rec.get("reason", "desynced"), rec=rec)
                elif state.sync_state == "fenced":
                    _open_fence(state, metrics, reason=rec.get("reason", "fenced"), rec=rec)
                continue

            if record_type == "snapshot_seed":
                payload = rec.get("payload", {})
                _snapshot_to_book(state, payload)
                state.last_snapshot_update_id = rec.get("lastUpdateId")
                state.prev_update_id = rec.get("lastUpdateId")
                state.sync_state = "snapshot_seeded"
                metrics.snapshot_seed_count += 1
                _close_fence(state, metrics, rec=rec, recovered=True)

                snapshot = _snapshot_deltas(
                    state,
                    payload,
                    sequence=int(rec.get("lastUpdateId") or 0),
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
                if snapshot is not None:
                    deltas_out.append(snapshot)
                    metrics.delta_events_written += 1
                if emit_depth10:
                    depth = _depth10_from_state(state, ts_event=ts_event, ts_init=ts_init)
                    if depth is not None:
                        depth10_out.append(depth)
                        state.last_depth10_emit_ns = ts_event
                        metrics.depth10_written += 1
                continue

            if record_type != "depth_update":
                continue

            if state.prev_update_id is None:
                _open_fence(state, metrics, reason="no_snapshot_seed", rec=rec)
                continue

            if not _should_accept_update(state, rec):
                state.sync_state = "desynced"
                metrics.desync_events += 1
                _open_fence(state, metrics, reason="continuity_break", rec=rec)
                continue

            _apply_levels(state.bids, rec.get("payload", {}).get("bids", []))
            _apply_levels(state.asks, rec.get("payload", {}).get("asks", []))
            state.prev_update_id = rec.get("u")
            state.sync_state = "live_synced"

            event = _live_deltas(
                state,
                rec.get("payload", {}),
                sequence=int(rec.get("u") or 0),
                ts_event=ts_event,
                ts_init=ts_init,
            )
            if event is not None:
                deltas_out.append(event)
                metrics.delta_events_written += 1

            _close_fence(state, metrics, rec=rec, recovered=True)

            if emit_depth10:
                should_emit = (
                    state.last_depth10_emit_ns is None
                    or interval_ns <= 0
                    or (ts_event - state.last_depth10_emit_ns) >= interval_ns
                )
                if should_emit:
                    depth = _depth10_from_state(state, ts_event=ts_event, ts_init=ts_init)
                    if depth is not None:
                        depth10_out.append(depth)
                        state.last_depth10_emit_ns = ts_event
                        metrics.depth10_written += 1
        except Exception:
            logger.exception("Phase 2 replay error for %s/%s", venue, symbol)
            metrics.bad_lines += 1

    if state.fence_open is not None:
        state.fence_open["end_ts_ns"] = metrics.last_ts_ns
        state.fence_open["recovered"] = False
        metrics.fenced_ranges.append(state.fence_open)
        state.fence_open = None

    return deltas_out, depth10_out, metrics
