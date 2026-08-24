"""
Deterministic Binance-native depth replay.

Reads ``depth_v2`` raw records, enforces snapshot/bootstrap and continuity
rules, emits primary ``OrderBookDeltas`` data, and optionally derives
``OrderBookDepth10`` from the same replayed book state.

All records are sorted by ``(stream_session_id, session_seq)`` — the
committed canonical ordering from the recorder.  Book state uses exact
``Decimal`` representation throughout.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from nautilus_trader.model.data import (
    BookOrder,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth10,
)
from nautilus_trader.model.enums import BookAction, OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity

from config import (
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
)
from converter.readers import stream_raw_records
from converter.depth_repartition import (
    date_shift as _date_shift,
    dedupe_key as _dedupe_key,
    is_epoch_like_ns as _is_epoch_like_ns,
    target_bounds_ns as _target_bounds_ns,
    ts_event_ns as _ts_event_ns,
)
from converter.spool import DedupeSet, RawRecordSpool

logger = logging.getLogger(__name__)

F_LAST = 1 << 7
F_SNAPSHOT = 1 << 5
DEFAULT_CONVERTER_BATCH_SIZE = 5000


def fence_canonical_key(fence: Dict[str, Any]) -> Tuple[Any, ...]:
    """Canonical identity for one fenced (unrecovered discontinuity) range,
    independent of wall-clock diagnostic metadata (e.g. detection time) or
    display-only annotations (e.g. `classification` added by
    convert_day.py's `_annotated_fence_examples()`). Used both to key
    set-difference comparison (validation.catalog_compare.compare_fenced_ranges_semantic())
    and to compute a stable digest over a complete fence collection
    (canonical_fence_digest() below), so both the reference (convert_day.py)
    and candidate (replay_catalog_reconstruct.py) sides agree on what
    "the same fence" means."""
    return (
        fence.get("venue"),
        fence.get("symbol"),
        fence.get("start_ts_ns", fence.get("start")),
        fence.get("end_ts_ns", fence.get("end")),
        fence.get("severity"),
        fence.get("reason", fence.get("kind")),
    )


def canonical_fence_digest(fences: Iterable[Dict[str, Any]]) -> str:
    """Deterministic SHA-256 hex digest over the complete canonical identity
    of every fence in `fences`, sorted for order-independence.

    Issue #20 Phase 1 correction: convert_day.py's own report JSON only
    persists up to 3 example fences per symbol (a human-readability
    truncation), which cannot by itself prove candidate/reference
    equivalence for symbols with more than 3 fences. This digest is
    computed over the COMPLETE in-memory `Phase2ReplayMetrics.fenced_ranges`
    list (already fully materialized by the existing depth-conversion
    engine for the day being converted — this digest adds no new
    full-day materialization) and is a bounded, constant-size stand-in for
    the full fence list: any difference in count, content, or a single
    additional/missing fence anywhere in the collection (including beyond
    the first 3 examples) changes the digest.
    """
    keys = sorted(
        (fence_canonical_key(fence) for fence in fences),
        key=lambda key: json.dumps(list(key), sort_keys=True, default=str),
    )
    payload = json.dumps([list(key) for key in keys], sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass
class Phase2ReplayMetrics:
    bad_lines: int = 0
    snapshot_seed_count: int = 0
    resync_count: int = 0
    desync_events: int = 0
    delta_events_written: int = 0
    depth10_written: int = 0
    derived_depth_snapshots_written: int = 0
    derived_depth_snapshot_levels: int = 10
    derived_depth_snapshot_type: str = "OrderBookDepth10"
    requested_depth_snapshot_levels: int = 10
    requested_depth_snapshot_levels_applied: int = 10
    first_ts_ns: Optional[int] = None
    last_ts_ns: Optional[int] = None
    fenced_ranges: List[Dict[str, Any]] = field(default_factory=list)
    # Raw record type counts (diagnostic only — do not affect conversion output)
    raw_record_count: int = 0
    depth_update_record_count: int = 0
    sync_state_record_count: int = 0
    stream_lifecycle_record_count: int = 0
    # Bad lines tracking (compact diagnostic)
    bad_lines_by_exception_type: Dict[str, int] = field(default_factory=dict)
    bad_lines_by_record_type: Dict[str, int] = field(default_factory=dict)
    bad_line_examples: List[Dict[str, Any]] = field(default_factory=list)
    carried_seed_from_previous_day: bool = False
    carried_seed_date: Optional[str] = None
    carried_seed_session_id: Optional[int] = None
    carried_seed_last_update_id: Optional[int] = None
    carry_replay_record_count: int = 0
    carry_recovery_failed_reason: Optional[str] = None
    synthetic_opening_snapshot_written: bool = False
    timestamp_repartition_enabled: bool = True
    extra_raw_partitions_scanned: List[str] = field(default_factory=list)
    records_imported_from_previous_folder: int = 0
    records_imported_from_next_folder: int = 0
    records_dropped_outside_target_utc: int = 0
    duplicate_records_suppressed: int = 0


@dataclass
class ReplayState:
    instrument_id: InstrumentId
    venue: str
    symbol: str
    price_prec: int
    size_prec: int
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)
    current_stream_session_id: Optional[int] = None
    sync_state: str = "unsynced"
    last_snapshot_update_id: Optional[int] = None
    prev_update_id: Optional[int] = None
    fence_open: Optional[Dict[str, Any]] = None
    last_depth10_emit_ns: Optional[int] = None

    def reset_book(self) -> None:
        self.bids.clear()
        self.asks.clear()


def _sort_key(raw_index: int, rec: dict) -> Tuple[int, int, int]:
    """Sort by committed canonical order: (session, session_seq, raw_index fallback)."""
    return (
        int(rec.get("stream_session_id", 0)),
        int(rec.get("session_seq", rec.get("connection_seq", 0))),
        raw_index,
    )


def _state_payload(state: ReplayState) -> Dict[str, List[List[str]]]:
    return {
        "bids": [[str(price), str(size)] for price, size in sorted(state.bids.items(), reverse=True)],
        "asks": [[str(price), str(size)] for price, size in sorted(state.asks.items())],
    }


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


def _apply_levels(book: Dict[Decimal, Decimal], levels: Iterable[List[str]]) -> None:
    for price_s, size_s in levels:
        price = Decimal(price_s)
        size = Decimal(size_s)
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

    def _orders(side: OrderSide, levels: List[Tuple[Decimal, Decimal]]) -> List[BookOrder]:
        out: List[BookOrder] = []
        for price, size in levels:
            out.append(
                BookOrder(
                    side=side,
                    price=Price.from_str(str(price)),
                    size=Quantity.from_str(str(size)),
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


def _is_lifecycle_resync_reason(reason: object) -> bool:
    value = str(reason or "").lower()
    return (
        "bootstrap" in value
        or "websocket_closed" in value
        or "shutdown" in value
        or "startup_or_reconnect" in value
        or "new_stream_session" in value
        or "utc_day_rollover" in value
    )


def _should_accept_update(state: ReplayState, rec: dict) -> bool:
    """Check Binance depth continuity.

    For the first event after a snapshot (sync_state == 'snapshot_seeded'),
    use the bootstrap overlap rule.  For subsequent events ('live_synced'),
    use the ongoing continuity rule.  Must match the recorder's
    ``_check_continuity`` exactly.

    During bootstrap, stale messages (u < lastUpdateId for futures,
    u <= lastUpdateId for spot) are silently accepted as "skip" — the
    caller should not treat them as a desync.  In practice the recorder
    never commits stale-during-bootstrap messages, so the converter
    won't encounter them; this guard exists for 1:1 parity.
    """
    U = rec.get("U")
    u = rec.get("u")
    pu = rec.get("pu")
    prev = state.prev_update_id
    if U is None or u is None or prev is None:
        return False
    is_futures = state.venue == "BINANCE_USDTF"
    is_bootstrap = state.sync_state == "snapshot_seeded"

    # Stale drop during bootstrap — silent skip, not a desync
    if is_bootstrap:
        if is_futures:
            if u < prev:
                return False  # stale
        else:
            if u <= prev:
                return False  # stale

    if is_futures:
        if is_bootstrap:
            # Futures bootstrap: U <= lastUpdateId AND u >= lastUpdateId
            return (U <= prev) and (u >= prev)
        # Futures ongoing: pu == prev_u
        return pu == prev
    # Spot: same formula for bootstrap and ongoing
    return U <= (prev + 1) <= u


def _fail_carry(state: ReplayState, metrics: Phase2ReplayMetrics, reason: str) -> bool:
    state.current_stream_session_id = None
    state.sync_state = "unsynced"
    state.last_snapshot_update_id = None
    state.prev_update_id = None
    state.reset_book()
    metrics.carry_recovery_failed_reason = reason
    return False


def _recover_carry_state_from_spool(
    state: ReplayState,
    carry_spool: RawRecordSpool,
    first_update: dict,
    metrics: Phase2ReplayMetrics,
) -> bool:
    session_id = first_update.get("stream_session_id")
    seed = carry_spool.max_record(
        record_type="snapshot_seed",
        session_id=session_id,
        first_tie=True,
    )
    if seed is None:
        return _fail_carry(state, metrics, "no_previous_snapshot_seed")

    seed_key = _sort_key(0, seed)
    has_replay = False
    state.current_stream_session_id = session_id
    state.sync_state = "unsynced"
    state.last_snapshot_update_id = None
    state.prev_update_id = None
    state.reset_book()

    for rec in carry_spool.iter_records(session_id=session_id, min_sort_key=seed_key):
        has_replay = True
        metrics.carry_replay_record_count += 1
        record_type = rec.get("record_type", "depth_update")
        if record_type == "stream_lifecycle":
            continue
        if record_type == "sync_state":
            state.sync_state = rec.get("state", state.sync_state)
            continue
        if record_type == "snapshot_seed":
            _snapshot_to_book(state, rec.get("payload", {}))
            state.last_snapshot_update_id = rec.get("lastUpdateId")
            state.prev_update_id = rec.get("lastUpdateId")
            state.sync_state = "snapshot_seeded"
            continue
        if record_type != "depth_update":
            continue
        if state.prev_update_id is None:
            return _fail_carry(state, metrics, "carry_update_before_snapshot")
        if not _should_accept_update(state, rec):
            return _fail_carry(state, metrics, "carry_continuity_break")
        _apply_levels(state.bids, rec.get("payload", {}).get("bids", []))
        _apply_levels(state.asks, rec.get("payload", {}).get("asks", []))
        state.prev_update_id = rec.get("u")
        state.sync_state = "live_synced"

    if not has_replay:
        return _fail_carry(state, metrics, "empty_carry_replay")
    if state.prev_update_id is None or not state.bids or not state.asks:
        return _fail_carry(state, metrics, "carry_incomplete_book_state")

    metrics.carried_seed_from_previous_day = True
    metrics.carried_seed_date = seed.get("_source_date")
    metrics.carried_seed_session_id = int(session_id) if session_id is not None else None
    metrics.carried_seed_last_update_id = int(seed.get("lastUpdateId") or 0)
    metrics.carry_recovery_failed_reason = None
    return True


def _emit_synthetic_opening_snapshot(
    state: ReplayState,
    first_update: dict,
    metrics: Phase2ReplayMetrics,
    deltas_out: List[OrderBookDeltas],
    depth10_out: List[OrderBookDepth10],
    *,
    emit_depth10: bool,
) -> None:
    payload = _state_payload(state)
    ts_event = _ts_event_ns(first_update)
    ts_init = int(first_update.get("ts_recv_ns", ts_event))
    snapshot = _snapshot_deltas(
        state,
        payload,
        sequence=int(state.prev_update_id or 0),
        ts_event=ts_event,
        ts_init=ts_init,
    )
    if snapshot is not None:
        deltas_out.append(snapshot)
        metrics.delta_events_written += 1
        metrics.synthetic_opening_snapshot_written = True
    if emit_depth10:
        depth = _depth10_from_state(state, ts_event=ts_event, ts_init=ts_init)
        if depth is not None:
            depth10_out.append(depth)
            state.last_depth10_emit_ns = ts_event
            metrics.depth10_written += 1
            metrics.derived_depth_snapshots_written += 1


def _spool_repartitioned_records(
    venue: str,
    symbol: str,
    date_str: str,
    metrics: Phase2ReplayMetrics,
    target_spool: RawRecordSpool,
    carry_spool: RawRecordSpool,
    seen_target: DedupeSet,
) -> None:
    target_start_ns, target_end_ns = _target_bounds_ns(date_str)
    partition_dates = [_date_shift(date_str, -1), date_str, _date_shift(date_str, 1)]
    metrics.extra_raw_partitions_scanned = [partition_dates[0], partition_dates[2]]

    target_index = 0
    carry_index = 0
    for source_date in partition_dates:
        for rec in stream_raw_records(venue, symbol, "depth_v2", source_date):
            item = dict(rec)
            item["_source_date"] = source_date
            item["record_type"] = item.get("record_type", "depth_update")
            ts_ns = _ts_event_ns(item)

            # Existing unit fixtures often use tiny relative timestamps. Keep
            # them path-scoped so historical tests do not masquerade as 1970.
            if not _is_epoch_like_ns(ts_ns):
                if source_date == date_str:
                    key = _dedupe_key(item)
                    if not seen_target.add(key):
                        metrics.duplicate_records_suppressed += 1
                        continue
                    sort_key = _sort_key(target_index, item)
                    target_spool.insert(item, sort_key, target_index)
                    target_index += 1
                continue

            if target_start_ns <= ts_ns < target_end_ns:
                key = _dedupe_key(item)
                if not seen_target.add(key):
                    metrics.duplicate_records_suppressed += 1
                    continue
                sort_key = _sort_key(target_index, item)
                target_spool.insert(item, sort_key, target_index)
                target_index += 1
                if source_date == partition_dates[0]:
                    metrics.records_imported_from_previous_folder += 1
                elif source_date == partition_dates[2]:
                    metrics.records_imported_from_next_folder += 1
            else:
                metrics.records_dropped_outside_target_utc += 1
                if ts_ns < target_start_ns:
                    sort_key = _sort_key(carry_index, item)
                    carry_spool.insert(item, sort_key, carry_index)
                    carry_index += 1

    target_spool.commit()
    carry_spool.commit()
    seen_target.commit()


def convert_depth_v2(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    emit_depth10: bool = False,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
) -> Tuple[List[OrderBookDeltas], List[OrderBookDepth10], Phase2ReplayMetrics]:
    deltas_out: List[OrderBookDeltas] = []
    depth10_out: List[OrderBookDepth10] = []

    def collect_deltas(batch: List[OrderBookDeltas]) -> None:
        deltas_out.extend(batch)

    def collect_depth10(batch: List[OrderBookDepth10]) -> None:
        depth10_out.extend(batch)

    metrics = convert_depth_v2_streaming(
        venue,
        symbol,
        date_str,
        instrument_id,
        price_prec,
        size_prec,
        emit_depth10=emit_depth10,
        depth10_interval_sec=depth10_interval_sec,
        derived_depth_snapshot_levels=derived_depth_snapshot_levels,
        on_deltas_batch=collect_deltas,
        on_depth10_batch=collect_depth10,
        batch_size=DEFAULT_CONVERTER_BATCH_SIZE,
    )
    return deltas_out, depth10_out, metrics


def _init_depth_metrics(
    metrics: Phase2ReplayMetrics,
    derived_depth_snapshot_levels: int,
) -> int:
    """Initialise derived-depth snapshot level metrics. Returns applied levels."""
    requested_depth_snapshot_levels = max(0, int(derived_depth_snapshot_levels))
    applied_depth_snapshot_levels = min(requested_depth_snapshot_levels, 10)
    if applied_depth_snapshot_levels <= 0:
        applied_depth_snapshot_levels = 10
    metrics.requested_depth_snapshot_levels = requested_depth_snapshot_levels
    metrics.requested_depth_snapshot_levels_applied = applied_depth_snapshot_levels
    metrics.derived_depth_snapshot_levels = applied_depth_snapshot_levels
    metrics.derived_depth_snapshot_type = "OrderBookDepth10"
    return applied_depth_snapshot_levels


def _run_depth_replay_loop(
    records: Iterable[dict],
    *,
    venue: str,
    symbol: str,
    state: ReplayState,
    metrics: Phase2ReplayMetrics,
    on_deltas_batch: Callable[[List[OrderBookDeltas]], None],
    on_depth10_batch: Callable[[List[OrderBookDepth10]], None],
    batch_size: int,
    emit_depth10: bool,
    interval_ns: int,
    prime: Optional[Callable[[List[OrderBookDeltas], List[OrderBookDepth10]], None]] = None,
) -> None:
    """Deterministic depth replay over a normalized record iterable.

    This is the single shared engine used by BOTH:
      * the validated raw path (``convert_depth_v2_streaming``), which feeds a
        repartitioned / carry-primed spool, and
      * the replay_store path (``replay_records_to_depth_streaming``), which feeds
        adapter-normalized replay rows.

    The per-record semantics (session reset, sync_state fences, snapshot seeding,
    continuity-gated live deltas, derived depth10 emission) are byte-for-byte
    identical for both callers; only the record source differs. ``prime`` lets the
    raw path inject a synthetic opening snapshot recovered from carry state before
    the main loop runs.
    """
    delta_batch: List[OrderBookDeltas] = []
    depth10_batch: List[OrderBookDepth10] = []

    def flush_deltas() -> None:
        if not delta_batch:
            return
        on_deltas_batch(list(delta_batch))
        delta_batch.clear()

    def flush_depth10() -> None:
        if not depth10_batch:
            return
        on_depth10_batch(list(depth10_batch))
        depth10_batch.clear()

    def maybe_flush() -> None:
        if len(delta_batch) >= batch_size:
            flush_deltas()
        if len(depth10_batch) >= batch_size:
            flush_depth10()

    if prime is not None:
        prime(delta_batch, depth10_batch)
        maybe_flush()

    for rec in records:
        try:
            record_type = rec.get("record_type", "depth_update")
            metrics.raw_record_count += 1
            if record_type == "depth_update":
                metrics.depth_update_record_count += 1
            elif record_type == "sync_state":
                metrics.sync_state_record_count += 1
            elif record_type == "stream_lifecycle":
                metrics.stream_lifecycle_record_count += 1
            ts_event = _ts_event_ns(rec)
            ts_init = int(rec.get("ts_recv_ns", ts_event))

            if metrics.first_ts_ns is None:
                metrics.first_ts_ns = ts_event
            metrics.last_ts_ns = ts_event

            session_id = rec.get("stream_session_id")
            if state.current_stream_session_id != session_id:
                if state.fence_open is not None:
                    state.fence_open["closed_by_session_change"] = True
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
                    reason = rec.get("reason", "resync_required")
                    if not _is_lifecycle_resync_reason(reason):
                        metrics.resync_count += 1
                    _open_fence(state, metrics, reason=reason, rec=rec)
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
                    delta_batch.append(snapshot)
                    metrics.delta_events_written += 1
                if emit_depth10:
                    depth = _depth10_from_state(state, ts_event=ts_event, ts_init=ts_init)
                    if depth is not None:
                        depth10_batch.append(depth)
                        state.last_depth10_emit_ns = ts_event
                        metrics.depth10_written += 1
                        metrics.derived_depth_snapshots_written += 1
                maybe_flush()
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
                delta_batch.append(event)
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
                        depth10_batch.append(depth)
                        state.last_depth10_emit_ns = ts_event
                        metrics.depth10_written += 1
                        metrics.derived_depth_snapshots_written += 1
            maybe_flush()
        except Exception as exc:
            logger.exception("Phase 2 replay error for %s/%s", venue, symbol)
            metrics.bad_lines += 1

            # Capture compact diagnostic info for first 20 bad_lines
            exc_type = type(exc).__name__
            rec_type = rec.get("record_type", "unknown") if 'rec' in locals() else "unknown"

            # Track counts by exception type and record type
            metrics.bad_lines_by_exception_type[exc_type] = metrics.bad_lines_by_exception_type.get(exc_type, 0) + 1
            metrics.bad_lines_by_record_type[rec_type] = metrics.bad_lines_by_record_type.get(rec_type, 0) + 1

            # Keep first 20 examples (compact format)
            if len(metrics.bad_line_examples) < 20 and 'rec' in locals():
                metrics.bad_line_examples.append({
                    "venue": venue,
                    "symbol": symbol,
                    "record_type": rec_type,
                    "stream_session_id": rec.get("stream_session_id"),
                    "session_seq": rec.get("session_seq"),
                    "ts_event_ms": rec.get("ts_event_ms"),
                    "exception_type": exc_type,
                    "exception_message": str(exc)[:100],
                    "record_keys": list(rec.keys()) if isinstance(rec, dict) else [],
                })

    if state.fence_open is not None:
        state.fence_open["end_ts_ns"] = metrics.last_ts_ns
        state.fence_open["closed_at_eof"] = True
        state.fence_open["recovered"] = False
        metrics.fenced_ranges.append(state.fence_open)
        state.fence_open = None

    flush_deltas()
    flush_depth10()


def convert_depth_v2_streaming(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    on_deltas_batch: Callable[[List[OrderBookDeltas]], None],
    on_depth10_batch: Callable[[List[OrderBookDepth10]], None],
    batch_size: int = DEFAULT_CONVERTER_BATCH_SIZE,
    temp_dir: str | None = None,
    emit_depth10: bool = False,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
) -> Phase2ReplayMetrics:
    """Validated raw -> Nautilus depth path (repartition + carry + shared engine)."""
    batch_size = max(1, int(batch_size))
    metrics = Phase2ReplayMetrics()
    _init_depth_metrics(metrics, derived_depth_snapshot_levels)
    state = ReplayState(
        instrument_id=instrument_id,
        venue=venue,
        symbol=symbol,
        price_prec=price_prec,
        size_prec=size_prec,
    )
    interval_ns = int(depth10_interval_sec * 1e9)

    with (
        RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-depth-target-") as target_spool,
        RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-depth-carry-") as carry_spool,
        DedupeSet(temp_dir=temp_dir, prefix="cryptorecorder-depth-dedupe-") as seen_target,
    ):
        _spool_repartitioned_records(
            venue,
            symbol,
            date_str,
            metrics,
            target_spool,
            carry_spool,
            seen_target,
        )

        def _prime(
            delta_batch: List[OrderBookDeltas],
            depth10_batch: List[OrderBookDepth10],
        ) -> None:
            first_update = target_spool.first_record(record_type="depth_update")
            if first_update is not None and not target_spool.has_record_before(
                "snapshot_seed",
                _sort_key(0, first_update),
            ):
                if _recover_carry_state_from_spool(state, carry_spool, first_update, metrics):
                    _emit_synthetic_opening_snapshot(
                        state,
                        first_update,
                        metrics,
                        delta_batch,
                        depth10_batch,
                        emit_depth10=emit_depth10,
                    )

        _run_depth_replay_loop(
            target_spool.iter_records(),
            venue=venue,
            symbol=symbol,
            state=state,
            metrics=metrics,
            on_deltas_batch=on_deltas_batch,
            on_depth10_batch=on_depth10_batch,
            batch_size=batch_size,
            emit_depth10=emit_depth10,
            interval_ns=interval_ns,
            prime=_prime,
        )

    return metrics


def replay_records_to_depth_streaming(
    records: Iterable[dict],
    venue: str,
    symbol: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    on_deltas_batch: Callable[[List[OrderBookDeltas]], None],
    on_depth10_batch: Callable[[List[OrderBookDepth10]], None],
    batch_size: int = DEFAULT_CONVERTER_BATCH_SIZE,
    emit_depth10: bool = False,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
    carry_records: Optional[Iterable[dict]] = None,
    temp_dir: Optional[str] = None,
) -> Phase2ReplayMetrics:
    """Replay adapter-normalized depth records through the shared depth engine.

    Unlike :func:`convert_depth_v2_streaming` this performs NO raw
    repartitioning: the caller supplies the already-ordered normalized
    record stream for exactly one target date (e.g. adapter-mapped
    replay_store rows for that date). Clock-skewed records that a
    different day's raw partition would have repartitioned into this
    target window (or out of it) are NOT reproduced here — see
    ``docs/FULL_L2_REPLAY_CATALOG_PLAN.md`` for that documented caveat.

    ``carry_records``, when supplied, enables the SAME cross-day carry
    recovery ``convert_depth_v2_streaming()`` performs for the raw path
    (reusing its exact ``_recover_carry_state_from_spool()``/
    ``_emit_synthetic_opening_snapshot()`` helpers): if the target
    session's first record has no preceding ``snapshot_seed`` within the
    target date itself (i.e. the session began on a prior day), the most
    recent ``snapshot_seed`` for that session found in ``carry_records``
    is replayed forward (bounded, disk-backed via
    ``converter.spool.RawRecordSpool`` — never a full-day Python list) to
    reconstruct book state/continuity ids, and a synthetic opening
    snapshot is emitted before the target date's own records — exactly
    mirroring the raw path's behavior, so a replay-reconstructed
    fenced-range list matches the reference even when a session spans a
    UTC day boundary. The caller (``validation.replay_catalog_reconstruct``)
    supplies the previous day's REPLAY partition's own depth rows (via
    ``ReplayReader``/the depth adapter) as ``carry_records`` when that
    partition exists — this consumes it transiently to derive carry
    state; it is never copied or persisted into the target partition.
    If ``carry_records`` is omitted (the previous default), behavior is
    unchanged: no carry recovery is attempted.
    """
    batch_size = max(1, int(batch_size))
    metrics = Phase2ReplayMetrics()
    _init_depth_metrics(metrics, derived_depth_snapshot_levels)
    state = ReplayState(
        instrument_id=instrument_id,
        venue=venue,
        symbol=symbol,
        price_prec=price_prec,
        size_prec=size_prec,
    )
    interval_ns = int(depth10_interval_sec * 1e9)

    if carry_records is None:
        _run_depth_replay_loop(
            records,
            venue=venue,
            symbol=symbol,
            state=state,
            metrics=metrics,
            on_deltas_batch=on_deltas_batch,
            on_depth10_batch=on_depth10_batch,
            batch_size=batch_size,
            emit_depth10=emit_depth10,
            interval_ns=interval_ns,
        )
        return metrics

    with (
        RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-replay-carrytarget-") as target_spool,
        RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-replay-carry-") as carry_spool,
    ):
        target_index = 0
        for rec in records:
            sort_key = _sort_key(target_index, rec)
            target_spool.insert(rec, sort_key, target_index)
            target_index += 1
        target_spool.commit()

        carry_index = 0
        for rec in carry_records:
            sort_key = _sort_key(carry_index, rec)
            carry_spool.insert(rec, sort_key, carry_index)
            carry_index += 1
        carry_spool.commit()

        def _prime(
            delta_batch: List[OrderBookDeltas],
            depth10_batch: List[OrderBookDepth10],
        ) -> None:
            first_update = target_spool.first_record(record_type="depth_update")
            if first_update is not None and not target_spool.has_record_before(
                "snapshot_seed",
                _sort_key(0, first_update),
            ):
                if _recover_carry_state_from_spool(state, carry_spool, first_update, metrics):
                    _emit_synthetic_opening_snapshot(
                        state,
                        first_update,
                        metrics,
                        delta_batch,
                        depth10_batch,
                        emit_depth10=emit_depth10,
                    )

        _run_depth_replay_loop(
            target_spool.iter_records(),
            venue=venue,
            symbol=symbol,
            state=state,
            metrics=metrics,
            on_deltas_batch=on_deltas_batch,
            on_depth10_batch=on_depth10_batch,
            batch_size=batch_size,
            emit_depth10=emit_depth10,
            interval_ns=interval_ns,
            prime=_prime,
        )

    return metrics
