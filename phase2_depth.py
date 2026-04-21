"""Binance-native depth recorder.

Records raw exchange-native depth updates into ``depth_v2`` with explicit
snapshot/sync/lifecycle markers so the converter can perform deterministic
replay later.  session_seq is assigned only to committed canonical records.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import aiohttp

from config import (
    BINANCE_FUTURES_REST,
    BINANCE_SPOT_REST,
    DEPTH_INTERVAL_MS,
    DEPTH_V2_CHANNEL,
    PHASE2_MAX_RESYNCS_PER_SYMBOL_WINDOW,
    PHASE2_RESYNC_COOLDOWN_SEC,
    PHASE2_RESYNC_WINDOW_SEC,
    PHASE2_SNAPSHOT_LIMIT,
    PHASE2_SNAPSHOT_MAX_CONCURRENCY_PER_VENUE,
    PHASE2_SNAPSHOT_MIN_DELAY_SEC,
    PHASE2_SNAPSHOT_RETRY_BASE_DELAY_SEC,
    PHASE2_SNAPSHOT_RETRY_MAX_ATTEMPTS,
    WS_PING_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


SPOT_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_WS_BASE = "wss://fstream.binance.com/stream?streams="

SYNC_UNSYNCED = "unsynced"
SYNC_SNAPSHOT_SEEDED = "snapshot_seeded"
SYNC_LIVE_SYNCED = "live_synced"
SYNC_DESYNCED = "desynced"
SYNC_RESYNC_REQUIRED = "resync_required"
SYNC_FENCED = "fenced"


def _stream_name(symbol: str) -> str:
    return f"{symbol.lower()}@depth@{DEPTH_INTERVAL_MS}ms"


def _ws_url(venue: str, symbols: Iterable[str]) -> str:
    base = FUTURES_WS_BASE if venue == "BINANCE_USDTF" else SPOT_WS_BASE
    streams = "/".join(_stream_name(symbol) for symbol in symbols)
    return f"{base}{streams}"


def _snapshot_url(venue: str, symbol: str) -> str:
    if venue == "BINANCE_USDTF":
        return f"{BINANCE_FUTURES_REST}/fapi/v1/depth?symbol={symbol}&limit={PHASE2_SNAPSHOT_LIMIT}"
    return f"{BINANCE_SPOT_REST}/api/v3/depth?symbol={symbol}&limit={PHASE2_SNAPSHOT_LIMIT}"


@dataclass
class DepthSymbolState:
    venue: str
    symbol: str
    stream_session_id: int = 0
    next_ws_arrival_seq: int = 0
    next_session_seq: int = 0
    sync_state: str = SYNC_UNSYNCED
    previous_sync_state: Optional[str] = None
    last_update_id: Optional[int] = None
    prev_update_id: Optional[int] = None
    pending_updates: List[dict] = field(default_factory=list)
    resync_timestamps: Deque[float] = field(default_factory=deque)
    last_resync_request_monotonic: float = 0.0
    snapshot_task: Optional[asyncio.Task] = None
    fenced_reason: Optional[str] = None
    snapshot_seed_count: int = 0
    resync_count: int = 0
    desync_events: int = 0
    # ── rich diagnostics ──────────────────────────────────────────────
    accepted_update_count: int = 0
    rejected_update_count: int = 0
    last_snapshot_seed_ts: Optional[float] = None    # monotonic
    last_live_synced_ts: Optional[float] = None      # monotonic
    last_desync_ts: Optional[float] = None           # monotonic
    last_desync_reason: Optional[str] = None
    last_resync_reason: Optional[str] = None
    last_seen_U: Optional[int] = None
    last_seen_u: Optional[int] = None
    last_seen_pu: Optional[int] = None
    last_rejected_U: Optional[int] = None
    last_rejected_u: Optional[int] = None
    last_rejected_pu: Optional[int] = None

    def allocate_ws_arrival_seq(self) -> int:
        """Monotonic counter for all WS messages (internal ordering for buffering)."""
        self.next_ws_arrival_seq += 1
        return self.next_ws_arrival_seq

    def allocate_session_seq(self) -> int:
        """Monotonic counter for committed canonical records only."""
        self.next_session_seq += 1
        return self.next_session_seq

    def new_stream_session(self) -> None:
        self.stream_session_id += 1
        self.next_ws_arrival_seq = 0
        self.next_session_seq = 0
        self.pending_updates.clear()
        self.last_update_id = None
        self.prev_update_id = None
        self.fenced_reason = None


class SnapshotLimiter:
    """Simple per-venue concurrency and spacing guard for REST snapshots."""

    def __init__(self) -> None:
        self._semaphores = {
            "BINANCE_SPOT": asyncio.Semaphore(PHASE2_SNAPSHOT_MAX_CONCURRENCY_PER_VENUE),
            "BINANCE_USDTF": asyncio.Semaphore(PHASE2_SNAPSHOT_MAX_CONCURRENCY_PER_VENUE),
        }
        self._locks = defaultdict(asyncio.Lock)
        self._last_call_monotonic: Dict[str, float] = defaultdict(float)

    async def acquire(self, venue: str) -> None:
        await self._semaphores[venue].acquire()
        async with self._locks[venue]:
            elapsed = time.monotonic() - self._last_call_monotonic[venue]
            if elapsed < PHASE2_SNAPSHOT_MIN_DELAY_SEC:
                await asyncio.sleep(PHASE2_SNAPSHOT_MIN_DELAY_SEC - elapsed)
            self._last_call_monotonic[venue] = time.monotonic()

    def release(self, venue: str) -> None:
        self._semaphores[venue].release()


class BinanceNativeDepthRecorder:
    """Record Binance-native depth updates with explicit sync/resync metadata."""

    def __init__(
        self,
        *,
        storage_manager,
        health_monitor,
        shutdown_event: asyncio.Event,
    ) -> None:
        self.storage_manager = storage_manager
        self.health_monitor = health_monitor
        self.shutdown_event = shutdown_event
        self.snapshot_limiter = SnapshotLimiter()
        self._states: Dict[Tuple[str, str], DepthSymbolState] = {}
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def run(self, symbols_by_venue: Dict[str, List[str]]) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(timeout=timeout)
        try:
            for venue, symbols in symbols_by_venue.items():
                if symbols:
                    self._tasks.append(
                        asyncio.create_task(self._run_venue_loop(venue, list(symbols)))
                    )
            if self._tasks:
                await asyncio.gather(*self._tasks)
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        for task in self._tasks:
            if not task.done():
                task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _run_venue_loop(self, venue: str, symbols: List[str]) -> None:
        backoff = 1.0
        while not self.shutdown_event.is_set():
            for symbol in symbols:
                state = self._state_for(venue, symbol)
                state.new_stream_session()
                await self._emit_stream_lifecycle(
                    state,
                    event="session_start",
                    reason="startup_or_reconnect",
                )
                await self._transition_sync_state(state, SYNC_UNSYNCED, "new_stream_session")
                # NOTE: Do NOT trigger snapshot here.  The Binance bootstrap
                # protocol requires: connect WS → buffer messages → GET REST
                # snapshot → use buffer to bridge the gap.  The first WS
                # message handler triggers the snapshot via reason="bootstrap"
                # when last_update_id is None.

            try:
                ws_url = _ws_url(venue, symbols)
                assert self._session is not None
                async with self._session.ws_connect(
                    ws_url,
                    heartbeat=WS_PING_INTERVAL_SEC,
                ) as ws:
                    logger.info("Depth websocket connected: %s", ws_url)
                    backoff = 1.0
                    async for msg in ws:
                        if self.shutdown_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_ws_text(venue, msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(f"websocket error for {venue}: {ws.exception()}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Depth websocket error for %s: %s", venue, exc)
            finally:
                for symbol in symbols:
                    state = self._state_for(venue, symbol)
                    await self._emit_stream_lifecycle(
                        state,
                        event="session_end",
                        reason="websocket_closed",
                    )
                    await self._transition_sync_state(state, SYNC_RESYNC_REQUIRED, "websocket_closed")

            if self.shutdown_event.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    async def _handle_ws_text(self, venue: str, payload_text: str) -> None:
        try:
            message = json.loads(payload_text)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed depth frame for %s", venue)
            return

        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return

        symbol = data.get("s")
        if not symbol:
            return
        state = self._state_for(venue, symbol)

        ts_recv_ns = time.time_ns()
        ws_arrival_seq = state.allocate_ws_arrival_seq()

        # Build an internal buffered record (not yet committed).
        # session_seq is assigned only when the record is accepted.
        buffered = {
            "schema_version": 2,
            "record_type": "depth_update",
            "venue": venue,
            "symbol": symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "ws_arrival_seq": ws_arrival_seq,
            "ts_recv_ns": ts_recv_ns,
            "ts_event_ms": data.get("E"),
            "exchange_ts_ms": data.get("E"),
            "U": data.get("U"),
            "u": data.get("u"),
            "pu": data.get("pu"),
            "sync_state": state.sync_state,
            "payload": {
                "bids": data.get("b", []),
                "asks": data.get("a", []),
            },
        }

        update_id = data.get("u")
        self.health_monitor.record_message(
            venue=venue,
            symbol=symbol,
            ts_event=data.get("E"),
            update_id=update_id,
            channel=DEPTH_V2_CHANNEL,
        )

        if state.sync_state == SYNC_FENCED:
            return

        # Track last-seen continuity IDs for diagnostics
        state.last_seen_U = data.get("U")
        state.last_seen_u = data.get("u")
        state.last_seen_pu = data.get("pu")

        state.pending_updates.append(buffered)
        if len(state.pending_updates) > 5000:
            state.pending_updates = state.pending_updates[-5000:]

        if state.last_update_id is None:
            self._ensure_snapshot_task(state, reason="bootstrap")
            return

        # If we have a snapshot and are synced or seeded, try to accept immediately.
        if state.sync_state in (SYNC_LIVE_SYNCED, SYNC_SNAPSHOT_SEEDED):
            accepted = self._check_continuity(state, buffered)
            if accepted:
                await self._commit_depth_update(state, buffered)
            else:
                self._ensure_snapshot_task(state, reason="continuity_break")
        elif state.sync_state in (SYNC_DESYNCED, SYNC_RESYNC_REQUIRED):
            # Keep retrying snapshot acquisition after cooldown expires.
            # Without this, a symbol stuck in DESYNCED would never resync
            # because no code path re-triggers the snapshot task.
            self._ensure_snapshot_task(state, reason="desynced_retry")
        else:
            pass  # Still buffering (UNSYNCED — waiting for first WS message)

        self._report_symbol_state(state)

    def _state_for(self, venue: str, symbol: str) -> DepthSymbolState:
        key = (venue, symbol)
        if key not in self._states:
            self._states[key] = DepthSymbolState(venue=venue, symbol=symbol)
        return self._states[key]

    def _report_symbol_state(self, state: DepthSymbolState) -> None:
        """Push full diagnostic snapshot to health monitor."""
        self.health_monitor.record_phase2_symbol_state(
            venue=state.venue,
            symbol=state.symbol,
            sync_state=state.sync_state,
            last_update_id=state.last_update_id,
            prev_update_id=state.prev_update_id,
            snapshot_seed_count=state.snapshot_seed_count,
            resync_count=state.resync_count,
            desync_events=state.desync_events,
            stream_session_id=state.stream_session_id,
            accepted_update_count=state.accepted_update_count,
            rejected_update_count=state.rejected_update_count,
            buffered_update_count=len(state.pending_updates),
            last_snapshot_seed_ts=state.last_snapshot_seed_ts,
            last_live_synced_ts=state.last_live_synced_ts,
            last_desync_ts=state.last_desync_ts,
            last_desync_reason=state.last_desync_reason,
            last_resync_reason=state.last_resync_reason,
            fenced_reason=state.fenced_reason,
            last_seen_U=state.last_seen_U,
            last_seen_u=state.last_seen_u,
            last_seen_pu=state.last_seen_pu,
            last_rejected_U=state.last_rejected_U,
            last_rejected_u=state.last_rejected_u,
            last_rejected_pu=state.last_rejected_pu,
        )

    def _ensure_snapshot_task(self, state: DepthSymbolState, *, reason: str) -> None:
        if state.sync_state == SYNC_FENCED:
            return
        if state.snapshot_task and not state.snapshot_task.done():
            return
        now = time.monotonic()
        if now - state.last_resync_request_monotonic < PHASE2_RESYNC_COOLDOWN_SEC:
            return
        state.last_resync_request_monotonic = now
        state.snapshot_task = asyncio.create_task(self._snapshot_seed_task(state, reason=reason))

    async def _snapshot_seed_task(self, state: DepthSymbolState, *, reason: str) -> None:
        state.resync_count += 1
        self._trim_resync_window(state)
        state.resync_timestamps.append(time.monotonic())
        if len(state.resync_timestamps) > PHASE2_MAX_RESYNCS_PER_SYMBOL_WINDOW:
            state.sync_state = SYNC_FENCED
            state.fenced_reason = f"resync_limit_exceeded:{reason}"
            await self._transition_sync_state(state, SYNC_FENCED, state.fenced_reason)
            return

        await self._transition_sync_state(state, SYNC_RESYNC_REQUIRED, reason)
        url = _snapshot_url(state.venue, state.symbol)
        attempt = 0
        while not self.shutdown_event.is_set():
            await self.snapshot_limiter.acquire(state.venue)
            try:
                assert self._session is not None
                async with self._session.get(url) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"snapshot status={resp.status}")
                    raw = await resp.json()
                    await self._handle_snapshot_seed(state, raw, reason=reason)
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                logger.warning(
                    "Snapshot seed failed for %s/%s (%s): %s",
                    state.venue,
                    state.symbol,
                    reason,
                    exc,
                )
                if attempt >= PHASE2_SNAPSHOT_RETRY_MAX_ATTEMPTS:
                    await self._transition_sync_state(
                        state,
                        SYNC_DESYNCED,
                        f"snapshot_failed:{reason}",
                    )
                    return
                await asyncio.sleep(PHASE2_SNAPSHOT_RETRY_BASE_DELAY_SEC * attempt)
            finally:
                self.snapshot_limiter.release(state.venue)

    def _trim_resync_window(self, state: DepthSymbolState) -> None:
        cutoff = time.monotonic() - PHASE2_RESYNC_WINDOW_SEC
        while state.resync_timestamps and state.resync_timestamps[0] < cutoff:
            state.resync_timestamps.popleft()

    async def _handle_snapshot_seed(self, state: DepthSymbolState, raw: dict, *, reason: str) -> None:
        state.snapshot_seed_count += 1
        state.last_snapshot_seed_ts = time.monotonic()
        state.last_resync_reason = reason
        # Set IDs from snapshot but do NOT transition to SNAPSHOT_SEEDED yet.
        # Keep state as RESYNC_REQUIRED during promote so the WS handler just
        # buffers incoming messages without checking continuity.  The promote
        # method transitions the state after processing.
        state.last_update_id = raw.get("lastUpdateId")
        state.prev_update_id = raw.get("lastUpdateId")
        await self._promote_buffered_updates(state, raw, reason=reason)

    async def _promote_buffered_updates(
        self,
        state: DepthSymbolState,
        snapshot_raw: dict,
        *,
        reason: str,
    ) -> None:
        """Commit a deterministic bundle: snapshot_seed, accepted buffered updates, sync transition.

        session_seq is allocated atomically to the whole bundle so replay is
        guaranteed to see snapshot -> updates -> sync-transition in exact order.

        Implements the Binance depth bootstrap protocol:
          1. Drop buffered events where u <= lastUpdateId
          2. First accepted event must have U <= lastUpdateId+1 <= u (spot)
             or pu == lastUpdateId and u > lastUpdateId (futures)
          3. Subsequent events follow normal continuity rules

        The buffer is atomically swapped at entry so messages arriving during
        the async commit loop are preserved for subsequent live processing.
        """
        last_update_id = snapshot_raw.get("lastUpdateId")
        if last_update_id is None:
            state.pending_updates.clear()
            return

        # ── Atomically take the current buffer; new WS arrivals go into fresh list ──
        pending_snapshot = state.pending_updates
        state.pending_updates = []

        # 1. Sort buffered updates by ws_arrival_seq
        pending = sorted(
            pending_snapshot,
            key=lambda rec: (
                int(rec.get("stream_session_id", 0)),
                int(rec.get("ws_arrival_seq", 0)),
            ),
        )

        # 2. Apply Binance bootstrap: drop stale, find overlap, continue
        #
        # The stale-drop rule differs by venue:
        #   Spot:    drop events where u <= lastUpdateId
        #   Futures: drop events where u <  lastUpdateId
        #
        # The first accepted event (bootstrap overlap) rule differs too:
        #   Spot:    U <= lastUpdateId+1 <= u
        #   Futures: U <= lastUpdateId AND u >= lastUpdateId
        #
        # Subsequent events (ongoing continuity):
        #   Spot:    U <= prev_u+1 <= u   (generalised; U == prev_u+1 in practice)
        #   Futures: pu == prev_u
        is_futures = state.venue == "BINANCE_USDTF"
        accepted_updates: List[dict] = []
        prev_u = last_update_id
        stale_count = 0
        skip_count = 0
        found_first = False
        for rec in pending:
            u = rec.get("u")
            U = rec.get("U")
            pu = rec.get("pu")
            if u is None or U is None:
                continue
            # Drop stale events (Binance step 4)
            if is_futures:
                if u < last_update_id:
                    stale_count += 1
                    continue
            else:
                if u <= last_update_id:
                    stale_count += 1
                    continue
            # Bootstrap overlap check for the first non-stale event
            if not found_first:
                if is_futures:
                    ok = (U <= last_update_id) and (u >= last_update_id)
                else:
                    ok = (U <= last_update_id + 1 <= u)
                if ok:
                    found_first = True
                    accepted_updates.append(rec)
                    prev_u = u
                else:
                    skip_count += 1
            else:
                # Ongoing continuity
                if is_futures:
                    ok = (pu == prev_u)
                else:
                    ok = (U <= prev_u + 1 <= u)
                if ok:
                    accepted_updates.append(rec)
                    prev_u = u
                else:
                    skip_count += 1
        logger.info(
            "PROMOTE %s/%s: lastUpdateId=%s pending=%d stale=%d skip=%d accepted=%d",
            state.venue, state.symbol, last_update_id, len(pending), stale_count, skip_count, len(accepted_updates),
        )

        # 3. Commit the snapshot_seed record
        snapshot_session_seq = state.allocate_session_seq()
        snapshot_record = {
            "schema_version": 2,
            "record_type": "snapshot_seed",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "session_seq": snapshot_session_seq,
            "ts_recv_ns": time.time_ns(),
            "ts_event_ms": snapshot_raw.get("E") or snapshot_raw.get("T"),
            "exchange_ts_ms": snapshot_raw.get("E") or snapshot_raw.get("T"),
            "lastUpdateId": snapshot_raw.get("lastUpdateId"),
            "sync_reason": reason,
            "payload": {
                "bids": snapshot_raw.get("bids", []),
                "asks": snapshot_raw.get("asks", []),
            },
        }
        await self.storage_manager.write_record(
            state.venue, state.symbol, DEPTH_V2_CHANNEL, snapshot_record,
        )

        # 4. Commit each accepted buffered depth_update
        for rec in accepted_updates:
            session_seq = state.allocate_session_seq()
            committed = dict(rec)
            committed["session_seq"] = session_seq
            committed.pop("ws_arrival_seq", None)
            await self.storage_manager.write_record(
                state.venue, state.symbol, DEPTH_V2_CHANNEL, committed,
            )

        # 5. Update state and transition sync
        state.accepted_update_count += len(accepted_updates)
        if accepted_updates:
            last_accepted_u = accepted_updates[-1].get("u")
            if last_accepted_u is not None:
                state.prev_update_id = last_accepted_u
                state.last_update_id = last_accepted_u
            state.last_live_synced_ts = time.monotonic()
            await self._transition_sync_state(state, SYNC_LIVE_SYNCED, "bootstrap_promote")
        else:
            # No buffered updates bridged the gap — transition to SNAPSHOT_SEEDED
            # so the live WS handler will attempt continuity from prev_update_id.
            await self._transition_sync_state(state, SYNC_SNAPSHOT_SEEDED, reason)

    async def _commit_depth_update(self, state: DepthSymbolState, buffered: dict) -> None:
        """Commit a single live depth_update that passed continuity checks."""
        session_seq = state.allocate_session_seq()
        committed = dict(buffered)
        committed["session_seq"] = session_seq
        committed.pop("ws_arrival_seq", None)
        await self.storage_manager.write_record(
            state.venue, state.symbol, DEPTH_V2_CHANNEL, committed,
        )

    def _check_continuity(self, state: DepthSymbolState, record: dict) -> bool:
        """Check Binance depth continuity rules. Returns True if accepted.

        For the first event after a snapshot (SNAPSHOT_SEEDED), uses the
        bootstrap overlap rule.  For subsequent events (LIVE_SYNCED), uses
        the ongoing continuity rule.

        Updates state.last_update_id / prev_update_id / sync_state on success.
        """
        u = record.get("u")
        U = record.get("U")
        pu = record.get("pu")
        prev = state.prev_update_id
        if u is None or U is None or prev is None:
            return False

        is_futures = state.venue == "BINANCE_USDTF"
        is_bootstrap = state.sync_state == SYNC_SNAPSHOT_SEEDED

        if is_futures:
            if is_bootstrap:
                # Futures bootstrap: U <= lastUpdateId AND u >= lastUpdateId
                accepted = (U <= prev) and (u >= prev)
            else:
                # Futures ongoing: pu == prev_u
                accepted = pu == prev
        else:
            # Spot: same formula for bootstrap and ongoing
            accepted = U <= (prev + 1) <= u

        if accepted:
            state.prev_update_id = u
            state.last_update_id = u
            state.sync_state = SYNC_LIVE_SYNCED
            state.accepted_update_count += 1
            state.last_live_synced_ts = time.monotonic()
            return True

        state.desync_events += 1
        state.rejected_update_count += 1
        state.sync_state = SYNC_DESYNCED
        state.last_desync_ts = time.monotonic()
        state.last_desync_reason = (
            f"bootstrap_{'futures' if is_futures else 'spot'}" if is_bootstrap
            else f"continuity_{'futures' if is_futures else 'spot'}"
        )
        state.last_rejected_U = U
        state.last_rejected_u = u
        state.last_rejected_pu = pu
        return False

    async def _transition_sync_state(
        self,
        state: DepthSymbolState,
        new_state: str,
        reason: str,
    ) -> None:
        previous = state.sync_state
        state.previous_sync_state = previous
        state.sync_state = new_state
        session_seq = state.allocate_session_seq()
        record = {
            "schema_version": 2,
            "record_type": "sync_state",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "session_seq": session_seq,
            "ts_recv_ns": time.time_ns(),
            "previous_state": previous,
            "state": new_state,
            "reason": reason,
            "last_update_id": state.last_update_id,
            "prev_update_id": state.prev_update_id,
        }
        await self.storage_manager.write_record(state.venue, state.symbol, DEPTH_V2_CHANNEL, record)
        self._report_symbol_state(state)

    async def _emit_stream_lifecycle(
        self,
        state: DepthSymbolState,
        *,
        event: str,
        reason: str,
    ) -> None:
        session_seq = state.allocate_session_seq()
        record = {
            "schema_version": 2,
            "record_type": "stream_lifecycle",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "session_seq": session_seq,
            "ts_recv_ns": time.time_ns(),
            "event": event,
            "reason": reason,
        }
        await self.storage_manager.write_record(state.venue, state.symbol, DEPTH_V2_CHANNEL, record)
