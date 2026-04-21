"""
Phase 2 Binance-native depth recorder.

Records raw exchange-native depth updates into ``depth_v2`` with explicit
snapshot/sync/lifecycle markers so the converter can perform deterministic
replay later.
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
    next_connection_seq: int = 0
    next_file_position: int = 0
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

    def allocate_file_position(self) -> int:
        self.next_file_position += 1
        return self.next_file_position

    def allocate_connection_seq(self) -> int:
        self.next_connection_seq += 1
        return self.next_connection_seq

    def new_stream_session(self) -> None:
        self.stream_session_id += 1
        self.next_connection_seq = 0
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
                self._ensure_snapshot_task(state, reason="session_start")

            try:
                ws_url = _ws_url(venue, symbols)
                assert self._session is not None
                async with self._session.ws_connect(
                    ws_url,
                    heartbeat=WS_PING_INTERVAL_SEC,
                ) as ws:
                    logger.info("Phase 2 depth websocket connected: %s", ws_url)
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
                logger.warning("Phase 2 depth websocket error for %s: %s", venue, exc)
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
            logger.warning("Skipping malformed Phase 2 depth frame for %s", venue)
            return

        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return

        symbol = data.get("s")
        if not symbol:
            return
        state = self._state_for(venue, symbol)

        ts_recv_ns = time.time_ns()
        connection_seq = state.allocate_connection_seq()
        file_position = state.allocate_file_position()
        update_id = data.get("u")

        record = {
            "schema_version": 2,
            "record_type": "depth_update",
            "venue": venue,
            "symbol": symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "connection_seq": connection_seq,
            "file_position": file_position,
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
        await self.storage_manager.write_record(venue, symbol, DEPTH_V2_CHANNEL, record)

        self.health_monitor.record_message(
            venue=venue,
            symbol=symbol,
            ts_event=data.get("E"),
            update_id=update_id,
            channel=DEPTH_V2_CHANNEL,
        )

        if state.sync_state == SYNC_FENCED:
            return

        state.pending_updates.append(record)
        if len(state.pending_updates) > 5000:
            state.pending_updates = state.pending_updates[-5000:]

        if state.last_update_id is None:
            self._ensure_snapshot_task(state, reason="bootstrap")
            return

        accepted = self._update_continuity_state(state, record)
        self.health_monitor.record_phase2_symbol_state(
            venue=venue,
            symbol=symbol,
            sync_state=state.sync_state,
            last_update_id=state.last_update_id,
            prev_update_id=state.prev_update_id,
            snapshot_seed_count=state.snapshot_seed_count,
            resync_count=state.resync_count,
            desync_events=state.desync_events,
        )
        if not accepted:
            self._ensure_snapshot_task(state, reason="continuity_break")

    def _state_for(self, venue: str, symbol: str) -> DepthSymbolState:
        key = (venue, symbol)
        if key not in self._states:
            self._states[key] = DepthSymbolState(venue=venue, symbol=symbol)
        return self._states[key]

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
        state.last_update_id = raw.get("lastUpdateId")
        state.prev_update_id = raw.get("lastUpdateId")
        file_position = state.allocate_file_position()
        record = {
            "schema_version": 2,
            "record_type": "snapshot_seed",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "connection_seq": state.next_connection_seq,
            "file_position": file_position,
            "ts_recv_ns": time.time_ns(),
            "ts_event_ms": raw.get("E") or raw.get("T"),
            "exchange_ts_ms": raw.get("E") or raw.get("T"),
            "lastUpdateId": raw.get("lastUpdateId"),
            "sync_reason": reason,
            "payload": {
                "bids": raw.get("bids", []),
                "asks": raw.get("asks", []),
            },
        }
        await self.storage_manager.write_record(state.venue, state.symbol, DEPTH_V2_CHANNEL, record)
        await self._transition_sync_state(state, SYNC_SNAPSHOT_SEEDED, reason)
        self._promote_buffered_updates(state)

    def _promote_buffered_updates(self, state: DepthSymbolState) -> None:
        pending = sorted(
            state.pending_updates,
            key=lambda rec: (
                int(rec.get("stream_session_id", 0)),
                int(rec.get("connection_seq", 0)),
                int(rec.get("ts_recv_ns", 0)),
                int(rec.get("file_position", 0)),
            ),
        )
        for rec in pending:
            if self._update_continuity_state(state, rec):
                continue
            break

    def _update_continuity_state(self, state: DepthSymbolState, record: dict) -> bool:
        u = record.get("u")
        U = record.get("U")
        pu = record.get("pu")
        prev = state.prev_update_id
        if u is None or U is None or prev is None:
            return False

        if state.venue == "BINANCE_USDTF":
            accepted = pu == prev and u > prev
        else:
            accepted = U <= (prev + 1) <= u

        if accepted:
            state.prev_update_id = u
            state.last_update_id = u
            state.sync_state = SYNC_LIVE_SYNCED
            return True

        state.desync_events += 1
        state.sync_state = SYNC_DESYNCED
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
        file_position = state.allocate_file_position()
        record = {
            "schema_version": 2,
            "record_type": "sync_state",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "connection_seq": state.next_connection_seq,
            "file_position": file_position,
            "ts_recv_ns": time.time_ns(),
            "previous_state": previous,
            "state": new_state,
            "reason": reason,
            "last_update_id": state.last_update_id,
            "prev_update_id": state.prev_update_id,
        }
        await self.storage_manager.write_record(state.venue, state.symbol, DEPTH_V2_CHANNEL, record)
        self.health_monitor.record_phase2_symbol_state(
            venue=state.venue,
            symbol=state.symbol,
            sync_state=state.sync_state,
            last_update_id=state.last_update_id,
            prev_update_id=state.prev_update_id,
            snapshot_seed_count=state.snapshot_seed_count,
            resync_count=state.resync_count,
            desync_count=state.desync_events,
        )

    async def _emit_stream_lifecycle(
        self,
        state: DepthSymbolState,
        *,
        event: str,
        reason: str,
    ) -> None:
        file_position = state.allocate_file_position()
        record = {
            "schema_version": 2,
            "record_type": "stream_lifecycle",
            "venue": state.venue,
            "symbol": state.symbol,
            "channel": DEPTH_V2_CHANNEL,
            "stream_session_id": state.stream_session_id,
            "connection_seq": state.next_connection_seq,
            "file_position": file_position,
            "ts_recv_ns": time.time_ns(),
            "event": event,
            "reason": reason,
        }
        await self.storage_manager.write_record(state.venue, state.symbol, DEPTH_V2_CHANNEL, record)
