"""Binance-native trade recorder.

Records raw exchange-native trades into ``trade_v2`` with explicit
session/lifecycle markers.  ``trade_session_seq`` is assigned only to
committed canonical trade records — diagnostic events and lifecycle
markers never consume canonical ordering positions.

Spot streams use ``@trade``, futures use ``@aggTrade``.  Each record is
a tagged union (``market_type`` discriminator) so replay can decode
venue-specific fields unambiguously.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

from config import (
    FUTURES_TRADE_WS_FALLBACK_ENABLED,
    FUTURES_TRADE_WS_PROBE_SYMBOL,
    FUTURES_TRADE_WS_FALLBACK_PROBE_TIMEOUT_SEC,
    TRADE_WS_FIRST_MESSAGE_TIMEOUT_SEC,
    TRADE_WS_IDLE_TIMEOUT_SEC,
    TRADE_WS_MAX_SYMBOLS_PER_CONNECTION,
    TRADE_WS_SHARD_ADAPTIVE_SPLIT_ENABLED,
    TRADE_WS_SHARD_ENABLED,
    TRADE_WS_SHARD_SPLIT_SIZES,
    TRADE_V2_CHANNEL,
    WS_PING_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


SPOT_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_WS_BASE = "wss://fstream.binance.com/stream?streams="


def _new_diag_bucket() -> Dict[str, Any]:
    return {
        "ws_message_count": 0,
        "parsed_trade_count": 0,
        "skipped_message_count": 0,
        "skip_reasons": {},
        "lifecycle_only_sessions": 0,
        "reconnect_count": 0,
        "last_close_reason": None,
        "sample_payload_shape": None,
        "subscribed_symbols": [],
        "subscribed_symbol_count": 0,
        "per_symbol_parsed_trade_count": {},
        "stream_count": 0,
        "first_5_streams": [],
        "url": None,
        "url_length": 0,
        "task_started": False,
        "task_done": False,
        "task_cancelled": False,
        "connect_attempt_count": 0,
        "connected_once": False,
        "first_message_seen_at": None,
        "last_message_seen_at": None,
        "last_exception": None,
        # ── extended diagnostics ──────────────────────────────────────
        "first_message_timeout_count": 0,
        "no_first_message_since_connect_count": 0,
        "current_stream_mode": None,
        "endpoint": None,
        "last_url": None,
        "suggested_action": None,
    }


def _stream_name_spot(symbol: str) -> str:
    return f"{symbol.lower()}@trade"


def _stream_name_futures(symbol: str, mode: str = "aggTrade") -> str:
    """Return the futures stream name for the given symbol and mode.

    Binance USDT-M Futures supports @aggTrade (default) and @trade.
    """
    if mode == "trade":
        return f"{symbol.lower()}@trade"
    return f"{symbol.lower()}@aggTrade"


def _ws_url(venue: str, symbols: Iterable[str], *, stream_mode: str = "aggTrade") -> str:
    if venue == "BINANCE_USDTF":
        base = FUTURES_WS_BASE
        streams = "/".join(_stream_name_futures(s, stream_mode) for s in symbols)
    else:
        base = SPOT_WS_BASE
        streams = "/".join(_stream_name_spot(s) for s in symbols)
    return f"{base}{streams}"


def _chunk_symbols(symbols: List[str], max_size: int) -> List[List[str]]:
    if max_size <= 0 or len(symbols) <= max_size:
        return [list(symbols)]
    return [list(symbols[i : i + max_size]) for i in range(0, len(symbols), max_size)]


@dataclass
class TradeSymbolState:
    venue: str
    symbol: str
    market_type: str = ""  # "spot" or "futures"
    trade_stream_session_id: int = 0
    next_trade_session_seq: int = 0
    session_trade_count: int = 0
    total_trade_count: int = 0
    last_exchange_trade_id: Optional[int] = None
    trade_id_regressions: int = 0

    def allocate_trade_session_seq(self) -> int:
        """Monotonic counter for committed canonical trade records only."""
        self.next_trade_session_seq += 1
        return self.next_trade_session_seq

    def new_stream_session(self) -> None:
        self.trade_stream_session_id += 1
        self.next_trade_session_seq = 0
        self.session_trade_count = 0
        self.last_exchange_trade_id = None


class BinanceNativeTradeRecorder:
    """Record Binance-native trades with explicit session/lifecycle metadata.

    Commits each trade record with trade_session_seq, validates exchange trade-id
    monotonicity diagnostically (never reorders by it).
    """

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
        self._states: Dict[Tuple[str, str], TradeSymbolState] = {}
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._venue_diag: Dict[str, Dict[str, Any]] = defaultdict(self._new_venue_diag)
        self._symbol_to_shard_key: Dict[Tuple[str, str], str] = {}
        # Per-venue active stream mode ("aggTrade" or "trade").
        # Starts with aggTrade for futures; may fall back to trade.
        self._futures_trade_mode: Dict[str, str] = {}

    @staticmethod
    def _new_venue_diag() -> Dict[str, Any]:
        bucket = _new_diag_bucket()
        bucket["shards"] = {}
        return bucket

    def _diag_bucket(self, venue: str, shard_key: Optional[str] = None) -> Dict[str, Any]:
        venue_diag = self._venue_diag[venue]
        if shard_key is None:
            return venue_diag
        shards = venue_diag.setdefault("shards", {})
        if shard_key not in shards:
            shards[shard_key] = _new_diag_bucket()
        return shards[shard_key]

    def _set_subscription_diag(
        self,
        venue: str,
        symbols: List[str],
        *,
        shard_key: Optional[str] = None,
        stream_mode: str = "aggTrade",
    ) -> None:
        """Expose selected stream symbols in heartbeat diagnostics."""
        normalized = sorted({s for s in symbols if isinstance(s, str)})
        stream_names = [
            _stream_name_futures(symbol, stream_mode) if venue == "BINANCE_USDTF"
            else _stream_name_spot(symbol)
            for symbol in normalized
        ]
        endpoint = FUTURES_WS_BASE if venue == "BINANCE_USDTF" else SPOT_WS_BASE
        diag = self._diag_bucket(venue, shard_key)
        diag["subscribed_symbols"] = normalized
        diag["subscribed_symbol_count"] = len(normalized)
        diag["stream_count"] = len(stream_names)
        diag["first_5_streams"] = stream_names[:5]
        diag["current_stream_mode"] = stream_mode
        diag["endpoint"] = endpoint

        venue_diag = self._diag_bucket(venue)
        existing = set(venue_diag.get("subscribed_symbols", []))
        existing.update(normalized)
        venue_symbols = sorted(existing)
        venue_stream_names = [
            _stream_name_futures(symbol, stream_mode) if venue == "BINANCE_USDTF"
            else _stream_name_spot(symbol)
            for symbol in venue_symbols
        ]
        venue_diag["subscribed_symbols"] = venue_symbols
        venue_diag["subscribed_symbol_count"] = len(venue_symbols)
        venue_diag["stream_count"] = len(venue_stream_names)
        venue_diag["first_5_streams"] = venue_stream_names[:5]
        venue_diag["current_stream_mode"] = stream_mode
        venue_diag["endpoint"] = endpoint

    def _mark_task_started(self, venue: str, shard_key: str, url: str) -> None:
        diag = self._diag_bucket(venue, shard_key)
        diag["task_started"] = True
        diag["task_done"] = False
        diag["task_cancelled"] = False
        diag["url"] = url
        diag["last_url"] = url
        diag["url_length"] = len(url)

    def _mark_task_done(self, venue: str, shard_key: str, *, cancelled: bool = False) -> None:
        diag = self._diag_bucket(venue, shard_key)
        diag["task_done"] = True
        diag["task_cancelled"] = cancelled

    def _mark_connect_attempt(self, venue: str, shard_key: str) -> None:
        diag = self._diag_bucket(venue, shard_key)
        diag["connect_attempt_count"] = int(diag.get("connect_attempt_count", 0)) + 1

    def _mark_connected(self, venue: str, shard_key: str) -> None:
        self._diag_bucket(venue, shard_key)["connected_once"] = True

    def _mark_message_seen(self, venue: str, shard_key: str) -> None:
        now = time.time()
        diag = self._diag_bucket(venue, shard_key)
        if diag.get("first_message_seen_at") is None:
            diag["first_message_seen_at"] = now
        diag["last_message_seen_at"] = now

    def _mark_exception(self, venue: str, shard_key: str, exc: BaseException) -> None:
        self._diag_bucket(venue, shard_key)["last_exception"] = (
            f"{type(exc).__name__}: {exc}"
        )

    def _record_skip(self, venue: str, reason: str, *, shard_key: Optional[str] = None) -> None:
        for diag in {id(self._diag_bucket(venue)): self._diag_bucket(venue), **({id(self._diag_bucket(venue, shard_key)): self._diag_bucket(venue, shard_key)} if shard_key else {})}.values():
            diag["skipped_message_count"] += 1
            skip_reasons = diag["skip_reasons"]
            skip_reasons[reason] = int(skip_reasons.get(reason, 0)) + 1

    def _record_sample_payload_shape(
        self,
        venue: str,
        message: dict,
        data: dict,
        *,
        shard_key: Optional[str] = None,
    ) -> None:
        sample = {
            "venue": venue,
            "stream": message.get("stream") if isinstance(message, dict) else None,
            "stream_event": data.get("e"),
            "symbol": data.get("s"),
            "field_names": sorted(data.keys()),
            "event_type": data.get("e"),
        }
        for diag in {id(self._diag_bucket(venue)): self._diag_bucket(venue), **({id(self._diag_bucket(venue, shard_key)): self._diag_bucket(venue, shard_key)} if shard_key else {})}.values():
            if diag.get("sample_payload_shape") is None:
                diag["sample_payload_shape"] = sample

    def get_venue_diagnostics(self) -> Dict[str, Dict[str, Any]]:
        def _serialize(diag: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "ws_message_count": int(diag.get("ws_message_count", 0)),
                "parsed_trade_count": int(diag.get("parsed_trade_count", 0)),
                "skipped_message_count": int(diag.get("skipped_message_count", 0)),
                "skip_reasons": dict(diag.get("skip_reasons", {})),
                "lifecycle_only_sessions": int(diag.get("lifecycle_only_sessions", 0)),
                "reconnect_count": int(diag.get("reconnect_count", 0)),
                "last_close_reason": diag.get("last_close_reason"),
                "sample_payload_shape": diag.get("sample_payload_shape"),
                "subscribed_symbols": list(diag.get("subscribed_symbols", [])),
                "subscribed_symbol_count": int(diag.get("subscribed_symbol_count", 0)),
                "per_symbol_parsed_trade_count": dict(
                    diag.get("per_symbol_parsed_trade_count", {})
                ),
                "stream_count": int(diag.get("stream_count", 0)),
                "first_5_streams": list(diag.get("first_5_streams", [])),
                "url": diag.get("url"),
                "url_length": int(diag.get("url_length", 0)),
                "task_started": bool(diag.get("task_started", False)),
                "task_done": bool(diag.get("task_done", False)),
                "task_cancelled": bool(diag.get("task_cancelled", False)),
                "connect_attempt_count": int(diag.get("connect_attempt_count", 0)),
                "connected_once": bool(diag.get("connected_once", False)),
                "first_message_seen_at": diag.get("first_message_seen_at"),
                "last_message_seen_at": diag.get("last_message_seen_at"),
                "last_exception": diag.get("last_exception"),
                # extended
                "first_message_timeout_count": int(diag.get("first_message_timeout_count", 0)),
                "no_first_message_since_connect_count": int(
                    diag.get("no_first_message_since_connect_count", 0)
                ),
                "current_stream_mode": diag.get("current_stream_mode"),
                "endpoint": diag.get("endpoint"),
                "last_url": diag.get("last_url"),
                "suggested_action": diag.get("suggested_action"),
            }

        return {
            venue: {
                **_serialize(diag),
                "shards": {
                    shard_key: _serialize(shard_diag)
                    for shard_key, shard_diag in diag.get("shards", {}).items()
                },
            }
            for venue, diag in self._venue_diag.items()
        }

    async def _finalize_symbol_session(self, state: TradeSymbolState, *, reason: str) -> None:
        if state.session_trade_count == 0:
            self._venue_diag[state.venue]["lifecycle_only_sessions"] += 1
            logger.warning(
                "Lifecycle-only trade session %s/%s stream_session=%d reason=%s",
                state.venue,
                state.symbol,
                state.trade_stream_session_id,
                reason,
            )
        await self._emit_trade_lifecycle(
            state,
            event="session_end",
            reason=reason,
        )

    def _log_venue_diag_summary(
        self,
        venue: str,
        *,
        close_reason: str,
        shard_key: Optional[str] = None,
    ) -> None:
        diag = self._diag_bucket(venue, shard_key)
        diag["last_close_reason"] = close_reason
        scope = f"{venue}/{shard_key}" if shard_key else venue
        logger.info(
            "Trade ingest diag %s: ws_messages=%d parsed_trades=%d skipped=%d lifecycle_only_sessions=%d reconnects=%d close_reason=%s skip_reasons=%s sample_payload=%s",
            scope,
            diag["ws_message_count"],
            diag["parsed_trade_count"],
            diag["skipped_message_count"],
            diag["lifecycle_only_sessions"],
            diag["reconnect_count"],
            close_reason,
            diag["skip_reasons"],
            diag["sample_payload_shape"],
        )

    async def _probe_futures_trade_stream(
        self,
        probe_symbol: str,
        mode: str,
        timeout: float,
    ) -> bool:
        """Try a single-symbol WS connection; return True if a TEXT frame arrives."""
        url = _ws_url("BINANCE_USDTF", [probe_symbol], stream_mode=mode)
        logger.info(
            "Probing futures trade stream mode=%s symbol=%s url=%s timeout=%.1fs",
            mode, probe_symbol, url, timeout,
        )
        try:
            assert self._session is not None
            async with self._session.ws_connect(url, heartbeat=WS_PING_INTERVAL_SEC) as ws:
                try:
                    msg = await ws.receive(timeout=timeout)
                    got_text = msg.type == aiohttp.WSMsgType.TEXT
                    logger.info(
                        "Probe result mode=%s symbol=%s got_text=%s msg_type=%s",
                        mode, probe_symbol, got_text, msg.type,
                    )
                    return got_text
                except asyncio.TimeoutError:
                    logger.info(
                        "Probe timeout mode=%s symbol=%s timeout=%.1fs",
                        mode, probe_symbol, timeout,
                    )
                    return False
        except Exception as exc:
            logger.warning("Probe failed mode=%s symbol=%s: %s", mode, probe_symbol, exc)
            return False

    async def run(self, symbols_by_venue: Dict[str, List[str]]) -> None:
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(timeout=timeout)
        try:
            for venue, symbols in symbols_by_venue.items():
                if symbols:
                    sorted_symbols = sorted(symbols)
                    shards = (
                        _chunk_symbols(sorted_symbols, TRADE_WS_MAX_SYMBOLS_PER_CONNECTION)
                        if TRADE_WS_SHARD_ENABLED
                        else [sorted_symbols]
                    )
                    shard_count = len(shards)
                    # Initialise stream mode: futures → aggTrade, spot → trade (only mode)
                    if venue not in self._futures_trade_mode:
                        self._futures_trade_mode[venue] = (
                            "aggTrade" if venue == "BINANCE_USDTF" else "trade"
                        )
                    stream_mode = self._futures_trade_mode[venue]
                    for shard_index, shard_symbols in enumerate(shards, start=1):
                        shard_key = f"shard_{shard_index}_of_{shard_count}"
                        for symbol in shard_symbols:
                            self._symbol_to_shard_key[(venue, symbol)] = shard_key
                        self._set_subscription_diag(
                            venue,
                            list(shard_symbols),
                            shard_key=shard_key,
                            stream_mode=stream_mode,
                        )
                        self._tasks.append(
                            asyncio.create_task(
                                self._run_venue_loop(
                                    venue,
                                    list(shard_symbols),
                                    shard_index=shard_index,
                                    shard_count=shard_count,
                                )
                            )
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

    async def _run_venue_loop(
        self,
        venue: str,
        symbols: List[str],
        *,
        shard_index: int = 1,
        shard_count: int = 1,
    ) -> None:
        backoff = 1.0
        shard_key = f"shard_{shard_index}_of_{shard_count}"
        shard_label = f"{venue}[{shard_index}/{shard_count}]"

        # Per-shard adaptive state: shrinks on repeated first-message timeouts.
        current_symbols: List[str] = list(symbols)
        split_index: int = 0          # Index into TRADE_WS_SHARD_SPLIT_SIZES
        first_msg_timeout_count: int = 0

        def _current_mode() -> str:
            return self._futures_trade_mode.get(venue, "aggTrade")

        ws_url = _ws_url(venue, current_symbols, stream_mode=_current_mode())
        self._mark_task_started(venue, shard_key, ws_url)
        try:
            while not self.shutdown_event.is_set():
                close_reason = "websocket_closed"
                for symbol in current_symbols:
                    state = self._state_for(venue, symbol)
                    state.new_stream_session()
                    await self._emit_trade_lifecycle(
                        state,
                        event="session_start",
                        reason="startup_or_reconnect",
                    )

                try:
                    self._mark_connect_attempt(venue, shard_key)
                    assert self._session is not None
                    async with self._session.ws_connect(
                        ws_url,
                        heartbeat=WS_PING_INTERVAL_SEC,
                    ) as ws:
                        self._mark_connected(venue, shard_key)
                        diag = self._diag_bucket(venue, shard_key)
                        diag["last_url"] = ws_url
                        diag["current_stream_mode"] = _current_mode()
                        logger.info(
                            "Trade websocket connected %s: streams=%d mode=%s url_len=%d first_streams=%s",
                            shard_label,
                            len(current_symbols),
                            _current_mode(),
                            len(ws_url),
                            diag.get("first_5_streams", []),
                        )
                        backoff = 1.0
                        first_seen = False
                        last_seen = time.monotonic()
                        while not self.shutdown_event.is_set():
                            try:
                                msg = await ws.receive(timeout=1.0)
                            except asyncio.TimeoutError:
                                idle_for = time.monotonic() - last_seen
                                if (
                                    not first_seen
                                    and idle_for >= TRADE_WS_FIRST_MESSAGE_TIMEOUT_SEC
                                ):
                                    raise TimeoutError(
                                        f"no first trade websocket message for {shard_label} "
                                        f"after {TRADE_WS_FIRST_MESSAGE_TIMEOUT_SEC:.1f}s"
                                    )
                                if first_seen and idle_for >= TRADE_WS_IDLE_TIMEOUT_SEC:
                                    raise TimeoutError(
                                        f"trade websocket idle for {shard_label} "
                                        f"after {TRADE_WS_IDLE_TIMEOUT_SEC:.1f}s"
                                    )
                                continue

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                first_seen = True
                                last_seen = time.monotonic()
                                self._mark_message_seen(venue, shard_key)
                                await self._handle_ws_text(venue, msg.data, shard_key=shard_key)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                raise RuntimeError(
                                    f"trade websocket error for {shard_label}: {ws.exception()}"
                                )
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSING,
                            ):
                                break
                except asyncio.CancelledError:
                    self._mark_task_done(venue, shard_key, cancelled=True)
                    raise
                except TimeoutError as exc:
                    close_reason = f"websocket_error:{type(exc).__name__}"
                    self._mark_exception(venue, shard_key, exc)
                    logger.warning("Trade websocket error for %s: %s", shard_label, exc)

                    # ── first-message timeout: try mode fallback + adaptive split ──
                    if "no first trade websocket message" in str(exc) and venue == "BINANCE_USDTF":
                        first_msg_timeout_count += 1
                        shard_diag = self._diag_bucket(venue, shard_key)
                        shard_diag["first_message_timeout_count"] = first_msg_timeout_count
                        shard_diag["no_first_message_since_connect_count"] = (
                            int(shard_diag.get("no_first_message_since_connect_count", 0)) + 1
                        )

                        # 1. Try @trade fallback probe on first timeout
                        if FUTURES_TRADE_WS_FALLBACK_ENABLED and first_msg_timeout_count == 1:
                            current_mode = _current_mode()
                            fallback_mode = "trade" if current_mode == "aggTrade" else "aggTrade"
                            logger.warning(
                                "Futures trade shard %s got no messages with mode=%s; "
                                "probing mode=%s on %s",
                                shard_label, current_mode, fallback_mode, FUTURES_TRADE_WS_PROBE_SYMBOL,
                            )
                            probe_ok = await self._probe_futures_trade_stream(
                                FUTURES_TRADE_WS_PROBE_SYMBOL,
                                fallback_mode,
                                FUTURES_TRADE_WS_FALLBACK_PROBE_TIMEOUT_SEC,
                            )
                            if probe_ok:
                                logger.warning(
                                    "Probe SUCCEEDED: switching %s from mode=%s to mode=%s",
                                    venue, current_mode, fallback_mode,
                                )
                                self._futures_trade_mode[venue] = fallback_mode
                                shard_diag["current_stream_mode"] = fallback_mode
                                shard_diag["suggested_action"] = f"switched_to_{fallback_mode}"
                                # Rebuild URL with new mode and reset split
                                current_symbols = list(symbols)
                                split_index = 0
                                ws_url = _ws_url(
                                    venue, current_symbols, stream_mode=fallback_mode
                                )
                                self._set_subscription_diag(
                                    venue, current_symbols,
                                    shard_key=shard_key, stream_mode=fallback_mode,
                                )
                            else:
                                logger.warning(
                                    "Probe FAILED for mode=%s; both modes silent for %s",
                                    fallback_mode, shard_label,
                                )
                                shard_diag["suggested_action"] = (
                                    "check_futures_trade_ws_endpoint_or_ip_restrictions"
                                )
                                # 2. Adaptive shard split on probe failure
                                if (
                                    TRADE_WS_SHARD_ADAPTIVE_SPLIT_ENABLED
                                    and len(current_symbols) > 1
                                    and split_index < len(TRADE_WS_SHARD_SPLIT_SIZES)
                                ):
                                    new_size = TRADE_WS_SHARD_SPLIT_SIZES[split_index]
                                    split_index += 1
                                    if new_size < len(current_symbols):
                                        old_count = len(current_symbols)
                                        current_symbols = current_symbols[:new_size]
                                        logger.warning(
                                            "Adaptive split %s: reducing %d → %d symbols "
                                            "(split_index=%d sizes=%s)",
                                            shard_label,
                                            old_count,
                                            len(current_symbols),
                                            split_index,
                                            TRADE_WS_SHARD_SPLIT_SIZES,
                                        )
                                        shard_diag["suggested_action"] = (
                                            f"adaptive_split_to_{len(current_symbols)}_symbols"
                                        )
                                        ws_url = _ws_url(
                                            venue, current_symbols,
                                            stream_mode=_current_mode(),
                                        )
                        elif TRADE_WS_SHARD_ADAPTIVE_SPLIT_ENABLED and first_msg_timeout_count > 1:
                            # Subsequent timeouts: keep splitting if possible
                            if (
                                len(current_symbols) > 1
                                and split_index < len(TRADE_WS_SHARD_SPLIT_SIZES)
                            ):
                                new_size = TRADE_WS_SHARD_SPLIT_SIZES[split_index]
                                split_index += 1
                                if new_size < len(current_symbols):
                                    old_count = len(current_symbols)
                                    current_symbols = current_symbols[:new_size]
                                    logger.warning(
                                        "Adaptive split %s (timeout #%d): reducing %d → %d symbols",
                                        shard_label, first_msg_timeout_count,
                                        old_count, len(current_symbols),
                                    )
                                    ws_url = _ws_url(
                                        venue, current_symbols, stream_mode=_current_mode()
                                    )
                                    self._diag_bucket(venue, shard_key)["suggested_action"] = (
                                        f"adaptive_split_to_{len(current_symbols)}_symbols"
                                    )
                            elif len(current_symbols) == 1:
                                logger.error(
                                    "SINGLE SYMBOL %s still silent in %s mode=%s — "
                                    "likely IP blocking or endpoint issue",
                                    current_symbols[0], shard_label, _current_mode(),
                                )
                                self._diag_bucket(venue, shard_key)["suggested_action"] = (
                                    "single_symbol_silent_check_ip_or_endpoint"
                                )

                except Exception as exc:
                    close_reason = f"websocket_error:{type(exc).__name__}"
                    self._mark_exception(venue, shard_key, exc)
                    logger.warning("Trade websocket error for %s: %s", shard_label, exc)
                finally:
                    self._venue_diag[venue]["reconnect_count"] += 1
                    self._diag_bucket(venue, shard_key)["reconnect_count"] += 1
                    for symbol in current_symbols:
                        state = self._state_for(venue, symbol)
                        await self._finalize_symbol_session(state, reason=close_reason)
                    self._log_venue_diag_summary(venue, close_reason=close_reason)
                    self._log_venue_diag_summary(venue, close_reason=close_reason, shard_key=shard_key)

                if self.shutdown_event.is_set():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
        finally:
            self._mark_task_done(
                venue,
                shard_key,
                cancelled=asyncio.current_task().cancelled() if asyncio.current_task() else False,
            )

    # ------------------------------------------------------------------
    # WS message handling
    # ------------------------------------------------------------------

    async def _handle_ws_text(
        self,
        venue: str,
        payload_text: str,
        *,
        shard_key: Optional[str] = None,
    ) -> None:
        self._diag_bucket(venue)["ws_message_count"] += 1
        if shard_key is not None:
            self._diag_bucket(venue, shard_key)["ws_message_count"] += 1
        try:
            message = json.loads(payload_text)
        except json.JSONDecodeError:
            self._record_skip(venue, "malformed_json", shard_key=shard_key)
            return

        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            self._record_skip(venue, "missing_combined_data", shard_key=shard_key)
            return

        symbol = data.get("s")
        if not symbol:
            self._record_skip(venue, "missing_symbol", shard_key=shard_key)
            return

        shard_key = shard_key or self._symbol_to_shard_key.get((venue, symbol))
        self._record_sample_payload_shape(venue, message, data, shard_key=shard_key)

        try:
            state = self._state_for(venue, symbol)
            ts_recv_ns = time.time_ns()

            if venue == "BINANCE_USDTF":
                current_mode = self._futures_trade_mode.get(venue, "aggTrade")
                if current_mode == "aggTrade":
                    # aggTrade payload must include aggregate trade id `a`.
                    if data.get("a") is None:
                        self._record_skip(venue, "missing_futures_agg_trade_id", shard_key=shard_key)
                        return
                else:
                    # @trade payload must include trade id `t`.
                    if data.get("t") is None:
                        self._record_skip(venue, "missing_futures_trade_id", shard_key=shard_key)
                        return
                record = self._build_futures_trade(state, data, ts_recv_ns)
            else:
                # Spot trade payload must include trade id `t`.
                if data.get("t") is None:
                    self._record_skip(venue, "missing_spot_trade_id", shard_key=shard_key)
                    return
                record = self._build_spot_trade(state, data, ts_recv_ns)

            # Diagnostic: validate exchange trade-id monotonicity
            exchange_trade_id = record.get("exchange_trade_id")
            if exchange_trade_id is not None and state.last_exchange_trade_id is not None:
                if exchange_trade_id <= state.last_exchange_trade_id:
                    state.trade_id_regressions += 1
                    logger.warning(
                        "Trade-id regression %s/%s: %d <= %d (regressions=%d)",
                        venue,
                        symbol,
                        exchange_trade_id,
                        state.last_exchange_trade_id,
                        state.trade_id_regressions,
                    )
            if exchange_trade_id is not None:
                state.last_exchange_trade_id = exchange_trade_id

            # Commit the canonical trade record
            trade_session_seq = state.allocate_trade_session_seq()
            record["trade_session_seq"] = trade_session_seq

            await self.storage_manager.write_record(
                venue, symbol, TRADE_V2_CHANNEL, record,
            )
            state.session_trade_count += 1
            state.total_trade_count += 1

            self.health_monitor.record_message(
                venue=venue,
                symbol=symbol,
                ts_event=record.get("ts_event_ms"),
                channel=TRADE_V2_CHANNEL,
            )
            self._diag_bucket(venue)["parsed_trade_count"] += 1
            venue_symbol_counts = self._diag_bucket(venue)["per_symbol_parsed_trade_count"]
            venue_symbol_counts[symbol] = int(venue_symbol_counts.get(symbol, 0)) + 1
            if shard_key is not None:
                self._diag_bucket(venue, shard_key)["parsed_trade_count"] += 1
                shard_symbol_counts = self._diag_bucket(venue, shard_key)[
                    "per_symbol_parsed_trade_count"
                ]
                shard_symbol_counts[symbol] = int(shard_symbol_counts.get(symbol, 0)) + 1
        except Exception:
            self._record_skip(venue, "handler_exception", shard_key=shard_key)
            logger.exception("Trade message handling error for %s/%s", venue, symbol)

    def _build_spot_trade(
        self,
        state: TradeSymbolState,
        data: dict,
        ts_recv_ns: int,
    ) -> dict:
        """Build a canonical spot trade record from Binance @trade payload."""
        return {
            "schema_version": 2,
            "record_type": "trade",
            "venue": state.venue,
            "market_type": "spot",
            "symbol": state.symbol,
            "channel": TRADE_V2_CHANNEL,
            "trade_stream_session_id": state.trade_stream_session_id,
            # trade_session_seq assigned after building
            "ts_recv_ns": ts_recv_ns,
            "ts_event_ms": data.get("E"),          # event time
            "ts_trade_ms": data.get("T"),           # trade time
            "price": data.get("p"),
            "quantity": data.get("q"),
            "is_buyer_maker": data.get("m", False),
            "exchange_trade_id": data.get("t"),
            # Spot-specific fields
            "best_match_flag": data.get("M"),
            "buyer_order_id": data.get("b"),
            "seller_order_id": data.get("a"),
            "native_payload": data,
        }

    def _build_futures_trade(
        self,
        state: TradeSymbolState,
        data: dict,
        ts_recv_ns: int,
    ) -> dict:
        """Build a canonical futures trade record from Binance @aggTrade payload."""
        return {
            "schema_version": 2,
            "record_type": "trade",
            "venue": state.venue,
            "market_type": "futures",
            "symbol": state.symbol,
            "channel": TRADE_V2_CHANNEL,
            "trade_stream_session_id": state.trade_stream_session_id,
            # trade_session_seq assigned after building
            "ts_recv_ns": ts_recv_ns,
            "ts_event_ms": data.get("E"),          # event time
            "ts_trade_ms": data.get("T"),           # trade time
            "price": data.get("p"),
            "quantity": data.get("q"),
            "is_buyer_maker": data.get("m", False),
            "exchange_trade_id": data.get("a"),     # aggregate trade ID
            # Futures-specific fields
            "first_trade_id": data.get("f"),
            "last_trade_id": data.get("l"),
            "native_payload": data,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _state_for(self, venue: str, symbol: str) -> TradeSymbolState:
        key = (venue, symbol)
        if key not in self._states:
            market_type = "futures" if venue == "BINANCE_USDTF" else "spot"
            self._states[key] = TradeSymbolState(
                venue=venue,
                symbol=symbol,
                market_type=market_type,
            )
        return self._states[key]

    async def _emit_trade_lifecycle(
        self,
        state: TradeSymbolState,
        *,
        event: str,
        reason: str,
    ) -> None:
        """Emit a lifecycle marker.  Does NOT consume trade_session_seq."""
        record = {
            "schema_version": 2,
            "record_type": "trade_stream_lifecycle",
            "venue": state.venue,
            "market_type": state.market_type,
            "symbol": state.symbol,
            "channel": TRADE_V2_CHANNEL,
            "trade_stream_session_id": state.trade_stream_session_id,
            "ts_recv_ns": time.time_ns(),
            "event": event,
            "reason": reason,
        }
        await self.storage_manager.write_record(
            state.venue, state.symbol, TRADE_V2_CHANNEL, record,
        )
