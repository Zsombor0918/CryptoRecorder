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
from collections import defaultdict, deque
from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import aiohttp

from config import (
    TRADE_V2_CHANNEL,
    WS_PING_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


SPOT_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_WS_BASE = "wss://fstream.binance.com/stream?streams="


def _stream_name_spot(symbol: str) -> str:
    return f"{symbol.lower()}@trade"


def _stream_name_futures(symbol: str) -> str:
    return f"{symbol.lower()}@aggTrade"


def _ws_url(venue: str, symbols: Iterable[str]) -> str:
    if venue == "BINANCE_USDTF":
        base = FUTURES_WS_BASE
        streams = "/".join(_stream_name_futures(s) for s in symbols)
    else:
        base = SPOT_WS_BASE
        streams = "/".join(_stream_name_spot(s) for s in symbols)
    return f"{base}{streams}"


@dataclass
class TradeSymbolState:
    venue: str
    symbol: str
    market_type: str = ""  # "spot" or "futures"
    trade_stream_session_id: int = 0
    next_trade_session_seq: int = 0
    last_exchange_trade_id: Optional[int] = None
    trade_id_regressions: int = 0

    def allocate_trade_session_seq(self) -> int:
        """Monotonic counter for committed canonical trade records only."""
        self.next_trade_session_seq += 1
        return self.next_trade_session_seq

    def new_stream_session(self) -> None:
        self.trade_stream_session_id += 1
        self.next_trade_session_seq = 0
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
                await self._emit_trade_lifecycle(
                    state,
                    event="session_start",
                    reason="startup_or_reconnect",
                )

            try:
                ws_url = _ws_url(venue, symbols)
                assert self._session is not None
                async with self._session.ws_connect(
                    ws_url,
                    heartbeat=WS_PING_INTERVAL_SEC,
                ) as ws:
                    logger.info("Trade websocket connected: %s", ws_url)
                    backoff = 1.0
                    async for msg in ws:
                        if self.shutdown_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_ws_text(venue, msg.data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(
                                f"trade websocket error for {venue}: {ws.exception()}"
                            )
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                        ):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Trade websocket error for %s: %s", venue, exc)
            finally:
                for symbol in symbols:
                    state = self._state_for(venue, symbol)
                    await self._emit_trade_lifecycle(
                        state,
                        event="session_end",
                        reason="websocket_closed",
                    )

            if self.shutdown_event.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

    # ------------------------------------------------------------------
    # WS message handling
    # ------------------------------------------------------------------

    async def _handle_ws_text(self, venue: str, payload_text: str) -> None:
        try:
            message = json.loads(payload_text)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed trade frame for %s", venue)
            return

        data = message.get("data") if isinstance(message, dict) else None
        if not isinstance(data, dict):
            return

        symbol = data.get("s")
        if not symbol:
            return

        state = self._state_for(venue, symbol)
        ts_recv_ns = time.time_ns()

        if venue == "BINANCE_USDTF":
            record = self._build_futures_trade(state, data, ts_recv_ns)
        else:
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

        self.health_monitor.record_message(
            venue=venue,
            symbol=symbol,
            ts_event=record.get("ts_event_ms"),
            channel=TRADE_V2_CHANNEL,
        )

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
