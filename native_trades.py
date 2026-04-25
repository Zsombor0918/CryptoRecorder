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
    TRADE_WS_MAX_SYMBOLS_PER_CONNECTION,
    TRADE_WS_SHARD_ENABLED,
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
    }


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
                    for shard_index, shard_symbols in enumerate(shards, start=1):
                        shard_key = f"shard_{shard_index}_of_{shard_count}"
                        for symbol in shard_symbols:
                            self._symbol_to_shard_key[(venue, symbol)] = shard_key
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
        while not self.shutdown_event.is_set():
            close_reason = "websocket_closed"
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
                    logger.info("Trade websocket connected %s: %s", shard_label, ws_url)
                    backoff = 1.0
                    async for msg in ws:
                        if self.shutdown_event.is_set():
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_ws_text(venue, msg.data, shard_key=shard_key)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError(
                                f"trade websocket error for {shard_label}: {ws.exception()}"
                            )
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                        ):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                close_reason = f"websocket_error:{type(exc).__name__}"
                logger.warning("Trade websocket error for %s: %s", shard_label, exc)
            finally:
                self._venue_diag[venue]["reconnect_count"] += 1
                self._diag_bucket(venue, shard_key)["reconnect_count"] += 1
                for symbol in symbols:
                    state = self._state_for(venue, symbol)
                    await self._finalize_symbol_session(state, reason=close_reason)
                self._log_venue_diag_summary(venue, close_reason=close_reason)
                self._log_venue_diag_summary(venue, close_reason=close_reason, shard_key=shard_key)

            if self.shutdown_event.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)

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
                # Futures aggTrade payload must include aggregate trade id `a`.
                if data.get("a") is None:
                    self._record_skip(venue, "missing_futures_agg_trade_id", shard_key=shard_key)
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

            self.health_monitor.record_message(
                venue=venue,
                symbol=symbol,
                ts_event=record.get("ts_event_ms"),
                channel=TRADE_V2_CHANNEL,
            )
            self._diag_bucket(venue)["parsed_trade_count"] += 1
            if shard_key is not None:
                self._diag_bucket(venue, shard_key)["parsed_trade_count"] += 1
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
