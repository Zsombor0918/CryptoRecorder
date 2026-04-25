from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import native_trades as native_trades_mod
from native_trades import BinanceNativeTradeRecorder


def _make_recorder() -> BinanceNativeTradeRecorder:
    return BinanceNativeTradeRecorder(
        storage_manager=AsyncMock(),
        health_monitor=MagicMock(),
        shutdown_event=asyncio.Event(),
    )


def _written_record_types(recorder: BinanceNativeTradeRecorder) -> list[str]:
    types: list[str] = []
    for call in recorder.storage_manager.write_record.await_args_list:
        record = call.args[3]
        types.append(record.get("record_type"))
    return types


class _AsyncContextManager:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubWebSocket:
    def __init__(self, messages=None):
        self._messages = messages or []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        async def _gen():
            for item in self._messages:
                yield item
        return _gen()

    def exception(self):
        return None


@pytest.mark.asyncio
async def test_spot_combined_stream_trade_message_writes_trade_record() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_SPOT", "BTCUSDT")
    state.new_stream_session()

    msg = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "70000.01",
            "q": "0.010",
            "b": 10,
            "a": 11,
            "T": 1710000000001,
            "m": False,
            "M": True,
        },
    }

    await recorder._handle_ws_text("BINANCE_SPOT", json.dumps(msg))

    assert recorder.storage_manager.write_record.await_count == 1
    args = recorder.storage_manager.write_record.await_args.args
    assert args[0] == "BINANCE_SPOT"
    assert args[1] == "BTCUSDT"
    assert args[2] == "trade_v2"
    record = args[3]
    assert record["record_type"] == "trade"
    assert record["market_type"] == "spot"
    assert record["exchange_trade_id"] == 12345
    assert record["trade_session_seq"] == 1


@pytest.mark.asyncio
async def test_futures_combined_stream_aggtrade_message_writes_trade_record() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "a": 987654,
            "p": "70001.00",
            "q": "0.050",
            "f": 111,
            "l": 113,
            "T": 1710000000002,
            "m": True,
        },
    }

    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps(msg))

    assert recorder.storage_manager.write_record.await_count == 1
    args = recorder.storage_manager.write_record.await_args.args
    assert args[0] == "BINANCE_USDTF"
    assert args[1] == "BTCUSDT"
    assert args[2] == "trade_v2"
    record = args[3]
    assert record["record_type"] == "trade"
    assert record["market_type"] == "futures"
    assert record["exchange_trade_id"] == 987654
    assert record["trade_session_seq"] == 1


@pytest.mark.asyncio
async def test_futures_missing_agg_trade_id_is_skipped_and_counted() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "p": "70001.00",
            "q": "0.050",
            "f": 111,
            "l": 113,
            "T": 1710000000002,
            "m": True,
            # missing `a`
        },
    }

    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps(msg))

    assert recorder.storage_manager.write_record.await_count == 0
    diag = recorder.get_venue_diagnostics()["BINANCE_USDTF"]
    assert diag["ws_message_count"] == 1
    assert diag["parsed_trade_count"] == 0
    assert diag["skipped_message_count"] == 1
    assert diag["skip_reasons"]["missing_futures_agg_trade_id"] == 1
    assert state.next_trade_session_seq == 0


@pytest.mark.asyncio
async def test_missing_spot_trade_id_is_skipped_and_counted() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_SPOT", "BTCUSDT")
    state.new_stream_session()

    msg = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            # missing `t`
            "p": "70000.01",
            "q": "0.010",
            "T": 1710000000001,
            "m": False,
        },
    }

    await recorder._handle_ws_text("BINANCE_SPOT", json.dumps(msg))

    assert recorder.storage_manager.write_record.await_count == 0
    diag = recorder.get_venue_diagnostics()["BINANCE_SPOT"]
    assert diag["skipped_message_count"] == 1
    assert diag["skip_reasons"]["missing_spot_trade_id"] == 1
    assert state.next_trade_session_seq == 0


@pytest.mark.asyncio
async def test_malformed_json_is_counted() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    await recorder._handle_ws_text("BINANCE_USDTF", "{bad-json")

    assert recorder.storage_manager.write_record.await_count == 0
    diag = recorder.get_venue_diagnostics()["BINANCE_USDTF"]
    assert diag["skipped_message_count"] == 1
    assert diag["skip_reasons"]["malformed_json"] == 1
    assert state.next_trade_session_seq == 0


@pytest.mark.asyncio
async def test_missing_combined_data_is_counted() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    # Missing top-level combined-stream "data" dict
    msg = {"stream": "btcusdt@aggTrade", "unexpected": {"s": "BTCUSDT"}}
    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps(msg))

    assert recorder.storage_manager.write_record.await_count == 0
    diag = recorder.get_venue_diagnostics()["BINANCE_USDTF"]
    assert diag["skipped_message_count"] == 1
    assert diag["skip_reasons"]["missing_combined_data"] == 1
    assert state.next_trade_session_seq == 0


@pytest.mark.asyncio
async def test_lifecycle_only_session_is_detected_in_diagnostics() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    await recorder._emit_trade_lifecycle(
        state,
        event="session_start",
        reason="startup_or_reconnect",
    )

    await recorder._finalize_symbol_session(state, reason="websocket_closed")

    diag = recorder.get_venue_diagnostics()["BINANCE_USDTF"]
    assert diag["lifecycle_only_sessions"] == 1
    assert state.next_trade_session_seq == 0
    types = _written_record_types(recorder)
    assert types == ["trade_stream_lifecycle", "trade_stream_lifecycle"]


@pytest.mark.asyncio
async def test_diagnostics_bounded_and_raw_schema_unchanged() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    await recorder._emit_trade_lifecycle(
        state,
        event="session_start",
        reason="startup_or_reconnect",
    )

    valid_msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "a": 987654,
            "p": "70001.00",
            "q": "0.050",
            "f": 111,
            "l": 113,
            "T": 1710000000002,
            "m": True,
        },
    }
    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps(valid_msg))
    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps({"stream": "btcusdt@aggTrade"}))
    await recorder._finalize_symbol_session(state, reason="websocket_closed")

    diag = recorder.get_venue_diagnostics()["BINANCE_USDTF"]
    sample = diag["sample_payload_shape"]
    assert sample["venue"] == "BINANCE_USDTF"
    assert sample["stream"] == "btcusdt@aggTrade"
    assert sample["symbol"] == "BTCUSDT"
    assert sample["field_names"] == sorted(sample["field_names"])
    # bounded: exactly one sample shape per venue
    assert isinstance(sample, dict)

    # Raw schema unchanged: only trade + lifecycle record types written
    types = set(_written_record_types(recorder))
    assert types.issubset({"trade", "trade_stream_lifecycle"})


@pytest.mark.asyncio
async def test_trade_sharding_creates_two_tasks_for_fifty_symbols(monkeypatch) -> None:
    recorder = _make_recorder()
    symbols = [f"SYM{i:02d}USDT" for i in range(50)]
    calls: list[tuple[str, list[str], int, int]] = []

    async def _fake_run_venue_loop(venue, shard_symbols, *, shard_index=1, shard_count=1):
        calls.append((venue, list(shard_symbols), shard_index, shard_count))

    recorder._run_venue_loop = _fake_run_venue_loop  # type: ignore[method-assign]
    recorder._session = SimpleNamespace(closed=False, close=AsyncMock())

    class _FakeClientSession:
        def __init__(self, *args, **kwargs):
            self.closed = False

        async def close(self):
            self.closed = True

    monkeypatch.setattr(native_trades_mod.aiohttp, "ClientSession", _FakeClientSession)
    monkeypatch.setattr(native_trades_mod, "TRADE_WS_SHARD_ENABLED", True)
    monkeypatch.setattr(native_trades_mod, "TRADE_WS_MAX_SYMBOLS_PER_CONNECTION", 25)

    await recorder.run({"BINANCE_USDTF": symbols})

    assert len(calls) == 2
    assert all(len(chunk) == 25 for _, chunk, _, _ in calls)
    assert calls[0][2:] == (1, 2)
    assert calls[1][2:] == (2, 2)


@pytest.mark.asyncio
async def test_trade_shard_url_contains_only_shard_symbols() -> None:
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    url = native_trades_mod._ws_url("BINANCE_USDTF", symbols[:2])

    assert "btcusdt@aggTrade" in url
    assert "ethusdt@aggTrade" in url
    assert "solusdt@aggTrade" not in url


@pytest.mark.asyncio
async def test_trade_reconnect_only_finalizes_symbols_in_one_shard(monkeypatch) -> None:
    recorder = _make_recorder()
    ws_urls: list[str] = []

    class _FakeSession:
        closed = False

        def ws_connect(self, url, heartbeat):
            ws_urls.append(url)
            recorder.shutdown_event.set()
            return _StubWebSocket([])

    recorder._session = _FakeSession()
    monkeypatch.setattr(native_trades_mod.asyncio, "sleep", AsyncMock())

    await recorder._run_venue_loop(
        "BINANCE_USDTF",
        ["BTCUSDT", "ETHUSDT"],
        shard_index=1,
        shard_count=2,
    )

    written_symbols = [call.args[1] for call in recorder.storage_manager.write_record.await_args_list]
    assert ws_urls and "btcusdt@aggTrade" in ws_urls[0] and "ethusdt@aggTrade" in ws_urls[0]
    assert set(written_symbols) == {"BTCUSDT", "ETHUSDT"}
    assert "SOLUSDT" not in written_symbols


@pytest.mark.asyncio
async def test_trade_session_seq_only_consumed_by_trade_records_with_sharding() -> None:
    recorder = _make_recorder()
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    await recorder._emit_trade_lifecycle(state, event="session_start", reason="startup_or_reconnect")
    await recorder._handle_ws_text(
        "BINANCE_USDTF",
        json.dumps(
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1710000000000,
                    "s": "BTCUSDT",
                    "a": 123,
                    "p": "1.0",
                    "q": "2.0",
                    "f": 1,
                    "l": 2,
                    "T": 1710000000001,
                    "m": False,
                },
            }
        ),
        shard_key="shard_1_of_2",
    )
    await recorder._finalize_symbol_session(state, reason="websocket_closed")

    trade_records = [
        call.args[3]
        for call in recorder.storage_manager.write_record.await_args_list
        if call.args[3].get("record_type") == "trade"
    ]
    assert len(trade_records) == 1
    assert trade_records[0]["trade_session_seq"] == 1
