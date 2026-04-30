"""Tests for trade health integration in heartbeat."""
from __future__ import annotations

import asyncio
import importlib
import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

import native_trades as native_trades_mod
from native_trades import BinanceNativeTradeRecorder


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


def _patch_state_root(tmpdir: Path):
    """Patch STATE_ROOT for testing."""
    import config

    original_state_root = config.STATE_ROOT
    config.STATE_ROOT = tmpdir / "state"
    config.STATE_ROOT.mkdir(parents=True, exist_ok=True)

    import health_monitor

    importlib.reload(health_monitor)
    return config, health_monitor, original_state_root


def test_heartbeat_includes_empty_trade_health_by_default() -> None:
    """Test that heartbeat includes trade_health dict even if empty."""
    tmpdir = Path(tempfile.mkdtemp(prefix="trade_health_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.set_startup_coverage({
                "spot": {"requested_count": 1},
                "futures": {"requested_count": 1},
            })
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1)

            asyncio.run(hm.write_heartbeat())

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            assert "trade_health" in hb
            assert hb["trade_health"] == {}  # Empty by default
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_heartbeat_includes_trade_health_when_set() -> None:
    """Test that heartbeat includes trade_health diagnostics when set."""
    tmpdir = Path(tempfile.mkdtemp(prefix="trade_health_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.set_startup_coverage({
                "spot": {"requested_count": 1},
                "futures": {"requested_count": 1},
            })
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1)

            # Set trade health diagnostics
            trade_diagnostics = {
                "BINANCE_SPOT": {
                    "ws_message_count": 100,
                    "parsed_trade_count": 95,
                    "skipped_message_count": 5,
                    "skip_reasons": {"invalid_field": 5},
                    "lifecycle_only_sessions": 0,
                    "reconnect_count": 2,
                    "last_close_reason": "normal_close",
                    "sample_payload_shape": None,
                    "shards": {},
                },
                "BINANCE_USDTF": {
                    "ws_message_count": 200,
                    "parsed_trade_count": 190,
                    "skipped_message_count": 10,
                    "skip_reasons": {"missing_field": 10},
                    "lifecycle_only_sessions": 1,
                    "reconnect_count": 1,
                    "last_close_reason": None,
                    "sample_payload_shape": None,
                    "shards": {},
                },
            }
            hm.set_trade_health(trade_diagnostics)

            asyncio.run(hm.write_heartbeat())

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            assert "trade_health" in hb
            assert hb["trade_health"]["BINANCE_SPOT"]["ws_message_count"] == 100
            assert hb["trade_health"]["BINANCE_SPOT"]["parsed_trade_count"] == 95
            assert hb["trade_health"]["BINANCE_SPOT"]["skipped_message_count"] == 5
            assert hb["trade_health"]["BINANCE_SPOT"]["skip_reasons"] == {"invalid_field": 5}
            assert hb["trade_health"]["BINANCE_SPOT"]["reconnect_count"] == 2
            
            assert hb["trade_health"]["BINANCE_USDTF"]["ws_message_count"] == 200
            assert hb["trade_health"]["BINANCE_USDTF"]["parsed_trade_count"] == 190
            assert hb["trade_health"]["BINANCE_USDTF"]["lifecycle_only_sessions"] == 1
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_heartbeat_trade_health_includes_shards() -> None:
    """Test that trade_health includes shard diagnostics."""
    tmpdir = Path(tempfile.mkdtemp(prefix="trade_health_shards_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.set_startup_coverage({
                "spot": {"requested_count": 1},
                "futures": {"requested_count": 1},
            })
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1)

            # Set trade health with shard diagnostics
            trade_diagnostics = {
                "BINANCE_SPOT": {
                    "ws_message_count": 100,
                    "parsed_trade_count": 95,
                    "skipped_message_count": 5,
                    "skip_reasons": {},
                    "lifecycle_only_sessions": 0,
                    "reconnect_count": 0,
                    "last_close_reason": None,
                    "sample_payload_shape": None,
                    "shards": {
                        "shard_1_of_2": {
                            "ws_message_count": 50,
                            "parsed_trade_count": 48,
                            "skipped_message_count": 2,
                            "skip_reasons": {},
                            "lifecycle_only_sessions": 0,
                            "reconnect_count": 0,
                            "last_close_reason": None,
                            "sample_payload_shape": None,
                        },
                        "shard_2_of_2": {
                            "ws_message_count": 50,
                            "parsed_trade_count": 47,
                            "skipped_message_count": 3,
                            "skip_reasons": {},
                            "lifecycle_only_sessions": 0,
                            "reconnect_count": 0,
                            "last_close_reason": None,
                            "sample_payload_shape": None,
                        },
                    },
                },
            }
            hm.set_trade_health(trade_diagnostics)

            asyncio.run(hm.write_heartbeat())

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            assert "shards" in hb["trade_health"]["BINANCE_SPOT"]
            assert "shard_1_of_2" in hb["trade_health"]["BINANCE_SPOT"]["shards"]
            assert "shard_2_of_2" in hb["trade_health"]["BINANCE_SPOT"]["shards"]
            assert hb["trade_health"]["BINANCE_SPOT"]["shards"]["shard_1_of_2"]["parsed_trade_count"] == 48
            assert hb["trade_health"]["BINANCE_SPOT"]["shards"]["shard_2_of_2"]["parsed_trade_count"] == 47
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_runtime_trade_health_refresh_populates_heartbeat() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="trade_health_runtime_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            import recorder

            hm = health_monitor.HealthMonitor()
            hm.set_startup_coverage({
                "futures": {
                    "venue": "BINANCE_USDTF",
                    "requested_count": 2,
                    "requested_raw": ["BTCUSDT", "ETHUSDT"],
                },
                "spot": {"venue": "BINANCE_SPOT", "requested_count": 0, "requested_raw": []},
            })

            class _FakeTradeRecorder:
                def get_venue_diagnostics(self):
                    return {
                        "BINANCE_USDTF": {
                            "ws_message_count": 10,
                            "parsed_trade_count": 7,
                            "skipped_message_count": 0,
                            "skip_reasons": {},
                            "lifecycle_only_sessions": 0,
                            "reconnect_count": 0,
                            "last_close_reason": None,
                            "sample_payload_shape": {"stream_event": "aggTrade"},
                            "subscribed_symbols": ["BTCUSDT", "ETHUSDT"],
                            "subscribed_symbol_count": 2,
                            "per_symbol_parsed_trade_count": {"BTCUSDT": 4, "ETHUSDT": 3},
                            "shards": {},
                        }
                    }

            original_hm = recorder.health_monitor
            original_trade_recorder = recorder.native_trade_recorder
            recorder.health_monitor = hm
            recorder.native_trade_recorder = _FakeTradeRecorder()
            try:
                recorder._refresh_trade_health_from_recorder()
                asyncio.run(hm.write_heartbeat())
            finally:
                recorder.health_monitor = original_hm
                recorder.native_trade_recorder = original_trade_recorder

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            futures_health = hb["trade_health"]["BINANCE_USDTF"]
            assert futures_health["parsed_trade_count"] == 7
            assert {"BTCUSDT", "ETHUSDT"}.issubset(
                set(futures_health["subscribed_symbols"])
            )
            assert futures_health["per_symbol_parsed_trade_count"]["BTCUSDT"] == 4
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_high_liquidity_zero_futures_trades_warns_without_l2_failure() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="trade_health_warning_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.start_time -= 400
            hm.set_startup_coverage({
                "futures": {
                    "venue": "BINANCE_USDTF",
                    "requested_count": 2,
                    "requested_raw": ["BTCUSDT", "QUIETUSDT"],
                },
                "spot": {"venue": "BINANCE_SPOT", "requested_count": 0, "requested_raw": []},
            })
            hm.record_message("BINANCE_USDTF", "BTCUSDT", ts_event=1, channel="depth_v2")
            hm.record_message("BINANCE_USDTF", "QUIETUSDT", ts_event=1, channel="depth_v2")
            hm.set_trade_health({
                "BINANCE_USDTF": {
                    "ws_message_count": 2,
                    "parsed_trade_count": 0,
                    "skipped_message_count": 0,
                    "skip_reasons": {},
                    "lifecycle_only_sessions": 0,
                    "reconnect_count": 0,
                    "last_close_reason": None,
                    "sample_payload_shape": None,
                    "subscribed_symbols": ["BTCUSDT", "QUIETUSDT"],
                    "subscribed_symbol_count": 2,
                    "per_symbol_parsed_trade_count": {},
                    "shards": {
                        "shard_1_of_1": {
                            "subscribed_symbols": ["BTCUSDT", "QUIETUSDT"],
                            "subscribed_symbol_count": 2,
                        }
                    },
                }
            })

            asyncio.run(hm.write_heartbeat())

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            warnings = hb["trade_health"]["BINANCE_USDTF"]["warnings"]
            assert [warning["symbol"] for warning in warnings] == ["BTCUSDT"]
            assert warnings[0]["l2_failure"] is False
            assert warnings[0]["reason"] == "high_liquidity_futures_zero_trades_with_active_depth"
            assert "QUIETUSDT" not in {warning["symbol"] for warning in warnings}
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_universe_health_checkpoint_shape_and_zero_message_candidate() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="universe_health_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.start_time -= 400
            hm.set_startup_coverage({
                "spot": {
                    "venue": "BINANCE_SPOT",
                    "requested_count": 2,
                    "requested_raw": ["BTCUSDT", "UTKUSDT"],
                },
                "futures": {"venue": "BINANCE_USDTF", "requested_count": 0, "requested_raw": []},
            })
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1, channel="depth_v2")
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1, channel="trade_v2")
            hm.record_phase2_symbol_state(
                venue="BINANCE_SPOT",
                symbol="BTCUSDT",
                sync_state="live_synced",
                last_update_id=1,
                prev_update_id=1,
                snapshot_seed_count=1,
            )

            hm.write_universe_health_checkpoint(force=True)

            path = tmpdir / "state" / "universe_health" / f"{health_monitor.datetime_date_str()}.json"
            summary = json.loads(path.read_text())
            btc = summary["symbols"]["BINANCE_SPOT"]["BTCUSDT"]
            utk = summary["symbols"]["BINANCE_SPOT"]["UTKUSDT"]
            assert btc["suggested_action"] == "keep"
            assert btc["depth_live_synced_ever"] is True
            assert utk["depth_message_count"] == 0
            assert utk["trade_message_count"] == 0
            assert utk["suggested_action"] == "temporary_exclude_candidate"
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_zero_message_symbol_is_watch_before_observation_threshold() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="universe_health_watch_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.start_time -= 30
            hm.set_startup_coverage({
                "spot": {
                    "venue": "BINANCE_SPOT",
                    "requested_count": 1,
                    "requested_raw": ["UTKUSDT"],
                },
                "futures": {"venue": "BINANCE_USDTF", "requested_count": 0, "requested_raw": []},
            })

            summary = hm.build_universe_health_summary()
            utk = summary["symbols"]["BINANCE_SPOT"]["UTKUSDT"]
            assert utk["suggested_action"] == "watch"
        finally:
            config.STATE_ROOT = original_state_root
            importlib.reload(health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.asyncio
async def test_futures_parsed_trade_count_increments() -> None:
    """Test that parsed_trade_count increments after a valid aggTrade message."""
    recorder = BinanceNativeTradeRecorder(
        storage_manager=AsyncMock(),
        health_monitor=MagicMock(),
        shutdown_event=asyncio.Event(),
    )
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    # Record initial counts
    initial_count = recorder._diag_bucket("BINANCE_USDTF").get("parsed_trade_count", 0)

    # Send a valid aggTrade message
    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "a": 12345,  # aggregate trade ID
            "p": "70000.01",
            "q": "0.010",
            "f": 100,  # first trade ID
            "l": 110,  # last trade ID
            "T": 1710000000001,
            "m": False,
        },
    }

    import json
    await recorder._handle_ws_text("BINANCE_USDTF", json.dumps(msg))

    # Verify parsed_trade_count incremented
    final_count = recorder._diag_bucket("BINANCE_USDTF").get("parsed_trade_count", 0)
    assert final_count == initial_count + 1


@pytest.mark.asyncio
async def test_futures_skipped_message_increments_skip_reasons() -> None:
    """Test that skipped messages increment skip_reasons."""
    recorder = BinanceNativeTradeRecorder(
        storage_manager=AsyncMock(),
        health_monitor=MagicMock(),
        shutdown_event=asyncio.Event(),
    )
    state = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state.new_stream_session()

    # Record initial skip reason counts
    diag = recorder._diag_bucket("BINANCE_USDTF")
    initial_skipped = diag.get("skipped_message_count", 0)
    initial_reasons = dict(diag.get("skip_reasons", {}))

    # Manually record a skip (simulating an invalid message)
    recorder._record_skip("BINANCE_USDTF", "missing_required_field")

    # Verify counts incremented
    diag = recorder._diag_bucket("BINANCE_USDTF")
    assert diag.get("skipped_message_count", 0) == initial_skipped + 1
    assert diag.get("skip_reasons", {}).get("missing_required_field", 0) == \
           initial_reasons.get("missing_required_field", 0) + 1


@pytest.mark.asyncio
async def test_trade_diagnostics_remain_json_serializable() -> None:
    """Test that trade diagnostics remain JSON-serializable."""
    recorder = BinanceNativeTradeRecorder(
        storage_manager=AsyncMock(),
        health_monitor=MagicMock(),
        shutdown_event=asyncio.Event(),
    )

    # Simulate some activity
    state_spot = recorder._state_for("BINANCE_SPOT", "BTCUSDT")
    state_spot.new_stream_session()
    state_futures = recorder._state_for("BINANCE_USDTF", "BTCUSDT")
    state_futures.new_stream_session()

    # Record some diagnostics
    diag_spot = recorder._diag_bucket("BINANCE_SPOT")
    diag_spot["ws_message_count"] = 100
    diag_spot["parsed_trade_count"] = 95
    diag_spot["skipped_message_count"] = 5
    diag_spot["skip_reasons"]["test_skip"] = 5
    diag_spot["reconnect_count"] = 2
    
    diag_futures = recorder._diag_bucket("BINANCE_USDTF")
    diag_futures["ws_message_count"] = 200
    diag_futures["parsed_trade_count"] = 190
    diag_futures["lifecycle_only_sessions"] = 1

    # Get diagnostics
    diagnostics = recorder.get_venue_diagnostics()

    # Verify it's JSON-serializable
    json_str = json.dumps(diagnostics)
    assert json_str is not None
    
    # Verify we can deserialize it back
    deserialized = json.loads(json_str)
    assert deserialized["BINANCE_SPOT"]["ws_message_count"] == 100
    assert deserialized["BINANCE_SPOT"]["parsed_trade_count"] == 95
    assert deserialized["BINANCE_USDTF"]["ws_message_count"] == 200
    assert deserialized["BINANCE_USDTF"]["lifecycle_only_sessions"] == 1
