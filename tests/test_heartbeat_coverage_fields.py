from __future__ import annotations

import asyncio
import importlib
import json
import shutil
import tempfile
from datetime import datetime
from pathlib import Path


def _patch_state_root(tmpdir: Path):
    import config

    original_state_root = config.STATE_ROOT
    config.STATE_ROOT = tmpdir / "state"
    config.STATE_ROOT.mkdir(parents=True, exist_ok=True)

    import health_monitor

    importlib.reload(health_monitor)
    return config, health_monitor, original_state_root


def test_heartbeat_coverage_fields_are_exposed_consistently() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="heartbeat_cov_"))
    try:
        config, health_monitor, original_state_root = _patch_state_root(tmpdir)
        try:
            hm = health_monitor.HealthMonitor()
            hm.set_startup_coverage({
                "spot": {
                    "requested_count": 5,
                    "runtime_dropped_raw": ["UTKUSDT", "USDCUSDT"],
                },
                "futures": {
                    "requested_count": 4,
                    "runtime_dropped_raw": ["AGIXUSDT"],
                },
            })
            hm.record_message("BINANCE_SPOT", "BTCUSDT", ts_event=1)
            hm.record_message("BINANCE_SPOT", "ETHUSDT", ts_event=2)
            hm.record_message("BINANCE_USDTF", "BTCUSDT", ts_event=3)
            hm.set_writer_queue_telemetry({
                "writers": {
                    "BINANCE_SPOT:BTCUSDT:depth_v2": {
                        "venue": "BINANCE_SPOT",
                        "symbol": "BTCUSDT",
                        "channel": "depth_v2",
                        "queue_size": 3,
                        "queue_max_size": 20000,
                        "queue_high_watermark": 4,
                        "drop_count": 0,
                        "enqueued_count": 7,
                        "write_count": 4,
                        "blocked": False,
                        "current_block_sec": 0.0,
                        "max_block_sec": 0.0,
                    },
                },
                "totals": {
                    "writer_count": 1,
                    "queued_records": 3,
                    "total_drops": 0,
                    "depth_blocked_writer_count": 0,
                },
                "top_pressure_writers": [],
                "compression": {
                    "queued": 0,
                    "active": 0,
                    "completed": 0,
                    "failed": 0,
                    "last_error": None,
                    "worker_count": 1,
                },
            })

            asyncio.run(hm.write_heartbeat())

            hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
            assert hb["spot_symbols_active"] == 2
            assert hb["futures_symbols_active"] == 1
            assert hb["spot_symbols_requested"] == 5
            assert hb["futures_symbols_requested"] == 4
            assert hb["spot_symbols_dropped"] == 2
            assert hb["futures_symbols_dropped"] == 1
            assert hb["spot_symbols_dropped_list"] == ["UTKUSDT", "USDCUSDT"]
            assert hb["futures_symbols_dropped_list"] == ["AGIXUSDT"]
            assert hb["spot_coverage_ratio"] == 0.4
            assert hb["futures_coverage_ratio"] == 0.25
            assert hb["writer_queue_telemetry"]["totals"]["queued_records"] == 3
            writer_tel = hb["writer_queue_telemetry"]["writers"]["BINANCE_SPOT:BTCUSDT:depth_v2"]
            assert writer_tel["queue_high_watermark"] == 4
            assert hb["writer_queue_telemetry"]["compression"]["failed"] == 0

            hb_ts = datetime.fromisoformat(hb["timestamp"])
            sym_ts = datetime.fromisoformat(
                hb["by_venue"]["BINANCE_SPOT"][0]["last_heartbeat"]
            )
            allowed_offsets = {3600.0, 7200.0}
            assert hb_ts.utcoffset() is not None
            assert hb_ts.utcoffset().total_seconds() in allowed_offsets
            assert sym_ts.utcoffset() is not None
            assert sym_ts.utcoffset().total_seconds() in allowed_offsets
        finally:
            config.STATE_ROOT = original_state_root
            import health_monitor as restored_health_monitor

            importlib.reload(restored_health_monitor)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
