#!/usr/bin/env python3
"""
tests/test_heartbeat_coverage_fields.py — Verify top-level heartbeat venue fields.

Usage:
    python tests/test_heartbeat_coverage_fields.py
"""
from __future__ import annotations

import asyncio
import importlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def _patch_state_root(tmpdir: Path):
    import config

    config.STATE_ROOT = tmpdir / "state"
    config.STATE_ROOT.mkdir(parents=True, exist_ok=True)

    import health_monitor
    importlib.reload(health_monitor)
    return health_monitor


def run_tests() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="heartbeat_cov_"))
    try:
        health_monitor = _patch_state_root(tmpdir)
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

        asyncio.run(hm.write_heartbeat())

        hb = json.loads((tmpdir / "state" / "heartbeat.json").read_text())
        ok1 = hb["spot_symbols_active"] == 2 and hb["futures_symbols_active"] == 1
        print(
            f"  [{'PASS' if ok1 else 'FAIL'}] active_counts "
            f"(spot={hb.get('spot_symbols_active')}, futures={hb.get('futures_symbols_active')})"
        )

        ok2 = hb["spot_symbols_requested"] == 5 and hb["futures_symbols_requested"] == 4
        print(
            f"  [{'PASS' if ok2 else 'FAIL'}] requested_counts "
            f"(spot={hb.get('spot_symbols_requested')}, futures={hb.get('futures_symbols_requested')})"
        )

        ok3 = hb["spot_symbols_dropped"] == 2 and hb["futures_symbols_dropped"] == 1
        print(
            f"  [{'PASS' if ok3 else 'FAIL'}] dropped_counts "
            f"(spot={hb.get('spot_symbols_dropped')}, futures={hb.get('futures_symbols_dropped')})"
        )

        ok4 = hb["spot_symbols_dropped_list"] == ["UTKUSDT", "USDCUSDT"]
        ok4 = ok4 and hb["futures_symbols_dropped_list"] == ["AGIXUSDT"]
        print(
            f"  [{'PASS' if ok4 else 'FAIL'}] dropped_lists "
            f"(spot={hb.get('spot_symbols_dropped_list')}, futures={hb.get('futures_symbols_dropped_list')})"
        )

        ok5 = hb["spot_coverage_ratio"] == 0.4 and hb["futures_coverage_ratio"] == 0.25
        print(
            f"  [{'PASS' if ok5 else 'FAIL'}] coverage_ratio "
            f"(spot={hb.get('spot_coverage_ratio')}, futures={hb.get('futures_coverage_ratio')})"
        )

        results = [ok1, ok2, ok3, ok4, ok5]
        passed = sum(results)
        total = len(results)
        print(f"\n  {passed}/{total} passed")
        return 0 if passed == total else 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(run_tests())
