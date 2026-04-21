#!/usr/bin/env python3
"""
acceptance_test.py — Full acceptance test suite.

This script runs comprehensive tests to verify the complete pipeline:
  1. Recorder can run with 50 symbols for 10 minutes
  2. Converter can process data into Nautilus catalog
  3. Catalog is queryable and valid

Usage:
    python scripts/acceptance_test.py
    python scripts/acceptance_test.py --skip-recorder  # Skip recorder, test converter only
    python scripts/acceptance_test.py --runtime 300    # 5 minutes instead of 10

Exit codes:
    0 = All tests passed
    1 = Some tests failed
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_ROOT, STATE_ROOT, NAUTILUS_CATALOG_ROOT

DEFAULT_RUNTIME_SEC = 600  # 10 minutes
TARGET_SYMBOLS = 50


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


class AcceptanceTest:
    """Full pipeline acceptance test."""

    def __init__(
        self,
        runtime_sec: int = DEFAULT_RUNTIME_SEC,
        skip_recorder: bool = False,
        depth_mode: str = "phase1",
        emit_phase2_depth10: bool = False,
    ):
        self.runtime_sec = runtime_sec
        self.skip_recorder = skip_recorder
        self.depth_mode = depth_mode
        self.emit_phase2_depth10 = emit_phase2_depth10
        self.results: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "runtime_sec": runtime_sec,
            "depth_mode": depth_mode,
            "tests": {},
            "passed": False,
        }
        self._t0 = 0.0

    def run(self) -> Dict[str, Any]:
        """Run the full acceptance test."""
        print(f"\n{Colors.BOLD}{'═' * 60}{Colors.RESET}")
        print(f"{Colors.BOLD}  Full Acceptance Test{Colors.RESET}")
        print(f"{Colors.BOLD}{'═' * 60}{Colors.RESET}\n")

        tests = []

        if not self.skip_recorder:
            tests.append(("1. Recorder Test", self._test_recorder))

        tests.append(("2. Converter Test", self._test_converter))
        tests.append(("3. Catalog Test", self._test_catalog))

        all_passed = True
        for name, test_func in tests:
            print(f"\n{Colors.BOLD}{name}{Colors.RESET}")
            print("-" * 40)
            try:
                ok, details = test_func()
            except Exception as e:
                ok, details = False, {"error": str(e)}

            status = f"{Colors.GREEN}PASS{Colors.RESET}" if ok else f"{Colors.RED}FAIL{Colors.RESET}"
            print(f"\n  Result: {status}")

            self.results["tests"][name] = {"passed": ok, "details": details}
            all_passed &= ok

        self.results["passed"] = all_passed

        # Summary
        print(f"\n{Colors.BOLD}{'═' * 60}{Colors.RESET}")
        if all_passed:
            print(f"{Colors.GREEN}✓ All acceptance tests PASSED{Colors.RESET}")
        else:
            print(f"{Colors.RED}✗ Some acceptance tests FAILED{Colors.RESET}")
        print(f"{Colors.BOLD}{'═' * 60}{Colors.RESET}\n")

        # Save results
        results_path = STATE_ROOT / "acceptance_test_results.json"
        results_path.parent.mkdir(parents=True, exist_ok=True)
        results_path.write_text(json.dumps(self.results, indent=2, default=str))
        print(f"Results saved to: {results_path}")

        return self.results

    def _test_recorder(self) -> tuple[bool, dict]:
        """Test recorder can run with target symbols."""
        print(f"  Starting recorder with {TARGET_SYMBOLS} symbols for {self.runtime_sec}s...")

        venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
        py = str(venv_py) if venv_py.exists() else sys.executable

        self._t0 = time.time()
        env = os.environ.copy()
        env["CRYPTO_RECORDER_TOP_SYMBOLS"] = str(TARGET_SYMBOLS)
        channel = "depth_v2" if self.depth_mode == "phase2" else "depth"

        proc = subprocess.Popen(
            [py, str(PROJECT_ROOT / "recorder.py"), "--depth-mode", self.depth_mode],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            cwd=str(PROJECT_ROOT),
        )
        print(f"  PID: {proc.pid}")

        try:
            stdout, _ = proc.communicate(timeout=self.runtime_sec)
        except subprocess.TimeoutExpired:
            proc.send_signal(signal.SIGINT)
            try:
                stdout, _ = proc.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                proc.terminate()
                stdout, _ = proc.communicate(timeout=15)

        log = (stdout or b"").decode("utf-8", errors="replace")

        # Save log
        log_path = STATE_ROOT / "acceptance_recorder.log"
        log_path.write_text(log)

        # Check results
        details = {}

        # Count symbols with data
        spot_syms = self._count_symbols_with_data("BINANCE_SPOT", channel)
        fut_syms = self._count_symbols_with_data("BINANCE_USDTF", channel)
        details["spot_symbols_with_data"] = spot_syms
        details["futures_symbols_with_data"] = fut_syms
        details["depth_channel"] = channel

        # Check heartbeat
        hb = STATE_ROOT / "heartbeat.json"
        if hb.exists():
            hb_data = json.loads(hb.read_text())
            details["total_messages"] = hb_data.get("total_messages", 0)
            details["futures_enabled"] = hb_data.get("futures_enabled", False)
            details["phase"] = hb_data.get("phase")

        # Check for rate limits
        rate_limits = len(re.findall(r"429|418", log, re.I))
        details["rate_limit_hits"] = rate_limits

        # Pass criteria
        ok = (
            spot_syms >= 10
            and details.get("total_messages", 0) > 0
            and rate_limits == 0
        )

        for key, value in details.items():
            print(f"    {key}: {value}")

        return ok, details

    def _test_converter(self) -> tuple[bool, dict]:
        """Test converter can process data."""
        # Find a date with data
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

        # Check if we have data for yesterday, or use today
        date_dirs = self._find_dates_with_data()
        if date_str not in date_dirs and date_dirs:
            date_str = date_dirs[0]

        print(f"  Converting date: {date_str}")

        venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
        py = str(venv_py) if venv_py.exists() else sys.executable

        result = subprocess.run(
            [
                py,
                str(PROJECT_ROOT / "convert_day.py"),
                "--date",
                date_str,
                "--depth-mode",
                self.depth_mode,
                *(
                    ["--emit-phase2-depth10"]
                    if self.depth_mode == "phase2" and self.emit_phase2_depth10
                    else []
                ),
            ],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=300,
        )

        details = {
            "date": date_str,
            "exit_code": result.returncode,
        }

        # Check report
        report_path = STATE_ROOT / "convert_reports" / f"{date_str}.json"
        if report_path.exists():
            report = json.loads(report_path.read_text())
            details["phase"] = report.get("phase", "phase1")
            details["instruments_written"] = report.get("instruments_written", 0)
            details["trades_written"] = report.get("total_trades_written", 0)
            details["depth_written"] = report.get("total_depth_snapshots_written", 0)
            details["delta_written"] = report.get("total_order_book_deltas_written", 0)
            details["crossed_book_events"] = report.get("crossed_book_events_total", 0)
            details["fenced_ranges_total"] = report.get("fenced_ranges_total", 0)

        for key, value in details.items():
            print(f"    {key}: {value}")

        ok = (
            result.returncode == 0
            and details.get("instruments_written", 0) > 0
        )

        return ok, details

    def _test_catalog(self) -> tuple[bool, dict]:
        """Test catalog is queryable."""
        details = {}

        if not NAUTILUS_CATALOG_ROOT.exists():
            print("    Catalog doesn't exist")
            return False, {"error": "Catalog doesn't exist"}

        try:
            from nautilus_trader.persistence.catalog import ParquetDataCatalog

            catalog = ParquetDataCatalog(str(NAUTILUS_CATALOG_ROOT))
            instruments = catalog.instruments()
            details["instruments"] = len(instruments)

            # Try querying data
            if instruments:
                sample = instruments[0]
                try:
                    trades = catalog.trade_ticks(instrument_ids=[sample.id])
                    details["sample_trades"] = len(trades)
                except Exception:
                    details["sample_trades"] = 0

                try:
                    depth = catalog.order_book_depth10(instrument_ids=[sample.id])
                    details["sample_depth"] = len(depth)
                except Exception:
                    details["sample_depth"] = 0
                try:
                    deltas = catalog.order_book_deltas(
                        instrument_ids=[sample.id],
                        batched=True,
                    )
                    details["sample_deltas"] = len(deltas)
                except Exception:
                    details["sample_deltas"] = 0

        except Exception as e:
            details["error"] = str(e)
            print(f"    Error: {e}")
            return False, details

        for key, value in details.items():
            print(f"    {key}: {value}")

        ok = details.get("instruments", 0) > 0
        return ok, details

    def _count_symbols_with_data(self, venue: str, channel: str) -> int:
        """Count symbols with data files created after t0."""
        venue_dir = DATA_ROOT / venue / channel
        if not venue_dir.exists():
            return 0

        symbols: Set[str] = set()
        for sym_dir in venue_dir.iterdir():
            if not sym_dir.is_dir():
                continue
            for f in sym_dir.rglob("*.jsonl*"):
                if f.stat().st_mtime >= self._t0:
                    symbols.add(sym_dir.name)
                    break
        return len(symbols)

    def _find_dates_with_data(self) -> List[str]:
        """Find dates with raw data."""
        dates: Set[str] = set()
        if not DATA_ROOT.exists():
            return []

        for venue_dir in DATA_ROOT.iterdir():
            if not venue_dir.is_dir():
                continue
            for ch_dir in venue_dir.iterdir():
                if not ch_dir.is_dir():
                    continue
                for sym_dir in ch_dir.iterdir():
                    if not sym_dir.is_dir():
                        continue
                    for d in sym_dir.iterdir():
                        if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                            dates.add(d.name)

        return sorted(dates, reverse=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Full acceptance test")
    parser.add_argument(
        "--runtime",
        type=int,
        default=DEFAULT_RUNTIME_SEC,
        help=f"Recorder runtime in seconds (default: {DEFAULT_RUNTIME_SEC})",
    )
    parser.add_argument(
        "--skip-recorder",
        action="store_true",
        help="Skip recorder test (useful if you already have data)",
    )
    parser.add_argument(
        "--depth-mode",
        choices=("phase1", "phase2"),
        default="phase1",
        help="Acceptance mode for depth pipeline validation.",
    )
    parser.add_argument(
        "--emit-phase2-depth10",
        action="store_true",
        help="When depth mode is phase2, also require optional derived depth10 output.",
    )
    args = parser.parse_args()

    test = AcceptanceTest(
        runtime_sec=args.runtime,
        skip_recorder=args.skip_recorder,
        depth_mode=args.depth_mode,
        emit_phase2_depth10=args.emit_phase2_depth10,
    )
    result = test.run()
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
