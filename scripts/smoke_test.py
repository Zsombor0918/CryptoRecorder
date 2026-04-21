#!/usr/bin/env python3
"""
smoke_test.py — Quick recorder smoke test (3 minutes).

This script:
  1. Starts the recorder with 3 symbols for 3 minutes
  2. Stops it cleanly
  3. Verifies basic functionality worked

Use this to verify the recorder works after making changes.

Usage:
    python scripts/smoke_test.py
    python scripts/smoke_test.py --runtime 60  # 1 minute instead

Exit codes:
    0 = All checks passed
    1 = Some checks failed
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
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_ROOT, STATE_ROOT

DEFAULT_RUNTIME_SEC = 180  # 3 minutes
TEST_SYMBOLS = 3


class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


def run_smoke_test(runtime_sec: int = DEFAULT_RUNTIME_SEC, depth_mode: str = "phase1") -> Dict[str, Any]:
    """Run the recorder and check basic functionality."""
    print(f"\n{Colors.BOLD}═══════════════════════════════════════════════════{Colors.RESET}")
    print(f"{Colors.BOLD}  Smoke Test ({runtime_sec}s with {TEST_SYMBOLS} symbols){Colors.RESET}")
    print(f"{Colors.BOLD}═══════════════════════════════════════════════════{Colors.RESET}\n")

    results: Dict[str, Any] = {
        "runtime_sec": runtime_sec,
        "checks": {},
        "passed": False,
    }

    # Find Python interpreter
    venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
    py = str(venv_py) if venv_py.exists() else sys.executable

    # Start recorder
    t0 = time.time()
    env = os.environ.copy()
    env["CRYPTO_RECORDER_TOP_SYMBOLS"] = str(TEST_SYMBOLS)

    print(f"Starting recorder (pid will be shown)...")
    proc = subprocess.Popen(
        [py, str(PROJECT_ROOT / "recorder.py"), "--depth-mode", depth_mode],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        cwd=str(PROJECT_ROOT),
    )
    print(f"Recorder started (PID: {proc.pid})")
    print(f"Running for {runtime_sec} seconds...\n")

    # Wait and then stop
    try:
        stdout, _ = proc.communicate(timeout=runtime_sec)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGINT)
        try:
            stdout, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            stdout, _ = proc.communicate(timeout=10)

    log = (stdout or b"").decode("utf-8", errors="replace")
    elapsed = time.time() - t0

    # Save log for debugging
    log_path = STATE_ROOT / "smoke_test.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(log)

    # Run checks
    checks = [
        ("No rate limit errors", _check_no_rate_limit(log)),
        ("No callback errors", _check_no_callback_errors(log)),
        ("Raw files created", _check_raw_files_created(t0, depth_mode)),
        ("Heartbeat exists", _check_heartbeat()),
        ("Clean shutdown", _check_clean_shutdown(log, proc.returncode)),
    ]

    print(f"\n{Colors.BOLD}Results:{Colors.RESET}")
    all_passed = True
    for name, (ok, details) in checks:
        status = f"{Colors.GREEN}✓{Colors.RESET}" if ok else f"{Colors.RED}✗{Colors.RESET}"
        print(f"  {status} {name}")
        if not ok and details:
            print(f"      {details}")
        results["checks"][name] = {"passed": ok, "details": details}
        all_passed &= ok

    results["passed"] = all_passed
    results["elapsed_sec"] = round(elapsed, 1)

    # Summary
    print(f"\n{Colors.BOLD}═══════════════════════════════════════════════════{Colors.RESET}")
    if all_passed:
        print(f"{Colors.GREEN}✓ Smoke test PASSED{Colors.RESET}")
    else:
        print(f"{Colors.RED}✗ Smoke test FAILED{Colors.RESET}")
        print(f"  Check {log_path} for details")
    print(f"{Colors.BOLD}═══════════════════════════════════════════════════{Colors.RESET}\n")

    # Save results
    results_path = STATE_ROOT / "smoke_test_results.json"
    results_path.write_text(json.dumps(results, indent=2, default=str))

    return results


def _check_no_rate_limit(log: str) -> tuple[bool, str]:
    hits_429 = len(re.findall(r"429|rate.limit", log, re.I))
    hits_418 = len(re.findall(r"418|ban", log, re.I))
    if hits_429 or hits_418:
        return False, f"429: {hits_429}, 418: {hits_418}"
    return True, ""


def _check_no_callback_errors(log: str) -> tuple[bool, str]:
    hits = len(re.findall(r"Error in on_l2_book|Error in on_trade", log, re.I))
    if hits:
        return False, f"{hits} callback errors"
    return True, ""


def _check_raw_files_created(t0: float, depth_mode: str) -> tuple[bool, str]:
    if not DATA_ROOT.exists():
        return False, "data_raw/ doesn't exist"

    files = []
    for f in DATA_ROOT.rglob("*.jsonl*"):
        if f.stat().st_mtime >= t0:
            files.append(f)

    if not files:
        return False, "No files created"
    depth_channel = "depth_v2" if depth_mode == "phase2" else "depth"
    depth_files = [f for f in files if f"/{depth_channel}/" in str(f)]
    return True, f"{len(files)} files ({len(depth_files)} {depth_channel})"


def _check_heartbeat() -> tuple[bool, str]:
    hb = STATE_ROOT / "heartbeat.json"
    if not hb.exists():
        return False, "heartbeat.json missing"
    try:
        d = json.loads(hb.read_text())
        msgs = d.get("total_messages", 0)
        syms = d.get("total_symbols", 0)
        return True, f"{msgs} messages, {syms} symbols"
    except Exception as e:
        return False, str(e)


def _check_clean_shutdown(log: str, returncode: int) -> tuple[bool, str]:
    # Check for async issues
    issues = []
    if re.search(r"watchdog fired", log, re.I):
        issues.append("watchdog fired")
    if re.search(r"different loop|never awaited|loop.*closed", log, re.I):
        issues.append("async issues")

    if returncode not in (0, -2, -signal.SIGINT, -signal.SIGTERM):
        issues.append(f"exit code {returncode}")

    if issues:
        return False, ", ".join(issues)
    return True, ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Recorder smoke test")
    parser.add_argument(
        "--runtime",
        type=int,
        default=DEFAULT_RUNTIME_SEC,
        help=f"Seconds to run (default: {DEFAULT_RUNTIME_SEC})",
    )
    parser.add_argument(
        "--depth-mode",
        choices=("phase1", "phase2"),
        default="phase1",
        help="Depth pipeline mode to validate.",
    )
    args = parser.parse_args()

    result = run_smoke_test(args.runtime, depth_mode=args.depth_mode)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
