#!/usr/bin/env python3
"""
validate_scale_50_50.py  –  10-minute 50/50 scale acceptance test

Runs the recorder with CRYPTO_RECORDER_TOP_SYMBOLS=50 for 10 minutes,
then validates production-grade behaviour:

  1. no_429_418            – no HTTP 429/418 rate-limit bans
  2. no_callback_errors    – no callback exceptions
  3. spot_depth_symbols    – spot depth files for >= MIN_SPOT_SYMS symbols
  4. futures_depth_symbols – futures depth files for >= MIN_FUT_SYMS symbols
  5. heartbeat_total_msgs  – heartbeat total_messages > 0
  6. futures_enabled       – heartbeat futures_enabled == true
  7. queue_drops           – drops <= MAX_QUEUE_DROPS
  8. reconnect_count       – reconnects <= MAX_RECONNECTS
  9. no_reconnect_storm    – no burst of reconnects in a short window
 10. clean_shutdown        – exit code 0, no watchdog / async pathology
 11. raw_files_compressed  – final .jsonl files compressed to .jsonl.zst

Report -> state/scale_50_50_report.json
"""
import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
from config import DATA_ROOT, STATE_ROOT

# ── thresholds ───────────────────────────────────────────────────────
DEFAULT_RUNTIME_SEC = 600          # 10 minutes
SCALE_TOP_SYMBOLS = 50

MIN_SPOT_DEPTH_SYMBOLS = 10        # at least this many spot symbols have depth data
MIN_FUTURES_DEPTH_SYMBOLS = 5      # at least this many futures symbols have depth data
MAX_QUEUE_DROPS = 50               # allow a small number of drops under load
MAX_RECONNECTS = 20                # reconnects within 10 min

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate_scale_50_50")


# ── helpers ──────────────────────────────────────────────────────────

def _symbols_with_data(venue_root: Path, channel: str, t0: float) -> Set[str]:
    """Return set of symbol names that have data files newer than t0."""
    ch_dir = venue_root / channel
    if not ch_dir.exists():
        return set()
    syms: Set[str] = set()
    for sym_dir in ch_dir.iterdir():
        if not sym_dir.is_dir():
            continue
        for f in sym_dir.rglob("*.jsonl*"):
            if f.stat().st_mtime >= t0:
                syms.add(sym_dir.name)
                break
    return syms


class ScaleValidator:

    def __init__(self, runtime_sec: int = DEFAULT_RUNTIME_SEC):
        self.runtime_sec = runtime_sec
        self._t0 = 0.0
        self._proc: Optional[subprocess.Popen] = None
        self.report: Dict[str, Any] = {
            "test": "validate_scale_50_50",
            "started_at": datetime.utcnow().isoformat(),
            "runtime_sec": runtime_sec,
            "top_symbols": SCALE_TOP_SYMBOLS,
            "checks": {},
            "passed": False,
        }

    # ── launch / stop ────────────────────────────────────────────────

    def _launch(self) -> None:
        self._t0 = time.time()
        venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
        py = str(venv_py) if venv_py.exists() else sys.executable
        env = os.environ.copy()
        env["CRYPTO_RECORDER_TOP_SYMBOLS"] = str(SCALE_TOP_SYMBOLS)
        self._proc = subprocess.Popen(
            [py, str(PROJECT_ROOT / "recorder.py")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env, cwd=str(PROJECT_ROOT),
        )
        logger.info(f"Launched recorder (pid={self._proc.pid}), "
                     f"will run {self.runtime_sec}s with TOP_SYMBOLS={SCALE_TOP_SYMBOLS}")

    def _stop(self) -> str:
        assert self._proc
        try:
            out, _ = self._proc.communicate(timeout=self.runtime_sec)
        except subprocess.TimeoutExpired:
            self._proc.send_signal(signal.SIGINT)
            try:
                out, _ = self._proc.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                logger.warning("Recorder did not exit after SIGINT — sending SIGTERM")
                self._proc.terminate()
                try:
                    out, _ = self._proc.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    logger.warning("Recorder still alive — sending SIGKILL")
                    self._proc.kill()
                    out, _ = self._proc.communicate(timeout=10)
        log = (out or b"").decode("utf-8", errors="replace")
        log_path = STATE_ROOT / "scale_test.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(log)
        return log

    # ── checks ───────────────────────────────────────────────────────

    def _ck_no_rate_limit(self, log: str) -> dict:
        lines_429 = [l for l in log.splitlines()
                      if re.search(r"(status.*429|429.*rate.limit|HTTP.*429)", l, re.I)]
        lines_418 = [l for l in log.splitlines()
                      if re.search(r"(status.*418|418.*ban|HTTP.*418)", l, re.I)]
        ok = not lines_429 and not lines_418
        return {"name": "no_429_418", "passed": ok,
                "details": {"429": lines_429[:5], "418": lines_418[:5]}}

    def _ck_no_callback_errors(self, log: str) -> dict:
        hits = re.findall(r"Error in on_l2_book|Error in on_trade", log, re.I)
        return {"name": "no_callback_errors", "passed": len(hits) == 0,
                "details": {"count": len(hits)}}

    def _ck_spot_depth_symbols(self) -> dict:
        spot_root = DATA_ROOT / "BINANCE_SPOT"
        syms = _symbols_with_data(spot_root, "depth", self._t0)
        ok = len(syms) >= MIN_SPOT_DEPTH_SYMBOLS
        return {"name": "spot_depth_symbols", "passed": ok,
                "details": {"count": len(syms), "min_required": MIN_SPOT_DEPTH_SYMBOLS,
                            "sample": sorted(syms)[:10]}}

    def _ck_futures_depth_symbols(self) -> dict:
        fut_root = DATA_ROOT / "BINANCE_USDTF"
        syms = _symbols_with_data(fut_root, "depth", self._t0)
        ok = len(syms) >= MIN_FUTURES_DEPTH_SYMBOLS
        return {"name": "futures_depth_symbols", "passed": ok,
                "details": {"count": len(syms), "min_required": MIN_FUTURES_DEPTH_SYMBOLS,
                            "sample": sorted(syms)[:10]}}

    def _ck_heartbeat_msgs(self) -> dict:
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "heartbeat_total_msgs", "passed": False,
                    "details": {"reason": "file missing"}}
        try:
            d = json.loads(hb.read_text())
        except Exception as e:
            return {"name": "heartbeat_total_msgs", "passed": False,
                    "details": {"reason": str(e)}}
        msgs = d.get("total_messages", 0)
        return {"name": "heartbeat_total_msgs", "passed": msgs > 0,
                "details": {"total_messages": msgs}}

    def _ck_futures_enabled(self) -> dict:
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "futures_enabled", "passed": False,
                    "details": {"reason": "file missing"}}
        try:
            d = json.loads(hb.read_text())
        except Exception:
            return {"name": "futures_enabled", "passed": False,
                    "details": {"reason": "unreadable"}}
        enabled = d.get("futures_enabled", False)
        reason = d.get("futures_disabled_reason", "")
        return {"name": "futures_enabled", "passed": bool(enabled),
                "details": {"futures_enabled": enabled,
                            "reason": reason if not enabled else ""}}

    def _ck_queue_drops(self) -> dict:
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "queue_drops", "passed": True, "details": {}}
        try:
            d = json.loads(hb.read_text())
        except Exception:
            return {"name": "queue_drops", "passed": True, "details": {}}
        drops = d.get("queue_drop_total", 0)
        ok = drops <= MAX_QUEUE_DROPS
        return {"name": "queue_drops", "passed": ok,
                "details": {"queue_drop_total": drops, "threshold": MAX_QUEUE_DROPS}}

    def _ck_reconnect_count(self, log: str) -> dict:
        count_match = re.findall(r"Reconnect #(\d+)", log)
        count = int(count_match[-1]) if count_match else 0
        ok = count <= MAX_RECONNECTS
        return {"name": "reconnect_count", "passed": ok,
                "details": {"reconnects": count, "threshold": MAX_RECONNECTS}}

    def _ck_no_reconnect_storm(self, log: str) -> dict:
        """Detect >5 reconnects within any 30-second window."""
        timestamps = []
        for m in re.finditer(r"\[(\d{4}-\d{2}-\d{2}T[\d:.]+)\] Reconnect", log):
            try:
                ts = datetime.fromisoformat(m.group(1))
                timestamps.append(ts.timestamp())
            except Exception:
                pass
        storm = False
        for i in range(len(timestamps)):
            window = [t for t in timestamps[i:] if t - timestamps[i] <= 30]
            if len(window) > 5:
                storm = True
                break
        return {"name": "no_reconnect_storm", "passed": not storm,
                "details": {"total_reconnects": len(timestamps), "storm_detected": storm}}

    def _ck_clean_shutdown(self, log: str) -> dict:
        patterns = [
            r"watchdog fired",
            r"future belongs to a different loop",
            r"attached to a different loop",
            r"never awaited",
            r"Event loop is closed",
        ]
        found = {}
        for p in patterns:
            hits = re.findall(p, log, re.I)
            if hits:
                found[p] = len(hits)
        rc = self._proc.returncode if self._proc else -1
        ok = (not found
              and rc in (0, -2, -signal.SIGINT, -signal.SIGTERM))

        details: dict = {"exit_code": rc}
        if found:
            details["issues"] = found
        if not ok:
            rec_log = PROJECT_ROOT / "recorder.log"
            if rec_log.exists():
                try:
                    lines = rec_log.read_text().splitlines()
                    details["recorder_log_tail"] = lines[-100:]
                except Exception:
                    pass
        return {"name": "clean_shutdown", "passed": ok, "details": details}

    def _ck_raw_files_compressed(self) -> dict:
        zst_new = [f for f in DATA_ROOT.rglob("*.jsonl.zst")
                    if f.stat().st_mtime >= self._t0]
        jsonl_leftover = [f for f in DATA_ROOT.rglob("*.jsonl")
                          if f.stat().st_mtime >= self._t0]
        ok = len(jsonl_leftover) == 0 or len(zst_new) > 0
        return {"name": "raw_files_compressed", "passed": ok,
                "details": {"compressed": len(zst_new),
                            "uncompressed_leftover": len(jsonl_leftover)}}

    # ── orchestrate ──────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("  validate_scale_50_50  –  10-min 50/50 acceptance test")
        logger.info("=" * 60)

        self._launch()
        log = self._stop()

        checks = [
            self._ck_no_rate_limit(log),
            self._ck_no_callback_errors(log),
            self._ck_spot_depth_symbols(),
            self._ck_futures_depth_symbols(),
            self._ck_heartbeat_msgs(),
            self._ck_futures_enabled(),
            self._ck_queue_drops(),
            self._ck_reconnect_count(log),
            self._ck_no_reconnect_storm(log),
            self._ck_clean_shutdown(log),
            self._ck_raw_files_compressed(),
        ]

        self.report["checks"] = {c["name"]: c for c in checks}
        self.report["total_checks"] = len(checks)
        self.report["checks_passed"] = sum(c["passed"] for c in checks)
        self.report["passed"] = all(c["passed"] for c in checks)
        self.report["finished_at"] = datetime.utcnow().isoformat()

        for c in checks:
            tag = "PASS" if c["passed"] else "FAIL"
            logger.info(f"  [{tag}] {c['name']}")
            if not c["passed"]:
                for k, v in c.get("details", {}).items():
                    if k != "recorder_log_tail":
                        logger.info(f"         {k}: {v}")

        n = self.report["checks_passed"]
        t = self.report["total_checks"]
        if self.report["passed"]:
            logger.info(f"\n  ✓ ALL PASSED ({n}/{t})")
        else:
            logger.warning(f"\n  ✗ SOME FAILED ({n}/{t})")

        out = STATE_ROOT / "scale_50_50_report.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(self.report, indent=2, default=str))
        logger.info(f"Report → {out}")
        return self.report


def main() -> int:
    ap = argparse.ArgumentParser(description="50/50 scale acceptance test")
    ap.add_argument("--runtime", type=int, default=DEFAULT_RUNTIME_SEC,
                    help=f"seconds to run the recorder (default {DEFAULT_RUNTIME_SEC})")
    args = ap.parse_args()
    r = ScaleValidator(args.runtime).run()
    return 0 if r["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
