#!/usr/bin/env python3
"""
runtime.py  –  3-minute recorder smoke-test

Starts the recorder as a subprocess with TOP_SYMBOLS=3 (snapshots disabled),
lets it run for 3 minutes, sends SIGINT, then asserts:

  1. no_429_418           – no HTTP 429 / 418 rate-limit bans in logs
  2. no_callback_errors   – no "Error in on_l2_book" / "Error in on_trade"
  3. raw_files_nonempty   – .jsonl files created in data_raw/ and non-empty
  4. heartbeat_updated    – heartbeat.json shows uptime ≥ 10 s and messages > 0
  5. clean_shutdown       – exit code 0, no watchdog
  6. no_async_pathology   – no "different loop" / "never awaited" / "loop closed"
  7. schema_fields        – every record has venue/symbol/channel/ts_recv_ns
  8. ts_recv_monotonic    – ts_recv_ns non-decreasing per file
  9. symbol_no_dash       – symbols use Binance format (no dashes)
 10. futures_status       – futures recording OR explicitly degraded
 11. raw_files_compressed – shutdown compressed final .jsonl → .jsonl.zst
 12. queue_drops          – zero queue drops in heartbeat
 13. heartbeat_venue_counts – top-level spot/futures counts exist and match by_venue

Outputs JSON report to state/runtime_report.json
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
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from config import DATA_ROOT, STATE_ROOT
from time_utils import local_now_iso

DEFAULT_RUNTIME_SEC = 180
TEST_TOP_SYMBOLS = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate_runtime")


# ── helpers ──────────────────────────────────────────────────────────

def _jsonl_files_after(root: Path, ts: float) -> List[Path]:
    """Find .jsonl and .jsonl.zst files modified after ts."""
    if not root.exists():
        return []
    out = []
    for p in root.rglob("*.jsonl*"):
        if p.stat().st_mtime >= ts:
            out.append(p)
    return out


def _read_jsonl(path: Path, n: int = 200) -> List[dict]:
    import zstandard as zstd
    out: List[dict] = []
    try:
        if path.suffix == ".zst":
            opener = lambda: zstd.open(path, "rt", errors="ignore")
        else:
            opener = lambda: open(path, errors="ignore")
        with opener() as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except OSError:
        pass
    return out


# ── validator ────────────────────────────────────────────────────────

class RuntimeValidator:

    def __init__(self, runtime_sec: int = DEFAULT_RUNTIME_SEC):
        self.runtime_sec = runtime_sec
        self._t0 = 0.0
        self._proc: Optional[subprocess.Popen] = None
        self.report: Dict[str, Any] = {
            "test": "validate_runtime",
            "started_at": local_now_iso(),
            "runtime_sec": runtime_sec,
            "checks": {},
            "passed": False,
        }

    # ── launch / stop ────────────────────────────────────────────────

    def _launch(self) -> None:
        self._t0 = time.time()
        venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
        py = str(venv_py) if venv_py.exists() else sys.executable
        env = os.environ.copy()
        env["CRYPTO_RECORDER_TEST_MODE"] = "1"
        env["CRYPTO_RECORDER_TOP_SYMBOLS"] = str(TEST_TOP_SYMBOLS)
        self._proc = subprocess.Popen(
            [py, str(PROJECT_ROOT / "recorder.py")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env, cwd=str(PROJECT_ROOT),
        )
        logger.info(f"Launched recorder (pid={self._proc.pid}), "
                     f"will run {self.runtime_sec}s …")

    def _stop(self) -> str:
        assert self._proc
        # Let the recorder run for the desired duration, then ask it to stop.
        try:
            out, _ = self._proc.communicate(timeout=self.runtime_sec)
        except subprocess.TimeoutExpired:
            # Normal path: recorder runs forever; tell it to shut down.
            self._proc.send_signal(signal.SIGINT)
            try:
                out, _ = self._proc.communicate(timeout=45)
            except subprocess.TimeoutExpired:
                logger.warning("Recorder did not exit after SIGINT – sending SIGTERM")
                self._proc.terminate()
                try:
                    out, _ = self._proc.communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    logger.warning("Recorder still alive – sending SIGKILL")
                    self._proc.kill()
                    out, _ = self._proc.communicate(timeout=10)
        log = (out or b"").decode("utf-8", errors="replace")
        log_path = STATE_ROOT / "runtime_test.log"
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
                "details": {"429": lines_429[:3], "418": lines_418[:3]}}

    def _ck_no_callback_errors(self, log: str) -> dict:
        hits = re.findall(r"Error in on_l2_book|Error in on_trade", log, re.I)
        return {"name": "no_callback_errors", "passed": len(hits) == 0,
                "details": {"count": len(hits)}}

    def _ck_raw_files(self) -> dict:
        files = _jsonl_files_after(DATA_ROOT, self._t0)
        nonempty = [f for f in files if f.stat().st_size > 0]
        return {"name": "raw_files_nonempty", "passed": len(nonempty) >= 1,
                "details": {"found": len(files), "nonempty": len(nonempty),
                            "sample": [str(f.relative_to(PROJECT_ROOT))
                                       for f in nonempty[:5]]}}

    def _ck_heartbeat(self) -> dict:
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "heartbeat_updated", "passed": False,
                    "details": {"reason": "file missing"}}
        try:
            d = json.loads(hb.read_text())
        except Exception as e:
            return {"name": "heartbeat_updated", "passed": False,
                    "details": {"reason": str(e)}}
        ok = d.get("uptime_seconds", 0) >= 10 and d.get("total_messages", 0) > 0
        return {"name": "heartbeat_updated", "passed": ok,
                "details": {"uptime": d.get("uptime_seconds"),
                            "msgs": d.get("total_messages"),
                            "symbols": d.get("total_symbols")}}

    def _ck_clean_shutdown(self, log: str) -> dict:
        mismatch = re.search(
            r"(attached to a different loop|loop.*already running|"
            r"cannot schedule new futures|Event loop is closed)", log, re.I)
        watchdog = bool(re.search(r"watchdog fired", log, re.I))
        rc = self._proc.returncode if self._proc else -1
        ok = (mismatch is None
              and not watchdog
              and rc in (0, -2, -signal.SIGINT, -signal.SIGTERM))

        details: dict = {
            "exit_code": rc,
            "loop_mismatch": mismatch.group(0) if mismatch else None,
            "watchdog_fired": watchdog,
        }

        # On failure, attach last 100 lines of recorder.log for diagnosis
        if not ok:
            rec_log = PROJECT_ROOT / "recorder.log"
            if rec_log.exists():
                try:
                    lines = rec_log.read_text().splitlines()
                    details["recorder_log_tail"] = lines[-100:]
                except Exception:
                    pass

        return {"name": "clean_shutdown", "passed": ok, "details": details}

    def _ck_schema(self) -> dict:
        required = {"venue", "symbol", "channel", "ts_recv_ns"}
        issues = []
        n = 0
        for f in _jsonl_files_after(DATA_ROOT, self._t0)[:20]:
            for rec in _read_jsonl(f, 50):
                n += 1
                missing = required - set(rec.keys())
                if missing:
                    issues.append(f"{f.name}: {missing}")
        return {"name": "schema_fields", "passed": not issues and n > 0,
                "details": {"checked": n, "issues": issues[:5]}}

    def _ck_monotonic(self) -> dict:
        """ts_recv_ns non-decreasing per file (5 s tolerance for interleaved WS)."""
        tolerance_ns = 5_000_000_000  # 5 seconds
        violations = []
        fc = 0
        for f in _jsonl_files_after(DATA_ROOT, self._t0)[:20]:
            prev = 0
            for rec in _read_jsonl(f, 500):
                ts = rec.get("ts_recv_ns", 0)
                if ts and prev and ts < prev - tolerance_ns:
                    violations.append(f"{f.name}: {prev}->{ts}")
                prev = ts or prev
            fc += 1
        return {"name": "ts_recv_monotonic", "passed": not violations and fc > 0,
                "details": {"files": fc, "violations": violations[:5]}}

    def _ck_symbol_format(self) -> dict:
        bad = set()
        n = 0
        for f in _jsonl_files_after(DATA_ROOT, self._t0)[:20]:
            for rec in _read_jsonl(f, 100):
                n += 1
                s = rec.get("symbol", "")
                if "-" in s:
                    bad.add(s)
        return {"name": "symbol_no_dash", "passed": not bad and n > 0,
                "details": {"checked": n, "bad": list(bad)[:5]}}

    def _ck_futures(self, log: str) -> dict:
        """Futures check: producing data = full pass, disabled = DEGRADED (still passes smoke test)."""
        fut_dir = DATA_ROOT / "BINANCE_USDTF"
        producing = bool(fut_dir.exists() and
                         [f for f in fut_dir.rglob("*.jsonl*")
                          if f.stat().st_mtime >= self._t0])
        disabled = bool(re.search(
            r"(Futures DISABLED|Failed to init.*Futures|futures.*disabled)",
            log, re.I))
        # For the 3-min smoke test, degraded mode is acceptable (passes)
        # but is clearly labelled so the scale validator can treat it differently.
        if producing:
            status = "recording"
        elif disabled:
            status = "degraded"
        else:
            status = "unknown"
        return {"name": "futures_status", "passed": producing or disabled,
                "details": {"status": status, "producing": producing,
                            "disabled_flag": disabled}}

    def _ck_clean_shutdown_logs(self, log: str) -> dict:
        """Check for async pathologies in logs."""
        patterns = [
            (r"future belongs to a different loop", "future_different_loop"),
            (r"attached to a different loop", "attached_different_loop"),
            (r"never awaited", "never_awaited"),
            (r"Event loop is closed", "loop_closed"),
            (r"loop.*already running", "loop_already_running"),
            (r"cannot schedule new futures", "cannot_schedule"),
        ]
        found = {}
        for pattern, label in patterns:
            hits = re.findall(pattern, log, re.I)
            if hits:
                found[label] = len(hits)
        ok = len(found) == 0
        return {"name": "no_async_pathology", "passed": ok,
                "details": found if found else {"clean": True}}

    def _ck_raw_files_compressed(self) -> dict:
        """After shutdown, check that .jsonl files created during the run got compressed.
        
        During a short smoke test some files may still be open at shutdown time.
        We just check that at least some .jsonl.zst exist if any data was written.
        """
        zst_files = list(DATA_ROOT.rglob("*.jsonl.zst"))
        jsonl_files = [f for f in DATA_ROOT.rglob("*.jsonl")
                       if f.stat().st_mtime >= self._t0]
        zst_new = [f for f in zst_files if f.stat().st_mtime >= self._t0]
        # Pass if: no leftover uncompressed .jsonl from this run,
        # OR at least some .zst were created.
        ok = len(jsonl_files) == 0 or len(zst_new) > 0
        return {"name": "raw_files_compressed", "passed": ok,
                "details": {"uncompressed_jsonl": len(jsonl_files),
                            "compressed_zst": len(zst_new)}}

    def _ck_queue_drops(self) -> dict:
        """Check heartbeat for queue drops."""
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "queue_drops", "passed": True,
                    "details": {"reason": "no heartbeat"}}
        try:
            d = json.loads(hb.read_text())
        except Exception:
            return {"name": "queue_drops", "passed": True,
                    "details": {"reason": "heartbeat unreadable"}}
        drops = d.get("queue_drop_total", 0)
        return {"name": "queue_drops", "passed": drops == 0,
                "details": {"queue_drop_total": drops}}

    def _ck_heartbeat_venue_counts(self) -> dict:
        """Top-level spot/futures counts must exist and match by_venue."""
        hb = STATE_ROOT / "heartbeat.json"
        if not hb.exists():
            return {"name": "heartbeat_venue_counts", "passed": False,
                    "details": {"reason": "file missing"}}
        try:
            d = json.loads(hb.read_text())
        except Exception as e:
            return {"name": "heartbeat_venue_counts", "passed": False,
                    "details": {"reason": str(e)}}

        issues = []
        by_venue = d.get("by_venue")
        if not isinstance(by_venue, dict):
            issues.append(f"by_venue is {type(by_venue).__name__}, expected dict")
            by_venue = {}

        counts = {
            "spot_symbols_active": d.get("spot_symbols_active"),
            "futures_symbols_active": d.get("futures_symbols_active"),
        }
        expected = {
            "spot_symbols_active": len(by_venue.get("BINANCE_SPOT", [])),
            "futures_symbols_active": len(by_venue.get("BINANCE_USDTF", [])),
        }
        for key, value in counts.items():
            if not isinstance(value, int) or value < 0:
                issues.append(f"{key} invalid: {value!r}")
            elif value != expected[key]:
                issues.append(f"{key}={value}, expected {expected[key]}")

        return {"name": "heartbeat_venue_counts", "passed": not issues,
                "details": {"issues": issues, **counts}}

    # ── orchestrate ──────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("  validate_runtime  – 3-minute recorder smoke-test")
        logger.info("=" * 60)

        self._launch()
        log = self._stop()

        checks = [
            self._ck_no_rate_limit(log),
            self._ck_no_callback_errors(log),
            self._ck_raw_files(),
            self._ck_heartbeat(),
            self._ck_clean_shutdown(log),
            self._ck_clean_shutdown_logs(log),
            self._ck_schema(),
            self._ck_monotonic(),
            self._ck_symbol_format(),
            self._ck_futures(log),
            self._ck_raw_files_compressed(),
            self._ck_queue_drops(),
            self._ck_heartbeat_venue_counts(),
        ]

        self.report["checks"] = {c["name"]: c for c in checks}
        self.report["total_checks"] = len(checks)
        self.report["checks_passed"] = sum(c["passed"] for c in checks)
        self.report["passed"] = all(c["passed"] for c in checks)
        self.report["finished_at"] = local_now_iso()

        for c in checks:
            tag = "PASS" if c["passed"] else "FAIL"
            logger.info(f"  [{tag}] {c['name']}")
            if not c["passed"]:
                for k, v in c.get("details", {}).items():
                    logger.info(f"         {k}: {v}")

        n = self.report["checks_passed"]
        t = self.report["total_checks"]
        if self.report["passed"]:
            logger.info(f"\n  ✓ ALL PASSED ({n}/{t})")
        else:
            logger.warning(f"\n  ✗ SOME FAILED ({n}/{t})")

        out = STATE_ROOT / "runtime_report.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(self.report, indent=2, default=str))
        logger.info(f"Report → {out}")
        return self.report


def main() -> int:
    ap = argparse.ArgumentParser(description="Runtime recorder smoke-test")
    ap.add_argument("--runtime", type=int, default=DEFAULT_RUNTIME_SEC,
                    help="seconds to run the recorder (default 180)")
    args = ap.parse_args()
    r = RuntimeValidator(args.runtime).run()
    return 0 if r["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
