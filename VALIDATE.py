#!/usr/bin/env python3
"""
VALIDATE.py  –  Master validation entrypoint for CryptoRecorder

Three validators, three modes:

  python VALIDATE.py system     validate_system.py   (imports + config, ~5 s)
  python VALIDATE.py runtime    validate_runtime.py   (live 3-min smoke-test)
  python VALIDATE.py converter  validate_converter_e2e.py (parquet roundtrip)
  python VALIDATE.py all        all three in sequence
"""
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

# ── colours ──────────────────────────────────────────────────────────

class C:
    G = '\033[92m'   # green
    R = '\033[91m'   # red
    Y = '\033[93m'   # yellow
    B = '\033[1m'    # bold
    _  = '\033[0m'   # reset


PROJECT = Path(__file__).parent
STATE   = PROJECT / "state"
STATE.mkdir(exist_ok=True)


# ── runner ───────────────────────────────────────────────────────────

def _run(cmd: str, timeout: int = 300) -> Tuple[bool, str]:
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=str(PROJECT), executable="/bin/bash")
        return r.returncode == 0, r.stdout + r.stderr
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def _load_report(name: str) -> Optional[dict]:
    p = STATE / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


# ── individual validators ────────────────────────────────────────────

def run_system() -> bool:
    print(f"\n{C.B}► System validation (validate_system.py){C._}")
    ok, out = _run("source .venv/bin/activate && python3 validate_system.py --quick")
    if ok:
        print(f"  {C.G}✓ system checks passed{C._}")
    else:
        print(f"  {C.R}✗ system checks failed{C._}")
        print(out[-500:])
    return ok


def run_runtime(runtime_sec: int = 180) -> bool:
    print(f"\n{C.B}► Runtime smoke-test (validate_runtime.py, {runtime_sec}s){C._}")
    ok, out = _run(
        f"source .venv/bin/activate && python3 validate_runtime.py --runtime {runtime_sec}",
        timeout=runtime_sec + 60)
    rpt = _load_report("runtime_report.json")
    passed = rpt.get("passed", False) if rpt else False
    n = rpt.get("checks_passed", "?") if rpt else "?"
    t = rpt.get("total_checks", "?") if rpt else "?"
    if passed:
        print(f"  {C.G}✓ runtime checks passed ({n}/{t}){C._}")
    else:
        print(f"  {C.R}✗ runtime checks failed ({n}/{t}){C._}")
        if rpt:
            for name, chk in rpt.get("checks", {}).items():
                if not chk.get("passed"):
                    print(f"    {C.R}✗ {name}{C._}")
    return passed


def run_converter() -> bool:
    print(f"\n{C.B}► Converter E2E (validate_converter_e2e.py){C._}")
    ok, out = _run("source .venv/bin/activate && python3 validate_converter_e2e.py")
    rpt = _load_report("converter_e2e_report.json")
    if rpt and rpt.get("skipped"):
        print(f"  {C.Y}⚠ skipped: {rpt.get('reason', '?')}{C._}")
        return True  # skip ≠ failure
    passed = rpt.get("passed", False) if rpt else False
    n = rpt.get("checks_passed", "?") if rpt else "?"
    t = rpt.get("total_checks", "?") if rpt else "?"
    if passed:
        print(f"  {C.G}✓ converter checks passed ({n}/{t}){C._}")
    else:
        print(f"  {C.R}✗ converter checks failed ({n}/{t}){C._}")
    return passed


# ── summary ──────────────────────────────────────────────────────────

def _save_and_print(results: Dict[str, bool]) -> int:
    report = {
        "timestamp": datetime.now().isoformat(),
        "results": {k: {"passed": v} for k, v in results.items()},
        "total": len(results),
        "passed": sum(results.values()),
        "failed": sum(not v for v in results.values()),
    }
    (STATE / "master_validation_report.json").write_text(
        json.dumps(report, indent=2))

    print(f"\n{'=' * 50}")
    for name, ok in results.items():
        tag = f"{C.G}PASS{C._}" if ok else f"{C.R}FAIL{C._}"
        print(f"  {name:20s} {tag}")
    total = len(results)
    passed = sum(results.values())
    pct = 100 * passed / total if total else 0
    print(f"\n  {passed}/{total} passed ({pct:.0f}%)")
    print(f"{'=' * 50}\n")
    return 0 if passed == total else 1


# ── CLI ──────────────────────────────────────────────────────────────

USAGE = f"""\
{C.B}VALIDATE.py{C._}  –  CryptoRecorder validation

  python VALIDATE.py system       Import / config checks (~5 s)
  python VALIDATE.py runtime      Live recorder 3-min smoke-test
  python VALIDATE.py converter    Converter E2E parquet roundtrip
  python VALIDATE.py all          All three in order
  python VALIDATE.py --help       This message
"""


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print(USAGE)
        return 0

    mode = sys.argv[1]
    results: Dict[str, bool] = {}

    if mode == "system":
        results["system"] = run_system()
    elif mode == "runtime":
        results["runtime"] = run_runtime()
    elif mode == "converter":
        results["converter"] = run_converter()
    elif mode == "all":
        results["system"] = run_system()
        results["runtime"] = run_runtime()
        results["converter"] = run_converter()
    else:
        print(f"Unknown mode: {mode}")
        print(USAGE)
        return 1

    return _save_and_print(results)


if __name__ == "__main__":
    sys.exit(main())
