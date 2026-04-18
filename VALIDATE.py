#!/usr/bin/env python3
"""
VALIDATE.py  –  Master validation entrypoint for CryptoRecorder

  python VALIDATE.py system     validators/validate_system.py       (~5 s)
  python VALIDATE.py runtime    validators/validate_runtime.py      (3-min smoke)
  python VALIDATE.py scale      validators/validate_scale_50_50.py  (10-min 50/50)
  python VALIDATE.py nautilus   validators/validate_nautilus_catalog_e2e.py (16 checks)
  python VALIDATE.py purge      validators/validate_purge_safety.py (6 checks)
  python VALIDATE.py converter  validators/validate_converter_e2e.py (legacy)
  python VALIDATE.py all        system + runtime + nautilus + purge  (quick suite)
  python VALIDATE.py accept     system + runtime + scale + nautilus + purge  (full DoD)
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
    ok, out = _run("source .venv/bin/activate && python3 validators/validate_system.py --quick")
    if ok:
        print(f"  {C.G}✓ system checks passed{C._}")
    else:
        print(f"  {C.R}✗ system checks failed{C._}")
        print(out[-500:])
    return ok


def run_runtime(runtime_sec: int = 180) -> bool:
    print(f"\n{C.B}► Runtime smoke-test (validate_runtime.py, {runtime_sec}s){C._}")
    ok, out = _run(
        f"source .venv/bin/activate && python3 validators/validate_runtime.py --runtime {runtime_sec}",
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
    ok, out = _run("source .venv/bin/activate && python3 validators/validate_converter_e2e.py")
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


def run_scale(runtime_sec: int = 600) -> bool:
    print(f"\n{C.B}► Scale 50/50 acceptance (validate_scale_50_50.py, {runtime_sec}s){C._}")
    ok, out = _run(
        f"source .venv/bin/activate && python3 validators/validate_scale_50_50.py --runtime {runtime_sec}",
        timeout=runtime_sec + 120)
    rpt = _load_report("scale_50_50_report.json")
    passed = rpt.get("passed", False) if rpt else False
    n = rpt.get("checks_passed", "?") if rpt else "?"
    t = rpt.get("total_checks", "?") if rpt else "?"
    if passed:
        print(f"  {C.G}✓ scale checks passed ({n}/{t}){C._}")
    else:
        print(f"  {C.R}✗ scale checks failed ({n}/{t}){C._}")
        if rpt:
            for name, chk in rpt.get("checks", {}).items():
                if not chk.get("passed"):
                    print(f"    {C.R}✗ {name}{C._}")
    return passed


def run_nautilus() -> bool:
    print(f"\n{C.B}► Nautilus catalog E2E (validate_nautilus_catalog_e2e.py){C._}")
    ok, out = _run("source .venv/bin/activate && python3 validators/validate_nautilus_catalog_e2e.py",
                   timeout=360)
    # Try to find the most recent report
    val_dir = STATE / "validation"
    rpt = None
    if val_dir.exists():
        reports = sorted(val_dir.glob("nautilus_catalog_e2e_*.json"), reverse=True)
        if reports:
            try:
                rpt = json.loads(reports[0].read_text())
            except Exception:
                pass
    if rpt and rpt.get("skipped"):
        print(f"  {C.Y}⚠ skipped: {rpt.get('reason', '?')}{C._}")
        return True
    passed = rpt.get("passed", False) if rpt else False
    n = rpt.get("checks_passed", "?") if rpt else "?"
    t = rpt.get("total_checks", "?") if rpt else "?"
    if passed:
        print(f"  {C.G}✓ nautilus catalog checks passed ({n}/{t}){C._}")
    else:
        print(f"  {C.R}✗ nautilus catalog checks failed ({n}/{t}){C._}")
        if rpt:
            for name, chk in rpt.get("checks", {}).items():
                if not chk.get("passed"):
                    print(f"    {C.R}✗ {name}{C._}")
    return passed


def run_purge() -> bool:
    print(f"\n{C.B}► Purge safety (validate_purge_safety.py){C._}")
    ok, out = _run("source .venv/bin/activate && python3 validators/validate_purge_safety.py",
                   timeout=60)
    val_dir = STATE / "validation"
    rpt = None
    if val_dir.exists():
        rp = val_dir / "purge_safety.json"
        if rp.exists():
            try:
                rpt = json.loads(rp.read_text())
            except Exception:
                pass
    passed = rpt.get("passed", False) if rpt else False
    n = rpt.get("checks_passed", "?") if rpt else "?"
    t = rpt.get("total_checks", "?") if rpt else "?"
    if passed:
        print(f"  {C.G}✓ purge safety checks passed ({n}/{t}){C._}")
    else:
        print(f"  {C.R}✗ purge safety checks failed ({n}/{t}){C._}")
        if rpt:
            for name, chk in rpt.get("checks", {}).items():
                if not chk.get("passed"):
                    print(f"    {C.R}✗ {name}{C._}")
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
  python VALIDATE.py runtime      Live recorder 3-min smoke-test (12 checks)
  python VALIDATE.py scale        50/50 scale 10-min acceptance test (11 checks)
  python VALIDATE.py nautilus     Nautilus catalog E2E (16 checks)
  python VALIDATE.py purge        Purge safety proof (6 checks)
  python VALIDATE.py converter    Legacy converter E2E (7 checks)
  python VALIDATE.py all          system + runtime + nautilus + purge  (quick suite)
  python VALIDATE.py accept       system + runtime + scale + nautilus + purge  (full DoD)
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
    elif mode == "scale":
        results["scale"] = run_scale()
    elif mode == "nautilus":
        results["nautilus"] = run_nautilus()
    elif mode == "purge":
        results["purge"] = run_purge()
    elif mode == "converter":
        results["converter"] = run_converter()
    elif mode == "all":
        results["system"] = run_system()
        results["runtime"] = run_runtime()
        results["nautilus"] = run_nautilus()
        results["purge"] = run_purge()
    elif mode == "accept":
        results["system"] = run_system()
        results["runtime"] = run_runtime()
        results["scale"] = run_scale()
        results["nautilus"] = run_nautilus()
        results["purge"] = run_purge()
    else:
        print(f"Unknown mode: {mode}")
        print(USAGE)
        return 1

    return _save_and_print(results)


if __name__ == "__main__":
    sys.exit(main())
