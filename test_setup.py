#!/usr/bin/env python
"""
Quick setup validation script for Binance Recorder.
Tests infrastructure, dependencies, and connectivity.
"""
import sys
import os
import subprocess
from pathlib import Path

# Color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def test(name: str, condition: bool, error_msg: str = ""):
    """Print test result."""
    status = f"{GREEN}✓ PASS{RESET}" if condition else f"{RED}✗ FAIL{RESET}"
    print(f"[{status}] {name}")
    if not condition and error_msg:
        print(f"      {RED}{error_msg}{RESET}")
    return condition

def main():
    print(f"\n{YELLOW}=== Binance Recorder Setup Validation ==={RESET}\n")
    
    results = []
    
    # 1. Python version
    py_version = sys.version_info
    results.append(test(
        "Python 3.12+",
        py_version.major == 3 and py_version.minor >= 12,
        f"Found Python {py_version.major}.{py_version.minor}"
    ))
    
    # 2. Venv check
    venv_marker = Path(sys.prefix) / "pyvenv.cfg"
    in_venv = hasattr(sys, 'real_prefix') or venv_marker.exists()
    results.append(test(
        "Virtual environment active",
        in_venv,
        "Please activate venv: source .venv/bin/activate"
    ))
    
    # 3. Check dependencies
    deps = ['asyncio', 'aiohttp', 'zstandard', 'pandas', 'pyarrow']
    all_deps_ok = True
    for dep in deps:
        try:
            __import__(dep)
            results.append(test(f"Module: {dep}", True))
        except ImportError:
            all_deps_ok = False
            results.append(test(f"Module: {dep}", False, 
                f"Install with: pip install {dep}"))
    
    # 4. Check for cryptofeed (may not be installed yet)
    try:
        import cryptofeed
        results.append(test("Module: cryptofeed", True))
    except ImportError:
        results.append(test(
            "Module: cryptofeed",
            False,
            "Not installed yet (optional, install with: pip install cryptofeed==2.4.1)"
        ))
    
    # 5. Project structure
    project_root = Path(__file__).parent
    required_files = ['config.py', 'recorder.py', 'storage.py', 'health_monitor.py']
    for file in required_files:
        results.append(test(
            f"File: {file}",
            (project_root / file).exists(),
            f"File not found: {project_root / file}"
        ))
    
    # 6. Directory structure
    required_dirs = ['data_raw', 'meta', 'state']
    for dirn in required_dirs:
        dir_path = project_root / dirn
        results.append(test(
            f"Directory: {dirn}/",
            dir_path.exists(),
            f"Run: mkdir -p {dir_path}"
        ))
    
    # 7. Network connectivity
    try:
        result = subprocess.run(
            ['curl', '-s', 'https://api.binance.com/api/v3/ping', '-m', '3'],
            capture_output=True,
            timeout=5
        )
        binance_ok = result.returncode == 0
        results.append(test(
            "Network: api.binance.com",
            binance_ok,
            "Unable to reach Binance API (check firewall/proxy)"
        ))
    except Exception as e:
        results.append(test(
            "Network: api.binance.com",
            False,
            f"Error: {e}"
        ))
    
    # 8. Disk space
    try:
        stat = os.statvfs(project_root)
        free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
        results.append(test(
            "Disk space available",
            free_gb > 50,
            f"Only {free_gb:.1f}GB free (recommended: 200GB)"
        ))
    except Exception as e:
        results.append(test("Disk space available", False, str(e)))
    
    # Summary
    print(f"\n{YELLOW}=== Summary ==={RESET}")
    passed = sum(results)
    total = len(results)
    percentage = (passed / total * 100) if total > 0 else 0
    
    if passed == total:
        print(f"{GREEN}All checks passed! ({passed}/{total}){RESET}")
        print(f"\n{YELLOW}Next steps:{RESET}")
        print("  1. Start recorder: python recorder.py")
        print("  2. Monitor: tail -f recorder.log")
        print("  3. Check health: tail -f state/heartbeat.json")
        return 0
    else:
        print(f"{RED}Some checks failed ({passed}/{total}){RESET}")
        print(f"\nFix the issues above and re-run: python test_setup.py")
        return 1

if __name__ == '__main__':
    sys.exit(main())
