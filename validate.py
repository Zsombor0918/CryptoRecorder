#!/usr/bin/env python3
"""
validate.py — Setup validation for CryptoRecorder.

This script checks if your environment is correctly configured to run CryptoRecorder.
Run this after cloning the repo or setting up on a new machine.

Usage:
    python validate.py           # Full validation
    python validate.py --quick   # Quick dependency check only

What it checks:
    1. Python dependencies (nautilus_trader, aiohttp, etc.)
    2. Project structure (required directories exist)
    3. Configuration (config.py loads correctly)
    4. Core modules (can be imported)

This is NOT for testing functionality - use pytest for that:
    pytest tests/

For operational acceptance tests, see scripts/:
    python scripts/smoke_test.py      # 3-min recorder smoke test
    python scripts/acceptance_test.py # Full acceptance test
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


# ══════════════════════════════════════════════════════════════════════════════
# Colors for terminal output
# ══════════════════════════════════════════════════════════════════════════════

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

    @staticmethod
    def ok(text: str) -> str:
        return f"{Colors.GREEN}✓{Colors.RESET} {text}"

    @staticmethod
    def fail(text: str) -> str:
        return f"{Colors.RED}✗{Colors.RESET} {text}"

    @staticmethod
    def warn(text: str) -> str:
        return f"{Colors.YELLOW}!{Colors.RESET} {text}"

    @staticmethod
    def header(text: str) -> str:
        return f"\n{Colors.BOLD}{text}{Colors.RESET}"


# ══════════════════════════════════════════════════════════════════════════════
# Validation checks
# ══════════════════════════════════════════════════════════════════════════════

def check_python_version() -> tuple[bool, str]:
    """Check Python version is 3.10+."""
    version = sys.version_info
    ok = version >= (3, 10)
    msg = f"Python {version.major}.{version.minor}.{version.micro}"
    if not ok:
        msg += " (need 3.10+)"
    return ok, msg


def check_dependencies() -> list[tuple[str, bool, str]]:
    """Check required Python packages."""
    packages = [
        ("nautilus_trader", "nautilus_trader"),
        ("aiohttp", "aiohttp"),
        ("zstandard", "zstandard"),
        ("pandas", "pandas"),
        ("pyarrow", "pyarrow"),
    ]

    results = []
    for import_name, display_name in packages:
        try:
            __import__(import_name)
            results.append((display_name, True, "installed"))
        except ImportError as e:
            results.append((display_name, False, f"missing: {e}"))

    return results


def check_directories() -> list[tuple[str, bool, str]]:
    """Check required directories exist."""
    required = [
        ("data_raw", "Raw data storage"),
        ("state", "Runtime state"),
        ("meta", "Metadata storage"),
        ("converter", "Converter package"),
        ("tests", "Unit tests"),
    ]

    results = []
    for dirname, desc in required:
        path = PROJECT_ROOT / dirname
        exists = path.exists()
        results.append((dirname, exists, desc if exists else "MISSING"))

    return results


def check_config() -> tuple[bool, str]:
    """Check config.py loads correctly."""
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        from config import VENUES, TOP_SYMBOLS, DATA_ROOT

        if not VENUES:
            return False, "VENUES is empty"
        if TOP_SYMBOLS <= 0:
            return False, f"TOP_SYMBOLS={TOP_SYMBOLS} (should be >0)"
        return True, f"OK: {len(VENUES)} venues, {TOP_SYMBOLS} symbols"
    except Exception as e:
        return False, f"Failed: {e}"


def check_core_modules() -> list[tuple[str, bool, str]]:
    """Check core modules can be imported."""
    modules = [
        ("recorder", "Recorder"),
        ("storage", "Storage manager"),
        ("health_monitor", "Health monitor"),
        ("binance_universe", "Universe selector"),
        ("convert_day", "Converter CLI"),
        ("converter.trades", "Trade conversion"),
        ("converter.depth_phase2", "Depth replay"),
        ("converter.instruments", "Instrument builder"),
    ]

    results = []
    for module_name, desc in modules:
        try:
            __import__(module_name)
            results.append((desc, True, "OK"))
        except Exception as e:
            results.append((desc, False, str(e)[:50]))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Main validation runner
# ══════════════════════════════════════════════════════════════════════════════

def run_validation(quick: bool = False) -> bool:
    """Run all validation checks."""
    print(Colors.header("═" * 60))
    print(Colors.header("  CryptoRecorder Setup Validation"))
    print(Colors.header("═" * 60))

    all_passed = True

    # Python version
    print(Colors.header("\n1. Python Version"))
    ok, msg = check_python_version()
    print(f"   {Colors.ok(msg) if ok else Colors.fail(msg)}")
    all_passed &= ok

    # Dependencies
    print(Colors.header("\n2. Dependencies"))
    for name, ok, msg in check_dependencies():
        status = Colors.ok(name) if ok else Colors.fail(f"{name}: {msg}")
        print(f"   {status}")
        all_passed &= ok

    if quick:
        print(Colors.header("\n[Quick mode - skipping detailed checks]"))
        return all_passed

    # Directories
    print(Colors.header("\n3. Project Structure"))
    for name, ok, msg in check_directories():
        status = Colors.ok(f"{name}/ — {msg}") if ok else Colors.fail(f"{name}/ — {msg}")
        print(f"   {status}")
        all_passed &= ok

    # Config
    print(Colors.header("\n4. Configuration"))
    ok, msg = check_config()
    print(f"   {Colors.ok(msg) if ok else Colors.fail(msg)}")
    all_passed &= ok

    # Core modules
    print(Colors.header("\n5. Core Modules"))
    for name, ok, msg in check_core_modules():
        status = Colors.ok(name) if ok else Colors.fail(f"{name}: {msg}")
        print(f"   {status}")
        all_passed &= ok

    # Summary
    print(Colors.header("\n" + "═" * 60))
    if all_passed:
        print(Colors.ok("All checks passed! Environment is ready."))
        print("\nNext steps:")
        print("  • Run unit tests:     pytest tests/")
        print("  • Start recorder:     python recorder.py")
        print("  • Run smoke test:     python scripts/smoke_test.py")
    else:
        print(Colors.fail("Some checks failed. Please fix the issues above."))
        print("\nCommon fixes:")
        print("  • Install dependencies: pip install -r requirements.txt")
        print("  • Create directories:   mkdir -p data_raw state meta")
    print("═" * 60)

    return all_passed


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate CryptoRecorder setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate.py          Full validation
  python validate.py --quick  Quick dependency check only

After validation passes:
  pytest tests/               Run unit tests
  python recorder.py          Start the recorder
  python scripts/smoke_test.py  Run operational smoke test
        """,
    )
    parser.add_argument("--quick", action="store_true", help="Quick dependency check only")

    args = parser.parse_args()

    success = run_validation(quick=args.quick)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
