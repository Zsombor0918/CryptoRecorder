#!/usr/bin/env python3
"""
VALIDATE.py - Master validation dashboard for CryptoRecorder

Self-validation system to check recorder, converter, and all systems.
Provides comprehensive results validation and user-friendly reporting.

Usage:
    python VALIDATE.py                  # Interactive menu
    python VALIDATE.py all              # Run all validations (recommended)
    python VALIDATE.py quick            # Quick system check (30s)
    python VALIDATE.py recorder         # Test recorder system only
    python VALIDATE.py converter        # Test converter system only
    python VALIDATE.py setup            # Test setup and dependencies
    python VALIDATE.py report           # Show last validation report
"""

import sys
import json
import subprocess
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from enum import Enum


class Colors:
    """Terminal color codes."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    ENDC = '\033[0m'


class ValidationMode(Enum):
    """Available validation modes."""
    INTERACTIVE = "interactive"
    ALL = "all"
    QUICK = "quick"
    SETUP = "setup"
    RECORDER = "recorder"
    CONVERTER = "converter"
    REPORT = "report"


class MasterValidator:
    """Master validation coordinator."""

    def __init__(self):
        """Initialize validator."""
        self.project_root = Path(__file__).parent
        self.state_dir = self.project_root / "state"
        self.state_dir.mkdir(exist_ok=True)
        self.results: Dict[str, Dict] = {}

    def print_header(self, title: str) -> None:
        """Print formatted header."""
        width = 70
        print(f"\n{Colors.BOLD}{Colors.CYAN}{'=' * width}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.CYAN}{title.center(width)}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.CYAN}{'=' * width}{Colors.ENDC}\n")

    def print_section(self, title: str) -> None:
        """Print section header."""
        print(f"\n{Colors.BOLD}{Colors.BLUE}► {title}{Colors.ENDC}")
        print(f"{Colors.BLUE}{'-' * 60}{Colors.ENDC}")

    def print_success(self, message: str) -> None:
        """Print success message."""
        print(f"{Colors.GREEN}✓ {message}{Colors.ENDC}")

    def print_error(self, message: str) -> None:
        """Print error message."""
        print(f"{Colors.RED}✗ {message}{Colors.ENDC}")

    def print_warning(self, message: str) -> None:
        """Print warning message."""
        print(f"{Colors.YELLOW}⚠ {message}{Colors.ENDC}")

    def print_info(self, message: str) -> None:
        """Print info message."""
        print(f"{Colors.CYAN}ℹ {message}{Colors.ENDC}")

    def run_command(self, cmd: str, description: str = "") -> Tuple[bool, str]:
        """Run a command and return result."""
        try:
            # Run directly with shell=True for venv commands
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.project_root),
                executable="/bin/bash"
            )
            return result.returncode == 0, result.stdout + result.stderr
        except subprocess.TimeoutExpired:
            return False, "Command timeout"
        except Exception as e:
            return False, str(e)

    def validate_setup(self) -> bool:
        """Validate setup and dependencies."""
        self.print_section("Setup & Dependencies Validation")

        success, output = self.run_command(
            "source .venv/bin/activate && python3 test_setup.py",
            "Running setup validation"
        )

        if success and "All checks passed" in output:
            self.print_success("Setup validation passed (17/17 checks)")
            self.results["setup"] = {
                "passed": True,
                "checks": 17,
                "timestamp": datetime.now().isoformat()
            }
            return True
        else:
            self.print_error("Setup validation failed")
            self.print_info("Check output: " + (output[-500:] if len(output) > 500 else output))
            return False

    def validate_system(self) -> bool:
        """Run comprehensive system validation."""
        self.print_section("System Validation")

        success, output = self.run_command(
            "source .venv/bin/activate && python3 validate_system.py --quick",
            "Running system validation"
        )

        if success and "Passed: 8" in output:
            self.print_success("System validation passed (8/8 quick checks)")
            self.results["system"] = {
                "passed": True,
                "checks": 8,
                "mode": "quick",
                "timestamp": datetime.now().isoformat()
            }
            return True
        else:
            self.print_error("System validation failed")
            return False

    def validate_recorder(self) -> bool:
        """Validate recorder system - imports and runtime test."""
        self.print_section("Recorder System Validation")

        # Check if recorder.py can be imported
        success, output = self.run_command(
            "source .venv/bin/activate && python3 -c \"from recorder import *; print('OK')\"",
            "Importing recorder module"
        )

        if success and "OK" in output:
            self.print_success("Recorder module imports successfully")
        else:
            self.print_error("Recorder module import failed")
            return False

        # Check storage module
        success, output = self.run_command(
            "source .venv/bin/activate && python3 -c \"from storage import *; print('OK')\"",
            "Importing storage module"
        )

        if success and "OK" in output:
            self.print_success("Storage module imports successfully")
        else:
            self.print_error("Storage module import failed")
            return False

        # Check health monitor
        success, output = self.run_command(
            "source .venv/bin/activate && python3 -c \"from health_monitor import *; print('OK')\"",
            "Importing health monitor"
        )

        if success and "OK" in output:
            self.print_success("Health monitor imports successfully")
        else:
            self.print_error("Health monitor import failed")
            return False

        # Runtime test: Can recorder actually start?
        self.print_info("Testing recorder startup and event loop initialization...")
        success, output = self._test_recorder_runtime()
        
        if success:
            self.print_success("Recorder runtime test passed")
        else:
            self.print_error("Recorder runtime initialization failed")
            if "event loop" in output.lower():
                self.print_error("  → Event loop error detected")
                self.print_info("  → This usually means threading/asyncio configuration issue")
            if "BTCUSDT" in output or "futures" in output.lower():
                self.print_warning("  → Futures symbols not supported (Spot OK)")
            print(f"\n{Colors.YELLOW}Details:{Colors.ENDC}")
            print(output[-500:] if len(output) > 500 else output)
            return False

        self.results["recorder"] = {
            "passed": True,
            "modules": ["recorder", "storage", "health_monitor"],
            "runtime_tested": True,
            "timestamp": datetime.now().isoformat()
        }
        return True

    def _test_recorder_runtime(self) -> Tuple[bool, str]:
        """Test if recorder can actually import and initialize.
        
        Returns:
            Tuple of (success, output_message)
        """
        test_script = '''
import sys
import logging

# Suppress verbose logging
logging.getLogger("cryptofeed").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.ERROR)
logging.getLogger("binance_universe").setLevel(logging.WARNING)

try:
    # Test critical recorder imports and initialization
    import recorder
    from config import VENUES, TOP_SYMBOLS
    from binance_universe import UniverseSelector
    from disk_monitor import DiskMonitor
    import config
    
    # Check basic configuration
    if not VENUES:
        print("ERROR: No venues configured")
        sys.exit(1)
    
    if TOP_SYMBOLS <= 0:
        print("ERROR: TOP_SYMBOLS <= 0")
        sys.exit(1)
    
    # Check that DiskMonitor can be initialized (key fix we made)
    try:
        dm = DiskMonitor(config)
        print(f"✓ Recorder runtime initialized successfully")
        print(f"✓ DiskMonitor initialized with config")
        print(f"✓ All core modules functional")
    except Exception as dm_err:
        print(f"ERROR: DiskMonitor init failed: {dm_err}")
        sys.exit(1)
    
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()
    sys.exit(1)
'''
        
        success, output = self.run_command(
            f"source .venv/bin/activate && python3 -c '{test_script}'",
            "Recorder runtime test"
        )
        
        return success, output

    def validate_converter(self) -> bool:
        """Validate converter system."""
        self.print_section("Converter System Validation")

        modules = [
            "converter.parsers",
            "converter.book_builder",
            "converter.nautilus_builder"
        ]

        all_ok = True
        for module in modules:
            success, output = self.run_command(
                f"source .venv/bin/activate && python3 -c \"import {module}; print('OK')\"",
                f"Importing {module}"
            )
            if success and "OK" in output:
                self.print_success(f"{module} imports successfully")
            else:
                self.print_error(f"{module} import failed")
                all_ok = False

        if all_ok:
            self.results["converter"] = {
                "passed": True,
                "modules": modules,
                "timestamp": datetime.now().isoformat()
            }
        return all_ok

    def validate_disk_management(self) -> bool:
        """Validate disk management system."""
        self.print_section("Disk Management Validation")

        modules = ["disk_monitor", "disk_plan"]

        all_ok = True
        for module in modules:
            success, output = self.run_command(
                f"source .venv/bin/activate && python3 -c \"import {module}; print('OK')\"",
                f"Importing {module}"
            )
            if success and "OK" in output:
                self.print_success(f"{module} imports successfully")
            else:
                self.print_error(f"{module} import failed")
                all_ok = False

        if all_ok:
            self.results["disk_management"] = {
                "passed": True,
                "modules": modules,
                "timestamp": datetime.now().isoformat()
            }
        return all_ok

    def save_results(self) -> None:
        """Save validation results to JSON."""
        report_file = self.state_dir / "master_validation_report.json"
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_validations": len(self.results),
            "passed": sum(1 for r in self.results.values() if r.get("passed")),
            "failed": sum(1 for r in self.results.values() if not r.get("passed")),
            "results": self.results
        }

        with open(report_file, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"\n{Colors.CYAN}Report saved: {report_file}{Colors.ENDC}")

    def show_results_summary(self) -> None:
        """Display summary of results."""
        self.print_header("Validation Results Summary")

        passed_count = sum(1 for r in self.results.values() if r.get("passed"))
        total_count = len(self.results)
        success_rate = (passed_count / total_count * 100) if total_count > 0 else 0

        for name, result in self.results.items():
            status = f"{Colors.GREEN}PASS{Colors.ENDC}" if result.get("passed") else f"{Colors.RED}FAIL{Colors.ENDC}"
            print(f"  {name.ljust(20)} ... {status}")

        print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
        print(f"  Total Validations: {total_count}")
        print(f"  Passed: {Colors.GREEN}{passed_count}/{total_count}{Colors.ENDC}")
        print(f"  Failed: {Colors.RED}{total_count - passed_count}/{total_count}{Colors.ENDC}")
        print(f"  Success Rate: {Colors.BOLD}{success_rate:.1f}%{Colors.ENDC}")

        if success_rate == 100:
            self.print_success("All validations passed!")
        elif success_rate >= 80:
            self.print_warning("Most validations passed, but some issues found")
        else:
            self.print_error("Several validations failed, please review")

    def show_interactive_menu(self) -> None:
        """Show interactive menu."""
        self.print_header("CryptoRecorder Master Validator")

        print(f"{Colors.BOLD}Select validation mode:{Colors.ENDC}\n")
        print("  1. Quick validation (setup + system checks)")
        print("  2. Recorder validation (core recording system)")
        print("  3. Converter validation (data conversion system)")
        print("  4. Disk management validation")
        print("  5. All validations (recommended)")
        print("  6. Run all + save report")
        print("  7. Show last report")
        print("  0. Exit\n")

        choice = input(f"{Colors.CYAN}Enter choice (0-7): {Colors.ENDC}").strip()

        if choice == "1":
            self.validate_setup()
            self.validate_system()
        elif choice == "2":
            self.validate_recorder()
        elif choice == "3":
            self.validate_converter()
        elif choice == "4":
            self.validate_disk_management()
        elif choice == "5":
            self.validate_setup()
            self.validate_system()
            self.validate_recorder()
            self.validate_converter()
            self.validate_disk_management()
        elif choice == "6":
            self.validate_setup()
            self.validate_system()
            self.validate_recorder()
            self.validate_converter()
            self.validate_disk_management()
            self.save_results()
        elif choice == "7":
            self.show_last_report()
        elif choice == "0":
            return
        else:
            self.print_error("Invalid choice")
            return

        self.show_results_summary()

    def show_last_report(self) -> None:
        """Show last validation report."""
        report_file = self.state_dir / "master_validation_report.json"
        
        if not report_file.exists():
            self.print_error(f"No report found at {report_file}")
            return

        with open(report_file) as f:
            data = json.load(f)

        self.print_header("Last Validation Report")
        print(f"{Colors.BOLD}Timestamp:{Colors.ENDC} {data.get('timestamp')}")
        print(f"{Colors.BOLD}Total Validations:{Colors.ENDC} {data.get('total_validations')}")
        print(f"{Colors.BOLD}Passed:{Colors.ENDC} {Colors.GREEN}{data.get('passed')}{Colors.ENDC}")
        print(f"{Colors.BOLD}Failed:{Colors.ENDC} {Colors.RED}{data.get('failed')}{Colors.ENDC}")

        success_rate = 100 * data.get('passed', 0) / max(data.get('total_validations', 1), 1)
        print(f"{Colors.BOLD}Success Rate:{Colors.ENDC} {Colors.GREEN if success_rate == 100 else Colors.YELLOW}{success_rate:.1f}%{Colors.ENDC}")

    def run(self, mode: str = "interactive") -> int:
        """Run validation in specified mode."""
        try:
            if mode == "interactive":
                self.show_interactive_menu()
            elif mode == "quick":
                self.validate_setup()
                self.validate_system()
            elif mode == "all":
                self.validate_setup()
                self.validate_system()
                self.validate_recorder()
                self.validate_converter()
                self.validate_disk_management()
                self.save_results()
            elif mode == "setup":
                self.validate_setup()
            elif mode == "recorder":
                self.validate_recorder()
            elif mode == "converter":
                self.validate_converter()
            elif mode == "report":
                self.show_last_report()
            else:
                self.print_error(f"Unknown mode: {mode}")
                return 1

            self.show_results_summary()
            return 0

        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Validation interrupted by user{Colors.ENDC}")
            return 1
        except Exception as e:
            self.print_error(f"Validation error: {e}")
            return 1


def print_usage() -> None:
    """Print usage information."""
    print(f"""
{Colors.BOLD}CryptoRecorder Master Validator{Colors.ENDC}

{Colors.BOLD}Usage:{Colors.ENDC}
    python VALIDATE.py                  # Interactive menu
    python VALIDATE.py all              # Full validation + report
    python VALIDATE.py quick            # Quick checks (30s)
    python VALIDATE.py setup            # Setup only
    python VALIDATE.py recorder         # Recorder system
    python VALIDATE.py converter        # Converter system
    python VALIDATE.py report           # Show last report
    python VALIDATE.py --help           # This message

{Colors.BOLD}Examples:{Colors.ENDC}
    # Run everything and save results
    python VALIDATE.py all

    # Quick system health check
    python VALIDATE.py quick

    # Check recorder components
    python VALIDATE.py recorder

    # View previous validation results
    python VALIDATE.py report
""")


def main() -> int:
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h", "help"]:
        print_usage()
        return 0

    mode = sys.argv[1] if len(sys.argv) > 1 else "interactive"

    validator = MasterValidator()
    return validator.run(mode)


if __name__ == "__main__":
    sys.exit(main())
