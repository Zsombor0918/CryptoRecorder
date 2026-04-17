#!/usr/bin/env python3
"""
run_validators.py - User-friendly test runner for CryptoRecorder system.

Quick access to all validation and testing tools:
  - System validation (validate_system.py)
  - Setup validation (test_setup.py)
  - Disk usage reporting (disk_plan.py)

Usage:
    python run_validators.py              # Interactive menu
    python run_validators.py full         # Run all validators
    python run_validators.py quick        # Quick checks
    python run_validators.py --help       # Show options

This script provides an easy way to validate the entire system
without needing to remember individual command names.
"""

import sys
import subprocess
import argparse
from pathlib import Path
from enum import Enum
from typing import List


class Colors:
    """Terminal colors."""
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


class ValidatorMode(Enum):
    """Available validation modes."""
    SYSTEM_QUICK = ("quick", "Quick system checks", "python validate_system.py --quick")
    SYSTEM_FULL = ("full", "Complete system validation", "python validate_system.py --verbose")
    SYSTEM_AUTO_FIX = ("fix", "Auto-fix missing directories", "python validate_system.py --fix")
    SETUP_TEST = ("setup", "Setup and dependency test", "python test_setup.py")
    DISK_PLAN = ("disk", "Generate disk retention plan", "python disk_plan.py")
    ALL_VALIDATORS = ("all", "Run all validators sequentially", None)


class ValidatorRunner:
    """User-friendly validator runner."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
    
    def print_header(self, title: str, color: str = Colors.BLUE):
        """Print formatted header."""
        width = 70
        print(f"\n{color}{Colors.BOLD}")
        print("=" * width)
        print(title.center(width))
        print("=" * width)
        print(Colors.ENDC)
    
    def print_section(self, title: str):
        """Print formatted section."""
        print(f"\n{Colors.BOLD}{title}{Colors.ENDC}")
        print("─" * 70)
    
    def print_option(self, key: str, description: str, command: str = None):
        """Print a menu option."""
        key_str = f"{Colors.GREEN}{key:<8}{Colors.ENDC}"
        desc_str = f"{description:<40}"
        if command:
            cmd_str = f"{Colors.YELLOW}{command}{Colors.ENDC}"
            print(f"  {key_str} {desc_str} {cmd_str}")
        else:
            print(f"  {key_str} {desc_str}")
    
    def run_command(self, description: str, command: str) -> bool:
        """Run a command and return success status."""
        print(f"\n{Colors.BOLD}Running: {description}{Colors.ENDC}")
        print(f"Command: {Colors.YELLOW}{command}{Colors.ENDC}")
        print("─" * 70)
        
        try:
            result = subprocess.run(command, shell=True, cwd=self.project_root)
            print()
            if result.returncode == 0:
                print(f"{Colors.GREEN}✓ Success{Colors.ENDC}")
            else:
                print(f"{Colors.RED}✗ Failed with exit code {result.returncode}{Colors.ENDC}")
            return result.returncode == 0
        except Exception as e:
            print(f"{Colors.RED}✗ Error: {e}{Colors.ENDC}")
            return False
    
    def show_menu(self):
        """Show interactive menu."""
        self.print_header("CryptoRecorder Test & Validation Suite")
        
        print(f"""
{Colors.GREEN}Quick Validation Options:{Colors.ENDC}
""")
        
        self.print_option("1", "Quick system checks", "(30 seconds)")
        self.print_option("2", "Full system validation", "(2 minutes)")
        self.print_option("3", "Setup & dependencies", "(1 minute)")
        self.print_option("4", "Disk retention plan", "(5 seconds)")
        self.print_option("5", "Auto-fix issues", "(auto-create dirs)")
        
        print(f"""
{Colors.BLUE}Batch Options:{Colors.ENDC}
""")
        
        self.print_option("6", "Run all validators", "(5 minutes total)")
        self.print_option("7", "Show results", "(view last report)")
        
        print(f"""
{Colors.YELLOW}Info Options:{Colors.ENDC}
""")
        
        self.print_option("8", "System status")
        self.print_option("9", "Help & documentation")
        self.print_option("0", "Exit")
        
        print()
    
    def show_status(self):
        """Show system status."""
        self.print_header("System Status", Colors.BLUE)
        
        status_items = [
            ("Project Root", str(self.project_root)),
            ("Python Version", f"{sys.version.split()[0]}"),
            ("Working Directory", str(Path.cwd())),
        ]
        
        # Check key files
        key_files = {
            "recorder.py": "Recording system",
            "disk_monitor.py": "Disk monitoring",
            "disk_plan.py": "Disk planning",
            "converter/": "Conversion system",
            "systemd/": "Service files",
            "state/": "State directory",
            "config.py": "Configuration",
        }
        
        print("\n" + Colors.BOLD + "Files/Directories:" + Colors.ENDC)
        for file_path, description in key_files.items():
            full_path = self.project_root / file_path
            exists = "✓" if full_path.exists() else "✗"
            status = Colors.GREEN if full_path.exists() else Colors.RED
            print(f"  {status}{exists}{Colors.ENDC} {file_path:<20} - {description}")
        
        print("\n" + Colors.BOLD + "Reports:" + Colors.ENDC)
        report_files = [
            "state/validation_report.json",
            "state/disk_usage.json",
            "state/disk_plan_report.json",
        ]
        
        for report in report_files:
            full_path = self.project_root / report
            if full_path.exists():
                size = full_path.stat().st_size
                print(f"  {Colors.GREEN}✓{Colors.ENDC} {report:<35} ({size} bytes)")
            else:
                print(f"  {Colors.RED}·{Colors.ENDC} {report:<35} (not yet generated)")
    
    def show_results(self):
        """Show last validation results."""
        report_file = self.project_root / "state" / "validation_report.json"
        
        if not report_file.exists():
            print(f"{Colors.YELLOW}No validation report found yet.{Colors.ENDC}")
            print("Run validation with: python run_validators.py full")
            return
        
        self.print_header("Last Validation Results", Colors.GREEN)
        
        import json
        try:
            with open(report_file) as f:
                report = json.load(f)
            
            print(f"\nTimestamp: {report['timestamp']}")
            print(f"Total Tests: {report['total_tests']}")
            print(f"Passed: {Colors.GREEN}{report['passed']}✓{Colors.ENDC}")
            print(f"Failed: {Colors.RED}{report['failed']}✗{Colors.ENDC}")
            print(f"Success Rate: {Colors.BOLD}{report['success_rate']:.1f}%{Colors.ENDC}")
            
            if report['failed'] > 0:
                print(f"\n{Colors.YELLOW}Failed Tests:{Colors.ENDC}")
                for test_name, result in report['details'].items():
                    if result['status'] == 'fail':
                        print(f"  ✗ {test_name}")
                        print(f"    {result['message']}")
        except Exception as e:
            print(f"{Colors.RED}Error reading report: {e}{Colors.ENDC}")
    
    def show_help(self):
        """Show help and documentation."""
        self.print_header("Help & Documentation", Colors.BLUE)
        
        print("""
CryptoRecorder consists of three main systems:

1. RECORDING SYSTEM (recorder.py)
   - Captures real-time Binance market data
   - Records L2 depth deltas, trades, snapshots
   - 24/7 operation with auto-reconnect
   → Validation: Quick or Full system checks

2. DISK MANAGEMENT (disk_monitor.py + disk_plan.py)
   - Real-time disk usage monitoring
   - Automatic cleanup when thresholds exceeded
   - Data-driven retention recommendations
   → Validation: Disk retention plan

3. CONVERSION SYSTEM (converter/)
   - Transforms raw JSONL to Nautilus format
   - Daily batch processing
   - Parquet output for backtesting
   → Validation: Full system checks

Quick Start:
  1. python run_validators.py quick    # Fast validation
  2. python run_validators.py full     # Complete test
  3. python recorder.py                # Start recording

Documentation:
  - README.md              Main documentation
  - QUICKSTART.md          Quick start guide
  - DISK_MANAGEMENT.md     Disk system overview
  - IMPLEMENTATION_SUMMARY.md  Full technical details

For detailed help, see: QUICKSTART.md
""")
    
    def run_single(self, mode_key: str) -> bool:
        """Run a single validator mode."""
        # Map keys to modes
        modes_by_key = {m.value[0]: m for m in ValidatorMode}
        
        if mode_key not in modes_by_key:
            print(f"{Colors.RED}Unknown mode: {mode_key}{Colors.ENDC}")
            return False
        
        mode = modes_by_key[mode_key]
        
        if mode == ValidatorMode.ALL_VALIDATORS:
            return self.run_all_validators()
        
        _, description, command = mode.value
        return self.run_command(description, command)
    
    def run_all_validators(self) -> bool:
        """Run all validators sequentially."""
        self.print_header("Running All Validators", Colors.BLUE)
        
        results = {}
        validators = [
            ValidatorMode.SYSTEM_QUICK,
            ValidatorMode.SETUP_TEST,
            ValidatorMode.DISK_PLAN,
            ValidatorMode.SYSTEM_FULL,
        ]
        
        for validator in validators:
            _, description, command = validator.value
            results[description] = self.run_command(description, command)
        
        # Summary
        self.print_header("All Validators Complete", Colors.GREEN)
        print()
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        
        for description, success in results.items():
            status = Colors.GREEN + "✓" if success else Colors.RED + "✗"
            print(f"  {status}{Colors.ENDC} {description}")
        
        print(f"\n{Colors.BOLD}Overall: {passed}/{total} validators passed{Colors.ENDC}\n")
        
        return passed == total
    
    def interactive_mode(self):
        """Run interactive menu mode."""
        while True:
            self.show_menu()
            
            choice = input(f"{Colors.BOLD}Enter option (0-9): {Colors.ENDC}").strip()
            
            if choice == "0":
                print(f"\n{Colors.BLUE}Goodbye!{Colors.ENDC}\n")
                return
            
            elif choice == "1":
                self.run_single("quick")
            elif choice == "2":
                self.run_single("full")
            elif choice == "3":
                self.run_single("setup")
            elif choice == "4":
                self.run_single("disk")
            elif choice == "5":
                self.run_single("fix")
            elif choice == "6":
                self.run_all_validators()
            elif choice == "7":
                self.show_results()
            elif choice == "8":
                self.show_status()
            elif choice == "9":
                self.show_help()
            else:
                print(f"{Colors.RED}Invalid option{Colors.ENDC}")
            
            input(f"\n{Colors.YELLOW}Press Enter to continue...{Colors.ENDC}")
    
    def run(self, argv: List[str] = None):
        """Main entry point."""
        parser = argparse.ArgumentParser(
            description="CryptoRecorder Test & Validation Suite",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  python run_validators.py              # Interactive menu
  python run_validators.py full         # Full validation
  python run_validators.py quick        # Quick checks
  python run_validators.py all          # All validators
  python run_validators.py status       # System status
  python run_validators.py help         # Show help

Modes:
  quick      Quick system checks (30s)
  full       Complete validation (2min)
  setup      Setup test (1min)
  disk       Disk plan (5s)
  fix        Auto-fix issues
  all        All validators (5min)
  status     Show system status
  help       Show this help
  results    Show last results
            """
        )
        
        parser.add_argument('mode', nargs='?', help='Validation mode')
        
        args = parser.parse_args(argv)
        
        if not args.mode:
            self.interactive_mode()
        elif args.mode == "status":
            self.show_status()
        elif args.mode == "results":
            self.show_results()
        elif args.mode == "help":
            parser.print_help()
        elif args.mode == "all":
            self.run_all_validators()
        else:
            self.run_single(args.mode)


def main():
    """Main entry point."""
    runner = ValidatorRunner()
    runner.run(sys.argv[1:] if len(sys.argv) > 1 else None)


if __name__ == '__main__':
    main()
