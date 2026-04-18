#!/usr/bin/env python3
"""
validate_system.py - Comprehensive system validation for CryptoRecorder.

Validates:
  - All imports and dependencies
  - Configuration loading and settings
  - Disk monitor functionality
  - Recorder integration
  - Converter script
  - All relative paths and directories
  - State directory creation
  - Async functionality

Usage:
    python validators/validate_system.py              # Run all validations
    python validators/validate_system.py --quick      # Quick checks only
    python validators/validate_system.py --verbose    # Detailed output
    python validators/validate_system.py --fix        # Auto-create missing dirs

Output:
    - Console report with ✓/✗ indicators
    - state/validation_report.json (detailed results)
    - state/validation.log (debug logs)
"""

import sys
import json
import logging
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Ensure project root is on sys.path so `from config import ...` works
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))


class SystemValidator:
    """Comprehensive system validator."""
    
    def __init__(self, verbose: bool = False, quick: bool = False, fix: bool = False):
        self.verbose = verbose
        self.quick = quick
        self.fix = fix
        self.project_root = Path(__file__).resolve().parent.parent
        self.results = {}
        self.passed = 0
        self.failed = 0
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging to file and console."""
        state_dir = self.project_root / "state"
        state_dir.mkdir(exist_ok=True)
        
        log_file = state_dir / "validation.log"
        
        logging.basicConfig(
            level=logging.DEBUG if self.verbose else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(),
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def test(self, name: str, func, *args, **kwargs) -> bool:
        """Run a test and record result."""
        try:
            result = func(*args, **kwargs)
            self.results[name] = {"status": "pass", "message": result}
            self.passed += 1
            status = "✓"
            print(f"{status} {name}")
            if self.verbose and result:
                print(f"  └─ {result}")
            return True
        except Exception as e:
            self.results[name] = {"status": "fail", "message": str(e)}
            self.failed += 1
            status = "✗"
            print(f"{status} {name}")
            if self.verbose:
                print(f"  └─ Error: {e}")
            self.logger.error(f"{name}: {e}", exc_info=True)
            return False
    
    def run_all(self) -> bool:
        """Run all validation tests."""
        print("\n" + "=" * 70)
        print("CryptoRecorder System Validation")
        print("=" * 70 + "\n")
        
        # Phase 1: Dependencies
        print("──────── Phase 1: Dependencies & Imports ────────")
        self.validate_dependencies()
        
        if self.quick:
            print("\n(Quick mode: skipping detailed checks)")
            return self.report_summary()
        
        # Phase 2: Configuration
        print("\n──────── Phase 2: Configuration ────────")
        self.validate_configuration()
        
        # Phase 3: Core Modules
        print("\n──────── Phase 3: Core Modules ────────")
        self.validate_core_modules()
        
        # Phase 4: Disk Management
        print("\n──────── Phase 4: Disk Management ────────")
        self.validate_disk_management()
        
        # Phase 5: Directories & Paths
        print("\n──────── Phase 5: Directories & Paths ────────")
        self.validate_directories()
        
        # Phase 6: Recorder Integration
        print("\n──────── Phase 6: Recorder Integration ────────")
        self.validate_recorder()
        
        # Phase 7: Converter System
        print("\n──────── Phase 7: Converter System ────────")
        self.validate_converter()
        
        # Phase 8: Async Functionality
        print("\n──────── Phase 8: Async Functionality ────────")
        self.validate_async()
        
        return self.report_summary()
    
    def validate_dependencies(self):
        """Validate required dependencies are installed."""
        required = [
            'asyncio', 'aiohttp', 'cryptofeed', 'zstandard',
            'yaml', 'pandas', 'pyarrow', 'logging', 'nautilus_trader',
        ]
        
        for pkg in required:
            try:
                __import__(pkg)
                self.test(
                    f"Import {pkg}",
                    lambda: f"✓ {pkg} available"
                )
            except ImportError:
                self.results[f"Import {pkg}"] = {
                    "status": "fail",
                    "message": f"{pkg} not installed"
                }
                self.failed += 1
                print(f"✗ Import {pkg}")
    
    def validate_configuration(self):
        """Validate configuration can be loaded."""
        def check_config():
            from config import (
                VENUES, TOP_SYMBOLS, DEPTH_INTERVAL_MS,
                DISK_SOFT_LIMIT_GB, DISK_CLEANUP_TARGET_GB
            )
            assert VENUES, "VENUES not defined"
            assert TOP_SYMBOLS > 0, "TOP_SYMBOLS must be > 0"
            assert DISK_SOFT_LIMIT_GB > 0, "DISK_SOFT_LIMIT_GB not set"
            return f"Loaded: {len(VENUES)} venues, {TOP_SYMBOLS} symbols"
        
        self.test("Load config.py", check_config)
        
        def check_yaml():
            config_yaml = self.project_root / 'config.yaml'
            if config_yaml.exists():
                import yaml
                with open(config_yaml) as f:
                    cfg = yaml.safe_load(f)
                assert cfg, "config.yaml is empty"
                return f"Loaded: {len(cfg)} sections"
            return "config.yaml not found (optional)"
        
        self.test("Load config.yaml", check_yaml)
    
    def validate_core_modules(self):
        """Validate core modules import correctly."""
        modules = [
            ('storage', 'StorageManager'),
            ('health_monitor', 'HealthMonitor'),
            ('binance_universe', 'UniverseSelector'),
            ('disk_monitor', 'DiskMonitor'),
        ]
        
        for module_name, class_name in modules:
            def test_import(m=module_name, c=class_name):
                mod = __import__(m)
                assert hasattr(mod, c), f"{c} not found in {m}"
                return f"{c} available"
            
            self.test(f"Import {module_name}.{class_name}", test_import)
    
    def validate_disk_management(self):
        """Validate disk management modules."""
        def check_disk_monitor():
            import config as cfg
            from disk_monitor import DiskMonitor
            monitor = DiskMonitor(cfg)
            
            assert hasattr(monitor, 'check_disk_usage'), "check_disk_usage missing"
            assert hasattr(monitor, 'get_growth_rate'), "get_growth_rate missing"
            assert hasattr(monitor, 'disk_check_task'), "disk_check_task missing"
            
            return "DiskMonitor OK"
        
        self.test("DiskMonitor functionality", check_disk_monitor)
    
    def validate_directories(self):
        """Validate directory structure."""
        def check_root():
            required = ['data_raw', 'meta', 'state']
            missing = [d for d in required if not (self.project_root / d).exists()]
            if self.fix and missing:
                for d in missing:
                    (self.project_root / d).mkdir(exist_ok=True)
                return f"Created: {missing}"
            if missing:
                raise ValueError(f"Missing directories: {missing}")
            return "All directories present"
        
        self.test("Directory structure", check_root)
        
        def check_converter():
            converter_file = self.project_root / 'convert_yesterday.py'
            if not converter_file.exists():
                raise ValueError("convert_yesterday.py not found")
            return "Converter script present"
        
        self.test("Converter module structure", check_converter)
        
        def check_systemd():
            systemd_dir = self.project_root / 'systemd'
            required_files = ['cryptofeed-recorder.service', 'nautilus-convert.service', 'nautilus-convert.timer']
            if systemd_dir.exists():
                missing = [f for f in required_files if not (systemd_dir / f).exists()]
                if missing:
                    raise ValueError(f"Missing in systemd/: {missing}")
                return "Systemd files present"
            return "systemd/ directory not found (will be installed with install.sh)"
        
        self.test("Systemd service files", check_systemd)
    
    def validate_recorder(self):
        """Validate recorder module."""
        def check_imports():
            from recorder import (
                storage_manager, health_monitor, disk_monitor,
                shutdown_event
            )
            return "Global variables defined"
        
        def check_classes():
            from recorder import MetadataFetcher
            assert hasattr(MetadataFetcher, 'maybe_fetch_exchangeinfo')
            return "Required classes present"
        
        self.test("Recorder globals", check_imports)
        self.test("Recorder classes", check_classes)
    
    def validate_converter(self):
        """Validate converter system."""
        def check_converter_imports():
            from convert_yesterday import (
                convert_date, resolve_universe, load_exchange_info,
                build_instruments, convert_trades, convert_depth,
            )
            return "convert_yesterday.py imports OK"

        self.test("Converter modules", check_converter_imports)

        def check_convert_script():
            convert_script = self.project_root / 'convert_yesterday.py'
            if not convert_script.exists():
                raise FileNotFoundError("convert_yesterday.py not found")
            return "convert_yesterday.py present"

        self.test("Converter entry point", check_convert_script)
    
    def validate_async(self):
        """Validate async functionality."""
        async def check_async_imports():
            import asyncio
            import aiohttp
            return "Async libraries available"
        
        def run_async_test():
            loop = asyncio.new_event_loop()
            result = loop.run_until_complete(check_async_imports())
            loop.close()
            return result
        
        self.test("Async functionality", run_async_test)
    
    def report_summary(self) -> bool:
        """Print and save summary report."""
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        print(f"Total Tests: {self.passed + self.failed}")
        print(f"Passed: {self.passed} ✓")
        print(f"Failed: {self.failed} ✗")
        
        success_rate = (self.passed / (self.passed + self.failed) * 100) if (self.passed + self.failed) > 0 else 0
        print(f"Success Rate: {success_rate:.1f}%")
        
        # Save report
        state_dir = self.project_root / "state"
        state_dir.mkdir(exist_ok=True)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_tests": self.passed + self.failed,
            "passed": self.passed,
            "failed": self.failed,
            "success_rate": success_rate,
            "details": self.results
        }
        
        report_file = state_dir / "validation_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nDetailed report saved to: state/validation_report.json")
        print("=" * 70 + "\n")
        
        return self.failed == 0


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Validate CryptoRecorder system components",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_system.py              # Full validation
  python validate_system.py --quick      # Quick checks only
  python validate_system.py --verbose    # Detailed output
  python validate_system.py --fix        # Auto-create missing directories
        """
    )
    
    parser.add_argument('--quick', action='store_true', help='Quick validation only')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--fix', action='store_true', help='Auto-fix issues')
    
    args = parser.parse_args()
    
    validator = SystemValidator(
        verbose=args.verbose,
        quick=args.quick,
        fix=args.fix
    )
    
    success = validator.run_all()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
