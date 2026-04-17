#!/usr/bin/env python3
"""
Runtime validation - smoke test if recorder actually works (not just imports).

Tests:
1. Runs recorder for N seconds
2. Checks for HTTP 429/418 bans
3. Verifies files are created and non-empty
4. Checks heartbeat updates
5. Tests graceful shutdown
"""

import asyncio
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
from typing import Dict, List, Tuple

# Keep logs clean for this validation
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class RuntimeValidator:
    """Validate actual recorder runtime behavior."""
    
    def __init__(self, test_duration_sec: int = 180, top_symbols: int = 3):
        """Initialize validator with test parameters."""
        self.project_root = Path(__file__).parent
        self.data_root = self.project_root / "data_raw"
        self.state_root = self.project_root / "state"
        self.log_file = self.project_root / "recorder.log"
        
        self.test_duration = test_duration_sec
        self.top_symbols = top_symbols
        self.test_start_time = None
        self.test_end_time = None
        
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "test_duration": test_duration_sec,
            "checks": {}
        }
        
    def check_http_ban(self) -> Tuple[bool, str]:
        """Check if any HTTP 429/418 occurred."""
        if not self.log_file.exists():
            return False, "Log file not found"
        
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        has_429 = '429' in log_content
        has_418 = '418' in log_content or 'teapot' in log_content
        
        if has_429:
            return False, "HTTP 429 (rate limited) detected"
        if has_418:
            return False, "HTTP 418 (banned) detected - Binance IP ban!"
        
        return True, "No HTTP bans detected"
    
    def check_callback_exceptions(self) -> Tuple[bool, str]:
        """Check if callbacks threw exceptions."""
        if not self.log_file.exists():
            return False, "Log file not found"
        
        with open(self.log_file, 'r') as f:
            log_content = f.read()
        
        # Count on_l2_book errors
        l2_errors = len(re.findall(r'Error in on_l2_book', log_content))
        trade_errors = len(re.findall(r'Error in on_trade', log_content))
        
        total_errors = l2_errors + trade_errors
        
        if total_errors > 0:
            return False, f"Callback exceptions: on_l2_book={l2_errors}, on_trade={trade_errors}"
        
        return True, "No callback exceptions"
    
    def check_file_creation(self) -> Tuple[bool, str]:
        """Check if raw data files are created and non-empty."""
        if not self.data_root.exists():
            return False, f"Data directory not found: {self.data_root}"
        
        # Look for any .jsonl.zst files
        zst_files = list(self.data_root.rglob("*.jsonl.zst"))
        
        if not zst_files:
            return False, f"No data files created in {self.data_root}"
        
        # Check file sizes
        total_size = sum(f.stat().st_size for f in zst_files)
        
        if total_size == 0:
            return False, "Data files created but are empty"
        
        num_files = len(zst_files)
        return True, f"{num_files} data files created, total size: {total_size} bytes"
    
    def check_heartbeat(self) -> Tuple[bool, str]:
        """Check if heartbeat.json exists and updates."""
        heartbeat_file = self.state_root / "heartbeat.json"
        
        if not heartbeat_file.exists():
            return False, "heartbeat.json not found"
        
        # Read heartbeat timestamps during test
        try:
            with open(heartbeat_file, 'r') as f:
                first_hb = json.load(f)
            first_hb_time = first_hb.get('timestamp')
            
            # Sleep and check again
            time.sleep(2)
            
            with open(heartbeat_file, 'r') as f:
                second_hb = json.load(f)
            second_hb_time = second_hb.get('timestamp')
            
            if first_hb_time != second_hb_time:
                return True, "heartbeat.json updates correctly"
            else:
                return False, "heartbeat.json not updating"
        except Exception as e:
            return False, f"Error reading heartbeat.json: {e}"
    
    def check_graceful_shutdown(self, process: subprocess.Popen) -> Tuple[bool, str]:
        """Check if recorder shuts down gracefully."""
        try:
            # Send SIGINT
            process.send_signal(signal.SIGINT)
            
            # Wait for process to exit
            return_code = process.wait(timeout=10)
            
            if return_code == 0:
                pass  # OK
            else:
                return False, f"Process exited with code {return_code}"
            
            # Check for shutdown errors
            if not self.log_file.exists():
                return True, "Shutdown clean (no errors in logs)"
            
            with open(self.log_file, 'r') as f:
                log_content = f.read()
            
            if "different loop" in log_content.lower():
                return False, "Event loop mismatch error on shutdown"
            if "coroutine" in log_content.lower() and "never awaited" in log_content.lower():
                return False, "Coroutine not awaited on shutdown"
            
            return True, "Graceful shutdown - no errors"
        
        except subprocess.TimeoutExpired:
            process.kill()
            return False, "Process did not shutdown after SIGINT"
        except Exception as e:
            return False, f"Shutdown test error: {e}"
    
    def run_test(self) -> bool:
        """Run the full runtime validation test."""
        logger.info(f"Starting recorder runtime test ({self.test_duration}s, TOP_SYMBOLS={self.top_symbols})...")
        
        # Clear previous log
        if self.log_file.exists():
            self.log_file.unlink()
        
        # Set environment
        env = os.environ.copy()
        env['CR_TEST_MODE'] = '1'
        env['CR_TOP_SYMBOLS'] = str(self.top_symbols)
        env['CR_SNAPSHOT_MODE'] = 'disabled'
        
        try:
            # Start recorder
            logger.info("Launching recorder process...")
            process = subprocess.Popen(
                ['python3', 'recorder.py'],
                cwd=str(self.project_root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.test_start_time = time.time()
            
            # Let it run
            logger.info(f"Recorder running, will wait {self.test_duration}s...")
            time.sleep(self.test_duration)
            
            self.test_end_time = time.time()
            
            # Now run checks while recorder is still running
            logger.info("Running validation checks...")
            
            logger.info("  1. Checking for HTTP bans...")
            result, msg = self.check_http_ban()
            self.results["checks"]["http_ban"] = {"pass": result, "message": msg}
            logger.info(f"     {'✓' if result else '✗'} {msg}")
            
            logger.info("  2. Checking for callback exceptions...")
            result, msg = self.check_callback_exceptions()
            self.results["checks"]["callback_exceptions"] = {"pass": result, "message": msg}
            logger.info(f"     {'✓' if result else '✗'} {msg}")
            
            logger.info("  3. Checking file creation...")
            result, msg = self.check_file_creation()
            self.results["checks"]["file_creation"] = {"pass": result, "message": msg}
            logger.info(f"     {'✓' if result else '✗'} {msg}")
            
            logger.info("  4. Checking heartbeat updates...")
            result, msg = self.check_heartbeat()
            self.results["checks"]["heartbeat"] = {"pass": result, "message": msg}
            logger.info(f"     {'✓' if result else '✗'} {msg}")
            
            logger.info("  5. Testing graceful shutdown...")
            result, msg = self.check_graceful_shutdown(process)
            self.results["checks"]["shutdown"] = {"pass": result, "message": msg}
            logger.info(f"     {'✓' if result else '✗'} {msg}")
            
            # Calculate overall result
            passed = sum(1 for c in self.results["checks"].values() if c.get("pass", False))
            total = len(self.results["checks"])
            
            self.results["total_checks"] = total
            self.results["passed_checks"] = passed
            self.results["success"] = passed == total
            
            return passed == total
        
        except Exception as e:
            logger.error(f"Test execution error: {e}", exc_info=True)
            self.results["error"] = str(e)
            self.results["success"] = False
            return False
    
    def save_report(self) -> Path:
        """Save validation report to JSON."""
        report_dir = self.state_root / "validation"
        report_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = report_dir / f"runtime_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"\nReport saved: {report_file}")
        return report_file
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 70)
        print("RUNTIME VALIDATION SUMMARY")
        print("=" * 70)
        
        for check_name, result in self.results.get("checks", {}).items():
            status = "✓ PASS" if result.get("pass") else "✗ FAIL"
            msg = result.get("message", "")
            print(f"{status:8} {check_name:25} {msg}")
        
        passed = self.results.get("passed_checks", 0)
        total = self.results.get("total_checks", 0)
        
        print(f"\n{'=' * 70}")
        print(f"Result: {passed}/{total} checks passed")
        print(f"Status: {'✓ SUCCESS' if self.results.get('success') else '✗ FAILURE'}")
        print("=" * 70 + "\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Runtime validation for CryptoRecorder")
    parser.add_argument('--duration', type=int, default=180, help='Test duration in seconds (default: 180)')
    parser.add_argument('--symbols', type=int, default=3, help='Number of symbols to use (default: 3)')
    
    args = parser.parse_args()
    
    validator = RuntimeValidator(test_duration_sec=args.duration, top_symbols=args.symbols)
    
    # Run test
    success = validator.run_test()
    
    # Save and print report
    validator.save_report()
    validator.print_summary()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
