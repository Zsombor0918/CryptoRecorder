#!/usr/bin/env python3
"""
cleanup.py - Optional: Clean up old/redundant documentation files

This is OPTIONAL. These files are kept for reference but are now superseded
by the new consolidated documentation.

Old documentation (can be safely removed):
  - VALIDATION_SUMMARY.md      -> Replaced by VALIDATE.py + GUIDE.md
  - VALIDATION_INDEX.md        -> Replaced by START_HERE.md
  - VALIDATION_GUIDE.md        -> Replaced by GUIDE.md
  - IMPLEMENTATION_SUMMARY.md  -> Reference only (dev notes)
  - .validation_quick_reference -> Superseded by VALIDATE.py menu
  - run_validators.py          -> Replaced by VALIDATE.py

New documentation (USE THESE):
  - START_HERE.md              -> 3-minute quick start
  - GUIDE.md                   -> Complete reference 
  - VALIDATE.py                -> Master validation tool

New core tools (USE THESE):
  - VALIDATE.py                -> All-in-one validator
  - validate_system.py         -> Still available if needed
  - test_setup.py              -> Still available if needed

Usage:
    python3 cleanup.py --info    # Show what would be removed
    python3 cleanup.py --remove  # Actually remove files
    python3 cleanup.py --help    # This message

NOTE: This cleanup is OPTIONAL. The old files do no harm if left.
"""

import sys
import os
from pathlib import Path
from enum import Enum


class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


OLD_FILES = [
    "VALIDATION_SUMMARY.md",
    "VALIDATION_INDEX.md", 
    "VALIDATION_GUIDE.md",
    "IMPLEMENTATION_SUMMARY.md",
    ".validation_quick_reference",
    "run_validators.py"
]

NEW_FILES = [
    "START_HERE.md",
    "GUIDE.md",
    "VALIDATE.py"
]


def print_header(title):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{title}{Colors.ENDC}\n")


def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.ENDC}")


def print_warning(msg):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.ENDC}")


def print_error(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.ENDC}")


def get_file_size(path):
    """Get human-readable file size."""
    try:
        size = os.path.getsize(path)
        for unit in ['B', 'KB', 'MB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}GB"
    except:
        return "?"


def show_info():
    """Show what would be removed."""
    print_header("Cleanup Information")
    
    project_root = Path(__file__).parent
    total_size = 0
    
    print(f"{Colors.BOLD}Files to be removed:{Colors.ENDC}\n")
    
    for filename in OLD_FILES:
        filepath = project_root / filename
        if filepath.exists():
            size = get_file_size(filepath)
            total_size += os.path.getsize(filepath)
            print(f"  {Colors.YELLOW}→{Colors.ENDC} {filename:<30} ({size})")
        else:
            print(f"  {Colors.BLUE}⊘{Colors.ENDC} {filename:<30} (not found)")
    
    print(f"\n{Colors.BOLD}Total space to free:{Colors.ENDC} {get_file_size(total_size)}\n")
    
    print(f"{Colors.BOLD}New documentation files (keep these):{Colors.ENDC}\n")
    for filename in NEW_FILES:
        filepath = project_root / filename
        if filepath.exists():
            size = get_file_size(filepath)
            print(f"  {Colors.GREEN}✓{Colors.ENDC} {filename:<30} ({size})")
        else:
            print(f"  {Colors.RED}✗{Colors.ENDC} {filename:<30} (missing)")
    
    print(f"\n{Colors.BOLD}What changed:{Colors.ENDC}")
    print("""
  ✓ Created master validator: VALIDATE.py
    - Replaces run_validators.py
    - Simpler interface with clear output
    - Single command: python3 VALIDATE.py all
  
  ✓ Created quick start: START_HERE.md
    - 3-minute setup guide
    - Essential info only
    - Replaces VALIDATION_INDEX.md
  
  ✓ Created complete guide: GUIDE.md
    - Full reference documentation
    - Configuration, troubleshooting, workflows
    - Replaces VALIDATION_GUIDE.md
  
  ✓ Kept legacy validators (optional)
    - validate_system.py (still works)
    - test_setup.py (still works)
    - Use VALIDATE.py instead
  
  ✓ Documentation consolidation
    - Reduced 12 markdown files to 3 core docs
    - Eliminated duplication
    - Improved navigation
""")


def remove_files():
    """Remove old files."""
    print_header("Removing Old Files")
    
    project_root = Path(__file__).parent
    removed = 0
    failed = 0
    
    for filename in OLD_FILES:
        filepath = project_root / filename
        try:
            if filepath.exists():
                filepath.unlink()
                print_success(f"Removed {filename}")
                removed += 1
            else:
                print(f"{Colors.BLUE}⊘{Colors.ENDC} Skipped {filename} (not found)")
        except Exception as e:
            print_error(f"Failed to remove {filename}: {e}")
            failed += 1
    
    print(f"\n{Colors.BOLD}Results:{Colors.ENDC}")
    print(f"  Removed: {Colors.GREEN}{removed}{Colors.ENDC}")
    print(f"  Failed: {Colors.RED if failed > 0 else Colors.GREEN}{failed}{Colors.ENDC}")
    print(f"  Skipped: {len(OLD_FILES) - removed - failed}")


def print_usage():
    print(f"""
{Colors.BOLD}Cleanup Tool - Remove Old Documentation{Colors.ENDC}

{Colors.BOLD}Usage:{Colors.ENDC}
    python3 cleanup.py --info    # Show what would be removed
    python3 cleanup.py --remove  # Actually remove files
    python3 cleanup.py --help    # This message

{Colors.BOLD}What this does:{Colors.ENDC}
    Removes 6 old documentation files (~45KB total)
    Keeps 3 new consolidated files (22KB)
    
{Colors.BOLD}Note:{Colors.ENDC}
    This is OPTIONAL. Old files do no harm if left.
    You can still run validate_system.py and test_setup.py

{Colors.BOLD}New tools to use:{Colors.ENDC}
    python3 VALIDATE.py all      # Master validator
    python3 START_HERE.md        # Quick 3-min setup
    python3 GUIDE.md             # Complete reference

{Colors.BOLD}Old tools (replaced but still work):{Colors.ENDC}
    python3 validate_system.py
    python3 test_setup.py
    python3 run_validators.py
""")


def main():
    if len(sys.argv) < 2:
        print_info()
        return 0
    
    arg = sys.argv[1].lower()
    
    if arg in ["--help", "-h", "help"]:
        print_usage()
    elif arg in ["--info", "-i", "info"]:
        show_info()
    elif arg in ["--remove", "-r", "remove"]:
        show_info()
        response = input(f"\n{Colors.YELLOW}Remove these files? (y/N): {Colors.ENDC}")
        if response.lower() == 'y':
            remove_files()
            print_success("\nCleanup complete!")
        else:
            print(f"{Colors.YELLOW}Cancelled.{Colors.ENDC}")
    else:
        print_error(f"Unknown option: {arg}")
        print_usage()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
