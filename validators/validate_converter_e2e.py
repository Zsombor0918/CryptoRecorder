#!/usr/bin/env python3
"""
validate_converter_e2e.py  –  DEPRECATED: stub that delegates to
validate_nautilus_catalog_e2e.py

The converter now writes a Nautilus-native ParquetDataCatalog instead of
plain Parquet.  All converter validation lives in
validate_nautilus_catalog_e2e.py.  This stub exists so that
``python VALIDATE.py converter`` still works.

Use ``python VALIDATE.py nautilus`` instead.
"""
import subprocess
import sys
from pathlib import Path

def main() -> int:
    print("⚠  validate_converter_e2e.py is DEPRECATED.")
    print("   Delegating to validate_nautilus_catalog_e2e.py …\n")
    here = Path(__file__).resolve().parent
    return subprocess.call(
        [sys.executable, str(here / "validate_nautilus_catalog_e2e.py")] + sys.argv[1:],
    )

if __name__ == "__main__":
    sys.exit(main())
