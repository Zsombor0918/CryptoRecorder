from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_REPORT_FIELDS = (
    "architecture",
    "total_order_book_deltas_written",
    "total_trades_written",
    "snapshot_seed_count",
    "resync_count",
    "desync_events",
    "fenced_ranges_total",
)


def validate_report(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text())
    missing = [field for field in REQUIRED_REPORT_FIELDS if field not in report]
    return {
        "path": str(path),
        "architecture": report.get("architecture"),
        "missing_fields": missing,
        "passed": report.get("architecture") == "deterministic_native" and not missing,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate a convert report.")
    parser.add_argument("path", type=Path)
    args = parser.parse_args()

    result = validate_report(args.path)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
