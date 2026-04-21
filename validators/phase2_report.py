from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_PHASE2_FIELDS = (
    "phase",
    "total_order_book_deltas_written",
    "snapshot_seed_count",
    "resync_count",
    "desync_events",
    "fenced_ranges_total",
)


def validate_phase2_report(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text())
    missing = [field for field in REQUIRED_PHASE2_FIELDS if field not in report]
    return {
        "path": str(path),
        "phase": report.get("phase"),
        "missing_fields": missing,
        "passed": report.get("phase") == "phase2" and not missing,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate a Phase 2 convert report.")
    parser.add_argument("path", type=Path)
    args = parser.parse_args()

    result = validate_phase2_report(args.path)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
