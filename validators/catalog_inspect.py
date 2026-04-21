from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nautilus_trader.persistence.catalog import ParquetDataCatalog


def inspect_catalog(
    catalog_root: Path,
    instrument_id: str,
    *,
    include_depth10: bool = True,
) -> dict[str, Any]:
    catalog = ParquetDataCatalog(str(catalog_root))
    details: dict[str, Any] = {
        "catalog_root": str(catalog_root),
        "instrument_id": instrument_id,
        "trades": 0,
        "order_book_deltas": 0,
        "order_book_depth10": 0,
    }
    details["trades"] = len(catalog.trade_ticks(instrument_ids=[instrument_id]))
    details["order_book_deltas"] = len(
        catalog.order_book_deltas(instrument_ids=[instrument_id], batched=True)
    )
    if include_depth10:
        details["order_book_depth10"] = len(
            catalog.order_book_depth10(instrument_ids=[instrument_id])
        )
    return details


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Inspect a Nautilus catalog instrument.")
    parser.add_argument("catalog_root", type=Path)
    parser.add_argument("instrument_id", type=str)
    parser.add_argument("--skip-depth10", action="store_true")
    args = parser.parse_args()

    report = inspect_catalog(
        args.catalog_root,
        args.instrument_id,
        include_depth10=not args.skip_depth10,
    )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
