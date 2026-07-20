"""validation.audit_storage_size — measure on-disk size of CryptoRecorder artifacts.

Audit-only (no build/transform). Reports the bytes used by the replay_store /
generated catalog for a single venue/symbol/date so storage growth can be
tracked honestly instead of guessed.

Example::

    python -m validation.audit_storage_size \\
      --venue BINANCE_SPOT --symbol ADAUSDT --date 2026-06-12 \\
      --replay-root /tmp/cr-full-l2/replay_store \\
      --catalog-root /tmp/cr-full-l2/new_catalog/job_validation_new \\
      --json
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _dir_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _human(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TiB"


def _replay_partition(replay_root: Path, venue: str, symbol: str, date: str) -> Path:
    return replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"


def audit_storage_size(
    *,
    venue: str,
    symbol: str,
    date: str,
    replay_root: Optional[Path] = None,
    catalog_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Collect byte sizes for the replay/catalog artifacts of one partition."""
    components: list[dict[str, Any]] = []

    if replay_root is not None:
        partition = _replay_partition(replay_root, venue, symbol, date)
        depth = partition / "depth.parquet"
        trades = partition / "trades.parquet"
        components.append({"artifact": "replay.depth.parquet", "path": str(depth), "bytes": _dir_bytes(depth)})
        components.append({"artifact": "replay.trades.parquet", "path": str(trades), "bytes": _dir_bytes(trades)})

    if catalog_root is not None:
        data_root = catalog_root / "data"
        base = data_root if data_root.exists() else catalog_root
        for sub in ("trade_tick", "order_book_deltas", "order_book_depths", "order_book_depth10"):
            target = base / sub
            if target.exists():
                components.append({"artifact": f"catalog.{sub}", "path": str(target), "bytes": _dir_bytes(target)})
        components.append({"artifact": "catalog.total", "path": str(catalog_root), "bytes": _dir_bytes(catalog_root)})

    total = sum(c["bytes"] for c in components if not c["artifact"].endswith(".total"))
    return {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "components": components,
        "total_bytes_excluding_catalog_total": total,
        "note": (
            "Single venue/symbol/date measurement. Extrapolating to the full "
            "universe is a rough estimate, not a benchmark — liquidity varies "
            "widely across symbols."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit on-disk size of CryptoRecorder artifacts.")
    parser.add_argument("--venue", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    parser.add_argument("--replay-root", type=Path, default=None)
    parser.add_argument("--catalog-root", type=Path, default=None, help="A generated job_* catalog root")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    report = audit_storage_size(
        venue=args.venue,
        symbol=args.symbol,
        date=args.date,
        replay_root=args.replay_root,
        catalog_root=args.catalog_root,
    )

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"Storage audit: {args.venue}/{args.symbol}/{args.date}")
    print(f"{'artifact':<28} {'size':>12}")
    print("-" * 42)
    for component in report["components"]:
        print(f"{component['artifact']:<28} {_human(component['bytes']):>12}")
    print("-" * 42)
    print(f"{'sum (excl catalog.total)':<28} {_human(report['total_bytes_excluding_catalog_total']):>12}")
    print(f"\n{report['note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
