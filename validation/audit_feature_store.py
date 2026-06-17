"""Audit feature_store outputs for date bounds, sparsity, and null coverage."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from config import FEATURE_ROOT


def _split_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _date_bounds_ns(date: str) -> tuple[int, int]:
    start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return (
        int(start.timestamp() * 1_000_000_000),
        int(end.timestamp() * 1_000_000_000),
    )


def _timeframe_to_ns(timeframe: str) -> int:
    mapping = {
        "100ms": 100_000_000,
        "1s": 1_000_000_000,
        "1m": 60_000_000_000,
    }
    if timeframe not in mapping:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return mapping[timeframe]


def _expected_dense_count(timeframe: str) -> int:
    return 86_400_000_000_000 // _timeframe_to_ns(timeframe)


def _discover_values(root: Path, prefix: str) -> list[str]:
    return sorted(
        path.name.split("=", 1)[1]
        for path in root.glob(f"{prefix}=*")
        if path.is_dir() and "=" in path.name
    )


def _sum_numeric(values: list[Any]) -> float:
    total = 0.0
    for value in values:
        if value is not None:
            total += float(value)
    return total


def audit_feature_file(path: Path, *, date: str, timeframe: str) -> dict[str, Any]:
    start_ns, end_ns = _date_bounds_ns(date)
    expected_dense = _expected_dense_count(timeframe)
    report: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "timeframe": timeframe,
        "expected_dense_row_count": expected_dense,
        "actual_row_count": 0,
        "min_timestamp_ns": None,
        "max_timestamp_ns": None,
        "outside_date_rows": 0,
        "duplicate_timestamp_count": 0,
        "missing_windows_count_if_dense": expected_dense,
        "null_ratio_by_column": {},
        "all_null_columns": [],
        "quality_ok_false_count": 0,
        "crossed_book_count_sum": 0,
    }
    if not path.exists():
        return report

    table = pq.ParquetFile(path).read()
    row_count = table.num_rows
    report["actual_row_count"] = row_count
    if row_count == 0:
        return report

    timestamps = table.column("timestamp_ns").to_pylist() if "timestamp_ns" in table.column_names else []
    if timestamps:
        unique_timestamps = set(timestamps)
        report["min_timestamp_ns"] = min(timestamps)
        report["max_timestamp_ns"] = max(timestamps)
        report["outside_date_rows"] = sum(
            1 for ts in timestamps if ts is None or ts < start_ns or ts >= end_ns
        )
        report["duplicate_timestamp_count"] = row_count - len(unique_timestamps)
        in_date_unique = {ts for ts in unique_timestamps if ts is not None and start_ns <= ts < end_ns}
        report["missing_windows_count_if_dense"] = max(0, expected_dense - len(in_date_unique))

    null_ratios: dict[str, float] = {}
    all_null_columns: list[str] = []
    for name in table.column_names:
        column = table.column(name)
        ratio = column.null_count / row_count if row_count else 0.0
        null_ratios[name] = ratio
        if column.null_count == row_count:
            all_null_columns.append(name)
    report["null_ratio_by_column"] = null_ratios
    report["all_null_columns"] = all_null_columns

    if "quality_ok" in table.column_names:
        values = table.column("quality_ok").to_pylist()
        report["quality_ok_false_count"] = sum(1 for value in values if value is False)
    if "crossed_book_count" in table.column_names:
        report["crossed_book_count_sum"] = _sum_numeric(
            table.column("crossed_book_count").to_pylist()
        )
    return report


def audit_feature_store(
    *,
    feature_root: Path,
    date: str,
    symbols: list[str],
    venues: list[str],
    timeframes: list[str],
) -> dict[str, Any]:
    if symbols == ["ALL"]:
        discovered: set[str] = set()
        for timeframe in timeframes:
            tf_root = feature_root / f"timeframe={timeframe}"
            for venue in _discover_values(tf_root, "venue"):
                discovered.update(
                    _discover_values(tf_root / f"venue={venue}", "symbol")
                )
        symbols = sorted(discovered)
    if venues == ["ALL"]:
        discovered_venues: set[str] = set()
        for timeframe in timeframes:
            discovered_venues.update(
                _discover_values(feature_root / f"timeframe={timeframe}", "venue")
            )
        venues = sorted(discovered_venues)

    files = []
    for timeframe in timeframes:
        for venue in venues:
            for symbol in symbols:
                path = (
                    feature_root
                    / f"timeframe={timeframe}"
                    / f"venue={venue}"
                    / f"symbol={symbol}"
                    / f"{date}.parquet"
                )
                item = audit_feature_file(path, date=date, timeframe=timeframe)
                item.update({"venue": venue, "symbol": symbol})
                files.append(item)

    return {
        "date": date,
        "feature_root": str(feature_root),
        "symbols": symbols,
        "venues": venues,
        "timeframes": timeframes,
        "files": files,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit feature_store Parquet outputs.")
    parser.add_argument("--feature-root", type=Path, default=FEATURE_ROOT)
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols or all")
    parser.add_argument("--venues", required=True, help="Comma-separated venues or all")
    parser.add_argument("--timeframes", default="1m,1s,100ms")
    parser.add_argument("--report-path", type=Path, default=None)
    args = parser.parse_args()

    report = audit_feature_store(
        feature_root=args.feature_root,
        date=args.date,
        symbols=["ALL"] if args.symbols.lower() == "all" else _split_csv(args.symbols),
        venues=["ALL"] if args.venues.lower() == "all" else _split_csv(args.venues),
        timeframes=[item.strip() for item in args.timeframes.split(",") if item.strip()],
    )
    payload = json.dumps(report, indent=2, default=str)
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(payload)
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
