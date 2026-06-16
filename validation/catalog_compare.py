"""Semantic comparison utilities for Nautilus ParquetDataCatalog outputs."""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

from nautilus_trader.persistence.catalog import ParquetDataCatalog


def load_instrument_ids(catalog_root: Path) -> list[str]:
    """Return sorted instrument ids from a Nautilus catalog."""
    catalog = ParquetDataCatalog(str(catalog_root))
    return sorted(str(instrument.id) for instrument in catalog.instruments())


def load_trade_ticks(
    catalog_root: Path,
    instrument_id: str,
    start: int | None = None,
    end: int | None = None,
) -> list[Any]:
    """Load TradeTick objects for one instrument from a Nautilus catalog."""
    catalog = ParquetDataCatalog(str(catalog_root))
    kwargs: dict[str, Any] = {"instrument_ids": [instrument_id]}
    if start is not None:
        kwargs["start"] = start
    if end is not None:
        kwargs["end"] = end
    ticks = catalog.trade_ticks(**kwargs)
    return list(ticks or [])


def _enum_name(value: Any) -> str:
    return str(getattr(value, "name", value)).split(".")[-1]


def _tick_to_record(tick: Any) -> dict[str, Any]:
    return {
        "instrument_id": str(tick.instrument_id),
        "trade_id": str(tick.trade_id),
        "price": str(tick.price),
        "size": str(tick.size),
        "aggressor_side": _enum_name(tick.aggressor_side),
        "ts_event": int(tick.ts_event),
        "ts_init": int(tick.ts_init),
    }


def _normalize_ticks(ticks: Iterable[Any]) -> list[dict[str, Any]]:
    records = [_tick_to_record(tick) for tick in ticks]
    return sorted(records, key=lambda item: (item["ts_event"], item["trade_id"]))


def summarize_trade_ticks(ticks: Iterable[Any]) -> dict[str, Any]:
    """Build a compact count/timestamp summary for a TradeTick sequence."""
    records = _normalize_ticks(ticks)
    return _summarize_records(records)


def _summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "count": 0,
            "ts_min": None,
            "ts_max": None,
            "first": None,
            "last": None,
        }
    timestamps = [record["ts_event"] for record in records]
    return {
        "count": len(records),
        "ts_min": min(timestamps),
        "ts_max": max(timestamps),
        "first": records[0],
        "last": records[-1],
    }


def _sample_indexes(length: int, sample_count: int) -> list[int]:
    if length <= 0:
        return []
    if length <= sample_count:
        return list(range(length))
    indexes = {0, length - 1}
    if sample_count > 2:
        step = (length - 1) / float(sample_count - 1)
        for i in range(sample_count):
            indexes.add(round(i * step))
    return sorted(indexes)


def _float_equal(left: str, right: str, tolerance: float) -> bool:
    try:
        return math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)
    except (TypeError, ValueError):
        return False


def _compare_decimal_field(
    field: str,
    old_record: dict[str, Any],
    new_record: dict[str, Any],
    tolerance: float,
) -> tuple[bool, bool]:
    old_value = str(old_record.get(field))
    new_value = str(new_record.get(field))
    if old_value == new_value:
        return True, False
    if _float_equal(old_value, new_value, tolerance):
        return True, True
    return False, False


def compare_trade_ticks_semantic(
    old_ticks: Iterable[Any],
    new_ticks: Iterable[Any],
    sample_count: int = 100,
    numeric_tolerance: float = 0.0,
) -> dict[str, Any]:
    """Compare TradeTick streams by semantic fields, not Parquet bytes."""
    old_records = _normalize_ticks(old_ticks)
    new_records = _normalize_ticks(new_ticks)
    old_summary = _summarize_records(old_records)
    new_summary = _summarize_records(new_records)

    result: dict[str, Any] = {
        "trade_count_old": len(old_records),
        "trade_count_new": len(new_records),
        "trade_count_match": len(old_records) == len(new_records),
        "ts_min_old": old_summary["ts_min"],
        "ts_min_new": new_summary["ts_min"],
        "ts_max_old": old_summary["ts_max"],
        "ts_max_new": new_summary["ts_max"],
        "timestamp_range_match": (
            old_summary["ts_min"] == new_summary["ts_min"]
            and old_summary["ts_max"] == new_summary["ts_max"]
        ),
        "first_match": old_summary["first"] == new_summary["first"],
        "last_match": old_summary["last"] == new_summary["last"],
        "sample_mismatches": [],
        "missing_keys": [],
        "extra_keys": [],
        "numeric_fallback_fields": [],
    }

    old_keys = {(item["ts_event"], item["trade_id"]) for item in old_records}
    new_keys = {(item["ts_event"], item["trade_id"]) for item in new_records}
    result["missing_keys"] = [list(key) for key in sorted(old_keys - new_keys)[:50]]
    result["extra_keys"] = [list(key) for key in sorted(new_keys - old_keys)[:50]]

    numeric_fallback_fields: set[str] = set()
    mismatch_limit = 50
    for index in _sample_indexes(min(len(old_records), len(new_records)), sample_count):
        old_record = old_records[index]
        new_record = new_records[index]
        mismatches: dict[str, dict[str, Any]] = {}
        for field in ("instrument_id", "trade_id", "aggressor_side", "ts_event", "ts_init"):
            if old_record[field] != new_record[field]:
                mismatches[field] = {
                    "old": old_record[field],
                    "new": new_record[field],
                }
        for field in ("price", "size"):
            equal, used_numeric = _compare_decimal_field(
                field,
                old_record,
                new_record,
                numeric_tolerance,
            )
            if used_numeric:
                numeric_fallback_fields.add(field)
            if not equal:
                mismatches[field] = {
                    "old": old_record[field],
                    "new": new_record[field],
                }
        if mismatches and len(result["sample_mismatches"]) < mismatch_limit:
            result["sample_mismatches"].append(
                {
                    "sample_index": index,
                    "old": old_record,
                    "new": new_record,
                    "fields": mismatches,
                }
            )

    result["numeric_fallback_fields"] = sorted(numeric_fallback_fields)
    result["passed"] = (
        result["trade_count_match"]
        and result["timestamp_range_match"]
        and not result["missing_keys"]
        and not result["extra_keys"]
        and not result["sample_mismatches"]
    )
    return result


def write_validation_report(report: dict[str, Any], output_path: Path) -> Path:
    """Write a validation report as pretty JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str))
    return output_path
