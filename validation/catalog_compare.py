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


# ---------------------------------------------------------------------------
# OrderBookDeltas
# ---------------------------------------------------------------------------


def load_order_book_deltas(
    catalog_root: Path,
    instrument_id: str,
    start: int | None = None,
    end: int | None = None,
) -> list[Any]:
    """Load OrderBookDelta(s) for one instrument from a Nautilus catalog."""
    catalog = ParquetDataCatalog(str(catalog_root))
    kwargs: dict[str, Any] = {"instrument_ids": [instrument_id]}
    if start is not None:
        kwargs["start"] = start
    if end is not None:
        kwargs["end"] = end
    deltas = catalog.order_book_deltas(**kwargs)
    return list(deltas or [])


def _flatten_deltas(objects: Iterable[Any]) -> list[Any]:
    """Flatten grouped OrderBookDeltas into individual OrderBookDelta objects."""
    flat: list[Any] = []
    for obj in objects:
        inner = getattr(obj, "deltas", None)
        if inner is not None:
            flat.extend(inner)
        else:
            flat.append(obj)
    return flat


def _book_order_record(order: Any) -> dict[str, Any]:
    """Defensively normalize a BookOrder (CLEAR deltas carry a null order)."""
    if order is None:
        return {"side": "NULL", "price": "", "size": "", "order_id": 0}
    out: dict[str, Any] = {}
    try:
        out["side"] = _enum_name(order.side)
    except Exception:
        out["side"] = "NULL"
    try:
        out["price"] = str(order.price)
    except Exception:
        out["price"] = ""
    try:
        out["size"] = str(order.size)
    except Exception:
        out["size"] = ""
    try:
        out["order_id"] = int(order.order_id)
    except Exception:
        out["order_id"] = 0
    return out


def _delta_to_record(delta: Any) -> dict[str, Any]:
    record = {
        "instrument_id": str(delta.instrument_id),
        "action": _enum_name(delta.action),
        "flags": int(delta.flags),
        "sequence": int(delta.sequence),
        "ts_event": int(delta.ts_event),
        "ts_init": int(delta.ts_init),
    }
    record.update(_book_order_record(getattr(delta, "order", None)))
    return record


def _delta_sort_key(record: dict[str, Any]) -> tuple:
    return (
        record["ts_init"],
        record["ts_event"],
        record["sequence"],
        record["action"],
        record["side"],
        record["price"],
        record["size"],
        record["order_id"],
        record["flags"],
    )


def _normalize_deltas(objects: Iterable[Any]) -> list[dict[str, Any]]:
    records = [_delta_to_record(delta) for delta in _flatten_deltas(objects)]
    return sorted(records, key=_delta_sort_key)


def summarize_order_book_deltas(objects: Iterable[Any]) -> dict[str, Any]:
    """Build a compact count/timestamp summary for an OrderBookDeltas sequence."""
    return _summarize_records(_normalize_deltas(objects))


def compare_order_book_deltas_semantic(
    old_objects: Iterable[Any],
    new_objects: Iterable[Any],
    sample_count: int = 100,
    numeric_tolerance: float = 0.0,
) -> dict[str, Any]:
    """Compare OrderBookDeltas streams by semantic fields (multiset equality)."""
    old_records = _normalize_deltas(old_objects)
    new_records = _normalize_deltas(new_objects)
    old_summary = _summarize_records(old_records)
    new_summary = _summarize_records(new_records)

    result: dict[str, Any] = {
        "delta_count_old": len(old_records),
        "delta_count_new": len(new_records),
        "delta_count_match": len(old_records) == len(new_records),
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
        "numeric_fallback_fields": [],
    }

    numeric_fallback_fields: set[str] = set()
    mismatch_limit = 50
    for index in _sample_indexes(min(len(old_records), len(new_records)), sample_count):
        old_record = old_records[index]
        new_record = new_records[index]
        mismatches: dict[str, dict[str, Any]] = {}
        for field in (
            "instrument_id",
            "action",
            "side",
            "order_id",
            "flags",
            "sequence",
            "ts_event",
            "ts_init",
        ):
            if old_record[field] != new_record[field]:
                mismatches[field] = {"old": old_record[field], "new": new_record[field]}
        for field in ("price", "size"):
            equal, used_numeric = _compare_decimal_field(
                field, old_record, new_record, numeric_tolerance
            )
            if used_numeric:
                numeric_fallback_fields.add(field)
            if not equal:
                mismatches[field] = {"old": old_record[field], "new": new_record[field]}
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
        result["delta_count_match"]
        and result["timestamp_range_match"]
        and not result["sample_mismatches"]
    )
    return result


# ---------------------------------------------------------------------------
# OrderBookDepth10
# ---------------------------------------------------------------------------


def load_order_book_depth10(
    catalog_root: Path,
    instrument_id: str,
    start: int | None = None,
    end: int | None = None,
) -> list[Any]:
    """Load OrderBookDepth10 objects for one instrument from a Nautilus catalog."""
    catalog = ParquetDataCatalog(str(catalog_root))
    kwargs: dict[str, Any] = {"instrument_ids": [instrument_id]}
    if start is not None:
        kwargs["start"] = start
    if end is not None:
        kwargs["end"] = end
    depths = catalog.order_book_depth10(**kwargs)
    return list(depths or [])


def _depth10_to_record(depth: Any) -> dict[str, Any]:
    return {
        "instrument_id": str(depth.instrument_id),
        "sequence": int(depth.sequence),
        "flags": int(depth.flags),
        "ts_event": int(depth.ts_event),
        "ts_init": int(depth.ts_init),
        "bids": [_book_order_record(order) for order in depth.bids],
        "asks": [_book_order_record(order) for order in depth.asks],
    }


def _normalize_depth10(objects: Iterable[Any]) -> list[dict[str, Any]]:
    records = [_depth10_to_record(depth) for depth in objects]
    return sorted(records, key=lambda r: (r["ts_init"], r["ts_event"], r["sequence"]))


def summarize_order_book_depth10(objects: Iterable[Any]) -> dict[str, Any]:
    """Build a compact count/timestamp summary for an OrderBookDepth10 sequence."""
    return _summarize_records(_normalize_depth10(objects))


def compare_depth10_semantic(
    old_objects: Iterable[Any],
    new_objects: Iterable[Any],
    sample_count: int = 100,
    numeric_tolerance: float = 0.0,
) -> dict[str, Any]:
    """Compare OrderBookDepth10 streams by per-level semantic fields."""
    old_records = _normalize_depth10(old_objects)
    new_records = _normalize_depth10(new_objects)
    old_summary = _summarize_records(old_records)
    new_summary = _summarize_records(new_records)

    result: dict[str, Any] = {
        "depth10_count_old": len(old_records),
        "depth10_count_new": len(new_records),
        "depth10_count_match": len(old_records) == len(new_records),
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
        "numeric_fallback_fields": [],
    }

    numeric_fallback_fields: set[str] = set()
    mismatch_limit = 50
    for index in _sample_indexes(min(len(old_records), len(new_records)), sample_count):
        old_record = old_records[index]
        new_record = new_records[index]
        mismatches: dict[str, Any] = {}
        for field in ("instrument_id", "sequence", "flags", "ts_event", "ts_init"):
            if old_record[field] != new_record[field]:
                mismatches[field] = {"old": old_record[field], "new": new_record[field]}
        for side_key in ("bids", "asks"):
            old_levels = old_record[side_key]
            new_levels = new_record[side_key]
            if len(old_levels) != len(new_levels):
                mismatches[f"{side_key}_len"] = {
                    "old": len(old_levels),
                    "new": len(new_levels),
                }
                continue
            for level_index, (old_level, new_level) in enumerate(zip(old_levels, new_levels)):
                for field in ("side", "order_id"):
                    if old_level[field] != new_level[field]:
                        mismatches[f"{side_key}[{level_index}].{field}"] = {
                            "old": old_level[field],
                            "new": new_level[field],
                        }
                for field in ("price", "size"):
                    equal, used_numeric = _compare_decimal_field(
                        field, old_level, new_level, numeric_tolerance
                    )
                    if used_numeric:
                        numeric_fallback_fields.add(field)
                    if not equal:
                        mismatches[f"{side_key}[{level_index}].{field}"] = {
                            "old": old_level[field],
                            "new": new_level[field],
                        }
        if mismatches and len(result["sample_mismatches"]) < mismatch_limit:
            result["sample_mismatches"].append(
                {
                    "sample_index": index,
                    "ts_event_old": old_record["ts_event"],
                    "ts_event_new": new_record["ts_event"],
                    "fields": mismatches,
                }
            )

    result["numeric_fallback_fields"] = sorted(numeric_fallback_fields)
    result["passed"] = (
        result["depth10_count_match"]
        and result["timestamp_range_match"]
        and not result["sample_mismatches"]
    )
    return result


# ---------------------------------------------------------------------------
# Book checkpoint reconstruction
# ---------------------------------------------------------------------------


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _apply_delta_to_book(
    bids: dict[str, str],
    asks: dict[str, str],
    delta: Any,
) -> None:
    action = _enum_name(delta.action)
    if action == "CLEAR":
        bids.clear()
        asks.clear()
        return
    order = getattr(delta, "order", None)
    if order is None:
        return
    try:
        side = _enum_name(order.side)
        price = str(order.price)
        size = str(order.size)
    except Exception:
        return
    book = bids if side == "BUY" else asks
    if action == "DELETE" or _safe_float(size) == 0.0:
        book.pop(price, None)
    else:
        book[price] = size


def _top_of_book(
    bids: dict[str, str],
    asks: dict[str, str],
    levels: int,
) -> dict[str, list[list[str]]]:
    bid_levels = sorted(bids.items(), key=lambda kv: -_safe_float(kv[0]))[:levels]
    ask_levels = sorted(asks.items(), key=lambda kv: _safe_float(kv[0]))[:levels]
    return {
        "bids": [[price, size] for price, size in bid_levels],
        "asks": [[price, size] for price, size in ask_levels],
    }


def reconstruct_book_checkpoints_from_deltas(
    objects: Iterable[Any],
    checkpoint_tss: Iterable[int],
    *,
    ts_field: str = "ts_init",
    levels: int = 10,
) -> dict[int, dict[str, list[list[str]]]]:
    """Reconstruct top-N book state at each checkpoint ts in a single pass.

    Deltas are applied in ``(ts, sequence, read_index)`` order. At each
    checkpoint ``T`` the returned book reflects every delta whose ``ts <= T``.
    """
    flat = _flatten_deltas(objects)
    indexed: list[tuple[int, int, int, Any]] = []
    for read_index, delta in enumerate(flat):
        ts = int(delta.ts_init) if ts_field == "ts_init" else int(delta.ts_event)
        indexed.append((ts, int(delta.sequence), read_index, delta))
    indexed.sort(key=lambda item: (item[0], item[1], item[2]))

    sorted_cps = sorted(set(int(ts) for ts in checkpoint_tss))
    snapshots: dict[int, dict[str, list[list[str]]]] = {}
    bids: dict[str, str] = {}
    asks: dict[str, str] = {}
    cp_idx = 0

    for ts, _seq, _read_index, delta in indexed:
        while cp_idx < len(sorted_cps) and ts > sorted_cps[cp_idx]:
            snapshots[sorted_cps[cp_idx]] = _top_of_book(bids, asks, levels)
            cp_idx += 1
        if cp_idx >= len(sorted_cps):
            break
        _apply_delta_to_book(bids, asks, delta)

    while cp_idx < len(sorted_cps):
        snapshots[sorted_cps[cp_idx]] = _top_of_book(bids, asks, levels)
        cp_idx += 1

    return snapshots


def _checkpoint_labels(start_ns: int, end_ns: int) -> list[tuple[str, int]]:
    span = max(end_ns - start_ns, 0)
    one_min = 60_000_000_000
    return [
        ("start+1min", min(start_ns + one_min, end_ns)),
        ("10%", start_ns + span // 10),
        ("25%", start_ns + span // 4),
        ("50%", start_ns + span // 2),
        ("75%", start_ns + (3 * span) // 4),
        ("90%", start_ns + (9 * span) // 10),
        ("end-1min", max(end_ns - one_min, start_ns)),
    ]


def _is_crossed(book: dict[str, list[list[str]]]) -> bool:
    if not book["bids"] or not book["asks"]:
        return False
    return _safe_float(book["bids"][0][0]) >= _safe_float(book["asks"][0][0])


def compare_book_checkpoints(
    old_objects: Iterable[Any],
    new_objects: Iterable[Any],
    start_ns: int,
    end_ns: int,
    *,
    ts_field: str = "ts_init",
    levels: int = 10,
) -> dict[str, Any]:
    """Reconstruct and compare top-N book state at canonical checkpoints."""
    old_objects = list(old_objects)
    new_objects = list(new_objects)
    labeled = _checkpoint_labels(start_ns, end_ns)
    checkpoint_tss = [ts for _, ts in labeled]

    old_snaps = reconstruct_book_checkpoints_from_deltas(
        old_objects, checkpoint_tss, ts_field=ts_field, levels=levels
    )
    new_snaps = reconstruct_book_checkpoints_from_deltas(
        new_objects, checkpoint_tss, ts_field=ts_field, levels=levels
    )

    empty_book = {"bids": [], "asks": []}
    results: list[dict[str, Any]] = []
    all_match = True
    for label, ts in labeled:
        old_book = old_snaps.get(ts, empty_book)
        new_book = new_snaps.get(ts, empty_book)
        match = old_book == new_book
        if not match:
            all_match = False
        results.append(
            {
                "label": label,
                "ts": ts,
                "match": match,
                "old_bid_levels": len(old_book["bids"]),
                "old_ask_levels": len(old_book["asks"]),
                "new_bid_levels": len(new_book["bids"]),
                "new_ask_levels": len(new_book["asks"]),
                "old_best_bid": old_book["bids"][0] if old_book["bids"] else None,
                "new_best_bid": new_book["bids"][0] if new_book["bids"] else None,
                "old_best_ask": old_book["asks"][0] if old_book["asks"] else None,
                "new_best_ask": new_book["asks"][0] if new_book["asks"] else None,
                "old_crossed": _is_crossed(old_book),
                "new_crossed": _is_crossed(new_book),
            }
        )

    return {
        "passed": all_match,
        "checkpoint_count": len(labeled),
        "any_crossed_old": any(item["old_crossed"] for item in results),
        "any_crossed_new": any(item["new_crossed"] for item in results),
        "checkpoints": results,
    }


def write_validation_report(report: dict[str, Any], output_path: Path) -> Path:
    """Write a validation report as pretty JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str))
    return output_path