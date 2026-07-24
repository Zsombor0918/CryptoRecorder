"""Semantic comparison utilities for Nautilus ParquetDataCatalog outputs."""
from __future__ import annotations

import itertools
import json
import math
from pathlib import Path
from typing import Any, Iterable, Iterator

from nautilus_trader.persistence.catalog import ParquetDataCatalog

# Sentinel used by the exhaustive streaming comparators below to detect
# length divergence via itertools.zip_longest without confusing a genuine
# `None` field value for "this stream ran out of events here".
_MISSING = object()


def load_instrument_ids(catalog_root: Path) -> list[str]:
    """Return sorted instrument ids from a Nautilus catalog."""
    catalog = ParquetDataCatalog(str(catalog_root))
    return sorted(str(instrument.id) for instrument in catalog.instruments())


def load_instruments(catalog_root: Path) -> dict[str, Any]:
    """Return {instrument_id: Instrument object} for a Nautilus catalog.

    Unlike load_instrument_ids(), this keeps the full Instrument object so
    precision/increment fields can be compared, not just identity.
    """
    catalog = ParquetDataCatalog(str(catalog_root))
    return {str(instrument.id): instrument for instrument in catalog.instruments()}


def _instrument_to_record(instrument: Any) -> dict[str, Any]:
    """Normalize the subset of Instrument fields required for exact full-L2
    reconstruction: precision and price/size increment (tick/step size).
    `price_precision`/`size_precision` alone do not define valid tick/step
    sizes (per issue #20's explicit correction) — `price_increment` and
    `size_increment` are the authoritative Binance PRICE_FILTER.tickSize /
    LOT_SIZE.stepSize-derived values Nautilus actually uses for rounding."""
    return {
        "instrument_id": str(instrument.id),
        "price_precision": int(instrument.price_precision),
        "size_precision": int(instrument.size_precision),
        "price_increment": str(instrument.price_increment),
        "size_increment": str(instrument.size_increment),
    }


def compare_instruments_semantic(
    old_instruments: dict[str, Any],
    new_instruments: dict[str, Any],
) -> dict[str, Any]:
    """Compare instrument identity AND precision/increment metadata.

    load_instrument_ids()-based comparison only proves the *set* of
    instrument ids matches; it does not prove the reconstructed instrument
    has the same price/size precision or tick/step size as the reference,
    which would silently corrupt exact-decimal reconstruction downstream.
    This closes that gap (issue #20 Phase 1 oracle coverage audit finding).
    """
    old_ids = set(old_instruments)
    new_ids = set(new_instruments)
    missing_in_new = sorted(old_ids - new_ids)
    extra_in_new = sorted(new_ids - old_ids)

    mismatches: list[dict[str, Any]] = []
    for instrument_id in sorted(old_ids & new_ids):
        old_record = _instrument_to_record(old_instruments[instrument_id])
        new_record = _instrument_to_record(new_instruments[instrument_id])
        field_mismatches = {
            field: {"old": old_record[field], "new": new_record[field]}
            for field in ("price_precision", "size_precision", "price_increment", "size_increment")
            if old_record[field] != new_record[field]
        }
        if field_mismatches:
            mismatches.append({"instrument_id": instrument_id, "fields": field_mismatches})

    return {
        "instrument_count_old": len(old_ids),
        "instrument_count_new": len(new_ids),
        "missing_in_new": missing_in_new,
        "extra_in_new": extra_in_new,
        "precision_mismatches": mismatches,
        "passed": not missing_in_new and not extra_in_new and not mismatches,
    }


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


def iter_trade_ticks_windowed(
    catalog_root: Path,
    instrument_id: str,
    start_ns: int,
    end_ns: int,
    *,
    window_ns: int = 3_600_000_000_000,  # 1 hour
) -> Iterator[Any]:
    """Yield TradeTick objects for one instrument across the half-open
    caller range [start_ns, end_ns) in bounded time windows (default:
    1 hour; configurable via `window_ns` for both testing and production
    tuning), so that at most one window's worth of ticks is materialized by
    the Nautilus catalog query at a time, rather than the full requested
    range up front like load_trade_ticks() does.

    IMPORTANT — Nautilus's own `catalog.trade_ticks(start=a, end=b)` query
    is INCLUSIVE on both `a` and `b` (verified directly against a real
    on-disk catalog in tests/test_windowed_loader_boundaries.py — an event
    at exactly `ts == b` is returned by that query, not excluded). Naively
    chaining windows with `next_start = previous_end` therefore yields any
    event that lands exactly on an internal window boundary TWICE (once as
    the inclusive `end` of one window, once as the inclusive `start` of the
    next). To honor the caller's half-open [start_ns, end_ns) contract
    while querying Nautilus's inclusive-both-ends interface without
    duplication, this function partitions the range into non-overlapping
    CLOSED sub-windows: each window's query uses
    `end = min(window_start + window_ns - 1, end_ns - 1)`, and the next
    window starts at exactly `end + 1`. Since all Nautilus event
    timestamps are integer nanoseconds, `+1`/`-1` unambiguously identifies
    "the next/previous distinct representable instant" with no possible
    event landing in the gap between two adjacent closed windows.

    IMPORTANT — this bounds the query result size per window, which is a
    reasonable proxy for reduced peak memory versus loading the whole day
    in one call, but a fixed *time* window does not by itself guarantee a
    fixed *event-count* (and therefore RSS) bound: an unusually active
    window (e.g. a volatility spike) can still contain far more events than
    a quiet window of the same duration. Treat `window_ns` as a tuning knob
    to be validated against measured per-window RSS on real production data
    (issue #20 Tier 3), not as a proven strict memory ceiling from time
    alone."""
    if end_ns <= start_ns:
        return
    catalog = ParquetDataCatalog(str(catalog_root))
    last_inclusive_ns = end_ns - 1
    window_start = start_ns
    while window_start <= last_inclusive_ns:
        window_end = min(window_start + window_ns - 1, last_inclusive_ns)
        ticks = catalog.trade_ticks(instrument_ids=[instrument_id], start=window_start, end=window_end)
        for tick in ticks or []:
            yield tick
        window_start = window_end + 1


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


def compare_trade_ticks_exhaustive(
    old_ticks: Iterable[Any],
    new_ticks: Iterable[Any],
    *,
    numeric_tolerance: float = 0.0,
    max_reported_mismatches: int = 200,
) -> dict[str, Any]:
    """Exhaustively compare every TradeTick between two streams, in original
    (deterministic arrival) order — no sampling, no re-sorting.

    Unlike compare_trade_ticks_semantic() (which samples up to
    `sample_count` positions after re-sorting both streams by
    `(ts_event, trade_id)`), this function:

    - compares EVERY event at its original stream position, so a
      difference anywhere in the stream is detected, not only at one of a
      fixed number of sampled positions;
    - does NOT re-sort before comparing, so a reordering of two otherwise-
      valid events is detected (the swapped positions will each show a
      mismatch) instead of disappearing into a re-sorted canonical order —
      compare_trade_ticks_semantic()'s sort-then-sample approach cannot
      detect a pure reordering of the same set of trades;
    - accepts and streams *iterables* (including one-shot generators) for
      both `old_ticks` and `new_ticks` and never materializes either into a
      list internally, so memory use is independent of total event count —
      suitable for a complete production day's tens/hundreds of millions of
      trades when paired with iter_trade_ticks_windowed() as the loader,
      and runs in O(N) time (a single pass, no per-event bookkeeping
      structure beyond position counters), so it remains practical at
      200M+ events.

    Duplicate-event semantics: equivalence means the reference and
    candidate streams are identical, INCLUDING any identical duplicate
    occurrences either side may (legitimately or not) contain. Two streams
    that both contain the exact same duplicate event at the exact same
    position are, by definition, equivalent, and this function reports
    `passed=True` for that case. An extra, missing, or differently
    positioned duplicate is not given special-cased duplicate detection —
    it is caught the same way any other insertion/deletion/reorder is
    caught: it shifts every subsequent position, producing
    `first_length_divergence_position` and/or `position_mismatches` from
    that point on, which already fails `passed`. A prior version of this
    function additionally flagged "a duplicate exists on either side" as
    an independent failure condition and used an O(window)-per-event
    bookkeeping structure to do so; that was incorrect (identical
    duplicates present on both sides do not indicate non-equivalence) and
    has been removed rather than merely made more efficient, since the
    positional/length comparison already provides full detection power for
    every duplicate-related discrepancy that can actually indicate
    non-equivalence.
    """
    position_mismatches: list[dict[str, Any]] = []

    old_count = 0
    new_count = 0
    first_length_divergence_position: int | None = None
    position = -1

    for position, (old_tick, new_tick) in enumerate(
        itertools.zip_longest(old_ticks, new_ticks, fillvalue=_MISSING)
    ):
        old_present = old_tick is not _MISSING
        new_present = new_tick is not _MISSING
        if old_present:
            old_count += 1
        if new_present:
            new_count += 1
        if old_present != new_present and first_length_divergence_position is None:
            first_length_divergence_position = position

        if old_present and new_present:
            old_record = _tick_to_record(old_tick)
            new_record = _tick_to_record(new_tick)
            mismatches: dict[str, Any] = {}
            for field in ("instrument_id", "trade_id", "aggressor_side", "ts_event", "ts_init"):
                if old_record[field] != new_record[field]:
                    mismatches[field] = {"old": old_record[field], "new": new_record[field]}
            for field in ("price", "size"):
                equal, _used_numeric = _compare_decimal_field(field, old_record, new_record, numeric_tolerance)
                if not equal:
                    mismatches[field] = {"old": old_record[field], "new": new_record[field]}
            if mismatches and len(position_mismatches) < max_reported_mismatches:
                position_mismatches.append(
                    {"position": position, "old": old_record, "new": new_record, "fields": mismatches}
                )

    positions_compared = position + 1
    passed = (
        old_count == new_count
        and first_length_divergence_position is None
        and not position_mismatches
    )
    return {
        "positions_compared": positions_compared,
        "trade_count_old": old_count,
        "trade_count_new": new_count,
        "trade_count_match": old_count == new_count,
        "first_length_divergence_position": first_length_divergence_position,
        "position_mismatches": position_mismatches,
        "position_mismatch_count_capped_at": max_reported_mismatches,
        "passed": passed,
    }


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


def iter_order_book_deltas_windowed(
    catalog_root: Path,
    instrument_id: str,
    start_ns: int,
    end_ns: int,
    *,
    window_ns: int = 3_600_000_000_000,  # 1 hour
) -> Iterator[Any]:
    """Yield OrderBookDeltas group objects for one instrument across the
    half-open caller range [start_ns, end_ns) in bounded (configurable)
    time windows; see iter_trade_ticks_windowed() for the discovered
    Nautilus inclusive-both-ends query semantics, the closed-window
    partitioning this function uses to avoid double-yielding a boundary
    event, and the explicit caveat that a fixed time window bounds query
    result size per window but is not by itself a proven strict
    event-count/RSS bound — it must be tuned against measured per-window
    memory on real production data (issue #20 Tier 3). Pass the result
    directly to compare_order_book_deltas_exhaustive(), which flattens the
    grouped objects internally without materializing the full flattened
    stream."""
    if end_ns <= start_ns:
        return
    catalog = ParquetDataCatalog(str(catalog_root))
    last_inclusive_ns = end_ns - 1
    window_start = start_ns
    while window_start <= last_inclusive_ns:
        window_end = min(window_start + window_ns - 1, last_inclusive_ns)
        deltas = catalog.order_book_deltas(instrument_ids=[instrument_id], start=window_start, end=window_end)
        for obj in deltas or []:
            yield obj
        window_start = window_end + 1


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


def _iter_flatten_deltas(objects: Iterable[Any]) -> Iterator[Any]:
    """Streaming (generator) equivalent of _flatten_deltas(): yields each
    individual OrderBookDelta one at a time without ever materializing the
    flattened stream into a list. Used by
    compare_order_book_deltas_exhaustive() to keep memory bounded."""
    for obj in objects:
        inner = getattr(obj, "deltas", None)
        if inner is not None:
            yield from inner
        else:
            yield obj


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


def compare_order_book_deltas_exhaustive(
    old_objects: Iterable[Any],
    new_objects: Iterable[Any],
    *,
    numeric_tolerance: float = 0.0,
    max_reported_mismatches: int = 200,
) -> dict[str, Any]:
    """Exhaustively compare every OrderBookDelta between two streams, in
    original (deterministic emission) order — no sampling, no re-sorting.

    Unlike compare_order_book_deltas_semantic() (a multiset comparison
    that re-sorts both streams by a canonical key before comparing, so two
    streams containing the exact same deltas in a different order are
    reported as equal), this function compares every delta at its original
    stream position. Two deltas that touch different, independent book
    levels ("commutative-looking" — applying them in either order yields
    the same eventual book state) are still positionally distinct events;
    a reordering between them is detected here even though it would be
    invisible both to compare_order_book_deltas_semantic() and to
    compare_book_checkpoints()'s deterministic book-state reconstruction at
    checkpoint granularity (the final book state can be identical while the
    emission order differs — this function is the one that actually
    detects that class of difference).

    Streams both inputs lazily via _iter_flatten_deltas() (never
    materializes the flattened delta stream into a list), so memory is
    bounded and independent of total event count — suitable for a complete
    production day's tens/hundreds of millions of depth events when paired
    with iter_order_book_deltas_windowed() as the loader, and runs in O(N)
    time (a single pass, no per-event bookkeeping structure), remaining
    practical at 200M+ events.

    Duplicate-event semantics: identical to compare_trade_ticks_exhaustive()
    — two identical ordered streams pass even if both contain the same
    duplicate delta at the same position; equivalence means the two streams
    are identical, including identical duplicate occurrences. An extra,
    missing, or differently positioned duplicate delta is caught by the
    positional/length comparison, not by a separate duplicate-presence
    check (removed; see compare_trade_ticks_exhaustive()'s docstring for
    the full rationale, which applies identically here).
    """
    old_stream = _iter_flatten_deltas(old_objects)
    new_stream = _iter_flatten_deltas(new_objects)

    position_mismatches: list[dict[str, Any]] = []

    old_count = 0
    new_count = 0
    first_length_divergence_position: int | None = None
    position = -1

    for position, (old_delta, new_delta) in enumerate(
        itertools.zip_longest(old_stream, new_stream, fillvalue=_MISSING)
    ):
        old_present = old_delta is not _MISSING
        new_present = new_delta is not _MISSING
        if old_present:
            old_count += 1
        if new_present:
            new_count += 1
        if old_present != new_present and first_length_divergence_position is None:
            first_length_divergence_position = position

        if old_present and new_present:
            old_record = _delta_to_record(old_delta)
            new_record = _delta_to_record(new_delta)
            mismatches: dict[str, Any] = {}
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
                equal, _used_numeric = _compare_decimal_field(field, old_record, new_record, numeric_tolerance)
                if not equal:
                    mismatches[field] = {"old": old_record[field], "new": new_record[field]}
            if mismatches and len(position_mismatches) < max_reported_mismatches:
                position_mismatches.append(
                    {"position": position, "old": old_record, "new": new_record, "fields": mismatches}
                )

    positions_compared = position + 1
    passed = (
        old_count == new_count
        and first_length_divergence_position is None
        and not position_mismatches
    )
    return {
        "positions_compared": positions_compared,
        "delta_count_old": old_count,
        "delta_count_new": new_count,
        "delta_count_match": old_count == new_count,
        "first_length_divergence_position": first_length_divergence_position,
        "position_mismatches": position_mismatches,
        "position_mismatch_count_capped_at": max_reported_mismatches,
        "passed": passed,
    }


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


# ---------------------------------------------------------------------------
# Continuity / sync-desync-resync / fenced-range diagnostics
# ---------------------------------------------------------------------------
#
# The Nautilus catalog itself (TradeTick / OrderBookDeltas / Depth10) never
# stores snapshot-seed, sync/desync/resync, or fenced-range bookkeeping —
# those are process-level diagnostics emitted alongside the catalog by the
# converter engine (converter/depth_phase2.py's shared Phase2ReplayMetrics),
# not catalog contents. Comparing only the Nautilus objects (as
# compare_order_book_deltas_semantic() etc. do) therefore cannot detect a
# difference in continuity/quality behavior between the reference and
# candidate routes even though the issue's contract explicitly requires it.
# This is the Phase 1 oracle-coverage gap closed here.
#
# Reference-side (convert_day.py) per-symbol shape:
#   report["per_symbol_depth"]["VENUE/SYMBOL"] = {
#       "snapshot_seed_count": int, "resync_count": int,
#       "desync_events": int, "fenced_ranges": int, ...
#   }
# Candidate-side (validation/replay_catalog_reconstruct.py) manifest shape:
#   manifest["depth_diagnostics"] = {
#       "snapshot_seeds": int, "resyncs": int, "desyncs": int,
#       "fenced_range_count": int, ...
#   }
#   manifest["fenced_ranges"] = [ {..fence dict with venue/symbol/date..}, ... ]
#
# Both ultimately originate from the same shared
# converter.depth_phase2.Phase2ReplayMetrics dataclass, but the two call
# sites (convert_day.py vs. replay_catalog_reconstruct.py) independently
# renamed the aggregated fields when assembling their own report/manifest
# dicts — this comparator normalizes both naming conventions rather than
# assuming either one.

_CONTINUITY_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "snapshot_seed_count": ("snapshot_seed_count", "snapshot_seeds"),
    "resync_count": ("resync_count", "resyncs"),
    "desync_events": ("desync_events", "desyncs"),
    "fenced_range_count": ("fenced_ranges", "fenced_range_count"),
}


def _extract_continuity_value(source: dict[str, Any], canonical_field: str) -> Any:
    for alias in _CONTINUITY_FIELD_ALIASES[canonical_field]:
        if alias in source:
            value = source[alias]
            # convert_day.py's "fenced_ranges" field is an int *count*
            # (`len(depth_metrics.fenced_ranges)`), not the list itself —
            # but guard defensively in case a caller passes the raw list.
            if canonical_field == "fenced_range_count" and isinstance(value, list):
                return len(value)
            return value
    return None


def compare_continuity_diagnostics_semantic(
    old_per_symbol_depth: dict[str, Any],
    new_depth_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    """Compare snapshot/resync/desync/fenced-range *counts* between the
    reference route's per-symbol depth report and the candidate route's
    depth_diagnostics manifest section.

    This is a count-level comparison (both sides already aggregate to a
    scalar count per symbol/day); a stronger content-level comparison of
    individual fenced-range boundaries is provided by
    compare_fenced_ranges_semantic() below.
    """
    result: dict[str, Any] = {"field_mismatches": {}}
    for canonical_field in _CONTINUITY_FIELD_ALIASES:
        old_value = _extract_continuity_value(old_per_symbol_depth, canonical_field)
        new_value = _extract_continuity_value(new_depth_diagnostics, canonical_field)
        result[f"{canonical_field}_old"] = old_value
        result[f"{canonical_field}_new"] = new_value
        if old_value is None or new_value is None:
            result["field_mismatches"][canonical_field] = {
                "old": old_value,
                "new": new_value,
                "reason": "missing on one side",
            }
        elif old_value != new_value:
            result["field_mismatches"][canonical_field] = {"old": old_value, "new": new_value}

    result["passed"] = not result["field_mismatches"]
    return result


def _fence_key(fence: dict[str, Any]) -> tuple[Any, ...]:
    """A fence's identity should not depend on wall-clock diagnostic
    metadata (e.g. detection time); key on the boundary itself."""
    return (
        fence.get("venue"),
        fence.get("symbol"),
        fence.get("start_ts_ns", fence.get("start")),
        fence.get("end_ts_ns", fence.get("end")),
        fence.get("severity"),
        fence.get("reason", fence.get("kind")),
    )


def compare_fenced_ranges_semantic(
    old_fenced_ranges: Iterable[dict[str, Any]],
    new_fenced_ranges: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Compare individual fenced (unrecovered discontinuity) ranges by
    content, not just by count. Only meaningful when both the reference and
    candidate routes expose a per-fence list (convert_day.py's own
    per-symbol report today only exposes a count — see
    compare_continuity_diagnostics_semantic() for that case); this function
    is for routes/versions that do expose the list (e.g. the candidate
    manifest's `fenced_ranges`), and is forward-looking for a reference-side
    fenced-range list export."""
    old_list = list(old_fenced_ranges)
    new_list = list(new_fenced_ranges)
    old_keys = {_fence_key(f) for f in old_list}
    new_keys = {_fence_key(f) for f in new_list}
    missing = sorted(str(k) for k in (old_keys - new_keys))
    extra = sorted(str(k) for k in (new_keys - old_keys))
    return {
        "count_old": len(old_list),
        "count_new": len(new_list),
        "count_match": len(old_list) == len(new_list),
        "missing_in_new": missing,
        "extra_in_new": extra,
        "passed": len(old_list) == len(new_list) and not missing and not extra,
    }


# ---------------------------------------------------------------------------
# Quality-flag behavior
# ---------------------------------------------------------------------------


def compare_quality_flags_semantic(
    old_quality_flags: Iterable[Any],
    new_quality_flags: Iterable[Any],
) -> dict[str, Any]:
    """Compare per-event quality-flag payloads by decoded content (multiset
    of parsed JSON dicts), not raw string equality — the underlying replay
    schema stores `quality_flags` as a JSON-encoded string
    (`stores/replay_schema.py`), and two logically identical flag sets could
    differ in key ordering or whitespace without differing semantically.

    Per issue #20 Phase 1: quality-flag behavior is part of the required
    semantic comparison and must remain reconstructable — this proves the
    oracle can actually detect a difference in quality-flag content, ahead
    of any decision about whether/how to compact that field's physical
    representation."""

    def _parse(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(value)
        except (TypeError, ValueError, json.JSONDecodeError):
            return value

    old_parsed = [_parse(v) for v in old_quality_flags]
    new_parsed = [_parse(v) for v in new_quality_flags]

    def _to_comparable(value: Any) -> str:
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True)
        return str(value)

    old_counter: dict[str, int] = {}
    for value in old_parsed:
        key = _to_comparable(value)
        old_counter[key] = old_counter.get(key, 0) + 1
    new_counter: dict[str, int] = {}
    for value in new_parsed:
        key = _to_comparable(value)
        new_counter[key] = new_counter.get(key, 0) + 1

    all_keys = set(old_counter) | set(new_counter)
    mismatches = {
        key: {"old_count": old_counter.get(key, 0), "new_count": new_counter.get(key, 0)}
        for key in all_keys
        if old_counter.get(key, 0) != new_counter.get(key, 0)
    }
    return {
        "count_old": len(old_parsed),
        "count_new": len(new_parsed),
        "distinct_values_old": len(old_counter),
        "distinct_values_new": len(new_counter),
        "mismatches": mismatches,
        "passed": not mismatches,
    }