"""Dependency-free shared depth event-time repartitioning primitives.

These helpers define the raw-record selection boundary shared by the legacy
Nautilus converter and the schema-v2 replay builder.  Keeping them outside the
Nautilus object-construction module lets production replay builds run without
installing reconstruction-only dependencies while preserving one exact
implementation of timestamp selection and deduplication.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

EPOCH_LIKE_NS_MIN = 946684800000000000  # 2000-01-01T00:00:00Z


def ts_event_ns(record: dict) -> int:
    """Return the canonical event timestamp used by both conversion paths."""
    pre_ns = record.get("ts_event_ns")
    if pre_ns is not None:
        return int(pre_ns)
    ts_event_ms = record.get("ts_event_ms") or record.get("exchange_ts_ms")
    ts_recv_ns = int(record.get("ts_recv_ns", 0))
    return int(ts_event_ms) * 1_000_000 if ts_event_ms else ts_recv_ns


def date_shift(date_str: str, days: int) -> str:
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return (base + timedelta(days=days)).strftime("%Y-%m-%d")


def target_bounds_ns(date_str: str) -> Tuple[int, int]:
    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1_000_000_000), int(end.timestamp() * 1_000_000_000)


def is_epoch_like_ns(timestamp_ns: int) -> bool:
    return timestamp_ns >= EPOCH_LIKE_NS_MIN


def dedupe_key(record: dict) -> Tuple[object, ...]:
    return (
        record.get("record_type", "depth_update"),
        record.get("stream_session_id"),
        record.get("session_seq", record.get("connection_seq")),
        record.get("U"),
        record.get("u"),
        record.get("pu"),
        record.get("lastUpdateId"),
        ts_event_ns(record),
    )
