"""Helpers for human-facing timestamps in state and report files.

Operational day partitioning remains UTC elsewhere in the codebase. These
helpers are only for report-style timestamps that humans read directly.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import REPORT_TIMEZONE_NAME

try:
    REPORT_TIMEZONE = ZoneInfo(REPORT_TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    REPORT_TIMEZONE = timezone.utc


def local_now() -> datetime:
    """Return the current report timestamp in the configured local timezone."""
    return datetime.now(REPORT_TIMEZONE)


def local_now_iso() -> str:
    """Return an ISO-8601 string with timezone offset."""
    return local_now().isoformat()


def timestamp_to_local_iso(ts: float) -> str:
    """Convert a POSIX timestamp to an ISO-8601 string in report timezone."""
    return datetime.fromtimestamp(ts, tz=REPORT_TIMEZONE).isoformat()
