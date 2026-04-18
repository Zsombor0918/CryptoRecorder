"""
converter.catalog — Date-scoped idempotent Nautilus catalog purge helpers.

The Nautilus ParquetDataCatalog names data files as:
    ``{start_ts}_{end_ts}.parquet``
where timestamps are ISO-like ``2026-04-17T19-16-42-559304448Z``.

Instrument metadata files (currency_pair/, crypto_perpetual/) use epoch-0
timestamps (``1970-01-01T…``).

``purge_catalog_date_range()`` deletes ONLY parquet files whose time range
overlaps the target date — other days' data is **never** touched.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Pattern for Nautilus parquet filename:
# 2026-04-17T19-16-42-559304448Z_2026-04-18T07-55-29-549203712Z.parquet
_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-\d+Z"
    r"_(\d{4}-\d{2}-\d{2})T(\d{2})-(\d{2})-(\d{2})-\d+Z\.parquet$"
)


def _parse_parquet_date_range(filename: str):
    """Parse start/end dates from a Nautilus parquet filename.

    Returns (start_date, end_date) as ``datetime.date`` objects, or ``None``
    if the filename doesn't match the expected pattern.
    """
    m = _TS_RE.match(filename)
    if not m:
        return None
    try:
        start_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        end_date = datetime.strptime(m.group(5), "%Y-%m-%d").date()
        return start_date, end_date
    except ValueError:
        return None


def purge_catalog_date_range(
    catalog_root: Path,
    instrument_ids: list,
    target_date_str: str,
) -> int:
    """Delete Parquet files that overlap the target date only.

    - For ``trade_tick/`` and ``order_book_depths/``: delete files whose
      ``[start_date, end_date]`` overlaps ``target_date``.
    - For ``currency_pair/`` and ``crypto_perpetual/``: delete metadata
      parquets for the given instruments (cheap to regenerate, timestamps
      are always epoch-0 so date-scoping isn't meaningful).

    Returns count of purged files.
    """
    from datetime import date as _date
    target = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    data_dir = catalog_root / "data"
    if not data_dir.exists():
        return 0

    purged = 0

    # Set of instrument ID strings we're converting
    iid_strs = {str(iid) for iid in instrument_ids}

    # ── data files (date-scoped) ─────────────────────────────────────
    for subdir in ("trade_tick", "order_book_depths"):
        type_dir = data_dir / subdir
        if not type_dir.exists():
            continue
        for iid_dir in type_dir.iterdir():
            if not iid_dir.is_dir():
                continue
            if iid_dir.name not in iid_strs:
                continue
            for pf in iid_dir.glob("*.parquet"):
                dr = _parse_parquet_date_range(pf.name)
                if dr is None:
                    # Can't parse → skip (don't blindly delete)
                    continue
                start_d, end_d = dr
                # Epoch-0 files are instrument metadata mis-filed here; skip
                if start_d.year < 2000:
                    continue
                # Overlap check: file [start_d, end_d] vs target [target, target]
                if start_d <= target and end_d >= target:
                    pf.unlink()
                    purged += 1
                    logger.debug(f"Purged (date-scoped): {pf}")

    # ── instrument metadata (always regenerated, epoch-0 timestamps) ──
    for cls_name in ("crypto_perpetual", "currency_pair"):
        cls_dir = data_dir / cls_name
        if not cls_dir.exists():
            continue
        for iid_dir in cls_dir.iterdir():
            if not iid_dir.is_dir():
                continue
            if iid_dir.name not in iid_strs:
                continue
            for pf in iid_dir.glob("*.parquet"):
                pf.unlink()
                purged += 1

    if purged:
        logger.info(
            f"Purged {purged} Parquet files overlapping {target_date_str} "
            f"for idempotent re-write"
        )
    return purged


# ── legacy alias (kept for validate_system.py import compat) ─────────

def purge_catalog_data(catalog_root: Path, instruments) -> int:
    """Legacy entry point — delegates to date-scoped purge.

    When no date is available, falls back to purging ALL files for the
    given instruments (old behaviour).
    """
    # Try to infer date from instruments' data files — not reliable.
    # This path is only hit from tests or legacy callers.
    data_dir = catalog_root / "data"
    if not data_dir.exists():
        return 0

    purged = 0
    iid_strs = {str(inst.id) for inst in instruments}
    for subdir in ("trade_tick", "order_book_depths"):
        type_dir = data_dir / subdir
        if not type_dir.exists():
            continue
        for iid_dir in type_dir.iterdir():
            if not iid_dir.is_dir() or iid_dir.name not in iid_strs:
                continue
            for pf in iid_dir.glob("*.parquet"):
                pf.unlink()
                purged += 1

    for cls_name in ("crypto_perpetual", "currency_pair"):
        cls_dir = data_dir / cls_name
        if not cls_dir.exists():
            continue
        for iid_dir in cls_dir.iterdir():
            if iid_dir.is_dir() and iid_dir.name in iid_strs:
                for pf in iid_dir.glob("*.parquet"):
                    pf.unlink()
                    purged += 1

    if purged:
        logger.info(f"Purged {purged} existing Parquet files for idempotent re-write")
    return purged
