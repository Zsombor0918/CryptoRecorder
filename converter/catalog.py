"""
converter.catalog — Idempotent Nautilus catalog writing helpers.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def purge_catalog_data(catalog_root: Path, instruments) -> int:
    """Remove existing Parquet files for the given instruments.

    This allows re-running the converter for the same date without
    'non-disjoint intervals' errors from the Nautilus catalog writer.

    Returns count of purged files.
    """
    data_dir = catalog_root / "data"
    if not data_dir.exists():
        return 0

    purged = 0
    for inst in instruments:
        iid_str = str(inst.id)
        for subdir in ("trade_tick", "order_book_depths"):
            inst_dir = data_dir / subdir / iid_str
            if inst_dir.exists():
                for pf in inst_dir.glob("*.parquet"):
                    pf.unlink()
                    purged += 1

    # Purge instrument metadata parquets (cheap to regenerate)
    for cls_name in ("crypto_perpetual", "currency_pair"):
        cls_dir = data_dir / cls_name
        if cls_dir.exists():
            for iid_dir in cls_dir.iterdir():
                if iid_dir.is_dir():
                    for pf in iid_dir.glob("*.parquet"):
                        pf.unlink()
                        purged += 1

    if purged:
        logger.info(f"Purged {purged} existing Parquet files for idempotent re-write")
    return purged
