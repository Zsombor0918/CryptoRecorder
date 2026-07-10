"""
converter.readers — Streaming JSONL decompression and record parsing.
"""
from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Generator, Optional

import zstandard as zstd

from config import DATA_ROOT

logger = logging.getLogger(__name__)


def stream_raw_records(
    venue: str,
    symbol: str,
    channel: str,
    date_str: str,
    root: Optional[Path] = None,
) -> Generator[dict, None, None]:
    """
    Yield parsed JSON dicts from ``{root}/{venue}/{channel}/{symbol}/{date}/``.

    Args:
        venue: Venue name (e.g., 'BINANCE_SPOT')
        symbol: Symbol name (e.g., 'BTCUSDT')
        channel: Channel name (e.g., 'depth_v2', 'trade_v2')
        date_str: Date string (e.g., '2026-06-15')
        root: Optional custom root path. If None, uses config.DATA_ROOT (backward compatible).
    """
    if root is None:
        root = DATA_ROOT

    day_path = root / venue / channel / symbol / date_str
    if not day_path.exists():
        return
    for file_path in sorted(day_path.glob("*.jsonl*")):
        try:
            if file_path.suffix == ".zst":
                opener = lambda p=file_path: zstd.open(p, "rt", errors="ignore")
            elif file_path.suffix == ".gz":
                opener = lambda p=file_path: gzip.open(p, "rt", errors="ignore")
            else:
                opener = lambda p=file_path: open(p, "r", errors="ignore")

            with opener() as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass  # counted at call-site
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
