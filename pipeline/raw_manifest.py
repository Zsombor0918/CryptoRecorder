"""
pipeline.raw_manifest — Raw data coverage scanning for daily manifest.

Scans raw directory to determine available venues, symbols, and channels.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from config import DATA_ROOT

logger = logging.getLogger(__name__)


def scan_raw_coverage(
    date_str: str,
    data_root: Optional[Path] = None,
) -> dict:
    """
    Scan raw data directory for available venues/symbols/channels on a given date.

    Args:
        date_str: Date string (YYYY-MM-DD)
        data_root: Optional custom data_root. If None, uses config.DATA_ROOT.

    Returns:
        Dict with structure:
        {
            "date": "2026-06-15",
            "venues": ["BINANCE_SPOT", "BINANCE_USDTF"],
            "data": {
                "BINANCE_SPOT": {
                    "BTCUSDT": {"depth_v2": True, "trade_v2": True},
                    "ETHUSDT": {"depth_v2": True, "trade_v2": True},
                },
                "BINANCE_USDTF": {...}
            },
            "symbol_count": 2,
            "errors": [],
        }
    """
    if data_root is None:
        data_root = DATA_ROOT

    result = {
        "date": date_str,
        "venues": [],
        "data": {},
        "symbol_count": 0,
        "errors": [],
    }

    data_root = Path(data_root)
    if not data_root.exists():
        result["errors"].append(f"data_root does not exist: {data_root}")
        return result

    try:
        # List venues (directories like BINANCE_SPOT, BINANCE_USDTF)
        venue_dirs = sorted([d for d in data_root.iterdir() if d.is_dir()])

        for venue_dir in venue_dirs:
            venue = venue_dir.name
            result["venues"].append(venue)
            result["data"][venue] = {}

            try:
                # List channels (depth_v2, trade_v2, exchangeinfo)
                for channel_dir in sorted(venue_dir.iterdir()):
                    if not channel_dir.is_dir():
                        continue

                    channel = channel_dir.name

                    # List symbols for this channel/date
                    for symbol_dir in sorted(channel_dir.iterdir()):
                        if not symbol_dir.is_dir():
                            continue

                        symbol = symbol_dir.name

                        # Check if date exists for this symbol/channel
                        date_dir = symbol_dir / date_str
                        if date_dir.exists():
                            # Record this symbol/channel combination
                            if symbol not in result["data"][venue]:
                                result["data"][venue][symbol] = {}
                            result["data"][venue][symbol][channel] = True

            except Exception as e:
                result["errors"].append(f"Error scanning venue {venue}: {e}")
                logger.error(f"Error scanning venue {venue}: {e}")

        # Count unique symbols across all venues
        all_symbols = set()
        for venue_data in result["data"].values():
            all_symbols.update(venue_data.keys())
        result["symbol_count"] = len(all_symbols)

        logger.info(
            f"Raw coverage scan for {date_str}: {len(result['venues'])} venues, "
            f"{result['symbol_count']} symbols"
        )

    except Exception as e:
        result["errors"].append(f"Critical error scanning raw_manifest: {e}")
        logger.error(f"Critical error scanning raw_manifest: {e}")

    return result
