"""
pipeline.raw_manifest — Raw data coverage scanning for daily manifest.

Scans raw directory to determine available venues, symbols, and channels.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Optional

from config import DATA_ROOT

logger = logging.getLogger(__name__)


def _sha256_file(path: Path) -> str:
    """Stream a file through SHA-256 in bounded (64 KiB) chunks — never reads
    a whole raw file into memory at once."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_raw_source_identity(
    venue: str,
    symbol: str,
    date_str: str,
    channels: "list[str]",
    data_root: Optional[Path] = None,
) -> dict:
    """Record per-file identity (path + SHA-256 + size) for the raw files that
    back one venue/symbol/date/channel set.

    This is the minimal, best-effort "source-identity/checksum information"
    the issue #20 Phase 2 traceability design (docs/IMPLEMENTATION_AUDIT.md,
    "Traceability design") requires new replay manifests to carry (item 1 of
    its planned hierarchy: "Raw file/chunk identity + SHA-256 checksum,
    recorded per partition in the manifest"). It does NOT implement the full
    planned hierarchy (block-level checksums, deterministic event->source
    mapping, etc. remain design-only / not implemented) and does NOT replace
    the per-event ``native_payload_hash`` retained in the v1 replay schema.

    Returns:
        {
            "channels": {"depth_v2": [{"path": "<file>", "sha256": "<hex>",
                                        "size_bytes": <int>}, ...], ...},
            "complete": bool,  # True only if every requested channel had at
                                # least one raw file found on disk
            "missing_channels": [<channel names with no raw files found>],
        }
    """
    if data_root is None:
        data_root = DATA_ROOT
    data_root = Path(data_root)

    result: dict = {"channels": {}, "complete": True, "missing_channels": []}
    for channel in channels:
        channel_dir = data_root / venue / channel / symbol / date_str
        entries: "list[dict]" = []
        if channel_dir.exists():
            for fpath in sorted(channel_dir.iterdir()):
                if not fpath.is_file():
                    continue
                try:
                    entries.append({
                        "path": str(fpath.relative_to(data_root)),
                        "sha256": _sha256_file(fpath),
                        "size_bytes": fpath.stat().st_size,
                    })
                except Exception as exc:
                    logger.warning(f"Could not checksum raw file {fpath}: {exc}")
        result["channels"][channel] = entries
        if not entries:
            result["complete"] = False
            result["missing_channels"].append(channel)
    return result


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
