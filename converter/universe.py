"""
converter.universe — Resolve which symbols to convert for a given date.

Priority: meta/universe/{VENUE}/{date}.json → fallback to disk scan.

The recorder writes universe files as a dict with all venues:
  {"BINANCE_SPOT": [...], "BINANCE_USDTF": [...], "timestamp": "...", "top_count": 50}

Both venue-specific files contain the same dict.  This module extracts
the correct venue key.  Legacy list format ``[sym, ...]`` is also accepted.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Set

from config import DATA_ROOT, META_ROOT

logger = logging.getLogger(__name__)

VENUES = ("BINANCE_SPOT", "BINANCE_USDTF")


def resolve_universe(date_str: str) -> Dict[str, List[str]]:
    """Return ``{venue: [symbol, ...]}`` for the given date.

    Reads ``meta/universe/{VENUE}/{date}.json``.  Accepts:
      - dict with venue keys  (recorder format):  ``{"BINANCE_SPOT": [...], ...}``
      - plain list            (legacy format):    ``["BTCUSDT", ...]``

    Falls back to disk scan if no meta file is found.
    """
    result: Dict[str, List[str]] = {}
    for venue in VENUES:
        ufile = META_ROOT / "universe" / venue / f"{date_str}.json"
        if ufile.exists():
            try:
                raw = json.loads(ufile.read_text())
                syms = _extract_symbols(raw, venue)
                if syms:
                    result[venue] = syms
                    logger.debug(f"Universe {venue}/{date_str}: {len(syms)} symbols from meta")
                    continue
            except Exception as e:
                logger.warning(f"Failed to parse {ufile}: {e}")
        # Fallback: scan raw directories
        syms_set = _discover_symbols_from_disk(venue, date_str)
        if syms_set:
            logger.debug(f"Universe {venue}/{date_str}: {len(syms_set)} symbols from disk scan (fallback)")
            result[venue] = sorted(syms_set)
    return result


def _extract_symbols(raw, venue: str) -> List[str]:
    """Extract symbol list from either dict or list universe format.

    Dict format (recorder writes this):
        {"BINANCE_SPOT": ["BTCUSDT", ...], "BINANCE_USDTF": [...], "timestamp": "...", "top_count": 50}
    List format (legacy):
        ["BTCUSDT", ...]
    """
    if isinstance(raw, list):
        # Legacy: plain list of symbols
        return [s for s in raw if isinstance(s, str) and s] if raw else []

    if isinstance(raw, dict):
        # Recorder dict format: look for venue key
        venue_data = raw.get(venue)
        if isinstance(venue_data, list) and venue_data:
            return [s for s in venue_data if isinstance(s, str) and s]
        # Maybe the dict IS a single-venue list stored oddly — not expected, ignore
    return []


def _discover_symbols_from_disk(venue: str, date_str: str) -> Set[str]:
    syms: Set[str] = set()
    venue_dir = DATA_ROOT / venue
    if not venue_dir.exists():
        return syms
    for ch_dir in venue_dir.iterdir():
        if not ch_dir.is_dir() or ch_dir.name == "exchangeinfo":
            continue
        for sym_dir in ch_dir.iterdir():
            if sym_dir.is_dir() and (sym_dir / date_str).is_dir():
                syms.add(sym_dir.name)
    return syms
