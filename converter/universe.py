"""
converter.universe — Resolve which symbols to convert for a given date.

Priority: meta/universe/{VENUE}/{date}.json → fallback to disk scan.
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
    """Return ``{venue: [symbol, ...]}`` for the given date."""
    result: Dict[str, List[str]] = {}
    for venue in VENUES:
        ufile = META_ROOT / "universe" / venue / f"{date_str}.json"
        if ufile.exists():
            try:
                syms = json.loads(ufile.read_text())
                if isinstance(syms, list) and syms:
                    result[venue] = syms
                    continue
            except Exception:
                pass
        # Fallback: scan raw directories
        syms = _discover_symbols_from_disk(venue, date_str)
        if syms:
            result[venue] = sorted(syms)
    return result


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
