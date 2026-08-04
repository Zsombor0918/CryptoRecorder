"""Dependency-free Binance exchangeInfo loading and filter helpers."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import zstandard as zstd

from config import DATA_ROOT


def _precision_from_str(s: str) -> int:
    """Count decimal precision from a Binance filter string like ``0.01000000``."""
    s = s.rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _get_filter(filters: list, filter_type: str) -> dict:
    """Return the first Binance filter matching ``filter_type``."""
    for item in filters:
        if item.get("filterType") == filter_type:
            return item
    return {}


def load_exchange_info(
    venue: str,
    date_str: str,
    data_root: "Path | None" = None,
) -> Dict[str, dict]:
    """Load Binance exchangeInfo and return ``{symbol_str: info_dict}``.

    ``data_root`` defaults to ``config.DATA_ROOT`` for compatibility. Callers
    consuming an explicit raw root must pass that same root here.
    """
    root = Path(data_root) if data_root is not None else DATA_ROOT
    info_dir = root / venue / "exchangeinfo" / "EXCHANGEINFO" / date_str
    if not info_dir.exists():
        return {}

    symbol_map: Dict[str, dict] = {}
    files = sorted(info_dir.glob("*.jsonl*"), reverse=True)
    for file_path in files:
        try:
            if file_path.suffix == ".zst":
                opener = lambda p=file_path: zstd.open(p, "rt", errors="ignore")
            else:
                opener = lambda p=file_path: open(p, "r", errors="ignore")
            with opener() as file_handle:
                for line in file_handle:
                    payload = json.loads(line.strip())
                    for symbol in payload.get("symbols", []):
                        symbol_map[symbol["symbol"]] = symbol
                    if symbol_map:
                        return symbol_map
        except Exception:
            continue
    return symbol_map
