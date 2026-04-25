"""Raw trade coverage and readiness helpers."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set

from config import DATA_ROOT, MIN_TRADE_RECORDS_FOR_FULL_READY, VENUES
from converter.readers import stream_raw_records


def _trade_ts_ns(record: dict) -> Optional[int]:
    ts_trade_ms = record.get("ts_trade_ms")
    ts_event_ms = record.get("ts_event_ms")
    ts_recv_ns = record.get("ts_recv_ns")
    if ts_trade_ms is not None:
        return int(ts_trade_ms) * 1_000_000
    if ts_event_ms is not None:
        return int(ts_event_ms) * 1_000_000
    if ts_recv_ns is not None:
        return int(ts_recv_ns)
    return None


def _discover_symbols(date_str: str, universe: Optional[Dict[str, Sequence[str]]] = None) -> Dict[str, List[str]]:
    discovered: Dict[str, Set[str]] = defaultdict(set)
    if universe:
        for venue, symbols in universe.items():
            discovered[venue].update(symbols)
    for venue in VENUES:
        base = Path(DATA_ROOT) / venue / "trade_v2"
        if not base.exists():
            continue
        for symbol_dir in sorted(base.iterdir()):
            if symbol_dir.is_dir() and (symbol_dir / date_str).exists():
                discovered[venue].add(symbol_dir.name)
    return {venue: sorted(symbols) for venue, symbols in discovered.items()}


def summarize_trade_coverage(
    date_str: str,
    universe: Optional[Dict[str, Sequence[str]]] = None,
) -> Dict[str, object]:
    symbols_by_venue = _discover_symbols(date_str, universe)
    venues: Dict[str, dict] = {}
    per_symbol: Dict[str, dict] = {}

    for venue, symbols in sorted(symbols_by_venue.items()):
        symbols_with_trades: List[str] = []
        symbols_without_trades: List[str] = []
        lifecycle_only_symbols: List[str] = []
        trade_counts: List[tuple[str, int]] = []

        for symbol in symbols:
            raw_record_count = 0
            trade_record_count = 0
            lifecycle_record_count = 0
            first_trade_ts = None
            last_trade_ts = None

            for rec in stream_raw_records(venue, symbol, "trade_v2", date_str):
                raw_record_count += 1
                record_type = rec.get("record_type", "trade")
                if record_type == "trade":
                    trade_record_count += 1
                    ts_ns = _trade_ts_ns(rec)
                    if ts_ns is not None:
                        if first_trade_ts is None or ts_ns < first_trade_ts:
                            first_trade_ts = ts_ns
                        if last_trade_ts is None or ts_ns > last_trade_ts:
                            last_trade_ts = ts_ns
                elif record_type == "trade_stream_lifecycle":
                    lifecycle_record_count += 1

            will_create_tradetick = trade_record_count > 0
            if will_create_tradetick:
                symbols_with_trades.append(symbol)
            else:
                symbols_without_trades.append(symbol)
            if trade_record_count == 0 and lifecycle_record_count > 0:
                lifecycle_only_symbols.append(symbol)
            trade_counts.append((symbol, trade_record_count))

            per_symbol[f"{venue}/{symbol}"] = {
                "raw_record_count": raw_record_count,
                "trade_record_count": trade_record_count,
                "lifecycle_record_count": lifecycle_record_count,
                "first_trade_ts": first_trade_ts,
                "last_trade_ts": last_trade_ts,
                "will_create_tradetick": will_create_tradetick,
            }

        venues[venue] = {
            "symbols": symbols,
            "symbols_with_trades": symbols_with_trades,
            "symbols_without_trades": symbols_without_trades,
            "lifecycle_only_symbols": lifecycle_only_symbols,
            "top_symbols_by_trade_count": [
                {"symbol": symbol, "trade_record_count": count}
                for symbol, count in sorted(trade_counts, key=lambda item: (-item[1], item[0]))[:10]
            ],
        }

    return {
        "date": date_str,
        "venues": venues,
        "per_symbol": per_symbol,
    }


def classify_symbol_readiness(*, has_trade_ticks: bool, has_depth: bool) -> str:
    if has_trade_ticks and has_depth:
        return "full_ready"
    if has_depth:
        return "l2_ready"
    if has_trade_ticks:
        return "trade_only"
    return "not_ready"


def build_readiness_summary(
    per_symbol_trade: Dict[str, Dict[str, int]],
    per_symbol_depth: Dict[str, Dict[str, int]],
    *,
    min_trade_records_for_full_ready: int = MIN_TRADE_RECORDS_FOR_FULL_READY,
) -> Dict[str, object]:
    readiness_counts = {
        "full_ready": 0,
        "l2_ready": 0,
        "trade_only": 0,
        "not_ready": 0,
    }
    suggested_full_ready_by_venue: Dict[str, List[str]] = defaultdict(list)
    per_symbol: Dict[str, dict] = {}
    by_venue: Dict[str, dict] = defaultdict(
        lambda: {
            "counts": {
                "full_ready": 0,
                "l2_ready": 0,
                "trade_only": 0,
                "not_ready": 0,
            },
            "suggested_full_ready_symbols": [],
            "lifecycle_only_symbols": [],
        }
    )

    for key in sorted(set(per_symbol_trade) | set(per_symbol_depth)):
        venue, symbol = key.split("/", 1)
        trade_info = per_symbol_trade.get(key, {})
        depth_info = per_symbol_depth.get(key, {})
        trade_ticks_written = int(trade_info.get("ticks_written", 0))
        trade_raw_trade_record_count = int(trade_info.get("raw_trade_record_count", 0))
        trade_raw_lifecycle_record_count = int(trade_info.get("raw_lifecycle_record_count", 0))
        delta_events_written = int(depth_info.get("deltas_written", 0))
        depth10_written = int(depth_info.get("depth10_written", 0))
        has_trade_ticks = trade_ticks_written > 0
        has_depth = delta_events_written > 0 or depth10_written > 0
        readiness = classify_symbol_readiness(
            has_trade_ticks=has_trade_ticks,
            has_depth=has_depth,
        )
        lifecycle_only = (
            trade_raw_trade_record_count == 0 and trade_raw_lifecycle_record_count > 0
        )
        eligible_full_ready = (
            readiness == "full_ready"
            and trade_raw_trade_record_count >= min_trade_records_for_full_ready
        )

        readiness_counts[readiness] += 1
        by_venue[venue]["counts"][readiness] += 1
        if lifecycle_only:
            by_venue[venue]["lifecycle_only_symbols"].append(symbol)
        if eligible_full_ready:
            suggested_full_ready_by_venue[venue].append(symbol)

        per_symbol[key] = {
            "readiness": readiness,
            "has_trade_ticks": has_trade_ticks,
            "has_depth": has_depth,
            "lifecycle_only": lifecycle_only,
            "eligible_for_full_ready_universe": eligible_full_ready,
        }

    for venue in by_venue:
        by_venue[venue]["suggested_full_ready_symbols"] = sorted(
            suggested_full_ready_by_venue.get(venue, [])
        )
        by_venue[venue]["lifecycle_only_symbols"] = sorted(by_venue[venue]["lifecycle_only_symbols"])

    return {
        "min_trade_records_for_full_ready": min_trade_records_for_full_ready,
        "counts": readiness_counts,
        "per_symbol": per_symbol,
        "by_venue": dict(by_venue),
        "suggested_full_ready_universe": {
            venue: sorted(symbols)
            for venue, symbols in suggested_full_ready_by_venue.items()
        },
    }