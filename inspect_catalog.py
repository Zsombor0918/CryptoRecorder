"""
inspect_catalog.py — Catalog quality inspection for crossed-book and spread sanity.

Provides inspection functions used by validators to verify that the Nautilus
catalog contains no crossed-book snapshots and meets spread/quality thresholds.

This module is imported by validators/nautilus_catalog.py.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def inspect_catalog_depth(
    catalog,
    *,
    fail_limit: int = 5,
    date_str: Optional[str] = None,
) -> Dict[str, Any]:
    """Inspect OrderBookDepth10 snapshots for crossed-book and spread issues.

    Args:
        catalog: ParquetDataCatalog instance
        fail_limit: Max failed instruments to report per venue
        date_str: Optional date filter (YYYY-MM-DD)

    Returns:
        Dict with per-venue inspection results:
        {
            "venues": {
                "spot": {
                    "checked_instruments": N,
                    "checked_snapshots": N,
                    "valid_snapshots": N,
                    "crossed_snapshots": N,
                    "empty_snapshots": N,
                    "positive_spread_ratio": float,
                    "failed_instruments": [...],
                    "passed": bool,
                },
                "futures": {...},
            },
            "passed": bool,
        }
    """
    from nautilus_trader.model.instruments import CryptoPerpetual, CurrencyPair

    result: Dict[str, Any] = {
        "venues": {
            "spot": _empty_venue_result(),
            "futures": _empty_venue_result(),
        },
        "passed": False,
    }

    try:
        instruments = catalog.instruments()
    except Exception as e:
        logger.warning(f"Failed to load instruments: {e}")
        return result

    spot_instruments = [i for i in instruments if isinstance(i, CurrencyPair)]
    futures_instruments = [i for i in instruments if isinstance(i, CryptoPerpetual)]

    result["venues"]["spot"] = _inspect_venue_depth(
        catalog, spot_instruments, "spot", fail_limit, date_str
    )
    result["venues"]["futures"] = _inspect_venue_depth(
        catalog, futures_instruments, "futures", fail_limit, date_str
    )

    result["passed"] = (
        result["venues"]["spot"]["passed"]
        and result["venues"]["futures"]["passed"]
    )
    return result


def _empty_venue_result() -> Dict[str, Any]:
    return {
        "checked_instruments": 0,
        "checked_snapshots": 0,
        "valid_snapshots": 0,
        "crossed_snapshots": 0,
        "empty_snapshots": 0,
        "positive_spread_ratio": 1.0,
        "failed_instruments": [],
        "passed": True,
    }


def _inspect_venue_depth(
    catalog,
    instruments: list,
    venue_name: str,
    fail_limit: int,
    date_str: Optional[str],
) -> Dict[str, Any]:
    """Inspect depth snapshots for a single venue."""
    result = _empty_venue_result()

    if not instruments:
        return result

    result["checked_instruments"] = len(instruments)
    total_snapshots = 0
    valid_snapshots = 0
    crossed_snapshots = 0
    empty_snapshots = 0
    failed_instruments: List[str] = []

    for inst in instruments:
        try:
            depth_list = catalog.order_book_depth10(instrument_ids=[inst.id])
            if not depth_list:
                continue

            inst_crossed = 0
            inst_empty = 0

            for snap in depth_list:
                total_snapshots += 1

                # Extract best bid/ask prices
                best_bid = _best_price(snap.bids, "bid")
                best_ask = _best_price(snap.asks, "ask")

                if best_bid is None or best_ask is None:
                    empty_snapshots += 1
                    inst_empty += 1
                elif best_bid >= best_ask:
                    crossed_snapshots += 1
                    inst_crossed += 1
                else:
                    valid_snapshots += 1

            if inst_crossed > 0 and len(failed_instruments) < fail_limit:
                failed_instruments.append(
                    f"{inst.id}: {inst_crossed} crossed of {len(depth_list)}"
                )

        except Exception as e:
            logger.debug(f"Failed to inspect {inst.id}: {e}")
            continue

    result["checked_snapshots"] = total_snapshots
    result["valid_snapshots"] = valid_snapshots
    result["crossed_snapshots"] = crossed_snapshots
    result["empty_snapshots"] = empty_snapshots
    result["failed_instruments"] = failed_instruments

    # Calculate positive spread ratio
    if total_snapshots > 0:
        result["positive_spread_ratio"] = round(
            valid_snapshots / total_snapshots, 4
        )
    else:
        result["positive_spread_ratio"] = 1.0

    # Pass if no crossed snapshots (empty is allowed in sparse data)
    result["passed"] = crossed_snapshots == 0

    return result


def _best_price(orders, side: str) -> Optional[float]:
    """Extract best price from a list of BookOrder objects."""
    for order in orders:
        try:
            size = float(str(order.size))
            if size > 0:
                return float(str(order.price))
        except Exception:
            continue
    return None


# ── data presence inspection ─────────────────────────────────────────


def inspect_data_presence(catalog, date_str: Optional[str] = None) -> Dict[str, Any]:
    """Inspect which instruments have trade/depth data.

    Returns:
        {
            "instruments_defined": N,
            "instruments_with_trades": N,
            "instruments_with_depth": N,
            "instruments_with_both": N,
            "instruments_with_no_data": N,
            "no_data_list": [...],
            "spot": {...},
            "futures": {...},
        }
    """
    from nautilus_trader.model.instruments import CryptoPerpetual, CurrencyPair

    result: Dict[str, Any] = {
        "instruments_defined": 0,
        "instruments_with_trades": 0,
        "instruments_with_depth": 0,
        "instruments_with_both": 0,
        "instruments_with_no_data": 0,
        "no_data_list": [],
        "spot": {
            "defined": 0,
            "with_trades": 0,
            "with_depth": 0,
            "with_both": 0,
            "with_no_data": 0,
        },
        "futures": {
            "defined": 0,
            "with_trades": 0,
            "with_depth": 0,
            "with_both": 0,
            "with_no_data": 0,
        },
    }

    try:
        instruments = catalog.instruments()
    except Exception as e:
        logger.warning(f"Failed to load instruments: {e}")
        return result

    result["instruments_defined"] = len(instruments)

    for inst in instruments:
        is_futures = isinstance(inst, CryptoPerpetual)
        venue_key = "futures" if is_futures else "spot"
        result[venue_key]["defined"] += 1

        has_trades = False
        has_depth = False

        try:
            trades = catalog.trade_ticks(instrument_ids=[inst.id])
            has_trades = len(trades) > 0
        except Exception:
            pass

        try:
            depth = catalog.order_book_depth10(instrument_ids=[inst.id])
            has_depth = len(depth) > 0
        except Exception:
            pass

        if has_trades:
            result["instruments_with_trades"] += 1
            result[venue_key]["with_trades"] += 1
        if has_depth:
            result["instruments_with_depth"] += 1
            result[venue_key]["with_depth"] += 1
        if has_trades and has_depth:
            result["instruments_with_both"] += 1
            result[venue_key]["with_both"] += 1
        if not has_trades and not has_depth:
            result["instruments_with_no_data"] += 1
            result[venue_key]["with_no_data"] += 1
            if len(result["no_data_list"]) < 20:
                result["no_data_list"].append(str(inst.id))

    return result
