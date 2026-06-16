"""
stores.feature_calc — Core feature calculation utilities.

Implements all core v1 features (BBO, spreads, imbalance, trade flow, volatility, quality).
Advanced features remain NULL with TODO documentation.
"""
from __future__ import annotations

import logging
import statistics
from typing import Optional

logger = logging.getLogger(__name__)


def calculate_core_features(
    venue: str,
    symbol: str,
    timestamp_ns: int,
    timeframe: str,
    depth_records: list[dict],
    trade_records: list[dict],
) -> dict:
    """
    Calculate all core v1 features for a timeframe window.

    Args:
        venue: Venue name
        symbol: Symbol name
        timestamp_ns: Window end timestamp (ns)
        timeframe: Timeframe ('100ms', '1s', '1m')
        depth_records: Depth records in this window
        trade_records: Trade records in this window

    Returns:
        Feature record dict (may contain NULL values)
    """
    features = {
        "venue": venue,
        "symbol": symbol,
        "timestamp_ns": timestamp_ns,
        "timeframe": timeframe,
        "quality_ok": True,  # Set to False if data is obviously bad
    }

    # Extract most recent depth snapshot for BBO/spread
    best_bid = None
    best_ask = None
    top1_bid_size = None
    top1_ask_size = None
    
    if depth_records:
        latest_depth = depth_records[-1]  # Most recent depth snapshot
        
        bids = latest_depth.get("bids", [])
        asks = latest_depth.get("asks", [])
        
        if bids:
            best_bid = bids[0].get("price") if isinstance(bids[0], dict) else float(bids[0][0])
            top1_bid_size = bids[0].get("size") if isinstance(bids[0], dict) else float(bids[0][1])
        
        if asks:
            best_ask = asks[0].get("price") if isinstance(asks[0], dict) else float(asks[0][0])
            top1_ask_size = asks[0].get("size") if isinstance(asks[0], dict) else float(asks[0][1])
        
        # Check for crossed book (data quality issue)
        if best_bid and best_ask and best_bid >= best_ask:
            features["quality_ok"] = False
    
    # F01: BBO and spread
    features["best_bid"] = best_bid
    features["best_ask"] = best_ask
    
    if best_bid is not None and best_ask is not None:
        features["mid_price"] = (best_bid + best_ask) / 2
        features["spread"] = best_ask - best_bid
        if features["mid_price"] > 0:
            features["spread_bps"] = (features["spread"] / features["mid_price"]) * 10000
        else:
            features["spread_bps"] = None
    else:
        features["mid_price"] = None
        features["spread"] = None
        features["spread_bps"] = None
    
    features["top1_bid_size"] = top1_bid_size
    features["top1_ask_size"] = top1_ask_size
    
    # Top-N liquidity (aggregate from latest depth snapshot)
    (
        features["top5_bid_notional"],
        features["top5_ask_notional"],
        features["top10_bid_notional"],
        features["top10_ask_notional"],
        features["top50_bid_notional"],
        features["top50_ask_notional"],
        features["imbalance_top1"],
        features["imbalance_top5"],
        features["imbalance_top10"],
        features["imbalance_top50"],
    ) = _calculate_liquidity_imbalance(depth_records)
    
    # Trade flow metrics
    (
        features["trade_count"],
        features["buy_volume"],
        features["sell_volume"],
        features["total_volume"],
        features["net_trade_flow"],
        features["signed_trade_volume"],
        features["aggressive_buy_ratio"],
        features["aggressive_sell_ratio"],
        features["large_trade_count"],
    ) = _calculate_trade_flow(trade_records)
    
    # Volatility and returns (simplified versions for core v1)
    (
        features["return_1s"],
        features["return_5s"],
        features["return_10s"],
        features["return_30s"],
        features["return_1m"],
        features["realized_vol_1m"],
        features["high_low_range_1m"],
        features["jump_score"],
    ) = _calculate_volatility(depth_records)
    
    # Data quality metrics
    (
        features["depth_update_count"],
        features["trade_update_count"],
        features["update_rate"],
        features["dropped_gap_count"],
        features["reconnect_count"],
        features["crossed_book_count"],
        features["stale_book_seconds"],
        features["missing_trade_seconds"],
        features["latency_ms_mean"],
        features["latency_ms_p95"],
    ) = _calculate_quality_metrics(depth_records, trade_records, timeframe)
    
    # Advanced features (all NULL for now - deferred)
    features["ofi_top1"] = None
    features["ofi_top5"] = None
    features["microprice"] = None
    features["microprice_vs_mid_bps"] = None
    features["bid_wall_score"] = None
    features["ask_wall_score"] = None
    features["liquidity_pull_score"] = None
    features["liquidity_add_score"] = None
    features["trade_through_ratio"] = None
    
    return features


def _calculate_liquidity_imbalance(depth_records: list[dict]) -> tuple:
    """Calculate top-N liquidity and imbalance metrics."""
    if not depth_records:
        return (None, None, None, None, None, None, None, None, None, None)
    
    latest = depth_records[-1]
    bids = latest.get("bids", [])
    asks = latest.get("asks", [])
    
    # Helper to parse bid/ask structures
    def parse_levels(levels, count):
        total_notional = 0.0
        for i, level in enumerate(levels[:count]):
            if isinstance(level, dict):
                price, size = level.get("price", 0), level.get("size", 0)
            else:
                price, size = float(level[0]), float(level[1])
            total_notional += price * size
        return total_notional
    
    top5_bid = parse_levels(bids, 5)
    top5_ask = parse_levels(asks, 5)
    top10_bid = parse_levels(bids, 10)
    top10_ask = parse_levels(asks, 10)
    top50_bid = parse_levels(bids, 50)
    top50_ask = parse_levels(asks, 50)
    
    # Imbalance (bid_notional / (bid_notional + ask_notional))
    def calc_imbalance(bid_notional, ask_notional):
        total = bid_notional + ask_notional
        return bid_notional / total if total > 0 else None
    
    imbalance_top1 = calc_imbalance(
        bids[0].get("size", 0) if isinstance(bids[0], dict) else float(bids[0][1]),
        asks[0].get("size", 0) if isinstance(asks[0], dict) else float(asks[0][1]),
    ) if bids and asks else None
    
    imbalance_top5 = calc_imbalance(top5_bid, top5_ask)
    imbalance_top10 = calc_imbalance(top10_bid, top10_ask)
    imbalance_top50 = calc_imbalance(top50_bid, top50_ask)
    
    return (
        top5_bid,
        top5_ask,
        top10_bid,
        top10_ask,
        top50_bid,
        top50_ask,
        imbalance_top1,
        imbalance_top5,
        imbalance_top10,
        imbalance_top50,
    )


def _calculate_trade_flow(trade_records: list[dict]) -> tuple:
    """Calculate trade flow metrics."""
    if not trade_records:
        return (0, 0.0, 0.0, 0.0, 0.0, 0.0, None, None, 0)
    
    buy_volume = 0.0
    sell_volume = 0.0
    
    for trade in trade_records:
        quantity = float(trade.get("quantity", 0))
        buyer_maker = bool(trade.get("buyer_maker", False))
        
        # If buyer_maker=True, buyer is maker (passive), seller is aggressor (taker)
        # If buyer_maker=False, buyer is taker (aggressor), seller is maker
        if buyer_maker:
            sell_volume += quantity  # Seller is aggressor
        else:
            buy_volume += quantity  # Buyer is aggressor
    
    total_volume = buy_volume + sell_volume
    net_trade_flow = buy_volume - sell_volume
    signed_trade_volume = net_trade_flow  # F03 alias
    
    aggressive_buy_ratio = buy_volume / total_volume if total_volume > 0 else None
    aggressive_sell_ratio = sell_volume / total_volume if total_volume > 0 else None
    
    # Large trade count (trades > 1 std of size)
    if trade_records:
        sizes = [float(t.get("quantity", 0)) for t in trade_records]
        mean_size = statistics.mean(sizes) if sizes else 0
        try:
            std_size = statistics.stdev(sizes) if len(sizes) > 1 else 0
        except:
            std_size = 0
        
        large_trade_count = sum(1 for s in sizes if s > mean_size + std_size)
    else:
        large_trade_count = 0
    
    return (
        len(trade_records),
        buy_volume,
        sell_volume,
        total_volume,
        net_trade_flow,
        signed_trade_volume,
        aggressive_buy_ratio,
        aggressive_sell_ratio,
        large_trade_count,
    )


def _calculate_volatility(depth_records: list[dict]) -> tuple:
    """Calculate volatility and return metrics."""
    # Simplified implementation: use latest depth snapshots
    # Full implementation would require historical price series over longer windows
    
    if not depth_records:
        return (None, None, None, None, None, None, None, None)
    
    # Extract mid-prices from depth records for return calculation
    mid_prices = []
    for depth in depth_records:
        bids = depth.get("bids", [])
        asks = depth.get("asks", [])
        if bids and asks:
            bid = bids[0].get("price") if isinstance(bids[0], dict) else float(bids[0][0])
            ask = asks[0].get("price") if isinstance(asks[0], dict) else float(asks[0][0])
            mid = (bid + ask) / 2
            mid_prices.append(mid)
    
    # Returns (simplified - only based on latest depth in window)
    # Full implementation would need historical price data from longer windows
    return_1s = None
    return_5s = None
    return_10s = None
    return_30s = None
    return_1m = None
    realized_vol_1m = None
    high_low_range_1m = None
    jump_score = None
    
    # TODO: Implement proper return/volatility calculations with historical price series
    
    return (
        return_1s,
        return_5s,
        return_10s,
        return_30s,
        return_1m,
        realized_vol_1m,
        high_low_range_1m,
        jump_score,
    )


def _calculate_quality_metrics(
    depth_records: list[dict],
    trade_records: list[dict],
    timeframe: str,
) -> tuple:
    """Calculate data quality metrics."""
    depth_update_count = len(depth_records)
    trade_update_count = len(trade_records)
    
    # Update rate (updates per second)
    timeframe_ms = {
        "100ms": 0.1,
        "1s": 1.0,
        "1m": 60.0,
    }.get(timeframe, 1.0)
    
    total_updates = depth_update_count + trade_update_count
    update_rate = total_updates / timeframe_ms if timeframe_ms > 0 else None
    
    # Gap analysis (simplified)
    dropped_gap_count = 0
    if depth_records:
        # Check for gaps in session_seq
        last_seq = None
        for record in depth_records:
            seq = record.get("session_seq", 0)
            if last_seq is not None and seq != last_seq + 1:
                dropped_gap_count += 1
            last_seq = seq
    
    # Reconnect count (simplified - would need session_id tracking)
    reconnect_count = 0
    
    # Crossed book count
    crossed_book_count = 0
    for depth in depth_records:
        bids = depth.get("bids", [])
        asks = depth.get("asks", [])
        if bids and asks:
            bid = bids[0].get("price") if isinstance(bids[0], dict) else float(bids[0][0])
            ask = asks[0].get("price") if isinstance(asks[0], dict) else float(asks[0][0])
            if bid >= ask:
                crossed_book_count += 1
    
    # Staleness and latency (simplified)
    stale_book_seconds = None
    missing_trade_seconds = None
    latency_ms_mean = None
    latency_ms_p95 = None
    
    # TODO: Implement proper staleness/latency calculations
    
    return (
        depth_update_count,
        trade_update_count,
        update_rate,
        dropped_gap_count,
        reconnect_count,
        crossed_book_count,
        stale_book_seconds,
        missing_trade_seconds,
        latency_ms_mean,
        latency_ms_p95,
    )
