"""
stores.feature_schema — Parquet schema for feature_store.

Defines all features including core v1 (required) and advanced (nullable/TODO).
Strict lookahead-bias rule: no future data, no labels, only current/past.
"""
from __future__ import annotations

import pyarrow as pa


# ============================================================================
# Core v1 Features (Required - stably implemented)
# ============================================================================

FEATURE_SCHEMA_CORE_V1 = pa.schema([
    # Identifiers
    pa.field("venue", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp_ns", pa.int64(), nullable=False),  # Nanosecond timestamp
    pa.field("timeframe", pa.string(), nullable=False),  # '100ms', '1s', '1m'
    
    # Quality indicator
    pa.field("quality_ok", pa.bool_(), nullable=False),  # Whether this bar is valid
    
    # F01: BBO / Spread / Mid-price
    pa.field("best_bid", pa.float64(), nullable=True),
    pa.field("best_ask", pa.float64(), nullable=True),
    pa.field("mid_price", pa.float64(), nullable=True),
    pa.field("spread", pa.float64(), nullable=True),
    pa.field("spread_bps", pa.float64(), nullable=True),  # Spread in basis points
    
    # Top-N Liquidity (best bid/ask sizes)
    pa.field("top1_bid_size", pa.float64(), nullable=True),
    pa.field("top1_ask_size", pa.float64(), nullable=True),
    
    # Top-N Notional (cumulative value at each level)
    pa.field("top5_bid_notional", pa.float64(), nullable=True),
    pa.field("top5_ask_notional", pa.float64(), nullable=True),
    pa.field("top10_bid_notional", pa.float64(), nullable=True),
    pa.field("top10_ask_notional", pa.float64(), nullable=True),
    pa.field("top50_bid_notional", pa.float64(), nullable=True),
    pa.field("top50_ask_notional", pa.float64(), nullable=True),
    
    # F02/F07: Imbalance (bid_size / (bid_size + ask_size) for each level)
    pa.field("imbalance_top1", pa.float64(), nullable=True),
    pa.field("imbalance_top5", pa.float64(), nullable=True),
    pa.field("imbalance_top10", pa.float64(), nullable=True),
    pa.field("imbalance_top50", pa.float64(), nullable=True),
    
    # Trade Flow (from trade_v2 records in this window)
    pa.field("trade_count", pa.int64(), nullable=True),
    pa.field("buy_volume", pa.float64(), nullable=True),  # Volume where buyer is aggressor
    pa.field("sell_volume", pa.float64(), nullable=True),  # Volume where seller is aggressor
    pa.field("total_volume", pa.float64(), nullable=True),  # buy_volume + sell_volume
    pa.field("net_trade_flow", pa.float64(), nullable=True),  # buy_volume - sell_volume
    pa.field("signed_trade_volume", pa.float64(), nullable=True),  # F03: Signed volume (same as net_trade_flow)
    pa.field("aggressive_buy_ratio", pa.float64(), nullable=True),  # buy_volume / total_volume if total > 0
    pa.field("aggressive_sell_ratio", pa.float64(), nullable=True),
    pa.field("large_trade_count", pa.int64(), nullable=True),  # Count of trades > 1 std of size
    
    # F08: Volatility & Movement
    pa.field("return_1s", pa.float64(), nullable=True),  # Log return over 1s
    pa.field("return_5s", pa.float64(), nullable=True),
    pa.field("return_10s", pa.float64(), nullable=True),
    pa.field("return_30s", pa.float64(), nullable=True),
    pa.field("return_1m", pa.float64(), nullable=True),
    pa.field("realized_vol_1m", pa.float64(), nullable=True),  # Realized volatility over 1m
    pa.field("high_low_range_1m", pa.float64(), nullable=True),  # (high - low) / mid over 1m
    pa.field("jump_score", pa.float64(), nullable=True),  # Estimate of jump component
    
    # Data Quality Metrics
    pa.field("depth_update_count", pa.int64(), nullable=True),  # # of depth updates in window
    pa.field("trade_update_count", pa.int64(), nullable=True),  # # of trade updates
    pa.field("update_rate", pa.float64(), nullable=True),  # Updates per second
    pa.field("dropped_gap_count", pa.int64(), nullable=True),  # Gaps in sequence numbers
    pa.field("reconnect_count", pa.int64(), nullable=True),  # WebSocket reconnects
    pa.field("crossed_book_count", pa.int64(), nullable=True),  # Times bid >= ask
    pa.field("stale_book_seconds", pa.float64(), nullable=True),  # Max staleness in window
    pa.field("missing_trade_seconds", pa.float64(), nullable=True),  # Max gap in trades
    pa.field("latency_ms_mean", pa.float64(), nullable=True),  # Mean ts_receive - ts_exchange
    pa.field("latency_ms_p95", pa.float64(), nullable=True),  # 95th percentile latency
])


# ============================================================================
# Advanced Features (Nullable/TODO - deferred to stable implementation)
# ============================================================================

FEATURE_SCHEMA_ADVANCED = pa.schema([
    # F05: OFI / Order-flow Imbalance (DEFERRED - requires careful implementation)
    pa.field("ofi_top1", pa.float64(), nullable=True),  # TODO
    pa.field("ofi_top5", pa.float64(), nullable=True),  # TODO
    pa.field("microprice", pa.float64(), nullable=True),  # (best_bid * ask_size + best_ask * bid_size) / (bid_size + ask_size)
    pa.field("microprice_vs_mid_bps", pa.float64(), nullable=True),  # Microprice deviation
    
    # Order book pressure
    pa.field("bid_wall_score", pa.float64(), nullable=True),  # TODO
    pa.field("ask_wall_score", pa.float64(), nullable=True),  # TODO
    pa.field("liquidity_pull_score", pa.float64(), nullable=True),  # TODO
    pa.field("liquidity_add_score", pa.float64(), nullable=True),  # TODO
    
    # F06: Trade-through ratio (DEFERRED)
    pa.field("trade_through_ratio", pa.float64(), nullable=True),  # TODO
    
    # F09: Cross-symbol correlation (DEFERRED - requires multi-symbol context)
    # Not included here; would be computed separately if needed
])


# Combined schema for full feature store
FEATURE_SCHEMA_FULL = pa.schema(
    list(FEATURE_SCHEMA_CORE_V1)
    + list(FEATURE_SCHEMA_ADVANCED)
)


# ============================================================================
# Feature Documentation
# ============================================================================

FEATURE_DEFINITIONS = {
    # Core v1
    "best_bid": "Highest bid price (top of bid side)",
    "best_ask": "Lowest ask price (top of ask side)",
    "mid_price": "(best_bid + best_ask) / 2",
    "spread": "best_ask - best_bid",
    "spread_bps": "spread / mid_price * 10000 (basis points)",
    
    "top1_bid_size": "Quantity available at best_bid",
    "top1_ask_size": "Quantity available at best_ask",
    
    "top5_bid_notional": "Sum of (price * size) for top 5 bid levels",
    "top5_ask_notional": "Sum of (price * size) for top 5 ask levels",
    "top10_bid_notional": "Sum of (price * size) for top 10 bid levels",
    "top10_ask_notional": "Sum of (price * size) for top 10 ask levels",
    "top50_bid_notional": "Sum of (price * size) for top 50 bid levels",
    "top50_ask_notional": "Sum of (price * size) for top 50 ask levels",
    
    "imbalance_top1": "top1_bid_size / (top1_bid_size + top1_ask_size); range [0, 1]",
    "imbalance_top5": "bid_notional_5 / (bid_notional_5 + ask_notional_5); range [0, 1]",
    "imbalance_top10": "bid_notional_10 / (bid_notional_10 + ask_notional_10)",
    "imbalance_top50": "bid_notional_50 / (bid_notional_50 + ask_notional_50)",
    
    "trade_count": "Number of trades in timeframe",
    "buy_volume": "Sum of quantities where buyer is taker (aggressor)",
    "sell_volume": "Sum of quantities where seller is taker (aggressor)",
    "total_volume": "buy_volume + sell_volume",
    "net_trade_flow": "buy_volume - sell_volume",
    "signed_trade_volume": "F03: Alias for net_trade_flow; indicates directional flow",
    "aggressive_buy_ratio": "buy_volume / total_volume",
    "aggressive_sell_ratio": "sell_volume / total_volume",
    "large_trade_count": "Trades > (mean + 1*std)",
    
    "return_1s": "Log return (1s back from bar end)",
    "return_5s": "Log return (5s back)",
    "return_10s": "Log return (10s back)",
    "return_30s": "Log return (30s back)",
    "return_1m": "Log return (1m back)",
    "realized_vol_1m": "F08: Realized volatility (std of returns over 1m window)",
    "high_low_range_1m": "(high - low) / mid over 1m window",
    "jump_score": "Estimated proportion of variance from jumps vs. diffusion",
    
    "depth_update_count": "Number of depth_v2 messages in window",
    "trade_update_count": "Number of trade_v2 messages in window",
    "update_rate": "(depth_update_count + trade_update_count) / timeframe_sec",
    "dropped_gap_count": "Session sequence gaps (indicates dropped messages)",
    "reconnect_count": "WebSocket reconnection events in window",
    "crossed_book_count": "Times bid >= ask (data quality issue)",
    "stale_book_seconds": "Maximum time without an update",
    "missing_trade_seconds": "Maximum gap in trade stream",
    "latency_ms_mean": "Mean (ts_receive - ts_exchange) / 1e6",
    "latency_ms_p95": "95th percentile latency_ms",
    
    # Advanced / Deferred
    "ofi_top1": "F05 (DEFERRED): Order flow imbalance (incoming buy - sell orders)",
    "microprice": "Weighted mid-price: (bid * ask_size + ask * bid_size) / (bid_size + ask_size)",
    "microprice_vs_mid_bps": "Deviation of microprice from mid_price in bps",
    "bid_wall_score": "Score indicating large static bid depth",
    "ask_wall_score": "Score indicating large static ask depth",
    "trade_through_ratio": "F06 (DEFERRED): Proportion of trades vs. walk-through at levels",
}


# Lookahead-bias rule enforcement
LOOKAHEAD_BIAS_RULE = """
STRICT RULE: feature_store may only use CURRENT/PAST data within the timeframe.

FORBIDDEN:
- Future returns (beyond the bar end)
- Future MAE/MFE (Maximum Adverse/Favorable Excursion)
- Future slippage
- Strategy outcomes
- Any labels or targets

ONLY ALLOWED:
- Order book state at each timestamp
- Historical trade flow
- Latency/gap metrics
- Quality flags
- Current/past volatility
- Current/past imbalance

Violations are enforced via:
1. Code review comments
2. Assertion tests
3. Schema validation (no future_* fields)
"""
