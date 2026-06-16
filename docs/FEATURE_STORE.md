# Feature Store Design

## Overview

The feature store contains time-aggregated market microstructure features computed from the replay_store. Features are designed for ML pipelines, statistical analysis, and real-time monitoring with strict **no lookahead bias** constraints.

Current implementation note: replay input is read through `ReplayReader`, but feature aggregation currently collects one symbol/date into memory before producing windows. Use explicit temp roots and benchmark RSS before enabling large-symbol daily runs.

## Core Features (v1)

These features are mandatory and computed for every symbol/timeframe/date.

### Best Bid/Ask (BBO)

```python
best_bid (float64)      # Best bid price
best_ask (float64)      # Best ask price
best_bid_size (float64) # Size at best bid
best_ask_size (float64) # Size at best ask
```

**Calculation**:
- From depth records: top of order book at window close
- If no depth data: from trade prices (last trade bid/ask)
- Quality check: `is_crossed_book` if bid >= ask

### Spreads

```python
mid_price (float64)     # (best_bid + best_ask) / 2
spread_bps (float64)    # Spread in basis points: (ask - bid) / mid * 10000
spread_pct (float64)    # Spread in percent: (ask - bid) / mid * 100
```

**Interpretation**:
- Tight spread (<10 bps): Liquid market, low transaction costs
- Wide spread (>50 bps): Illiquid market, high slippage
- Negative spread: Data quality issue (flagged in `is_crossed_book`)

### Liquidity Metrics

```python
bid_imbalance_l1 (float64)  # best_bid_size / (best_bid_size + best_ask_size)
ask_imbalance_l1 (float64)  # best_ask_size / (best_bid_size + best_ask_size)

bid_imbalance_5 (float64)   # Sum of top 5 bid sizes / total depth (top 5)
ask_imbalance_5 (float64)   # Sum of top 5 ask sizes / total depth (top 5)
```

**Interpretation**:
- 0.5 = balanced order book (equal bid/ask liquidity)
- 0.7+ = buyer pressure (more bid liquidity)
- <0.3 = seller pressure (more ask liquidity)

**Limitations**:
- Single-point-in-time metric (window close)
- Does not capture intra-window dynamics
- For high-frequency features, use raw replay_store instead

### Trade Flow

```python
buy_volume (float64)    # Volume where aggressor is buyer
sell_volume (float64)   # Volume where aggressor is seller
buy_count (int32)       # Number of buy trades
sell_count (int32)      # Number of sell trades
buy_sell_ratio (float64)# buy_volume / sell_volume (NaN if sell_volume=0)
vwap (float64)          # Volume-weighted average price
```

**Calculation**:
```python
buy_volume = sum(qty for trade in window if trade.buyer_maker == False)
sell_volume = sum(qty for trade in window if trade.buyer_maker == True)
vwap = sum(qty * price for trade in window) / sum(qty for trade in window)
```

**Interpretation**:
- buy_sell_ratio > 1.0: More buying pressure (potential uptrend)
- buy_sell_ratio < 1.0: More selling pressure (potential downtrend)
- Rising vwap: Buyers willing to pay more
- Falling vwap: Sellers forcing prices down

**Quality**:
- Must be ≥ 0 for both volumes
- Flagged if buy_count + sell_count = 0 (no trade data)

### Returns and Volatility

```python
returns (float64)       # Log return from window open to close
high (float64)          # Highest price in window
low (float64)           # Lowest price in window
```

**Calculation**:
```python
open_price = first_trade_price or first_depth_mid
close_price = last_trade_price or last_depth_mid
returns = log(close_price / open_price)

high = max(trade.price for trade in window)
low = min(trade.price for trade in window)
```

**Interpretation**:
- returns > 0: Price increased (positive return)
- returns < 0: Price decreased (negative return)
- |returns| > 0.05: High volatility day/timeframe
- high - low: Daily range (useful for stop-loss placement)

### Quality Metrics

```python
trade_count (int32)             # Total trades in window
is_crossed_book (bool_)         # True if any depth had bid >= ask
is_gap_detected (bool_)         # True if price jumped >2σ
is_reconnect_detected (bool_)   # True if seqnum gap detected
```

**Interpretation**:
- `is_crossed_book = True`: Data problem (possibly during reconnect)
  - Skip crossed book records from analysis
  - May indicate API glitch or stale data
- `is_gap_detected = True`: Potential news event or error
  - Investigate cause of large price move
  - May warrant separate analysis
- `is_reconnect_detected = True`: Streaming interruption occurred
  - Data may be missing from window
  - Treat as incomplete record

## Advanced Features (v2, Deferred)

These features are optional and marked `nullable` in schema. Calculation is deferred to Phase 2.

### Order Flow Imbalance (OFI)

```python
ofi (float64)  # Cumulative order flow imbalance
ofi_buysell (float64)  # (buy_volume - sell_volume) / (buy_volume + sell_volume)
```

**Calculation**:
```python
# Simple OFI: buy - sell
ofi = buy_volume - sell_volume

# Normalized OFI: (buy - sell) / total
ofi_buysell = (buy_volume - sell_volume) / (buy_volume + sell_volume)

# More advanced: depth-weighted OFI
# Requires tracking order book deltas, not yet implemented
```

**Interpretation**:
- OFI > 0: Positive sentiment (more buying)
- OFI < 0: Negative sentiment (more selling)
- |OFI| spike: Potential reversal signal

**Lookahead Bias Risk**: HIGH
- OFI is forward-looking predictive feature
- Must not use future trades to calculate
- Ensure calculation uses only up-to-window trades

### Microstructure Patterns

```python
# TODO: Define microstructure features
# Potential candidates:
# - Price reversal probability (mean reversion)
# - Momentum (trend continuation)
# - Volatility regimes (high/low/medium)
```

## Schema Definition

```python
FEATURE_SCHEMA_CORE_V1 = pa.schema([
    pa.field("ts_ns", pa.int64()),                    # Window timestamp (nanoseconds, UTC)
    
    # BBO
    pa.field("best_bid", pa.float64()),
    pa.field("best_ask", pa.float64()),
    pa.field("best_bid_size", pa.float64()),
    pa.field("best_ask_size", pa.float64()),
    
    # Spreads
    pa.field("mid_price", pa.float64()),
    pa.field("spread_bps", pa.float64()),
    pa.field("spread_pct", pa.float64()),
    
    # Liquidity
    pa.field("bid_imbalance_l1", pa.float64()),
    pa.field("ask_imbalance_l1", pa.float64()),
    pa.field("bid_imbalance_5", pa.float64()),
    pa.field("ask_imbalance_5", pa.float64()),
    
    # Trade flow
    pa.field("buy_volume", pa.float64()),
    pa.field("sell_volume", pa.float64()),
    pa.field("buy_count", pa.int32()),
    pa.field("sell_count", pa.int32()),
    pa.field("buy_sell_ratio", pa.float64()),
    pa.field("vwap", pa.float64()),
    
    # Returns and range
    pa.field("returns", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    
    # Quality
    pa.field("trade_count", pa.int32()),
    pa.field("is_crossed_book", pa.bool_()),
    pa.field("is_gap_detected", pa.bool_()),
    pa.field("is_reconnect_detected", pa.bool_()),
])

FEATURE_SCHEMA_ADVANCED = pa.schema([
    # All core fields above, plus:
    pa.field("ofi", pa.float64()),                    # nullable
    pa.field("ofi_buysell", pa.float64()),            # nullable
])
```

## Lookahead Bias Prevention

**CRITICAL RULE**: Features must not use data after the window end timestamp.

### Correct ✓

```python
# Window: 2026-06-15T12:00:00Z to 2026-06-15T12:00:01Z (1 second)

# Use only trades with ts_ns < window_end_ns
window_trades = [t for t in trades if t.ts_ns < window_end_ns]

# Calculate VWAP from window_trades only
vwap = sum(t.price * t.qty for t in window_trades) / sum(t.qty for t in window_trades)

# Use depth at window_end_ns (not after)
depth_at_end = [d for d in depths if d.ts_ns <= window_end_ns][-1]
```

### Incorrect ✗

```python
# WRONG: Using trades from next window
next_trades = [t for t in trades if window_end_ns <= t.ts_ns < window_end_ns + 1000]
vwap_with_future = calculate_vwap(next_trades)  # LOOKAHEAD BIAS!

# WRONG: Using depth after window
depth_after = [d for d in depths if d.ts_ns > window_end_ns][0]
```

### Validation

- Code review: Ensure all feature calculations use only data up to window_end_ns
- Unit tests: Compare against manual hand-calculated examples
- Backtesting: Verify features don't have suspiciously high predictive power (may indicate lookahead)

## Missing Data Handling

### No Trades in Window

```python
buy_volume = 0.0
sell_volume = 0.0
buy_count = 0
sell_count = 0
buy_sell_ratio = NaN
vwap = NaN
trade_count = 0
returns = NaN
```

### No Depth Data

```python
best_bid = NaN
best_ask = NaN
best_bid_size = NaN
best_ask_size = NaN
mid_price = NaN
spread_bps = NaN
spread_pct = NaN
bid_imbalance_l1 = NaN
ask_imbalance_l1 = NaN
bid_imbalance_5 = NaN
ask_imbalance_5 = NaN
is_crossed_book = False  # No depth to cross
```

### Entire Window Missing

```python
# Mark all features as NaN, set quality flags:
all_features = NaN
is_gap_detected = True
is_reconnect_detected = True
trade_count = 0
```

## Examples

### BTC/USDT, 1-second window, 2026-06-15 12:34:56 UTC

```json
{
  "ts_ns": 1718459696000000000,
  "best_bid": 67500.50,
  "best_ask": 67501.25,
  "best_bid_size": 2.5,
  "best_ask_size": 3.1,
  "mid_price": 67500.875,
  "spread_bps": 1.11,
  "spread_pct": 0.0111,
  
  "bid_imbalance_l1": 0.45,
  "ask_imbalance_l1": 0.55,
  "bid_imbalance_5": 0.48,
  "ask_imbalance_5": 0.52,
  
  "buy_volume": 123.45,
  "sell_volume": 98.76,
  "buy_count": 156,
  "sell_count": 142,
  "buy_sell_ratio": 1.25,
  "vwap": 67500.90,
  
  "returns": 0.0002,
  "high": 67501.50,
  "low": 67499.75,
  
  "trade_count": 298,
  "is_crossed_book": false,
  "is_gap_detected": false,
  "is_reconnect_detected": false
}
```

## Query Examples

### Find moments of high spread

```python
from pyarrow import parquet
import pyarrow.compute as pc

table = parquet.read_table("/path/to/feature_store/timeframe=1s/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15.parquet")

# Filter for wide spreads
wide_spreads = table.filter(pc.greater(table.spread_bps, 50))
print(f"Wide spread events: {len(wide_spreads)}")
```

### Find trade imbalance events

```python
# Find buy-dominated minutes
buy_dominated = table.filter(pc.greater(table.buy_sell_ratio, 2.0))
print(f"Strong buy pressure: {len(buy_dominated)} minutes")

# Find sell-dominated minutes
sell_dominated = table.filter(pc.less(table.buy_sell_ratio, 0.5))
print(f"Strong sell pressure: {len(sell_dominated)} minutes")
```

### Detect data quality issues

```python
# Find crossed books (data errors)
crossed = table.filter(table.is_crossed_book)
print(f"Crossed book events: {len(crossed)}")

# Find reconnection events
reconnects = table.filter(table.is_reconnect_detected)
print(f"Reconnection events: {len(reconnects)}")
```

## See Also

- [STORAGE_ARCHITECTURE.md](STORAGE_ARCHITECTURE.md) — Overall data pipeline
- [REPLAY_STORE.md](REPLAY_STORE.md) — Raw data schema and format
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — How to build feature stores
