# Guarantees

This document states what CryptoRecorder guarantees and what it does not.

## What This Repository Guarantees

### Recording

| Guarantee | Description |
|-----------|-------------|
| Native Binance WebSocket depth | `depth_v2` records with exchange-native update IDs and raw level payloads |
| Native Binance WebSocket trades | `trade_v2` records: spot `@trade`, futures `@aggTrade` |
| Deterministic session ordering | Committed-only `session_seq` / `trade_session_seq` counters |
| Tagged union trade schema | `market_type` discriminator with venue-specific fields |
| Explicit sync lifecycle | `unsynced`, `snapshot_seeded`, `live_synced`, `desynced`, `resync_required` states tracked |
| Timestamps preserved | Both exchange time (`ts_event_ms` / `ts_trade_ms`) and local receipt (`ts_recv_ns`) |
| 50 instruments target | Top symbols by 24h quote volume (spot + futures) |

### Conversion

| Guarantee | Description |
|-----------|-------------|
| Deterministic replay | Same raw data always produces identical Nautilus output |
| Exact Decimal book state | Book reconstruction uses `Decimal` throughout (no float) |
| Delta-first L2 output | Primary depth output is `OrderBookDeltas` |
| Optional derived Depth10 | `OrderBookDepth10` derived only from replayed deterministic book state |
| Valid instruments | `CurrencyPair` for spot, `CryptoPerpetual` for futures |
| Queryable catalog | Standard Nautilus `ParquetDataCatalog` API |
| Fenced bad ranges | Excluded ranges visible in reports instead of silently reconstructed |
| Idempotent re-conversion | Same date re-run produces identical output |

### Validation

| Guarantee | Description |
|-----------|-------------|
| Sync lifecycle tracking | resync_count, desync_events, fenced_ranges reported |
| Data presence tracking | instruments_with_trades / depth / no_data tracked |
| Exchange-native continuity | Spot U/u, futures pu checked against update IDs |
| Schema validation | Records have required fields |

## What This Repository Does NOT Guarantee

### Not Perfect Historical Hygiene

| Non-guarantee | Reason |
|---------------|--------|
| All instruments have data | Market may be inactive |
| Zero desyncs | Reconnects happen |
| Perfect timestamp ordering | Interleaved WS messages |

### Not Consumer-Side Concerns

| Non-guarantee | Reason |
|---------------|--------|
| BacktestNode configuration | Consumer responsibility |
| Strategy code | Not this repo's scope |
| Data viewer tools | Separate project |
| Full Tardis equivalence | Different design choices at the edge |

## Scope Boundaries

### This Repository Handles

- Recording raw market data via native Binance WebSockets
- Converting to Nautilus catalog format with deterministic replay
- Validating recording and conversion quality
- Documenting what was produced

### Consumer Handles

- Historical catalog inspection across many days
- Visualization of gaps and quality metrics
- BacktestNode configuration and strategy execution
- Cross-day data continuity analysis

## Quality Thresholds

| Metric | Threshold | Meaning |
|--------|-----------|---------|
| fenced_ranges | reported | Excluded ranges visible in reports |
| queue_drops | 0 (smoke test) | No drops in normal operation |
| rate_limit_hits | 0 | No 429/418 errors |

## Summary

**CryptoRecorder is a deterministic native Binance market-data pipeline that
records depth and trades via native WebSockets, replays them through exact
Decimal book state, and produces Nautilus `OrderBookDeltas` and `TradeTick`
objects suitable for backtesting research.**
