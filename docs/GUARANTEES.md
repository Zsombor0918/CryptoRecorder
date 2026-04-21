# Guarantees

This document clearly states what CryptoRecorder Phase 1 guarantees and what it does not.

## What This Repository Guarantees

### Recording

| Guarantee | Description |
|-----------|-------------|
| L2 deltas recorded | cryptofeed-normalized delta updates, not full snapshots |
| Trades recorded | Individual trade ticks with price, quantity, side, trade_id |
| size==0 semantics preserved | Delete semantics maintained in raw data |
| Timestamps preserved | Both exchange time (ts_event_ms) and local receipt (ts_recv_ns) |
| Queue drops tracked | Heartbeat reports queue_drop_total |
| 50 spot instruments | Top symbols by 24h quote volume |

### Conversion

| Guarantee | Description |
|-----------|-------------|
| Nautilus-native output | Real `TradeTick` and `OrderBookDepth10` objects |
| Valid instruments | `CurrencyPair` for spot, `CryptoPerpetual` for futures |
| Queryable catalog | Standard Nautilus `ParquetDataCatalog` API works |
| No crossed snapshots | Crossed books trigger reset, not catalog write |
| Idempotent re-conversion | Same date re-run produces identical counts |

### Validation

| Guarantee | Description |
|-----------|-------------|
| Crossed-book detection | crossed_rate reported, threshold enforced |
| Gap tracking | gaps_suspected, book_resets_total reported |
| Data presence | instruments_with_trades/depth/both/no_data tracked |
| Schema validation | Records have required fields |

## Phase 2 Guarantees

When Phase 2 is explicitly enabled:

| Guarantee | Description |
|-----------|-------------|
| Binance-native raw depth | `depth_v2` stores exchange-native update ids and raw level payloads |
| Explicit sync lifecycle | `unsynced`, `snapshot_seeded`, `live_synced`, `desynced`, `resync_required`, and fenced ranges are surfaced |
| Deterministic replay ordering | Replay order is stable by stream session and recorded arrival/order fields |
| Delta-first Nautilus output | Primary L2 catalog output is `OrderBookDeltas` |
| Optional derived depth10 | `OrderBookDepth10` is derived only from replayed deterministic book state |
| Fenced bad ranges reported | Excluded ranges are visible in conversion reports instead of silently reconstructed |

## What This Repository Does NOT Guarantee

### Not Deterministic Replay

| Non-guarantee | Reason |
|---------------|--------|
| Binance U/u/pu sequence numbers | Not recorded or used |
| Bit-exact book reconstruction | Approximate by design |
| Perfect gap detection | 30s timestamp heuristic only |
| Sub-second depth resolution | 1-second snapshots |

### Not Perfect Historical Hygiene

| Non-guarantee | Reason |
|---------------|--------|
| All instruments have data | Market may be inactive |
| Zero gaps | Reconnects happen |
| Perfect timestamp ordering | Interleaved WS messages |

### Not Consumer-Side Concerns

| Non-guarantee | Reason |
|---------------|--------|
| BacktestNode configuration | Consumer responsibility |
| Strategy code | Not this repo's scope |
| Data viewer tools | Separate project |

## Scope Boundaries

### This Repository Handles

- Recording raw market data
- Converting to Nautilus catalog format
- Validating recording and conversion quality
- Documenting what was produced

### Viewer/Consumer Handles

- Historical catalog inspection across many days
- Visualization of gaps and quality metrics
- BacktestNode configuration and strategy execution
- Cross-day data continuity analysis

## Phase 1 vs Future Phases

| Feature | Phase 1 | Future |
|---------|---------|--------|
| L2 reconstruction | Approximate | Deterministic (U/u/pu) |
| Depth output | 1-second snapshots | Per-delta or configurable |
| Gap detection | Timestamp heuristic | Sequence-based |
| Futures | Secondary, graceful degradation | Full parity with spot |

## Quality Thresholds

Phase 1 enforces these thresholds:

| Metric | Threshold | Meaning |
|--------|-----------|---------|
| crossed_rate | < 0.1% | Crossed events very rare |
| gap_rate | < 100% | Not all records are gaps |
| queue_drops | 0 (smoke test) | No drops in normal operation |

## Summary

**CryptoRecorder Phase 1 is a production-safe recording and conversion pipeline
that produces approximate L2 data suitable for spread/mid/TOB analytics and
basic execution simulation.**

**CryptoRecorder Phase 2 is an opt-in deterministic replay path for Binance
native depth which is materially closer to Tardis-style delta-first usage, but
still does not claim full Tardis equivalence.**

The viewer and consumer code handle their own concerns separately.
