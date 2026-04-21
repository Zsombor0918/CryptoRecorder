# Architecture

## Phase 1 Goal

CryptoRecorder Phase 1 targets:
- **50 spot instruments** (Binance, top by 24h volume)
- **Trades + approximate L2 depth**
- **Nautilus-queryable ParquetDataCatalog**
- Useful for spread/mid/top-of-book analytics and basic execution realism

## Pipeline

1. `binance_universe.py` builds a ranked daily universe for spot and futures.
2. `recorder.py` starts `cryptofeed` with the selected symbols.
3. `storage.py` writes append-only raw files under `data_raw/`.
4. `health_monitor.py` publishes `state/heartbeat.json`.
5. Recorder startup writes `state/startup_coverage.json`.
6. `convert_day.py` converts a UTC date into Nautilus catalog output.

## Universe and startup flow

The preferred startup vocabulary is:

- `candidate_pool`: ranked symbols considered for a venue
- `pre_filter_rejected`: symbols rejected before recorder startup because they fail sanity or known-friction filters
- `selected`: symbols handed from universe selection into recorder startup
- `runtime_dropped`: selected symbols lost during feed initialization / compatibility stripping
- `active`: symbols that actually start recording

Compatibility fields from earlier iterations are still emitted where existing
tests and tooling rely on them.

## Raw storage layout

`data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)`

Channels:
- `trade` — individual trade ticks
- `depth` — L2 book delta updates (cryptofeed-normalized)
- `exchangeinfo` — periodic exchangeInfo snapshots

## Conversion model

- Trades are converted to Nautilus `TradeTick`.
- Depth deltas are replayed into an approximate book and emitted as `OrderBookDepth10` snapshots (1-second intervals).
- Instruments are built from `exchangeInfo` data (`CurrencyPair` for spot, `CryptoPerpetual` for futures).
- Conversion reports are written to `state/convert_reports/YYYY-MM-DD.json`.

### Crossed-book handling

When the reconstructed book becomes crossed (best_bid >= best_ask):
1. The event is logged and counted (`crossed_book_events_total`)
2. The book is reset to empty
3. No crossed snapshot is emitted to the catalog

This ensures the final catalog contains only valid, uncrossed depth snapshots.

### Data presence tracking

The converter tracks which instruments have actual data:
- `instruments_with_trades` — instruments with ≥1 TradeTick
- `instruments_with_depth` — instruments with ≥1 OrderBookDepth10
- `instruments_with_both` — instruments with both
- `instruments_with_no_data` — instruments defined but no raw data for that date

## Phase 1 design boundaries

### What Phase 1 does

- Records L2 deltas (not full snapshots) via cryptofeed
- Preserves size==0 delete semantics
- Tracks queue drops, reconnects, gaps
- Produces Nautilus-native `TradeTick` and `OrderBookDepth10`
- Validates catalog quality (crossed-book, gap rate, data presence)

### What Phase 1 does NOT do

- **No deterministic replay** — Binance U/u/pu sequence numbers are not used
- **No REST depth polling** — causes rate limiting (429/418)
- **No bit-exact book reconstruction** — approximate by design
- **No L3 / queue position tracking**
- **No perfect historical catalog hygiene** — some noise is expected

### Acceptable Phase 1 limitations

| Limitation | Reason |
|------------|--------|
| Gap detection via 30s timestamp heuristic | Simple, conservative |
| 1-second depth snapshots (not every delta) | Sufficient for approximate backtesting |
| Some instruments with no data | Market was inactive |
| Futures may degrade gracefully | Secondary priority |

## Reliability model

- Bad/unsupported symbols are rejected or dropped while startup continues.
- Venue-level graceful degradation is preserved: surviving active symbols continue recording.
- Queue-drop metrics, heartbeat state, and startup coverage are surfaced for operators.

## Validation layers

See [VALIDATION.md](VALIDATION.md) for the complete validation layer structure:

- **A. Recorder validation** — raw files, schema, heartbeat
- **B. Converter validation** — catalog queries, instrument mapping
- **C. Catalog quality** — crossed-book, gap rate, data presence
- **D. Infrastructure** — dependencies, purge safety
