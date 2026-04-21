# Architecture

## Design Overview

CryptoRecorder is a deterministic Binance market-data pipeline that records
native WebSocket depth and trade streams, converts them to Nautilus-native
objects, and writes a queryable `ParquetDataCatalog`.

**Target:** 50 instruments (spot + USDT-M futures) with deterministic L2 depth
and trades, recorded via native Binance WebSockets (no third-party feed
handlers).

## Pipeline

1. `binance_universe.py` builds a ranked daily universe for spot and futures.
2. `recorder.py` launches native WebSocket recorders for `depth_v2` and `trade_v2`.
3. `storage.py` writes append-only raw JSONL under `data_raw/`.
4. `health_monitor.py` publishes `state/heartbeat.json`.
5. Recorder startup writes `state/startup_coverage.json`.
6. `convert_day.py` converts a UTC date into Nautilus catalog output.

## Key Components

| Module | Purpose |
|--------|---------|
| `recorder.py` | Main entry — single asyncio loop, launches depth + trade recorders |
| `phase2_depth.py` | `BinanceNativeDepthRecorder` — depth_v2 with snapshot seeding and sync lifecycle |
| `native_trades.py` | `BinanceNativeTradeRecorder` — trade_v2 with tagged union schema |
| `binance_universe.py` | Universe selection by 24h quote volume + REST-based futures precheck |
| `storage.py` | Hourly-rotated JSONL(.zst) file writer |
| `convert_day.py` | CLI converter orchestrator |
| `converter/trades.py` | Raw trade_v2 → Nautilus `TradeTick` |
| `converter/depth_phase2.py` | Deterministic depth_v2 replay → `OrderBookDeltas` (+ optional `OrderBookDepth10`) |

## Session Ordering

All ordering is based on **committed-only monotonic counters**:

- **Depth:** `(stream_session_id, session_seq)` — `session_seq` is allocated only
  for committed records (snapshot seeds, accepted depth updates, sync-state
  transitions). Internal WS arrival tracking (`ws_arrival_seq`) is separate and
  never persisted.

- **Trades:** `(trade_stream_session_id, trade_session_seq)` — `trade_session_seq`
  is allocated only for committed trade records. Lifecycle markers do not consume
  sequence numbers.

This two-level committed ordering guarantees deterministic replay from the raw
JSONL alone — no external state, file position, or timestamp coincidence needed.

## Raw Storage Layout

```
data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)
```

Channels:
- `depth_v2` — Binance-native depth updates, snapshot seeds, sync-state, and lifecycle markers
- `trade_v2` — Native trade records (spot `@trade` / futures `@aggTrade`) with tagged union schema
- `exchangeinfo` — Periodic exchangeInfo snapshots

## Depth Sync Lifecycle

The depth recorder maintains explicit sync state per symbol:

1. **unsynced** → initial state, awaiting snapshot
2. **snapshot_seeded** → REST snapshot received, buffering WS updates
3. **live_synced** → continuity established, streaming accepted updates
4. **desynced** → continuity broken (gap or U/u/pu violation)
5. **resync_required** → awaiting new snapshot to re-establish sync

Continuity acceptance rules (exchange-native):
- Spot: `U <= last_update_id + 1 <= u`
- Futures: `pu == last_update_id`

Records outside a valid sync window are fenced and surfaced in reports.

## Trade Schema

Trades use a **tagged union** schema with a `market_type` discriminator:

- `"spot"` records include `best_match_flag`, `buyer_order_id`, `seller_order_id`
- `"futures"` records include `first_trade_id`, `last_trade_id`
- Both include `native_payload` (the full raw Binance message)

Exchange trade IDs are preserved as diagnostic metadata but do not affect
ordering — ordering comes solely from `trade_session_seq`.

## Conversion Model

- Trades are sorted by `(trade_stream_session_id, trade_session_seq)` and
  converted to Nautilus `TradeTick`.
- Depth records are sorted by `(stream_session_id, session_seq, raw_index)` and
  replayed through an exact `Decimal` book state to produce `OrderBookDeltas`.
- `OrderBookDepth10` is optional (off by default) and derived only from the
  replayed deterministic book state.
- Instruments are built from exchangeInfo (`CurrencyPair` for spot,
  `CryptoPerpetual` for futures).
- Reports are written to `state/convert_reports/YYYY-MM-DD.json`.

## Reliability Model

- Bad/unsupported symbols are rejected at startup while recording continues.
- Futures support is validated via REST exchangeInfo (no cryptofeed needed).
- Venue-level graceful degradation: surviving active symbols continue recording.
- Queue-drop metrics, heartbeat state, and startup coverage are surfaced for operators.

## Validation Layers

See [VALIDATION.md](VALIDATION.md) for the complete validation layer structure:

- **A. Recorder validation** — raw files, schema, heartbeat
- **B. Converter validation** — catalog queries, instrument mapping
- **C. Catalog quality** — fenced ranges, data presence, sync tracking
- **D. Infrastructure** — dependencies, purge safety
