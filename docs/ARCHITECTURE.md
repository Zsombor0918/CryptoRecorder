# Architecture

## Design Overview

CryptoRecorder is a deterministic Binance market-data pipeline that records
native WebSocket depth and trade streams, converts them to Nautilus-native
objects, and writes a queryable `ParquetDataCatalog`.

**Target:** 50 instruments (spot + USDT-M futures) with deterministic L2 depth
and trades, recorded via native Binance WebSockets (no third-party feed
handlers).

## Current Pipeline Paths

Validated full-L2 path:

```text
data_raw -> convert_day.py -> Nautilus catalog
```

Validated replay v0 path (the stable external contract consumed by downstream
repositories, e.g. KovacsTrader):

```text
data_raw -> replay_store
```

Internal validation-only catalog reconstruction path (no CLI; used only by
`validation.validate_catalog_equivalence`):

```text
replay_store -> validation.replay_catalog_reconstruct --profile trades_only
replay_store -> validation.replay_catalog_reconstruct --profile full_l2
```

The `full_l2` profile is implemented and semantically validated on the ADAUSDT
single-day smoke against `convert_day.py`; broader top50/multi-day validation
is pending. `convert_day.py` remains the production reference full-L2
converter. CryptoRecorder does not build a feature/label layer or a
general-purpose consumer catalog from replay_store; those are downstream
responsibilities.

## Recorder Pipeline

1. `binance_universe.py` builds a ranked daily universe for spot and futures.
2. `recorder.py` launches native WebSocket recorders for `depth_v2` and `trade_v2`.
3. `storage.py` writes append-only raw JSONL under `data_raw/`.
4. `health_monitor.py` publishes `state/heartbeat.json`.
5. Recorder startup writes `state/startup_coverage.json`.
6. `convert_day.py` converts a UTC date into Nautilus catalog output.

## Disk Monitoring Safety Invariant

`disk_monitor.py` measures `data_raw/`, the Nautilus catalog, `meta/`, and
`state/` on a fixed interval (`DISK_CHECK_INTERVAL_SEC`, default 600s) via a
recursive `du -s -B1` scan, and enforces:

> If a directory-size measurement fails or is unavailable, monitoring must
> become visibly unhealthy. It must never optimistically report zero, and
> automatic cleanup must fail closed.

Concretely:

- Every scan result is a `DirectoryMeasurement` (`ok`, `status`, `error`,
  `value_bytes`) — never a bare number. `status` is one of `ok`, `missing`,
  `timeout`, `command_error`, `malformed_output`, `error`. A genuinely empty
  directory reports `ok=True, status="ok", value_bytes>=0`; a failed scan
  never reports `value_bytes=0`.
- On failure, the monitor falls back to the last-known-good value for that
  directory (persisted in `state/disk_monitor_state.json` so it survives a
  restart), reported with `stale=True` and a `measurement_age_seconds`. If no
  prior value exists, the field is `null`, never `0`.
- `state/disk_usage.json` exposes `monitoring_health` (`healthy` / `degraded`
  / `unhealthy`), per-component `measurement_ok` / `measurement_status` /
  `stale` fields, and an `alerts` list. Retention percentage, growth rate, and
  `days_to_full` are only computed from known, trustworthy values — otherwise
  they are `null`, not misleadingly derived from a stale or missing sample.
- Filesystem-level free space is measured independently via
  `shutil.disk_usage(DATA_ROOT)` (fast, not a recursive scan) and reported
  under `filesystem` with its own `DISK_FS_FREE_WARN_GB` /
  `DISK_FS_FREE_CRITICAL_GB` thresholds. This stays operational even when the
  recursive `data_raw` scan fails, and is never summed with retention GB.
- Automatic cleanup (`cleanup_old_data()`) refuses to run or continue unless
  the current cycle's `data_raw` measurement is fresh and successful
  (`retention_measurement_trustworthy=True`); a missing, failed, timed-out, or
  merely-stale (last-known-good) value is never treated as "below threshold".
  Cleanup re-validates this before each destructive deletion, not just once
  up front.
- Overlapping scans are prevented with an `asyncio.Lock`; a scan already in
  flight causes the next call to return the previous report with
  `skipped_duplicate=True` rather than queuing or running concurrently.
- `disk_usage.json` and the companion state file are written atomically
  (temp file in the same directory + `os.replace()`), with the temp file
  cleaned up on any write failure.

See `docs/OPERATIONS.md` for the full field reference and environment knobs.

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
| `converter/spool.py` | Temporary SQLite spools used to keep heavy conversions memory-bounded |
| `stores/` | Replay Parquet schemas/readers/writers (no feature/label schemas) |
| `pipeline/build_replay_store.py` | Raw JSONL -> replay_store v0 |
| `validation/audit_replay_store.py` | Non-mutating replay partition audit |
| `validation/replay_catalog_reconstruct.py` | replay_store -> temporary Nautilus catalog (validation-only, no CLI) |
| `validation/validate_catalog_equivalence.py` | Old-vs-new semantic comparison (trades_only, full_l2, depth_only, depth10) |

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
- Heavy conversions spool raw records and per-symbol Nautilus outputs to
  temporary SQLite files, then stream catalog writes in the same `ts_init` order
  previously produced by in-memory list sorting.
- `OrderBookDepth10` is enabled by default and derived only from the
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


---

## Storage Details

> Content merged from the former `STORAGE_ARCHITECTURE.md`.

## Overview

The replay architecture implements a v0 layered pipeline around the existing
recorder and converter. `replay_store` is the stable external contract handed
off to downstream repositories (e.g. KovacsTrader); CryptoRecorder itself does
not build a feature-store, label-store, or general-purpose consumer catalog
from it (removed, issue #17).

```
data_raw -> convert_day.py -> full_l2 Nautilus catalog
  current validated full-L2 path

data_raw -> replay_store
  current implemented replay layer (stable external contract)

replay_store -> validation.replay_catalog_reconstruct (validation-only, no CLI)
  internal helper for old-vs-new equivalence checking; trades_only and full_l2
  both implemented; full_l2 validated on the ADAUSDT single-day smoke
```

### Key Design Principles

1. **Raw retention, not automatic deletion** — Raw is the original capture/audit source while retained. Replay store is a candidate long-term replay layer after validation.
2. **Deterministic replay** — Replay store sorts by committed stream keys plus `raw_index` for reproducible rebuilds.
3. **Hive-style partitioning** — All stores use `venue=X/symbol=Y/date=Z` for efficient directory-based filtering.
4. **Atomic writes** — All writers use staging directory + move pattern to prevent half-written data.
5. **Memory status is explicit** — v0 replay writing still materializes one symbol/date. Do not claim full production memory safety until RSS benchmarks pass.

## Storage Layers

### 1. Raw Data (`data_raw/`)

**Purpose**: Original capture/audit source while retained.

**Format**: JSONL with gzip or zstd compression
- `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)` — directory-based hourly files
- `BINANCE_SPOT/depth_v2/{symbol}/{date}/{date}THH.jsonl(.zst)` — Order book snapshots and updates
- `BINANCE_SPOT/trade_v2/{symbol}/{date}/{date}THH.jsonl(.zst)` — Trade executions and stream lifecycle records
- `BINANCE_USDTF/` — Same structure for futures

**Content**:
- **depth_v2**: Seqnum, bids/asks, server timestamp, receive timestamp
- **trade_v2**: Trade ID, price, quantity, buyer_maker flag, timestamps
- **exchangeinfo**: Symbol metadata, decimals, min/max price limits

**Retention**: Retained as audit source until replay validation supports an explicit archival policy.

### 2. Replay Store (`replay_store/`)

**Purpose**: The stable external contract consumed by downstream repositories
(e.g. KovacsTrader). Also feeds the internal, validation-only
`validation.replay_catalog_reconstruct` helper used for old-vs-new equivalence
checking. CryptoRecorder does not build a feature-store, label-store, or
general-purpose consumer catalog from this data.

**Format**: Parquet with ZSTD compression (level 3), Hive-style partitioning

**Structure**:
```
replay_store/
  venue=BINANCE_SPOT/
    symbol=BTCUSDT/
      date=2026-06-15/
        depth.parquet      # Order book deltas with nested struct<price, size>
        trades.parquet     # Trade ticks
        instrument.json    # Symbol metadata (decimals, limits, etc)
        manifest.json      # Partition metadata (record counts, checksums)
```

**Schemas**:

#### Depth (Order Book) Parquet Schema:
```python
DEPTH_REPLAY_SCHEMA = pa.schema([
    pa.field("venue", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("date", pa.string()),
    pa.field("stream_session_id", pa.uint64()),
    pa.field("session_seq", pa.uint64()),
    pa.field("raw_index", pa.uint32()),
    pa.field("record_type", pa.string()),
    pa.field("U", pa.string()),
    pa.field("u", pa.string()),
    pa.field("pu", pa.string()),
    pa.field("ts_exchange_ns", pa.int64()),
    pa.field("ts_receive_ns", pa.int64()),
    pa.field("bids", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
        pa.field("price_str", pa.string()),
        pa.field("size_str", pa.string()),
    ]))),
    pa.field("asks", pa.list_(pa.struct([
        pa.field("price", pa.float64()),
        pa.field("size", pa.float64()),
        pa.field("price_str", pa.string()),
        pa.field("size_str", pa.string()),
    ]))),
    pa.field("is_snapshot_seed", pa.bool_()),
    pa.field("is_depth_update", pa.bool_()),
    pa.field("quality_flags", pa.string()),
    pa.field("native_payload_hash", pa.string()),
])
```

#### Trade Parquet Schema:
```python
TRADE_REPLAY_SCHEMA = pa.schema([
    pa.field("venue", pa.string()),
    pa.field("symbol", pa.string()),
    pa.field("date", pa.string()),
    pa.field("trade_stream_session_id", pa.uint64()),
    pa.field("trade_session_seq", pa.uint64()),
    pa.field("raw_index", pa.uint32()),
    pa.field("record_type", pa.string()),
    pa.field("market_type", pa.string()),
    pa.field("trade_id", pa.string()),
    pa.field("agg_trade_id", pa.string()),
    pa.field("ts_exchange_ns", pa.int64()),
    pa.field("ts_receive_ns", pa.int64()),
    pa.field("price", pa.float64()),
    pa.field("quantity", pa.float64()),
    pa.field("price_str", pa.string()),
    pa.field("quantity_str", pa.string()),
    pa.field("buyer_maker", pa.bool_()),
    pa.field("aggressor_side", pa.string()),
    pa.field("quality_flags", pa.string()),
    pa.field("native_payload_hash", pa.string()),
])
```

**Manifest**:
```json
{
  "date": "2026-06-15",
  "symbol": "BTCUSDT",
  "venue": "BINANCE_SPOT",
  "depth_record_count": 86400,
  "trade_record_count": 45230,
  "ts_range_start_ns": 1718400000000000000,
  "ts_range_end_ns": 1718486399999999999,
  "depth_checksum": "sha256_hash_of_depth_parquet",
  "trades_checksum": "sha256_hash_of_trades_parquet"
}
```

**Retention**: Candidate long-term replay layer after old-vs-new validation passes.

### 3. Validation-Only Catalog Reconstruction (ephemeral, no fixed store)

**Purpose**: Temporary Nautilus `ParquetDataCatalog` artifacts reconstructed
from `replay_store` by `validation.replay_catalog_reconstruct`, for
equivalence checking only. There is no CLI and no persistent product-facing
catalog store; each invocation writes to an explicit, caller-provided
catalog root (conventionally under a local, gitignored temp directory) and is
not a supported downstream runtime API.

**Structure** (example of one reconstruction run):
```
<catalog_root>/
  job_<id>/
    manifest.json                    # Job metadata
    data/
      currency_pair/
      crypto_perpetual/
      trade_tick/
      order_book_deltas/
      order_book_depths/
```

## Build Pipelines

### Build Replay Store

Converts raw JSONL.zst → replay_store Parquet with deterministic sorting.

**CLI**:
```bash
python -m pipeline.build_replay_store --date 2026-06-15 [--symbols BTCUSDT,ETHUSDT] [--data-root /path/to/raw] [--replay-root /path/to/replay]
```

**Processing**:
1. Scan raw directory for available symbols/channels for date
2. Per symbol: stream raw JSONL records, accumulate session IDs
3. Deterministic sort by (session_id, session_seq, raw_index)
4. Write as Parquet with nested bids/asks
5. Compute SHA256 checksum, write manifest
6. Atomic move from staging → published

**Determinism**:
- Session ID from server timestamp (e.g., hour boundary)
- Session seq from server sequence number
- Raw index from original file position
- Result: Two runs on same raw data produce identical Parquet files

### Reconstruct a Validation-Only Catalog

Invoked exclusively by `validation.validate_catalog_equivalence` (no standalone
CLI); reconstructs a temporary Nautilus `ParquetDataCatalog` from replay_store
for a specific time window, for equivalence checking only.

**Python API** (`validation/replay_catalog_reconstruct.py`):
```python
from validation.replay_catalog_reconstruct import generate_catalog_from_replay

generate_catalog_from_replay(
    replay_root="/path/to/replay_store",
    catalog_root="/path/to/tmp_catalog",
    job_id="validation_new",
    symbols=["BTCUSDT", "ETHUSDT"],
    venues=["BINANCE_SPOT", "BINANCE_USDTF"],
    start="2026-06-15T00:00:00Z",
    end="2026-06-16T00:00:00Z",
    profile="trades_only",
)
```

**Processing**:
1. Parse the requested time window (or the UTC-day shortcut passed through by the caller)
2. Determine date range and Hive partitions to scan
3. Per symbol: stream replay data, filter by time window
4. Convert to Nautilus TradeTick objects using exact replay price/quantity strings
5. Write a temporary Nautilus `ParquetDataCatalog` under the caller-provided `catalog_root`
6. Generate a report with coverage info, including found/missing partitions and records read/written

Current status: `trades_only` is implemented and smoke-tested. The `full_l2`,
`depth_only`, and `depth10` profiles are implemented and semantically validated on
the ADAUSDT single-day smoke against `convert_day.py`; broader top50/multi-day
validation is pending and `convert_day.py` remains the production reference
full-L2 path. This helper is not a supported downstream runtime API.

## Daily Build Orchestrator

Runs the raw manifest scan and replay store build for a single date and
writes a report. Replay-only; there is no `--steps` flag.

**CLI**:
```bash
python -m pipeline.daily_build --date 2026-06-15 [--symbols BTCUSDT,ETHUSDT]
```

**Execution**:
1. Scan raw directory for available data
2. Build replay_store
3. Generate daily_build_report.json with stats and errors
4. Exit with success/failure status

**Report `status` values**:
- `success` — every eligible venue/symbol partition for the date built successfully
- `partial` — at least one partition succeeded and at least one failed
- `failed` — one or more partitions were attempted and none succeeded
- `no_data` — zero raw partitions were eligible for the date (empty/missing raw data); distinct from `success`, since `0 successful == 0 attempted` must never be reported as a successful build

All non-`success` statuses (`partial`, `failed`, `no_data`) produce a nonzero
process exit code from `pipeline.daily_build.main()`.

**Report**:
```json
{
  "date": "2026-06-15",
  "runtime_sec": 3600.0,
  "status": "success",
  "raw_coverage": {
    "venues": ["BINANCE_SPOT", "BINANCE_USDTF"],
    "symbol_count": 2500
  },
  "replay_build": {
    "symbols_processed": 2500,
    "depth_records": 216000000,
    "trade_records": 112300000
  },
  "errors": []
}
```

## Systemd Integration

**Service**: `cryptorecorder-replay-build.service`
- Runs daily build orchestrator
- Loads env vars from `/etc/cryptorecorder/cryptorecorder.env`
- Restarts on failure with 5min backoff

**Timer**: `cryptorecorder-replay-build.timer`
- Triggers at 01:00 UTC daily
- Allows previous day hourly rotation/compression to complete
- Persistent: runs immediately if system was down

## Migration from Old Pipeline

**Validated full-L2 path** (`convert_day.py`):
```
data_raw → Nautilus ParquetDataCatalog
```

**Implemented replay v0 path** (the stable external contract for downstream
repositories):
```
data_raw → replay_store
```

The internal `validation.replay_catalog_reconstruct` helper (no CLI) supports
`trades_only` and `full_l2` reconstruction for equivalence checking only.

**Rollout**:
1. Keep `convert_day.py` as the validated full-L2 path.
2. Validate trades-only semantic equivalence with `validation.validate_catalog_equivalence`.
3. Validate full-L2 semantic equivalence the same way; broaden past the ADAUSDT smoke before declaring `v2.0.0`.
4. Benchmark the replay writer's RSS before large-symbol production runs.
5. Decide raw archival policy only after replay validation has enough history.

**Backward Compatibility**:
- `convert_day.py` remains functional and is still the full-L2 baseline
- Can be run alongside the replay pipeline for comparison
- Legacy code paths preserved for rollback

## See Also

- [REPLAY_STORE.md](REPLAY_STORE.md) — Replay store schema and usage
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — Operations and examples
- [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md) — Validation-only full-L2 reconstruction plan
- [IMPLEMENTATION_AUDIT.md](IMPLEMENTATION_AUDIT.md) — Current validation status and limitations

---

## System Guarantees

> Content merged from the former `GUARANTEES.md`.

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
