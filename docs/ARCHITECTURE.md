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

Versioned replay path (the stable external contract consumed by downstream
repositories, e.g. KovacsTrader; the repository production template now
intentionally requests schema 2, but has not been deployed):

```text
data_raw -> replay_store
```

Shared internal catalog reconstruction engine (no direct CLI; used by
`validation.validate_catalog_equivalence` and the supported selected boundary):

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

`disk_monitor.py` measures `data_raw/`, replay storage, `meta/`, and
state/report evidence on a fixed interval. Replay is classified in one bounded,
non-symlink-following traversal into canonical published data, staging,
backups, quarantine, and lifecycle metadata; the retired persistent Nautilus
catalog root is not an active monitoring component.

> If a directory-size measurement fails or is unavailable, monitoring must
> become visibly unhealthy. It must never optimistically report zero, and
> raw retirement must never be authorized by a failed measurement or by a
> cross-filesystem logical total.

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
- Capacity is grouped by `st_dev`; each filesystem's free bytes appear once.
  When raw and replay share a device, their allocated usage is combined with
  replay transient pressure. Separate devices remain separate; free-space
  values are never added into a fictitious logical pool.
- Automatic destructive raw cleanup is disabled. `cleanup_old_data()` never
  moves or deletes a channel/date directory. `plan_raw_retention()` can build
  a proof-only unit inventory for exact `venue/symbol/source-date` pairs,
  requiring depth+trade together and the adjacent replay dependencies, but
  reports `cleanup_required` until a separately accepted durable transaction
  journal/move/rollback implementation exists. ExchangeInfo is never in this
  unit.
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
| `converter/depth_repartition.py` | Dependency-free event-time selection/deduplication contract shared by converter and replay builder |
| `converter/spool.py` | Temporary SQLite spools used to keep heavy conversions memory-bounded |
| `stores/` | Replay Parquet schemas/readers/writers (no feature/label schemas) |
| `pipeline/build_replay_store.py` | Raw JSONL -> versioned replay partitions; production CLI default v2 |
| `pipeline/replay_lifecycle.py` | Shared advisory lock, cross-date recovery, atomic reports/sizing |
| `pipeline/daily_build.py` | Bounded oldest-first replay backlog and honest per-date/run reporting |
| `validation/audit_replay_store.py` | Non-mutating replay partition audit |
| `validation/replay_catalog_reconstruct.py` | Shared replay_store -> temporary Nautilus catalog engine (no direct CLI) |
| `pipeline/reconstruct_selected_catalog.py` | Supported explicit development-computer temporary-catalog CLI/API |
| `validation/validate_catalog_equivalence.py` | Old-vs-new semantic comparison (trades_only, full_l2, depth_only, depth10) |
| `validation/validate_dependency_environment.py` | Read-only uv lock, dependency separation, import, CLI, and optional tiny production replay smoke validation |

## Dependency and Import Boundaries

`pyproject.toml` plus committed `uv.lock` are the only Python dependency
authority. CryptoRecorder remains a flat, non-packaged uv virtual project with
no build backend; `VERSION` alone carries the application release value.

| Environment | Direct contract | Deliberately absent |
|-------------|-----------------|---------------------|
| production | aiohttp, NumPy, PyArrow, zstandard | Nautilus, pytest tooling |
| reconstruction | production + `nautilus_trader==1.225.0` | pytest tooling |
| development/test | production + reconstruction + pytest + pytest-asyncio | no implicit group |

NumPy is an explicit production contract because routine/deep replay
validation exercises PyArrow's NumPy conversion API and PyArrow does not
declare NumPy for that optional interoperation. Pandas is not a direct
CryptoRecorder dependency; it is present only where required transitively by
the pinned Nautilus distribution.

The production replay builder formerly imported event-time helpers through
`converter.depth_phase2`, which imported Nautilus at module load. Those pure
helpers now live in `converter.depth_repartition`; both callers share the same
implementation while Nautilus object construction stays in the reconstruction
boundary. A production environment can therefore import recorder, monitoring,
lifecycle, replay build/read/write, and routine/deep validation without the
reconstruction extra. Missing reconstruction dependencies fail with the
canonical frozen uv-extra command rather than a generic traceback.

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

The replay architecture implements a versioned layered pipeline around the
existing recorder and converter. `replay_store` is the stable external contract handed
off to downstream repositories (e.g. KovacsTrader); CryptoRecorder itself does
not build a feature-store, label-store, or general-purpose consumer catalog
from it (removed, issue #17).

```
data_raw -> convert_day.py -> full_l2 Nautilus catalog
  current validated full-L2 path

data_raw -> replay_store
  current implemented replay layer (stable external contract)

replay_store -> pipeline.reconstruct_selected_catalog (supported selected temporary jobs)
  explicit development-computer boundary wrapping the shared internal engine;
  trades_only and full_l2 both implemented; full_l2 validated on the ADAUSDT smoke
```

### Key Design Principles

1. **Raw retention, not automatic deletion** — Raw is the original capture/audit source while retained. Replay store is a candidate long-term replay layer after validation.
2. **Deterministic replay** — Replay store sorts by committed stream keys plus `raw_index` for reproducible rebuilds.
3. **Hive-style partitioning** — All stores use `venue=X/symbol=Y/date=Z` for efficient directory-based filtering.
4. **Exclusive atomic writes** — Supported mutations share a kernel advisory
   lock; bounded cross-date reconciliation precedes staging + backup/restore
   publication, and quarantine evidence is preserved.
5. **Memory status is explicit** — Replay writes are batch-bounded through a
   SQLite spool and incremental Parquet writer. The Phase 7 full-day evidence
   passed without swap/OOM but reached the exact 10 GiB limit; headroom work
   remains separate and the new 12 GiB service template is not production-tested.

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
python -m pipeline.build_replay_store --date 2026-06-15 --schema-version 2 [--symbols BTCUSDT,ETHUSDT] [--data-root /path/to/raw] [--replay-root /path/to/replay]
```

**Processing**:
1. Scan raw directory for available symbols/channels for date
2. Per symbol: stream raw JSONL records, accumulate session IDs
3. Deterministic sort by (session_id, session_seq, raw_index)
4. Write as Parquet with nested bids/asks
5. Compute SHA256 checksum, write manifest
6. Fsync manifest/staging, then backup/restore atomic publication under the
   common replay lifecycle lock

**Determinism**:
- Session ID from server timestamp (e.g., hour boundary)
- Session seq from server sequence number
- Raw index from original file position
- Result: Two runs on same raw data produce identical Parquet files

### Reconstruct a Selected Temporary Catalog

The supported `pipeline.reconstruct_selected_catalog` CLI/API reconstructs a
temporary Nautilus `ParquetDataCatalog` from replay_store for an explicit
venue/symbol/[start,end) selection. It wraps the same internal engine invoked
by `validation.validate_catalog_equivalence`; the engine has no direct CLI.
The supported boundary preflights and cryptographically binds exact replay
inputs and atomically publishes only a job-scoped temporary output.

**Python API** (`pipeline/reconstruct_selected_catalog.py`):
```python
from pathlib import Path

from pipeline.reconstruct_selected_catalog import (
    SelectedCatalogRequest,
    reconstruct_selected_catalog,
)

job_path = reconstruct_selected_catalog(request=SelectedCatalogRequest(
    replay_root=Path("/path/to/replay_store"),
    output_root=Path("/external/temporary/catalog_jobs"),
    job_id="selected-20260615",
    symbols=["BTCUSDT", "ETHUSDT"],
    venues=["BINANCE_SPOT"],
    start="2026-06-15T12:00:00Z",
    end="2026-06-16T00:00:00Z",
    profile="full_l2",
))
```

**Processing**:
1. Validate explicit UTC scope and a safe immediate-child job path.
2. Preflight every target/carry Hive partition, manifest, checksum, schema, and instrument.
3. Per symbol: stream replay data and reuse the shared reconstruction engine.
4. Rehash exact replay inputs and inventory every catalog file.
5. Atomically publish `<output-root>/<job-id>/` with `job_manifest.json`.

Current status: `trades_only` is implemented and smoke-tested. The `full_l2`,
`depth_only`, and `depth10` profiles are implemented and semantically validated on
the ADAUSDT single-day smoke against `convert_day.py`; broader top50/multi-day
validation is pending and `convert_day.py` remains the production reference
full-L2 path. The internal helper is not a supported direct API; callers use
the selected pipeline boundary.

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

The internal `validation.replay_catalog_reconstruct` engine (no direct CLI)
supports `trades_only` and `full_l2` reconstruction for equivalence checking
and the supported selected temporary-catalog boundary.

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

## Durable recording-scope retirement evidence (ledger 1, readiness 2)

`recording_coverage.py` owns the shared recorder state contract. Native feeds
start only after an immutable session-start record and its atomic ledger head
are durable under `COVERAGE_ROOT` (default `STATE_ROOT/recording_coverage`).
Starts contain the exact sorted selected universe, digest, UTC time, process,
repository/version identity, random session ID, dataset identity, sequence,
and predecessor start hash. An exclusive recorder lock spans the session;
a separate ledger lock serializes start/final writes with replay proof use.
There is one authoritative ledger per dataset. All recorder processes must
use this protocol; legacy/bypassing writers and privileged coordinated
rewriting of data plus evidence are outside this unsigned trust model.

Shutdown stops native producers and cancels/joins background feed tasks before
closing storage admission. Storage reports queue/write/drop/error counts,
writer task results, handle-close failures, and compression failures/timeouts.
Zero drops and errors are required for this conservative retirement proof;
WebSocket reconnects do not invalidate physical scope closure. Compression
workers fsync and hash completed compressed files off the ingest path, before
removing their sources and fsyncing the raw parent directory. Finalization
uses those bounded cached identities, rather than rehashing a long session
under the shutdown watchdog. Existing hourly inputs are explicitly marked
`unowned_raw_files`; opening a file cannot retroactively certify old bytes.

Only a successful shutdown can publish `closed_clean`. Failed closure has a
`failed` final or remains open; watchdog exit and evidence-publication failure
leave untrusted evidence. Start/final files are immutable, exclusive
publications. Temporary files are file-fsynced; publication is directory-
fsynced before the pending hard link is removed. A crash may resurrect that
unsynced pending unlink and block the ledger conservatively. No cleanup
heuristic silently upgrades interrupted evidence.

A missing-D+1 schema-v2 build can use the last selected session only when it
closed at least one hour before D ends and a timely successor excludes the
symbol. Every intervening handoff up to the readiness horizon must be clean
and ordered; a later selection, unknown/open handoff, overlapping sessions,
missing start, or conflicting raw owner blocks proof. The operational horizon
is D+1 01:00 UTC; offline equivalence requires all of D+1 to have elapsed.
Every selected D-1/D/D+1 depth and D trade file must have exactly one matching
session binding, including SHA-256 and size, and must be compressed, regular,
and free of symlinks/variants. This intentionally conservative first contract
can defer days spanning several owning sessions or inherited hourly files.
It proves physical recording scope, not uninterrupted market coverage.

Retirement-backed attempts optimistically read/hash the full ledger, then
briefly lock and compare its exact file inventory and inode/size/mtime/ctime
identities. Ledger JSON parsing, proof construction, raw hashing and replay
conversion hold no shared ledger lock. Evidence changes abort the attempt;
they are never compression retries. Raw inventory and hashes are rechecked
before streaming and immediately before publication. The final metadata
check and canonical rename/directory-fsync run under the shared lock; routine
post-publication validation runs after releasing it.

Recorder start/final publication waits at most 41 nonblocking lock attempts,
50 ms apart, additionally capped by a two-second monotonic deadline. Only
lock contention is retried. A remaining lock conflict is an explicit logged
failure, never a clean final or authorized start. The recorder lifetime lock
still rejects overlapping processes immediately. The shutdown watchdog stays
active until evidence finalization completes. A normal shutdown/successor can
proceed while replay conversion is running; the changed evidence makes that
replay attempt fail safely and a later invocation can retry it.

Readiness contract 2 stores the minimal consecutive whole-record proof once,
with a digest reference under manifest integrity. Contract 1 remains readable
and reusable without rewriting old partitions. Full live ledger validation
and exact original record hashes remain authoritative. See REPLAY_STORE.md
for the retained owner/handoff-final size limitation.

Bounds are 4,096 ledger entries, 64 MiB of serialized ledger records, 250,000
session file identities, and 72 entries per selected raw day directory.
Overflow fails closed and requires a separately designed ledger maintenance
procedure; no automatic evidence pruning is implemented.

Dataset identity is a portable random identifier carried in the hash-linked
ledger. Explicitly transferred raw/evidence bundles are verified by relative
raw paths and exact bytes, rather than absolute source paths. Recorder startup
additionally verifies the ledger head's local raw-root binding (path/device/
inode digest), preventing a copied ledger from silently starting another
recorder. The reader supports a read-only transferred bundle; recorder root
migration/rebinding is separate work. See OPERATIONS.md for bootstrap and
rollback limits. Raw JSONL schemas, event-time selection, replay rows, and `convert_day.py`
are unchanged; the additive Issue #30 naming extension is described below.


## Same-hour raw preservation (Issue #30)

The additive filename contract is recorded in REPO_STRUCTURE.md. The first
allocation keeps `YYYY-MM-DDTHH.jsonl`; every subsequent allocation for the
same venue/channel/symbol/day/hour uses `.segment-000001.jsonl`, then
`.segment-000002.jsonl`, etc. Existing artifacts are never appended, rewritten,
renamed, or assigned to a newer session. Plain, Zstandard and historical gzip
representations all reserve their ordinal; an exclusive create and brief
Linux directory advisory lock serialize allocation with publication/removal.
Unsafe existing hour entries or exhausted ordinals fail visibly. The existing
recorder lifetime ledger lock still prevents concurrent recorder processes.
No extra raw lock file or external dependency is introduced.

The hourly name cannot safely represent several immutable artifacts. Safe
continuation would require rewriting previously hashed compressed bytes, or
mutating an old plain file and mixing ownership; appending Zstandard frames
would likewise invalidate previous hashes and introduce interrupted-append
ambiguity. Separate ordered segments preserve both bytes and identities without
a raw record/schema change. Counters retain event ordering; canonical-first,
fixed-width ordinal enumeration preserves the existing raw-index tie-breaker
without using receive timestamps to infer ordering. Ordinals are monotonic
among retained/reserved artifacts, not time-derived or session-ID encodings.
The ledger assigns each segment by its exact touched relative path and bytes.

Writers flush/fsync/close before scheduling the existing compression workers.
A worker locks its source inode, fsyncs it, streams compression to an exclusive
`.compress-<source-stem>-<nonce>.pending` sibling with Zstandard frame checksum,
fsyncs the closed output, and verifies decoded SHA-256 against source bytes.
It atomically hard-links the temporary inode to the final `.jsonl.zst` name;
any existing destination, including a symlink, prevents publication. It fsyncs
the directory, removes/fsyncs its temporary link, checks the final single-link
artifact with the unchanged coverage identity validator, rechecks source
metadata, and only then unlinks the source and fsyncs the directory again.
The potentially large hashing/compression reads hold no directory lock.

Handled prepublication failures remove only the worker's disposable output,
retaining source bytes. After publication, uncertainty retains the source and
any remaining pending/dual representations; no completed identity is cached.
A crash or shutdown timeout can leave work in flight or residue. New allocations
never reuse its source identity. No automatic recovery infers clean closure.
Readers reject pending files and representation ambiguity before consuming
records; a live pending file can safely defer/fail a replay attempt, requiring
a later invocation after completion/inspection. Existing bounded whole-attempt
plain-to-compressed retries also recognize exact segment names.

Legacy full-L2 and replay readers already enumerate `*.jsonl*` in lexical order:
old hourly files and ordered segments coexist and are read exactly once when
unambiguous. T00/T23 readiness now checks **all segments** of the boundary hour;
a compressed canonical file cannot mask an active same-hour segment. The
existing later-hour closure policy, event-time repartitioning, source range
semantics, Parquet schemas and builder versions stay intact. Historical
single-hour files and manifests need no rewrite or migration.

Coverage final validation accepts only the exact positive six-digit extension,
with all existing path, universe, hash, size, ownership and closure predicates.
Earlier session evidence remains immutable. Retirement still requires a single
owner of all selected raw inputs; multi-session dates conservatively defer
without D+1. Its 72-entry directory limit is unchanged: a closed 24-hour day
leaves at most 48 extra segment entries, fewer during transient coexistence.
Overflow remains a blocker, not a reason to delete evidence or raise the limit.
Pre-fix coverage validators reject new segment final records; update consumers
before producing segments, and follow OPERATIONS.md for rollback constraints.
