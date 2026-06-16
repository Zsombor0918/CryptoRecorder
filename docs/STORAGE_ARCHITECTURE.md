# CryptoRecorder Storage Architecture

## Overview

The new architecture implements a v0 layered pipeline around the existing recorder and converter. It is not yet the final full-L2 replacement.

```
data_raw -> convert_day.py -> full_l2 Nautilus catalog
  current validated full-L2 path

data_raw -> replay_store -> generate_catalog --profile trades_only
  current implemented replay-based catalog path

data_raw -> replay_store -> generate_catalog --profile full_l2
  target path, not implemented yet
```

### Key Design Principles

1. **Raw retention, not automatic deletion** — Raw is the original capture/audit source while retained. Replay store is a candidate long-term replay layer after validation.
2. **Deterministic replay** — Replay store sorts by committed stream keys plus `raw_index` for reproducible rebuilds.
3. **Hive-style partitioning** — All stores use `venue=X/symbol=Y/date=Z` for efficient directory-based filtering.
4. **Atomic writes** — All writers use staging directory + move pattern to prevent half-written data.
5. **Memory status is explicit** — v0 replay writing and feature aggregation still materialize one symbol/date. Do not claim full production memory safety until RSS benchmarks pass.

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

**Purpose**: Candidate long-term replay layer. Feeds feature store and the currently implemented trades-only replay catalog path.

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

### 3. Feature Store (`feature_store/`)

**Purpose**: AI/selection layer computed from replay_store.

**Format**: Parquet with ZSTD compression, Hive-style partitioning by timeframe

**Structure**:
```
feature_store/
  timeframe=1s/
    venue=BINANCE_SPOT/
      symbol=BTCUSDT/
        date=2026-06-15.parquet      # sparse rows for 1s windows with data
  timeframe=100ms/
    venue=BINANCE_SPOT/
      symbol=BTCUSDT/
        date=2026-06-15.parquet      # sparse rows for 100ms windows with data
  timeframe=1m/
    venue=BINANCE_SPOT/
      symbol=BTCUSDT/
        date=2026-06-15.parquet      # sparse rows for 1m windows with data
```

**Window behavior**:
- `--date YYYY-MM-DD` clamps records to `[date 00:00:00 UTC, next date 00:00:00 UTC)`.
- Output is sparse: empty windows are skipped.
- Dense UTC-day expectations are useful for audit only: 1m = 1440, 1s = 86400, 100ms = 864000.

See [FEATURE_STORE.md](FEATURE_STORE.md) for the exact current schema. Do not use older field names such as `ts_ns`, `best_bid_size`, or `bid_imbalance_l1`; the actual schema uses `timestamp_ns`, `top1_bid_size`, and `imbalance_top1`.

### 4. Catalog Jobs (`catalog_jobs/`)

**Purpose**: Temporary runtime/backtest artifacts for specific time windows and symbols.

**Structure**:
```
catalog_jobs/
  job_20260615_120000/
    manifest.json                    # Job metadata
    data/
      currency_pair/
      crypto_perpetual/
      trade_tick/
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

### Build Feature Store

Aggregates replay_store data into time-windowed features.

**CLI**:
```bash
python -m pipeline.build_feature_store --date 2026-06-15 [--timeframes 100ms,1s,1m] [--replay-root /path/to/replay] [--feature-root /path/to/features]
```

**Processing**:
1. Per symbol/venue/timeframe: load replay trades and depths
2. Clamp records to the requested UTC day and bin observed records into sparse windows (100ms, 1s, 1m, etc)
3. Calculate core features per window:
   - BBO, spreads, liquidity metrics
   - Trade flow statistics
   - Quality checks (crossed books, gaps, reconnects)
4. Write as Parquet with Hive-style partitioning
5. Atomic move from staging → published

**Feature Lookahead Bias Rule**:
- Features must not use future data
- Close-of-window features use only data up to window end
- Next window open is available for next window calculation

### Generate Catalog

Creates Nautilus ParquetDataCatalog from replay_store for specific time windows.

**CLI**:
```bash
python -m pipeline.generate_catalog \
  --input /path/to/replay_store \
  --symbols BTCUSDT,ETHUSDT \
  --venues BINANCE_SPOT,BINANCE_USDTF \
  --start 2026-06-15T12:00:00Z \
  --end 2026-06-15T13:00:00Z \
  --profile trades_only \
  --output /path/to/catalog_jobs
```

**Processing**:
1. Parse ISO 8601 time window (--start/--end)
2. Determine date range and Hive partitions to scan
3. Per symbol: stream replay data, filter by time window
4. Convert to Nautilus TradeTick objects using exact replay price/quantity strings
5. Write a Nautilus `ParquetDataCatalog` under `catalog_jobs/job_*`
6. Generate report with coverage info

Current status: `trades_only` is implemented and smoke-tested. Depth/full-L2 catalog generation is deferred; `convert_day.py` remains the only validated full-L2 path.

## Daily Build Orchestrator

Runs all pipelines for a single date with dependency ordering.

**CLI**:
```bash
python -m pipeline.daily_build --date 2026-06-15 [--steps replay,features] [--symbols BTCUSDT,ETHUSDT]
```

**Execution**:
1. Scan raw directory for available data
2. Build replay_store (if --steps includes replay)
3. Build feature_store (if --steps includes features)
4. Generate daily_build_report.json with stats and errors
5. Exit with success/failure status

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
  "feature_build": {
    "symbols_processed": 2500,
    "feature_records": 18720000
  },
  "errors": []
}
```

## Systemd Integration

**Service**: `cryptorecorder-daily-build.service`
- Runs daily build orchestrator
- Loads env vars from `/etc/cryptorecorder/cryptorecorder.env`
- Restarts on failure with 5min backoff

**Timer**: `cryptorecorder-daily-build.timer`
- Triggers at 01:00 UTC daily
- Allows previous day hourly rotation/compression to complete
- Persistent: runs immediately if system was down

## Migration from Old Pipeline

**Validated full-L2 path** (`convert_day.py`):
```
data_raw → Nautilus ParquetDataCatalog
```

**Implemented replay v0 path**:
```
data_raw → replay_store → feature_store
data_raw → replay_store → generate_catalog --profile trades_only
```

**Rollout**:
1. Keep `convert_day.py` as the validated full-L2 path.
2. Validate trades-only semantic equivalence with `pipeline.validate_catalog_equivalence`.
3. Implement replay-based full-L2 generation only after validation requirements are met.
4. Benchmark replay writer and feature builder RSS before large-symbol production runs.
5. Decide raw archival policy only after replay validation has enough history.

**Backward Compatibility**:
- `convert_day.py` remains functional and is still the full-L2 baseline
- Can be run alongside new pipeline for comparison
- Legacy code paths preserved for rollback

## See Also

- [REPLAY_STORE.md](REPLAY_STORE.md) — Replay store schema and usage
- [FEATURE_STORE.md](FEATURE_STORE.md) — Feature calculations and lookahead bias
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — Operations and examples
- [GENERATE_CATALOG.md](GENERATE_CATALOG.md) — On-demand catalog examples
- [IMPLEMENTATION_AUDIT.md](IMPLEMENTATION_AUDIT.md) — Current validation status and limitations
