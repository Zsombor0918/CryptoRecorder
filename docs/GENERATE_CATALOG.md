# Generate Catalog

## Overview

The `generate_catalog` module creates on-demand Nautilus `ParquetDataCatalog` exports from the replay_store for specific symbols, venues, and time windows.

Current status:

```text
data_raw -> replay_store -> generate_catalog --profile trades_only
  implemented, semantically validated

data_raw -> replay_store -> generate_catalog --profile full_l2
  implemented; semantically validated on the ADAUSDT single-day smoke
  (trades + OrderBookDeltas + OrderBookDepth10 + book checkpoints match
   convert_day.py). Broader top50/multi-day validation pending.

data_raw -> convert_day.py -> full_l2 Nautilus catalog
  production reference full-L2 path (unchanged)
```

**Key use cases**:
1. **Validation**: Compare new pipeline output vs old `convert_day.py` (semantic equivalence test)
2. **Time-windowed queries**: Extract data for specific hours/days without rebuilding entire daily catalog
3. **Symbol subsets**: Create catalogs for specific coins without processing full venue
4. **External analysis**: Export replay data to Nautilus format for external tools

## CLI

```bash
python -m pipeline.generate_catalog [OPTIONS]
```

### Required Arguments

```
--input PATH
    Replay store root
    
    Default: config.REPLAY_ROOT
    Example: /data/replay_store

--symbols SYMBOLS
    Comma-separated symbols to include
    
    Required (no default; explicitly specify symbols)
    Examples:
    - --symbols BTCUSDT
    - --symbols BTCUSDT,ETHUSDT,BNBUSDT

--start START
    Start datetime (ISO 8601 UTC)
    
    Format: YYYY-MM-DDTHH:MM:SSZ (with Z suffix for UTC)
    Examples:
    - --start 2026-06-15T00:00:00Z  (midnight)
    - --start 2026-06-15T12:00:00Z  (noon)
    - --start 2026-06-15T12:30:45Z  (with seconds)

--end END
    End datetime (ISO 8601 UTC)
    
    Format: YYYY-MM-DDTHH:MM:SSZ
    Must be after start. The window is half-open: start <= ts < end.

--date DATE
    UTC date shortcut (YYYY-MM-DD)

    Equivalent to:
      --start DATET00:00:00Z
      --end   NEXT_DATET00:00:00Z

    Use either --date or --start/--end, not both.
```

### Optional Arguments

```
--venues VENUES
    Comma-separated venues to include
    
    Default: BINANCE_SPOT,BINANCE_USDTF
    Examples:
    - --venues BINANCE_SPOT
    - --venues BINANCE_SPOT,BINANCE_USDTF

--output PATH
    Catalog output root
    
    Default: config.CATALOG_JOBS_ROOT
    Example: /data/catalog_jobs

--profile PROFILE
    Catalog profile (data included)
    
    Options:
    - trades_only: Trades only, no order book
    - full_l2:     Trades + OrderBookDeltas + (optional) OrderBookDepth10
    - depth_only:  OrderBookDeltas only (no trades, no Depth10)
    - depth10:     OrderBookDepth10 snapshots only
    
    Default: trades_only

--emit-depth10 / --no-emit-depth10
    For full_l2 / depth10: emit derived OrderBookDepth10 snapshots.
    Default: emit (config.EMIT_DEPTH10_DEFAULT).

--depth10-interval-sec SECONDS
    Sampling interval for derived Depth10 snapshots.
    Default: config.DEPTH10_INTERVAL_SEC (1.0).

--derived-depth-snapshot-levels N
    Number of book levels per derived Depth10 snapshot.
    Default: config.DERIVED_DEPTH_SNAPSHOT_LEVELS (10).

--time-filter {ts_init,ts_event}
    Field used for the [start, end) window filter when reading back.
    Default: ts_init (matches Nautilus catalog query semantics).

--job-id NAME
    Optional deterministic job id. Output directory is job_{NAME}.

--overwrite
    Delete and recreate job_{NAME} if it already exists.
```

For `full_l2`, the catalog reproduces the `convert_day.py` semantics through the
shared depth engine in `converter/depth_phase2.py` (via
`stores/replay_depth_adapter.py`). The manifest records `depth_diagnostics`,
`fenced_ranges`, and `equivalence_caveats`. See
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md) for the
equivalence boundary and the ADAUSDT smoke result.

## Examples

### Basic usage: Single symbol, single hour

```bash
python -m pipeline.generate_catalog \
  --input /data/replay_store \
  --symbols BTCUSDT \
  --start 2026-06-15T12:00:00Z \
  --end 2026-06-15T13:00:00Z
```

Output:
```
catalog_jobs/job_20260615_142530/
├── manifest.json
└── data/
    ├── currency_pair/
    │   └── BTCUSDT.BINANCE/
    │       └── *.parquet
    └── trade_tick/
        └── BTCUSDT.BINANCE/
            └── *.parquet
```

### Multiple symbols, full day

```bash
python -m pipeline.generate_catalog \
  --input /data/replay_store \
  --symbols BTCUSDT,ETHUSDT,BNBUSDT \
  --venues BINANCE_SPOT,BINANCE_USDTF \
  --date 2026-06-15 \
  --profile trades_only
```

Output:
```
catalog_jobs/job_20260615_142530/
├── manifest.json
└── data/
    ├── currency_pair/
    ├── crypto_perpetual/
    └── trade_tick/
```

### Smoke sample using explicit temp roots

```bash
# Build replay first, then generate a trades-only Nautilus catalog.
python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root /tmp/test_replay

python -m pipeline.generate_catalog \
  --input /tmp/test_replay \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --date 2026-06-12 \
  --profile trades_only \
  --output /tmp/test_catalogs_new \
  --job-id validation_new \
  --overwrite
```

Compare output with the old pipeline using the validation CLI:
```bash
python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --old-catalog-root /tmp/cryptorecorder-equivalence/old_catalog \
  --replay-root /tmp/cryptorecorder-equivalence/replay_store \
  --new-catalog-root /tmp/cryptorecorder-equivalence/new_catalog \
  --profile trades_only \
  --overwrite
```

### Full-L2 generation + equivalence (ADAUSDT smoke)

```bash
# Generate a full-L2 catalog (trades + OrderBookDeltas + Depth10).
python -m pipeline.generate_catalog \
  --input /tmp/test_replay \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --date 2026-06-12 \
  --profile full_l2 \
  --output /tmp/test_catalogs_new \
  --job-id validation_full_l2 \
  --overwrite

# One-shot build + convert_day + compare (writes a report under validation_reports/).
python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --profile full_l2 \
  --overwrite
```

## Output Structure

### Manifest

```json
{
  "job_id": "20260615_142530",
  "created_at_utc": "2026-06-15T14:25:30.123456Z",
  "profile": "trades_only",
  "requested_symbols": ["BTCUSDT", "ETHUSDT"],
  "requested_venues": ["BINANCE_SPOT", "BINANCE_USDTF"],
  "symbols": [
    "BINANCE_SPOT:BTCUSDT",
    "BINANCE_SPOT:ETHUSDT",
    "BINANCE_USDTF:BTCUSDT"
  ],
  "found_partitions": [
    {"venue": "BINANCE_SPOT", "symbol": "BTCUSDT", "date": "2026-06-15"}
  ],
  "missing_partitions": [],
  "date_partitions_scanned": ["BINANCE_SPOT:BTCUSDT:2026-06-15"],
  "time_filter": "ts_init",
  "time_window": {
    "start": "2026-06-15T00:00:00Z",
    "end": "2026-06-16T00:00:00Z"
  },
  "records_read": {
    "trades": 123456,
    "depth": 0
  },
  "record_counts": {
    "trade_ticks": 123456,
    "order_book_deltas": 0,
    "order_book_depth10": 0
  },
  "records_skipped": {
    "outside_window": 0,
    "invalid_trade": 0
  },
  "skipped_invalid_records": 0,
  "replay_source": "/data/replay_store"
}
```

## Profiles

### trades_only (default)

Includes only TradeTick events.

```python
# Exported as Nautilus trade_tick Parquet files under data/trade_tick/{instrument_id}/
TradeTick(
    instrument_id=InstrumentId(symbol, venue),
    price=Price.from_str(trade['price_str']),
    size=Quantity.from_str(trade['quantity_str']),
    aggressor_side="BUY" if not trade['buyer_maker'] else "SELL",
    trade_id=str(trade['trade_id']),
    ts_event=trade['ts_exchange_ns'],
    ts_init=trade['ts_receive_ns'],
)
```

**Use cases**:
- Fast catalog generation (no depth processing)
- Semantic equivalence test vs old convert_day.py
- Trade flow analysis
- Minimal storage footprint

**Size**: ~100 KB per 100K trades

### full_l2 (implemented; validated on ADAUSDT smoke)

Includes TradeTick, OrderBookDeltas, and (optionally) OrderBookDepth10.

```python
# Exported as Nautilus Parquet under the job's data/ dir:
# - trade_tick/{instrument_id}/*.parquet
# - order_book_deltas/{instrument_id}/*.parquet (one row per book change)
# - order_book_depths/{instrument_id}/*.parquet (Depth10 snapshots, top 10 levels)
```

**Use cases**:
- Full market microstructure analysis
- Advanced feature engineering
- Order book dynamics research
- Portfolio risk modeling

**Size**: Measured ADAUSDT 2026-06-12 full day ≈ **32 MiB** (order_book_deltas
22.5 MiB + order_book_depths 6.9 MiB + trade_tick 2.6 MiB), the same size class as
the `convert_day.py` catalog (33 MiB). See
[IMPLEMENTATION_AUDIT.md](IMPLEMENTATION_AUDIT.md).

**Status**: Implemented. Semantically validated on the ADAUSDT single-day smoke
against `convert_day.py`; broader top50/multi-day validation pending.

### depth_only (implemented)

Includes only OrderBookDeltas (no trades, no Depth10).

```python
# Exported as:
# - order_book_deltas/{instrument_id}/*.parquet
```

**Use cases**:
- Order book reconstruction without trade information
- Liquidity / book-shape research

**Size**: ≈ order_book_deltas component of full_l2 (≈ 22 MiB/day for ADAUSDT).

**Status**: Implemented (shares the full_l2 depth engine).

### depth10 (implemented)

Includes only derived OrderBookDepth10 snapshots.

```python
# Exported as:
# - order_book_depths/{instrument_id}/*.parquet
```

**Use cases**:
- Order book shape analysis
- Liquidity distribution research
- Microstructure without trade information

**Size**: ≈ Depth10 component of full_l2 (≈ 7 MiB/day for ADAUSDT).

**Status**: Implemented (shares the full_l2 depth engine).

## Future Full-L2 Validation Requirements

## Full-L2 Validation Status

The replay-based full-L2 path is validated for semantic equivalence against the
old converter **on the ADAUSDT single-day smoke**; broader universe/multi-day
validation is still pending before `v2.0.0`:

```text
data_raw -> convert_day.py
matches (ADAUSDT 2026-06-12 smoke)
data_raw -> replay_store -> generate_catalog --profile full_l2
```

The comparison covers:

- instruments;
- TradeTick count and sampled equality;
- OrderBookDeltas count;
- first, last, and sampled deltas;
- action, side, price, size, order_id, flags, and sequence where applicable;
- reconstructed book checkpoints at start + 1 minute, quarter day, half day, and end - 1 minute;
- top 10 bid/ask equality at checkpoints;
- gap/fenced range report equality or an explicit acceptable-difference explanation;
- optional OrderBookDepth10 semantic comparison if emitted.

Acceptance is semantic equality, not byte-for-byte Parquet equality.

## Processing

### Step 1: Parse time window

```python
start = datetime.fromisoformat("2026-06-15T12:00:00Z".replace("Z", "+00:00"))
end = datetime.fromisoformat("2026-06-15T13:00:00Z".replace("Z", "+00:00"))
```

The `--date YYYY-MM-DD` shortcut expands to the half-open UTC day
`[date 00:00:00 UTC, next date 00:00:00 UTC)`.

### Step 2: Determine date range and Hive partitions

```python
# Date range from time window
dates_to_scan = ["2026-06-15"]  # end timestamps at midnight do not include the next date

# Hive partitions to scan
partitions = [
    "replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/",
    "replay_store/venue=BINANCE_USDTF/symbol=BTCUSDT/date=2026-06-15/",
]
```

### Step 3: Stream replay data per symbol

```python
reader = ReplayReader(replay_root)

for venue in venues:
    for symbol in symbols:
        # Stream trades from partition
        for trade in reader.iter_trades(venue, symbol, date):
            ts_init_ns = trade['ts_receive_ns']
            
            # Filter by Nautilus catalog query time (ts_init)
            if ts_init_ns < start_ns:
                continue
            if ts_init_ns >= end_ns:
                continue
            
            # Convert and export
            tick = convert_trade_to_nautilus(trade, venue, symbol)
            write_to_catalog(tick)
```

### Step 4: Write manifest and report

```json
{
  "job_id": "...",
  "requested_symbols": [...],
  "requested_venues": [...],
  "symbols": [...],
  "found_partitions": [...],
  "missing_partitions": [...],
  "date_partitions_scanned": [...],
  "time_filter": "ts_init",
  "records_read": {
    "trades": 123456
  },
  "record_counts": {
    "trade_ticks": 123456
  },
  "records_skipped": {
    "outside_window": 0,
    "invalid_trade": 0
  },
  "skipped_invalid_records": 0
}
```

## Time Window Filtering

All records are filtered by `ts_receive_ns`, which becomes Nautilus `ts_init`. This matches bounded reads from `ParquetDataCatalog` and the old `convert_day.py` output. `ts_exchange_ns` is still preserved as `ts_event`.

Future design note: if exchange-time filtering becomes useful, add an explicit
`--time-filter ts_init|ts_event` option. Do not silently change the current
`ts_init` behavior.

### Example: 1-hour window

```
--start 2026-06-15T12:00:00Z  → 1718424000 seconds → 1718424000_000000000 ns
--end   2026-06-15T13:00:00Z  → 1718427600 seconds → 1718427600_000000000 ns

Included: records where 1718424000_000000000 <= ts_init < 1718427600_000000000
```

### Partial partition filtering

If time window spans multiple dates, only relevant dates are scanned:

```python
dates = _date_range_from_window(start, end)
# Output: ["2026-06-15", "2026-06-16"] for start=2026-06-15T12:00:00Z, end=2026-06-16T12:00:00Z
```

## Validation Examples

### Verify record count matches replay window

```bash
# Generate catalog and check
python -m pipeline.generate_catalog \
  --input /data/replay_store \
  --symbols BTCUSDT \
  --date 2026-06-15

# Check manifest
cat catalog_jobs/job_*/manifest.json | jq .record_counts.trade_ticks
# This should match the number of replay trades whose ts_receive_ns is inside [start, end).
```

### Compare old vs new pipeline

```bash
# Old pipeline (convert_day.py)
python convert_day.py --date 2026-06-15

# New pipeline (generate_catalog)
python -m pipeline.generate_catalog \
  --input /data/replay_store \
  --symbols BTCUSDT \
  --date 2026-06-15

# Load both with Nautilus ParquetDataCatalog or inspect the new files under:
# catalog_jobs/job_*/data/trade_tick/{instrument_id}/*.parquet

# Compare instruments, trade counts, timestamp min/max, and bounded samples.
```

## Troubleshooting

### Issue: "No data found for symbols"

**Symptom**:
```
INFO: Processing BINANCE_SPOT/BTCUSDT...
INFO: No replay data for BINANCE_SPOT/BTCUSDT in 2026-06-15
```

**Cause**: Replay store partition missing or not built yet

**Solution**:
```bash
# Check if replay_store exists
ls -la /data/replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/

# If missing, build from raw first
python -m pipeline.build_replay_store --date 2026-06-15 --symbols BTCUSDT
```

### Issue: "Empty catalog output"

**Symptom**: Job directory created but no files under `data/trade_tick/`

**Cause**: Time window doesn't overlap with available data

**Solution**:
```bash
# Check actual data timestamps
python -c "
from stores.replay_reader import ReplayReader
reader = ReplayReader()
trades = list(reader.iter_trades('BINANCE_SPOT', 'BTCUSDT', '2026-06-15'))
if trades:
    from datetime import datetime, timezone
    first_ts = trades[0]['ts_exchange_ns'] // 1e9
    last_ts = trades[-1]['ts_exchange_ns'] // 1e9
    print(f'Data range: {datetime.fromtimestamp(first_ts, tz=timezone.utc)} to {datetime.fromtimestamp(last_ts, tz=timezone.utc)}')
else:
    print('No trades found')
"

# Adjust --start/--end to match actual data range
```

### Issue: "ISO datetime parse error"

**Symptom**:
```
ERROR: Invalid ISO 8601 datetime: 2026-06-15 12:00:00
```

**Cause**: Wrong datetime format

**Solution**: Use ISO 8601 with Z suffix:
```bash
# Correct
--start 2026-06-15T12:00:00Z

# Incorrect (missing T and Z)
--start 2026-06-15 12:00:00
--start 2026-06-15T12:00:00
```

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) — Overall pipeline
- [REPLAY_STORE.md](REPLAY_STORE.md) — Replay data format
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — Daily build operations
- [test_semantic_equivalence.py](../tests/test_semantic_equivalence.py) — Validation testing
