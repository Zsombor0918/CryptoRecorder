# Replay Store

## Overview

The replay_store is a Parquet-based columnar replay layer. It is a candidate long-term replay source after validation, not yet a replacement for the validated full-L2 `convert_day.py` path.

**Key properties**:
- **Immutable after publication** — Each date/symbol partition is written once and never modified
- **Deterministically sorted** — Same raw data always produces identical Parquet (enables validation)
- **Columnar format** — Efficient for time-series queries and feature calculations
- **Streaming access** — Load via [ReplayReader](#replayreader-api) without materializing full days in memory
- **Exact decimals preserved** — Float fields exist for feature convenience, and string fields preserve source price/size values for Nautilus reconstruction
- **v0 write limitation** — `ReplayWriter` currently accumulates one symbol/date in memory before writing; use RSS benchmarks before large production runs

## Structure

```
replay_store/
├── venue=BINANCE_SPOT/
│   └── symbol=BTCUSDT/
│       └── date=2026-06-15/
│           ├── depth.parquet           # Order book snapshots
│           ├── trades.parquet          # Trade executions
│           ├── instrument.json         # Symbol metadata (decimals, limits)
│           └── manifest.json           # Partition metadata (record counts, checksums)
└── venue=BINANCE_USDTF/
    └── symbol=BTCUSDT/
        └── date=2026-06-15/
            ├── depth.parquet
            ├── trades.parquet
            ├── instrument.json
            └── manifest.json
```

## Schemas

### Depth (Order Book) Schema

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
    pa.field("is_sync_state", pa.bool_()),
    pa.field("is_desync", pa.bool_()),
    pa.field("is_resync", pa.bool_()),
    pa.field("quality_flags", pa.string()),
    pa.field("native_payload_hash", pa.string()),
])
```

### Trade Schema

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

## Manifest Format

Each partition contains a `manifest.json` with metadata:

```json
{
  "date": "2026-06-15",
  "symbol": "BTCUSDT",
  "venue": "BINANCE_SPOT",
  "status": "complete",
  "depth_record_count": 86400,
  "trade_record_count": 45230,
  "ts_range_start_ns": 1718400000000000000,
  "ts_range_end_ns": 1718486399999999999,
  "depth_checksum": "abc123def456...",
  "trades_checksum": "xyz789abc123...",
  "created_at_utc": "2026-06-16T01:02:03.000000+00:00",
  "errors": []
}
```

**Fields**:
- `depth_record_count` / `trade_record_count`: Number of rows in each Parquet file
- `depth_checksum` / `trades_checksum`: SHA256 hashes for integrity verification
- `ts_range_start_ns` / `ts_range_end_ns`: Min/max server timestamps in UTC nanoseconds

## ReplayReader API

Load and query replay_store data via the [ReplayReader](../stores/replay_reader.py) interface.

### Initialize

```python
from stores.replay_reader import ReplayReader

reader = ReplayReader(root=Path("/path/to/replay_store"))
```

### List available data

```python
# All venues
for venue in reader.iter_venues():
    print(f"Venue: {venue}")
    # Output: Venue: BINANCE_SPOT
    #         Venue: BINANCE_USDTF

# All symbols for venue
for symbol in reader.iter_symbols(venue="BINANCE_SPOT"):
    print(f"Symbol: {symbol}")
    # Output: Symbol: BTCUSDT, Symbol: ETHUSDT, ...

# All dates for symbol
for date in reader.iter_dates(venue="BINANCE_SPOT", symbol="BTCUSDT"):
    print(f"Date: {date}")
    # Output: Date: 2026-06-15, Date: 2026-06-16, ...
```

### Stream trades

```python
# Stream all trades for a symbol on a date
for trade in reader.iter_trades(
    venue="BINANCE_SPOT",
    symbol="BTCUSDT",
    date="2026-06-15",
):
    print(
        f"Trade: ID={trade['trade_id']}, "
        f"Price={trade['price_str']}, Qty={trade['quantity_str']}"
    )
    
    # Output:
    # Trade: ID=123456, Price=67500.25, Qty=0.5
    # Trade: ID=123457, Price=67500.50, Qty=1.2
    # ...
```

**Features**:
- Streams from Parquet (no full materialization)
- Batch-fetches 5000 records at a time
- Maintains iteration state across multiple calls

### Stream depth records

```python
# Stream all depth snapshots for a symbol on a date
for depth in reader.iter_depths(
    venue="BINANCE_SPOT",
    symbol="BTCUSDT",
    date="2026-06-15",
):
    bids = depth["bids"]  # List of {price, size, price_str, size_str} dicts
    asks = depth["asks"]  # List of {price, size, price_str, size_str} dicts
    
    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    
    print(f"Depth: Best bid={best_bid}, best ask={best_ask}, #{len(bids)} bid levels")
    
    # Output:
    # Depth: Best bid=67500.25, best ask=67500.50, #20 bid levels
    # Depth: Best bid=67500.30, best ask=67500.51, #19 bid levels
    # ...
```

### Load metadata

```python
# Load instrument metadata (decimals, limits, etc.)
instrument = reader.load_instrument_metadata(
    venue="BINANCE_SPOT",
    symbol="BTCUSDT",
    date="2026-06-15",
)

print(f"Decimals: {instrument.get('base_decimals')}")
print(f"Min price: {instrument.get('min_price')}")
print(f"Max price: {instrument.get('max_price')}")

# Output:
# Decimals: 8
# Min price: 0.01
# Max price: 1000000.00
```

### Load manifest

```python
# Load partition manifest with record counts and checksums
manifest = reader.load_manifest(
    venue="BINANCE_SPOT",
    symbol="BTCUSDT",
    date="2026-06-15",
)

print(f"Depth records: {manifest['depth_record_count']}")
print(f"Trade records: {manifest['trade_record_count']}")
print(f"Depth checksum: {manifest['depth_checksum']}")

# Output:
# Depth records: 86400
# Trade records: 45230
# Depth checksum: abc123def456...
```

## Building Replay Store

### CLI

```bash
python -m pipeline.build_replay_store --date 2026-06-15 [OPTIONS]
```

**Options**:
```
--date DATE              Date (YYYY-MM-DD)
--symbols SYMBOLS        Comma-separated symbols (default: all)
--data-root PATH         Raw data root (default: config.DATA_ROOT)
--replay-root PATH       Replay store root (default: config.REPLAY_ROOT)
```

### Example

```bash
# Build all symbols for a date
python -m pipeline.build_replay_store \
  --date 2026-06-15 \
  --data-root /data/raw \
  --replay-root /data/replay

# Build specific symbols
python -m pipeline.build_replay_store \
  --date 2026-06-15 \
  --symbols BTCUSDT,ETHUSDT \
  --data-root /data/raw \
  --replay-root /data/replay
```

### Processing Details

For each symbol/date:

1. **Stream raw data**: Read from `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)`
2. **Accumulate symbol/date records**: v0 accumulates one symbol/date before sorting and writing
3. **Deterministic sort**: Sort by the depth/trade composite keys to ensure reproducibility
4. **Convert to Parquet**: Transform raw records to columnar schema
5. **Compute checksums**: SHA256 hash of each Parquet file for integrity
6. **Write manifest**: Store metadata and checksums
7. **Atomic move**: Move from staging to published directory

Future optimization: replace symbol/date accumulation with a streaming external sort or SQLite-backed spool before claiming memory-bounded production replay writes.

### Deterministic Sorting

**Critical for reproducibility**: Same raw data always produces identical Parquet.

**Sort keys**:

- Depth: `(stream_session_id, session_seq, raw_index)`
- Trades: `(trade_stream_session_id, trade_session_seq, raw_index)`

- `stream_session_id` / `trade_stream_session_id`: Streaming session identifier
- `session_seq`: Sequence number from streaming server
- `raw_index`: Original position in raw file (for ties)

**Validation**:
```bash
# Build twice, compare checksums
python -m pipeline.build_replay_store --date 2026-06-15
CHECKSUM1=$(cat replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/manifest.json | jq -r .trades_checksum)

# Delete and rebuild
rm -rf replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/
python -m pipeline.build_replay_store --date 2026-06-15

CHECKSUM2=$(cat replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/manifest.json | jq -r .trades_checksum)

# Should be identical
echo "Checksums match: $([[ $CHECKSUM1 == $CHECKSUM2 ]] && echo YES || echo NO)"
```

## Compression

All Parquet files use **ZSTD compression at level 3**:

```python
# Configuration
writer = parquet.ParquetWriter(
    path,
    schema,
    compression="zstd",
    compression_level=3,
)
```

**Trade-offs**:
- Level 3: Balances compression ratio (~50-70%) with write speed (~5-10 MB/s)
- Level 6+: Higher compression (~60-75%) but slower writes (~1-3 MB/s)
- Level 1: Fastest writes (~50 MB/s) but lowest compression (~30-40%)

**Rationale for level 3**:
- Write speed critical for daily batch builds
- Compression ratio acceptable for long-term storage
- Can be recompressed offline if storage cost becomes issue

## Use Cases

### Feature Calculation

```python
# Read trades and depths for a date
reader = ReplayReader()
trades = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-15"))
depths = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-15"))

# Calculate VWAP
vwap = sum(t['price'] * t['quantity'] for t in trades) / sum(t['quantity'] for t in trades)
```

### Catalog Generation

```python
# Generate Nautilus TradeTick from replay trades
for trade in reader.iter_trades(venue, symbol, date):
    tick = TradeTick(
        instrument_id=InstrumentId(symbol, venue),
        price=Price(trade['price'], 8),
        size=Quantity(trade['quantity'], 8),
        aggressor_side="BUY" if not trade['buyer_maker'] else "SELL",
        trade_id=str(trade['trade_id']),
        ts_event=trade['ts_exchange_ns'],
        ts_init=trade['ts_receive_ns'],
    )
```

### Data Validation

```python
# Check data continuity
manifest = reader.load_manifest(venue, symbol, date)
expected_depth_records = 86400  # 1 per second

if manifest['depth_record_count'] != expected_depth_records:
    print(f"Warning: Expected {expected_depth_records} depth records, "
          f"got {manifest['depth_record_count']}")
```

### Time Series Analysis

```python
# Backtest strategy on replay data
for trade in reader.iter_trades(venue, symbol, date):
    ts_sec = trade['ts_exchange_ns'] // 1_000_000_000
    
    # Skip overnight hours
    if 22 <= (ts_sec % 86400) // 3600 or (ts_sec % 86400) // 3600 < 8:
        continue
    
    # Process trade during trading hours
    process_trade(trade)
```

## Troubleshooting

### Issue: "Parquet file corrupt"

**Symptom**:
```
ArrowInvalid: Parquet magic bytes not found
```

**Cause**: Write interrupted or disk full during build

**Solution**:
```bash
# Delete partition and rebuild
rm -rf /path/to/replay_store/venue=X/symbol=Y/date=Z/

# Rebuild from raw
python -m pipeline.build_replay_store --date 2026-06-15 --symbols Y
```

### Issue: "Checksum mismatch"

**Symptom**: Manifest checksum doesn't match file contents

**Cause**: Very rare; indicates disk corruption or RAM error

**Solution**:
```bash
# Verify Parquet file integrity
parquet-tools inspect depth.parquet

# If corrupted, rebuild from raw
```

### Issue: "ReplayReader: No data found"

**Symptom**:
```python
list(reader.iter_trades(venue, symbol, date))  # Returns empty list
```

**Cause**: Parquet file exists but is empty, or query parameters wrong

**Solution**:
```bash
# Check what data is available
python -c "
from stores.replay_reader import ReplayReader
reader = ReplayReader()
print(list(reader.iter_venues()))
print(list(reader.iter_symbols('BINANCE_SPOT')))
print(list(reader.iter_dates('BINANCE_SPOT', 'BTCUSDT')))
"

# Check file exists
ls -lh /path/to/replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/
```

## See Also

- [STORAGE_ARCHITECTURE.md](STORAGE_ARCHITECTURE.md) — Overall pipeline
- [FEATURE_STORE.md](FEATURE_STORE.md) — Feature calculations
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — How to build
