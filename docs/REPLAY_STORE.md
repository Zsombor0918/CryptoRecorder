# Replay Store

## Overview

The replay_store is a Parquet-based columnar replay layer. It is the stable
external contract consumed by downstream repositories (e.g. KovacsTrader), and
it backs the internal, validation-only `validation.replay_catalog_reconstruct`
helper for both `trades_only` and `full_l2` reconstruction (the latter
semantically validated on the ADAUSDT single-day smoke against `convert_day.py`,
with broader validation pending). `convert_day.py` is still the production
reference full-L2 path. CryptoRecorder does not build a feature-store,
label-store, or general-purpose consumer catalog from replay_store.

**Key properties**:
- **Fail-closed publication** — Published partitions are reused only when
  schema/source/checksum-valid; an intentional exact-partition source/schema
  replacement uses backup/restore and never silently overwrites legacy data
- **Deterministically sorted** — Same raw data always produces identical Parquet (enables validation)
- **Columnar format** — Efficient for time-series queries and feature calculations
- **Streaming access** — Load via [ReplayReader](#replayreader-api) without materializing full days in memory
- **Exact decimals preserved** — Float fields exist for feature convenience, and string fields preserve source price/size values for Nautilus reconstruction
- **Memory-bounded writes** — `ReplayWriter` spools records to a SQLite file inside the staging directory (`staging_dir/scratch/`) and writes Parquet incrementally in bounded batches; peak RSS is O(batch), not O(symbol/day)

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

## Versioning (v0 / v1 / v2)

Legacy **v0** is accepted without a `schema_version` only when both physical
Parquet channels exactly match the recognized v0 schemas above, including the
required exact decimal-string fields. A missing/malformed manifest, a compact
fixed-point layout without an explicit version, an unsupported version, or a
manifest/physical-schema contradiction fails before decoding. There is no
compact-to-v0 fallback.

Explicit `schema_version` values 0, 1, and 2 dispatch to their matching
physical readers. v1 and v2 manifests also carry `format_version`,
`builder_version`, and `encoding_profile`. The current builders are
`cryptorecorder-replay-writer-v1.2.1` and
`cryptorecorder-replay-writer-v2.0.1`. Those patch increments identify a
trade-normalization correction; they do not change either physical schema or
format version. Existing schema-v1 manifests with a non-empty recorded
builder remain readable/auditable under the existing physical contract, but
the canonical builder does not silently reuse them as v1.2.1 output.

**v1 physical differences from v0** (all restored to the exact v0 logical
row shape by `ReplayReader.iter_depths()`/`iter_trades()` — downstream code,
including `stores/replay_depth_adapter.py` and
`validation/validate_catalog_equivalence.py`, requires no changes to read
either version):

- `venue`/`symbol`/`date` are removed from every row and read from the
  manifest/partition path instead (see the checked-in Phase 3 field matrix
  in `docs/IMPLEMENTATION_AUDIT.md`).
- `record_type` is stored as an `int8` enum code (`record_type_code`)
  instead of a string.
- The 5 depth boolean columns (`is_snapshot_seed`, `is_depth_update`,
  `is_sync_state`, `is_desync`, `is_resync`) are packed into a single `int8`
  bitmask (`flags`).
- `price`/`size`/`quantity` are stored as an exact fixed-point integer
  mantissa (`int64`) instead of `float64` + lexical string columns. The
  scale is derived once per partition from date-specific Binance
  `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize` (spot and futures
  independently) and recorded in the manifest (`price_scale`/`qty_scale`).
  Encoding/decoding uses `Decimal` only — never a binary float
  intermediate.
- `native_payload_hash` is stored as 32 raw bytes (`fixed_size_binary[32]`)
  instead of a 64-character hex string — the hash itself is retained (the
  Phase 2 Section 3 traceability replacement remains design-only, so hash
  removal is not authorized), only its physical encoding is compacted.
- v1 manifests additionally carry a best-effort `source_identity` field
  (per-file SHA-256 + size for the raw files that produced the partition,
  via `pipeline.raw_manifest.compute_raw_source_identity`) — provenance
  evidence only, not a substitute for the per-event hash and not required
  for reconstruction (a v1 partition remains fully self-contained without
  it).

**Not compacted in v1** (deferred — the matrix marks these "pending proof"/
"benchmark-needed", not overlooked): `U`/`u`/`pu` continuity ids,
`trade_id`/`agg_trade_id`, `market_type`, and `quality_flags` remain in
their v0 lexical/JSON representations.

**v2** retains v1's compact fixed-point physical rows but replaces the
per-event payload hash with the manifest-level source/file/block integrity
hierarchy documented in `docs/IMPLEMENTATION_AUDIT.md`. New v2 builds use the
current v2.0.1 builder. Existing v2.0.0 partitions remain physically readable
and auditable under their recorded digest method, but the canonical builder
will not silently reuse them as current output and the current artifact-bound
semantic gate requires v2.0.1. An intentional rebuild is therefore required
before an old v2.0.0 partition can provide final-checkpoint evidence under the
corrected normalization.

Across every physical version, replay publication now requires each supported
trade row to contain a non-empty `trade_id` or `agg_trade_id`. Canonical raw
normalization preserves existing top-level identifiers; if every top-level
identifier is absent it recovers Binance native `trade.t` or `aggTrade.a`
exactly. No sequence, row index, hash, or synthetic identifier is substituted.
Reconstruction independently raises on an anonymous replay row so corrupt or
historical unusable data cannot produce a successful shorter comparison.

Local development measurements and semantic evidence remain scoped to the
specific symbols/days recorded in `docs/PROJECT_STATUS.md` and
`docs/CHANGE_AUDIT.md`; they are not a production or full-universe claim.



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
--schema-version {0,1,2} Physical schema (production default: 2)
--rebuild-source-changed Explicit exact-partition source-change replacement
--replace-incompatible  Explicit exact-partition legacy/schema replacement
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
  --replay-root /data/replay \
  --schema-version 2
```

### Processing Details

For each symbol/date:

1. **Stream raw data**: Read from `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)`
2. **Spool to disk**: Each batch of converted records is written immediately to a temporary SQLite spool (`converter.spool.RawRecordSpool`) — no full-day Python list is retained
3. **Deterministic sort**: SQLite index enforces sort order `(stream_session_id, session_seq, raw_index)` for depth and `(trade_stream_session_id, trade_session_seq, raw_index)` for trades
4. **Incremental Parquet write**: Spool is read back in bounded batches (default 5 000 rows) through `pyarrow.parquet.ParquetWriter`; depth channel is fully written and closed before the trade channel begins
5. **Compute checksums**: SHA256 hash of each Parquet file for integrity
6. **Write manifest**: Store metadata and checksums
7. **Durable evidence**: fsync manifest and staging directory before rename
8. **Atomic publication**: preserve any existing canonical as one backup,
   publish staging, routine-validate the new canonical, restore on failure,
   and remove the obsolete valid backup only after success

Temporary spool files live inside `staging_dir/scratch/`. Every supported
mutation entrypoint holds `<replay-root>/.lifecycle/build.lock`. Before a
direct/daily build, a bounded cross-date reconciliation moves stale staging
to unique quarantine, restores one unambiguous valid backup, safely removes
an obsolete valid backup beside a valid canonical, and refuses unknown,
symlinked, corrupt, or ambiguous state. Quarantine is never automatically
deleted. Lock metadata and every recovery action are carried into daily/run
reports; kernel lock ownership, not PID-file age, is authoritative.

The scheduled/default path requests schema 2 and does not set either
replacement policy. A matching v2/source partition is `skipped_valid`; changed
source is `source_changed_rebuild_required`; legacy/incompatible schema is
`incompatible_schema_rebuild_required`; corrupt replay is `failed`.

## Auditing Replay Store

Use the non-mutating audit CLI to verify a replay partition after building:

```bash
python -m validation.audit_replay_store \
  --replay-root /path/to/replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT
```

The audit reports:

- manifest counts vs Parquet row counts;
- SHA256 checksum matches;
- deterministic sortedness;
- duplicate sequence-key counts;
- timestamp ranges;
- `U/u/pu` null ratios;
- trade `price_str` / `quantity_str` null ratios;
- nested depth `price_str` / `size_str` field presence;
- `instrument.json` and `manifest.json` presence.

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
# Build twice in an isolated external root and compare checksums
python -m pipeline.build_replay_store --date 2026-06-15 --schema-version 2 \
  --replay-root /external/replay-check
CHECKSUM1=$(jq -r .trades_checksum /external/replay-check/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/manifest.json)

# A second ordinary run must skip the same valid artifact.
python -m pipeline.build_replay_store --date 2026-06-15 --schema-version 2 \
  --replay-root /external/replay-check

CHECKSUM2=$(jq -r .trades_checksum /external/replay-check/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/manifest.json)

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

**Solution**: stop. Preserve the canonical, lock metadata, backups, staging,
and quarantine. Run the non-mutating replay audit and inspect the daily/run
recovery report. Corrupt replay is never implicitly replaced by either
source/schema policy; obtain a separate exact-partition owner decision after
the root cause and a valid source identity are proven.

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

- [ARCHITECTURE.md](ARCHITECTURE.md) — Overall pipeline
- [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) — How to build
