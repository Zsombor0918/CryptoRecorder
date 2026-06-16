# Daily Build Pipeline Operations

## Overview

The daily build pipeline orchestrates the conversion of raw market data into replay_store and feature_store. It's designed to run once per day via systemd timer and supports flexible on-demand execution with custom parameters.

## Quick Start

### Run today's build

```bash
cd /home/zsom/services/CryptoRecorder
python -m pipeline.daily_build --date today
```

### Run yesterday's build (typical cron usage)

```bash
python -m pipeline.daily_build --date yesterday
```

### Custom date and symbols

```bash
python -m pipeline.daily_build --date 2026-06-15 --symbols BTCUSDT,ETHUSDT --steps replay,features
```

## CLI Interface

```bash
python -m pipeline.daily_build [OPTIONS]
```

### Required Arguments

```
--date DATE
    Date to build (YYYY-MM-DD or 'yesterday')
    
    Special values:
    - 'yesterday': Previous completed UTC day (e.g., if today is 2026-06-16T00:15, yesterday = 2026-06-15)
    - YYYY-MM-DD: Explicit date (e.g., 2026-06-15)
```

### Optional Arguments

```
--steps STEPS
    Pipeline steps to run (default: replay,features)
    
    Options:
    - replay: Build replay_store only
    - features: Build feature_store only
    - replay,features: Run both (default)

--symbols SYMBOLS
    Comma-separated symbols to process (default: all from raw data)
    
    Examples:
    - --symbols BTCUSDT
    - --symbols BTCUSDT,ETHUSDT,BNBUSDT

--timeframes TIMEFRAMES
    Feature timeframes (default: 100ms,1s,1m)
    
    Examples:
    - --timeframes 1s
    - --timeframes 100ms,1s,1m,5m

--data-root PATH
    Raw data root (default: from config.DATA_ROOT)
    
    Use this to override config for testing

--replay-root PATH
    Replay store root (default: from config.REPLAY_ROOT)

--feature-root PATH
    Feature store root (default: from config.FEATURE_ROOT)

--report-root PATH
    Report output root (default: from config.DAILY_REPORT_ROOT)
```

## Examples

### Production: Daily systemd timer

The systemd timer automatically runs at 01:00 UTC:

```bash
systemctl start cryptorecorder-daily-build.timer
systemctl status cryptorecorder-daily-build.timer
```

This executes:
```bash
python -m pipeline.daily_build --date yesterday
```

View logs:
```bash
journalctl -u cryptorecorder-daily-build.service -f
```

### Development: Run specific date with symbols

```bash
python -m pipeline.daily_build \
  --date 2026-06-15 \
  --symbols BTCUSDT,ETHUSDT \
  --steps replay \
  --data-root /tmp/test_raw
```

### Testing: Custom storage paths

```bash
python -m pipeline.daily_build \
  --date 2026-06-15 \
  --data-root /tmp/test_raw \
  --replay-root /tmp/test_replay \
  --feature-root /tmp/test_features \
  --report-root /tmp/test_reports
```

### Monitoring: Check daily report

```bash
cat /path/to/daily_reports/daily_build_2026-06-15.json | jq .
```

Expected output:
```json
{
  "date": "2026-06-15",
  "created_at_utc": "2026-06-16T01:02:34.567890Z",
  "runtime_sec": 3600.0,
  "status": "success",
  
  "raw_coverage": {
    "venues": ["BINANCE_SPOT", "BINANCE_USDTF"],
    "symbol_count": 2500
  },
  
  "replay_build": {
    "status": "success",
    "symbols_processed": 2500,
    "symbols_total": 2500,
    "depth_records": 216000000,
    "trade_records": 112300000
  },
  
  "feature_build": {
    "status": "success",
    "symbols_processed": 2500,
    "symbols_total": 2500,
    "feature_records": 18720000
  },
  
  "errors": []
}
```

## Processing Steps

### 1. Raw Data Manifest Scan

**Time**: ~5 seconds

```python
from pipeline.raw_manifest import scan_raw_coverage

coverage = scan_raw_coverage("2026-06-15", DATA_ROOT)
# Returns: {"venues": [...], "symbol_count": N, "data": {...}}
```

Scans raw directory structure:
- `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)`
- Builds availability map for all symbols/channels

**Possible issues**:
- No raw data for date → symbols_total = 0 (skip further steps)
- Partial venues → report with available venues

### 2. Build Replay Store

**Time**: ~45-120 minutes (depending on raw size and disk I/O)

```bash
python -m pipeline.build_replay_store --date 2026-06-15
```

Per symbol:
1. Stream raw JSONL.zst
2. Accumulate one symbol/date of records for deterministic sorting
3. Deterministically sort by (session_id, session_seq, raw_index)
4. Write Parquet with nested bids/asks structs
5. Compute SHA256 checksum
6. Atomic move from staging → published

**Output structure**:
```
replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/
├── depth.parquet
├── trades.parquet
├── instrument.json
└── manifest.json
```

**Possible issues**:
- Raw file corrupt → skip symbol, continue others (per-symbol isolation)
- Out of disk space → all symbols fail
- Schema mismatch → error logged, retry recommended

### 3. Build Feature Store

**Time**: ~30-60 minutes (depends on replay size and feature calculations)

```bash
python -m pipeline.build_feature_store --date 2026-06-15
```

Per symbol/timeframe:
1. Load one symbol/date of replay_store data (trades + depths) into memory in v0
2. Clamp records to the requested UTC day
3. Aggregate into sparse time windows (100ms, 1s, 1m); empty windows are skipped
4. Calculate core features per window
5. Write Parquet with Hive-style partitioning
6. Atomic move from staging → published

**Output structure**:
```
feature_store/
├── timeframe=100ms/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15.parquet
├── timeframe=1s/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15.parquet
└── timeframe=1m/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15.parquet
```

**Possible issues**:
- Missing replay_store data → skip symbol
- Malformed replay records → skip record, continue
- Quality flags triggered → logged in feature rows
- Large symbol/day memory use → benchmark RSS before broad production runs

Audit feature output:

```bash
python -m pipeline.audit_feature_store \
  --feature-root /tmp/test_features \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --timeframes 1m,1s,100ms
```

### 4. Generate Daily Report

**Time**: <1 second

Aggregates results from previous steps into `daily_build_{date}.json`:

```json
{
  "date": "2026-06-15",
  "status": "success|partial|failed",
  "runtime_sec": 3600.0,
  "raw_coverage": {...},
  "replay_build": {...},
  "feature_build": {...},
  "errors": [...]
}
```

## Local Testing Workflow (temp-root smoke)

Use this workflow to validate the current replay/feature path without touching production roots.

### 1. Prepare test data

```bash
# Use an existing raw data root, or point --data-root at a copied/symlinked fixture.
BASE=/tmp/cryptorecorder-replay-feature-validation
rm -rf "$BASE"
mkdir -p "$BASE"
```

### 2. Build replay_store for one tested day

```bash
python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root "$BASE/replay_store"
```

### 3. Build feature_store

```bash
python -m pipeline.build_feature_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --timeframes 1m \
  --replay-root "$BASE/replay_store" \
  --feature-root "$BASE/feature_store"
```

### 4. Generate a trades-only Nautilus catalog

```bash
python -m pipeline.generate_catalog \
  --input "$BASE/replay_store" \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --start 2026-06-12T00:00:00Z \
  --end 2026-06-13T00:00:00Z \
  --output "$BASE/catalog_jobs" \
  --profile trades_only
```

### 5. Run the daily orchestrator with temp roots

```bash
python -m pipeline.daily_build \
  --date 2026-06-12 \
  --steps replay,features \
  --symbols ADAUSDT \
  --timeframes 1m \
  --data-root ./data_raw \
  --replay-root "$BASE/daily_replay_store" \
  --feature-root "$BASE/daily_feature_store" \
  --report-root "$BASE/daily_reports"
```

### 6. Validate outputs

Check:
- replay partitions exist under `venue=.../symbol=.../date=2026-06-12`;
- `manifest.json` counts match Parquet metadata;
- SHA256 checksums match the published files;
- rows are sorted by the composite replay sort keys;
- feature Parquet files exist for the requested timeframe;
- the generated catalog opens with Nautilus `ParquetDataCatalog`.

### 7. Semantic equivalence test

After the smoke test, compare against old `convert_day.py` for the same date/symbol set:

```bash
python convert_day.py --date 2026-06-12 --symbols ADAUSDT --staging
```

Compare instruments, row counts, timestamp min/max, and bounded sample readability. Full depth/full-L2 semantic equivalence is still pending because `generate_catalog` currently implements `trades_only`.

The reusable validator automates the trades-only comparison:

```bash
python -m pipeline.validate_catalog_equivalence \
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

### 8. Local 3-day validation recipe

Use this only after the one-day smoke passes. Replace the dates if your local raw fixture uses different UTC days.

```bash
BASE=/tmp/cryptorecorder-3day-validation
rm -rf "$BASE"
mkdir -p "$BASE"

for date in 2026-06-12 2026-06-13 2026-06-14; do
  python -m pipeline.build_replay_store \
    --date "$date" \
    --symbols ADAUSDT \
    --data-root ./data_raw \
    --replay-root "$BASE/replay_store"

  python -m pipeline.build_feature_store \
    --date "$date" \
    --symbols ADAUSDT \
    --timeframes 1m \
    --replay-root "$BASE/replay_store" \
    --feature-root "$BASE/feature_store"
done

python -m pipeline.generate_catalog \
  --input "$BASE/replay_store" \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --start 2026-06-12T00:00:00Z \
  --end 2026-06-15T00:00:00Z \
  --output "$BASE/catalog_jobs" \
  --profile trades_only \
  --job-id validation_3day \
  --overwrite
```

Run old-vs-new trades-only equivalence one day at a time:

```bash
for date in 2026-06-12 2026-06-13 2026-06-14; do
  python -m pipeline.validate_catalog_equivalence \
    --date "$date" \
    --symbols ADAUSDT \
    --venues BINANCE_SPOT \
    --data-root ./data_raw \
    --work-root "$BASE/equivalence_$date" \
    --old-catalog-root "$BASE/equivalence_$date/old_catalog" \
    --replay-root "$BASE/equivalence_$date/replay_store" \
    --new-catalog-root "$BASE/equivalence_$date/new_catalog" \
    --profile trades_only \
    --overwrite
done
```

## Troubleshooting

### Issue: "No symbols found for date"

**Symptom**: 
```
INFO: Date range: 2026-06-15 to 2026-06-15 (1 days)
ERROR: No symbols available for 2026-06-15
```

**Cause**: Raw data missing or path misconfigured

**Solution**:
```bash
# Check raw data exists
ls -la /path/to/data_raw/BINANCE_SPOT/depth_v2/
ls -la /path/to/data_raw/BINANCE_SPOT/trade_v2/

# Verify dates have data
find /path/to/data_raw -name "*2026-06-15*" -type f
```

### Issue: "Out of disk space during replay build"

**Symptom**:
```
ERROR: Failed to write parquet file: No space left on device
```

**Cause**: Disk full or insufficient free space for staging

**Solution**:
```bash
# Check available space
df -h /path/to/replay_store

# Delete old staging directories if any
rm -rf /path/to/replay_store/.staging_*

# Free up space by archiving old replay_store dates
```

### Issue: "Deterministic sort mismatch"

**Symptom**: Different output files on repeated runs of same date

**Cause**: Session ID or raw index calculation inconsistency

**Solution**:
```bash
# Compare checksums in manifests
cat /path/to/replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/manifest.json | jq '{depth_checksum,trades_checksum}'

# If different, investigate:
# 1. Raw file changed?
# 2. Time settings (for session_id)?
# 3. Corrupted replay from previous run?

# Rebuild from scratch
rm -rf /path/to/replay_store/venue=BINANCE_SPOT/symbol=BTCUSDT/date=2026-06-15/
python -m pipeline.build_replay_store --date 2026-06-15
```

### Issue: Feature calculation is slow

**Symptom**:
```
INFO: Building feature_store for 2026-06-15...
# ... stuck for >30 minutes with no output ...
```

**Cause**: Large replay_store file (>1GB per symbol) or slow disk I/O

**Solution**:
```bash
# Monitor progress
watch -n 5 'ls -lah /tmp/test_features/.staging_*/venue=BINANCE_SPOT/symbol=BTCUSDT/'

# If stuck, check for I/O bottleneck
iotop -o  # (if available)

# Increase batch size in feature_calc.py
# (modify BATCH_SIZE constant, default 5000)

# Or limit to fewer symbols
python -m pipeline.build_feature_store --date 2026-06-15 --symbols BTCUSDT
```

## Systemd Integration

### View service status

```bash
systemctl status cryptorecorder-daily-build.service
systemctl status cryptorecorder-daily-build.timer
```

### View recent runs

```bash
journalctl -u cryptorecorder-daily-build.service -n 100
```

### View full log for latest run

```bash
journalctl -u cryptorecorder-daily-build.service --since "2 hours ago" -f
```

### Manually trigger service (for testing)

```bash
systemctl start cryptorecorder-daily-build.service
journalctl -u cryptorecorder-daily-build.service -f
```

### Check timer schedule

```bash
systemctl list-timers cryptorecorder-daily-build.timer
```

### Edit environment variables

```bash
sudo nano /etc/cryptorecorder/cryptorecorder.env
# Then reload:
sudo systemctl daemon-reload
sudo systemctl restart cryptorecorder-daily-build.timer
```

## See Also

- [STORAGE_ARCHITECTURE.md](STORAGE_ARCHITECTURE.md) — Data pipeline overview
- [REPLAY_STORE.md](REPLAY_STORE.md) — Replay store schema
- [FEATURE_STORE.md](FEATURE_STORE.md) — Feature calculations
- [GENERATE_CATALOG.md](GENERATE_CATALOG.md) — On-demand catalog generation
- [OPERATIONS.md](OPERATIONS.md) — General system operations
