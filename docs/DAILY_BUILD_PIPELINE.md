# Daily Build Pipeline Operations

## Overview

The daily build pipeline orchestrates the conversion of raw market data into
`replay_store`. It's designed to run once per day via systemd timer and
supports flexible on-demand execution with custom parameters. It is
replay-only: CryptoRecorder does not build a feature-store or label-store
(removed, issue #17); `replay_store` is the stable external contract handed
off to downstream repositories (e.g. KovacsTrader).

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
python -m pipeline.daily_build --date 2026-06-15 --symbols BTCUSDT,ETHUSDT
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
--symbols SYMBOLS
    Comma-separated symbols to process (default: all from raw data)
    
    Examples:
    - --symbols BTCUSDT
    - --symbols BTCUSDT,ETHUSDT,BNBUSDT

--data-root PATH
    Raw data root (default: from config.DATA_ROOT)
    
    Use this to override config for testing

--replay-root PATH
    Replay store root (default: from config.REPLAY_ROOT)

--report-root PATH
    Report output root (default: from config.DAILY_REPORT_ROOT)
```

There is no `--steps`, `--timeframes`, or `--feature-root` flag. `daily_build`
always scans raw coverage and builds the replay store; there is no feature
step to select.

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
  --data-root /tmp/test_raw
```

### Testing: Custom storage paths

```bash
python -m pipeline.daily_build \
  --date 2026-06-15 \
  --data-root /tmp/test_raw \
  --replay-root /tmp/test_replay \
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

### 3. Generate Daily Report

**Time**: <1 second

Aggregates results from the previous steps into `daily_build_{date}.json`:

```json
{
  "date": "2026-06-15",
  "status": "success|partial|failed",
  "runtime_sec": 3600.0,
  "raw_coverage": {...},
  "replay_build": {...},
  "errors": [...]
}
```

## Local Testing Workflow (temp-root smoke)

Use this workflow to validate the current replay path without touching production roots.

### 1. Prepare test data

```bash
# Use an existing raw data root, or point --data-root at a copied/symlinked fixture.
BASE=/tmp/cryptorecorder-replay-validation
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

### 3. Run the daily orchestrator with temp roots

```bash
python -m pipeline.daily_build \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root "$BASE/daily_replay_store" \
  --report-root "$BASE/daily_reports"
```

### 4. Validate outputs

Check:
- replay partitions exist under `venue=.../symbol=.../date=2026-06-12`;
- `manifest.json` counts match Parquet metadata;
- SHA256 checksums match the published files;
- rows are sorted by the composite replay sort keys.

### 5. Semantic equivalence check

Compare against old `convert_day.py` for the same date/symbol set. There is no
`generate_catalog` product CLI; the reusable validator drives the internal
`validation.replay_catalog_reconstruct` helper to rebuild a temporary catalog
from `replay_store` and compares it directly:

```bash
python convert_day.py --date 2026-06-12 --symbols ADAUSDT --staging

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

Use `--profile full_l2` for the full order-book comparison (validated on the
ADAUSDT single-day smoke; broader top50/multi-day validation is pending — see
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md)).

### 6. Local 3-day validation recipe

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
done
```

Run old-vs-new trades-only equivalence one day at a time:

```bash
for date in 2026-06-12 2026-06-13 2026-06-14; do
  python -m validation.validate_catalog_equivalence \
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

- [ARCHITECTURE.md](ARCHITECTURE.md) — Data pipeline overview
- [REPLAY_STORE.md](REPLAY_STORE.md) — Replay store schema
- [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md) — Validation-only full-L2 reconstruction plan
- [OPERATIONS.md](OPERATIONS.md) — General system operations

