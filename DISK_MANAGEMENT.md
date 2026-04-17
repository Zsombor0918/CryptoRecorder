# Disk Management System

This document explains the disk usage monitoring, retention policy, and cleanup mechanisms for CryptoRecorder.

## Overview

The disk management system ensures reliable 24/7 operation by:

1. **Monitoring**: `disk_monitor.py` - Tracks disk usage and growth metrics
2. **Planning**: `disk_plan.py` - Generates data retention recommendations
3. **Enforcement**: `recorder.py` - Implements cleanup when thresholds are exceeded

## Architecture

### Three-Layer Disk Thresholds

```
┌─────────────────────────────────────────────────────────┐
│                  500GB Total Capacity                   │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  SOFT LIMIT: 400GB (80%)     ← Warning triggered       │
│  ├─ Action: Begin cleanup to 350GB target             │
│  ├─ Speed: Gradual (remove oldest data)               │
│  └─ User: Can interact normally                       │
│                                                       │
│  CLEANUP TARGET: 350GB (70%)  ← Safe zone              │
│  ├─ Action: None (recovery point)                    │
│  ├─ Retention: ~30 days of raw data                  │
│  └─ Growth buffer: 4× weekly growth                  │
│                                                       │
│  HARD LIMIT: 475GB (95%)      ← Emergency              │
│  ├─ Action: Aggressive cleanup needed                │
│  ├─ Speed: Fast                                       │
│  └─ User: May experience delays                      │
│                                                       │
└─────────────────────────────────────────────────────────┘
```

### Disk Usage Breakdown

Typical disk usage composition:

```
Raw Data: 80% of capacity  (400GB at cleanup target)
  └─ L2 updates: ~60%  (spot + futures)
  └─ Trades:     ~15%
  └─ Snapshots:  ~5%

Catalog Data: 20% of capacity  (100GB at cleanup target)
  └─ ParquetDataCatalog for backtesting
  └─ NOT cleaned up during retention (kept indefinitely)
  └─ Can be rebuild from raw data if needed
```

## Configuration

Set these in `config.py`:

```python
# Disk Thresholds (GB)
DISK_SOFT_LIMIT_GB = 400        # Warn at this level
DISK_HARD_LIMIT_GB = 475        # Emergency cleanup
DISK_CLEANUP_TARGET_GB = 350    # Target after cleanup
RAW_RETENTION_DAYS = 30         # Days of raw data to keep

# Monitoring
DISK_CHECK_INTERVAL_SEC = 300   # Check every 5 minutes
```

### How to Generate Recommendations

Use `disk_plan.py` to generate data-driven recommendations:

```bash
# Generate plan based on measured growth (reads state/disk_usage.json)
python disk_plan.py

# Override with specific metrics
python disk_plan.py --measured 12.5  # 12.5 GB/day growth

# Analyze different capacity scenario
python disk_plan.py --capacity 1000  # 1TB capacity
```

Output: `state/disk_plan_report.json` with detailed analysis and recommendations.

## Monitoring

### disk_monitor.py

Records disk usage snapshots and growth metrics.

**Main functions:**

- `DiskMonitor.record_snapshot()` - Capture usage at a point in time
- `DiskMonitor.get_disk_usage_gb()` - Get current disk usage in GB
- `DiskMonitor.calculate_growth_rate()` - Compute daily growth (GB/day)
- `DiskMonitor.shutdown()` - Cleanup and final snapshot

**Output files:**

```
state/
├─ disk_usage.json          # Latest snapshot + growth metrics
├─ disk_usage_history.jsonl # Appended snapshots (for analysis)
└─ disk_monitor.log         # Monitor's debug logs
```

### Metrics Recorded

Each snapshot captures:

```json
{
  "timestamp": "2024-01-15T23:45:30.123456Z",
  "disk_usage_gb": 365.2,
  "disk_used_percent": 73.0,
  "growth_rate_gb_day": 12.4,
  "retention_days": 29.4,
  "notes": "Normal operation"
}
```

**Interpretation:**

- `growth_rate_gb_day`: Average daily growth (updated with exponential smoothing)
- `retention_days`: Estimated days of raw data kept at current usage
- `disk_used_percent`: Usage relative to 500GB

## Retention Policy

### How Cleanup Works

1. **Trigger**: Usage exceeds `DISK_SOFT_LIMIT_GB` (400GB)
2. **Target**: Delete oldest raw data until usage drops below `DISK_CLEANUP_TARGET_GB` (350GB)
3. **Skip**: Catalog data is NOT deleted (needed for backtesting)
4. **Speed**: Removes entire date directories (slow to prevent I/O overload)

### Raw Data Retention

Keeps raw data (L2 updates, trades, snapshots) for analysis:

- **Current setting**: 30 days of raw data
- **Based on**: 12.5 GB/day growth rate
- **Capacity needed**: 375 GB at 70% utilization

### Catalog Retention

ParquetDataCatalog is kept indefinitely because:

1. Much smaller than raw data (20% of capacity)
2. Essential for backtesting workflow
3. Can be rebuilt from raw if needed
4. User has explicit control over deletion

## Usage Examples

### Monitor Growth Rate

```bash
# Start recorder normally (records metrics every 5 minutes)
python recorder.py

# After 24 hours, generate recommendation
python disk_plan.py
# Shows measured growth, retention days, recommended settings
```

### Prepare Disk Usage Report

```bash
# Generate detailed report
python disk_plan.py > disk_report.txt

# Or create JSON for programmatic use
python disk_plan.py  # Creates state/disk_plan_report.json
```

### Check Current Usage

```python
from disk_monitor import DiskMonitor
import asyncio

async def check():
    monitor = DiskMonitor()
    usage = await monitor.get_disk_usage_gb()
    print(f"Current usage: {usage:.1f}GB")
    
    history = await monitor.get_usage_history()
    print(f"Last 7 days: {len(history)} samples")
    print(f"Growth rate: {history[-1].get('growth_rate_gb_day'):.1f} GB/day")

asyncio.run(check())
```

## Cleanup Scenarios

### Scenario 1: Normal Operations

```
Time: 2024-01-15 10:00 UTC
Disk: 365GB (73%)
Action: None (below soft limit)
Status: ✓ OK
```

### Scenario 2: Approaching Limit

```
Time: 2024-01-15 20:00 UTC  
Disk: 405GB (81%)  
Soft Limit: 400GB
Action: Begin cleanup to 350GB target
Status: ⚠️ WARNING - Cleanup in progress
```

### Scenario 3: Emergency Cleanup

```
Time: 2024-01-15 23:50 UTC
Disk: 480GB (96%)
Hard Limit: 475GB
Action: Aggressive cleanup (possible delays)
Status: 🔴 CRITICAL - Emergency cleanup
```

## Troubleshooting

### High Growth Rate

If `disk_plan.py` shows unexpectedly high growth:

1. Check for unusual symbols being recorded
2. Verify L2 update frequency hasn't increased
3. Look for duplicate events in storage

```bash
# Find largest date directories
du -h state/ | sort -rh | head -10
```

### Cleanup Not Happening

If disk usage exceeds soft limit but cleanup doesn't start:

1. Check `state/disk_monitor.log` for errors
2. Verify `DISK_CHECK_INTERVAL_SEC` setting (default: 300s)
3. Ensure `DATA_ROOT` path permissions are correct

```bash
# Manual check
python -c "
from disk_monitor import DiskMonitor
import asyncio
async def check():
    m = DiskMonitor()
    print(f'Usage: {await m.get_disk_usage_gb():.1f}GB')
    await m.record_snapshot()
asyncio.run(check())
"
```

### Understanding Retention Days

Retention days indicates how far back raw data extends:

- **High retention** (>40 days): Low growth rate, ample capacity
- **Normal** (25-35 days): Designed operating point (30 days)
- **Low** (<20 days): High growth, approaching cleanup needs

Adjust `RAW_RETENTION_DAYS` in config.py to control this.

## Integration

### With recorder.py

Disk monitor runs automatically as background task:

```python
# In recorder.py startup:
background_tasks.append(asyncio.create_task(DiskManager.disk_check_task()))

# Periodic checks every DISK_CHECK_INTERVAL_SEC
# Records metrics and triggers cleanup if needed
```

### With External Monitoring

Example for Prometheus/monitoring:

```bash
# Read latest metrics
cat state/disk_usage.json | \
  python -c "import sys, json; d=json.load(sys.stdin); print(f'disk_gb {d[\"disk_usage_gb\"]}')"

# Parse growth rate
python -c "
import json
with open('state/disk_usage.json') as f:
    print(json.load(f)['growth_rate_gb_day'])
"
```

## Performance Impact

### Minimal Overhead

- **CPU**: <0.1% (just disk reads/writes)
- **Memory**: ~10MB per 1000 snapshots kept
- **I/O**: One `du` command every 5 minutes
- **Disk**: ~500KB for metrics history

### Cleanup Impact

- **Speed**: ~10-30 seconds per cleanup cycle
- **I/O**: Mostly disk reads (deletion is fast)
- **Recorder**: No resyncing needed (cleanup removes complete date dirs)

## Best Practices

1. **Monitor Growth**: Run `disk_plan.py` weekly
2. **Adjust Settings**: Update `RAW_RETENTION_DAYS` based on actual growth
3. **Test Cleanup**: Verify cleanup works on low-usage days
4. **Export Data**: Archive important catalogs before retention window closes
5. **Plan Storage**: Use SSD for raw data (better random I/O for L2 reads)

## Future Enhancements

Possible improvements:

- [ ] Compress old raw data (tarz with gz)
- [ ] Archive to cloud storage (S3)
- [ ] Per-symbol retention settings
- [ ] Diskless mode (cloud storage only)
- [ ] Catalog migration to cold storage
- [ ] Automatic cap at hard limit (prevent accidental disk full)

## References

- `config.py` - Disk settings and thresholds
- `disk_monitor.py` - Monitoring implementation
- `disk_plan.py` - Planning tool  
- `recorder.py` - Integration entry point
- `QUICKSTART.md` - Quick setup guide
