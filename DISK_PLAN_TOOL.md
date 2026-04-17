# Disk Plan Tool Reference

Tool for generating data-driven disk retention policy recommendations.

## Overview

`disk_plan.py` analyzes measured disk growth rates and generates recommended configuration settings for optimal storage utilization.

## Installation

No additional dependencies required. Uses built-in modules and DiskMonitor from project.

## Usage

### Basic: Generate Plan from Measured Data

```bash
cd CryptoRecorder
python disk_plan.py
```

Reads measured growth from `state/disk_usage.json` and generates recommendations.

**Output:**
```
======================================================================
DISK RETENTION PLAN
======================================================================

Capacity: 500GB
Measured growth: 12.40 GB/day
               86.80 GB/week
              372.00 GB/month

----------------------------------------------------------------------
RECOMMENDED CONFIG.PY SETTINGS:
----------------------------------------------------------------------

DISK_SOFT_LIMIT_GB             = 400
DISK_HARD_LIMIT_GB             = 475
DISK_CLEANUP_TARGET_GB         = 350
RAW_RETENTION_DAYS             = 28

----------------------------------------------------------------------
UTILIZATION POINTS:
----------------------------------------------------------------------

AT_SOFT_LIMIT:
  Used: 400GB (80%)
  Action: Begin cleanup to cleanup_target

AT_CLEANUP_TARGET:
  Used: 350GB (70%)
  Action: Safe operating point

AT_HARD_LIMIT:
  Used: 475GB (95%)
  Action: Critical - emergency cleanup

----------------------------------------------------------------------
RETENTION ANALYSIS:
----------------------------------------------------------------------

Raw data daily growth: 9.92GB
Catalog daily growth: 2.48GB
At cleanup target (350GB):
  - 28 days of raw data retained
  - 277.76GB raw data

----------------------------------------------------------------------
RECOMMENDATIONS:
----------------------------------------------------------------------

1. Use measured growth rate of 12.4 GB/day
2. Set soft limit to 400GB (80% of 500GB)
3. Cleanup to 350GB target (70% capacity)
4. Retain 28 days of raw data
5. Expected monthly growth: 372GB
6. Expected yearly growth: 4527GB
7. Monitor disk_usage.json for actual growth trends
8. Adjust retention settings if actual growth differs from measured
```

Also saves to `state/disk_plan_report.json` for programmatic use.

### Override Measured Growth

```bash
# Specify different growth rate (GB/day)
python disk_plan.py --measured 15.0
```

Useful when:
- Recent growth has changed significantly
- Testing different scenarios
- No historical data available yet

### Different Disk Capacity

```bash
# Plan for different capacity
python disk_plan.py --capacity 1000  # Plan for 1TB
python disk_plan.py --capacity 250   # Plan for 250GB
```

Adjusts all thresholds proportionally.

### Full Example Workflow

```bash
# 1. Let recorder run for a week to collect data
python recorder.py &
sleep 604800  # 7 days

# 2. Generate plan based on actual growth
python disk_plan.py

# 3. Review recommendations
cat state/disk_plan_report.json | python -m json.tool

# 4. Update config.py with recommended values
# (manually copy-paste RECOMMENDED CONFIG.PY SETTINGS)

# 5. Restart recorder with new settings
kill %1
python recorder.py
```

## Output Files

### Text Output

Printed to stdout (can redirect to file):

```bash
python disk_plan.py > disk_plan.txt
```

### JSON Report

Machine-readable report saved to `state/disk_plan_report.json`:

```bash
python disk_plan.py  # Generates report
cat state/disk_plan_report.json
```

**Report structure:**

```json
{
  "timestamp": "2024-01-15T23:45:30.123456Z",
  "capacity_gb": 500,
  "measured_growth": {
    "daily_gb": 12.4,
    "weekly_gb": 86.8,
    "monthly_gb": 372.0,
    "yearly_gb": 4527.0
  },
  "buffers_and_safety": {
    "weekly_growth_multiplier": 4,
    "buffer_gb": 347.2,
    "recommended_min_free_gb": 520.8
  },
  "recommended_config": {
    "DISK_SOFT_LIMIT_GB": 400,
    "DISK_HARD_LIMIT_GB": 475,
    "DISK_CLEANUP_TARGET_GB": 350,
    "RAW_RETENTION_DAYS": 28
  },
  "utilization_scenarios": {
    "at_soft_limit": {
      "capacity_used_gb": 400,
      "capacity_used_percent": 80,
      "action": "Begin cleanup to cleanup_target"
    },
    ...
  },
  "retention_analysis": {
    "raw_data_daily_gb": 9.92,
    "catalog_daily_gb": 2.48,
    "retention_at_cleanup_target": {
      "days": 28,
      "raw_data_gb_retained": 277.76,
      "catalog_retained": "Kept for ~90 days (not cleaned)"
    }
  },
  "recommendations": [
    "Use measured growth rate of 12.4 GB/day",
    ...
  ]
}
```

## Interpretation

### What the Numbers Mean

**Growth Rate:**
- `daily_gb`: Raw data added per day (L2 updates, trades, snapshots)
- `weekly_gb`: 7× daily (for trend analysis)
- `monthly_gb`: 30× daily (for long-term planning)
- `yearly_gb`: 365× daily (disk capacity planning)

**Thresholds:**
- `DISK_SOFT_LIMIT_GB`: Trigger cleanup warning (80% of capacity)
- `DISK_HARD_LIMIT_GB`: Emergency threshold (95%, should rarely reached)
- `DISK_CLEANUP_TARGET_GB`: Target after cleanup (70% of capacity)
- `RAW_RETENTION_DAYS`: Keep N days of raw data

**Safety Buffer:**
- `weekly_growth_multiplier`: Cleanup targets 4× weekly growth above cleanup target
- `recommended_min_free_gb`: Minimum recommended free space = buffer × 1.5
- Ensures cleanup doesn't thrash on growth spikes

### Example Interpretation

```
Measured growth: 12.40 GB/day
Weekly growth: 86.80 GB/week
Safety buffer: 347.2 GB (4 weeks of growth)

At cleanup target (350GB):
- Free space: 150GB (30% of 500GB)
- Raw data retained: ~28 days
- Growth buffer: 350 - 150 = 200GB < 347GB buffer
→ Room for growth spikes before cleanup needed again
```

## Decision Tree

### Do I need to change settings?

1. **Check current retention**: Run `python disk_plan.py`
   - If retention >= 30 days: OK as-is
   - If retention < 20 days: Consider larger capacity or higher growth rate
   - If retention > 60 days: Can reduce retention or increase limits

2. **Check cleanup frequency**: Monitor logs
   - If cleaning every day: Limits too tight, consider increasing capacity
   - If cleaning weekly: Normal with planned cleanup
   - If cleaning monthly: Excellent, plenty of headroom

3. **Check growth trend**: Compare weekly plans
   - If growth increasing: May need to tighten retention soon
   - If growth stable: Current settings are optimal
   - If growth decreasing: Can increase retention window

### Scenario: Growth Rate Increased

```bash
# Old plan: 10 GB/day, 34 days retention
python disk_plan.py  # Now shows 15 GB/day, 23 days retention
```

**Actions:**
1. Review why growth increased (added symbols? higher frequency?)
2. If expected: Update config and monitor closely
3. If unexpected: Investigate (may be data quality issue)
4. Consider larger disk if trend continues

### Scenario: Disk Getting Full

```bash
# Storage at 90% utilization despite cleanup running
```

**Diagnosis:**
1. Check measured growth rate: `python disk_plan.py`
2. Review cleanup logs for errors
3. Verify cleanup target < soft limit
4. Consider:
   - Reducing retention window
   - Increasing disk capacity
   - Archiving catalog to external storage

### Scenario: Plenty of Free Space

```bash
# Disk at 50% utilization, but cleanup running daily
```

**Adjustment:**
1. Get current plan: `python disk_plan.py`
2. If retention < 45 days: Increase `RAW_RETENTION_DAYS` in config.py
3. If utilization < 60%: Increase `DISK_SOFT_LIMIT_GB` (e.g., 400 → 450)
4. Re-run plan to verify recommended settings

## Advanced Use

### Integration with External Monitoring

```python
import json
import subprocess

def get_retention_days():
    result = subprocess.run(
        ['python', 'disk_plan.py'],
        capture_output=True,
        text=True
    )
    
    # Parse JSON report
    report = json.load(open('state/disk_plan_report.json'))
    return report['retention_analysis']['retention_at_cleanup_target']['days']

# Alert if retention drops below threshold
retention = get_retention_days()
if retention < 21:
    alert_ops("Low data retention: {} days".format(retention))
```

### Automated Config Update

```bash
#!/bin/bash
# update_disk_config.sh - Auto-update config based on plan

python disk_plan.py

# Extract recommended values
SOFT=$(python -c "import json; print(json.load(open('state/disk_plan_report.json'))['recommended_config']['DISK_SOFT_LIMIT_GB'])")
HARD=$(python -c "import json; print(json.load(open('state/disk_plan_report.json'))['recommended_config']['DISK_HARD_LIMIT_GB'])")
TARGET=$(python -c "import json; print(json.load(open('state/disk_plan_report.json'))['recommended_config']['DISK_CLEANUP_TARGET_GB'])")
DAYS=$(python -c "import json; print(json.load(open('state/disk_plan_report.json'))['recommended_config']['RAW_RETENTION_DAYS'])")

# Update config.py (requires careful implementation)
echo "Recommended limits:"
echo "DISK_SOFT_LIMIT_GB = $SOFT"
echo "DISK_HARD_LIMIT_GB = $HARD"
echo "DISK_CLEANUP_TARGET_GB = $TARGET"
echo "RAW_RETENTION_DAYS = $DAYS"
```

### Trend Analysis

```python
import json
import subprocess
from datetime import datetime

# Run plan daily and log results
results = []

for day in range(30):
    subprocess.run(['python', 'disk_plan.py'])
    report = json.load(open('state/disk_plan_report.json'))
    
    results.append({
        'date': datetime.now().isoformat(),
        'growth_rate': report['measured_growth']['daily_gb'],
        'retention_days': report['retention_analysis']['retention_at_cleanup_target']['days']
    })

# Analyze trends
growths = [r['growth_rate'] for r in results]
print(f"Growth trend: {growths[0]} → {growths[-1]} GB/day")

retentions = [r['retention_days'] for r in results]
print(f"Retention trend: {retentions[0]} → {retentions[-1]} days")
```

## Troubleshooting

### "No usage history" Warning

```
WARNING: No usage history at state/disk_usage.json
Using default estimate: 10.0 GB/day
```

**Solution:**
- Must run recorder for at least 12 hours to collect data
- See metrics at: `state/disk_usage.json`

### "Growth rate is unrealistic"

```
Measured growth: 50.0 GB/day
(seems too high)

Recommendations:
1. Verify no archive operation is happening
2. Check if symbols were just added to universe
3. Review L2 update frequency settings
```

### Retention Days Seems Wrong

```python
# Manual verification
from disk_monitor import DiskMonitor
import asyncio

async def verify():
    monitor = DiskMonitor()
    usage_gb = await monitor.get_disk_usage_gb()
    raw_data_gb = usage_gb * 0.80
    growth_rate = await monitor.calculate_growth_rate(days=7)
    
    retention_days = raw_data_gb / growth_rate
    print(f"Calculated: {retention_days:.1f} days")
    
    report = json.load(open('state/disk_plan_report.json'))
    print(f"Reported: {report['retention_analysis']['retention_at_cleanup_target']['days']} days")

asyncio.run(verify())
```

## Performance

- **Typical runtime**: 1-5 seconds
- **Disk I/O**: Minimal (reads config files)
- **CPU**: <1% (just calculations)
- **Output size**: 
  - Text: 2-3 KB
  - JSON: 3-5 KB

## API for Programmatic Use

### Import and Use

```python
from disk_plan import DiskPlanner

planner = DiskPlanner(capacity_gb=500, analysis_days=30)

# Generate plan
plan = planner.generate_plan(measured_daily_gb=12.4)

# Validate
if planner.validate_plan(plan):
    # Extract settings
    settings = plan['recommended_config']
    print(f"DISK_SOFT_LIMIT_GB = {settings['DISK_SOFT_LIMIT_GB']}")
```

### Result Structure

```python
plan = {
    'timestamp': '2024-01-15T...',
    'capacity_gb': 500,
    'measured_growth': {...},
    'recommended_config': {
        'DISK_SOFT_LIMIT_GB': 400,
        'DISK_HARD_LIMIT_GB': 475,
        'DISK_CLEANUP_TARGET_GB': 350,
        'RAW_RETENTION_DAYS': 28,
    },
    'recommendations': [...]
}
```

## See Also

- [DISK_MANAGEMENT.md](DISK_MANAGEMENT.md) - System overview
- [DISK_MONITOR_API.md](DISK_MONITOR_API.md) - Monitor API
- [config.py](config.py) - Configuration settings
- [disk_monitor.py](disk_monitor.py) - Monitor implementation
