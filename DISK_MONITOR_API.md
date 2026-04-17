# Disk Monitor API Reference

Complete API documentation for disk monitoring components.

## DiskMonitor Class

Main monitoring class for tracking disk usage and computing growth metrics.

### Constructor

```python
monitor = DiskMonitor()
```

**Parameters:**
- None (uses config.DATA_ROOT and state/ directory)

**Attributes:**
- `state_root: Path` - Directory for metrics storage
- `usage_log: Path` - Current usage snapshot file
- `history_file: Path` - Appended history file
- `history_limit: int` - Max snapshots to keep in memory (default: 1000)

### Methods

#### record_snapshot()

Record a disk usage snapshot.

```python
await monitor.record_snapshot()
```

**Purpose:**
- Captures current disk usage
- Computes growth rate (exponential smoothing)
- Stores in `state/disk_usage.json`
- Appends to history

**Returns:** `None`

**Raises:**
- `OSError` - If disk_usage command fails
- `IOError` - If state directory can't be created

**Example:**

```python
import asyncio
from disk_monitor import DiskMonitor

async def capture():
    monitor = DiskMonitor()
    
    # Record snapshot every 5 minutes
    for i in range(288):  # 24 hours
        await monitor.record_snapshot()
        await asyncio.sleep(300)

asyncio.run(capture())
```

#### get_disk_usage_gb()

Get current disk usage in GB.

```python
usage_gb = await monitor.get_disk_usage_gb()
```

**Parameters:** None

**Returns:** 
- `float` - Usage in GB (e.g., 365.2)

**Raises:**
- `OSError` - If `du` command fails

**Performance:**
- Speed: ~100-500ms (depends on disk size)
- Can call frequently without issue

**Example:**

```python
usage = await monitor.get_disk_usage_gb()
if usage > 400:
    print(f"Usage high: {usage:.1f}GB")
```

#### calculate_growth_rate(days=7)

Calculate average daily growth rate.

```python
rate_gb_day = await monitor.calculate_growth_rate(days=7)
```

**Parameters:**
- `days: int` - Number of days to analyze (default: 7)

**Returns:**
- `float` - Growth rate in GB/day

**Notes:**
- Uses exponential smoothing (α=0.3)
- Less volatile than simple mean
- Returns 0 if insufficient history

**Example:**

```python
rate = await monitor.calculate_growth_rate(days=14)
print(f"2-week growth: {rate:.2f} GB/day")
```

#### load_history()

Load usage history from file.

```python
history = await monitor.load_history()
```

**Returns:**
- `list[dict]` - List of snapshots with structure:
  ```python
  {
    "timestamp": "2024-01-15T10:30:45.123456Z",
    "disk_usage_gb": 365.2,
    "disk_used_percent": 73.0,
    "growth_rate_gb_day": 12.4
  }
  ```

**Notes:**
- Caches in memory (up to `history_limit`)
- Initially empty if file doesn't exist
- Automatically grows as snapshots are added

**Example:**

```python
history = await monitor.load_history()
print(f"Total samples: {len(history)}")
if history:
    latest = history[-1]
    print(f"Latest: {latest['disk_usage_gb']:.1f}GB")
    print(f"Growth: {latest['growth_rate_gb_day']:.1f}GB/day")
```

#### get_usage_history(days=7)

Get usage history for specified period.

```python
recent = await monitor.get_usage_history(days=7)
```

**Parameters:**
- `days: int` - Number of days to retrieve (default: 7)

**Returns:**
- `list[dict]` - Filtered snapshots from last N days

**Example:**

```python
week = await monitor.get_usage_history(days=7)
oldest = week[0]['disk_usage_gb']
newest = week[-1]['disk_usage_gb']
total_growth = newest - oldest
print(f"Week growth: {total_growth:.1f}GB")
```

#### estimate_retention_days()

Estimate days of raw data retained.

```python
days = await monitor.estimate_retention_days()
```

**Returns:**
- `float` - Estimated retention in days

**Formula:**
- `(usage_gb * 0.80) / (growth_rate_gb_day)`
- Assumes 80% of disk is raw data
- Uses current growth rate

**Example:**

```python
retention = await monitor.estimate_retention_days()
print(f"Can retain: {retention:.1f} days of raw data")
```

#### get_status()

Get human-readable status.

```python
status = await monitor.get_status()
```

**Returns:**
- `dict` with keys:
  - `utilization: str` - "Low" / "Normal" / "High" / "Critical"
  - `usage_gb: float` - Current usage
  - `usage_percent: float` - Percentage of 500GB
  - `growth_rate: float` - GB/day
  - `retention_days: float` - Estimated days
  - `recommendation: str` - Action to take

**Example:**

```python
status = await monitor.get_status()
print(f"Status: {status['utilization']}")
print(f"Usage: {status['usage_percent']:.0f}%")
print(f"Recommendation: {status['recommendation']}")
```

#### shutdown()

Cleanup and save final snapshot.

```python
await monitor.shutdown()
```

**Purpose:**
- Records final snapshot
- Writes state to file
- Closes any open resources

**Notes:**
- Call during graceful shutdown
- Saves progress even if interrupted

**Example:**

```python
try:
    await run_recorder()
finally:
    await monitor.shutdown()
```

### Context Manager

Use DiskMonitor as context manager:

```python
async with DiskMonitor() as monitor:
    usage = await monitor.get_disk_usage_gb()
    print(f"Usage: {usage:.1f}GB")
# Automatically calls shutdown()
```

## State Files

### disk_usage.json

Latest usage snapshot (updated every check).

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

**Used by:**
- `disk_plan.py` - Reads growth rate
- Dashboard/monitoring - Latest metrics
- Health checks

### disk_usage_history.jsonl

Appended history of all snapshots (one per line).

```
{"timestamp": "...", "disk_usage_gb": 365.2, ...}
{"timestamp": "...", "disk_usage_gb": 365.5, ...}
{"timestamp": "...", "disk_usage_gb": 366.1, ...}
```

**Used by:**
- Growth rate calculation
- Trend analysis
- Retention planning

### disk_monitor.log

Debug logs from monitor.

**Typical entries:**
```
[INFO] Disk usage snapshot recorded: 365.2GB (73.0%)
[INFO] Growth rate updated: 12.4 GB/day
[DEBUG] Exponential smoothing: α=0.3
[WARNING] Usage approaching soft limit: 365GB > 400GB
```

## Error Handling

### Common Errors

#### OSError: du command failed

```python
try:
    usage = await monitor.get_disk_usage_gb()
except OSError as e:
    logger.error(f"Failed to get disk usage: {e}")
    # Fallback to previous snapshot
```

**Causes:**
- Permission denied on DATA_ROOT
- Filesystem failure
- du command not available

**Recovery:**
- Check directory permissions
- Verify disk is healthy (`fsck`)
- Ensure `du` is in PATH

#### StateDirectory doesn't exist

```python
# State directory auto-created on first snapshot
await monitor.record_snapshot()  # Creates state/ directory
```

**Notes:**
- Never fails, creates directory if needed
- Requires write permission to project root

## Integration Examples

### With recorder.py

```python
# In background task:
async def disk_check_loop():
    monitor = DiskMonitor()
    
    while not shutdown_event.is_set():
        try:
            # Record metrics
            await monitor.record_snapshot()
            
            # Check usage
            status = await monitor.get_status()
            if status['utilization'] == 'Critical':
                trigger_cleanup()
            
            await asyncio.sleep(300)  # 5 minutes
        except Exception as e:
            logger.error(f"Monitor error: {e}")
```

### With Monitoring System

```python
# Export to Prometheus format
async def prometheus_metrics():
    monitor = DiskMonitor()
    status = await monitor.get_status()
    
    return f"""
disk_usage_gb {status['usage_gb']}
disk_used_percent {status['usage_percent']}
disk_growth_gb_day {status['growth_rate']}
disk_retention_days {status['retention_days']}
"""
```

### Programmatic Planning

```python
async def plan_retention():
    monitor = DiskMonitor()
    
    # Get measured growth
    growth = await monitor.calculate_growth_rate(days=14)
    retention = await monitor.estimate_retention_days()
    
    # Recommendation
    if retention < 20:
        logger.warning(f"Low retention: {retention:.0f} days")
        recommend_larger_disk()
    elif retention > 60:
        logger.info(f"ample retention: {retention:.0f} days")
        recommend_increase_retention()
```

## Performance Characteristics

- **get_disk_usage_gb()**: 100-500ms (depends on disk size)
- **calculate_growth_rate()**: <1ms (uses cache)
- **record_snapshot()**: 100-600ms (disk I/O + calculation)
- **load_history()**: <50ms (file read + JSON parse)
- **Memory per snapshot**: ~200 bytes (history kept in memory)

## Limitations

- **Snapshot frequency**: Limited by `du` command speed (~100ms minimum)
- **History retention**: Keep ~1000 samples max (14 days at 5-min intervals)
- **Growth calculation**: Needs at least 3 samples for accuracy
- **Retention estimate**: Assumes linear growth (changes at hard limits)

## Thread Safety

- **NOT thread-safe**: Concurrent calls to record_snapshot() may cause issues
- **Async-safe**: Multiple tasks can await on same monitor
- **Recommendation**: One global DiskMonitor instance, accessed via background task

## See Also

- [DISK_MANAGEMENT.md](DISK_MANAGEMENT.md) - System overview
- [disk_plan.py](disk_plan.py) - Retention planning tool
- [recorder.py](recorder.py) - Integration point
- [config.py](config.py) - Configuration section
