# 📖 CryptoRecorder - Complete Guide

A production-grade **Binance 24/7 Market Data Recorder** for NautilusTrader backtesting.

**Quick Commands:**
- 🚀 **Quick start**: `python3 VALIDATE.py all`
- 📋 **View status**: `tail -f state/heartbeat.json`
- 🔍 **Check disk**: `python3 disk_plan.py`
- 🛠️ **See logs**: `tail -f recorder.log`

---

## 📊 System Overview

### Recording System
- **Source**: Binance Spot & Futures WebSocket streams
- **Data**: L2 order book updates (@100ms), trades, snapshots
- **Universe**: Top 50 USDT-trading pairs by 24h volume
- **Format**: Compressed JSONL (zstandard) with hourly rotation
- **Storage**: ~15GB/day, configurable 30-day retention

### Conversion System
- **Input**: Raw JSONL logs from previous day
- **Process**: Reconstruct L2 state, generate TradeTicks and OrderBookDeltas
- **Output**: ParquetDataCatalog (NautilusTrader native format)
- **Schedule**: Daily @00:30 UTC via systemd timer

### Monitoring
- **Health**: Per-symbol statistics (count, gaps, reconnects)
- **Heartbeat**: `state/heartbeat.json` (30s updates)
- **Reports**: JSON validation reports with detailed results
- **Logs**: Rotating debug logs for troubleshooting

---

## 🚀 Getting Started (3 minutes)

### 1. Initialize Environment
```bash
cd ~/services/CryptoRecorder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Validate Everything
```bash
python3 VALIDATE.py all
```

✅ Expected: Setup, System, Recorder, Converter, Disk Management all PASS

### 3. Start Recording
```bash
python3 recorder.py
```

Monitor with: `tail -f recorder.log`

### (Optional) Install as Service
```bash
# Auto-start recorder
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cryptofeed-recorder --now

# Auto-convert daily
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nautilus-convert.timer --now
```

---

## 🔍 Validation & Testing

### Master Validator: `VALIDATE.py`
The primary tool for testing the entire system.

**All Systems (recommended)**
```bash
python3 VALIDATE.py all        # 2 minutes, saves JSON report
```

**Quick Test (30 seconds)**
```bash
python3 VALIDATE.py quick      # Basic setup + system checks
```

**Specific Systems**
```bash
python3 VALIDATE.py setup      # Dependencies only
python3 VALIDATE.py recorder   # Recorder modules
python3 VALIDATE.py converter  # Converter modules
python3 VALIDATE.py report     # Show last report
```

**Interactive Menu**
```bash
python3 VALIDATE.py            # Choose from menu
```

### What Gets Tested
- ✅ Python 3.12+, virtual environment
- ✅ Dependencies: asyncio, aiohttp, cryptofeed, zstandard, pandas, pyarrow
- ✅ Core modules: config, storage, recorder, health_monitor
- ✅ Converter modules: parsers, book_builder, nautilus_builder
- ✅ Disk management: disk_monitor, disk_plan
- ✅ Network connectivity to api.binance.com
- ✅ Disk space availability

### Legacy Validators (still available)
```bash
python3 validate_system.py     # Comprehensive 25-test suite
python3 test_setup.py          # Setup verification (17 tests)
```

---

## 📁 Project Structure

```
CryptoRecorder/
├── START_HERE.md                    ← Read this first!
├── GUIDE.md                         ← Complete guide (this file)
├── README.md                        ← Architecture details
├── INSTALL.md                       ← Installation instructions
│
├── Core Recording
│   ├── recorder.py                  # Main entry point, orchestrator
│   ├── storage.py                   # File I/O, compression, rotation
│   ├── binance_universe.py          # Symbol selection (top 50)
│   ├── health_monitor.py            # Per-symbol statistics
│   └── config.py                    # All configuration
│
├── Daily Conversion  
│   ├── convert_yesterday.py         # Daily batch script
│   └── converter/
│       ├── parsers.py               # JSONL decompression
│       ├── book_builder.py          # L2 state reconstruction
│       └── nautilus_builder.py      # Nautilus object creation
│
├── Disk Management
│   ├── disk_monitor.py              # Real-time usage tracking
│   └── disk_plan.py                 # Retention planning + recommendations
│
├── Validation & Testing
│   ├── VALIDATE.py                  # ⭐ Master validator (use this!)
│   ├── validate_system.py           # Comprehensive 25-test suite
│   ├── test_setup.py                # Dependency verification
│   └── run_validators.py            # Interactive test runner
│
├── Configuration
│   ├── config.py                    # Python configuration
│   ├── config.yaml                  # YAML settings
│   └── requirements.txt             # Python packages
│
├── Systemd Services
│   └── systemd/
│       ├── cryptofeed-recorder.service
│       ├── nautilus-convert.service
│       └── nautilus-convert.timer
│
└── Data & State
    ├── data_raw/                    # Raw JSONL logs (24/7 updated)
    ├── data_nautilus/               # Converted ParquetDataCatalog
    ├── meta/                        # Metadata (instrument specs)
    └── state/                       # Reports, logs, heartbeat
```

---

## 💾 Data Locations

### Raw Recording
```
~/data_raw/
├── binance_um/                      # Binance Futures
│   ├── depth/                       # L2 updates
│   │   └── BTCUSDT/
│   │       └── 2026-04-17-23.jsonl.zst
│   └── trades/                      # Trade events
│       └── BTCUSDT/
│           └── 2026-04-17-23.jsonl.zst
└── binance_spot/                    # Binance Spot
    └── ...
```

**Format**: JSONL compressed with zstandard
**Retention**: 30 days (configurable)
**Hourly rotation**: Each hour creates new file

### Converted Data
```
~/data_nautilus/
├── binance-spot/
│   └── BTCUSDT/
│       ├── trades.parquet
│       ├── book_depth_1_[timestamp].parquet
│       └── book_depth_10_[timestamp].parquet
└── binance-um/
    └── ...
```

**Format**: ParquetDataCatalog (NautilusTrader native)
**Updated**: Daily @00:30 UTC
**Contains**: TradeTicks, OrderBookDeltas, Depth10 snapshots

---

## ⚙️ Configuration

### Main Settings (`config.py`)

```python
# Recording
SIDE_CHANNELS = 50              # Top 50 symbols by volume
BINANCE_SPOT = True             # Record Binance Spot
BINANCE_FUTURES = True          # Record Binance Futures

# Storage
MAX_DATA_GB = 500               # Target total size
RETENTION_DAYS = 30             # Delete older data
HOURLY_ROTATION = True          # Rotate every hour
COMPRESSION = "zstd"            # Compression codec

# Snapshots  
SNAPSHOT_INTERVAL_MS = 600000   # 10 minutes REST snapshots
BATCH_SIZE = 10000              # Buffering before write

# Monitoring
HEARTBEAT_INTERVAL_S = 30       # Status update frequency
HEALTH_CHECK_ENABLED = True     # Per-symbol stats
```

### YAML Config (`config.yaml`)

All settings can also be in YAML. Python config takes precedence.

```yaml
recorder:
  venues: [binance_spot, binance_um]
  channels: [depth, trades]
  symbols_count: 50
  
storage:
  max_total_gb: 500
  retention_days: 30
  compression: zstd
  
conversion:
  snapshot_interval_ms: 600000
  batch_output_dir: ~/data_nautilus
```

---

## 🔧 Typical Workflows

### Monitor Live Recording
```bash
# Terminal 1: Start recorder
python3 recorder.py

# Terminal 2: Watch health stats (updates every 30s)
tail -f state/heartbeat.json

# Terminal 3: Check logs
tail -f recorder.log
```

### Check Disk Usage
```bash
# Current usage
du -sh ~/data_raw/ ~/data_nautilus/

# Full report with recommendations
python3 disk_plan.py
```

### Convert Yesterday's Data
```bash
# Manual conversion
python3 convert_yesterday.py

# Automated (if systemd is setup)
# Runs automatically daily @00:30 UTC
```

### Validate System Health
```bash
# Quick check
python3 VALIDATE.py quick

# Full validation with report
python3 VALIDATE.py all
cat state/master_validation_report.json | python3 -m json.tool
```

---

## 📊 Understanding the Output

### Heartbeat File (`state/heartbeat.json`)
```json
{
  "timestamp": "2026-04-17T15:30:45Z",
  "venue": "binance_spot",
  "symbols": 50,
  "total_events": 1234567,
  "per_symbol": {
    "BTCUSDT": {
      "depth_updates": 45600,
      "trades": 1200,
      "gaps_detected": 0,
      "resyncs": 1,
      "status": "healthy"
    }
  }
}
```

### Validation Report (`state/master_validation_report.json`)
```json
{
  "timestamp": "2026-04-17T15:30:45Z",
  "total_validations": 5,
  "passed": 5,
  "failed": 0,
  "results": {
    "setup": {"passed": true, "checks": 17},
    "system": {"passed": true, "checks": 8},
    "recorder": {"passed": true, "modules": 3},
    "converter": {"passed": true, "modules": 3},
    "disk_management": {"passed": true, "modules": 2}
  }
}
```

---

## 🐛 Troubleshooting

### Issue: "All checks passed" but files don't exist

**Solution**: Auto-create missing directories
```bash
python3 validate_system.py --fix
```

### Issue: Import errors when starting recorder

**Solution**: Verify environment setup
```bash
source .venv/bin/activate
python3 test_setup.py
```

If setup passes but imports fail:
```bash
pip install -r requirements.txt --force-reinstall
```

### Issue: Recorder starts but no data appears

**Check 1**: Verify network connectivity
```bash
ping api.binance.com
```

**Check 2**: Check logs for errors
```bash
tail -f recorder.log | grep ERROR
```

**Check 3**: Verify configuration loaded
```bash
python3 -c "from config import *; print(f'Recording {SYMBOL_COUNT} symbols')"
```

### Issue: Disk space running out

**Check**: Current usage and retention plan
```bash
python3 disk_plan.py
python3 disk_monitor.py
```

**Solution**: Reduce retention days or expand storage
```python
# In config.py
RETENTION_DAYS = 14  # Changed from 30
```

---

## 📚 Related Documentation

- **START_HERE.md** - Quick 3-minute setup
- **README.md** - Architecture details
- **INSTALL.md** - Detailed installation guide
- **VALIDATION_GUIDE.md** - Testing procedures
- **DISK_MANAGEMENT.md** - Storage system internals

---

## ✅ Production Checklist

- [ ] Run `python3 VALIDATE.py all` (all pass)
- [ ] Set up systemd services
- [ ] Verify data appearing in `~/data_raw/`
- [ ] Check daily conversions in `~/data_nautilus/`
- [ ] Configure disk retention appropriate for your setup
- [ ] Set up backup/monitoring for raw data
- [ ] Monitor disk growth rate with `python3 disk_monitor.py`
- [ ] Test failover by stopping/restarting recorder

---

**Status**: ✅ Production Ready | **Created**: April 17, 2026 | **Version**: 1.0
