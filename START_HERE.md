# 🚀 CryptoRecorder - START HERE

**Production-ready Binance 24/7 market data recorder for NautilusTrader backtesting**

---

## ⚡ Quick Start (3 minutes)

### 1. Setup Environment
```bash
cd ~/services/CryptoRecorder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Validate System
```bash
python3 VALIDATE.py all
```

Expected output: **All validations passed! (5/5)**

### 3. Start Recording
```bash
python3 recorder.py
```

Monitor with: `tail -f recorder.log`

---

## 📋 What You Have

### Core Components ✅
- **Recorder** (`recorder.py`) - 24/7 Binance L2 order book + trades streaming
- **Storage Layer** (`storage.py`) - Hourly compressed JSONL files with zstandard
- **Converter** (`convert_yesterday.py`) - Daily batch to Nautilus ParquetDataCatalog
- **Health Monitor** (`health_monitor.py`) - Real-time per-symbol statistics
- **Disk Management** - Automatic retention and cleanup (configurable)

### Validation Tools ✅
- **VALIDATE.py** - Master validator (all systems in one command)
- **validate_system.py** - Comprehensive 25-test suite
- **test_setup.py** - Dependency and setup verification

### Configuration
- **config.py** - All constants and settings
- **config.yaml** - YAML configuration
- **requirements.txt** - Python dependencies

---

## 🔍 Validation Options

### Option 1: Full Validation (2 minutes)
```bash
python3 VALIDATE.py all
```
Tests: Setup, System, Recorder, Converter, Disk Management
Result: JSON report saved to `state/master_validation_report.json`

### Option 2: Quick Check (30 seconds)
```bash
python3 VALIDATE.py quick
```
Tests: Basic setup and system checks

### Option 3: Specific Component
```bash
python3 VALIDATE.py setup      # Dependencies
python3 VALIDATE.py recorder   # Recorder system
python3 VALIDATE.py converter  # Converter modules
```

### Option 4: View Last Report
```bash
python3 VALIDATE.py report
```

### Option 5: Interactive Menu
```bash
python3 VALIDATE.py
```

---

## 💾 Data Storage

### Raw Data
- **Location**: `~/data_raw/{venue}/{channel}/{symbol}/`
- **Format**: JSONL (zstandard compressed)
- **Retention**: Configurable (default: 30 days)
- **Size**: ~15GB/day for 50 symbols

### Converted Data  
- **Location**: `~/data_nautilus/`
- **Format**: ParquetDataCatalog (NautilusTrader format)
- **Schedule**: Daily conversion @00:30 UTC (via systemd timer)

### Monitoring
- **Heartbeat**: `state/heartbeat.json` (30s updates)
- **Logs**: `recorder.log` (rotating, debug mode)
- **Reports**: `state/validation_report.json`

---

## 🛠️ Systemd Services

### Auto-start Recorder
```bash
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cryptofeed-recorder --now
```

### Auto-convert Daily
```bash
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable nautilus-convert.timer --now
```

### Check Status
```bash
sudo systemctl status cryptofeed-recorder
journalctl -u cryptofeed-recorder -f  # Follow logs
```

---

## 📊 Configuration

### Size & Retention (`config.py`)
```python
# Recording universe
QUOTE_SYMBOLS = 50  # Top 50 by 24h volume

# Storage
RETENTION_DAYS = 30
TARGET_FREE_GB = 200

# Conversion
SNAPSHOT_INTERVAL_MS = 10 * 60 * 1000  # 10 minutes
```

### YAML Config (`config.yaml`)
```yaml
recorder:
  venues: [binance_spot, binance_um]
  channels: [depth, trades]
  snapshot_interval_ms: 600000  # 10 minutes
```

---

## 🐛 Troubleshooting

### All Validations Pass But File Issues
```bash
# Auto-fix missing directories
python3 validate_system.py --fix
```

### Recorder Failing to Start
```bash
# Check dependencies
python3 test_setup.py

# Check logs
tail -f recorder.log

# Manual test
python3 -c "from recorder import RawDataRecorder; print('OK')"
```

### Import Errors
```bash
# Ensure venv is active
source .venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Disk Space Issues
```bash
# Check disk status
python3 disk_plan.py

# View retention plan
cat state/disk_plan_report.json
```

---

## 📚 Detailed Documentation

- [README.md](README.md) - Architecture and features
- [INSTALL.md](INSTALL.md) - Detailed installation
- [QUICKSTART.md](QUICKSTART.md) - Step-by-step guide
- [DISK_MANAGEMENT.md](DISK_MANAGEMENT.md) - Storage system
- [VALIDATION_GUIDE.md](VALIDATION_GUIDE.md) - Testing guide

---

## ✅ System Health Checks

```bash
# Check recorder is running
ps aux | grep recorder.py

# Monitor health in real-time
tail -f state/heartbeat.json

# Check disk usage
du -sh ~/data_raw/ ~/data_nautilus/

# Full disk report
python3 disk_monitor.py
```

---

## 🎯 Next Steps

1. **Run validation**: `python3 VALIDATE.py all`
2. **Check logs**: `tail -f recorder.log`
3. **Monitor symbols**: `tail -f state/heartbeat.json`
4. **Check disk**: `python3 disk_plan.py`
5. **Deploy systemd**: Follow services section above

---

**Status**: ✅ Production Ready | **Last Validated**: April 17, 2026
