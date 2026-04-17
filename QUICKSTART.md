# Quick Start Guide

**Binance 24/7 Market Data Recorder** - Get up and running in 5 minutes.

## 1️⃣ Prerequisites

- Python 3.12+
- 200+ GB disk space
- Network access to api.binance.com

## 2️⃣ Setup (5 minutes)

```bash
# Go to project directory
cd ~/services/CryptoRecorder

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify setup
python test_setup.py
```

Expected output: All checks passed ✓

## 2b️⃣ Validate System (Optional but Recommended)

```bash
# Quick validation (30 seconds)
python run_validators.py quick

# Full validation (2 minutes)
python run_validators.py full

# All validators + disk plan (5 minutes)
python run_validators.py all

# Interactive menu
python run_validators.py
```

Expected output: All tests pass ✓

This validates:
- ✓ All dependencies installed
- ✓ Configuration loaded correctly
- ✓ Recorder integration
- ✓ Disk management system
- ✓ Converter module structure
- ✓ Async functionality

### Option A: Manual (Testing)
```bash
python recorder.py

# Should output:
# INFO - Recorder starting...
# INFO - Universe selected: BINANCE_SPOT=[BTCUSDT, ETHUSDT, ...]
# INFO - Feed handler starting...
```

### Option B: As Service (Production)
```bash
# Install service files
sudo cp systemd/*.service systemd/*.timer /etc/systemd/system/

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable cryptofeed-recorder
sudo systemctl start cryptofeed-recorder

# Check status
sudo systemctl status cryptofeed-recorder
```

## 4️⃣ Monitor Data

```bash
# Check recording is working
ls -lh data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/

# Should show files like: 2024-01-15T00.jsonl.zst (50-500 MB each)

# View live stats
tail -f state/heartbeat.json | jq

# Monitor disk usage
watch -n 5 'du -sh data_raw/'
```

## 5️⃣ Set Up Automated Conversion

```bash
# Enable automatic daily conversion (runs at 00:10 UTC)
sudo systemctl enable nautilus-convert.timer
sudo systemctl start nautilus-convert.timer

# Check timer
sudo systemctl status nautilus-convert.timer

# Or manually convert today's data
python convert_yesterday.py --date $(date -d yesterday +%Y-%m-%d)
```

## 🔍 Useful Commands

### View Logs
```bash
# Real-time logs
sudo journalctl -u cryptofeed-recorder -f

# Last 50 lines
sudo journalctl -u cryptofeed-recorder -n 50

# Only errors
sudo journalctl -u cryptofeed-recorder -p err
```

### Recorder Status
```bash
# Is it running?
pgrep -f 'python recorder.py' && echo "✓ Running" || echo "✗ Stopped"

# Disk usage
du -sh data_raw/

# Symbol count
find data_raw -type d -name "BTCUSDT" -o -name "ETHUSDT" | wc -l
```

### Data Inspection
```bash
# View a sample record
zstd -d < data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/*.jsonl.zst | head -1 | jq

# Count records in a file
zstd -d < data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/*.jsonl.zst | wc -l

# Check for gaps in logs
grep -i gap recorder.log
```

## ⚙️ Common Configuration Changes

### Reduce data rate (slower systems)
Edit `config.py`:
```python
TOP_SYMBOLS = 25            # from 50
DEPTH_INTERVAL_MS = 1000    # from 100
QUEUE_MAX_SIZE = 5000       # from 10000
```
Then restart: `sudo systemctl restart cryptofeed-recorder`

### Increase data rate (fast systems)
```python
TOP_SYMBOLS = 100           # from 50
QUEUE_MAX_SIZE = 20000      # from 10000
```

### Disk cleanup more aggressively
```python
DISK_SOFT_LIMIT_GB = 150    # from 180
DISK_CLEANUP_TARGET_GB = 100  # from 160
```

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| cryptofeed not found | `pip install cryptofeed==2.4.1` |
| Service won't start | `sudo systemctl status cryptofeed-recorder` (check logs) |
| No data files created | Check network: `curl https://api.binance.com/api/v3/ping` |
| Disk filling up fast | Check config.py TOP_SYMBOLS setting |
| High memory usage | Reduce QUEUE_MAX_SIZE in config.py |
| Converter fails | Ensure data_raw/ has files for the date |

## 📊 Performance

- **Daily Data**: ~9-10 GB (100 symbols)
- **Compressed**: ~1-2 GB (7-10x compression)
- **Monthly**: ~30-60 GB = ~2.5-5 TB annually
- **200 GB Disk**: ~50-60 days retention

## 📚 Full Docs

- **README.md** - Complete overview
- **INSTALL.md** - Detailed setup + troubleshooting
- **PROJECT_STATUS.md** - Architecture + features
- **config.py** - All configuration options

## 💡 Tips

1. **First time?** Start with manual run: `python recorder.py`
2. **Running 24/7?** Use systemd service
3. **Monitoring?** Watch `state/heartbeat.json` (updates every 30s)
4. **Errors?** Check `recorder.log` + `journalctl`
5. **Disk issues?** Run `du -sh data_raw/` to check size

---

**Next Steps:**
- [ ] Run `python validate_system.py` for comprehensive validation
- [ ] Start recorder with `python recorder.py`
- [ ] Let it run for a few hours to collect data
- [ ] Convert data with `python convert_yesterday.py`
- [ ] Set up systemd service for production
- [ ] Monitor with `journalctl -f`

Happy recording! 🚀
