# Installation & Setup Guide

Complete step-by-step guide to set up the Binance 24/7 Market Data Recorder on WSL2 or native Linux.

## System Requirements

- **OS**: WSL2 Ubuntu 22.04+ (with systemd enabled) OR native Linux
- **Python**: Python 3.12+ with pip
- **Disk**: 200+ GB available
- **Memory**: 2+ GB RAM
- **Network**: Unrestricted access to api.binance.com (no proxy/firewall blocking)

## Initial Setup (One-time)

### 1. Enable systemd on WSL2 (if using WSL2)

Edit `/etc/wsl.conf` in WSL2 (or create it):

```bash
sudo nano /etc/wsl.conf
```

Add these lines:

```ini
[boot]
systemd=true
```

Save (Ctrl+X, Y, Enter) and restart WSL2:

```bash
# In PowerShell on Windows
wsl --shutdown
wsl
```

Verify systemd is available:

```bash
systemctl --version
```

### 2. Install System Dependencies

```bash
# Update package lists
sudo apt update

# Install Python 3.12 and build tools
sudo apt install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    build-essential \
    curl \
    git \
    jq

# Verify Python
python3.12 --version
```

### 3. Clone the Repository

```bash
# Create services directory
mkdir -p ~/services
cd ~/services

# Clone repository (replace with actual repo URL)
git clone <repository-url> CryptoRecorder
cd CryptoRecorder

# List contents to verify
ls -la
```

### 4. Create Virtual Environment

```bash
# Create venv
python3.12 -m venv .venv

# Activate venv
source .venv/bin/activate

# Verify activation (prompt should show (.venv))
which python
```

### 5. Install Python Dependencies

```bash
# Upgrade pip
pip install --upgrade pip setuptools wheel

# Install from requirements.txt
pip install -r requirements.txt

# Verify installations
pip list | grep -E "cryptofeed|aiohttp|zstandard|pandas|pyarrow"

# Test imports
python -c "
import asyncio
import aiohttp
import zstandard
import pandas
import pyarrow
print('✓ All dependencies installed successfully')
"
```

**Note**: If `cryptofeed` installation fails due to C++ compilation:

```bash
# Ensure build tools are installed
sudo apt install -y build-essential python3.12-dev

# Retry installation
pip install cryptofeed==2.4.1
```

### 6. Verify Setup

```bash
# Run validation script
python test_setup.py

# Should output:
# [✓ PASS] Python 3.12+
# [✓ PASS] Virtual environment active
# [✓ PASS] Module: asyncio
# [✓ PASS] Module: aiohttp
# ...
```

## Configuration

### Default Settings

The recorder comes with production-safe defaults in `config.py`:

```python
TOP_SYMBOLS = 50                    # Top 50 by 24h volume per venue
DEPTH_INTERVAL_MS = 100             # L2 updates every 100ms
SNAPSHOT_INTERVAL_SEC = 600         # REST snapshots every 10 min
EXCHANGEINFO_INTERVAL_SEC = 21600   # Metadata every 6 hours
HEARTBEAT_INTERVAL_SEC = 30         # Health stats every 30s
DISK_SOFT_LIMIT_GB = 180            # Cleanup threshold
DISK_CLEANUP_TARGET_GB = 160        # Cleanup to this level
```

### Customizing Configuration

Edit `config.py` for your needs:

```python
# Reduce data rate (slower systems)
TOP_SYMBOLS = 25
DEPTH_INTERVAL_MS = 1000

# Increase data rate (high-speed systems)
TOP_SYMBOLS = 100
QUEUE_MAX_SIZE = 20000

# Adjust storage limits
DISK_SOFT_LIMIT_GB = 250  # 250 GB threshold
DISK_CLEANUP_TARGET_GB = 200  # Clean to 200 GB
```

## Running the Recorder

### Manual Start (Testing)

```bash
# Ensure venv is activated
source .venv/bin/activate

# Start recorder
python recorder.py

# Expected output:
# 2024-01-15 10:00:00 - recorder - INFO - Recorder starting...
# 2024-01-15 10:00:01 - recorder - INFO - Universe selected: BINANCE_SPOT=[BTCUSDT, ETHUSDT, ...]
# 2024-01-15 10:00:02 - recorder - INFO - Feed handler starting...
# ... (no errors = success)
```

### Monitor in Another Terminal

```bash
# Watch real-time logs
tail -f recorder.log

# Monitor health stats (updates every 30s)
watch -n 1 'jq . state/heartbeat.json'

# Monitor disk usage
watch -n 5 'du -sh data_raw/'
```

### Graceful Shutdown

```bash
# Press Ctrl+C in terminal running recorder
# OR in another terminal
pkill -TERM -f 'python recorder.py'

# Wait for pending writes (10-30 seconds)
# Check if process exited
pgrep -f 'python recorder.py' || echo "Recorder stopped"
```

### Systemd Service Setup (Production)

```bash
# Ensure systemd is available
systemctl --version

# Copy service files to systemd
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/

# Reload systemd configuration
sudo systemctl daemon-reload

# Enable recorder to start on boot
sudo systemctl enable cryptofeed-recorder

# Start recorder now
sudo systemctl start cryptofeed-recorder

# Check status
sudo systemctl status cryptofeed-recorder

# View logs
sudo journalctl -u cryptofeed-recorder -f
```

### Enable Daily Data Conversion

```bash
# Enable conversion timer (runs at 00:10 UTC daily)
sudo systemctl enable nautilus-convert.timer

# Start timer
sudo systemctl start nautilus-convert.timer

# Verify it's running
sudo systemctl status nautilus-convert.timer

# See next scheduled run
sudo systemctl list-timers nautilus-convert.timer

# View conversion logs
sudo journalctl -u nautilus-convert -f
```

## Verification

### Health Check 1: Data Recording

```bash
# Check if data files are being created
ls -lh data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/

# Should show files like:
# 2024-01-15T00.jsonl.zst (50-500 MB)
# 2024-01-15T01.jsonl.zst (50-500 MB)
```

### Health Check 2: Record Format

```bash
# Decompress and view a sample record
zstd -d < data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/*.jsonl.zst | head -5

# Should show JSON records like:
# {"venue": "BINANCE_SPOT", "symbol": "BTCUSDT", "channel": "depth", ...}
# {"venue": "BINANCE_SPOT", "symbol": "BTCUSDT", "channel": "trade", ...}
```

### Health Check 3: Process Status

```bash
# Check if recorder is running
pgrep -f 'python recorder.py' && echo "✓ Recorder running" || echo "✗ Recorder stopped"

# If using systemd
sudo systemctl is-active cryptofeed-recorder

# Check for errors
grep ERROR recorder.log | tail -20
```

### Health Check 4: Disk Usage

```bash
# Show directory sizes
du -sh data_raw data_raw/*

# Expected growth rate (per day):
# ~9-10 GB/day for 50 symbols per venue

# Check available space
df -h data_raw
```

### Health Check 5: Data Completeness

```bash
# Count records in a file
zstd -d < data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date +%Y-%m-%d)/*.jsonl.zst | wc -l

# Expected: ~10,000-50,000 per file depending on venue
```

## Troubleshooting

### Issue: Module not found errors

```bash
# Verify venv is activated
echo $VIRTUAL_ENV
# Should show: /home/zsom/services/CryptoRecorder/.venv

# If not, activate it
source .venv/bin/activate

# If module still missing, reinstall
pip install -r requirements.txt
```

### Issue: cryptofeed installation fails

```bash
# Install build tools
sudo apt install -y build-essential python3.12-dev

# Clear pip cache and retry
pip cache purge
pip install cryptofeed==2.4.1 --no-cache-dir
```

### Issue: WebSocket connection errors

```bash
# Check network connectivity
curl -I https://api.binance.com/api/v3/ping
# Should return HTTP 200

# If behind proxy, set environment variables
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080
python recorder.py

# Check firewall
sudo ufw status
# If enabled, allow outbound: sudo ufw allow out to any port 443
```

### Issue: High memory usage

```bash
# Check memory consumption
ps aux | grep recorder | grep python

# Reduce queue sizes in config.py
QUEUE_MAX_SIZE = 5000  # Default 10000
TOP_SYMBOLS = 25  # Reduce from 50

# Restart recorder
sudo systemctl restart cryptofeed-recorder
```

### Issue: Disk filling up too quickly

```bash
# Check data rate
du -sh data_raw
watch -n 60 'du -sh data_raw'  # Update every minute

# Solutions:
# 1. Reduce symbols
TOP_SYMBOLS = 25

# 2. Increase cleanup threshold (aggressive cleanup)
DISK_CLEANUP_TARGET_GB = 100  # Clean to 100 GB

# 3. Reduce L2 update frequency
DEPTH_INTERVAL_MS = 1000  # Every 1 second instead of 100ms

# After changes, restart
sudo systemctl restart cryptofeed-recorder
```

### Issue: Converter fails

```bash
# Check if raw data exists for previous day
ls data_raw/BINANCE_SPOT/depth/BTCUSDT/$(date -d yesterday +%Y-%m-%d)

# If missing, recorder may not have collected data yet

# Manually run converter with debug
python convert_yesterday.py --date 2024-01-14

# Check conversion report
cat state/convert_reports/2024-01-14.json | jq

# Verify output Parquet files
ls -lh nautilus_data/catalog/
```

### Issue: systemd service fails to start

```bash
# Check service status and errors
sudo systemctl status cryptofeed-recorder

# View detailed logs
sudo journalctl -u cryptofeed-recorder -n 50 -p err

# Test Python directly
source /home/zsom/services/CryptoRecorder/.venv/bin/activate
python /home/zsom/services/CryptoRecorder/recorder.py

# Common issue: file permissions
ls -la /etc/systemd/system/cryptofeed-recorder.service
# Should be 644

# If not:
sudo chmod 644 /etc/systemd/system/cryptofeed-recorder.service
sudo systemctl daemon-reload
```

## Uninstallation

If you need to remove the recorder:

```bash
# Stop services
sudo systemctl stop cryptofeed-recorder
sudo systemctl disable cryptofeed-recorder

# Remove service files
sudo rm /etc/systemd/system/cryptofeed-recorder.service
sudo rm /etc/systemd/system/nautilus-convert.service
sudo rm /etc/systemd/system/nautilus-convert.timer
sudo systemctl daemon-reload

# Remove project (keep data if needed)
rm -rf ~/services/CryptoRecorder

# Or keep data for analysis
mkdir -p ~/backups/crypto_data
mv ~/services/CryptoRecorder/data_raw ~/backups/crypto_data/
rm -rf ~/services/CryptoRecorder
```

## Performance Tuning

### For WSL2 with limited resources

Edit `config.py`:

```python
# Recorder configuration
TOP_SYMBOLS = 25              # Reduce from 50
QUEUE_MAX_SIZE = 5000         # Reduce from 10000
WRITER_BATCH_SIZE = 50        # Reduce from 100
WRITER_FLUSH_INTERVAL_SEC = 10  # Wait longer

# Snapshots
SNAPSHOT_INTERVAL_SEC = 1200  # Every 20 min instead of 10
EXCHANGEINFO_INTERVAL_SEC = 43200  # Every 12 hours

# Disk
DISK_CLEANUP_TARGET_GB = 100  # Cleanup more aggressively
```

Then restart:

```bash
sudo systemctl restart cryptofeed-recorder
```

### For high-throughput systems

Edit `config.py`:

```python
TOP_SYMBOLS = 100
QUEUE_MAX_SIZE = 20000
WRITER_BATCH_SIZE = 200
WRITER_FLUSH_INTERVAL_SEC = 2
```

## Monitoring with Prometheus

The health stats can be scraped for monitoring:

```bash
# Endpoint example (optional, requires custom exporter)
curl localhost:9090/metrics

# Or parse heartbeat JSON
cat state/heartbeat.json | jq '.total_messages'
```

## Support

For issues or questions:

1. Check logs: `tail -f recorder.log`
2. Run validation: `python test_setup.py`
3. Check systemd: `sudo systemctl status cryptofeed-recorder`
4. Review config: `cat config.py | grep -A 2 "# Issue-related setting"`

---

**Last Updated**: 2024-01-15
**Version**: 1.0.0-installation-guide
