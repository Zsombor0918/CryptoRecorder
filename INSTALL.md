# Installation & Setup Guide

Step-by-step guide for WSL2 Ubuntu or native Linux.

## Requirements

- **OS**: WSL2 Ubuntu 22.04+ (with systemd) or native Linux
- **Python**: 3.12+
- **Disk**: 200+ GB available (soft limit 400 GB, hard limit 480 GB)
- **RAM**: 2+ GB
- **Network**: Unrestricted access to `api.binance.com`

## 1. Enable systemd on WSL2 (skip on native Linux)

```bash
sudo tee /etc/wsl.conf <<'EOF'
[boot]
systemd=true
EOF
```

Restart WSL2 from PowerShell: `wsl --shutdown`, then reopen.

## 2. Clone and set up the environment

```bash
cd ~
git clone <repo-url> CryptoRecorder
cd CryptoRecorder

python3 -m venv .venv
source .venv/bin/activate
sudo apt-get install -y build-essential python3-dev g++
pip install -r requirements.txt
```

### Verify installation

```bash
python VALIDATE.py system
```

All dependency and config checks should pass.

## 3. Configure (optional)

Edit `config.py` or override with environment variables:

| Setting | Default | Env Override |
|---------|---------|-------------|
| Symbols per venue | 50 | `CRYPTO_RECORDER_TOP_SYMBOLS` |
| Depth update interval | 100 ms | — |
| File rotation | 60 min | — |
| ExchangeInfo refresh | 6 h | — |
| Heartbeat interval | 30 s | — |
| Disk soft limit | 400 GB | — |
| Disk hard limit | 480 GB | — |
| Disk cleanup target | 350 GB | — |

For a quick test with fewer symbols:

```bash
CRYPTO_RECORDER_TOP_SYMBOLS=3 python recorder.py
```

## 4. Run the recorder

```bash
source .venv/bin/activate
python recorder.py          # foreground (Ctrl-C to stop)
```

Data is written to `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/`.

## 5. Install systemd services (production)

Edit the service files in `systemd/` to match your username and paths, then:

```bash
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.service   /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer     /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now cryptofeed-recorder.service
sudo systemctl enable --now nautilus-convert.timer
```

Check status:

```bash
sudo systemctl status cryptofeed-recorder
sudo journalctl -u cryptofeed-recorder -f
```

## 6. Daily conversion to Nautilus catalog

The `nautilus-convert.timer` runs daily at 00:10 UTC.  To convert manually:

```bash
python convert_yesterday.py --date 2026-04-17
```

The Nautilus `ParquetDataCatalog` is written to `~/nautilus_data/catalog/`.

## 7. Validation

```bash
python VALIDATE.py system      # dependency & config checks (~5 s)
python VALIDATE.py runtime     # 3-min live smoke-test (12 checks)
python VALIDATE.py nautilus    # Nautilus catalog E2E (9 checks)
python VALIDATE.py all         # system + runtime + nautilus
python VALIDATE.py scale       # 10-min 50/50 acceptance (11 checks)
python VALIDATE.py accept      # full DoD: system + runtime + scale + nautilus
```

## 8. Directory layout

```
CryptoRecorder/
├── recorder.py              # main recorder
├── config.py                # all constants and paths
├── config.yaml              # optional YAML overrides
├── storage.py               # file rotation, compression, async writers
├── health_monitor.py        # heartbeat, per-symbol stats
├── disk_monitor.py          # disk usage, automatic cleanup
├── binance_universe.py      # top-N symbol selection
├── convert_yesterday.py     # daily raw → Nautilus catalog
├── VALIDATE.py              # unified validation entry point
├── validators/
│   ├── validate_system.py
│   ├── validate_runtime.py
│   ├── validate_scale_50_50.py
│   └── validate_nautilus_catalog_e2e.py
├── systemd/                 # service & timer units
├── data_raw/                # raw JSONL (gitignored)
├── meta/universe/           # per-day symbol lists
└── state/                   # heartbeat, reports (gitignored)
```
