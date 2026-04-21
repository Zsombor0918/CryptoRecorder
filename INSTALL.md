# Installation

## Requirements

- Linux or WSL2 with systemd (for service mode)
- Python 3.10+
- Network access to Binance APIs
- Sufficient disk for raw + catalog data

## Setup

```bash
cd ~
git clone <repo-url> CryptoRecorder
cd CryptoRecorder

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Verify Setup

```bash
python validate.py
```

This checks dependencies, directories, and configuration.

## Run Unit Tests

```bash
pytest tests/
```

## Run Recorder

```bash
source .venv/bin/activate
python recorder.py
```

## Convert a Day

```bash
python convert_day.py --date YYYY-MM-DD
```

## Service Install (Optional)

```bash
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cryptofeed-recorder
sudo systemctl enable --now nautilus-convert.timer
```

## Useful Commands

```bash
# Check service status
systemctl status cryptofeed-recorder
journalctl -u cryptofeed-recorder -f

# Check heartbeat
cat state/heartbeat.json | python3 -m json.tool

# Quick smoke test
python scripts/smoke_test.py

# Full acceptance test
python scripts/acceptance_test.py
```

## More Documentation

- [docs/VALIDATION.md](docs/VALIDATION.md) — Testing guide
- [docs/OPERATIONS.md](docs/OPERATIONS.md) — Operations guide
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — System design
