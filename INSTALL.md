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

The recorder launches native Binance WebSocket connections for `depth_v2` and
`trade_v2` channels. No additional flags are needed.

## Convert a Day

```bash
python convert_day.py --date YYYY-MM-DD

# Enable optional derived depth10
python convert_day.py --date YYYY-MM-DD --emit-depth10
```

## Service Install (Optional)

```bash
sudo cp systemd/crypto-recorder.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-recorder 
sudo systemctl enable --now nautilus-convert.timer
```

## Useful Commands

```bash
# Check service status
systemctl status crypto-recorder
journalctl -u crypto-recorder -f    

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
- [docs/SCHEMAS.md](docs/SCHEMAS.md) — Raw and state file schemas
- [docs/GUARANTEES.md](docs/GUARANTEES.md) — Scope boundaries
