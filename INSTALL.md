# Installation

## Requirements

- Linux or WSL2 with systemd (for service mode)
- Python 3.12+
- Network access to Binance APIs
- Sufficient disk for raw + catalog data

## Setup

```bash
cd ~
git clone <repo-url> CryptoRecorder
cd CryptoRecorder

python3 -m venv .venv
source .venv/bin/activate
sudo apt-get install -y build-essential python3-dev g++
pip install -r requirements.txt
```

## Basic verification

```bash
python VALIDATE.py system
```

More detailed validation and operations notes:
- [docs/VALIDATION.md](docs/VALIDATION.md)
- [docs/OPERATIONS.md](docs/OPERATIONS.md)

## Run locally

```bash
source .venv/bin/activate
python recorder.py
```

## Convert a day

```bash
python convert_day.py --date YYYY-MM-DD
```

Legacy-compatible command:

```bash
python convert_yesterday.py --date YYYY-MM-DD
```

## Service install (optional)

```bash
sudo cp systemd/cryptofeed-recorder.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.service /etc/systemd/system/
sudo cp systemd/nautilus-convert.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cryptofeed-recorder
sudo systemctl enable --now nautilus-convert.timer
```

The service units now use `convert_day.py` as the preferred converter CLI.
`convert_yesterday.py` remains available as a legacy-compatible wrapper.

## Useful checks

```bash
systemctl status cryptofeed-recorder
journalctl -u cryptofeed-recorder -f
cat state/heartbeat.json | python3 -m json.tool
python VALIDATE.py all
```
