# CryptoRecorder

CryptoRecorder is a Phase 1 Binance market-data pipeline for backtesting.

It records Binance **spot** and **USDT-M futures** market data (trades + L2 deltas),
stores raw append-only files, and converts daily data into a NautilusTrader
`ParquetDataCatalog`.

## What Phase 1 does

- 24/7 recording via `cryptofeed`
- Channels: `trade`, `depth`, `exchangeinfo`
- Hourly raw file rotation with compression
- Daily conversion to Nautilus-native catalog
- Startup coverage + heartbeat + validation workflows

## What Phase 1 does **not** do

- No periodic REST `/depth` polling
- No deterministic Binance `U/u/pu` replay
- No exact matching-engine reconstruction

L2 reconstruction in Phase 1 is **approximate** by design. Deterministic replay
is still roadmap work, not a current guarantee.

## Quick start

```bash
cd ~/CryptoRecorder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python recorder.py
```

In another shell:

```bash
source .venv/bin/activate
python VALIDATE.py all
```

Setup details and service installation live in [INSTALL.md](INSTALL.md).

## Conversion

Recommended CLI name:

```bash
python convert_day.py --date YYYY-MM-DD
```

Backward-compatible legacy entrypoint still works:

```bash
python convert_yesterday.py --date YYYY-MM-DD
```

## Validation entrypoint

`VALIDATE.py` remains the master CLI:

```bash
python VALIDATE.py system
python VALIDATE.py runtime
python VALIDATE.py scale
python VALIDATE.py nautilus
python VALIDATE.py purge
python VALIDATE.py all
python VALIDATE.py accept
```

Preferred validator modules now live under `validators/system.py`,
`validators/runtime.py`, `validators/scale.py`,
`validators/nautilus_catalog.py`, and `validators/purge_safety.py`.
Legacy `validate_*.py` filenames are still kept for compatibility.

## Repository map (high level)

- `recorder.py` – recorder runtime entrypoint
- `convert_day.py` / `convert_yesterday.py` – conversion CLIs
- `converter/` – conversion internals
- `validators/` – validator implementations
- `state/` – runtime reports (`heartbeat.json`, `startup_coverage.json`, conversion reports)
- `docs/` – detailed documentation

## Detailed docs

- [INSTALL.md](INSTALL.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Operations](docs/OPERATIONS.md)
- [Validation](docs/VALIDATION.md)
- [State schemas](docs/SCHEMAS.md)

## Phase 1 status statement

This repository is intentionally optimized for robust, production-safe recording
with graceful degradation (skip bad symbols, continue with survivors), not for
exchange-perfect deterministic replay.
