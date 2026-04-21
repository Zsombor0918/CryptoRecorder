# CryptoRecorder

CryptoRecorder is a Phase 1 Binance market-data pipeline for backtesting.

**Phase 1 Target:** 50 spot instruments with trades + approximate L2 depth,
converted to a Nautilus-queryable `ParquetDataCatalog`.

It records Binance **spot** and **USDT-M futures** market data (trades + L2 deltas),
stores raw append-only files, and converts daily data into a NautilusTrader
`ParquetDataCatalog`.

## What Phase 1 does

- 24/7 recording via `cryptofeed`
- Channels: `trade`, `depth`, `exchangeinfo`
- Hourly raw file rotation with compression
- Daily conversion to Nautilus-native catalog (`TradeTick`, `OrderBookDepth10`)
- Crossed-book detection and exclusion from catalog
- Data presence tracking per instrument
- Startup coverage + heartbeat + validation workflows

## What Phase 1 does **not** do

- No periodic REST `/depth` polling (causes rate limiting)
- No deterministic Binance `U/u/pu` replay
- No bit-exact matching-engine reconstruction
- No L3 / queue position tracking

L2 reconstruction in Phase 1 is **approximate** by design. Deterministic replay
is roadmap work, not a current guarantee.

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

```bash
python convert_day.py --date YYYY-MM-DD
```

The converter produces:
- `CurrencyPair` instruments (spot) / `CryptoPerpetual` (futures)
- `TradeTick` objects from raw trade data
- `OrderBookDepth10` snapshots from L2 delta reconstruction

## Validation entrypoint

`VALIDATE.py` remains the master CLI:

```bash
python VALIDATE.py system      # Infrastructure checks
python VALIDATE.py runtime     # 3-min recorder smoke test
python VALIDATE.py scale       # 10-min 50/50 acceptance test
python VALIDATE.py nautilus    # Converter + catalog validation
python VALIDATE.py purge       # Purge safety proof
python VALIDATE.py all         # Quick suite
python VALIDATE.py accept      # Full acceptance suite
```

Preferred validator modules live under `validators/`.

## Repository map (high level)

- `recorder.py` – recorder runtime entrypoint
- `convert_day.py` – conversion CLI
- `converter/` – conversion internals (book reconstruction, trades, instruments)
- `validators/` – validator implementations
- `inspect_catalog.py` – catalog quality inspection (crossed-book, data presence)
- `state/` – runtime reports (`heartbeat.json`, `startup_coverage.json`, conversion reports)
- `docs/` – detailed documentation

## Detailed docs

- [INSTALL.md](INSTALL.md)
- [Architecture](docs/ARCHITECTURE.md) – pipeline and conversion model
- [Validation](docs/VALIDATION.md) – validation layers and checks
- [Guarantees](docs/GUARANTEES.md) – what Phase 1 guarantees and does not
- [Operations](docs/OPERATIONS.md)
- [State schemas](docs/SCHEMAS.md)

## Phase 1 status statement

This repository is intentionally optimized for robust, production-safe recording
with graceful degradation (skip bad symbols, continue with survivors), not for
exchange-perfect deterministic replay.

**The final catalog contains no crossed-book snapshots.** Crossed events during
reconstruction trigger resets, not catalog writes.
