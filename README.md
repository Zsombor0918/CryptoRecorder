# CryptoRecorder

CryptoRecorder is a Binance market-data pipeline for backtesting with a
preserved Phase 1 path and an opt-in Phase 2 deterministic L2 path.

**Phase 1 Target:** 50 spot instruments with trades + approximate L2 depth,
converted to a Nautilus-queryable `ParquetDataCatalog`.

## Quick Start

```bash
# 1. Clone and setup
git clone <repo>
cd CryptoRecorder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Validate setup
python validate.py

# 3. Run unit tests
pytest tests/

# 4. Start the recorder
python recorder.py

# Optional: Phase 2 native depth mode
python recorder.py --depth-mode phase2
```

## Project Structure

```
CryptoRecorder/
├── recorder.py          # Main recorder (starts here)
├── convert_day.py       # Convert raw data to Nautilus catalog
├── validate.py          # Setup validation (run on new machine)
│
├── converter/           # Conversion logic
│   ├── book.py          # L2 book reconstruction
│   ├── trades.py        # Trade conversion
│   └── instruments.py   # Instrument building
│
├── tests/               # Unit tests (run with pytest)
│   ├── test_bookbuilder.py
│   ├── test_depth_reconstruction_phase1.py
│   └── ...
│
├── scripts/             # Operational scripts
│   ├── smoke_test.py    # Quick 3-min recorder test
│   └── acceptance_test.py # Full pipeline test
│
├── docs/                # Documentation
│   ├── ARCHITECTURE.md  # System design
│   ├── VALIDATION.md    # Testing/validation details
│   └── GUARANTEES.md    # What Phase 1 guarantees
│
├── data_raw/            # Raw recorded data (gitignored)
├── state/               # Runtime state files
└── meta/                # Metadata storage
```

## Testing & Validation

| What | Command | When |
|------|---------|------|
| Setup validation | `python validate.py` | After cloning/setup |
| Unit tests | `pytest tests/` | After code changes |
| Smoke test | `python scripts/smoke_test.py` | Verify recorder works |
| Full acceptance | `python scripts/acceptance_test.py` | Release readiness |

## Conversion

Convert recorded data to Nautilus catalog:

```bash
python convert_day.py --date 2026-04-20

# Optional: Phase 2 deterministic replay -> OrderBookDeltas
python convert_day.py --date 2026-04-20 --depth-mode phase2
```

This produces:
- `TradeTick` objects from raw trades
- Phase 1: `OrderBookDepth10` snapshots from approximate L2 deltas
- Phase 2: `OrderBookDeltas` as the primary L2 output
- Phase 2 optional: derived `OrderBookDepth10`
- `CurrencyPair` / `CryptoPerpetual` instruments

## Phase 1 Scope

**What it does:**
- Records trades + L2 deltas via cryptofeed
- Converts to Nautilus-native format
- Ensures no crossed-book snapshots in catalog
- Tracks data quality metrics

**What it doesn't do:**
- Deterministic Binance U/u/pu replay
- Bit-exact order book reconstruction
- REST depth polling (causes rate limits)

See [docs/GUARANTEES.md](docs/GUARANTEES.md) for full details.

## Phase 2 Scope

Phase 2 keeps the repo workflow familiar while changing the L2 truth model:

- raw source of truth becomes Binance-native `depth_v2`
- snapshot seeding and sync/resync state are explicit
- deterministic replay writes Nautilus `OrderBookDeltas`
- derived `OrderBookDepth10` stays optional and off by default

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — System design and pipeline
- [Validation](docs/VALIDATION.md) — Testing layers and checks
- [Guarantees](docs/GUARANTEES.md) — Phase 1 scope boundaries
- [Installation](INSTALL.md) — Detailed setup guide
