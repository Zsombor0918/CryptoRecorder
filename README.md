# CryptoRecorder

CryptoRecorder is a deterministic Binance market-data pipeline for backtesting
with [Nautilus Trader](https://nautilustrader.io/). It records native Binance
WebSocket depth and trade streams and converts them to a queryable
`ParquetDataCatalog`.

**Target:** 50 instruments (spot + USDT-M futures) with deterministic L2 depth
replay and native trades.

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
```

## Project Structure

```
CryptoRecorder/
├── recorder.py          # Main recorder (starts here)
├── phase2_depth.py      # Native Binance depth recorder
├── native_trades.py     # Native Binance trade recorder
├── convert_day.py       # Convert raw data to Nautilus catalog
├── validate.py          # Setup validation (run on new machine)
│
├── converter/           # Conversion logic
│   ├── trades.py        # trade_v2 → TradeTick
│   ├── depth_phase2.py  # depth_v2 → OrderBookDeltas (Decimal book state)
│   └── instruments.py   # Instrument building from exchangeInfo
│
├── tests/               # Unit tests (run with pytest)
│   ├── test_depth_deterministic.py
│   ├── test_trade_deterministic.py
│   ├── test_converter_integration.py
│   └── ...
│
├── scripts/             # Operational scripts
│   ├── smoke_test.py    # Quick 3-min recorder test
│   └── acceptance_test.py # Full pipeline test
│
├── docs/                # Documentation
│   ├── ARCHITECTURE.md  # System design
│   ├── SCHEMAS.md       # Raw and state file schemas
│   ├── VALIDATION.md    # Testing layers & checks
│   └── GUARANTEES.md    # Scope boundaries
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
# Convert yesterday (default)
python convert_day.py

# Convert specific date
python convert_day.py --date 2026-04-20

# Enable optional derived OrderBookDepth10
python convert_day.py --date 2026-04-20 --emit-depth10
```

This produces:
- `TradeTick` objects from raw `trade_v2`
- `OrderBookDeltas` from deterministic `depth_v2` replay (Decimal book state)
- Optional: derived `OrderBookDepth10` (off by default)
- `CurrencyPair` / `CryptoPerpetual` instruments from exchangeInfo

## Key Design Decisions

- **No cryptofeed** — native Binance WebSocket connections via aiohttp
- **Committed-only ordering** — `session_seq` / `trade_session_seq` counters
  allocated only for committed records, giving deterministic replay from raw JSONL
- **Exact Decimal book state** — no float rounding during depth replay
- **Tagged union trades** — `market_type` discriminator for spot vs futures fields
- **OrderBookDeltas as primary** — `OrderBookDepth10` is optional and derived

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — System design and pipeline
- [Schemas](docs/SCHEMAS.md) — Raw and state file schemas
- [Validation](docs/VALIDATION.md) — Testing layers & checks
- [Guarantees](docs/GUARANTEES.md) — Scope boundaries
- [Operations](docs/OPERATIONS.md) — Operations guide
- [Installation](INSTALL.md) — Detailed setup guide
