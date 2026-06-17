# CryptoRecorder

CryptoRecorder records native Binance spot and USDT-M futures market data for
deterministic Nautilus Trader backtesting.

The repo currently has two catalog-related paths:

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
  validated production/full-L2 path

data_raw -> replay_store -> feature_store
data_raw -> replay_store -> generate_catalog --profile trades_only
  validated v0 replay/feature foundation
```

The next milestone is replay-based full-L2 catalog generation:

```text
data_raw -> replay_store -> generate_catalog --profile full_l2
```

That full-L2 replay path is not implemented yet. Until it is semantically
validated against `convert_day.py`, the old converter remains the source of
truth for full-L2 Nautilus catalogs.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python validate.py
pytest
```

Run the recorder:

```bash
python recorder.py
```

Convert one UTC day with the validated full-L2 converter:

```bash
python convert_day.py --date 2026-06-12 --staging
```

Build the replay/feature v0 path with explicit temp roots:

```bash
python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root /tmp/replay_store

python -m pipeline.build_feature_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --replay-root /tmp/replay_store \
  --feature-root /tmp/feature_store

python -m pipeline.generate_catalog \
  --input /tmp/replay_store \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --date 2026-06-12 \
  --profile trades_only \
  --output /tmp/catalog_jobs \
  --job-id validation_trades \
  --overwrite
```

## Main Components

| Path | Purpose |
| --- | --- |
| `recorder.py` | Main raw recorder entrypoint |
| `phase2_depth.py` | Native Binance depth recorder |
| `native_trades.py` | Native Binance trade recorder |
| `storage.py` | Hourly JSONL(.zst) raw writer |
| `convert_day.py` | Validated raw -> Nautilus full-L2 converter |
| `converter/` | Legacy converter implementation |
| `stores/` | Replay and feature Parquet schemas/readers/writers |
| `pipeline/` | Replay, feature, catalog, audit, and equivalence CLIs |
| `validation/` | Semantic catalog comparison utilities |
| `scripts/` | Manual recorder and legacy-converter smoke scripts |
| `docs/` | Detailed documentation |

## Documentation

Start with [docs/README.md](docs/README.md).

Key references:

- [Architecture](docs/ARCHITECTURE.md)
- [Storage Architecture](docs/STORAGE_ARCHITECTURE.md)
- [Replay Store](docs/REPLAY_STORE.md)
- [Feature Store](docs/FEATURE_STORE.md)
- [Generate Catalog](docs/GENERATE_CATALOG.md)
- [Implementation Audit](docs/IMPLEMENTATION_AUDIT.md)
- [Full-L2 Replay Plan](docs/FULL_L2_REPLAY_CATALOG_PLAN.md)
- [Operations](docs/OPERATIONS.md)
- [Installation](INSTALL.md)

## Current Guarantees

- Raw recorder behavior and raw layout are unchanged.
- `convert_day.py` remains the validated full-L2 Nautilus converter.
- Replay store preserves exact price/quantity strings and depth continuity
  fields needed for future full-L2 reconstruction.
- Feature store is UTC-day clamped and sparse.
- `generate_catalog --profile trades_only` can be validated against the old
  converter for TradeTick semantic equality.
- Replay-based `full_l2` catalog generation is deferred until the next milestone.
