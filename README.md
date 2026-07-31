# CryptoRecorder

CryptoRecorder records native Binance spot and USDT-M futures market data for
deterministic Nautilus Trader backtesting.

The repo currently has these catalog-related paths:

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
  production reference full-L2 path

data_raw -> replay_store
  validated v0 replay layer; the stable external contract handed off to
  downstream repositories (e.g. KovacsTrader)

replay_store -> pipeline.reconstruct_selected_catalog -> temporary Nautilus catalog
  supported development-computer CLI/API with explicit venue, symbol,
  end-exclusive UTC window, output root, job ID, and profile; it wraps the
  internal validation.replay_catalog_reconstruct engine and publishes only a
  checksum-bound job directory
```

CryptoRecorder does not build a feature-store, label-store, or
permanent or unscoped consumer catalog from `replay_store` (removed, issue
#17). Feature, strategy, risk, execution, and backtest orchestration remain
downstream responsibilities.

The `full_l2` reconstruction reuses the old converter's shared depth engine. It
passes the ADAUSDT single-day smoke against `convert_day.py`, but **broader
top50/multi-day validation is still pending** — that wider validation is the
`v2.0.0` gate, and `v2.0.0` is not declared. `convert_day.py` remains the
production reference for full-L2 Nautilus catalogs.

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

Build the replay v0 path with explicit temp roots:

```bash
python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root /tmp/replay_store
```

Reconstruct a selected temporary catalog on the development computer:

```bash
python -m pipeline.reconstruct_selected_catalog \
  --replay-root /path/to/replay_store \
  --venues BINANCE_SPOT \
  --symbols ADAUSDT BTCUSDT \
  --start 2026-06-11T12:00:00Z \
  --end 2026-06-12T00:00:00Z \
  --output-root /external/temporary/catalog_jobs \
  --job-id selected-20260611 \
  --profile full_l2
```

The interval is `[start,end)`. The command never expands an empty selection,
refuses unsafe/existing job paths by default, preflights all replay artifacts,
and atomically publishes `<output-root>/<job-id>/` with `job_manifest.json`.
Use `--overwrite` only to replace that exact completed job.

To validate replay-store equivalence against `convert_day.py`, use:

```bash
python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --old-catalog-root /tmp/cryptorecorder-equivalence/old_catalog \
  --replay-root /tmp/cryptorecorder-equivalence/replay_store \
  --new-catalog-root /tmp/cryptorecorder-equivalence/new_catalog \
  --profile trades_only \
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
| `stores/` | Replay Parquet schemas/readers/writers (no feature/label schemas) |
| `pipeline/` | Replay build/transform CLIs plus explicitly selected temporary-catalog reconstruction |
| `validation/` | Audit, equivalence check, and catalog inspection CLIs |
| `scripts/` | Manual recorder and legacy-converter smoke scripts |
| `docs/` | Detailed documentation |

## Documentation

Start with [docs/README.md](docs/README.md).

Key references:

- [Project Status](docs/PROJECT_STATUS.md) — validated vs deferred.
- [Repo Structure](docs/REPO_STRUCTURE.md) — frozen folder contract.
- [Architecture](docs/ARCHITECTURE.md) — design, storage layers, guarantees.
- [Operations](docs/OPERATIONS.md) — deployment, Linux server, state schemas.
- [Implementation Audit](docs/IMPLEMENTATION_AUDIT.md) — ground-truth, audit history.
- [Replay Store](docs/REPLAY_STORE.md)
- [Full-L2 Replay Plan](docs/FULL_L2_REPLAY_CATALOG_PLAN.md)
- [AI Workflow](docs/AI_WORKFLOW.md) · [Versioning Policy](CHANGELOG.md) · [Docs Index](docs/README.md)
- [Installation](INSTALL.md)

Agent rules: [AGENTS.md](AGENTS.md). Version: see [VERSION](VERSION) and [CHANGELOG.md](CHANGELOG.md).

## Current Guarantees

- Raw recorder behavior and raw layout are unchanged.
- `convert_day.py` remains the validated full-L2 Nautilus converter.
- Replay store preserves exact price/quantity strings and depth continuity
  fields needed for full-L2 reconstruction, and is the stable external
  contract handed off to downstream repositories (e.g. KovacsTrader).
- CryptoRecorder does not build a feature-store, label-store, persistent
  catalog service, or unscoped consumer catalog from replay_store.
- `pipeline.reconstruct_selected_catalog` is the supported development-
  computer boundary for explicit, job-scoped temporary catalogs; the
  reconstruction engine remains `validation.replay_catalog_reconstruct`.
- The internal `validation.replay_catalog_reconstruct` `trades_only` profile
  can be validated against the old converter for TradeTick semantic equality
  via `validation.validate_catalog_equivalence`.
- The `full_l2` profile of the same validation-only helper is implemented and
  semantically validated on the ADAUSDT single-day smoke against `convert_day.py`
  (trades, OrderBookDeltas, OrderBookDepth10, and book checkpoints all match).
  Broader top50/multi-day validation is pending before `v2.0.0`.
