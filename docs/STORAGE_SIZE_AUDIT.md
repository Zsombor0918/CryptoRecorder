# Storage Size Audit

> **Scope honesty**: every number below is **measured from a single
> venue/symbol/date** (`BINANCE_SPOT/ADAUSDT/2026-06-12`). Universe-level
> figures are **rough extrapolations, not benchmarks**. Liquidity (and therefore
> depth-update volume) varies by one to two orders of magnitude across the
> universe, so do not budget production storage from this page alone — re-measure
> on a representative symbol sample before scaling.

## How to reproduce

```bash
python -m validation.audit_storage_size \
  --venue BINANCE_SPOT --symbol ADAUSDT --date 2026-06-12 \
  --replay-root <replay_store_root> \
  --catalog-root <generated job_* catalog root>
```

The CLI is audit-only (it lives in `validation/`); it reads file sizes and writes
nothing.

## Measured: ADAUSDT 2026-06-12 (1 symbol, 1 UTC day)

Record counts (from the replay build / catalog generation logs):

| Stage | depth records | trade records | depth10 snapshots |
|---|---:|---:|---:|
| raw (`data_raw`) | 412,336 | 124,457 | — |
| replay_store | 412,336 | 124,457 | — |
| full_l2 catalog | 1,231,284 deltas | 124,457 trades | 71,341 |

Note: one raw/replay depth record expands to several individual
`OrderBookDelta` rows (one per changed price level), which is why the flattened
delta count (1.23M) is larger than the depth-record count (412K).

On-disk bytes:

| Artifact | Size |
|---|---:|
| raw depth (`.jsonl.zst`) | 19.0 MiB |
| raw trades (`.jsonl.zst`) | 3.5 MiB |
| **raw total** | **~22.5 MiB** |
| replay `depth.parquet` | 32.9 MiB |
| replay `trades.parquet` | 6.8 MiB |
| **replay_store total** | **~39.7 MiB** |
| catalog `trade_tick` | 2.6 MiB |
| catalog `order_book_deltas` | 22.5 MiB |
| catalog `order_book_depths` (Depth10) | 6.9 MiB |
| **full_l2 catalog total** | **~32.1 MiB** |

The `convert_day.py` full-L2 catalog for the same day measured **33 MiB**, i.e.
the replay `full_l2` catalog is the **same size class** as the validated
reference (it carries the same TradeTick / OrderBookDeltas / Depth10 content —
see [VALIDATION.md](VALIDATION.md) and the equivalence report under
`validation_reports/`).

Feature store (1m, same day): a sparse UTC-day file with **1,428 of 1,440**
possible one-minute rows populated (12 empty minutes skipped). The 1m parquet is
small (tens of KiB) relative to the L2 catalog and is not the storage driver.

## Rough universe extrapolation (NOT a benchmark)

ADAUSDT is a moderately liquid mid-cap. Treating it as a single sample and
scaling linearly by symbol count gives only an order-of-magnitude estimate:

| Quantity (per day) | ADAUSDT measured | × 50 (rough) |
|---|---:|---:|
| raw (compressed) | ~22.5 MiB | ~1.1 GiB |
| replay_store | ~39.7 MiB | ~1.9 GiB |
| full_l2 catalog | ~32.1 MiB | ~1.6 GiB |

Caveats that make the ×50 column unreliable:

- The most active majors (e.g. BTC/ETH) produce far more depth updates per day
  than ADAUSDT; the least active symbols produce far fewer.
- Futures (`BINANCE_USDTF`) carry a different update cadence than spot.
- A real budget must measure a stratified sample across the liquidity spectrum,
  not multiply one mid-cap by the symbol count.

## Status

- **Measured**: ADAUSDT single-day replay + full_l2 catalog footprint.
- **Pending**: a stratified multi-symbol storage benchmark across the top50 and a
  multi-day projection. Until that exists, production storage sizing is an open
  question, not a settled number.
