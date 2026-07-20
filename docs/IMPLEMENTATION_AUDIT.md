# Implementation Audit

## Summary

CryptoRecorder's scope is Binance native market streams -> `data_raw` ->
deterministic `replay_store` (the stable contract consumed by downstream
repositories, e.g. KovacsTrader), plus the legacy `convert_day.py` full-L2
converter. It does not own a feature-store, label-store, or general-purpose
catalog-generation service (issue #17 removed the former feature-store
subsystem and the `pipeline/generate_catalog.py` product CLI).

The internal `validation.replay_catalog_reconstruct` helper (no CLI, used only
by `validation.validate_catalog_equivalence`) is **semantically validated on
the ADAUSDT single-day smoke** against `convert_day.py`. Broader top50/multi-day
validation is still pending. Old `convert_day.py` remains the production
reference full-L2 path.

Current paths:

```text
data_raw -> convert_day.py -> Nautilus catalog
  validated full-L2 path

data_raw -> replay_store
  implemented v0; the stable external contract for downstream repositories

replay_store -> validation.replay_catalog_reconstruct (validation-only, no CLI)
  implemented; semantically validated on the ADAUSDT single-day smoke
  vs convert_day.py. Broader top50/multi-day validation pending.
```

## What Works

Replay store:

- Builds Hive-style partitions under `venue=.../symbol=.../date=...`.
- Writes `depth.parquet`, `trades.parquet`, `instrument.json`, and `manifest.json`.
- Sorts depth records by `(stream_session_id, session_seq, raw_index)`.
- Sorts trade records by `(trade_stream_session_id, trade_session_seq, raw_index)`.
- Preserves exact trade `price_str` / `quantity_str`.
- Preserves exact depth level `price_str` / `size_str`.
- Preserves depth continuity fields `U`, `u`, and `pu` where raw provides them.
- Writes SHA256 checksums after Parquet staging files are written.
- Publishes completed staging directories atomically.
- Supports custom temp roots through `stream_raw_records(..., root=...)`.

Catalog reconstruction (validation-only, `validation/replay_catalog_reconstruct.py`):

- Supports `trades_only`, `full_l2`, `depth_only`, and `depth10` profiles.
- `full_l2`/`depth_only`/`depth10` reuse the shared depth engine in
  `converter/depth_phase2.py` via `stores/replay_depth_adapter.py` (no second
  depth converter); the manifest records `depth_diagnostics`, `fenced_ranges`, and
  `equivalence_caveats`.
- It uses exact replay strings for Nautilus `Price` and `Quantity`.
- It supports `--date`-equivalent `start`/`end` shortcuts (UTC-day window) via
  its Python API (there is no CLI; it is invoked only from
  `validation.validate_catalog_equivalence` and from tests).
- It supports deterministic `job_id` and safe `overwrite`.
- It writes coverage fields for requested/found/missing partitions, records read,
  records written, skipped invalid records, and `time_filter=ts_init`.
- It rejects unknown profiles.

Validation:

- `validation.validate_catalog_equivalence` builds old and new catalogs and compares semantic TradeTick equality.
- It compares instrument IDs, counts, timestamp ranges, first/last/sample trades, price, size, side, `trade_id`, `ts_event`, and `ts_init`.
- For `full_l2` it also compares `OrderBookDeltas` (multiset-semantic),
  `OrderBookDepth10`, and reconstructed top-10 book checkpoints.
- It writes JSON reports and exits nonzero on failed comparison.

## Tested

Automated tests cover:

- CLI help without touching unwritable default `/data` roots.
- Custom raw root reading.
- Replay manifest counts and sorted records.
- Replay audit counts, checksums, ordering, continuity null ratios, and exact
  nested price/size field presence.
- Exact replay decimal preservation.
- Depth `U/u/pu` preservation.
- `validation.replay_catalog_reconstruct` exclusive end behavior.
- Nautilus readability for generated trades-only catalogs.
- Deterministic job id and overwrite behavior.
- Synthetic trades-only catalog semantic comparison.
- Synthetic full-L2 convert_day-vs-replay semantic equivalence (clean bootstrap day).
- Replay depth adapter mapping and canonical re-sort.
- `full_l2`/`depth_only`/`depth10` profile write-flags and manifest diagnostics.
- Validator skips unsupported profiles (`depth10`).
- Real-data equivalence behind `pytest.mark.realdata`.

Last full local suite:

```text
240 passed, 3 skipped
```

## Smoke-Tested

Real local validation was run for:

```text
date: 2026-06-12
venue: BINANCE_SPOT
symbol: ADAUSDT
profile: trades_only
```

Result:

```text
status: passed
instrument_ids_match: true
old trades: 124457
new trades: 124457
timestamp_range_match: true
sample_mismatches: 0
```

Full-L2 real local validation was run for the same date/venue/symbol:

```text
profile: full_l2
status: passed
trade_ticks         old 124457   new 124457   range match  0 mismatches
order_book_deltas   old 1231284  new 1231284  range match  0 mismatches
order_book_depth10  old 71341    new 71341    range match  0 mismatches
book checkpoints    7/7 match, no crossed books
```

Report: `validation_reports/full_l2_equivalence_2026-06-12_ADAUSDT.json` (local,
gitignored). This is a single-symbol, single-day smoke — not a universe benchmark.

Feature audit for `2026-06-12`, `BINANCE_SPOT/ADAUSDT`, `1m`:

```text
actual_row_count: 1428
expected_dense_row_count: 1440
outside_date_rows: 0
duplicate_timestamp_count: 0
missing_windows_count_if_dense: 12
all-null examples: return_1s, return_5s, return_10s, return_30s, return_1m
```

This confirms the feature store is UTC-day clamped and sparse. It does not force a dense full-day grid.

Report path from the local run:

```text
/tmp/cryptorecorder-equivalence-smoke-20260616/catalog_equivalence_2026-06-12.json
```

## Deferred

Broader `full_l2` validation across the top50 universe and multiple days is not
yet done. The `full_l2` path is implemented and passes the ADAUSDT single-day
smoke (see Smoke-Tested above), but it is **not** declared `v2.0.0` until the
wider validation passes.

Pending validation path:

```text
top50 + multi-day:
data_raw -> convert_day.py
must match
data_raw -> replay_store -> validation.replay_catalog_reconstruct (full_l2, validation-only)
```

That wider validation compares the same fields proven on the ADAUSDT smoke:

- instruments;
- TradeTick count and sampled equality;
- OrderBookDeltas count;
- first, last, and sampled deltas;
- action, side, price, size, order_id, flags, and sequence where applicable;
- reconstructed book checkpoints;
- top 10 bid/ask equality at checkpoints;
- gap/fenced range report equality or documented acceptable differences;
- OrderBookDepth10 semantic equality when emitted.

See [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md) for the
documented equivalence boundary (which old-converter internals are intentionally
not reproduced byte-for-byte).

## Known Limitations

- `ReplayWriter` still accumulates one symbol/date in memory before writing.
- `validation.replay_catalog_reconstruct` uses replay `instrument.json` only when it contains
  exchangeInfo-shaped metadata (`filters` or `exchange_info`). Current
  normalized v0 metadata otherwise falls back to the existing converter defaults.
- The real-data equivalence test is gated by environment variables and is not part of normal CI.

## Reproduce One-Day Smoke

```bash
BASE=/tmp/cryptorecorder-replay-feature-validation
rm -rf "$BASE"
mkdir -p "$BASE"

python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root "$BASE/replay_store"
```

The `validation.replay_catalog_reconstruct` helper has no CLI; it is exercised
via `python -m validation.validate_catalog_equivalence` (see below) or directly
from Python/tests.

## Run Old-vs-New Validation

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

## Audit Replay Output

```bash
python -m validation.audit_replay_store \
  --replay-root /tmp/cryptorecorder-replay-feature-validation/replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT
```

The audit reports manifest-vs-Parquet counts, checksums, deterministic
sortedness, duplicate sequence keys, timestamp ranges, `U/u/pu` null ratios,
trade exact string null ratios, and depth nested exact string field presence.

## Next Milestone

Next milestone:

```text
data_raw -> convert_day.py
semantically matches
data_raw -> replay_store -> validation.replay_catalog_reconstruct (full_l2, validation-only)
```

Only after broader top50/multi-day validation passes should the project
consider replay-store raw archival policy. CryptoRecorder does not build a
feature/label layer of its own (issue #17); that responsibility belongs to
downstream consumer repositories such as KovacsTrader.


---

## Repo Cleanup History

> Content merged from the former `IMPLEMENTATION_AUDIT.md`.

Date: 2026-06-16

## Summary

This cleanup pass keeps the repository understandable before the next full-L2
replay milestone. It does not change recorder behavior, raw schemas, raw
layout, raw retention, or the legacy `convert_day.py` full-L2 converter.

No code files were deleted or moved in this pass. The only script cleanup was
wording: existing script names were kept, but their scope is now explicit.

## A. Core Recorder Runtime

These files are the live raw recording path and should stay root-level or in
their existing packages:

- `recorder.py`
- `phase2_depth.py`
- `native_trades.py`
- `storage.py`
- `binance_universe.py`
- `health_monitor.py`
- `disk_monitor.py`
- `config.py`
- `time_utils.py`
- `validate.py`

Decision: keep. Do not alter recorder semantics during replay cleanup.

### Addendum (2026-07-20, issue #19): `disk_monitor.py` fail-safe measurement

`disk_monitor.py` was rewritten to fix a false-zero reporting defect: a failed
or timed-out recursive `du` scan previously returned numeric `0.0`, which was
then published as `data_raw_gb=0.0` and silently disabled capacity alerts and
automatic cleanup. It now returns a structured `DirectoryMeasurement`
(`ok`/`status`/`error`), falls back to a persisted last-known-good value
marked `stale` (or `null` if none exists), reports independent filesystem
capacity via `shutil.disk_usage()`, and fails closed in `cleanup_old_data()`
whenever the current `data_raw` measurement is not fresh and successful. See
`docs/ARCHITECTURE.md` ("Disk Monitoring Safety Invariant") and
`docs/OPERATIONS.md` ("Disk Monitoring") for the full field/threshold
reference, and `tests/test_disk_monitor_fail_safe.py` for coverage. This
addendum does not change the status of the replay/feature/full_l2 items
described elsewhere in this document, which predate issue #17's removal of
the feature-store subsystem and are tracked separately.

## B. Legacy Full-L2 Converter

This remains the validated full-L2 Nautilus catalog path:

- `convert_day.py`
- `converter/`
- converter-focused tests such as:
  - `tests/test_convert_day_phase2.py`
  - `tests/test_converter_integration.py`
  - `tests/test_depth_deterministic.py`
  - `tests/test_trade_deterministic.py`
  - `tests/test_staging_publish.py`
  - `tests/test_streaming_conversion_memory.py`

Decision: keep. `convert_day.py` is still the source of truth for full-L2
catalog behavior until replay-based full-L2 is implemented and validated.

## C. New Replay Pipeline

Validated v0 foundation (updated 2026-07-15 — see "Completed Cleanup Items"
below: issue #17 removed the feature-store subsystem and the
`generate_catalog` product CLI from this list):

- `stores/`
  - `replay_schema.py`
  - `replay_writer.py`
  - `replay_reader.py`
- `pipeline/`
  - `raw_manifest.py`
  - `build_replay_store.py`
  - `daily_build.py`
- `validation/`
  - `catalog_compare.py`
  - `audit_replay_store.py`
  - `validate_catalog_equivalence.py`
  - `replay_catalog_reconstruct.py` (validation-only, no CLI; formerly
    `pipeline/generate_catalog.py`)
  - `catalog_inspect.py`
  - `phase2_report.py`

Decision: keep. The `full_l2` reconstruction profile is implemented and
validated on the ADAUSDT smoke; broader validation is deferred.

## D. Operational Scripts

Current scripts:

- `scripts/smoke_test.py` — recorder-only smoke test.
- `scripts/acceptance_test.py` — legacy recorder + `convert_day.py` acceptance.
- `scripts/README.md` — script scope documentation.

Decision: keep filenames to avoid operational churn. Updated wording so
`acceptance_test.py` no longer claims to validate replay full-L2.

Future optional cleanup:

- Add `scripts/smoke_replay_feature.py` if a shell-level replay/feature smoke is
  useful after the full-L2 replay milestone.
- Add `scripts/validate_trades_only_equivalence.py` only if the CLI wrapper adds
  value beyond `python -m validation.validate_catalog_equivalence`.

## E. Tests

Normal unit/integration tests live under `tests/`.

Current grouping by purpose:

- Recorder/raw runtime:
  - `tests/test_native_trades_ingest.py`
  - `tests/test_storage_rotation.py`
  - `tests/test_writer_backpressure.py`
  - heartbeat/universe/disk-monitor tests
- Legacy converter:
  - converter, depth, trade, staging, purge, and memory tests
- Replay/feature/catalog v0:
  - `tests/test_pipeline_validation.py`
- Semantic equivalence guards:
  - `tests/test_semantic_equivalence.py`

Decision (updated): the `full_l2` replay path is now implemented and validated on
the ADAUSDT single-day smoke, so the former "skipped because deferred" test was
replaced by real synthetic + real-data equivalence tests
(`tests/test_catalog_equivalence_full_l2.py`, the synthetic full-L2 test in
`tests/test_catalog_equivalence.py`, and the `realdata`-gated
`tests/test_full_l2_realdata_gate.py`). Misleading manual/TODO skip piles were
removed. Real-data equivalence stays behind `pytest.mark.realdata`.

## F. Generated, Local, Or Trash

These should not be committed:

- Python/tool caches:
  - `__pycache__/`
  - `.pytest_cache/`
  - `.mypy_cache/`
  - `.ruff_cache/`
- Raw/runtime data:
  - `data_raw/`
  - `meta/`
  - `state/`
  - `*.log`
  - `*.jsonl`
  - `*.jsonl.zst`
  - `*.jsonl.gz`
- Generated replay/feature/catalog artifacts:
  - `*.parquet`
  - `replay_store/`
  - `feature_store/`
  - `catalog_jobs/`
  - `daily_reports/`
  - `validation_reports/`
  - `.staging_*`
  - `*.staging.*`

Decision: update `.gitignore` for generated replay/feature/catalog outputs and
tool caches. Generated Python/test caches were removed locally. Runtime `state/`
files and logs were left in place to avoid deleting potentially useful local
operator context. No source code was deleted in this pass.

## Current Clean Organization

Recommended root-level Python entrypoints:

- `recorder.py` — raw recorder.
- `convert_day.py` — validated full-L2 converter.
- `config.py` — lazy configuration and roots.
- `validate.py` — setup validation.

New pipeline code remains in:

- `pipeline/`
- `stores/`
- `validation/`

Docs remain in:

- `docs/`

Tests remain in:

- `tests/`

Manual scripts remain in:

- `scripts/`

## Completed Cleanup Items (2026-06-17 structure stabilization)

- `validators/` removed. `trade_coverage.py` moved to `converter/trade_coverage.py`.
  `catalog_inspect.py` and `phase2_report.py` moved to `validation/`.
- `pipeline/audit_feature_store.py`, `pipeline/audit_replay_store.py`, and
  `pipeline/validate_catalog_equivalence.py` moved to `validation/`.
- `tests/test_pipeline_validation.py` split into focused files:
  `test_replay_store.py`, `test_feature_store.py`, `test_generate_catalog.py`,
  `test_catalog_equivalence.py`.
- `tests/test_repo_structure.py` added to enforce folder contract.
- `docs/REPO_STRUCTURE.md` created as the binding structure contract.

## Open Cleanup Items

- Consider renaming `scripts/acceptance_test.py` to
  `scripts/acceptance_legacy_converter.py` in a future low-risk PR if operators
  are not depending on the old filename.

## Completed Cleanup Items (2026-07-15 — issue #17 recorder + replay-store ownership refactor)

- Removed the feature-store subsystem entirely: `stores/feature_schema.py`,
  `stores/feature_calc.py`, `stores/feature_writer.py`,
  `pipeline/build_feature_store.py`, `validation/audit_feature_store.py`,
  `tests/test_feature_store.py`, and the `cryptorecorder-feature-build`
  systemd service + timer.
- Removed `pipeline/generate_catalog.py` as a product/runtime CLI. Its
  reconstruction logic moved to `validation/replay_catalog_reconstruct.py`, an
  internal, CLI-less helper used only by
  `validation.validate_catalog_equivalence`.
- Removed `config.py` roots `FEATURE_ROOT`, `LABEL_ROOT`, and
  `CATALOG_JOBS_ROOT`.
- Simplified `pipeline.daily_build` to replay-only: dropped `--steps`,
  `--timeframes`, and `--feature-root` CLI flags.
- Deleted `docs/FEATURE_STORE.md` and `docs/GENERATE_CATALOG.md`; the docs/
  fixed count dropped from 14 to 12 files.
- Superseded issue #15 (a `generate_catalog` product-CLI proposal) in favor of
  the narrower recorder + replay-store ownership boundary.

See `CHANGELOG.md` `[Unreleased]` for the full change list.

---

## Feature Store Requirements Audit (REMOVED — issue #17)

> **This subsystem no longer exists.** The table below is preserved as a
> historical record of the feature-store's status at the time it was removed
> (issue #17: recorder + replay-store ownership refactor). CryptoRecorder does
> not build a feature-store or label-store layer; that responsibility belongs
> to downstream consumer repositories (e.g. KovacsTrader). Do not recreate any
> of the files referenced below without a new, explicit task.

> Content merged from the former `IMPLEMENTATION_AUDIT.md`.

> Honest status snapshot of the v0 feature store against its intended
> requirements, as it stood immediately before removal. "Status" uses:
> **met**, **partial**, **deferred**. Evidence points at code/tests/docs that
> backed the claim at the time.

The feature store was an **AI/selection analysis layer**, not a backtest data
source. The validated backtest path remains the Nautilus catalog (see
[VALIDATION.md](VALIDATION.md)).

## Requirements

| # | Requirement | Status | Evidence | Gap / Next action |
|---|---|---|---|---|
| 1 | Build features from `replay_store` only (no raw, no future data) | met | `pipeline/build_feature_store.py` reads `replay_store/.../depth.parquet`+`trades.parquet`; [FEATURE_STORE.md](FEATURE_STORE.md) "Inputs" | — |
| 2 | No look-ahead leakage (no future returns/labels/MAE/MFE/slippage) | met | No label/outcome fields in `stores/feature_schema.py::FEATURE_SCHEMA_CORE_V1`; leakage rule stated in [FEATURE_STORE.md](FEATURE_STORE.md) | Keep leakage guard in code review; no automated leakage test yet |
| 3 | Deterministic, UTC-day clamped windows | met | `--date` clamps to `[00:00:00Z, next 00:00:00Z)`; `timestamp_ns = window_end - 1ns` | — |
| 4 | Sparse windows documented (no dense-grid guarantee) | met (by design) | ADAUSDT 2026-06-12 1m = 1,428/1,440 rows; dense counts labeled "audit references" | Dense-grid output is **deferred**, not a bug |
| 5 | Core L1/L2 microstructure fields (spread, imbalance, notionals) | met | Schema fields `best_bid…imbalance_top50`; computed in `stores/feature_calc.py` | — |
| 6 | Trade-flow fields (signed volume, aggressor ratios) | met | Schema `trade_count…large_trade_count`; `buyer_maker` maps to aggressor side | — |
| 7 | Return / realized-vol / jump fields | partial | Fields exist in schema but are placeholders/simplified in v0 | Implement rolling history windows; nulls are expected for now |
| 8 | Quality fields (gaps, crossed book, stale, latency) | partial | Schema `depth_update_count…latency_ms_p95` present | Some fields summarized only; verify population per field |
| 9 | Advanced schema (`FEATURE_SCHEMA_ADVANCED`) | deferred | Defined in code but **not written**; `FeatureWriter` writes core v1 only | Do not rely on advanced fields until writer emits them |
| 10 | Memory-safe for full top50 production | deferred | Builder loads one symbol/date into memory before aggregating | Streaming/windowed aggregation + RSS benchmark before scaling |
| 11 | Auditable output (row counts, nulls, dupes, crossed books) | met | `validation/audit_feature_store.py`; null-ratio/dup/crossed-book report | — |
| 12 | Storage footprint understood | partial | 1m parquet is tens of KiB/day for ADAUSDT (see [STORAGE_SIZE_AUDIT.md](IMPLEMENTATION_AUDIT.md)) | Multi-symbol/timeframe storage projection pending |

## Summary

- **Met**: input boundary, no-leakage construction, deterministic UTC-day
  windows, core microstructure + trade-flow fields, auditability.
- **Partial**: return/vol/jump placeholders, some quality fields, storage
  projection.
- **Deferred**: advanced schema emission, memory-safe streaming for the full
  universe, dense-grid output.

The feature store is **useful for validation and small runs today** and is
explicitly **not yet proven for full top50 production**. That limitation is
recorded here and in [FEATURE_STORE.md](FEATURE_STORE.md); it is not closed.

---

## Storage Size Audit

> Content merged from the former `IMPLEMENTATION_AUDIT.md`.

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
