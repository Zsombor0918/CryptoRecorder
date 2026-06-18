# Feature Store Requirements Audit

> Honest status snapshot of the v0 feature store against its intended
> requirements. "Status" uses: **met**, **partial**, **deferred**. Evidence
> points at code/tests/docs that back the claim. This audit does not change
> behavior; it records where the feature store actually stands.

The feature store is an **AI/selection analysis layer**, not a backtest data
source. The validated backtest path is the Nautilus catalog (see
[VALIDATION.md](VALIDATION.md)). Nothing here promotes the feature store to a
catalog replacement.

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
| 12 | Storage footprint understood | partial | 1m parquet is tens of KiB/day for ADAUSDT (see [STORAGE_SIZE_AUDIT.md](STORAGE_SIZE_AUDIT.md)) | Multi-symbol/timeframe storage projection pending |

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
