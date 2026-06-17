# Repo Cleanup Audit

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

## C. New Replay/Feature Pipeline

Current validated v0 foundation:

- `stores/`
  - `replay_schema.py`
  - `replay_writer.py`
  - `replay_reader.py`
  - `feature_schema.py`
  - `feature_calc.py`
  - `feature_writer.py`
- `pipeline/`
  - `raw_manifest.py`
  - `build_replay_store.py`
  - `build_feature_store.py`
  - `generate_catalog.py`
  - `daily_build.py`
- `validation/`
  - `catalog_compare.py`
  - `audit_replay_store.py`
  - `audit_feature_store.py`
  - `validate_catalog_equivalence.py`
  - `catalog_inspect.py`
  - `phase2_report.py`

Decision: keep. `generate_catalog` is currently `trades_only`; full-L2 replay
catalog generation is deferred.

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

Decision: keep one clear skipped full-L2 replay test with the reason that
`generate_catalog full_l2` is deferred. Remove misleading manual/TODO skip piles.
Real-data equivalence stays behind `pytest.mark.realdata`.

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
