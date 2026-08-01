# Implementation Audit

## Summary

CryptoRecorder's scope is Binance native market streams -> `data_raw` ->
deterministic `replay_store` (the stable contract consumed by downstream
repositories, e.g. KovacsTrader), plus the legacy `convert_day.py` full-L2
converter. It does not own a feature-store, label-store, or general-purpose
catalog-generation service (issue #17 removed the former feature-store
subsystem and the `pipeline/generate_catalog.py` product CLI).

The internal `validation.replay_catalog_reconstruct` engine (no direct CLI,
shared by validation and `pipeline.reconstruct_selected_catalog`) is
**semantically validated on
the ADAUSDT single-day smoke** against `convert_day.py`. Broader top50/multi-day
validation is still pending. Old `convert_day.py` remains the production
reference full-L2 path.

Current paths:

```text
data_raw -> convert_day.py -> Nautilus catalog
  validated full-L2 path

data_raw -> replay_store
  versioned v0/v1/v2; intended production template explicitly requests v2

replay_store -> pipeline.reconstruct_selected_catalog -> temporary catalog
  supported explicit development-computer CLI/API wrapping the internal
  replay_catalog_reconstruct engine; broader top50/multi-day validation pending
```

## Issue #20 closure checkpoint 3 — authoritative uv environments

The dependency authority is now `pyproject.toml` plus committed `uv.lock`;
the former hand-maintained `requirements.txt` is removed. The application
retains its flat module layout and has no build backend (`tool.uv.package =
false`, `default-groups = []`). `VERSION` remains the sole application release
value; the neutral project metadata version exists only because standardized
project metadata requires a value and does not declare a release.

The import-derived direct dependency matrix is:

| Direct dependency | First-party consumer/contract | Environment | Constraint rationale |
|-------------------|-------------------------------|-------------|----------------------|
| `aiohttp` | recorder WebSocket/REST and universe resolution | production | direct imported runtime API; compatible floor retained |
| `numpy` | PyArrow `to_numpy()` during replay integrity validation | production | contractual optional interop that PyArrow does not install itself |
| `pyarrow` | schema-v2 writer, reader, checksums, routine/deep integrity | production | direct storage API; compatible floor retained |
| `zstandard` | compressed raw readers and recording/storage | production | direct codec API; compatible floor retained |
| `nautilus_trader` | reference converter, selected reconstruction, catalogs/equivalence | reconstruction | exact accepted compatibility pin `1.225.0` |
| `pytest` | test runner | development | test-only, bounded `<10` compatibility range |
| `pytest-asyncio` | async tests | development | test-only, bounded major range |

Pandas is not imported directly and is no longer declared; it remains a
Nautilus transitive dependency only. The supported range is CPython
`>=3.12,<3.15`, derived from the pinned Nautilus distribution and tested on
CPython 3.12.3 with uv 0.11.29. No Windows or macOS clean-environment claim is
made.

`converter.depth_repartition` now owns the dependency-free event-time bounds,
timestamp, and deduplication primitives previously reached through the
Nautilus-importing `converter.depth_phase2` module. Replay semantics are
unchanged; the import boundary lets recorder/lifecycle/replay production
modules load without Nautilus. The selected reconstruction error points to
`uv sync --frozen --no-default-groups --extra reconstruction`.

`validation.validate_dependency_environment` checks lock freshness and hash,
required/forbidden packages, imports, CLI help, exact Nautilus version, and an
optional tiny external schema-v2 production build/integrity smoke. The deploy
wrapper uses an operator-supplied uv executable, runs `uv lock --check`, syncs
only the frozen production selection into a same-parent candidate, validates
before promotion, and keeps `.venv/bin/python` as the systemd path. Legacy
`.venv` replacement is explicit, service-inactivity-gated, backup-preserving,
and rollback-capable. Runtime units never invoke uv. Repository templates have
not been installed or production-accepted.

Clean-environment acceptance used three new external uv environments under the
non-repository evidence root
`issue20_checkpoint3_uv_20260801T064955Z`.
The production selection excluded Nautilus/pytest and passed imports plus a
2-depth/1-trade schema-v2 routine/deep-integrity smoke. The reconstruction
selection pinned Nautilus 1.225.0, excluded pytest, and produced a readable
selected `full_l2` catalog with 1 TradeTick, 4 flattened deltas, and 1
Depth10. The development selection passed the 160-test focused set and the
full suite (834 passed, 3 skipped). Frozen sync/validation left lock SHA-256
`976451a3c49b0098bc6e620acb889aa9fb6aa8aaf8098d20db0416721ed1b5af`
unchanged. External acceptance JSON SHA-256:
`fde42fe0b6f2ba88294e093ab645e80a7490a8bcb9cc34e44e53344d9c118b01`.
The first production smoke remains preserved: it exposed missing NumPy in the
initial classification before any accepted result, prompting the direct
production declaration and a fresh successful attempt. No production system,
service, or repository `.venv` was changed.

## Issue #20 closure checkpoint 2 — replay lifecycle and operations boundary

The current branch implements the repository-side replay-build lifecycle; no
production system was changed.

- `pipeline.replay_lifecycle` owns one Linux `fcntl.flock` lock per replay
  root. It is nonblocking, validates lock type/ownership/link/mode, records
  run/process/root/repository metadata, and relies on kernel release after
  process death. Direct builds and daily/backlog orchestration use the same
  context; nested code never reacquires it.
- Before backlog/raw inspection, a bounded root-wide scan recognizes only
  canonical partitions plus staging, backup, quarantine, and lifecycle
  metadata. It restores one valid backup when unambiguous, quarantines stale
  staging/invalid canonical evidence, safely removes an obsolete valid backup
  only beside a valid canonical, preserves quarantine, and refuses corruption,
  symlinks, unknown entries, and multiple candidates.
- `pipeline.daily_build` scans 1–31 days oldest-first and builds no more than
  the configured 1–31 incomplete dates. Current artifacts, not prior reports,
  decide truth. Outcomes are `built`, `skipped_valid`,
  `deferred_not_ready`, `missing_required_raw`,
  `source_changed_rebuild_required`,
  `incompatible_schema_rebuild_required`, `recovered`, or `failed`. Anything
  outside built/skipped/recovered is nonzero.
- Schema 2 is the validated production configuration value and explicit
  checked-in service argument. Live source mismatch and legacy/incompatible
  partitions require their separate exact-partition flags. Corrupt replay is
  never an implicit rebuild candidate.
- Per-date and invocation JSON evidence is file-fsynced, atomically replaced,
  directory-fsynced, and includes the lock/run identity, policies, recovery,
  exact inventory/results/counts, record/size observations, timing, and exit
  classification. Report failure cannot produce process success.
- Replay monitoring is one bounded classified scan of canonical, staging,
  backup, quarantine, and lifecycle bytes. Capacity groups roots by actual
  device and includes raw/replay growth and transient pressure without adding
  free space twice. The retired persistent catalog root is absent.
- The unsafe single-channel raw deletion path is removed. Automatic retirement
  defaults disabled and remains non-mutating even if configured on; a
  proof-only paired depth/trade planner checks grace, open day, variants,
  D-1/D/D+1 replay validity/integrity, and exact source identity. A durable
  paired move journal/rollback/recovery implementation is not present and must
  be separately accepted before raw deletion can exist.
- The repository oneshot service retains `Restart=no`/23h timeout and now sets
  `MemoryMax=12G`, `MemorySwapMax=0`, schema 2, seven lookback days, and three
  build dates. Deployment dry-run validates these fields. This template has
  not been installed or production-validated, and the existing production
  replay root is not migrated.

The implementation does not change recorder ingestion, raw layout, replay
row ordering/precision/fields, schema-v2 reconstruction, or `convert_day.py`.
The isolated real lifecycle smoke and full practical suite evidence for this
checkpoint are recorded in the matching `docs/CHANGE_AUDIT.md` entry. The
focused checkpoint set passed 380 tests; the full suite passed 820 with 3
skipped. The external schema-v2 ADAUSDT smoke built 303,293 depth and 129,824
trade rows, passed routine/deep validation with zero anonymous trades, and
returned `skipped_valid` on its second run. Build peak was 1,196,359,680 bytes
under an effective 12 GiB/zero-swap scope, with zero memory pressure or OOM
events; summary SHA-256 is
`1aacc5f402f9a42c17fe6aac71b1c92cd3b77494d4d64e44357918be9d3c7561`.

## Issue #20 Phase 7 — BTWUSDT trade-identifier correction

The preserved `BINANCE_USDTF/BTWUSDT/2026-06-11` failure isolated one replay
normalization defect. All 1,371,217 raw trade events had null normalized
top-level identifiers because the futures recorder had switched to Binance's
ordinary `trade` stream, whose native identifier is `native_payload.t`.
The unchanged reference converter already falls back from
`exchange_trade_id` to that exact native `t`; replay normalization previously
did not, so its catalog reconstruction skipped every trade.

The correction is deliberately narrow:

- existing top-level `trade_id`, `agg_trade_id`, and `exchange_trade_id`
  values remain authoritative;
- only when every supported top-level identifier is absent, native Binance
  `trade` recovers exact `t` and native `aggTrade` recovers exact aggregate
  `a`;
- identifier text is preserved (`str(integer)` for integer input, exact
  lexical content for string input); no ordering key, index, hash, rounding,
  or synthetic value is substituted;
- `ReplayWriter.write_trades_batch()` rejects a row lacking both replay
  identifiers for every physical schema before it can be published, while
  reconstruction independently raises rather than treating the same defect
  as an invalid-row skip.

This is a normalization/builder change, not a physical schema change.
`BUILDER_VERSION_V1` is v1.2.1 and `BUILDER_VERSION_V2` is v2.0.1;
format/schema versions remain 1/1 and 2/2. Existing v1 compact partitions
remain readable/auditable under their recorded physical contract, and known
v2.0.0 partitions remain readable/auditable because their physical contract
is unchanged. Neither older builder is silently reused as current output;
current artifact-bound schema-v2 semantic validation requires v2.0.1.

Fresh external corrected-attempt evidence reused the preserved raw source and
unchanged reference catalog, rebuilt only the BTW replay/candidate, and passed:
1,371,172/1,371,172 exhaustive ordered TradeTicks;
11,507,066/11,507,066 flattened OrderBookDelta rows;
40,398/40,398 Depth10; 7/7 checkpoints; continuity exact (61 seeds, 3
resyncs, 0 desyncs, 249 fences); 249/249 fenced ranges with identical digest;
350,157 depth and 1,371,217 trade metadata rows exact; live source identity
equal to both manifest copies; routine and deep integrity. The rebuilt replay
has 1,371,217 non-null `trade_id` values, zero non-null `agg_trade_id` values,
zero anonymous rows, and all identifiers derive from native `trade.t`.
The 45 zero-quantity replay rows are preserved and both catalog paths exclude
them, yielding the exact 1,371,172 TradeTicks. Peak cgroup memory was
2,371,661,824 bytes with zero OOM/OOM-kill; final report SHA-256 is
`2ae29713f09dd10988566c10c3bb040ec55a0252d936a5e60e08032295af4d85`.

This paragraph records the state at the time of the correction. Subsequent
Issue #20 checkpoints recorded the completed representative matrix, accepted
Phase 7 storage build, lifecycle boundary, and authoritative uv migration.
Broader top50/multi-day release gating, Phase 8, production deployment,
transactional retention, and KovacsTrader remain pending.

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

Catalog reconstruction:

- Supports `trades_only`, `full_l2`, `depth_only`, and `depth10` profiles.
- `full_l2`/`depth_only`/`depth10` reuse the shared depth engine in
  `converter/depth_phase2.py` via `stores/replay_depth_adapter.py` (no second
  depth converter); the manifest records `depth_diagnostics`, `fenced_ranges`, and
  `equivalence_caveats`.
- It uses exact replay strings for Nautilus `Price` and `Quantity`.
- The internal engine accepts explicit UTC `start`/`end` through its Python API
  and is invoked by validation and the supported pipeline boundary.
- It supports deterministic `job_id` and safe `overwrite`.
- It writes coverage fields for requested/found/missing partitions, records read,
  records written, skipped invalid records, and `time_filter=ts_init`.
- It rejects unknown profiles.
- `pipeline.reconstruct_selected_catalog` exposes only `full_l2` and
  `trades_only`, requires explicit venue/symbol/[start,end)/output/job scope,
  rejects generic instrument fallback, revalidates exact replay identities
  before and after reconstruction, and atomically publishes a cryptographically
  inventoried temporary job.

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
834 passed, 3 skipped
```

The checkpoint-3 dependency/deployment/reconstruction/lifecycle/guard focus is
160 passed. Earlier selected-boundary evidence remains 84 passed and the
broader replay/reconstruction/carry/reader/guard set remains 222 passed,
2 skipped. A real
schema-v2 ADAUSDT five-minute full-L2 smoke published an artifact-bound job,
was readable with pinned Nautilus 1.225.0, and recorded 51.437 seconds wall
time plus 640,143,360 bytes peak under the 10 GiB/zero-swap wrapper with no
memory or OOM events. Its external job-manifest SHA-256 is
`7d3eef0020c210911d485dff5f1d9d933e55981c70b0a95edb9a3b13446011ff`.

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

Use `pipeline.reconstruct_selected_catalog` for supported selected temporary
catalog construction. The internal engine has no direct CLI and is also
exercised via `validation.validate_catalog_equivalence` and tests.

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

> Content merged from the former `REPO_CLEANUP_AUDIT.md`.

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
  - `reconstruct_selected_catalog.py` (supported explicit development-computer
    temporary-catalog CLI/API)
- `validation/`
  - `catalog_compare.py`
  - `audit_replay_store.py`
  - `validate_catalog_equivalence.py`
  - `replay_catalog_reconstruct.py` (shared internal engine, no direct CLI;
    formerly `pipeline/generate_catalog.py`)
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

## Completed Cleanup Items (2026-07-21 — deployment boundary: converter removed from production systemd path)

- `scripts/deploy_linux_server.sh`: removed `legacy-converter` as a deployable
  `--target` (it is no longer in `VALID_TARGETS`); `--target all` now installs
  only `cryptorecorder-recorder.service` and
  `cryptorecorder-replay-build.{service,timer}`.
- `cryptorecorder-convert.service` and `cryptorecorder-convert.timer` were
  added to the `cleanup_stale_units()` list, so any already-installed copy on
  an existing server is stopped, disabled, and removed the next time the
  deploy script runs, exactly like the pre-issue-#17 feature-build units.
- `convert_day.py`, `converter/`, and `validation/replay_catalog_reconstruct.py`
  are **not** removed and remain required implementation/reference code — for
  replay building, validation, and local test-computer catalog
  reconstruction. Only their presence in the automated production
  systemd/deployment path changed.
- `docs/OPERATIONS.md` updated: the "Targets" and "Service groups" tables no
  longer list `legacy-converter`; the "daily chain runs convert → replay"
  ordering claim was corrected (`replay-build` already reads directly from
  `data_raw` and never depended on converter output).
- `systemd/cryptorecorder-convert.service` and `.timer` were deleted from the
  repository in PR #18 finalization (2026-07-22) — converter systemd
  automation is not part of the supported production architecture. Manual
  reconstruction uses documented CLI commands (e.g.
  `python convert_day.py --date YYYY-MM-DD --staging`), not systemd templates.
  Stale installed converter units are still removed by
  `scripts/deploy_linux_server.sh` cleanup (unchanged).

See `CHANGELOG.md` `[Unreleased]` for the full change list.

---

## Feature Store Requirements Audit (REMOVED — issue #17)

> **This subsystem no longer exists.** The table below is preserved as a
> historical record of the feature-store's status at the time it was removed
> (issue #17: recorder + replay-store ownership refactor). CryptoRecorder does
> not build a feature-store or label-store layer; that responsibility belongs
> to downstream consumer repositories (e.g. KovacsTrader). Do not recreate any
> of the files referenced below without a new, explicit task.

> Content merged from the former `FEATURE_STORE.md`.

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

> Content merged from the former `STORAGE_SIZE_AUDIT.md`.

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

---

## Issue #20 — Compact Replay Storage: Phase 2 Design

**Status: design only, not implemented.** This section records the raw-retention
safety contract, legacy-v0 inventory approach, traceability design, versioning
contract, and `encoding_profile` design that must exist and be reviewed
**before** any compact physical replay schema is implemented (Phase 5+ of the
issue #20 plan). No code in this section has been built yet; every claim below
is labeled `planned`, never `implemented` or `validated`.

### 1. Raw-retention safety contract (corrects a prior false assumption)

**Verified fact, not an assumption:** `disk_monitor.py::cleanup_old_data()`
(confirmed directly in code, `disk_monitor.py` lines ~863–945) **automatically
deletes raw data** whenever fresh `data_raw` usage exceeds
`CRYPTO_RECORDER_DISK_SOFT_LIMIT_GB` (default 750 GB), repeatedly deleting the
single oldest `(venue, channel, symbol, date)` directory found by
`get_oldest_date_dir()` (lines ~827–859) — up to 10 deletions per invocation —
until usage falls to `CRYPTO_RECORDER_DISK_CLEANUP_TARGET_GB` (default
700 GB). Because `get_oldest_date_dir()`'s glob walks
`venue → channel → symbol → date`, **`depth_v2` and `trade_v2` for the same
symbol/date are separate, independently-deletable directories** — there is no
atomicity across the channels that make up one logical raw partition today.
This means `data_raw` is **not** retained forever, and a legacy replay
partition cannot be assumed rebuildable without checking actual raw
availability (see Section 2 below).

**Corrected atomic deletion unit (planned):** the deletion unit must become
**all raw channels and metadata required to reconstruct one logical
partition** — i.e. `depth_v2` **and** `trade_v2` (and any other channel a
given replay partition actually consumed) for the same `(venue, symbol,
date)`, treated as one atomic candidate for deletion, never partially deleted.
This is a change to `disk_monitor.py`'s deletion-unit granularity, planned for
implementation alongside the raw-retention gate itself (Phase 10 of the
approved plan), not in this design phase.

**Preconditions (planned) — all must hold before any raw deletion is
permitted for a given deletion unit:**

1. The corresponding compact replay partition exists.
2. Its manifest `status` is `complete`.
3. Every required raw source file/channel for that partition is represented
   in the partition's source-identity manifest (Section 4 below).
4. Source checksums recorded in the manifest match the actual raw files
   currently on disk (detects silent raw repair/corruption since the replay
   was built).
5. Published replay file **and** block checksums pass (Section 3 below).
6. Instrument/exchange metadata required for reconstruction is embedded in
   the replay partition itself, not merely referenced in raw `exchangeinfo`.
7. The partition reconstructs successfully **without consulting `data_raw`** —
   proven by a dedicated self-contained-replay acceptance test (planned,
   Phase 9 of the approved plan; not implemented in this design phase).
8. The compact replay format has already passed the global semantic **and**
   representative-day (Tier 3) acceptance gates — this precondition cannot be
   satisfied by an unvalidated schema.
9. No partial, failed, unknown-version, or source-identity-changed replay is
   ever treated as sufficient.

**Fail-closed default (planned):** if any precondition cannot be positively
proven, raw cleanup for that unit is refused and logged clearly; the existing
`cleanup_old_data()` soft/hard-limit behavior continues to operate on units
that *do* pass the gate. This is an additive safety layer over the existing
mechanism, not a redesign of its trigger thresholds.

**Shared `exchangeinfo` handling (planned):** shared exchange-info data for a
venue/date must not be removed while any other retained partition for that
venue/date still requires it, unless the necessary instrument metadata has
already been embedded directly into every affected replay partition
(satisfying precondition 6 independently per partition).

**Never automatically pruned by this issue:** valid, already-published replay
partitions. Only staging/quarantine/backup lifecycle cleanup (a separate,
already-scoped piece of the approved plan) is in scope for automated deletion
of replay-side artifacts; raw deletion remains gated by the above contract,
layered over the existing (already-shipping) `cleanup_old_data()` mechanism.

### 2. Legacy v0 inventory design

Because `data_raw` can already have been partially or fully deleted by the
existing mechanism above, **no existing v0 replay partition may be assumed
rebuildable.** Planned inventory pass, to run once the compact schema exists
(Phase 5+), classifying every existing v0 partition as one of:

- **Rebuildable** — complete matching `data_raw` (all channels required for
  that venue/symbol/date) still exists and its identity can be verified
  against the partition's recorded source identity.
- **Not rebuildable** — required raw data has already been deleted.
- **Uncertain** — source identity cannot be proven either way (e.g. raw
  exists but checksums/coverage cannot be confirmed).

For **rebuildable** partitions: prefer an isolated rebuild from raw into a
separate candidate root, validated per the semantic oracle (Phase 1, already
implemented — see above) before promotion.

For **non-rebuildable or uncertain** partitions: preserve them as-is; keep the
legacy v0 reader available for them **indefinitely**, not for a fixed
migration window; never automatically delete them; report their reduced
audit/migration confidence honestly once the inventory pass actually runs.

**Legacy-reader removal condition (planned):** removal is permitted only
after an explicit inventory run proves no retained v0 partition depends on
it. The reader is never described as "necessarily temporary."

### 3. Traceability design (replaces "hash demotion is low-risk")

Per-event 64-character hex `native_payload_hash` removal remains **unresolved**,
not "low-risk," pending this design. Verified in code: no internal reader
consumes the hash's *value* today (`stores/replay_depth_adapter.py` and every
file in `validation/` were grep'd; only a non-null check exists in
`audit_replay_store.py`) — but that absence of an internal consumer does not
prove an external consumer (e.g. KovacsTrader) or the audit contract itself
doesn't need it.

**Planned replacement traceability hierarchy, evaluated in this order:**

1. Raw file/chunk identity + SHA-256 checksum, recorded per partition in the
   manifest (extends `pipeline/raw_manifest.py`'s existing coverage scan,
   which today records channel presence but not checksums).
2. Source file/chunk ordinal + source record offset/index recorded per
   replay block (not per row).
3. Replay block-level checksums (e.g. per Parquet row group).
4. Canonical published-file checksums (already exist today via
   `depth_checksum`/`trades_checksum` in the manifest).
5. A deterministic mapping function from a replay event (partition + row
   contribution ordinal) back to its exact source raw file. For
   event-time-repartitioned depth, the ordinal is among that file's accepted
   records; locating a physical JSON line requires replaying the same filter.
6. Corruption detection at all four levels: raw, build-time (spool/merge),
   block, and published-file.

Per-event hash removal is permitted only if this design, once implemented, is
shown to provide **equivalent or stronger** integrity/traceability than
today's per-row hex hash. If a per-event hash remains judged necessary after
implementation, a 32-byte binary representation must be benchmarked against
the current 64-character hex column before being adopted.

**Status update (issue #20 Phase 7, uncommitted prototype):** the hierarchy
above is now implemented as a `schema_version=2` candidate on the
`refactor/recorder-replay-only` branch (`stores/replay_writer.py`,
`stores/replay_reader.py`, `pipeline/raw_manifest.py`,
`pipeline/build_replay_store.py`), gated behind an explicit,
mandatory-`source_identity`, fail-closed contract — **not yet committed, not
yet benchmarked at scale, and not a production claim**. The threat/integrity
matrix below was written and used to audit that prototype before any
representative-symbol or full-universe benchmarking began, per the review
checklist that gated this work.

**Validation-tier correction (issue #20 Phase 7, second review round):** the
first prototype performed block-level digest re-verification inside EVERY
`validate_partition()` call, using a per-row Python/JSON digest
(`_canonical_row_bytes`/`_update_block_digest_with_row`, since removed).
Measured on the representative VELVETUSDT partition (11.4M trade rows,
2026-06-11): a single `validate_partition()` call took **~582 seconds**, and
the deep self-contained check took **~1097 seconds** — a production blocker
for routine skip-if-valid/publication checks across a multi-hundred-symbol
universe. This was corrected by splitting validation into two explicit
tiers:

- **Routine** (`validate_partition`, unchanged call sites — see 3b below):
  manifest/schema/builder contract, expected files, Parquet readability and
  exact physical schema, integrity-metadata STRUCTURAL shape, and the
  COMPLETE-FILE SHA-256 checksum — never deserializes/hashes individual
  rows. Measured: **~0.2s** on the same VELVETUSDT partition (vs. v1's own
  routine check at ~0.5s — materially comparable).
- **Deep** (`audit_partition_deep`,
  explicitly requested only): re-verifies every block's digest, now using a
  vectorized, Arrow-native canonical encoding (`_canon_array`/
  `_canon_table_hash`) instead of per-row Python/JSON — measured at **~35s**
  for the same VELVETUSDT partition, a ~17x-31x speedup over the original
  per-row approach. The current `arrow_canonical_v2` method remains
  deterministic and round-trip-stable
  (verified for nested `list<struct>` bids/asks columns specifically, which
  do NOT survive a naive raw-Arrow-IPC-buffer hash unchanged across a real
  Parquet write+read cycle — see `_canon_array`'s docstring), length-frames
  variable-width components, and includes primitive/list/struct validity.
  Tests distinguish null list from empty list and null struct from a valid
  struct with identical null children. Full benchmark gate results in
  `docs/CHANGE_AUDIT.md`.

Existing artifacts whose manifests record `arrow_canonical_v1` remain
auditable with that unchanged legacy method. It is deterministic for the
historically produced non-null nested replay rows, but it is not a general
collision-proof Arrow serialization: strings/names are not length framed and
nested validity is omitted. New artifacts record `arrow_canonical_v2`;
verifiers dispatch on the manifest method rather than silently reinterpreting
old block digests. Exact physical-schema validation and complete-file SHA-256
remain separate integrity layers: routine validation checks the latter over
the complete Parquet bytes, while deep validation checks logical row-group
content under the recorded canonical method.

Offline equivalence construction now fails closed until the complete D+1 raw
depth scope is proven closed (a closed final-hour file), and a failed non-empty
previous-day carry build aborts catalog reconstruction. The production 01:00
daily build retains the narrower closed-T00 readiness policy: that is an
explicit operational latency assumption, not a code-enforced maximum-delay
invariant.

**Semantic-gate catalog reader boundary (issue #20 Phase 7, BTCUSDT OOM
forensics and hardening):** the exhaustive catalog comparator no longer uses
Nautilus/DataFusion for full-day event streams. Nautilus 1.225.0 appends a
global `ORDER BY ts_init`, which was proven to materialize the full
multi-file BTCUSDT delta result natively before yielding. The replacement in
`validation/catalog_compare.py` reads selected Parquet files directly in
bounded PyArrow batches.

This reader has a deliberately fail-closed compatibility contract:

- `ParquetDataCatalog._query_files()` is a **private** method. It is used only
  because Nautilus 1.225.0 has no supported public file-pruning API that
  avoids the DataFusion query. The authoritative `pyproject.toml`
  reconstruction extra and `uv.lock` pin `nautilus_trader==1.225.0`; runtime
  version/signature mismatches raise
  clearly, with no DataFusion fallback.
- The reference writer (`convert_day.py` via `ObjectSpool`) and replay
  reconstruction writer (`validation/replay_catalog_reconstruct.py`, also via
  `ObjectSpool`) both order by `(ts_init, ordinal)` and call ordinary
  `ParquetDataCatalog.write_data()` without `skip_disjoint_check`. Under the
  pinned Nautilus version, every successfully written distinct file is
  internally non-decreasing and adjacent closed file ranges are strictly
  disjoint. This is a CryptoRecorder validation-output invariant, not a
  general Nautilus-catalog guarantee.
- Before yielding, the reader scans `ts_init` for every selected file one
  file/row group/batch at a time, validates class/instrument/schema identity,
  including exact pinned physical field order/types/nullability, actual range
  against the filename, deterministic row-group/file order, and strict
  non-overlap. Equal timestamps at a file boundary are rejected as overlap;
  equal timestamps inside one file retain physical ordinal order.
- `ArrowSerializer.deserialize()` and the relevant
  `from_pyo3_list()` conversion operate only on the current Arrow batch.
  Before yielding any object from a bounded decoded batch, the reader validates
  decoded count, Arrow-to-object `ts_init` identity/order, monotonicity, and
  instrument identity for the whole batch. Focused real-Parquet tests verify
  at most one file is open, decoded batch size stays at or below the configured
  bound, prior Arrow batch wrappers are released at the next decode and after
  exhaustion, and conversion does not retain its input list.

One separate writer limitation was found and is not hidden: Nautilus 1.225.0
checks whether a timestamp-derived filename already exists before it performs
the disjointness check. A later write chunk with the exact same singleton
interval can therefore be skipped. The reader can reject overlap among files
which exist but cannot recover a chunk already omitted by the writer.
Non-overlap proves the supported layout, not arbitrary writer completeness.
The preserved BTCUSDT artifacts have strictly disjoint files and their
exhaustive trade/delta streams match exactly, so this finding does not require
a rebuild of those accepted artifacts.

The remaining full-day semantic components can run as separate
`validation.stage_runner_cli` subprocesses: `depth10`, `checkpoints`,
`continuity`, `fences`, `metadata`, and `integrity`. Metadata combines the bounded
event-keyed raw-to-replay metadata comparison with a fresh raw source-identity
recomputation and requires equality with both `manifest.source_identity` and
`manifest.integrity.source_identity`; integrity separately runs routine and
deep replay-partition verification. Report aggregation can require an exact
stage set and a shared date/venue/symbol/source/candidate scope, preventing a
missing or cross-artifact fragment from being reported as a pass. The obsolete
combined `depth` compatibility command was removed; Depth10 and checkpoints
are explicit isolated stages.

#### 3a. Threat/integrity matrix: per-event hash (v1) vs. hierarchical
     traceability (v2 prototype)

Both designs are **unsigned** (SHA-256 only, no asymmetric signing/HMAC with
a secret key never present in this repository or its build environment).
Neither design, alone or together, can prevent a sufficiently privileged
attacker from rewriting both the data and every checksum that describes it
consistently — this is stated plainly in the final row and must not be
overclaimed as "prevented."

Note (routine vs. deep): rows 4-8 below distinguish which tier of v2
validation catches each scenario. **Routine** validation (always run, on
every publish/skip-valid/daily-build check) catches whole-file-level
corruption via the unchanged complete-file SHA-256 mechanism. **Deep**
audit (explicitly requested only, never automatic) additionally catches
sub-file (block-level) corruption/tampering that a whole-file checksum
alone cannot localize — e.g. a value changed AND the whole-file checksum
recomputed to match, which routine validation cannot detect by design (see
`test_routine_validate_partition_does_not_catch_stale_block_checksum` and
its paired `test_deep_audit_catches_changed_value_via_stale_block_checksum`).

| # | Threat / corruption scenario | Old design: per-event `native_payload_hash` (v1) | New design: manifest-level hierarchy (v2 prototype) | Verdict |
|---|---|---|---|---|
| 1 | Changed raw bytes (a single byte flipped in a `data_raw` file after ingestion) | Not detected at all — the hash is computed from the CONVERTED replay row, never re-derived from or compared against the raw byte stream; nothing in v1 re-reads or checksums `data_raw`. | Detected: `compute_raw_source_identity`'s per-file `sha256` (over the raw file's on-disk, i.e. compressed, bytes) changes; any later re-verification against a stored `source_identity` catches it. Requires a stored/prior `source_identity` to compare against — a single ad hoc read of `data_raw` with no baseline cannot detect it. | v2 strictly stronger — v1 provides **zero** raw-tamper detection. |
| 2 | Wrong source file (a replay event's declared/implied provenance points at the wrong raw file, e.g. after a rename/reshuffle) | Not detected — v1 has no source-file linkage of any kind; the per-event hash says nothing about *which* raw file produced a row. | Detected/preventable: `resolve_source_record` gives an exact source-file path plus contribution ordinal for every `raw_index`, derived from `record_range`. For non-repartitioned trades this is also the parsed-record ordinal; for event-time-repartitioned depth it is the ordinal among that file's accepted contribution, and exact physical-line location requires rescanning the filter/dedupe rule. A wrong-file claim remains falsifiable from the stored path/checksum identity. | v2 strictly stronger for source-file identity — a capability v1 never had — without overstating physical-line resolution for repartitioned depth. |
| 3 | Missing/extra/reordered raw records (a raw file loses, gains, or reorders lines before/at conversion time) | Not detected — v1's hash is per converted-row and carries no raw-side record-count or ordering information. | Detected, via two independent mechanisms that together cover both count and order changes: (a) **ordinary reordering of records within an unchanged raw file changes that file's SHA-256** (`compute_raw_source_identity`'s per-file `sha256` is computed over the file's exact on-disk byte sequence, so reordering the lines inside it — even with no line added/removed — produces different bytes and therefore a different hash); (b) a missing/extra raw record additionally changes `record_count`/`record_range` (a file's count and every subsequent file's cumulative range). Both (a) and (b) require comparison against a previously-recorded, trusted `source_identity` snapshot — a single fresh scan of already-mutated raw data with no baseline cannot itself prove a prior state existed, only that today's scan disagrees with a stored one. | v2 strictly stronger — raw-file SHA-256 already catches *any* byte-level reordering/edit (not merely a count change), and `record_count`/`record_range` additionally localizes count changes to a specific file, given a baseline to compare against. |
| 4 | Changed replay value (one field of one already-converted row is altered in place) | Detected: the per-event hash was computed from the original row's native payload; if a consumer recomputes and compares that same hash, a changed value is caught. In practice, no internal consumer ever recomputes/compares it (see the consumer-inventory finding above), so this detection capability, while structurally present, has never been exercised by anything in this codebase. | **DEEP tier only**: the containing Parquet row-group's block SHA-256 (vectorized canonical Arrow encoding — see `_canon_array`) changes; `verify_block_integrity`, invoked by `audit_partition_deep`, recomputes and compares it — proven by `test_deep_audit_catches_changed_value_via_stale_block_checksum`, which changes one logical value, updates only the whole-file checksum, leaves the block checksum stale, and shows deep audit still reports a problem (while ROUTINE `validate_partition()` does NOT catch this exact scenario by design — see `test_routine_validate_partition_does_not_catch_stale_block_checksum` — routine validation instead catches an *ordinary* corruption where the whole-file checksum is NOT separately recomputed to match, per row 7). | Materially equivalent detection capability at the deep tier, actively exercised by real tests (v1's per-event hash was never read/compared by anything internal). |
| 5 | Missing/extra/reordered replay rows (a row is dropped, duplicated, or reordered within/across a partition) | Not detected — a per-event hash says nothing about how many other rows exist or their order; a dropped row simply has its hash silently disappear along with it. | Detected via two layers: **whole-file level (ROUTINE tier, always run)** — the existing `depth_checksum`/`trades_checksum` (unchanged mechanism from v0/v1) changes for any row addition/removal/reordering that isn't separately recomputed into the manifest, since it covers the complete Parquet file's bytes; **block level (DEEP tier, explicit only)** — `verify_block_integrity` additionally checks `num_rows` per block AND the row-group count against `len(blocks)`, and ordinary reordering of replay rows (even preserving count) changes the block's vectorized canonical digest, localizing the exact affected block. | v2 strictly stronger at both tiers — v1 provides **zero** row-count/order detection at either level. |
| 6 | Damaged replay block (one Parquet row-group's bytes are corrupted, e.g. bad disk sector, partial write) | Only detected indirectly, and only if the whole-file `depth_checksum`/`trades_checksum` (unchanged from v0, already present in both v1 and v2) happens to be recomputed and compared — the per-event hash itself gives no finer-grained (sub-file) localization of the damage. | **ROUTINE tier** catches it via the unchanged whole-file checksum (same as v1/v0, no localization). **DEEP tier** additionally localizes: `verify_block_integrity` reads and re-verifies each row-group independently, reporting exactly which block index failed and why (row-count mismatch vs. checksum mismatch vs. unreadable row-group). | v2 at least as strong at routine tier (parity with v1/v0), strictly stronger at deep tier (adds localization neither v1 nor v0 ever had). |
| 7 | Damaged complete Parquet file (whole-file corruption/truncation) | Detected only via the existing whole-file `depth_checksum`/`trades_checksum` (identical mechanism already present pre-v2; the per-event hash adds nothing here). | **ROUTINE tier**: same whole-file checksum mechanism (unchanged) — this is routine validation's primary, cheap defense, proven directly by `test_routine_validation_detects_ordinary_file_corruption_via_complete_checksum`. **DEEP tier** additionally fails per-block with a specific "could not read row-group"/row-count-mismatch message for a truncated file, giving strictly more diagnostic detail. | v2 at least as strong at routine tier, with better diagnostics available at the deep tier. |
| 8 | Changed/missing manifest (the `manifest.json` itself is edited or deleted) | `validate_partition` already rejects a manifest with a missing/invalid `depth_checksum`/`trades_checksum` field, or a missing file entirely (pre-existing v0/v1 behavior, unrelated to the per-event hash). | **ROUTINE tier**: same manifest-presence/checksum-field checks, PLUS `_validate_schema_version_contract`'s v2 branch requires a well-formed `integrity` dict (`source_identity`, `depth_blocks`, `trade_blocks` all present and correctly typed) — a manifest edited to strip or malform the integrity hierarchy is explicitly rejected by ROUTINE validation alone (`test_both_modes_reject_missing_integrity_metadata`, `test_both_modes_reject_malformed_integrity_metadata` — both tiers fail closed, deep tier never needed for this check). | v2 strictly stronger — adds a structural check v1 never had, and it is cheap enough to be part of the always-run routine tier. |
| 9 | Raw data no longer locally present (`data_raw` pruned/archived/moved, e.g. after Syncthing rotation) | v1's per-event hash lives entirely inside the replay row itself and was never raw-dependent to begin with — trivially "unaffected," but this was never a meaningful integrity guarantee in the first place (see rows 1-3: v1 detects nothing raw-side regardless of whether raw is present). | Explicitly designed for this: both `validate_partition` (routine) and `audit_partition_deep` (deep) use ONLY `manifest.json`/`depth.parquet`/`trades.parquet` — never touch `data_raw` — and are proven to keep passing after raw is deleted (`test_replay_integrity_verification_self_contained_after_raw_removed`, `test_self_contained_reconstruction_requires_neither_raw_nor_deep_audit`). Only the raw-dependent *cross-check* (comparing a fresh `data_raw` scan against the stored `source_identity`) becomes unavailable, and is reported as `complete: False`/`missing_channels`, never silently treated as a pass. | v2 strictly stronger for what it actually claims (replay-side integrity, at both tiers), and honest about the one thing that becomes genuinely unavailable (raw cross-verification) rather than papering over it. |
| 10 | Coordinated malicious rewrite with recomputed, self-consistent (but unsigned) checksums (an attacker with write access to both `data_raw` and `replay_store` rewrites data and recomputes every SHA-256/manifest field to match) | **Not prevented.** A per-event hash is just another unsigned SHA-256 that the same attacker can recompute and overwrite alongside the row it "protects." | **Not prevented either, at either tier.** Every checksum in the v2 hierarchy (raw file sha256, block sha256, whole-file checksum, manifest `integrity` dict) is unsigned SHA-256 computed and stored by the same trust domain that would need to be compromised to rewrite the data in the first place — an attacker capable of rewriting `replay_store` files can trivially recompute and rewrite every one of them (including block digests, if they run the deep tier's own digest function) to stay internally consistent. | **Neither design prevents this, at any tier.** This is stated here explicitly and must not be overclaimed: SHA-256 (signed by nothing) proves *accidental* corruption and *unintentional* mismatch, never protects against a privileged adversary who controls the storage. A genuine defense against this threat (detached signing with a key never co-located with the data, WORM/immutable storage, or an independent out-of-band audit trail) is out of scope for this issue #20 Phase 7 candidate and is not claimed. |

**Conclusion drawn from this matrix:** the v2 hierarchy is equivalent-or-
stronger than the removed per-event hash for every scenario. Ordinary raw-
record reordering is caught by the raw-file SHA-256 (row 3); ordinary
replay-row reordering is caught by both the block checksum and the
whole-file checksum (row 5) — v1 detected neither. No scenario exists where
v1 detects something v2 cannot. The one universally-unprevented scenario
(row 10, a privileged, coordinated rewrite that recomputes all unsigned
checksums to stay internally consistent) is common to both designs and is
not a regression introduced by removing the per-event hash — it is stated
plainly and is not claimed as covered by either.

#### 3b. Routine vs. deep validation contract

Two explicit, separately-named tiers (issue #20 Phase 7 second review round
— see the "Validation-tier correction" note above for the measured
performance problem this fixes):

- **Routine — `stores.replay_writer.validate_partition(partition_dir) ->
  bool`.** Called automatically and unconditionally at every: (a)
  `ReplayWriter.publish()` post-publish check; (b)
  `pipeline.build_replay_store`'s skip-if-valid check (before deciding to
  skip a partition that already looks complete); (c)
  `pipeline.build_replay_store`'s crash-recovery check (deciding whether a
  possibly-interrupted prior build's output is safe to keep or must be
  rebuilt). Bounded cost: manifest/schema/builder contract validity,
  expected-files presence, Parquet-footer-only physical schema match,
  integrity-metadata structural shape (never its row content), and the
  COMPLETE-FILE SHA-256 checksum. Never deserializes or hashes individual
  rows/blocks.
- **Deep — `stores.replay_writer.audit_partition_deep(partition_dir) ->
  list[str]`** (and its self-contained-focused sibling entry point
  `audit_partition_deep`, identical behavior, documented
  separately for the "data_raw absent" proof). Never called automatically
  by any of the three call sites above — only invoked when explicitly
  requested (e.g. a dedicated audit CLI/script, an ad hoc investigation, or
  a test). Recomputes every block's digest via the vectorized
  `_canon_table_hash`, localizing any row-count or content mismatch to a
  specific block index, in addition to everything routine validation
  checks (it calls routine validation first).

This split is enforced by
`tests/test_replay_hierarchical_integrity_v2.py::test_publish_and_skip_valid_call_routine_not_deep`,
which monkeypatches `audit_partition_deep` to raise if invoked and confirms
both a fresh build and a repeat (skip-valid) build over the same partition
complete successfully without ever calling it.

### 4. Versioning and `encoding_profile` design

**Verified gap:** today's manifest (see `docs/REPLAY_STORE.md`'s example) has
**no** `format_version`, `schema_version`, or `builder_version` field at all.
A missing field today has no defined meaning.

**Planned contract:**

- A manifest with no `schema_version` field is explicitly defined as
  **legacy v0** — never silently reinterpreted as a newer schema.
- New manifests will carry `format_version`, `schema_version`,
  `builder_version`, and a new `encoding_profile` field (see below)
  explicitly. `ReplayReader` will reject any `schema_version` it does not
  explicitly support, with a clear error naming the found vs. supported
  version — never a silent fallback parse.
- `encoding_profile` (planned new manifest field) is an explicit identity
  for the build configuration that produced a partition — e.g. schema
  version, compression codec/level, row-group sizing, and any other
  build-time choice that could otherwise cause two "valid" builds of the
  same input to differ physically. This is what will make the planned
  deterministic-rebuild proof (build twice into separate empty roots for the
  same raw source identity + `schema_version` + `builder_version` +
  `encoding_profile` + compression configuration + partition scope, and
  require identical logical event order/values, identical replay data-file
  checksums where the format supports byte-deterministic output, and
  manifest equality except explicitly-named observational fields such as
  build timestamp) auditable and reproducible.
- Old and new partitions may coexist during validation: a new (candidate)
  partition is built into an isolated candidate root, never overwriting the
  existing published v0 partition in place. A new partition becomes
  canonical only after it passes the full semantic-equivalence gate (Phase 1
  oracle, already implemented) **and**, where relevant, the Section 1
  raw-retention preconditions, for that specific venue/symbol/date.
- Old replay data may be manually removed only after the corresponding new
  partition has passed validation, an operator has explicitly confirmed it
  canonical, and a documented retention window has passed — never automatic.

### 5. Explicit statement of what remains not-yet-implemented

Everything in this section is a **design record only**. No compact physical
replay schema, no `pipeline/raw_manifest.py` checksum extension, no
`ReplayReader` version-rejection logic, no `encoding_profile` manifest field,
no legacy-v0 inventory scan, and no raw-retention precondition gate has been
implemented as of this entry. Implementation of any of the above requires the
Phase 0–4 review checkpoint (this design, the Phase 1 oracle, and the Phase 3
field/consumer/integrity matrix) to be reviewed and approved first, per the
plan.

---

## Issue #20 — Compact Replay Storage: Phase 3 Field / Consumer / Integrity Matrix

**Status: design/audit only, not implemented.** This is the finalized
field-necessity audit for `stores/replay_schema.py`'s current `DEPTH_REPLAY_SCHEMA`
and `TRADE_REPLAY_SCHEMA` (both verified directly in code), classifying every
stored field by writer, current physical representation, reconstruction
consumer, audit/integrity consumer, whether it is required for exact
semantics, whether it is partition-constant, its proposed compact
representation, and any migration/compatibility concern. No field below is
removed, renamed, or repacked by this entry — this is analysis, gating future
schema work (Phase 5+), not schema implementation.

### Depth fields

| Field | Writer | Current representation | Reconstruction consumer | Audit/integrity consumer | Required for exact semantics? | Partition-constant? | Proposed compact representation | Migration/compatibility concern |
|---|---|---|---|---|---|---|---|---|
| `venue`, `symbol`, `date` | `stores/replay_writer.py` | string, repeated every row | none directly (path-derivable) | schema check (`validation/audit_replay_store.py`) | No — derivable from the Hive partition path | **Yes** | manifest/partition metadata only | readers must stop expecting these columns — `schema_version` bump |
| `stream_session_id`, `session_seq`, `raw_index` | writer | uint64/uint64/uint32 | ordering key in `stores/replay_depth_adapter.py`, `stores/replay_reader.py` | ordering audit (`audit_replay_store.py`) | Yes — deterministic order | No | keep integer; consider Parquet delta-binary-packing | none — same logical values |
| `record_type` | writer | string (`snapshot_seed`/`depth_update`) | branch logic in adapter and `converter/depth_phase2.py` | schema completeness | Yes | No | small enum/dictionary; consider int8 | reader enum mapping table |
| `U`, `u`, `pu` | writer | nullable string | continuity/gap logic in `depth_phase2.py`/adapter | gap/fence diagnostics (`tests/test_gap_and_fence_diagnostics.py`) | **Yes — continuity contract** | No | int64 nullable, **only after proving range/null/overflow safety separately for spot and futures** | benchmark-needed before type change |
| `ts_exchange_ns`, `ts_receive_ns` | writer | int64 ns | book/trade timestamps | equivalence timestamp compare (`validation/catalog_compare.py`) | Yes | No | keep int64; Parquet delta encoding, not a custom scheme | none |
| `bids`/`asks` (nested `price`, `size`, `price_str`, `size_str`) | writer | `list<struct{float64, float64, string, string}>` per level | `stores/replay_depth_adapter.py` reconstructs deltas | book-checkpoint compare (`compare_book_checkpoints()` in `catalog_compare.py`) | Yes — exact decimal reconstruction | No | fixed-point integer mantissa derived from Binance `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize`/`MARKET_LOT_SIZE` (date-specific exchangeInfo; spot vs. futures may differ) — **not** from `pricePrecision`/`quantityPrecision` alone; parsed via `Decimal`, never float | must prove, in tests, before dropping the lexical strings: (1) numeric value exactly reconstructable from mantissa+scale; (2) instrument's required scale exactly reconstructable from manifest/instrument metadata; (3) the replay partition itself — not `data_raw`, which is not guaranteed to survive per Phase 2 Section 1 — carries this reconstructability; (4) replay-to-source traceability preserved per Phase 2 Section 3 |
| `is_snapshot_seed`, `is_depth_update`, `is_sync_state`, `is_desync`, `is_resync` | writer | 5 separate bool columns | book-state/continuity logic | continuity/gap tests | **Yes** | No | packed flags byte/enum, **only after a consumer-and-semantics proof that no equivalence-critical information lives only here** | reader bit-unpacking helper |
| `quality_flags` | writer | JSON string, nullable | **no confirmed internal reconstruction consumer** (verified: zero matches across `stores/replay_depth_adapter.py` and all of `validation/`; only writer-side pass-through in `pipeline/build_replay_store.py`) | none confirmed internally | **Unproven — treated as required until the Phase 1 `compare_quality_flags_semantic()` oracle (implemented) and a KovacsTrader-contract check both prove otherwise** | No | compact enum/flags for common cases + sparse side table for rare cases, pending that proof | must show no equivalence-critical info lives only here |
| `native_payload_hash` | writer | 64-character hex SHA-256, every row | none internal (only non-null check in `audit_replay_store.py`) | corruption/traceability, **unresolved — see Phase 2 Section 3** | **Unresolved** | No | manifest/chunk/file-level checksum, pending the Phase 2 Section 3 traceability design being implemented and proven equivalent-or-stronger | also feeds the Phase 2 Section 1 raw-retention preconditions |

### Trade fields

| Field | Writer | Current representation | Reconstruction consumer | Audit/integrity consumer | Required for exact semantics? | Partition-constant? | Proposed compact representation | Migration/compatibility concern |
|---|---|---|---|---|---|---|---|---|
| `venue`, `symbol`, `date` | writer | string, repeated every row | none directly | schema check | No — path-derivable | **Yes** | manifest/partition metadata only | `schema_version` bump |
| `trade_stream_session_id`, `trade_session_seq`, `raw_index` | writer | uint64/uint64/uint32 | ordering key | ordering audit | Yes | No | keep integer; consider delta-binary Parquet | none |
| `record_type` | writer | string (`trade`/`agg_trade`) | branch logic | schema completeness | Yes | No | small enum/dictionary | reader enum table |
| `market_type` | writer | string (`spot`/`futures`) | instrument routing | schema | Partly — derivable from venue in practice | **Yes, pending an invariant proof that no row ever has a differing value for its partition** | move to manifest if the invariant holds | confirm invariant across all rows before removing |
| `trade_id`, `agg_trade_id` | writer | nullable string | `TradeTick` id fields | equivalence compare where exposed (`compare_trade_ticks_semantic()`) | Yes where exposed | No | numeric id + delta encoding, **only after proving range/nullable/monotonic behavior separately for spot and futures** | benchmark-needed |
| `ts_exchange_ns`, `ts_receive_ns` | writer | int64 ns | trade timestamps | equivalence compare | Yes | No | keep int64; Parquet delta encoding | none |
| `price`, `quantity`, `price_str`, `quantity_str` | writer | float64 ×2 + string ×2 | `TradeTick` reconstruction | trade-tick equivalence | Yes | No | fixed-point mantissa, same 4-condition proof as depth levels above | same as depth `bids`/`asks` |
| `buyer_maker`, `aggressor_side` | writer | bool + nullable string | aggressor logic | equivalence compare | Yes | No | compact enum | none |
| `quality_flags` | writer | JSON string, nullable | no confirmed consumer | none confirmed | Unproven — same status as the depth field above | No | compact enum/flags, pending proof | same as depth `quality_flags` |
| `native_payload_hash` | writer | 64-character hex, every row | none internal | corruption/traceability, unresolved | Unresolved | No | same as depth `native_payload_hash` | same |

### New fields identified as required by Phase 2 design (not yet implemented)

| Field | Purpose | Required by |
|---|---|---|
| Source raw-file identity/checksums (per partition) | Proves replay-to-raw traceability; a precondition for any future raw-retention auto-delete gate | Phase 2 Section 1 (raw-retention safety) and Section 3 (traceability design) |
| `encoding_profile` / build-configuration identity (manifest field) | Makes output provenance reproducible; the basis for a future deterministic-rebuild proof | Phase 2 Section 4 (versioning design) |
| `format_version`, `schema_version`, `builder_version` (manifest fields) | Explicit version contract; today's manifest has none of these — a missing field today has no defined meaning | Phase 2 Section 4 |

### Matrix conclusion

No field in either table above is approved for removal or repacking as of
this entry. Every "pending proof"/"unproven"/"unresolved" cell above is an
explicit precondition that must be satisfied by evidence — produced during
Phase 5+ implementation and reviewed — before the corresponding compaction is
applied. This matrix, together with the Phase 1 oracle (implemented, see
above) and the Phase 2 design (raw-retention, legacy-v0, traceability,
versioning — design only, see above), constitutes the review checkpoint the
approved plan requires before any compact physical schema implementation
begins.
