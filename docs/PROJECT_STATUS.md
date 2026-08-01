# Project Status

**Version:** `v1.1.0-dev`
**Last updated:** 2026-08-01

This document is the single source of truth for **what is validated** vs **what is
deferred** in CryptoRecorder. Keep it honest. Do not promote a deferred item to
validated without recorded evidence.

---

## Validated (works today)

- **Recorder** — Binance spot + USDT-M futures ingestion to `data_raw/`
  (deterministic-native depth_v2 + trade_v2). Stable.
- **Replay-aware disk monitoring (fail-safe measurement)** — `disk_monitor.py` never reports a
  failed/timed-out directory-size scan as zero. Failures fall back to a persisted
  last-known-good value marked `stale`, or `null` if none exists;
  `state/disk_usage.json` exposes `monitoring_health` (healthy/degraded/unhealthy)
  and alerts. A single bounded scan separates canonical replay, staging,
  backups, quarantine, and lifecycle metadata; actual filesystems group raw,
  replay, metadata, and reports without double-counting free bytes. The
  retired persistent catalog root is not monitored. Automatic raw deletion is
  disabled/removed; proof-only paired depth/trade planning reports
  `cleanup_required` without moving data.
  Covered by `tests/test_disk_monitor_fail_safe.py` and
  `tests/test_disk_monitor_cleanup.py` plus
  `tests/test_replay_monitoring_retention.py`. Real-server verification (post-deploy
  log/report inspection) is pending — see the later owner-run checklist in
  `docs/OPERATIONS.md`.
- **`data_raw/ → convert_day.py → Nautilus full-L2 catalog`** — the reference
  production conversion path. This is the byte-for-byte validated path and must not
  regress.
- **`data_raw/ → replay_store`** (versioned v0/v1/v2) — normalized deterministic Parquet replay layer
  (`stores/replay_*`, `pipeline/build_replay_store.py`). This is the stable
  external contract consumed by downstream repositories (e.g. KovacsTrader).
  CryptoRecorder does not build a feature/label layer or a general-purpose
  consumer catalog from it. `ReplayWriter` is memory-bounded (SQLite spool +
  incremental Parquet write). Supported mutation entrypoints share one
  nonblocking kernel advisory lock; the daily path reconciles bounded
  cross-date staging/backup/quarantine state, builds a bounded oldest-first
  backlog, and writes atomic per-date/run reports. The intended repository
  service explicitly requests schema 2 and keeps `Restart=no`, 12 GiB, and
  zero swap, but it has not been installed or production-accepted.

**Replay lifecycle isolated smoke (checkpoint 2)**

The repository-side candidate built only
`BINANCE_SPOT/ADAUSDT/2026-06-11` into fresh external replay/report roots under
the common lock with schema 2. It published 303,293 depth and 129,824 trade
rows, passed the replay audit and deep integrity with zero anonymous trades,
and a second identical invocation returned `skipped_valid`. Build runtime was
45.081 seconds with 1,196,359,680 bytes peak; audit/deep/skip also passed.
Every subprocess used an effective 12,884,901,888-byte MemoryMax with zero
swap, `memory.high`, `memory.max`, OOM, OOM-kill, and OOM-group-kill. The
published partition is 8,110,080 allocated bytes and has no residual staging
or backup. External summary SHA-256:
`1aacc5f402f9a42c17fe6aac71b1c92cd3b77494d4d64e44357918be9d3c7561`.
This is isolated development evidence, not production deployment or service
acceptance.

**Authoritative uv environments (closure checkpoint 3)**

`pyproject.toml` plus committed `uv.lock` are the sole dependency authority;
the loose requirements file is removed. Production, reconstruction, and
development/test selections are explicit, frozen, and non-default. Production
imports recorder, monitoring, replay lifecycle/build/read/write, and routine/
deep validation with neither Nautilus nor pytest installed. Reconstruction
adds exact `nautilus_trader==1.225.0` and excludes pytest; development adds the
two test dependencies.

Three fresh external environments passed on Linux/WSL with CPython 3.12.3 and
uv 0.11.29. The final lock SHA-256 is
`976451a3c49b0098bc6e620acb889aa9fb6aa8aaf8098d20db0416721ed1b5af`
and remained byte-identical. Production built and routine/deep-validated a
tiny schema-v2 fixture (2 depth, 1 identified trade); reconstruction produced
a Nautilus-readable selected `full_l2` catalog (1 trade, 4 flattened deltas,
1 Depth10); the clean development suite passed 834 tests with 3 skipped.
External acceptance-report SHA-256:
`fde42fe0b6f2ba88294e093ab645e80a7490a8bcb9cc34e44e53344d9c118b01`.
This is local environment acceptance, not production deployment, service
control, production `.venv` migration, broader semantic acceptance, or a
`v2.0.0` declaration.

- **`replay_store → pipeline.reconstruct_selected_catalog`** — supported
  development-computer CLI/API for an explicitly selected venue/symbol,
  end-exclusive UTC window, output root, job ID, and profile. It strictly
  preflights complete replay manifests, checksums, schema/builder contracts,
  and instrument metadata; binds every target/carry partition and final
  catalog file into a deterministic job manifest; and atomically publishes
  only `<output-root>/<job-id>/`. It is not a Linux service/timer and owns no
  feature, strategy, backtest, risk, or execution orchestration.
  `validation.replay_catalog_reconstruct` remains the shared internal engine
  also used by `validation.validate_catalog_equivalence` to reconstruct a Nautilus
  catalog from replay_store for equivalence checking. Supports `trades_only`
  (matches the reference converter for trades) and `full_l2` (full order-book
  reconstruction via the shared depth engine). **Semantically validated on the
  ADAUSDT smoke with additional completed high-volume BTCUSDT schema-v2
  representative-day evidence** against `convert_day.py`; broader
  top50/multi-day validation is still pending (see Deferred). This is the
  `v2.0.0` gate and `v2.0.0` is **not** declared. The internal helper is not a
  supported direct API; callers use the pipeline boundary.

**Supported selected-reconstruction real smoke (checkpoint 1)**

The committed-boundary candidate was exercised against the preserved
schema-v2 `BINANCE_SPOT/ADAUSDT` replay for the end-exclusive interval
`2026-06-11T12:00:00Z` to `12:05:00Z`. It consumed and bound the exact
2026-06-10 carry plus 2026-06-11 target partitions, atomically published one
external job, and was readable with pinned `nautilus_trader==1.225.0`: 475
TradeTicks, 3,328 flattened OrderBookDelta rows, and 227 Depth10 objects.
The catalog object manifest records 991 `OrderBookDeltas` containers. Runtime
was 51.437 seconds and cgroup peak was 640,143,360 bytes under 10 GiB with
zero swap, `memory.high`, `memory.max`, OOM, and OOM-kill. External job manifest
SHA-256: `7d3eef0020c210911d485dff5f1d9d933e55981c70b0a95edb9a3b13446011ff`.
This is a selected development-computer smoke, not broader top50/multi-day or
production acceptance.

### Recorded validation evidence

**Trades-only catalog equivalence**

```
2026-06-12 BINANCE_SPOT/ADAUSDT trades_only:
  old trades            124457
  new trades            124457
  timestamp range match true
  sample mismatches     0
```

**Full-L2 catalog equivalence (ADAUSDT smoke)**

```
2026-06-12 BINANCE_SPOT/ADAUSDT full_l2 (replay vs convert_day.py):
  trade_ticks         old 124457   new 124457   range match  0 mismatches
  order_book_deltas   old 1231284  new 1231284  range match  0 mismatches
  order_book_depth10  old 71341    new 71341    range match  0 mismatches
  book checkpoints    7/7 match, no crossed books
  status              passed
```

Report: `validation_reports/full_l2_equivalence_2026-06-12_ADAUSDT.json` (local,
gitignored). Reproduce with `python -m validation.validate_catalog_equivalence
--date 2026-06-12 --symbols ADAUSDT --venues BINANCE_SPOT --profile full_l2`.
This is a **single-symbol single-day smoke**, not a universe benchmark.

**Full-L2 catalog equivalence (BTCUSDT schema-v2 representative day)**

```
2026-06-11 BINANCE_SPOT/BTCUSDT full_l2 (schema-v2 replay vs convert_day.py):
  reference construction                         passed (preserved)
  replay construction/catalog reconstruction      passed (preserved)
  trade_ticks        old 3418712   new 3418712    exact, 0 mismatches
  order_book_deltas  old 30009655  new 30009655   exact, 0 mismatches
  order_book_depth10 old 84066     new 84066       exact, 0 mismatches
  book checkpoints   7/7 canonical hashes match
  continuity         seeds 7/7, resyncs 0/0, desyncs 0/0, fences 25/25
  fenced ranges      25/25, canonical digest match
  raw/replay metadata depth 846430/846430, trades 3419004/3419004, exact
  live source identity matches manifest + integrity copy (25 depth, 24 trade files)
  status             passed
```

The accepted external report has SHA-256
`69c4466d1a6cb4206110f07def6f9d9c2b751a65f6923bd270ac16956668c281`.
A compact, path-sanitized local summary is generated at
`validation_reports/issue20_phase7_btcusdt_spot_2026-06-11.json` (SHA-256
`aa3d116b269355e267058491562245a8e4f3e0add3182e653d48e59fbbf61782`);
`validation_reports/` is contractually gitignored. Catalogs, replay
partitions, raw data, large logs, and cgroup sample streams remain external.
The report reuses the preserved passing reference, replay, and trade
fragments; the hardened reader reran only deltas and produced the same passing
fragment SHA-256 as the accepted Round 5 result. Every new substantial stage
ran serially in its own 10 GiB cgroup with zero OOM events.

This is strong high-volume, single-symbol/single-day local development
evidence. It does **not** satisfy or narrow the broader top50/multi-day
`v2.0.0` gate and does not replace `convert_day.py` as the production
reference. Schema 2 is now the explicit intended replay-build configuration
in repository templates; those templates have not been deployed or accepted
on the production server.

**Full-L2 catalog equivalence (BTWUSDT corrected schema-v2 futures case)**

```
2026-06-11 BINANCE_USDTF/BTWUSDT full_l2:
  replay trade rows       1371217 (all identified; 45 zero-quantity)
  trade_ticks             old 1371172   new 1371172   exhaustive exact
  order_book_deltas       old 11507066  new 11507066  exhaustive exact
  order_book_depth10      old 40398     new 40398     exhaustive exact
  book checkpoints        7/7 canonical hashes match
  continuity/fences       exact (61 seeds, 3 resyncs, 0 desyncs, 249 fences)
  raw/replay metadata     exact (350157 depth, 1371217 trades)
  source identity         current raw = manifest = integrity copy
  routine/deep integrity  passed
  status                  passed
```

This result corrects the replay normalization defect exposed by the preserved
first BTW attempt: its raw native `trade.t` identifiers are now recovered
exactly, while anonymous trades fail before replay publication. The preserved
reference catalog was reused; only replay/candidate artifacts were rebuilt in
a fresh external directory. Peak cgroup memory was 2,371,661,824 bytes with
zero OOM/OOM-kill. Final report SHA-256:
`2ae29713f09dd10988566c10c3bb040ec55a0252d936a5e60e08032295af4d85`.

This is representative local development evidence for one futures
symbol/day, not a completed representative matrix, top50/full-universe,
multi-day, Tier-3, or production validation. The old v2.0.0 spot reports
remain valid historical evidence for their exact artifacts, but final
checkpoint evidence under the corrected v2.0.1 builder will require
intentional spot replay/candidate revalidation; those large spot cases were
not rebuilt in this correction round.

**Full-universe schema-v2 storage/build acceptance (partial-source fixture)**

The accepted local three-day fixture was processed once for all 150 available
target-day partitions on 2026-06-11 (72 `BINANCE_SPOT`, 78 `BINANCE_USDTF`) by
the v2.0.1 builder. Persisted evidence shows 150/150 successful builds,
150/150 successful routine validations, 150/150 successful deep-integrity
validations, zero reported validation problems, zero anonymous trades, schema
version 2 throughout, complete available-source identities, valid artifact
checksums, and no staging or duplicate partitions. This is storage/build
evidence, not another semantic matrix or a complete raw-day proof.

The final replay tree measured 4,137,099,264 allocated bytes (3.85 GiB) and
4,134,547,170 apparent bytes: the hard 5 GiB gate passes, while the 2 GiB
stretch target fails. The persisted evidence permits a non-exact conservative
missing-tail estimate of 1,681,900 bytes; observed plus allowance is
4,138,781,164 bytes, still below 5 GiB. The 100 partitions with D+1 depth
enclosure are distinguished from 50 partitions marked
`partial_missing_d_plus_1`; raw completeness therefore remains **PARTIAL / NOT
PROVEN**.

The strict supervisor result remains **FAILED**, because the cgroup reached its
10 GiB `MemoryMax` and recorded 855,983 `memory.max` events. Peak memory was
10,737,418,240 bytes, with zero swap, OOM, OOM-kill, and OOM-group-kill
events. Build-stage and validation-stage `memory.max` event deltas were
765,905 and 90,078 respectively; per-symbol pressure attribution was not
persisted. This yields **PASS WITH OPERATIONAL CAVEATS** for Issue #20 Phase 7
core acceptance, not a strict zero-pressure pass. No further full-day rebuild
is required for Phase 7 acceptance; memory-headroom optimization is a separate
follow-up concern.

External aggregated report:
`/home/z0055upd/cryptorecorder_phase7_bench/full_day_schema_v2_partial_source_2026-06-11_20260731T130753Z/reports/final_acceptance_report.json`
(SHA-256
`7dd82ba51b54d990c9c4fe37565402489eeaa9da58d90384a2f47f92a961a772`). The
`FAILED` sentinel and original run evidence remain preserved. This does not
declare `v2.0.0`, production deployment, or Phase 8 completion.

---

## Deferred (NOT done — do not claim otherwise)

- **Broader `full_l2` validation (top50 + multi-day)** — the replay full-L2
  reconstruction path is implemented and **passes the ADAUSDT single-day smoke
  plus the BTCUSDT schema-v2 representative day** against `convert_day.py` (see
  evidence above), but it is **not** yet validated across the top50 universe or
  multiple days. `v2.0.0` remains **ungated** until that wider validation passes.
  See [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md).
- **Syncthing archive / backup** — `ARCHIVE_DAYS_ROOT` is a **placeholder** env path
  only. No archive code exists.
- **Import / restore tooling** — not implemented.
- **Production replay-build acceptance/deployment** — schema-v2 bounded
  service/env templates and deployment dry-run guards exist, but no installed
  unit, `/etc` env file, or production replay root was changed. The isolated
  owner-run acceptance in `docs/OPERATIONS.md` remains pending.
- **Transactional raw retirement** — automatic mutation is absent. A
  proof-only paired depth/trade planner reports blockers, but durable
  journaled move/rollback/startup recovery remains separate future work before
  any deletion can be enabled.

CryptoRecorder does not own a feature-store or label-store subsystem (removed,
issue #17); `FEATURE_ROOT`, `LABEL_ROOT`, and `CATALOG_JOBS_ROOT` no longer
exist in `config.py`. That responsibility belongs to downstream consumer
repositories (e.g. KovacsTrader).

---

## Current recommended production behavior

1. Run the **recorder** continuously (`recorder.py`).
2. Convert the previous UTC day with **`convert_day.py --staging`** (the validated
   full-L2 path) shortly after 00:00 UTC.
3. After separate owner acceptance/deployment, build the **replay store** with
   the explicit bounded schema-v2 `pipeline.daily_build` service contract.
   Until then, treat the repository unit as an uninstalled template.
4. On the development computer, reconstruct an explicitly selected temporary
   catalog with `python -m pipeline.reconstruct_selected_catalog` and all
   required venue/symbol/start/end/output/job/profile arguments. Use
   `validation.validate_catalog_equivalence` only for old-vs-new validation.
5. The replay `full_l2` reconstruction path passes the ADAUSDT smoke and the
   BTCUSDT schema-v2 representative day, but `convert_day.py` remains the
   **production reference** for full order-book catalogs until broader
   top50/multi-day validation passes.

See [OPERATIONS.md](OPERATIONS.md) for the service layout that runs these steps.
