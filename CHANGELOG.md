# Changelog

All notable changes to CryptoRecorder are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).
Version policy is described in the [Versioning Policy](#versioning-policy) section below.

## Versioning Policy

> Content merged from the former `docs/CHANGELOG.md`.

CryptoRecorder follows [Semantic Versioning](https://semver.org/) with a
project-specific interpretation of the major/minor lines. The current version is in
[`VERSION`](../VERSION) and the change history is in [`CHANGELOG.md`](../CHANGELOG.md).

## Version lines

### v1.x — recorder + reference converter + replay store foundation
The `v1` line covers the **validated core**:
- the deterministic-native **recorder**,
- the reference **`convert_day.py`** full-L2 converter,
- the **replay store** v0 foundation (the stable contract consumed by
  downstream repositories, e.g. KovacsTrader).

This is the production baseline. Everything in `v1.x` is expected to keep the
validated `data_raw → convert_day.py → full-L2 catalog` path intact.

### v1.1.x — stability, cleanup, testing, deployment
The `v1.1` line adds **no new data semantics**. It focuses on:
- frozen repository structure and enforcement tests,
- AI-agent governance,
- versioning + changelog discipline,
- Linux server deployment docs and per-service systemd units,
- the `deploy_linux_server.sh` wrapper.

### v1.2.x — one-command Linux deploy + stronger validation
The planned `v1.2` line will harden operations:
- a fully tested one-command Linux server deployment,
- stronger automated validation/audit gates around the replay build.

It still must **not** introduce a general-purpose or persistent consumer
catalog-generation service. Explicit development-computer selected temporary
reconstruction is permitted through its supported scoped boundary.

### v2.0.0 — validated replay_store full-L2 equivalence
The `v2.0.0` release is reserved for one thing: the shared
**replay_store full-L2 reconstruction path** (`validation/replay_catalog_reconstruct.py`,
exercised through validation and the supported selected boundary) being **validated for
semantic equivalence against `convert_day.py`** across the broader top50
universe and multiple days.

**No `v2` release may ship until that broader full-L2 semantic equivalence
passes.** Until then, broader full-L2 equivalence stays **deferred** (see
[PROJECT_STATUS.md](PROJECT_STATUS.md) and
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md)).

## Rules

- Bump `VERSION` and add a `CHANGELOG.md` entry in the same change that ships a feature.
- Keep `## [Unreleased]` current; move entries into a dated version section on release.
- Do **not** create git tags as part of routine agent work unless explicitly asked.
- A deferred capability never counts toward a version milestone until its validation
  evidence is recorded in [PROJECT_STATUS.md](PROJECT_STATUS.md).

---

## [Unreleased]

### Added
- **Issue #20 owner-approved closure amendment and replacement-PR status** —
  checkpoint 4 stopped before semantic execution as
  `BLOCKED_SOURCE_UNAVAILABLE`: the retained fixture has no date-aligned June
  top50 universe, lacks 32 target cases and 10 D+1 depth cases, provides only
  68/100 schema-v2 replay partitions, and has neither June 9 nor June 13 depth
  required to enclose a genuine two-day range. Preserved inventory/report
  SHA-256 values are
  `f7f4eb92d0aa5bc5e58a9ac3c5d7cf80166baa498f9a6dc9182cfcbf74d5abe2`
  and `28da7cf44bf58240bb345a8cb85dc43070b10e051ca51b4670506401927c2d2b`.
  The original gate was not weakened or erased: follow-up issue #21 now owns
  exact top50/multi-day acceptance and `v2.0.0` promotion. Issue #20's
  implementation is release-candidate complete under its explicit amendment,
  subject to exact-head PR review and manual isolated production acceptance.
  `VERSION` is unchanged, `convert_day.py` remains the reference, and no
  production deployment or semantic workload occurred in this checkpoint.
  Fresh locked-development validation passed 834 tests with 3 skipped; the
  focused reconstruction/lifecycle/monitoring/guard set passed 182 tests.
- **Issue #20 closure checkpoint 3: authoritative uv dependency and
  environment migration** — committed `pyproject.toml` plus `uv.lock` now
  define explicit production, reconstruction, and development/test selections
  for the existing flat non-packaged application; the hand-maintained
  `requirements.txt` is removed and `VERSION` remains the application-version
  authority. Production no longer imports Nautilus through the replay builder:
  dependency-free depth event-time/repartition primitives are shared from
  `converter.depth_repartition`, while object construction stays in the
  reconstruction boundary. The read-only environment validator proves lock
  freshness/hash, exact selected closure, required/forbidden packages,
  imports/CLI help, exact Nautilus 1.225.0, and an optional tiny external
  schema-v2 smoke. Deployment now requires operator-supplied uv, uses only
  `uv lock --check` plus a frozen production sync, preserves
  `.venv/bin/python`, and replaces a legacy environment only through explicit
  inactive-service migration with a validated candidate, preserved backup,
  and fail-closed rollback/evidence rules. Runtime services never invoke uv;
  no production deployment or environment migration occurred. Three clean
  external CPython 3.12.3/uv 0.11.29 environments passed; the focused set is
  160 passed and the full clean-development suite is 834 passed, 3 skipped.
  Lock SHA-256:
  `976451a3c49b0098bc6e620acb889aa9fb6aa8aaf8098d20db0416721ed1b5af`;
  external acceptance-report SHA-256:
  `fde42fe0b6f2ba88294e093ab645e80a7490a8bcb9cc34e44e53344d9c118b01`.
  Broader top50/multi-day semantics, `v2.0.0`, production deployment, and
  checkpoint 4 remain pending.
- **Issue #20 closure checkpoint 2: bounded replay-build lifecycle and
  operations contract** — all supported replay-root mutation boundaries now
  share one nonblocking Linux advisory lock with run/root/repository identity.
  The schema-v2 daily path performs bounded cross-date staging/backup/
  quarantine reconciliation, scans an oldest-first bounded backlog, requires
  explicit source-change and incompatible-schema replacement policies, and
  publishes fsynced atomic per-date and invocation reports with distinct
  built/skipped/deferred/missing/rebuild-required/recovered/failed outcomes.
  Replay-aware disk monitoring classifies canonical and transient storage in
  one bounded scan, groups actual filesystems without summing free space, and
  includes replay growth/transient pressure. The unsafe single-channel raw
  deletion path is disabled; only a non-mutating paired depth/trade proof plan
  remains until durable transactional retirement is separately accepted. The
  repository service template explicitly selects schema 2, a seven-day/
  three-build backlog, `Restart=no`, `MemoryMax=12G`, and
  `MemorySwapMax=0`; deployment dry-run validates those fields. This template
  has not been installed or production-accepted, existing production replay
  is not migrated, and replay semantics, raw ingestion, and `convert_day.py`
  are unchanged. Focused checkpoint verification is 380 passed; the full
  suite is 820 passed, 3 skipped. An isolated real ADAUSDT lifecycle smoke
  built 303,293 depth and 129,824 trade rows, passed routine/deep integrity,
  then returned `skipped_valid`; its build peak was 1,196,359,680 bytes under
  12 GiB with zero swap, memory pressure, or OOM. External smoke-summary
  SHA-256: `1aacc5f402f9a42c17fe6aac71b1c92cd3b77494d4d64e44357918be9d3c7561`.
- **Issue #20 closure checkpoint 1: supported selected full-L2 reconstruction
  CLI/API** — `pipeline.reconstruct_selected_catalog` promotes the existing
  shared reconstruction engine through an explicit development-computer
  boundary. Venue, symbol, timezone-aware `[start,end)` UTC window, replay
  root, external output root, safe job ID, and profile are mandatory; empty or
  duplicate selection, generic instrument fallback, missing/unsupported replay
  contracts, checksum failures, mutation, symlink traversal, and ambiguous
  overwrite fail closed. Same-parent staging publishes only a completed
  `<output-root>/<job-id>/` and preserves failed evidence separately. Its job
  manifest cryptographically inventories every target/carry replay manifest,
  data file, instrument, source-identity/integrity reference, and generated
  catalog file. This is not a Linux service, permanent catalog lifecycle,
  feature/backtest boundary, broader top50/multi-day gate, or v2.0.0 release.
  Focused boundary/guard verification is 84 passed; the broader focused replay
  set is 222 passed, 2 skipped; the full suite is 773 passed, 3 skipped. The
  preserved schema-v2 ADAUSDT five-minute real smoke was Nautilus-readable and
  completed in 51.437 seconds at 640,143,360 bytes peak with zero swap,
  pressure events, or OOM; external job-manifest SHA-256:
  `7d3eef0020c210911d485dff5f1d9d933e55981c70b0a95edb9a3b13446011ff`.
- **Issue #20 Phase 7 full-universe schema-v2 storage/build acceptance
  aggregation (partial-source fixture)** — persisted evidence from the one
  detached 2026-06-11 local build covers all 150 available target partitions
  (72 spot, 78 futures), with 150/150 successful builds and 150/150 successful
  routine plus deep-integrity validation subprocesses. All published
  partitions are schema v2.0.1, unique, staging-free, checksum-valid, source
  identity-valid for the available files, fixed-point-scale-valid, and contain
  zero anonymous trades. Final allocated replay size is 4,137,099,264 bytes
  (3.85 GiB), so the 5 GiB gate passes and the 2 GiB stretch target fails. A
  persisted-evidence-only, non-exact missing-tail estimate adds 1,681,900
  bytes, leaving 4,138,781,164 bytes and still passing the 5 GiB gate.
  One hundred partitions had D+1 depth enclosure; 50 are explicitly marked
  partial_missing_d_plus_1, so raw completeness remains partial/not proven.
  The strict supervisor result remains FAILED: the cgroup reached the exact
  10 GiB MemoryMax and recorded 855,983 memory.max events, despite zero
  swap/OOM/OOM-kill/OOM-group-kill events. The resulting classification is
  PASS WITH OPERATIONAL CAVEATS for Phase 7 core acceptance, not a strict
  zero-pressure pass. Aggregated external report SHA-256:
  7dd82ba51b54d990c9c4fe37565402489eeaa9da58d90384a2f47f92a961a772.
  No further full-day rebuild is required for Phase 7 acceptance; memory
  headroom is a separate follow-up. This is not complete raw-day proof,
  v2.0.0, production deployment, or Phase 8 completion.
- **Issue #20 Phase 7 BTWUSDT trade-identifier normalization correction** —
  replay normalization now matches the unchanged reference converter's
  Binance identifier precedence: existing normalized top-level identifiers
  remain authoritative, legacy native `trade` payloads recover exact field
  `t`, and native `aggTrade` payloads recover exact aggregate field `a`.
  Anonymous supported trade events now fail before replay publication, and
  replay reconstruction independently refuses an anonymous `TradeTick`
  instead of succeeding with a shorter catalog. The physical replay schemas
  remain unchanged; builder identities advance to v1.2.1/v2.0.1. Existing
  compact partitions remain readable/auditable under their recorded physical
  contracts, but older builders cannot be silently reused as current output;
  known v2.0.0 partitions are not accepted by the current artifact-bound gate.
  The fresh external corrected BTWUSDT futures gate reused the preserved
  reference catalog, rebuilt only replay/candidate artifacts, and passed all
  nine components: 1,371,172/1,371,172 exhaustive TradeTicks,
  11,507,066/11,507,066 flattened deltas, 40,398/40,398 Depth10, 7/7
  checkpoints, continuity and 249/249 fences exact, metadata/source identity
  exact, and routine/deep integrity passed. All 1,371,217 replay trade rows
  carry `trade_id` recovered from `native_payload.t`; 45 zero-quantity rows
  are identically excluded by both reconstructions. Final report SHA-256:
  `2ae29713f09dd10988566c10c3bb040ec55a0252d936a5e60e08032295af4d85`.
  Focused verification passed and the full ordinary suite is 745 passed,
  3 skipped.
  This is one corrected futures representative, not a completed
  representative matrix, top50/multi-day validation, or production
  promotion.
- **Issue #20 Phase 7 Round 5 reader hardening and completed BTCUSDT
  semantic report** — the validation-only exhaustive catalog reader now has
  a fail-closed, exactly pinned `nautilus_trader==1.225.0` compatibility
  boundary around the private `ParquetDataCatalog._query_files()` selector.
  It preflights every selected real Parquet file before yielding: exact
  data-class physical schema, instrument metadata, filename/content
  timestamp bounds, explicit row-group order, internal non-decreasing
  `ts_init`, and strictly disjoint closed file intervals (including
  rejection of equal timestamp boundaries). Each bounded decoded batch is
  also checked in full against its Arrow `ts_init` rows and instrument before
  its first object can escape. Version/signature/layout incompatibility fails
  clearly and can never fall back to the memory-unbounded DataFusion global
  sort. Focused real-Parquet tests cover non-overlap, deliberate overlap,
  equal boundaries/ties, internal disorder, wrong class/instrument,
  multi-instrument selection, exact endpoints, many-file file/batch lifetime,
  decoder reorder, and changed/missing/extra/reordered near-end events.
- Writer-path audit proved strict file-range non-overlap for every
  successfully produced CryptoRecorder reference/replay validation catalog
  under the pinned Nautilus version: both writers feed
  `(ts_init, ordinal)`-sorted `ObjectSpool` batches to ordinary
  `write_data()` and do not bypass the disjointness check. The scope is not
  generalized to arbitrary Nautilus catalogs. A separate pinned-writer
  limitation remains explicit: a second identical singleton interval can be
  skipped before Nautilus checks overlap, which a reader cannot recover.
- Production-day checks have honest isolated subcommands: `depth10`,
  `checkpoints`, `continuity`, `fences`, `metadata` (exhaustive event-keyed
  raw/replay metadata plus fresh source identity), and `integrity` (routine
  plus deep replay-partition verification). Required-stage report aggregation fails for missing,
  duplicate, unexpected, or cross-artifact fragments; the obsolete combined
  `depth` compatibility command was removed.
- Replay reader schema dispatch now accepts a versionless historical manifest
  only when both physical Parquet channels exactly match the legacy-v0 schema
  (including exact decimal-string fields). Missing/malformed manifests,
  compact v1/v2 files without an explicit version, and every declared-version
  versus physical-schema contradiction fail before decoding.
- Every semantic-stage fragment is now cryptographically bound to the
  canonical artifact-identity document and its exact component hashes. Each
  stage performs full content verification before and after execution; final
  aggregation independently recomputes the identity and rejects label-only,
  copied, mixed-build, mutated, missing, duplicate, or unexpected fragments.
- New schema-v2 builds record the length-framed `arrow_canonical_v2` block
  digest, including primitive/list/struct validity. Tests prove null-list
  versus empty-list and null-struct versus valid-struct separation. Existing
  `arrow_canonical_v1` artifacts retain their recorded verifier and remain
  auditable without reinterpretation or rebuild; complete-file SHA-256
  remains the separate routine integrity layer.
- The replay-store audit now interprets schema-v1/v2 fixed-point mantissa
  fields as the compact schemas' exact numeric representation instead of
  falsely reporting that only legacy schema-v0 decimal-string fields are
  exact.
- Preserved BTCUSDT 2026-06-11 reference/replay/trade artifacts were not
  rebuilt or rerun. The algorithm-changing reader hardening triggered only
  the required delta rerun under 10 GiB: 30,009,655/30,009,655 rows matched
  exactly, zero mismatches, and its fragment SHA-256 is identical to the
  accepted Round 5 fragment. Remaining results: Depth10
  84,066/84,066 exact; 7/7 checkpoint hashes match; continuity 7 seeds,
  0 resyncs, 0 desyncs, 25 fences on both sides; 25/25 canonical fence
  digest match; raw/replay metadata exact for 846,430 depth and 3,419,004
  trade records; current raw source identity exactly matches both manifest
  copies (25 depth files, 24 trade files). Every substantial new stage ran
  serially in its own 10 GiB cgroup and recorded zero OOM events. The accepted
  external report SHA-256 is
  `69c4466d1a6cb4206110f07def6f9d9c2b751a65f6923bd270ac16956668c281`;
  a compact sanitized local summary is generated under the gitignored
  `validation_reports/` structure.
  Full suite: 728 passed, 3 skipped. This is single-symbol/single-day local
  evidence only; top50/multi-day validation, the 150-partition build,
  deployment, retention, KovacsTrader, Phase 8+, and any production-status
  promotion remain out of scope.
- **Issue #20 Phase 7 Stage A: measured v1 Parquet encoding optimization
  (local development evidence, NOT a final production storage-gate
  claim)** — using the complete real 2026-06-11 local universe (150/150
  valid partitions for both v0 and v1, corrected at `b5a3555`) as the
  Stage A development baseline. A representative-symbol sweep (ADAUSDT/
  BTCUSDT/ETHUSDT spot, BTCUSDT/ETHUSDT/VELVETUSDT/LABUSDT futures —
  the two highest-local-volume symbols and one of the five
  scale-corrected anomalous futures symbols) measured row-group target
  (~64/128/256 MiB), ZSTD level (3/6/9), dictionary on/off,
  `DELTA_BINARY_PACKED` (monotonic session/sequence/timestamp integer
  columns), and `BYTE_STREAM_SPLIT` (int64 fixed-point mantissa columns,
  including the nested `bids`/`asks` list-of-struct mantissas)
  combinations before any full-universe rebuild.
  - **Selected encoding** (`stores/replay_writer.py`, `schema_version=1`
    only — v0's physical output is completely unaffected): ZSTD level 6
    (was 3), dictionary encoding disabled entirely (measured smaller on
    every representative symbol, including already-low-cardinality
    repeated strings like trades' `market_type` — verified directly at
    scale on `VELVETUSDT`'s 11.3M trade rows: a negligible ~17 KB
    difference against an ~8 MB smaller overall file), larger measured
    row-group batch sizes (depth: 20,000 rows; trades: 50,000 rows, both
    configurable via new env vars), `DELTA_BINARY_PACKED` for
    `session_seq`/`raw_index`/`ts_exchange_ns`/`ts_receive_ns` (depth)
    and `trade_session_seq`/`raw_index`/`ts_exchange_ns`/`ts_receive_ns`
    (trades), and `BYTE_STREAM_SPLIT` for `price_mantissa`/
    `quantity_mantissa` (trades) and the nested `bids`/`asks`
    `price_mantissa`/`size_mantissa` (depth). Combining delta-encoded
    integers with byte-stream-split mantissas measured a further ~20%
    reduction beyond delta-alone on large futures symbols (e.g.
    `BINANCE_USDTF/BTCUSDT` depth: 325,308,871 → 255,321,595 bytes,
    ~21.5% smaller). This is a pure Parquet *physical encoding* change:
    every field's logical type, nullability, and semantic meaning is
    completely unchanged, so any standard Parquet/Arrow reader
    (`stores/replay_reader.py` needed zero changes) reads back
    byte-for-byte identical logical values.
  - `stores/replay_schema.py`'s `BUILDER_VERSION_V1` bumped to `v1.2.0`
    (the encoding profile changed); `FORMAT_VERSION_V1`/`SCHEMA_VERSION_V1`
    (the reader contract) unchanged — no schema/reader change occurred.
  - The v1 manifest's `encoding_profile` now records the exact selected
    encoding (`use_dictionary`, `depth_column_encoding`,
    `trade_column_encoding`, per-channel row-group batch sizes) so any
    built partition's physical format is self-describing.
  - **Full-universe rebuild** (2026-06-11, all 150 real partitions, into
    a fresh isolated candidate root — production `replay_store` never
    touched): **150/150 succeeded**. Total size: **8.18 GiB** (8,783,383,810
    bytes) — down from the corrected-scale v1 baseline's 11.12 GiB
    (26.4% further reduction) and from v0's 15.84 GiB (48.3%
    reduction). Wall time 2:53:55, max RSS 2,410,720 KiB (2.30 GiB).
    Every one of the 150 manifests validates via
    `stores.replay_writer.validate_partition()`.
  - **Per-column evidence after this optimization**: `native_payload_hash`
    (a 32-byte SHA-256 hash per event, explicitly unresolved/out of
    scope per `docs/IMPLEMENTATION_AUDIT.md`'s Phase 2 Section 3
    traceability design) is now the single dominant remaining cost
    driver — 28.1% of depth (1.39 GiB) and 86.2% of trades (3.30 GiB),
    **54% of the total candidate size** — because a cryptographic hash
    is high-entropy and therefore not meaningfully compressible by any
    Parquet encoding. No further *encoding-only* lever can materially
    reduce this without changing what is stored per event, which is an
    explicitly out-of-scope architectural decision here.
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): both
    `--schema-version 0` and `--schema-version 1` (with the new encoding)
    still pass all 7 gating components, `fenced_ranges` still
    byte-identical (34/34, same canonical digest as every prior Phase 6/7
    result) — confirming the encoding change is purely physical with zero
    semantic effect.
  - Full suite: 561 passed, 3 skipped (unchanged — no new tests were
    needed since `stores/replay_reader.py` required zero changes and
    existing v1 tests already exercise round-trip decode correctness
    with real Parquet files); guards: 56 passed.
  - **Decision (per Stage A instructions)**: the best measured local
    Parquet candidate (8.18 GiB) remains above the 5 GiB hard target.
    This is recorded as strong local development evidence, NOT a claim
    that the final Tier-3 production storage gate has passed — Stage B
    (running the finalized candidate against a real production day,
    e.g. 2026-07-22/23, in an isolated production-server candidate
    root) remains a separate, deferred step. Custom binary format
    (Phase 8) has NOT been started and requires explicit approval.
  - No change to `convert_day.py`. Still strictly out of scope: Phase 8,
    self-contained/raw-retention work, lifecycle hardening,
    reconstruction CLI, monitoring, deterministic-rebuild phase, `uv`,
    deployment, KovacsTrader, final documentation.
- **Issue #20 Phase 7 blocking correction: observed-scale-aware v1
  fixed-point encoding + EXCHANGEINFO eligibility fix** — discovered
  during the first real full-production-day (2026-06-11, 100% local
  symbol coverage, 150 real venue/symbol partitions) Phase 7 build: the
  v1 compact schema's automatic `price_scale`/`qty_scale` derivation
  used the exchange's *declared* `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize`
  alone, and 5 of 78 real BINANCE_USDTF futures symbols
  (`BTWUSDT`/`GUAUSDT`/`HOMEUSDT`/`IRYSUSDT`/`LABUSDT`) failed to build
  with "cannot be represented exactly at scale N" — the exchange's
  actual recorded tick granularity on that day was finer than its own
  declared filter (e.g. `BTWUSDT` declared price scale 5, real observed
  depth/trade values like `0.0795760` require scale 6). This is a
  genuine data anomaly, not a benchmark artifact, and blocks any v1
  complete-day candidate from being valid.
  - `stores/replay_schema.py`: added `normalized_decimal_scale()` (exact
    `Decimal`-based minimum-fractional-digits computation, insignificant
    trailing zeros stripped via `.normalize()` — never lexical string
    length, never `float`). `encode_fixed_point()` now also validates the
    resulting mantissa fits a signed int64 (the physical
    `*_mantissa` field type), raising clearly instead of silently
    overflowing.
  - `stores/replay_writer.py`: `ReplayWriter` now tracks a small running
    maximum observed price/qty scale incrementally as each depth/trade
    batch is spooled (`write_depth_batch`/`write_trades_batch`) — never
    by rescanning a partition or materializing a full-day list. The
    automatically-derived `price_scale`/`qty_scale` (when not explicitly
    supplied) becomes `max(declared exchangeInfo scale, observed scale)`.
    An *explicitly* supplied scale is never silently enlarged: if
    observed data would require more precision than an explicit
    override, `finalize_staging()` raises clearly instead. The
    manifest's `encoding_profile` now separately records
    `price_scale_declared`/`price_scale_observed`/`qty_scale_declared`/
    `qty_scale_observed` so any anomalous partition remains explainable
    from the manifest alone. `BUILDER_VERSION_V1` bumped to
    `v1.1.0` (the deterministic scale-selection algorithm changed); the
    physical schema/format version is unchanged (the reader contract
    did not change).
  - `pipeline/raw_manifest.py`: `scan_raw_coverage()` now only derives
    tradable venue/symbol partitions from genuine market channels
    (`depth_v2`/`trade_v2`, via the new `ELIGIBLE_MARKET_CHANNELS`
    allow-list) — the venue-level `exchangeinfo` metadata channel's
    single `EXCHANGEINFO` pseudo-"symbol" directory can no longer leak
    into the eligible-symbol universe for either schema version (it
    previously did, and v0 silently "succeeded" building a meaningless
    partition for it while v1 correctly refused). Real symbols missing
    one of the two channels are still reported honestly (not hidden).
  - Added `tests/test_replay_scale_selection_and_eligibility.py` (22
    tests): the exact real failing value (`0.0795760`, declared scale 5)
    selects encoding scale 6 (not 7, the naive lexical-length count) and
    round-trips exactly; insignificant trailing zeros never inflate
    scale; observed scale below declared retains the declared floor;
    mixed depth+trade values select the true maximum; qty_scale
    considers depth size, trade quantity, `LOT_SIZE`, and
    `MARKET_LOT_SIZE` together; scientific-notation/zero/negative-exponent
    decimal strings are handled exactly; an explicit insufficient scale
    fails clearly instead of being silently enlarged; int64 mantissa
    overflow fails clearly; `EXCHANGEINFO` is excluded from eligible
    symbols for both venues and both schema versions end-to-end; real
    symbol/channel coverage and missing-single-channel reporting remain
    correct and honest; a malformed non-directory venue entry still
    surfaces safely rather than crashing.
  - Updated 3 pre-existing tests whose expectations were tied to the
    fixed behavior: `tests/test_replay_schema_v1.py::test_fixed_point_no_float_intermediate`
    (adjusted its float-imprecision-boundary test value to one that also
    fits the newly-enforced int64 mantissa range — the original test
    value could never have fit the real Parquet int64 field anyway) and
    `tests/test_daily_build.py`'s two `EXCHANGEINFO`-exclusion tests
    (updated to assert `EXCHANGEINFO` is now excluded at
    `scan_raw_coverage()`'s source, not merely filtered downstream by
    `pipeline.daily_build`'s pre-existing `ELIGIBLE_REPLAY_CHANNELS`
    check).
  - **Rebuilt the complete real 2026-06-11 universe** (150 real
    venue/symbol partitions: 72 BINANCE_SPOT + 78 BINANCE_USDTF, into
    fresh isolated candidate roots) with the fix: **v0: 150/150
    succeeded; v1: 150/150 succeeded** (up from 145/150 before this
    fix), including all 5 previously-failing futures symbols. Every
    manifest (300 total) validates via `stores.replay_writer.validate_partition()`.
    Focused semantic proof: every sampled raw price/size string for each
    of the 5 corrected symbols encodes and decodes exactly at its
    selected scale (50,000+ depth levels sampled for `LABUSDT` alone,
    zero precision-loss failures).
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): both
    `--schema-version 0` and `--schema-version 1` still pass all 7
    gating components, `fenced_ranges` still byte-identical (34/34, same
    canonical digest as every prior Phase 6 result) — confirming this
    correction has zero effect on already-correct partitions.
  - Full suite: 561 passed, 3 skipped (up from 539, +22 new); guards: 56
    passed.
  - No change to `convert_day.py`. This is a Phase 7 prerequisite
    correction only — the Parquet format-selection/optimization sweep
    itself has not yet started (that work explicitly requires this fix
    first, since a v1 complete-day candidate could not previously be
    built at all). Still strictly out of scope: Phase 8, self-contained/
    raw-retention work, lifecycle hardening, reconstruction CLI,
    monitoring, deterministic-rebuild phase, `uv`, deployment,
    KovacsTrader, final documentation.
- **Issue #20 Phase 6 correction #2: transactionally retryable spool
  merge state** — a review of `bc00b1e` (accepted the byte-budget/
  bounded-fan-in/atomic-write/descriptor fixes) found one remaining
  transactional-state gap: `_ensure_single_run()` only reassigned
  `self._run_paths` once the *entire* multi-level reduction finished. If
  one or more batches merged successfully and unlinked their inputs, and
  a *later* batch (or a later merge pass) then failed, `self._run_paths`
  still referenced the already-deleted inputs from the successful
  batches — making retry impossible and causing `close()` to leak the
  completed intermediate merge outputs. Fixed on top of `bc00b1e` (kept,
  not reverted):
  - `_ensure_single_run()` is now a single rolling reduction (not
    level-by-level): it repeatedly takes the next `fan_in`-sized batch
    directly from the front of `self._run_paths`, merges it, and — only
    once that merge has fully and durably succeeded — reassigns
    `self._run_paths` in one atomic Python assignment (`[merged_path] +
    self._run_paths[len(batch):]`) *before* unlinking the batch's
    now-superseded input files. `self._run_paths` is therefore a
    complete, valid, retryable set of sorted runs at every point in the
    reduction, including immediately after a caught failure — every
    already-completed batch remains correctly reflected.
  - Added `self._owned_paths`, an append-only ownership record of every
    run file this spool has ever created (initial flushed runs, every
    merge output including a `*.run.part` that fails before its rename).
    `close()` now unlinks the union of `self._run_paths` and
    `self._owned_paths`, guaranteeing every owned file is removed —
    including any intermediate output already logically superseded, and
    any partial output left behind by a caught failure — independent of
    the spool's current logical state.
  - `_merge_batch()` now also wraps the `os.replace()` call itself in a
    try/except: if the atomic rename fails (disk full, cross-device,
    permission error), the fully written and closed `*.run.part` file is
    treated as a partial output and removed exactly like a mid-write
    failure, then the exception propagates.
  - `_ensure_single_run()` now unconditionally flushes any pending
    buffered records first, and `insert()` now unconditionally
    invalidates the cached merged state (`self._merged = False`) on
    every call — so additional insertion after a query is still fully
    supported (matching the original SQLite-backed implementation, where
    every query was a live view of the current table), a later query
    always folds in newly inserted records, and no record is ever
    silently ignored. No manual cache reset is needed anywhere anymore:
    retry after a caught failure is fully automatic, since `self._merged`
    is only ever set `True` once the whole reduction completes without
    error.
  - Added 4 new tests to `tests/test_spool_external_merge.py` (now 21
    total, up from 17): a failure injected into the *second* merge batch
    after the first batch already succeeded (proves the exact retryable
    state and a full recovery on retry), a failure injected several
    batches into a larger reduction (representative of a failure during
    a later merge pass), a failure injected into `os.replace()` itself
    (proves the same partial-output cleanup and retry-safety), and an
    insert-after-query-then-commit-then-query test (proves later records
    are folded in, not silently dropped, with no duplication or loss).
    Every new failure-injection test also asserts `close()` leaves zero
    files behind under the spool's temp-file prefix.
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): both
    `--schema-version 0` and `--schema-version 1` still pass all 7
    gating components, `fenced_ranges` still byte-identical (34/34, same
    canonical digest as before this correction, `bc00b1e`, `c131217`,
    and the original Phase 6 commit).
  - Full suite: 539 passed, 3 skipped (up from 535 in `bc00b1e`, +4 new
    tests); spool-dependent regression set (15 files): 165 passed, 1
    skipped; guards: 56 passed.
  - No change to `convert_day.py`, schema semantics, `DedupeSet`, or
    `ObjectSpool`. Broader unknown/SIGKILL crash-lifecycle discovery of
    stray temp files after this process exits remains deferred to the
    already-planned Phase 11. Still strictly out of scope: Phase 7,
    Tier 3, production deployment, custom binary format, retention gate,
    uv, KovacsTrader.
- **Issue #20 Phase 6 correction: byte-budgeted buffering + bounded
  fan-in hierarchical merge** — a review of the first Phase 6 commit
  (`c131217`, not approved) correctly identified 4 violations of the
  approved Phase 6 RAM/scratch model, all now fixed on top of `c131217`
  (which is kept, not reverted):
  1. `CRYPTO_RECORDER_SPOOL_RUN_SIZE` bounded the run buffer by *record
     count*, not memory bytes — unsuitable as a 16 GB safety guarantee
     given how much raw depth records vary in size (a `depth_update`
     with a large nested `bids`/`asks` array vs. a tiny `sync_state`
     record). Replaced with `CRYPTO_RECORDER_SPOOL_RUN_BYTES` (default
     64 MiB): each record is serialized to bytes immediately at
     `insert()` and only the resulting bytes are buffered; the buffer
     flushes to a new sorted run file once accumulated serialized bytes
     reach the budget. A single record whose own serialized size already
     exceeds the budget is still accepted — it becomes its own
     one-record run, flushed immediately, so it can never block or grow
     the buffer without bound.
  2. `heapq.merge(*all_run_iterators)` had unbounded fan-in — memory and
     file-descriptor usage were proportional to the number of runs, not
     bounded. Replaced with a bounded fan-in hierarchical (multi-pass)
     merge (`CRYPTO_RECORDER_SPOOL_FAN_IN`, default 16): each pass merges
     at most `fan_in` run files into one new output run file (opening at
     most `fan_in + 1` file handles at a time), repeated until a single
     fully sorted run remains. The single merged run is cached
     (`self._merged`) so repeated queries per partition build (`first_record`/
     `has_record_before`/`max_record`, each called a small, bounded
     number of times) never re-trigger a fresh merge or exceed the same
     descriptor bound.
  3. Each intermediate merge output is now written atomically: to a
     temporary `*.run.part` name, flushed and `fsync`'d, closed, then
     atomically renamed (`os.replace`) to its final `*.run` name — a
     reader can never observe a partial merge output under its final
     name. Input run files for a pass are unlinked only *after* their
     replacement output is durably renamed into place. If an exception
     is raised while writing a merge output, the partial `.part` file is
     removed and the not-yet-consumed inputs for that pass are left
     untouched. Broader unknown/SIGKILL crash-lifecycle cleanup of stray
     temp files remains explicitly deferred to the already-planned
     Phase 11.
  4. **Measured, not merely claimed**: `/tmp/bench_spool.py` (a one-off
     benchmark script, not part of the repo) fed the identical real
     ADAUSDT 2026-06-12 `depth_v2` raw fixture (412,464 records) to both
     the original pre-Phase-6 SQLite-backed spool (reconstructed
     verbatim from commit `59e28d8` for comparison only) and the
     corrected external-merge spool, each in an isolated subprocess:
     - SQLite (old): wall 5.68s, peak apparent scratch bytes on disk
       259,747,840 (247.7 MiB), max RSS 1,187,312 KiB (1159.5 MiB).
     - External-merge (corrected): wall 4.41s, peak apparent scratch
       bytes on disk 204,063,710 (194.6 MiB), max RSS 1,293,964 KiB
       (1263.6 MiB).
     - Honest caveat: this benchmark isolates peak scratch bytes on disk
       and wall time cleanly (new is ~22% faster, ~21% smaller on-disk
       footprint), but max RSS in this benchmark is dominated by holding
       all 412K raw records in a Python list in-process before feeding
       the spool (identical overhead for both variants), so it does not
       cleanly isolate the spool's own incremental RSS contribution.
       Full isolated per-spool RSS/Tier-3 measurement remains Phase 7,
       per the approved plan.
  - Added 6 new tests to `tests/test_spool_external_merge.py` (now 17
    total, replacing the 3 record-count-based tests that no longer
    apply): a byte-budget flush-boundary prediction test, a
    highly-variable/large-nested-payload test, an oversized-single-record
    test, a multi-pass correctness test (forces >20 runs and multiple
    merge levels via a small `fan_in`), a bounded-file-descriptor proof
    (wraps `builtins.open` to empirically prove peak concurrent open
    files never exceeds `fan_in + 2` across 130+ runs, including for
    repeated query passes which reuse the cached merged run), and a
    merge-failure-cleanup test (injects a failure into `os.fsync`,
    proves no partial `.part` file remains and no input run file is
    deleted prematurely, then proves recovery on retry).
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): both
    `--schema-version 0` and `--schema-version 1` still pass all 7
    gating components, `fenced_ranges` still byte-identical (34/34, same
    canonical digest as before this correction and before Phase 6) —
    confirming the correction preserves exact semantic behavior.
  - Full suite: 535 passed, 3 skipped (up from 529 in `c131217`, net +6
    tests: 17 new − 11 superseded); spool-dependent regression set (15
    files): 165 passed, 1 skipped; guards: 56 passed.
  - No change to `convert_day.py`, `DedupeSet`, or `ObjectSpool`. Still
    strictly out of scope, per the approved checkpoint: Phase 7, Tier 3,
    production deployment, custom binary format, retention gate, uv,
    KovacsTrader. Broader crash/SIGKILL lifecycle cleanup remains
    deferred to Phase 11.
- **Issue #20 Phase 6: bounded external-merge replacement for the
  SQLite-backed conversion scratch spool** — per the approved plan's
  correction #13 ("scratch-inefficient"), `converter/spool.py`'s
  `RawRecordSpool` (used by `convert_day.py`'s raw repartition/carry
  spools, `stores/replay_writer.py`'s write batching,
  `stores/replay_depth_adapter.py`'s replay-side resort, and
  `validation/validate_catalog_equivalence.py`'s raw metadata sort) was
  rewritten from a single on-disk SQLite B-tree table (3 secondary
  indexes, full JSON-text payload per row) to a genuine bounded
  external merge sort: records are buffered in memory up to a
  configurable run size (`CRYPTO_RECORDER_SPOOL_RUN_SIZE`, default
  20000), each run sorted and flushed to a disk-backed pickle file, and
  the final sorted stream produced via a k-way `heapq.merge` across run
  files (peak memory is O(run size), not O(total record count)).
  - Public interface, constructor signature, and every method's
    external behavior are unchanged: `insert()`, `commit()`,
    `iter_records()` (with `record_type`/`session_id`/`min_sort_key`
    filters), `first_record()`, `has_record_before()`, `max_record()`
    (both `first_tie=True` and `first_tie=False` tie-break semantics,
    matching the prior SQL `ORDER BY` behavior exactly), and `close()`
    (removes all run files plus the placeholder marker path). `DedupeSet`
    and `ObjectSpool` in the same file are unchanged (still SQLite-backed;
    out of Phase 6 scope).
  - Added `tests/test_spool_external_merge.py` (11 tests): sort-order
    correctness, filter correctness, `first_record`/`has_record_before`/
    `max_record` (`first_tie` True and False) semantics matching the
    prior SQL behavior, `close()` cleanup of all run files, an explicit
    proof that exceeding the configured run size produces multiple
    on-disk run files while still yielding a fully correct merged sort,
    and a live-object-counter bounded-memory proof (5,000 records,
    run size 200) showing peak simultaneously-alive spooled records
    stays under 1,000 — bounded by run size, not proportional to total
    record count.
  - Full suite: 529 passed, 3 skipped (up from 518, +11 new spool
    tests); spool-dependent regression set (14 files covering
    `convert_day_phase2`, `replay_store`, `replay_depth_adapter`,
    `replay_memory_bounded`, `replay_sync_continuity`,
    `replay_catalog_reconstruct`, `semantic_equivalence`,
    `catalog_equivalence*`, etc.): 162 passed, 1 skipped; guards
    (`test_repo_structure.py` + `test_agent_infrastructure.py`): 56
    passed.
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): all 7 gating
    components pass identically for both `--schema-version 0` and
    `--schema-version 1`, with `fenced_ranges` byte-identical (34/34,
    same canonical digest as before the spool rewrite) — proving the
    scratch-mechanism replacement is a pure implementation change with
    zero semantic impact.
  - No change to `convert_day.py`, to `DedupeSet`/`ObjectSpool`, or to
    any caller's use of the spool's public interface.
  - Still strictly out of scope, per the approved checkpoint: Phase 7,
    Tier 3, production deployment, custom binary format, retention
    gate, uv, KovacsTrader.
- **Issue #20 Phase 5 semantic correction: preserve synchronization
  continuity events** — the canonical Tier-2 gate correctly reported
  `continuity_diagnostics`/`fenced_ranges` as failed (reference: 34
  fenced ranges; candidate: 1) for both `schema_version=0` and `=1`,
  proving a pre-existing replay-builder semantic defect, not an
  acceptable v1-specific gap. Root cause, found by direct inventory of
  the ADAUSDT 2026-06-12 raw fixture (record_type values present:
  `depth_update`, `sync_state`, `snapshot_seed`, `stream_lifecycle` —
  no others guessed or assumed): `pipeline/build_replay_store.py`'s
  `_convert_depth_record()` dropped every record type except
  `snapshot_seed`/`depth_update`, silently discarding `sync_state`
  records before the shared depth-replay engine's
  `record_type == "sync_state"` branch (which drives desync/resync state
  and fenced-range open/close) could ever see them.
  - `_convert_depth_record()` now preserves `sync_state` and
    `stream_lifecycle` records. `sync_state` records carry no book
    payload and no `U`/`u`/`pu` (they use `last_update_id`/
    `prev_update_id` instead); their full state transition
    (`state`/`previous_state`/`reason`/`last_update_id`/`prev_update_id`)
    is round-tripped through the existing, already-nullable
    `quality_flags` JSON column — no new physical schema field for
    either v0 or v1. `stream_lifecycle` records are preserved because the
    shared engine's session-change fence-close/open detection runs
    unconditionally for every record type using the CURRENT record's
    timestamp; since `stream_lifecycle` records are the actual first/
    last record of every raw session, dropping them shifted the observed
    fence-close/open timestamp to whatever record happened to follow —
    confirmed by diffing the reference's and pre-fix candidate's full
    fenced-range lists: 31 of 34 fences differed ONLY in `end_ts_ns`, by
    exactly the raw gap between the dropped `stream_lifecycle` record
    and the next preserved record.
  - `stores/replay_schema.py`'s `DEPTH_RECORD_TYPE_CODES` v1 enum gained
    `sync_state=2`/`stream_lifecycle=3` (v0's existing codes for
    `snapshot_seed`/`depth_update` are unchanged — v0 physical-schema
    compatibility preserved, `record_type` remains the existing string
    field).
  - `stores/replay_depth_adapter.py`'s `replay_row_to_depth_record()`
    recovers a `sync_state` row's `state`/`reason`/`previous_state`/
    `last_update_id`/`prev_update_id` from `quality_flags` (via new
    `_sync_state_transition()`), which is exactly what the shared depth
    engine's `sync_state` branch reads.
  - `validation/validate_catalog_equivalence.py`'s raw-to-replay metadata
    comparator (`_DEPTH_ACCEPTED_RECORD_TYPES`, `_normalize_raw_depth_record()`)
    was updated identically, so it compares `sync_state`/
    `stream_lifecycle` records like-for-like instead of flagging a
    spurious raw-vs-replay mismatch now that they are written to replay.
  - **Cross-day carry recovery** (a second, distinct gap found while
    diffing the remaining single fence after the sync_state/
    stream_lifecycle fix): a session that began on the prior UTC day (its
    first record in the target day's raw file has no preceding
    `snapshot_seed` within that day) was immediately fenced from its
    very first record by the replay reconstruction path, whereas
    `convert_day.py`'s raw path recovers such sessions via its documented
    cross-day carry-spool mechanism (reading the adjacent day's raw
    partition). Added an equivalent, bounded, disk-backed carry mechanism
    to `converter/depth_phase2.py`'s `replay_records_to_depth_streaming()`
    (new optional `carry_records` parameter, reusing the exact
    `_recover_carry_state_from_spool()`/`_emit_synthetic_opening_snapshot()`
    helpers the raw path already uses — never a full-day Python list;
    omitting `carry_records`, the previous default, leaves behavior
    unchanged for any other caller). `validation/replay_catalog_reconstruct.py`
    now also builds the previous day's replay partition (if raw data for
    it exists) purely to supply its depth rows as `carry_records` — it is
    never itself part of the requested date's reconstructed catalog
    output. This mirrors convert_day.py's behavior for both v0 and v1
    logical replay identically (same shared engine, same helpers).
  - Added `tests/test_replay_sync_continuity.py` (14 tests): proves
    `_convert_depth_record()` no longer drops `sync_state`/
    `stream_lifecycle`, still deliberately drops unsupported record
    types, `sync_state` survives v0 and v1 writer/reader round-trip with
    ordering relative to snapshots/depth_updates preserved,
    desync/resync flags survive, dropping a `sync_state` record changes
    reconstructed continuity evidence (`metrics.resync_count`), the
    candidate reconstructs the expected fenced ranges from a synthetic
    desync/resync/re-snapshot sequence, and cross-day carry recovery
    both recovers a session started on a prior day (matching
    `convert_day.py`'s behavior) and remains fully backward-compatible
    when `carry_records` is omitted.
  - **Tier-2 re-run** (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`): **all 7 gating
    components now pass for both `--schema-version 1` and
    `--schema-version 0`** — instrument IDs, instrument precision,
    exhaustive trade_ticks, exhaustive order_book_deltas, book_checkpoints
    (7/7), Depth10, `continuity_diagnostics`, and `fenced_ranges` (34/34
    fences, byte-identical canonical digest). Overall report status:
    `"passed"` for both schema versions.
  - Full suite: 518 passed, 3 skipped (up from 504); guards 56 passed.
  - No change to `convert_day.py` or to the fenced-range/continuity
    comparators' semantics — the reference remains the unweakened
    behavioral oracle; only the replay-side conversion/reconstruction
    gained the missing continuity-event preservation and cross-day carry
    it was previously missing.
  - Still strictly out of scope, per the approved checkpoint: Phase 6,
    Tier 3, production deployment, custom format, retention gate, uv,
    KovacsTrader.
- **Issue #20 Phase 5 (revised-plan phase numbering) — compact versioned
  replay schema v1 prototype**, implemented after the Phase 0–4 review
  checkpoint (baseline, semantic oracle, raw-retention/traceability/
  versioning design, field/consumer/integrity matrix, repo-boundary
  alignment — all previously completed and approved). Legacy v0 remains
  fully intact, unchanged, and the default; v1 is opt-in via
  `ReplayWriter(..., schema_version=1)`.
  - `stores/replay_schema.py`: added `DEPTH_REPLAY_SCHEMA_V1`/
    `TRADE_REPLAY_SCHEMA_V1` plus `format_version`/`schema_version`/
    `builder_version` constants, enum-code maps, a packed depth-flags
    bitmask, and `Decimal`-only fixed-point mantissa encode/decode
    helpers — every compaction lever is justified inline against the
    checked-in Phase 3 field/consumer/integrity matrix
    (`docs/IMPLEMENTATION_AUDIT.md`). Compacted: `venue`/`symbol`/`date`
    (moved to manifest — matrix: partition-constant), `record_type` (int8
    enum), the 5 depth boolean columns (packed into one int8 bitmask),
    `price`/`size`/`quantity` (exact fixed-point int64 mantissa, scale
    derived once per partition from date-specific Binance
    `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize`/`MARKET_LOT_SIZE`, spot and
    futures independently, parsed via `Decimal` only), and
    `native_payload_hash` (32 raw bytes instead of 64-character hex — the
    hash itself is retained, since the Phase 2 Section 3 traceability
    replacement remains design-only and hash removal is not authorized).
    Deliberately NOT compacted (matrix: "pending proof"/"benchmark-needed"):
    `U`/`u`/`pu`, `trade_id`/`agg_trade_id`, `market_type`,
    `quality_flags`.
  - `stores/replay_writer.py`: `ReplayWriter` gained `schema_version`
    (0 default / 1), `price_scale`, `qty_scale` constructor args;
    `_derive_fixed_point_scales()` derives exact scales from exchangeInfo
    filters (raises clearly, never guesses, if filters are missing);
    `_project_depth_row_v1()`/`_project_trade_row_v1()` project a v0-shaped
    spooled record down to the compact v1 physical row, one record at a
    time (bounded memory, unchanged from v0's batch-bounded write path).
    v1 manifests additionally carry `format_version`, `schema_version`,
    `builder_version`, `encoding_profile`, `price_scale`, `qty_scale`, and a
    best-effort `source_identity` (per-file SHA-256 + size for the raw
    files that produced the partition, via the new
    `pipeline.raw_manifest.compute_raw_source_identity()` — provenance
    evidence only, not required for reconstruction).
  - `stores/replay_reader.py`: `ReplayReader.get_schema_version()`
    dispatches on the manifest's `schema_version` (missing = v0; explicit
    version outside `{0, 1}` raises `ValueError` naming found vs
    supported); `iter_depths()`/`iter_trades()` decode v1 physical rows
    (via new `_decode_depth_row_v1()`/`_decode_trade_row_v1()`) back to the
    exact v0 logical row shape, so every existing downstream consumer
    (`stores/replay_depth_adapter.py`,
    `validation/validate_catalog_equivalence.py`) requires zero changes to
    read either version. This decode logic is independent of, and does not
    import, `convert_day.py`/`converter/depth_phase2.py`.
  - `pipeline/raw_manifest.py`: added `compute_raw_source_identity()`
    (bounded-memory streaming SHA-256 over raw files, per the Phase 2
    traceability design's item 1).
  - Added `tests/test_replay_schema_v1.py` (46 tests): version dispatch,
    unsupported-version failure, v0-fixture-unaffected, exact fixed-point
    round trips (including values not exactly representable as float64),
    spot/futures filter-derived scale independence, depth/trade record
    types and packed flags round-trip, null/optional ID handling, int64
    boundary mantissas, partition constants restored via the manifest (not
    physically present in rows), quality/continuity survival,
    integrity/source-identity fields, canonical ordering preservation,
    writer/reader bounded-memory proofs (live-object-counter pattern and a
    `pq.ParquetFile.iter_batches` batch-size spy), and a physical-size
    development-evidence assertion (v1 depth.parquet smaller than v0 for
    identical logical rows).
  - **Oracle correction discovered during the Tier-2 gate**: the real
    ADAUSDT 2026-06-12 comparison (see Tier-2 results below) found that
    `compare_book_checkpoints_streaming()`'s book-state hash/comparison
    was literal-string-sensitive — v1 formats prices/quantities at the
    instrument's exact required scale (e.g. 4 decimals) while legacy v0
    preserves Binance's literal 8-decimal wire-format padding; both
    represent the exact same numeric value. Added
    `_canonical_decimal_str()`/`_canonical_book_state()` to
    `validation/catalog_compare.py` (`Decimal`-only, never a float
    intermediate, never rounds/quantizes genuinely different values into
    equality — only strips numerically insignificant zero-padding) and
    applied them to `compare_book_checkpoints_streaming()`'s comparison
    and hash. Added `tests/test_book_checkpoint_hash_canonicalization.py`
    (22 tests) proving: equal-value-different-padding strings canonicalize
    identically; genuinely different values remain distinct; no scientific
    notation; no float intermediate (including a value beyond 2**53);
    whole-book-state and hash-level equivalence. This is an oracle
    (validation-only) correction — no change to `convert_day.py`, the
    reference converter, replay encoding, or Nautilus catalog behavior.
  - `docs/REPLAY_STORE.md`: added a "Versioning (v0 / v1)" section
    describing the v0/v1 contract and every v1 physical difference
    honestly, including local ADAUSDT development-evidence size
    measurements (see `docs/CHANGE_AUDIT.md` for exact figures).
  - **Tier 1 (synthetic)**: all 46 + 22 new tests pass; full suite 482
    passed, 3 skipped (up from 460); repo-structure/agent-infrastructure
    guards 56 passed.
  - **Tier 2 (local real data, ADAUSDT, 2026-06-12, single symbol/day —
    development evidence only, not a Tier-3 representative-day claim)**:
    reference (`data_raw -> convert_day.py -> temporary Nautilus catalog`)
    vs candidate (`data_raw -> compact replay v1 -> existing replay
    reconstruction path -> temporary Nautilus catalog`), compared via the
    exhaustive, order-preserving, gating semantic oracle
    (`compare_trade_ticks_exhaustive`, `compare_order_book_deltas_exhaustive`,
    `compare_book_checkpoints_streaming`,
    `compare_order_book_depth10_exhaustive`) — all four passed
    (124,457 trades; 412,317 order_book_deltas; 71,341 depth10 records;
    7 book checkpoints, all hash-matching after the canonicalization fix).
    Local size comparison: v0 depth.parquet 38,997,712 bytes
    (94.58 bytes/depth event) vs v1 29,071,749 bytes
    (70.50 bytes/depth event) — 1.34x; v0 trades.parquet 7,347,664 bytes
    (59.04 bytes/trade) vs v1 6,538,715 bytes (52.54 bytes/trade) — 1.12x;
    combined 1.30x. This is single-symbol/single-day development evidence
    only — it is not, and must not be read as, a 5 GiB/2 GiB complete-day
    Tier-3 target claim, which remains a later, explicitly out-of-scope
    gate.
  - Strictly out of scope for this entry (per the approved checkpoint) and
    **not started**: Phase 6 external-merge/SQLite replacement, full
    Tier-3 production-day build, format-selection Phase 7, a custom binary
    format, Phase 9 self-contained-replay acceptance, raw-retention
    deletion-gate implementation, staging lifecycle/locking/quarantine/
    backlog reconciliation, disk-monitor/systemd changes, a selected-
    reconstruction CLI, production deployment/data cleanup, uv migration,
    and any KovacsTrader change.
- **Issue #20 Phase 5 corrective commit: complete v1 logical and
  validation contract** — a review of `76a61e5` correctly found 4
  blockers in the Phase 5 prototype (commit not reverted; the prototype
  remains directionally accepted):
  1. **Complete logical-row contract restored**: `ReplayReader`'s v1
     depth/trade decoders (`_decode_depth_row_v1()`/`_decode_trade_row_v1()`
     in `stores/replay_reader.py`) omitted `venue`/`symbol`/`date` from
     every decoded row even though v0 rows carry them and the module
     docstring claimed parity. Both decoders now accept the partition
     identity and include `venue`/`symbol`/`date` in every row. Proved by
     new tests comparing the COMPLETE key set and values of equivalent v0
     and v1 rows (not just the manifest).
  2. **Version-aware partition validation**: `stores/replay_writer.py`'s
     `validate_partition()` previously validated only status/checksums,
     regardless of `schema_version`. It now dispatches on the manifest's
     `schema_version` (absent = legacy v0) and, for v1, additionally
     requires `format_version` compatibility, valid integer
     `price_scale`/`qty_scale`, a complete `encoding_profile`, and that the
     on-disk depth/trades Parquet physical schemas actually match the
     declared version's expected schema (`_schema_matches()`) — an
     unsupported version, missing v1 metadata, or a v0/v1 physical-schema
     mismatch now fails validation clearly instead of being silently
     accepted as a valid, skippable partition.
  3. **Explicit non-default v1 path through the canonical builder**:
     `pipeline/build_replay_store.py`'s `build_replay_for_symbol()` gained
     an explicit `schema_version` argument (default `0`, unchanged
     production behavior) plus a `--schema-version {0,1}` CLI flag on
     `python -m pipeline.build_replay_store`, so v1 can be built through
     the canonical `data_raw -> build_replay_for_symbol` route for
     development validation without any ad-hoc Python build script. No
     systemd/production configuration was changed; `pipeline/daily_build.py`
     does not pass this argument and is therefore unaffected.
  4. **Source identity bound to the raw root actually consumed**:
     `ReplayWriter` no longer calls `compute_raw_source_identity()`
     itself (which previously used the global `config.DATA_ROOT` by
     default); the canonical builder now computes it explicitly using the
     EXACT `data_root` and channels it streamed from, and supplies it via
     a new `ReplayWriter.set_source_identity()`/`source_identity`
     constructor argument. Also fixed `converter.instruments.load_exchange_info()`
     and `stores.replay_writer._derive_fixed_point_scales()` to accept an
     explicit `data_root` (previously hardcoded to `config.DATA_ROOT`),
     so a custom `--data-root` build's fixed-point scale derivation also
     reads exchangeInfo from the same root it consumed for everything
     else. Proved by a new test building the same venue/symbol/date from
     two different raw roots with different file content and asserting
     each manifest's `source_identity` reflects only its own root's
     checksums.
  - **Pre-existing gap fixed while re-running Tier-2 through the
    canonical builder** (independent of `schema_version`, affects v0 and
    v1 identically): `build_replay_for_symbol()`'s `instrument_metadata`
    never included the raw exchangeInfo `filters` list, causing
    `validation.replay_catalog_reconstruct`'s `build_instruments()` to
    silently fall back to `converter.instruments._default_info()`'s
    generic defaults — producing a different price/size precision than
    the reference `convert_day.py` path and failing the canonical
    instrument-precision gate for any replay-based candidate. Fixed by
    including `filters` in `instrument_metadata`; regression test added.
  - Also fixed a pre-existing bug in
    `validation/validate_catalog_equivalence.py`'s CLI `main()` summary
    print, which referenced stale `comparison` keys
    (`trade_count_old`/`timestamp_range_match`) that no longer exist in
    the per-instrument report shape and crashed with `KeyError` after a
    successful comparison — cosmetic only, no comparison-logic change.
  - Added `tests/test_replay_schema_v1_corrections.py` (20 tests) covering
    all 4 blockers above plus the instrument-metadata regression.
  - **Tier 2 re-run through the canonical builder + canonical validator**
    (`validation.validate_catalog_equivalence`, not a manual four-function
    subset), ADAUSDT, 2026-06-12, `schema_version=1`, with normal
    instrument-metadata publication: instrument IDs match, instrument
    precision matches (after the filters fix), exhaustive trade_ticks
    match, exhaustive order_book_deltas match, book_checkpoints match
    (7/7), Depth10 matches, and `raw_to_replay_metadata` (quality/
    continuity evidence) matches — **6 of 7 gating components pass**.
    `continuity_diagnostics`/`fenced_ranges` do NOT pass
    (34 old vs 1 new fenced ranges) — confirmed, by re-running the
    identical canonical validator with `schema_version=0`, to be an
    **exactly identical, pre-existing v0 gap**, not a v1 regression: it is
    the already-documented `sync_state`-fenced-range-bookkeeping caveat in
    `docs/FULL_L2_REPLAY_CATALOG_PLAN.md` (the replay builder drops
    `sync_state` records for both schema versions identically). Per this
    correction's own instruction, this is reported honestly as **partial**
    evidence, not silently downgraded or hidden: **Tier 2 status is
    "6/7 canonical gating components pass; fenced-range/continuity
    evidence has the same pre-existing gap as legacy v0, not a v1-specific
    regression."** This is not claimed as a full Tier-2 pass.
  - Focused/broad test commands and full-suite/compliance results are
    recorded in `docs/CHANGE_AUDIT.md`'s entry for this correction.
  - No Phase 6, Tier 3, production deployment, custom format, retention,
    uv, or KovacsTrader work was started.
- **Issue #20 Phase 1 second follow-up correction: gating book checkpoints,
  Depth10, complete fenced-range digest, and RAM-bounded raw-to-replay
  metadata comparison** — the previous correction wired exhaustive
  trade/delta comparison into the real path, but book checkpoints and
  Depth10 were still marked `"gating": False` full-day-list diagnostics,
  fenced-range comparison only checked the reference's 3-example
  truncation and treated a candidate's extra fence as expected/non-gating,
  and quality-flag comparison collected a full day into Python lists and
  compared them as an order-independent multiset. All four are corrected:
  - **Book checkpoints**: new `reconstruct_book_checkpoints_streaming()`/
    `compare_book_checkpoints_streaming()` in `validation/catalog_compare.py`
    process a windowed `OrderBookDeltas` iterator sequentially, retaining
    only the current book state plus the requested checkpoint snapshots —
    never materializing a full-day list — and add a deterministic SHA-256
    hash per checkpoint. `validate_catalog_equivalence()` now feeds this
    from a second pair of windowed delta iterators (checkpoints need an
    independent traversal from the exhaustive delta comparison) and its
    `passed` result gates the final status; the full-day
    `load_order_book_deltas()`-based `compare_book_checkpoints()` path is
    no longer used by the acceptance path at all.
  - **Depth10**: new `iter_order_book_depth10_windowed()` +
    `compare_order_book_depth10_exhaustive()` compare every Depth10
    snapshot positionally (no sampling, no re-sorting) when
    `--emit-depth10`/`emit_depth10=True` is enabled, and gate `passed`;
    when explicitly disabled, it is reported as `{"skipped": True,
    "passed": True}` rather than compared-but-ignored.
  - **Fenced ranges**: `convert_day.py` now computes a
    `canonical_count`/`canonical_digest` (SHA-256 over the COMPLETE
    per-symbol fenced-range collection — new `canonical_fence_digest()` in
    `converter/depth_phase2.py`) alongside its existing 3-example
    `examples` field, and `validate_catalog_equivalence.py`'s new
    `compare_fenced_ranges_digest()` gates on that complete digest instead
    of the truncated example list — an extra candidate fence, or a
    difference beyond the 3rd example, now correctly fails; the previous
    `gating_passed`/"extra_in_new is expected" carve-out is removed.
  - **Quality/continuity metadata**: new
    `compare_event_metadata_exhaustive()` in `validation/catalog_compare.py`
    compares raw-vs-replay logical metadata (continuity IDs, sync/desync/
    resync state, `quality_flags`) at each canonical event position,
    keeping information associated with its source event instead of a
    multiset — detecting a value moved to a different event even when the
    overall multiset is unchanged. `validate_catalog_equivalence.py`'s raw
    side is sorted into canonical `(session_id, session_seq, raw_index)`
    order via `converter.spool.RawRecordSpool` (an existing disk-backed
    bounded spool, not a full-day Python list); the replay side streams
    from `stores.replay_reader.ReplayReader`, already guaranteed sorted by
    the replay-store contract. Both channels (depth_v2, trade_v2) are
    filtered to the same record types the replay writer actually converts,
    to avoid spurious mismatches from intentionally-dropped raw record
    types (e.g. `sync_state`).
  - Added `tests/test_streaming_gating_bounded_memory.py` (6 tests):
    empirical live-object-counter proofs that
    `compare_book_checkpoints_streaming()`,
    `compare_order_book_depth10_exhaustive()`, and
    `compare_event_metadata_exhaustive()` all stay bounded-memory
    (independent of stream length, 20,000–50,000 synthetic events) while
    still detecting a difference injected near the end of the stream,
    including a value moved between two events with the multiset
    unchanged.
  - Rewrote `tests/test_validate_catalog_equivalence_exhaustive_wiring.py`
    (23 tests) to prove, through the real `validate_catalog_equivalence()`
    orchestration: book-checkpoint mismatch, enabled-Depth10 mismatch,
    Depth10 reorder, Depth10-disabled-is-intentionally-skipped, a
    fenced-range mismatch after the first 3 matching examples, an extra
    candidate fenced range, a quality flag moved to the wrong event with
    an unchanged overall multiset (via the real raw→replay pipeline,
    monkeypatching only the raw-side generator), changed `U`/`u`/`pu`,
    changed sync/desync/resync state, and missing/extra diagnostic
    events — plus the existing trade/delta/instrument/continuity
    scenarios and two regression guards (a static import check and a
    call-counting spy proving the gating comparators are genuinely
    invoked).
  - No compact replay schema was changed. This remains Phase 1 (oracle
    hardening) work, still gating any future compact schema
    implementation (Phase 5+, not started).
- **`validation/validate_catalog_equivalence.py` — wire the exhaustive
  oracle into the real acceptance command (follow-up correction to the
  Phase 1 oracle work)** — the previous Phase 1 correction added
  `compare_trade_ticks_exhaustive()`, `compare_order_book_deltas_exhaustive()`,
  and the windowed catalog loaders, but the actual `validate_catalog_equivalence()`
  orchestration — the function the CLI and Tier-2/Tier-3 acceptance path
  call — still used the old sampled trade comparator, the multiset delta
  comparator, and full-day list loaders. This is now corrected: the real
  path uses `load_instruments()` + `compare_instruments_semantic()`;
  `iter_trade_ticks_windowed()` + `compare_trade_ticks_exhaustive()`;
  `iter_order_book_deltas_windowed()` + `compare_order_book_deltas_exhaustive()`;
  `compare_continuity_diagnostics_semantic()` (reference-side
  `per_symbol_depth` from convert_day.py's own report JSON, loaded via a
  new `_load_old_convert_report()`, against the candidate manifest's
  `depth_diagnostics`); `compare_fenced_ranges_semantic()` (reference-side
  `per_symbol_fenced_ranges[...]["examples"]` against the candidate
  manifest's `fenced_ranges` filtered to that venue/symbol — the
  reference's known example-truncation to 3 fences is documented and only
  the `missing_in_new` direction gates `passed`, exposed as
  `gating_passed`); and `compare_quality_flags_semantic()` (the permanent
  raw source read directly via `stream_raw_records()` against the replay
  pipeline's own `ReplayReader`-read `quality_flags` column — the only
  place this field exists on both a reference and candidate side, since
  Nautilus catalog objects never persist it). Every one of these `passed`
  results now contributes to `report["status"]`. `compare_book_checkpoints()`
  and Depth10 comparison remain wired but are explicitly non-gating
  diagnostics (`"gating": False`) since both still require full-day list
  materialization with no windowed equivalent today — a documented,
  deliberate limitation, not silently hidden. The legacy sampled/multiset
  comparators and the full-day `load_trade_ticks()`/`load_order_book_deltas()`-
  as-the-primary-loader pattern are no longer imported or used by this
  module at all.
- **Duplicate-event semantics corrected** in
  `compare_trade_ticks_exhaustive()`/`compare_order_book_deltas_exhaustive()`:
  two identical ordered streams now correctly pass even when both contain
  the exact same duplicate event at the same position — equivalence means
  the reference and candidate streams are identical, including identical
  duplicate occurrences. The previous version incorrectly treated "a
  duplicate exists on either side" as an independent failure condition,
  backed by an O(window)-per-event `_BoundedDedupeWindow` bookkeeping
  structure using a Python list with `pop(0)` (O(N) per eviction once the
  window filled). Both were removed rather than merely made more
  efficient: the existing positional/length comparison already fully
  detects any duplicate-related discrepancy that can actually indicate
  non-equivalence (an extra, missing, or differently-positioned duplicate
  shifts every subsequent position). This keeps the exhaustive comparison
  O(N) end-to-end, remaining practical at 200M+ events.
- **Windowed-loader boundary bug found and fixed**:
  `iter_trade_ticks_windowed()`/`iter_order_book_deltas_windowed()`
  previously assumed Nautilus's `catalog.trade_ticks(start=a, end=b)` /
  `catalog.order_book_deltas(start=a, end=b)` queries were half-open
  `[a, b)`. Direct testing against a real on-disk `ParquetDataCatalog`
  (`tests/test_windowed_loader_boundaries.py`, new) proved this assumption
  false: the query is inclusive on **both** `a` and `b`. The previous
  window-chaining logic (`next_window_start = previous_window_end`)
  therefore double-yielded any event landing exactly on an internal window
  boundary. Both loaders now partition the caller's half-open
  `[start_ns, end_ns)` range into non-overlapping **closed** sub-windows
  (`window_end = window_start + window_ns - 1`, `next_window_start =
  window_end + 1`), which is safe because all Nautilus event timestamps
  are integer nanoseconds. `window_ns` remains fully configurable (proven
  via `test_window_duration_is_configurable`), and the docstrings no
  longer claim a fixed time window is a strict event-count/RSS memory
  bound — only that it bounds query result size per window, to be tuned
  against measured per-window RSS on real production data (issue #20
  Tier 3).
- **`tests/test_validate_catalog_equivalence_exhaustive_wiring.py`** (new,
  12 tests) — end-to-end integration tests through
  `validate_catalog_equivalence()`'s real orchestration (not the
  comparator helpers called directly), proving it fails for: a trade
  mismatch beyond the legacy sampled comparator's 100 positions; reordered
  trades; reordered commutative-looking depth deltas (with a sanity check
  that the non-gating book-checkpoint diagnostic legitimately still
  matches, proving why the exhaustive comparison must be the gate);
  extra/missing trades; an instrument precision/increment mismatch; a
  continuity (resync-count) mismatch; a fenced-range mismatch; and a
  quality-flags mismatch — plus a passing baseline and two regression
  guards (a static check that the sampled/multiset comparator names are
  no longer importable from the module, and a call-counting spy proving
  the exhaustive comparators are genuinely invoked during a real run, not
  merely importable-but-unused).
- **`validation/catalog_compare.py` — Phase 1 correction: exhaustive,
  order-preserving, bounded-memory oracle comparators** — the original
  Phase 1 oracle hardening left two real gaps against the issue #20
  contract: `compare_trade_ticks_semantic()` only samples up to
  `sample_count` positions after re-sorting both streams, and
  `compare_order_book_deltas_semantic()` is a multiset (sorted)
  comparison — neither can detect a difference outside the sampled
  positions, a pure reordering of otherwise-valid events (sorting erases
  position), or a reordering of "commutative-looking" depth deltas that
  happens to produce the same final book state. Added
  `compare_trade_ticks_exhaustive()` and
  `compare_order_book_deltas_exhaustive()`: both compare **every** event
  at its original stream position (no sampling, no re-sorting), stream
  both inputs lazily via `itertools.zip_longest` (never materializing
  either side into a list internally, so memory is bounded and
  independent of total event count), detect duplicate events via a new
  `_BoundedDedupeWindow` (O(window) memory, documented trade-off vs. a
  true O(total-events) global duplicate check), and report the first
  position where stream lengths diverge (extra/missing events). Added
  `iter_trade_ticks_windowed()` / `iter_order_book_deltas_windowed()`:
  bounded-memory catalog loaders that fetch in fixed time windows
  (default 1 hour) rather than materializing an entire requested range up
  front — the necessary companion to the new comparators for a complete
  production day's tens/hundreds of millions of events. Added
  `tests/test_semantic_oracle_exhaustive_streaming.py` (11 tests) proving:
  a difference outside the legacy sampler's selected positions is caught;
  a pure reordering the legacy sampled/multiset comparators miss is
  caught (for both trades and deltas); a reordering of commutative-
  looking depth deltas that produces an *identical* final book state (as
  verified via the existing `compare_book_checkpoints()`) is still caught
  by the new comparator; extra/missing/duplicate (both added and removed)
  events are detected; and — empirically, via a live-object counter, not
  merely asserted — peak simultaneously-alive event objects stays small
  and independent of stream length (proven on a 20,000-trade / 5,000-delta
  synthetic stream) while a difference injected 3–5 positions before the
  end is still detected, proving the whole stream is genuinely scanned.
  No compact replay schema was changed; this is a correction to the
  already-committed Phase 1 oracle-hardening work, still gating any
  future compact schema implementation.
- **`validation/audit_storage_size.py` — issue #20 Phase 0 baseline
  breakdown** — the storage-size audit CLI now reports allocated (actual
  disk blocks, `st_blocks * 512`) *and* apparent (`st_size`) bytes for every
  component, instead of apparent bytes only. It also computes per-trade,
  per-depth-event, and per-depth-level byte estimates from a partition's
  manifest record counts and an exact depth-level count (via pyarrow),
  explicitly flagged as orientation-only since depth events carry a varying
  number of book levels — a single "bytes per replay row" average hides
  that variance. A new `audit_scratch_bytes()` function / `--scratch-only`
  CLI flag performs a **root-wide** scan of `.staging_*`/`.backup_*`/
  `.quarantine_*` directories across the entire `--replay-root`,
  independent of any single day's eligible venue/symbol universe — this is
  measurement-only and never deletes, renames, or otherwise mutates any
  discovered directory (it is the diagnostic groundwork for detecting, but
  not yet cleaning, orphans like the known BANKUSDT `2026-07-21` staging
  directory). This is Phase 0 of the issue #20 compact-replay-storage plan;
  no replay schema, builder, or lifecycle behavior was changed.
- **`validation/catalog_compare.py` — issue #20 Phase 1 oracle-coverage
  gaps closed** — the semantic-equivalence comparator gained five new
  comparison functions the coverage audit found missing against the
  issue's full contract: `compare_instruments_semantic()` (precision and
  price/size increment, not just instrument-id set membership — a wrong
  `price_precision`/`tick size` previously would not have been detected);
  `compare_continuity_diagnostics_semantic()` (snapshot-seed/resync/
  desync/fenced-range **counts**, comparing the reference route's
  `per_symbol_depth` report fields against the candidate route's
  `depth_diagnostics` manifest fields — normalizing the two independently
  chosen naming conventions, e.g. `resync_count` vs. `resyncs`, since both
  originate from the same shared `converter.depth_phase2.Phase2ReplayMetrics`
  dataclass but were renamed differently at each call site);
  `compare_fenced_ranges_semantic()` (per-fence content comparison, not
  just a count); `compare_quality_flags_semantic()` (decoded JSON-content
  comparison of the `quality_flags` field, not raw string equality). None
  of these fields are visible in the Nautilus catalog objects themselves
  (`TradeTick`/`OrderBookDeltas`/`OrderBookDepth10`), so the pre-existing
  comparators could never have caught a regression in them.
- **`tests/test_semantic_oracle_detects_injected_faults.py`** (new, 19
  tests) — proves the oracle actually detects each required fault class
  by injecting exactly one deliberate corruption at a time into an
  otherwise-passing synthetic pair: wrong trade price, wrong trade
  timestamp, dropped trade, dropped delta, wrong sequence number, wrong
  flag, wrong side, missing snapshot-seed/CLEAR delta, wrong Depth10
  level, a mismatched deterministic book-state checkpoint, wrong
  instrument precision, a missing instrument, wrong
  snapshot/resync/desync/fenced-range counts, a missing fenced range by
  content, and a corrupted quality-flag value. A structural test also
  proves the reference and candidate decoding paths remain independently
  implemented (the comparator does not import the replay-schema-specific
  decoder, and the decoder does not import the comparator), so a shared
  bug in new compact-decoding logic could not silently pass both sides of
  a future schema comparison. This is Phase 1 of the issue #20
  compact-replay-storage plan; no compact schema was implemented.
- **`docs/IMPLEMENTATION_AUDIT.md` — issue #20 Phases 2–3 design record**
  (docs only, no code) — added the raw-retention safety contract (the
  precondition gate that must hold before any raw deletion of a
  venue/symbol/date unit is permitted, and the corrected atomic
  per-partition deletion-unit design that fixes the existing
  per-channel-independent deletion gap in `disk_monitor.py`'s
  `get_oldest_date_dir()`/`cleanup_old_data()`), the legacy-v0 inventory
  design (rebuildable/not-rebuildable/uncertain classification, with the
  legacy reader kept available indefinitely for the latter two — never
  described as "necessarily temporary"), the traceability design that
  replaces the previously-assumed "hash demotion is low-risk" framing with
  an explicit unresolved-pending-proof status, the versioning contract
  (legacy-v0-by-absence, explicit `format_version`/`schema_version`/
  `builder_version`, and a new planned `encoding_profile` manifest field),
  and the finalized field/consumer/integrity matrix for every column in
  `stores/replay_schema.py`'s current `DEPTH_REPLAY_SCHEMA` and
  `TRADE_REPLAY_SCHEMA`. This is design/audit documentation only — no
  compact schema, no `disk_monitor.py` deletion-unit change, no manifest
  field, and no raw-retention gate was implemented in this change.- **`docs/REPO_STRUCTURE.md` / `tests/test_repo_structure.py` — issue #20
  Phase 4 repository-boundary alignment** — deliberately re-scoped the
  `pipeline/` package contract to permit exactly one future, explicitly-
  scoped selected-reconstruction CLI (development-computer, temporary
  catalog, explicit venue/symbol list and start/end time window only),
  reversing the issue #17 removal of the old unscoped
  `pipeline/generate_catalog.py` product CLI for that one narrow, bounded
  case. The old unscoped name/shape remains permanently forbidden. Rewrote
  (not deleted) the corresponding guard test —
  `test_pipeline_does_not_contain_generate_catalog_cli()` became
  `test_pipeline_reconstruction_cli_stays_explicitly_scoped()` — which
  still asserts `pipeline/generate_catalog.py` must never exist, and now
  additionally scans any other module that may later appear in `pipeline/`
  for unscoped-reconstruction markers (`all_symbols`, `full_universe`,
  etc.), so a future CLI cannot silently default to an unscoped selection.
  Added a 2026-07-24 amendment-log entry to `docs/REPO_STRUCTURE.md`. No
  reconstruction CLI has been implemented yet — this is a contract/guard
  change only, ahead of the future implementation phase.
### Changed
- **`systemd/cryptorecorder-replay-build.service` — `TimeoutStartSec` raised
  from `3600` (1 hour) to `23h`** — the replay-build `oneshot` unit's
  systemd-imposed maximum runtime was too short at 1 hour for a full-universe
  replay build of the previous completed UTC day. An unbounded (`infinity`)
  timeout was considered and **rejected**: the daily timer fires once at
  `01:00 UTC` and systemd will not start a new instance while an existing one
  is still active, so a genuinely stuck invocation must not be allowed to
  remain active indefinitely — that would silently block every later
  scheduled run. `23h` gives ample room for a long daily build while still
  guaranteeing systemd terminates a stuck/hung run before the next `01:00
  UTC` activation. If the ceiling is reached, systemd marks the invocation
  failed; because `Restart=no` (unchanged), no restart loop is created — the
  operator must inspect the journal and rerun manually.
  `StartLimitIntervalSec=86400` / `StartLimitBurst=3` in `[Unit]` are
  unchanged and still cap *restart* attempts if `Restart` is ever
  re-enabled. The installed service only ever builds the previous completed
  UTC day (`pipeline.daily_build --date yesterday`); it does not perform
  `--force` rebuilds or arbitrary historical backfills — those are run
  manually via the documented CLI or a separately controlled transient
  systemd scope with its own explicit timeout. See
  [docs/OPERATIONS.md](docs/OPERATIONS.md) "Replay-build memory and restart
  behaviour" for the updated "Start timeout" note.
- **`systemd/cryptorecorder-replay-build.timer` — stale converter comment
  removed** — the timer's `OnCalendar` comment previously said "Run after
  the legacy converter has had time to finish the previous UTC day", which
  no longer applies: converter systemd automation was removed from the
  supported architecture. Reworded to "Run at 01:00 UTC, after the previous
  UTC recording day has closed."

### Fixed (PR #18 finalization — fail-closed crash-recovery, best-effort backup deletion, converter files deleted)
- **`pipeline/build_replay_store.py` — `recover_partition_state()` extracted as
  tested helper** — all 7 filesystem states (Cases A-G: canonical+backup combos,
  valid/invalid/missing) are now handled by a dedicated function with explicit
  action return values (`"skip"` / `"rebuild"` / `"fail"`). The old inline
  crash-recovery handled only one case and silently dropped invalid backups.
  Cases A, C, D now restore or clean up backups before proceeding; cases B and E
  preserve invalid files for operator inspection and return `action="fail"`.
- **`pipeline/build_replay_store.py` — backup deletion is best-effort after
  successful `os.replace(staging, output)`** — previously any exception during
  backup deletion was re-raised, turning a successful partition publication into
  a build failure. Now backup deletion on the happy path is wrapped in a
  `try/except` that logs a warning and does not re-raise. The partition is
  published and the build returns `success` even if the old backup directory
  cannot be deleted.
- **`pipeline/build_replay_store.py` — all failure status values use `"failed"`**
  — all `status["status"] = "error"` occurrences in `build_replay_for_symbol`
  replaced with `"failed"` to match the values counted by `pipeline.daily_build`
  (`r["status"] == "failed"`). The old `"error"` value was silently excluded
  from the failed-partition count, making builds appear more successful than they
  were.
- **`systemd/cryptorecorder-convert.service` and `systemd/cryptorecorder-convert.timer`
  deleted** — converter systemd automation is not part of the supported production
  architecture. Stale installed converter units are still removed by
  `scripts/deploy_linux_server.sh` cleanup (no change to the deploy script).
  `convert_day.py` and `converter/` are unchanged and required.
- **`INSTALL.md` note updated** — the `> **Note:**` that previously claimed the
  converter templates were still present in the repo for manual use now
  correctly states they were deleted in PR #18 and references the manual CLI
  command instead.
- **`docs/OPERATIONS.md` updated** — references to converter units corrected to
  say "deleted from the repository in PR #18".

### Added (PR #18 finalization — recovery case tests and failure injection tests)
- `tests/test_replay_memory_bounded.py` — 10 new tests:
  - `test_recovery_case_f_valid_no_backup` — Case F: skip already-valid partition
  - `test_recovery_case_g_missing_no_backup` — Case G: rebuild when missing
  - `test_recovery_case_a_restores_valid_backup` — Case A: restore valid backup
  - `test_recovery_case_b_fails_on_invalid_backup_no_output` — Case B: fail on invalid backup
  - `test_recovery_case_c_valid_output_removes_stale_backup` — Case C: remove stale backup
  - `test_recovery_case_d_restores_backup_when_output_invalid` — Case D: quarantine+restore
  - `test_recovery_case_e_both_invalid` — Case E: both invalid, preserve both
  - `test_recovery_failure_counts_as_failed_status` — fail returns `"failed"` not `"error"`
  - `test_publish_backup_deletion_failure_does_not_fail_build` — best-effort backup deletion
  - `test_scratch_nonempty_prevents_publication` — fail-closed scratch cleanup

### Fixed (PR #18 — crash-recovery, fail-closed cleanup, partition layout, INSTALL.md)
- **`pipeline/build_replay_store.py` — crash-recovery for mid-publish SIGKILL**
  — a SIGKILL between the two `os.replace()` calls in `publish()` left
  `output_dir` missing and `.backup_{date}_{symbol}` present. The next
  `build_replay_for_symbol()` call now checks for this state before the
  stale-staging cleanup: validates the backup manifest/checksums and restores
  it to the canonical output path; fails closed (status=error) if the restore
  fails; removes an invalid backup and rebuilds. Both cases (backup+missing
  output, backup+existing output) are handled explicitly.
- **`pipeline/build_replay_store.py` — stale-staging cleanup fails closed**
  — previously used `ignore_errors=True` which silently continued on top of
  stale files if `rmtree` failed. Now catches `rmtree` exceptions and
  verifies the directory is gone; returns `status=error` if the staging dir
  still exists after cleanup, refusing to build on top of stale state.
- **`stores/replay_writer.py` — remove `scratch/` before publication**
  — after spools are closed and deleted, `finalize_staging()` now removes the
  empty `scratch/` subdirectory before writing the manifest. The published
  partition therefore contains only the supported files (`depth.parquet`,
  `trades.parquet`, `manifest.json`, `instrument.json`) and no subdirectories.
- **`INSTALL.md` — remove converter unit from manual installation and
  start-services sections** — the manual installation loop, `systemd-analyze
  verify` command, enable/start/stop/restart/status command blocks, and the
  troubleshooting section all previously referenced `cryptorecorder-convert.
  service` / `cryptorecorder-convert.timer` as active units to install.
  These sections now reference only the production units
  (`cryptorecorder-recorder.service`, `cryptorecorder-replay-build.service`,
  `cryptorecorder-replay-build.timer`). The stale "converter timer date"
  troubleshooting sub-section was removed. At the time of this change a
  `> **Note:**` was added stating the converter systemd unit files were kept
  in the repo for manual/reference use only; those unit files were
  subsequently **deleted** in a later PR #18 finalization commit (see the
  "converter files deleted" entry above) — `INSTALL.md` no longer contains
  that note and instead documents the manual `convert_day.py` CLI command.

### Added (PR #18 — crash-recovery and layout regression tests)
- `test_published_partition_layout_is_clean` — verifies that the published
  partition contains only supported files (`depth.parquet`, `trades.parquet`,
  `manifest.json`) and no subdirectories (no `scratch/`, SQLite, backups).
- `test_crash_recovery_restores_backup_on_startup` — simulates mid-publish
  SIGKILL (renames output to backup, asserts output is gone), then calls
  `build_replay_for_symbol()` and verifies backup is restored to canonical
  output without rebuilding.
- `test_stale_staging_cleanup_fails_closed` — places a non-removable stale
  staging dir (chmod no-write) and verifies the build returns `status=error`
  rather than building on top of stale files.

### Fixed (PR #18 — spool lifetime, atomic publication, force-rebuild)
- **`stores/replay_writer.py` — spool files now live inside `staging_dir/scratch/`**
  — previously spool files were created in the system temp directory
  (`/tmp` or `CRYPTO_RECORDER_REPLAY_SPOOL_TEMP_DIR`). On a SIGKILL/OOM,
  Python cleanup cannot run so orphaned multi-GiB SQLite files remained on
  disk indefinitely. Spools are now created under
  `staging_dir/scratch/` so that the existing stale-staging cleanup
  (`shutil.rmtree(.staging_*)`) removes both the partial Parquet output and
  all spool files in one step. The spool temp dir config is removed from the
  construction path (spools are always co-located with staging).
- **`stores/replay_writer.py` — `publish()` backup/restore for atomic
  publication** — the previous implementation called
  `shutil.rmtree(output_dir)` before `os.replace(staging, output)`. If
  `os.replace()` failed (I/O error, permissions, filesystem issue), the
  previously valid partition was already deleted. The fixed implementation:
  renames the existing valid partition to `.backup_{date}_{symbol}`, then
  calls `os.replace(staging, output)`, removes the backup on success, and
  restores the backup on failure. The valid published store is never lost.
- **`pipeline/build_replay_store.py` — `--force` flag and `force` parameter**
  — skip-if-valid only checks output integrity, not whether the raw inputs
  have changed or been backfilled. `--force` / `force=True` rebuilds even
  a fully valid partition. Documents the provenance contract: without
  `--force`, a complete checksum-valid partition is always skipped.

### Added (PR #18 — spool/publication regression tests)
- `test_spool_files_live_inside_staging_dir` — verifies spool files are under
  `staging_dir/scratch`, not in `/tmp`.
- `test_stale_staging_cleanup_removes_spools` — simulates a SIGKILL by
  creating a stale staging+scratch+fake-spool, confirms the next build
  removes them and succeeds.
- `test_publish_preserves_existing_partition_on_replace_error` — injects a
  failure at `os.replace(staging→output)` and confirms the original
  partition is restored intact.
- `test_force_rebuild_overrides_valid_partition` — confirms `force=True`
  rebuilds a partition that would otherwise be skipped.

### Real-data memory evidence (BTCUSDT 2026-06-12, local development machine)

```
Symbol:       BTCUSDT (heaviest locally available: 509 MB raw)
Venues:       BINANCE_SPOT + BINANCE_USDTF
Records:      835,403 depth + 3,112,086 trades (SPOT)
              563,875 depth + 3,200,399 trades (USDTF)
Exit status:  0 (success)
Elapsed:      10:30 wall-clock
Maximum RSS:  855,432 kB (~835 MiB)
Limit:        12,288,000 kB (12 GiB systemd MemoryMax)
Headroom:     ~93% free (835 MiB of 12 GiB)
```

Command used:
```
/usr/bin/time -v python -m pipeline.build_replay_store \
  --date 2026-06-12 --symbols BTCUSDT \
  --data-root data_raw \
  --replay-root /tmp/test_replay_btcusdt_<ts>
```


- **`stores/replay_writer.py` — replace unbounded Python-list accumulation with
  disk-backed SQLite spooling** — the previous implementation retained all
  depth and trade records for an entire symbol/day in `depth_batches` and
  `trade_batches` Python lists before writing, causing OOM kills on
  high-volume symbols (observed: `BINANCE_SPOT/DEXEUSDT` on 2026-07-21 with
  `MemoryPeak=12884901888`, `Result=oom-kill` against a 12 GiB systemd
  `MemoryMax`). Records are now spooled to a SQLite file on disk immediately
  via `converter.spool.RawRecordSpool` (the same proven infrastructure used
  by the converter). Parquet output is written incrementally through
  `pyarrow.parquet.ParquetWriter` in bounded batches of 5 000 rows (tunable
  via `CRYPTO_RECORDER_REPLAY_PARQUET_BATCH`). The depth channel is fully
  written and closed before the trade channel begins, so both channels are
  never simultaneously in memory. Peak RSS is now O(batch) rather than O(day).
- **`stores/replay_writer.py` — `cleanup_staging()` method** — safely removes
  staging directory and closes/deletes open spool files on error paths.
  `build_replay_for_symbol()` calls `cleanup_staging()` in the `except`
  branch so a failed build never leaves a stale staging directory.
- **`pipeline/build_replay_store.py` — skip already-valid partitions** — on
  restart, a partition is skipped only after strict validation: manifest must
  exist with `status=complete`, both Parquet files must be present, and their
  SHA-256 checksums must match the manifest values. An incomplete staging
  directory is never treated as valid. Stale staging directories from previous
  SIGKILL are removed before starting a new build.
- **`pipeline/build_replay_store.py` — `_partition_is_valid()` helper** — the
  validation logic is now a standalone, testable function.
- **`pipeline/daily_build.py` — count skipped partitions honestly** — the
  daily build result now distinguishes `success`, `skipped`, `failed`, and
  `no_data`. Skipped (already-valid) partitions count toward success; the log
  and report accurately reflect built vs skipped vs failed counts.
- **`systemd/cryptorecorder-replay-build.service` — stop endless retry loop**
  — changed `Restart=on-failure` to `Restart=no`. Added
  `StartLimitIntervalSec=86400` / `StartLimitBurst=3` in `[Unit]` as a safety
  net if restart is ever re-enabled. Previously, more than 100 restarts were
  observed as the service repeatedly rebuilt earlier partitions, reached
  DEXEUSDT, was OOM-killed, and immediately restarted from scratch. With
  `Restart=no` and per-partition skip-if-valid, a rerun makes durable forward
  progress instead of looping.
- **`config.py` — `REPLAY_SPOOL_TEMP_DIR`** — new optional configuration for
  the spool temp directory. Defaults to `None` (SQLite spool files are placed
  via `tempfile.NamedTemporaryFile`, which obeys `TMPDIR` / `/tmp`). Set
  `CRYPTO_RECORDER_REPLAY_SPOOL_TEMP_DIR` to a path on the data filesystem to
  avoid landing large spool files on a small root filesystem.
- **`systemd/cryptorecorder.env.example`** — documents
  `CRYPTO_RECORDER_REPLAY_SPOOL_TEMP_DIR` and
  `CRYPTO_RECORDER_REPLAY_PARQUET_BATCH`.

### Added (PR #18 — memory-bounded replay-store building)
- **`tests/test_replay_memory_bounded.py`** — 17 regression tests verifying:
  memory boundedness (no Python lists); correct schemas, counts, ordering,
  checksums, manifests; empty-channel schema preservation; large-record-set
  spanning many batches; cleanup on success and exception; stale staging
  handling; skip-if-valid logic; corrupt/incomplete partitions not skipped;
  cross-batch deterministic ordering.

### Note: real-data production validation
The DEXEUSDT partition test must be run on the production server with access to
`/data/cryptorecorder/data_raw` — the development machine does not have
production raw data. The required command for production validation is:

```
CRYPTO_RECORDER_DATA_ROOT=/data/cryptorecorder/data_raw \
CRYPTO_RECORDER_REPLAY_ROOT=/tmp/test_replay_$(date +%s) \
/usr/bin/time -v python -m pipeline.build_replay_store \
  --date 2026-07-21 --symbols DEXEUSDT
```

Or via a transient cgroup with 12 GiB memory limit:
```
systemd-run --scope -p MemoryMax=12G \
  /path/to/.venv/bin/python -m pipeline.build_replay_store \
  --date 2026-07-21 --symbols DEXEUSDT \
  --data-root /data/cryptorecorder/data_raw \
  --replay-root /tmp/test_replay_dexeusdt
```

### Changed (PR #18 — deployment boundary: converter removed from automated production path)
- **`scripts/deploy_linux_server.sh`** — `legacy-converter` is no longer a
  deployable `--target`; it was removed from `VALID_TARGETS`, so
  `--target legacy-converter` is now rejected exactly like any other unknown
  target. `--target all` now installs/controls only
  `cryptorecorder-recorder.service` and `cryptorecorder-replay-build.{service,timer}`
  — production automatically runs **only** those two units. This closes the
  gap where `--target all` still silently installed the legacy converter
  service/timer.
- `cryptorecorder-convert.service` and `cryptorecorder-convert.timer` were
  added to `cleanup_stale_units()`'s stale-unit list, so any copy already
  installed on an existing server (from before this change) is stopped,
  disabled, and removed automatically the next time the deploy script runs,
  the same way the pre-issue-#17 feature-build units are handled.
- **No converter/reconstruction code was removed.** `convert_day.py`,
  `converter/`, and `validation/replay_catalog_reconstruct.py` remain in
  place and required — for replay building, validation, and local
  test-computer catalog reconstruction. Replay stores continue to be synced
  separately by the operator; on the test computer, the synced replay stores
  may still be reconstructed into temporary Nautilus catalogs by symbol (e.g.
  for KovacsTrader) via `validation.replay_catalog_reconstruct`, run
  manually — this is unaffected by the deployment-path change.
  `systemd/cryptorecorder-convert.{service,timer}` were deleted from the repo
  in a subsequent pass (PR #18 finalization — see latest `[Unreleased]` entry).
- `docs/OPERATIONS.md` updated: the "Targets" and "Service groups" tables no
  longer list `legacy-converter`; the stale "daily chain runs convert →
  replay" ordering claim is corrected (`replay-build` reads directly from
  `data_raw` and never depended on converter output). `docs/IMPLEMENTATION_AUDIT.md`
  gained a matching completed-cleanup-items entry. New/updated tests in
  `tests/test_agent_infrastructure.py` (`DEPLOY_TARGETS`, `LEGACY_STALE_UNITS`,
  and new `test_deploy_script_rejects_legacy_converter_target` /
  `test_deploy_script_all_target_never_installs_converter`).

### Fixed (PR #18 third review round)
- **`pipeline/daily_build.py` — exchange-info-only dates report `no_data`,
  never `failed`** — `run_build_replay_store()` now derives eligible
  venue/symbol replay-build attempts from actual raw channel coverage
  (`depth_v2`/`trade_v2`) instead of treating every raw-manifest "symbol"
  entry (including `EXCHANGEINFO`, from `data_raw/<venue>/exchangeinfo/
  EXCHANGEINFO/<date>/`) as a market symbol. A date containing only an
  exchange-info partition now attempts zero replay partitions and reports
  `no_data` (nonzero exit code), matching the existing "zero eligible
  partitions" contract. `EXCHANGEINFO` can never be attempted even via
  explicit `--symbols EXCHANGEINFO`, because eligibility is computed from
  channel coverage rather than a literal symbol-name exclusion — protecting
  against future non-market metadata channels too. `success`/`partial`/
  `failed`/`no_data` semantics for genuinely eligible depth/trade symbols are
  unchanged. New tests in `tests/test_daily_build.py`.
- **`disk_monitor.py` — consistent report-timestamp timezone contract** — the
  top-level `"timestamp"` field of a normal disk-usage scan now uses
  `time_utils.local_now_iso()` (Europe/Budapest), matching the
  already-local-time skipped/overlapping-scan path and the documented
  `docs/OPERATIONS.md` contract, instead of a bare UTC `now.isoformat()`.
  Internal `measured_at` fields, growth-history epoch ordering, and
  measurement-age/staleness calculations remain UTC/epoch-based and are
  unaffected. `docs/OPERATIONS.md` gains an explicit `timestamp` field row
  documenting this. New tests in `tests/test_disk_monitor_fail_safe.py`.
- **`CHANGELOG.md` — corrected stale active-state framing** — the two
  pre-issue-#17 `[Unreleased]` "Changed" entries describing
  `pipeline.generate_catalog --profile full_l2` and `docs/GENERATE_CATALOG.md`/
  `docs/FEATURE_STORE.md` are now explicitly marked historical/superseded,
  with an inline note that the CLI and both doc files were later removed
  by issue #17 and are not available/do not exist today. History is
  preserved; only the current-state framing was corrected. New guard test
  `test_changelog_unreleased_has_no_active_stale_feature_catalog_claims` in
  `tests/test_agent_infrastructure.py`.

### Removed (issue #17 — recorder + replay-store ownership refactor)
- The entire **feature-store subsystem**: `stores/feature_schema.py`,
  `stores/feature_calc.py`, `stores/feature_writer.py`,
  `pipeline/build_feature_store.py`, `validation/audit_feature_store.py`,
  `tests/test_feature_store.py`, and the
  `systemd/cryptorecorder-feature-build.{service,timer}` units.
- `pipeline/generate_catalog.py` as a **product/runtime CLI**. Its
  `generate_catalog_from_replay` reconstruction logic and helpers moved to
  `validation/replay_catalog_reconstruct.py` — an internal, CLI-less,
  validation-only helper used exclusively by
  `validation/validate_catalog_equivalence.py`. CryptoRecorder no longer offers
  a general-purpose consumer catalog-generation service; any repository
  needing a temporary Nautilus catalog from replay data (e.g. KovacsTrader) is
  expected to own that reconstruction itself.
- `config.py`: `FEATURE_ROOT`, `LABEL_ROOT`, and `CATALOG_JOBS_ROOT` — all were
  downstream-only or product-CLI-only placeholders with no recorder/replay
  responsibility. `ARCHIVE_DAYS_ROOT` is unaffected (still a placeholder for a
  future recorder-side archive feature, not implemented).
- `docs/FEATURE_STORE.md` and `docs/GENERATE_CATALOG.md` — deleted rather than
  left as tombstones. The documented fixed docs/ count drops from 14 to 12.
- Feature-related CLI flags on `pipeline.daily_build`: `--steps`,
  `--timeframes`, `--feature-root`. `daily_build` now always builds the replay
  store only (`--date`, `--symbols`, `--data-root`, `--replay-root`,
  `--report-root`).

### Changed
- CryptoRecorder's scope is now explicitly narrowed to
  `Binance native market streams -> data_raw -> deterministic replay_store`,
  handed off to downstream consumer repositories (KovacsTrader). See
  `docs/ARCHITECTURE.md` and `README.md` for the updated ownership diagram.
- `pipeline/daily_build.py` docstring, report shape (`daily_build_<date>.json`
  no longer contains a `feature_build` section or `feature_root` path), and CLI
  simplified to replay-only.
- Superseded issue #15 (generate_catalog product-CLI proposal) in favor of
  issue #17's narrower recorder+replay-store scope; commented and closed as
  not planned.

### Fixed
- **Disk monitor false-zero reporting (issue #19)** — `disk_monitor.py` no longer
  reports a failed or timed-out `data_raw` size scan as numeric `0.0`. Every scan
  now returns a structured `DirectoryMeasurement` (`ok`/`status`/`error`); failures
  fall back to a persisted last-known-good value marked `stale`, or `null` if none
  exists. `state/disk_usage.json` gains `monitoring_health`
  (`healthy`/`degraded`/`unhealthy`), per-component measurement status, an `alerts`
  list, and independent filesystem-capacity fields via `shutil.disk_usage()`.
  Automatic cleanup (`cleanup_old_data()`) now fails closed: it never runs (or
  continues) unless the current `data_raw` measurement is fresh and successful.
  Retention thresholds (`DISK_SOFT_LIMIT_GB`/`DISK_HARD_LIMIT_GB`) and new
  filesystem free-space thresholds (`DISK_FS_FREE_WARN_GB`/`DISK_FS_FREE_CRITICAL_GB`)
  are kept semantically separate. Growth rate / `days_to_full` are computed from
  real sample timestamps and only successful, non-stale samples, with bounded
  persisted history. Overlapping scans are prevented with an `asyncio.Lock`. Report
  writes are atomic (temp file + `os.replace()`). Timed-out `du` child processes are
  terminated and reaped rather than left running. New env vars:
  `CRYPTO_RECORDER_DISK_SCAN_TIMEOUT_SEC`,
  `CRYPTO_RECORDER_DISK_MEASUREMENT_STALE_AFTER_SEC`,
  `CRYPTO_RECORDER_DISK_FS_FREE_WARN_GB`, `CRYPTO_RECORDER_DISK_FS_FREE_CRITICAL_GB`,
  `CRYPTO_RECORDER_DISK_HISTORY_MAX_SAMPLES`, `CRYPTO_RECORDER_DISK_HISTORY_MAX_AGE_SEC`.
  See `docs/ARCHITECTURE.md` and `docs/OPERATIONS.md`. Tests:
  `tests/test_disk_monitor_fail_safe.py` (new), `tests/test_disk_monitor_cleanup.py`
  (updated for the new cleanup-trust contract).

### Removed (issue #17 completion)
- `feature_root` parameter, CLI flag, and `feature_store` report component from
  `validation/audit_storage_size.py` — the feature-store subsystem no longer
  exists, so the audit no longer measures or accepts a path for it.
- `docs/GUARANTEES.md` — fully superseded by the "System Guarantees" section
  already present in `docs/ARCHITECTURE.md`; no unique content was lost.
- Root-level `inspect_catalog.py` — dead code left over from a stale merge; not
  imported by any module, and its docstring referenced a `validators/` package
  that does not exist in this repository. Use `validation/catalog_inspect.py`.
- Duplicate/stale systemd unit files superseded by their real, currently-used
  counterparts: `systemd/crypto-recorder.service` (superseded by
  `cryptorecorder-recorder.service`), `systemd/nautilus-convert.{service,timer}`
  (superseded by `cryptorecorder-convert.{service,timer}`), and
  `systemd/cryptorecorder-daily-build.{service,timer}` (superseded by
  `cryptorecorder-replay-build.{service,timer}`, the unit actually referenced by
  `scripts/deploy_linux_server.sh`).

### Fixed (issue #17/#19 completion)
- `pipeline/daily_build.py` no longer reports a false `"success"` when zero
  raw partitions were eligible for the requested date. `run_build_replay_store()`
  now distinguishes `"no_data"` (zero eligible partitions) from `"success"`
  (all eligible partitions built) and `"partial"` (some failed). `main()` exits
  nonzero for any non-`"success"` status, including `"no_data"`. See
  `tests/test_daily_build.py` (new) and `docs/DAILY_BUILD_PIPELINE.md`.
- `validate.py` reverted to its working, dependency-correct form after a stale
  `main`-branch regression reintroduced unused `cryptofeed`/`yaml` imports, a
  reference to a nonexistent `converter.book` module, and hardcoded paths that
  bypassed `config.py`'s configurable data/state/meta roots.
- `tests/test_repo_structure.py` hardened with 7 additional tests enforcing the
  exact allowed root Python/other files and the exact `docs/` file set from
  `docs/REPO_STRUCTURE.md`, absence of stray Python/`validators` imports, and
  absence of feature-store CLI flags/systemd units.

### Changed (documentation corrections)
- `INSTALL.md`: replaced every stale `crypto-recorder.service` reference with
  the real `cryptorecorder-recorder.service` unit name; replaced
  `nautilus-convert.{service,timer}` with the real
  `cryptorecorder-convert.{service,timer}` names; added the real
  `cryptorecorder-replay-build.{service,timer}` units to the manual-install
  walkthrough; pointed installers at `scripts/deploy_linux_server.sh` as the
  preferred install path; fixed a duplicate `## 10.` section heading
  (renumbered sections 10-15 to 11-16).
- `docs/OPERATIONS.md` and `AGENTS.md`: fixed self-referential
  "content merged from the former `OPERATIONS.md`" provenance notes (now
  correctly attribute `DEPLOYMENT.md`, `LINUX_SERVER.md`, `SCHEMAS.md`) and a
  broken same-file Markdown link; `AGENTS.md` Section 5 no longer links to a
  nonexistent `docs/LINUX_SERVER.md`.
- `docs/ARCHITECTURE.md` and `docs/IMPLEMENTATION_AUDIT.md`: fixed similarly
  self-referential "content merged from the former" provenance notes (now
  correctly attribute `STORAGE_ARCHITECTURE.md`, `GUARANTEES.md`,
  `REPO_CLEANUP_AUDIT.md`, `FEATURE_STORE.md`, `STORAGE_SIZE_AUDIT.md`); fixed
  a stale `pipeline/audit_replay_store.py` table row to read
  `validation/audit_replay_store.py`.
- `docs/PROJECT_STATUS.md`: removed a duplicate `[OPERATIONS.md](OPERATIONS.md)`
  link.
- `docs/DAILY_BUILD_PIPELINE.md`: removed a misleading `--date today` example
  (`daily_build` only accepts an explicit `YYYY-MM-DD` date or the literal
  `yesterday`; there is no `today` shortcut); documented the `"no_data"` status
  value and its nonzero exit code.
- `docs/VALIDATION.md`: added a "Replay Store Validation" section documenting
  `validation.audit_replay_store`, `validation.validate_catalog_equivalence`,
  and `validation.audit_change_compliance` usage (previously undocumented in
  this file).
- `docs/REPO_STRUCTURE.md`: updated the "Date" header to 2026-07-20 and
  expanded the Root-Level Files table to include every real root `.py` module
  (`binance_universe.py`, `disk_monitor.py`, `health_monitor.py`,
  `native_trades.py`, `phase2_depth.py`, `storage.py`, `time_utils.py`,
  `debug_futures_trade_ws.py`) that was previously missing from the doc.

### Fixed (PR #18 second review round)
- **Disk monitor fail-closed on overlapping scans** — `disk_monitor.py`'s
  `check_disk_usage()` now forces `retention_measurement_trustworthy=False`
  whenever a scan is skipped due to an overlapping scan already in progress
  (`skipped_duplicate=True`), even if the previous cycle's own report was
  trustworthy; adds a `WARNING`/`ERROR` alert and downgrades
  `monitoring_health` to at least `degraded`. `cleanup_old_data()` now
  explicitly refuses to act (`return False`) whenever the current cycle's
  report has `skipped_duplicate=True`, rather than only checking
  trustworthiness. New test
  `test_no_rmtree_when_lock_held_and_previous_report_trustworthy` proves no
  `shutil.rmtree` call occurs in that case.
- **No current-looking derived metrics from a stale fallback** —
  `percent_of_soft_limit`, `percent_of_hard_limit`, `growth_rate_gb_day`, and
  `days_to_full` are now all `null` (not computed from the persisted
  last-known-good value) whenever the current cycle's `data_raw` measurement
  itself is not fresh and successful. New test
  `test_stale_last_known_good_nulls_all_derived_fields` covers all four
  fields.
- **Retention accounting scoped to `data_raw` only** — soft/hard-limit and
  cleanup-target comparisons, `percent_of_soft_limit`/`percent_of_hard_limit`,
  and growth-rate/`days_to_full` are now derived exclusively from fresh
  `data_raw` usage (`data_raw_gb_for_retention`), never from `total_gb` (the
  cross-root `data_raw + catalog + meta + state` sum, which may span
  different filesystems and is retained purely as an observability field).
  `GrowthSample.total_bytes` renamed to `GrowthSample.data_raw_bytes`
  throughout `disk_monitor.py` and its tests. `docs/OPERATIONS.md`,
  `config.py`, and `systemd/cryptorecorder.env.example` comments updated to
  match.
- **`pipeline/daily_build.py` — explicit `"failed"` status** —
  `run_build_replay_store()` now reports `"failed"` (distinct from `"partial"`
  and `"no_data"`) when one or more venue/symbol partitions were attempted
  for the date and *none* of them succeeded (zero successful, nonzero
  attempted). `generate_daily_report()` propagates `"failed"` distinctly
  rather than collapsing it into `"partial"`. New test
  `test_all_partitions_failed_reports_failed_status` in
  `tests/test_daily_build.py`. `docs/ARCHITECTURE.md` and
  `docs/DAILY_BUILD_PIPELINE.md` document all four status values
  (`success`/`partial`/`failed`/`no_data`).
- **`scripts/deploy_linux_server.sh` — complete legacy-unit cleanup and honest
  flags** — `cleanup_stale_units()` now removes every legacy/renamed unit
  name this repo has ever shipped (`crypto-recorder.service`,
  `nautilus-convert.{service,timer}`,
  `cryptorecorder-daily-build.{service,timer}`, in addition to the existing
  `cryptorecorder-feature-build.{service,timer}`), runs for every `--target`
  (not just `all`/`replay-build`), and now runs *before* `install_units`
  installs the canonical replacements. `--user`/`--app-dir`/`--env-file` are
  now actually rendered into each installed unit file's
  `User=`/`Group=`/`WorkingDirectory=`/`ExecStart=`/`EnvironmentFile=` lines
  via `sed`, and `--data-root` is rendered into a newly created env file's
  `CRYPTO_RECORDER_*_ROOT` values (an existing env file is still never
  overwritten). New tests in `tests/test_agent_infrastructure.py`:
  `test_deploy_script_cleans_up_every_legacy_unit_name`,
  `test_deploy_script_renders_user_app_dir_and_env_file_flags`,
  `test_deploy_script_never_overwrites_existing_env_file`. `INSTALL.md` and
  `docs/OPERATIONS.md` updated to describe the rendering behavior.
- Stale systemd unit name references in docs corrected to the real,
  currently-shipped names: `docs/DAILY_BUILD_PIPELINE.md` (11 occurrences of
  `cryptorecorder-daily-build.{service,timer}` -> the real
  `cryptorecorder-replay-build.{service,timer}`), `docs/ARCHITECTURE.md` (2
  occurrences, same rename), `INSTALL.md` (`nautilus-convert.{service,timer}`
  -> the real `cryptorecorder-convert.{service,timer}` in the Troubleshooting
  section).

### Added (previous entry, retained)

  whose message does not match `<type>(<scope>): <subject>` where type is one of
  `feat|fix|docs|style|refactor|perf|test|chore`, subject starts lowercase, and
  subject has no trailing period. Blank-line-before-body is also enforced.
- `AGENTS.md` Section 7 "Commit message style" — full format reference, type table,
  subject rules, valid/invalid examples, and bypass guidance.
- Docs structure consolidation: merged 9 small docs into 4 comprehensive files
  (all content preserved). `ARCHITECTURE.md` absorbs `STORAGE_ARCHITECTURE.md`
  and `GUARANTEES.md`; `OPERATIONS.md` absorbs `DEPLOYMENT.md`, `LINUX_SERVER.md`,
  and `SCHEMAS.md`; `IMPLEMENTATION_AUDIT.md` absorbs `REPO_CLEANUP_AUDIT.md`,
  `FEATURE_STORE_REQUIREMENTS_AUDIT.md`, and `STORAGE_SIZE_AUDIT.md`;
  `CHANGELOG.md` absorbs versioning policy from `VERSIONING.md`.
- `docs/README.md` rewritten as navigation index with "Where to update what" table.
- "No New Docs Files" rule added to `AGENTS.md` Section 2 and `docs/REPO_STRUCTURE.md`:
  docs/ is fixed at 12 files; new content goes in existing sections.
- Mandatory change-audit infrastructure (`AGENTS.md` Section 6,
  `docs/AI_WORKFLOW.md` Step 7, `docs/CHANGE_AUDIT.md`, `INSTALL.md` Section 7):
  every non-trivial commit now requires an append-only audit entry in
  `docs/CHANGE_AUDIT.md` before the change is considered complete.
- `validation/audit_change_compliance.py` — compliance-check CLI that inspects
  staged or branch-diff changes and reports PASS/FAIL. Supports
  `--staged` (pre-commit hook mode), `--base <branch>`, and `--allow-docs-only`.
- `.githooks/pre-commit` — git hook that runs `audit_change_compliance --staged`
  before every commit and blocks if compliance fails. Activate per-clone with
  `git config core.hooksPath .githooks` (see `INSTALL.md`).

### Changed (historical — pre-issue #17 full_l2 rollout; superseded, see "Removed (issue #17...)" above)
> The `pipeline.generate_catalog` CLI and `docs/GENERATE_CATALOG.md`/
> `docs/FEATURE_STORE.md` referenced below were later **removed** by the
> issue #17 refactor (see the "Removed" sections earlier in this
> `[Unreleased]` block). They are recorded here only as history of when
> `full_l2` support was first added; `pipeline.generate_catalog` is **not**
> an available production CLI today, and neither doc file currently exists.
- `generate_catalog --profile full_l2` (and `depth_only`, `depth10`): replay-based
  full order-book catalog generation reusing the shared depth engine in
  `converter/depth_phase2.py` via `stores/replay_depth_adapter.py` (no second
  depth converter).
- Catalog comparison for order books in `validation/catalog_compare.py`:
  `OrderBookDeltas` (multiset-semantic), `OrderBookDepth10`, and reconstructed
  book checkpoints (top-10 bid/ask at 7 sampled timestamps).
- `validation/validate_catalog_equivalence.py` extended to compare the `full_l2`
  profile (trades + deltas + depth10 + checkpoints) and emit a per-instrument
  report under `validation_reports/`.
- New flags on the then-existing `pipeline.generate_catalog`: `--emit-depth10/--no-emit-depth10`,
  `--depth10-interval-sec`, `--derived-depth-snapshot-levels`, `--time-filter`.
- `validation/audit_storage_size.py` — audit-only CLI measuring on-disk size of
  replay/feature/catalog artifacts (the `feature` artifact measurement was
  itself later removed along with the feature-store subsystem; see "Removed
  (issue #17 completion)" above).
- Docs: `docs/IMPLEMENTATION_AUDIT.md`, `docs/IMPLEMENTATION_AUDIT.md`.
- Tests: `tests/test_replay_depth_adapter.py`, `tests/test_generate_catalog_full_l2.py`,
  `tests/test_catalog_equivalence_full_l2.py`, `tests/test_full_l2_realdata_gate.py`,
  plus a synthetic full-L2 convert_day-vs-replay equivalence test in
  `tests/test_catalog_equivalence.py`.

### Changed (historical — pre-issue #17; generate_catalog since removed)
- `replay_store → generate_catalog --profile full_l2` moved from **deferred** to
  **implemented, semantically validated on the ADAUSDT single-day smoke** against
  `convert_day.py` (trades, OrderBookDeltas, OrderBookDepth10, and book
  checkpoints all match). `convert_day.py` remains the production reference.
  This ADAUSDT-smoke validation result still holds today, but the
  reconstruction now lives in the validation-only
  `validation/replay_catalog_reconstruct.py` helper, not the (since removed)
  `pipeline.generate_catalog` CLI — see `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`
  for the current gate status.
- Docs updated at the time to reflect full_l2 support: `docs/REPO_STRUCTURE.md`,
  `docs/PROJECT_STATUS.md`, `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`,
  `docs/GENERATE_CATALOG.md`, `docs/FEATURE_STORE.md`, `docs/README.md`,
  `README.md`. The latter two doc files were subsequently deleted by the
  issue #17 refactor and no longer exist.

### Deferred
- Broader `full_l2` validation across the top50 universe and multiple days. This
  remains the `v2.0.0` gate; `v2.0.0` is **not** declared (VERSION stays
  `1.1.0-dev`).
- Syncthing archive/backup (`ARCHIVE_DAYS_ROOT`) — placeholder env path only.
- Import / restore tooling — not implemented.

## [1.1.0-dev] - 2026-06-17

### Added
- Frozen repository structure contract (`docs/REPO_STRUCTURE.md`) with an amendment
  log and a structure-enforcing test (`tests/test_repo_structure.py`).
- AI-agent governance: `AGENTS.md` and `.github/copilot-instructions.md` defining the
  required read order, hard constraints, and Definition of Done.
- Status and versioning docs: `docs/PROJECT_STATUS.md`, `docs/CHANGELOG.md`,
  `VERSION`, and this `CHANGELOG.md`.
- Linux server deployment documentation: `docs/OPERATIONS.md`, `docs/OPERATIONS.md`,
  and `docs/AI_WORKFLOW.md`.
- Service-target deployment scaffolding: explicit per-service systemd units
  (`cryptorecorder-recorder`, `cryptorecorder-convert`, `cryptorecorder-replay-build`,
  `cryptorecorder-feature-build`) and the `scripts/deploy_linux_server.sh` wrapper.
- `tests/test_agent_infrastructure.py` validating the governance/deployment docs and
  the deploy script's dry-run.

### Changed
- Moved audit/compare/inspect CLIs into `validation/` and kept `pipeline/` as
  build/transform CLIs only.
- Moved `trade_coverage` into `converter/` and updated `convert_day.py` to import it
  from there.
- Split the former `tests/test_pipeline_validation.py` into focused per-store tests.
- Documentation updated across `README.md`, `INSTALL.md`, and `docs/` to reflect the
  frozen structure and the validated-vs-deferred status.

### Deferred
- `replay_store → generate_catalog --profile full_l2` (full order-book catalog) remains
  unvalidated against `convert_day.py`. Tracked for `v2.0.0`.
- Syncthing archive/backup (`ARCHIVE_DAYS_ROOT`) — placeholder env path only.
- Label / target store (`LABEL_ROOT`) — placeholder env path only.
- Import / restore tooling — not implemented.

---

## Version baselines (git history, informational)

These SHAs document the development chain. **No git tags are created.**

| Marker | SHA | Meaning |
|--------|-----|---------|
| `v1.0.0` baseline | `65b24b3` | State **before** the replay/feature refactor (parent of `f726bb0`). |
| refactor chain | `f726bb0` → `9e94b06` → `7b52b12` → `7abd9fd` → `5ae0e1e` | replay/feature foundation, cleanup, and structure freeze. |
| `v1.1.0-dev` | current | Structure freeze + AI/deployment infrastructure. |
