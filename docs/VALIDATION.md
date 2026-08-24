# Validation

CryptoRecorder has a clear separation between validation, tests, and operational checks.

## Quick Reference

| What | Command | When |
|------|---------|------|
| Setup validation | `python validate.py` | After cloning/setup |
| Dependency environment | `python -m validation.validate_dependency_environment --kind <production|reconstruction|development>` | After a frozen uv sync |
| Unit tests | `pytest tests/` | After code changes |
| Smoke test | `python scripts/smoke_test.py` | Verify recorder works |
| Full acceptance | `python scripts/acceptance_test.py` | Release readiness |
| Replay partition audit | `python -m validation.audit_replay_store` | After building `replay_store` |
| Old-vs-new semantic equivalence | `python -m validation.validate_catalog_equivalence` | Validate `replay_store` against `convert_day.py` |
| Change-compliance audit | `python -m validation.audit_change_compliance` | Before every commit (pre-commit hook) |

## Setup Validation (`validate.py`)

Run this after the explicit frozen uv sync for the selected environment:

```bash
python validate.py          # Full validation
python validate.py --quick  # Quick dependency check only
```

Checks:
- Python version (`>=3.12,<3.15`)
- Production dependencies (`aiohttp`, NumPy, PyArrow, and zstandard)
- Project structure (directories exist)
- Configuration loads correctly
- Production core modules can be imported without Nautilus or pytest

`validation.validate_dependency_environment` is the fail-closed environment
contract. It checks `uv lock --check` without changing the lock, exact required
and forbidden packages, production or reconstruction imports, and relevant CLI
help boundaries. With `--kind production --production-smoke-root <new-external-
path>` it may additionally build and routine/deep-validate one tiny synthetic
schema-v2 partition. The path must be new, non-symlinked, owned by the caller,
and outside both the repository and `.venv`; the validator never deletes an
arbitrary path.

## Unit Tests (`tests/`)

Run with pytest:

```bash
pytest tests/              # All tests
pytest tests/ -v           # Verbose output
pytest tests/ -x           # Stop on first failure
pytest tests/test_depth_deterministic.py   # Depth ordering/session tests
pytest tests/test_trade_deterministic.py   # Trade ordering/schema tests
pytest tests/test_converter_integration.py # Converter pipeline tests
```

Tests cover:
- Deterministic depth ordering by `(session_id, session_seq)`
- Committed-only session_seq allocation (no gaps from lifecycle/rejects)
- Futures U/u/pu continuity enforcement and fencing
- Reconnect session boundary handling
- Depth10 enabled by default
- Trade canonical ordering and aggressor mapping
- Lifecycle marker exclusion from TradeTick output
- Spot vs futures tagged union schema decoding
- Converter integration (trade_v2 → TradeTick, depth_v2 → OrderBookDeltas)
- convert_date report shape and catalog queryability
- REST-based futures support precheck
- Date-scoped catalog purging
- Heartbeat field coverage
- Universe resolution

## Operational Scripts (`scripts/`)

### Smoke Test

Quick 3-minute recorder test:

```bash
python scripts/smoke_test.py              # 3 minutes
python scripts/smoke_test.py --runtime 60 # 1 minute
```

Checks:
- Recorder starts and runs
- Raw files created (depth_v2 + trade_v2)
- Heartbeat written with `architecture: deterministic_native`
- No rate limit errors
- Clean shutdown

### Acceptance Test

Full pipeline test (recorder → converter → catalog):

```bash
python scripts/acceptance_test.py              # Full test (10 min)
python scripts/acceptance_test.py --runtime 300 # 5 minutes
python scripts/acceptance_test.py --skip-recorder # Test converter only
python scripts/acceptance_test.py --emit-depth10  # Also check derived depth10
```

Checks:
- Recorder works with 50 symbols (both depth_v2 and trade_v2 channels)
- Converter produces valid output with `architecture: deterministic_native`
- Catalog is queryable (instruments, OrderBookDeltas, TradeTick)
- Fenced ranges reported in convert report

## Replay Store Validation (`validation/`)

These commands validate `replay_store` — the recorder + replay-store output
contract handed off to downstream repositories. They are non-mutating audits;
none of them modify `data_raw/`, production `replay_store`, or `/etc` files.

### Replay partition audit

Checks schema, sort order, checksum, and null-ratio invariants for one or more
already-built `replay_store` partitions:

```bash
python -m validation.audit_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --replay-root ./replay_store
```

Use `--symbols all` / `--venues all` to audit every partition present for a
date. Pass `--report-path` to write the JSON report to a file instead of
stdout.

### Old-vs-new semantic equivalence

Compares `replay_store` (rebuilt into a temporary Nautilus catalog via the
internal `validation.replay_catalog_reconstruct` helper — there is no
`generate_catalog` product CLI) against the legacy `convert_day.py` catalog
for the same date/symbol set:

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

`--profile` accepts `trades_only`, `full_l2`, `depth_only`, or `depth10`. The
`full_l2` profile is validated on the ADAUSDT single-day smoke; broader
top50/multi-day validation is pending — see
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md).

### Supported selected temporary-catalog reconstruction

For development-computer consumers that need a bounded selection rather than
an equivalence run, the supported boundary is:

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

The selection and `[start,end)` UTC window are mandatory. Supported profiles
are `full_l2` and `trades_only`; the engine's validation-specific secondary
profiles are intentionally not exposed. Before output publication, the CLI
requires every requested replay partition, any available/required preceding
carry partition, complete supported manifests, complete-file checksums, and
exact instrument metadata. It never falls back to generic instrument defaults.

The final `<output-root>/<job-id>/job_manifest.json` binds the exact replay
manifest, depth, trades, instrument, source-identity, and integrity inputs plus
the complete catalog file inventory. Inputs are rehashed after reconstruction
and again before same-parent atomic publication. Unsafe IDs, symlink traversal,
ambiguous ownership, missing partitions, mutation, and existing jobs fail
closed. `--overwrite` is limited to that exact already-complete job. Failed
staging is preserved as a non-complete `.failed_<job-id>_*` sibling when
possible. This is a temporary artifact builder, not a semantic comparator,
Linux service, persistent catalog owner, or backtest orchestrator.

#### Bounded catalog-reader compatibility boundary

The exhaustive TradeTick, OrderBookDelta, and Depth10 comparisons do not use
Nautilus's normal DataFusion query for full-day streams. In
`nautilus_trader==1.225.0`, that query unconditionally applies a global
`ORDER BY ts_init`; real BTCUSDT diagnostics showed that the native sort can
materialize the complete multi-file result before yielding a batch. The
validation reader instead uses `ParquetDataCatalog._query_files()` to select
candidate files, then reads one Parquet file and row group at a time with
bounded PyArrow batches.

`_query_files()` is a **private Nautilus method**, not a supported public API.
It is currently used because Nautilus 1.225.0 exposes no public
class/instrument/time file-pruning API that avoids the DataFusion query.
The authoritative `pyproject.toml` reconstruction extra therefore pins exactly
`nautilus_trader==1.225.0` (resolved in committed `uv.lock`), and the reader
also checks the installed version and private method signature at
runtime. A different, missing, or incompatible implementation raises a clear
compatibility error. It must never silently fall back to `catalog.query()`,
`_query_rust()`, or `backend_session()`.

The direct file reader accepts only one requested instrument and one supported
data class per stream. Before yielding any object it preflights every selected
file and requires:

- the expected catalog class path, Arrow schema, and exact `instrument_id`
  metadata (the physical field names, order, types, and nullability must
  exactly match the pinned Nautilus 1.225.0 schema for that data class);
- a parseable Nautilus filename whose interval equals the file's actual
  `ts_init` range;
- explicit ascending row-group order and internally non-decreasing `ts_init`;
- strictly disjoint adjacent closed file intervals
  (`next.first_ts > previous.last_ts`).

This layout is a proved invariant of the successfully written
CryptoRecorder reference and replay-reconstruction validation catalogs under
Nautilus 1.225.0: both paths write `ObjectSpool` output ordered by
`(ts_init, ordinal)` and neither bypasses Nautilus's closed-interval
disjointness check. It is not a guarantee for arbitrary Nautilus catalogs,
which can opt out of that check. Overlap, including equal `ts_init` at a file
boundary, therefore fails closed; the reader never silently concatenates it.
Equal timestamps inside one file retain physical `(ts_init, ordinal)` order.

One writer-side limitation remains explicit: Nautilus 1.225.0 checks for an
already-existing timestamp-derived filename before its overlap check. Two
separate write chunks with the exact same singleton interval can cause the
later write to be skipped. The reader can reject overlap among files that
exist, but cannot recover a chunk already omitted by the writer. Thus
non-overlap proves the supported file layout, not independently the
completeness of arbitrary equal-timestamp write chunks. The preserved
BTCUSDT reference/replay artifacts contain strictly disjoint files and their
exhaustive trade/delta comparisons match; no rebuild is implied by this
documented boundary.

The preflight and decode passes keep at most one Parquet file open and one
bounded Arrow batch live. `ArrowSerializer.deserialize()` receives only that
current batch, and `from_pyo3_list()` converts only its returned current-batch
list. Before yielding the first object from a decoded batch, the reader checks
its row count, every decoded `ts_init` against the corresponding Arrow row,
non-decreasing order, and instrument identity. Focused tests verify the prior
Arrow batch is released at the next batch transition, conversion does not
retain its input list, and no batch references remain after iteration. Caller
ranges remain half-open `[start, end)`; internally the reader applies inclusive
`[start, end - 1]` filtering, matching the prior Nautilus catalog-query
contract exactly. Memory is bounded by Arrow batch size, not a time
subdivision; the obsolete no-op `window_ns`/`--window-hours` compatibility
surface has been removed.

#### Process-isolated semantic stages

Large production-day comparisons are split into independent
`validation.stage_runner_cli` subprocesses so allocator state is released
between stages and every substantial stage can have its own cgroup limit.
The comparison/diagnostic subcommands are:

- `trades` and `deltas`: exhaustive event-by-event comparisons;
- `depth10`: exhaustive Depth10 comparison;
- `checkpoints`: bounded delta replay at the canonical book checkpoints;
- `continuity`: persisted reference/candidate continuity-count comparison;
- `fences`: complete fenced-range count and canonical-digest comparison;
- `metadata`: exhaustive event-keyed raw-to-replay metadata comparison plus
  a fresh raw source-identity recomputation against the replay manifest; it
  first requires the complete D+1 raw scope to be closed;
- `integrity`: exact schema/version routine validation plus the explicit
  row-group-by-row-group deep digest and boundary audit.

The obsolete combined `depth` compatibility subcommand was removed; Depth10
and checkpoints are always explicit stages.

Before the first comparison, create an artifact identity in its own bounded
validation process:

```bash
python -m validation.artifact_identity \
  --config artifact-identity-config.json \
  --out artifact-identity.json
```

The config selects exactly one `date`, `venue`, `symbol`, and `instrument_id`
and supplies `data_root`, `replay_root`, `reference_catalog_root`,
`reference_report_path`, `candidate_catalog_root`, and
`candidate_reconstruction_manifest_path`. It also supplies an explicit
previous-day `carry` object. `carry.kind=replay_manifest` requires and hashes
the complete previous-day schema-v2 replay manifest.
`carry.kind=no_carry_prelisting` is not a caller assertion: the command
requires `data_root`, probes the previous date with the replay builder's
bounded source-record predicate, and fails if any contributing depth/trade
record or previous-day replay partition exists. Its sanitized recorded result
is `not_applicable_pre_listing`; the data-root path is never emitted.

The source composite covers the reference catalog tree, normalized reference
convert report, and freshly recomputed target raw-source identity. The
candidate composite covers the reconstructed catalog tree, normalized
reconstruction manifest, exact target replay manifest, and either the exact
carry manifest or verified pre-listing marker. Replay manifests must be
`status=complete`, `schema_version=2`, `format_version=2`, built by the current
`cryptorecorder-replay-writer-v2.0.1`, and carry a complete, structurally valid
source identity which exactly matches its integrity copy. The target replay
identity must also exactly equal the fresh raw identity.

Catalog trees use bounded, deterministic, length-framed SHA-256 hashing over
sorted root-relative POSIX names, file sizes, and file-byte digests. Absolute
tree roots, mtimes, ownership, and inode numbers are excluded; symlinks,
non-regular files, mutation during hashing, and an empty selected tree fail
closed. JSON hashing canonicalizes content and replaces absolute machine paths
with one fixed marker only after the report/manifest's configured-root
provenance has been checked. The compact identity JSON contains no machine
paths and its output is created exclusively: an existing file is never
overwritten.

Every stage config includes `artifact_identity_path`, the same explicit
artifact input fields used to create it, and a `scope` whose date, venue,
symbol, instrument, source hash, and candidate hash exactly equal the
validated document. The document itself has one canonical length-framed
SHA-256 representation. Before and after running its comparison, every stage
revalidates and re-hashes the configured artifacts; mutation or substitution
turns the stage into a failure. Each fragment records the canonical identity
digest, sanitized stage configuration (profile/window plus
schema/format/builder identity), and the exact source/candidate/component
hashes. Caller-provided labels alone are never an identity.

The `report` subcommand independently loads and recomputes the identity before
aggregation, performs the same post-stage recheck, and requires every
fragment's complete cryptographic binding to equal the verified document. It
rejects matching labels with different hashes, old/new build mixtures,
missing, duplicate, unexpected, failed, or cross-artifact fragments. Reusing
a preserved fragment is valid only when its canonical identity binding still
matches the independently verified artifacts.

Wrap each substantial CLI invocation with
`scripts/run_under_cgroup.sh 10G <fresh-output-dir> <fresh-unit-name> -- ...`.
The wrapper verifies the effective limit and cgroup membership before release,
enforces zero swap, samples `memory.current`, records kernel `memory.peak` and
final `memory.events`, propagates the child exit status, never retries, and
fails on missing telemetry, a zero peak, or any OOM event. It refuses to
overwrite prior evidence. `validation.serial_gate` likewise
rejects an existing output, a fragment larger than 8 MiB, non-object JSON, or
an exit-zero fragment whose `passed` field is not exactly `true`.

See [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) `## Local Testing
Workflow (temp-root smoke)` for the full end-to-end build-then-validate
recipe using temporary roots.

### Change-compliance audit

Enforces the mandatory change-audit rules in `AGENTS.md` Section 6 (run
automatically by the pre-commit hook on staged changes):

```bash
python -m validation.audit_change_compliance --staged   # staged changes only
python -m validation.audit_change_compliance --base main # full branch diff vs main
```

## Reports

All validation/test results are saved to `state/`:

| File | Content |
|------|---------|
| `state/smoke_test_results.json` | Smoke test results |
| `state/acceptance_test_results.json` | Acceptance test results |
| `state/smoke_test.log` | Recorder output from smoke test |

## Quality Metrics

The deterministic native pipeline tracks these quality indicators:

| Metric | Where | Meaning |
|--------|-------|---------|
| `fenced_ranges_total` | Convert report | Ranges excluded from deterministic replay |
| `desync_events` | Convert report / heartbeat | Times continuity was lost |
| `resync_count` | Convert report / heartbeat | Successful re-synchronizations |
| `snapshot_seed_count` | Convert report | REST snapshots used to seed replay |
| `queue_drop_total` | Heartbeat | WebSocket messages dropped due to backpressure |
| `instruments_with_no_data` | Convert report | Instruments defined but missing raw data |
