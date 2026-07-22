# Repository Structure Contract

**Date**: 2026-07-20

This document is the binding contract for all future implementation in this
repository. Any Codex task or contributor must read this before adding files.

CryptoRecorder's scope is intentionally narrow: it records Binance native
market streams into `data_raw/`, and deterministically converts that raw data
into `replay_store/` (and, via the reference converter, a validated full-L2
Nautilus catalog). It does **not** own a feature-store, label-store, or
general-purpose consumer catalog-generation service — those are downstream
responsibilities (e.g. the KovacsTrader repository). See `docs/ARCHITECTURE.md`
for the ownership boundary.

---

## Root-Level Files (allowed)

These are the only Python entrypoints and project files permitted at the root:

| File | Purpose |
|---|---|
| `recorder.py` | Main raw recorder entrypoint |
| `phase2_depth.py` | `BinanceNativeDepthRecorder` — depth_v2 recording (core recorder module, never change without explicit task allowance) |
| `native_trades.py` | `BinanceNativeTradeRecorder` — trade_v2 recording (core recorder module, never change without explicit task allowance) |
| `storage.py` | Hourly-rotated JSONL(.zst) file writer (core recorder module, never change without explicit task allowance) |
| `binance_universe.py` | Universe selection by 24h quote volume + futures precheck |
| `health_monitor.py` | Publishes `state/heartbeat.json` |
| `disk_monitor.py` | Fail-safe disk usage monitoring (see issue #19 / `docs/ARCHITECTURE.md`) |
| `time_utils.py` | Shared timestamp helpers |
| `convert_day.py` | Validated raw → Nautilus full-L2 converter |
| `config.py` | Configuration and storage roots |
| `validate.py` | Setup and import validation |
| `debug_futures_trade_ws.py` | Standalone developer debug script for the futures/spot trade WebSocket (not imported by production code) |
| `README.md` | Project overview |
| `INSTALL.md` | Machine setup guide |
| `requirements.txt` | Python dependencies |
| `pytest.ini` | Test runner config |
| `AGENTS.md` | Binding rules for AI agents |
| `VERSION` | Current version string (e.g. `1.1.0-dev`) |
| `CHANGELOG.md` | Keep-a-Changelog history |

All other Python entrypoints belong in a package, not at the root.

The `.github/` directory (e.g. `copilot-instructions.md`) is an allowed
configuration directory; it contains no importable Python.

The `.githooks/` directory contains version-controlled git hook scripts:
- `pre-commit` — runs change-audit compliance check before every commit.
- `commit-msg` — validates commit message format (conventional commits style).

It is an allowed configuration directory; it contains no importable Python.
Activate hooks with `git config core.hooksPath .githooks`.

---

## Root-Level Packages (allowed)

These are the only top-level packages permitted:

| Package | Purpose |
|---|---|
| `converter/` | Legacy full-L2 raw → Nautilus converter implementation |
| `pipeline/` | Build and transform data artifacts (raw manifest, replay store) |
| `stores/` | Parquet schema definitions and reader/writer implementations |
| `validation/` | Inspect, audit, and compare data artifacts |
| `scripts/` | Thin operator shell wrappers only (no importable business logic) |
| `tests/` | All automated tests |
| `docs/` | Documentation only (no importable code) |
| `systemd/` | Unit files for deployment |

### Package Role Definitions

**`converter/`** — The legacy full-L2 converter implementation. This includes
`depth_phase2.py`, `trades.py`, `readers.py`, `catalog.py`, `instruments.py`,
`universe.py`, `spool.py`, and `trade_coverage.py`. Do not add replay pipeline
logic here; this package is the old-converter boundary.

**`pipeline/`** — Build and transform commands that create data artifacts from
raw data. Contains daily build orchestration and the replay builder. Does
**not** contain a feature-store builder or a product-facing catalog-generation
CLI (removed; see docs/ARCHITECTURE.md), and does **not** contain audit or
equivalence check commands; those belong in `validation/`.

**`stores/`** — Parquet schemas, writers, and readers for `replay_store` only.
Pure data access layer; no CLI entrypoints. There is no feature-store or
label-store schema/reader/writer in this package.

**`validation/`** — The single general validation package. Contains all
audit CLIs (audit_replay_store, audit_storage_size), equivalence checks
(validate_catalog_equivalence), the internal validation-only replay→catalog
reconstruction helper (replay_catalog_reconstruct — no CLI), catalog
comparison utilities (catalog_compare), catalog inspection (catalog_inspect),
and report validation (phase2_report). Does **not** contain build/transform
logic; that belongs in `pipeline/`.

**`scripts/`** — Thin operator wrappers only. Scripts may call subprocesses or
invoke pipeline/validation CLIs, but must not contain importable business logic.
The replay full-L2 reconstruction path is implemented (validation-only) and
validated on the ADAUSDT smoke; no script may claim broader top50/multi-day
full-L2 equivalence until that wider validation is run and declared done.

**`tests/`** — All automated tests. No test file outside this directory. Tests
are organized by subsystem:
- `test_replay_store.py` — replay build and audit
- `test_replay_catalog_reconstruct.py` — validation-only full_l2 reconstruction
- `test_catalog_equivalence.py` — old-vs-new catalog comparison
- `test_repo_structure.py` — folder contract enforcement
- Legacy converter tests remain named as-is

There is no `test_feature_store.py` and no `test_generate_catalog.py` — the
feature-store subsystem and the `generate_catalog` product CLI were removed
(issue #17). See `CHANGELOG.md`.

**`docs/`** — Documentation and plans. No importable Python code here.
The docs structure is fixed at **12 files** (see "No New Docs Files" rule below).

| File | Content home |
|------|-------------|
| `README.md` | Navigation index and "where to update what" map |
| `REPO_STRUCTURE.md` | This file — frozen folder contract |
| `PROJECT_STATUS.md` | Validated vs deferred truth |
| `AI_WORKFLOW.md` | Step-by-step agent workflow |
| `CHANGE_AUDIT.md` | Append-only change audit log |
| `ARCHITECTURE.md` | System design, storage layers, schemas, guarantees, ownership boundary |
| `OPERATIONS.md` | Operations, deployment script, Linux server, state schemas |
| `VALIDATION.md` | Validation layer reference |
| `IMPLEMENTATION_AUDIT.md` | Ground-truth audit, cleanup history, requirements, sizes |
| `REPLAY_STORE.md` | Replay store feature reference (the stable external contract) |
| `DAILY_BUILD_PIPELINE.md` | Daily build pipeline reference (replay-only) |
| `FULL_L2_REPLAY_CATALOG_PLAN.md` | Full-L2 reconstruction plan and gate status |

`FEATURE_STORE.md` and `GENERATE_CATALOG.md` were deleted (issue #17) — the
feature-store subsystem and the `generate_catalog` product CLI no longer
exist. Do not recreate either file; see `CHANGELOG.md` for the removal record.

---

## Generated / Local Folders (must be gitignored, never committed)

These paths are runtime outputs or local caches and must never be tracked.
`feature_store/` and `catalog_jobs/` may still exist on some machines as
residual data from before issue #17; they are not deleted automatically and
must not be deleted by an agent without explicit instruction, but no current
code writes to them as a supported product path.

```
data_raw/
replay_store/
feature_store/
catalog_jobs/
validation_reports/
daily_reports/
state/
meta/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.venv/
.staging_*
*.parquet
*.jsonl
*.jsonl.zst
*.jsonl.gz
*.log
recorder.log
```

---

## Rules

### No New Docs Files Without Contract Amendment

The docs structure is intentionally **fixed at 12 files**. Before creating any
new file in `docs/`, you **must** identify which existing file is the right home
for the content, add it as a new section there, and (only if no existing file
fits) amend this file (`docs/REPO_STRUCTURE.md`) with a justification.

Use the table in the `docs/` package entry above to find the right file for any
new content. If the agent is unsure which file to update, **stop and ask** rather
than creating a new file. See also the matching rule in `AGENTS.md` Section 2.

### No New Top-Level Packages Without Contract Amendment

Before creating any new root-level directory that contains Python code, you
**must** edit this file (`docs/REPO_STRUCTURE.md`) and explain why the existing
packages are insufficient.

### No Duplicate Singular/Plural Packages

There must be exactly **one** validation package: `validation/`. The old
`validators/` package has been removed. Do not recreate it or add a similarly
named package.

There must be exactly **one** converter package: `converter/`. Do not add a
`converters/` package.

### No Business Logic in `scripts/`

Scripts must be thin wrappers. They may call:
- `python -m pipeline.*`
- `python -m validation.*`
- `subprocess.run([sys.executable, "convert_day.py", ...])`

They must not contain functions that are imported by other packages.

### No Importable Code in `docs/`

Documentation is Markdown and plaintext only.

### No Tests Outside `tests/`

All `test_*.py` files must live under `tests/`. Do not add test files to
`scripts/`, `docs/`, or package directories.

### No Generated Runtime Data Committed

Never commit Parquet files, JSONL files, raw data directories, state files,
catalog outputs, or tool caches. Use `.gitignore` to block them.

### `pipeline/` Is Build-Only

`pipeline/` contains only modules that **build or transform** data artifacts:

- `daily_build.py`
- `build_replay_store.py`
- `raw_manifest.py`

Audit and equivalence modules live in `validation/`, not `pipeline/`. There is
no feature-store builder and no product-facing catalog-generation CLI here.

### `validation/` Is Inspect/Audit-Only

`validation/` contains only modules that **inspect, compare, or audit**
existing artifacts (or, in the case of `replay_catalog_reconstruct.py`, an
internal helper with no CLI used exclusively by the equivalence check):

- `audit_change_compliance.py`
- `audit_replay_store.py`
- `audit_storage_size.py`
- `validate_catalog_equivalence.py`
- `replay_catalog_reconstruct.py` (no CLI; validation-only)
- `catalog_compare.py`
- `catalog_inspect.py`
- `phase2_report.py`

Build and transform modules live in `pipeline/`, not `validation/`.

---

## Final CLI Command Reference

These are the canonical commands. Documentation must not use any other paths.

### Build / Transform (pipeline)

```bash
# Build replay store for one day
python -m pipeline.build_replay_store --date YYYY-MM-DD [OPTIONS]

# Run daily build (raw manifest scan + replay store build + report)
python -m pipeline.daily_build --date YYYY-MM-DD [OPTIONS]
```

### Audit / Validate (validation)

```bash
# Audit replay store partitions
python -m validation.audit_replay_store --date YYYY-MM-DD [OPTIONS]

# Check change-audit compliance (pre-commit hook mode)
python -m validation.audit_change_compliance --staged

# Check change-audit compliance vs a base branch
python -m validation.audit_change_compliance --base main

# Compare old convert_day catalog vs new replay-reconstructed catalog
python -m validation.validate_catalog_equivalence --date YYYY-MM-DD [OPTIONS]

# Inspect a Nautilus catalog instrument
python -m validation.catalog_inspect CATALOG_ROOT INSTRUMENT_ID

# Validate a convert report JSON
python -m validation.phase2_report PATH_TO_REPORT
```

`validation.replay_catalog_reconstruct` has no CLI — it is an internal helper
imported only by `validation.validate_catalog_equivalence`.

### Legacy Full-L2 Converter (root-level)

```bash
# Validated full-L2 raw → Nautilus converter
python convert_day.py --date YYYY-MM-DD --staging
```

### Recorder (root-level)

```bash
python recorder.py
```

---

## Current Implementation Status

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
  VALIDATED: current production full-L2 path

data_raw -> replay_store
  IMPLEMENTED: v0 validated
  This is the stable external contract handed off to downstream repositories
  (e.g. KovacsTrader). CryptoRecorder does not build a feature/label layer or
  a general-purpose consumer catalog from it.

replay_store -> validation.replay_catalog_reconstruct (validation-only, no CLI)
  IMPLEMENTED: semantically validated on the ADAUSDT smoke
  (BINANCE_SPOT/ADAUSDT/2026-06-12 vs convert_day.py: trades, OrderBookDeltas,
   OrderBookDepth10, and book checkpoints all match). Broader top50/multi-day
   validation still pending. See docs/FULL_L2_REPLAY_CATALOG_PLAN.md. Not a
   supported downstream runtime API.
```

---

## Amendment Log

| Date | Change |
|---|---|
| 2026-06-17 | Initial structure contract created; `validators/` removed, `converter/trade_coverage.py` added; audit CLIs moved from `pipeline/` to `validation/` |
| 2026-06-17 | Added AI/deployment infrastructure: root files `AGENTS.md`, `VERSION`, `CHANGELOG.md`; `.github/copilot-instructions.md`; deployment/status docs; per-service systemd units; `scripts/deploy_linux_server.sh`; `tests/test_agent_infrastructure.py` |
| 2026-07-09 | Added mandatory change-audit infrastructure: `docs/CHANGE_AUDIT.md`; `validation/audit_change_compliance.py`; `.githooks/pre-commit`; AGENTS.md Section 6; AI_WORKFLOW.md Step 7 |
| 2026-07-09 | Docs structure consolidation: merged 9 small docs into ARCHITECTURE.md, OPERATIONS.md, IMPLEMENTATION_AUDIT.md, and CHANGELOG.md; fixed docs/ at 14 files; added "No New Docs" rules in AGENTS.md and REPO_STRUCTURE.md |
| 2026-07-09 | Added `.githooks/commit-msg` — conventional commits enforcement hook; AGENTS.md Section 7 commit style rules |
| 2026-07-15 | Issue #17: removed the feature-store subsystem (`stores/feature_*.py`, `pipeline/build_feature_store.py`, `validation/audit_feature_store.py`, `tests/test_feature_store.py`, feature-build systemd units) and the `pipeline/generate_catalog.py` product CLI (reconstruction logic moved to `validation/replay_catalog_reconstruct.py`, an internal CLI-less helper). Removed `config.py` `FEATURE_ROOT`/`LABEL_ROOT`/`CATALOG_JOBS_ROOT`. Simplified `pipeline.daily_build` to replay-only (dropped `--steps`/`--timeframes`/`--feature-root`). Deleted `docs/FEATURE_STORE.md` and `docs/GENERATE_CATALOG.md`; docs/ fixed count dropped from 14 to 12. Superseded issue #15. |
| 2026-07-20 | Issues #17/#19 completion: expanded the Root-Level Files table to list every real root `.py` module; deleted stale duplicate systemd units (`crypto-recorder.service`, `nautilus-convert.{service,timer}`, `cryptorecorder-daily-build.{service,timer}`) superseded by `cryptorecorder-recorder.service`, `cryptorecorder-convert.{service,timer}`, and `cryptorecorder-replay-build.{service,timer}`, the units actually referenced by `scripts/deploy_linux_server.sh`; removed root `inspect_catalog.py` (dead code); removed `docs/GUARANTEES.md` (superseded by `ARCHITECTURE.md`); added 7 tests to `tests/test_repo_structure.py` enforcing this contract exactly. |
| 2026-07-22 | PR #18 finalization: deleted `systemd/cryptorecorder-convert.service` and `systemd/cryptorecorder-convert.timer` — converter systemd automation is not part of the supported production architecture. Manual reconstruction uses documented CLI commands, not systemd templates. Stale installed converter units are still removed by `scripts/deploy_linux_server.sh` cleanup. Converter Python code (`convert_day.py`, `converter/`) is unchanged and required. |
