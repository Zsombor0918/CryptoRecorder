# Repository Structure Contract

**Date**: 2026-06-17

This document is the binding contract for all future implementation in this
repository. Any Codex task or contributor must read this before adding files.

---

## Root-Level Files (allowed)

These are the only Python entrypoints and project files permitted at the root:

| File | Purpose |
|---|---|
| `recorder.py` | Main raw recorder entrypoint |
| `convert_day.py` | Validated raw → Nautilus full-L2 converter |
| `config.py` | Configuration and storage roots |
| `validate.py` | Setup and import validation |
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
| `pipeline/` | Build and transform data artifacts (replay, features, catalog) |
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
raw or replay data. Contains daily build orchestration, replay builder, feature
builder, and catalog generator. Does **not** contain audit or equivalence check
commands; those belong in `validation/`.

**`stores/`** — Parquet schemas, writers, and readers for replay_store and
feature_store. Pure data access layer; no CLI entrypoints.

**`validation/`** — The single general validation package. Contains all
audit CLIs (audit_feature_store, audit_replay_store, audit_storage_size),
equivalence checks (validate_catalog_equivalence), catalog comparison utilities
(catalog_compare), catalog inspection (catalog_inspect), and report validation
(phase2_report). Does **not** contain build/transform logic; that belongs in
`pipeline/`.

**`scripts/`** — Thin operator wrappers only. Scripts may call subprocesses or
invoke pipeline/validation CLIs, but must not contain importable business logic.
The replay full-L2 path is implemented and validated on the ADAUSDT smoke; no
script may claim broader top50/multi-day full-L2 equivalence until that wider
validation is run and declared done.

**`tests/`** — All automated tests. No test file outside this directory. Tests
are organized by subsystem:
- `test_replay_store.py` — replay build and audit
- `test_feature_store.py` — feature build and audit
- `test_generate_catalog.py` — catalog generation
- `test_catalog_equivalence.py` — old-vs-new catalog comparison
- `test_repo_structure.py` — folder contract enforcement
- Legacy converter tests remain named as-is

**`docs/`** — Documentation and plans. No importable Python code here.
The docs structure is fixed at **14 files** (see "No New Docs Files" rule below).

| File | Content home |
|------|-------------|
| `README.md` | Navigation index and "where to update what" map |
| `REPO_STRUCTURE.md` | This file — frozen folder contract |
| `PROJECT_STATUS.md` | Validated vs deferred truth |
| `AI_WORKFLOW.md` | Step-by-step agent workflow |
| `CHANGE_AUDIT.md` | Append-only change audit log |
| `ARCHITECTURE.md` | System design, storage layers, schemas, guarantees |
| `OPERATIONS.md` | Operations, deployment script, Linux server, state schemas |
| `VALIDATION.md` | Validation layer reference |
| `IMPLEMENTATION_AUDIT.md` | Ground-truth audit, cleanup history, requirements, sizes |
| `REPLAY_STORE.md` | Replay store feature reference |
| `FEATURE_STORE.md` | Feature store feature reference |
| `GENERATE_CATALOG.md` | Catalog generation reference |
| `DAILY_BUILD_PIPELINE.md` | Daily build pipeline reference |
| `FULL_L2_REPLAY_CATALOG_PLAN.md` | Full-L2 plan and gate status |

---

## Generated / Local Folders (must be gitignored, never committed)

These paths are runtime outputs or local caches and must never be tracked:

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

The docs structure is intentionally **fixed at 14 files**. Before creating any
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
- `build_feature_store.py`
- `generate_catalog.py`
- `raw_manifest.py`

Audit and equivalence modules live in `validation/`, not `pipeline/`.

### `validation/` Is Inspect/Audit-Only

`validation/` contains only modules that **inspect, compare, or audit**
existing artifacts:

- `audit_change_compliance.py`
- `audit_feature_store.py`
- `audit_replay_store.py`
- `audit_storage_size.py`
- `validate_catalog_equivalence.py`
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

# Build feature store from replay
python -m pipeline.build_feature_store --date YYYY-MM-DD [OPTIONS]

# Generate trades-only Nautilus catalog from replay
python -m pipeline.generate_catalog --date YYYY-MM-DD [OPTIONS]

# Run full daily build (replay + features)
python -m pipeline.daily_build --date YYYY-MM-DD [OPTIONS]
```

### Audit / Validate (validation)

```bash
# Audit feature store outputs
python -m validation.audit_feature_store --date YYYY-MM-DD [OPTIONS]

# Audit replay store partitions
python -m validation.audit_replay_store --date YYYY-MM-DD [OPTIONS]

# Check change-audit compliance (pre-commit hook mode)
python -m validation.audit_change_compliance --staged

# Check change-audit compliance vs a base branch
python -m validation.audit_change_compliance --base main

# Compare old convert_day catalog vs new replay catalog
python -m validation.validate_catalog_equivalence --date YYYY-MM-DD [OPTIONS]

# Inspect a Nautilus catalog instrument
python -m validation.catalog_inspect CATALOG_ROOT INSTRUMENT_ID

# Validate a convert report JSON
python -m validation.phase2_report PATH_TO_REPORT
```

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

replay_store -> feature_store
  IMPLEMENTED: v0, UTC-day clamped, sparse windows

replay_store -> generate_catalog --profile trades_only
  IMPLEMENTED: semantically validated

replay_store -> generate_catalog --profile full_l2
  IMPLEMENTED: semantically validated on ADAUSDT smoke
  (BINANCE_SPOT/ADAUSDT/2026-06-12 vs convert_day.py: trades, OrderBookDeltas,
   OrderBookDepth10, and book checkpoints all match). Broader top50/multi-day
   validation still pending. See docs/FULL_L2_REPLAY_CATALOG_PLAN.md.
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
