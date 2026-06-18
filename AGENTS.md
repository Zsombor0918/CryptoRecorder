# AGENTS.md — Rules for AI Agents Working in CryptoRecorder

This file is **binding** for any AI agent (Copilot, Claude, Cursor, etc.) making
changes in this repository. Read it fully before editing anything.

CryptoRecorder records Binance **spot** and **USDT-M futures** market data and
converts it into deterministic artifacts for Nautilus Trader backtesting. It is a
data-integrity system: silent corruption or dishonest status claims are worse than
doing nothing.

---

## 1. Required read order (before any change)

Read these, in order, before proposing or making changes:

1. [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) — the frozen folder/file contract.
2. [docs/PROJECT_STATUS.md](docs/PROJECT_STATUS.md) — what is validated vs deferred.
3. [docs/IMPLEMENTATION_AUDIT.md](docs/IMPLEMENTATION_AUDIT.md) — ground-truth of what exists.
4. [docs/FULL_L2_REPLAY_CATALOG_PLAN.md](docs/FULL_L2_REPLAY_CATALOG_PLAN.md) — full-L2 replay path (validated on the ADAUSDT smoke; broader validation pending).
5. [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — how the system is deployed.
6. [docs/LINUX_SERVER.md](docs/LINUX_SERVER.md) — dev (WSL) vs prod (Ubuntu) layout.
7. [docs/AI_WORKFLOW.md](docs/AI_WORKFLOW.md) — the step-by-step working procedure.
8. [CHANGELOG.md](CHANGELOG.md) — recent changes and version state.

If any of these contradict the task you were given, **stop and ask** instead of guessing.

---

## 2. Hard constraints (do NOT violate)

### Recorder / raw ingestion
- Do **not** change `recorder.py`, `phase2_depth.py`, `native_trades.py`, or
  `storage.py` unless the task **explicitly** says raw-side changes are allowed.
- Do **not** change the **raw data schema or on-disk layout** under `data_raw/`.
  The path `data_raw/ → convert_day.py → Nautilus full-L2 catalog` is the validated
  production path and must keep working byte-for-byte.
- Do **not** delete or rewrite `convert_day.py`. It is the reference converter.

### Status honesty
- The `replay_store → full_l2 catalog` path is implemented and validated on the
  **ADAUSDT single-day smoke** against `convert_day.py`. Do **not** claim broader
  top50/multi-day equivalence (the `v2.0.0` gate) until that wider validation
  passes; `convert_day.py` remains the production reference.
- Do **not** describe Syncthing, archive, or import features as implemented.
  `ARCHIVE_DAYS_ROOT` and `LABEL_ROOT` in `config.py` are **placeholders only**.
- Never mark a deferred item as done.

### Folder boundaries (see docs/REPO_STRUCTURE.md)
- Do **not** add new top-level folders without first amending
  [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) (amendment log entry required).
- `pipeline/` = build/transform CLIs only. **No audit/compare/inspect CLIs here.**
- `validation/` = audit/compare/inspect CLIs only. **No build CLIs here.**
- `converter/` = legacy full-L2 raw→Nautilus converter internals.
- `stores/` = replay/feature schemas, readers, writers.
- `scripts/` = thin operator wrappers only. **No business logic in scripts/.**
- Do **not** recreate the deleted `validators/` package.

---

## 3. Definition of Done

A change is **not** done until all of the following are true:

1. Relevant docs are updated (`docs/`, `README.md`, and `docs/PROJECT_STATUS.md`
   if status changed).
2. [CHANGELOG.md](CHANGELOG.md) `## [Unreleased]` section is updated.
3. `pytest` passes locally (`source .venv/bin/activate && pytest`).
4. If you touched replay / feature / catalog code, you ran the relevant
   audit/validation CLI (`validation/audit_replay_store.py`,
   `validation/audit_feature_store.py`, or
   `validation/validate_catalog_equivalence.py`) and reported the result.
5. Status stays **honest**: validated stays validated, deferred stays deferred.
6. You stated explicitly **what was not done** / out of scope.

---

## 4. Required tests by task type

| Task type | Run at minimum |
|-----------|----------------|
| Repo structure / file moves | `pytest tests/test_repo_structure.py` |
| Replay store changes | `pytest tests/test_replay_store.py` + `validation/audit_replay_store.py` |
| Feature store changes | `pytest tests/test_feature_store.py` + `validation/audit_feature_store.py` |
| Catalog generation | `pytest tests/test_generate_catalog.py tests/test_catalog_equivalence.py` |
| Converter / raw (only if explicitly allowed) | full `pytest` + `validation/validate_catalog_equivalence.py` |
| AI / deployment infrastructure | `pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py` |
| Anything else | full `pytest` |

---

## 5. Agent behavior

- **Stop and ask** if you are uncertain about a path, a destructive action, or
  whether raw-side changes are in scope.
- Do **not** invent production paths. The canonical prod layout lives in
  [docs/LINUX_SERVER.md](docs/LINUX_SERVER.md); use those values, do not guess.
- Do **not** delete data directories (`data_raw/`, `replay_store/`, `feature_store/`,
  `state/`, catalog outputs).
- Do **not** mark deferred work as done to "close" a task.
- Prefer **small, reviewable changes** over large sweeping rewrites.
- When in doubt about whether a folder/file is allowed, consult
  [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) first.
