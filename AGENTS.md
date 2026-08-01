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
5. [docs/OPERATIONS.md](docs/OPERATIONS.md) — how the system is deployed and run (includes Linux server layout, deployment script, and state file schemas).
6. [docs/AI_WORKFLOW.md](docs/AI_WORKFLOW.md) — the step-by-step working procedure.
7. [CHANGELOG.md](CHANGELOG.md) — recent changes, version state, and versioning policy.

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
- Do **not** describe Syncthing or archive features as implemented.
  `ARCHIVE_DAYS_ROOT` in `config.py` is a **placeholder only**. `LABEL_ROOT` was
  removed entirely (issue #17) — do not reintroduce a label/target store.
- Never mark a deferred item as done.

### No New Docs Files Without Contract Amendment

The docs structure is intentionally **fixed at 12 files**. Before creating any
new file in `docs/`, you **must** identify which existing file is the right home
for the content, add it as a new section there, and (only if no existing file
fits) amend `docs/REPO_STRUCTURE.md` with a justification.

| New content type | Write it into |
|-----------------|---------------|
| Status / validation changes | `docs/PROJECT_STATUS.md` |
| Architecture / design decisions | `docs/ARCHITECTURE.md` |
| Storage schemas / build pipeline details | `docs/ARCHITECTURE.md` |
| Operational procedures / deployment | `docs/OPERATIONS.md` |
| Linux server / production paths | `docs/OPERATIONS.md` |
| State file schemas (heartbeat, startup, convert) | `docs/OPERATIONS.md` |
| Ground-truth what exists / audit snapshot | `docs/IMPLEMENTATION_AUDIT.md` |
| Requirements audit / storage size measurement | `docs/IMPLEMENTATION_AUDIT.md` |
| Validation layers and CLI reference | `docs/VALIDATION.md` |
| Feature reference (replay) | `docs/REPLAY_STORE.md` |
| Feature reference (daily build) | `docs/DAILY_BUILD_PIPELINE.md` |
| Full-L2 plan / gate status | `docs/FULL_L2_REPLAY_CATALOG_PLAN.md` |
| Change audit entries | `docs/CHANGE_AUDIT.md` (append-only) |
| Version history | `CHANGELOG.md` |

There is no `docs/FEATURE_STORE.md` or `docs/GENERATE_CATALOG.md` (removed,
issue #17) — CryptoRecorder does not own a feature-store or a product-facing
catalog-generation CLI. Do not recreate either file.

If the agent is unsure which file to update, **stop and ask** rather than
creating a new file.

### Folder boundaries (see docs/REPO_STRUCTURE.md)
- Do **not** add new top-level folders without first amending
  [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) (amendment log entry required).
- `pipeline/` = build/transform CLIs only. **No audit/compare/inspect CLIs here.**
- `validation/` = audit/compare/inspect CLIs only. **No build CLIs here.**
- `converter/` = legacy full-L2 raw→Nautilus converter internals.
- `stores/` = replay schemas, readers, writers (no feature/label schemas).
- `scripts/` = thin operator wrappers only. **No business logic in scripts/.**
- Do **not** recreate the deleted `validators/` package or the deleted
  feature-store subsystem (`stores/feature_*.py`, `pipeline/build_feature_store.py`,
  `validation/audit_feature_store.py`).

---

## 3. Definition of Done

A change is **not** done until all of the following are true:

1. Relevant docs are updated (`docs/`, `README.md`, and `docs/PROJECT_STATUS.md`
   if status changed).
2. [CHANGELOG.md](CHANGELOG.md) `## [Unreleased]` section is updated.
3. `pytest` passes from the explicit locked development environment
   (`uv sync --frozen --no-default-groups --extra reconstruction --group dev`).
4. If you touched replay or catalog-reconstruction code, you ran the relevant
   audit/validation CLI (`validation/audit_replay_store.py` or
   `validation/validate_catalog_equivalence.py`) and reported the result.
5. Status stays **honest**: validated stays validated, deferred stays deferred.
6. You stated explicitly **what was not done** / out of scope.
7. **A change audit entry has been written** in [docs/CHANGE_AUDIT.md](docs/CHANGE_AUDIT.md)
   and `python -m validation.audit_change_compliance --staged` reports PASS
   (see Section 6 below).

---

## 4. Required tests by task type

| Task type | Run at minimum |
|-----------|----------------|
| Repo structure / file moves | `pytest tests/test_repo_structure.py` |
| Replay store changes | `pytest tests/test_replay_store.py` + `validation/audit_replay_store.py` |
| Catalog reconstruction (validation-only) | `pytest tests/test_replay_catalog_reconstruct.py tests/test_catalog_equivalence.py` |
| Converter / raw (only if explicitly allowed) | full `pytest` + `validation/validate_catalog_equivalence.py` |
| AI / deployment infrastructure | `pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py` |
| Anything else | full `pytest` |

---

## 5. Agent behavior

- **Stop and ask** if you are uncertain about a path, a destructive action, or
  whether raw-side changes are in scope.
- Do **not** invent production paths. The canonical prod layout lives in the
  `## Linux Server Layout` section of [docs/OPERATIONS.md](docs/OPERATIONS.md);
  use those values, do not guess.
- Do **not** delete data directories (`data_raw/`, `replay_store/`,
  `state/`, catalog outputs, or any pre-existing local `feature_store/` left
  over from before issue #17 — even though CryptoRecorder no longer writes to
  it).
- Do **not** mark deferred work as done to "close" a task.
- Prefer **small, reviewable changes** over large sweeping rewrites.
- `pyproject.toml` plus the committed `uv.lock` are the only dependency
  authority. Never restore a hand-maintained `requirements.txt`, use pip for
  deployment installation, or run an unfrozen deployment sync. Production is
  `uv sync --frozen --no-default-groups`; reconstruction adds
  `--extra reconstruction`; development adds that extra plus `--group dev`.
- When in doubt about whether a folder/file is allowed, consult
  [docs/REPO_STRUCTURE.md](docs/REPO_STRUCTURE.md) first.

---

## 6. Mandatory change audit before commit

Every non-trivial change — code, schema, pipeline, storage, deployment, feature,
validation, or documentation-affecting — **must** include a change audit entry in
[docs/CHANGE_AUDIT.md](docs/CHANGE_AUDIT.md) before the change is considered
complete.

### When is an audit entry required?

An entry is required for **any** commit that touches:
- Python source files (`*.py`) anywhere in the repo
- Schema, dependency, config, or deployment files (`config.py`,
  `pyproject.toml`, `uv.lock`, `systemd/`)
- Documentation files if the change affects status claims, validated/deferred state,
  or the repo structure contract

An entry **may be skipped** only for:
- Commits that exclusively fix typos or whitespace in docs (no status/claim changes)
- The audit entry commit itself (to avoid infinite recursion)

### Checklist an agent must satisfy before claiming complete

No agent may claim the task is done unless **all** of the following are true:

- [ ] Relevant contract docs were read (AGENTS.md, REPO_STRUCTURE.md, PROJECT_STATUS.md,
      IMPLEMENTATION_AUDIT.md, and any relevant feature doc).
- [ ] Affected docs were updated **or** explicitly marked "No docs update required
      because: \<reason\>" in the audit entry.
- [ ] `CHANGELOG.md [Unreleased]` was updated **or** explicitly justified as not
      applicable in the audit entry.
- [ ] `docs/PROJECT_STATUS.md` was updated if any validated/deferred status changed.
- [ ] Tests were run and listed in the audit entry.
- [ ] Required validation/audit CLIs were run and listed in the audit entry.
- [ ] Out-of-scope and deferred work was explicitly stated in the audit entry.
- [ ] No deferred feature was promoted to validated without recorded evidence.
- [ ] `python -m validation.audit_change_compliance --staged` passes.

If the agent is **unsure** whether docs, status, or changelog are required, it must
**stop and ask** rather than guess or skip the entry.

### What to do if the pre-commit hook blocks a commit

Run `python -m validation.audit_change_compliance --staged` to see the full
PASS/FAIL report with actionable messages. Fix each listed failure before
retrying the commit.

---

## 7. Commit message style

Every commit must follow **conventional commits** format. The `.githooks/commit-msg`
hook enforces this automatically and blocks non-conforming commits.

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Fields

| Field | Required | Rules |
|-------|----------|-------|
| `type` | **mandatory** | one of the 8 types below; lowercase |
| `scope` | recommended | in parentheses; indicates affected area, e.g. `(stores)`, `(pipeline)` |
| `subject` | **mandatory** | imperative present tense; **no capital first letter**; **no trailing dot** |
| `body` | optional | explains motivation and contrasts with previous behavior; separated from subject by a blank line |
| `footer` | optional | breaking changes, issue references |

### Allowed types

| Type | When to use |
|------|-------------|
| `feat` | new feature for the user |
| `fix` | bug fix relevant to the user |
| `docs` | documentation changes only |
| `style` | formatting, whitespace, missing semicolons (no logic change) |
| `refactor` | neither fixes a bug nor adds a feature |
| `perf` | performance improvement |
| `test` | changes to tests only |
| `chore` | build process, hooks, auxiliary tools, not relevant to users |

### Subject rules

- Use **imperative, present tense**: write `add` not `added` or `adds`
- **Do not** capitalize the first letter: `add adapter` not `Add adapter`
- **Do not** end with a period: `add adapter` not `add adapter.`

### Footer examples

```
Closes #313
BREAKING CHANGE: replay schema v1 removed; migrate with migration/replay_v0_to_v1.py
```

### Valid examples

```
feat(stores): add replay depth adapter
fix(pipeline): correct date parsing for yesterday flag
docs: update AI_WORKFLOW step numbering
chore(.githooks): add commit-msg style enforcement hook
test(catalog): add synthetic full-l2 equivalence case
refactor(converter): extract book state into separate module
```

### Invalid examples (hook will block)

```
Add replay depth adapter          ← missing type
feat: Add replay depth adapter    ← capital first letter in subject
feat(stores): add adapter.        ← trailing period
feat(stores): add adapter
no blank line here                ← body requires blank separator line
```

### What to do if the commit-msg hook blocks a commit

Read the error message — it states exactly which rule was violated and shows an
example of the correct format. Fix the commit message with `git commit --amend`
or re-run `git commit` with the corrected message.

To bypass in genuinely exceptional circumstances:

```bash
git commit --no-verify
```

`--no-verify` does **not** exempt you from the audit-entry requirement in Section 6.
