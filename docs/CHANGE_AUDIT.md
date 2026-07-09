# Change Audit Log

**Purpose:** This is the mandatory append-only log of non-trivial changes made to
CryptoRecorder. Every agent (AI or human) must write an entry here before a commit
or PR is considered complete.

This log protects data integrity: it ensures every change is traceable, every
status claim is honest, and every reviewer can understand what changed and why.

---

## When an entry is required

An entry is required for **any** commit that touches:

- Python source files (`*.py`) anywhere in the repo
- Schema, config, or deployment files (`config.py`, `systemd/`, `requirements.txt`)
- Documentation files where the change affects status claims, validated/deferred
  state, or the repo structure contract

## When an entry may be skipped

An entry may be skipped **only** for:

- Commits that exclusively fix typos or whitespace in docs (no status/claim changes)
- The audit entry commit itself (to avoid infinite recursion)

**If in doubt: write the entry.**

---

## Rules for agents

- **Do not delete old entries** except during an explicit, approved maintenance task
  (and even then, the deletion itself requires an audit entry).
- Add new entries at the **top** of the log (newest-first).
- Fill in every field. If a field does not apply, say why (e.g.,
  "No docs update required because: this is a test-only fix with no public API change").
- Never leave `yes/no` placeholders unfilled.
- Never promote a deferred feature to validated without recorded evidence in
  "Evidence for any new validation claim".

---

## Required entry template

```markdown
## YYYY-MM-DD — <short task title>

### Change summary
- <one bullet per logical change>

### Files/packages touched
- <path/to/file.py>
- <package/>

### Docs reviewed
- [ ] AGENTS.md
- [ ] docs/REPO_STRUCTURE.md
- [ ] docs/PROJECT_STATUS.md
- [ ] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - <list or "none">

### Docs updated
- [ ] CHANGELOG.md
- [ ] README.md
- [ ] docs/PROJECT_STATUS.md
- [ ] docs/REPO_STRUCTURE.md
- [ ] relevant feature docs:
  - <list or "none">
- No docs update required because: <reason, or delete this line if docs were updated>

### Status / validation impact
- Validated status changed: yes/no
- Deferred status changed: yes/no
- New claims added: yes/no
- Evidence for any new validation claim:
  - <command + output summary, or "n/a">

### Tests run
```bash
<exact commands>
```

### Validation CLIs run
```bash
<exact commands, or "none required for this change type">
```

### Known limitations / out of scope
- <explicit list of what was NOT done>
- <or "none — task fully completed">
```

---

## Example of a GOOD entry

```markdown
## 2026-07-01 — Add audit_storage_size CLI

### Change summary
- Added `validation/audit_storage_size.py` to measure on-disk artifact sizes.
- Added `docs/IMPLEMENTATION_AUDIT.md` describing the audit output format.

### Files/packages touched
- validation/audit_storage_size.py (new)
- docs/STORAGE_SIZE_AUDIT.md (new)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; audit-only tool, not a user-facing feature
- [ ] docs/PROJECT_STATUS.md — no status change; tool is an inspect/report utility
- [x] docs/REPO_STRUCTURE.md — added audit_storage_size.py to validation/ module list
- [x] relevant feature docs:
  - docs/STORAGE_SIZE_AUDIT.md (new)

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q
# 22 passed
```

### Validation CLIs run
```bash
# none required — this is a new inspect CLI, not a replay/feature/catalog change
```

### Known limitations / out of scope
- Does not yet track per-symbol breakdown; that is deferred.
```

---

## Example of a BAD entry (do not do this)

```markdown
## 2026-07-01 — Fixed stuff

### Change summary
- Fixed some things.

### Files/packages touched
- various

### Docs reviewed
- (skipped — no time)

### Docs updated
- not needed

### Status / validation impact
- no change

### Tests run
```bash
pytest
```

### Validation CLIs run
- skipped

### Known limitations / out of scope
- none
```

**Why this is bad:** "various", "some things", "no time", "not needed" are not
auditable. Reviewers cannot tell what changed, what was reviewed, or whether
status claims are honest.

---

## Audit entries (newest first)

---

## 2026-07-09 — Conventional commits enforcement (commit-msg hook + AGENTS.md Section 7)

### Change summary
- Created `.githooks/commit-msg` — bash hook that validates every commit message
  against the conventional commits format before the commit is accepted.
  Enforces: correct type, no capital first letter, no trailing period, blank line
  before body. Skips auto-generated messages (Merge, Revert, fixup!, squash!).
- Added `AGENTS.md` Section 7 "Commit message style" with full format reference,
  type table, subject rules, valid/invalid examples, and bypass guidance.
- Updated `docs/AI_WORKFLOW.md` Step 7 to include commit message format requirement.
- Added a new "bad behavior" example for malformed commit messages.
- Fixed workflow heading: "The 8-step workflow" → "The 9-step workflow".
- Updated `docs/REPO_STRUCTURE.md`: expanded `.githooks/` description to list both hooks.
- Updated `INSTALL.md` Section 7 to describe both hooks.
- Updated `docs/REPO_STRUCTURE.md` amendment log.

### Files/packages touched
- .githooks/commit-msg (new)
- AGENTS.md
- docs/AI_WORKFLOW.md
- docs/REPO_STRUCTURE.md
- INSTALL.md
- docs/CHANGE_AUDIT.md (this entry)
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed
- [ ] docs/PROJECT_STATUS.md — no status change
- [x] docs/REPO_STRUCTURE.md — .githooks/ description + amendment log
- [ ] relevant feature docs:
  - none applicable

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
# 37 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- The hook cannot verify imperative tense ("add" vs "adding") — that is documented
  but not mechanically checked.
- No changes to recorder, stores, pipeline, converter, or validation Python code.

---

## 2026-07-09 — Docs structure consolidation (14-file fixed structure)

### Change summary
- Merged 9 small docs into 4 larger comprehensive files (all content preserved).
- `docs/STORAGE_ARCHITECTURE.md` + `docs/GUARANTEES.md` → appended to `docs/ARCHITECTURE.md`.
- `docs/DEPLOYMENT.md` + `docs/LINUX_SERVER.md` + `docs/SCHEMAS.md` → appended to `docs/OPERATIONS.md`.
- `docs/REPO_CLEANUP_AUDIT.md` + `docs/FEATURE_STORE_REQUIREMENTS_AUDIT.md` + `docs/STORAGE_SIZE_AUDIT.md` → appended to `docs/IMPLEMENTATION_AUDIT.md`.
- `docs/VERSIONING.md` → appended to `CHANGELOG.md` as "Versioning Policy" section.
- Deleted the 9 source files after merging.
- Rewrote `docs/README.md` as a navigation index with a "Where to update what" table.
- Added "No New Docs Files" rule to `AGENTS.md` Section 2 and `docs/REPO_STRUCTURE.md`.
- Updated `tests/test_agent_infrastructure.py` REQUIRED_FILES and link checks.
- Fixed all cross-references throughout the repo to point to the new consolidated files.
- Updated `REPO_STRUCTURE.md` docs/ package table and amendment log.
- Updated `README.md` (root) key-references section.

### Files/packages touched
- docs/ARCHITECTURE.md (expanded: +STORAGE_ARCHITECTURE + GUARANTEES)
- docs/OPERATIONS.md (expanded: +DEPLOYMENT + LINUX_SERVER + SCHEMAS)
- docs/IMPLEMENTATION_AUDIT.md (expanded: +REPO_CLEANUP_AUDIT + FS_REQ_AUDIT + STORAGE_SIZE_AUDIT)
- CHANGELOG.md (expanded: +VERSIONING policy section)
- docs/STORAGE_ARCHITECTURE.md (deleted)
- docs/GUARANTEES.md (deleted)
- docs/DEPLOYMENT.md (deleted)
- docs/LINUX_SERVER.md (deleted)
- docs/SCHEMAS.md (deleted)
- docs/REPO_CLEANUP_AUDIT.md (deleted)
- docs/FEATURE_STORE_REQUIREMENTS_AUDIT.md (deleted)
- docs/STORAGE_SIZE_AUDIT.md (deleted)
- docs/VERSIONING.md (deleted)
- docs/README.md (rewritten as navigation index)
- AGENTS.md (No New Docs rule + updated read order)
- docs/REPO_STRUCTURE.md (docs/ table + No New Docs rule + amendment log)
- tests/test_agent_infrastructure.py (REQUIRED_FILES updated)
- README.md (key references updated)
- INSTALL.md (More Documentation section updated)
- docs/AI_WORKFLOW.md, docs/PROJECT_STATUS.md, docs/REPLAY_STORE.md, docs/GENERATE_CATALOG.md, docs/DAILY_BUILD_PIPELINE.md (cross-reference links updated)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — this is documentation infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [x] README.md
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change
- [x] docs/REPO_STRUCTURE.md — docs/ table, No New Docs rule, amendment log

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
# 37 passed
pytest -q
# 238 passed, 3 skipped
```

### Validation CLIs run
```bash
# none required — documentation-only restructure
```

### Known limitations / out of scope
- The merged sections in ARCHITECTURE.md, OPERATIONS.md, and IMPLEMENTATION_AUDIT.md
  retain "Content merged from the former X" markers; future contributors may do a
  prose clean-up pass to integrate them more smoothly.
- No changes to recorder, stores, pipeline, converter, or validation Python code.
- The -2 test count vs previous run (238 vs 240) is expected: DEPLOYMENT.md,
  LINUX_SERVER.md, and VERSIONING.md removed from REQUIRED_FILES; OPERATIONS.md added.

---

## 2026-07-09 — Mandatory change-audit infrastructure

### Change summary
- Added `AGENTS.md` Section 6: mandatory change audit before commit.
- Updated `AGENTS.md` Definition of Done (item 7) to require an audit entry.
- Extended `docs/AI_WORKFLOW.md` with Step 7 (write the change audit entry) and a
  new "bad behavior" example for skipping the audit.
- Created `docs/CHANGE_AUDIT.md` (this file) as the append-only audit log.
- Created `validation/audit_change_compliance.py` — a compliance-check CLI that
  inspects staged or branch-diff changes and reports PASS/FAIL.
- Created `.githooks/pre-commit` — a git hook that runs the compliance CLI on every
  commit and blocks if it reports FAIL.
- Updated `docs/REPO_STRUCTURE.md`:
  - Added `.githooks/` to allowed configuration directories.
  - Added `audit_change_compliance.py` to the `validation/` module list.
  - Added amendment log entry.
- Updated `INSTALL.md` with instructions to activate the git hook.
- Updated `CHANGELOG.md [Unreleased]`.

### Files/packages touched
- AGENTS.md
- docs/AI_WORKFLOW.md
- docs/CHANGE_AUDIT.md (new)
- validation/audit_change_compliance.py (new)
- .githooks/pre-commit (new)
- docs/REPO_STRUCTURE.md
- INSTALL.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — this is infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; audit infrastructure is internal tooling
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change
- [x] docs/REPO_STRUCTURE.md — added .githooks/, updated validation/ module list
- [ ] relevant feature docs:
  - none applicable

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- The compliance CLI performs heuristic text-pattern checks on the latest audit
  entry; it does not do deep semantic validation of claim honesty.
- The pre-commit hook must be activated manually per-clone via
  `git config core.hooksPath .githooks` (see INSTALL.md).
- No changes to recorder, stores, pipeline, or converter code.
