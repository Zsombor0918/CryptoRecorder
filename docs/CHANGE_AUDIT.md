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
## 2026-07-20 — fix disk monitor false-zero reporting and fail-open cleanup (issue #19)

### Change summary
- Rewrote `disk_monitor.py` to eliminate the false-zero measurement defect: a
  failed/timed-out recursive `du` scan previously returned numeric `0.0`, which was
  published as `data_raw_gb=0.0` and silently disabled capacity alerts and
  automatic cleanup (observed ~442 timeouts since June 2 against a ~410GB raw tree
  with a 30s hard-coded timeout).
- Added `measure_directory()` / `DirectoryMeasurement` (`ok`, `status` — one of
  `ok`/`missing`/`timeout`/`command_error`/`malformed_output`/`error`, `error`,
  `value_bytes`, `measured_at`, `duration_seconds`) so a failure can never be
  represented as a bare numeric zero. A genuinely empty directory still reports
  `ok=True, status="ok"`.
- Switched the scan command from `du -sb` (apparent size) to `du -s -B1` (allocated
  bytes) — documented as the intended, more honest-for-retention semantics.
- Added last-known-good persistence (`state/disk_monitor_state.json`): on measurement
  failure the monitor falls back to the prior successful value marked `stale=True`
  with `measurement_age_seconds`; if no prior value exists the field is `null`, never
  `0`. State survives process restarts (loaded in `DiskMonitor.__init__`).
- `state/disk_usage.json` now reports per-component `measurement_ok` /
  `measurement_status` / `measurement_error` / `measurement_timestamp` /
  `measurement_age_seconds` / `stale`, a top-level `monitoring_health`
  (`healthy`/`degraded`/`unhealthy`), and an `alerts` list. Retention percentages,
  growth rate, and `days_to_full` are `null` (never derived) when the backing data is
  unknown/stale.
- `cleanup_old_data()` now fails closed: it refuses to run or continue unless the
  current cycle's `data_raw` measurement is fresh and successful
  (`retention_measurement_trustworthy=True`), re-validating before every destructive
  deletion phase, and logs an `ERROR` (with a report alert) when skipped.
- Added independent filesystem-capacity reporting via `measure_filesystem()`
  (`shutil.disk_usage`), exposed under `filesystem.*` in the report, with its own
  `DISK_FS_FREE_WARN_GB`/`DISK_FS_FREE_CRITICAL_GB` thresholds — kept semantically
  separate from the raw-retention `DISK_SOFT_LIMIT_GB`/`DISK_HARD_LIMIT_GB` limits.
- Growth-rate/`days_to_full` now use real sample timestamps (bounded, persisted
  `GrowthSample` history capped by `DISK_HISTORY_MAX_SAMPLES`/`DISK_HISTORY_MAX_AGE_SEC`),
  only recording a sample when every monitored root was measured fresh and
  successfully in the same cycle; non-increasing timestamps are rejected; growth and
  `days_to_full` are `null` when the valid sample span is under 1 hour.
- Added an `asyncio.Lock` around `check_disk_usage()` to prevent overlapping scans;
  an overlapping call returns the previous report with `skipped_duplicate=True`
  instead of queuing or running concurrently. The lock is released via `async with`
  on every exception path.
- Report and companion-state writes are now atomic (`tempfile.NamedTemporaryFile` in
  the same directory + `os.replace()`), with the temp file cleaned up on any
  write failure.
- `config.py`: added `DISK_SCAN_TIMEOUT_SEC` (default 60s, validated > 0),
  `DISK_MEASUREMENT_STALE_AFTER_SEC`, `DISK_FS_FREE_WARN_GB`,
  `DISK_FS_FREE_CRITICAL_GB`, `DISK_HISTORY_MAX_SAMPLES`, `DISK_HISTORY_MAX_AGE_SEC`;
  existing `DISK_SOFT_LIMIT_GB`/`DISK_HARD_LIMIT_GB`/`DISK_CLEANUP_TARGET_GB` env vars
  are unchanged for backward compatibility.
- `recorder.py`: `disk_check_task()` updated to use `usage.get('data_raw_gb')` (no
  longer defaults a missing/None value to `0`) before comparing against the soft
  limit.
- Added `tests/test_disk_monitor_fail_safe.py` (30 new tests) covering: successful/
  empty/missing/timeout/nonzero-exit/malformed-output/unexpected-exception `du`
  parsing; invalid-timeout config validation; last-known-good fallback marked
  stale; restart-persisted state; no-prior-value → `null`; staleness alert;
  misleading percentage/growth omission; cleanup skipped on unknown/stale
  measurement with no destructive `shutil.rmtree` call; independent filesystem
  capacity fields and low-free-space alert; separate retention/filesystem threshold
  semantics; atomic report writing and temp-file cleanup on failure; growth from
  real timestamps, short-span exclusion, non-increasing-timestamp rejection,
  failed/stale-sample exclusion; overlapping-scan prevention and lock release on
  exception.
- Updated `tests/test_disk_monitor_cleanup.py`'s two existing fakes to include
  `retention_measurement_trustworthy: True` (new required field in the cleanup
  trust contract) — no behavioral change to those tests' assertions.
- No production data, service, or `/etc` changes were made. No destructive cleanup
  was run against real data during implementation or testing (temp dirs / mocks
  only).

### Files/packages touched
- `disk_monitor.py` (rewritten)
- `recorder.py` (`disk_check_task` — safe `.get()` for `data_raw_gb`)
- `config.py` (new disk-monitor env vars + docstring clarifying retention vs
  filesystem-threshold semantics)
- `systemd/cryptorecorder.env.example` (documented new env vars)
- `tests/test_disk_monitor_fail_safe.py` (new)
- `tests/test_disk_monitor_cleanup.py` (updated fakes for the trust contract)
- `docs/ARCHITECTURE.md` (new "Disk Monitoring Safety Invariant" section)
- `docs/OPERATIONS.md` (new "Disk Monitoring" field/alert/threshold reference)
- `docs/IMPLEMENTATION_AUDIT.md` (addendum under Section A)
- `docs/PROJECT_STATUS.md` (new validated bullet + `Last updated` bump)
- `INSTALL.md` (runtime file table: `disk_usage.json` description +
  `disk_monitor_state.json` row)
- `CHANGELOG.md` (`[Unreleased]` → new "Fixed" section)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/OPERATIONS.md, docs/VALIDATION.md (no disk-monitor
    content existed there; not amended), CHANGELOG.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md
- [x] docs/PROJECT_STATUS.md
- [ ] docs/REPO_STRUCTURE.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/OPERATIONS.md, docs/IMPLEMENTATION_AUDIT.md,
    INSTALL.md, systemd/cryptorecorder.env.example
- No docs update required for README.md/REPO_STRUCTURE.md because: no new
  top-level files/folders or root-entrypoint changes were introduced; this is an
  internal-module fix within the existing `disk_monitor.py` file already listed
  in `docs/REPO_STRUCTURE.md`.

### Status / validation impact
- Validated status changed: yes — `docs/PROJECT_STATUS.md` gained a new
  "Disk monitoring (fail-safe measurement)" validated bullet.
- Deferred status changed: no.
- New claims added: yes — the fail-safe measurement behavior is claimed as
  validated by the focused test suite below; **real-server verification is
  explicitly NOT claimed** (deployment/log/report inspection is documented as a
  manual, not-yet-performed checklist item — see PR body).
- Evidence for any new validation claim:
  - `pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q`
    → `30 passed`
  - Full suite: `pytest -q` → `266 passed, 3 skipped`

### Tests run
```bash
source .venv/bin/activate
pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q
pytest -q
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- Real production-server verification (restarting the monitor/recorder service,
  inspecting logs and `disk_usage.json` against the actual ~410GB raw tree) was
  **not performed** as part of this change — see the manual deployment/
  verification checklist in the PR description. No production data or services
  were touched.
- `get_dir_size_gb()` is retained only as a best-effort single-directory helper
  for cleanup log messages; it is not used for any retention/cleanup decision
  (which relies solely on the current cycle's `data_raw` `DirectoryMeasurement`).
- Concurrent/parallel scanning across roots was deliberately **not** introduced
  (roots are measured sequentially against the same disk) since the issue asked
  to avoid concurrent recursive scans unless benchmarked as safe; no such
  benchmark was performed in this change.
- `disk_check_task()` in `recorder.py` still runs on a fixed
  `DISK_CHECK_INTERVAL_SEC` sleep loop (unchanged); the new `asyncio.Lock`-based
  overlap guard lives inside `DiskMonitor.check_disk_usage()` itself, which is
  sufficient because `disk_check_task()` awaits each cycle in sequence and does
  not spawn concurrent calls itself — this is noted for completeness, not as a
  gap.


---

## 2026-07-15 — Issue #17: narrow scope to recorder + replay-store ownership, remove feature-store subsystem

### Change summary
- Removed the entire **feature-store subsystem**: `stores/feature_schema.py`,
  `stores/feature_calc.py`, `stores/feature_writer.py`,
  `pipeline/build_feature_store.py`, `validation/audit_feature_store.py`,
  `tests/test_feature_store.py`, and the
  `systemd/cryptorecorder-feature-build.{service,timer}` units.
- Removed `pipeline/generate_catalog.py` as a **product/runtime CLI**. Moved its
  `generate_catalog_from_replay` reconstruction logic and helpers to
  `validation/replay_catalog_reconstruct.py` — an internal, CLI-less,
  validation-only helper used exclusively by
  `validation/validate_catalog_equivalence.py`. Renamed
  `tests/test_generate_catalog_full_l2.py` to
  `tests/test_replay_catalog_reconstruct.py` and removed
  `tests/test_generate_catalog.py` (trades_only product-CLI tests, no longer
  applicable).
- Removed `config.py`: `FEATURE_ROOT`, `LABEL_ROOT`, `CATALOG_JOBS_ROOT`.
  `ARCHIVE_DAYS_ROOT` is unaffected (still a placeholder, not implemented).
- Simplified `pipeline/daily_build.py`: removed `--steps`, `--timeframes`,
  `--feature-root` CLI flags and the feature-build execution path. It now
  always scans raw coverage and builds the replay store only; report shape no
  longer contains a `feature_build` section.
- Deleted `docs/FEATURE_STORE.md` and `docs/GENERATE_CATALOG.md` rather than
  leaving tombstones — the fixed docs/ file count drops from 14 to 12.
- Rewrote `docs/REPO_STRUCTURE.md` (12-file contract, narrowed `pipeline/`,
  `stores/`, `validation/` package definitions, updated CLI Command Reference,
  new Amendment Log entry), `docs/IMPLEMENTATION_AUDIT.md` (feature-store
  sections marked removed with preservation banners, historical evidence
  retained), `docs/PROJECT_STATUS.md` (Validated/Deferred sections updated,
  replay_store framed as the stable external contract for downstream
  repositories), `docs/ARCHITECTURE.md` (removed the "Feature Store" storage
  layer and "Build Feature Store"/"Generate Catalog" pipeline sections,
  replaced with the validation-only `validation.replay_catalog_reconstruct`
  helper description), `docs/DAILY_BUILD_PIPELINE.md` (fully rewritten,
  replay-only), `docs/OPERATIONS.md` (removed `feature-build` service group and
  `FEATURE_ROOT`/`CATALOG_JOBS_ROOT`/`LABEL_ROOT` path rows), `docs/REPLAY_STORE.md`,
  `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`, `docs/AI_WORKFLOW.md`, `docs/README.md`,
  `README.md` (root), `AGENTS.md`, and `.github/copilot-instructions.md`.
- Rewrote `tests/test_repo_structure.py`: added
  `test_pipeline_does_not_contain_feature_store_modules()` and
  `test_pipeline_does_not_contain_generate_catalog_cli()`; updated
  `test_docs_do_not_reference_pipeline_audit_modules()` forbidden-pattern list;
  updated `test_validation_contains_audit_and_equivalence_modules()` required
  module list; removed `test_validation_audit_feature_store_cli_help()`.
- Updated `tests/test_agent_infrastructure.py` `DEPLOY_TARGETS` (removed
  `feature-build`); updated `tests/test_semantic_equivalence.py` and
  `tests/test_replay_depth_adapter.py` to reference
  `validation.replay_catalog_reconstruct` instead of the removed
  `pipeline/generate_catalog.py` module.
- Cleaned `scripts/deploy_linux_server.sh` (removed the `feature-build` target
  throughout: `VALID_TARGETS`, help text, unit/control case statements,
  directory creation list, plus an explicit stale-unit cleanup step that
  stops/disables/removes any previously-installed
  `cryptorecorder-feature-build.{service,timer}` on `all`/`replay-build`
  deploys so upgraded servers don't keep firing the removed
  `daily_build --steps features` command), `systemd/cryptorecorder.env.example` (removed
  `CRYPTO_RECORDER_FEATURE_ROOT`, `CRYPTO_RECORDER_CATALOG_JOBS_ROOT`,
  `CRYPTO_RECORDER_LABEL_ROOT`), `systemd/cryptorecorder-replay-build.service`
  (removed the now-nonexistent `--steps replay` flag), and `scripts/README.md`.
- Updated package docstrings: `pipeline/__init__.py`, `stores/__init__.py`,
  `validation/__init__.py`.
- `validation/audit_change_compliance.py`: `_REPLAY_CATALOG_PATTERNS` no longer
  includes `stores/feature`, `pipeline/build_feature`, `pipeline/generate_catalog`,
  or `validation/audit_feature`.
- `validation/audit_storage_size.py` deliberately left unchanged (see "Known
  limitations" below).

### Files/packages touched
- pipeline/__init__.py, pipeline/daily_build.py
- pipeline/build_feature_store.py (deleted), pipeline/generate_catalog.py (deleted)
- stores/__init__.py
- stores/feature_schema.py, stores/feature_calc.py, stores/feature_writer.py (all deleted)
- stores/replay_depth_adapter.py
- validation/__init__.py
- validation/audit_feature_store.py (deleted)
- validation/replay_catalog_reconstruct.py (new)
- validation/audit_change_compliance.py
- config.py
- tests/test_repo_structure.py
- tests/test_agent_infrastructure.py
- tests/test_semantic_equivalence.py
- tests/test_replay_depth_adapter.py
- tests/test_feature_store.py (deleted)
- tests/test_generate_catalog.py (deleted)
- tests/test_generate_catalog_full_l2.py → tests/test_replay_catalog_reconstruct.py (renamed)
- scripts/acceptance_test.py, scripts/deploy_linux_server.sh, scripts/README.md
- systemd/cryptorecorder.env.example, systemd/cryptorecorder-replay-build.service
- systemd/cryptorecorder-feature-build.service, systemd/cryptorecorder-feature-build.timer (both deleted)
- docs/REPO_STRUCTURE.md, docs/IMPLEMENTATION_AUDIT.md, docs/PROJECT_STATUS.md,
  docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
  docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md, docs/AI_WORKFLOW.md,
  docs/README.md
- docs/FEATURE_STORE.md, docs/GENERATE_CATALOG.md (both deleted)
- README.md (root), AGENTS.md, .github/copilot-instructions.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
    docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md,
    docs/AI_WORKFLOW.md, docs/README.md, docs/VALIDATION.md (verified, no
    change needed), INSTALL.md (verified, no change needed)

### Docs updated
- [x] CHANGELOG.md
- [x] README.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/REPO_STRUCTURE.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
    docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md,
    docs/AI_WORKFLOW.md, docs/README.md, docs/IMPLEMENTATION_AUDIT.md

### Status / validation impact
- Validated status changed: no — the previously validated
  `data_raw -> replay_store` contract and the ADAUSDT single-day `full_l2`
  smoke evidence are unchanged and preserved verbatim in
  `docs/PROJECT_STATUS.md` and `docs/IMPLEMENTATION_AUDIT.md`.
- Deferred status changed: no new deferrals added beyond removing the
  feature-store/label-store scope entirely (it is no longer "deferred", it is
  "not this repository's responsibility").
- New claims added: no. This is a scope-narrowing and cleanup change; no new
  validation claims were made. Broader top50/multi-day full_l2 equivalence
  (the `v2.0.0` gate) remains explicitly not claimed.
- Evidence for any new validation claim:
  - n/a — no new validation claims; existing ADAUSDT smoke evidence preserved
    unchanged.

### Tests run
```bash
source .venv/bin/activate && pytest -q
# 227 passed, 3 skipped
```

### Validation CLIs run
```bash
# none required — this change removes/renames modules and rewrites docs; it
# does not alter recorder, converter, or replay-store semantics. The existing
# ADAUSDT full_l2 smoke evidence was not re-run because no code paths it
# exercises were modified (only its call site moved from
# pipeline/generate_catalog.py to validation/replay_catalog_reconstruct.py with
# behavior otherwise unchanged, and this is covered by the passing pytest run
# above, including tests/test_replay_catalog_reconstruct.py and
# tests/test_catalog_equivalence_full_l2.py).
```

### Known limitations / out of scope
- `validation/audit_storage_size.py` still has a generic `--feature-root` CLI
  flag and a `feature_store` component label for measuring arbitrary directory
  sizes. Left unchanged deliberately: it imports nothing from the deleted
  feature-store modules and is a generic size-measurement tool, not a
  feature-store consumer. Renaming its flag was judged out of scope for this
  issue.
- Broader top50/multi-day `full_l2` validation (the `v2.0.0` gate) is still
  pending; not addressed by this change.
- Issue #15 (the superseded `generate_catalog` product-CLI proposal) needs to
  be manually commented on and closed as "not planned" on GitHub — not done as
  part of this local change; requires user confirmation before performing any
  GitHub write action.
- Pushing the `refactor/recorder-replay-only` branch and opening a PR are not
  done as part of this change; both require explicit user confirmation first
  per this repository's operational safety rules.


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
