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

## 2026-07-22 — Fix spool lifetime, atomic publication, force-rebuild, stale docs (PR #18)

### Change summary
- `stores/replay_writer.py`: spool files moved from system temp to
  `staging_dir/scratch/` — stale staging cleanup now also removes orphaned
  SQLite spools; `_spool_temp_dir` removed from constructor (no longer
  configurable separately — spools are always co-located with staging).
- `stores/replay_writer.py`: `publish()` now does a backup/restore atomic
  swap — renames existing valid partition to `.backup_{date}_{symbol}` before
  `os.replace(staging→output)`, restores backup on failure; the previously
  valid partition can no longer be lost by a failed rename.
- `pipeline/build_replay_store.py`: added `force` kwarg and `--force` CLI
  flag; skip-if-valid respects `force=True`; documents the provenance contract
  (without `--force`, output integrity is validated, raw inputs are not).
- `tests/test_replay_memory_bounded.py`: 4 new tests — spool-in-staging, stale
  staging removes spools, backup/restore on replace error, force-rebuild.
- `docs/OPERATIONS.md`: fixed stale `crypto-recorder` unit name in quick-
  reference commands → `cryptorecorder-recorder`.
- `docs/IMPLEMENTATION_AUDIT.md`: removed stale feature-store audit content
  from active `Smoke-Tested` section (feature-store was removed in issue #17).
- `docs/CHANGE_AUDIT.md`: updated previous entry's Docs-updated section.
- `CHANGELOG.md`: added `[Unreleased]` section for P1/P2 fixes.
- Real-data RAM test: BTCUSDT 2026-06-12 (509 MB raw) — pending `/usr/bin/time
  -v` peak RSS output.

### Files/packages touched
- `stores/replay_writer.py`
- `pipeline/build_replay_store.py`
- `tests/test_replay_memory_bounded.py`
- `docs/OPERATIONS.md`
- `docs/IMPLEMENTATION_AUDIT.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] docs/REPLAY_STORE.md, docs/OPERATIONS.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no public interface change; not required
- [ ] docs/PROJECT_STATUS.md — no new validated/deferred status change
- [ ] docs/REPO_STRUCTURE.md — no new folders/files
- [x] docs/OPERATIONS.md — fixed stale unit name in quick-reference
- [x] docs/IMPLEMENTATION_AUDIT.md — removed stale feature-store smoke content

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```
pytest tests/test_replay_memory_bounded.py   # 21 passed (incl. 4 new tests)
pytest -q                                    # 304 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   (pending — run after commit)
Real-data BTCUSDT 2026-06-12 RAM test:
  BINANCE_SPOT:  835,403 depth + 3,112,086 trades
  BINANCE_USDTF: 563,875 depth + 3,200,399 trades
  Maximum RSS:   855,432 kB (~835 MiB) — well under 12 GiB systemd MemoryMax
  Exit status:   0
```

### Known limitations / out of scope
- `REPLAY_SPOOL_TEMP_DIR` config in `config.py` and `cryptorecorder.env.example`
  is now dead (spools always use staging/scratch). Will be removed in a
  follow-up cleanup.
- Production DEXEUSDT 2026-07-21 test still requires the production server.
- uv migration (issue #20) excluded.

---

## 2026-07-22 — Memory-bounded replay-store builder (PR #18)

### Change summary
- `stores/replay_writer.py`: replaced unbounded `depth_batches`/`trade_batches`
  Python-list accumulation with disk-backed SQLite spooling via
  `converter.spool.RawRecordSpool`; incremental Parquet writing via
  `pyarrow.parquet.ParquetWriter`; added `cleanup_staging()` method
- `pipeline/build_replay_store.py`: import `REPLAY_SPOOL_TEMP_DIR`; pass
  `spool_temp_dir` to `ReplayWriter`; added `_partition_is_valid()` helper with
  checksum validation; skip-if-valid logic; stale staging removal;
  `cleanup_staging()` on error
- `pipeline/daily_build.py`: track `skipped` partitions; treat skipped-valid as
  success; update log and return dict
- `config.py`: added `REPLAY_SPOOL_TEMP_DIR` (optional, env-controlled)
- `systemd/cryptorecorder-replay-build.service`: `Restart=on-failure` →
  `Restart=no`; `StartLimitIntervalSec=86400` / `StartLimitBurst=3` in `[Unit]`
- `systemd/cryptorecorder.env.example`: documented `CRYPTO_RECORDER_REPLAY_SPOOL_TEMP_DIR`
- `tests/test_replay_memory_bounded.py`: new file, 17 regression tests
- `CHANGELOG.md`: updated `[Unreleased]`

### Files/packages touched
- `stores/replay_writer.py`
- `pipeline/build_replay_store.py`
- `pipeline/daily_build.py`
- `config.py`
- `systemd/cryptorecorder-replay-build.service`
- `systemd/cryptorecorder.env.example`
- `tests/test_replay_memory_bounded.py`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`
- `docs/REPLAY_STORE.md`
- `docs/OPERATIONS.md`
- `docs/PROJECT_STATUS.md`
- `INSTALL.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs: docs/REPLAY_STORE.md, docs/OPERATIONS.md,
      docs/FULL_L2_REPLAY_CATALOG_PLAN.md, docs/VALIDATION.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no public interface change; not required
- [x] docs/PROJECT_STATUS.md — updated replay_store v0 bullet to reflect memory-bounded writer and fixed restart policy; noted production RAM measurement still pending
- [ ] docs/REPO_STRUCTURE.md — no new folders/files; not required
- [x] docs/REPLAY_STORE.md — removed stale "v0 write limitation" bullet; replaced "Future optimization" note with implemented-solution description; updated Processing Details section
- [x] docs/OPERATIONS.md — added "Replay-build memory and restart behaviour" section documenting bounded writes, spool temp dir, `Restart=no`, durable forward progress, and recovery commands
- [x] INSTALL.md — removed stale `legacy-converter` target from deploy command reference

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```
pytest tests/test_replay_memory_bounded.py         # 17 passed
pytest tests/test_replay_store.py                  # 3 passed
pytest tests/test_streaming_conversion_memory.py   # 10 passed
pytest tests/test_daily_build.py                   # 9 passed
pytest tests/test_agent_infrastructure.py          # passed
pytest tests/test_repo_structure.py                # passed
pytest -q                                          # 300 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   # PASS
bash -n scripts/deploy_linux_server.sh                     # OK
systemd-analyze verify systemd/cryptorecorder-replay-build.service
  # expected path-only warning on dev machine (no /home/zsom)
```

### Known limitations / out of scope
- Real-data DEXEUSDT 2026-07-21 test not run — production raw data unavailable
  on development machine. Required command documented in CHANGELOG.md.
- uv migration (issue #20) explicitly excluded.
- No changes to recorder.py, phase2_depth.py, native_trades.py, storage.py,
  raw schemas, replay-store v0 schema, or existing production data.

---



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

## 2026-07-21 — Deployment boundary: converter removed from automated production systemd path (issue #17 follow-up)

### Change summary
- `scripts/deploy_linux_server.sh`: removed `legacy-converter` from
  `VALID_TARGETS` (it is no longer a deployable `--target`); `--target all`
  now only installs/controls `cryptorecorder-recorder.service` and
  `cryptorecorder-replay-build.{service,timer}`. Removed the now-dead
  `legacy-converter)` cases from `units_for_target()`/`control_for_target()`,
  and dropped it from `selected_targets()`'s `all` expansion.
- Added `cryptorecorder-convert.service` and `cryptorecorder-convert.timer`
  to the `cleanup_stale_units()` `STALE_UNITS` list, so any copy already
  installed on an existing server is stopped/disabled/removed automatically
  on the next deploy, matching how the pre-issue-#17 feature-build units are
  already handled.
- Marked `systemd/cryptorecorder-convert.service` and `.timer` as
  manual/reference-only templates via an in-file comment (not rendered or
  installed by the deploy script for any target); the files themselves were
  **not** deleted.
- `docs/OPERATIONS.md`: updated the "Targets" table, "Safety notes" stale-unit
  list, and "Service groups"/ordering text in the Linux Server Layout section
  to remove `legacy-converter` and correct the "daily chain runs convert →
  replay" claim (`replay-build` reads directly from `data_raw` via
  `pipeline.raw_manifest` and never depended on converter output — there was
  no real ordering dependency to begin with).
- `docs/IMPLEMENTATION_AUDIT.md`: added a new "Completed Cleanup Items
  (2026-07-21 — deployment boundary...)" entry documenting this change.
- `CHANGELOG.md`: added a new `[Unreleased]` `### Changed (PR #18 —
  deployment boundary...)` section.
- `tests/test_agent_infrastructure.py`: removed `legacy-converter` from
  `DEPLOY_TARGETS`; added `cryptorecorder-convert.timer`/`.service` to
  `LEGACY_STALE_UNITS`; added two new regression tests —
  `test_deploy_script_rejects_legacy_converter_target` (asserts
  `--target legacy-converter` now fails like any unknown target) and
  `test_deploy_script_all_target_never_installs_converter` (asserts
  `--target all --dry-run` output never mentions `cryptorecorder-convert`).

### Files/packages touched
- scripts/deploy_linux_server.sh
- systemd/cryptorecorder-convert.service
- systemd/cryptorecorder-convert.timer
- docs/OPERATIONS.md
- docs/IMPLEMENTATION_AUDIT.md
- CHANGELOG.md
- tests/test_agent_infrastructure.py

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md (Deployment Script Reference, Linux Server Layout)

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; deployment-path detail, not a user-facing feature description
- [ ] docs/PROJECT_STATUS.md — no change needed; this is a deployment-boundary change, not a validated/deferred data-path status change
- [ ] docs/REPO_STRUCTURE.md — no change needed; no top-level folder added/removed, `systemd/` package purpose text is unchanged
- [x] relevant feature docs:
  - docs/OPERATIONS.md
  - docs/IMPLEMENTATION_AUDIT.md

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this narrows the automated production deployment surface; it does not change the validated/deferred status of `convert_day.py`, the full_l2 replay path, or any data artifact
- Evidence for any new validation claim: n/a

### Tests run
```bash
source .venv/bin/activate
pytest tests/test_agent_infrastructure.py -q   # 28 passed
pytest -q                                       # 283 passed, 3 skipped
```

### Validation CLIs run
```bash
bash scripts/deploy_linux_server.sh --target all --dry-run --no-systemd
# confirms units: cryptorecorder-recorder.service cryptorecorder-replay-build.service cryptorecorder-replay-build.timer
bash scripts/deploy_linux_server.sh --target legacy-converter --dry-run --no-systemd
# confirms exit 1: invalid --target 'legacy-converter' (expected: all recorder replay-build)
python -m validation.audit_change_compliance --base main
```

### Known limitations / out of scope
- No converter/reconstruction Python code was removed or modified:
  `convert_day.py`, `converter/`, and `validation/replay_catalog_reconstruct.py`
  remain fully in place and required for replay building, validation, and
  local test-computer catalog reconstruction.
- The `systemd/cryptorecorder-convert.{service,timer}` unit-file templates
  were kept in the repo (marked manual/reference-only) rather than deleted;
  deleting them was judged out of scope since the task only required removing
  the converter from the *active* deployment path, not the reference templates.
- This change was not tested against a real production server (no `sudo`/
  real systemd actions were run); only `--dry-run --no-systemd` was exercised,
  consistent with this being a WSL/dev sandbox, not the production host.
- Full_l2 broader top50/multi-day validation remains deferred, unaffected by
  this change (no data-path code was touched).
- Merge remains deferred; this change is pushed to
  `refactor/recorder-replay-only` only, per explicit instruction.

---

---
## 2026-07-21 — PR #18 third review round: exchangeinfo-only no_data classification, disk-report timestamp consistency, stale changelog claims (issues #17, #19)

### Change summary
- `pipeline/daily_build.py` — `run_build_replay_store()` now filters eligible
  venue/symbol partitions by actual raw channel coverage (`depth_v2`/`trade_v2`
  in `ELIGIBLE_REPLAY_CHANNELS`) instead of assuming every raw-manifest
  "symbol" entry is a market symbol. A date containing only an `exchangeinfo`
  partition (e.g. `data_raw/<venue>/exchangeinfo/EXCHANGEINFO/<date>/`) now
  attempts zero replay partitions and reports `no_data`, never `failed`.
  `EXCHANGEINFO` can never become an attempted replay symbol even if a caller
  explicitly passes `--symbols EXCHANGEINFO`, because filtering is based on
  channel coverage, not a literal symbol-name exclusion — so other future
  non-market metadata channels are protected the same way.
- `disk_monitor.py` — `_check_disk_usage_locked()`'s top-level `"timestamp"`
  field now uses `time_utils.local_now_iso()` (Europe/Budapest) instead of a
  bare UTC `now.isoformat()`, matching the already-local-time skipped/overlap
  path and the documented `docs/OPERATIONS.md` contract. Internal
  `measured_at`, growth-history epoch ordering, and measurement-age/staleness
  calculations are untouched and remain UTC/epoch-based.
- `docs/OPERATIONS.md` — added an explicit `timestamp` row to the
  `disk_usage.json` fields table clarifying the Europe/Budapest top-level
  contract and that internal growth calculations stay UTC/epoch-based.
- `CHANGELOG.md` — the two pre-issue-#17 `[Unreleased]` "Changed" blocks that
  described `pipeline/generate_catalog.py --profile full_l2` and
  `docs/GENERATE_CATALOG.md`/`docs/FEATURE_STORE.md` as if still active are
  now explicitly headed "(historical — ... superseded)" with an inline note
  stating the CLI and both doc files were later removed by issue #17 and do
  not exist today. No history was deleted; only the currently-active-state
  framing was corrected.
- Tests: `tests/test_daily_build.py` (4 new: exchangeinfo-only → `no_data`,
  exchangeinfo + one valid symbol → only the valid symbol attempted, explicit
  `--symbols EXCHANGEINFO` filtering still yields `no_data`, main() exits
  nonzero); `tests/test_disk_monitor_fail_safe.py` (3 new: normal report
  timestamp carries the Europe/Budapest offset, the overlapping/no-prior path
  carries the same offset, the timestamp change does not alter
  growth/measurement-age logic); `tests/test_agent_infrastructure.py` (1 new:
  `[Unreleased]` may not present the removed `generate_catalog` CLI or the
  deleted feature/catalog docs as currently available outside a
  historical/removed context).

### Files/packages touched
- pipeline/daily_build.py
- disk_monitor.py
- docs/OPERATIONS.md
- CHANGELOG.md
- tests/test_daily_build.py
- tests/test_disk_monitor_fail_safe.py
- tests/test_agent_infrastructure.py
- docs/CHANGE_AUDIT.md (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/DAILY_BUILD_PIPELINE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no user-facing behavior/API change
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change; these are
  bugfixes to already-documented statuses (`no_data` classification,
  timestamp contract), not new capability claims
- [ ] docs/REPO_STRUCTURE.md — no structural change
- [x] relevant feature docs:
  - docs/OPERATIONS.md (added `timestamp` field row to the `disk_usage.json` table)

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry corrects a stale-claim framing bug in
  `CHANGELOG.md`, it does not add or remove any validated/deferred capability
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_daily_build.py -q                                    # 9 passed
pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q  # 36 passed
pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q  # 49 passed
pytest -q                                                               # 282 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # RESULT: PASS
```

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence remains deferred (unchanged
  by this entry).
- No production data, services, or `/etc/cryptorecorder/cryptorecorder.env`
  were touched; all tests use `tmp_path`-scoped temporary roots.
- The ADAUSDT replay-equivalence smoke was not re-run as part of this change
  (no code path it depends on — `convert_day.py`, replay writer/reader
  schemas, catalog reconstruction — was touched); see final report for the
  smoke-availability statement.

---
## 2026-07-20 — PR #18 second review round: fail-closed disk monitor, data_raw-only retention accounting, daily_build failed status, deploy-script legacy cleanup + honest flags, stale doc references (issues #17, #19)

### Change summary
- `disk_monitor.py` — `check_disk_usage()` now forces
  `retention_measurement_trustworthy=False` whenever a scan is skipped due to
  an overlapping scan already in progress (`skipped_duplicate=True`), even if
  the previous cycle's own report was trustworthy; adds a `WARNING`/`ERROR`
  alert and downgrades `monitoring_health` to at least `degraded`.
  `cleanup_old_data()` now explicitly refuses to act (`return False`)
  whenever the current cycle's report has `skipped_duplicate=True`.
- `disk_monitor.py` — `percent_of_soft_limit`, `percent_of_hard_limit`,
  `growth_rate_gb_day`, and `days_to_full` are now all `null` whenever the
  current cycle's `data_raw` measurement is not itself fresh and successful
  (never derived from the persisted last-known-good fallback).
- `disk_monitor.py` — soft/hard-limit and cleanup-target comparisons,
  `percent_of_soft_limit`/`percent_of_hard_limit`, and growth-rate/
  `days_to_full` are now derived exclusively from fresh `data_raw` usage
  (`data_raw_gb_for_retention`), never from `total_gb` (the cross-root
  `data_raw + catalog + meta + state` observability sum, which may span
  different filesystems). `GrowthSample.total_bytes` renamed to
  `GrowthSample.data_raw_bytes` throughout the module and its tests.
- `pipeline/daily_build.py` — `run_build_replay_store()` now reports
  `"failed"` (distinct from `"partial"` and `"no_data"`) when one or more
  venue/symbol partitions were attempted and *none* succeeded.
  `generate_daily_report()` propagates `"failed"` distinctly rather than
  collapsing it into `"partial"`.
- `scripts/deploy_linux_server.sh` — `cleanup_stale_units()` now removes
  every legacy/renamed unit name this repo has ever shipped
  (`crypto-recorder.service`, `nautilus-convert.{service,timer}`,
  `cryptorecorder-daily-build.{service,timer}`, in addition to the existing
  `cryptorecorder-feature-build.{service,timer}`), runs for every `--target`,
  and now runs before `install_units`. `--user`/`--app-dir`/`--env-file` are
  now rendered into each installed unit file via `sed`
  (`User=`/`Group=`/`WorkingDirectory=`/`ExecStart=`/`EnvironmentFile=`), and
  `--data-root` is rendered into a newly created env file's
  `CRYPTO_RECORDER_*_ROOT` values (an existing env file is still never
  overwritten).
- Corrected stale systemd unit name references: `docs/DAILY_BUILD_PIPELINE.md`
  (11 occurrences of `cryptorecorder-daily-build.{service,timer}` -> the real
  `cryptorecorder-replay-build.{service,timer}`), `docs/ARCHITECTURE.md` (2
  occurrences, same rename), `INSTALL.md` (`nautilus-convert.{service,timer}`
  -> the real `cryptorecorder-convert.{service,timer}` in Troubleshooting).
- Corrected stale "tracked retention usage (data_raw + catalog + meta +
  state)" comments to reflect data_raw-only retention semantics in
  `config.py`, `systemd/cryptorecorder.env.example`, `docs/OPERATIONS.md`,
  and `INSTALL.md`.
- `docs/ARCHITECTURE.md` and `docs/DAILY_BUILD_PIPELINE.md` now document all
  four `daily_build` report statuses (`success`/`partial`/`failed`/`no_data`).
- `docs/OPERATIONS.md`'s Deployment Script Reference updated to describe the
  rendering behavior of `--user`/`--app-dir`/`--data-root`/`--env-file` and
  the expanded stale-unit cleanup list.

### Files/packages touched
- disk_monitor.py
- tests/test_disk_monitor_fail_safe.py
- pipeline/daily_build.py
- tests/test_daily_build.py
- scripts/deploy_linux_server.sh
- tests/test_agent_infrastructure.py
- config.py
- systemd/cryptorecorder.env.example
- docs/OPERATIONS.md
- docs/ARCHITECTURE.md
- docs/DAILY_BUILD_PIPELINE.md
- INSTALL.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md,
    INSTALL.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no stale references found in this file
- [ ] docs/PROJECT_STATUS.md — no status/claim change; full_l2 top50/multi-day
  validation remains pending as before
- [ ] docs/REPO_STRUCTURE.md — no structural change (no files added/removed)
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md,
    INSTALL.md, systemd/cryptorecorder.env.example

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry fixes fail-open/fail-closed edge cases,
  accounting scope, deployment honesty, and stale references; it does not
  change what is validated vs deferred (full_l2 top50/multi-day validation
  remains pending, as before)
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest -q
# 274 passed, 3 skipped

pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q
# 33 passed

pytest tests/test_daily_build.py -q
# 5 passed

pytest tests/test_agent_infrastructure.py -q
# 26 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main
# RESULT: PASS (64 changed files vs main)

python -m pipeline.build_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/cr_smoke_replay
# Built replay: BINANCE_SPOT/ADAUSDT/2026-06-12 (412336 depth, 124457 trades)
# Built replay: BINANCE_USDTF/ADAUSDT/2026-06-12 (442834 depth, 401883 trades)
# Replay build complete: 2 successful, 0 failed, 855170 depth, 526340 trades

python -m validation.audit_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --venues BINANCE_SPOT,BINANCE_USDTF --replay-root /tmp/cr_smoke_replay
# depth/trades parquet for both venues: sorted=true, 0 duplicate sequence
# keys, schema OK, no errors

python -m pipeline.daily_build --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/cr_smoke_replay \
  --report-root /tmp/cr_daily_reports
# status=success, runtime=38.7s, 2/2 symbols, 855170 depth, 526340 trades

bash scripts/deploy_linux_server.sh --target all --dry-run --user customuser \
  --app-dir /opt/customdir --data-root /srv/customdata \
  --env-file /etc/customenv/cr.env
# confirmed all 4 custom values appear in the rendered dry-run plan and none
# of the hardcoded defaults leak through
```
All commands ran against temporary roots (`/tmp/...`) using the existing
local `./data_raw` fixture; no production data, `/etc` files, or running
services were touched. Temp directories were removed after the run.

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence validation remains pending
  (unchanged from before this entry); the `v2.0.0` gate is still not declared.
- No live systemd install/enable/start was performed (out of scope — dry-run
  only, no root/sudo access in this environment).
- The migration/cleanup test for the deploy script's `cleanup_stale_units()`
  verifies the unit-name list and rendered-flag behavior via source
  inspection and `--dry-run` output, not a live `/etc/systemd/system`
  install, since this environment has no sudo/systemd access.

---
## 2026-07-20 — complete PR #18 remaining work: strip feature-store residue, harden structure tests, fix daily_build false-success, correct systemd/doc references (issues #17, #19)

### Change summary
- Merged current `main` into `refactor/recorder-replay-only` (via cherry-pick of
  commit `9c639b8` from `fix/disk-monitor-fail-safe-measurement`, completed as
  `45356f9`), resolving conflicts in `CHANGELOG.md`, `docs/PROJECT_STATUS.md`, and
  `docs/CHANGE_AUDIT.md` by hand.
- Stripped feature-store naming from `validation/audit_storage_size.py`: removed
  the `feature_root` parameter, its `feature_store` report component, and the
  `--feature-root` CLI flag (the feature-store subsystem no longer exists).
- Deleted `docs/GUARANTEES.md` — fully superseded by the existing
  "System Guarantees" section in `docs/ARCHITECTURE.md`; no unique content lost.
- Deleted root-level `inspect_catalog.py` — dead code from a stale merge, not
  imported anywhere, with a docstring referencing a nonexistent `validators/`
  package. `validation/catalog_inspect.py` is the real, currently-used CLI.
- Reverted `validate.py` to its working form: the `main`-branch merge had
  regressed it to import unused `cryptofeed`/`yaml` dependencies, reference a
  nonexistent `converter.book` module, and hardcode paths instead of using
  `config.py`'s configurable `DATA_ROOT`/`META_ROOT`/`STATE_ROOT`.
- Hardened `tests/test_repo_structure.py` with 7 new tests enforcing the exact
  root Python/other file sets and exact `docs/` file set from
  `docs/REPO_STRUCTURE.md`, absence of stray Python files in `docs/`, absence of
  feature-store config roots/CLI flags/systemd units, and absence of
  `validators` imports. Updated `docs/REPO_STRUCTURE.md`'s Root-Level Files
  table to list every real root `.py` module (several were previously missing).
- Fixed `pipeline/daily_build.py`'s false-success bug: `run_build_replay_store()`
  now reports `"no_data"` (distinct from `"success"`) when zero raw partitions
  were eligible for the date, instead of falsely reporting `"success"`.
  `generate_daily_report()` checks `"no_data"` explicitly before the generic
  `"partial"` fallback. `main()` now logs a warning and returns nonzero for any
  non-`"success"` status. Added `tests/test_daily_build.py` (4 new tests) and
  updated `docs/DAILY_BUILD_PIPELINE.md`'s status-semantics documentation.
- Deleted stale duplicate systemd unit files superseded by the units actually
  referenced by `scripts/deploy_linux_server.sh`: `systemd/crypto-recorder.service`
  (superseded by `cryptorecorder-recorder.service`),
  `systemd/nautilus-convert.{service,timer}` (superseded by
  `cryptorecorder-convert.{service,timer}`), and
  `systemd/cryptorecorder-daily-build.{service,timer}` (superseded by
  `cryptorecorder-replay-build.{service,timer}`).
- Corrected numerous stale documentation references: `INSTALL.md`'s
  `crypto-recorder.service`/`nautilus-convert.*` unit names and a duplicate
  `## 10.` heading; `AGENTS.md`'s and `docs/OPERATIONS.md`'s broken
  self-referential "merged from the former `OPERATIONS.md`" provenance notes
  and a broken same-file link; similar self-referential provenance notes in
  `docs/ARCHITECTURE.md` and `docs/IMPLEMENTATION_AUDIT.md`; a stale
  `pipeline/audit_replay_store.py` table row in `docs/ARCHITECTURE.md` (real
  path is `validation/audit_replay_store.py`); a duplicate
  `[OPERATIONS.md](OPERATIONS.md)` link in `docs/PROJECT_STATUS.md`; a
  misleading `--date today` example in `docs/DAILY_BUILD_PIPELINE.md` (only
  `YYYY-MM-DD` and `yesterday` are implemented); and added a new
  "Replay Store Validation" section to `docs/VALIDATION.md` documenting
  `validation.audit_replay_store`, `validation.validate_catalog_equivalence`,
  and `validation.audit_change_compliance` (previously undocumented there).

### Files/packages touched
- validation/audit_storage_size.py
- docs/GUARANTEES.md (deleted)
- inspect_catalog.py (deleted)
- validate.py
- tests/test_repo_structure.py
- docs/REPO_STRUCTURE.md
- pipeline/daily_build.py
- tests/test_daily_build.py (new)
- docs/DAILY_BUILD_PIPELINE.md
- systemd/crypto-recorder.service (deleted)
- systemd/nautilus-convert.service (deleted)
- systemd/nautilus-convert.timer (deleted)
- systemd/cryptorecorder-daily-build.service (deleted)
- systemd/cryptorecorder-daily-build.timer (deleted)
- systemd/cryptorecorder-recorder.service
- systemd/cryptorecorder.env.example
- scripts/deploy_linux_server.sh
- INSTALL.md
- AGENTS.md
- docs/OPERATIONS.md
- docs/ARCHITECTURE.md
- docs/IMPLEMENTATION_AUDIT.md
- docs/PROJECT_STATUS.md
- docs/VALIDATION.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/DAILY_BUILD_PIPELINE.md, docs/VALIDATION.md, docs/OPERATIONS.md,
    docs/ARCHITECTURE.md, INSTALL.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no stale references found in this file
- [x] docs/PROJECT_STATUS.md — fixed duplicate link and stale "Date" header
- [x] docs/REPO_STRUCTURE.md — root-file table completed; amendment log entry added
- [x] relevant feature docs:
  - docs/DAILY_BUILD_PIPELINE.md, docs/VALIDATION.md, docs/OPERATIONS.md,
    docs/ARCHITECTURE.md, docs/IMPLEMENTATION_AUDIT.md, INSTALL.md, AGENTS.md

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry fixes structural/doc/test defects and stale
  references; it does not change what is validated vs deferred (full_l2
  top50/multi-day validation remains pending, as before)
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest -q
# 267 passed, 3 skipped

pytest tests/test_repo_structure.py tests/test_replay_store.py \
  tests/test_pipeline_validation.py tests/test_agent_infrastructure.py -q
# 22 passed (test_repo_structure.py); 36 passed, 1 skipped (combined others)

pytest tests/test_daily_build.py -q
# 4 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main
# RESULT: PASS (52 changed files vs main)

python -m pipeline.build_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/tmp.fRQ8vNOyNf/replay_store
# Built replay: BINANCE_SPOT/ADAUSDT/2026-06-12 (412336 depth, 124457 trades)
# Built replay: BINANCE_USDTF/ADAUSDT/2026-06-12 (442834 depth, 401883 trades)
# Replay build complete: 2 successful, 0 failed

python -m validation.audit_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --venues BINANCE_SPOT --replay-root /tmp/tmp.fRQ8vNOyNf/replay_store
# depth.parquet: 412336 rows, sorted=true, 0 duplicate sequence keys, schema OK
# trades.parquet: 124457 rows, sorted=true, 0 duplicate sequence keys, schema OK
```
All commands ran against temporary roots (`/tmp/...`) using the existing local
`./data_raw` fixture; no production data, `/etc` files, or running services
were touched.

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence validation remains pending
  (unchanged from before this entry); the `v2.0.0` gate is still not declared.
- No live systemd install/enable/start was performed (out of scope — this is a
  documentation/reference correction pass, not a deployment).
- The `full_l2` semantic-equivalence smoke re-run (`convert_day.py` vs
  `validate_catalog_equivalence --profile full_l2`) was not re-executed in this
  session; the existing ADAUSDT smoke evidence in `docs/PROJECT_STATUS.md` and
  `docs/IMPLEMENTATION_AUDIT.md` is unchanged and still applies.

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
