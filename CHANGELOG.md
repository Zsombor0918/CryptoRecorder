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

It still must **not** introduce a general-purpose consumer catalog-generation
service from the replay store; catalog reconstruction stays validation-only.

### v2.0.0 — validated replay_store full-L2 equivalence
The `v2.0.0` release is reserved for one thing: the internal
**replay_store full-L2 reconstruction path** (`validation/replay_catalog_reconstruct.py`,
exercised via `validation/validate_catalog_equivalence.py`) being **validated for
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

### Changed
- **`systemd/cryptorecorder-replay-build.service` — `TimeoutStartSec` raised
  from `3600` (1 hour) to `infinity`** — the replay-build `oneshot` unit no
  longer has a systemd-imposed maximum runtime. A finite `TimeoutStartSec`
  risked systemd sending `SIGTERM`/`SIGKILL` to an in-progress, otherwise-
  healthy build (e.g. a `--force` rebuild, a large backfill across many
  missing days, or a full top50-universe run) purely because it exceeded 1
  hour of wall-clock time. `StartLimitIntervalSec=86400` / `StartLimitBurst=3`
  in `[Unit]` are unchanged and still cap *restart* attempts if `Restart` is
  ever re-enabled; `Restart=no` is unchanged. See
  [docs/OPERATIONS.md](docs/OPERATIONS.md) "Replay-build memory and restart
  behaviour" for the updated "Start timeout" note.

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
