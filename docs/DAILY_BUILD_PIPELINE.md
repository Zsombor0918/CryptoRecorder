# Daily Replay Build Pipeline

## Supported contract

`pipeline.daily_build` is the replay-only production orchestration boundary.
It scans a bounded inclusive date lookback, reconciles replay lifecycle state,
processes dates oldest-first, builds at most a configured number of incomplete
dates, and writes atomic per-date and invocation reports. It never builds a
feature store or persistent Nautilus catalog.

Its raw exchangeInfo parsing and compact fixed-point scale derivation are
dependency-free production operations. Nautilus remains confined to explicit
reconstruction/converter environments and is not required by this pipeline.

The intended repository service invocation is explicit:

```bash
python -m pipeline.daily_build \
  --date yesterday \
  --backlog-days 7 \
  --max-build-dates 3 \
  --schema-version 2
```

`--date` is the newest date. `yesterday` means the previous completed UTC
date. `--backlog-days` is an inclusive lookback bounded to 1–31 days;
`--max-build-dates` is bounded to 1–31. Defaults come from
`CRYPTO_RECORDER_REPLAY_BACKLOG_DAYS=7`,
`CRYPTO_RECORDER_REPLAY_MAX_BUILD_DATES=3`, and
`CRYPTO_RECORDER_REPLAY_SCHEMA_VERSION=2`. The schema environment value is
validated as exactly 2 for production configuration; historical validation
callers may still explicitly request schema 0 or 1 on the direct builder.

Optional arguments:

- `--venues VENUE,...` restricts each inspected date to explicit venues.
- `--symbols SYMBOL,...` restricts each inspected date to explicit symbols.
- `--data-root`, `--replay-root`, and `--report-root` select isolated roots.
- `--rebuild-source-changed` permits replacement only when an otherwise-valid
  selected partition's live raw source identity changed.
- `--replace-incompatible` permits replacement only when a selected valid
  partition uses a legacy/incompatible schema or builder contract.

The scheduled service passes neither replacement flag. It never silently
migrates legacy partitions or overwrites source-changed data.

## Exclusive ownership

Every supported replay mutation boundary uses the common Linux advisory lock:

```text
<replay-root>/.lifecycle/build.lock
```

`fcntl.flock(LOCK_EX | LOCK_NB)` is the authority. A second invocation exits
nonzero with `build already active`; a PID/timestamp file is never used to
break ownership, and kernel process-death release is authoritative. The lock
path must be a single-link, non-group/world-writable regular file owned by the
current user. Symlinks, unsafe ownership, or an unsafe lifecycle directory
fail closed.

After acquiring the lock, the process records contract version, run ID, PID,
hostname, command, start UTC, repository SHA, and exact data/replay/report
roots. Nested orchestration receives the already-held lifecycle context and
does not reacquire the lock.

## Cross-date recovery

Before raw/backlog inspection, one bounded scan covers the configured replay
root across every date. It recognizes only canonical `date=YYYY-MM-DD`
partitions and canonical `.staging_*`, `.backup_*`, `.quarantine_*`, and
`.lifecycle` artifacts. It never follows symlinks.

- A valid canonical partition is authoritative. A single obsolete valid
  backup is removed only through this safe cleanup path.
- When canonical output is missing/invalid and one valid backup exists, an
  invalid canonical is moved to a unique quarantine path and the backup is
  restored atomically.
- Stale staging is moved to a unique quarantine path after the global lock
  proves no active builder owns it.
- Quarantine is always preserved and reported; it is never automatically
  deleted.
- Multiple candidates, an invalid backup, an invalid canonical with no valid
  backup, unknown entries, symlinks, or conflicting names fail the entire run.

The scan is bounded by `CRYPTO_RECORDER_REPLAY_RECOVERY_MAX_ENTRIES` (20,000)
and `CRYPTO_RECORDER_REPLAY_RECOVERY_MAX_ACTIONS` (2,000). An interrupted
rename leaves an explicit state the next locked run can reconcile.

The per-partition writer retains its existing same-parent staging and
backup/restore publication design. Publication refuses a pre-existing backup,
fsyncs manifest/evidence before rename, validates the new canonical before
removing the old valid backup, and assigns every invalid output a unique
non-overwriting quarantine path.

## Backlog and outcomes

Each date derives its eligible venue/symbol inventory from current raw and
replay artifacts. Both target-day `depth_v2` and `trade_v2` are required.
Dates are inspected oldest to newest. Valid dates are skipped without consuming
a build-date slot. An incomplete date consumes one slot only when a partition
is actually built; later incomplete dates remain reported and rediscoverable.
No unbounded queue is persisted.

Every partition has exactly one outcome:

| Outcome | Meaning |
|---|---|
| `built` | constructed and post-publication routine-valid |
| `skipped_valid` | existing schema/source/checksum-valid partition reused |
| `deferred_not_ready` | adjacent-depth readiness or build-date bound not satisfied |
| `missing_required_raw` | target depth or trade channel is absent |
| `source_changed_rebuild_required` | live source identity differs; explicit policy absent |
| `incompatible_schema_rebuild_required` | schema/builder contract differs; explicit policy absent |
| `recovered` | valid backup restored during this locked invocation |
| `failed` | corrupt/ambiguous/build/validation/reporting failure |

Only a nonempty scope in which every eligible partition finishes as `built`,
`skipped_valid`, or `recovered` exits zero. Deferred, missing, source-changed,
incompatible, corrupt, empty, and reporting-failure states are nonzero.

## Source and schema policy

For schema 2, reuse requires the complete replay manifest and files to pass
routine validation, current builder identity to match, and a new strict raw
source-identity scan to equal the stored manifest identity.

- Source change defaults to `source_changed_rebuild_required`; use
  `--rebuild-source-changed` for an intentional exact-partition replacement.
- Legacy/incompatible schema or builder defaults to
  `incompatible_schema_rebuild_required`; use `--replace-incompatible` for an
  intentional exact-partition replacement.
- Corrupt replay is `failed`; neither flag authorizes silently replacing
  corruption.

For an isolated migration, point `--replay-root` and `--report-root` at new
empty external roots, run explicit schema 2, validate the result, then obtain
separate owner approval for any production path change. Do not use a migration
flag against the existing production replay root without that approval.

## Atomic reports

Each inspected date publishes `daily_build_<date>.json`; the invocation
publishes `replay_backlog_<run-id>.json`. Both use same-directory temporary
files, file fsync, `os.replace`, and directory fsync. A write failure makes the
process nonzero.

Reports include contract/run/lock/repository identity; requested/effective
schema; date bounds; roots and replacement policies; recovery actions; dates
inspected/selected; exact eligible inventory; every partition outcome/reason;
separate counts for all outcomes; record counts; allocated/apparent partition
bytes when measurable; staging observations; UTC timing/runtime; final status;
and process-exit classification. Missing, deferred, skipped, incompatible,
source-changed, and failed are never collapsed into `partial`.

## Repository systemd template

`systemd/cryptorecorder-replay-build.service` remains `Type=oneshot` and
`Restart=no`, with `TimeoutStartSec=23h`, `MemoryMax=12G`, and
`MemorySwapMax=0`. Its `ExecStart` explicitly supplies schema 2, seven lookback
days, and at most three build dates. The timer remains at 01:00 UTC.

This is an intended template for the known 16 GiB host. It has not been
installed, run, or production-accepted in this checkpoint. The existing
`/etc/cryptorecorder/cryptorecorder.env` is not modified automatically.
Memory-headroom optimization remains a follow-up after Phase 7's accepted
10 GiB pressure evidence.

## Local isolated smoke

Use fresh paths outside production data:

```bash
SMOKE_ROOT=/external/cryptorecorder-replay-lifecycle-smoke

python -m pipeline.daily_build \
  --date 2026-06-11 \
  --venues BINANCE_SPOT \
  --symbols ADAUSDT \
  --backlog-days 1 \
  --max-build-dates 1 \
  --schema-version 2 \
  --data-root /path/to/read-only/data_raw \
  --replay-root "$SMOKE_ROOT/replay" \
  --report-root "$SMOKE_ROOT/reports"
```

Run the same command a second time: it must report `skipped_valid`, preserve
the same source identity, and leave no staging or backup. For resource
evidence, wrap each invocation independently with the supported cgroup wrapper
and an explicit `12G`/zero-swap scope.

## Later owner-run production acceptance (do not execute during development)

1. Fetch and verify the exact approved commit; confirm the production raw,
   replay, report, and state roots and back up the current env/unit files.
2. Render/diff the repository service and env templates; verify schema 2,
   backlog bounds, `Restart=no`, `TimeoutStartSec=23h`, `MemoryMax=12G`, and
   `MemorySwapMax=0`. Do not enable/start yet.
3. Run a dry-run deployment and a read-only backlog/source/schema inventory.
   Resolve every legacy, source-changed, corrupt, backup, quarantine, or
   capacity finding explicitly.
4. Install the approved unit without starting it. Run one manually observed
   isolated production acceptance with operator-selected exact partitions and
   no replacement flags, then inspect the kernel lock metadata, run/date
   reports, cgroup memory/swap/events, published checksums, and residual
   staging/backup/quarantine.
5. Only after a separate owner decision, enable the timer. Never combine first
   migration/replacement authorization with routine scheduled operation.

No production acceptance or deployment is claimed by this document.

The checkpoint-2 isolated development smoke used only
`BINANCE_SPOT/ADAUSDT/2026-06-11`: the first invocation built 303,293 depth
and 129,824 trade rows, routine/deep validation passed, and the second
invocation returned `skipped_valid`. Its highest peak was 1,196,359,680 bytes
under the 12 GiB/zero-swap wrapper, with no pressure or OOM event. This does
not validate the installed production service.
