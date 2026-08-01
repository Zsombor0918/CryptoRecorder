# Operations

## Quick Reference

| Task | Command |
|------|---------|
| Start recorder | `python recorder.py` |
| Convert a day | `python convert_day.py --date YYYY-MM-DD --staging` |
| Setup validation | `python validate.py` |
| Run tests | `pytest tests/` |
| Smoke test | `python scripts/smoke_test.py` |
| Full acceptance | `python scripts/acceptance_test.py` |

## Service Mode

Systemd units are in `systemd/`.

```bash
# Control recorder service
sudo systemctl start cryptorecorder-recorder
sudo systemctl stop cryptorecorder-recorder
sudo systemctl restart cryptorecorder-recorder
sudo systemctl status cryptorecorder-recorder

# View logs
journalctl -u cryptorecorder-recorder -f
```

## Important Runtime Files

| File | Description |
|------|-------------|
| `state/heartbeat.json` | Live recorder status (architecture=deterministic_native) |
| `state/startup_coverage.json` | Startup symbol coverage |
| `state/convert_reports/YYYY-MM-DD.json` | Conversion reports |
| `state/disk_usage.json` | Disk usage + monitoring-health report (see Disk Monitoring below) |
| `state/disk_monitor_state.json` | Last-known-good measurements + bounded growth history (restart-safe) |
| `recorder.log` | Recorder log file |

Report timestamps use Hungary local time (`Europe/Budapest`).
Day-scoped dates in file names remain UTC.

## Disk Monitoring

`disk_monitor.py` runs every `DISK_CHECK_INTERVAL_SEC` (default 600s) and
writes `state/disk_usage.json`. A failed or unavailable directory-size
measurement is never reported as zero — see the safety invariant in
`docs/ARCHITECTURE.md`.

### `disk_usage.json` fields

| Field | Meaning |
|-------|---------|
| `timestamp` | Top-level, operator-facing report timestamp — always Hungary local time (`Europe/Budapest`, ISO-8601 with offset), for both a normal scan and the skipped/overlapping-scan path. Internal growth-rate/days-to-full calculations are computed from separate, UTC-based epoch values and are unaffected by this field's timezone. |
| `components.<data_raw\|replay_published\|replay_staging\|replay_backups\|replay_quarantine\|replay_metadata\|metadata\|state_reports>.value_gb` | Current or last-known-good allocated size in GB, or `null` if never measured. The retired persistent catalog root is not monitored. |
| `components.<name>.measurement_ok` | `true` only if *this cycle's* scan succeeded |
| `components.<name>.measurement_status` | `ok` / `missing` / `timeout` / `command_error` / `malformed_output` / `error` |
| `components.<name>.stale` | `true` when `value_gb` is a last-known-good fallback, not a fresh measurement |
| `components.<name>.measurement_age_seconds` | Age of the fallback value, or `null` |
| `total_gb`, `percent_of_soft_limit`, `percent_of_hard_limit` | `total_gb` is observability-only and may span devices. Retention percentages use fresh `data_raw` only; neither authorizes deletion. |
| `filesystem_capacity[]` | One entry per actual device (`st_dev`), with roots on that device, free space once, and combined allocated use. Shared raw/replay capacity is never double-counted; separate filesystems remain separate. |
| `replay_artifacts` | staging/backup/quarantine counts and oldest ages; old staging/backups alert, while quarantine remains visible without automatic deletion |
| `capacity_projection` | raw growth, replay growth, their combined rate, and current replay transient pressure; `null` when current replay measurement is unavailable |
| `growth_rate_gb_day`, `days_to_full` | `null` unless the current cycle's raw and replay measurements are fresh and successful, and there is a real, non-stale timestamped history spanning enough time |
| `growth_sample_interval_sec`, `growth_sample_oldest_timestamp`, `growth_sample_newest_timestamp` | Provenance of the growth estimate (based on `data_raw` history only) |
| `monitoring_health` | `healthy` / `degraded` / `unhealthy` |
| `alerts` | List of human-readable alert strings (measurement failure, staleness, low free space, threshold breaches, skipped overlapping scans) |
| `retention_measurement_trustworthy` | Fresh raw measurement indicator. It does not authorize deletion; automatic destructive retirement is disabled. |
| `raw_retention_enabled`, `cleanup_required` | Configuration visibility and capacity/policy alert. `cleanup_required` is not a deletion command. |
| `skipped_duplicate` | `true` if this cycle was skipped because a previous scan was still running; the returned report is the previous cycle's, not a fresh measurement |

### Threshold semantics (kept separate)

| Threshold class | Env var | Applies to |
|---|---|---|
| Raw-retention soft/hard limit | `CRYPTO_RECORDER_DISK_SOFT_LIMIT_GB` / `CRYPTO_RECORDER_DISK_HARD_LIMIT_GB` | fresh `data_raw` policy alerts only; never authorizes cleanup |
| Historical cleanup target | `CRYPTO_RECORDER_DISK_CLEANUP_TARGET_GB` | retained configuration compatibility only; no destructive path consumes it |
| Filesystem free-space warn/critical | `CRYPTO_RECORDER_DISK_FS_FREE_WARN_GB` / `CRYPTO_RECORDER_DISK_FS_FREE_CRITICAL_GB` | raw filesystem free bytes, independent of retention accounting |

Retention, replay-size observability, and filesystem safety stay distinct. No
cross-filesystem total or free-space sum authorizes cleanup.

### Raw retention

The historical single-channel/date `shutil.rmtree` path is removed.
`CRYPTO_RECORDER_RAW_RETENTION_ENABLED=0` is the default. Even if changed to
`1`, current code performs no move or deletion because the required durable
paired depth/trade transaction journal, rollback, and startup recovery are not
implemented. The monitor reports `cleanup_required` instead.

`DiskMonitor.plan_raw_retention()` is proof-only. Under the common replay
lifecycle lock it inventories exact `venue/symbol/UTC-source-date` units,
never exchangeInfo, and checks grace/open-day status, paired nonempty stable
depth/trade directories, compression ambiguity, schema-v2 routine/deep-valid
D-1/D/D+1 replay dependencies, and exact source identities. Passing proof is
still not deletion authorization.

### Other disk-monitor environment knobs

| Env var | Default | Purpose |
|---|---|---|
| `CRYPTO_RECORDER_DISK_SCAN_TIMEOUT_SEC` | `60.0` | Timeout for one recursive `du` scan; a scan that exceeds this reports `status="timeout"`, never zero |
| `CRYPTO_RECORDER_DISK_MEASUREMENT_STALE_AFTER_SEC` | `1800.0` | How long a last-known-good value may be reused before it triggers a staleness alert |
| `CRYPTO_RECORDER_DISK_HISTORY_MAX_SAMPLES` | `288` | Bounded growth-history sample count |
| `CRYPTO_RECORDER_DISK_HISTORY_MAX_AGE_SEC` | `172800.0` | Bounded growth-history max age (48h) |
| `CRYPTO_RECORDER_REPLAY_MONITOR_MAX_ENTRIES` | `250000` | Bound for the single classified replay-tree scan |
| `CRYPTO_RECORDER_REPLAY_TRANSIENT_WARN_AGE_SEC` | `86400` | Old staging/backup alert age |
| `CRYPTO_RECORDER_RAW_RETENTION_ENABLED` | `0` | Destructive cleanup switch; current implementation still refuses mutation |
| `CRYPTO_RECORDER_RAW_RETENTION_DAYS` | `7` | Minimum proof-plan grace period |
| `CRYPTO_RECORDER_RAW_RETENTION_STABLE_AGE_SEC` | `3600` | Minimum unchanged-file age used by the proof-only retention inventory |

## Coverage Terminology


Startup and runtime reporting uses these terms:

- `candidate_pool`: ranked symbols considered for a venue
- `pre_filter_rejected`: symbols rejected before recorder startup
- `selected`: symbols passed from universe selection into startup
- `runtime_dropped`: selected symbols that fail during feed initialization
- `active`: symbols successfully recording

## Failure Handling

- Unsupported symbols are logged and skipped
- Startup continues with surviving symbols
- Futures support is validated via REST exchangeInfo
- Depth sync lifecycle handles reconnects deterministically (desync → resync)

## Writer Backpressure

Recorder storage uses one writer queue per `venue/symbol/channel`.
`depth_v2` is protected first: by default it waits for writer capacity instead
of dropping records. `trade_v2` keeps a bounded timeout and may drop under
sustained pressure.

Important environment knobs:

```bash
CRYPTO_RECORDER_DEPTH_WRITER_QUEUE_MAX_SIZE=20000
CRYPTO_RECORDER_TRADE_WRITER_QUEUE_MAX_SIZE=5000
CRYPTO_RECORDER_WRITER_BATCH_SIZE=1000
CRYPTO_RECORDER_WRITER_FLUSH_INTERVAL_SEC=5.0
CRYPTO_RECORDER_DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC=0
CRYPTO_RECORDER_TRADE_WRITER_ENQUEUE_TIMEOUT_SEC=1.0
CRYPTO_RECORDER_DEPTH_BLOCK_WARN_INTERVAL_SEC=10.0
CRYPTO_RECORDER_DEPTH_BLOCK_ALERT_SEC=30.0
CRYPTO_RECORDER_WRITER_TELEMETRY_LOG_INTERVAL_SEC=60
CRYPTO_RECORDER_WRITER_COMPRESSION_WORKERS=1
CRYPTO_RECORDER_WRITER_COMPRESSION_SHUTDOWN_TIMEOUT_SEC=60.0
```

Use `state/heartbeat.json` → `writer_queue_telemetry` to inspect pressure:

- `top_pressure_writers`: symbols/channels causing queue pressure
- `queue_size` / `queue_high_watermark`: current and peak backlog
- `drop_count`: lossy queue drops, normally trade-side
- `blocked` / `current_block_sec`: depth writers waiting for capacity
- `compression`: queued/active/completed/failed background compression work

Hourly rotation closes old files quickly and queues compression in the
background, so compression should not block the active ingest path. Any
compression failures are left as uncompressed `.jsonl` files and surfaced in
heartbeat telemetry.

## Conversion

```bash
# Convert yesterday UTC using the safe staged publish flow
python convert_day.py --staging

# Convert specific date using the safe staged publish flow
python convert_day.py --date 2026-04-20 --staging

# Direct non-staging conversion writes into the live catalog immediately
python convert_day.py --date 2026-04-20

# Enable optional derived depth10
python convert_day.py --date 2026-04-20 --staging --emit-depth10
```

The converter is memory-bounded for heavy days. Raw trade/depth records are
sorted through temporary SQLite spools, and per-symbol Nautilus outputs are
spooled before catalog writes so the old `ts_init` write order is preserved
without retaining full-day Python lists. This can make conversion slower on
large days, but peak memory should track batch size plus spool overhead instead
of compressed raw input size.

Optional converter temp directory:

```bash
CRYPTO_RECORDER_CONVERTER_TEMP_DIR=/fast/local/tmp
```

For production Depth10 emission, keep the derived snapshot interval at 30s:

```bash
CRYPTO_RECORDER_DERIVED_DEPTH_SNAPSHOT_INTERVAL_SEC=30.0
```

This is a converter/runtime setting; changing converter code or temp location
does not require restarting the live recorder service.

## Validation & Testing

```bash
# Check setup (run on new machine)
python validate.py

# Run unit tests
pytest tests/

# Quick recorder test (3 minutes)
python scripts/smoke_test.py

# Full pipeline test (10 minutes)
python scripts/acceptance_test.py
```

See [VALIDATION.md](VALIDATION.md) for details.


---

## Deployment Script Reference

> Content merged from the former `DEPLOYMENT.md`.

CryptoRecorder is deployed on a Linux server with one wrapper script:

```bash
./scripts/deploy_linux_server.sh
```

The script is a **thin operator wrapper** (no business logic). It prepares the
environment, installs the selected systemd units, and optionally enables/starts them.
The canonical paths and service groups it uses are defined below in the
[Linux Server Layout](#linux-server-layout) section.

## Targets

`--target` selects which service group to act on (default: `all`).

| Target | Service group |
|--------|---------------|
| `all` | every group below (default) |
| `recorder` | `cryptorecorder-recorder.service` |
| `replay-build` | `cryptorecorder-replay-build.service` + `.timer` |

`legacy-converter` is **not** a deployable target. Production automatically runs
only `cryptorecorder-recorder.service` and `cryptorecorder-replay-build.timer`;
the converter (`convert_day.py`) is never installed, enabled, or started as a
systemd unit by this script. It remains required implementation/reference code
for replay building, validation, and local test-computer catalog
reconstruction (see [Linux Server Layout](#linux-server-layout) below), and can
still be run manually (`python convert_day.py --staging`) when needed — it is
just not part of the automated deployment path.

## Flags

| Flag | Meaning |
|------|---------|
| `--target <name>` | Service group to act on. Default `all`. |
| `--dry-run` | Print every action; change nothing. |
| `--no-systemd` | Skip systemd install/control and `/etc` writes (safe in WSL). A real environment promotion still checks supported units inactive when `systemctl` exists. |
| `--install-only` | Prepare env + install units; do not enable/start. |
| `--enable` | `systemctl enable` the selected units. |
| `--start` | `systemctl start` the selected units. |
| `--restart` | `systemctl restart` the selected units. |
| `--user <name>` | Service user/group. Default `zsom`. Rendered into the `User=`/`Group=` lines of every installed unit file. |
| `--app-dir <path>` | Repo checkout dir. Default `/home/zsom/services/CryptoRecorder`. Rendered into each unit's `WorkingDirectory=`/`ExecStart=` paths. |
| `--data-root <path>` | Data base dir. Default `/data/cryptorecorder`. Rendered into a newly created env file's `CRYPTO_RECORDER_*_ROOT` values; has no effect if the env file already exists (never overwritten). |
| `--env-file <path>` | Env file path. Default `/etc/cryptorecorder/cryptorecorder.env`. Rendered into each unit's `EnvironmentFile=` line. |
| `--uv-bin <path>` | Operator-installed uv executable or command name. The script never downloads uv. |
| `--migrate-venv` | Explicitly replace an unrecognized legacy `.venv` through a validated same-parent candidate and preserved backup. |

## Common steps performed

For the selected target, the script runs these steps (in order):

1. **Verify Linux** — refuse to run on non-Linux hosts.
2. **Verify repo root** — must be run from the repository checkout.
3. **Verify structure** — `docs/REPO_STRUCTURE.md` must exist (the frozen contract).
4. **Verify dependency authority** — locate the operator-supplied uv, record its
   version, run `uv lock --check`, and record the committed lock SHA-256.
5. **Prepare the locked environment** — accept an existing `.venv` only when
   its marker and live validation prove the current production-only lock
   selection. Otherwise build and validate a same-parent candidate with
   `uv sync --frozen --no-default-groups`; an unrecognized legacy environment
   requires the explicit `--migrate-venv` policy.
6. **Create env file** — render `systemd/cryptorecorder.env.example` (substituting `--data-root`)
   to the env-file path **only if it does not already exist** (never silently overwrite an
   existing env file).
7. **Create data dirs** — create the data roots under `--data-root`.
8. **Run validation** — validate dependency separation, imports, lock freshness,
   and production CLIs through `.venv/bin/python`.
9. **Validate unit template** — verify `Type=oneshot`, `Restart=no`, schema-v2
   backlog arguments, and 12 GiB/zero-swap limits without starting a service.
10. **Clean up stale units** — stop/disable/remove every legacy or renamed unit name this repo
   has ever shipped (see Safety notes below). Runs for every target, before units are installed.
11. **Install units** — render each unit file for the selected target (substituting `--user`,
    `--app-dir`, and `--env-file`) and install it to `/etc/systemd/system/`.
12. **Control units** — `enable`/`start`/`restart` the selected units if those flags were given.
13. **Print target** — show which units/groups were selected and their status.

When `--no-systemd` is set, steps that touch systemd or `/etc` are skipped; the script
still prepares the locked environment and data dirs (or prints them under `--dry-run`).

## Examples

```bash
# See exactly what a full install would do, without touching the system:
./scripts/deploy_linux_server.sh --target all --dry-run --no-systemd \
  --uv-bin "$(command -v uv)"

# Full server install: prepare, install units, enable + start everything:
./scripts/deploy_linux_server.sh --target all --uv-bin "$(command -v uv)" \
  --enable --start

# Restart just the recorder after a code update:
./scripts/deploy_linux_server.sh --target recorder --restart

# Install the daily replay-build timer but do not start it yet:
./scripts/deploy_linux_server.sh --target replay-build --install-only
```

## Safety notes

- The script never overwrites an existing env file (step 6).
- `--dry-run` makes no changes; `--no-systemd` avoids systemd mutation and
  `/etc` entirely. A non-dry-run environment promotion still uses a read-only
  `systemctl is-active` proof when systemd is available, so this flag cannot
  bypass the inactive-service migration boundary.
- It never installs uv, uses pip, resolves an unlocked environment, or invokes
  uv from a runtime service. `pyproject.toml` plus `uv.lock` are authoritative.
- An existing `.venv` that lacks the current lock marker is preserved and
  refused by default. Explicit migration requires every supported unit to be
  inactive, promotes only a validated same-parent candidate, keeps the old
  environment as `.venv.backup.<UTC>.<PID>`, and rolls back on an unambiguous
  post-promotion failure. Symlinks, unsafe ownership, collisions, or interrupted
  candidate evidence fail closed.
- The script does **not** deploy Syncthing, archive, or import features — none exist.
- It does **not** modify `recorder.py`, the raw schema, or `convert_day.py`.
- For every target (not just `all`/`replay-build`), the script stops, disables, and removes
  any of these legacy/renamed unit files left over from a previous deploy, before installing
  the current unit set: `cryptorecorder-feature-build.{service,timer}` (pre-issue-#17
  feature-build group), `crypto-recorder.service` (renamed to
  `cryptorecorder-recorder.service`), `nautilus-convert.{service,timer}` (renamed to
  `cryptorecorder-convert.{service,timer}`), `cryptorecorder-daily-build.{service,timer}`
  (renamed to `cryptorecorder-replay-build.{service,timer}`), and
  `cryptorecorder-convert.{service,timer}` itself (the legacy converter is no longer part
  of the automated production deployment path; any copy installed by an older deploy is
  stopped/disabled/removed). This cleanup step is skipped under `--no-systemd`.

---

## Linux Server Layout

> Content merged from the former `LINUX_SERVER.md`.

CryptoRecorder runs in two environments. Keep them clearly separated; do not hardcode
one environment's paths into the other.

## Environments

| Aspect | Development (WSL) | Production (Ubuntu server) |
|--------|-------------------|----------------------------|
| OS | Windows + WSL2 (Ubuntu) | Ubuntu Server (bare metal / VM) |
| Purpose | editing, tests, dry-runs | continuous recording + daily builds |
| systemd | usually unavailable | required (services + timers) |
| Data disk | local working copy | dedicated data volume |
| Deploy mode | `--dry-run` / `--no-systemd` | real install + enable + start |

The repository is developed in **WSL** and deployed on an **Ubuntu server**. The
deploy script (`scripts/deploy_linux_server.sh`) is safe to dry-run in WSL and performs
real systemd actions only on the server.

Dependency environments are explicit and locked. CPython `>=3.12,<3.15` is
supported; checkpoint 3 was resolved and locally exercised with CPython 3.12.3
and uv 0.11.29. `VERSION` remains the application-version authority; the
non-packaged uv project's neutral metadata version is not a release value.

```bash
# Production: recorder + replay lifecycle/build/validation only.
uv lock --check
UV_PROJECT_ENVIRONMENT="$APP_DIR/.venv" \
  uv sync --frozen --no-default-groups

# Reconstruction: production plus pinned Nautilus, in an external environment.
uv lock --check
UV_PROJECT_ENVIRONMENT=/external/cryptorecorder-reconstruction-env \
  uv sync --frozen --no-default-groups --extra reconstruction

# Development: production + reconstruction + tests, also external.
uv lock --check
UV_PROJECT_ENVIRONMENT=/external/cryptorecorder-development-env \
  uv sync --frozen --no-default-groups \
    --extra reconstruction \
    --group dev
```

Plain production sync contains neither `nautilus_trader` nor pytest. The
reconstruction extra pins `nautilus_trader==1.225.0` and still excludes test
tools. The development selection adds only pytest and pytest-asyncio. Frozen
syncs must leave `uv.lock` byte-identical; `requirements.txt` is intentionally
absent so there is no second hand-maintained dependency authority.

Services continue to execute `<app-dir>/.venv/bin/python`. They never execute
uv, resolve dependencies, update the lock, or access a package index at
startup. Repository templates have not yet been deployed or production-
accepted.

### Legacy `.venv` migration and rollback

Do not stop a unit automatically merely to make an environment migration
succeed. The operator sequence is:

1. stop the supported recorder and replay-build service/timer manually;
2. run the deploy command with `--dry-run --install-only --migrate-venv`;
3. inspect the exact candidate, backup, lock, and unit plan;
4. run the same command without `--dry-run` but still with `--install-only`;
5. validate the promoted `.venv` and preserved timestamped backup;
6. start or restart services only through a separate explicit command.

The deployment wrapper refuses an active supported unit before promotion. It
renames the existing environment to a timestamped backup, atomically renames
the validated candidate to `.venv`, verifies the production contract again,
and restores the backup if that post-promotion check fails unambiguously. It
does not delete the backup. For manual rollback, keep all units inactive,
inspect every sibling `.venv.*` artifact, move the failed active environment
aside, and restore exactly one preserved backup to `.venv`; any collision or
ambiguous interrupted state requires operator diagnosis rather than cleanup.

## Canonical production paths

| Name | Value | Notes |
|------|-------|-------|
| `APP_DIR` | `/home/zsom/services/CryptoRecorder` | repository checkout on the server |
| `VENV` | `$APP_DIR/.venv` | Python virtualenv |
| `ENV_FILE` | `/etc/cryptorecorder/cryptorecorder.env` | non-secret runtime env (copied from the template) |
| `DATA_BASE` | `/data/cryptorecorder` | parent of all generated data roots |

Generated data roots under `DATA_BASE` (see `config.py` and the env template):

```
/data/cryptorecorder/data_raw          # CRYPTO_RECORDER_DATA_ROOT
/data/cryptorecorder/replay_store       # CRYPTO_RECORDER_REPLAY_ROOT
/data/cryptorecorder/archive_days       # CRYPTO_RECORDER_ARCHIVE_DAYS_ROOT (placeholder)
```

> `archive_days` is a **placeholder** root. No archive, Syncthing, or import/restore
> code reads or writes it yet. `FEATURE_ROOT`, `CATALOG_JOBS_ROOT`, and `LABEL_ROOT`
> no longer exist (removed, issue #17) — CryptoRecorder does not own a
> feature-store, catalog-jobs, or label-store data root.

## Service groups

Production work is split into two service groups plus a meta target `all`.
Production automatically runs **only** `cryptorecorder-recorder.service` and
`cryptorecorder-replay-build.timer`.

| Group | systemd unit(s) | Kind | Schedule | Command (in `.venv`) |
|-------|-----------------|------|----------|----------------------|
| `recorder` | `cryptorecorder-recorder.service` | long-running | always on | `python recorder.py` |
| `replay-build` | `cryptorecorder-replay-build.service` + `.timer` | oneshot | ~01:00 UTC | `python -m pipeline.daily_build --date yesterday --backlog-days 7 --max-build-dates 3 --schema-version 2` |

Meta target **`all`** installs/controls both groups together.

`replay-build` reads directly from `data_raw` (via `pipeline.raw_manifest`); it
does not depend on `convert_day.py` or any converter output, so there is no
ordering dependency between them. The legacy converter systemd units
(`cryptorecorder-convert.service` + `.timer`) were **deleted from the repository**
in PR #18 — converter systemd automation is not part of the supported production
architecture. `convert_day.py`, `converter/`, and `validation/replay_catalog_reconstruct.py`
remain required implementation/reference code for replay building, validation,
and local test-computer catalog reconstruction: replay stores are synced
separately by the operator, and on the test computer the synced replay stores
may be reconstructed into temporary Nautilus catalogs by symbol (e.g. for
KovacsTrader) through the supported `pipeline.reconstruct_selected_catalog`
boundary — run manually, not via systemd. Any
`cryptorecorder-convert.{service,timer}` already installed on
an existing server (from before this change) is stopped, disabled, and removed
automatically the next time the deploy script runs (see "Safety notes" above).
There is no feature-build step; CryptoRecorder's scope ends at `replay_store`
(removed, issue #17).

### Development-computer selected reconstruction

This command is deliberately outside the Linux service/timer groups:

```bash
/external/cryptorecorder-reconstruction-env/bin/python \
  -m pipeline.reconstruct_selected_catalog \
  --replay-root /path/to/synced/replay_store \
  --venues BINANCE_SPOT \
  --symbols ADAUSDT BTCUSDT \
  --start 2026-06-11T12:00:00Z \
  --end 2026-06-12T00:00:00Z \
  --output-root /external/temporary/catalog_jobs \
  --job-id selected-20260611 \
  --profile full_l2
```

Create the external output root first and keep it outside production
`replay_store`. The final job is `<output-root>/<job-id>/`, containing
`catalog/` and its cryptographic `job_manifest.json`. The interval is
end-exclusive. Every venue, symbol, endpoint, output root, job ID, and profile
must be explicit; empty scope never means everything. Use `--overwrite` only
for intentional replacement of that exact completed job. No unit, timer, or
unattended catalog lifecycle is installed for this command.

> The replay-build service invokes `pipeline.daily_build` because
> `pipeline.build_replay_store` requires an explicit `YYYY-MM-DD` date and does
> not understand the literal `yesterday`. `daily_build` resolves `yesterday` to
> the previous completed UTC date.

### Replay-build memory and restart behaviour

**Memory-bounded writes**: `ReplayWriter` spools each symbol/day to a SQLite
file inside the staging directory (`staging_dir/scratch/`) via
`converter.spool.RawRecordSpool`, and writes Parquet incrementally in bounded
batches through `pyarrow.parquet.ParquetWriter`. Peak RSS is O(batch), not
O(symbol/day). Batch size is controlled by `CRYPTO_RECORDER_REPLAY_PARQUET_BATCH`
(default 5 000 rows). Because spool files live inside the staging directory,
build-wide reconciliation moves an abandoned staging tree and its scratch
files together to a unique quarantine directory after the kernel lock proves
there is no active owner. It never silently deletes the interrupted evidence.

**Restart policy**: The service uses `Restart=no`. A deterministic failure
(e.g. bad raw data) will not trigger an automatic retry loop.
`StartLimitIntervalSec=86400` / `StartLimitBurst=3` in `[Unit]` cap restarts
if the policy is ever re-enabled. To re-run the build manually after fixing the
root cause: `sudo systemctl start cryptorecorder-replay-build.service`.

**Resource policy**: the repository template sets `MemoryMax=12G` and
`MemorySwapMax=0` for the known 16 GiB host. This template has not been
installed or production-validated. Phase 7's accepted full-day build reached
its previous exact 10 GiB cgroup ceiling without swap/OOM, so the 12 GiB value
is an explicit bounded template, not proof of production headroom.

**Start timeout**: `TimeoutStartSec=23h`. A `oneshot` unit is considered
"hung" by systemd if it does not exit before `TimeoutStartSec` elapses, at
which point systemd sends `SIGTERM`/`SIGKILL` to the still-running process
and marks the invocation failed. The daily timer fires once at `01:00 UTC`,
and systemd will not start a new instance of this service while an existing
invocation is still active — so a genuinely stuck run must not be allowed to
stay active indefinitely, or it would silently block every later scheduled
run. The original `3600` (1 hour) value was too short for a full-universe
replay build; `infinity` (no cap at all) was considered and rejected because
it could let a truly stuck job block all future daily activations forever.
`23h` gives ample room for a long daily build of the previous completed UTC
day across a large symbol universe, while still guaranteeing systemd
terminates a stuck/hung run before the next `01:00 UTC` activation.
`Restart=no` is unchanged, so a timeout failure does not create a restart
loop; `StartLimitIntervalSec`/`StartLimitBurst` still bound restart attempts
if `Restart` is ever re-enabled. If the ceiling is ever reached: `systemctl
status`/`journalctl` will show the invocation as failed; because
`Restart=no`, no automatic retry occurs — the operator must inspect the
journal, resolve the root cause, and rerun manually (see Recovery command
below). The installed service only ever builds the previous completed UTC
day via `pipeline.daily_build --date yesterday`; it does not perform
source/schema replacements. Those require the distinct
`--rebuild-source-changed` or `--replace-incompatible` policy and are run
manually against exact selected partitions via the documented CLI (preferably
first against an isolated replay root) or a separately
controlled transient systemd scope (e.g. `systemd-run`) where an operator
chooses an explicit timeout appropriate to the manual job — not through this
installed daily service or its timer. The memory-bounded writer plus
crash-recovery state machine already make a `SIGKILL`/OOM/timeout mid-build
safe to recover from on the next run.

**Durable progress and recovery**: one nonblocking kernel advisory lock owns
all supported replay mutations. Before backlog scanning, a bounded all-date
reconciliation restores one unambiguous valid backup, quarantines stale
staging/invalid canonical evidence, safely removes an obsolete valid backup
beside a valid canonical, preserves every quarantine, and refuses symlinked,
unknown, corrupt, or ambiguous state. Already-valid schema/source partitions
are `skipped_valid` without consuming a build-date slot. Every action appears
in the per-date and run reports.

**Recovery command** (after a confirmed OOM or failure):
```bash
sudo systemctl start cryptorecorder-replay-build.service
# Then inspect:
journalctl -u cryptorecorder-replay-build.service -n 100
```

### Later owner-run isolated production acceptance (not executed)

This remains the manual production-acceptance condition in the owner-approved
2026-08-01 Issue #20 closure amendment. The implementation is release-
candidate complete, but the checked-in templates have not been deployed and
no production unit, environment, raw source, or replay root has been changed.
The replacement PR may close Issue #20 only after exact-head review and this
isolated acceptance; issue #21 separately retains the original top50/multi-day
semantic and `v2.0.0` gate.

At the separately approved exact commit, the owner should first preserve the
installed env/unit files, run `scripts/deploy_linux_server.sh --target
replay-build --dry-run --install-only`, and diff the rendered source template.
It must show `Restart=no`, `TimeoutStartSec=23h`, `MemoryMax=12G`,
`MemorySwapMax=0`, `--schema-version 2`, `--backlog-days 7`, and
`--max-build-dates 3`.

Before enabling the timer, use operator-approved isolated replay/report roots
on the production filesystem and run one exact small date/symbol through
`pipeline.daily_build` twice: first `built`, then `skipped_valid`. Verify the
lock metadata, atomic run/date reports, manifest/source/file checksums,
routine/deep integrity, zero residual staging/backup, visible quarantine,
filesystem capacity grouping, and cgroup swap/OOM/pressure telemetry. Do not
set either replacement policy during this first acceptance. Resolve every
legacy/source-changed/corrupt finding separately. Installing/enabling the real
timer requires a distinct owner decision after that evidence is reviewed.

No command in this checkpoint deployed, started, stopped, restarted, or
enabled a production unit, and `/etc/cryptorecorder/cryptorecorder.env` was
not edited.

## Explicitly out of scope

The following are **not** part of the deployment and have **no** services here:

- **Syncthing** archive/backup,
- **archive** export,
- **import / restore** tooling.

`ARCHIVE_DAYS_ROOT` exists only as a configuration placeholder. `LABEL_ROOT` and
`CATALOG_JOBS_ROOT` no longer exist (removed, issue #17).

See the [Deployment Script Reference](#deployment-script-reference) section
above for the deploy command and flags.

---

## State File Schemas

> Content merged from the former `SCHEMAS.md`.

These schemas document stable operational fields used by tooling and operators.
They are interface notes, not a strict JSON Schema contract.

## `state/heartbeat.json`

Top-level fields:

- `timestamp` (ISO-8601 with Hungary local offset, `Europe/Budapest`)
- `uptime_seconds`
- `total_symbols`
- `spot_symbols_active`
- `futures_symbols_active`
- `spot_symbols_requested`
- `futures_symbols_requested`
- `spot_symbols_dropped`
- `futures_symbols_dropped`
- `spot_symbols_dropped_list`
- `futures_symbols_dropped_list`
- `spot_coverage_ratio`
- `futures_coverage_ratio`
- `total_messages`
- `total_gaps`
- `total_reconnects`
- `queue_drop_total`
- `queue_drop_by_writer`
- `writer_queue_telemetry`
- `futures_enabled`
- `futures_disabled_reason`
- `architecture` — always `"deterministic_native"`
- `trade_health` — trade_v2 ingest diagnostics by venue
- `by_venue`

Notes:

- Human-facing report timestamps use Hungary local time with DST-aware offset
  (`+01:00` or `+02:00` depending on the date).
- `spot_symbols_dropped*` / `futures_symbols_dropped*` summarize startup
  `runtime_dropped` symbols, not the full universe `candidate_pool`.

`writer_queue_telemetry` reports recorder-side storage pressure:

- `writers` — map keyed by `VENUE:SYMBOL:CHANNEL`
- Per writer: `venue`, `symbol`, `channel`, `queue_size`,
  `queue_max_size`, `queue_high_watermark`, `drop_count`,
  `enqueued_count`, `write_count`, `blocked`, `current_block_sec`,
  `max_block_sec`, `last_block_started_ts`, `last_block_ended_ts`
- `totals` — `writer_count`, `queued_records`, `total_drops`,
  `depth_blocked_writer_count`
- `top_pressure_writers` — highest-pressure writers, including the writer
  `key` plus the per-writer fields above
- `compression` — background compression status: `queued`, `active`,
  `completed`, `failed`, `last_error`, `worker_count`

Depth writer queues do not drop on normal saturation by default; they block and
surface `blocked` / `current_block_sec`. Trade writer queues remain bounded and
may increment `drop_count` under sustained pressure.

`trade_health` is a map keyed by venue (e.g. `BINANCE_SPOT`, `BINANCE_USDTF`) containing venue-level ingest diagnostics:

- `ws_message_count` — total WebSocket messages received
- `parsed_trade_count` — trade records successfully parsed and committed
- `skipped_message_count` — messages skipped (validation or processing errors)
- `skip_reasons` — map of skip reason → count
- `lifecycle_only_sessions` — stream sessions with zero trade records
- `reconnect_count` — number of stream reconnections
- `last_close_reason` — most recent WebSocket close reason
- `sample_payload_shape` — example of first parsed message structure (diagnostic)
- `subscribed_symbols` / `subscribed_symbol_count` — native trade stream subscription coverage
- `per_symbol_parsed_trade_count` — parsed trade counts keyed by raw Binance symbol
- `stream_count`, `first_5_streams`, `url`, `url_length` — shard subscription/connect details
- `task_started`, `task_done`, `task_cancelled`, `connect_attempt_count`, `connected_once` — shard lifecycle details
- `first_message_seen_at`, `last_message_seen_at`, `last_exception` — liveness diagnostics for silent or failing trade shards
- `warnings` / `warning_count` — trade-ingest warnings, such as high-liquidity futures with active depth but zero parsed trades
- `shards` — if connection sharding is enabled, per-shard diagnostics with same structure

Empty if no trade recorder is running.

`by_venue` is a map keyed by venue (e.g. `BINANCE_SPOT`, `BINANCE_USDTF`) containing per-symbol objects with:

- `venue`
- `symbol`
- `message_count`
- `last_ts_event`
- `last_update_id`
- `prev_update_id`
- `gap_count`
- `sync_state`
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `last_heartbeat`

## `state/startup_coverage.json`

Startup audit summary with top-level `timestamp`, `warnings`, and nested
per-venue `spot` / `futures` sections.

`timestamp` uses Hungary local time (`Europe/Budapest`). The date-scoped file
names and conversion target dates elsewhere in the pipeline still stay on UTC.

Per-venue fields:

- `venue`
- `requested_raw`, `requested_count`
- `selected_raw`, `selected_count`
- `candidate_pool`
- `pre_filter_rejected_count`, `pre_filter_rejected_sample`
- `runtime_dropped_count`
- `active_raw`, `active_count`
- `coverage_ratio`
- `warnings`

Futures-specific fields:

- `candidate_pool_raw_count`
- `candidate_pool_after_sanity_count`
- `candidate_pool_after_support_check_count`
- `support_precheck_available`
- `support_precheck_error`
- `support_precheck_rejected_count`
- `support_precheck_rejected_sample`

## `state/convert_reports/YYYY-MM-DD.json`

Per-day converter report.

Core fields:

- `date`
- `timestamp`
- `runtime_sec`
- `status` — `ok`, `empty`, or `no_data`
- `architecture` — always `"deterministic_native"`
- `instruments_written`
- `total_trades_written`
- `total_order_book_deltas_written`
- `total_depth10_written`
- `total_derived_depth_snapshots_written`
- `full_depth_source` — currently `"OrderBookDeltas"`
- `derived_depth_snapshot_type` — currently `"OrderBookDepth10"`
- `derived_depth_snapshot_levels`
- `requested_depth_snapshot_levels`
- `requested_depth_snapshot_levels_applied`
- `snapshot_seed_limit` — Binance REST snapshot seed depth, not catalog snapshot depth
- `bad_lines` — unexpected converter exceptions only; intentional venue skips are counted separately
- `bad_lines_by_exception_type`
- `bad_lines_by_record_type`
- `bad_lines_by_venue_symbol`
- `bad_line_examples`
- `zero_size_trade_skipped_total` — raw venue trade records skipped before
  `TradeTick` construction because `quantity == 0`
- `zero_size_trade_skipped_by_venue_symbol`
- `zero_size_trade_examples` — up to 20 examples with symbol,
  `ts_event_ms`, price, quantity, and trade IDs
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `fenced_ranges_total`
- `fenced_ranges_low`
- `fenced_ranges_medium`
- `fenced_ranges_high`
- `unrecovered_fences` — compatibility alias for unrecovered real data-quality fences
- `bootstrap_fences` — normal startup/bootstrap fences
- `shutdown_fences` — graceful end-of-run websocket close fences
- `reconnect_fences` — live stream boundary fences requiring a new session/bootstrap
- `utc_day_rollover_fences` — UTC rollover reseed fences, counted as lifecycle
- `real_desync_fences` — continuity/desync/snapshot-quality fences
- `unrecovered_real_fences` — unrecovered `real_desync_fences`
- `standalone_depth_day` — every symbol with target-day depth updates has an in-day
  raw seed or carry-derived synthetic opening snapshot
- `timestamp_repartition_enabled`
- `extra_raw_partitions_scanned` — bounded adjacent raw folders scanned (`D-1`, `D+1`)
- `records_imported_from_previous_folder`
- `records_imported_from_next_folder`
- `records_dropped_outside_target_utc`
- `duplicate_records_suppressed`
- `carried_seed_symbol_count`
- `synthetic_opening_snapshot_count`
- `gap_warning_counts`
- `top_real_gap_offenders` — top symbols by depth-update gap, never by informational trade gap
- `per_symbol_fenced_ranges`
- `per_symbol_gap_diagnostics`
- `data_presence`
- `futures_enabled`
- `symbols_processed`
- `venues`
- `ts_ranges` (`trade`, `order_book_deltas`, `order_book_depths` start/end nanoseconds)
- `catalog_root`

`status` meanings:

- `ok`: converted trade and/or depth data was written
- `empty`: raw inputs resolved but no trade/depth output was produced
- `no_data`: no raw data was found for the requested date

`venues` is keyed by venue and contains:

- `symbols`
- `trades_written`
- `delta_events_written`
- `depth10_written`
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `fenced_ranges`
- `carried_seed_symbol_count`
- `synthetic_opening_snapshot_count`
- `duplicate_records_suppressed`

`data_presence` tracks which instruments have actual data:

- `instruments_defined`: Total instruments from exchangeInfo
- `instruments_with_trades`: Instruments with ≥1 TradeTick
- `instruments_with_depth`: Instruments with ≥1 OrderBookDeltas
- `instruments_with_no_data`: Instruments with neither
- `no_data_list`: List of instruments with no data (up to 20)

`per_symbol_fenced_ranges` maps `"VENUE/SYMBOL"` to:

- `fenced_ranges`: Count of intentionally excluded ranges
- `fenced_ranges_low`
- `fenced_ranges_medium`
- `fenced_ranges_high`
- `unrecovered_fences`
- `bootstrap_fences`
- `shutdown_fences`
- `reconnect_fences`
- `utc_day_rollover_fences`
- `real_desync_fences`
- `unrecovered_real_fences`
- `examples`: Up to 3 sample fenced ranges with session/time/reason/classification metadata
- `lifecycle_examples`: Bootstrap, UTC rollover, and graceful shutdown examples
- `real_examples`: Reconnect and real desync examples

`per_symbol_depth` maps `"VENUE/SYMBOL"` to depth conversion counts and recovery
diagnostics:

- `raw_record_count`
- `snapshot_seed_count` — raw exchange `snapshot_seed` records in the target UTC day
- `depth_update_record_count`
- `deltas_written`
- `depth10_written`
- `carried_seed_from_previous_day`
- `carried_seed_date`
- `carried_seed_session_id`
- `carried_seed_last_update_id`
- `carry_replay_record_count`
- `carry_recovery_failed_reason`
- `synthetic_opening_snapshot_written` — catalog opening snapshot derived from carry;
  not counted in `snapshot_seed_count`
- `timestamp_repartition_enabled`
- `extra_raw_partitions_scanned`
- `records_imported_from_previous_folder`
- `records_imported_from_next_folder`
- `records_dropped_outside_target_utc`
- `duplicate_records_suppressed`

`per_symbol_gap_diagnostics` maps `"VENUE/SYMBOL"` to:

- `max_depth_update_gap_sec`
- `depth_gap_count_over_1s`
- `depth_gap_count_over_5s`
- `depth_gap_count_over_60s`
- `max_trade_gap_sec` (informational; trade inactivity is not an L2 failure)
- `max_depth10_gap_sec`
- `session_boundary_gap_count`
- `shutdown_boundary_gap_count`
- `reconnect_boundary_gap_count`

`per_symbol_trade` maps `"VENUE/SYMBOL"` to trade conversion counts:

- `raw_record_count`
- `raw_trade_record_count`
- `raw_lifecycle_record_count`
- `ticks_written`
- `zero_size_trade_skipped`
- `first_trade_ts_ns`
- `last_trade_ts_ns`
- `will_create_tradetick`

`ts_ranges` is the authoritative indication of actual temporal coverage.

`timestamp` is the report creation time in Hungary local time
(`Europe/Budapest`), not the UTC trading day boundary.

## Raw Record Schemas

### depth_v2 records

All depth_v2 records have `record_type` and `stream_session_id`.

**snapshot_seed:**
```json
{
  "record_type": "snapshot_seed",
  "stream_session_id": 1,
  "session_seq": 1,
  "raw_index": 0,
  "ts_recv_ns": 1713400000000000000,
  "last_update_id": 12345,
  "bids": [["50000.00", "1.5"], ...],
  "asks": [["50001.00", "2.0"], ...]
}
```

**depth_update:**
```json
{
  "record_type": "depth_update",
  "stream_session_id": 1,
  "session_seq": 2,
  "raw_index": 0,
  "ts_recv_ns": 1713400001000000000,
  "first_update_id": 12346,
  "last_update_id": 12346,
  "bids": [["50000.00", "1.6"]],
  "asks": []
}
```

**sync_state / stream_lifecycle:** metadata records with `session_seq` (for sync_state) or without (for lifecycle).

### trade_v2 records

**trade (spot):**
```json
{
  "record_type": "trade",
  "market_type": "spot",
  "trade_stream_session_id": 1,
  "trade_session_seq": 1,
  "ts_recv_ns": 1713400000000000000,
  "ts_trade_ms": 1713400000000,
  "exchange_trade_id": 987654,
  "price": "50000.00",
  "quantity": "0.5",
  "is_buyer_maker": false,
  "buyer_order_id": 111,
  "seller_order_id": 222,
  "best_match_flag": true,
  "native_payload": { ... }
}
```

**trade (futures):**
```json
{
  "record_type": "trade",
  "market_type": "futures",
  "trade_stream_session_id": 1,
  "trade_session_seq": 1,
  "ts_recv_ns": 1713400000000000000,
  "ts_trade_ms": 1713400000000,
  "exchange_trade_id": 987654,
  "price": "50000.00",
  "quantity": "0.5",
  "is_buyer_maker": true,
  "first_trade_id": 100,
  "last_trade_id": 105,
  "native_payload": { ... }
}
```

**trade_stream_lifecycle:**
```json
{
  "record_type": "trade_stream_lifecycle",
  "trade_stream_session_id": 1,
  "ts_recv_ns": 1713400000000000000,
  "event": "connected"
}
```
Lifecycle markers do NOT consume `trade_session_seq`.
