# Linux Server Installation

This guide prepares CryptoRecorder for a later Ubuntu/Linux server deployment.
It does not require starting the live recorder during installation.

CryptoRecorder currently:

- records live Binance REST/WebSocket market data with `recorder.py`
- converts one UTC day at a time with `convert_day.py`
- stores operational health/readiness as JSON files, not web pages
- exposes an in-repo CLI catalog inspector, not a browser catalog viewer
- uses `requirements.txt` plus a virtual environment; there is no `pyproject.toml`,
  `uv.lock`, or Node/npm dependency in this repository

## Assumptions Used Below

These commands assume:

- Ubuntu or another Linux host with `systemd`
- the deploy user is the current shell user, exported as `APP_USER="$USER"`
- project code lives at `~/services/CryptoRecorder`
- optional large data storage lives at `/data/cryptorecorder`
- Python `3.10+`
- outbound network access to Binance REST and WebSocket endpoints

Adjust the exported variables once if your server differs.

```bash
export APP_USER="$USER"
export APP_HOME="$HOME/services"
export APP_DIR="$APP_HOME/CryptoRecorder"
export DATA_BASE="/data/cryptorecorder"
```

## 1. Install OS Packages

Ubuntu/Debian example:

```bash
sudo apt update
sudo apt install -y \
  git \
  curl \
  ca-certificates \
  build-essential \
  python3 \
  python3-venv \
  python3-pip

python3 --version
```

`python3 --version` must show Python `3.10` or newer.

## 2. Clone the Repository

```bash
mkdir -p "$APP_HOME"
cd "$APP_HOME"
git clone <repo-url> CryptoRecorder
cd "$APP_DIR"
```

Verification:

```bash
pwd
git status --short
```

## 3. Choose Python Environment Setup

### Option A: repo-native `venv` + `pip`

```bash
cd "$APP_DIR"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Option B: optional `uv`

`uv` is optional. The repository does not require it, but it can manage the same
virtual environment and dependency installation workflow.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

cd "$APP_DIR"
uv venv .venv --python python3
source .venv/bin/activate
uv pip install -r requirements.txt
```

Verification for either option:

```bash
cd "$APP_DIR"
source .venv/bin/activate
python --version
python -m pip --version || true
```

## 4. Understand Current Paths Before First Import

`config.py` currently defines these paths directly:

| Purpose | Current code path |
|---|---|
| raw JSONL/Zstandard data | `CryptoRecorder/data_raw/` |
| metadata/universe cache | `CryptoRecorder/meta/` |
| heartbeat, reports, state | `CryptoRecorder/state/` |
| recorder file log | `CryptoRecorder/recorder.log` |
| Nautilus catalog | sibling path `../nautilus_data/catalog/` |

Important:

- importing `config.py` creates `data_raw/`, `meta/`, `state/`, and
  `state/convert_reports/`
- if you want large data on `/data`, create symlinks before running
  `validate.py`, tests that import config, or live commands
- `recorder.log` still stays inside the project checkout unless code/config is
  changed later

## 5. Recommended Large-Disk Layout

Recommended server layout:

```text
~/services/CryptoRecorder/              project code
/data/cryptorecorder/data_raw/          raw exchange data
/data/cryptorecorder/meta/              daily universe cache and metadata
/data/cryptorecorder/state/             heartbeat, reports, runtime state
/data/cryptorecorder/nautilus_data/     Nautilus catalog and sibling reports
```

Create the external directories:

```bash
sudo mkdir -p \
  "$DATA_BASE/data_raw" \
  "$DATA_BASE/meta" \
  "$DATA_BASE/state" \
  "$DATA_BASE/nautilus_data/catalog"

sudo chown -R "$APP_USER:$APP_USER" "$DATA_BASE"
```

Create symlinks before the first app import. These commands intentionally stop
if the repo-local paths already exist, so existing data is never overwritten
silently.

```bash
cd "$APP_DIR"

for path in data_raw meta state; do
  if [ -e "$path" ] || [ -L "$path" ]; then
    echo "Refusing to replace existing $path. Inspect it manually first."
    exit 1
  fi
done

if [ -e "$APP_HOME/nautilus_data" ] || [ -L "$APP_HOME/nautilus_data" ]; then
  echo "Refusing to replace existing $APP_HOME/nautilus_data. Inspect it manually first."
  exit 1
fi

ln -s "$DATA_BASE/data_raw" data_raw
ln -s "$DATA_BASE/meta" meta
ln -s "$DATA_BASE/state" state
ln -s "$DATA_BASE/nautilus_data" "$APP_HOME/nautilus_data"
```

Verification:

```bash
cd "$APP_DIR"
ls -ld data_raw meta state "$APP_HOME/nautilus_data"
```

If you prefer all data inside the project tree, skip the symlinks and create the
default directory shape explicitly before validation:

```bash
cd "$APP_DIR"
mkdir -p \
  data_raw \
  meta \
  state/convert_reports \
  "$APP_HOME/nautilus_data/catalog"
```

## 6. Validate the Python Setup Without Live Recording

These are safe local checks. They do not start the live recorder.

```bash
cd "$APP_DIR"
source .venv/bin/activate

python validate.py
python -m pytest tests/
python convert_day.py --help
```

Expected:

- `validate.py` reports dependency/import/path checks
- the unit test suite passes
- `convert_day.py --help` exits successfully and documents `--staging`

## 7. Activate the Git Pre-Commit Hook

This repository ships a pre-commit hook in `.githooks/pre-commit` that runs the
mandatory change-audit compliance check before every commit. It blocks the commit
if staged changes violate the rules in `AGENTS.md` Section 6.

Activate it **once per clone**:

```bash
cd "$APP_DIR"
git config core.hooksPath .githooks
```

Verify it is wired up:

```bash
git config core.hooksPath
# expected output: .githooks
```

Both hooks are active once `.githooks` is the hooks path:
- **`pre-commit`** — runs `python -m validation.audit_change_compliance --staged`
  before every commit and blocks on failure.
- **`commit-msg`** — validates the commit message format:
  `<type>(<scope>): <subject>` (conventional commits; see `AGENTS.md` Section 7).

To test the hook without committing:

```bash
source .venv/bin/activate
python -m validation.audit_change_compliance --staged
```

**Bypass (exceptional use only):**

```bash
git commit --no-verify
```

Using `--no-verify` does **not** exempt you from writing the audit entry; you
must still add it to `docs/CHANGE_AUDIT.md` before the PR is complete.

## 8. Configuration and Environment Variables

There is no secret config file in this repo. Operational tuning is read from
environment variables in `config.py` and `recorder.py`.

Common examples:

| Variable | Default | Purpose |
|---|---:|---|
| `CRYPTO_RECORDER_TOP_SYMBOLS` | `50` | symbols selected per venue |
| `CRYPTO_RECORDER_TOP_SYMBOL_CANDIDATES` | `120` | spot candidate pool |
| `CRYPTO_RECORDER_FUTURES_TOP_SYMBOL_CANDIDATES` | `200` | futures candidate pool |
| `CRYPTO_RECORDER_EMIT_DERIVED_DEPTH_SNAPSHOTS` | `1` | derived depth snapshots |
| `CRYPTO_RECORDER_DERIVED_DEPTH_SNAPSHOT_INTERVAL_SEC` | `1.0` | derived snapshot cadence |
| `CRYPTO_RECORDER_DEPTH_WS_MAX_SYMBOLS_PER_CONNECTION` | `10` | depth WebSocket sharding |
| `CRYPTO_RECORDER_TRADE_WS_MAX_SYMBOLS_PER_CONNECTION` | `10` | trade WebSocket sharding |

The systemd units load this optional non-secret file:

```text
/etc/cryptorecorder/cryptorecorder.env
```

Install the example:

```bash
sudo install -d -m 0755 /etc/cryptorecorder
sudo install -m 0644 \
  "$APP_DIR/systemd/cryptorecorder.env.example" \
  /etc/cryptorecorder/cryptorecorder.env
```

Review before live operation:

```bash
sudo sed -n '1,200p' /etc/cryptorecorder/cryptorecorder.env
```

Path caveat:

- data/catalog roots are not environment-configurable today
- use the symlink layout above if the server should store large data under
  `/data/cryptorecorder`

## 9. Install systemd Units Safely

Do not copy the repository unit files unchanged. They still contain the
development checkout path `/home/zsom/services/CryptoRecorder` and
`User=zsom`.

The repository ships `scripts/deploy_linux_server.sh`, a thin operator wrapper
that substitutes the path/user, installs the unit files for a chosen target,
and removes any stale units from a previous deploy. It contains no business
logic. Prefer it over manual `sed`/`cp`:

```bash
cd "$APP_DIR"
./scripts/deploy_linux_server.sh \
  --target all \
  --user "$APP_USER" \
  --app-dir "$APP_DIR" \
  --data-root "$DATA_BASE" \
  --dry-run
```

Drop `--dry-run` once the printed actions look correct. Valid `--target`
values are `all`, `recorder`, `legacy-converter`, and `replay-build`
(`scripts/deploy_linux_server.sh --help` documents every flag).

If you prefer manual installation, generate host-specific units with the same
path/user substitution:

```bash
cd "$APP_DIR"

for unit in \
  cryptorecorder-recorder.service \
  cryptorecorder-convert.service \
  cryptorecorder-convert.timer \
  cryptorecorder-replay-build.service \
  cryptorecorder-replay-build.timer
do
  sed \
    -e "s|User=zsom|User=$APP_USER|g" \
    -e "s|/home/zsom/services/CryptoRecorder|$APP_DIR|g" \
    "systemd/$unit" | sudo tee "/etc/systemd/system/$unit" >/dev/null
done

sudo systemctl daemon-reload
sudo systemd-analyze verify \
  /etc/systemd/system/cryptorecorder-recorder.service \
  /etc/systemd/system/cryptorecorder-convert.service \
  /etc/systemd/system/cryptorecorder-convert.timer \
  /etc/systemd/system/cryptorecorder-replay-build.service \
  /etc/systemd/system/cryptorecorder-replay-build.timer
```

Current unit behavior:

- `cryptorecorder-recorder.service` runs `recorder.py` (live recording)
- `cryptorecorder-convert.service` runs `convert_day.py --staging` (legacy
  full-L2 converter, previous UTC day)
- `cryptorecorder-convert.timer` schedules the legacy converter once daily at
  `00:10 UTC`
- `cryptorecorder-replay-build.service` runs
  `python -m pipeline.daily_build --date yesterday` (builds `replay_store`)
- `cryptorecorder-replay-build.timer` schedules the replay build once daily
  at `01:00 UTC`
- all services write stdout/stderr to `journald`
- all services load `/etc/cryptorecorder/cryptorecorder.env` if present

Inspect the installed units:

```bash
systemctl cat cryptorecorder-recorder.service
systemctl cat cryptorecorder-convert.service
systemctl cat cryptorecorder-convert.timer
systemctl list-timers --all cryptorecorder-convert.timer
systemctl cat cryptorecorder-replay-build.service
systemctl cat cryptorecorder-replay-build.timer
systemctl list-timers --all cryptorecorder-replay-build.timer
```

## 10. Start Services Only When Live Recording Is Intended

These commands begin real server operation.

Enable automatic startup:

```bash
sudo systemctl enable cryptorecorder-recorder.service
sudo systemctl enable cryptorecorder-convert.timer
sudo systemctl enable cryptorecorder-replay-build.timer
```

Start live operation:

```bash
sudo systemctl start cryptorecorder-recorder.service
sudo systemctl start cryptorecorder-convert.timer
sudo systemctl start cryptorecorder-replay-build.timer
```

Stop/restart later:

```bash
sudo systemctl stop cryptorecorder-recorder.service
sudo systemctl restart cryptorecorder-recorder.service
sudo systemctl stop cryptorecorder-convert.timer
sudo systemctl start cryptorecorder-convert.timer
sudo systemctl stop cryptorecorder-replay-build.timer
sudo systemctl start cryptorecorder-replay-build.timer
```

Status and logs:

```bash
systemctl status cryptorecorder-recorder.service
systemctl status cryptorecorder-convert.timer
systemctl status cryptorecorder-convert.service
systemctl status cryptorecorder-replay-build.timer
systemctl status cryptorecorder-replay-build.service

journalctl -u cryptorecorder-recorder.service -f
journalctl -u cryptorecorder-convert.service -f
journalctl -u cryptorecorder-replay-build.service -f
```

## 11. Manual Commands

### Live recorder

This starts real Binance recording and writes raw data:

```bash
cd "$APP_DIR"
source .venv/bin/activate
python recorder.py
```

### Safe staged conversion

Preferred manual converter command for an existing UTC raw-data date:

```bash
cd "$APP_DIR"
source .venv/bin/activate
python convert_day.py --date YYYY-MM-DD --staging
```

`--staging` now:

- converts into an isolated temporary catalog
- validates staged output
- publishes only parquet files overlapping the requested UTC date
- preserves all unrelated catalog files
- backs up only files being replaced for that date
- rolls back on publish failure

Do not use `--allow-partial-overwrite` casually. It bypasses the converter’s
low-depth-coverage refusal guard for direct overwrite mode.

## 12. Validation, Readiness, and Catalog Inspection

Health/readiness output is file-based:

| Path | Meaning |
|---|---|
| `state/heartbeat.json` | live recorder heartbeat |
| `state/startup_coverage.json` | startup universe/coverage audit |
| `state/universe_health/YYYY-MM-DD.json` | daily universe-health checkpoints |
| `state/universe_health/symbol_health.json` | aggregated universe-health state |
| `state/convert_reports/YYYY-MM-DD.json` | converter report |
| `../nautilus_data/convert_reports/YYYY-MM-DD.json` | sibling converter report copy |
| `state/disk_usage.json` | disk usage + monitoring-health snapshot (see docs/OPERATIONS.md) |
| `state/disk_monitor_state.json` | last-known-good disk measurements + growth history (restart-safe) |
| `state/reconnects.log` | reconnect events |

Useful checks after real data exists:

```bash
cd "$APP_DIR"
source .venv/bin/activate

python -m json.tool state/heartbeat.json | sed -n '1,120p'
python -m json.tool state/startup_coverage.json | sed -n '1,160p'
python -m validation.phase2_report state/convert_reports/YYYY-MM-DD.json
```

The repository does not ship a browser catalog viewer. Use the CLI inspector:

```bash
python -m validation.catalog_inspect \
  "$APP_HOME/nautilus_data/catalog" \
  BTCUSDT.BINANCE
```

## 13. Live Smoke and Acceptance Tests

These scripts start the recorder unless you use the documented skip mode.
Run them only when live Binance test recording is acceptable.

```bash
# Starts a short live recorder session
python scripts/smoke_test.py --runtime 60

# Starts live recorder unless --skip-recorder is used
python scripts/acceptance_test.py --runtime 300

# Converter/catalog-only path if raw data already exists
python scripts/acceptance_test.py --skip-recorder
```

## 14. Disk Cleanup Warning

While the live recorder is running, `disk_monitor.py` can delete the oldest raw
date directories after raw data storage crosses:

- raw soft limit: `750 GB`
- raw cleanup target: `700 GB`
- total tracked hard alert threshold: `850 GB`

Review those constants in `config.py` before a long-running server deployment.
`RAW_RETENTION_DAYS = 7` exists in `config.py`, but the active cleanup logic is
currently size-triggered. Catalog, `meta`, and `state` sizes are still reported
for observability, but only raw data size triggers raw cleanup.

## 15. Troubleshooting

### `validate.py` created repo-local directories before symlinks

Stop and inspect:

```bash
cd "$APP_DIR"
ls -ld data_raw meta state "$APP_HOME/nautilus_data" 2>/dev/null || true
```

Do not replace directories that already contain data until you have manually
decided how to migrate them.

### systemd service fails immediately

Check path/user substitution first:

```bash
systemctl cat cryptorecorder-recorder.service
systemctl cat nautilus-convert.service
```

Then inspect logs:

```bash
journalctl -u cryptorecorder-recorder.service -n 200 --no-pager
journalctl -u nautilus-convert.service -n 200 --no-pager
```

### converter timer date seems wrong

The timer is meant to run at `00:10 UTC`, and the converter default date is
“yesterday UTC.” Confirm the installed timer rather than relying on local wall
clock intuition:

```bash
systemctl cat nautilus-convert.timer
systemctl list-timers --all nautilus-convert.timer
```

### converter refuses partial overwrite

This is a safety feature. Prefer investigating raw depth coverage and the JSON
report instead of forcing conversion:

```bash
python -m json.tool state/convert_reports/YYYY-MM-DD.json | sed -n '1,220p'
```

## 16. Data Sync / Syncthing Later

Data sync is intentionally not configured here.

Placeholder for the future deployment phase:

- choose the final external data mount path
- decide which raw/catalog/state directories should sync
- configure Syncthing or another sync strategy after the recorder and converter
  are stable on the Linux server

## Linux Server Deployment Checklist

- [ ] Linux host has Python `3.10+`, `git`, and build tools
- [ ] repository cloned to the intended deploy path
- [ ] `.venv` created and dependencies installed
- [ ] large data directories and symlinks reviewed before first import
- [ ] `python validate.py` passes
- [ ] `python -m pytest tests/` passes
- [ ] `python convert_day.py --help` works
- [ ] `/etc/cryptorecorder/cryptorecorder.env` reviewed
- [ ] systemd units generated with the correct user/path
- [ ] `systemd-analyze verify` passes
- [ ] live recorder is started only when intentionally beginning real collection

## More Documentation

- [docs/VALIDATION.md](docs/VALIDATION.md)
- [docs/OPERATIONS.md](docs/OPERATIONS.md) — operations, deployment, Linux server, state schemas
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — design, storage layers, guarantees
