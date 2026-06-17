# Deployment

CryptoRecorder is deployed on a Linux server with one wrapper script:

```bash
./scripts/deploy_linux_server.sh
```

The script is a **thin operator wrapper** (no business logic). It prepares the
environment, installs the selected systemd units, and optionally enables/starts them.
The canonical paths and service groups it uses are defined in
[LINUX_SERVER.md](LINUX_SERVER.md).

## Targets

`--target` selects which service group to act on (default: `all`).

| Target | Service group |
|--------|---------------|
| `all` | every group below (default) |
| `recorder` | `cryptorecorder-recorder.service` |
| `legacy-converter` | `cryptorecorder-convert.service` + `.timer` |
| `replay-build` | `cryptorecorder-replay-build.service` + `.timer` |
| `feature-build` | `cryptorecorder-feature-build.service` + `.timer` |

## Flags

| Flag | Meaning |
|------|---------|
| `--target <name>` | Service group to act on. Default `all`. |
| `--dry-run` | Print every action; change nothing. |
| `--no-systemd` | Skip all systemd/`/etc` actions (safe in WSL). |
| `--install-only` | Prepare env + install units; do not enable/start. |
| `--enable` | `systemctl enable` the selected units. |
| `--start` | `systemctl start` the selected units. |
| `--restart` | `systemctl restart` the selected units. |
| `--user <name>` | Service user. Default `zsom`. |
| `--app-dir <path>` | Repo checkout dir. Default `/home/zsom/services/CryptoRecorder`. |
| `--data-root <path>` | Data base dir. Default `/data/cryptorecorder`. |
| `--env-file <path>` | Env file path. Default `/etc/cryptorecorder/cryptorecorder.env`. |

## Common steps performed

For the selected target, the script runs these steps (in order):

1. **Verify Linux** — refuse to run on non-Linux hosts.
2. **Verify repo root** — must be run from the repository checkout.
3. **Verify structure** — `docs/REPO_STRUCTURE.md` must exist (the frozen contract).
4. **Create venv** — create `.venv` if missing.
5. **Install requirements** — `pip install -r requirements.txt` into `.venv`.
6. **Create env file** — copy `systemd/cryptorecorder.env.example` to the env-file path
   **only if it does not already exist** (never silently overwrite an existing env file).
7. **Create data dirs** — create the data roots under `--data-root`.
8. **Run validation** — `python validate.py --quick`.
9. **Print target** — show which units/groups were selected and their status.

When `--no-systemd` is set, steps that touch systemd or `/etc` are skipped; the script
still prepares the venv, dependencies, and data dirs (or prints them under `--dry-run`).

## Examples

```bash
# See exactly what a full install would do, without touching the system:
./scripts/deploy_linux_server.sh --target all --dry-run --no-systemd

# Full server install: prepare, install units, enable + start everything:
./scripts/deploy_linux_server.sh --target all --enable --start

# Restart just the recorder after a code update:
./scripts/deploy_linux_server.sh --target recorder --restart

# Install the daily build timers but do not start them yet:
./scripts/deploy_linux_server.sh --target replay-build --install-only
./scripts/deploy_linux_server.sh --target feature-build --install-only
```

## Safety notes

- The script never overwrites an existing env file (step 6).
- `--dry-run` makes no changes; `--no-systemd` avoids systemd and `/etc` entirely.
- The script does **not** deploy Syncthing, archive, or import features — none exist.
- It does **not** modify `recorder.py`, the raw schema, or `convert_day.py`.
