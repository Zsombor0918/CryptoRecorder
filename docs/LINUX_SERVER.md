# Linux Server Layout (Dev vs Production)

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
/data/cryptorecorder/feature_store      # CRYPTO_RECORDER_FEATURE_ROOT
/data/cryptorecorder/catalog_jobs       # CRYPTO_RECORDER_CATALOG_JOBS_ROOT
/data/cryptorecorder/archive_days       # CRYPTO_RECORDER_ARCHIVE_DAYS_ROOT (placeholder)
/data/cryptorecorder/label_store        # CRYPTO_RECORDER_LABEL_ROOT (placeholder)
```

> `archive_days` and `label_store` are **placeholder** roots. No archive, Syncthing,
> or label code reads or writes them yet.

## Service groups

Production work is split into four service groups plus a meta target `all`.

| Group | systemd unit(s) | Kind | Schedule | Command (in `.venv`) |
|-------|-----------------|------|----------|----------------------|
| `recorder` | `cryptorecorder-recorder.service` | long-running | always on | `python recorder.py` |
| `legacy-converter` | `cryptorecorder-convert.service` + `.timer` | oneshot | ~00:10 UTC | `python convert_day.py --staging` (defaults to yesterday UTC) |
| `replay-build` | `cryptorecorder-replay-build.service` + `.timer` | oneshot | ~01:00 UTC | `python -m pipeline.daily_build --steps replay --date yesterday` |
| `feature-build` | `cryptorecorder-feature-build.service` + `.timer` | oneshot | ~02:30 UTC | `python -m pipeline.daily_build --steps features --date yesterday` |

Meta target **`all`** installs/controls all four groups together.

Ordering: the daily chain runs **convert → replay → features** (each after the
previous day has closed and the prior step has produced output).

> The replay-build and feature-build services invoke `pipeline.daily_build` because
> `pipeline.build_replay_store` and `pipeline.build_feature_store` require an explicit
> `YYYY-MM-DD` date and do not understand the literal `yesterday`. `daily_build`
> resolves `yesterday` to the previous completed UTC date.

## Explicitly out of scope

The following are **not** part of the deployment and have **no** services here:

- **Syncthing** archive/backup,
- **archive** export,
- **import / restore** tooling.

`ARCHIVE_DAYS_ROOT` and `LABEL_ROOT` exist only as configuration placeholders.

See [DEPLOYMENT.md](DEPLOYMENT.md) for the deploy command and flags.
