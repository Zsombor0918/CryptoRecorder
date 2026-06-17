# Versioning Policy

CryptoRecorder follows [Semantic Versioning](https://semver.org/) with a
project-specific interpretation of the major/minor lines. The current version is in
[`VERSION`](../VERSION) and the change history is in [`CHANGELOG.md`](../CHANGELOG.md).

## Version lines

### v1.x — recorder + reference converter + replay/feature foundation
The `v1` line covers the **validated core**:
- the deterministic-native **recorder**,
- the reference **`convert_day.py`** full-L2 converter,
- the **replay store** and **feature store** v0 foundation.

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
- stronger automated validation/audit gates around replay and feature builds.

It still must **not** introduce full-L2 catalog generation from the replay store.

### v2.0.0 — validated replay_store → full_l2 catalog
The `v2.0.0` release is reserved for one thing: the
**`replay_store → generate_catalog --profile full_l2`** path being **validated for
semantic equivalence against `convert_day.py`**.

**No `v2` release may ship until full-L2 semantic equivalence passes.** Until then,
full-L2 catalog generation from the replay store stays **deferred** (see
[PROJECT_STATUS.md](PROJECT_STATUS.md) and
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md)).

## Rules

- Bump `VERSION` and add a `CHANGELOG.md` entry in the same change that ships a feature.
- Keep `## [Unreleased]` current; move entries into a dated version section on release.
- Do **not** create git tags as part of routine agent work unless explicitly asked.
- A deferred capability never counts toward a version milestone until its validation
  evidence is recorded in [PROJECT_STATUS.md](PROJECT_STATUS.md).
