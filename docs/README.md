# CryptoRecorder Docs

Use this page as the current documentation map.

## Start Here

- [Project Status](PROJECT_STATUS.md) — validated vs deferred; read this first.
- [Architecture](ARCHITECTURE.md) — high-level system paths and module roles.
- [Repo Structure](REPO_STRUCTURE.md) — binding folder contract; read before adding files.
- [Implementation Audit](IMPLEMENTATION_AUDIT.md) — what is validated, smoke-tested, limited, and deferred.
- [Repo Cleanup Audit](REPO_CLEANUP_AUDIT.md) — repository structure, generated files, and cleanup decisions.

## Governance And Deployment

- [AI Workflow](AI_WORKFLOW.md) — the step-by-step procedure agents must follow.
- [Versioning](VERSIONING.md) — version lines and the v2.0.0 full-L2 gate.
- [Deployment](DEPLOYMENT.md) — the `deploy_linux_server.sh` targets and flags.
- [Linux Server](LINUX_SERVER.md) — dev (WSL) vs production (Ubuntu) layout and services.
- Agent rules live in [../AGENTS.md](../AGENTS.md); change history in [../CHANGELOG.md](../CHANGELOG.md).


## Runtime And Conversion

- [Operations](OPERATIONS.md) — recorder and converter operations.
- [Schemas](SCHEMAS.md) — raw recorder schemas and state files.
- [Guarantees](GUARANTEES.md) — current scope boundaries.
- [Validation](VALIDATION.md) — validation layers.
- [Installation](../INSTALL.md) — machine setup.

## Replay/Feature v0

- [Storage Architecture](STORAGE_ARCHITECTURE.md) — roles of `data_raw`, `replay_store`, `feature_store`, catalog jobs, and reports.
- [Replay Store](REPLAY_STORE.md) — actual replay Parquet schema and replay reader behavior.
- [Feature Store](FEATURE_STORE.md) — actual feature schema, sparse UTC-day window behavior, and deferred feature fields.
- [Generate Catalog](GENERATE_CATALOG.md) — implemented `trades_only` catalog generation and validation commands.

## Next Milestone

- [Full-L2 Replay Catalog Plan](FULL_L2_REPLAY_CATALOG_PLAN.md) — plan for implementing `replay_store -> generate_catalog --profile full_l2` by reusing the old converter semantics.

Current status:

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
  validated full-L2 path

data_raw -> replay_store -> feature_store
replay_store -> generate_catalog --profile trades_only
  validated v0 replay/feature foundation

replay_store -> generate_catalog --profile full_l2
  deferred
```
