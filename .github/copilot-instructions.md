# GitHub Copilot Instructions — CryptoRecorder

This is a short pointer file. The full, binding rules live in:

- [AGENTS.md](../AGENTS.md) — complete agent rules and Definition of Done.
- [docs/REPO_STRUCTURE.md](../docs/REPO_STRUCTURE.md) — frozen folder/file contract.
- [docs/PROJECT_STATUS.md](../docs/PROJECT_STATUS.md) — validated vs deferred status.
- [docs/FULL_L2_REPLAY_CATALOG_PLAN.md](../docs/FULL_L2_REPLAY_CATALOG_PLAN.md) — deferred full-L2 path.

## Non-negotiable rules

- **Never** create random new top-level folders. Amend `docs/REPO_STRUCTURE.md` first.
- **Never** change `recorder.py`, `phase2_depth.py`, `native_trades.py`, `storage.py`,
  or the raw `data_raw/` schema/layout unless the task explicitly allows raw changes.
- **Never** delete or rewrite `convert_day.py` (the reference converter).
- **Never** claim the `replay_store → full_l2` catalog path works until it is validated
  against `convert_day.py`. It is **deferred**.
- **Never** implement Syncthing, archive, or import features. `ARCHIVE_DAYS_ROOT` and
  `LABEL_ROOT` are placeholders only.

## Package boundaries

- `converter/` — legacy full-L2 raw→Nautilus converter internals.
- `pipeline/` — build/transform CLIs only (no audit CLIs).
- `validation/` — audit/compare/inspect CLIs only (no build CLIs).
- `stores/` — replay/feature schemas, readers, writers.
- `scripts/` — thin operator wrappers only (no business logic).

When unsure: **stop and ask**. Keep status honest. Prefer small PRs.
