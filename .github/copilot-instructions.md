# GitHub Copilot Instructions — CryptoRecorder

This is a short pointer file. The full, binding rules live in:

- [AGENTS.md](../AGENTS.md) — complete agent rules and Definition of Done.
- [docs/REPO_STRUCTURE.md](../docs/REPO_STRUCTURE.md) — frozen folder/file contract.
- [docs/PROJECT_STATUS.md](../docs/PROJECT_STATUS.md) — validated vs deferred status.
- [docs/FULL_L2_REPLAY_CATALOG_PLAN.md](../docs/FULL_L2_REPLAY_CATALOG_PLAN.md) — full-L2 replay path (validated on the ADAUSDT smoke; broader validation pending).

## Non-negotiable rules

- **Never** create random new top-level folders. Amend `docs/REPO_STRUCTURE.md` first.
- **Never** change `recorder.py`, `phase2_depth.py`, `native_trades.py`, `storage.py`,
  or the raw `data_raw/` schema/layout unless the task explicitly allows raw changes.
- **Never** delete or rewrite `convert_day.py` (the reference converter).
- **Never** claim broader top50/multi-day `full_l2` equivalence (the `v2.0.0` gate)
  until that wider validation passes. The `full_l2` path is implemented and passes
  the ADAUSDT single-day smoke vs `convert_day.py`, which stays the production reference.
- **Never** implement Syncthing, archive, or import features. `ARCHIVE_DAYS_ROOT` is
  a placeholder only. `LABEL_ROOT` was removed entirely (issue #17); do not
  reintroduce a label/target store.
- **Never** reintroduce `requirements.txt` or pip-based deployment installation.
  `pyproject.toml` and committed `uv.lock` are authoritative; deployment syncs
  are explicit, frozen, and install no default dependency groups.

## Package boundaries

- `converter/` — legacy full-L2 raw→Nautilus converter internals.
- `pipeline/` — build/transform CLIs only (no audit CLIs).
- `validation/` — audit/compare/inspect CLIs only (no build CLIs).
- `stores/` — replay schemas, readers, writers (no feature/label schemas).
- `scripts/` — thin operator wrappers only (no business logic).

When unsure: **stop and ask**. Keep status honest. Prefer small PRs.
