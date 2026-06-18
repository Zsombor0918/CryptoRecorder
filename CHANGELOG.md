# Changelog

All notable changes to CryptoRecorder are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).
Version policy is described in [docs/VERSIONING.md](docs/VERSIONING.md).

## [Unreleased]

### Added
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
- New flags on `pipeline.generate_catalog`: `--emit-depth10/--no-emit-depth10`,
  `--depth10-interval-sec`, `--derived-depth-snapshot-levels`, `--time-filter`.
- `validation/audit_storage_size.py` — audit-only CLI measuring on-disk size of
  replay/feature/catalog artifacts.
- Docs: `docs/STORAGE_SIZE_AUDIT.md`, `docs/FEATURE_STORE_REQUIREMENTS_AUDIT.md`.
- Tests: `tests/test_replay_depth_adapter.py`, `tests/test_generate_catalog_full_l2.py`,
  `tests/test_catalog_equivalence_full_l2.py`, `tests/test_full_l2_realdata_gate.py`,
  plus a synthetic full-L2 convert_day-vs-replay equivalence test in
  `tests/test_catalog_equivalence.py`.

### Changed
- `replay_store → generate_catalog --profile full_l2` moved from **deferred** to
  **implemented, semantically validated on the ADAUSDT single-day smoke** against
  `convert_day.py` (trades, OrderBookDeltas, OrderBookDepth10, and book
  checkpoints all match). `convert_day.py` remains the production reference.
- Docs updated to reflect full_l2 support: `docs/REPO_STRUCTURE.md`,
  `docs/PROJECT_STATUS.md`, `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`,
  `docs/GENERATE_CATALOG.md`, `docs/FEATURE_STORE.md`, `docs/README.md`,
  `README.md`.

### Deferred
- Broader `full_l2` validation across the top50 universe and multiple days. This
  remains the `v2.0.0` gate; `v2.0.0` is **not** declared (VERSION stays
  `1.1.0-dev`).
- Syncthing archive/backup (`ARCHIVE_DAYS_ROOT`) — placeholder env path only.
- Label / target store (`LABEL_ROOT`) — placeholder env path only.
- Import / restore tooling — not implemented.

## [1.1.0-dev] - 2026-06-17

### Added
- Frozen repository structure contract (`docs/REPO_STRUCTURE.md`) with an amendment
  log and a structure-enforcing test (`tests/test_repo_structure.py`).
- AI-agent governance: `AGENTS.md` and `.github/copilot-instructions.md` defining the
  required read order, hard constraints, and Definition of Done.
- Status and versioning docs: `docs/PROJECT_STATUS.md`, `docs/VERSIONING.md`,
  `VERSION`, and this `CHANGELOG.md`.
- Linux server deployment documentation: `docs/LINUX_SERVER.md`, `docs/DEPLOYMENT.md`,
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
