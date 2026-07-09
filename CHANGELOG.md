# Changelog

All notable changes to CryptoRecorder are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/).
Version policy is described in the [Versioning Policy](#versioning-policy) section below.

## Versioning Policy

> Content merged from the former `docs/CHANGELOG.md`.

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

---

## [Unreleased]

### Added
- `.githooks/commit-msg` — conventional commits enforcement hook. Blocks commits
  whose message does not match `<type>(<scope>): <subject>` where type is one of
  `feat|fix|docs|style|refactor|perf|test|chore`, subject starts lowercase, and
  subject has no trailing period. Blank-line-before-body is also enforced.
- `AGENTS.md` Section 7 "Commit message style" — full format reference, type table,
  subject rules, valid/invalid examples, and bypass guidance.
- Docs structure consolidation: merged 9 small docs into 4 comprehensive files
  (all content preserved). `ARCHITECTURE.md` absorbs `STORAGE_ARCHITECTURE.md`
  and `GUARANTEES.md`; `OPERATIONS.md` absorbs `DEPLOYMENT.md`, `LINUX_SERVER.md`,
  and `SCHEMAS.md`; `IMPLEMENTATION_AUDIT.md` absorbs `REPO_CLEANUP_AUDIT.md`,
  `FEATURE_STORE_REQUIREMENTS_AUDIT.md`, and `STORAGE_SIZE_AUDIT.md`;
  `CHANGELOG.md` absorbs versioning policy from `VERSIONING.md`.
- `docs/README.md` rewritten as navigation index with "Where to update what" table.
- "No New Docs Files" rule added to `AGENTS.md` Section 2 and `docs/REPO_STRUCTURE.md`:
  docs/ is fixed at 14 files; new content goes in existing sections.
- Mandatory change-audit infrastructure (`AGENTS.md` Section 6,
  `docs/AI_WORKFLOW.md` Step 7, `docs/CHANGE_AUDIT.md`, `INSTALL.md` Section 7):
  every non-trivial commit now requires an append-only audit entry in
  `docs/CHANGE_AUDIT.md` before the change is considered complete.
- `validation/audit_change_compliance.py` — compliance-check CLI that inspects
  staged or branch-diff changes and reports PASS/FAIL. Supports
  `--staged` (pre-commit hook mode), `--base <branch>`, and `--allow-docs-only`.
- `.githooks/pre-commit` — git hook that runs `audit_change_compliance --staged`
  before every commit and blocks if compliance fails. Activate per-clone with
  `git config core.hooksPath .githooks` (see `INSTALL.md`).

### Changed
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
- Docs: `docs/IMPLEMENTATION_AUDIT.md`, `docs/IMPLEMENTATION_AUDIT.md`.
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
- Status and versioning docs: `docs/PROJECT_STATUS.md`, `docs/CHANGELOG.md`,
  `VERSION`, and this `CHANGELOG.md`.
- Linux server deployment documentation: `docs/OPERATIONS.md`, `docs/OPERATIONS.md`,
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
