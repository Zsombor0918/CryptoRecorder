# AI Workflow

This is the step-by-step procedure an AI agent must follow when working in
CryptoRecorder. It operationalizes the rules in [../AGENTS.md](../AGENTS.md).

## The 9-step workflow

1. **Read the contract.** Read [REPO_STRUCTURE.md](REPO_STRUCTURE.md),
   [PROJECT_STATUS.md](PROJECT_STATUS.md), and the relevant feature doc before
   touching anything. Confirm what is validated vs deferred.
2. **Scope the task.** Identify which package(s) the change belongs in
   (`converter/`, `pipeline/`, `validation/`, `stores/`, `scripts/`) and whether the
   task **explicitly** allows raw/recorder changes. If it does not, raw side stays
   frozen.
3. **Check boundaries.** Confirm the change does not add a top-level folder, put an
   audit CLI in `pipeline/`, put a build CLI in `validation/`, or add business logic
   to `scripts/`. If a new folder is genuinely needed, amend `REPO_STRUCTURE.md` first.
4. **Make the smallest change.** Implement the minimal edit that satisfies the task.
   Prefer adding explicit files over rewriting shared ones.
5. **Run the required tests.** Run the test set for the task type (see the table in
   [../AGENTS.md](../AGENTS.md)). For replay/feature/catalog changes, also run the
   matching audit/validation CLI and capture the numbers.
6. **Update docs + changelog.** Update the affected `docs/`, `README.md` if needed,
   `PROJECT_STATUS.md` if status changed, and add a `CHANGELOG.md [Unreleased]` entry.
7. **Write the change audit entry.** Before commit or PR handoff, append an entry to
   [../docs/CHANGE_AUDIT.md](CHANGE_AUDIT.md) using the required template. The entry
   must list: docs reviewed, docs updated (or explicit "no docs impact" reasoning),
   CHANGELOG updated (or explicit justification), status/validation impact,
   tests run with commands, validation CLIs run with commands, and known limitations
   or out-of-scope work. Then run `python -m validation.audit_change_compliance --staged`
   and confirm it reports PASS. **This step is not optional.**
   Then write the commit message following the **conventional commits** format
   (see [AGENTS.md](../AGENTS.md) Section 7): `<type>(<scope>): <subject>` with
   imperative lowercase subject, blank line before body, and no trailing period.
   The `.githooks/commit-msg` hook enforces this and will block a malformed message.
8. **Keep status honest.** Do not promote deferred work to validated without recorded
   evidence. Do not claim full-L2, Syncthing, archive, or import work as done.
9. **Report what was NOT done.** State explicitly what is out of scope or left for
   later, and surface any uncertainty instead of guessing.

## Examples of BAD behavior (do not do these)

- **Skipping the change audit entry.** Every non-trivial commit requires an entry in
  `docs/CHANGE_AUDIT.md` and a passing `python -m validation.audit_change_compliance
  --staged`. Claiming "done" without the audit entry violates the Definition of Done.
- **Writing a non-conforming commit message.** Messages must follow
  `<type>(<scope>): <subject>` with a lowercase subject and no trailing period.
  The `.githooks/commit-msg` hook blocks commits that don't conform. Fix with
  `git commit --amend`. See AGENTS.md Section 7 for the full reference.
- **Recreating `validators/`.** That package was removed; audit CLIs live in
  `validation/`. Re-adding `validators/` violates the frozen structure.
- **Putting an audit CLI in `pipeline/`.** `pipeline/` is build/transform only.
  Audit/compare/inspect tools belong in `validation/`.
- **Changing the recorder schema during replay/feature work.** A task about the
  replay or feature store must not modify `recorder.py`, `phase2_depth.py`,
  `native_trades.py`, `storage.py`, or the `data_raw/` layout.
- **Adding Syncthing during deployment work.** Deployment covers recorder, converter,
  replay-build, and feature-build only. Syncthing/archive/import are not implemented
  and must not be wired up.
- **Claiming that `full_l2` already works.** The `replay_store → full_l2` catalog path
  is deferred until validated against `convert_day.py`. Never describe it as working.
- **Inventing a deployment path or data root.** Use the canonical values in
  [OPERATIONS.md](OPERATIONS.md) (`APP_DIR`, `DATA_BASE`, `ENV_FILE`). Do not
  guess production paths.

When any step is unclear: **stop and ask.**
