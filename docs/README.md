# CryptoRecorder — Docs Index

This directory contains all project documentation. The structure is **fixed at
12 files**. Before creating a new file, use the "Where to update what" table
below to find the right home for your content.

---

## Navigation

| File | What it covers | Read when |
|------|---------------|-----------|
| [REPO_STRUCTURE.md](REPO_STRUCTURE.md) | Frozen folder/file contract, package roles, allowed files | Before any change |
| [PROJECT_STATUS.md](PROJECT_STATUS.md) | Validated vs deferred truth, evidence records | Before any change |
| [AI_WORKFLOW.md](AI_WORKFLOW.md) | Step-by-step agent workflow (9 steps) | Before any change |
| [CHANGE_AUDIT.md](CHANGE_AUDIT.md) | Append-only change audit log | Before/after every commit |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, pipeline paths, components, storage layers, guarantees | Understanding the system |
| [OPERATIONS.md](OPERATIONS.md) | Quick-ref commands, deployment script, Linux server paths, service groups, state file schemas | Running or deploying |
| [VALIDATION.md](VALIDATION.md) | Validation layers, test commands, audit CLIs | Validating changes |
| [IMPLEMENTATION_AUDIT.md](IMPLEMENTATION_AUDIT.md) | Ground-truth of what exists, cleanup history, requirements audit, storage sizes | Auditing status |
| [REPLAY_STORE.md](REPLAY_STORE.md) | Replay store schema, build CLI, audit CLI | Working on replay |
| [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) | Daily orchestrator, systemd timers | Working on daily pipeline |
| [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md) | Full-L2 replay plan and gate status | full_l2 work only |

Root-level docs: [AGENTS.md](../AGENTS.md) · [CHANGELOG.md](../CHANGELOG.md) ·
[README.md](../README.md) · [INSTALL.md](../INSTALL.md)

---

## Where to update what

When making a change, use this table to find the right file for your update.
**Do not create a new file** — add a new section to the right existing file.

| Content type | Update this file |
|--------------|-----------------|
| Status changes (validated / deferred) | `PROJECT_STATUS.md` |
| Architecture or design decisions | `ARCHITECTURE.md` |
| Storage layer schemas or design | `ARCHITECTURE.md` |
| System guarantees | `ARCHITECTURE.md` |
| Operations quick reference | `OPERATIONS.md` |
| Deployment script flags or targets | `OPERATIONS.md` |
| Linux server paths, service groups | `OPERATIONS.md` |
| State file schemas (heartbeat, startup, convert) | `OPERATIONS.md` |
| Validation layer documentation | `VALIDATION.md` |
| Ground-truth of what exists | `IMPLEMENTATION_AUDIT.md` |
| Historical audit / cleanup record | `IMPLEMENTATION_AUDIT.md` |
| Requirements status (met / partial / deferred) | `IMPLEMENTATION_AUDIT.md` |
| Storage size measurements | `IMPLEMENTATION_AUDIT.md` |
| Replay store schema, CLI, behavior | `REPLAY_STORE.md` |
| Daily build orchestrator, timers | `DAILY_BUILD_PIPELINE.md` |
| Full-L2 plan, gate progress | `FULL_L2_REPLAY_CATALOG_PLAN.md` |
| Change audit entries | `CHANGE_AUDIT.md` (append-only) |
| Version history, versioning policy | `../CHANGELOG.md` |
| Folder/file contract changes | `REPO_STRUCTURE.md` (amendment log) |
| Agent workflow procedure | `AI_WORKFLOW.md` |

---

## The No New Docs rule

The 12-file structure above is designed to be **comprehensive and permanent**.

Before creating any new file in `docs/`:
1. Find the matching row in the table above.
2. Add a new section to that file.
3. Only if genuinely no existing file fits, amend `REPO_STRUCTURE.md`
   with a clear justification (same process as adding a new package).

This rule mirrors the "No New Top-Level Packages Without Contract Amendment"
rule for Python code. It prevents documentation sprawl and ensures every agent
can find and update the right file.
