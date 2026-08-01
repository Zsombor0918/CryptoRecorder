"""Audit change compliance before commit or PR.

Checks whether staged (or branch-diff) changes satisfy the mandatory audit rules
defined in AGENTS.md Section 6 and docs/CHANGE_AUDIT.md.

Usage
-----
    # Check staged changes (pre-commit hook mode)
    python -m validation.audit_change_compliance --staged

    # Check all changes relative to a base branch
    python -m validation.audit_change_compliance --base main

    # Suppress the FAIL when only docs files changed
    python -m validation.audit_change_compliance --staged --allow-docs-only

Exit codes
----------
    0  PASS
    1  FAIL (missing audit entry or required updates)
    2  Configuration / git error
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

# Packages / paths whose changes always require an audit entry and careful
# docs review.
_CORE_CODE_PREFIXES: tuple[str, ...] = (
    "stores/",
    "pipeline/",
    "converter/",
    "validation/",
    "recorder.py",
    "config.py",
    "storage.py",
    "phase2_depth.py",
    "native_trades.py",
    "time_utils.py",
)

_DEPLOYMENT_PREFIXES: tuple[str, ...] = (
    "systemd/",
    "scripts/",
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",  # deletion/restoration remains compliance-relevant
)

_REPLAY_CATALOG_PATTERNS: tuple[str, ...] = (
    "stores/replay",
    "pipeline/build_replay",
    "validation/replay_catalog_reconstruct",
    "validation/validate_catalog",
    "validation/audit_replay",
)

# Paths whose changes are considered "docs-only" (may be exempt with
# --allow-docs-only).
_DOCS_ONLY_SUFFIXES: tuple[str, ...] = (".md", ".rst", ".txt")
_DOCS_ONLY_PREFIXES: tuple[str, ...] = ("docs/", "README", "INSTALL", "CHANGELOG",
                                         "AGENTS", "VERSION")

# Paths that are always exempt regardless of mode (e.g. the audit entry itself,
# generated lock files, etc.)
_ALWAYS_EXEMPT_PATHS: tuple[str, ...] = (
    "docs/CHANGE_AUDIT.md",
    ".githooks/",
)


def _is_docs_only(path: str) -> bool:
    return (
        any(path.startswith(p) for p in _DOCS_ONLY_PREFIXES)
        or any(path.endswith(s) for s in _DOCS_ONLY_SUFFIXES)
    )


def _is_exempt(path: str) -> bool:
    return any(path.startswith(p) for p in _ALWAYS_EXEMPT_PATHS)


def _is_core_code(path: str) -> bool:
    return any(path.startswith(p) for p in _CORE_CODE_PREFIXES)


def _is_deployment(path: str) -> bool:
    return any(path.startswith(p) for p in _DEPLOYMENT_PREFIXES)


def _is_replay_catalog(path: str) -> bool:
    return any(p in path for p in _REPLAY_CATALOG_PATTERNS)


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _run_git(*args: str) -> list[str]:
    """Run a git command and return stripped non-empty output lines."""
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed:\n{result.stderr.strip()}"
        )
    return [line for line in result.stdout.splitlines() if line.strip()]


def _staged_files() -> list[str]:
    lines = _run_git("diff", "--cached", "--name-only", "--diff-filter=ACDMR")
    return lines


def _branch_diff_files(base: str) -> list[str]:
    lines = _run_git("diff", "--name-only", "--diff-filter=ACDMR", f"{base}...HEAD")
    return lines


# ---------------------------------------------------------------------------
# Audit entry parsing
# ---------------------------------------------------------------------------

_AUDIT_FILE = Path("docs/CHANGE_AUDIT.md")

# Phrases that indicate "no docs update required" (case-insensitive search in
# the audit entry text).
_NO_DOCS_JUSTIFICATION = re.compile(
    r"no docs update required because",
    re.IGNORECASE,
)

# Phrases that indicate "changelog not applicable".
_NO_CHANGELOG_JUSTIFICATION = re.compile(
    r"changelog[^\n]*not\s+(required|applicable|needed)|"
    r"no changelog",
    re.IGNORECASE,
)

# Checked-box CHANGELOG line: "[x] CHANGELOG.md"
_CHANGELOG_CHECKED = re.compile(
    r"\[x\]\s+CHANGELOG\.md",
    re.IGNORECASE,
)

# Tests section contains something other than whitespace / placeholder.
_TESTS_SECTION = re.compile(
    r"###\s+Tests\s+run\s*\n([\s\S]*?)(?=\n###|\Z)",
    re.IGNORECASE,
)

# Validation CLIs section contains something other than whitespace / "none".
_VALIDATION_SECTION = re.compile(
    r"###\s+Validation\s+CLIs\s+run\s*\n([\s\S]*?)(?=\n###|\Z)",
    re.IGNORECASE,
)

# Latest entry header: first "## YYYY-MM-DD — ..."
_ENTRY_HEADER = re.compile(r"^##\s+\d{4}-\d{2}-\d{2}", re.MULTILINE)

# Docs reviewed section: look for checked boxes.
_DOCS_REVIEWED_AGENTS = re.compile(r"\[x\]\s+AGENTS\.md", re.IGNORECASE)
_DOCS_REVIEWED_REPO_STRUCTURE = re.compile(
    r"\[x\]\s+docs/REPO_STRUCTURE\.md", re.IGNORECASE
)
_DOCS_REVIEWED_PROJECT_STATUS = re.compile(
    r"\[x\]\s+docs/PROJECT_STATUS\.md", re.IGNORECASE
)


def _latest_audit_entry() -> str | None:
    """Return the text of the most-recent (topmost) audit entry, or None."""
    if not _AUDIT_FILE.exists():
        return None
    text = _AUDIT_FILE.read_text(encoding="utf-8")
    matches = list(_ENTRY_HEADER.finditer(text))
    if not matches:
        return None
    start = matches[0].start()
    end = matches[1].start() if len(matches) > 1 else len(text)
    return text[start:end]


def _section_has_content(section_text: str, *, skip_words: tuple[str, ...] = ()) -> bool:
    """Return True if a section body has meaningful content beyond placeholders."""
    body = section_text.strip()
    if not body:
        return False
    lowered = body.lower()
    # Accept as "has content" if it contains a code block or a bullet
    if "```" in body or "- " in body or lowered.startswith("pytest") or "python" in lowered:
        return True
    # Reject pure placeholder values
    placeholder_patterns = ("none required", "skipped", "n/a", "<exact commands>",
                             "# none", "# skipped")
    return not any(p in lowered for p in placeholder_patterns)


# ---------------------------------------------------------------------------
# Compliance checks
# ---------------------------------------------------------------------------

class _Failure:
    def __init__(self, message: str, is_warning: bool = False) -> None:
        self.message = message
        self.is_warning = is_warning

    def __str__(self) -> str:
        tag = "WARN" if self.is_warning else "FAIL"
        return f"  [{tag}] {self.message}"


def _check(
    changed_files: list[str],
    *,
    allow_docs_only: bool,
    audit_file_changed: bool,
) -> list[_Failure]:
    failures: list[_Failure] = []

    # Split files into categories
    non_exempt = [f for f in changed_files if not _is_exempt(f)]
    non_docs = [f for f in non_exempt if not _is_docs_only(f)]
    has_core_code = any(_is_core_code(f) for f in non_docs)
    has_deployment = any(_is_deployment(f) for f in non_docs)
    has_replay_catalog = any(_is_replay_catalog(f) for f in non_exempt)

    # Determine if this is a docs-only change
    docs_only = len(non_docs) == 0 and len(non_exempt) > 0

    # Trivially exempt?
    if not non_exempt:
        return []  # nothing to check (e.g. only CHANGE_AUDIT.md itself)

    # If docs-only and caller allows it, pass immediately.
    if docs_only and allow_docs_only:
        return []

    # -----------------------------------------------------------------------
    # Rule 1: CHANGE_AUDIT.md must be in the staged/diff set
    # -----------------------------------------------------------------------
    if not audit_file_changed:
        failures.append(_Failure(
            "docs/CHANGE_AUDIT.md was not updated. "
            "Every non-trivial change requires an audit entry. "
            "Add an entry using the template in docs/CHANGE_AUDIT.md."
        ))

    # Parse the latest audit entry for all subsequent checks.
    entry = _latest_audit_entry()

    if not entry:
        failures.append(_Failure(
            "No audit entries found in docs/CHANGE_AUDIT.md. "
            "Add an entry using the required template."
        ))
        return failures  # no point checking further

    # -----------------------------------------------------------------------
    # Rule 2: CHANGELOG.md must be changed OR entry justifies it
    # -----------------------------------------------------------------------
    changelog_changed = "CHANGELOG.md" in changed_files
    changelog_checked = bool(_CHANGELOG_CHECKED.search(entry))
    changelog_justified = bool(_NO_CHANGELOG_JUSTIFICATION.search(entry))

    if not changelog_changed and not changelog_checked and not changelog_justified:
        failures.append(_Failure(
            "CHANGELOG.md was not updated and no justification was found in the "
            "latest audit entry. Either update CHANGELOG.md or add "
            "'CHANGELOG not required because: <reason>' to the audit entry."
        ))

    # -----------------------------------------------------------------------
    # Rule 3: "No docs update required" must be explicit if docs weren't updated
    # -----------------------------------------------------------------------
    doc_files_changed = any(
        f.startswith("docs/") or f in ("README.md", "INSTALL.md", "AGENTS.md")
        for f in changed_files
        if not _is_exempt(f)
    )
    if has_core_code and not doc_files_changed:
        if not _NO_DOCS_JUSTIFICATION.search(entry):
            failures.append(_Failure(
                "Core code changed (stores/, pipeline/, converter/, validation/, "
                "or recorder/config) but no docs were updated and the audit entry "
                "does not contain 'No docs update required because: <reason>'. "
                "Update the relevant docs or justify skipping them."
            ))

    # -----------------------------------------------------------------------
    # Rule 4: Key contract docs must be marked as reviewed for core code changes
    # -----------------------------------------------------------------------
    if has_core_code or has_deployment:
        if not _DOCS_REVIEWED_AGENTS.search(entry):
            failures.append(_Failure(
                "Audit entry does not show AGENTS.md was reviewed ([x] AGENTS.md). "
                "Core/deployment changes require this."
            ))
        if not _DOCS_REVIEWED_REPO_STRUCTURE.search(entry):
            failures.append(_Failure(
                "Audit entry does not show docs/REPO_STRUCTURE.md was reviewed. "
                "Core/deployment changes require this."
            ))

    # -----------------------------------------------------------------------
    # Rule 5: replay/catalog code must list the relevant audit CLIs
    # -----------------------------------------------------------------------
    if has_replay_catalog:
        val_match = _VALIDATION_SECTION.search(entry)
        if val_match:
            val_body = val_match.group(1)
            if not _section_has_content(val_body, skip_words=()):
                failures.append(_Failure(
                    "Replay/catalog/feature code was changed but the 'Validation CLIs run' "
                    "section in the audit entry appears empty or contains only placeholders. "
                    "List the audit/validation CLI commands and their output."
                ))
        else:
            failures.append(_Failure(
                "Replay/catalog/feature code was changed but the audit entry is "
                "missing a '### Validation CLIs run' section. "
                "Add this section and list the relevant commands."
            ))

    # -----------------------------------------------------------------------
    # Rule 6: Tests must be listed
    # -----------------------------------------------------------------------
    tests_match = _TESTS_SECTION.search(entry)
    if tests_match:
        tests_body = tests_match.group(1)
        if not _section_has_content(tests_body):
            failures.append(_Failure(
                "The '### Tests run' section in the audit entry appears empty or "
                "contains only placeholders. List the exact test commands run."
            ))
    else:
        failures.append(_Failure(
            "The audit entry is missing a '### Tests run' section. "
            "Add this section with the exact test commands run."
        ))

    return failures


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _report(changed_files: list[str], failures: list[_Failure], *, mode: str) -> int:
    print()
    print("=" * 68)
    print("  Change Compliance Audit")
    print(f"  Mode: {mode}")
    print(f"  Changed files: {len(changed_files)}")
    print("=" * 68)

    if not changed_files:
        print()
        print("  No changed files detected. Nothing to check.")
        print()
        print("  RESULT: PASS (no changes)")
        print("=" * 68)
        return 0

    print()
    print("  Changed files:")
    for f in sorted(changed_files):
        print(f"    {f}")

    hard_failures = [f for f in failures if not f.is_warning]
    warnings = [f for f in failures if f.is_warning]

    if warnings:
        print()
        print("  Warnings:")
        for w in warnings:
            print(str(w))

    if hard_failures:
        print()
        print("  Failures:")
        for fail in hard_failures:
            print(str(fail))
        print()
        print("  RESULT: FAIL")
        print()
        print("  To fix: update docs/CHANGE_AUDIT.md with a complete audit entry,")
        print("  then re-run: python -m validation.audit_change_compliance --staged")
        print("=" * 68)
        return 1

    print()
    print("  RESULT: PASS")
    print("=" * 68)
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit whether staged or branch-diff changes satisfy the mandatory "
            "change audit rules defined in AGENTS.md Section 6."
        )
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--staged",
        action="store_true",
        help="Check staged (git index) changes. Used by the pre-commit hook.",
    )
    mode_group.add_argument(
        "--base",
        metavar="BRANCH",
        help="Check all commits on HEAD not yet in BRANCH (e.g. --base main).",
    )
    parser.add_argument(
        "--allow-docs-only",
        action="store_true",
        help=(
            "If the change set contains only documentation files (*.md, docs/, "
            "README, INSTALL, CHANGELOG, AGENTS), treat it as PASS without "
            "requiring an audit entry. Useful for pure-docs PRs."
        ),
    )
    args = parser.parse_args(argv)

    if not args.staged and not args.base:
        # Default to --staged for backwards compatibility and hook use.
        args.staged = True

    try:
        if args.staged:
            changed_files = _staged_files()
            mode_label = "staged"
        else:
            changed_files = _branch_diff_files(args.base)
            mode_label = f"vs {args.base}"
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    audit_file_changed = "docs/CHANGE_AUDIT.md" in changed_files

    failures = _check(
        changed_files,
        allow_docs_only=args.allow_docs_only,
        audit_file_changed=audit_file_changed,
    )

    return _report(changed_files, failures, mode=mode_label)


if __name__ == "__main__":
    sys.exit(main())
