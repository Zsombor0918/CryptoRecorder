"""
Structural enforcement tests.

This file fails if the repository violates docs/REPO_STRUCTURE.md.
Run with normal pytest; no real data required.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
TESTS = ROOT / "tests"
PIPELINE = ROOT / "pipeline"
STORES = ROOT / "stores"
VALIDATION = ROOT / "validation"


# ---------------------------------------------------------------------------
# Exact root-level file/docs enforcement (docs/REPO_STRUCTURE.md contract)
# ---------------------------------------------------------------------------

# Kept in sync with the "Root-Level Files (allowed)" table in
# docs/REPO_STRUCTURE.md. If you add/remove a root file, update both.
ALLOWED_ROOT_PY_FILES = {
    "recorder.py",
    "phase2_depth.py",
    "native_trades.py",
    "storage.py",
    "binance_universe.py",
    "health_monitor.py",
    "disk_monitor.py",
    "time_utils.py",
    "convert_day.py",
    "config.py",
    "validate.py",
    "debug_futures_trade_ws.py",
}

ALLOWED_ROOT_OTHER_FILES = {
    "README.md",
    "INSTALL.md",
    "requirements.txt",
    "pytest.ini",
    "AGENTS.md",
    "VERSION",
    "CHANGELOG.md",
    ".gitignore",
}

# Kept in sync with the fixed 12-file docs/ table in docs/REPO_STRUCTURE.md.
ALLOWED_DOCS_FILES = {
    "README.md",
    "REPO_STRUCTURE.md",
    "PROJECT_STATUS.md",
    "AI_WORKFLOW.md",
    "CHANGE_AUDIT.md",
    "ARCHITECTURE.md",
    "OPERATIONS.md",
    "VALIDATION.md",
    "IMPLEMENTATION_AUDIT.md",
    "REPLAY_STORE.md",
    "DAILY_BUILD_PIPELINE.md",
    "FULL_L2_REPLAY_CATALOG_PLAN.md",
}


def test_exact_allowed_root_python_files() -> None:
    """Only the .py files listed in docs/REPO_STRUCTURE.md may exist at the
    repository root. This prevents unauthorized root entrypoints (e.g. a
    stray inspect_catalog.py) from silently reappearing."""
    actual_py = {p.name for p in ROOT.glob("*.py")}
    unauthorized = actual_py - ALLOWED_ROOT_PY_FILES
    missing = ALLOWED_ROOT_PY_FILES - actual_py
    assert not unauthorized, (
        f"Unauthorized root-level Python files: {sorted(unauthorized)}. "
        "Edit docs/REPO_STRUCTURE.md before adding new root entrypoints, or "
        "delete the file if unused."
    )
    assert not missing, (
        f"Expected root-level Python files are missing: {sorted(missing)}. "
        "Either restore them or update docs/REPO_STRUCTURE.md and this test."
    )


def test_exact_allowed_root_other_files() -> None:
    """Only the non-Python files listed in docs/REPO_STRUCTURE.md may exist
    at the repository root (directories are checked separately)."""
    actual = {
        p.name
        for p in ROOT.iterdir()
        if p.is_file() and not p.name.startswith(".") or p.name == ".gitignore"
    }
    # Exclude .py files (checked above) and any local/generated files.
    actual = {name for name in actual if not name.endswith(".py")}
    ignored_generated = {"recorder.log"}
    actual -= ignored_generated
    unauthorized = actual - ALLOWED_ROOT_OTHER_FILES
    assert not unauthorized, (
        f"Unauthorized root-level files: {sorted(unauthorized)}. "
        "Edit docs/REPO_STRUCTURE.md before adding new root-level project files."
    )


def test_exact_docs_file_set() -> None:
    """docs/ must contain exactly the fixed 12-file set. This prevents stale
    files (e.g. docs/GUARANTEES.md, docs/FEATURE_STORE.md) from silently
    reappearing via a merge."""
    actual = {p.name for p in DOCS.glob("*.md")}
    unauthorized = actual - ALLOWED_DOCS_FILES
    missing = ALLOWED_DOCS_FILES - actual
    assert not unauthorized, (
        f"Unauthorized docs/ files: {sorted(unauthorized)}. docs/ is fixed at "
        "12 files per docs/REPO_STRUCTURE.md; consolidate content into an "
        "existing file or amend the contract."
    )
    assert not missing, f"Expected docs/ files are missing: {sorted(missing)}."


def test_no_stray_python_modules_in_docs() -> None:
    """docs/ must never contain importable Python (redundant with
    test_docs_contains_no_python_modules, kept as an explicit named guard)."""
    assert not list(DOCS.rglob("*.py"))


# ---------------------------------------------------------------------------
# Forbidden directories
# ---------------------------------------------------------------------------

def test_validators_package_does_not_exist() -> None:
    """validators/ was merged into converter/ and validation/. It must not return."""
    assert not (ROOT / "validators").exists(), (
        "validators/ must not exist. Use converter/trade_coverage.py and "
        "validation/ instead. See docs/REPO_STRUCTURE.md."
    )


def test_no_unauthorized_top_level_packages() -> None:
    """Only the packages listed in docs/REPO_STRUCTURE.md are allowed."""
    allowed = {
        "converter",
        "pipeline",
        "stores",
        "validation",
        "scripts",
        "tests",
        "docs",
        "systemd",
        # non-package dirs
        "data_raw",
        "replay_store",
        "feature_store",
        "catalog_jobs",
        "validation_reports",
        "daily_reports",
        "state",
        "meta",
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
    }
    actual = {
        item.name
        for item in ROOT.iterdir()
        if item.is_dir() and not item.name.startswith(".")
        # allow hidden and venv dirs
    }
    # Also allow any .staging_* dirs (generated, should be gitignored)
    actual = {name for name in actual if not name.startswith(".staging_")}
    unauthorized = actual - allowed
    assert not unauthorized, (
        f"Unauthorized top-level directories: {sorted(unauthorized)}. "
        "Edit docs/REPO_STRUCTURE.md before adding new root-level packages."
    )


# ---------------------------------------------------------------------------
# pipeline/ must not contain audit or equivalence modules
# ---------------------------------------------------------------------------

def test_pipeline_does_not_contain_audit_modules() -> None:
    """audit_replay_store must live in validation/, not pipeline/."""
    assert not (PIPELINE / "audit_replay_store.py").exists(), (
        "pipeline/audit_replay_store.py must not exist; use validation/audit_replay_store.py"
    )


def test_pipeline_does_not_contain_feature_store_modules() -> None:
    """The feature-store subsystem was removed (issue #17); it must not return
    in pipeline/, stores/, or validation/."""
    assert not (PIPELINE / "build_feature_store.py").exists(), (
        "pipeline/build_feature_store.py must not exist; the feature-store "
        "subsystem was removed. See docs/ARCHITECTURE.md."
    )
    assert not (VALIDATION / "audit_feature_store.py").exists(), (
        "validation/audit_feature_store.py must not exist; the feature-store "
        "subsystem was removed. See docs/ARCHITECTURE.md."
    )
    for name in ("feature_schema.py", "feature_calc.py", "feature_writer.py"):
        assert not (STORES / name).exists(), (
            f"stores/{name} must not exist; the feature-store subsystem was removed."
        )
    assert not (TESTS / "test_feature_store.py").exists(), (
        "tests/test_feature_store.py must not exist; the feature-store subsystem was removed."
    )


def test_pipeline_does_not_contain_generate_catalog_cli() -> None:
    """generate_catalog is no longer a product/runtime CLI (issue #17). Its
    reconstruction logic moved to validation/replay_catalog_reconstruct.py,
    which has no CLI entrypoint."""
    assert not (PIPELINE / "generate_catalog.py").exists(), (
        "pipeline/generate_catalog.py must not exist; use "
        "validation/replay_catalog_reconstruct.py (validation-only, no CLI)."
    )
    assert (VALIDATION / "replay_catalog_reconstruct.py").exists(), (
        "validation/replay_catalog_reconstruct.py is missing."
    )


def test_pipeline_does_not_contain_equivalence_module() -> None:
    """validate_catalog_equivalence must live in validation/, not pipeline/."""
    assert not (PIPELINE / "validate_catalog_equivalence.py").exists(), (
        "pipeline/validate_catalog_equivalence.py must not exist; "
        "use validation/validate_catalog_equivalence.py"
    )


def test_config_does_not_contain_feature_store_roots() -> None:
    """FEATURE_ROOT, LABEL_ROOT, and CATALOG_JOBS_ROOT were removed (issue
    #17) and must not be reintroduced as live config.py attributes."""
    config_text = (ROOT / "config.py").read_text()
    for name in ("FEATURE_ROOT", "LABEL_ROOT", "CATALOG_JOBS_ROOT"):
        assert not re.search(rf"^{name}\s*=", config_text, re.MULTILINE), (
            f"config.py must not define {name}; the feature-store subsystem "
            "was removed (issue #17)."
        )


def test_no_feature_store_cli_flags_in_pipeline_daily_build() -> None:
    """daily_build must not expose feature-store-era CLI flags."""
    text = (PIPELINE / "daily_build.py").read_text()
    for flag in ("--steps", "--timeframes", "--feature-root"):
        assert flag not in text, (
            f"pipeline/daily_build.py must not accept '{flag}'; "
            "daily_build is replay-only (issue #17)."
        )


def test_no_feature_build_systemd_units() -> None:
    """The feature-build systemd units were deleted (issue #17)."""
    systemd = ROOT / "systemd"
    for name in (
        "cryptorecorder-feature-build.service",
        "cryptorecorder-feature-build.timer",
    ):
        assert not (systemd / name).exists(), (
            f"systemd/{name} must not exist; the feature-store subsystem was removed."
        )


def test_no_validators_imports_in_source() -> None:
    """No Python source file (outside .venv) may import the removed
    validators/ package."""
    forbidden = re.compile(r"^\s*(from|import)\s+validators\b", re.MULTILINE)
    leaked: list[str] = []
    for path in ROOT.rglob("*.py"):
        if ".venv" in path.parts or "__pycache__" in path.parts:
            continue
        text = path.read_text(errors="ignore")
        if forbidden.search(text):
            leaked.append(str(path.relative_to(ROOT)))
    assert not leaked, f"Files still import the removed validators/ package: {leaked}"


# ---------------------------------------------------------------------------
# validation/ must contain the audit/equivalence modules
# ---------------------------------------------------------------------------

def test_validation_contains_audit_and_equivalence_modules() -> None:
    """The audit/equivalence/reconstruction modules must exist in validation/."""
    for name in (
        "audit_replay_store.py",
        "validate_catalog_equivalence.py",
        "catalog_compare.py",
        "replay_catalog_reconstruct.py",
    ):
        assert (VALIDATION / name).exists(), (
            f"validation/{name} is missing. See docs/REPO_STRUCTURE.md."
        )


# ---------------------------------------------------------------------------
# converter/ must contain trade_coverage.py
# ---------------------------------------------------------------------------

def test_converter_contains_trade_coverage() -> None:
    """trade_coverage.py must live in converter/, not validators/."""
    assert (ROOT / "converter" / "trade_coverage.py").exists(), (
        "converter/trade_coverage.py is missing. "
        "It was moved from validators/trade_coverage.py."
    )


# ---------------------------------------------------------------------------
# No tests outside tests/
# ---------------------------------------------------------------------------

def test_no_test_files_outside_tests_dir() -> None:
    """All test_*.py files must live under tests/."""
    leaked = [
        path
        for path in ROOT.rglob("test_*.py")
        if not path.is_relative_to(TESTS)
        and ".venv" not in path.parts
        and "__pycache__" not in path.parts
    ]
    assert not leaked, (
        f"Test files found outside tests/: {[str(p) for p in leaked]}"
    )


# ---------------------------------------------------------------------------
# docs/ must not contain importable Python
# ---------------------------------------------------------------------------

def test_docs_contains_no_python_modules() -> None:
    """docs/ must contain only Markdown and plaintext, no .py files."""
    py_files = [path for path in DOCS.rglob("*.py")]
    assert not py_files, (
        f"Python files found in docs/: {[str(p) for p in py_files]}"
    )


# ---------------------------------------------------------------------------
# CLI modules are importable under their new homes
# ---------------------------------------------------------------------------

def test_validation_audit_replay_store_cli_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "validation.audit_replay_store", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_validation_validate_catalog_equivalence_cli_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "validation.validate_catalog_equivalence", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Docs must not reference old (now-deleted) module paths
# ---------------------------------------------------------------------------

def test_docs_do_not_reference_pipeline_audit_modules() -> None:
    """After the move, docs must use validation.audit_* paths, not pipeline.audit_*,
    and must not reference the removed feature-store subsystem or the removed
    pipeline.generate_catalog product CLI."""
    forbidden_patterns = [
        "pipeline.audit_replay_store",
        "pipeline.validate_catalog_equivalence",
        "pipeline.generate_catalog",
        "pipeline.build_feature_store",
        "validation.audit_feature_store",
    ]
    for md_path in DOCS.glob("*.md"):
        text = md_path.read_text()
        for pattern in forbidden_patterns:
            assert pattern not in text, (
                f"{md_path.name} still references '{pattern}'. "
                f"Update to validation.{pattern.split('.', 1)[1]}"
            )


def test_docs_do_not_claim_deleted_converter_systemd_files_exist() -> None:
    """
    systemd/cryptorecorder-convert.service and .timer were deleted in PR #18
    finalization. Current-state documentation must never claim they still
    exist in the repo as manual/reference templates -- that sends operators
    looking for files that are gone. docs/CHANGE_AUDIT.md is an append-only
    historical log and is exempt: past dated entries may describe a
    superseded decision as long as they are not the file being checked here.
    """
    assert not (ROOT / "systemd" / "cryptorecorder-convert.service").exists(), (
        "systemd/cryptorecorder-convert.service must stay deleted (PR #18)."
    )
    assert not (ROOT / "systemd" / "cryptorecorder-convert.timer").exists(), (
        "systemd/cryptorecorder-convert.timer must stay deleted (PR #18)."
    )

    forbidden_normalized_phrases = [
        "kept in the repo as manual-only reference templates",
        "kept in the repo as manual/reference templates",
        "kept in the repo as manual",
        "converter systemd templates remain",
    ]

    # Wording varies ("templates remain in the repo", "template files remain
    # in the repo as manual/reference templates only", etc.) so also match a
    # loose regex: "remain(s)" near "repo" and "manual" within the same
    # sentence-ish window, scoped to a converter/systemd context.
    forbidden_pattern = re.compile(
        r"convert(?:er)?[^.]{0,80}remain[^.]{0,60}(?:repo|manual)"
        r"|remain[^.]{0,80}repo[^.]{0,60}manual[^.]{0,40}template",
        re.IGNORECASE,
    )

    current_state_docs = [
        ROOT / "CHANGELOG.md",
        ROOT / "INSTALL.md",
        ROOT / "AGENTS.md",
        *[p for p in DOCS.glob("*.md") if p.name != "CHANGE_AUDIT.md"],
    ]
    for doc_path in current_state_docs:
        if not doc_path.exists():
            continue
        normalized = re.sub(r"\s+", " ", doc_path.read_text())
        for phrase in forbidden_normalized_phrases:
            assert phrase not in normalized, (
                f"{doc_path.relative_to(ROOT)} still claims deleted converter "
                f"systemd templates remain in the repo (found: {phrase!r}). "
                "Update to state they were deleted in PR #18."
            )
        match = forbidden_pattern.search(normalized)
        assert match is None, (
            f"{doc_path.relative_to(ROOT)} still claims deleted converter "
            f"systemd templates remain in the repo "
            f"(matched: {match.group(0) if match else None!r}). "
            "Update to state they were deleted in PR #18."
        )


def test_docs_do_not_reference_validators_package() -> None:
    """After removal, no docs should reference the old validators/ package as an active path."""
    # These are patterns that would indicate a live import or command reference.
    # Mentions in a "was removed" or historical context are acceptable.
    forbidden_patterns = [
        "from validators.",
        "import validators.",
        "python validators/",
        "python -m validators.",
    ]
    for md_path in DOCS.glob("*.md"):
        text = md_path.read_text()
        for pattern in forbidden_patterns:
            assert pattern not in text, (
                f"{md_path.name} still references active path '{pattern}'. "
                "The validators/ package was removed."
            )
    # Also check INSTALL.md
    install_text = (ROOT / "INSTALL.md").read_text()
    for pattern in forbidden_patterns:
        assert pattern not in install_text, (
            f"INSTALL.md still references active path '{pattern}'. "
            "Update to validation.* module paths."
        )
