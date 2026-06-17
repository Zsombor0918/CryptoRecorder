"""
Structural enforcement tests.

This file fails if the repository violates docs/REPO_STRUCTURE.md.
Run with normal pytest; no real data required.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
TESTS = ROOT / "tests"
PIPELINE = ROOT / "pipeline"
VALIDATION = ROOT / "validation"


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
    """audit_feature_store and audit_replay_store must live in validation/, not pipeline/."""
    assert not (PIPELINE / "audit_feature_store.py").exists(), (
        "pipeline/audit_feature_store.py must not exist; use validation/audit_feature_store.py"
    )
    assert not (PIPELINE / "audit_replay_store.py").exists(), (
        "pipeline/audit_replay_store.py must not exist; use validation/audit_replay_store.py"
    )


def test_pipeline_does_not_contain_equivalence_module() -> None:
    """validate_catalog_equivalence must live in validation/, not pipeline/."""
    assert not (PIPELINE / "validate_catalog_equivalence.py").exists(), (
        "pipeline/validate_catalog_equivalence.py must not exist; "
        "use validation/validate_catalog_equivalence.py"
    )


# ---------------------------------------------------------------------------
# validation/ must contain the audit/equivalence modules
# ---------------------------------------------------------------------------

def test_validation_contains_audit_and_equivalence_modules() -> None:
    """The three audit/equivalence modules must exist in validation/."""
    for name in (
        "audit_feature_store.py",
        "audit_replay_store.py",
        "validate_catalog_equivalence.py",
        "catalog_compare.py",
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

def test_validation_audit_feature_store_cli_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "validation.audit_feature_store", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


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
    """After the move, docs must use validation.audit_* paths, not pipeline.audit_*."""
    forbidden_patterns = [
        "pipeline.audit_feature_store",
        "pipeline.audit_replay_store",
        "pipeline.validate_catalog_equivalence",
    ]
    for md_path in DOCS.glob("*.md"):
        text = md_path.read_text()
        for pattern in forbidden_patterns:
            assert pattern not in text, (
                f"{md_path.name} still references '{pattern}'. "
                f"Update to validation.{pattern.split('.', 1)[1]}"
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
