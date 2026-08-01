"""Authoritative uv metadata, separation, and deployment migration guards."""
from __future__ import annotations

import ast
import hashlib
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
LOCK = ROOT / "uv.lock"
DEPLOY = ROOT / "scripts" / "deploy_linux_server.sh"


def _project() -> dict:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _lock_sha() -> str:
    return hashlib.sha256(LOCK.read_bytes()).hexdigest()


def _declared_names(values: list[str]) -> set[str]:
    return {
        re.split(r"[<>=!~;\[]", value, maxsplit=1)[0].strip().lower().replace("_", "-")
        for value in values
    }


def test_pyproject_is_nonpackaged_and_version_file_remains_authoritative() -> None:
    project = _project()
    assert project["tool"]["uv"] == {"package": False, "default-groups": []}
    assert "build-system" not in project
    assert project["project"]["requires-python"] == ">=3.12,<3.15"
    assert project["project"]["version"] == "0"
    assert project["project"]["version"] != (ROOT / "VERSION").read_text().strip()


def test_dependency_groups_are_explicit_and_nonoverlapping() -> None:
    project = _project()
    production = _declared_names(project["project"]["dependencies"])
    reconstruction = _declared_names(project["project"]["optional-dependencies"]["reconstruction"])
    development = _declared_names(project["dependency-groups"]["dev"])
    assert production == {"aiohttp", "numpy", "pyarrow", "zstandard"}
    assert reconstruction == {"nautilus-trader"}
    assert project["project"]["optional-dependencies"]["reconstruction"] == [
        "nautilus_trader==1.225.0"
    ]
    assert development == {"pytest", "pytest-asyncio"}
    assert not (production & reconstruction)
    assert not (production & development)
    assert not (reconstruction & development)


def test_direct_third_party_imports_are_classified() -> None:
    stdlib = set(sys.stdlib_module_names)
    first_party = {
        "converter", "pipeline", "stores", "validation", "replay_schema",
        *(path.stem for path in ROOT.glob("*.py")),
    }
    production_files = [
        *ROOT.glob("*.py"),
        *ROOT.glob("pipeline/*.py"),
        *ROOT.glob("stores/*.py"),
        ROOT / "converter" / "readers.py",
        ROOT / "converter" / "spool.py",
        ROOT / "converter" / "depth_repartition.py",
    ]
    reconstruction_files = [
        ROOT / "convert_day.py",
        *ROOT.glob("converter/*.py"),
        ROOT / "validation" / "replay_catalog_reconstruct.py",
        ROOT / "validation" / "catalog_compare.py",
        ROOT / "validation" / "catalog_inspect.py",
        ROOT / "validation" / "validate_catalog_equivalence.py",
    ]

    def imports(paths: list[Path]) -> set[str]:
        found: set[str] = set()
        for path in paths:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    found.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    found.add(node.module.split(".")[0])
        return found - stdlib - first_party - {"__future__"}

    production_imports = imports(production_files) - {"nautilus_trader"}
    reconstruction_imports = imports(reconstruction_files)
    assert production_imports <= {"aiohttp", "pyarrow", "zstandard"}
    assert {"aiohttp", "pyarrow", "zstandard"} <= production_imports
    # Replay deep-integrity uses PyArrow's ``to_numpy`` contract. PyArrow
    # deliberately does not depend on NumPy, so the production environment
    # must declare it even though the call is reached through PyArrow.
    assert "numpy" in _declared_names(_project()["project"]["dependencies"])
    assert "nautilus_trader" in reconstruction_imports
    assert "pandas" not in production_imports | reconstruction_imports


def test_production_import_boundary_does_not_require_nautilus() -> None:
    code = """
import sys
sys.modules['nautilus_trader'] = None
import recorder
import disk_monitor
import pipeline.replay_lifecycle
import pipeline.daily_build
import pipeline.build_replay_store
import stores.replay_reader
import validation.audit_replay_store
"""
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


def test_lock_is_fresh_tracked_and_contains_no_unsafe_sources() -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "uv.lock"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    # The file is intentionally untracked until the checkpoint commit, but it
    # must not be ignored and will be explicitly staged before compliance.
    assert subprocess.run(
        ["git", "check-ignore", "-q", "uv.lock"], cwd=ROOT, check=False
    ).returncode != 0
    if tracked.returncode != 0:
        assert LOCK.is_file()
    text = LOCK.read_text(encoding="utf-8")
    assert "source = { virtual = \".\" }" in text
    assert "file:///home/" not in text
    assert "editable =" not in text
    assert "git+" not in text
    assert "@" not in "\n".join(line for line in text.splitlines() if "registry" in line)
    before = _lock_sha()
    result = subprocess.run(["uv", "lock", "--check"], cwd=ROOT, check=False)
    assert result.returncode == 0
    assert _lock_sha() == before


def test_frozen_selection_commands_do_not_change_lock(tmp_path: Path) -> None:
    before = _lock_sha()
    for args in (
        ["--no-default-groups"],
        ["--no-default-groups", "--extra", "reconstruction"],
        ["--no-default-groups", "--extra", "reconstruction", "--group", "dev"],
    ):
        env = os.environ.copy()
        env["UV_PROJECT_ENVIRONMENT"] = str(tmp_path / ("env-" + str(len(args))))
        result = subprocess.run(
            ["uv", "sync", "--frozen", "--dry-run", *args],
            cwd=ROOT, env=env, capture_output=True, text=True, check=False,
        )
        assert result.returncode == 0, result.stderr
        assert _lock_sha() == before


def test_deploy_uses_frozen_uv_and_never_pip_requirements() -> None:
    text = DEPLOY.read_text(encoding="utf-8")
    assert "uv.lock" in text
    assert "lock --check" in text
    assert "sync" in text and "--frozen --no-default-groups" in text
    assert "UV_PROJECT_ENVIRONMENT" in text
    assert "pip install" not in text
    assert "requirements.txt" not in text
    assert 'VENV="$APP_DIR/.venv"' in text
    assert "--uv-bin" in text and "--migrate-venv" in text


def test_missing_uv_and_legacy_environment_fail_closed(tmp_path: Path) -> None:
    missing = subprocess.run(
        ["bash", str(DEPLOY), "--dry-run", "--no-systemd", "--uv-bin", str(tmp_path / "missing")],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    assert missing.returncode != 0
    assert "uv is required" in missing.stderr

    app = tmp_path / "app"
    (app / ".venv").mkdir(parents=True)
    legacy = subprocess.run(
        ["bash", str(DEPLOY), "--dry-run", "--no-systemd", "--app-dir", str(app)],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    assert legacy.returncode != 0
    assert "legacy/unrecognized" in legacy.stderr
    assert (app / ".venv").is_dir()


def test_migration_dry_run_is_nonmutating_and_interruption_is_visible(tmp_path: Path) -> None:
    app = tmp_path / "app"
    legacy = app / ".venv"
    legacy.mkdir(parents=True)
    marker = legacy / "legacy-marker"
    marker.write_text("preserve", encoding="utf-8")
    result = subprocess.run(
        [
            "bash", str(DEPLOY), "--dry-run", "--no-systemd",
            "--app-dir", str(app), "--migrate-venv",
        ],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "timestamped backup" in result.stdout
    assert marker.read_text() == "preserve"
    assert not list(app.glob(".venv.backup.*"))

    (app / ".venv.candidate.interrupted").mkdir()
    interrupted = subprocess.run(
        [
            "bash", str(DEPLOY), "--dry-run", "--no-systemd",
            "--app-dir", str(app), "--migrate-venv",
        ],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    assert interrupted.returncode != 0
    assert "interrupted .venv migration evidence" in interrupted.stderr


def test_deploy_contains_candidate_validation_backup_and_rollback_guards() -> None:
    text = DEPLOY.read_text(encoding="utf-8")
    for proof in (
        ".venv.candidate.",
        ".venv.backup.",
        ".venv.failed.",
        "assert_services_inactive",
        "systemctl is-active --quiet",
        "systemctl unavailable and --no-systemd selected",
        "validate_production_environment",
        "post-promotion validation failed",
        "previous .venv restored",
        "must not be a symlink",
        "unsafe ownership",
        "--app-dir must be the checkout containing this deploy script",
    ):
        assert proof in text


def test_dependency_smoke_root_refuses_existing_repo_and_symlink_paths(
    tmp_path: Path,
) -> None:
    from validation.validate_dependency_environment import (
        DependencyEnvironmentError,
        _new_external_root,
    )

    with pytest.raises(DependencyEnvironmentError, match="outside the repository"):
        _new_external_root(ROOT / "forbidden-smoke")
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(DependencyEnvironmentError, match="new non-symlink"):
        _new_external_root(existing)
    link = tmp_path / "link"
    link.symlink_to(existing, target_is_directory=True)
    with pytest.raises(DependencyEnvironmentError, match="new non-symlink"):
        _new_external_root(link)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(tmp_path, target_is_directory=True)
    (tmp_path / "child").mkdir()
    with pytest.raises(DependencyEnvironmentError, match="parent is missing or unsafe"):
        _new_external_root(linked_parent / "child" / "smoke")


def test_environment_validator_uses_exact_locked_selection() -> None:
    text = (ROOT / "validation" / "validate_dependency_environment.py").read_text(
        encoding="utf-8"
    )
    assert '"sync", "--check", "--frozen"' in text
    assert 'exact_env["UV_PROJECT_ENVIRONMENT"]' in text
