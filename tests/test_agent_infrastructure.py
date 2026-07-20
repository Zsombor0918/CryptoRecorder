"""
Tests for the AI-agent + deployment infrastructure.

Validates that the governance, status, versioning, and deployment files exist,
cross-link correctly, stay honest about deferred work (no false `full_l2` or
Syncthing claims), and that the deploy script dry-run is safe.

Run with normal pytest; no real data required.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
GITHUB = ROOT / ".github"
DEPLOY_SCRIPT = ROOT / "scripts" / "deploy_linux_server.sh"

DEPLOY_TARGETS = ["all", "recorder", "legacy-converter", "replay-build"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_markdown() -> list[Path]:
    """Every tracked Markdown doc (root, docs/, .github/)."""
    files = list(ROOT.glob("*.md")) + list(DOCS.glob("*.md")) + list(GITHUB.glob("*.md"))
    return sorted(set(files))


def _normalize(text: str) -> str:
    """Lowercase, strip markdown emphasis/backticks, collapse whitespace."""
    text = text.lower().replace("`", "").replace("*", "")
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------
# Required files exist
# ---------------------------------------------------------------------------

REQUIRED_FILES = [
    ROOT / "AGENTS.md",
    ROOT / "VERSION",
    ROOT / "CHANGELOG.md",
    DOCS / "PROJECT_STATUS.md",
    DOCS / "OPERATIONS.md",
    DOCS / "AI_WORKFLOW.md",
    GITHUB / "copilot-instructions.md",
    ROOT / "systemd" / "cryptorecorder.env.example",
    DEPLOY_SCRIPT,
]


@pytest.mark.parametrize("path", REQUIRED_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_required_infrastructure_file_exists(path: Path) -> None:
    assert path.is_file(), f"required infrastructure file missing: {path.relative_to(ROOT)}"


# ---------------------------------------------------------------------------
# Cross-links
# ---------------------------------------------------------------------------

def test_readme_links_docs_readme() -> None:
    text = (ROOT / "README.md").read_text()
    assert "docs/README.md" in text, "README.md must link to docs/README.md"


def test_docs_readme_links_key_docs() -> None:
    text = (DOCS / "README.md").read_text()
    for target in (
        "PROJECT_STATUS.md",
        "REPO_STRUCTURE.md",
        "AI_WORKFLOW.md",
        "OPERATIONS.md",
    ):
        assert target in text, f"docs/README.md must link {target}"


def test_changelog_contains_current_version() -> None:
    version = (ROOT / "VERSION").read_text().strip()
    changelog = (ROOT / "CHANGELOG.md").read_text()
    assert version in changelog, f"CHANGELOG.md must mention current version {version!r}"


# ---------------------------------------------------------------------------
# Honesty guards
# ---------------------------------------------------------------------------

def test_docs_do_not_reference_validators_active_path() -> None:
    """No doc may reference the removed validators/ package as an active path."""
    forbidden = [
        "from validators.",
        "import validators.",
        "python validators/",
        "python -m validators.",
    ]
    for md in _all_markdown():
        text = md.read_text()
        for pattern in forbidden:
            assert pattern not in text, (
                f"{md.relative_to(ROOT)} references active validators path '{pattern}'"
            )


# Affirmative "it is done" claim phrases (word-boundary matched after normalization).
_CLAIM_PHRASES = [
    "is implemented",
    "is validated",
    "is working",
    "works",
    "is complete",
    "is done",
    "is ready",
    "is production",
    "is available",
    "is supported",
    "is enabled",
    "is configured",
    "is deployed",
    "is running",
]


def _claim_present(normalized: str, token: str, claim: str) -> bool:
    """True if `token claim` reads as an affirmative "it is done" assertion.

    Matches preceded by a deferral qualifier (e.g. "until full-L2 is validated")
    are treated as honest deferral statements, not claims of completion.
    """
    deferral = ("until", "not", "once", "when", "after", "pending", "before", "unvalidated")
    pattern = r"(?<![\w])" + re.escape(token) + r"\s+" + re.escape(claim) + r"(?![\w])"
    for match in re.finditer(pattern, normalized):
        prefix_words = normalized[max(0, match.start() - 16):match.start()].split()
        if any(word in deferral for word in prefix_words):
            continue
        return True
    return False


def test_no_doc_claims_full_l2_done() -> None:
    """The `full_l2` replay profile is implemented and *synthetic*-validated, but
    real-data semantic equivalence vs convert_day.py is still pending. No doc may
    claim it is validated / complete / done / production-ready (the v2.0.0 gate),
    though "implemented" / "supported" is now accurate.

    Only the underscore profile identifier `full_l2` is guarded. Prose about the
    *validated* ``convert_day.py`` full-L2 catalog path (written "full-L2") is allowed.
    """
    overclaims = [
        "is validated",
        "is complete",
        "is done",
        "is ready",
        "is production",
        "works",
    ]
    for md in _all_markdown():
        normalized = _normalize(md.read_text())
        for claim in overclaims:
            assert not _claim_present(normalized, "full_l2", claim), (
                f"{md.relative_to(ROOT)} claims 'full_l2 {claim}', but full_l2 real-data "
                "validation against convert_day.py is still pending (not yet v2.0.0)"
            )


def test_no_doc_claims_syncthing_done() -> None:
    """Syncthing is not implemented; no doc may describe it as implemented/enabled."""
    for md in _all_markdown():
        normalized = _normalize(md.read_text())
        for claim in _CLAIM_PHRASES:
            assert not _claim_present(normalized, "syncthing", claim), (
                f"{md.relative_to(ROOT)} claims 'syncthing {claim}', but Syncthing is not implemented"
            )


# ---------------------------------------------------------------------------
# Deploy script
# ---------------------------------------------------------------------------

def test_deploy_script_has_safe_header() -> None:
    text = DEPLOY_SCRIPT.read_text()
    assert text.startswith("#!/usr/bin/env bash"), "deploy script must have a bash shebang"
    assert "set -euo pipefail" in text, "deploy script must use 'set -euo pipefail'"


def test_deploy_script_documents_targets_and_flags() -> None:
    text = DEPLOY_SCRIPT.read_text()
    for target in DEPLOY_TARGETS:
        assert target in text, f"deploy script must mention target '{target}'"
    for flag in ("--dry-run", "--no-systemd", "--install-only", "--enable", "--start", "--restart"):
        assert flag in text, f"deploy script must support flag '{flag}'"


def test_deploy_script_dry_run_all_is_safe() -> None:
    """`--target all --dry-run --no-systemd` must exit 0 and touch nothing."""
    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "--target", "all", "--dry-run", "--no-systemd"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "skipped (--no-systemd)" in result.stdout


@pytest.mark.parametrize("target", DEPLOY_TARGETS)
def test_deploy_script_dry_run_each_target(target: str) -> None:
    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "--target", target, "--dry-run", "--no-systemd"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"target {target} dry-run failed: {result.stderr}"


def test_deploy_script_rejects_invalid_target() -> None:
    result = subprocess.run(
        ["bash", str(DEPLOY_SCRIPT), "--target", "syncthing", "--dry-run", "--no-systemd"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0, "deploy script must reject unknown targets"


# Every legacy/renamed systemd unit that predates the current canonical
# names and must be stopped/disabled/removed on upgrade so it can't keep
# firing its old (removed or renamed) command on a stale schedule.
LEGACY_STALE_UNITS = [
    "cryptorecorder-feature-build.timer",
    "cryptorecorder-feature-build.service",
    "crypto-recorder.service",
    "nautilus-convert.timer",
    "nautilus-convert.service",
    "cryptorecorder-daily-build.timer",
    "cryptorecorder-daily-build.service",
]


def test_deploy_script_cleans_up_every_legacy_unit_name() -> None:
    """cleanup_stale_units() must remove every unit name this repo has ever
    shipped and later renamed/removed, not just the pre-issue-#17
    feature-build units."""
    text = DEPLOY_SCRIPT.read_text()
    for unit in LEGACY_STALE_UNITS:
        assert unit in text, (
            f"deploy script must list legacy unit '{unit}' for stale-unit cleanup"
        )


def test_deploy_script_renders_user_app_dir_and_env_file_flags() -> None:
    """--user/--app-dir/--env-file/--data-root must actually be rendered into
    the dry-run plan (not silently ignored placeholders)."""
    result = subprocess.run(
        [
            "bash", str(DEPLOY_SCRIPT),
            "--target", "recorder",
            "--dry-run",
            "--user", "customuser",
            "--app-dir", "/opt/customdir",
            "--data-root", "/srv/customdata",
            "--env-file", "/etc/customenv/cr.env",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "customuser" in out, "rendered --user value must appear in the dry-run plan"
    assert "/opt/customdir" in out, "rendered --app-dir value must appear in the dry-run plan"
    assert "/srv/customdata" in out, "rendered --data-root value must appear in the dry-run plan"
    assert "/etc/customenv/cr.env" in out, "rendered --env-file value must appear in the dry-run plan"
    # The script must not silently fall back to the hardcoded defaults.
    assert "/home/zsom/services/CryptoRecorder" not in out
    assert "/data/cryptorecorder" not in out
    assert "/etc/cryptorecorder/cryptorecorder.env" not in out


def test_deploy_script_never_overwrites_existing_env_file() -> None:
    """create_env_file() must check for an existing env file and return
    before any render/copy when one is already present."""
    text = DEPLOY_SCRIPT.read_text()
    match = re.search(r"create_env_file\(\) \{.*?\n\}\n", text, flags=re.DOTALL)
    assert match, "create_env_file() function not found in deploy script"
    body = match.group(0)
    assert '-f "$ENV_FILE"' in body, "must check whether the env file already exists"
    assert "never overwrite" in body.lower()
    # The existing-file branch must return before any sed/tee render happens.
    exists_idx = body.index('-f "$ENV_FILE"')
    render_idx = body.index("sed \\") if "sed \\" in body else body.index("sed ")
    return_idx = body.index("return 0", exists_idx)
    assert exists_idx < return_idx < render_idx, (
        "existing-file check must return before the env file is rendered/copied"
    )
