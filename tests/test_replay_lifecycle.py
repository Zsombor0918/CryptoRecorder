"""Focused replay lifecycle ownership and cross-date reconciliation tests."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from pipeline.replay_lifecycle import (
    ReplayBuildActiveError,
    ReplayLifecycleSafetyError,
    acquire_replay_build_lock,
    atomic_write_json,
    reconcile_replay_root,
)


def _lock(root: Path, run_id: str = "test-run"):
    return acquire_replay_build_lock(
        replay_root=root / "replay",
        data_root=root / "raw",
        report_root=root / "reports",
        command=["pytest"],
        run_id=run_id,
    )


def _candidate(root: Path, name: str, *, valid: bool = True) -> Path:
    path = root / "replay" / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / name
    path.mkdir(parents=True)
    if valid:
        (path / "VALID").write_text("yes")
    return path


def _valid(path: Path) -> bool:
    return (path / "VALID").is_file()


def test_exclusive_lock_rejects_concurrent_owner_and_releases(tmp_path: Path) -> None:
    with _lock(tmp_path) as first:
        first.assert_held(tmp_path / "replay")
        with pytest.raises(ReplayBuildActiveError, match="build already active"):
            with _lock(tmp_path, "second"):
                pass
    with _lock(tmp_path, "after-clean-exit") as later:
        later.assert_held()


def test_kernel_lock_truth_ignores_stale_metadata(tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    lock_path = replay / ".lifecycle" / "build.lock"
    lock_path.parent.mkdir(parents=True)
    lock_path.write_text(json.dumps({"pid": 999999, "start_utc": "old"}))
    with _lock(tmp_path) as context:
        assert context.metadata["pid"] == os.getpid()


def test_lock_symlink_fails_closed(tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    lifecycle = replay / ".lifecycle"
    lifecycle.mkdir(parents=True)
    target = tmp_path / "outside"
    target.write_text("outside")
    (lifecycle / "build.lock").symlink_to(target)
    with pytest.raises(ReplayLifecycleSafetyError):
        with _lock(tmp_path):
            pass


def test_group_writable_lifecycle_directory_fails_closed(tmp_path: Path) -> None:
    lifecycle = tmp_path / "replay" / ".lifecycle"
    lifecycle.mkdir(parents=True)
    lifecycle.chmod(0o770)
    with pytest.raises(ReplayLifecycleSafetyError, match="ownership/type/mode"):
        with _lock(tmp_path):
            pass


def test_lock_releases_after_process_death(tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    raw = tmp_path / "raw"
    reports = tmp_path / "reports"
    code = (
        "import time; from pathlib import Path; "
        "from pipeline.replay_lifecycle import acquire_replay_build_lock; "
        f"cm=acquire_replay_build_lock(replay_root=Path({str(replay)!r}), "
        f"data_root=Path({str(raw)!r}), report_root=Path({str(reports)!r})); "
        "ctx=cm.__enter__(); print('LOCKED', flush=True); time.sleep(60)"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None and child.stdout.readline().strip() == "LOCKED"
    child.send_signal(signal.SIGKILL)
    child.wait(timeout=5)
    with _lock(tmp_path, "after-death") as context:
        context.assert_held()


def test_nested_orchestration_uses_same_context_without_deadlock(tmp_path: Path) -> None:
    with _lock(tmp_path) as context:
        context.assert_held()
        context.assert_held(tmp_path / "replay")


def test_old_date_staging_is_quarantined(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    staging = _candidate(tmp_path, ".staging_2026-01-01_ADAUSDT")
    with _lock(tmp_path) as context:
        actions = reconcile_replay_root(context)
    assert not staging.exists()
    assert any(action["action"] == "stale_staging_quarantined" for action in actions)
    quarantines = list(staging.parent.glob(".quarantine_2026-01-01_ADAUSDT_stale_staging.*"))
    assert len(quarantines) == 1


def test_single_valid_backup_restores_missing_canonical(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    backup = _candidate(tmp_path, ".backup_2026-01-02_ADAUSDT")
    with _lock(tmp_path) as context:
        actions = reconcile_replay_root(context)
    canonical = backup.parent / "date=2026-01-02"
    assert canonical.exists() and not backup.exists()
    assert any(action["action"] == "backup_restored" for action in actions)


def test_valid_canonical_wins_over_valid_backup_safely(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    canonical = _candidate(tmp_path, "date=2026-01-02")
    backup = _candidate(tmp_path, ".backup_2026-01-02_ADAUSDT")
    with _lock(tmp_path) as context:
        actions = reconcile_replay_root(context)
    assert canonical.exists() and not backup.exists()
    assert any(action["action"] == "obsolete_valid_backup_removed" for action in actions)


def test_ambiguous_multiple_backups_fail_closed(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    _candidate(tmp_path, ".backup_2026-01-02_ADAUSDT")
    _candidate(tmp_path, ".backup_2026-01-02_ADAUSDT_second")
    with _lock(tmp_path) as context:
        with pytest.raises(ReplayLifecycleSafetyError, match="multiple backups"):
            reconcile_replay_root(context)


def test_symlink_and_unknown_artifact_fail_closed(tmp_path: Path) -> None:
    symbol = tmp_path / "replay" / "venue=BINANCE_SPOT" / "symbol=ADAUSDT"
    symbol.mkdir(parents=True)
    (symbol / "unexpected").write_text("x")
    with _lock(tmp_path) as context:
        with pytest.raises(ReplayLifecycleSafetyError, match="unknown"):
            reconcile_replay_root(context)
    (symbol / "unexpected").unlink()
    (symbol / "date=2026-01-01").symlink_to(tmp_path)
    with _lock(tmp_path, "symlink") as context:
        with pytest.raises(ReplayLifecycleSafetyError, match="symlink"):
            reconcile_replay_root(context)


def test_interrupted_restore_remains_recoverable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    canonical = _candidate(tmp_path, "date=2026-01-02", valid=False)
    backup = _candidate(tmp_path, ".backup_2026-01-02_ADAUSDT", valid=True)
    real_replace = os.replace
    failed_once = False

    def fail_restore(source, destination):
        nonlocal failed_once
        if Path(source) == backup and not failed_once:
            failed_once = True
            raise OSError("injected interruption")
        return real_replace(source, destination)

    monkeypatch.setattr("pipeline.replay_lifecycle.os.replace", fail_restore)
    with _lock(tmp_path) as context:
        with pytest.raises(OSError, match="injected"):
            reconcile_replay_root(context)
    assert backup.exists() and not canonical.exists()
    monkeypatch.setattr("pipeline.replay_lifecycle.os.replace", real_replace)
    with _lock(tmp_path, "next-run") as context:
        actions = reconcile_replay_root(context)
    assert canonical.exists() and not backup.exists()
    assert any(action["action"] == "backup_restored" for action in actions)


def test_quarantine_is_never_deleted(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", _valid)
    quarantine = _candidate(tmp_path, ".quarantine_2026-01-02_ADAUSDT_evidence")
    with _lock(tmp_path) as context:
        actions = reconcile_replay_root(context)
    assert quarantine.exists()
    assert any(action["action"] == "quarantine_preserved" for action in actions)


def test_reconciliation_bounds_fail_closed(tmp_path: Path) -> None:
    _candidate(tmp_path, "date=2026-01-01")
    with _lock(tmp_path) as context:
        with pytest.raises(ReplayLifecycleSafetyError, match="max entries"):
            reconcile_replay_root(context, max_entries=1)


def test_atomic_report_write_leaves_no_temp_and_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "reports" / "run.json"
    atomic_write_json(target, {"status": "success"})
    assert json.loads(target.read_text()) == {"status": "success"}
    assert not list(target.parent.glob(".run.json.*.tmp"))
    target.unlink()
    outside = tmp_path / "outside"
    outside.write_text("preserve")
    target.symlink_to(outside)
    with pytest.raises(ReplayLifecycleSafetyError):
        atomic_write_json(target, {"status": "wrong"})
    assert outside.read_text() == "preserve"
