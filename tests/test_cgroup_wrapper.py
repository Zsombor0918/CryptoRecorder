"""Fail-closed argument and evidence guards for the cgroup stage wrapper."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_under_cgroup.sh"


def _run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


def _fake_cgroup_env(
    tmp_path: Path,
    *,
    peak_bytes: int = 8192,
    oom: int = 0,
    oom_kill: int = 0,
    memory_max_bytes: int = 10 * 1024**3,
    swap_max_bytes: int = 0,
    omit_file: str = "",
    include_member: bool = True,
    update_delay: float = 0.0,
) -> tuple[dict[str, str], Path]:
    """Return an explicitly guarded fake systemd/cgroup environment.

    No real systemd scope is created. The fake launches the wrapper's
    handshake shim as an ordinary child and exposes deterministic cgroup-v2
    control files under pytest's temporary directory.
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    fake_root = tmp_path / "fake_cgroup"
    fake_systemd_run = tmp_path / "fake-systemd-run"
    fake_systemd_run.write_text(
        """#!/usr/bin/env bash
set -uo pipefail
unit=""
while (( $# )); do
  case "$1" in
    --unit=*) unit="${1#--unit=}"; shift ;;
    -p) shift 2 ;;
    --) shift; break ;;
    *) shift ;;
  esac
done
[[ -n "$unit" ]] || exit 90
cgroup_dir="$FAKE_CGROUP_ROOT/${unit}.scope"
mkdir -p "$cgroup_dir" || exit 91
if [[ "$FAKE_OMIT_FILE" != "memory.max" ]]; then
  printf '%s\\n' "$FAKE_MEMORY_MAX_BYTES" > "$cgroup_dir/memory.max"
fi
if [[ "$FAKE_OMIT_FILE" != "memory.swap.max" ]]; then
  printf '%s\\n' "$FAKE_SWAP_MAX_BYTES" > "$cgroup_dir/memory.swap.max"
fi
if [[ "$FAKE_OMIT_FILE" != "memory.current" ]]; then
  printf '4096\\n' > "$cgroup_dir/memory.current"
fi
if [[ "$FAKE_OMIT_FILE" != "memory.peak" ]]; then
  printf '%s\\n' "$FAKE_INITIAL_PEAK_BYTES" > "$cgroup_dir/memory.peak"
fi
if [[ "$FAKE_OMIT_FILE" != "memory.events" ]]; then
  printf 'low 0\\nhigh 0\\nmax 0\\noom 0\\noom_kill 0\\n' > "$cgroup_dir/memory.events"
fi
"$@" &
shim_pid=$!
if [[ "$FAKE_INCLUDE_MEMBER" == "1" ]]; then
  printf '%s\\n' "$shim_pid" > "$cgroup_dir/cgroup.procs"
else
  printf '999999\\n' > "$cgroup_dir/cgroup.procs"
fi
if [[ "$FAKE_UPDATE_DELAY" != "0.0" ]]; then
  (
    sleep "$FAKE_UPDATE_DELAY"
    if [[ "$FAKE_OMIT_FILE" != "memory.peak" ]]; then
      printf '%s\\n' "$FAKE_FINAL_PEAK_BYTES" > "$cgroup_dir/memory.peak"
    fi
    if [[ "$FAKE_OMIT_FILE" != "memory.events" ]]; then
      printf 'low 0\\nhigh 0\\nmax 1\\noom %s\\noom_kill %s\\n' \
        "$FAKE_FINAL_OOM" "$FAKE_FINAL_OOM_KILL" > "$cgroup_dir/memory.events"
    fi
  ) &
fi
if wait "$shim_pid"; then
  exit 0
else
  exit $?
fi
"""
    )
    fake_systemd_run.chmod(0o755)
    env = dict(os.environ)
    env.update(
        {
            "CRYPTO_RECORDER_ALLOW_CGROUP_TEST_OVERRIDES": "1",
            "CRYPTO_RECORDER_CGROUP_TEST_ROOT": str(fake_root),
            "CRYPTO_RECORDER_SYSTEMD_RUN_TEST_BIN": str(fake_systemd_run),
            "FAKE_CGROUP_ROOT": str(fake_root),
            "FAKE_MEMORY_MAX_BYTES": str(memory_max_bytes),
            "FAKE_SWAP_MAX_BYTES": str(swap_max_bytes),
            "FAKE_INITIAL_PEAK_BYTES": str(peak_bytes),
            "FAKE_FINAL_PEAK_BYTES": str(peak_bytes),
            "FAKE_FINAL_OOM": str(oom),
            "FAKE_FINAL_OOM_KILL": str(oom_kill),
            "FAKE_OMIT_FILE": omit_file,
            "FAKE_INCLUDE_MEMBER": "1" if include_member else "0",
            "FAKE_UPDATE_DELAY": str(update_delay),
        }
    )
    return env, fake_root


def test_requires_complete_argument_set() -> None:
    result = _run()

    assert result.returncode == 2
    assert "usage:" in result.stderr


def test_rejects_unsafe_unit_name(tmp_path: Path) -> None:
    result = _run("10G", str(tmp_path), "../bad", "--", "true")

    assert result.returncode == 2
    assert "invalid unit_name" in result.stderr


def test_rejects_missing_command(tmp_path: Path) -> None:
    result = _run("10G", str(tmp_path), "safe-unit", "--")

    assert result.returncode == 2
    assert "missing command" in result.stderr


def test_refuses_to_overwrite_existing_evidence(tmp_path: Path) -> None:
    existing = tmp_path / "safe-unit.result.txt"
    existing.write_text("preserved")

    result = _run("10G", str(tmp_path), "safe-unit", "--", "true")

    assert result.returncode == 2
    assert "refusing to overwrite existing evidence" in result.stderr
    assert existing.read_text() == "preserved"


def test_test_only_overrides_require_explicit_guard(tmp_path: Path) -> None:
    env = dict(os.environ)
    env["CRYPTO_RECORDER_CGROUP_TEST_ROOT"] = str(tmp_path / "fake")

    result = _run(
        "10G", str(tmp_path / "evidence"), "safe-unit", "--", "true", env=env
    )

    assert result.returncode == 2
    assert "test-only cgroup/systemd overrides require" in result.stderr


def test_verified_fake_scope_persists_exact_peak_wall_and_zero_oom(
    tmp_path: Path,
) -> None:
    env, _ = _fake_cgroup_env(tmp_path, peak_bytes=123456)
    evidence = tmp_path / "evidence"

    result = _run(
        "10G",
        str(evidence),
        "safe-unit",
        "--",
        "bash",
        "-c",
        "sleep 0.02",
        env=env,
    )

    assert result.returncode == 0, result.stderr
    summary = (evidence / "safe-unit.result.txt").read_text()
    assert "requested_memory_max_bytes=10737418240" in summary
    assert "effective_memory_max_bytes=10737418240" in summary
    assert "effective_memory_swap_max_bytes=0" in summary
    assert "exit_code=0" in summary
    assert "peak_bytes=123456" in summary
    assert "oom=0" in summary
    assert "oom_kill=0" in summary
    assert "telemetry_status=passed" in summary
    wall_line = next(line for line in summary.splitlines() if line.startswith("wall_seconds="))
    assert float(wall_line.split("=", 1)[1]) > 0
    events = (evidence / "safe-unit.memory_events.log").read_text()
    assert "oom 0" in events
    assert "oom_kill 0" in events
    assert (evidence / "safe-unit.memory_samples.log").stat().st_size > 0


def test_preserves_nonzero_child_exit(tmp_path: Path) -> None:
    env, _ = _fake_cgroup_env(tmp_path)

    result = _run(
        "10G",
        str(tmp_path / "evidence"),
        "safe-unit",
        "--",
        "bash",
        "-c",
        "exit 7",
        env=env,
    )

    assert result.returncode == 7
    assert "exit_code=7" in result.stdout


def test_positive_oom_fails_even_when_child_succeeds(tmp_path: Path) -> None:
    env, _ = _fake_cgroup_env(
        tmp_path,
        peak_bytes=9999,
        oom=1,
        oom_kill=1,
        update_delay=0.05,
    )

    result = _run(
        "10G",
        str(tmp_path / "evidence"),
        "safe-unit",
        "--",
        "bash",
        "-c",
        "sleep 0.15",
        env=env,
    )

    assert result.returncode != 0
    assert "telemetry_status=failed" in result.stdout
    assert "cgroup reported OOM activity" in result.stdout


def test_missing_startup_telemetry_never_releases_command(tmp_path: Path) -> None:
    env, _ = _fake_cgroup_env(tmp_path, omit_file="memory.peak")
    marker = tmp_path / "command-ran"

    result = _run(
        "10G",
        str(tmp_path / "evidence"),
        "safe-unit",
        "--",
        "touch",
        str(marker),
        env=env,
    )

    assert result.returncode != 0
    assert not marker.exists()
    assert "memory.peak is missing or unreadable" in result.stdout


def test_zero_final_peak_fails_closed(tmp_path: Path) -> None:
    env, _ = _fake_cgroup_env(tmp_path, peak_bytes=0)

    result = _run(
        "10G",
        str(tmp_path / "evidence"),
        "safe-unit",
        "--",
        "true",
        env=env,
    )

    assert result.returncode != 0
    assert "final memory.peak is zero" in result.stdout


def test_effective_limit_swap_and_membership_must_match(tmp_path: Path) -> None:
    cases = [
        (
            {"memory_max_bytes": 9 * 1024**3},
            "memory.max mismatch",
        ),
        (
            {"swap_max_bytes": 1024},
            "memory.swap.max mismatch",
        ),
        (
            {"include_member": False},
            "is not present",
        ),
    ]
    for index, (kwargs, expected) in enumerate(cases):
        case_root = tmp_path / f"case-{index}"
        env, _ = _fake_cgroup_env(case_root, **kwargs)
        marker = case_root / "command-ran"
        result = _run(
            "10G",
            str(case_root / "evidence"),
            f"safe-unit-{index}",
            "--",
            "touch",
            str(marker),
            env=env,
        )
        assert result.returncode != 0
        assert expected in result.stdout
        assert not marker.exists()


def test_evidence_setup_failure_is_nonzero_and_never_runs_command(
    tmp_path: Path,
) -> None:
    env, _ = _fake_cgroup_env(tmp_path)
    invalid_log_dir = tmp_path / "not-a-directory"
    invalid_log_dir.write_text("occupied")
    marker = tmp_path / "command-ran"

    result = _run(
        "10G",
        str(invalid_log_dir),
        "safe-unit",
        "--",
        "touch",
        str(marker),
        env=env,
    )

    assert result.returncode != 0
    assert not marker.exists()
