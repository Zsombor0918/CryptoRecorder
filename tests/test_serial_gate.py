"""Tests for validation.serial_gate (generic subprocess-isolated stage
orchestration) and validation.stage_runner_cli (the per-stage semantic-gate
entrypoints).

These tests use tiny inline Python scripts (via `sys.executable -c ...`)
as synthetic "stages" for the generic orchestrator tests, so they run in
milliseconds and need no real catalog/raw-data fixtures — they prove the
orchestration CONTRACT (serial execution, honest failure reporting, no
automatic retry, bounded parent memory) independent of what a real stage
happens to do. The semantic comparison subcommands are tested separately
against small real on-disk Nautilus catalogs and replay/raw fixtures.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from validation.serial_gate import MAX_RESULT_FRAGMENT_BYTES, run_serial_gate, run_stage


def _write_marker_script(tmp_path: Path, name: str, marker_dir: Path, sleep_s: float, exit_code: int = 0) -> list[str]:
    """Build a `python -c ...` command for a synthetic stage that: records
    its own start/end wall-clock time to a small marker file (so tests can
    verify non-overlap), optionally sleeps briefly, writes a tiny JSON
    result fragment, and exits with `exit_code`."""
    script = tmp_path / f"{name}_script.py"
    script.write_text(
        f"""
import json, time, sys
from pathlib import Path

marker_dir = Path({str(marker_dir)!r})
marker_dir.mkdir(parents=True, exist_ok=True)
start = time.time()
time.sleep({sleep_s})
end = time.time()
(marker_dir / "{name}.json").write_text(json.dumps({{"start": start, "end": end}}))
result_path = Path(sys.argv[1])
result_path.write_text(json.dumps({{"passed": {exit_code == 0}}}))
sys.exit({exit_code})
"""
    )
    return [sys.executable, str(script)]


def test_stages_run_strictly_serially_not_concurrently(tmp_path: Path) -> None:
    """Three stages, each recording a (start, end) wall-clock interval,
    must never overlap — proving the orchestrator runs them one at a time,
    not in parallel."""
    marker_dir = tmp_path / "markers"
    specs = []
    for i, name in enumerate(["a", "b", "c"]):
        result_path = tmp_path / f"{name}_result.json"
        base_cmd = _write_marker_script(tmp_path, name, marker_dir, sleep_s=0.05)
        specs.append(
            {
                "name": name,
                "cmd": base_cmd + [str(result_path)],
                "log_path": str(tmp_path / f"{name}.log"),
                "result_path": str(result_path),
                "cwd": str(tmp_path),
            }
        )

    report = run_serial_gate(specs)

    assert report["status"] == "passed"
    intervals = []
    for name in ["a", "b", "c"]:
        data = json.loads((marker_dir / f"{name}.json").read_text())
        intervals.append((data["start"], data["end"]))

    # No two intervals may overlap.
    for i in range(len(intervals)):
        for j in range(i + 1, len(intervals)):
            start_i, end_i = intervals[i]
            start_j, end_j = intervals[j]
            assert end_i <= start_j or end_j <= start_i, (
                f"stage intervals {intervals[i]} and {intervals[j]} overlapped; "
                "stages must run strictly serially"
            )


def test_child_stage_failure_produces_honest_failed_result(tmp_path: Path) -> None:
    """A stage that exits non-zero must be reported as `status: failed`
    with a clear error message, not silently ignored or marked passed."""
    result_path = tmp_path / "fail_result.json"
    cmd = _write_marker_script(tmp_path, "failing", tmp_path / "markers2", sleep_s=0.0, exit_code=3)
    result = run_stage(
        stage_name="failing",
        cmd=cmd + [str(result_path)],
        log_path=tmp_path / "failing.log",
        result_path=result_path,
        cwd=tmp_path,
    )
    assert result["status"] == "failed"
    assert result["returncode"] == 3
    assert "failing" in result["error"]


def test_stage_missing_result_fragment_is_reported_as_failed(tmp_path: Path) -> None:
    """A stage that exits 0 WITHOUT writing its result fragment must still
    be treated as a failure, not silently treated as passed."""
    script = tmp_path / "no_fragment.py"
    script.write_text("import sys\nsys.exit(0)\n")
    result_path = tmp_path / "missing_result.json"
    result = run_stage(
        stage_name="no_fragment",
        cmd=[sys.executable, str(script)],
        log_path=tmp_path / "no_fragment.log",
        result_path=result_path,
        cwd=tmp_path,
    )
    assert result["status"] == "failed"
    assert "did not write a result fragment" in result["error"]


def test_zero_exit_with_failed_fragment_is_reported_as_failed(tmp_path: Path) -> None:
    script = tmp_path / "false_pass.py"
    result_path = tmp_path / "false_pass.json"
    script.write_text(
        "import pathlib, sys\n"
        "pathlib.Path(sys.argv[1]).write_text('{\"passed\": false}')\n"
    )
    result = run_stage(
        stage_name="false_pass",
        cmd=[sys.executable, str(script), str(result_path)],
        log_path=tmp_path / "false_pass.log",
        result_path=result_path,
        cwd=tmp_path,
    )

    assert result["status"] == "failed"
    assert "did not pass" in result["error"]


def test_stage_refuses_to_overwrite_existing_outputs(tmp_path: Path) -> None:
    result_path = tmp_path / "existing.json"
    result_path.write_text('{"passed": true}')

    result = run_stage(
        stage_name="existing",
        cmd=[sys.executable, "-c", "raise SystemExit(0)"],
        log_path=tmp_path / "new.log",
        result_path=result_path,
        cwd=tmp_path,
    )

    assert result["status"] == "failed"
    assert "refusing to overwrite" in result["error"]
    assert result_path.read_text() == '{"passed": true}'


def test_oversized_result_fragment_fails_closed(tmp_path: Path) -> None:
    script = tmp_path / "oversized.py"
    result_path = tmp_path / "oversized.json"
    script.write_text(
        "import pathlib, sys\n"
        f"pathlib.Path(sys.argv[1]).write_bytes(b' ' * ({MAX_RESULT_FRAGMENT_BYTES} + 1))\n"
    )
    result = run_stage(
        stage_name="oversized",
        cmd=[sys.executable, str(script), str(result_path)],
        log_path=tmp_path / "oversized.log",
        result_path=result_path,
        cwd=tmp_path,
    )

    assert result["status"] == "failed"
    assert "limit is" in result["error"]


def test_serial_gate_stops_at_first_failure_and_does_not_run_later_stages(tmp_path: Path) -> None:
    """A failing stage must stop the whole gate immediately; later stages
    in the list must never be executed at all."""
    marker_dir = tmp_path / "markers3"
    ok_result = tmp_path / "ok_result.json"
    fail_result = tmp_path / "fail_result.json"
    never_result = tmp_path / "never_result.json"

    ok_cmd = _write_marker_script(tmp_path, "ok", marker_dir, sleep_s=0.0)
    fail_cmd = _write_marker_script(tmp_path, "fail", marker_dir, sleep_s=0.0, exit_code=1)
    never_cmd = _write_marker_script(tmp_path, "never", marker_dir, sleep_s=0.0)

    specs = [
        {"name": "ok", "cmd": ok_cmd + [str(ok_result)], "log_path": str(tmp_path / "ok.log"), "result_path": str(ok_result), "cwd": str(tmp_path)},
        {"name": "fail", "cmd": fail_cmd + [str(fail_result)], "log_path": str(tmp_path / "fail.log"), "result_path": str(fail_result), "cwd": str(tmp_path)},
        {"name": "never", "cmd": never_cmd + [str(never_result)], "log_path": str(tmp_path / "never.log"), "result_path": str(never_result), "cwd": str(tmp_path)},
    ]

    report = run_serial_gate(specs)

    assert report["status"] == "failed"
    assert report["failed_stage"] == "fail"
    assert len(report["stages"]) == 2  # "ok" and "fail" ran; "never" did not
    assert not never_result.exists()
    assert not (marker_dir / "never.json").exists()


def test_no_automatic_retry_of_a_failed_stage(tmp_path: Path) -> None:
    """A failing stage must be attempted exactly ONCE — the orchestrator
    must never re-invoke it automatically. Proven by counting invocations
    via a persistent counter file the script increments on every run."""
    counter_path = tmp_path / "invocation_count.txt"
    script = tmp_path / "count_and_fail.py"
    script.write_text(
        f"""
import sys
from pathlib import Path
counter_path = Path({str(counter_path)!r})
count = int(counter_path.read_text()) if counter_path.exists() else 0
counter_path.write_text(str(count + 1))
sys.exit(1)
"""
    )
    result_path = tmp_path / "retry_result.json"
    result = run_stage(
        stage_name="count_and_fail",
        cmd=[sys.executable, str(script)],
        log_path=tmp_path / "count_and_fail.log",
        result_path=result_path,
        cwd=tmp_path,
    )
    assert result["status"] == "failed"
    assert counter_path.read_text() == "1"  # exactly one invocation, no retry


def test_timeout_is_a_bounded_failed_stage_result(tmp_path: Path) -> None:
    script = tmp_path / "timeout.py"
    script.write_text("import time\ntime.sleep(10)\n")
    result_path = tmp_path / "timeout_result.json"
    result = run_stage(
        stage_name="timeout",
        cmd=[sys.executable, str(script)],
        log_path=tmp_path / "timeout.log",
        result_path=result_path,
        cwd=tmp_path,
        timeout=0.05,
    )

    assert result["status"] == "failed"
    assert result["timed_out"] is True
    assert result["timeout_seconds"] == 0.05
    assert result["returncode"] is None
    assert "exceeded timeout" in result["error"]
    assert result["wall_seconds"] < 2


def test_timeout_stops_serial_gate_without_retry_or_later_stage(tmp_path: Path) -> None:
    counter = tmp_path / "timeout_invocations.txt"
    timeout_script = tmp_path / "count_timeout.py"
    timeout_script.write_text(
        "import pathlib, time\n"
        f"p = pathlib.Path({str(counter)!r})\n"
        "p.write_text(str(int(p.read_text()) + 1) if p.exists() else '1')\n"
        "time.sleep(10)\n"
    )
    later_marker = tmp_path / "later-ran"
    report = run_serial_gate([
        {
            "name": "timeout",
            "cmd": [sys.executable, str(timeout_script)],
            "log_path": tmp_path / "timeout-gate.log",
            "result_path": tmp_path / "timeout-gate-result.json",
            "cwd": tmp_path,
            "timeout": 0.05,
        },
        {
            "name": "later",
            "cmd": [
                sys.executable,
                "-c",
                f"from pathlib import Path; Path({str(later_marker)!r}).touch()",
            ],
            "log_path": tmp_path / "later.log",
            "result_path": tmp_path / "later-result.json",
            "cwd": tmp_path,
        },
    ])

    assert report["status"] == "failed"
    assert report["failed_stage"] == "timeout"
    assert report["stages"][0]["timed_out"] is True
    assert counter.read_text() == "1"
    assert not later_marker.exists()


def test_parent_process_memory_stays_small_regardless_of_child_output_size(tmp_path: Path) -> None:
    """A stage that produces a LARGE amount of stdout (megabytes) must not
    cause the parent orchestrator to hold that output in memory — the
    parent's own result dict must stay small (paths + short strings),
    since output is redirected straight to a log file on disk."""
    script = tmp_path / "chatty.py"
    script.write_text(
        """
import sys
from pathlib import Path
# Print several MB of output directly (would balloon a PIPE-captured parent).
for _ in range(20000):
    print("x" * 200)
result_path = Path(sys.argv[1])
result_path.write_text('{"passed": true}')
"""
    )
    result_path = tmp_path / "chatty_result.json"
    log_path = tmp_path / "chatty.log"
    result = run_stage(
        stage_name="chatty",
        cmd=[sys.executable, str(script), str(result_path)],
        log_path=log_path,
        result_path=result_path,
        cwd=tmp_path,
    )
    assert result["status"] == "completed"
    # The parent's own result dict is small regardless of the ~4MB of
    # stdout the child produced (verify the log file, not the result
    # dict, holds that output).
    assert log_path.stat().st_size > 1_000_000
    serialized_result_size = len(json.dumps(result))
    assert serialized_result_size < 10_000
