"""Generic serial, per-process-isolated semantic-equivalence orchestration.

Each stage of the gate (reference conversion, replay reconstruction, trade
comparison, delta comparison, depth10/checkpoint/fence comparison, report
aggregation) is run as its OWN child process via `subprocess.run()`, one at
a time, never in parallel. This exists because a single long-lived Python
process accumulates native Arrow/Rust/SQLite allocator memory across stages
that Python's own garbage collector cannot reliably reclaim (pyarrow Table
buffers, DataFusion sessions, sqlite3 connections) — process exit is the
only fully reliable way to release that memory back to the OS between
stages, and it also lets each stage be independently measured/capped by an
external cgroup.

Design constraints (see docstrings below for how each is met):
- The parent (this module) NEVER captures a child's full stdout/stderr in
  memory: every child's output is redirected directly to a persistent log
  file via `subprocess.run(..., stdout=<file handle>, stderr=STDOUT)`, not
  `capture_output=True`/`PIPE`.
- Each stage writes ONLY a small JSON "result fragment" to its own
  `result_path`; the parent reads that (small) JSON back and retains
  nothing else from the child.
- Stages run strictly in order; the first failing stage stops the whole
  gate immediately — there is no automatic retry of a failed/killed stage.
"""
from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)
MAX_RESULT_FRAGMENT_BYTES = 8 * 1024 * 1024


def run_stage(
    *,
    stage_name: str,
    cmd: list[str],
    log_path: Path,
    result_path: Path,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Run exactly one gate stage as a single, isolated child process.

    Returns a SMALL dict (paths + a short status + the stage's own small
    result fragment, if any) — never the stage's raw event data. The
    child's combined stdout/stderr is streamed directly to `log_path` on
    disk (never buffered through a Python `PIPE`/`capture_output=True`),
    so parent memory usage does not grow with how much a stage logs.

    A non-zero exit code, a missing result fragment, or an unparseable
    result fragment are all reported as `status: "failed"` with a short
    `error` message — this function does not retry; the caller
    (`run_serial_gate`) is likewise required to stop rather than retry.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    existing = [path for path in (log_path, result_path) if path.exists()]
    if existing:
        return {
            "stage": stage_name,
            "cmd": cmd,
            "status": "failed",
            "error": (
                "refusing to overwrite existing stage output(s): "
                + ", ".join(str(path) for path in existing)
            ),
            "log_path": str(log_path),
            "result_path": str(result_path),
        }

    start = time.monotonic()
    with open(log_path, "wb") as log_file:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    wall_seconds = time.monotonic() - start

    stage_result: dict[str, Any] = {
        "stage": stage_name,
        "cmd": cmd,
        "returncode": proc.returncode,
        "wall_seconds": wall_seconds,
        "log_path": str(log_path),
        "result_path": str(result_path),
    }

    if proc.returncode != 0:
        stage_result["status"] = "failed"
        stage_result["error"] = (
            f"stage {stage_name!r} exited with code {proc.returncode}; see {log_path} for details"
        )
        return stage_result

    if not result_path.exists():
        stage_result["status"] = "failed"
        stage_result["error"] = (
            f"stage {stage_name!r} exited 0 but did not write a result fragment at {result_path}"
        )
        return stage_result

    fragment_size = result_path.stat().st_size
    if fragment_size > MAX_RESULT_FRAGMENT_BYTES:
        stage_result["status"] = "failed"
        stage_result["error"] = (
            f"stage {stage_name!r} result fragment is {fragment_size} bytes; "
            f"limit is {MAX_RESULT_FRAGMENT_BYTES} bytes"
        )
        return stage_result

    try:
        fragment = json.loads(result_path.read_text())
    except (OSError, ValueError) as exc:
        stage_result["status"] = "failed"
        stage_result["error"] = f"stage {stage_name!r} result fragment unreadable: {exc}"
        return stage_result

    if not isinstance(fragment, dict):
        stage_result["status"] = "failed"
        stage_result["error"] = (
            f"stage {stage_name!r} result fragment must be a JSON object"
        )
        return stage_result
    if fragment.get("passed") is not True:
        stage_result["status"] = "failed"
        stage_result["error"] = (
            f"stage {stage_name!r} exited 0 but its result did not pass"
        )
        stage_result["result"] = fragment
        return stage_result

    stage_result["status"] = "completed"
    stage_result["result"] = fragment
    return stage_result


def run_serial_gate(stage_specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Run a list of stage specs STRICTLY IN ORDER, one child process at a
    time (never concurrently), stopping immediately at the first failure
    (no automatic retry of a failed or killed stage — see `run_stage()`).

    Each `stage_specs[i]` is a dict with keys: `name` (str), `cmd`
    (list[str]), `log_path` (str|Path), `result_path` (str|Path), `cwd`
    (str|Path), and optionally `env` (dict[str,str]) / `timeout` (float).

    Returns `{"stages": [...small per-stage dicts...], "status":
    "passed"|"failed", "failed_stage": name|None}`. The parent never
    retains anything beyond these small per-stage dicts (each stage's own
    `result` fragment is itself required to be small — see individual
    stage CLI implementations in `validation.stage_runner_cli`).
    """
    report: dict[str, Any] = {"stages": [], "status": "passed", "failed_stage": None}
    for stage_spec in stage_specs:
        result = run_stage(
            stage_name=stage_spec["name"],
            cmd=stage_spec["cmd"],
            log_path=Path(stage_spec["log_path"]),
            result_path=Path(stage_spec["result_path"]),
            cwd=Path(stage_spec["cwd"]),
            env=stage_spec.get("env"),
            timeout=stage_spec.get("timeout"),
        )
        report["stages"].append(result)
        if result["status"] != "completed":
            report["status"] = "failed"
            report["failed_stage"] = stage_spec["name"]
            logger.error("gate stage %r failed; stopping (no retry): %s", stage_spec["name"], result.get("error"))
            break
    return report
