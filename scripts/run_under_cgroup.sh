#!/usr/bin/env bash
# Transient cgroup memory-safety harness for serial validation stages. Wraps a
# command in a `systemd-run --user --scope` transient unit with an enforced
# MemoryMax and no swap. A startup handshake keeps the command blocked until
# the external monitor has verified the effective cgroup limits, membership,
# and telemetry files. A completion handshake keeps the scope alive after the
# command exits so the monitor can persist the kernel's exact memory.peak and
# final memory.events before teardown.
#
# Contract:
#   - a cgroup-resident shim is verified in cgroup.procs before it launches
#     the wrapped command; the command inherits that same cgroup;
#   - memory.max exactly matches the requested limit and memory.swap.max is 0;
#   - no retry or respawn is performed;
#   - stdout/stderr stay attached to the caller;
#   - a non-zero wrapped-command exit is preserved;
#   - exact memory.peak, final memory.events, sampled memory.current, and
#     command wall_seconds are persisted before the scope disappears;
#   - missing/invalid telemetry, zero peak, positive oom/oom_kill, or evidence
#     write failure makes an otherwise-successful command fail closed;
#   - existing evidence files are never overwritten.
#
# Usage:
#   scripts/run_under_cgroup.sh <memory_max> <log_dir> <unit_name> -- <cmd...>
#
# Example:
#   scripts/run_under_cgroup.sh 10G validation_runs/stage1 cr-stage1 -- \
#       python3 -m validation.stage_runner_cli trades --config cfg.json --out out.json
set -uo pipefail

HARNESS_FAILURE_EXIT=125
SAMPLE_INTERVAL_SECONDS=0.1
STARTUP_WAIT_ATTEMPTS=200

fail_usage() {
  echo "$1" >&2
  exit 2
}

if (( $# < 4 )); then
  fail_usage "usage: $0 <memory_max> <log_dir> <unit_name> -- <cmd...>"
fi

MEMORY_MAX="$1"; shift
LOG_DIR="$1"; shift
UNIT_NAME="$1"; shift
if [[ ! "$MEMORY_MAX" =~ ^[1-9][0-9]*(K|M|G|T|KiB|MiB|GiB|TiB)$ ]]; then
  fail_usage "invalid memory_max '$MEMORY_MAX'"
fi
if [[ ! "$UNIT_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
  fail_usage "invalid unit_name '$UNIT_NAME'"
fi
if [[ "$1" != "--" ]]; then
  fail_usage "expected '--' before the command to run"
fi
shift
if (( $# == 0 )); then
  fail_usage "missing command after '--'"
fi

memory_arg_to_bytes() {
  local value="$1"
  local number suffix factor
  if [[ ! "$value" =~ ^([1-9][0-9]*)(K|M|G|T|KiB|MiB|GiB|TiB)$ ]]; then
    return 1
  fi
  number="${BASH_REMATCH[1]}"
  suffix="${BASH_REMATCH[2]}"
  case "$suffix" in
    K|KiB) factor=1024 ;;
    M|MiB) factor=1048576 ;;
    G|GiB) factor=1073741824 ;;
    T|TiB) factor=1099511627776 ;;
    *) return 1 ;;
  esac
  local bytes=$(( number * factor ))
  if (( bytes <= 0 )); then
    return 1
  fi
  printf '%s\n' "$bytes"
}

MEMORY_MAX_BYTES="$(memory_arg_to_bytes "$MEMORY_MAX")" \
  || fail_usage "memory_max '$MEMORY_MAX' cannot be represented safely in bytes"

# Test overrides are deliberately inert unless this explicit guard is set.
# Production callers must never redirect cgroup inspection or systemd-run.
ALLOW_TEST_OVERRIDES="${CRYPTO_RECORDER_ALLOW_CGROUP_TEST_OVERRIDES:-0}"
TEST_CGROUP_ROOT="${CRYPTO_RECORDER_CGROUP_TEST_ROOT:-}"
TEST_SYSTEMD_RUN="${CRYPTO_RECORDER_SYSTEMD_RUN_TEST_BIN:-}"
if [[ -n "$TEST_CGROUP_ROOT" || -n "$TEST_SYSTEMD_RUN" ]]; then
  if [[ "$ALLOW_TEST_OVERRIDES" != "1" ]]; then
    fail_usage "test-only cgroup/systemd overrides require CRYPTO_RECORDER_ALLOW_CGROUP_TEST_OVERRIDES=1"
  fi
fi

SYSTEMD_RUN_BIN="${TEST_SYSTEMD_RUN:-systemd-run}"
if ! command -v "$SYSTEMD_RUN_BIN" >/dev/null 2>&1; then
  fail_usage "systemd-run executable not found: $SYSTEMD_RUN_BIN"
fi

if ! mkdir -p "$LOG_DIR"; then
  echo "cannot create evidence directory: $LOG_DIR" >&2
  exit "$HARNESS_FAILURE_EXIT"
fi
MEMORY_SAMPLE_LOG="$LOG_DIR/${UNIT_NAME}.memory_samples.log"
RESULT_LOG="$LOG_DIR/${UNIT_NAME}.result.txt"
EVENTS_LOG="$LOG_DIR/${UNIT_NAME}.memory_events.log"
for output_path in "$MEMORY_SAMPLE_LOG" "$RESULT_LOG" "$EVENTS_LOG"; do
  if [[ -e "$output_path" ]]; then
    fail_usage "refusing to overwrite existing evidence: $output_path"
  fi
  if ! (set -o noclobber; : > "$output_path") 2>/dev/null; then
    echo "cannot create evidence file without overwrite: $output_path" >&2
    exit "$HARNESS_FAILURE_EXIT"
  fi
done

HANDSHAKE_DIR="$(mktemp -d "$LOG_DIR/.${UNIT_NAME}.handshake.XXXXXX")"
if [[ -z "$HANDSHAKE_DIR" || ! -d "$HANDSHAKE_DIR" ]]; then
  echo "cannot create cgroup handshake directory under $LOG_DIR" >&2
  exit "$HARNESS_FAILURE_EXIT"
fi
START_SIGNAL="$HANDSHAKE_DIR/start.signal"
FINISH_SIGNAL="$HANDSHAKE_DIR/finish.signal"
SHIM_PID_FILE="$HANDSHAKE_DIR/shim.pid"
CHILD_EXIT_FILE="$HANDSHAKE_DIR/child.exit"
STAGE_START_NS_FILE="$HANDSHAKE_DIR/stage_start_ns"
STAGE_END_NS_FILE="$HANDSHAKE_DIR/stage_end_ns"

if [[ -n "$TEST_CGROUP_ROOT" ]]; then
  CGROUP_BASE="$TEST_CGROUP_ROOT/${UNIT_NAME}.scope"
else
  CGROUP_BASE="/sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/app.slice/${UNIT_NAME}.scope"
fi

RUNNER_PID=""
SHIM_PID=""
cleanup() {
  local pid
  if [[ -n "$SHIM_PID" ]] && kill -0 "$SHIM_PID" 2>/dev/null; then
    kill -TERM "$SHIM_PID" 2>/dev/null || true
    for _ in {1..20}; do
      kill -0 "$SHIM_PID" 2>/dev/null || break
      sleep 0.05
    done
    if kill -0 "$SHIM_PID" 2>/dev/null; then
      kill -KILL "$SHIM_PID" 2>/dev/null || true
    fi
  fi
  if [[ -n "$RUNNER_PID" ]] && kill -0 "$RUNNER_PID" 2>/dev/null; then
    kill -TERM "$RUNNER_PID" 2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
  fi
  for pid in "$START_SIGNAL" "$FINISH_SIGNAL" "$SHIM_PID_FILE" \
    "$CHILD_EXIT_FILE" "$STAGE_START_NS_FILE" "$STAGE_END_NS_FILE"; do
    if [[ -e "$pid" || -p "$pid" ]]; then
      rm -f -- "$pid" 2>/dev/null || true
    fi
  done
  rmdir "$HANDSHAKE_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

SHIM_CODE='
set -uo pipefail
start_signal="$1"
finish_signal="$2"
pid_file="$3"
child_exit_file="$4"
stage_start_file="$5"
stage_end_file="$6"
shift 6
stage_pid=""
terminate_stage() {
  if [[ -n "$stage_pid" ]] && kill -0 "$stage_pid" 2>/dev/null; then
    kill -TERM "$stage_pid" 2>/dev/null || true
    wait "$stage_pid" 2>/dev/null || true
  fi
  exit 125
}
trap terminate_stage TERM INT HUP
printf "%s\n" "$$" > "$pid_file" || exit 125
while [[ ! -s "$start_signal" ]]; do sleep 0.01; done
IFS= read -r start_token < "$start_signal" || exit 125
[[ "$start_token" == "start" ]] || exit 125
date +%s%N > "$stage_start_file" || exit 125
"$@" &
stage_pid=$!
if wait "$stage_pid"; then
  child_exit=0
else
  child_exit=$?
fi
stage_pid=""
date +%s%N > "$stage_end_file" || exit 125
printf "%s\n" "$child_exit" > "$child_exit_file" || exit 125
while [[ ! -s "$finish_signal" ]]; do sleep 0.01; done
IFS= read -r finish_token < "$finish_signal" || exit 125
[[ "$finish_token" == "finish" ]] || exit 125
exit "$child_exit"
'

(
  "$SYSTEMD_RUN_BIN" --user --unit="$UNIT_NAME" --scope \
    -p "MemoryMax=${MEMORY_MAX}" -p "MemorySwapMax=0" \
    --same-dir -- bash -c "$SHIM_CODE" cgroup-stage-shim \
    "$START_SIGNAL" "$FINISH_SIGNAL" "$SHIM_PID_FILE" "$CHILD_EXIT_FILE" \
    "$STAGE_START_NS_FILE" "$STAGE_END_NS_FILE" "$@"
) &
RUNNER_PID=$!

HARNESS_ERROR=""
EVIDENCE_WRITE_FAILED=0
CHILD_EXIT=""
RUNNER_EXIT=""
PEAK_BYTES=""
WALL_SECONDS=""
FINAL_OOM=""
FINAL_OOM_KILL=""

set_harness_error() {
  if [[ -z "$HARNESS_ERROR" ]]; then
    HARNESS_ERROR="$1"
  fi
}

read_numeric_file() {
  local path="$1"
  local label="$2"
  local target_var="$3"
  local value=""
  if [[ ! -r "$path" ]] || ! IFS= read -r value < "$path"; then
    set_harness_error "$label is missing or unreadable at $path"
    return 1
  fi
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    set_harness_error "$label is not numeric at $path: $value"
    return 1
  fi
  printf -v "$target_var" '%s' "$value"
}

read_memory_events() {
  local path="$1"
  local key value
  local oom_value=""
  local oom_kill_value=""
  if [[ ! -r "$path" ]]; then
    set_harness_error "memory.events is missing or unreadable at $path"
    return 1
  fi
  while read -r key value; do
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
      set_harness_error "memory.events contains a non-numeric value for $key: $value"
      return 1
    fi
    case "$key" in
      oom) oom_value="$value" ;;
      oom_kill) oom_kill_value="$value" ;;
    esac
  done < "$path"
  if [[ -z "$oom_value" || -z "$oom_kill_value" ]]; then
    set_harness_error "memory.events lacks numeric oom/oom_kill counters at $path"
    return 1
  fi
  FINAL_OOM="$oom_value"
  FINAL_OOM_KILL="$oom_kill_value"
}

stop_active_scope() {
  if [[ -n "$SHIM_PID" ]] && kill -0 "$SHIM_PID" 2>/dev/null; then
    kill -TERM "$SHIM_PID" 2>/dev/null || true
  fi
}

# Wait for both the in-cgroup shim PID and the expected cgroup directory.
READY=0
for (( attempt=0; attempt<STARTUP_WAIT_ATTEMPTS; attempt++ )); do
  if [[ -s "$SHIM_PID_FILE" && -d "$CGROUP_BASE" ]]; then
    READY=1
    break
  fi
  if ! kill -0 "$RUNNER_PID" 2>/dev/null; then
    set_harness_error "systemd-run exited before the cgroup startup handshake completed"
    break
  fi
  sleep 0.05
done
if (( READY == 0 )) && [[ -z "$HARNESS_ERROR" ]]; then
  set_harness_error "timed out waiting for cgroup startup handshake at $CGROUP_BASE"
fi

EFFECTIVE_MEMORY_MAX=""
EFFECTIVE_SWAP_MAX=""
STARTUP_PEAK=""
if [[ -z "$HARNESS_ERROR" ]]; then
  if ! IFS= read -r SHIM_PID < "$SHIM_PID_FILE" \
    || [[ ! "$SHIM_PID" =~ ^[1-9][0-9]*$ ]]; then
    set_harness_error "cgroup shim PID is missing or invalid"
  fi
fi
if [[ -z "$HARNESS_ERROR" ]]; then
  read_numeric_file "$CGROUP_BASE/memory.max" "memory.max" EFFECTIVE_MEMORY_MAX || true
  read_numeric_file "$CGROUP_BASE/memory.swap.max" "memory.swap.max" EFFECTIVE_SWAP_MAX || true
  read_numeric_file "$CGROUP_BASE/memory.peak" "memory.peak" STARTUP_PEAK || true
  read_memory_events "$CGROUP_BASE/memory.events" || true
fi
if [[ -z "$HARNESS_ERROR" && "$EFFECTIVE_MEMORY_MAX" != "$MEMORY_MAX_BYTES" ]]; then
  set_harness_error "memory.max mismatch: requested $MEMORY_MAX_BYTES, found $EFFECTIVE_MEMORY_MAX"
fi
if [[ -z "$HARNESS_ERROR" && "$EFFECTIVE_SWAP_MAX" != "0" ]]; then
  set_harness_error "memory.swap.max mismatch: expected 0, found $EFFECTIVE_SWAP_MAX"
fi
if [[ -z "$HARNESS_ERROR" && ( "$FINAL_OOM" != "0" || "$FINAL_OOM_KILL" != "0" ) ]]; then
  set_harness_error "fresh cgroup has nonzero startup OOM counters: oom=$FINAL_OOM oom_kill=$FINAL_OOM_KILL"
fi
if [[ -z "$HARNESS_ERROR" ]]; then
  MEMBER_FOUND=0
  if [[ -r "$CGROUP_BASE/cgroup.procs" ]]; then
    while IFS= read -r member_pid; do
      if [[ "$member_pid" == "$SHIM_PID" ]]; then
        MEMBER_FOUND=1
        break
      fi
    done < "$CGROUP_BASE/cgroup.procs"
  fi
  if (( MEMBER_FOUND == 0 )); then
    set_harness_error "cgroup shim PID $SHIM_PID is not present in $CGROUP_BASE/cgroup.procs"
  fi
fi

if [[ -n "$HARNESS_ERROR" ]]; then
  stop_active_scope
else
  if ! printf 'start\n' > "$START_SIGNAL"; then
    set_harness_error "failed to release the verified cgroup startup handshake"
    stop_active_scope
  fi
fi

# Sample memory.current only for growth diagnostics. The authoritative peak
# recorded below is memory.peak, read while the completion handshake still
# keeps the cgroup alive.
while [[ -z "$HARNESS_ERROR" && ! -s "$CHILD_EXIT_FILE" ]]; do
  CURRENT_BYTES=""
  if ! read_numeric_file "$CGROUP_BASE/memory.current" "memory.current" CURRENT_BYTES; then
    stop_active_scope
    break
  fi
  if ! printf '%s %s\n' "$(date -u +%s.%N)" "$CURRENT_BYTES" >> "$MEMORY_SAMPLE_LOG"; then
    EVIDENCE_WRITE_FAILED=1
    set_harness_error "failed to append memory.current evidence to $MEMORY_SAMPLE_LOG"
    stop_active_scope
    break
  fi
  if ! kill -0 "$RUNNER_PID" 2>/dev/null && [[ ! -s "$CHILD_EXIT_FILE" ]]; then
    set_harness_error "cgroup scope exited before the command completion handshake"
    break
  fi
  sleep "$SAMPLE_INTERVAL_SECONDS"
done

if [[ -z "$HARNESS_ERROR" ]]; then
  if ! IFS= read -r CHILD_EXIT < "$CHILD_EXIT_FILE" \
    || [[ ! "$CHILD_EXIT" =~ ^[0-9]+$ ]]; then
    set_harness_error "wrapped command exit status is missing or invalid"
  fi
fi

STAGE_START_NS=""
STAGE_END_NS=""
if [[ -z "$HARNESS_ERROR" ]]; then
  read_numeric_file "$STAGE_START_NS_FILE" "stage start timestamp" STAGE_START_NS || true
  read_numeric_file "$STAGE_END_NS_FILE" "stage end timestamp" STAGE_END_NS || true
fi
if [[ -z "$HARNESS_ERROR" ]]; then
  if (( STAGE_END_NS < STAGE_START_NS )); then
    set_harness_error "stage end timestamp precedes stage start timestamp"
  else
    WALL_NS=$(( STAGE_END_NS - STAGE_START_NS ))
    printf -v WALL_SECONDS '%d.%09d' \
      "$(( WALL_NS / 1000000000 ))" "$(( WALL_NS % 1000000000 ))"
  fi
fi

if [[ -z "$HARNESS_ERROR" ]]; then
  read_numeric_file "$CGROUP_BASE/memory.peak" "final memory.peak" PEAK_BYTES || true
  read_memory_events "$CGROUP_BASE/memory.events" || true
fi
if [[ -z "$HARNESS_ERROR" && "$PEAK_BYTES" == "0" ]]; then
  set_harness_error "final memory.peak is zero"
fi
if [[ -z "$HARNESS_ERROR" ]] && (( PEAK_BYTES > MEMORY_MAX_BYTES )); then
  set_harness_error "final memory.peak $PEAK_BYTES exceeds memory.max $MEMORY_MAX_BYTES"
fi
if [[ -z "$HARNESS_ERROR" && ( "$FINAL_OOM" != "0" || "$FINAL_OOM_KILL" != "0" ) ]]; then
  set_harness_error "cgroup reported OOM activity: oom=$FINAL_OOM oom_kill=$FINAL_OOM_KILL"
fi

if [[ -r "$CGROUP_BASE/memory.events" ]]; then
  if ! {
    printf 'memory_events_final\n'
    while IFS= read -r event_line; do
      printf '%s\n' "$event_line"
    done < "$CGROUP_BASE/memory.events"
  } >> "$EVENTS_LOG"; then
    EVIDENCE_WRITE_FAILED=1
    set_harness_error "failed to persist final memory.events to $EVENTS_LOG"
  fi
else
  EVIDENCE_WRITE_FAILED=1
  set_harness_error "cannot persist missing final memory.events from $CGROUP_BASE"
fi

# Release a surviving shim even when final telemetry failed; the command has
# already completed at this point. Startup/sample failures instead terminate
# the blocked/running shim via stop_active_scope().
if [[ -s "$CHILD_EXIT_FILE" ]] && kill -0 "$SHIM_PID" 2>/dev/null; then
  if ! printf 'finish\n' > "$FINISH_SIGNAL"; then
    set_harness_error "failed to release the cgroup completion handshake"
    stop_active_scope
  fi
fi

if wait "$RUNNER_PID"; then
  RUNNER_EXIT=0
else
  RUNNER_EXIT=$?
fi
RUNNER_PID=""
SHIM_PID=""

append_result() {
  if ! printf '%s=%s\n' "$1" "$2" >> "$RESULT_LOG"; then
    EVIDENCE_WRITE_FAILED=1
    return 1
  fi
}

append_result "requested_memory_max_bytes" "$MEMORY_MAX_BYTES" || true
append_result "effective_memory_max_bytes" "${EFFECTIVE_MEMORY_MAX:-unavailable}" || true
append_result "effective_memory_swap_max_bytes" "${EFFECTIVE_SWAP_MAX:-unavailable}" || true
append_result "exit_code" "${CHILD_EXIT:-unavailable}" || true
append_result "systemd_run_exit_code" "${RUNNER_EXIT:-unavailable}" || true
append_result "wall_seconds" "${WALL_SECONDS:-unavailable}" || true
append_result "peak_bytes" "${PEAK_BYTES:-unavailable}" || true
if [[ "$PEAK_BYTES" =~ ^[0-9]+$ ]]; then
  append_result "peak_mb" "$(( PEAK_BYTES / 1024 / 1024 ))" || true
else
  append_result "peak_mb" "unavailable" || true
fi
append_result "oom" "${FINAL_OOM:-unavailable}" || true
append_result "oom_kill" "${FINAL_OOM_KILL:-unavailable}" || true
if (( EVIDENCE_WRITE_FAILED != 0 )); then
  set_harness_error "one or more evidence writes failed"
fi
if [[ -n "$HARNESS_ERROR" ]]; then
  append_result "telemetry_status" "failed" || true
  append_result "harness_error" "${HARNESS_ERROR//$'\n'/ }" || true
else
  append_result "telemetry_status" "passed" || true
fi

if ! cat "$RESULT_LOG"; then
  EVIDENCE_WRITE_FAILED=1
fi
if ! cat "$EVENTS_LOG"; then
  EVIDENCE_WRITE_FAILED=1
fi
if (( EVIDENCE_WRITE_FAILED != 0 )) && [[ -z "$HARNESS_ERROR" ]]; then
  HARNESS_ERROR="one or more evidence reads/writes failed"
fi

# Preserve the wrapped command's non-zero status. Harness/telemetry failures
# only replace a successful (or unavailable) child status.
if [[ "$CHILD_EXIT" =~ ^[0-9]+$ ]] && (( CHILD_EXIT != 0 )); then
  exit "$CHILD_EXIT"
fi
if [[ "$RUNNER_EXIT" =~ ^[0-9]+$ ]] && (( RUNNER_EXIT != 0 )); then
  exit "$RUNNER_EXIT"
fi
if [[ -n "$HARNESS_ERROR" || "$CHILD_EXIT" != "0" ]]; then
  exit "$HARNESS_FAILURE_EXIT"
fi
exit 0
