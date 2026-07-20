#!/usr/bin/env bash
#
# deploy_linux_server.sh — thin operator wrapper for deploying CryptoRecorder on a
# Linux server. Contains NO business logic: it only prepares the environment and
# installs/controls systemd units for the selected service group.
#
# See docs/DEPLOYMENT.md and docs/LINUX_SERVER.md for the full reference.
#
# It NEVER deploys Syncthing, archive, or import features (none exist), and it never
# modifies recorder.py, the raw schema, or convert_day.py.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TARGET="all"
DRY_RUN="false"
USE_SYSTEMD="true"
INSTALL_ONLY="false"
DO_ENABLE="false"
DO_START="false"
DO_RESTART="false"
SERVICE_USER="zsom"
APP_DIR="/home/zsom/services/CryptoRecorder"
DATA_ROOT="/data/cryptorecorder"
ENV_FILE="/etc/cryptorecorder/cryptorecorder.env"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VALID_TARGETS=("all" "recorder" "legacy-converter" "replay-build")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { printf '[deploy] %s\n' "$*"; }
warn() { printf '[deploy][warn] %s\n' "$*" >&2; }
die()  { printf '[deploy][error] %s\n' "$*" >&2; exit 1; }

# Run a command, logging it first. Under --dry-run, log only.
run() {
  printf '    + %s\n' "$*"
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  "$@"
}

usage() {
  cat <<'EOF'
Usage: scripts/deploy_linux_server.sh [flags]

Flags:
  --target <name>     all | recorder | legacy-converter | replay-build  (default: all)
  --dry-run           Print every action; change nothing.
  --no-systemd        Skip all systemd / /etc actions (safe in WSL).
  --install-only      Prepare env + install units; do not enable/start.
  --enable            systemctl enable the selected units.
  --start             systemctl start the selected units.
  --restart           systemctl restart the selected units.
  --user <name>       Service user (default: zsom).
  --app-dir <path>    Repo checkout dir (default: /home/zsom/services/CryptoRecorder).
  --data-root <path>  Data base dir (default: /data/cryptorecorder).
  --env-file <path>   Env file path (default: /etc/cryptorecorder/cryptorecorder.env).
  -h, --help          Show this help.
EOF
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)     TARGET="${2:-}"; shift 2 ;;
    --dry-run)    DRY_RUN="true"; shift ;;
    --no-systemd) USE_SYSTEMD="false"; shift ;;
    --install-only) INSTALL_ONLY="true"; shift ;;
    --enable)     DO_ENABLE="true"; shift ;;
    --start)      DO_START="true"; shift ;;
    --restart)    DO_RESTART="true"; shift ;;
    --user)       SERVICE_USER="${2:-}"; shift 2 ;;
    --app-dir)    APP_DIR="${2:-}"; shift 2 ;;
    --data-root)  DATA_ROOT="${2:-}"; shift 2 ;;
    --env-file)   ENV_FILE="${2:-}"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *)            usage; die "unknown argument: $1" ;;
  esac
done

VENV="$APP_DIR/.venv"

# Validate target.
_valid="false"
for t in "${VALID_TARGETS[@]}"; do
  [[ "$TARGET" == "$t" ]] && _valid="true"
done
[[ "$_valid" == "true" ]] || die "invalid --target '$TARGET' (expected: ${VALID_TARGETS[*]})"

# ---------------------------------------------------------------------------
# Target -> systemd unit mapping
# ---------------------------------------------------------------------------
# units_for_target: every unit file to install for a target.
# control_for_target: the units to enable/start/restart (timers for oneshots).
units_for_target() {
  case "$1" in
    recorder)         echo "cryptorecorder-recorder.service" ;;
    legacy-converter) echo "cryptorecorder-convert.service cryptorecorder-convert.timer" ;;
    replay-build)     echo "cryptorecorder-replay-build.service cryptorecorder-replay-build.timer" ;;
  esac
}

control_for_target() {
  case "$1" in
    recorder)         echo "cryptorecorder-recorder.service" ;;
    legacy-converter) echo "cryptorecorder-convert.timer" ;;
    replay-build)     echo "cryptorecorder-replay-build.timer" ;;
  esac
}

selected_targets() {
  if [[ "$TARGET" == "all" ]]; then
    echo "recorder legacy-converter replay-build"
  else
    echo "$TARGET"
  fi
}

all_units() {
  local t
  for t in $(selected_targets); do units_for_target "$t"; done
}

all_control_units() {
  local t
  for t in $(selected_targets); do control_for_target "$t"; done
}

# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
verify_linux() {
  log "Step 1/9: verify Linux host"
  [[ "$(uname -s)" == "Linux" ]] || die "this script must run on Linux (got $(uname -s))"
}

verify_repo_root() {
  log "Step 2/9: verify repository root ($REPO_ROOT)"
  [[ -f "$REPO_ROOT/recorder.py" ]] || die "not a CryptoRecorder checkout: recorder.py missing in $REPO_ROOT"
}

verify_structure() {
  log "Step 3/9: verify frozen structure contract"
  [[ -f "$REPO_ROOT/docs/REPO_STRUCTURE.md" ]] || die "docs/REPO_STRUCTURE.md missing; refusing to deploy"
}

create_venv() {
  log "Step 4/9: ensure virtualenv ($VENV)"
  if [[ -d "$VENV" ]]; then
    log "venv already exists"
  else
    run python3 -m venv "$VENV"
  fi
}

install_requirements() {
  log "Step 5/9: install Python requirements"
  run "$VENV/bin/pip" install --upgrade pip
  run "$VENV/bin/pip" install -r "$REPO_ROOT/requirements.txt"
}

create_env_file() {
  log "Step 6/9: ensure env file ($ENV_FILE)"
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "  --no-systemd: skipping env-file creation under /etc"
    return 0
  fi
  if [[ -f "$ENV_FILE" ]]; then
    log "  env file exists; leaving untouched (never overwrite)"
  else
    run sudo mkdir -p "$(dirname "$ENV_FILE")"
    run sudo cp "$REPO_ROOT/systemd/cryptorecorder.env.example" "$ENV_FILE"
  fi
}

create_data_dirs() {
  log "Step 7/9: ensure data directories under $DATA_ROOT"
  local d
  for d in data_raw replay_store state \
           daily_build_reports catalog archive_days; do
    run mkdir -p "$DATA_ROOT/$d"
  done
}

run_validation() {
  log "Step 8/9: run validation (validate.py --quick)"
  if [[ -x "$VENV/bin/python" || "$DRY_RUN" == "true" ]]; then
    run "$VENV/bin/python" "$REPO_ROOT/validate.py" --quick || warn "validation reported issues (continuing)"
  else
    warn "  venv python not found; skipping validation"
  fi
}

print_target() {
  log "Step 9/9: target summary"
  log "  target:        $TARGET"
  log "  units:         $(all_units)"
  log "  control units: $(all_control_units)"
  log "  user:          $SERVICE_USER"
  log "  app-dir:       $APP_DIR"
  log "  data-root:     $DATA_ROOT"
  log "  env-file:      $ENV_FILE"
}

install_units() {
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "Install units: skipped (--no-systemd)"
    return 0
  fi
  log "Install systemd units"
  local unit
  for unit in $(all_units); do
    run sudo cp "$REPO_ROOT/systemd/$unit" "/etc/systemd/system/$unit"
  done
  run sudo systemctl daemon-reload
}

control_units() {
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "Control units: skipped (--no-systemd)"
    return 0
  fi
  if [[ "$INSTALL_ONLY" == "true" ]]; then
    log "Control units: skipped (--install-only)"
    return 0
  fi
  local unit
  if [[ "$DO_ENABLE" == "true" ]]; then
    for unit in $(all_control_units); do run sudo systemctl enable "$unit"; done
  fi
  if [[ "$DO_RESTART" == "true" ]]; then
    for unit in $(all_control_units); do run sudo systemctl restart "$unit"; done
  elif [[ "$DO_START" == "true" ]]; then
    for unit in $(all_control_units); do run sudo systemctl start "$unit"; done
  fi
  if [[ "$DO_ENABLE" == "false" && "$DO_START" == "false" && "$DO_RESTART" == "false" ]]; then
    log "No --enable/--start/--restart given; units installed but not activated"
  fi
}

# Stop/disable/remove systemd units that this repo used to install but no
# longer ships (e.g. the feature-build service group removed in issue #17).
# On servers deployed before that refactor, the stale unit files may still be
# present under /etc/systemd/system and would otherwise keep firing the
# removed command on their old schedule after this repo is upgraded.
cleanup_stale_units() {
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "Cleanup stale units: skipped (--no-systemd)"
    return 0
  fi
  case "$TARGET" in
    all|replay-build) ;;
    *) return 0 ;;
  esac
  local stale_unit removed_any="false"
  for stale_unit in cryptorecorder-feature-build.timer cryptorecorder-feature-build.service; do
    if [[ -f "/etc/systemd/system/$stale_unit" ]]; then
      removed_any="true"
      log "Removing stale unit from a pre-issue-#17 deploy: $stale_unit"
      run sudo systemctl stop "$stale_unit" || true
      run sudo systemctl disable "$stale_unit" || true
      run sudo rm -f "/etc/systemd/system/$stale_unit"
    fi
  done
  if [[ "$removed_any" == "true" ]]; then
    run sudo systemctl daemon-reload
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  log "CryptoRecorder deploy (dry-run=$DRY_RUN, systemd=$USE_SYSTEMD, target=$TARGET)"
  verify_linux
  verify_repo_root
  verify_structure
  create_venv
  install_requirements
  create_env_file
  create_data_dirs
  run_validation
  install_units
  cleanup_stale_units
  control_units
  print_target
  log "Done."
}

main "$@"
