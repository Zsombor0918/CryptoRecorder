#!/usr/bin/env bash
#
# deploy_linux_server.sh — thin operator wrapper for deploying CryptoRecorder on a
# Linux server. Contains NO business logic: it only prepares the environment and
# installs/controls systemd units for the selected service group.
#
# See docs/OPERATIONS.md for the full reference (Deployment Script Reference and
# Linux Server Layout sections).
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
UV_BIN="uv"
MIGRATE_VENV="false"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VALID_TARGETS=("all" "recorder" "replay-build")

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
  --target <name>     all | recorder | replay-build  (default: all)
  --dry-run           Print every action; change nothing.
  --no-systemd        Skip systemd installation/control and /etc writes. A real
                      .venv promotion still performs a read-only inactive-unit
                      check when systemctl exists (safe in WSL).
  --install-only      Prepare env + install units; do not enable/start.
  --enable            systemctl enable the selected units.
  --start             systemctl start the selected units.
  --restart           systemctl restart the selected units.
  --user <name>       Service user/group rendered into installed unit files
                      (User=/Group= lines) and the CryptoRecorder checkout
                      path they run as (default: zsom).
  --app-dir <path>    Repo checkout dir rendered into WorkingDirectory=,
                      ExecStart=, and Documentation= lines of installed unit
                      files (default: /home/zsom/services/CryptoRecorder).
  --data-root <path>  Data base dir rendered into a newly created env file's
                      CRYPTO_RECORDER_*_ROOT values and used for
                      create_data_dirs (default: /data/cryptorecorder). Has
                      no effect if the env file already exists (never
                      overwritten).
  --env-file <path>   Env file path rendered into installed unit files'
                      EnvironmentFile= lines (default:
                      /etc/cryptorecorder/cryptorecorder.env). An existing
                      file at this path is never overwritten.
  --uv-bin <path>     uv executable or command name (default: uv). The script
                      never downloads uv.
  --migrate-venv      Explicitly replace an unrecognized legacy .venv through
                      a validated same-parent candidate and preserved backup.
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
    --uv-bin)     UV_BIN="${2:-}"; shift 2 ;;
    --migrate-venv) MIGRATE_VENV="true"; shift ;;
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
    replay-build)     echo "cryptorecorder-replay-build.service cryptorecorder-replay-build.timer" ;;
  esac
}

control_for_target() {
  case "$1" in
    recorder)         echo "cryptorecorder-recorder.service" ;;
    replay-build)     echo "cryptorecorder-replay-build.timer" ;;
  esac
}

selected_targets() {
  if [[ "$TARGET" == "all" ]]; then
    echo "recorder replay-build"
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

verify_app_checkout() {
  # A dry-run may intentionally render a future checkout path. A mutating run
  # must validate and install the exact checkout containing this script;
  # otherwise candidate imports could pass against different code than the
  # rendered systemd WorkingDirectory.
  [[ "$DRY_RUN" == "true" ]] && return 0
  validate_owned_directory "$APP_DIR" "application directory"
  [[ -f "$APP_DIR/recorder.py" && -f "$APP_DIR/pyproject.toml" && -f "$APP_DIR/uv.lock" ]] \
    || die "application directory is not a complete CryptoRecorder checkout: $APP_DIR"
  [[ "$(realpath "$APP_DIR")" == "$REPO_ROOT" ]] \
    || die "--app-dir must be the checkout containing this deploy script"
}

UV_BIN_RESOLVED=""
UV_VERSION=""
LOCK_SHA256=""
ENV_MARKER_NAME=".cryptorecorder-uv-environment.json"

verify_uv_and_lock() {
  log "Step 4/9: verify authoritative uv lock"
  if [[ "$UV_BIN" == */* ]]; then
    [[ -x "$UV_BIN" ]] || die "uv is required; --uv-bin is not executable: $UV_BIN"
    UV_BIN_RESOLVED="$(cd "$(dirname "$UV_BIN")" && pwd)/$(basename "$UV_BIN")"
  else
    UV_BIN_RESOLVED="$(command -v "$UV_BIN" || true)"
    [[ -n "$UV_BIN_RESOLVED" ]] || die "uv is required; install it separately and pass --uv-bin or add it to PATH"
  fi
  UV_VERSION="$($UV_BIN_RESOLVED --version 2>&1)" || die "could not execute uv: $UV_BIN_RESOLVED"
  log "  $UV_VERSION"
  [[ -f "$REPO_ROOT/pyproject.toml" ]] || die "pyproject.toml missing"
  [[ -f "$REPO_ROOT/uv.lock" ]] || die "uv.lock missing"
  (cd "$REPO_ROOT" && "$UV_BIN_RESOLVED" lock --check) || die "uv.lock is stale or invalid"
  LOCK_SHA256="$(sha256sum "$REPO_ROOT/uv.lock" | awk '{print $1}')"
}

validate_owned_directory() {
  local path="$1" label="$2"
  [[ ! -L "$path" ]] || die "$label must not be a symlink: $path"
  [[ -d "$path" ]] || die "$label must be a directory: $path"
  [[ "$(stat -c %u "$path")" == "$(id -u)" ]] || die "$label has unsafe ownership: $path"
}

environment_marker_matches() {
  local env_dir="$1"
  local marker="$env_dir/$ENV_MARKER_NAME"
  [[ -f "$marker" && ! -L "$marker" ]] || return 1
  python3 - "$marker" "$LOCK_SHA256" <<'PY'
import json, pathlib, sys
path, expected = pathlib.Path(sys.argv[1]), sys.argv[2]
try:
    value = json.loads(path.read_text())
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if (
    value.get("contract_version") == 1
    and value.get("lock_sha256") == expected
    and value.get("selection") == "production"
    and value.get("sync_arguments") == ["--frozen", "--no-default-groups"]
) else 1)
PY
}

validate_production_environment() {
  local env_dir="$1"
  validate_owned_directory "$env_dir" "uv environment"
  [[ -x "$env_dir/bin/python" ]] || return 1
  env PYTHONPATH="$APP_DIR" "$env_dir/bin/python" \
    -m validation.validate_dependency_environment \
    --kind production --uv-bin "$UV_BIN_RESOLVED" >/dev/null
}

write_environment_marker() {
  local env_dir="$1"
  local marker="$env_dir/$ENV_MARKER_NAME"
  "$env_dir/bin/python" - "$marker" "$LOCK_SHA256" "$UV_VERSION" <<'PY'
import json, os, pathlib, sys, tempfile
path = pathlib.Path(sys.argv[1])
document = {
    "contract_version": 1,
    "lock_sha256": sys.argv[2],
    "uv_version": sys.argv[3],
    "selection": "production",
    "sync_arguments": ["--frozen", "--no-default-groups"],
}
fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(document, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_name, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
finally:
    if os.path.exists(temp_name):
        os.unlink(temp_name)
PY
}

assert_services_inactive() {
  if ! command -v systemctl >/dev/null 2>&1; then
    [[ "$USE_SYSTEMD" != "true" ]] \
      || die "systemctl is required to prove supported services inactive before .venv promotion"
    log "  systemctl unavailable and --no-systemd selected; no supported systemd unit can be active"
    return 0
  fi
  local unit
  for unit in cryptorecorder-recorder.service \
              cryptorecorder-replay-build.service \
              cryptorecorder-replay-build.timer; do
    if systemctl is-active --quiet "$unit"; then
      die "service is active; stop supported units manually before .venv migration: $unit"
    fi
  done
}

build_candidate_environment() {
  local candidate="$1"
  [[ ! -e "$candidate" && ! -L "$candidate" ]] || die "candidate collision: $candidate"
  if ! (cd "$APP_DIR" && env UV_PROJECT_ENVIRONMENT="$candidate" \
      "$UV_BIN_RESOLVED" sync --frozen --no-default-groups); then
    die "candidate uv sync failed; preserved evidence at $candidate"
  fi
  if ! validate_production_environment "$candidate"; then
    die "candidate production validation failed; preserved evidence at $candidate"
  fi
  write_environment_marker "$candidate"
}

prepare_locked_environment() {
  log "Step 5/9: ensure locked production environment ($VENV)"
  local interrupted candidate backup failed stamp
  interrupted="$(find "$APP_DIR" -maxdepth 1 -mindepth 1 \
    \( -name '.venv.candidate.*' -o -name '.venv.failed.*' \) -print -quit 2>/dev/null || true)"
  [[ -z "$interrupted" ]] || die "ambiguous interrupted .venv migration evidence exists: $interrupted"

  if [[ -e "$VENV" || -L "$VENV" ]]; then
    validate_owned_directory "$VENV" "existing .venv"
    if environment_marker_matches "$VENV" && validate_production_environment "$VENV"; then
      log "  existing .venv matches uv.lock and production-only contract"
      return 0
    fi
    [[ "$MIGRATE_VENV" == "true" ]] || die "existing .venv is legacy/unrecognized; rerun only after review with --migrate-venv"
  fi

  stamp="$(date -u +%Y%m%dT%H%M%SZ).$$"
  candidate="$APP_DIR/.venv.candidate.$stamp"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "  would build and validate candidate $candidate from uv.lock"
    if [[ -e "$VENV" || -L "$VENV" ]]; then
      log "  would preserve existing .venv as a timestamped backup"
    fi
    return 0
  fi
  validate_owned_directory "$APP_DIR" "application directory"
  assert_services_inactive
  build_candidate_environment "$candidate"

  if [[ ! -e "$VENV" && ! -L "$VENV" ]]; then
    mv "$candidate" "$VENV"
    if ! validate_production_environment "$VENV"; then
      failed="$APP_DIR/.venv.failed.$stamp"
      mv "$VENV" "$failed"
      die "post-promotion validation failed; failed environment preserved at $failed"
    fi
    return 0
  fi

  backup="$APP_DIR/.venv.backup.$stamp"
  [[ ! -e "$backup" && ! -L "$backup" ]] || die "backup collision: $backup"
  mv "$VENV" "$backup"
  if ! mv "$candidate" "$VENV"; then
    mv "$backup" "$VENV"
    die "candidate promotion failed; prior .venv restored"
  fi
  if ! validate_production_environment "$VENV"; then
    failed="$APP_DIR/.venv.failed.$stamp"
    mv "$VENV" "$failed"
    if ! mv "$backup" "$VENV"; then
      die "post-promotion validation failed and rollback is ambiguous; inspect $failed and $backup"
    fi
    die "post-promotion validation failed; previous .venv restored and failed candidate preserved at $failed"
  fi
  log "  migration complete; previous environment preserved at $backup"
}

create_env_file() {
  log "Step 6/9: ensure env file ($ENV_FILE)"
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "  --no-systemd: skipping env-file creation under /etc"
    return 0
  fi
  if [[ -f "$ENV_FILE" ]]; then
    log "  env file exists; leaving untouched (never overwrite)"
    return 0
  fi
  run sudo mkdir -p "$(dirname "$ENV_FILE")"
  printf '    + render systemd/cryptorecorder.env.example -> %s (data-root=%s)\n' \
    "$ENV_FILE" "$DATA_ROOT"
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  sed "s#/data/cryptorecorder#$DATA_ROOT#g" \
    "$REPO_ROOT/systemd/cryptorecorder.env.example" | sudo tee "$ENV_FILE" >/dev/null
}

create_data_dirs() {
  log "Step 7/9: ensure data directories under $DATA_ROOT"
  local d
  for d in data_raw replay_store state \
           daily_build_reports archive_days; do
    run mkdir -p "$DATA_ROOT/$d"
  done
}

validate_unit_templates() {
  log "Validate replay-build unit policy (no service start)"
  local unit="$REPO_ROOT/systemd/cryptorecorder-replay-build.service"
  grep -q '^Type=oneshot$' "$unit" || die "replay-build unit must remain Type=oneshot"
  grep -q '^Restart=no$' "$unit" || die "replay-build unit must retain Restart=no"
  grep -q '^MemoryMax=12G$' "$unit" || die "replay-build unit must set MemoryMax=12G"
  grep -q '^MemorySwapMax=0$' "$unit" || die "replay-build unit must set MemorySwapMax=0"
  grep -q -- '--schema-version 2' "$unit" || die "replay-build unit must request schema 2"
  grep -q -- '--backlog-days 7' "$unit" || die "replay-build unit must bound backlog lookback"
  grep -q -- '--max-build-dates 3' "$unit" || die "replay-build unit must bound build dates"
}

run_validation() {
  log "Step 8/9: run locked production validation"
  if [[ -x "$VENV/bin/python" || "$DRY_RUN" == "true" ]]; then
    run env PYTHONPATH="$APP_DIR" "$VENV/bin/python" \
      -m validation.validate_dependency_environment \
      --kind production --uv-bin "$UV_BIN_RESOLVED"
  else
    die "locked production interpreter is missing: $VENV/bin/python"
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
  log "  uv:            $UV_BIN_RESOLVED ($UV_VERSION)"
  log "  lock-sha256:   $LOCK_SHA256"
}

install_units() {
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "Install units: skipped (--no-systemd)"
    return 0
  fi
  log "Install systemd units (rendered for user=$SERVICE_USER app-dir=$APP_DIR env-file=$ENV_FILE)"
  local unit
  for unit in $(all_units); do
    printf '    + render+install %s (user=%s app-dir=%s env-file=%s)\n' \
      "$unit" "$SERVICE_USER" "$APP_DIR" "$ENV_FILE"
    if [[ "$DRY_RUN" == "true" ]]; then
      continue
    fi
    sed \
      -e "s#/home/zsom/services/CryptoRecorder#$APP_DIR#g" \
      -e "s#^User=zsom#User=$SERVICE_USER#" \
      -e "s#^Group=zsom#Group=$SERVICE_USER#" \
      -e "s#/etc/cryptorecorder/cryptorecorder.env#$ENV_FILE#g" \
      "$REPO_ROOT/systemd/$unit" | sudo tee "/etc/systemd/system/$unit" >/dev/null
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
# longer ships: the pre-issue-#17 feature-build service group, every
# obsolete/renamed unit superseded by the current canonical names
# (crypto-recorder.service -> cryptorecorder-recorder.service,
# nautilus-convert.{service,timer} -> cryptorecorder-convert.{service,timer},
# cryptorecorder-daily-build.{service,timer} -> cryptorecorder-replay-build.{service,timer}),
# and cryptorecorder-convert.{service,timer} itself: the legacy converter
# (convert_day.py) is deployment-boundary work only now -- it remains
# required implementation/reference code for replay building, validation,
# and local test-computer catalog reconstruction (see docs/OPERATIONS.md),
# but production no longer runs it automatically. Only
# cryptorecorder-recorder.service and cryptorecorder-replay-build.timer are
# installed/enabled/started by this script.
# On servers deployed before these changes, the stale unit files may still be
# present under /etc/systemd/system and would otherwise keep firing the
# removed/renamed/retired command on their old schedule after this repo is
# upgraded. This step always runs (regardless of --target) so an upgrade to
# any target still cleans up every stale unit, and it always runs before
# install_units installs the canonical replacements.
STALE_UNITS=(
  cryptorecorder-feature-build.timer
  cryptorecorder-feature-build.service
  crypto-recorder.service
  nautilus-convert.timer
  nautilus-convert.service
  cryptorecorder-daily-build.timer
  cryptorecorder-daily-build.service
  cryptorecorder-convert.timer
  cryptorecorder-convert.service
)

cleanup_stale_units() {
  if [[ "$USE_SYSTEMD" != "true" ]]; then
    log "Cleanup stale units: skipped (--no-systemd)"
    return 0
  fi
  local stale_unit removed_any="false"
  for stale_unit in "${STALE_UNITS[@]}"; do
    if [[ -f "/etc/systemd/system/$stale_unit" ]]; then
      removed_any="true"
      log "Removing stale/obsolete unit from a previous deploy: $stale_unit"
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
  verify_uv_and_lock
  verify_app_checkout
  prepare_locked_environment
  create_env_file
  create_data_dirs
  run_validation
  validate_unit_templates
  cleanup_stale_units
  install_units
  control_units
  print_target
  log "Done."
}

main "$@"
