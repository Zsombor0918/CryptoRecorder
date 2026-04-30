"""
Configuration and constants for CryptoRecorder.
"""
import os
from pathlib import Path
from typing import Final

# ============================================================================
# Paths
# ============================================================================

PROJECT_ROOT: Final = Path(__file__).parent
DATA_ROOT: Final = PROJECT_ROOT / "data_raw"
META_ROOT: Final = PROJECT_ROOT / "meta"
STATE_ROOT: Final = PROJECT_ROOT / "state"

# Canonical data channels.  depth_v2 and trade_v2 are the only raw
# sources on the deterministic-native mainline.
CHANNELS: Final = ["depth_v2", "trade_v2", "exchangeinfo"]

# Create required directories on import
for directory in [DATA_ROOT, META_ROOT, STATE_ROOT, STATE_ROOT / "convert_reports"]:
    directory.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Binance API Endpoints
# ============================================================================

BINANCE_SPOT_REST: Final = "https://api.binance.com"
BINANCE_FUTURES_REST: Final = "https://fapi.binance.com"

# ============================================================================
# Venue / selection policy
# ============================================================================

VENUES: Final = ["BINANCE_SPOT", "BINANCE_USDTF"]
VENUE_FULL_NAMES: Final = {
    "BINANCE_SPOT": "Binance Spot",
    "BINANCE_USDTF": "Binance USDT-M Futures",
}

# Target number of symbols per venue (selected by 24h quote volume)
# Override with CRYPTO_RECORDER_TOP_SYMBOLS env var for testing
TOP_SYMBOLS: Final = int(os.environ.get("CRYPTO_RECORDER_TOP_SYMBOLS", "50"))

# Universe selection uses a larger ranked candidate pool before applying
# sanity/support filters, then keeps the best TOP_SYMBOLS survivors.
TOP_SYMBOL_CANDIDATES: Final = int(
    os.environ.get("CRYPTO_RECORDER_TOP_SYMBOL_CANDIDATES", "120")
)
FUTURES_TOP_SYMBOL_CANDIDATES: Final = int(
    os.environ.get("CRYPTO_RECORDER_FUTURES_TOP_SYMBOL_CANDIDATES", "200")
)

# Selection policy metadata written into the cached universe files.
UNIVERSE_FILTER_VERSION: Final = "v6_spot_support_cache_trade_health"
UNIVERSE_REJECT_SAMPLE_SIZE: Final = 12

# Base quote asset for universe selection
QUOTE_ASSET_SPOT: Final = "USDT"
QUOTE_ASSET_FUTURES: Final = "USDT"  # For USDT-M perpetuals

# ============================================================================
# Recorder runtime
# ============================================================================

# WebSocket depth update frequency (100ms or 1000ms)
DEPTH_INTERVAL_MS: Final = 100

# Canonical raw channel names.
DEPTH_V2_CHANNEL: Final = "depth_v2"
TRADE_V2_CHANNEL: Final = "trade_v2"

TRADE_WS_SHARD_ENABLED: Final = (
    os.environ.get("CRYPTO_RECORDER_TRADE_WS_SHARD_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
TRADE_WS_MAX_SYMBOLS_PER_CONNECTION: Final = int(
    os.environ.get("CRYPTO_RECORDER_TRADE_WS_MAX_SYMBOLS_PER_CONNECTION", "25")
)
TRADE_WS_FIRST_MESSAGE_TIMEOUT_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_TRADE_WS_FIRST_MESSAGE_TIMEOUT_SEC", "15")
)
TRADE_WS_IDLE_TIMEOUT_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_TRADE_WS_IDLE_TIMEOUT_SEC", "120")
)

# ── Futures trade-stream mode fallback ────────────────────────────────────────
# BINANCE_USDTF normally uses @aggTrade.  If no messages arrive after the probe
# timeout, the recorder optionally probes @trade on a single well-known symbol.
# If @trade works the full shard is reconnected using @trade.
FUTURES_TRADE_WS_FALLBACK_ENABLED: Final = (
    os.environ.get("CRYPTO_RECORDER_FUTURES_TRADE_WS_FALLBACK_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
FUTURES_TRADE_WS_PROBE_SYMBOL: Final = os.environ.get(
    "CRYPTO_RECORDER_FUTURES_TRADE_WS_PROBE_SYMBOL", "BTCUSDT"
).strip().upper()
FUTURES_TRADE_WS_FALLBACK_PROBE_TIMEOUT_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_FUTURES_TRADE_WS_FALLBACK_PROBE_TIMEOUT_SEC", "10")
)

# ── Adaptive shard split ──────────────────────────────────────────────────────
# When a futures trade shard gets its first-message timeout, the shard is
# progressively split into smaller chunks to identify poisoned symbols.
TRADE_WS_SHARD_ADAPTIVE_SPLIT_ENABLED: Final = (
    os.environ.get("CRYPTO_RECORDER_TRADE_WS_SHARD_ADAPTIVE_SPLIT_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
# Ordered sequence of chunk sizes to try on successive first-message timeouts.
# Default: try 5, then 1 (single-symbol probe).
_shard_split_env = os.environ.get("CRYPTO_RECORDER_TRADE_WS_SHARD_SPLIT_SIZES", "5,1")
TRADE_WS_SHARD_SPLIT_SIZES: Final[list[int]] = [
    int(x.strip()) for x in _shard_split_env.split(",") if x.strip().isdigit()
] or [5, 1]
TRADE_HEALTH_HIGH_LIQUIDITY_USDTF: Final = tuple(
    symbol.strip().upper()
    for symbol in os.environ.get(
        "CRYPTO_RECORDER_TRADE_HEALTH_HIGH_LIQUIDITY_USDTF",
        "BTCUSDT,ETHUSDT,BNBUSDT",
    ).split(",")
    if symbol.strip()
)

DEPTH_WS_SHARD_ENABLED: Final = (
    os.environ.get("CRYPTO_RECORDER_DEPTH_WS_SHARD_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
DEPTH_WS_MAX_SYMBOLS_PER_CONNECTION: Final = int(
    os.environ.get("CRYPTO_RECORDER_DEPTH_WS_MAX_SYMBOLS_PER_CONNECTION", "25")
)

# Optional derived depth snapshot defaults (converter-side).
#
# Nautilus currently exposes OrderBookDepth10 as the catalog-native snapshot
# type. OrderBookDeltas remains the full-depth replay source.
EMIT_DERIVED_DEPTH_SNAPSHOTS_DEFAULT: Final = (
    os.environ.get(
        "CRYPTO_RECORDER_EMIT_DERIVED_DEPTH_SNAPSHOTS",
        os.environ.get("CRYPTO_RECORDER_EMIT_DEPTH10", "1"),
    ).strip().lower()
    in {"1", "true", "yes", "on"}
)
DERIVED_DEPTH_SNAPSHOT_INTERVAL_SEC: Final = float(
    os.environ.get(
        "CRYPTO_RECORDER_DERIVED_DEPTH_SNAPSHOT_INTERVAL_SEC",
        os.environ.get("CRYPTO_RECORDER_DEPTH10_INTERVAL_SEC", "1.0"),
    )
)
DERIVED_DEPTH_SNAPSHOT_LEVELS: Final = int(
    os.environ.get("CRYPTO_RECORDER_DERIVED_DEPTH_SNAPSHOT_LEVELS", "10")
)

# Backward-compatible aliases.
EMIT_DEPTH10_DEFAULT: Final = EMIT_DERIVED_DEPTH_SNAPSHOTS_DEFAULT
DEPTH10_INTERVAL_SEC: Final = DERIVED_DEPTH_SNAPSHOT_INTERVAL_SEC

# Exchange info fetch interval (seconds)
EXCHANGEINFO_INTERVAL_SEC: Final = 21600  # 6 hours

# ============================================================================
# Storage Configuration
# ============================================================================

# File rotation interval (minutes)
ROTATION_INTERVAL_MIN: Final = 60  # Hourly rotation

# Compression format (must be 'zstd')
COMPRESSION_FORMAT: Final = "zstd"

# ============================================================================
# Monitoring / observability
# ============================================================================

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL_SEC: Final = 30

# Health check interval (seconds)
HEALTH_CHECK_INTERVAL_SEC: Final = 10

UNIVERSE_HEALTH_ENABLED: Final = (
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_HEALTH_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
UNIVERSE_ZERO_MESSAGE_GRACE_SEC: Final = int(
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_ZERO_MESSAGE_GRACE_SEC", "300")
)
UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS: Final = int(
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS", "2")
)
UNIVERSE_HEALTH_EXCLUDE_DAYS: Final = int(
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_HEALTH_EXCLUDE_DAYS", "1")
)
UNIVERSE_HEALTH_MIN_OBSERVATION_SEC: Final = int(
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_HEALTH_MIN_OBSERVATION_SEC", "300")
)
UNIVERSE_HEALTH_CHECKPOINT_INTERVAL_SEC: Final = int(
    os.environ.get("CRYPTO_RECORDER_UNIVERSE_HEALTH_CHECKPOINT_INTERVAL_SEC", "120")
)

# Human-facing timestamps in reports/heartbeat use Hungary local time.
REPORT_TIMEZONE_NAME: Final = "Europe/Budapest"

# ============================================================================
# Disk management
# ============================================================================

# Disk usage check interval (seconds)
DISK_CHECK_INTERVAL_SEC: Final = 600  # 10 minutes

# Disk usage limits (GB)
DISK_SOFT_LIMIT_GB: Final = 400
DISK_HARD_LIMIT_GB: Final = 480
DISK_CLEANUP_TARGET_GB: Final = 350

# ============================================================================
# Logging Configuration
# ============================================================================

LOG_LEVEL: Final = "INFO"
LOG_FILE: Final = PROJECT_ROOT / "recorder.log"
LOG_FORMAT: Final = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ============================================================================
# Queue / writer backpressure
# ============================================================================

# Max size of in-memory queues (to prevent memory bloat)
QUEUE_MAX_SIZE: Final = 10000

# Writer batch size (items per write)
WRITER_BATCH_SIZE: Final = 100

# Writer flush interval (seconds)
WRITER_FLUSH_INTERVAL_SEC: Final = 5

# ============================================================================
# Network / reconnect
# ============================================================================

# API request timeout (seconds)
API_REQUEST_TIMEOUT_SEC: Final = 30

# WebSocket ping/pong interval (seconds)
WS_PING_INTERVAL_SEC: Final = 30

# Reconnect attempt intervals (seconds)
RECONNECT_INITIAL_DELAY_SEC: Final = 1
RECONNECT_MAX_DELAY_SEC: Final = 60
RECONNECT_MAX_ATTEMPTS: Final = 0  # 0 = unlimited

# ============================================================================
# Phase 2 native depth snapshot / resync controls
# ============================================================================

PHASE2_SNAPSHOT_LIMIT: Final = int(
    os.environ.get("CRYPTO_RECORDER_PHASE2_SNAPSHOT_LIMIT", "1000")
)
PHASE2_SNAPSHOT_MAX_CONCURRENCY_PER_VENUE: Final = int(
    os.environ.get("CRYPTO_RECORDER_PHASE2_SNAPSHOT_MAX_CONCURRENCY_PER_VENUE", "2")
)
PHASE2_SNAPSHOT_MIN_DELAY_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_PHASE2_SNAPSHOT_MIN_DELAY_SEC", "0.35")
)
PHASE2_SNAPSHOT_RETRY_MAX_ATTEMPTS: Final = int(
    os.environ.get("CRYPTO_RECORDER_PHASE2_SNAPSHOT_RETRY_MAX_ATTEMPTS", "5")
)
PHASE2_SNAPSHOT_RETRY_BASE_DELAY_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_PHASE2_SNAPSHOT_RETRY_BASE_DELAY_SEC", "1.5")
)
PHASE2_RESYNC_COOLDOWN_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_PHASE2_RESYNC_COOLDOWN_SEC", "5.0")
)
PHASE2_MAX_RESYNCS_PER_SYMBOL_WINDOW: Final = int(
    os.environ.get("CRYPTO_RECORDER_PHASE2_MAX_RESYNCS_PER_SYMBOL_WINDOW", "6")
)
PHASE2_RESYNC_WINDOW_SEC: Final = float(
    os.environ.get("CRYPTO_RECORDER_PHASE2_RESYNC_WINDOW_SEC", "300")
)

# ============================================================================
# Converter / catalog output
# ============================================================================

NAUTILUS_CATALOG_ROOT: Final = PROJECT_ROOT.parent / "nautilus_data" / "catalog"
CONVERTER_BATCH_SIZE: Final = 1000
MIN_TRADE_RECORDS_FOR_FULL_READY: Final = int(
    os.environ.get("CRYPTO_RECORDER_MIN_TRADE_RECORDS_FOR_FULL_READY", "1")
)

# Raw data retention days for disk cleanup.
RAW_RETENTION_DAYS: Final = 7

# ============================================================================
# Validation / test mode
# ============================================================================

# Validation helpers kept here so smoke tests share one source of truth.
TEST_MODE: Final = False
TEST_MODE_SYMBOLS: Final = 3  # Use only 3 symbols in test mode
TEST_MODE_SNAPSHOT_INTERVAL_SEC: Final = 60  # Legacy placeholder, snapshots disabled.
TEST_MODE_HEARTBEAT_INTERVAL_SEC: Final = 5  # Faster heartbeat in test (vs 30s)
