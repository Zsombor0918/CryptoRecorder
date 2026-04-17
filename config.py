"""
Configuration and constants for Binance Market Data Recorder.
"""
from pathlib import Path
from typing import Final

# ============================================================================
# Paths
# ============================================================================

PROJECT_ROOT: Final = Path(__file__).parent
DATA_ROOT: Final = PROJECT_ROOT / "data_raw"
META_ROOT: Final = PROJECT_ROOT / "meta"
STATE_ROOT: Final = PROJECT_ROOT / "state"

# Data subdirectories (snapshot removed – REST snapshots cause 429/418)
CHANNELS: Final = ["depth", "trade", "exchangeinfo"]

# Create required directories on import
for directory in [DATA_ROOT, META_ROOT, STATE_ROOT, STATE_ROOT / "convert_reports"]:
    directory.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Binance API Endpoints
# ============================================================================

BINANCE_SPOT_REST: Final = "https://api.binance.com"
BINANCE_FUTURES_REST: Final = "https://fapi.binance.com"

# ============================================================================
# Venues & Symbols
# ============================================================================

VENUES: Final = ["BINANCE_SPOT", "BINANCE_USDTF"]
VENUE_FULL_NAMES: Final = {
    "BINANCE_SPOT": "Binance Spot",
    "BINANCE_USDTF": "Binance USDT-M Futures",
}

import os as _os

# Target number of symbols per venue (selected by 24h quote volume)
# Override with CRYPTO_RECORDER_TOP_SYMBOLS env var for testing
TOP_SYMBOLS: Final = int(_os.environ.get("CRYPTO_RECORDER_TOP_SYMBOLS", "50"))

# Base quote asset for universe selection
QUOTE_ASSET_SPOT: Final = "USDT"
QUOTE_ASSET_FUTURES: Final = "USDT"  # For USDT-M perpetuals

# ============================================================================
# Stream Configuration
# ============================================================================

# WebSocket depth update frequency (100ms or 1000ms)
DEPTH_INTERVAL_MS: Final = 100

# REST snapshot interval (seconds)
SNAPSHOT_INTERVAL_SEC: Final = 600  # 10 minutes

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
# Monitoring & Health
# ============================================================================

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL_SEC: Final = 30

# Health check interval (seconds)
HEALTH_CHECK_INTERVAL_SEC: Final = 10

# ============================================================================
# Disk Management (expanded to 500GB capacity)
# ============================================================================

# Disk usage check interval (seconds)
DISK_CHECK_INTERVAL_SEC: Final = 600  # 10 minutes

# Disk usage limits (GB) - for 500GB total capacity
DISK_SOFT_LIMIT_GB: Final = 400  # Warn at 80% (400GB)
DISK_HARD_LIMIT_GB: Final = 480  # Critical at 96% (480GB)
DISK_CLEANUP_TARGET_GB: Final = 350  # Clean down to 70% (350GB)

# ============================================================================
# Logging Configuration
# ============================================================================

LOG_LEVEL: Final = "INFO"
LOG_FILE: Final = PROJECT_ROOT / "recorder.log"
LOG_FORMAT: Final = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ============================================================================
# Queue Configuration
# ============================================================================

# Max size of in-memory queues (to prevent memory bloat)
QUEUE_MAX_SIZE: Final = 10000

# Writer batch size (items per write)
WRITER_BATCH_SIZE: Final = 100

# Writer flush interval (seconds)
WRITER_FLUSH_INTERVAL_SEC: Final = 5

# ============================================================================
# Network Configuration
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
# Converter Configuration (for Nautilus conversion)
# ============================================================================

NAUTILUS_CATALOG_ROOT: Final = PROJECT_ROOT.parent / "nautilus_data" / "catalog"
CONVERTER_BATCH_SIZE: Final = 1000

# Raw data retention days (converter will not delete raw data < this many days old)
RAW_RETENTION_DAYS: Final = 7

# ============================================================================
# Test Mode Configuration (for validation/smoke testing)
# ============================================================================

# Test mode overrides (set via environment or direct import)
TEST_MODE: Final = False
TEST_MODE_SYMBOLS: Final = 3  # Use only 3 symbols in test mode
TEST_MODE_SNAPSHOT_INTERVAL_SEC: Final = 60  # Faster snapshots in test (vs 600s)
TEST_MODE_HEARTBEAT_INTERVAL_SEC: Final = 5  # Faster heartbeat in test (vs 30s)
