"""
Main Binance Market Data Recorder — deterministic native architecture.

Records L2 depth and trades directly from Binance WebSockets, plus periodic
exchangeInfo metadata.  All raw streams are native Binance, and
recorder-owned committed ordering (session_seq / trade_session_seq) is
authoritative for replay.

No cryptofeed dependency.  No Phase 1/Phase 2 split.
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import config
from config import (
    VENUES,
    EXCHANGEINFO_INTERVAL_SEC,
    LOG_LEVEL,
    LOG_FILE,
    LOG_FORMAT,
    HEARTBEAT_INTERVAL_SEC,
    DISK_CHECK_INTERVAL_SEC,
    DISK_SOFT_LIMIT_GB,
    DISK_CLEANUP_TARGET_GB,
    DATA_ROOT,
    BINANCE_SPOT_REST,
    BINANCE_FUTURES_REST,
    TOP_SYMBOLS,
)
from binance_universe import UniverseSelector
from disk_monitor import DiskMonitor
from health_monitor import HealthMonitor
from native_trades import BinanceNativeTradeRecorder
from phase2_depth import BinanceNativeDepthRecorder
from storage import StorageManager
from time_utils import local_now_iso

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Global State
# ============================================================================

storage_manager: Optional[StorageManager] = None
health_monitor: Optional[HealthMonitor] = None
disk_monitor: Optional[DiskMonitor] = None
shutdown_event: Optional[asyncio.Event] = None
native_depth_recorder: Optional[BinanceNativeDepthRecorder] = None
native_trade_recorder: Optional[BinanceNativeTradeRecorder] = None
futures_enabled: bool = True
futures_disabled_reason: str = ""

# Configurable watchdog timeout (seconds)
SHUTDOWN_WATCHDOG_SEC: int = int(os.environ.get(
    "CRYPTO_RECORDER_WATCHDOG_SEC", "120"))

SPOT_COVERAGE_WARN_RATIO: float = 0.90
FUTURES_COVERAGE_WARN_RATIO: float = 0.80


# ============================================================================
# Symbol helpers  (native Binance format — no dash conversion needed)
# ============================================================================

def _find_suspicious_symbols(venue: str, raw_symbols: List[str], quote: str = "USDT") -> List[str]:
    """Warn about unusual symbol shapes without blocking them."""
    warnings: List[str] = []
    for symbol in raw_symbols:
        reasons: List[str] = []
        if not symbol.isascii():
            reasons.append("contains non-ASCII characters")

        if symbol.endswith(quote) and len(symbol) > len(quote):
            base = symbol[:-len(quote)]
            if len(base) < 2:
                reasons.append(f"base asset shorter than 2 chars ({base!r})")
            if base.isascii() and not all(ch.isalnum() for ch in base):
                reasons.append("unexpected base asset characters")
        else:
            reasons.append(f"unexpected symbol structure for quote {quote}")

        if reasons:
            warnings.append(f"{venue} suspicious symbol {symbol}: {'; '.join(reasons)}")
    return warnings


def _make_venue_coverage(venue: str, requested_raw: List[str], suspicious_warnings: List[str]) -> Dict[str, Any]:
    return {
        "venue": venue,
        "requested_raw": list(requested_raw),
        "requested_count": len(requested_raw),
        "active_raw": list(requested_raw),
        "active_count": len(requested_raw),
        "coverage_ratio": 1.0 if requested_raw else 0.0,
        "warnings": list(suspicious_warnings),
    }


def _emit_symbol_audit_logs(label: str, requested_raw: List[str], warnings: List[str]) -> None:
    """Log requested symbols and suspicious selections before feed init."""
    logger.info(f"{label} requested: {len(requested_raw)}")
    logger.debug(f"{label} requested symbols (raw): {', '.join(requested_raw)}")
    for warning in warnings:
        logger.warning(warning)


def _persist_startup_coverage(coverage: Dict[str, Any]) -> None:
    """Write startup coverage summary for later audit."""
    out = config.STATE_ROOT / "startup_coverage.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(coverage, indent=2, ensure_ascii=False))
    logger.info(f"Startup coverage report → {out}")


# ============================================================================
# ExchangeInfo Metadata Fetcher
# ============================================================================

class MetadataFetcher:
    """Fetches exchangeInfo only.  No depth snapshots — they cause 429/418."""

    def __init__(self, storage: StorageManager):
        self.storage = storage
        self.last_exchangeinfo_time: Dict[str, float] = {}

    async def maybe_fetch_exchangeinfo(self, venue: str) -> None:
        now = time.time()
        if venue in self.last_exchangeinfo_time:
            if now - self.last_exchangeinfo_time[venue] < EXCHANGEINFO_INTERVAL_SEC:
                return
        try:
            data = await self._fetch_exchangeinfo(venue)
            if data:
                await self.storage.write_record(
                    venue, "EXCHANGEINFO", "exchangeinfo", data)
                self.last_exchangeinfo_time[venue] = now
        except Exception as e:
            logger.error(f"Error fetching exchangeInfo for {venue}: {e}")

    async def _fetch_exchangeinfo(self, venue: str) -> Optional[dict]:
        url = (f"{BINANCE_SPOT_REST}/api/v3/exchangeInfo"
               if venue == "BINANCE_SPOT"
               else f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        raw = await resp.json()
                        return {
                            'venue': venue,
                            'symbol': 'EXCHANGEINFO',
                            'channel': 'exchangeinfo',
                            'ts_recv_ns': time.time_ns(),
                            'ts_event_ms': raw.get('serverTime'),
                            'timezone': raw.get('timezone'),
                            'symbols': raw.get('symbols', []),
                        }
                    logger.warning(
                        f"exchangeInfo {venue} returned {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"exchangeInfo error ({venue}): {e}")
            return None


# ============================================================================
# Disk check  (delegates to DiskMonitor)
# ============================================================================

async def disk_check_task() -> None:
    """Periodic disk check — runs until shutdown_event is set."""
    if not disk_monitor:
        return
    while not shutdown_event.is_set():
        try:
            usage = await disk_monitor.check_disk_usage()
            total = usage.get('total_gb', 0)
            if total > DISK_SOFT_LIMIT_GB:
                logger.warning(
                    f"Disk {total:.1f}GB > soft limit {DISK_SOFT_LIMIT_GB}GB")
                await disk_monitor.cleanup_old_data()
        except Exception as e:
            logger.error(f"disk_check_task error: {e}", exc_info=True)
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=DISK_CHECK_INTERVAL_SEC)
            break
        except asyncio.TimeoutError:
            pass


# ============================================================================
# Metadata background task  (exchangeInfo only)
# ============================================================================

async def metadata_task(venues: List[str]) -> None:
    """Periodically fetch exchangeInfo for each active venue."""
    fetcher = MetadataFetcher(storage_manager)
    while not shutdown_event.is_set():
        try:
            for v in venues:
                await fetcher.maybe_fetch_exchangeinfo(v)
        except Exception as e:
            logger.error(f"metadata_task error: {e}", exc_info=True)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            pass


# ============================================================================
# Initialisation
# ============================================================================

async def initialize() -> Dict[str, Any]:
    """Create infrastructure objects and fetch the trading universe."""
    global storage_manager, health_monitor, disk_monitor, shutdown_event

    logger.info("=" * 70)
    logger.info("Binance Market Data Recorder Starting (deterministic native)")
    logger.info("=" * 70)

    shutdown_event = asyncio.Event()
    storage_manager = StorageManager()
    health_monitor = HealthMonitor()
    disk_monitor = DiskMonitor(config)

    selector = UniverseSelector()
    universe = await selector.get_or_select_universe()

    # Trim to TOP_SYMBOLS (cache may have more from a previous run)
    for venue in ('BINANCE_SPOT', 'BINANCE_USDTF'):
        if venue in universe and len(universe[venue]) > TOP_SYMBOLS:
            universe[venue] = universe[venue][:TOP_SYMBOLS]

    spot_n = len(universe.get('BINANCE_SPOT', []))
    fut_n = len(universe.get('BINANCE_USDTF', []))
    logger.info(f"Universe: Spot={spot_n}, Futures={fut_n}")
    _emit_symbol_audit_logs(
        "Spot",
        universe.get('BINANCE_SPOT', []),
        _find_suspicious_symbols("BINANCE_SPOT", universe.get('BINANCE_SPOT', [])),
    )
    _emit_symbol_audit_logs(
        "Futures",
        universe.get('BINANCE_USDTF', []),
        _find_suspicious_symbols("BINANCE_USDTF", universe.get('BINANCE_USDTF', [])),
    )
    return universe


def _build_startup_coverage(universe: Dict[str, Any]) -> Dict[str, Any]:
    """Build startup coverage report for all venues — no cryptofeed friction."""
    coverage: Dict[str, Any] = {
        "timestamp": local_now_iso(),
        "architecture": "deterministic_native",
        "spot": _make_venue_coverage(
            "BINANCE_SPOT",
            universe.get('BINANCE_SPOT', []),
            _find_suspicious_symbols("BINANCE_SPOT", universe.get('BINANCE_SPOT', [])),
        ),
        "futures": _make_venue_coverage(
            "BINANCE_USDTF",
            universe.get('BINANCE_USDTF', []),
            _find_suspicious_symbols("BINANCE_USDTF", universe.get('BINANCE_USDTF', [])),
        ),
        "warnings": [],
    }
    coverage["warnings"].extend(coverage["spot"]["warnings"])
    coverage["warnings"].extend(coverage["futures"]["warnings"])

    # Include universe selection metadata if available
    selection_metadata = universe.get("selection_metadata", {}) if isinstance(universe, dict) else {}
    for venue_key, cov_key in [("BINANCE_SPOT", "spot"), ("BINANCE_USDTF", "futures")]:
        sel = selection_metadata.get(venue_key, {}) if isinstance(selection_metadata, dict) else {}
        coverage[cov_key]["candidate_pool"] = sel.get("candidate_pool_count", 0)
        coverage[cov_key]["pre_filter_rejected_count"] = sel.get(
            "pre_filter_rejected_count",
            sel.get("rejected_pre_filter_count", 0),
        )
        coverage[cov_key]["pre_filter_rejected_sample"] = sel.get(
            "pre_filter_rejected_sample",
            sel.get("rejected_pre_filter_sample", []),
        )

    return coverage


# ============================================================================
# Shutdown
# ============================================================================

def _refresh_trade_health_from_recorder() -> None:
    """Copy live native trade diagnostics into heartbeat state."""
    if native_trade_recorder is None or health_monitor is None:
        return
    try:
        health_monitor.set_trade_health(native_trade_recorder.get_venue_diagnostics())
    except Exception as exc:
        logger.debug("Could not refresh runtime trade health: %s", exc)


async def shutdown(background_tasks: List[asyncio.Task]) -> None:
    """Stop recorders → cancel tasks → flush storage → write heartbeat."""
    import threading, os as _os
    global native_depth_recorder, native_trade_recorder

    # Watchdog: configurable hard timeout (default 120 s)
    def _force_exit():
        logger.warning("Shutdown watchdog fired — forcing exit")
        _os._exit(0)
    wd = threading.Timer(SHUTDOWN_WATCHDOG_SEC, _force_exit)
    wd.daemon = True
    wd.start()

    logger.info("Shutting down recorder …")

    # 1. Signal shutdown so callbacks stop enqueuing
    if shutdown_event:
        shutdown_event.set()

    # 2. Stop native depth recorder
    if native_depth_recorder is not None:
        try:
            await asyncio.wait_for(native_depth_recorder.shutdown(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning("Native depth recorder shutdown timed out (10s) — continuing")
        except Exception as e:
            logger.warning(f"Native depth recorder shutdown error (non-fatal): {e}")
        native_depth_recorder = None

    # 3. Stop native trade recorder
    if native_trade_recorder is not None:
        try:
            await asyncio.wait_for(native_trade_recorder.shutdown(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning("Native trade recorder shutdown timed out (10s) — continuing")
        except Exception as e:
            logger.warning(f"Native trade recorder shutdown error (non-fatal): {e}")

        # Capture trade diagnostics before setting recorder to None
        _refresh_trade_health_from_recorder()

        native_trade_recorder = None

    # 4. Cancel background tasks (heartbeat, disk check, metadata)
    for t in background_tasks:
        if not t.done():
            t.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)

    # 5. Flush writers → close & compress files
    if storage_manager:
        await storage_manager.shutdown()
    if disk_monitor:
        await disk_monitor.shutdown()

    # 6. Final heartbeat (with queue drop stats)
    if health_monitor:
        if storage_manager:
            health_monitor.queue_drop_total = storage_manager.get_total_drops()
            drops = storage_manager.get_drop_counts()
            health_monitor.queue_drop_by_writer = {
                k: v for k, v in drops.items() if v > 0
            }
        health_monitor.futures_enabled = futures_enabled
        health_monitor.futures_disabled_reason = futures_disabled_reason
        await health_monitor.write_heartbeat()
        health_monitor.write_universe_health_checkpoint(force=True)
        logger.info(f"Final stats: {health_monitor.get_summary()}")

    # 7. Cancel any orphaned asyncio tasks
    current = asyncio.current_task()
    orphans = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    for t in orphans:
        t.cancel()
    if orphans:
        try:
            await asyncio.wait_for(
                asyncio.gather(*orphans, return_exceptions=True), timeout=5)
        except asyncio.TimeoutError:
            logger.debug(f"{len(orphans)} orphan tasks did not cancel in 5s")

    wd.cancel()
    logger.info("Recorder shutdown complete")


# ============================================================================
# Drop stats feeder (keeps heartbeat in sync with queue drops)
# ============================================================================

async def _update_drop_stats_task() -> None:
    """Periodically copy runtime diagnostics into HealthMonitor."""
    while not shutdown_event.is_set():
        try:
            if storage_manager and health_monitor:
                health_monitor.queue_drop_total = storage_manager.get_total_drops()
                drops = storage_manager.get_drop_counts()
                health_monitor.queue_drop_by_writer = {
                    k: v for k, v in drops.items() if v > 0
                }
            _refresh_trade_health_from_recorder()
        except Exception:
            pass
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=10)
            break
        except asyncio.TimeoutError:
            pass


# ============================================================================
# Main — single event loop, native WS only, clean SIGINT
# ============================================================================

async def main() -> None:
    """Single-loop architecture with native Binance WebSocket feeds.

    Launches independent depth and trade recorders plus background tasks
    for heartbeat, disk monitoring, and metadata fetching.
    """
    background_tasks: List[asyncio.Task] = []

    try:
        global native_depth_recorder, native_trade_recorder
        universe = await initialize()
        startup_coverage = _build_startup_coverage(universe)

        health_monitor.futures_enabled = futures_enabled
        health_monitor.futures_disabled_reason = futures_disabled_reason
        health_monitor.set_startup_coverage(startup_coverage)
        _persist_startup_coverage(startup_coverage)

        if not futures_enabled:
            logger.warning(f"FUTURES DISABLED: {futures_disabled_reason}")

        # Background tasks — all on the same loop
        background_tasks.append(
            asyncio.create_task(health_monitor.heartbeat_task()))
        background_tasks.append(
            asyncio.create_task(_update_drop_stats_task()))
        background_tasks.append(
            asyncio.create_task(disk_check_task()))

        active_venues: List[str] = []
        for venue in ("BINANCE_SPOT", "BINANCE_USDTF"):
            if universe.get(venue):
                active_venues.append(venue)
        logger.info(f"Metadata active venues: {active_venues}")
        background_tasks.append(
            asyncio.create_task(metadata_task(active_venues)))

        # Build symbol map for native recorders
        native_symbols = {
            venue: list(symbols)
            for venue, symbols in universe.items()
            if venue in ("BINANCE_SPOT", "BINANCE_USDTF") and symbols
        }

        # Launch native depth recorder
        native_depth_recorder = BinanceNativeDepthRecorder(
            storage_manager=storage_manager,
            health_monitor=health_monitor,
            shutdown_event=shutdown_event,
        )
        background_tasks.append(
            asyncio.create_task(native_depth_recorder.run(native_symbols))
        )
        logger.info("Native depth recorder enabled")

        # Launch native trade recorder
        native_trade_recorder = BinanceNativeTradeRecorder(
            storage_manager=storage_manager,
            health_monitor=health_monitor,
            shutdown_event=shutdown_event,
        )
        background_tasks.append(
            asyncio.create_task(native_trade_recorder.run(native_symbols))
        )
        logger.info("Native trade recorder enabled")

        logger.info("Background tasks started")

        # Signal handling
        loop = asyncio.get_running_loop()

        def _on_signal(sig_num):
            logger.info(f"Received signal {sig_num}, shutting down …")
            shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _on_signal, sig)

        # Wait until told to stop
        await shutdown_event.wait()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
    finally:
        await shutdown(background_tasks)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Unhandled: {e}", exc_info=True)
        sys.exit(1)
    finally:
        sys.exit(0)
