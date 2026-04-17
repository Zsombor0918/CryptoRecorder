"""
Main Binance Market Data Recorder using cryptofeed.

Records L2 depth deltas, trades, and periodic exchangeInfo metadata.
REST depth snapshots are DISABLED (causes 429/418 ban).
Futures are attempted but gracefully disabled if they fail.
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
from cryptofeed import FeedHandler
from cryptofeed.defines import BID, ASK, L2_BOOK, TRADES
from cryptofeed.exchanges import Binance, BinanceFutures

import config
from config import (
    VENUES,
    DEPTH_INTERVAL_MS,
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
from storage import StorageManager

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
feed_handler: Optional[FeedHandler] = None
futures_enabled: bool = True
futures_disabled_reason: str = ""

# Configurable watchdog timeout (seconds)
SHUTDOWN_WATCHDOG_SEC: int = int(os.environ.get(
    "CRYPTO_RECORDER_WATCHDOG_SEC", "120"))


# ============================================================================
# Symbol format helpers  (BTCUSDT ↔ BTC-USDT)
# ============================================================================

def _to_cf_symbols(raw_symbols: List[str], quote: str = "USDT") -> List[str]:
    """Convert Binance raw symbols to cryptofeed dash format.
    BTCUSDT → BTC-USDT
    """
    out = []
    for s in raw_symbols:
        if s.endswith(quote) and len(s) > len(quote):
            out.append(f"{s[:-len(quote)]}-{quote}")
        else:
            out.append(s)  # fallback – will likely fail in cf
    return out


def _to_cf_futures_symbols(raw_symbols: List[str], quote: str = "USDT") -> List[str]:
    """Convert Binance USDT-M futures symbols to cryptofeed format.
    BTCUSDT → BTC-USDT-PERP
    """
    out = []
    for s in raw_symbols:
        if s.endswith(quote) and len(s) > len(quote):
            out.append(f"{s[:-len(quote)]}-{quote}-PERP")
        else:
            out.append(s)
    return out


def _from_cf_symbol(sym: str) -> str:
    """Strip the dash/PERP that cryptofeed inserts: BTC-USDT-PERP → BTCUSDT."""
    s = sym.replace("-PERP", "")
    return s.replace("-", "")


# ============================================================================
# ExchangeInfo-Only Metadata Fetcher  (depth snapshots permanently disabled)
# ============================================================================

class MetadataFetcher:
    """Fetches exchangeInfo only.  No depth snapshots – they cause 429/418."""

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
    """Periodic disk check – runs until shutdown_event is set."""
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
# cryptofeed Callbacks  (fixed – no SortedDict usage)
# ============================================================================

async def on_l2_book(book, receipt_timestamp: float) -> None:
    """Handle L2 order book delta – store only delta bids/asks."""
    if shutdown_event and shutdown_event.is_set():
        return
    try:
        venue = ("BINANCE_USDTF" if "future" in str(book.exchange).lower()
                 else "BINANCE_SPOT")
        symbol = _from_cf_symbol(str(book.symbol))

        health_monitor.record_message(
            venue=venue, symbol=symbol,
            ts_event=int(book.timestamp * 1000) if book.timestamp else None,
            channel="depth",
        )

        # Extract delta bids/asks from the Decimal-keyed structures
        # Do NOT call book.book.bids.items() (SortedDict issue)
        delta_bids = []
        delta_asks = []
        if book.delta:
            for entry in book.delta.get(BID, []):
                delta_bids.append([str(entry[0]), str(entry[1])])
            for entry in book.delta.get(ASK, []):
                delta_asks.append([str(entry[0]), str(entry[1])])

        record = {
            'venue': venue,
            'symbol': symbol,
            'channel': 'depth',
            'ts_recv_ns': int(receipt_timestamp * 1e9),
            'ts_event_ms': (int(book.timestamp * 1000)
                            if book.timestamp
                            else int(receipt_timestamp * 1000)),
            'sequence_number': book.sequence_number,
            'payload': {'bids': delta_bids, 'asks': delta_asks},
        }
        await storage_manager.write_record(venue, symbol, "depth", record)

    except Exception as e:
        logger.error(f"Error in on_l2_book: {e}", exc_info=True)


async def on_trade(trade, receipt_timestamp: float) -> None:
    """Handle trade tick."""
    if shutdown_event and shutdown_event.is_set():
        return
    try:
        venue = ("BINANCE_USDTF" if "future" in str(trade.exchange).lower()
                 else "BINANCE_SPOT")
        symbol = _from_cf_symbol(str(trade.symbol))

        health_monitor.record_message(
            venue=venue, symbol=symbol,
            ts_event=int(trade.timestamp * 1000) if trade.timestamp else None,
            channel="trade",
        )

        record = {
            'venue': venue,
            'symbol': symbol,
            'channel': 'trade',
            'ts_recv_ns': int(receipt_timestamp * 1e9),
            'ts_event_ms': (int(trade.timestamp * 1000)
                            if trade.timestamp
                            else int(receipt_timestamp * 1000)),
            'payload': {
                'price': str(trade.price),
                'quantity': str(trade.amount),
                'side': trade.side,
                'trade_id': (str(trade.id)
                             if hasattr(trade, 'id') else None),
            },
        }
        await storage_manager.write_record(venue, symbol, "trade", record)

    except Exception as e:
        logger.error(f"Error in on_trade: {e}", exc_info=True)


# ============================================================================
# Metadata background task  (exchangeInfo only, no REST snapshots)
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

def _filter_symbols(symbols: List[str]) -> List[str]:
    """Remove symbols known to be unsupported by cryptofeed."""
    bad = {'USDCUSDT', 'EURC', 'EURI', 'FDUSD'}
    out = [s for s in symbols if not any(b in s for b in bad)]
    removed = len(symbols) - len(out)
    if removed:
        logger.info(f"Filtered {removed} unsupported symbols")
    return out


async def initialize() -> Dict[str, List[str]]:
    """Create infrastructure objects and fetch the trading universe."""
    global storage_manager, health_monitor, disk_monitor, shutdown_event

    logger.info("=" * 70)
    logger.info("Binance Market Data Recorder Starting")
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
    return universe


def _setup_feeds(universe: Dict[str, List[str]]) -> FeedHandler:
    """Build FeedHandler with Spot + (optional) Futures feeds."""
    global futures_enabled, futures_disabled_reason

    fh = FeedHandler()

    # ── Spot ──
    spot_raw = _filter_symbols(universe.get('BINANCE_SPOT', []))
    spot_cf = _to_cf_symbols(spot_raw)
    if spot_cf:
        try:
            fh.add_feed(Binance(
                symbols=spot_cf,
                channels=[L2_BOOK, TRADES],
                callbacks={L2_BOOK: on_l2_book, TRADES: on_trade},
                depth_interval=f"{DEPTH_INTERVAL_MS}ms",
            ))
            logger.info(f"Added {len(spot_cf)} Binance Spot symbols")
        except Exception as e:
            logger.error(f"Failed to initialise Binance Spot: {e}")

    # ── Futures (retry loop: strip symbols unknown to cryptofeed) ──
    import re as _re
    fut_raw = _filter_symbols(universe.get('BINANCE_USDTF', []))
    fut_cf = _to_cf_futures_symbols(fut_raw)
    if fut_cf:
        remaining = list(fut_cf)
        max_removals = len(remaining)  # allow stripping all if needed
        for _attempt in range(max_removals):
            if not remaining:
                futures_enabled = False
                futures_disabled_reason = "all futures symbols unsupported"
                logger.warning("Futures DISABLED – no valid symbols remained")
                break
            try:
                fh.add_feed(BinanceFutures(
                    symbols=remaining,
                    channels=[L2_BOOK, TRADES],
                    callbacks={L2_BOOK: on_l2_book, TRADES: on_trade},
                ))
                logger.info(f"Added {len(remaining)} Binance Futures symbols")
                break
            except Exception as e:
                m = _re.match(r'(.+) is not supported on', str(e))
                if m:
                    bad = m.group(1)
                    remaining = [s for s in remaining if s != bad]
                    logger.debug(f"Removed unsupported futures symbol: {bad}")
                else:
                    futures_enabled = False
                    futures_disabled_reason = str(e)
                    logger.warning(
                        f"Futures DISABLED – cryptofeed init failed: {e}")
                    break
        else:
            futures_enabled = False
            futures_disabled_reason = "all futures symbols unsupported"
            logger.warning("Futures DISABLED – no valid symbols remained")

    return fh


# ============================================================================
# Shutdown
# ============================================================================

async def shutdown(background_tasks: List[asyncio.Task]) -> None:
    """Stop feeds → cancel tasks → flush storage → write heartbeat."""
    import threading, os as _os

    # Watchdog: configurable hard timeout (default 120 s)
    def _force_exit():
        logger.warning("Shutdown watchdog fired – forcing exit")
        _os._exit(0)
    wd = threading.Timer(SHUTDOWN_WATCHDOG_SEC, _force_exit)
    wd.daemon = True
    wd.start()

    logger.info("Shutting down recorder …")

    # 1. Signal shutdown so callbacks stop enqueuing
    if shutdown_event:
        shutdown_event.set()

    # 2. Stop cryptofeed websockets (wait up to 10 s)
    if feed_handler:
        try:
            await asyncio.wait_for(
                feed_handler.stop_async(loop=asyncio.get_running_loop()),
                timeout=10)
        except asyncio.TimeoutError:
            logger.warning("FeedHandler stop timed out (10s) – continuing")
        except Exception as e:
            logger.warning(f"FeedHandler stop error (non-fatal): {e}")

    # 3. Cancel background tasks (heartbeat, disk check, metadata)
    for t in background_tasks:
        if not t.done():
            t.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)

    # 4. Flush writers → close & compress files
    if storage_manager:
        await storage_manager.shutdown()
    if disk_monitor:
        await disk_monitor.shutdown()

    # 5. Final heartbeat (with queue drop stats)
    if health_monitor:
        if storage_manager:
            health_monitor.queue_drop_total = storage_manager.get_total_drops()
            drops = storage_manager.get_drop_counts()
            # Only include writers with drops > 0 to keep heartbeat small
            health_monitor.queue_drop_by_writer = {
                k: v for k, v in drops.items() if v > 0
            }
        health_monitor.futures_enabled = futures_enabled
        health_monitor.futures_disabled_reason = futures_disabled_reason
        await health_monitor.write_heartbeat()
        logger.info(f"Final stats: {health_monitor.get_summary()}")

    # 6. Cancel any orphaned asyncio tasks (cryptofeed internals)
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


# ============================================================================\n# Drop stats feeder (keeps heartbeat in sync with queue drops)\n# ============================================================================

async def _update_drop_stats_task() -> None:
    """Periodically copy queue drop counts from StorageManager to HealthMonitor."""
    while not shutdown_event.is_set():
        try:
            if storage_manager and health_monitor:
                health_monitor.queue_drop_total = storage_manager.get_total_drops()
                drops = storage_manager.get_drop_counts()
                health_monitor.queue_drop_by_writer = {
                    k: v for k, v in drops.items() if v > 0
                }
        except Exception:
            pass
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=10)
            break
        except asyncio.TimeoutError:
            pass


# ============================================================================
# Main  –  single event loop, no threads, clean SIGINT
# ============================================================================

async def main() -> None:
    """
    Single-loop architecture.

    Uses ``fh.run(start_loop=False)`` so cryptofeed registers its feeds
    on *this* event loop instead of spawning its own.  All background
    tasks share the same loop → no cross-thread event-loop mismatch.
    """
    background_tasks: List[asyncio.Task] = []

    try:
        global feed_handler
        universe = await initialize()
        fh = _setup_feeds(universe)
        feed_handler = fh

        if not futures_enabled:
            logger.warning(f"FUTURES DISABLED: {futures_disabled_reason}")

        # Background tasks  – all on the same loop
        background_tasks.append(
            asyncio.create_task(health_monitor.heartbeat_task()))
        background_tasks.append(
            asyncio.create_task(_update_drop_stats_task()))
        background_tasks.append(
            asyncio.create_task(disk_check_task()))

        active_venues = ["BINANCE_SPOT"]
        if futures_enabled:
            active_venues.append("BINANCE_USDTF")
        background_tasks.append(
            asyncio.create_task(metadata_task(active_venues)))

        logger.info("Background tasks started")

        # Signal handling
        loop = asyncio.get_running_loop()

        def _on_signal(sig_num):
            logger.info(f"Received signal {sig_num}, shutting down …")
            shutdown_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _on_signal, sig)

        # Start feeds on THIS loop  (no blocking thread)
        logger.info("Starting feed handler …")
        fh.run(start_loop=False)

        # Wait until told to stop
        await shutdown_event.wait()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
    finally:
        await shutdown(background_tasks)


# ============================================================================
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
