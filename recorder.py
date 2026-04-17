"""
Main Binance Market Data Recorder using cryptofeed.
Records L2 depth deltas, trades, snapshots, and instrument metadata.
"""
import asyncio
import json
import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
from cryptofeed import FeedHandler
from cryptofeed.defines import BID, ASK, L2_BOOK, TRADES
from cryptofeed.exchanges import Binance, BinanceFutures
from cryptofeed.symbols import Symbol

import config
from config import (
    VENUES,
    DEPTH_INTERVAL_MS,
    SNAPSHOT_INTERVAL_SEC,
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
)
from binance_universe import UniverseSelector
from disk_monitor import DiskMonitor
from health_monitor import HealthMonitor
from storage import StorageManager

# ============================================================================
# Logging Setup
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
feed_handler: Optional[FeedHandler] = None
background_tasks: List[asyncio.Task] = []
shutdown_event: Optional[asyncio.Event] = None
snapshot_fetcher = None


# ============================================================================
# Snapshot & Metadata Fetching
# ============================================================================

class SnapshotFetcher:
    """Fetches REST snapshots and exchange info."""
    
    def __init__(self, storage: StorageManager, monitor: HealthMonitor):
        self.storage = storage
        self.monitor = monitor
        self.last_snapshot_time: Dict[tuple, float] = {}  # (venue, symbol) -> time
        self.last_exchangeinfo_time: Dict[str, float] = {}  # venue -> time
    
    async def maybe_fetch_snapshot(self, venue: str, symbol: str) -> None:
        """Fetch snapshot if interval elapsed."""
        key = (venue, symbol)
        now = time.time()
        
        # Check if enough time has passed
        if key in self.last_snapshot_time:
            elapsed = now - self.last_snapshot_time[key]
            if elapsed < SNAPSHOT_INTERVAL_SEC:
                return
        
        try:
            snapshot = await self._fetch_depth_snapshot(venue, symbol)
            if snapshot:
                await self.storage.write_record(venue, symbol, "snapshot", snapshot)
                self.last_snapshot_time[key] = now
        except Exception as e:
            logger.error(f"Error fetching snapshot for {venue}/{symbol}: {e}")
    
    async def maybe_fetch_exchangeinfo(self, venue: str) -> None:
        """Fetch exchange info if interval elapsed."""
        now = time.time()
        
        if venue in self.last_exchangeinfo_time:
            elapsed = now - self.last_exchangeinfo_time[venue]
            if elapsed < EXCHANGEINFO_INTERVAL_SEC:
                return
        
        try:
            exchangeinfo = await self._fetch_exchangeinfo(venue)
            if exchangeinfo:
                await self.storage.write_record(venue, "EXCHANGEINFO", "exchangeinfo", exchangeinfo)
                self.last_exchangeinfo_time[venue] = now
        except Exception as e:
            logger.error(f"Error fetching exchange info for {venue}: {e}")
    
    async def _fetch_depth_snapshot(self, venue: str, symbol: str) -> Optional[dict]:
        """Fetch order book snapshot from REST API."""
        try:
            if venue == "BINANCE_SPOT":
                url = f"{BINANCE_SPOT_REST}/api/v3/depth"
                params = {'symbol': symbol, 'limit': 1000}
            else:  # USDT-M Futures
                url = f"{BINANCE_FUTURES_REST}/fapi/v1/depth"
                params = {'symbol': symbol, 'limit': 1000}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        return {
                            'venue': venue,
                            'symbol': symbol,
                            'channel': 'snapshot',
                            'ts_recv_ns': time.time_ns(),
                            'ts_event_ms': data.get('E'),
                            'lastUpdateId': data.get('lastUpdateId'),
                            'bids': data.get('bids', []),
                            'asks': data.get('asks', []),
                        }
                    else:
                        logger.warning(f"Failed to fetch snapshot for {symbol}: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"Error in _fetch_depth_snapshot: {e}")
            return None
    
    async def _fetch_exchangeinfo(self, venue: str) -> Optional[dict]:
        """Fetch exchange info from REST API."""
        try:
            if venue == "BINANCE_SPOT":
                url = f"{BINANCE_SPOT_REST}/api/v3/exchangeInfo"
            else:
                url = f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        return {
                            'venue': venue,
                            'symbol': 'EXCHANGEINFO',
                            'channel': 'exchangeinfo',
                            'ts_recv_ns': time.time_ns(),
                            'ts_event_ms': data.get('serverTime'),
                            'timezone': data.get('timezone'),
                            'symbols': data.get('symbols', []),
                        }
                    else:
                        logger.warning(f"Failed to fetch exchange info for {venue}: {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"Error in _fetch_exchangeinfo: {e}")
            return None


# ============================================================================
# Disk Management
# ============================================================================

class DiskManager:
    """Manages disk usage and cleanup."""
    
    @staticmethod
    async def get_disk_usage_gb(path: str = None) -> float:
        """Get disk usage in GB."""
        if path is None:
            path = str(DATA_ROOT)
        
        import os
        import subprocess
        
        try:
            result = subprocess.run(
                ['du', '-sb', path],
                capture_output=True,
                timeout=10
            )
            if result.returncode == 0:
                bytes_used = int(result.stdout.split()[0])
                return bytes_used / (1024 ** 3)
        except Exception as e:
            logger.error(f"Error calculating disk usage: {e}")
        
        return 0.0
    
    @staticmethod
    async def cleanup_old_data(target_gb: float) -> None:
        """Delete oldest data files until usage is below target."""
        try:
            current_usage = await DiskManager.get_disk_usage_gb()
            
            if current_usage <= target_gb:
                return
            
            logger.info(f"Disk usage {current_usage:.1f}GB > target {target_gb:.1f}GB, starting cleanup...")
            
            # Find all date directories and sort by name (oldest first)
            import os
            from pathlib import Path
            
            date_dirs = []
            for venue_dir in DATA_ROOT.iterdir():
                if not venue_dir.is_dir():
                    continue
                
                for channel_dir in venue_dir.iterdir():
                    if not channel_dir.is_dir():
                        continue
                    
                    for symbol_dir in channel_dir.iterdir():
                        if not symbol_dir.is_dir():
                            continue
                        
                        for date_dir in symbol_dir.iterdir():
                            if date_dir.is_dir():
                                date_dirs.append((date_dir.name, date_dir))
            
            # Remove oldest dates
            date_dirs_sorted = sorted(date_dirs, key=lambda x: x[0])
            
            for date_str, date_dir in date_dirs_sorted:
                if current_usage <= target_gb:
                    break
                
                # Delete entire date directory
                import shutil
                try:
                    size_gb = await DiskManager.get_disk_usage_gb(str(date_dir))
                    shutil.rmtree(date_dir)
                    current_usage -= size_gb
                    logger.info(f"Deleted {date_dir} ({size_gb:.1f}GB)")
                except Exception as e:
                    logger.error(f"Error deleting {date_dir}: {e}")
            
            logger.info(f"Cleanup complete. Current usage: {current_usage:.1f}GB")
            
        except Exception as e:
            logger.error(f"Error in cleanup_old_data: {e}", exc_info=True)
    
    @staticmethod
    async def disk_check_task() -> None:
        """Background task for disk management using DiskMonitor."""
        if not disk_monitor:
            logger.error("DiskMonitor not initialized")
            return
        
        while not shutdown_event.is_set():
            try:
                # Use DiskMonitor to check disk usage
                usage_info = await disk_monitor.check_disk_usage()
                usage = usage_info.get('total_gb', 0)
                
                if usage > DISK_SOFT_LIMIT_GB:
                    logger.warning(f"Disk usage {usage:.1f}GB exceeds soft limit {DISK_SOFT_LIMIT_GB}GB")
                    await DiskManager.cleanup_old_data(DISK_CLEANUP_TARGET_GB)
                else:
                    logger.debug(f"Disk usage ok: {usage:.1f}GB / {DISK_SOFT_LIMIT_GB}GB")
                
                await asyncio.sleep(DISK_CHECK_INTERVAL_SEC)
                
            except Exception as e:
                logger.error(f"Error in disk_check_task: {e}", exc_info=True)
                await asyncio.sleep(DISK_CHECK_INTERVAL_SEC)


# ============================================================================
# cryptofeed Callbacks
# ============================================================================

async def on_l2_book(book, receipt_timestamp: float) -> None:
    """Handle L2 order book updates."""
    try:
        # Determine venue
        venue = "BINANCE_SPOT"
        if "future" in str(book.exchange).lower():
            venue = "BINANCE_USDTF"
        
        symbol = str(book.symbol)
        
        # Record message
        health_monitor.record_message(
            venue=venue,
            symbol=symbol,
            ts_event=int(book.timestamp * 1000) if book.timestamp else None,
            channel="depth"
        )
        
        # Fetch snapshot if needed
        # DISABLED: snapshot fetcher causes 429/418 bans
        # if snapshot_fetcher:
        #     await snapshot_fetcher.maybe_fetch_snapshot(venue, symbol)
        
        # Create record
        delta_data = {
            'bids': [[str(p), str(s)] for p, s in (book.delta.get(BID, []) if book.delta else [])],
            'asks': [[str(p), str(s)] for p, s in (book.delta.get(ASK, []) if book.delta else [])],
            # TODO: SortedDict does not have .items() - fix cryptofeed API usage
            # 'top_bid': str(list(book.book.bids.items())[0][0]) if book.book.bids else None,
            # 'top_ask': str(list(book.book.asks.items())[0][0]) if book.book.asks else None,
        }
        
        record = {
            'venue': venue,
            'symbol': symbol,
            'channel': 'depth',
            'ts_recv_ns': int(receipt_timestamp * 1e9),
            'ts_event_ms': int(book.timestamp * 1000) if book.timestamp else None,
            'sequence_number': book.sequence_number,
            'payload': delta_data,
        }
        
        await storage_manager.write_record(venue, symbol, "depth", record)
        
    except Exception as e:
        logger.error(f"Error in on_l2_book: {e}", exc_info=True)


async def on_trade(trade, receipt_timestamp: float) -> None:
    """Handle trade updates."""
    try:
        # Determine venue
        venue = "BINANCE_SPOT"
        if "future" in str(trade.exchange).lower():
            venue = "BINANCE_USDTF"
        
        symbol = str(trade.symbol)
        
        # Record message
        health_monitor.record_message(
            venue=venue,
            symbol=symbol,
            ts_event=int(trade.timestamp * 1000) if trade.timestamp else None,
            channel="trade"
        )
        
        # Create record
        record = {
            'venue': venue,
            'symbol': symbol,
            'channel': 'trade',
            'ts_recv_ns': int(receipt_timestamp * 1e9),
            'ts_event_ms': int(trade.timestamp * 1000) if trade.timestamp else None,
            'payload': {
                'price': str(trade.price),
                'quantity': str(trade.amount),
                'side': trade.side,
                'trade_id': str(trade.id) if hasattr(trade, 'id') else None,
            },
        }
        
        await storage_manager.write_record(venue, symbol, "trade", record)
        
    except Exception as e:
        logger.error(f"Error in on_trade: {e}", exc_info=True)


async def snapshot_and_metadata_task(symbols_by_venue: Dict[str, List[str]]) -> None:
    """Background task for REST snapshots and exchange info."""
    global snapshot_fetcher
    
    snapshot_fetcher = SnapshotFetcher(storage_manager, health_monitor)
    
    while not shutdown_event.is_set():
        try:
            for venue, symbols in symbols_by_venue.items():
                # Fetch exchange info periodically
                await snapshot_fetcher.maybe_fetch_exchangeinfo(venue)
                
                # Fetch snapshots for each symbol
                for symbol in symbols:
                    await snapshot_fetcher.maybe_fetch_snapshot(venue, symbol)
                    await asyncio.sleep(0.1)  # Rate limit
            
            await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in snapshot_and_metadata_task: {e}", exc_info=True)
            await asyncio.sleep(5)


# ============================================================================
# Main Initialization & Shutdown
# ============================================================================

async def initialize() -> Dict[str, List[str]]:
    """Initialize recorder infrastructure."""
    global storage_manager, health_monitor, disk_monitor, shutdown_event
    
    logger.info("=" * 70)
    logger.info("Binance Market Data Recorder Starting")
    logger.info("=" * 70)
    
    shutdown_event = asyncio.Event()
    storage_manager = StorageManager()
    health_monitor = HealthMonitor()
    disk_monitor = DiskMonitor(config)
    
    # Select universe
    selector = UniverseSelector()
    universe = await selector.get_or_select_universe()
    
    logger.info(f"Universe: Spot={len(universe['BINANCE_SPOT'])} symbols, "
                f"Futures={len(universe['BINANCE_USDTF'])} symbols")
    
    return universe


async def setup_feeds(universe: Dict[str, List[str]]) -> FeedHandler:
    """Setup cryptofeed feeds."""
    global feed_handler
    
    feed_handler = FeedHandler()
    
    # Filter symbols to exclude known problematic ones
    def filter_symbols(symbols: List[str]) -> List[str]:
        """Filter out symbols that aren't supported by cryptofeed."""
        # Known problematic symbols that cryptofeed doesn't support
        problematic = {'USDCUSDT', 'EURC', 'EURI', 'FDUSD'}  # Stablecoins and test symbols
        filtered = [s for s in symbols if not any(p in s for p in problematic)]
        removed = len(symbols) - len(filtered)
        if removed > 0:
            logger.info(f"Filtered out {removed} unsupported symbols")
        return filtered
    
    # Spot
    spot_symbols_filtered = filter_symbols(universe['BINANCE_SPOT'])
    spot_symbols = [Symbol(s.replace('USDT', ''), 'USDT') for s in spot_symbols_filtered]
    
    try:
        feed_handler.add_feed(
            Binance(
                symbols=spot_symbols,
                channels=[L2_BOOK, TRADES],
                callbacks={
                    L2_BOOK: on_l2_book,
                    TRADES: on_trade,
                },
                depth_interval=f"{DEPTH_INTERVAL_MS}ms"
            )
        )
        logger.info(f"Added {len(spot_symbols)} Binance Spot symbols")
    except Exception as e:
        logger.error(f"Failed to initialize Binance Spot: {e}")
        # Continue without spot feed
    
    # Futures  
    futures_symbols_filtered = filter_symbols(universe['BINANCE_USDTF'])
    # For futures, use the raw symbols (they're already complete like "BTCUSDT")
    futures_symbols = futures_symbols_filtered
    
    try:
        feed_handler.add_feed(
            BinanceFutures(
                symbols=futures_symbols,
                channels=[L2_BOOK, TRADES],
                callbacks={
                    L2_BOOK: on_l2_book,
                    TRADES: on_trade,
                },
            )
        )
        logger.info(f"Added {len(futures_symbols)} Binance Futures symbols")
    except Exception as e:
        logger.error(f"Failed to initialize Binance Futures: {e}")
        # Continue without futures feed
    
    logger.info("Feed handler configured with all symbols")
    return feed_handler


def signal_handler(sig, frame):
    """Handle shutdown signal."""
    logger.info(f"Received signal {sig}, initiating shutdown...")
    if shutdown_event:
        shutdown_event.set()


def _run_feed_handler_blocking(feed_handler, event_loop):
    """
    Run feed handler synchronously in a separate thread.
    
    CryptoFeed's FeedHandler.run() is a blocking call that manages its own
    event loop internally. We run it in a separate thread while keeping
    the async event loop running in the main thread for background tasks.
    
    The key challenge: FeedHandler.run() tries to set up signal handlers, which
    can only be done from the main thread. We monkey-patch cryptofeed to skip 
    signal handler setup when not in the main thread.
    
    Args:
        feed_handler: The initialized FeedHandler
        event_loop: The main asyncio event loop (reference for coordination)
    """
    try:
        logger.info("Feed handler thread running...")
        
        # Monkey-patch cryptofeed to skip signal handlers in non-main thread
        import threading
        from cryptofeed import feedhandler as fh
        
        original_setup = fh.setup_signal_handlers
        
        def setup_signal_handlers_safe(loop):
            """Skip signal handler setup if not in main thread."""
            if threading.current_thread() is threading.main_thread():
                original_setup(loop)
            else:
                logger.info("Skipping signal handlers in non-main thread")
        
        fh.setup_signal_handlers = setup_signal_handlers_safe
        
        # Create a new event loop for this thread (required by uvloop)
        thread_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(thread_loop)
        try:
            # Feed handler manages its own event loop through uvloop
            # It will block until shutdown_event is set or feed stops
            feed_handler.run()
        finally:
            thread_loop.close()
        logger.info("Feed handler exited normally")
    except Exception as e:
        logger.error(f"Feed handler error: {e}", exc_info=True)
    finally:
        # Signal main loop to shutdown
        if shutdown_event:
            shutdown_event.set()


async def main():
    """Main entry point."""
    global feed_handler, background_tasks
    
    try:
        # Initialize
        universe = await initialize()
        
        # Setup feeds
        feed_handler = await setup_feeds(universe)
        
        # Start background tasks
        background_tasks.append(asyncio.create_task(health_monitor.heartbeat_task()))
        background_tasks.append(asyncio.create_task(DiskManager.disk_check_task()))
        # DISABLED: snapshot_and_metadata_task causes 429/418 ban (REST spam)
        # TODO: implement proper rate-limited snapshot strategy
        # background_tasks.append(asyncio.create_task(snapshot_and_metadata_task(universe)))
        
        logger.info("All background tasks started")
        logger.info("Starting feed handler (this runs until shutdown)...")
        
        # Set up signal handlers for async loop
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, signal_handler, signal.SIGINT, None)
        loop.add_signal_handler(signal.SIGTERM, signal_handler, signal.SIGTERM, None)
        
        # Run feed handler in a separate thread to avoid blocking the event loop
        # We need to pass the event loop so it can be used by the feed handler
        feed_thread = threading.Thread(
            target=_run_feed_handler_blocking,
            args=(feed_handler, loop),
            daemon=False,
            name="FeedHandler"
        )
        feed_thread.start()
        
        # Keep the main async loop running while feed_thread processes data
        # This allows background tasks to keep running alongside the feed
        while feed_thread.is_alive():
            await asyncio.sleep(0.1)
            # Check if shutdown was triggered
            if shutdown_event.is_set():
                logger.info("Shutdown triggered, waiting for feed handler to stop...")
                break
        
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await shutdown()


async def shutdown():
    """Graceful shutdown."""
    logger.info("Shutting down recorder...")
    
    # Stop background tasks
    for task in background_tasks:
        if not task.done():
            task.cancel()
    
    # Wait for tasks
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    
    # Shutdown storage
    if storage_manager:
        await storage_manager.shutdown()
    
    # Shutdown disk monitor
    if disk_monitor:
        await disk_monitor.shutdown()
    
    # Write final heartbeat
    if health_monitor:
        await health_monitor.write_heartbeat()
        logger.info(f"Final stats: {health_monitor.get_summary()}")
    
    logger.info("Recorder shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
