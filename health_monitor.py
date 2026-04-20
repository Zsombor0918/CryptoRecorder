"""
Health monitoring, statistics collection, and heartbeat.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, DefaultDict, Dict, List
from collections import defaultdict

from config import (
    STATE_ROOT,
    HEARTBEAT_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


class SymbolStats:
    """Statistics for a single symbol."""
    
    def __init__(self, venue: str, symbol: str):
        self.venue = venue
        self.symbol = symbol
        self.message_count = 0
        self.last_ts_event = None
        self.last_update_id = None
        self.gap_count = 0
        self.last_heartbeat = time.time()
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'venue': self.venue,
            'symbol': self.symbol,
            'message_count': self.message_count,
            'last_ts_event': self.last_ts_event,
            'last_update_id': self.last_update_id,
            'gap_count': self.gap_count,
            'last_heartbeat': datetime.utcfromtimestamp(self.last_heartbeat).isoformat(),
        }


class HealthMonitor:
    """Collects statistics and monitors system health."""
    
    def __init__(self):
        self.symbol_stats: Dict[tuple, SymbolStats] = {}  # (venue, symbol) -> stats
        self.reconnect_count = 0
        self.start_time = time.time()
        self.last_heartbeat_file = STATE_ROOT / "heartbeat.json"
        self.reconnects_log = STATE_ROOT / "reconnects.log"
        # Futures status – set by recorder before final heartbeat
        self.futures_enabled: bool = True
        self.futures_disabled_reason: str = ""
        # Snapshot mode
        self.snapshot_mode: str = "disabled"
        # Queue drop stats – updated from StorageManager before heartbeat writes
        self.queue_drop_total: int = 0
        self.queue_drop_by_writer: Dict[str, int] = {}
        # Startup coverage / symbol-loss visibility
        self.spot_symbols_requested: int = 0
        self.futures_symbols_requested: int = 0
        self.spot_symbols_dropped_list: List[str] = []
        self.futures_symbols_dropped_list: List[str] = []
        self.startup_coverage: Dict[str, Any] = {}
    
    def record_message(self, venue: str, symbol: str, ts_event: int = None, 
                      update_id: int = None, channel: str = None) -> None:
        """Record a message reception."""
        key = (venue, symbol)
        
        if key not in self.symbol_stats:
            self.symbol_stats[key] = SymbolStats(venue, symbol)
        
        stats = self.symbol_stats[key]
        stats.message_count += 1
        
        if ts_event:
            stats.last_ts_event = ts_event
        
        if update_id:
            stats.last_update_id = update_id
    
    def record_gap(self, venue: str, symbol: str) -> None:
        """Record a detected gap in sequence."""
        key = (venue, symbol)
        
        if key not in self.symbol_stats:
            self.symbol_stats[key] = SymbolStats(venue, symbol)
        
        self.symbol_stats[key].gap_count += 1
    
    def record_reconnect(self, venue: str, reason: str = None) -> None:
        """Record a reconnection event."""
        self.reconnect_count += 1
        
        timestamp = datetime.utcnow().isoformat()
        message = f"[{timestamp}] Reconnect #{self.reconnect_count} - {venue}"
        if reason:
            message += f": {reason}"
        
        logger.warning(message)
        
        # Append to reconnects log
        try:
            with open(self.reconnects_log, 'a') as f:
                f.write(message + '\n')
        except Exception as e:
            logger.error(f"Error writing to reconnects log: {e}")

    def set_startup_coverage(self, coverage: Dict[str, Any]) -> None:
        """Store startup coverage summary for heartbeat export."""
        self.startup_coverage = coverage
        spot = coverage.get("spot", {})
        futures = coverage.get("futures", {})
        self.spot_symbols_requested = int(spot.get("requested_count", 0) or 0)
        self.futures_symbols_requested = int(futures.get("requested_count", 0) or 0)
        self.spot_symbols_dropped_list = list(
            spot.get("runtime_dropped_raw", spot.get("dropped_all_raw", []))
        )
        self.futures_symbols_dropped_list = list(
            futures.get("runtime_dropped_raw", futures.get("dropped_all_raw", []))
        )
    
    async def write_heartbeat(self) -> None:
        """Write heartbeat with current stats."""
        try:
            now = time.time()
            uptime_sec = now - self.start_time
            
            # Group stats by venue
            by_venue: DefaultDict[str, list] = defaultdict(list)
            for (venue, symbol), stats in self.symbol_stats.items():
                stats.last_heartbeat = now
                by_venue[venue].append(stats.to_dict())
            
            # Aggregate stats
            total_messages = sum(stats.message_count for stats in self.symbol_stats.values())
            total_gaps = sum(stats.gap_count for stats in self.symbol_stats.values())
            spot_symbols_active = len(by_venue.get("BINANCE_SPOT", []))
            futures_symbols_active = len(by_venue.get("BINANCE_USDTF", []))
            spot_coverage_ratio = (
                round(spot_symbols_active / self.spot_symbols_requested, 4)
                if self.spot_symbols_requested else None
            )
            futures_coverage_ratio = (
                round(futures_symbols_active / self.futures_symbols_requested, 4)
                if self.futures_symbols_requested else None
            )
            
            heartbeat = {
                'timestamp': datetime.utcnow().isoformat(),
                'uptime_seconds': uptime_sec,
                'total_symbols': len(self.symbol_stats),
                'spot_symbols_active': spot_symbols_active,
                'futures_symbols_active': futures_symbols_active,
                'spot_symbols_requested': self.spot_symbols_requested,
                'futures_symbols_requested': self.futures_symbols_requested,
                'spot_symbols_dropped': len(self.spot_symbols_dropped_list),
                'futures_symbols_dropped': len(self.futures_symbols_dropped_list),
                'spot_symbols_dropped_list': self.spot_symbols_dropped_list,
                'futures_symbols_dropped_list': self.futures_symbols_dropped_list,
                'spot_coverage_ratio': spot_coverage_ratio,
                'futures_coverage_ratio': futures_coverage_ratio,
                'total_messages': total_messages,
                'total_gaps': total_gaps,
                'total_reconnects': self.reconnect_count,
                'queue_drop_total': self.queue_drop_total,
                'queue_drop_by_writer': self.queue_drop_by_writer,
                'futures_enabled': self.futures_enabled,
                'futures_disabled_reason': self.futures_disabled_reason,
                'snapshot_mode': self.snapshot_mode,
                'by_venue': dict(by_venue),
            }
            
            # Write to file
            with open(self.last_heartbeat_file, 'w') as f:
                json.dump(heartbeat, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error writing heartbeat: {e}", exc_info=True)
    
    async def heartbeat_task(self) -> None:
        """Background task that writes heartbeat periodically."""
        try:
            while True:
                try:
                    await self.write_heartbeat()
                    await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in heartbeat task: {e}", exc_info=True)
                    await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        except asyncio.CancelledError:
            pass  # clean exit on task cancellation
    
    def get_summary(self) -> dict:
        """Get a quick summary of current state."""
        total_messages = sum(stats.message_count for stats in self.symbol_stats.values())
        total_gaps = sum(stats.gap_count for stats in self.symbol_stats.values())
        
        return {
            'symbols_active': len(self.symbol_stats),
            'total_messages': total_messages,
            'total_gaps': total_gaps,
            'total_reconnects': self.reconnect_count,
            'uptime_seconds': time.time() - self.start_time,
        }
