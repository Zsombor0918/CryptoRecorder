"""
Health monitoring, statistics collection, and heartbeat.
"""
import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import DefaultDict, Dict
from collections import defaultdict

from config import (
    STATE_ROOT,
    HEARTBEAT_INTERVAL_SEC,
    HEALTH_CHECK_INTERVAL_SEC,
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
            'last_heartbeat': datetime.fromtimestamp(self.last_heartbeat).isoformat(),
        }


class HealthMonitor:
    """Collects statistics and monitors system health."""
    
    def __init__(self):
        self.symbol_stats: Dict[tuple, SymbolStats] = {}  # (venue, symbol) -> stats
        self.reconnect_count = 0
        self.start_time = time.time()
        self.last_heartbeat_file = STATE_ROOT / "heartbeat.json"
        self.reconnects_log = STATE_ROOT / "reconnects.log"
    
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
    
    async def write_heartbeat(self) -> None:
        """Write heartbeat with current stats."""
        try:
            uptime_sec = time.time() - self.start_time
            
            # Group stats by venue
            by_venue: DefaultDict[str, list] = defaultdict(list)
            for (venue, symbol), stats in self.symbol_stats.items():
                by_venue[venue].append(stats.to_dict())
            
            # Aggregate stats
            total_messages = sum(stats.message_count for stats in self.symbol_stats.values())
            total_gaps = sum(stats.gap_count for stats in self.symbol_stats.values())
            
            heartbeat = {
                'timestamp': datetime.utcnow().isoformat(),
                'uptime_seconds': uptime_sec,
                'total_symbols': len(self.symbol_stats),
                'total_messages': total_messages,
                'total_gaps': total_gaps,
                'total_reconnects': self.reconnect_count,
                'by_venue': dict(by_venue),
            }
            
            # Write to file
            with open(self.last_heartbeat_file, 'w') as f:
                json.dump(heartbeat, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error writing heartbeat: {e}", exc_info=True)
    
    async def heartbeat_task(self) -> None:
        """Background task that writes heartbeat periodically."""
        while True:
            try:
                await self.write_heartbeat()
                await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
            except Exception as e:
                logger.error(f"Error in heartbeat task: {e}", exc_info=True)
                await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
    
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
