"""
Health monitoring, statistics collection, and heartbeat.
"""
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, DefaultDict, Dict, List
from collections import defaultdict

from config import (
    STATE_ROOT,
    HEARTBEAT_INTERVAL_SEC,
)
from time_utils import local_now_iso, timestamp_to_local_iso

logger = logging.getLogger(__name__)


class SymbolStats:
    """Statistics for a single symbol."""
    
    def __init__(self, venue: str, symbol: str):
        self.venue = venue
        self.symbol = symbol
        self.message_count = 0
        self.last_ts_event = None
        self.last_update_id = None
        self.prev_update_id = None
        self.gap_count = 0
        self.last_heartbeat = time.time()
        self.sync_state = None
        self.snapshot_seed_count = 0
        self.resync_count = 0
        self.desync_events = 0
        # ── rich sync diagnostics ──
        self.stream_session_id: int = 0
        self.accepted_update_count: int = 0
        self.rejected_update_count: int = 0
        self.buffered_update_count: int = 0
        self.last_snapshot_seed_ts: float | None = None
        self.last_live_synced_ts: float | None = None
        self.last_desync_ts: float | None = None
        self.last_desync_reason: str | None = None
        self.last_resync_reason: str | None = None
        self.fenced_reason: str | None = None
        self.last_seen_U: int | None = None
        self.last_seen_u: int | None = None
        self.last_seen_pu: int | None = None
        self.last_rejected_U: int | None = None
        self.last_rejected_u: int | None = None
        self.last_rejected_pu: int | None = None
        self.bootstrap_stale_drop_count: int = 0
        self.promote_zero_accepted_count: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        now_mono = time.monotonic()
        return {
            'venue': self.venue,
            'symbol': self.symbol,
            'message_count': self.message_count,
            'last_ts_event': self.last_ts_event,
            'last_update_id': self.last_update_id,
            'prev_update_id': self.prev_update_id,
            'gap_count': self.gap_count,
            'sync_state': self.sync_state,
            'stream_session_id': self.stream_session_id,
            'snapshot_seed_count': self.snapshot_seed_count,
            'resync_count': self.resync_count,
            'desync_events': self.desync_events,
            'accepted_update_count': self.accepted_update_count,
            'rejected_update_count': self.rejected_update_count,
            'buffered_update_count': self.buffered_update_count,
            'last_snapshot_seed_ago_sec': (
                round(now_mono - self.last_snapshot_seed_ts, 1)
                if self.last_snapshot_seed_ts is not None else None
            ),
            'last_live_synced_ago_sec': (
                round(now_mono - self.last_live_synced_ts, 1)
                if self.last_live_synced_ts is not None else None
            ),
            'last_desync_ago_sec': (
                round(now_mono - self.last_desync_ts, 1)
                if self.last_desync_ts is not None else None
            ),
            'last_desync_reason': self.last_desync_reason,
            'last_resync_reason': self.last_resync_reason,
            'fenced_reason': self.fenced_reason,
            'last_seen_ids': (
                {'U': self.last_seen_U, 'u': self.last_seen_u, 'pu': self.last_seen_pu}
                if self.last_seen_u is not None else None
            ),
            'last_rejected_ids': (
                {'U': self.last_rejected_U, 'u': self.last_rejected_u, 'pu': self.last_rejected_pu}
                if self.last_rejected_u is not None else None
            ),
            'bootstrap_stale_drop_count': self.bootstrap_stale_drop_count,
            'promote_zero_accepted_count': self.promote_zero_accepted_count,
            'last_heartbeat': timestamp_to_local_iso(self.last_heartbeat),
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
        
        if update_id is not None:
            stats.last_update_id = update_id

    def record_phase2_symbol_state(
        self,
        *,
        venue: str,
        symbol: str,
        sync_state: str,
        last_update_id: int | None,
        prev_update_id: int | None,
        snapshot_seed_count: int = 0,
        resync_count: int = 0,
        desync_count: int | None = None,
        desync_events: int | None = None,
        # ── rich diagnostics (optional for backward compat) ──
        stream_session_id: int = 0,
        accepted_update_count: int = 0,
        rejected_update_count: int = 0,
        buffered_update_count: int = 0,
        last_snapshot_seed_ts: float | None = None,
        last_live_synced_ts: float | None = None,
        last_desync_ts: float | None = None,
        last_desync_reason: str | None = None,
        last_resync_reason: str | None = None,
        fenced_reason: str | None = None,
        last_seen_U: int | None = None,
        last_seen_u: int | None = None,
        last_seen_pu: int | None = None,
        last_rejected_U: int | None = None,
        last_rejected_u: int | None = None,
        last_rejected_pu: int | None = None,
        bootstrap_stale_drop_count: int = 0,
        promote_zero_accepted_count: int = 0,
    ) -> None:
        key = (venue, symbol)
        if key not in self.symbol_stats:
            self.symbol_stats[key] = SymbolStats(venue, symbol)
        stats = self.symbol_stats[key]
        stats.sync_state = sync_state
        stats.last_update_id = last_update_id
        stats.prev_update_id = prev_update_id
        stats.snapshot_seed_count = snapshot_seed_count
        stats.resync_count = resync_count
        stats.desync_events = (
            desync_events if desync_events is not None else desync_count or 0
        )
        # Rich diagnostics
        stats.stream_session_id = stream_session_id
        stats.accepted_update_count = accepted_update_count
        stats.rejected_update_count = rejected_update_count
        stats.buffered_update_count = buffered_update_count
        if last_snapshot_seed_ts is not None:
            stats.last_snapshot_seed_ts = last_snapshot_seed_ts
        if last_live_synced_ts is not None:
            stats.last_live_synced_ts = last_live_synced_ts
        if last_desync_ts is not None:
            stats.last_desync_ts = last_desync_ts
        if last_desync_reason is not None:
            stats.last_desync_reason = last_desync_reason
        if last_resync_reason is not None:
            stats.last_resync_reason = last_resync_reason
        stats.fenced_reason = fenced_reason
        if last_seen_U is not None:
            stats.last_seen_U = last_seen_U
            stats.last_seen_u = last_seen_u
            stats.last_seen_pu = last_seen_pu
        if last_rejected_U is not None:
            stats.last_rejected_U = last_rejected_U
            stats.last_rejected_u = last_rejected_u
            stats.last_rejected_pu = last_rejected_pu
        stats.bootstrap_stale_drop_count = bootstrap_stale_drop_count
        stats.promote_zero_accepted_count = promote_zero_accepted_count
    
    def record_gap(self, venue: str, symbol: str) -> None:
        """Record a detected gap in sequence."""
        key = (venue, symbol)
        
        if key not in self.symbol_stats:
            self.symbol_stats[key] = SymbolStats(venue, symbol)
        
        self.symbol_stats[key].gap_count += 1
    
    def record_reconnect(self, venue: str, reason: str = None) -> None:
        """Record a reconnection event."""
        self.reconnect_count += 1
        
        timestamp = local_now_iso()
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

            # ── venue-level sync-health summaries ─────────────────────
            sync_health: Dict[str, Dict[str, Any]] = {}
            for venue_key, sym_dicts in by_venue.items():
                live = sum(1 for s in sym_dicts if s.get("sync_state") == "live_synced")
                desynced = sum(1 for s in sym_dicts if s.get("sync_state") == "desynced")
                unsynced = sum(1 for s in sym_dicts if s.get("sync_state") == "unsynced")
                fenced = sum(1 for s in sym_dicts if s.get("sync_state") == "fenced")
                seeded = sum(1 for s in sym_dicts if s.get("sync_state") == "snapshot_seeded")
                resync_req = sum(1 for s in sym_dicts if s.get("sync_state") == "resync_required")
                with_desync = sum(1 for s in sym_dicts if (s.get("desync_events") or 0) > 0)
                max_resyncs = max((s.get("resync_count") or 0 for s in sym_dicts), default=0)
                total_accepted = sum(s.get("accepted_update_count", 0) for s in sym_dicts)
                total_rejected = sum(s.get("rejected_update_count", 0) for s in sym_dicts)
                total_bootstrap_stale_drops = sum(s.get("bootstrap_stale_drop_count", 0) for s in sym_dicts)
                total_promote_zero = sum(s.get("promote_zero_accepted_count", 0) for s in sym_dicts)
                sync_health[venue_key] = {
                    "live_synced_count": live,
                    "snapshot_seeded_count": seeded,
                    "desynced_count": desynced,
                    "resync_required_count": resync_req,
                    "unsynced_count": unsynced,
                    "fenced_count": fenced,
                    "symbols_with_desync_events": with_desync,
                    "max_resync_count": max_resyncs,
                    "total_accepted_updates": total_accepted,
                    "total_rejected_updates": total_rejected,
                    "total_bootstrap_stale_drops": total_bootstrap_stale_drops,
                    "total_promote_zero_accepted": total_promote_zero,
                }
            
            heartbeat = {
                'timestamp': local_now_iso(),
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
                'architecture': 'deterministic_native',
                'sync_health': sync_health,
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
