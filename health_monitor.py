"""
Health monitoring, statistics collection, and heartbeat.
"""
import asyncio
import copy
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, DefaultDict, Dict, List
from collections import defaultdict

from config import (
    STATE_ROOT,
    HEARTBEAT_INTERVAL_SEC,
    TRADE_HEALTH_HIGH_LIQUIDITY_USDTF,
    UNIVERSE_HEALTH_CHECKPOINT_INTERVAL_SEC,
    UNIVERSE_HEALTH_MIN_OBSERVATION_SEC,
    UNIVERSE_ZERO_MESSAGE_GRACE_SEC,
)
from time_utils import local_now_iso, timestamp_to_local_iso

logger = logging.getLogger(__name__)


def datetime_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class SymbolStats:
    """Statistics for a single symbol."""
    
    def __init__(self, venue: str, symbol: str):
        self.venue = venue
        self.symbol = symbol
        self.message_count = 0
        self.depth_message_count = 0
        self.trade_message_count = 0
        self.trade_committed_count = 0
        self.last_ts_event = None
        self.last_update_id = None
        self.prev_update_id = None
        self.gap_count = 0
        self.last_heartbeat = time.time()
        self.sync_state = None
        self.snapshot_seed_count = 0
        self.resync_count = 0
        self.bootstrap_attempt_count = 0
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
        self.depth_live_synced_ever: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        now_mono = time.monotonic()
        return {
            'venue': self.venue,
            'symbol': self.symbol,
            'message_count': self.message_count,
            'depth_message_count': self.depth_message_count,
            'trade_message_count': self.trade_message_count,
            'trade_committed_count': self.trade_committed_count,
            'last_ts_event': self.last_ts_event,
            'last_update_id': self.last_update_id,
            'prev_update_id': self.prev_update_id,
            'gap_count': self.gap_count,
            'sync_state': self.sync_state,
            'stream_session_id': self.stream_session_id,
            'snapshot_seed_count': self.snapshot_seed_count,
            'resync_count': self.resync_count,
            'bootstrap_attempt_count': self.bootstrap_attempt_count,
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
            'depth_live_synced_ever': self.depth_live_synced_ever,
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
        # Trade health diagnostics – set by recorder before final heartbeat
        self.trade_health: Dict[str, Any] = {}
        self.last_universe_health_checkpoint: float = 0.0
    
    def record_message(self, venue: str, symbol: str, ts_event: int = None, 
                      update_id: int = None, channel: str = None) -> None:
        """Record a message reception."""
        key = (venue, symbol)
        
        if key not in self.symbol_stats:
            self.symbol_stats[key] = SymbolStats(venue, symbol)
        
        stats = self.symbol_stats[key]
        stats.message_count += 1
        if channel == "depth_v2":
            stats.depth_message_count += 1
        elif channel == "trade_v2":
            stats.trade_message_count += 1
            stats.trade_committed_count += 1
        
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
        bootstrap_attempt_count: int = 0,
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
        if sync_state == "live_synced":
            stats.depth_live_synced_ever = True
        stats.last_update_id = last_update_id
        stats.prev_update_id = prev_update_id
        stats.snapshot_seed_count = snapshot_seed_count
        stats.resync_count = resync_count
        stats.bootstrap_attempt_count = bootstrap_attempt_count
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
    
    def set_trade_health(self, trade_diagnostics: Dict[str, Any]) -> None:
        """Store trade health diagnostics for heartbeat export."""
        self.trade_health = trade_diagnostics

    def _subscribed_symbols_from_trade_health(self, venue: str) -> set[str]:
        venue_diag = self.trade_health.get(venue, {})
        subscribed = set()
        if isinstance(venue_diag, dict):
            for symbol in venue_diag.get("subscribed_symbols", []) or []:
                if isinstance(symbol, str):
                    subscribed.add(symbol)
            shards = venue_diag.get("shards", {})
            if isinstance(shards, dict):
                for shard_diag in shards.values():
                    if not isinstance(shard_diag, dict):
                        continue
                    for symbol in shard_diag.get("subscribed_symbols", []) or []:
                        if isinstance(symbol, str):
                            subscribed.add(symbol)
        return subscribed

    def _build_trade_health_warnings(self, now: float) -> Dict[str, List[Dict[str, Any]]]:
        """Warn on impossible-looking zero-trade futures while depth is live."""
        selected = self._selected_symbols_by_venue()
        venue = "BINANCE_USDTF"
        selected_futures = set(selected.get(venue, []))
        subscribed = self._subscribed_symbols_from_trade_health(venue)
        warnings: List[Dict[str, Any]] = []
        observation_sec = max(0.0, now - self.start_time)

        if observation_sec < UNIVERSE_HEALTH_MIN_OBSERVATION_SEC:
            return {}

        for symbol in TRADE_HEALTH_HIGH_LIQUIDITY_USDTF:
            if symbol not in selected_futures or symbol not in subscribed:
                continue
            stats = self.symbol_stats.get((venue, symbol))
            if not stats:
                continue
            if stats.depth_message_count > 0 and stats.trade_message_count == 0:
                warnings.append({
                    "severity": "warning",
                    "venue": venue,
                    "symbol": symbol,
                    "reason": "high_liquidity_futures_zero_trades_with_active_depth",
                    "depth_message_count": stats.depth_message_count,
                    "trade_message_count": stats.trade_message_count,
                    "observation_sec": round(observation_sec, 3),
                    "subscribed": True,
                    "l2_failure": False,
                })

        return {venue: warnings} if warnings else {}

    def _trade_health_with_warnings(self, now: float) -> Dict[str, Any]:
        payload = copy.deepcopy(self.trade_health)
        warnings_by_venue = self._build_trade_health_warnings(now)
        for venue, warnings in warnings_by_venue.items():
            venue_diag = payload.setdefault(venue, {})
            if isinstance(venue_diag, dict):
                venue_diag["warnings"] = warnings
                venue_diag["warning_count"] = len(warnings)
        return payload

    def _selected_symbols_by_venue(self) -> Dict[str, List[str]]:
        selected: Dict[str, List[str]] = {}
        for cov_key in ("spot", "futures"):
            coverage = self.startup_coverage.get(cov_key, {})
            venue = coverage.get("venue")
            symbols = coverage.get("requested_raw", coverage.get("active_raw", []))
            if venue and isinstance(symbols, list):
                selected[venue] = [s for s in symbols if isinstance(s, str)]
        return selected

    def build_universe_health_summary(self) -> Dict[str, Any]:
        """Build a checkpoint consumed by the next universe selection."""
        now = time.time()
        date_str = datetime_date_str()
        selected = self._selected_symbols_by_venue()
        symbols_by_venue: Dict[str, Dict[str, Any]] = {}
        for venue, symbols in selected.items():
            venue_payload: Dict[str, Any] = {}
            for symbol in symbols:
                stats = self.symbol_stats.get((venue, symbol))
                depth_count = stats.depth_message_count if stats else 0
                trade_count = stats.trade_message_count if stats else 0
                snapshot_count = stats.snapshot_seed_count if stats else 0
                depth_live = stats.depth_live_synced_ever if stats else False
                trade_committed = stats.trade_committed_count if stats else 0
                observation_sec = max(0.0, now - self.start_time)
                zero_messages = depth_count == 0 and trade_count == 0
                no_data_reason = None
                suggested_action = "keep"
                if zero_messages:
                    no_data_reason = "zero_depth_and_trade_messages"
                    if observation_sec >= UNIVERSE_HEALTH_MIN_OBSERVATION_SEC:
                        suggested_action = "temporary_exclude_candidate"
                    elif observation_sec >= UNIVERSE_ZERO_MESSAGE_GRACE_SEC:
                        suggested_action = "watch"
                    else:
                        suggested_action = "watch"
                elif depth_count == 0:
                    no_data_reason = "zero_depth_messages"
                    suggested_action = "watch"
                elif trade_count == 0:
                    no_data_reason = "zero_trade_messages"
                    suggested_action = "keep"

                venue_payload[symbol] = {
                    "date": date_str,
                    "venue": venue,
                    "symbol": symbol,
                    "observation_sec": round(observation_sec, 3),
                    "depth_message_count": depth_count,
                    "trade_message_count": trade_count,
                    "depth_live_synced_ever": depth_live,
                    "trade_committed_count": trade_committed,
                    "snapshot_seed_count": snapshot_count,
                    "selected_in_universe": True,
                    "no_data_reason": no_data_reason,
                    "suggested_action": suggested_action,
                }
            symbols_by_venue[venue] = venue_payload

        return {
            "date": date_str,
            "timestamp": local_now_iso(),
            "uptime_seconds": round(now - self.start_time, 3),
            "symbols": symbols_by_venue,
        }

    def write_universe_health_checkpoint(self, *, force: bool = False) -> None:
        now = time.time()
        if (
            not force
            and self.last_universe_health_checkpoint
            and now - self.last_universe_health_checkpoint < UNIVERSE_HEALTH_CHECKPOINT_INTERVAL_SEC
        ):
            return
        summary = self.build_universe_health_summary()
        out = STATE_ROOT / "universe_health" / f"{summary['date']}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(summary, indent=2))
        self.last_universe_health_checkpoint = now

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
                total_bootstrap_attempts = sum(s.get("bootstrap_attempt_count", 0) for s in sym_dicts)
                total_continuity_resyncs = sum(s.get("resync_count", 0) for s in sym_dicts)
                total_snapshots = sum(s.get("snapshot_seed_count", 0) for s in sym_dicts)
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
                    "bootstrap_accounting": {
                        "total_bootstrap_attempts": total_bootstrap_attempts,
                        "total_continuity_resyncs": total_continuity_resyncs,
                        "total_snapshots_committed": total_snapshots,
                        "total_zero_accepted_promotes": total_promote_zero,
                        "total_stale_drops_in_bootstrap": total_bootstrap_stale_drops,
                    },
                }
            
            trade_health = self._trade_health_with_warnings(now)

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
                'trade_health': trade_health,
                'by_venue': dict(by_venue),
            }
            
            # Write to file
            with open(self.last_heartbeat_file, 'w') as f:
                json.dump(heartbeat, f, indent=2)
            self.write_universe_health_checkpoint()
            
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
