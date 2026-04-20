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
from typing import Any, Dict, List, Optional, Tuple

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
from binance_universe import UniverseSelector, partition_known_unsupported_symbols
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

SPOT_COVERAGE_WARN_RATIO: float = 0.90
FUTURES_COVERAGE_WARN_RATIO: float = 0.80


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


def _find_suspicious_symbols(venue: str, raw_symbols: List[str], quote: str = "USDT") -> List[str]:
    """Warn about unusual symbol shapes without blocking them."""
    warnings: List[str] = []
    for symbol in raw_symbols:
        reasons: List[str] = []
        has_non_ascii = not symbol.isascii()
        if has_non_ascii:
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
        "selected_raw": list(requested_raw),
        "selected_count": len(requested_raw),
        "filtered_raw": [],
        "filtered_cf": [],
        "filtered_count": 0,
        "dropped_raw": [],
        "dropped_cf": [],
        "dropped_count": 0,
        "runtime_dropped_raw": [],
        "runtime_dropped_cf": [],
        "runtime_dropped_count": 0,
        "active_raw": [],
        "active_cf": [],
        "active_count": 0,
        "coverage_ratio": 0.0,
        "warnings": list(suspicious_warnings),
    }


def _emit_symbol_audit_logs(label: str, requested_raw: List[str], warnings: List[str]) -> None:
    """Log requested symbols and suspicious selections before feed init."""
    logger.info(f"{label} requested: {len(requested_raw)}")
    logger.debug(f"{label} requested symbols (raw): {', '.join(requested_raw)}")
    for warning in warnings:
        logger.warning(warning)


def _finalize_venue_coverage(
    label: str,
    coverage: Dict[str, Any],
    *,
    warn_ratio: float,
) -> None:
    """Derive counts and emit startup coverage summary logs."""
    coverage["selected_raw"] = list(coverage.get("requested_raw", []))
    coverage["selected_count"] = int(coverage.get("requested_count", 0) or 0)
    coverage["filtered_count"] = len(coverage["filtered_raw"])
    coverage["dropped_count"] = len(coverage["dropped_raw"])
    coverage["active_count"] = len(coverage["active_raw"])
    requested = coverage["requested_count"]
    coverage["coverage_ratio"] = (
        round(coverage["active_count"] / requested, 4) if requested else 0.0
    )
    coverage["dropped_all_raw"] = coverage["filtered_raw"] + coverage["dropped_raw"]
    coverage["dropped_all_cf"] = coverage["filtered_cf"] + coverage["dropped_cf"]
    coverage["runtime_dropped_raw"] = list(coverage["dropped_all_raw"])
    coverage["runtime_dropped_cf"] = list(coverage["dropped_all_cf"])
    coverage["runtime_dropped_count"] = len(coverage["runtime_dropped_raw"])

    logger.info(f"{label} requested: {requested}")
    logger.info(f"{label} startup known-unsupported: {coverage['filtered_count']}")
    if coverage["filtered_raw"]:
        logger.info(
            f"Filtered {label.lower()} symbols (raw): "
            + ", ".join(coverage["filtered_raw"])
        )
        logger.debug(
            f"Filtered {label.lower()} symbols (cryptofeed): "
            + ", ".join(coverage["filtered_cf"])
        )

    logger.info(f"{label} runtime dropped during init: {coverage['dropped_count']}")

    if coverage["dropped_raw"]:
        logger.info(
            f"Dropped {label.lower()} symbols (raw): "
            + ", ".join(coverage["dropped_raw"])
        )
        logger.debug(
            f"Dropped {label.lower()} symbols (cryptofeed): "
            + ", ".join(coverage["dropped_cf"])
        )

    logger.info(f"{label} active: {coverage['active_count']}")
    if label == "Futures":
        logger.info(
            f"Futures runtime_dropped after setup: {coverage['runtime_dropped_count']}"
        )
    logger.debug(
        f"Active {label.lower()} symbols (raw): "
        + ", ".join(coverage["active_raw"])
    )
    logger.debug(
        f"Active {label.lower()} symbols (cryptofeed): "
        + ", ".join(coverage["active_cf"])
    )

    if requested and coverage["coverage_ratio"] < warn_ratio:
        logger.warning(
            f"{label} coverage low: {coverage['active_count']}/{requested} "
            f"active ({coverage['coverage_ratio']:.0%})"
        )
        if coverage["dropped_all_raw"]:
            logger.warning(
                f"{label} startup losses: " + ", ".join(coverage["dropped_all_raw"])
            )


def _persist_startup_coverage(coverage: Dict[str, Any]) -> None:
    """Write startup coverage summary for later audit."""
    out = config.STATE_ROOT / "startup_coverage.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(coverage, indent=2, ensure_ascii=False))
    logger.info(f"Startup coverage report → {out}")


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

async def initialize() -> Dict[str, Any]:
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


def _setup_feeds(universe: Dict[str, Any]) -> Tuple[FeedHandler, Dict[str, Any]]:
    """Build FeedHandler with Spot + (optional) Futures feeds."""
    global futures_enabled, futures_disabled_reason

    import re as _re

    futures_enabled = True
    futures_disabled_reason = ""
    fh = FeedHandler()
    coverage: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat(),
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
    selection_metadata = universe.get("selection_metadata", {}) if isinstance(universe, dict) else {}
    spot_selection = selection_metadata.get("BINANCE_SPOT", {}) if isinstance(selection_metadata, dict) else {}
    fut_selection = selection_metadata.get("BINANCE_USDTF", {}) if isinstance(selection_metadata, dict) else {}
    coverage["spot_candidate_pool"] = spot_selection.get("candidate_pool_count", 0)
    coverage["futures_candidate_pool"] = fut_selection.get("candidate_pool_count", 0)
    spot_pre_filter_rejected_count = spot_selection.get(
        "pre_filter_rejected_count",
        spot_selection.get("rejected_pre_filter_count", 0),
    )
    futures_pre_filter_rejected_count = fut_selection.get(
        "pre_filter_rejected_count",
        fut_selection.get("rejected_pre_filter_count", 0),
    )
    spot_pre_filter_rejected_sample = spot_selection.get(
        "pre_filter_rejected_sample",
        spot_selection.get("rejected_pre_filter_sample", []),
    )
    futures_pre_filter_rejected_sample = fut_selection.get(
        "pre_filter_rejected_sample",
        fut_selection.get("rejected_pre_filter_sample", []),
    )
    coverage["spot_pre_filter_rejected_count"] = spot_pre_filter_rejected_count
    coverage["futures_pre_filter_rejected_count"] = futures_pre_filter_rejected_count
    coverage["spot_pre_filter_rejected_sample"] = spot_pre_filter_rejected_sample
    coverage["futures_pre_filter_rejected_sample"] = futures_pre_filter_rejected_sample
    coverage["spot_rejected_pre_filter_count"] = spot_pre_filter_rejected_count
    coverage["futures_rejected_pre_filter_count"] = futures_pre_filter_rejected_count
    coverage["spot_rejected_pre_filter_sample"] = spot_pre_filter_rejected_sample
    coverage["futures_rejected_pre_filter_sample"] = futures_pre_filter_rejected_sample
    coverage["futures_candidate_pool_raw_count"] = fut_selection.get("candidate_pool_raw_count", 0)
    coverage["futures_candidate_pool_after_sanity_count"] = fut_selection.get("candidate_pool_after_sanity_count", 0)
    coverage["futures_candidate_pool_after_support_check_count"] = fut_selection.get("candidate_pool_after_support_check_count", 0)
    coverage["futures_support_precheck_rejected_count"] = fut_selection.get("support_precheck_rejected_count", 0)
    coverage["futures_support_precheck_rejected_sample"] = fut_selection.get("support_precheck_rejected_sample", [])
    coverage["futures_support_precheck_available"] = fut_selection.get("support_precheck_available", False)
    coverage["futures_support_precheck_error"] = fut_selection.get("support_precheck_error")
    coverage["spot"]["candidate_pool"] = coverage["spot_candidate_pool"]
    coverage["futures"]["candidate_pool"] = coverage["futures_candidate_pool"]
    coverage["spot"]["pre_filter_rejected_count"] = coverage["spot_pre_filter_rejected_count"]
    coverage["futures"]["pre_filter_rejected_count"] = coverage["futures_pre_filter_rejected_count"]
    coverage["spot"]["pre_filter_rejected_sample"] = coverage["spot_pre_filter_rejected_sample"]
    coverage["futures"]["pre_filter_rejected_sample"] = coverage["futures_pre_filter_rejected_sample"]
    coverage["spot"]["rejected_pre_filter_count"] = coverage["spot_rejected_pre_filter_count"]
    coverage["futures"]["rejected_pre_filter_count"] = coverage["futures_rejected_pre_filter_count"]
    coverage["spot"]["rejected_pre_filter_sample"] = coverage["spot_rejected_pre_filter_sample"]
    coverage["futures"]["rejected_pre_filter_sample"] = coverage["futures_rejected_pre_filter_sample"]
    coverage["futures"]["candidate_pool_raw_count"] = coverage["futures_candidate_pool_raw_count"]
    coverage["futures"]["candidate_pool_after_sanity_count"] = coverage["futures_candidate_pool_after_sanity_count"]
    coverage["futures"]["candidate_pool_after_support_check_count"] = coverage["futures_candidate_pool_after_support_check_count"]
    coverage["futures"]["support_precheck_rejected_count"] = coverage["futures_support_precheck_rejected_count"]
    coverage["futures"]["support_precheck_rejected_sample"] = coverage["futures_support_precheck_rejected_sample"]
    coverage["futures"]["support_precheck_available"] = coverage["futures_support_precheck_available"]
    coverage["futures"]["support_precheck_error"] = coverage["futures_support_precheck_error"]

    # ── Spot ──
    spot_cov = coverage["spot"]
    spot_raw, spot_filtered = partition_known_unsupported_symbols(universe.get('BINANCE_SPOT', []))
    spot_cov["filtered_raw"] = list(spot_filtered)
    spot_cov["filtered_cf"] = _to_cf_symbols(spot_filtered)
    spot_cf = _to_cf_symbols(spot_raw)
    if spot_cf:
        remaining = list(spot_cf)
        max_removals = len(remaining)
        for _attempt in range(max_removals):
            if not remaining:
                logger.warning("Spot DISABLED – no valid symbols remained")
                break
            try:
                fh.add_feed(Binance(
                    symbols=remaining,
                    channels=[L2_BOOK, TRADES],
                    callbacks={L2_BOOK: on_l2_book, TRADES: on_trade},
                    depth_interval=f"{DEPTH_INTERVAL_MS}ms",
                ))
                spot_cov["active_cf"] = list(remaining)
                spot_cov["active_raw"] = [_from_cf_symbol(sym) for sym in remaining]
                break
            except Exception as e:
                m = _re.match(r'(.+) is not supported on', str(e))
                if m:
                    bad = m.group(1)
                    remaining = [s for s in remaining if s != bad]
                    bad_raw = _from_cf_symbol(bad)
                    if bad not in spot_cov["dropped_cf"]:
                        spot_cov["dropped_cf"].append(bad)
                    if bad_raw not in spot_cov["dropped_raw"]:
                        spot_cov["dropped_raw"].append(bad_raw)
                    logger.info(
                        f"Removed unsupported spot symbol: raw={bad_raw} cf={bad}"
                    )
                else:
                    logger.error(f"Failed to initialise Binance Spot: {e}")
                    break
        else:
            logger.warning("Spot DISABLED – no valid symbols remained")
    _finalize_venue_coverage(
        "Spot",
        spot_cov,
        warn_ratio=SPOT_COVERAGE_WARN_RATIO,
    )

    # ── Futures (retry loop: strip symbols unknown to cryptofeed) ──
    fut_cov = coverage["futures"]
    fut_raw, fut_filtered = partition_known_unsupported_symbols(universe.get('BINANCE_USDTF', []))
    fut_cov["filtered_raw"] = list(fut_filtered)
    fut_cov["filtered_cf"] = _to_cf_futures_symbols(fut_filtered)
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
                fut_cov["active_cf"] = list(remaining)
                fut_cov["active_raw"] = [_from_cf_symbol(sym) for sym in remaining]
                break
            except Exception as e:
                m = _re.match(r'(.+) is not supported on', str(e))
                if m:
                    bad = m.group(1)
                    remaining = [s for s in remaining if s != bad]
                    bad_raw = _from_cf_symbol(bad)
                    if bad not in fut_cov["dropped_cf"]:
                        fut_cov["dropped_cf"].append(bad)
                    if bad_raw not in fut_cov["dropped_raw"]:
                        fut_cov["dropped_raw"].append(bad_raw)
                    logger.info(
                        f"Removed unsupported futures symbol: raw={bad_raw} cf={bad}"
                    )
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
    _finalize_venue_coverage(
        "Futures",
        fut_cov,
        warn_ratio=FUTURES_COVERAGE_WARN_RATIO,
    )

    logger.info(
        "Startup coverage summary | "
        f"Spot selected={spot_cov['selected_count']} "
        f"pre_filter_rejected={coverage['spot_pre_filter_rejected_count']} "
        f"runtime_dropped={spot_cov['runtime_dropped_count']} active={spot_cov['active_count']} | "
        f"Futures selected={fut_cov['selected_count']} "
        f"pre_filter_rejected={coverage['futures_pre_filter_rejected_count']} "
        f"runtime_dropped={fut_cov['runtime_dropped_count']} active={fut_cov['active_count']}"
    )

    return fh, coverage


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
        fh, startup_coverage = _setup_feeds(universe)
        feed_handler = fh

        health_monitor.futures_enabled = futures_enabled
        health_monitor.futures_disabled_reason = futures_disabled_reason
        health_monitor.set_startup_coverage(startup_coverage)
        _persist_startup_coverage(startup_coverage)

        if not futures_enabled:
            logger.warning(f"FUTURES DISABLED: {futures_disabled_reason}")

        # Background tasks  – all on the same loop
        background_tasks.append(
            asyncio.create_task(health_monitor.heartbeat_task()))
        background_tasks.append(
            asyncio.create_task(_update_drop_stats_task()))
        background_tasks.append(
            asyncio.create_task(disk_check_task()))

        active_venues: List[str] = []
        if startup_coverage["spot"]["active_count"] > 0:
            active_venues.append("BINANCE_SPOT")
        if futures_enabled and startup_coverage["futures"]["active_count"] > 0:
            active_venues.append("BINANCE_USDTF")
        logger.info(f"Metadata active venues: {active_venues}")
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
