"""
Select top 50 trading symbols from Binance by 24h quote volume.
"""
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import aiohttp

from config import (
    META_ROOT,
    BINANCE_SPOT_REST,
    BINANCE_FUTURES_REST,
    DATA_ROOT,
    TOP_SYMBOLS,
    TOP_SYMBOL_CANDIDATES,
    FUTURES_TOP_SYMBOL_CANDIDATES,
    QUOTE_ASSET_SPOT,
    QUOTE_ASSET_FUTURES,
    API_REQUEST_TIMEOUT_SEC,
    STATE_ROOT,
    UNIVERSE_HEALTH_ENABLED,
    UNIVERSE_HEALTH_EXCLUDE_DAYS,
    UNIVERSE_HEALTH_MIN_OBSERVATION_SEC,
    UNIVERSE_FILTER_VERSION,
    UNIVERSE_REJECT_SAMPLE_SIZE,
    UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS,
)
from time_utils import local_now_iso

logger = logging.getLogger(__name__)

_STANDARD_USDT_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")


def _venue_key(venue_type: str) -> str:
    if venue_type == "spot":
        return "BINANCE_SPOT"
    if venue_type == "futures":
        return "BINANCE_USDTF"
    raise ValueError(f"Unknown venue type: {venue_type}")


def _quote_asset(venue_type: str) -> str:
    return QUOTE_ASSET_SPOT if venue_type == "spot" else QUOTE_ASSET_FUTURES


def _candidate_pool_size(venue_type: str) -> int:
    return FUTURES_TOP_SYMBOL_CANDIDATES if venue_type == "futures" else TOP_SYMBOL_CANDIDATES


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _raw_to_cf_futures_symbol(symbol: str, quote: str = "USDT") -> str:
    if isinstance(symbol, str) and symbol.endswith(quote) and len(symbol) > len(quote):
        return f"{symbol[:-len(quote)]}-{quote}-PERP"
    return symbol


def partition_known_unsupported_symbols(symbols: List[str]) -> Tuple[List[str], List[str]]:
    """Split symbols into kept vs known-unsupported subsets.

    With the deterministic-native architecture there are no cryptofeed-specific
    friction symbols, so this always keeps everything.
    """
    return list(symbols), []


def known_unsupported_reason(symbol: str) -> str | None:
    """Return reason if the symbol matches a known unsupported pattern.

    With native Binance WebSockets there are no known friction symbols.
    """
    return None


def is_reasonable_symbol(symbol: str, venue_type: str) -> Tuple[bool, str]:
    """Conservative sanity filter for standard, recordable Binance tickers."""
    quote = _quote_asset(venue_type)

    if not isinstance(symbol, str) or not symbol:
        return False, "symbol missing or not a string"
    if not symbol.endswith(quote):
        return False, f"must end with {quote}"
    if not symbol.isascii():
        return False, "contains non-ASCII characters"
    if not _STANDARD_USDT_SYMBOL_RE.fullmatch(symbol):
        return False, "fails conservative regex ^[A-Z0-9]+USDT$"

    base = symbol[:-len(quote)]
    if not base:
        return False, "base asset empty"
    if len(base) < 2:
        return False, f"base asset shorter than 2 chars ({base!r})"
    if base.isdigit():
        return False, "base asset is digits only"

    return True, "ok"


class UniverseSelector:
    """Selects and manages trading universe."""
    
    def __init__(self):
        self.universe_path = META_ROOT / "universe"
        self.universe_path.mkdir(parents=True, exist_ok=True)
        self._futures_support_mapping_cache: set[str] | None = None
        self._spot_support_mapping_cache: set[str] | None = None

    def _empty_selection_metadata(
        self,
        venue_type: str,
        *,
        support_precheck_available: bool | None = None,
        support_precheck_error: str | None = None,
    ) -> Dict[str, Any]:
        """Return a zeroed selection-metadata payload for failures/fallbacks."""
        metadata: Dict[str, Any] = {
            "venue": _venue_key(venue_type),
            "configured_candidate_pool_size": _candidate_pool_size(venue_type),
            "requested_count": TOP_SYMBOLS,
            "eligible_count": 0,
            "survivor_count": 0,  # legacy alias
            "selected_count": 0,
            "selected_sample": [],
            "pre_filter_rejected_count": 0,
            "pre_filter_rejected_sample": [],
            "rejected_pre_filter_count": 0,  # legacy alias
            "rejected_pre_filter_sample": [],  # legacy alias
            "candidate_pool_count": 0,
            "candidate_pool_raw_count": 0,
            "candidate_pool_after_sanity_count": 0,
            "candidate_pool_after_support_check_count": 0,
            "candidate_pool_after_health_check_count": 0,
            "health_excluded_count": 0,
            "health_excluded_sample": [],
            "replacement_symbols": [],
            "final_selected": [],
        }

        if venue_type in {"spot", "futures"}:
            metadata.update({
                "support_precheck_available": bool(support_precheck_available),
                "support_precheck_error": support_precheck_error,
                "support_precheck_rejected_count": 0,
                "support_precheck_rejected_sample": [],
            })

        return metadata
    
    async def get_or_select_universe(self, force_refresh: bool = False) -> dict:
        """
        Get today's universe or select if not already cached.
        Returns {venue: [list of symbols]}
        """
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cache_file = self.universe_path / "BINANCE_SPOT" / f"{today_str}.json"
        
        # Check cache
        if cache_file.exists() and not force_refresh:
            try:
                logger.info(f"Loading universe from cache: {cache_file}")
                with open(cache_file, encoding="utf-8") as f:
                    cached = json.load(f)
                if self._cache_is_usable(cached):
                    self._log_cached_selection(cached)
                    return cached
                logger.info(
                    "Universe cache stale or missing metadata; refreshing "
                    f"(filter_version={cached.get('filter_version')!r}, top_count={cached.get('top_count')!r})"
                )
            except Exception as e:
                logger.warning(f"Failed to read universe cache {cache_file}: {e}; refreshing")
        
        logger.info(f"Selecting top {TOP_SYMBOLS} symbols for each venue...")
        
        # Select symbols
        spot_symbols, spot_meta = await self._select_top_symbols("spot")
        futures_symbols, futures_meta = await self._select_top_symbols("futures")
        
        universe = {
            "BINANCE_SPOT": spot_symbols,
            "BINANCE_USDTF": futures_symbols,
            "timestamp": local_now_iso(),
            "top_count": TOP_SYMBOLS,
            "candidate_pool_size": TOP_SYMBOL_CANDIDATES,
            "candidate_pool_sizes": {
                "BINANCE_SPOT": TOP_SYMBOL_CANDIDATES,
                "BINANCE_USDTF": FUTURES_TOP_SYMBOL_CANDIDATES,
            },
            "filter_version": UNIVERSE_FILTER_VERSION,
            "requested_counts": {
                "BINANCE_SPOT": TOP_SYMBOLS,
                "BINANCE_USDTF": TOP_SYMBOLS,
            },
            "selected_counts": {
                "BINANCE_SPOT": len(spot_symbols),
                "BINANCE_USDTF": len(futures_symbols),
            },
            "selection_metadata": {
                "BINANCE_SPOT": spot_meta,
                "BINANCE_USDTF": futures_meta,
            },
        }
        
        # Save cache
        (self.universe_path / "BINANCE_SPOT").mkdir(parents=True, exist_ok=True)
        (self.universe_path / "BINANCE_USDTF").mkdir(parents=True, exist_ok=True)
        
        spot_cache = self.universe_path / "BINANCE_SPOT" / f"{today_str}.json"
        futures_cache = self.universe_path / "BINANCE_USDTF" / f"{today_str}.json"
        
        with open(spot_cache, 'w', encoding="utf-8") as f:
            json.dump(universe, f, indent=2)
        
        with open(futures_cache, 'w', encoding="utf-8") as f:
            json.dump(universe, f, indent=2)
        
        logger.info(f"Saved universe to cache: {spot_cache}, {futures_cache}")
        return universe
    
    def _cache_is_usable(self, cached: Any) -> bool:
        """Require cache metadata to match the current selection policy."""
        if not isinstance(cached, dict):
            return False
        if cached.get("filter_version") != UNIVERSE_FILTER_VERSION:
            return False
        if cached.get("top_count") != TOP_SYMBOLS:
            return False

        selection_metadata = cached.get("selection_metadata")
        if not isinstance(selection_metadata, dict):
            return False

        for venue_type in ("spot", "futures"):
            venue_key = _venue_key(venue_type)
            symbols = cached.get(venue_key)
            meta = selection_metadata.get(venue_key)
            if not isinstance(symbols, list):
                return False
            if not isinstance(meta, dict):
                return False
            if meta.get("configured_candidate_pool_size") != _candidate_pool_size(venue_type):
                return False
            if venue_type in {"spot", "futures"} and meta.get("support_precheck_available") is not True:
                return False
            if meta.get("selected_count") != len(symbols):
                return False
        return True

    def _log_cached_selection(self, cached: dict) -> None:
        selection_metadata = cached.get("selection_metadata", {})
        for venue_type in ("spot", "futures"):
            venue = _venue_key(venue_type)
            meta = selection_metadata.get(venue, {})
            rejected_count = meta.get(
                "pre_filter_rejected_count",
                meta.get("rejected_pre_filter_count", "?"),
            )
            logger.info(
                f"Cached {venue_type} universe: selected={meta.get('selected_count', len(cached.get(venue, [])))} "
                f"from pool={meta.get('candidate_pool_count', '?')} "
                f"pre_filter_rejected={rejected_count} "
                f"support_rejected={meta.get('support_precheck_rejected_count', '?')} "
                f"filter_version={cached.get('filter_version')}"
            )

    async def _select_top_symbols(self, venue_type: str) -> Tuple[List[str], Dict[str, Any]]:
        """Select top N symbols by 24h quote volume."""
        logger.info(
            f"Fetching top {TOP_SYMBOLS} {venue_type} symbols "
            f"from a candidate pool of {_candidate_pool_size(venue_type)}..."
        )
        
        if venue_type == "spot":
            return await self._select_spot_symbols()
        elif venue_type == "futures":
            return await self._select_futures_symbols()
        else:
            raise ValueError(f"Unknown venue type: {venue_type}")
    
    def _rank_candidates(self, ticker_data: List[dict], venue_type: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Return quote-matched candidates and the capped candidate pool."""
        quote = _quote_asset(venue_type)
        quote_matched: List[Dict[str, Any]] = []
        for item in ticker_data:
            symbol = item.get("symbol")
            if not isinstance(symbol, str):
                continue
            if not symbol.endswith(quote):
                continue
            quote_matched.append({
                "symbol": symbol,
                "quoteVolume": _safe_float(item.get("quoteVolume", 0)),
            })

        quote_matched.sort(key=lambda x: x["quoteVolume"], reverse=True)
        candidate_pool = quote_matched[:_candidate_pool_size(venue_type)]
        return quote_matched, candidate_pool

    def _apply_sanity_filter(
        self,
        candidate_pool: List[Dict[str, Any]],
        venue_type: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Apply conservative shape and known-friction filters."""
        kept: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        for item in candidate_pool:
            symbol = item["symbol"]
            reason = known_unsupported_reason(symbol)
            if reason is None:
                ok, reason = is_reasonable_symbol(symbol, venue_type)
                if ok:
                    kept.append(item)
                else:
                    rejected.append({
                        "symbol": symbol,
                        "reason": reason,
                        "quote_volume": item["quoteVolume"],
                    })
            else:
                rejected.append({
                    "symbol": symbol,
                    "reason": reason,
                    "quote_volume": item["quoteVolume"],
                })

        return kept, rejected

    def _get_futures_exchange_info_symbols(self) -> Tuple[set[str] | None, str | None]:
        """Load the set of TRADING futures symbols from cached exchangeInfo on disk,
        or return None if unavailable.
        """
        if self._futures_support_mapping_cache is not None:
            return self._futures_support_mapping_cache, None

        info_dir = DATA_ROOT / "BINANCE_USDTF" / "exchangeinfo" / "EXCHANGEINFO"
        if not info_dir.is_dir():
            return None, "no exchangeInfo data on disk for BINANCE_USDTF"

        # Find the most recent exchangeInfo file
        candidates = sorted(info_dir.glob("*/*.jsonl*"), reverse=True)
        if not candidates:
            return None, "no exchangeInfo JSONL files found"

        trading_symbols: set[str] = set()
        for path in candidates[:5]:  # try a few recent files
            try:
                import zstandard as zstd
                opener = (
                    zstd.open if path.suffix == ".zst"
                    else open
                )
                with opener(str(path), "rt") as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        for sym_info in rec.get("symbols", []):
                            if sym_info.get("status") == "TRADING":
                                trading_symbols.add(sym_info.get("symbol", ""))
                if trading_symbols:
                    self._futures_support_mapping_cache = trading_symbols
                    return trading_symbols, None
            except Exception as exc:
                logger.debug("Could not read exchangeInfo %s: %s", path, exc)
                continue

        return None, "failed to parse any exchangeInfo files"

    async def _fetch_spot_exchangeinfo_from_rest(self) -> Tuple[set[str] | None, str | None]:
        """Fetch exchangeInfo from Binance REST API and cache it."""
        import time
        
        try:
            url = f"{BINANCE_SPOT_REST}/api/v3/exchangeInfo"
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=API_REQUEST_TIMEOUT_SEC)
                ) as resp:
                    if resp.status != 200:
                        return None, f"exchangeInfo REST API returned {resp.status}"
                    
                    raw = await resp.json()
            
            # Parse and extract trading symbols
            trading_symbols: set[str] = set()
            symbols_data = raw.get("symbols", [])
            
            for sym_info in symbols_data:
                symbol = sym_info.get("symbol", "")
                if sym_info.get("status") != "TRADING":
                    continue
                if sym_info.get("quoteAsset") != QUOTE_ASSET_SPOT:
                    continue
                if sym_info.get("isSpotTradingAllowed") is False:
                    continue
                permissions = sym_info.get("permissions")
                if isinstance(permissions, list) and permissions and "SPOT" not in permissions:
                    continue
                permission_sets = sym_info.get("permissionSets")
                if isinstance(permission_sets, list) and permission_sets:
                    if not any(
                        isinstance(group, list) and "SPOT" in group
                        for group in permission_sets
                    ):
                        continue
                if symbol:
                    trading_symbols.add(symbol)
            
            # Cache the result to disk in JSONL format
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                info_dir = DATA_ROOT / "BINANCE_SPOT" / "exchangeinfo" / "EXCHANGEINFO" / today_str
                info_dir.mkdir(parents=True, exist_ok=True)
                cache_file = info_dir / f"{int(time.time())}.jsonl"
                
                # Write as single JSONL record matching the format expected by _parse_exchangeinfo
                record = {
                    'venue': 'BINANCE_SPOT',
                    'symbol': 'EXCHANGEINFO',
                    'channel': 'exchangeinfo',
                    'ts_recv_ns': int(time.time_ns()),
                    'ts_event_ms': raw.get('serverTime'),
                    'timezone': raw.get('timezone'),
                    'symbols': symbols_data,
                }
                with open(cache_file, 'w') as f:
                    f.write(json.dumps(record) + '\n')
                logger.info(f"Cached BINANCE_SPOT exchangeInfo to {cache_file}")
            except Exception as e:
                logger.warning(f"Failed to cache exchangeInfo to disk: {e}")
            
            if trading_symbols:
                self._spot_support_mapping_cache = trading_symbols
                logger.info(f"Fetched {len(trading_symbols)} TRADING USDT spot symbols from REST")
                return trading_symbols, None
            else:
                return None, "no TRADING USDT spot symbols found in REST response"
        
        except Exception as e:
            return None, f"error fetching exchangeInfo from REST: {e}"

    async def _get_spot_exchange_info_symbols(self) -> Tuple[set[str] | None, str | None]:
        """Load TRADING, USDT-quoted spot symbols from cached exchangeInfo or fetch from REST."""
        if self._spot_support_mapping_cache is not None:
            return self._spot_support_mapping_cache, None

        info_dir = DATA_ROOT / "BINANCE_SPOT" / "exchangeinfo" / "EXCHANGEINFO"
        if info_dir.is_dir():
            candidates = sorted(info_dir.glob("*/*.jsonl*"), reverse=True)
            if candidates:
                trading_symbols: set[str] = set()
                for path in candidates[:5]:
                    try:
                        import zstandard as zstd
                        opener = zstd.open if path.suffix == ".zst" else open
                        with opener(str(path), "rt") as f:
                            for line in f:
                                try:
                                    rec = json.loads(line)
                                except json.JSONDecodeError:
                                    continue
                                for sym_info in rec.get("symbols", []):
                                    symbol = sym_info.get("symbol", "")
                                    if sym_info.get("status") != "TRADING":
                                        continue
                                    if sym_info.get("quoteAsset") != QUOTE_ASSET_SPOT:
                                        continue
                                    if sym_info.get("isSpotTradingAllowed") is False:
                                        continue
                                    permissions = sym_info.get("permissions")
                                    if isinstance(permissions, list) and permissions and "SPOT" not in permissions:
                                        continue
                                    permission_sets = sym_info.get("permissionSets")
                                    if isinstance(permission_sets, list) and permission_sets:
                                        if not any(
                                            isinstance(group, list) and "SPOT" in group
                                            for group in permission_sets
                                        ):
                                            continue
                                    if symbol:
                                        trading_symbols.add(symbol)
                        if trading_symbols:
                            self._spot_support_mapping_cache = trading_symbols
                            logger.info(f"Loaded {len(trading_symbols)} TRADING USDT spot symbols from disk")
                            return trading_symbols, None
                    except Exception as exc:
                        logger.debug("Could not read exchangeInfo %s: %s", path, exc)
                        continue

        # Disk data not available; fetch from REST and cache
        logger.info("No cached BINANCE_SPOT exchangeInfo on disk; fetching from REST API...")
        return await self._fetch_spot_exchangeinfo_from_rest()

    async def _apply_spot_support_precheck(
        self,
        candidates: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool, str | None]:
        """Filter spot candidates by exchangeInfo TRADING/USDT/spot support."""
        trading_symbols, error = await self._get_spot_exchange_info_symbols()
        if not trading_symbols:
            return list(candidates), [], False, error

        kept: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for item in candidates:
            symbol = item["symbol"]
            if symbol in trading_symbols:
                kept.append(item)
            else:
                rejected.append({
                    "symbol": symbol,
                    "reason": "not found in spot exchangeInfo TRADING USDT symbols",
                    "quote_volume": item["quoteVolume"],
                })
        return kept, rejected, True, None

    def _apply_futures_support_precheck(
        self,
        candidates: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool, str | None]:
        """Filter futures candidates by the exchange's own symbol list (REST-based)."""
        trading_symbols, error = self._get_futures_exchange_info_symbols()
        if not trading_symbols:
            return list(candidates), [], False, error

        kept: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        for item in candidates:
            symbol = item["symbol"]
            if symbol in trading_symbols:
                kept.append(item)
            else:
                rejected.append({
                    "symbol": symbol,
                    "reason": "not found in futures exchangeInfo TRADING symbols",
                    "quote_volume": item["quoteVolume"],
                })

        return kept, rejected, True, None

    def _health_summary_dir(self) -> Path:
        return STATE_ROOT / "universe_health"

    def _load_health_exclusions(
        self,
        venue_key: str,
    ) -> Tuple[set[str], Dict[str, Any]]:
        """Derive temporary exclusions from recent daily health summaries."""
        metadata = {
            "enabled": UNIVERSE_HEALTH_ENABLED,
            "health_excluded_count": 0,
            "health_excluded_sample": [],
            "symbol_health_path": str(self._health_summary_dir() / "symbol_health.json"),
        }
        if not UNIVERSE_HEALTH_ENABLED:
            return set(), metadata

        health_dir = self._health_summary_dir()
        if not health_dir.is_dir():
            return set(), metadata

        by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for path in sorted(health_dir.glob("*.json")):
            if path.name == "symbol_health.json":
                continue
            try:
                daily = json.loads(path.read_text())
            except Exception:
                continue
            symbols = daily.get("symbols", {})
            venue_symbols = symbols.get(venue_key, {}) if isinstance(symbols, dict) else {}
            if not isinstance(venue_symbols, dict):
                continue
            for symbol, info in venue_symbols.items():
                if isinstance(info, dict):
                    by_symbol.setdefault(symbol, []).append(info)

        now = datetime.now(timezone.utc)
        excluded: set[str] = set()
        aggregate_path = health_dir / "symbol_health.json"
        if aggregate_path.exists():
            try:
                aggregate = json.loads(aggregate_path.read_text())
            except Exception:
                aggregate = {}
        else:
            aggregate = {}
        if not isinstance(aggregate, dict):
            aggregate = {}
        aggregate.update({
            "timestamp": local_now_iso(),
            "exclude_days": UNIVERSE_HEALTH_EXCLUDE_DAYS,
            "min_observation_sec": UNIVERSE_HEALTH_MIN_OBSERVATION_SEC,
            "max_consecutive_zero_runs": UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS,
        })
        aggregate.setdefault("venues", {})
        venue_aggregate: Dict[str, Any] = {}
        for symbol, runs in by_symbol.items():
            runs_sorted = sorted(runs, key=lambda item: str(item.get("date", "")))
            consecutive = 0
            latest_date = None
            for info in reversed(runs_sorted):
                observation = float(info.get("observation_sec", 0) or 0)
                depth_count = int(info.get("depth_message_count", 0) or 0)
                trade_count = int(info.get("trade_message_count", 0) or 0)
                qualifies = (
                    observation >= UNIVERSE_HEALTH_MIN_OBSERVATION_SEC
                    and depth_count == 0
                    and trade_count == 0
                )
                if not qualifies:
                    break
                consecutive += 1
                latest_date = str(info.get("date", "")) or latest_date

            temporarily_unhealthy = False
            if consecutive >= UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS and latest_date:
                try:
                    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    temporarily_unhealthy = now - latest_dt <= timedelta(days=UNIVERSE_HEALTH_EXCLUDE_DAYS + 1)
                except ValueError:
                    temporarily_unhealthy = True
            if temporarily_unhealthy:
                excluded.add(symbol)
            venue_aggregate[symbol] = {
                "consecutive_zero_message_runs": consecutive,
                "temporarily_unhealthy": temporarily_unhealthy,
                "latest_date": latest_date,
            }

        aggregate["venues"][venue_key] = venue_aggregate
        try:
            health_dir.mkdir(parents=True, exist_ok=True)
            aggregate_path.write_text(json.dumps(aggregate, indent=2))
        except Exception as exc:
            logger.debug("Could not write symbol health aggregate: %s", exc)

        sample = [{"symbol": symbol, **venue_aggregate.get(symbol, {})} for symbol in sorted(excluded)[:UNIVERSE_REJECT_SAMPLE_SIZE]]
        metadata.update({
            "health_excluded_count": len(excluded),
            "health_excluded_sample": sample,
        })
        return excluded, metadata

    def _apply_health_exclusions(
        self,
        candidates: List[Dict[str, Any]],
        venue_key: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        excluded_symbols, health_metadata = self._load_health_exclusions(venue_key)
        if not excluded_symbols:
            return list(candidates), [], health_metadata

        kept: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []
        for item in candidates:
            symbol = item["symbol"]
            if symbol in excluded_symbols:
                rejected.append({
                    "symbol": symbol,
                    "reason": "temporarily_unhealthy_zero_messages",
                    "quote_volume": item["quoteVolume"],
                })
            else:
                kept.append(item)
        health_metadata["health_excluded_count"] = len(rejected)
        health_metadata["health_excluded_sample"] = rejected[:UNIVERSE_REJECT_SAMPLE_SIZE]
        return kept, rejected, health_metadata

    async def _select_from_tickers(self, ticker_data: List[dict], venue_type: str) -> Tuple[List[str], Dict[str, Any]]:
        """Build final selected universe from ranked Binance ticker data."""
        venue_key = _venue_key(venue_type)
        quote = _quote_asset(venue_type)
        configured_candidate_pool_size = _candidate_pool_size(venue_type)
        quote_matched, candidate_pool = self._rank_candidates(ticker_data, venue_type)
        sanity_candidates, sanity_rejected = self._apply_sanity_filter(candidate_pool, venue_type)

        support_candidates = list(sanity_candidates)
        support_rejected: List[Dict[str, Any]] = []
        support_precheck_available = False
        support_precheck_error: str | None = None
        if venue_type == "spot":
            support_candidates, support_rejected, support_precheck_available, support_precheck_error = (
                await self._apply_spot_support_precheck(sanity_candidates)
            )
        elif venue_type == "futures":
            support_candidates, support_rejected, support_precheck_available, support_precheck_error = (
                self._apply_futures_support_precheck(sanity_candidates)
            )

        health_candidates, health_rejected, health_metadata = self._apply_health_exclusions(
            support_candidates,
            venue_key,
        )
        selected = [item["symbol"] for item in health_candidates[:TOP_SYMBOLS]]
        selected_without_health = [item["symbol"] for item in support_candidates[:TOP_SYMBOLS]]
        replacement_symbols = [
            symbol for symbol in selected
            if symbol not in selected_without_health
        ]
        rejected_sample = sanity_rejected[:UNIVERSE_REJECT_SAMPLE_SIZE]
        support_rejected_sample = support_rejected[:UNIVERSE_REJECT_SAMPLE_SIZE]

        metadata = {
            "venue": venue_key,
            "quote_asset": quote,
            "raw_candidates_seen": len(ticker_data),
            "quote_matched_count": len(quote_matched),
            "configured_candidate_pool_size": configured_candidate_pool_size,
            "candidate_pool_size": configured_candidate_pool_size,
            "candidate_pool_count": len(candidate_pool),
            "candidate_pool_raw_count": len(candidate_pool),
            "candidate_pool_after_sanity_count": len(sanity_candidates),
            "candidate_pool_after_support_check_count": len(support_candidates),
            "candidate_pool_after_health_check_count": len(health_candidates),
            "requested_count": TOP_SYMBOLS,
            "eligible_count": len(health_candidates),
            "survivor_count": len(health_candidates),
            "selected_count": len(selected),
            "selected_sample": selected[:10],
            "final_selected": selected,
            "pre_filter_rejected_count": len(sanity_rejected),
            "pre_filter_rejected_sample": rejected_sample,
            "rejected_pre_filter_count": len(sanity_rejected),
            "rejected_pre_filter_sample": rejected_sample,
            "support_precheck_available": support_precheck_available,
            "support_precheck_error": support_precheck_error,
            "support_precheck_rejected_count": len(support_rejected),
            "support_precheck_rejected_sample": support_rejected_sample,
            "health_excluded_count": len(health_rejected),
            "health_excluded_sample": health_rejected[:UNIVERSE_REJECT_SAMPLE_SIZE],
            "health_metadata": health_metadata,
            "replacement_symbols": replacement_symbols,
        }

        logger.info(
            f"{venue_type.capitalize()} raw Binance candidates seen: {len(ticker_data)}"
        )
        logger.info(
            f"{venue_type.capitalize()} quote-matched candidates: {len(quote_matched)}"
        )
        logger.info(
            f"{venue_type.capitalize()} candidate pool used: {len(candidate_pool)} "
            f"(configured {configured_candidate_pool_size})"
        )
        logger.info(f"{venue_type.capitalize()} pre_filter_rejected: {len(sanity_rejected)}")
        if rejected_sample:
            logger.info(
                f"{venue_type.capitalize()} pre-filter rejected sample: "
                + ", ".join(
                    f"{item['symbol']} ({item['reason']})" for item in rejected_sample
                )
            )
        logger.info(
            f"{venue_type.capitalize()} after sanity filter: {len(sanity_candidates)}"
        )
        if venue_type in {"spot", "futures"}:
            if support_precheck_available:
                logger.info(
                    f"{venue_type.capitalize()} rejected by support precheck: {len(support_rejected)}"
                )
                if support_rejected_sample:
                    logger.info(
                        f"{venue_type.capitalize()} support-precheck rejected sample: "
                        + ", ".join(
                            f"{item['symbol']} ({item['reason']})"
                            for item in support_rejected_sample
                        )
                    )
                logger.info(
                    f"{venue_type.capitalize()} after support precheck: {len(support_candidates)}"
                )
            else:
                logger.warning(
                    f"{venue_type.capitalize()} support precheck unavailable; falling back to sanity-only selection: "
                    f"{support_precheck_error}"
                )
        if health_rejected:
            logger.info(
                f"{venue_type.capitalize()} temporarily health-excluded: {len(health_rejected)}"
            )
        logger.info(f"{venue_type.capitalize()} final selected: {len(selected)}")
        logger.debug(
            f"{venue_type.capitalize()} top selected symbols: {', '.join(selected[:10])}"
        )
        if len(health_candidates) < TOP_SYMBOLS:
            logger.warning(
                f"{venue_type.capitalize()} eligible_count below target: {len(health_candidates)}/{TOP_SYMBOLS}"
            )

        return selected[:TOP_SYMBOLS], metadata

    async def _select_spot_symbols(self) -> Tuple[List[str], Dict[str, Any]]:
        """Select top symbols from Binance Spot."""
        try:
            url = f"{BINANCE_SPOT_REST}/api/v3/ticker/24hr"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=API_REQUEST_TIMEOUT_SEC)
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch spot tickers: {resp.status}")
                        return [], self._empty_selection_metadata("spot")
                    
                    data = await resp.json()
            return await self._select_from_tickers(data, "spot")
            
        except Exception as e:
            logger.error(f"Error selecting spot symbols: {e}", exc_info=True)
            return [], self._empty_selection_metadata("spot")
    
    async def _select_futures_symbols(self) -> Tuple[List[str], Dict[str, Any]]:
        """Select top symbols from Binance USDT-M Futures."""
        try:
            url = f"{BINANCE_FUTURES_REST}/fapi/v1/ticker/24hr"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=API_REQUEST_TIMEOUT_SEC)
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Failed to fetch futures tickers: {resp.status}")
                        return [], self._empty_selection_metadata("futures")
                    
                    data = await resp.json()
            return await self._select_from_tickers(data, "futures")
            
        except Exception as e:
            logger.error(f"Error selecting futures symbols: {e}", exc_info=True)
            return [], self._empty_selection_metadata(
                "futures",
                support_precheck_available=False,
                support_precheck_error=str(e),
            )
