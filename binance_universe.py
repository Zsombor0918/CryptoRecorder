"""
Select top 50 trading symbols from Binance by 24h quote volume.
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import aiohttp
from cryptofeed.exchanges import BinanceFutures

from config import (
    META_ROOT,
    BINANCE_SPOT_REST,
    BINANCE_FUTURES_REST,
    TOP_SYMBOLS,
    TOP_SYMBOL_CANDIDATES,
    FUTURES_TOP_SYMBOL_CANDIDATES,
    QUOTE_ASSET_SPOT,
    QUOTE_ASSET_FUTURES,
    API_REQUEST_TIMEOUT_SEC,
    UNIVERSE_FILTER_VERSION,
    UNIVERSE_REJECT_SAMPLE_SIZE,
    KNOWN_UNSUPPORTED_FULL_SYMBOLS,
    KNOWN_UNSUPPORTED_BASE_ASSETS,
)

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
    """Split symbols into kept vs known-unsupported subsets."""
    kept: List[str] = []
    removed: List[str] = []
    for symbol in symbols:
        reason = known_unsupported_reason(symbol)
        if reason:
            removed.append(symbol)
        else:
            kept.append(symbol)
    return kept, removed


def known_unsupported_reason(symbol: str) -> str | None:
    """Return reason if the symbol matches a known unsupported pattern."""
    if symbol in KNOWN_UNSUPPORTED_FULL_SYMBOLS:
        return f"known unsupported symbol {symbol}"

    if symbol.endswith("USDT") and len(symbol) > len("USDT"):
        base = symbol[:-len("USDT")]
        if base in KNOWN_UNSUPPORTED_BASE_ASSETS:
            return f"known unsupported base asset {base}"

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
        self._futures_support_mapping_cache: Dict[str, str] | None = None

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
        }

        if venue_type == "futures":
            metadata.update({
                "support_precheck_available": bool(support_precheck_available),
                "support_precheck_error": support_precheck_error,
                "support_precheck_rejected_count": 0,
                "support_precheck_rejected_sample": [],
            })
        else:
            metadata.update({
                "support_precheck_available": False,
                "support_precheck_error": None,
                "support_precheck_rejected_count": 0,
                "support_precheck_rejected_sample": [],
            })

        return metadata
    
    async def get_or_select_universe(self, force_refresh: bool = False) -> dict:
        """
        Get today's universe or select if not already cached.
        Returns {venue: [list of symbols]}
        """
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
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
            "timestamp": datetime.utcnow().isoformat(),
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
            if venue_type == "futures" and meta.get("support_precheck_available") is False:
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

    def _get_futures_support_mapping(self) -> Tuple[Dict[str, str] | None, str | None]:
        """Load cryptofeed's Binance Futures symbol map for a support precheck."""
        if self._futures_support_mapping_cache is not None:
            return self._futures_support_mapping_cache, None

        last_error: str | None = None
        try:
            mapping = BinanceFutures.symbol_mapping(refresh=False)
            if not isinstance(mapping, dict):
                raise TypeError(f"unexpected symbol mapping type {type(mapping).__name__}")
            self._futures_support_mapping_cache = mapping
            return mapping, None
        except Exception as e:
            last_error = str(e)

        # Best-effort second attempt: force a refresh if no cached mapping was available.
        # Failure still degrades gracefully to sanity-only selection.
        try:
            mapping = BinanceFutures.symbol_mapping(refresh=True)
            if not isinstance(mapping, dict):
                raise TypeError(f"unexpected symbol mapping type {type(mapping).__name__}")
            self._futures_support_mapping_cache = mapping
            return mapping, None
        except Exception as e:
            err = str(e)
            if last_error:
                err = f"initial lookup failed ({last_error}); refresh failed ({err})"
            return None, err

    def _apply_futures_support_precheck(
        self,
        candidates: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool, str | None]:
        """Filter futures candidates by cryptofeed's actual symbol support map."""
        mapping, error = self._get_futures_support_mapping()
        if not mapping:
            return list(candidates), [], False, error

        kept: List[Dict[str, Any]] = []
        rejected: List[Dict[str, Any]] = []

        for item in candidates:
            symbol = item["symbol"]
            cf_symbol = _raw_to_cf_futures_symbol(symbol, _quote_asset("futures"))
            exchange_symbol = mapping.get(cf_symbol)
            if exchange_symbol == symbol:
                kept.append(item)
            else:
                reason = "missing from cryptofeed BinanceFutures symbol_mapping"
                if exchange_symbol is not None:
                    reason = f"cryptofeed maps {cf_symbol} to {exchange_symbol}, not {symbol}"
                rejected.append({
                    "symbol": symbol,
                    "cf_symbol": cf_symbol,
                    "reason": reason,
                    "quote_volume": item["quoteVolume"],
                })

        return kept, rejected, True, None

    def _select_from_tickers(self, ticker_data: List[dict], venue_type: str) -> Tuple[List[str], Dict[str, Any]]:
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
        if venue_type == "futures":
            support_candidates, support_rejected, support_precheck_available, support_precheck_error = (
                self._apply_futures_support_precheck(sanity_candidates)
            )

        selected = [item["symbol"] for item in support_candidates[:TOP_SYMBOLS]]
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
            "requested_count": TOP_SYMBOLS,
            "eligible_count": len(support_candidates),
            "survivor_count": len(support_candidates),
            "selected_count": len(selected),
            "selected_sample": selected[:10],
            "pre_filter_rejected_count": len(sanity_rejected),
            "pre_filter_rejected_sample": rejected_sample,
            "rejected_pre_filter_count": len(sanity_rejected),
            "rejected_pre_filter_sample": rejected_sample,
            "support_precheck_available": support_precheck_available,
            "support_precheck_error": support_precheck_error,
            "support_precheck_rejected_count": len(support_rejected),
            "support_precheck_rejected_sample": support_rejected_sample,
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
        if venue_type == "futures":
            if support_precheck_available:
                logger.info(
                    f"Futures rejected by support precheck: {len(support_rejected)}"
                )
                if support_rejected_sample:
                    logger.info(
                        "Futures support-precheck rejected sample: "
                        + ", ".join(
                            f"{item['symbol']} -> {item['cf_symbol']} ({item['reason']})"
                            for item in support_rejected_sample
                        )
                    )
                logger.info(
                    f"Futures after support precheck: {len(support_candidates)}"
                )
            else:
                logger.warning(
                    "Futures support precheck unavailable; falling back to sanity-only selection: "
                    f"{support_precheck_error}"
                )
        logger.info(f"{venue_type.capitalize()} final selected: {len(selected)}")
        logger.debug(
            f"{venue_type.capitalize()} top selected symbols: {', '.join(selected[:10])}"
        )
        if len(support_candidates) < TOP_SYMBOLS:
            logger.warning(
                f"{venue_type.capitalize()} eligible_count below target: {len(support_candidates)}/{TOP_SYMBOLS}"
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
            return self._select_from_tickers(data, "spot")
            
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
            return self._select_from_tickers(data, "futures")
            
        except Exception as e:
            logger.error(f"Error selecting futures symbols: {e}", exc_info=True)
            return [], self._empty_selection_metadata(
                "futures",
                support_precheck_available=False,
                support_precheck_error=str(e),
            )
