"""
Select top 50 trading symbols from Binance by 24h quote volume.
"""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List

import aiohttp

from config import (
    META_ROOT,
    BINANCE_SPOT_REST,
    BINANCE_FUTURES_REST,
    TOP_SYMBOLS,
    QUOTE_ASSET_SPOT,
    QUOTE_ASSET_FUTURES,
    API_REQUEST_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)


class UniverseSelector:
    """Selects and manages trading universe."""
    
    def __init__(self):
        self.universe_path = META_ROOT / "universe"
        self.universe_path.mkdir(parents=True, exist_ok=True)
    
    async def get_or_select_universe(self, force_refresh: bool = False) -> dict:
        """
        Get today's universe or select if not already cached.
        Returns {venue: [list of symbols]}
        """
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        cache_file = self.universe_path / "BINANCE_SPOT" / f"{today_str}.json"
        
        # Check cache
        if cache_file.exists() and not force_refresh:
            logger.info(f"Loading universe from cache: {cache_file}")
            with open(cache_file) as f:
                return json.load(f)
        
        logger.info("Selecting top 50 symbols for each venue...")
        
        # Select symbols
        spot_symbols = await self._select_top_symbols("spot")
        futures_symbols = await self._select_top_symbols("futures")
        
        universe = {
            "BINANCE_SPOT": spot_symbols,
            "BINANCE_USDTF": futures_symbols,
            "timestamp": datetime.utcnow().isoformat(),
            "top_count": TOP_SYMBOLS,
        }
        
        # Save cache
        (self.universe_path / "BINANCE_SPOT").mkdir(parents=True, exist_ok=True)
        (self.universe_path / "BINANCE_USDTF").mkdir(parents=True, exist_ok=True)
        
        spot_cache = self.universe_path / "BINANCE_SPOT" / f"{today_str}.json"
        futures_cache = self.universe_path / "BINANCE_USDTF" / f"{today_str}.json"
        
        with open(spot_cache, 'w') as f:
            json.dump(universe, f, indent=2)
        
        with open(futures_cache, 'w') as f:
            json.dump(universe, f, indent=2)
        
        logger.info(f"Saved universe to cache: {spot_cache}, {futures_cache}")
        return universe
    
    async def _select_top_symbols(self, venue_type: str) -> List[str]:
        """Select top N symbols by 24h quote volume."""
        logger.info(f"Fetching top {TOP_SYMBOLS} {venue_type} symbols...")
        
        if venue_type == "spot":
            return await self._select_spot_symbols()
        elif venue_type == "futures":
            return await self._select_futures_symbols()
        else:
            raise ValueError(f"Unknown venue type: {venue_type}")
    
    async def _select_spot_symbols(self) -> List[str]:
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
                        return []
                    
                    data = await resp.json()
            
            # Filter USDT pairs and sort by quote volume
            usdt_pairs = [
                {
                    'symbol': item['symbol'],
                    'quoteVolume': float(item.get('quoteVolume', 0))
                }
                for item in data
                if item['symbol'].endswith(QUOTE_ASSET_SPOT)
            ]
            
            usdt_pairs.sort(key=lambda x: x['quoteVolume'], reverse=True)
            
            symbols = [item['symbol'] for item in usdt_pairs[:TOP_SYMBOLS]]
            logger.info(f"Selected {len(symbols)} spot symbols")
            logger.debug(f"Top 5 spot symbols: {symbols[:5]}")
            
            return symbols
            
        except Exception as e:
            logger.error(f"Error selecting spot symbols: {e}", exc_info=True)
            return []
    
    async def _select_futures_symbols(self) -> List[str]:
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
                        return []
                    
                    data = await resp.json()
            
            # Filter USDT perpetuals and sort by quote volume
            usdt_perps = [
                {
                    'symbol': item['symbol'],
                    'quoteVolume': float(item.get('quoteVolume', 0))
                }
                for item in data
                if item['symbol'].endswith(QUOTE_ASSET_FUTURES) and len(item['symbol']) > len(QUOTE_ASSET_FUTURES)
            ]
            
            usdt_perps.sort(key=lambda x: x['quoteVolume'], reverse=True)
            
            symbols = [item['symbol'] for item in usdt_perps[:TOP_SYMBOLS]]
            logger.info(f"Selected {len(symbols)} futures symbols")
            logger.debug(f"Top 5 futures symbols: {symbols[:5]}")
            
            return symbols
            
        except Exception as e:
            logger.error(f"Error selecting futures symbols: {e}", exc_info=True)
            return []
