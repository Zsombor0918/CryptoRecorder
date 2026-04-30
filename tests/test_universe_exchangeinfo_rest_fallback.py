"""
Test that exchangeInfo REST fallback is used when disk cache is missing.

Tests for the feature that fetches BINANCE_SPOT exchangeInfo from REST API
if it's not available on disk, caches it, then runs support precheck.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import binance_universe as universe_mod
from binance_universe import UniverseSelector


@pytest.mark.asyncio
async def test_spot_exchangeinfo_fetched_from_rest_when_disk_missing() -> None:
    """Test that REST fetch is used when no disk cache exists."""
    selector = UniverseSelector()
    
    # Mock the REST response
    rest_response = {
        "serverTime": 1714078000000,
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
            {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
            {
                "symbol": "UTKUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": False,  # Will be filtered out
                "permissions": ["SPOT"],
            },
        ]
    }
    
    # Mock the session
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=rest_response)
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp
    
    with patch("binance_universe.aiohttp.ClientSession", return_value=mock_session):
        trading_symbols, error = await selector._get_spot_exchange_info_symbols()
    
    # Verify REST was called
    mock_session.get.assert_called_once()
    call_url = mock_session.get.call_args[0:1][0]
    assert "api/v3/exchangeInfo" in call_url
    
    # Verify results
    assert error is None
    assert trading_symbols is not None
    assert "BTCUSDT" in trading_symbols
    assert "ETHUSDT" in trading_symbols
    assert "UTKUSDT" not in trading_symbols  # isSpotTradingAllowed=False
    
    # Verify cache is populated
    assert selector._spot_support_mapping_cache == trading_symbols


@pytest.mark.asyncio
async def test_spot_exchangeinfo_from_disk_preferred() -> None:
    """Test that disk cache is preferred over REST."""
    selector = UniverseSelector()
    
    # Pre-populate the cache to simulate disk success
    selector._spot_support_mapping_cache = {"BTCUSDT", "ETHUSDT"}
    
    rest_response = {
        "serverTime": 1714078000000,
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
        ]
    }
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=rest_response)
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp
    
    with patch("binance_universe.aiohttp.ClientSession", return_value=mock_session):
        trading_symbols, error = await selector._get_spot_exchange_info_symbols()
    
    # REST should NOT be called since cache is populated
    mock_session.get.assert_not_called()
    
    # Should return cached values
    assert error is None
    assert trading_symbols == {"BTCUSDT", "ETHUSDT"}


@pytest.mark.asyncio
async def test_spot_support_precheck_uses_rest_fetched_data(tmp_path: Path) -> None:
    """Test that support precheck runs after REST fetch."""
    selector = UniverseSelector()
    
    # Mock REST response
    rest_response = {
        "serverTime": 1714078000000,
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
            {
                "symbol": "ETHUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
        ]
    }
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=rest_response)
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp
    
    candidates = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "ETHUSDT", "quoteVolume": "900"},
        {"symbol": "UTKUSDT", "quoteVolume": "800"},  # Not in exchangeInfo
    ]
    
    with patch("binance_universe.aiohttp.ClientSession", return_value=mock_session):
        kept, rejected, available, error = await selector._apply_spot_support_precheck(candidates)
    
    # Verify precheck results
    assert error is None
    assert available is True
    assert len(kept) == 2
    assert len(rejected) == 1
    
    # UTKUSDT should be rejected
    assert kept[0]["symbol"] == "BTCUSDT"
    assert kept[1]["symbol"] == "ETHUSDT"
    assert rejected[0]["symbol"] == "UTKUSDT"


@pytest.mark.asyncio
async def test_spot_exchangeinfo_rest_error_handling() -> None:
    """Test graceful error handling when REST API fails."""
    selector = UniverseSelector()
    
    # Mock a failed REST response
    mock_resp = AsyncMock()
    mock_resp.status = 500
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp
    
    with patch("binance_universe.aiohttp.ClientSession", return_value=mock_session):
        trading_symbols, error = await selector._get_spot_exchange_info_symbols()
    
    # Should return error gracefully
    assert trading_symbols is None
    assert error is not None
    assert "500" in error


@pytest.mark.asyncio
async def test_spot_exchangeinfo_rest_caches_to_disk(tmp_path: Path) -> None:
    """Test that REST-fetched exchangeInfo is cached to disk."""
    from unittest.mock import patch as mock_patch
    from config import DATA_ROOT
    
    # Create a temporary DATA_ROOT
    temp_data_root = tmp_path / "data"
    temp_data_root.mkdir()
    
    selector = UniverseSelector()
    
    rest_response = {
        "serverTime": 1714078000000,
        "timezone": "UTC",
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "quoteAsset": "USDT",
                "isSpotTradingAllowed": True,
                "permissions": ["SPOT"],
            },
        ]
    }
    
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=rest_response)
    
    mock_session = AsyncMock()
    mock_session.get = AsyncMock()
    mock_session.get.return_value.__aenter__.return_value = mock_resp
    
    with mock_patch("binance_universe.aiohttp.ClientSession", return_value=mock_session):
        with mock_patch("binance_universe.DATA_ROOT", temp_data_root):
            trading_symbols, error = await selector._get_spot_exchange_info_symbols()
    
    # Verify cache directory structure was created
    cache_dir = temp_data_root / "BINANCE_SPOT" / "exchangeinfo" / "EXCHANGEINFO"
    assert cache_dir.exists()
    
    # Verify JSONL file was written
    jsonl_files = list(cache_dir.glob("*/*.jsonl"))
    assert len(jsonl_files) > 0
    
    # Verify file content
    with open(jsonl_files[0]) as f:
        record = json.loads(f.readline())
    assert record["channel"] == "exchangeinfo"
    assert record["venue"] == "BINANCE_SPOT"
    assert len(record["symbols"]) > 0
