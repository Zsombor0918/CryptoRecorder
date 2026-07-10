"""Tests for futures support precheck using REST-based exchange info validation."""
from __future__ import annotations

import asyncio

from binance_universe import UniverseSelector


def _fake_tickers() -> list[dict]:
    return [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "ETHUSDT", "quoteVolume": "900"},
        {"symbol": "SOLUSDT", "quoteVolume": "800"},
        {"symbol": "AGIXUSDT", "quoteVolume": "700"},
        {"symbol": "FDUSDUSDT", "quoteVolume": "600"},
        {"symbol": "币安人生USDT", "quoteVolume": "500"},
    ]


def test_support_precheck_filters_by_exchange_info() -> None:
    """Futures symbols not in the exchange's TRADING set are rejected."""
    selector = UniverseSelector()
    # Inject a pre-populated cache of trading symbols
    selector._futures_support_mapping_cache = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

    selected, metadata = asyncio.run(selector._select_from_tickers(_fake_tickers(), "futures"))

    assert selected[:3] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert metadata["candidate_pool_raw_count"] == 6
    # FDUSDUSDT passes sanity in native architecture; only 币安人生USDT fails
    assert metadata["candidate_pool_after_sanity_count"] == 5
    assert metadata["candidate_pool_after_support_check_count"] == 3
    assert metadata["eligible_count"] == 3
    assert metadata["support_precheck_available"] is True


def test_support_precheck_rejected_sample_has_reason() -> None:
    selector = UniverseSelector()
    selector._futures_support_mapping_cache = {"BTCUSDT"}

    _, metadata = asyncio.run(selector._select_from_tickers(_fake_tickers(), "futures"))

    rejected_symbols = {
        item["symbol"] for item in metadata["support_precheck_rejected_sample"]
    }
    # ETHUSDT, SOLUSDT, AGIXUSDT pass sanity but not support
    assert "ETHUSDT" in rejected_symbols or "SOLUSDT" in rejected_symbols


def test_fallback_without_exchange_info_keeps_sane_survivors() -> None:
    """When no exchange info is available, all sanity-passing symbols survive."""
    selector = UniverseSelector()
    selector._futures_support_mapping_cache = None
    # Force the method to return None
    selector._get_futures_exchange_info_symbols = lambda: (None, "no data")

    selected, metadata = asyncio.run(selector._select_from_tickers(_fake_tickers(), "futures"))

    assert selected[:5] == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AGIXUSDT", "FDUSDUSDT"]
    assert metadata["support_precheck_available"] is False
    assert metadata["support_precheck_rejected_count"] == 0
    assert metadata["eligible_count"] == 5
