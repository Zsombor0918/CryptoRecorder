from __future__ import annotations

import binance_universe as universe_module
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


def test_support_precheck_filters_selected_futures_symbols() -> None:
    selector = UniverseSelector()
    selector._get_futures_support_mapping = lambda: (
        {
            "BTC-USDT-PERP": "BTCUSDT",
            "ETH-USDT-PERP": "ETHUSDT",
            "SOL-USDT-PERP": "SOLUSDT",
        },
        None,
    )

    selected, metadata = selector._select_from_tickers(_fake_tickers(), "futures")

    assert selected[:3] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert metadata["candidate_pool_raw_count"] == 6
    assert metadata["candidate_pool_after_sanity_count"] == 4
    assert metadata["candidate_pool_after_support_check_count"] == 3
    assert metadata["eligible_count"] == 3
    assert metadata["pre_filter_rejected_count"] == 2
    assert metadata["support_precheck_rejected_count"] == 1
    assert metadata["support_precheck_available"] is True


def test_support_precheck_rejected_sample_contains_missing_mapping_symbol() -> None:
    selector = UniverseSelector()
    selector._get_futures_support_mapping = lambda: (
        {"BTC-USDT-PERP": "BTCUSDT"},
        None,
    )
    _, metadata = selector._select_from_tickers(_fake_tickers(), "futures")

    rejected_symbols = {
        item["symbol"] for item in metadata["support_precheck_rejected_sample"]
    }
    assert "AGIXUSDT" in rejected_symbols


def test_fallback_without_support_map_keeps_sane_survivors() -> None:
    selector = UniverseSelector()
    selector._get_futures_support_mapping = lambda: (None, "mock mapping unavailable")

    selected, metadata = selector._select_from_tickers(_fake_tickers(), "futures")

    assert selected[:4] == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AGIXUSDT"]
    assert metadata["support_precheck_available"] is False
    assert metadata["support_precheck_rejected_count"] == 0
    assert metadata["candidate_pool_after_support_check_count"] == 4
    assert metadata["eligible_count"] == 4


def test_support_mapping_refresh_fallback_retries_with_refresh() -> None:
    selector = UniverseSelector()
    calls: list[bool] = []
    original_symbol_mapping = universe_module.BinanceFutures.symbol_mapping

    def _mock_symbol_mapping(refresh: bool = False):
        calls.append(refresh)
        if not refresh:
            raise RuntimeError("no cached mapping")
        return {"BTC-USDT-PERP": "BTCUSDT"}

    universe_module.BinanceFutures.symbol_mapping = _mock_symbol_mapping
    try:
        mapping, error = selector._get_futures_support_mapping()
    finally:
        universe_module.BinanceFutures.symbol_mapping = original_symbol_mapping

    assert isinstance(mapping, dict)
    assert mapping.get("BTC-USDT-PERP") == "BTCUSDT"
    assert error is None
    assert calls == [False, True]
