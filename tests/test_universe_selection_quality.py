from __future__ import annotations

from binance_universe import (
    UniverseSelector,
    is_reasonable_symbol,
    partition_known_unsupported_symbols,
)


def test_reasonable_standard_symbol_passes() -> None:
    ok, reason = is_reasonable_symbol("BTCUSDT", "spot")
    assert ok is True
    assert reason == "ok"


def test_numeric_prefix_symbol_is_allowed_for_futures() -> None:
    ok, reason = is_reasonable_symbol("1000PEPEUSDT", "futures")
    assert ok is True
    assert reason == "ok"


def test_short_base_symbol_is_rejected() -> None:
    ok, reason = is_reasonable_symbol("UUSDT", "spot")
    assert ok is False
    assert "shorter than 2 chars" in reason


def test_non_ascii_symbol_is_rejected() -> None:
    ok, reason = is_reasonable_symbol("币安人生USDT", "spot")
    assert ok is False
    assert "non-ASCII" in reason


def test_known_unsupported_symbols_are_partitioned() -> None:
    kept, removed = partition_known_unsupported_symbols(
        ["BTCUSDT", "FDUSDUSDT", "ETHUSDT"]
    )
    assert kept == ["BTCUSDT", "ETHUSDT"]
    assert removed == ["FDUSDUSDT"]


def test_selection_pipeline_reports_rejections_consistently() -> None:
    selector = UniverseSelector()
    fake_tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "ETHUSDT", "quoteVolume": "900"},
        {"symbol": "1000PEPEUSDT", "quoteVolume": "800"},
        {"symbol": "币安人生USDT", "quoteVolume": "700"},
        {"symbol": "UUSDT", "quoteVolume": "600"},
        {"symbol": "FDUSDUSDT", "quoteVolume": "500"},
        {"symbol": "AGIXBTC", "quoteVolume": "400"},
    ]
    selected, metadata = selector._select_from_tickers(fake_tickers, "spot")

    assert selected[:3] == ["BTCUSDT", "ETHUSDT", "1000PEPEUSDT"]
    assert metadata["eligible_count"] == 3
    assert metadata["survivor_count"] == 3
    assert metadata["pre_filter_rejected_count"] == 3
    assert metadata["rejected_pre_filter_count"] == 3

    rejected_symbols = {
        item["symbol"] for item in metadata["pre_filter_rejected_sample"]
    }
    assert {"币安人生USDT", "UUSDT", "FDUSDUSDT"}.issubset(rejected_symbols)
