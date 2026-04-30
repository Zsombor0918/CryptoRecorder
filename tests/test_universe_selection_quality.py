from __future__ import annotations

from binance_universe import (
    UniverseSelector,
    is_reasonable_symbol,
    partition_known_unsupported_symbols,
)
import binance_universe as universe_mod
import asyncio
import pytest


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
    """In the deterministic native architecture, partition keeps everything."""
    kept, removed = partition_known_unsupported_symbols(
        ["BTCUSDT", "FDUSDUSDT", "ETHUSDT"]
    )
    assert kept == ["BTCUSDT", "FDUSDUSDT", "ETHUSDT"]
    assert removed == []


def test_selection_pipeline_reports_rejections_consistently() -> None:
    selector = UniverseSelector()
    
    # Mock async method
    async def mock_get_symbols():
        return None, "no data"
    
    selector._get_spot_exchange_info_symbols = mock_get_symbols
    
    fake_tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "ETHUSDT", "quoteVolume": "900"},
        {"symbol": "1000PEPEUSDT", "quoteVolume": "800"},
        {"symbol": "币安人生USDT", "quoteVolume": "700"},
        {"symbol": "UUSDT", "quoteVolume": "600"},
        {"symbol": "FDUSDUSDT", "quoteVolume": "500"},
        {"symbol": "AGIXBTC", "quoteVolume": "400"},
    ]
    selected, metadata = asyncio.run(selector._select_from_tickers(fake_tickers, "spot"))

    # FDUSDUSDT passes sanity filter in native architecture (no cryptofeed friction)
    assert selected[:4] == ["BTCUSDT", "ETHUSDT", "1000PEPEUSDT", "FDUSDUSDT"]
    assert metadata["eligible_count"] == 4
    assert metadata["survivor_count"] == 4
    assert metadata["pre_filter_rejected_count"] == 2
    assert metadata["rejected_pre_filter_count"] == 2

    rejected_symbols = {
        item["symbol"] for item in metadata["pre_filter_rejected_sample"]
    }
    assert {"币安人生USDT", "UUSDT"}.issubset(rejected_symbols)


@pytest.mark.asyncio
async def test_spot_support_precheck_filters_break_symbol() -> None:
    selector = UniverseSelector()
    selector._spot_support_mapping_cache = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    fake_tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "UTKUSDT", "quoteVolume": "900"},
        {"symbol": "ETHUSDT", "quoteVolume": "800"},
        {"symbol": "SOLUSDT", "quoteVolume": "700"},
    ]

    selected, metadata = await selector._select_from_tickers(fake_tickers, "spot")

    assert "UTKUSDT" not in selected
    assert selected[:3] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert metadata["support_precheck_available"] is True
    assert metadata["support_precheck_rejected_count"] == 1
    assert metadata["support_precheck_rejected_sample"][0]["symbol"] == "UTKUSDT"


def test_old_spot_cache_without_successful_support_precheck_is_rejected() -> None:
    selector = UniverseSelector()
    base_cache = {
        "filter_version": universe_mod.UNIVERSE_FILTER_VERSION,
        "top_count": universe_mod.TOP_SYMBOLS,
        "BINANCE_SPOT": ["BTCUSDT"],
        "BINANCE_USDTF": ["BTCUSDT"],
        "selection_metadata": {
            "BINANCE_SPOT": {
                "configured_candidate_pool_size": universe_mod.TOP_SYMBOL_CANDIDATES,
                "selected_count": 1,
            },
            "BINANCE_USDTF": {
                "configured_candidate_pool_size": universe_mod.FUTURES_TOP_SYMBOL_CANDIDATES,
                "selected_count": 1,
                "support_precheck_available": True,
            },
        },
    }

    assert selector._cache_is_usable(base_cache) is False

    base_cache["selection_metadata"]["BINANCE_SPOT"]["support_precheck_available"] = False
    assert selector._cache_is_usable(base_cache) is False

    base_cache["selection_metadata"]["BINANCE_SPOT"]["support_precheck_available"] = True
    assert selector._cache_is_usable(base_cache) is True


def test_universe_health_excludes_only_after_repeated_zero_runs(monkeypatch, tmp_path) -> None:
    import binance_universe as universe_mod

    health_dir = tmp_path / "state" / "universe_health"
    health_dir.mkdir(parents=True)
    for date_str in ("2026-04-24", "2026-04-25"):
        (health_dir / f"{date_str}.json").write_text(
            """
{
  "date": "%s",
  "symbols": {
    "BINANCE_SPOT": {
      "UTKUSDT": {
        "date": "%s",
        "observation_sec": 400,
        "depth_message_count": 0,
        "trade_message_count": 0
      }
    }
  }
}
""" % (date_str, date_str)
        )

    monkeypatch.setattr(universe_mod, "STATE_ROOT", tmp_path / "state")
    monkeypatch.setattr(universe_mod, "TOP_SYMBOLS", 3)
    monkeypatch.setattr(universe_mod, "UNIVERSE_ZERO_MESSAGE_MAX_CONSECUTIVE_RUNS", 2)
    monkeypatch.setattr(universe_mod, "UNIVERSE_HEALTH_MIN_OBSERVATION_SEC", 300)
    monkeypatch.setattr(universe_mod, "UNIVERSE_HEALTH_EXCLUDE_DAYS", 10)

    selector = UniverseSelector()
    selector._spot_support_mapping_cache = {"BTCUSDT", "UTKUSDT", "ETHUSDT", "SOLUSDT"}
    fake_tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "UTKUSDT", "quoteVolume": "900"},
        {"symbol": "ETHUSDT", "quoteVolume": "800"},
        {"symbol": "SOLUSDT", "quoteVolume": "700"},
    ]

    selected, metadata = asyncio.run(selector._select_from_tickers(fake_tickers, "spot"))

    assert selected == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert metadata["health_excluded_count"] == 1
    assert metadata["replacement_symbols"] == ["SOLUSDT"]
    assert (health_dir / "symbol_health.json").exists()
