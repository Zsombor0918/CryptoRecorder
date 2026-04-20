#!/usr/bin/env python3
"""
tests/test_futures_support_precheck.py — Verify futures support-aware selection.

Usage:
    python tests/test_futures_support_precheck.py
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def run_tests() -> int:
    import binance_universe as universe_module
    from binance_universe import UniverseSelector

    results = []

    selector = UniverseSelector()
    selector._get_futures_support_mapping = lambda: (
        {
            "BTC-USDT-PERP": "BTCUSDT",
            "ETH-USDT-PERP": "ETHUSDT",
            "SOL-USDT-PERP": "SOLUSDT",
        },
        None,
    )
    fake_tickers = [
        {"symbol": "BTCUSDT", "quoteVolume": "1000"},
        {"symbol": "ETHUSDT", "quoteVolume": "900"},
        {"symbol": "SOLUSDT", "quoteVolume": "800"},
        {"symbol": "AGIXUSDT", "quoteVolume": "700"},
        {"symbol": "FDUSDUSDT", "quoteVolume": "600"},
        {"symbol": "币安人生USDT", "quoteVolume": "500"},
    ]

    selected, metadata = selector._select_from_tickers(fake_tickers, "futures")
    ok1 = selected[:3] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    ok1 = ok1 and metadata["candidate_pool_raw_count"] == 6
    ok1 = ok1 and metadata["candidate_pool_after_sanity_count"] == 4
    ok1 = ok1 and metadata["candidate_pool_after_support_check_count"] == 3
    ok1 = ok1 and metadata["eligible_count"] == 3
    ok1 = ok1 and metadata["pre_filter_rejected_count"] == 2
    ok1 = ok1 and metadata["support_precheck_rejected_count"] == 1
    ok1 = ok1 and metadata["support_precheck_available"] is True
    print(
        f"  [{'PASS' if ok1 else 'FAIL'}] support_precheck_filters "
        f"(selected={selected}, metadata={metadata})"
    )
    results.append(ok1)

    rejected_symbols = {
        item["symbol"] for item in metadata["support_precheck_rejected_sample"]
    }
    ok2 = "AGIXUSDT" in rejected_symbols
    print(
        f"  [{'PASS' if ok2 else 'FAIL'}] support_precheck_rejected_sample "
        f"(sample={metadata['support_precheck_rejected_sample']})"
    )
    results.append(ok2)

    fallback_selector = UniverseSelector()
    fallback_selector._get_futures_support_mapping = lambda: (None, "mock mapping unavailable")
    fallback_selected, fallback_metadata = fallback_selector._select_from_tickers(
        fake_tickers, "futures"
    )
    ok3 = fallback_selected[:4] == ["BTCUSDT", "ETHUSDT", "SOLUSDT", "AGIXUSDT"]
    ok3 = ok3 and fallback_metadata["support_precheck_available"] is False
    ok3 = ok3 and fallback_metadata["support_precheck_rejected_count"] == 0
    ok3 = ok3 and fallback_metadata["candidate_pool_after_support_check_count"] == 4
    ok3 = ok3 and fallback_metadata["eligible_count"] == 4
    print(
        f"  [{'PASS' if ok3 else 'FAIL'}] fallback_without_support_map "
        f"(selected={fallback_selected}, metadata={fallback_metadata})"
    )
    results.append(ok3)

    selector_refresh = UniverseSelector()
    calls = []
    original_symbol_mapping = universe_module.BinanceFutures.symbol_mapping

    def _mock_symbol_mapping(refresh: bool = False):
        calls.append(refresh)
        if not refresh:
            raise RuntimeError("no cached mapping")
        return {"BTC-USDT-PERP": "BTCUSDT"}

    universe_module.BinanceFutures.symbol_mapping = _mock_symbol_mapping
    try:
        mapping, error = selector_refresh._get_futures_support_mapping()
    finally:
        universe_module.BinanceFutures.symbol_mapping = original_symbol_mapping

    ok4 = isinstance(mapping, dict) and mapping.get("BTC-USDT-PERP") == "BTCUSDT" and error is None
    ok4 = ok4 and calls == [False, True]
    print(
        f"  [{'PASS' if ok4 else 'FAIL'}] support_mapping_refresh_fallback "
        f"(calls={calls}, error={error})"
    )
    results.append(ok4)

    passed = sum(results)
    total = len(results)
    print(f"\n  {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run_tests())
