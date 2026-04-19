#!/usr/bin/env python3
"""
tests/test_universe_selection_quality.py — Verify universe sanity filtering.

Usage:
    python tests/test_universe_selection_quality.py
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def run_tests() -> int:
    from binance_universe import (
        UniverseSelector,
        is_reasonable_symbol,
        partition_known_unsupported_symbols,
    )

    results = []

    ok1, reason1 = is_reasonable_symbol("BTCUSDT", "spot")
    res1 = ok1 and reason1 == "ok"
    print(f"  [{'PASS' if res1 else 'FAIL'}] standard_symbol ({reason1})")
    results.append(res1)

    ok2, reason2 = is_reasonable_symbol("1000PEPEUSDT", "futures")
    res2 = ok2 and reason2 == "ok"
    print(f"  [{'PASS' if res2 else 'FAIL'}] numeric_prefix_allowed ({reason2})")
    results.append(res2)

    ok3, reason3 = is_reasonable_symbol("UUSDT", "spot")
    res3 = (ok3 is False) and "shorter than 2 chars" in reason3
    print(f"  [{'PASS' if res3 else 'FAIL'}] short_base_rejected ({reason3})")
    results.append(res3)

    ok4, reason4 = is_reasonable_symbol("币安人生USDT", "spot")
    res4 = (ok4 is False) and "non-ASCII" in reason4
    print(f"  [{'PASS' if res4 else 'FAIL'}] non_ascii_rejected ({reason4})")
    results.append(res4)

    kept, removed = partition_known_unsupported_symbols(["BTCUSDT", "FDUSDUSDT", "ETHUSDT"])
    res5 = kept == ["BTCUSDT", "ETHUSDT"] and removed == ["FDUSDUSDT"]
    print(f"  [{'PASS' if res5 else 'FAIL'}] known_unsupported_partition (kept={kept}, removed={removed})")
    results.append(res5)

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
    res6 = selected[:3] == ["BTCUSDT", "ETHUSDT", "1000PEPEUSDT"]
    res6 = res6 and metadata["survivor_count"] == 3
    res6 = res6 and metadata["rejected_pre_filter_count"] == 3
    rejected_symbols = {item["symbol"] for item in metadata["rejected_pre_filter_sample"]}
    res6 = res6 and {"币安人生USDT", "UUSDT", "FDUSDUSDT"}.issubset(rejected_symbols)
    print(
        f"  [{'PASS' if res6 else 'FAIL'}] selection_pipeline "
        f"(selected={selected[:3]}, rejected={metadata['rejected_pre_filter_sample']})"
    )
    results.append(res6)

    passed = sum(results)
    total = len(results)
    print(f"\n  {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run_tests())
