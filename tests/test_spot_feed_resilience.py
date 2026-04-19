#!/usr/bin/env python3
"""
tests/test_spot_feed_resilience.py — Prove spot feed survives one bad symbol.

Scenario:
  1. Spot universe contains one unsupported symbol.
  2. _setup_feeds() strips only the bad symbol and still adds the spot feed.
  3. Coverage summary reports requested/dropped/active symbols correctly.
  4. Futures feed still adds normally.

Usage:
    python tests/test_spot_feed_resilience.py
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class DummyFeedHandler:
    """Minimal FeedHandler stand-in for unit testing."""

    def __init__(self):
        self.feeds = []

    def add_feed(self, feed):
        self.feeds.append(feed)


class DummySpotFeed:
    """Raise if an unsupported spot symbol is present."""

    def __init__(self, *, symbols, channels, callbacks, depth_interval):
        if "UTK-USDT" in symbols:
            raise ValueError("UTK-USDT is not supported on BINANCE")
        self.symbols = list(symbols)
        self.channels = list(channels)
        self.callbacks = callbacks
        self.depth_interval = depth_interval


class DummyFuturesFeed:
    """Accept all futures symbols."""

    def __init__(self, *, symbols, channels, callbacks):
        self.symbols = list(symbols)
        self.channels = list(channels)
        self.callbacks = callbacks


def run_tests() -> int:
    import recorder

    original_feed_handler = recorder.FeedHandler
    original_spot = recorder.Binance
    original_futures = recorder.BinanceFutures
    original_futures_enabled = recorder.futures_enabled
    original_futures_disabled_reason = recorder.futures_disabled_reason

    try:
        recorder.FeedHandler = DummyFeedHandler
        recorder.Binance = DummySpotFeed
        recorder.BinanceFutures = DummyFuturesFeed
        recorder.futures_enabled = True
        recorder.futures_disabled_reason = ""

        universe = {
            "BINANCE_SPOT": ["BTCUSDT", "UTKUSDT", "ETHUSDT"],
            "BINANCE_USDTF": ["BTCUSDT", "ETHUSDT"],
        }
        fh, coverage = recorder._setup_feeds(universe)

        ok1 = len(fh.feeds) == 2
        print(f"  [{'PASS' if ok1 else 'FAIL'}] both_feeds_added (count={len(fh.feeds)})")

        spot_feed = fh.feeds[0] if fh.feeds else None
        ok2 = spot_feed is not None and spot_feed.symbols == ["BTC-USDT", "ETH-USDT"]
        print(
            f"  [{'PASS' if ok2 else 'FAIL'}] bad_spot_symbol_stripped "
            f"(spot_symbols={getattr(spot_feed, 'symbols', None)})"
        )

        spot_cov = coverage["spot"]
        ok3 = (
            spot_cov["requested_count"] == 3
            and spot_cov["dropped_raw"] == ["UTKUSDT"]
            and spot_cov["active_raw"] == ["BTCUSDT", "ETHUSDT"]
        )
        print(
            f"  [{'PASS' if ok3 else 'FAIL'}] spot_coverage_summary "
            f"(coverage={spot_cov})"
        )

        fut_feed = fh.feeds[1] if len(fh.feeds) > 1 else None
        ok4 = fut_feed is not None and fut_feed.symbols == ["BTC-USDT-PERP", "ETH-USDT-PERP"]
        print(
            f"  [{'PASS' if ok4 else 'FAIL'}] futures_feed_untouched "
            f"(futures_symbols={getattr(fut_feed, 'symbols', None)})"
        )

        fut_cov = coverage["futures"]
        ok5 = (
            fut_cov["requested_count"] == 2
            and fut_cov["dropped_raw"] == []
            and fut_cov["active_raw"] == ["BTCUSDT", "ETHUSDT"]
        )
        print(
            f"  [{'PASS' if ok5 else 'FAIL'}] futures_coverage_summary "
            f"(coverage={fut_cov})"
        )

        ok6 = recorder.futures_enabled is True and recorder.futures_disabled_reason == ""
        print(
            f"  [{'PASS' if ok6 else 'FAIL'}] futures_status_unchanged "
            f"(enabled={recorder.futures_enabled}, reason={recorder.futures_disabled_reason!r})"
        )

        results = [ok1, ok2, ok3, ok4, ok5, ok6]
        passed = sum(results)
        total = len(results)
        print(f"\n  {passed}/{total} passed")
        return 0 if passed == total else 1
    finally:
        recorder.FeedHandler = original_feed_handler
        recorder.Binance = original_spot
        recorder.BinanceFutures = original_futures
        recorder.futures_enabled = original_futures_enabled
        recorder.futures_disabled_reason = original_futures_disabled_reason


if __name__ == "__main__":
    raise SystemExit(run_tests())
