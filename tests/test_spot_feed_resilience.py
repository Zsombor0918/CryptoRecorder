from __future__ import annotations


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


def test_spot_feed_survives_one_bad_symbol() -> None:
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

        assert len(fh.feeds) == 2

        spot_feed = fh.feeds[0]
        assert spot_feed.symbols == ["BTC-USDT", "ETH-USDT"]

        spot_cov = coverage["spot"]
        assert spot_cov["requested_count"] == 3
        assert spot_cov["selected_count"] == 3
        assert spot_cov["dropped_raw"] == ["UTKUSDT"]
        assert spot_cov["runtime_dropped_raw"] == ["UTKUSDT"]
        assert spot_cov["runtime_dropped_count"] == 1
        assert spot_cov["active_raw"] == ["BTCUSDT", "ETHUSDT"]

        fut_feed = fh.feeds[1]
        assert fut_feed.symbols == ["BTC-USDT-PERP", "ETH-USDT-PERP"]

        fut_cov = coverage["futures"]
        assert fut_cov["requested_count"] == 2
        assert fut_cov["selected_count"] == 2
        assert fut_cov["dropped_raw"] == []
        assert fut_cov["runtime_dropped_count"] == 0
        assert fut_cov["active_raw"] == ["BTCUSDT", "ETHUSDT"]

        assert recorder.futures_enabled is True
        assert recorder.futures_disabled_reason == ""
    finally:
        recorder.FeedHandler = original_feed_handler
        recorder.Binance = original_spot
        recorder.BinanceFutures = original_futures
        recorder.futures_enabled = original_futures_enabled
        recorder.futures_disabled_reason = original_futures_disabled_reason
