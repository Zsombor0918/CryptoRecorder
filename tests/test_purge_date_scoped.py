from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from converter.catalog import purge_catalog_date_range


def _make_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1_FAKE")


def _build_fake_catalog(tmpdir: Path) -> Path:
    cat = tmpdir / "catalog"

    for date_str in ("2026-04-17", "2026-04-18"):
        filename = f"{date_str}T00-00-00-000000000Z_{date_str}T23-59-59-000000000Z.parquet"
        _make_parquet(cat / "data" / "trade_tick" / "BTCUSDT.BINANCE" / filename)
        _make_parquet(cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE" / filename)

    fn17 = "2026-04-17T00-00-00-000000000Z_2026-04-17T23-59-59-000000000Z.parquet"
    _make_parquet(cat / "data" / "trade_tick" / "ETHUSDT.BINANCE" / fn17)

    meta_fn = "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
    _make_parquet(cat / "data" / "currency_pair" / "BTCUSDT.BINANCE" / meta_fn)
    _make_parquet(cat / "data" / "currency_pair" / "ETHUSDT.BINANCE" / meta_fn)

    return cat


class FakeIID:
    def __init__(self, value: str):
        self._value = value

    def __str__(self) -> str:
        return self._value


def test_catalog_purge_is_date_scoped() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="purge_test_"))
    try:
        cat = _build_fake_catalog(tmpdir)

        btc_trades = list((cat / "data" / "trade_tick" / "BTCUSDT.BINANCE").glob("*.parquet"))
        btc_depth = list((cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE").glob("*.parquet"))
        assert len(btc_trades) == 2
        assert len(btc_depth) == 2

        purged = purge_catalog_date_range(
            cat,
            [FakeIID("BTCUSDT.BINANCE")],
            "2026-04-17",
        )
        assert purged >= 2

        remaining_trades = list((cat / "data" / "trade_tick" / "BTCUSDT.BINANCE").glob("*.parquet"))
        remaining_depth = list((cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE").glob("*.parquet"))
        day18_names = [path.name for path in remaining_trades + remaining_depth]

        assert all("2026-04-18" in name for name in day18_names)
        assert len(remaining_trades) == 1
        assert len(remaining_depth) == 1

        btc_meta = list((cat / "data" / "currency_pair" / "BTCUSDT.BINANCE").glob("*.parquet"))
        assert len(btc_meta) == 0

        eth_trades = list((cat / "data" / "trade_tick" / "ETHUSDT.BINANCE").glob("*.parquet"))
        eth_meta = list((cat / "data" / "currency_pair" / "ETHUSDT.BINANCE").glob("*.parquet"))
        assert len(eth_trades) == 1
        assert len(eth_meta) == 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
