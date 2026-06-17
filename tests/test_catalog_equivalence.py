"""Tests for catalog equivalence checking (validation.validate_catalog_equivalence)."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from converter.instruments import build_instruments
from validation.catalog_compare import compare_trade_ticks_semantic, load_trade_ticks
from validation.validate_catalog_equivalence import validate_catalog_equivalence


def _write_ticks_catalog(catalog_root: Path, ticks: list[TradeTick]) -> None:
    instrument = build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]
    catalog = ParquetDataCatalog(str(catalog_root))
    catalog.write_data([instrument])
    catalog.write_data(ticks)


def test_trades_only_semantic_comparison_with_synthetic_catalogs(tmp_path: Path) -> None:
    instrument = build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]
    ticks = [
        TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("0.17060000"),
            size=Quantity.from_str("35.20000000"),
            aggressor_side=AggressorSide.SELLER,
            trade_id=TradeId("101"),
            ts_event=1_781_222_400_001_000_000,
            ts_init=1_781_222_400_000_000_010,
        ),
        TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("0.17070000"),
            size=Quantity.from_str("30.90000000"),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId("102"),
            ts_event=1_781_222_400_000_000_000,
            ts_init=1_781_222_400_000_000_020,
        ),
    ]
    old_root = tmp_path / "old_catalog"
    new_root = tmp_path / "new_catalog"
    _write_ticks_catalog(old_root, ticks)
    _write_ticks_catalog(new_root, list(ticks))

    old_ticks = load_trade_ticks(old_root, "ADAUSDT.BINANCE")
    new_ticks = load_trade_ticks(new_root, "ADAUSDT.BINANCE")
    comparison = compare_trade_ticks_semantic(old_ticks, new_ticks)

    assert comparison["passed"] is True
    assert comparison["trade_count_match"] is True
    assert comparison["timestamp_range_match"] is True
    assert comparison["sample_mismatches"] == []


def test_full_l2_validation_is_explicitly_deferred(tmp_path: Path) -> None:
    # generate_catalog --profile full_l2 is deferred; this test must remain
    # skipped (status=skipped) until full_l2 is implemented.
    report = validate_catalog_equivalence(
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        data_root=tmp_path / "raw",
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old",
        replay_root=tmp_path / "replay",
        new_catalog_root=tmp_path / "new",
        profile="full_l2",
        overwrite=True,
    )
    assert report["status"] == "skipped"
    assert "deferred" in report["notes"][0]


@pytest.mark.realdata
def test_real_data_catalog_equivalence_when_enabled(tmp_path: Path) -> None:
    real_root = os.environ.get("CRYPTO_RECORDER_REAL_DATA_ROOT")
    if not real_root:
        pytest.skip("Set CRYPTO_RECORDER_REAL_DATA_ROOT to run real-data equivalence")
    report = validate_catalog_equivalence(
        date=os.environ.get("CRYPTO_RECORDER_REAL_DATA_DATE", "2026-06-12"),
        symbols=[os.environ.get("CRYPTO_RECORDER_REAL_DATA_SYMBOL", "ADAUSDT")],
        venues=[os.environ.get("CRYPTO_RECORDER_REAL_DATA_VENUE", "BINANCE_SPOT")],
        data_root=Path(real_root),
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old_catalog",
        replay_root=tmp_path / "replay_store",
        new_catalog_root=tmp_path / "new_catalog",
        profile="trades_only",
        overwrite=True,
    )
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
