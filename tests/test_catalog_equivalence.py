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


def test_validator_skips_unsupported_profiles(tmp_path: Path) -> None:
    # The validator supports trades_only + full_l2; other profiles short-circuit
    # to status=skipped (depth_only / depth10 have no convert_day reference).
    report = validate_catalog_equivalence(
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        data_root=tmp_path / "raw",
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old",
        replay_root=tmp_path / "replay",
        new_catalog_root=tmp_path / "new",
        profile="depth10",
        overwrite=True,
    )
    assert report["status"] == "skipped"


def _write_clean_raw_day(data_root: Path) -> None:
    """A single-session bootstrap day (snapshot + continuous updates + trades).

    No cross-day carry, clock skew, sync_state, or duplicates — the regime where
    convert_day.py and the replay full_l2 path share the same engine output.
    """
    venue, symbol, date = "BINANCE_SPOT", "ADAUSDT", "2026-06-12"
    base = 1_781_222_400_000

    def _jsonl(path: Path, records: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

    _jsonl(
        data_root / venue / "depth_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "snapshot_seed", "venue": venue, "symbol": symbol,
                "stream_session_id": 1, "session_seq": 0,
                "ts_recv_ns": base * 1_000_000 + 1, "ts_event_ms": base,
                "lastUpdateId": 100,
                "payload": {
                    "bids": [["0.1700", "100.0"], ["0.1699", "50.0"]],
                    "asks": [["0.1710", "200.0"], ["0.1711", "60.0"]],
                },
            },
            {
                "record_type": "depth_update", "venue": venue, "symbol": symbol,
                "stream_session_id": 1, "session_seq": 1,
                "ts_recv_ns": base * 1_000_000 + 2_000_000_000,
                "ts_event_ms": base + 2_000,
                "U": 101, "u": 105, "pu": None,
                "payload": {"bids": [["0.1700", "120.0"]], "asks": [["0.1710", "180.0"]]},
            },
            {
                "record_type": "depth_update", "venue": venue, "symbol": symbol,
                "stream_session_id": 1, "session_seq": 2,
                "ts_recv_ns": base * 1_000_000 + 4_000_000_000,
                "ts_event_ms": base + 4_000,
                "U": 106, "u": 110, "pu": None,
                "payload": {"bids": [["0.1698", "40.0"]], "asks": [["0.1712", "70.0"]]},
            },
        ],
    )
    _jsonl(
        data_root / venue / "trade_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "trade", "venue": venue, "market_type": "spot",
                "symbol": symbol, "trade_stream_session_id": 1, "trade_session_seq": 1,
                "ts_recv_ns": base * 1_000_000 + 10, "ts_event_ms": base, "ts_trade_ms": base,
                "price": "0.17060000", "quantity": "35.20000000",
                "is_buyer_maker": True, "exchange_trade_id": 101, "native_payload": {"t": 101},
            },
            {
                "record_type": "trade", "venue": venue, "market_type": "spot",
                "symbol": symbol, "trade_stream_session_id": 1, "trade_session_seq": 2,
                "ts_recv_ns": base * 1_000_000 + 20, "ts_event_ms": base, "ts_trade_ms": base,
                "price": "0.17070000", "quantity": "30.90000000",
                "is_buyer_maker": False, "exchange_trade_id": 102, "native_payload": {"t": 102},
            },
        ],
    )
    # The canonical validation path is fail-closed for depth event-time
    # repartitioning. A later-hour path proves the next day's T00 file is
    # rotated/closed; both are empty because this fixture has no skewed tail.
    _jsonl(
        data_root / venue / "depth_v2" / symbol / "2026-06-13" / "2026-06-13T00.jsonl",
        [],
    )
    _jsonl(
        data_root / venue / "depth_v2" / symbol / "2026-06-13" / "2026-06-13T23.jsonl",
        [],
    )
    _jsonl(
        data_root / venue / "depth_v2" / symbol / "2026-06-14" / "2026-06-14T00.jsonl",
        [],
    )


def test_full_l2_validator_matches_convert_day_on_clean_synthetic_day(tmp_path: Path) -> None:
    """End-to-end gate: convert_day.py vs replay full_l2 on a clean bootstrap day.

    full_l2 is no longer deferred — the validator must run (not skip) and, on a
    clean single-session day, report semantic equivalence between the validated
    convert_day catalog and the replay-generated catalog.
    """
    data_root = tmp_path / "data_raw"
    _write_clean_raw_day(data_root)

    report = validate_catalog_equivalence(
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        data_root=data_root,
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old_catalog",
        replay_root=tmp_path / "replay_store",
        new_catalog_root=tmp_path / "new_catalog",
        profile="full_l2",
        overwrite=True,
    )

    assert report["status"] != "skipped", json.dumps(report, indent=2, default=str)
    assert report["profile"] == "full_l2"
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
    comparison = report["comparison"]
    assert comparison["trade_ticks"]["passed"] is True
    assert comparison["order_book_deltas"]["passed"] is True
    assert comparison["book_checkpoints"]["passed"] is True


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
