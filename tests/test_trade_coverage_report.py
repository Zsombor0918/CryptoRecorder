from __future__ import annotations

import json
from pathlib import Path

import converter.readers as readers_mod
from validators.trade_coverage import build_readiness_summary, summarize_trade_coverage
import validators.trade_coverage as trade_coverage_mod


def _write_raw_records(tmp_path: Path, venue: str, symbol: str, date_str: str, records: list[dict]) -> None:
    day_dir = tmp_path / venue / "trade_v2" / symbol / date_str
    day_dir.mkdir(parents=True, exist_ok=True)
    with (day_dir / "part-000.jsonl").open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


def test_trade_coverage_report_distinguishes_lifecycle_only_and_trade_symbols(monkeypatch, tmp_path: Path) -> None:
    date_str = "2026-04-25"
    monkeypatch.setattr(readers_mod, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(trade_coverage_mod, "DATA_ROOT", tmp_path)

    _write_raw_records(
        tmp_path,
        "BINANCE_USDTF",
        "BTCUSDT",
        date_str,
        [
            {
                "record_type": "trade_stream_lifecycle",
                "reason": "startup_or_reconnect",
            },
            {
                "record_type": "trade_stream_lifecycle",
                "reason": "websocket_closed",
            },
        ],
    )
    _write_raw_records(
        tmp_path,
        "BINANCE_USDTF",
        "ETHUSDT",
        date_str,
        [
            {
                "record_type": "trade_stream_lifecycle",
                "reason": "startup_or_reconnect",
            },
            {
                "record_type": "trade",
                "ts_trade_ms": 1710000000000,
                "price": "1.0",
                "quantity": "2.0",
            },
        ],
    )

    report = summarize_trade_coverage(
        date_str,
        universe={"BINANCE_USDTF": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]},
    )

    assert report["per_symbol"]["BINANCE_USDTF/BTCUSDT"]["will_create_tradetick"] is False
    assert report["per_symbol"]["BINANCE_USDTF/ETHUSDT"]["will_create_tradetick"] is True
    assert report["venues"]["BINANCE_USDTF"]["symbols_with_trades"] == ["ETHUSDT"]
    assert report["venues"]["BINANCE_USDTF"]["symbols_without_trades"] == ["BTCUSDT", "SOLUSDT"]
    assert report["venues"]["BINANCE_USDTF"]["lifecycle_only_symbols"] == ["BTCUSDT"]


def test_readiness_summary_classifies_honestly() -> None:
    readiness = build_readiness_summary(
        {
            "BINANCE_USDTF/BTCUSDT": {
                "raw_trade_record_count": 0,
                "raw_lifecycle_record_count": 2,
                "ticks_written": 0,
            },
            "BINANCE_USDTF/ETHUSDT": {
                "raw_trade_record_count": 3,
                "raw_lifecycle_record_count": 1,
                "ticks_written": 3,
            },
        },
        {
            "BINANCE_USDTF/BTCUSDT": {"deltas_written": 10, "depth10_written": 2},
            "BINANCE_USDTF/ETHUSDT": {"deltas_written": 10, "depth10_written": 2},
            "BINANCE_USDTF/SOLUSDT": {"deltas_written": 0, "depth10_written": 0},
        },
        min_trade_records_for_full_ready=1,
    )

    assert readiness["per_symbol"]["BINANCE_USDTF/BTCUSDT"]["readiness"] == "l2_ready"
    assert readiness["per_symbol"]["BINANCE_USDTF/BTCUSDT"]["lifecycle_only"] is True
    assert readiness["per_symbol"]["BINANCE_USDTF/ETHUSDT"]["readiness"] == "full_ready"
    assert readiness["per_symbol"]["BINANCE_USDTF/SOLUSDT"]["readiness"] == "not_ready"
    assert readiness["suggested_full_ready_universe"]["BINANCE_USDTF"] == ["ETHUSDT"]