"""Tests for replay store build and audit (validation.audit_replay_store)."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.build_replay_store import build_replay_for_symbol
from validation.audit_replay_store import audit_replay_store
from converter.readers import stream_raw_records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _sample_raw_root(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    date = "2026-06-12"
    venue = "BINANCE_SPOT"
    symbol = "ADAUSDT"
    base_ts_ms = 1_781_222_400_000
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 2,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 2,
                "ts_event_ms": base_ts_ms,
                "U": 11,
                "u": 12,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1700", "100.0"]],
                    "asks": [["0.1710", "200.0"]],
                },
            },
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 1,
                "ts_event_ms": base_ts_ms + 1,
                "U": 9,
                "u": 10,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1690", "150.0"]],
                    "asks": [["0.1720", "250.0"]],
                },
            },
        ],
    )
    _write_jsonl(
        root / venue / "trade_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "heartbeat",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 0,
                "ts_recv_ns": base_ts_ms * 1_000_000,
                "ts_event_ms": base_ts_ms,
            },
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 2,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 20,
                "ts_event_ms": base_ts_ms,
                "ts_trade_ms": base_ts_ms,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 102,
                "native_payload": {"t": 102},
            },
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 10,
                "ts_event_ms": base_ts_ms + 1,
                "ts_trade_ms": base_ts_ms + 1,
                "price": "0.17060000",
                "quantity": "35.20000000",
                "is_buyer_maker": True,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )
    return root


def test_stream_raw_records_uses_custom_root(tmp_path: Path) -> None:
    custom_root = tmp_path / "custom_raw"
    _write_jsonl(
        custom_root / "BINANCE_SPOT" / "trade_v2" / "ADAUSDT" / "2026-06-12" / "x.jsonl",
        [{"ok": True}],
    )

    records = list(
        stream_raw_records(
            "BINANCE_SPOT",
            "ADAUSDT",
            "trade_v2",
            "2026-06-12",
            root=custom_root,
        )
    )

    assert records == [{"ok": True}]


def test_replay_build_writes_sorted_manifested_partition(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"

    result = build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    assert result["status"] == "success"
    assert result["depth_count"] == 2
    assert result["trade_count"] == 2
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "date=2026-06-12"
    manifest = json.loads((partition / "manifest.json").read_text())
    assert manifest["depth_record_count"] == 2
    assert manifest["trade_record_count"] == 2
    assert manifest["depth_checksum"]
    assert manifest["trades_checksum"]

    trades = pq.ParquetFile(partition / "trades.parquet").read().to_pylist()
    assert [row["trade_session_seq"] for row in trades] == [1, 2]
    assert [row["price_str"] for row in trades] == ["0.17060000", "0.17070000"]
    assert [row["quantity_str"] for row in trades] == ["35.20000000", "30.90000000"]
    depths = pq.ParquetFile(partition / "depth.parquet").read().to_pylist()
    assert [row["session_seq"] for row in depths] == [1, 2]
    assert depths[0]["U"] == "9"
    assert depths[0]["bids"][0]["price_str"] == "0.1690"
    assert depths[0]["bids"][0]["size_str"] == "150.0"


def test_replay_store_audit_reports_counts_checksums_and_ordering(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    report = audit_replay_store(
        replay_root=replay_root,
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
    )

    assert report["missing_partitions"] == []
    partition = report["partitions"][0]
    assert partition["instrument_exists"] is False
    assert partition["manifest_count_match"] == {"depth": True, "trades": True}
    assert partition["checksum_match"] == {"depth": True, "trades": True}
    assert partition["depth"]["sorted"] is True
    assert partition["trades"]["sorted"] is True
    assert partition["depth"]["level_exact_fields_present"] is True
    assert partition["depth"]["null_ratio"]["U"] == 0
    assert partition["trades"]["null_ratio"]["price_str"] == 0
    assert partition["trades"]["null_ratio"]["quantity_str"] == 0
