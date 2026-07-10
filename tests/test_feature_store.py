"""Tests for feature store build and audit (validation.audit_feature_store)."""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.build_feature_store import build_features_for_symbol
from validation.audit_feature_store import audit_feature_store


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _sample_cross_day_feature_raw_root(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    date = "2026-06-12"
    venue = "BINANCE_SPOT"
    symbol = "ADAUSDT"
    day_start_ms = 1_781_222_400_000
    records = [
        ("prev", day_start_ms - 1_000, 1),
        ("in_first", day_start_ms, 2),
        ("in_third", day_start_ms + 120_000, 3),
        ("next", day_start_ms + 86_400_000, 4),
    ]
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": seq,
                "ts_recv_ns": ts_ms * 1_000_000 + seq,
                "ts_event_ms": ts_ms,
                "U": 100 + seq,
                "u": 100 + seq,
                "pu": 99 + seq,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1700", "100.0"]],
                    "asks": [["0.1710", "200.0"]],
                },
            }
            for _, ts_ms, seq in records
        ],
    )
    _write_jsonl(
        root / venue / "trade_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": seq,
                "ts_recv_ns": ts_ms * 1_000_000 + seq,
                "ts_event_ms": ts_ms,
                "ts_trade_ms": ts_ms,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 1000 + seq,
            }
            for _, ts_ms, seq in records
        ],
    )
    return root


def test_feature_build_clamps_to_utc_day_and_remains_sparse(tmp_path: Path) -> None:
    raw_root = _sample_cross_day_feature_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    feature_root = tmp_path / "features"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    result = build_features_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        ["1m"],
        replay_root,
        feature_root,
    )

    assert result["status"] == "success"
    assert result["timeframes_processed"]["1m"] == 2
    path = feature_root / "timeframe=1m" / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "2026-06-12.parquet"
    table = pq.ParquetFile(path).read()
    timestamps = table.column("timestamp_ns").to_pylist()
    day_start_ns = 1_781_222_400_000_000_000
    day_end_ns = day_start_ns + 86_400_000_000_000
    assert table.num_rows == 2
    assert all(day_start_ns <= ts < day_end_ns for ts in timestamps)
    assert timestamps == [
        day_start_ns + 60_000_000_000 - 1,
        day_start_ns + 180_000_000_000 - 1,
    ]


def test_feature_store_audit_reports_sparse_utc_day_output(tmp_path: Path) -> None:
    raw_root = _sample_cross_day_feature_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    feature_root = tmp_path / "features"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )
    build_features_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        ["1m"],
        replay_root,
        feature_root,
    )

    report = audit_feature_store(
        feature_root=feature_root,
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        timeframes=["1m"],
    )
    item = report["files"][0]
    assert item["actual_row_count"] == 2
    assert item["expected_dense_row_count"] == 1440
    assert item["outside_date_rows"] == 0
    assert item["duplicate_timestamp_count"] == 0
    assert item["missing_windows_count_if_dense"] == 1438
    assert "return_1s" in item["all_null_columns"]
