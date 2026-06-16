from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from converter.readers import stream_raw_records
from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import (
    _date_range_from_window,
    _parse_iso_datetime,
    generate_catalog_from_replay,
)


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
                "price": "0.1707",
                "quantity": "30.9",
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
                "price": "0.1706",
                "quantity": "35.2",
                "is_buyer_maker": True,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )
    return root


def test_pipeline_cli_help_does_not_touch_default_data_roots() -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("CRYPTO_RECORDER_")
    }
    for module in (
        "pipeline.build_replay_store",
        "pipeline.build_feature_store",
        "pipeline.generate_catalog",
        "pipeline.daily_build",
    ):
        result = subprocess.run(
            [sys.executable, "-m", module, "--help"],
            cwd=Path(__file__).resolve().parent.parent,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


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
    depths = pq.ParquetFile(partition / "depth.parquet").read().to_pylist()
    assert [row["session_seq"] for row in depths] == [1, 2]


def test_generate_catalog_date_range_uses_exclusive_end() -> None:
    dates = _date_range_from_window(
        _parse_iso_datetime("2026-06-12T00:00:00Z"),
        _parse_iso_datetime("2026-06-13T00:00:00Z"),
    )
    assert dates == ["2026-06-12"]


def test_generate_catalog_from_replay_writes_readable_trades_catalog(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    output_root = tmp_path / "catalog_jobs"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    result = generate_catalog_from_replay(
        replay_root,
        output_root,
        "smoke",
        ["ADAUSDT"],
        ["BINANCE_SPOT"],
        _parse_iso_datetime("2026-06-12T00:00:00Z"),
        _parse_iso_datetime("2026-06-13T00:00:00Z"),
        profile="trades_only",
    )

    assert result["status"] == "success"
    assert result["records_written"]["trade_ticks"] == 2
    catalog_root = output_root / "job_smoke"
    catalog = ParquetDataCatalog(str(catalog_root))
    instruments = catalog.instruments()
    assert [str(instrument.id) for instrument in instruments] == ["ADAUSDT.BINANCE"]
    ticks = catalog.trade_ticks(
        instrument_ids=["ADAUSDT.BINANCE"],
        start=1_781_222_400_000_000_000,
        end=1_781_222_400_001_000_000,
    )
    assert len(ticks) == 2
