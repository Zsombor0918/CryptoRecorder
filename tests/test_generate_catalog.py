"""Tests for catalog generation (pipeline.generate_catalog)."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import (
    _date_range_from_window,
    _parse_iso_datetime,
    _window_from_date,
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


def test_generate_catalog_date_range_uses_exclusive_end() -> None:
    dates = _date_range_from_window(
        _parse_iso_datetime("2026-06-12T00:00:00Z"),
        _parse_iso_datetime("2026-06-13T00:00:00Z"),
    )
    assert dates == ["2026-06-12"]
    start, end = _window_from_date("2026-06-12")
    assert start == _parse_iso_datetime("2026-06-12T00:00:00Z")
    assert end == _parse_iso_datetime("2026-06-13T00:00:00Z")


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
    assert result["time_filter"] == "ts_init"
    assert result["records_read"]["trades"] == 2
    assert result["found_partitions"] == [
        {"venue": "BINANCE_SPOT", "symbol": "ADAUSDT", "date": "2026-06-12"}
    ]
    assert result["missing_partitions"] == []
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
    assert str(ticks[0].price) == "0.17060000"
    assert str(ticks[0].size) == "35.20000000"


def test_generate_catalog_cli_date_shortcut(tmp_path: Path) -> None:
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

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline.generate_catalog",
            "--input",
            str(replay_root),
            "--symbols",
            "ADAUSDT",
            "--venues",
            "BINANCE_SPOT",
            "--date",
            "2026-06-12",
            "--output",
            str(output_root),
            "--job-id",
            "date_shortcut",
            "--overwrite",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((output_root / "job_date_shortcut" / "manifest.json").read_text())
    assert manifest["time_window"]["start"] == "2026-06-12T00:00:00+00:00"
    assert manifest["time_window"]["end"] == "2026-06-13T00:00:00+00:00"
    assert manifest["time_filter"] == "ts_init"
    assert manifest["records_read"]["trades"] == 2
    assert manifest["record_counts"]["trade_ticks"] == 2


def test_generate_catalog_fixed_job_id_and_overwrite(tmp_path: Path) -> None:
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
    kwargs = dict(
        replay_root=replay_root,
        catalog_root=output_root,
        job_id="validation_new",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        start=_parse_iso_datetime("2026-06-12T00:00:00Z"),
        end=_parse_iso_datetime("2026-06-13T00:00:00Z"),
        profile="trades_only",
    )

    first = generate_catalog_from_replay(**kwargs)
    second = generate_catalog_from_replay(**kwargs)
    third = generate_catalog_from_replay(**kwargs, overwrite=True)

    assert first["status"] == "success"
    assert (output_root / "job_validation_new").exists()
    assert second["status"] == "failed"
    assert "already exists" in second["errors"][0]
    assert third["status"] == "success"


def test_generate_catalog_help_lists_only_implemented_profiles() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.generate_catalog", "--help"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "trades_only" in result.stdout
    assert "full_l2" not in result.stdout
    assert "depth_only" not in result.stdout
