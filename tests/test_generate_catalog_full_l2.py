"""End-to-end tests for the ``full_l2`` (and ``depth_only`` / ``depth10``)
catalog-generation profiles.

These build a tiny synthetic ``data_raw`` tree (snapshot_seed + depth_updates +
trades), run ``build_replay_for_symbol`` then ``generate_catalog_from_replay``,
and assert the resulting Nautilus catalog contains readable TradeTick,
OrderBookDeltas, and OrderBookDepth10 data. They pin the wiring of the shared
depth-replay engine through the replay adapter.
"""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import _parse_iso_datetime, generate_catalog_from_replay
from validation.catalog_compare import (
    load_order_book_deltas,
    load_order_book_depth10,
)

VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
DATE = "2026-06-12"
INSTRUMENT_ID = "ADAUSDT.BINANCE"
BASE_TS_MS = 1_781_222_400_000  # 2026-06-12T00:00:00Z
WINDOW_START = _parse_iso_datetime("2026-06-12T00:00:00Z")
WINDOW_END = _parse_iso_datetime("2026-06-13T00:00:00Z")


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _sample_raw_root(tmp_path: Path) -> Path:
    """Raw tree with a snapshot_seed + two continuous depth_updates + two trades."""
    root = tmp_path / "raw"
    _write_jsonl(
        root / VENUE / "depth_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "snapshot_seed",
                "venue": VENUE,
                "symbol": SYMBOL,
                "stream_session_id": 1,
                "session_seq": 0,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 1,
                "ts_event_ms": BASE_TS_MS,
                "lastUpdateId": 100,
                "payload": {
                    "bids": [["0.1700", "100.0"], ["0.1699", "50.0"]],
                    "asks": [["0.1710", "200.0"], ["0.1711", "60.0"]],
                },
            },
            {
                "record_type": "depth_update",
                "venue": VENUE,
                "symbol": SYMBOL,
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 2_000_000_000,
                "ts_event_ms": BASE_TS_MS + 2_000,
                "U": 101,
                "u": 105,
                "pu": None,
                "payload": {
                    "bids": [["0.1700", "120.0"]],
                    "asks": [["0.1710", "180.0"]],
                },
            },
            {
                "record_type": "depth_update",
                "venue": VENUE,
                "symbol": SYMBOL,
                "stream_session_id": 1,
                "session_seq": 2,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 4_000_000_000,
                "ts_event_ms": BASE_TS_MS + 4_000,
                "U": 106,
                "u": 110,
                "pu": None,
                "payload": {
                    "bids": [["0.1698", "40.0"]],
                    "asks": [["0.1712", "70.0"]],
                },
            },
        ],
    )
    _write_jsonl(
        root / VENUE / "trade_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "trade",
                "venue": VENUE,
                "market_type": "spot",
                "symbol": SYMBOL,
                "trade_stream_session_id": 1,
                "trade_session_seq": 1,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 10,
                "ts_event_ms": BASE_TS_MS,
                "ts_trade_ms": BASE_TS_MS,
                "price": "0.17060000",
                "quantity": "35.20000000",
                "is_buyer_maker": True,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
            {
                "record_type": "trade",
                "venue": VENUE,
                "market_type": "spot",
                "symbol": SYMBOL,
                "trade_stream_session_id": 1,
                "trade_session_seq": 2,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 20,
                "ts_event_ms": BASE_TS_MS,
                "ts_trade_ms": BASE_TS_MS,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 102,
                "native_payload": {"t": 102},
            },
        ],
    )
    return root


def _build_replay(tmp_path: Path) -> Path:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root)
    return replay_root


def test_full_l2_writes_trades_deltas_and_depth10(tmp_path: Path) -> None:
    replay_root = _build_replay(tmp_path)
    output_root = tmp_path / "catalog_jobs"

    result = generate_catalog_from_replay(
        replay_root,
        output_root,
        "full",
        [SYMBOL],
        [VENUE],
        WINDOW_START,
        WINDOW_END,
        profile="full_l2",
    )

    assert result["status"] == "success"
    assert result["records_read"]["trades"] == 2
    assert result["records_written"]["trade_ticks"] == 2
    assert result["records_written"]["order_book_deltas"] > 0
    assert result["records_written"]["order_book_depth10"] >= 1

    diag = result["depth_diagnostics"]
    assert diag["snapshot_seeds"] == 1
    assert diag["raw_depth_records_read"] == 3
    assert diag["emit_depth10"] is True
    assert result["caveats"], "full_l2 must surface equivalence caveats"

    catalog_root = output_root / "job_full"
    deltas = load_order_book_deltas(catalog_root, INSTRUMENT_ID)
    assert len(deltas) > 0
    depth10 = load_order_book_depth10(catalog_root, INSTRUMENT_ID)
    assert len(depth10) >= 1


def test_depth_only_writes_no_trades(tmp_path: Path) -> None:
    replay_root = _build_replay(tmp_path)
    output_root = tmp_path / "catalog_jobs"

    result = generate_catalog_from_replay(
        replay_root,
        output_root,
        "depth",
        [SYMBOL],
        [VENUE],
        WINDOW_START,
        WINDOW_END,
        profile="depth_only",
    )

    assert result["status"] == "success"
    assert result["records_written"]["trade_ticks"] == 0
    assert result["records_written"]["order_book_deltas"] > 0


def test_depth10_profile_writes_only_depth10(tmp_path: Path) -> None:
    replay_root = _build_replay(tmp_path)
    output_root = tmp_path / "catalog_jobs"

    result = generate_catalog_from_replay(
        replay_root,
        output_root,
        "d10",
        [SYMBOL],
        [VENUE],
        WINDOW_START,
        WINDOW_END,
        profile="depth10",
    )

    assert result["status"] == "success"
    assert result["records_written"]["trade_ticks"] == 0
    assert result["records_written"]["order_book_deltas"] == 0
    assert result["records_written"]["order_book_depth10"] >= 1


def test_full_l2_manifest_records_depth_diagnostics_and_caveats(tmp_path: Path) -> None:
    replay_root = _build_replay(tmp_path)
    output_root = tmp_path / "catalog_jobs"

    generate_catalog_from_replay(
        replay_root,
        output_root,
        "manifest",
        [SYMBOL],
        [VENUE],
        WINDOW_START,
        WINDOW_END,
        profile="full_l2",
    )

    manifest = json.loads((output_root / "job_manifest" / "manifest.json").read_text())
    assert manifest["profile"] == "full_l2"
    assert manifest["record_counts"]["order_book_deltas"] > 0
    assert manifest["depth_diagnostics"]["snapshot_seeds"] == 1
    assert manifest["equivalence_caveats"], "manifest must carry equivalence caveats"
