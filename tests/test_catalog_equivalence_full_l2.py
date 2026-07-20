"""Semantic-equivalence tests for the ``full_l2`` comparators in
``validation.catalog_compare``, exercised against real generated catalogs.

Strategy: generate a ``full_l2`` catalog twice from the same replay store and
assert the OrderBookDeltas / OrderBookDepth10 / reconstructed-book comparators
report equivalence (deterministic generation). Then generate from a *different*
book and assert the comparators detect the divergence. This pins the
comparators that ``validate_catalog_equivalence`` relies on to gate v2.0.0.
"""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.build_replay_store import build_replay_for_symbol
from validation.replay_catalog_reconstruct import _parse_iso_datetime, generate_catalog_from_replay
from validation.catalog_compare import (
    compare_book_checkpoints,
    compare_depth10_semantic,
    compare_order_book_deltas_semantic,
    load_order_book_deltas,
    load_order_book_depth10,
)

VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
DATE = "2026-06-12"
INSTRUMENT_ID = "ADAUSDT.BINANCE"
BASE_TS_MS = 1_781_222_400_000  # 2026-06-12T00:00:00Z
START_NS = 1_781_222_400_000_000_000
END_NS = START_NS + 86_400_000_000_000
WINDOW_START = _parse_iso_datetime("2026-06-12T00:00:00Z")
WINDOW_END = _parse_iso_datetime("2026-06-13T00:00:00Z")


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _raw_root(tmp_path: Path, *, top_bid_size: str) -> Path:
    root = tmp_path / "raw"
    _write_jsonl(
        root / VENUE / "depth_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "snapshot_seed",
                "venue": VENUE, "symbol": SYMBOL,
                "stream_session_id": 1, "session_seq": 0,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 1,
                "ts_event_ms": BASE_TS_MS,
                "lastUpdateId": 100,
                "payload": {
                    "bids": [["0.1700", top_bid_size], ["0.1699", "50.0"]],
                    "asks": [["0.1710", "200.0"], ["0.1711", "60.0"]],
                },
            },
            {
                "record_type": "depth_update",
                "venue": VENUE, "symbol": SYMBOL,
                "stream_session_id": 1, "session_seq": 1,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 2_000_000_000,
                "ts_event_ms": BASE_TS_MS + 2_000,
                "U": 101, "u": 105, "pu": None,
                "payload": {"bids": [["0.1700", "120.0"]], "asks": [["0.1710", "180.0"]]},
            },
            {
                "record_type": "depth_update",
                "venue": VENUE, "symbol": SYMBOL,
                "stream_session_id": 1, "session_seq": 2,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 4_000_000_000,
                "ts_event_ms": BASE_TS_MS + 4_000,
                "U": 106, "u": 110, "pu": None,
                "payload": {"bids": [["0.1698", "40.0"]], "asks": [["0.1712", "70.0"]]},
            },
        ],
    )
    _write_jsonl(
        root / VENUE / "trade_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "trade",
                "venue": VENUE, "market_type": "spot", "symbol": SYMBOL,
                "trade_stream_session_id": 1, "trade_session_seq": 1,
                "ts_recv_ns": BASE_TS_MS * 1_000_000 + 10,
                "ts_event_ms": BASE_TS_MS, "ts_trade_ms": BASE_TS_MS,
                "price": "0.17060000", "quantity": "35.20000000",
                "is_buyer_maker": True, "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )
    return root


def _generate(tmp_path: Path, job_id: str, *, top_bid_size: str) -> Path:
    raw_root = _raw_root(tmp_path / job_id, top_bid_size=top_bid_size)
    replay_root = tmp_path / job_id / "replay"
    build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root)
    output_root = tmp_path / job_id / "catalog"
    generate_catalog_from_replay(
        replay_root, output_root, job_id, [SYMBOL], [VENUE],
        WINDOW_START, WINDOW_END, profile="full_l2",
    )
    return output_root / f"job_{job_id}"


def test_identical_full_l2_catalogs_are_semantically_equal(tmp_path: Path) -> None:
    cat_a = _generate(tmp_path, "a", top_bid_size="100.0")
    cat_b = _generate(tmp_path, "b", top_bid_size="100.0")

    deltas_a = load_order_book_deltas(cat_a, INSTRUMENT_ID)
    deltas_b = load_order_book_deltas(cat_b, INSTRUMENT_ID)
    assert len(deltas_a) > 0
    deltas_cmp = compare_order_book_deltas_semantic(deltas_a, deltas_b)
    assert deltas_cmp["passed"], deltas_cmp

    depth_a = load_order_book_depth10(cat_a, INSTRUMENT_ID)
    depth_b = load_order_book_depth10(cat_b, INSTRUMENT_ID)
    depth_cmp = compare_depth10_semantic(depth_a, depth_b)
    assert depth_cmp["passed"], depth_cmp

    ckpt = compare_book_checkpoints(deltas_a, deltas_b, START_NS, END_NS)
    assert ckpt["passed"], ckpt
    assert ckpt["any_crossed_new"] is False


def test_divergent_books_are_detected(tmp_path: Path) -> None:
    cat_a = _generate(tmp_path, "a", top_bid_size="100.0")
    cat_b = _generate(tmp_path, "b", top_bid_size="999.0")

    deltas_a = load_order_book_deltas(cat_a, INSTRUMENT_ID)
    deltas_b = load_order_book_deltas(cat_b, INSTRUMENT_ID)
    deltas_cmp = compare_order_book_deltas_semantic(deltas_a, deltas_b)
    ckpt = compare_book_checkpoints(deltas_a, deltas_b, START_NS, END_NS)
    # Either the multiset of deltas or the reconstructed top-of-book must diverge.
    assert not (deltas_cmp["passed"] and ckpt["passed"]), (deltas_cmp, ckpt)
