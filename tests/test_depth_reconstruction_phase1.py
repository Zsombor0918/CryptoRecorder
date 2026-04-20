from __future__ import annotations

from typing import Iterable, List

from nautilus_trader.model.identifiers import InstrumentId

import converter.book as book_mod


def _rec(
    *,
    ts_recv_ns: int,
    bids: List[List[str]] | None = None,
    asks: List[List[str]] | None = None,
    sequence_number: int = 0,
) -> dict:
    return {
        "ts_recv_ns": ts_recv_ns,
        "ts_event_ms": ts_recv_ns // 1_000_000,
        "sequence_number": sequence_number,
        "payload": {
            "bids": bids or [],
            "asks": asks or [],
        },
    }


def _active_prices(orders) -> List[float]:
    out: List[float] = []
    for order in orders:
        size = float(str(order.size))
        if size > 0:
            out.append(float(str(order.price)))
    return out


def test_stable_ts_init_sort() -> None:
    ordered = book_mod._sorted_depth_records(
        [
            _rec(ts_recv_ns=2_000_000_000, sequence_number=2),
            _rec(ts_recv_ns=1_000_000_000, sequence_number=1),
            _rec(ts_recv_ns=1_000_000_000, sequence_number=3),
        ]
    )
    ordered_seq = [rec["sequence_number"] for _, rec in ordered]
    assert ordered_seq == [1, 3, 2]


def test_zero_size_deletes_level_before_snapshot() -> None:
    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    book = book_mod.BookReconstructor(iid, price_prec=1, size_prec=3)
    book.apply_delta(
        _rec(
            ts_recv_ns=1_000_000_000,
            bids=[["100", "1"], ["99", "2"]],
            asks=[["101", "3"], ["102", "4"]],
        ),
        1_000_000_000,
    )
    book.apply_delta(
        _rec(
            ts_recv_ns=2_000_000_000,
            bids=[["100", "0"]],
            asks=[["102", "0"], ["101.5", "5"]],
        ),
        2_000_000_000,
    )
    snap = book.snapshot(2_000_000_000, 2_000_000_000)
    bid_prices = _active_prices(snap.bids) if snap else []
    ask_prices = _active_prices(snap.asks) if snap else []

    assert bid_prices == [99.0]
    assert ask_prices[:2] == [101.0, 101.5]
    assert 100.0 not in bid_prices
    assert 102.0 not in ask_prices


def test_crossed_book_reset_emits_no_crossed_snapshot() -> None:
    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    original_stream = book_mod.stream_raw_records

    try:
        def _fake_stream(*args, **kwargs) -> Iterable[dict]:
            yield _rec(
                ts_recv_ns=2_000_000_000,
                asks=[["101", "0"], ["102", "1"]],
                sequence_number=2,
            )
            yield _rec(
                ts_recv_ns=1_000_000_000,
                bids=[["100", "1"]],
                asks=[["101", "1"]],
                sequence_number=1,
            )
            yield _rec(
                ts_recv_ns=3_000_000_000,
                bids=[["102.5", "1"]],
                sequence_number=3,
            )
            yield _rec(
                ts_recv_ns=4_000_000_000,
                bids=[["99", "1"]],
                asks=[["103", "1"]],
                sequence_number=4,
            )

        book_mod.stream_raw_records = _fake_stream
        snapshots, bad, gaps, resets, crossed, examples, _, _ = book_mod.convert_depth(
            "BINANCE_USDTF",
            "BTCUSDT",
            "2026-04-18",
            iid,
            1,
            3,
            snapshot_interval_sec=0.0,
        )
    finally:
        book_mod.stream_raw_records = original_stream

    snapshot_pairs = [
        (_active_prices(snap.bids)[0], _active_prices(snap.asks)[0], snap.ts_init)
        for snap in snapshots
    ]

    assert bad == 0
    assert gaps == 0
    assert resets == 1
    assert crossed == 1
    assert snapshot_pairs == [
        (100.0, 101.0, 1_000_000_000),
        (100.0, 102.0, 2_000_000_000),
        (99.0, 103.0, 4_000_000_000),
    ]
    assert len(examples) == 1
    assert examples[0]["previous_snapshot"]["best_bid"] == 100.0
    assert examples[0]["best_bid"] == 102.5
    assert examples[0]["best_ask"] == 102.0
