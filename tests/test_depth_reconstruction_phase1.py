#!/usr/bin/env python3
"""
tests/test_depth_reconstruction_phase1.py — Phase 1 depth replay safeguards.

Checks:
  1. stable_ts_init_sort     – replay order is ts_init-stable
  2. zero_size_deletes_level – size=0 removes levels before snapshot build
  3. crossed_book_reset      – crossed ladder logs/resets instead of emitting

Usage:
    ./.venv/bin/python3 tests/test_depth_reconstruction_phase1.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


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


def run_tests() -> int:
    from nautilus_trader.model.identifiers import InstrumentId

    import converter.book as book_mod

    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    results: List[bool] = []

    ordered = book_mod._sorted_depth_records(
        [
            _rec(ts_recv_ns=2_000_000_000, sequence_number=2),
            _rec(ts_recv_ns=1_000_000_000, sequence_number=1),
            _rec(ts_recv_ns=1_000_000_000, sequence_number=3),
        ]
    )
    ordered_seq = [rec["sequence_number"] for _, rec in ordered]
    ok1 = ordered_seq == [1, 3, 2]
    print(
        f"  [{'PASS' if ok1 else 'FAIL'}] stable_ts_init_sort "
        f"(sequence_order={ordered_seq})"
    )
    results.append(ok1)

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
    ok2 = bid_prices == [99.0] and ask_prices[:2] == [101.0, 101.5] and 100.0 not in bid_prices and 102.0 not in ask_prices
    print(
        f"  [{'PASS' if ok2 else 'FAIL'}] zero_size_deletes_level "
        f"(bids={bid_prices}, asks={ask_prices[:3]})"
    )
    results.append(ok2)

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
    ok3 = (
        bad == 0
        and gaps == 0
        and resets == 1
        and crossed == 1
        and snapshot_pairs == [
            (100.0, 101.0, 1_000_000_000),
            (100.0, 102.0, 2_000_000_000),
            (99.0, 103.0, 4_000_000_000),
        ]
        and len(examples) == 1
        and examples[0]["previous_snapshot"]["best_bid"] == 100.0
        and examples[0]["best_bid"] == 102.5
        and examples[0]["best_ask"] == 102.0
    )
    print(
        f"  [{'PASS' if ok3 else 'FAIL'}] crossed_book_reset "
        f"(snapshots={snapshot_pairs}, crossed={crossed}, resets={resets})"
    )
    results.append(ok3)

    passed = sum(results)
    total = len(results)
    print(f"\n  {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run_tests())
