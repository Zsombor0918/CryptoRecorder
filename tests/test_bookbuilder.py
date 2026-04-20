from __future__ import annotations

import json
from pathlib import Path
from typing import List

from nautilus_trader.model.identifiers import InstrumentId

from converter.book import BookBuilder, BookBuilderPolicy, convert_depth_records

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "crossed_depth_musdt_perp.jsonl"


def _rec(
    *,
    ts_recv_ns: int,
    ts_event_ns: int | None = None,
    bids: List[List[str]] | None = None,
    asks: List[List[str]] | None = None,
    sequence_number: int = 0,
) -> dict:
    ts_event = ts_event_ns if ts_event_ns is not None else ts_recv_ns
    return {
        "ts_recv_ns": ts_recv_ns,
        "ts_event_ms": ts_event // 1_000_000,
        "sequence_number": sequence_number,
        "payload": {
            "bids": bids or [],
            "asks": asks or [],
        },
    }


def _active_prices(orders) -> List[float]:
    prices: List[float] = []
    for order in orders:
        size = float(str(order.size))
        if size > 0:
            prices.append(float(str(order.price)))
    return prices


def _load_fixture_records() -> List[dict]:
    return [
        json.loads(line)
        for line in FIXTURE_PATH.read_text().splitlines()
        if line.strip()
    ]


def test_bookbuilder_normal_delta_sequence_never_crossed() -> None:
    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    policy = BookBuilderPolicy(snapshot_emit_mode="every_delta")
    builder = BookBuilder(iid, 2, 3, venue="BINANCE_USDTF", symbol="BTCUSDT", policy=policy)

    records = [
        _rec(
            ts_recv_ns=1_000_000_000,
            bids=[["100.00", "1.0"], ["99.50", "2.0"]],
            asks=[["100.50", "1.5"], ["101.00", "1.0"]],
            sequence_number=1,
        ),
        _rec(
            ts_recv_ns=2_000_000_000,
            bids=[["100.10", "1.2"]],
            asks=[["100.50", "1.2"]],
            sequence_number=2,
        ),
        _rec(
            ts_recv_ns=3_000_000_000,
            bids=[["100.00", "0"]],
            asks=[["100.40", "1.0"]],
            sequence_number=3,
        ),
    ]

    snapshots = []
    for input_index, rec in enumerate(records):
        ts_ns = rec["ts_recv_ns"]
        snapshot = builder.process_event(
            rec,
            ts_event=ts_ns,
            ts_init=ts_ns,
            input_index=input_index,
        )
        if snapshot is not None:
            snapshots.append(snapshot)

    assert snapshots
    assert builder.metrics.crossed_book_drops == 0
    for snapshot in snapshots:
        best_bid = _active_prices(snapshot.bids)[0]
        best_ask = _active_prices(snapshot.asks)[0]
        assert best_bid < best_ask


def test_bookbuilder_crossed_policy_drop_skips_snapshot_and_counts() -> None:
    iid = InstrumentId.from_str("MUSDT-PERP.BINANCE")
    policy = BookBuilderPolicy(
        on_crossed_book="drop",
        snapshot_emit_mode="every_delta",
    )
    builder = BookBuilder(iid, 2, 3, venue="BINANCE_USDTF", symbol="MUSDT", policy=policy)

    good = _rec(
        ts_recv_ns=1_000_000_000,
        bids=[["10.00", "1.0"]],
        asks=[["10.05", "1.0"]],
        sequence_number=1,
    )
    crossed = _rec(
        ts_recv_ns=2_000_000_000,
        bids=[["10.06", "1.0"]],
        asks=[],
        sequence_number=2,
    )

    snapshot_1 = builder.process_event(
        good,
        ts_event=good["ts_recv_ns"],
        ts_init=good["ts_recv_ns"],
        input_index=0,
    )
    snapshot_2 = builder.process_event(
        crossed,
        ts_event=crossed["ts_recv_ns"],
        ts_init=crossed["ts_recv_ns"],
        input_index=1,
    )

    assert snapshot_1 is not None
    assert snapshot_2 is None
    assert builder.metrics.crossed_book_drops == 1
    assert builder.metrics.book_resets == 0
    assert builder.metrics.crossed_book_examples[0]["action"] == "drop"


def test_bookbuilder_crossed_policy_soft_reset_rebuilds_before_next_snapshot() -> None:
    iid = InstrumentId.from_str("MUSDT-PERP.BINANCE")
    policy = BookBuilderPolicy(
        on_crossed_book="soft_reset",
        snapshot_emit_mode="every_delta",
    )
    builder = BookBuilder(iid, 2, 3, venue="BINANCE_USDTF", symbol="MUSDT", policy=policy)

    records = [
        _rec(
            ts_recv_ns=1_000_000_000,
            bids=[["10.00", "1.0"]],
            asks=[["10.05", "1.0"]],
            sequence_number=1,
        ),
        _rec(
            ts_recv_ns=2_000_000_000,
            bids=[["10.06", "1.0"]],
            asks=[],
            sequence_number=2,
        ),
        _rec(
            ts_recv_ns=3_000_000_000,
            bids=[["9.98", "1.3"]],
            asks=[["10.08", "1.4"]],
            sequence_number=3,
        ),
    ]

    snapshots = []
    for input_index, rec in enumerate(records):
        ts_ns = rec["ts_recv_ns"]
        snapshot = builder.process_event(
            rec,
            ts_event=ts_ns,
            ts_init=ts_ns,
            input_index=input_index,
        )
        if snapshot is not None:
            snapshots.append(snapshot)

    assert len(snapshots) == 2
    assert builder.metrics.crossed_book_drops == 1
    assert builder.metrics.book_resets == 1
    assert builder.bids[9.98] == 1.3
    assert builder.asks[10.08] == 1.4
    assert 10.06 not in builder.bids


def test_bookbuilder_out_of_order_ts_event_is_counted() -> None:
    iid = InstrumentId.from_str("ETHUSDT-PERP.BINANCE")
    policy = BookBuilderPolicy(snapshot_emit_mode="every_delta")
    builder = BookBuilder(iid, 2, 3, venue="BINANCE_USDTF", symbol="ETHUSDT", policy=policy)

    first = _rec(
        ts_recv_ns=2_000_000_000,
        ts_event_ns=2_000_000_000,
        bids=[["100.00", "1.0"]],
        asks=[["100.10", "1.0"]],
        sequence_number=1,
    )
    second = _rec(
        ts_recv_ns=3_000_000_000,
        ts_event_ns=1_500_000_000,
        bids=[["100.01", "1.1"]],
        asks=[["100.11", "1.1"]],
        sequence_number=2,
    )

    builder.process_event(
        first,
        ts_event=first["ts_event_ms"] * 1_000_000,
        ts_init=first["ts_recv_ns"],
        input_index=0,
    )
    builder.process_event(
        second,
        ts_event=second["ts_event_ms"] * 1_000_000,
        ts_init=second["ts_recv_ns"],
        input_index=1,
    )

    assert builder.metrics.out_of_order_events == 1
    assert builder.metrics.depth_events_in == 2


def test_fixture_crossed_sequence_never_emits_crossed_snapshot() -> None:
    iid = InstrumentId.from_str("MUSDT-PERP.BINANCE")
    policy = BookBuilderPolicy(
        on_crossed_book="soft_reset",
        snapshot_emit_mode="every_delta",
    )

    (
        snapshots,
        bad_lines,
        gaps,
        resets,
        crossed_drops,
        _examples,
        _first_ts,
        _last_ts,
        metrics,
    ) = convert_depth_records(
        _load_fixture_records(),
        iid,
        3,
        3,
        venue="BINANCE_USDTF",
        symbol="MUSDT",
        policy=policy,
        include_metrics=True,
    )

    assert bad_lines == 0
    assert gaps == 0
    assert crossed_drops >= 1
    assert resets >= 1
    assert metrics["depth_snapshots_out"] == len(snapshots)

    for snapshot in snapshots:
        bids = _active_prices(snapshot.bids)
        asks = _active_prices(snapshot.asks)
        assert bids and asks
        assert bids[0] < asks[0]
