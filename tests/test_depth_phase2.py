from __future__ import annotations

from typing import Iterable

from nautilus_trader.model.identifiers import InstrumentId

import converter.depth_phase2 as phase2_mod


def _snapshot_seed(
    *,
    session: int,
    ts_recv_ns: int,
    last_update_id: int,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
    connection_seq: int = 0,
    file_position: int = 0,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "snapshot_seed",
        "stream_session_id": session,
        "connection_seq": connection_seq,
        "file_position": file_position,
        "ts_recv_ns": ts_recv_ns,
        "lastUpdateId": last_update_id,
        "payload": {
            "bids": bids or [],
            "asks": asks or [],
        },
    }


def _sync_state(
    *,
    session: int,
    ts_recv_ns: int,
    state: str,
    reason: str,
    connection_seq: int,
    file_position: int,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "sync_state",
        "stream_session_id": session,
        "connection_seq": connection_seq,
        "file_position": file_position,
        "ts_recv_ns": ts_recv_ns,
        "state": state,
        "reason": reason,
    }


def _depth_update(
    *,
    session: int,
    ts_recv_ns: int,
    U: int,
    u: int,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
    pu: int | None = None,
    connection_seq: int = 1,
    file_position: int = 1,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "depth_update",
        "stream_session_id": session,
        "connection_seq": connection_seq,
        "file_position": file_position,
        "ts_recv_ns": ts_recv_ns,
        "U": U,
        "u": u,
        "pu": pu,
        "payload": {
            "bids": bids or [],
            "asks": asks or [],
        },
    }


def test_phase2_spot_replay_bootstrap_and_stable_ordering() -> None:
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original_stream = phase2_mod.stream_raw_records

    try:
        def _fake_stream(*args, **kwargs) -> Iterable[dict]:
            yield _depth_update(
                session=1,
                ts_recv_ns=2_000_000_000,
                U=103,
                u=103,
                bids=[["100.0", "3.0"]],
                connection_seq=2,
                file_position=4,
            )
            yield _snapshot_seed(
                session=1,
                ts_recv_ns=1_000_000_000,
                last_update_id=100,
                bids=[["99.0", "1.0"]],
                asks=[["101.0", "2.0"]],
                connection_seq=0,
                file_position=1,
            )
            yield _depth_update(
                session=1,
                ts_recv_ns=1_500_000_000,
                U=101,
                u=102,
                bids=[["100.0", "2.0"]],
                asks=[["101.0", "1.5"]],
                connection_seq=1,
                file_position=2,
            )

        phase2_mod.stream_raw_records = _fake_stream
        deltas, depth10s, metrics = phase2_mod.convert_depth_v2(
            "BINANCE_SPOT",
            "BTCUSDT",
            "2026-04-21",
            iid,
            1,
            1,
            emit_depth10=False,
        )
    finally:
        phase2_mod.stream_raw_records = original_stream

    assert len(deltas) == 3
    assert depth10s == []
    assert metrics.snapshot_seed_count == 1
    assert metrics.delta_events_written == 3
    assert metrics.fenced_ranges == []
    assert [obj.sequence for obj in deltas] == [100, 102, 103]


def test_phase2_futures_continuity_break_is_fenced_until_new_snapshot() -> None:
    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    original_stream = phase2_mod.stream_raw_records

    try:
        def _fake_stream(*args, **kwargs) -> Iterable[dict]:
            yield _snapshot_seed(
                session=1,
                ts_recv_ns=1_000_000_000,
                last_update_id=200,
                bids=[["99.0", "1.0"]],
                asks=[["101.0", "2.0"]],
                file_position=1,
            )
            yield _depth_update(
                session=1,
                ts_recv_ns=1_100_000_000,
                U=201,
                u=202,
                pu=200,
                bids=[["100.0", "2.0"]],
                connection_seq=1,
                file_position=2,
            )
            yield _sync_state(
                session=1,
                ts_recv_ns=1_150_000_000,
                state="desynced",
                reason="continuity_break",
                connection_seq=2,
                file_position=3,
            )
            yield _depth_update(
                session=1,
                ts_recv_ns=1_200_000_000,
                U=203,
                u=204,
                pu=999,
                asks=[["101.0", "1.0"]],
                connection_seq=3,
                file_position=4,
            )
            yield _snapshot_seed(
                session=1,
                ts_recv_ns=1_300_000_000,
                last_update_id=204,
                bids=[["100.0", "2.0"]],
                asks=[["101.0", "1.0"]],
                connection_seq=4,
                file_position=5,
            )
            yield _depth_update(
                session=1,
                ts_recv_ns=1_400_000_000,
                U=205,
                u=205,
                pu=204,
                asks=[["101.5", "1.5"]],
                connection_seq=5,
                file_position=6,
            )

        phase2_mod.stream_raw_records = _fake_stream
        deltas, depth10s, metrics = phase2_mod.convert_depth_v2(
            "BINANCE_USDTF",
            "BTCUSDT",
            "2026-04-21",
            iid,
            1,
            1,
            emit_depth10=True,
            depth10_interval_sec=0.0,
        )
    finally:
        phase2_mod.stream_raw_records = original_stream

    assert len(deltas) == 4
    assert len(depth10s) >= 2
    assert metrics.snapshot_seed_count == 2
    assert metrics.desync_events >= 1
    assert len(metrics.fenced_ranges) == 1
    assert metrics.fenced_ranges[0]["reason"] in {"continuity_break", "desynced"}
    assert metrics.fenced_ranges[0]["recovered"] is True
