"""Tests for deterministic Binance-native depth replay.

All fixtures use session_seq (committed-only ordering) as the canonical
sort key instead of connection_seq / file_position.
"""
from __future__ import annotations

from typing import Iterable

from nautilus_trader.model.identifiers import InstrumentId

import converter.depth_phase2 as depth_mod


# ── record builders ───────────────────────────────────────────────────

def _snapshot_seed(
    *,
    session: int,
    session_seq: int,
    ts_recv_ns: int,
    last_update_id: int,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "snapshot_seed",
        "stream_session_id": session,
        "session_seq": session_seq,
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
    session_seq: int,
    ts_recv_ns: int,
    state: str,
    reason: str,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "sync_state",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "state": state,
        "reason": reason,
    }


def _depth_update(
    *,
    session: int,
    session_seq: int,
    ts_recv_ns: int,
    U: int,
    u: int,
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
    pu: int | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "depth_update",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "U": U,
        "u": u,
        "pu": pu,
        "payload": {
            "bids": bids or [],
            "asks": asks or [],
        },
    }


def _lifecycle(
    *,
    session: int,
    session_seq: int,
    ts_recv_ns: int,
    event: str,
) -> dict:
    return {
        "schema_version": 2,
        "record_type": "stream_lifecycle",
        "stream_session_id": session,
        "session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "event": event,
        "reason": "test",
    }


# ── tests ─────────────────────────────────────────────────────────────

def test_spot_replay_bootstrap_and_committed_ordering() -> None:
    """Committed bundle: snapshot_seed → accepted depths → live_synced.

    Records arrive out of disk order but are sorted by (session, session_seq).
    """
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = depth_mod.stream_raw_records

    try:
        def _fake(*a, **kw) -> Iterable[dict]:
            # Committed bundle: snapshot at seq=1, two depths at seq=2,3
            # Arrive out of order to prove sorting works
            yield _depth_update(
                session=1, session_seq=3,
                ts_recv_ns=3_000, U=103, u=103,
                bids=[["100.0", "3.0"]],
            )
            yield _snapshot_seed(
                session=1, session_seq=1,
                ts_recv_ns=1_000, last_update_id=100,
                bids=[["99.0", "1.0"]], asks=[["101.0", "2.0"]],
            )
            yield _depth_update(
                session=1, session_seq=2,
                ts_recv_ns=2_000, U=101, u=102,
                bids=[["100.0", "2.0"]], asks=[["101.0", "1.5"]],
            )

        depth_mod.stream_raw_records = _fake
        deltas, depth10s, metrics = depth_mod.convert_depth_v2(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21",
            iid, 1, 1, emit_depth10=False,
        )
    finally:
        depth_mod.stream_raw_records = original

    assert len(deltas) == 3, f"expected 3 delta events, got {len(deltas)}"
    assert depth10s == []
    assert metrics.snapshot_seed_count == 1
    assert metrics.delta_events_written == 3
    assert [obj.sequence for obj in deltas] == [100, 102, 103]


def test_session_seq_only_committed_records_no_gaps() -> None:
    """session_seq values in the raw stream should be contiguous (no gaps from
    rejected buffered events) because the recorder only assigns them on commit.
    
    Replay must process them in exact sequence.
    """
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = depth_mod.stream_raw_records

    try:
        def _fake(*a, **kw) -> Iterable[dict]:
            yield _snapshot_seed(
                session=1, session_seq=1,
                ts_recv_ns=1_000, last_update_id=50,
                bids=[["50.0", "1.0"]], asks=[["51.0", "1.0"]],
            )
            yield _depth_update(
                session=1, session_seq=2,
                ts_recv_ns=2_000, U=51, u=51,
                bids=[["50.0", "2.0"]],
            )
            yield _depth_update(
                session=1, session_seq=3,
                ts_recv_ns=3_000, U=52, u=52,
                asks=[["51.0", "0.5"]],
            )

        depth_mod.stream_raw_records = _fake
        deltas, _, metrics = depth_mod.convert_depth_v2(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21",
            iid, 1, 1,
        )
    finally:
        depth_mod.stream_raw_records = original

    assert metrics.delta_events_written == 3
    assert metrics.desync_events == 0
    assert metrics.fenced_ranges == []


def test_futures_U_u_pu_continuity_and_fencing() -> None:
    """Futures continuity: pu must equal prev_update_id & u > prev_update_id.
    
    A continuity break should open a fenced range until the next snapshot.
    """
    iid = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
    original = depth_mod.stream_raw_records

    try:
        def _fake(*a, **kw) -> Iterable[dict]:
            yield _snapshot_seed(
                session=1, session_seq=1,
                ts_recv_ns=1_000, last_update_id=200,
                bids=[["99.0", "1.0"]], asks=[["101.0", "2.0"]],
            )
            # Bootstrap succeeds: U=198 <= 200, u=202 >= 200
            yield _depth_update(
                session=1, session_seq=2,
                ts_recv_ns=2_000, U=198, u=202, pu=195,
                bids=[["100.0", "2.0"]],
            )
            yield _sync_state(
                session=1, session_seq=3,
                ts_recv_ns=2_500, state="desynced", reason="continuity_break",
            )
            # This depth_update has wrong pu -> should be fenced
            yield _depth_update(
                session=1, session_seq=4,
                ts_recv_ns=3_000, U=203, u=204, pu=999,
                asks=[["101.0", "1.0"]],
            )
            # New snapshot recovers
            yield _snapshot_seed(
                session=1, session_seq=5,
                ts_recv_ns=4_000, last_update_id=204,
                bids=[["100.0", "2.0"]], asks=[["101.0", "1.0"]],
            )
            # Bootstrap: U=202 <= 204, u=206 >= 204
            yield _depth_update(
                session=1, session_seq=6,
                ts_recv_ns=5_000, U=202, u=206, pu=200,
                asks=[["101.5", "1.5"]],
            )

        depth_mod.stream_raw_records = _fake
        deltas, depth10s, metrics = depth_mod.convert_depth_v2(
            "BINANCE_USDTF", "BTCUSDT", "2026-04-21",
            iid, 1, 1, emit_depth10=True, depth10_interval_sec=0.0,
        )
    finally:
        depth_mod.stream_raw_records = original

    assert len(deltas) == 4  # snapshot + 1 depth + new snapshot + 1 depth
    assert len(depth10s) >= 2
    assert metrics.snapshot_seed_count == 2
    assert metrics.desync_events >= 1
    assert len(metrics.fenced_ranges) == 1
    assert metrics.fenced_ranges[0]["recovered"] is True


def test_reconnect_session_boundary_resets_book() -> None:
    """Session change (reconnect) must reset book and start fresh."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = depth_mod.stream_raw_records

    try:
        def _fake(*a, **kw) -> Iterable[dict]:
            # Session 1
            yield _lifecycle(session=1, session_seq=1, ts_recv_ns=1_000, event="session_start")
            yield _snapshot_seed(
                session=1, session_seq=2,
                ts_recv_ns=2_000, last_update_id=10,
                bids=[["10.0", "1.0"]], asks=[["11.0", "1.0"]],
            )
            yield _depth_update(
                session=1, session_seq=3,
                ts_recv_ns=3_000, U=11, u=11,
                bids=[["10.0", "2.0"]],
            )
            # Session 2 — reconnect
            yield _lifecycle(session=2, session_seq=1, ts_recv_ns=4_000, event="session_start")
            yield _snapshot_seed(
                session=2, session_seq=2,
                ts_recv_ns=5_000, last_update_id=20,
                bids=[["20.0", "5.0"]], asks=[["21.0", "5.0"]],
            )
            yield _depth_update(
                session=2, session_seq=3,
                ts_recv_ns=6_000, U=21, u=21,
                asks=[["21.0", "4.0"]],
            )

        depth_mod.stream_raw_records = _fake
        deltas, _, metrics = depth_mod.convert_depth_v2(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21",
            iid, 1, 1,
        )
    finally:
        depth_mod.stream_raw_records = original

    # Two snapshots + two live updates = 4 delta events
    assert metrics.snapshot_seed_count == 2
    assert metrics.delta_events_written == 4


def test_deterministic_replay_stability() -> None:
    """Same input → same output regardless of disk arrival order."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = depth_mod.stream_raw_records

    def _make_records():
        return [
            _snapshot_seed(
                session=1, session_seq=1,
                ts_recv_ns=1_000, last_update_id=100,
                bids=[["50.0", "1.0"]], asks=[["51.0", "1.0"]],
            ),
            _depth_update(
                session=1, session_seq=2,
                ts_recv_ns=2_000, U=101, u=101,
                bids=[["50.0", "2.0"]],
            ),
            _depth_update(
                session=1, session_seq=3,
                ts_recv_ns=3_000, U=102, u=102,
                asks=[["51.0", "0.5"]],
            ),
        ]

    results = []
    for shuffle_fn in [lambda r: r, lambda r: r[::-1], lambda r: [r[2], r[0], r[1]]]:
        try:
            records = _make_records()
            shuffled = shuffle_fn(records)
            def _fake(*a, _recs=shuffled, **kw):
                return iter(_recs)
            depth_mod.stream_raw_records = _fake
            deltas, _, metrics = depth_mod.convert_depth_v2(
                "BINANCE_SPOT", "BTCUSDT", "2026-04-21",
                iid, 1, 1,
            )
            results.append([d.sequence for d in deltas])
        finally:
            depth_mod.stream_raw_records = original

    # All orderings must produce identical output
    assert results[0] == results[1] == results[2]


def test_depth10_off_by_default() -> None:
    """OrderBookDepth10 should be empty when emit_depth10 is False (default)."""
    iid = InstrumentId.from_str("BTCUSDT.BINANCE")
    original = depth_mod.stream_raw_records

    try:
        def _fake(*a, **kw):
            yield _snapshot_seed(
                session=1, session_seq=1,
                ts_recv_ns=1_000, last_update_id=100,
                bids=[["50.0", "1.0"]], asks=[["51.0", "1.0"]],
            )

        depth_mod.stream_raw_records = _fake
        deltas, depth10s, _ = depth_mod.convert_depth_v2(
            "BINANCE_SPOT", "BTCUSDT", "2026-04-21",
            iid, 1, 1, emit_depth10=False,
        )
    finally:
        depth_mod.stream_raw_records = original

    assert len(deltas) >= 1
    assert depth10s == []
