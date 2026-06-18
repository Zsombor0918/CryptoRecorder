"""Unit tests for stores.replay_depth_adapter.

These tests pin the field mapping and canonical ordering that let the
``replay_store -> generate_catalog --profile full_l2`` path reuse the validated
``convert_day.py`` depth-replay engine.
"""
from __future__ import annotations

from stores.replay_depth_adapter import (
    iter_replay_depth_records,
    replay_row_to_depth_record,
)


def _snapshot_row(**overrides):
    row = {
        "record_type": "snapshot_seed",
        "stream_session_id": "1",
        "session_seq": "0",
        "raw_index": "0",
        "U": None,
        "u": "100",  # replay builder stores snapshot lastUpdateId in the `u` column
        "pu": None,
        "ts_exchange_ns": 1_781_222_400_000_000_000,
        "ts_receive_ns": 1_781_222_400_000_000_111,
        "bids": [{"price_str": "0.1700", "size_str": "100.0"}],
        "asks": [{"price_str": "0.1710", "size_str": "200.0"}],
    }
    row.update(overrides)
    return row


def _update_row(**overrides):
    row = {
        "record_type": "depth_update",
        "stream_session_id": "1",
        "session_seq": "1",
        "raw_index": "1",
        "U": "101",
        "u": "105",
        "pu": None,
        "ts_exchange_ns": 1_781_222_400_000_000_200,
        "ts_receive_ns": 1_781_222_400_000_000_222,
        "bids": [{"price_str": "0.1700", "size_str": "120.0"}],
        "asks": [{"price_str": "0.1710", "size_str": "180.0"}],
    }
    row.update(overrides)
    return row


def test_snapshot_row_maps_last_update_id_from_u_column() -> None:
    rec = replay_row_to_depth_record(_snapshot_row())
    assert rec["record_type"] == "snapshot_seed"
    # The snapshot lastUpdateId is recovered from the replay `u` column.
    assert rec["lastUpdateId"] == 100
    assert rec["u"] is None
    assert rec["U"] is None
    assert rec["pu"] is None
    # Exact exchange timestamp is honoured (no ms rounding).
    assert rec["ts_event_ns"] == 1_781_222_400_000_000_000
    assert rec["ts_recv_ns"] == 1_781_222_400_000_000_111
    assert rec["payload"]["bids"] == [["0.1700", "100.0"]]
    assert rec["payload"]["asks"] == [["0.1710", "200.0"]]


def test_depth_update_row_maps_continuity_ids_as_ints() -> None:
    rec = replay_row_to_depth_record(_update_row(pu="100"))
    assert rec["record_type"] == "depth_update"
    assert rec["U"] == 101
    assert rec["u"] == 105
    assert rec["pu"] == 100
    assert rec["lastUpdateId"] is None
    assert rec["payload"]["bids"] == [["0.1700", "120.0"]]


def test_empty_and_missing_optional_ids_become_none() -> None:
    rec = replay_row_to_depth_record(_update_row(U="", pu=None))
    assert rec["U"] is None
    assert rec["pu"] is None


def test_iter_records_resorts_into_canonical_order() -> None:
    # Provide rows out of order; canonical order is (session, seq, raw_index).
    rows = [
        _update_row(session_seq="2", raw_index="2", U="106", u="110"),
        _snapshot_row(session_seq="0", raw_index="0"),
        _update_row(session_seq="1", raw_index="1", U="101", u="105"),
    ]
    records = list(iter_replay_depth_records(rows))
    assert [r["record_type"] for r in records] == [
        "snapshot_seed",
        "depth_update",
        "depth_update",
    ]
    assert [r["session_seq"] for r in records] == [0, 1, 2]
    assert [r["u"] for r in records] == [None, 105, 110]


def test_iter_records_orders_across_sessions() -> None:
    rows = [
        _update_row(stream_session_id="2", session_seq="0", raw_index="9", U="200", u="205"),
        _snapshot_row(stream_session_id="1", session_seq="0", raw_index="0"),
    ]
    records = list(iter_replay_depth_records(rows))
    assert [r["stream_session_id"] for r in records] == [1, 2]
