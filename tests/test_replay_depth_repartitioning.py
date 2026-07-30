"""Focused tests for the issue #20 Phase 7 semantic-oracle correction:
depth_v2 cross-day event-time repartitioning in
``pipeline.build_replay_store``, matching ``convert_day.py``'s reference
rule (``converter.depth_phase2._spool_repartitioned_records``) exactly.

Root cause (see docs/IMPLEMENTATION_AUDIT.md and this session's diagnosis):
the recorder writes hourly-rotated raw files keyed by wall-clock RECEIVE
time. A depth_update whose EXCHANGE event time is late on UTC day D can be
physically written into day D+1's raw directory (ordinary network/
processing latency near midnight, not a defect). convert_day.py's reference
route corrects for this by scanning D-1/D/D+1's raw depth_v2 directories and
assigning every record to whichever UTC day its EVENT time falls in. Prior
to this correction, ``pipeline.build_replay_store`` read only the single
requested date's raw directory, silently omitting any such late-D event
that landed under D+1 (confirmed directly: exactly 47 such records were
missing from the 2026-06-11 ADAUSDT OrderBookDeltas reconstruction,
identical for both schema_version=0 and schema_version=2).

Trade_v2 is NOT repartitioned by convert_day.py's reference route
(``converter.trades.convert_trades_streaming`` reads only the single
requested date) — this module must not invent trade repartitioning either.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import zstandard as zstd

from pipeline.build_replay_store import (
    build_replay_for_symbol,
    check_depth_repartition_readiness,
    compute_repartitioned_source_identity,
)
from stores.replay_reader import ReplayReader

VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
PREV_DATE = "2026-06-10"
DATE = "2026-06-11"
NEXT_DATE = "2026-06-12"


def _write_jsonl(path: Path, records: "list[dict]") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _depth_update(
    *, session_id: int, seq: int, ts_event_ms: int, U: int, u: int, pu=None,
    bids=None, asks=None,
) -> dict:
    return {
        "record_type": "depth_update",
        "stream_session_id": session_id,
        "session_seq": seq,
        "ts_event_ms": ts_event_ms,
        "ts_recv_ns": ts_event_ms * 1_000_000,
        "U": U,
        "u": u,
        "pu": pu,
        "payload": {"bids": bids or [["0.5000", "10.0"]], "asks": asks or [["0.5100", "5.0"]]},
    }


def _snapshot_seed(*, session_id: int, seq: int, ts_event_ms: int, last_update_id: int) -> dict:
    return {
        "record_type": "snapshot_seed",
        "stream_session_id": session_id,
        "session_seq": seq,
        "ts_event_ms": ts_event_ms,
        "ts_recv_ns": ts_event_ms * 1_000_000,
        "lastUpdateId": last_update_id,
        "payload": {"bids": [["0.5000", "100.0"]], "asks": [["0.5100", "50.0"]]},
    }


def _trade(*, session_id: int, seq: int, ts_event_ms: int, trade_id: int) -> dict:
    return {
        "record_type": "trade",
        "trade_stream_session_id": session_id,
        "trade_session_seq": seq,
        "ts_trade_ms": ts_event_ms,
        "ts_recv_ns": ts_event_ms * 1_000_000,
        "trade_id": trade_id,
        "price_str": "0.5000",
        "quantity_str": "1.0",
        "buyer_maker": False,
    }


def _ms_of(date_str: str, hh: int, mm: int, ss: int, fraction_ms: int = 0) -> int:
    import datetime as dt
    d = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return int(d.timestamp() * 1000) + hh * 3_600_000 + mm * 60_000 + ss * 1000 + fraction_ms


def _write_depth_raw(root: Path, date_str: str, records: "list[dict]", hour: str = "23") -> None:
    _write_jsonl(root / VENUE / "depth_v2" / SYMBOL / date_str / f"{date_str}T{hour}.jsonl", records)


def _mark_depth_first_hour_closed(root: Path, date_str: str) -> None:
    """Create the next recorder hour, proving that T00 was rotated closed."""
    _write_depth_raw(root, date_str, [], hour="01")


def _mark_depth_day_complete(root: Path, date_str: str) -> None:
    """Create a sole compressed T23 file, proving the raw day is closed."""
    last_hour = (
        root
        / VENUE
        / "depth_v2"
        / SYMBOL
        / date_str
        / f"{date_str}T23.jsonl.zst"
    )
    last_hour.parent.mkdir(parents=True, exist_ok=True)
    with zstd.open(last_hour, "wt") as output:
        output.write("")


def _write_trade_raw(root: Path, date_str: str, records: "list[dict]", hour: str = "23") -> None:
    _write_jsonl(root / VENUE / "trade_v2" / SYMBOL / date_str / f"{date_str}T{hour}.jsonl", records)


# ---------------------------------------------------------------------------
# 1/2/3/4. Core repartitioning contract: late-D under D+1, early-D under D-1,
#           D-1 event under D excluded, exactly-midnight belongs to D+1 only.
# ---------------------------------------------------------------------------


def test_late_day_event_physically_under_next_day_is_included(tmp_path):
    raw_root = tmp_path / "raw"
    session, seed_seq = 19, 1
    seed_ts = _ms_of(DATE, 23, 59, 0)
    late_ts = _ms_of(DATE, 23, 59, 58)  # last few seconds of D, event time

    # Snapshot seed physically stored under D itself.
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=session, seq=1, ts_event_ms=seed_ts, last_update_id=100)])
    # The late-D event is physically written under D+1 (00-hour file) due to
    # ordinary receive latency past local midnight.
    late_event = _depth_update(session_id=session, seq=2, ts_event_ms=late_ts, U=101, u=102)
    _write_depth_raw(raw_root, NEXT_DATE, [late_event], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "success", result

    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths(VENUE, SYMBOL, DATE))
    types = [d["record_type"] for d in depths]
    assert types == ["snapshot_seed", "depth_update"]
    assert depths[1]["u"] == "102"


def test_early_day_event_physically_under_previous_day_is_included(tmp_path):
    raw_root = tmp_path / "raw"
    session = 7
    # Seed and update both stored physically under D-1's raw file, but with
    # an event time that is actually the very start of D (analogous receive
    # latency the other direction is not the normal case, but the contract
    # explicitly requires handling early-D events found under D-1 if the
    # reference does -- convert_day.py's rule is symmetric: it scans D-1
    # unconditionally and applies the same event-time window test).
    seed_ts = _ms_of(DATE, 0, 0, 0)
    _write_depth_raw(raw_root, PREV_DATE, [_snapshot_seed(session_id=session, seq=1, ts_event_ms=seed_ts, last_update_id=200)], hour="23")
    # D+1 readiness file (empty day is fine, just needs to exist).
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "success", result
    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths(VENUE, SYMBOL, DATE))
    assert len(depths) == 1
    assert depths[0]["record_type"] == "snapshot_seed"


def test_previous_day_event_physically_under_d_minus_1_excluded_from_target(tmp_path):
    """An event whose event time genuinely belongs to D-1, stored under
    D-1, must NOT leak into D's partition (only D-1's own build would
    include it)."""
    raw_root = tmp_path / "raw"
    prev_ts = _ms_of(PREV_DATE, 12, 0, 0)
    _write_depth_raw(raw_root, PREV_DATE, [_snapshot_seed(session_id=1, seq=1, ts_event_ms=prev_ts, last_update_id=1)], hour="12")
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "success", result
    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths(VENUE, SYMBOL, DATE))
    assert depths == []


def test_exactly_midnight_event_belongs_only_to_next_day(tmp_path):
    """An event at exactly D+1's midnight boundary belongs to D+1, never D
    (end-exclusive window: [start(D), start(D+1)))."""
    raw_root = tmp_path / "raw"
    midnight_ts = _ms_of(NEXT_DATE, 0, 0, 0)  # exactly start(D+1)
    _write_depth_raw(raw_root, NEXT_DATE, [_snapshot_seed(session_id=5, seq=1, ts_event_ms=midnight_ts, last_update_id=1)], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "success", result
    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths(VENUE, SYMBOL, DATE))
    assert depths == [], "an exactly-midnight event must never be pulled into the previous day's partition"

    # And it IS captured when building D+1 itself (using D+1's own next day
    # as its readiness dependency).
    _write_depth_raw(raw_root, "2026-06-13", [], hour="00")
    _mark_depth_first_hour_closed(raw_root, "2026-06-13")
    replay_root_next = tmp_path / "replay_next"
    result_next = build_replay_for_symbol(
        VENUE, SYMBOL, NEXT_DATE, raw_root, replay_root_next, check_repartition_readiness=True
    )
    assert result_next["status"] == "success", result_next
    reader_next = ReplayReader(replay_root_next)
    depths_next = list(reader_next.iter_depths(VENUE, SYMBOL, NEXT_DATE))
    assert len(depths_next) == 1


# ---------------------------------------------------------------------------
# 5. Trades receive the same (correct) handling: NOT repartitioned.
# ---------------------------------------------------------------------------


def test_trades_are_not_repartitioned_across_day_boundary(tmp_path):
    """convert_day.py's reference route does not repartition trade_v2 at
    all -- a late-D trade physically stored under D+1 must NOT be pulled
    into D's partition (matching the reference exactly, never inventing a
    stronger rule than the oracle itself applies)."""
    raw_root = tmp_path / "raw"
    late_ts = _ms_of(DATE, 23, 59, 58)
    _write_trade_raw(raw_root, DATE, [_trade(session_id=1, seq=1, ts_event_ms=_ms_of(DATE, 12, 0, 0), trade_id=1)])
    _write_trade_raw(raw_root, NEXT_DATE, [_trade(session_id=1, seq=2, ts_event_ms=late_ts, trade_id=2)], hour="00")
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "success", result
    reader = ReplayReader(replay_root)
    trades = list(reader.iter_trades(VENUE, SYMBOL, DATE))
    assert len(trades) == 1
    assert trades[0]["trade_id"] == "1"


# ---------------------------------------------------------------------------
# 6. No duplicate inclusion across adjacent partitions.
# ---------------------------------------------------------------------------


def test_no_duplicate_across_adjacent_partitions(tmp_path):
    """The same physical D+1 record must appear in D's repartitioned
    output exactly once, and must NOT also appear when D+1 itself is
    later built (it legitimately belongs to D by event time, so D+1's own
    build -- which uses the identical event-time window test -- must
    exclude it)."""
    raw_root = tmp_path / "raw"
    session = 3
    late_ts = _ms_of(DATE, 23, 59, 59)
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=session, seq=1, ts_event_ms=_ms_of(DATE, 0, 0, 1), last_update_id=1)])
    late_event = _depth_update(session_id=session, seq=2, ts_event_ms=late_ts, U=2, u=3)
    _write_depth_raw(raw_root, NEXT_DATE, [late_event], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)
    _write_depth_raw(raw_root, "2026-06-13", [], hour="00")
    _mark_depth_first_hour_closed(raw_root, "2026-06-13")

    replay_root_d = tmp_path / "replay_d"
    result_d = build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root_d, check_repartition_readiness=True)
    assert result_d["status"] == "success", result_d
    depths_d = list(ReplayReader(replay_root_d).iter_depths(VENUE, SYMBOL, DATE))
    assert len(depths_d) == 2  # seed + the late update

    replay_root_next = tmp_path / "replay_next"
    result_next = build_replay_for_symbol(VENUE, SYMBOL, NEXT_DATE, raw_root, replay_root_next, check_repartition_readiness=True)
    assert result_next["status"] == "success", result_next
    depths_next = list(ReplayReader(replay_root_next).iter_depths(VENUE, SYMBOL, NEXT_DATE))
    assert depths_next == [], "the late-D event must not also appear in D+1's own partition"


# ---------------------------------------------------------------------------
# 7/8. Deterministic ordering/tie-breaks and independent rebuild.
# ---------------------------------------------------------------------------


def test_deterministic_ordering_and_independent_rebuild(tmp_path):
    raw_root = tmp_path / "raw"
    session = 11
    _write_depth_raw(raw_root, DATE, [
        _snapshot_seed(session_id=session, seq=1, ts_event_ms=_ms_of(DATE, 10, 0, 0), last_update_id=1),
        _depth_update(session_id=session, seq=2, ts_event_ms=_ms_of(DATE, 10, 0, 1), U=2, u=3),
        _depth_update(session_id=session, seq=3, ts_event_ms=_ms_of(DATE, 10, 0, 2), U=4, u=5),
    ])
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    replay_root_a = tmp_path / "replay_a"
    replay_root_b = tmp_path / "replay_b"
    result_a = build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root_a, check_repartition_readiness=True)
    result_b = build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root_b, check_repartition_readiness=True)
    assert result_a["status"] == "success" and result_b["status"] == "success"

    depths_a = list(ReplayReader(replay_root_a).iter_depths(VENUE, SYMBOL, DATE))
    depths_b = list(ReplayReader(replay_root_b).iter_depths(VENUE, SYMBOL, DATE))
    assert [d["record_type"] for d in depths_a] == ["snapshot_seed", "depth_update", "depth_update"]
    assert depths_a == depths_b

    manifest_a = json.loads((replay_root_a / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}" / "manifest.json").read_text())
    manifest_b = json.loads((replay_root_b / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}" / "manifest.json").read_text())
    assert manifest_a["depth_checksum"] == manifest_b["depth_checksum"]


# ---------------------------------------------------------------------------
# 9. Readiness/failure behavior: adjacent source missing -> defer.
# ---------------------------------------------------------------------------


def test_readiness_defers_when_next_day_missing(tmp_path):
    raw_root = tmp_path / "raw"
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=1, seq=1, ts_event_ms=_ms_of(DATE, 10, 0, 0), last_update_id=1)])
    # Deliberately do NOT create the next-day directory at all.

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "deferred"
    assert len(result["errors"]) == 1
    assert NEXT_DATE in result["errors"][0]
    # Nothing must have been published.
    pdir = replay_root / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    assert not pdir.exists()


def test_readiness_defers_when_next_day_dir_exists_but_empty(tmp_path):
    raw_root = tmp_path / "raw"
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=1, seq=1, ts_event_ms=_ms_of(DATE, 10, 0, 0), last_update_id=1)])
    (raw_root / VENUE / "depth_v2" / SYMBOL / NEXT_DATE).mkdir(parents=True)

    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, check_repartition_readiness=True
    )
    assert result["status"] == "deferred"


def test_readiness_check_function_directly(tmp_path):
    raw_root = tmp_path / "raw"
    assert check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root) is not None
    # An arbitrary later-hour file cannot stand in for the exact first hour.
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="12")
    reason = check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root)
    assert reason is not None
    assert "T00" in reason
    (
        raw_root / VENUE / "depth_v2" / SYMBOL / NEXT_DATE
        / f"{NEXT_DATE}T12.jsonl"
    ).unlink()

    # T00 exists but may still be the recorder's active handle.
    _write_depth_raw(raw_root, NEXT_DATE, [_snapshot_seed(session_id=1, seq=1, ts_event_ms=_ms_of(NEXT_DATE, 0, 0, 0), last_update_id=1)], hour="00")
    reason = check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root)
    assert reason is not None
    assert "still have it open" in reason

    # The next recorder hour proves that FileRotator closed T00.
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)
    assert check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root) is None


def test_readiness_accepts_sole_compressed_first_hour(tmp_path):
    raw_root = tmp_path / "raw"
    first_hour = (
        raw_root / VENUE / "depth_v2" / SYMBOL / NEXT_DATE
        / f"{NEXT_DATE}T00.jsonl.zst"
    )
    first_hour.parent.mkdir(parents=True)
    with zstd.open(first_hour, "wt") as output:
        output.write("")

    assert check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root) is None


def test_offline_readiness_requires_complete_closed_next_day(tmp_path):
    raw_root = tmp_path / "raw"
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    reason = check_depth_repartition_readiness(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        require_complete_next_day=True,
    )
    assert reason is not None
    assert "last-hour" in reason

    _mark_depth_day_complete(raw_root, NEXT_DATE)
    assert (
        check_depth_repartition_readiness(
            VENUE,
            SYMBOL,
            DATE,
            raw_root,
            require_complete_next_day=True,
        )
        is None
    )


def test_readiness_defers_while_compressed_and_uncompressed_variants_coexist(tmp_path):
    raw_root = tmp_path / "raw"
    _write_depth_raw(raw_root, NEXT_DATE, [], hour="00")
    compressed = (
        raw_root / VENUE / "depth_v2" / SYMBOL / NEXT_DATE
        / f"{NEXT_DATE}T00.jsonl.zst"
    )
    compressed.write_bytes(b"compression-in-progress-fixture")

    reason = check_depth_repartition_readiness(VENUE, SYMBOL, DATE, raw_root)
    assert reason is not None
    assert "multiple coexisting variants" in reason


def test_readiness_not_required_when_flag_is_false_default(tmp_path):
    """Backward-compatible default: check_repartition_readiness=False (the
    default) never defers, matching pre-correction behavior for callers
    that don't opt in (existing single-day test fixtures across the
    repository)."""
    raw_root = tmp_path / "raw"
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=1, seq=1, ts_event_ms=_ms_of(DATE, 10, 0, 0), last_update_id=1)])
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(VENUE, SYMBOL, DATE, raw_root, replay_root)
    assert result["status"] == "success"


# ---------------------------------------------------------------------------
# 10. Manifest/source-identity: exact source-record mapping and traceability.
# ---------------------------------------------------------------------------


def test_source_identity_records_adjacent_day_contribution(tmp_path):
    """The manifest's source_identity must be able to explain which
    adjacent-day physical source contributed a repartitioned event: each
    depth_v2 entry carries its own source_date, and entries whose
    source_date differs from the target date are the exact adjacent-source
    evidence requested."""
    raw_root = tmp_path / "raw"
    session = 9
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=session, seq=1, ts_event_ms=_ms_of(DATE, 23, 0, 0), last_update_id=1)])
    late_event = _depth_update(session_id=session, seq=2, ts_event_ms=_ms_of(DATE, 23, 59, 59), U=2, u=3)
    _write_depth_raw(raw_root, NEXT_DATE, [late_event], hour="00")
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    identity = compute_repartitioned_source_identity(VENUE, SYMBOL, DATE, raw_root, include_record_counts=True)
    depth_entries = identity["channels"]["depth_v2"]
    source_dates = {e["source_date"] for e in depth_entries}
    assert DATE in source_dates
    assert NEXT_DATE in source_dates, "must be able to explain the June-12-physical/June-11-event-time contribution"
    for e in depth_entries:
        assert "record_range" in e and "record_count" in e
        assert e["record_range"][1] - e["record_range"][0] == e["record_count"]


def test_schema_v0_v1_v2_use_identical_repartitioned_input(tmp_path):
    """The repartitioning correction applies identically to schema_version
    0, 1, and 2 -- all three must read the exact same set of depth
    records."""
    raw_root = tmp_path / "raw"
    session = 4
    _write_depth_raw(raw_root, DATE, [_snapshot_seed(session_id=session, seq=1, ts_event_ms=_ms_of(DATE, 10, 0, 0), last_update_id=1)])
    late_event = _depth_update(session_id=session, seq=2, ts_event_ms=_ms_of(DATE, 23, 59, 59), U=2, u=3)
    _write_depth_raw(raw_root, NEXT_DATE, [late_event], hour="00")
    _write_trade_raw(
        raw_root,
        DATE,
        [_trade(session_id=session, seq=1, ts_event_ms=_ms_of(DATE, 12, 0, 0), trade_id=1)],
    )
    _mark_depth_first_hour_closed(raw_root, NEXT_DATE)

    counts = {}
    for version in (0, 1, 2):
        replay_root = tmp_path / f"replay_v{version}"
        kwargs = {"schema_version": version, "check_repartition_readiness": True}
        if version in (1, 2):
            kwargs["price_scale"] = 4
            kwargs["qty_scale"] = 1
        result = build_replay_for_symbol(
            VENUE, SYMBOL, DATE, raw_root, replay_root, **kwargs,
        )
        assert result["status"] == "success", (version, result)
        counts[version] = result["depth_count"]
    assert counts[0] == counts[1] == counts[2] == 2
