"""
stores.replay_depth_adapter — Map replay_store depth rows into the normalized
record shape consumed by the shared depth replay engine
(``converter.depth_phase2._run_depth_replay_loop``).

This adapter lets the validation-only ``validation.replay_catalog_reconstruct``
full_l2 helper reuse the *exact* validated depth-conversion semantics of the
``data_raw -> convert_day.py`` path, instead of re-implementing a weaker,
independent depth converter.

Field mapping (replay ``depth.parquet`` row -> normalized engine record)::

    record_type            -> record_type ('snapshot_seed' | 'depth_update' | 'sync_state')
    stream_session_id      -> stream_session_id (int)
    session_seq            -> session_seq (int)
    raw_index              -> raw_index (int)
    U   (str)              -> U  (int | None)
    u   (str)              -> u  (int | None)   for depth_update
                              lastUpdateId (int | None) for snapshot_seed
                              (the replay builder stores the snapshot's
                              ``lastUpdateId`` in the ``u`` column)
    pu  (str)              -> pu (int | None)
    ts_exchange_ns (int)   -> ts_event_ns (int)   [exact; honoured by _ts_event_ns]
    ts_receive_ns  (int)   -> ts_recv_ns  (int)   [engine ts_init]
    bids/asks structs      -> payload.bids / payload.asks as [price_str, size_str]
                              pairs (EXACT decimal strings preserved)
    quality_flags          -> state / reason / previous_state / last_update_id /
                              prev_update_id  for sync_state rows only (see
                              :func:`_sync_state_transition`) — sync_state
                              records carry no book payload or U/u/pu, so
                              their state-transition fields are round-tripped
                              through the existing, already-nullable
                              ``quality_flags`` JSON column instead of a new
                              physical schema field.

Ordering: replay rows are stored in raw-file order. The validated raw path
re-sorts by ``(stream_session_id, session_seq, raw_index)`` via a disk-backed
spool before replay; :func:`iter_replay_depth_records` applies the *same*
canonical sort using the same :class:`~converter.spool.RawRecordSpool`, so the
replay path matches the raw path's committed ordering while remaining
memory-bounded.

NOT reproduced here (documented equivalence caveats — replay v0 does not store
the inputs required):

  * ``sync_state`` and ``stream_lifecycle`` records are now BOTH preserved
    and replayed (see the field mapping above), so sync_state-driven and
    session-boundary-driven fenced ranges ARE regenerated, matching the
    reference exactly (verified via the canonical Tier-2
    `validate_catalog_equivalence` gate: full fenced-range digest match on
    the ADAUSDT 2026-06-12 smoke, not merely a matching count);
  * cross-day carry / synthetic opening snapshot (no prev/next repartitioning);
  * UTC-boundary repartitioning of clock-skewed records;
  * duplicate suppression.

See ``docs/FULL_L2_REPLAY_CATALOG_PLAN.md`` for the full equivalence boundary.
"""
from __future__ import annotations

import json
from typing import Iterable, Iterator, List, Optional

from converter.spool import RawRecordSpool


def _opt_int(value: object) -> Optional[int]:
    """Parse an optional integer that may arrive as ``None``, ``int``, or ``str``."""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    return int(value)


def _levels(structs: object) -> List[List[str]]:
    """Map replay bid/ask structs to ``[price_str, size_str]`` pairs (exact strings)."""
    out: List[List[str]] = []
    if not structs:
        return out
    for level in structs:
        out.append([level["price_str"], level["size_str"]])
    return out


def _sync_state_transition(quality_flags: object) -> dict:
    """Recover the ``sync_state`` record's state-transition fields
    (``state``/``previous_state``/``reason``/``last_update_id``/
    ``prev_update_id``) from the replay row's ``quality_flags`` JSON column
    — the ONLY place these are preserved (see
    ``pipeline.build_replay_store._convert_depth_record()``'s docstring for
    why no new physical schema field was added)."""
    if not quality_flags:
        return {}
    try:
        parsed = json.loads(quality_flags) if isinstance(quality_flags, str) else quality_flags
    except (TypeError, ValueError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    transition = parsed.get("sync_state_transition")
    return transition if isinstance(transition, dict) else {}


def replay_row_to_depth_record(row: dict) -> dict:
    """Map a single replay_store depth row to a normalized engine record.

    The returned dict is JSON-serializable and carries exactly the fields the
    shared depth engine reads. ``snapshot_seed`` rows expose ``lastUpdateId``
    (recovered from the replay ``u`` column); ``depth_update`` rows expose the
    Binance continuity ids ``U`` / ``u`` / ``pu``; ``sync_state`` rows expose
    ``state``/``reason`` (recovered from ``quality_flags`` — see
    :func:`_sync_state_transition`), which is exactly what the shared depth
    engine's ``record_type == "sync_state"`` branch reads to drive
    synchronization/desync/resync state and fenced-range open/close.
    ``stream_lifecycle`` rows pass through with empty bids/asks and no
    U/u/pu — the shared engine does not read their content, but their
    ``record_type``/``stream_session_id``/timestamp ARE read by its
    unconditional session-change detection, which both opens/closes fences
    on session boundaries using whichever record is first observed for a
    new session — since ``stream_lifecycle`` records are the actual first/
    last record of every raw session, they must be present for the
    fence-close/open timestamp to match the reference exactly.
    """
    record_type = row.get("record_type", "depth_update")
    rec: dict = {
        "record_type": record_type,
        "stream_session_id": _opt_int(row.get("stream_session_id")) or 0,
        "session_seq": _opt_int(row.get("session_seq")) or 0,
        "raw_index": _opt_int(row.get("raw_index")) or 0,
        "U": _opt_int(row.get("U")),
        "pu": _opt_int(row.get("pu")),
        "ts_event_ns": int(row.get("ts_exchange_ns") or 0),
        "ts_recv_ns": int(row.get("ts_receive_ns") or 0),
        "payload": {
            "bids": _levels(row.get("bids")),
            "asks": _levels(row.get("asks")),
        },
    }
    if record_type == "snapshot_seed":
        # The replay builder stores the snapshot lastUpdateId in the `u` column.
        rec["lastUpdateId"] = _opt_int(row.get("u"))
        rec["u"] = None
    else:
        rec["u"] = _opt_int(row.get("u"))
        rec["lastUpdateId"] = None
    if record_type == "sync_state":
        transition = _sync_state_transition(row.get("quality_flags"))
        rec["state"] = transition.get("state")
        rec["reason"] = transition.get("reason")
        rec["previous_state"] = transition.get("previous_state")
        rec["last_update_id"] = transition.get("last_update_id")
        rec["prev_update_id"] = transition.get("prev_update_id")
    return rec


def iter_replay_depth_records(
    rows: Iterable[dict],
    *,
    temp_dir: Optional[str] = None,
) -> Iterator[dict]:
    """Yield normalized depth records in canonical ``(session, seq, raw_index)`` order.

    Rows are mapped via :func:`replay_row_to_depth_record` and re-sorted through
    a disk-backed :class:`~converter.spool.RawRecordSpool` so the replay path
    reproduces the raw path's committed ordering while remaining memory-bounded.

    Args:
        rows: Iterable of replay ``depth.parquet`` row dicts (e.g. from
            ``ReplayReader.iter_depths``).
        temp_dir: Optional spool directory override (defaults to the converter
            temp dir / system temp).
    """
    with RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-replay-depth-") as spool:
        for row in rows:
            rec = replay_row_to_depth_record(row)
            sort_key = (rec["stream_session_id"], rec["session_seq"], rec["raw_index"])
            spool.insert(rec, sort_key, rec["raw_index"])
        spool.commit()
        yield from spool.iter_records()
