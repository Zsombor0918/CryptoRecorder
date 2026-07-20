"""
stores.replay_depth_adapter — Map replay_store depth rows into the normalized
record shape consumed by the shared depth replay engine
(``converter.depth_phase2._run_depth_replay_loop``).

This adapter lets the validation-only ``validation.replay_catalog_reconstruct``
full_l2 helper reuse the *exact* validated depth-conversion semantics of the
``data_raw -> convert_day.py`` path, instead of re-implementing a weaker,
independent depth converter.

Field mapping (replay ``depth.parquet`` row -> normalized engine record)::

    record_type            -> record_type ('snapshot_seed' | 'depth_update')
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

Ordering: replay rows are stored in raw-file order. The validated raw path
re-sorts by ``(stream_session_id, session_seq, raw_index)`` via a disk-backed
spool before replay; :func:`iter_replay_depth_records` applies the *same*
canonical sort using the same :class:`~converter.spool.RawRecordSpool`, so the
replay path matches the raw path's committed ordering while remaining
memory-bounded.

NOT reproduced here (documented equivalence caveats — replay v0 does not store
the inputs required):

  * ``sync_state`` / ``stream_lifecycle`` records (dropped by the replay
    builder), so sync_state-driven fenced ranges are not regenerated;
  * cross-day carry / synthetic opening snapshot (no prev/next repartitioning);
  * UTC-boundary repartitioning of clock-skewed records;
  * duplicate suppression.

See ``docs/FULL_L2_REPLAY_CATALOG_PLAN.md`` for the full equivalence boundary.
"""
from __future__ import annotations

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


def replay_row_to_depth_record(row: dict) -> dict:
    """Map a single replay_store depth row to a normalized engine record.

    The returned dict is JSON-serializable and carries exactly the fields the
    shared depth engine reads. ``snapshot_seed`` rows expose ``lastUpdateId``
    (recovered from the replay ``u`` column); ``depth_update`` rows expose the
    Binance continuity ids ``U`` / ``u`` / ``pu``.
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
