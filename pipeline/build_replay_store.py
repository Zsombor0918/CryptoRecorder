"""
pipeline.build_replay_store — Daily replay store builder from raw data.

Converts raw JSONL.zst data to normalized deterministic Parquet replay_store.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import zstandard as zstd

from config import DATA_ROOT, REPLAY_ROOT
from converter.readers import stream_raw_records
from converter.spool import DedupeSet
from converter.depth_phase2 import (
    _date_shift as _depth_date_shift,
    _dedupe_key as _depth_dedupe_key,
    _is_epoch_like_ns as _depth_is_epoch_like_ns,
    _target_bounds_ns as _depth_target_bounds_ns,
    _ts_event_ns as _depth_ts_event_ns,
)
from stores.replay_writer import (
    ReplayWriter,
    validate_partition,
    validate_v2_source_identity,
)

logger = logging.getLogger(__name__)

# Suppress verbose library logs
logging.getLogger("pyarrow").setLevel(logging.WARNING)


# ============================================================================
# issue #20 Phase 7 semantic-oracle correction: depth_v2 cross-day event-time
# repartitioning.
#
# convert_day.py's reference route (converter.depth_phase2._spool_repartitioned_records)
# does NOT read a UTC calendar day's depth_v2 raw data from that day's raw
# directory alone. The recorder writes hourly-rotated raw files keyed by
# WALL-CLOCK RECEIVE time; a depth_update whose EXCHANGE event time is late on
# day D (e.g. 23:59:56Z) can be physically written into day D+1's first hourly
# file if the record was received/flushed a few seconds after local midnight
# (ordinary network/processing latency, not a defect). convert_day.py corrects
# for this by scanning D-1, D, and D+1's raw depth_v2 directories and assigning
# every record to whichever UTC day its EVENT time (not its physical storage
# location) falls in — see converter.depth_phase2._spool_repartitioned_records,
# reused here via the imports above so this module applies the EXACT SAME
# rule, never an invented one. This does NOT apply to trade_v2 records:
# convert_day.py's convert_trades_streaming() reads only the single requested
# date's trade_v2 directory, with no cross-day repartitioning at all — trades
# in this raw dataset do not exhibit event-time skew across the hourly file
# boundary in a way the reference route corrects for, so this module must not
# invent trade repartitioning either (see REPARTITIONING CONTRACT below).
#
# Applies identically to schema_version 0, 1, and 2 — this is a raw INPUT
# SELECTION correction (which raw records populate one date's replay
# partition), not a physical replay schema or storage-format change.
# ============================================================================


def check_depth_repartition_readiness(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    *,
    require_complete_next_day: bool = False,
) -> "Optional[str]":
    """Return a reason if the selected raw-readiness policy is not met.

    ``None`` means the target is ready under the selected production or
    offline-validation policy; the policies' different proof boundaries are
    explicit below.

    The production daily-build timer builds "yesterday" (date D) at
    01:00 UTC. The recorder names the first D+1 depth file exactly
    ``{D+1}T00.jsonl`` and, before opening a later hourly path, closes
    that handle and queues it for compression. Therefore readiness
    requires both the exact first-hour file and positive evidence that it
    is no longer active: either only its completed compressed form remains
    or a later D+1 hourly file exists. An arbitrary D+1 file, or a bare
    uncompressed T00 file with no later hour, is not sufficient.

    The default is the production-timer readiness policy. It assumes the live
    websocket receive delay cannot span beyond D+1's first hour; the recorder
    does not enforce a formal maximum delay, so this is an operational
    assumption rather than a source-completeness proof.

    Offline semantic validation passes ``require_complete_next_day=True``.
    That stricter mode requires proof D+1's last hour has closed, so the full
    adjacent-day scope scanned by the reference converter is immutable before
    candidate construction.

    D-1 is not required to exist (the very first day of a venue/symbol's
    recorded history has no prior day) — matching
    ``_spool_repartitioned_records``'s own behavior of scanning a missing
    prior-day directory as an empty source (``stream_raw_records`` yields
    nothing for a nonexistent date directory; no exception is raised).
    """
    next_date = _depth_date_shift(date, 1)
    next_dir = data_root / venue / "depth_v2" / symbol / next_date
    if not next_dir.exists():
        return (
            f"depth_v2 raw directory for the next UTC day ({next_date}) does "
            f"not exist yet at {next_dir} -- cannot yet prove no late-{date} "
            "event-time-skewed record is still pending under it. Defer this "
            "partition until at least the next day's first hourly raw file "
            "is written."
        )
    first_hour_base = next_dir / f"{next_date}T00.jsonl"
    first_hour_variants = [
        path
        for path in (
            first_hour_base,
            Path(f"{first_hour_base}.zst"),
            Path(f"{first_hour_base}.gz"),
        )
        if path.is_file()
    ]
    if not first_hour_variants:
        return (
            f"depth_v2 raw directory for the next UTC day ({next_date}) "
            f"exists at {next_dir} but does not contain the required first "
            f"hour file ({next_date}T00.jsonl[.zst|.gz]) -- defer this "
            "partition; an arbitrary later-hour file does not prove the "
            "cross-day input is complete."
        )

    # CompressionManager writes the compressed sibling before removing the
    # closed .jsonl source. Seeing both variants can mean compression is
    # still in progress (or failed part-way), and stream_raw_records() would
    # select both. Refuse that ambiguous layout.
    if len(first_hour_variants) != 1:
        return (
            f"depth_v2 first-hour raw file for {next_date} has multiple "
            f"coexisting variants at {next_dir}: "
            f"{[path.name for path in first_hour_variants]}. Compression "
            "may still be active or incomplete; defer until exactly one "
            "stable variant remains."
        )

    first_hour_path = first_hour_variants[0]
    first_hour_closed = first_hour_path.suffix in {".zst", ".gz"}
    if first_hour_closed:
        # The recorder removes the .jsonl source only after the compressed
        # writer has closed successfully, so a sole compressed variant is
        # positive closed-file evidence.
        pass
    else:
        later_hour_exists = any(
            path.is_file()
            for hour in range(1, 24)
            for path in (
                next_dir / f"{next_date}T{hour:02d}.jsonl",
                next_dir / f"{next_date}T{hour:02d}.jsonl.zst",
                next_dir / f"{next_date}T{hour:02d}.jsonl.gz",
            )
        )
        first_hour_closed = later_hour_exists
    if not first_hour_closed:
        return (
            f"depth_v2 first-hour raw file {first_hour_path} exists but is "
            "still the latest uncompressed hourly path, so the recorder may "
            "still have it open. Defer until its completed compressed form "
            "is the sole variant or a later D+1 hourly file proves T00 was "
            "rotated and closed."
        )

    if not require_complete_next_day:
        return None

    last_hour_base = next_dir / f"{next_date}T23.jsonl"
    last_hour_variants = [
        path
        for path in (
            last_hour_base,
            Path(f"{last_hour_base}.zst"),
            Path(f"{last_hour_base}.gz"),
        )
        if path.is_file()
    ]
    if len(last_hour_variants) != 1:
        return (
            f"offline validation requires exactly one closed last-hour raw "
            f"file for {next_date}, but found "
            f"{[path.name for path in last_hour_variants]} at {next_dir}. "
            "Refusing to treat a partial or ambiguous D+1 scope as complete."
        )
    last_hour_path = last_hour_variants[0]
    if last_hour_path.suffix in {".zst", ".gz"}:
        return None

    following_date = _depth_date_shift(next_date, 1)
    following_dir = data_root / venue / "depth_v2" / symbol / following_date
    following_hour_exists = any(
        path.is_file()
        for path in (
            following_dir / f"{following_date}T00.jsonl",
            following_dir / f"{following_date}T00.jsonl.zst",
            following_dir / f"{following_date}T00.jsonl.gz",
        )
    )
    if following_hour_exists:
        return None
    return (
        f"offline validation found uncompressed last-hour raw file "
        f"{last_hour_path} with no following-day T00 path proving it was "
        "rotated and closed. Refusing an unproven D+1 scope."
    )


def _stream_repartitioned_depth_records(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    *,
    strict: bool = False,
):
    """Yield (raw_index, raw_record, source_date) triples for ``date``'s
    depth_v2 channel, applying the EXACT SAME event-time repartitioning
    rule as ``convert_day.py``'s reference route
    (``converter.depth_phase2._spool_repartitioned_records``): scans
    ``date``'s previous, target, and next UTC-day raw depth_v2 directories
    (in that order), assigns each record to the target partition if and
    only if its canonical event time satisfies
    ``target_start_ns <= event_time_ns < target_end_ns``, and applies the
    reference's own deduplication key
    (``converter.depth_phase2._dedupe_key``) so a record physically
    duplicated across adjacent directories (should that ever occur) is
    never double-counted.

    ``raw_index`` is a single incrementing counter across the full
    prev/target/next scan (matching the reference's own ``target_index``
    semantics) — NOT the record's original raw_index within its own
    physical source file. ``source_date`` (the UTC calendar day the
    record was physically stored under, which may differ from ``date``
    itself for repartitioned records) is yielded alongside so callers can
    build exact source-file identity and contribution ranges.

    Test/epoch-like timestamps (``_depth_is_epoch_like_ns``) are handled
    exactly as the reference does: only accepted when physically stored
    under ``date`` itself, never repartitioned in from an adjacent day —
    existing small-relative-timestamp unit fixtures must not silently
    change behavior.

    Bounded memory throughout: the dedupe key set is a disk-backed
    ``converter.spool.DedupeSet`` (SQLite), never an in-memory Python set
    growing with day size.
    """
    target_start_ns, target_end_ns = _depth_target_bounds_ns(date)
    prev_date = _depth_date_shift(date, -1)
    next_date = _depth_date_shift(date, 1)
    scan_dates = [prev_date, date, next_date]

    target_index = 0
    with DedupeSet(prefix="cryptorecorder-replaybuild-depth-dedupe-") as seen_target:
        for source_date in scan_dates:
            records = (
                _stream_raw_records_strict(
                    venue,
                    symbol,
                    "depth_v2",
                    source_date,
                    data_root,
                )
                if strict
                else stream_raw_records(
                    venue,
                    symbol,
                    "depth_v2",
                    source_date,
                    root=data_root,
                )
            )
            for rec in records:
                item = dict(rec)
                item["record_type"] = item.get("record_type", "depth_update")
                ts_ns = _depth_ts_event_ns(item)

                if not _depth_is_epoch_like_ns(ts_ns):
                    if source_date == date:
                        key = _depth_dedupe_key(item)
                        if not seen_target.add(key):
                            continue
                        yield target_index, item, source_date
                        target_index += 1
                    continue

                if target_start_ns <= ts_ns < target_end_ns:
                    key = _depth_dedupe_key(item)
                    if not seen_target.add(key):
                        continue
                    yield target_index, item, source_date
                    target_index += 1
        seen_target.commit()


def replay_partition_has_source_records(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
) -> bool:
    """Return whether a replay partition has any depth or trade input.

    Depth uses the same D-1/D/D+1 event-time repartitioning as the builder,
    so a physically absent ``date`` directory alone is not enough to prove a
    pre-listing day is truly empty. This bounded probe is used by validation
    orchestration to distinguish a legitimate pre-listing previous day from
    a failed previous-day build; it never permits a failed non-empty carry
    partition to be silently ignored.
    """
    depth_records = _stream_repartitioned_depth_records(
        venue, symbol, date, data_root
    )
    try:
        if next(depth_records, None) is not None:
            return True
    finally:
        depth_records.close()

    trade_records = stream_raw_records(
        venue, symbol, "trade_v2", date, root=data_root
    )
    try:
        return next(trade_records, None) is not None
    finally:
        trade_records.close()


def _iter_raw_file_records(file_path: Path):
    """Yield parsed JSON dicts from a single raw file, decompressing
    ``.zst``/``.gz`` transparently (mirrors
    ``converter.readers.stream_raw_records``'s per-file opener logic
    exactly, but at single-file granularity so callers can track which
    file each record came from)."""
    if file_path.suffix == ".zst":
        opener = lambda: zstd.open(file_path, "rt", errors="ignore")
    elif file_path.suffix == ".gz":
        opener = lambda: gzip.open(file_path, "rt", errors="ignore")
    else:
        opener = lambda: open(file_path, "r", errors="ignore")
    with opener() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _stream_raw_records_strict(
    venue: str,
    symbol: str,
    channel: str,
    date: str,
    data_root: Path,
):
    """Strict build-side equivalent of ``stream_raw_records``.

    It uses the same selected-file glob/order, decompression behavior, and
    malformed-JSON skipping, but propagates file I/O/decompression failures
    and rejects ambiguous compression siblings. Schema v2 uses this iterator
    so a transient reader error cannot turn into a silently truncated replay
    partition even when pre/post file hashes happen to match.
    """
    from pipeline.raw_manifest import (
        _assert_no_compression_variants,
        _iter_consumed_raw_files,
    )

    channel_dir = data_root / venue / channel / symbol / date
    if not channel_dir.exists():
        return
    selected_files = _iter_consumed_raw_files(channel_dir)
    _assert_no_compression_variants(
        selected_files,
        context=f"{venue}/{channel}/{symbol}/{date}",
    )
    for file_path in selected_files:
        try:
            yield from _iter_raw_file_records(file_path)
        except Exception as exc:
            raise RuntimeError(
                f"Could not read selected raw input {file_path} during "
                f"schema-version-2 streaming for "
                f"{venue}/{symbol}/{date}/{channel}: {exc}"
            ) from exc


def compute_depth_repartitioned_source_identity(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    *,
    include_record_counts: bool = False,
    strict: bool = False,
) -> dict:
    """Compute the depth_v2 ``source_identity`` channel entry in the SAME
    repartitioned ``raw_index`` space that
    ``_stream_repartitioned_depth_records`` actually produces (issue #20
    Phase 7 semantic-oracle correction).

    This exists because ``pipeline.raw_manifest.compute_raw_source_identity``'s
    existing ``record_range`` computation counts records per-file for a
    SINGLE date only — that numbering no longer matches ``raw_index`` once
    depth_v2 records are repartitioned across D-1/D/D+1 (a record physically
    stored under D+1 can now be assigned a ``raw_index`` position interleaved
    among D's own records). This function performs the exact same
    scan/filter/dedup algorithm as ``_stream_repartitioned_depth_records``,
    but per-file (via ``_iter_raw_file_records``, tracking file boundaries
    directly, which the flat ``stream_raw_records`` generator does not
    expose) so each contributing physical file's exact ``record_range`` in
    the repartitioned ``raw_index`` space can be recorded — this is what
    makes ``stores.replay_writer.resolve_source_record`` map a repartitioned
    event's ``raw_index`` back to its exact physical source file (which may
    be under an adjacent UTC day) and its ordinal among that file's accepted
    contribution. The latter is not necessarily a physical JSON-line ordinal
    when the file also contains rejected adjacent-day or duplicate records.

    A second, independent bounded-memory scan pass (disk-backed
    ``DedupeSet``, never a full-day Python list/set) — matches the existing
    cost model of ``include_record_counts=True`` for schema_version=2 (an
    additional read pass over the raw data, never held fully in memory).

    Returns a ``{"channels": {"depth_v2": [...]}, "complete": bool,
    "missing_channels": [...]}`` shaped dict (matching
    ``compute_raw_source_identity``'s return shape) — each depth_v2 entry
    additionally carries ``source_date`` (the UTC calendar day the file is
    physically stored under, which may differ from ``date`` for
    repartitioned entries).
    """
    from pipeline.raw_manifest import (
        _assert_no_compression_variants,
        _iter_consumed_raw_files,
        _sha256_file,
    )

    target_start_ns, target_end_ns = _depth_target_bounds_ns(date)
    prev_date = _depth_date_shift(date, -1)
    next_date = _depth_date_shift(date, 1)
    scan_dates = [prev_date, date, next_date]

    entries: "list[dict]" = []
    target_index = 0
    with DedupeSet(prefix="cryptorecorder-replaybuild-depth-dedupe-count-") as seen_target:
        for source_date in scan_dates:
            channel_dir = data_root / venue / "depth_v2" / symbol / source_date
            if not channel_dir.exists():
                continue
            selected_files = _iter_consumed_raw_files(channel_dir)
            if strict:
                _assert_no_compression_variants(
                    selected_files,
                    context=f"{venue}/depth_v2/{symbol}/{source_date}",
                )
            for fpath in selected_files:
                try:
                    file_start_index = target_index
                    for rec in _iter_raw_file_records(fpath):
                        item = dict(rec)
                        item["record_type"] = item.get("record_type", "depth_update")
                        ts_ns = _depth_ts_event_ns(item)
                        accepted = False
                        if not _depth_is_epoch_like_ns(ts_ns):
                            if source_date == date:
                                key = _depth_dedupe_key(item)
                                if seen_target.add(key):
                                    accepted = True
                        elif target_start_ns <= ts_ns < target_end_ns:
                            key = _depth_dedupe_key(item)
                            if seen_target.add(key):
                                accepted = True
                        if accepted:
                            target_index += 1
                    if target_index > file_start_index:
                        entry = {
                            "path": fpath.relative_to(data_root).as_posix(),
                            "sha256": _sha256_file(fpath),
                            "size_bytes": fpath.stat().st_size,
                            "source_date": source_date,
                        }
                        if include_record_counts:
                            entry["record_count"] = target_index - file_start_index
                            entry["record_range"] = [file_start_index, target_index]
                        entries.append(entry)
                except Exception as exc:
                    if strict:
                        raise RuntimeError(
                            f"Could not read selected raw input {fpath} while "
                            f"computing repartitioned source identity for "
                            f"{venue}/{symbol}/{date}/depth_v2: {exc}"
                        ) from exc
                    raise
        seen_target.commit()

    return {
        "channels": {"depth_v2": entries},
        "complete": len(entries) > 0,
        "missing_channels": [] if entries else ["depth_v2"],
    }


def compute_repartitioned_source_identity(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    *,
    include_record_counts: bool = False,
    strict: bool = False,
) -> dict:
    """Merge the repartitioned depth_v2 source identity
    (:func:`compute_depth_repartitioned_source_identity`) with the
    unaffected, single-date trade_v2 source identity
    (``pipeline.raw_manifest.compute_raw_source_identity`` — trade_v2 is
    NOT repartitioned; see this module's cross-day-repartitioning docstring
    for why) into one manifest-shaped ``source_identity`` dict."""
    from pipeline.raw_manifest import compute_raw_source_identity

    depth_identity = compute_depth_repartitioned_source_identity(
        venue,
        symbol,
        date,
        data_root,
        include_record_counts=include_record_counts,
        strict=strict,
    )
    trade_identity = compute_raw_source_identity(
        venue, symbol, date, ["trade_v2"], data_root=data_root,
        include_record_counts=include_record_counts,
        strict=strict,
    )
    channels = {
        "depth_v2": depth_identity["channels"]["depth_v2"],
        "trade_v2": trade_identity["channels"]["trade_v2"],
    }
    missing = [c for c, entries in channels.items() if not entries]
    return {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "channels": channels,
        "complete": not missing,
        "missing_channels": missing,
    }


def _to_ns_from_ms(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value) * 1_000_000
    except (TypeError, ValueError):
        return None


def _event_ts_ns(raw_record: dict) -> int:
    return (
        _to_ns_from_ms(raw_record.get("ts_event_ms"))
        or _to_ns_from_ms(raw_record.get("exchange_ts_ms"))
        or _to_ns_from_ms(raw_record.get("ts_trade_ms"))
        or int(raw_record.get("ts_exchange_ns") or raw_record.get("ts_recv_ns") or 0)
    )


def _trade_event_ts_ns(raw_record: dict) -> int:
    return (
        _to_ns_from_ms(raw_record.get("ts_trade_ms"))
        or _to_ns_from_ms(raw_record.get("ts_event_ms"))
        or _to_ns_from_ms(raw_record.get("exchange_ts_ms"))
        or int(raw_record.get("ts_exchange_ns") or raw_record.get("ts_recv_ns") or 0)
    )


def _receive_ts_ns(raw_record: dict) -> int:
    return int(
        raw_record.get("ts_receive_ns")
        or raw_record.get("ts_recv_ns")
        or _event_ts_ns(raw_record)
    )


def _native_payload_hash(raw_record: dict) -> str | None:
    payload = raw_record.get("native_payload") or raw_record.get("payload")
    if payload is None:
        return None
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _as_optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _decimal_pair_to_level(level: object) -> dict:
    price = level[0]  # type: ignore[index]
    size = level[1]  # type: ignore[index]
    price_str = str(price)
    size_str = str(size)
    return {
        "price": float(price_str),
        "size": float(size_str),
        "price_str": price_str,
        "size_str": size_str,
    }


def _convert_depth_record(raw_record: dict, venue: str, symbol: str, date: str) -> Optional[dict]:
    """
    Convert raw depth record to replay schema.

    Raw schema expected:
        {
            "stream_session_id": uint64,
            "session_seq": uint64,
            "raw_index": uint32,
            "snapshot_seed": {...},  or
            "depth_update": {...},  or
            "sync_state": {...},
            ...
        }

    ``sync_state`` and ``stream_lifecycle`` records (inventoried directly
    against the ADAUSDT 2026-06-12 raw fixture: record_type values present
    are exactly ``depth_update``, ``sync_state``, ``snapshot_seed``,
    ``stream_lifecycle``) are the record types — besides the already-
    handled ``snapshot_seed``/``depth_update`` — that the shared
    depth-replay engine (``converter.depth_phase2._run_depth_replay_loop``,
    used identically by both ``convert_day.py`` and the
    replay-reconstruction path) needs for correct synchronization/desync/
    resync state and fenced-range construction:

    - ``sync_state`` records (``record_type == "sync_state"`` branch) carry
      the actual state transition (``state``/``reason``) that opens/closes
      fences on desync/resync/fenced states.
    - ``stream_lifecycle`` records are NOT read by any record_type-specific
      branch (the engine's ``if record_type == "stream_lifecycle": continue``
      is a no-op after the session-id check) — but the engine's
      session-change detection (``if state.current_stream_session_id !=
      session_id: ... close/open fence ...``) runs UNCONDITIONALLY for
      EVERY record type, before that no-op, using the CURRENT record's
      timestamp. ``stream_lifecycle`` records are the actual first
      (``session_start``) and last (``session_end``) record of every
      session in the raw data (verified: `session_seq=1` for every
      `session_start`), so dropping them shifts the observed "first
      record of the new session" to whatever record happens to follow,
      producing a systematically later fence-close timestamp than the
      reference — this was confirmed directly by diffing the reference's
      and (pre-fix) candidate's full fenced-range lists on the ADAUSDT
      2026-06-12 Tier-2 gate: 31 of 34 fences differed ONLY in
      `end_ts_ns`, each by exactly the raw gap between the dropped
      `stream_lifecycle` record and the next preserved record.

    Neither record type carries book payload; ``sync_state`` uses
    ``last_update_id``/``prev_update_id`` (not ``U``/``u``/``pu``) and
    ``stream_lifecycle`` carries only ``event``/``reason`` — none of the
    engine's record_type-specific branches need these values, only their
    presence-and-timestamp for session-change detection (``stream_lifecycle``)
    or their state-transition fields (``sync_state``, which the engine DOES
    read). Both are round-tripped through the existing, already-nullable
    ``quality_flags`` JSON column so no physical schema field is added or
    changed for either v0 or v1.
    """
    try:
        record_type = raw_record.get("record_type", "depth_update")
        if record_type not in {"snapshot_seed", "depth_update", "sync_state", "stream_lifecycle"}:
            return None

        session_id = raw_record.get("stream_session_id", 0)
        session_seq = raw_record.get("session_seq", 0)
        raw_index = raw_record.get("raw_index", 0)

        is_sync_state = record_type == "sync_state"
        is_stream_lifecycle = record_type == "stream_lifecycle"

        if is_sync_state or is_stream_lifecycle:
            bids_struct: list = []
            asks_struct: list = []
        else:
            payload = raw_record.get("payload") or {}
            bids = raw_record.get("bids", payload.get("bids", []))
            asks = raw_record.get("asks", payload.get("asks", []))

            # Parse bids/asks if they're strings (JSON-encoded)
            if isinstance(bids, str):
                bids = json.loads(bids)
            if isinstance(asks, str):
                asks = json.loads(asks)

            bids_struct = [_decimal_pair_to_level(b) for b in bids]
            asks_struct = [_decimal_pair_to_level(a) for a in asks]

        # Determine flags
        is_snapshot = record_type == "snapshot_seed"
        is_update = record_type == "depth_update"
        # A sync_state RECORD's own transition value lives in its "state"
        # field; a depth_update RECORD's embedded (legacy, informational)
        # sync marker lives in a differently-named "sync_state" field. Use
        # whichever is actually present for this record's type — conflating
        # them would silently miss real desynced/resync_required
        # transitions on sync_state records.
        transition_state = raw_record.get("state") if is_sync_state else raw_record.get("sync_state")
        is_desync = bool(raw_record.get("is_desync", False) or transition_state == "desynced")
        is_resync = bool(raw_record.get("is_resync", False) or transition_state == "resync_required")

        # Quality flags (JSON-encoded)
        quality_flags = raw_record.get("quality_flags")
        if quality_flags and isinstance(quality_flags, dict):
            quality_flags = json.dumps(quality_flags)

        if is_sync_state:
            # sync_state records have no payload/U/u/pu; preserve their
            # full state transition here instead — see the docstring above.
            quality_flags = json.dumps({
                "sync_state_transition": {
                    "state": raw_record.get("state"),
                    "previous_state": raw_record.get("previous_state"),
                    "reason": raw_record.get("reason"),
                    "last_update_id": raw_record.get("last_update_id"),
                    "prev_update_id": raw_record.get("prev_update_id"),
                }
            })
        elif is_stream_lifecycle:
            # stream_lifecycle records have no payload/U/u/pu either;
            # preserve their event/reason for completeness even though the
            # shared engine only needs their presence-and-timestamp for
            # session-change fence-close/open detection.
            quality_flags = json.dumps({
                "stream_lifecycle_event": {
                    "event": raw_record.get("event"),
                    "reason": raw_record.get("reason"),
                }
            })

        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "stream_session_id": session_id,
            "session_seq": session_seq,
            "raw_index": raw_index,
            "record_type": record_type,
            "U": _as_optional_str(raw_record.get("U")),
            "u": _as_optional_str(raw_record.get("u") or raw_record.get("lastUpdateId")),
            "pu": _as_optional_str(raw_record.get("pu")),
            "ts_exchange_ns": _event_ts_ns(raw_record),
            "ts_receive_ns": _receive_ts_ns(raw_record),
            "bids": bids_struct,
            "asks": asks_struct,
            "is_snapshot_seed": is_snapshot,
            "is_depth_update": is_update,
            "is_sync_state": is_sync_state,
            "is_desync": is_desync,
            "is_resync": is_resync,
            "quality_flags": quality_flags,
            "native_payload_hash": raw_record.get("native_payload_hash") or _native_payload_hash(raw_record),
        }
    except Exception as e:
        logger.warning(f"Error converting depth record for {venue}/{symbol}: {e}")
        return None


def _convert_trade_record(raw_record: dict, venue: str, symbol: str, date: str) -> Optional[dict]:
    """
    Convert raw trade record to replay schema.
    
    Raw schema expected:
        {
            "trade_stream_session_id": uint64,
            "trade_session_seq": uint64,
            "raw_index": uint32,
            "market_type": "spot" or "futures",
            "trade_id": str,  or
            "agg_trade_id": str,
            ...
        }
    """
    try:
        session_id = raw_record.get("trade_stream_session_id", 0)
        session_seq = raw_record.get("trade_session_seq", 0)
        raw_index = raw_record.get("raw_index", 0)
        market_type = raw_record.get("market_type", "spot")
        record_type = raw_record.get("record_type", "trade")
        if record_type not in {"trade", "agg_trade"}:
            return None

        # Trade IDs
        trade_id = raw_record.get("trade_id") or raw_record.get("exchange_trade_id")
        agg_trade_id = raw_record.get("agg_trade_id")

        # Trade details
        price_str = str(raw_record.get("price", "0"))
        quantity_str = str(raw_record.get("quantity", "0"))
        price = float(price_str)
        quantity = float(quantity_str)
        buyer_maker = bool(raw_record.get("is_buyer_maker", raw_record.get("buyer_maker", False)))
        aggressor_side = raw_record.get("aggressor_side")

        # Quality flags
        quality_flags = raw_record.get("quality_flags")
        if quality_flags and isinstance(quality_flags, dict):
            quality_flags = json.dumps(quality_flags)

        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "trade_stream_session_id": session_id,
            "trade_session_seq": session_seq,
            "raw_index": raw_index,
            "record_type": record_type,
            "market_type": market_type,
            "trade_id": _as_optional_str(trade_id),
            "agg_trade_id": _as_optional_str(agg_trade_id),
            "ts_exchange_ns": _trade_event_ts_ns(raw_record),
            "ts_receive_ns": _receive_ts_ns(raw_record),
            "price": price,
            "quantity": quantity,
            "price_str": price_str,
            "quantity_str": quantity_str,
            "buyer_maker": buyer_maker,
            "aggressor_side": aggressor_side,
            "quality_flags": quality_flags,
            "native_payload_hash": raw_record.get("native_payload_hash") or _native_payload_hash(raw_record),
        }
    except Exception as e:
        logger.warning(f"Error converting trade record for {venue}/{symbol}: {e}")
        return None


def _partition_is_valid(
    replay_root: Path,
    venue: str,
    symbol: str,
    date: str,
    *,
    _candidate: "Path | None" = None,
) -> bool:
    """Return True only if the partition is complete with a valid manifest and files.

    Pass _candidate to validate an alternate directory (e.g. a backup) instead
    of the canonical output location. Delegates to
    stores.replay_writer.validate_partition() so ReplayWriter's post-publish
    check and this skip-if-valid/crash-recovery check share one definition.
    """
    out_dir = _candidate or replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    return validate_partition(out_dir)


# ---------------------------------------------------------------------------
# Partition crash-recovery state machine
# ---------------------------------------------------------------------------

class _RecoveryAction:
    """Return value from recover_partition_state()."""
    __slots__ = ("action", "message")

    def __init__(self, action: str, message: str) -> None:
        # action: "skip" | "rebuild" | "fail"
        self.action = action
        self.message = message

    def __repr__(self) -> str:
        return f"_RecoveryAction(action={self.action!r}, message={self.message!r})"


def recover_partition_state(
    replay_root: Path,
    venue: str,
    symbol: str,
    date: str,
) -> _RecoveryAction:
    """
    Examine the filesystem state for one partition and return the required action.

    Handles every combination of canonical output, backup, and their validity:

    Case A: output missing, backup valid
        Restore backup → canonical output.
        Returns action="skip" after successful restore (partition is now valid).
        Returns action="fail" if restore fails (manual intervention required).

    Case B: output missing, backup invalid
        Preserve invalid backup for operator inspection.
        Returns action="fail" (do not silently delete and rebuild).

    Case C: output valid, backup exists
        Canonical is authoritative; delete stale backup best-effort.
        Returns action="skip".

    Case D: output invalid, backup valid
        Quarantine invalid output; restore valid backup to canonical.
        Returns action="skip" after successful restore.
        Returns action="fail" if restore fails.

    Case E: output invalid, backup invalid
        Preserve both for inspection.
        Returns action="fail".

    Case F: output valid, no backup
        Normal valid state.
        Returns action="skip".

    Case G: output missing, no backup
        Normal missing state.
        Returns action="rebuild".
    """
    import shutil as _shutil

    partition_dir = replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    backup_dir = replay_root / f"venue={venue}" / f"symbol={symbol}" / f".backup_{date}_{symbol}"

    output_exists = partition_dir.exists()
    backup_exists = backup_dir.exists()
    output_valid = output_exists and _partition_is_valid(replay_root, venue, symbol, date)
    backup_valid = backup_exists and _partition_is_valid(
        replay_root, venue, symbol, date, _candidate=backup_dir
    )

    # --- Case F / G: no backup ---
    if not backup_exists:
        if output_valid:
            return _RecoveryAction("skip", "Partition is complete and valid.")
        if not output_exists:
            return _RecoveryAction("rebuild", "No partition or backup; will build.")
        # output exists but invalid, no backup
        return _RecoveryAction(
            "rebuild",
            f"Partition {partition_dir} exists but is invalid; no backup. Will rebuild."
        )

    # --- Cases with backup present ---

    if output_valid and backup_valid:
        # Case F extended: both valid — canonical is authoritative.
        try:
            _shutil.rmtree(backup_dir)
            logger.info(f"Removed stale backup (canonical is valid): {backup_dir}")
        except Exception as e:
            logger.warning(f"Could not remove stale backup {backup_dir}: {e}")
        return _RecoveryAction("skip", "Canonical partition is valid; stale backup cleaned up.")

    if output_valid and not backup_valid:
        # Case C: canonical valid, stale/invalid backup.
        try:
            _shutil.rmtree(backup_dir)
            logger.info(f"Removed invalid backup (canonical is valid): {backup_dir}")
        except Exception as e:
            logger.warning(f"Could not remove backup {backup_dir}: {e}")
        return _RecoveryAction("skip", "Canonical partition is valid.")

    if not output_exists and backup_valid:
        # Case A: mid-publish SIGKILL — restore valid backup.
        logger.warning(
            f"Crash-recovery (Case A): output missing, valid backup present. "
            f"Restoring {backup_dir} -> {partition_dir}"
        )
        partition_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(backup_dir, partition_dir)
        except Exception as restore_err:
            return _RecoveryAction(
                "fail",
                f"Crash-recovery: restore of {backup_dir} -> {partition_dir} failed: "
                f"{restore_err}. Manual intervention required."
            )
        logger.info(f"Crash-recovery complete: {partition_dir}")
        return _RecoveryAction("skip", f"Restored backup to {partition_dir}.")

    if not output_exists and not backup_valid:
        # Case B: output missing, backup invalid.
        logger.error(
            f"Crash-recovery (Case B): output missing, backup {backup_dir} is invalid. "
            "Preserving backup for operator inspection. "
            "Rebuild requires manual removal of the invalid backup or --force."
        )
        return _RecoveryAction(
            "fail",
            f"Crash-recovery: output missing and backup {backup_dir} is invalid. "
            "Manual inspection required before rebuilding."
        )

    if output_exists and not output_valid and backup_valid:
        # Case D: canonical invalid, valid backup available — quarantine and restore.
        quarantine_dir = (
            replay_root / f"venue={venue}" / f"symbol={symbol}"
            / f".quarantine_{date}_{symbol}"
        )
        logger.warning(
            f"Crash-recovery (Case D): canonical {partition_dir} is invalid; "
            f"valid backup present. Quarantining invalid output, restoring backup."
        )
        try:
            if quarantine_dir.exists():
                _shutil.rmtree(quarantine_dir)
            os.replace(partition_dir, quarantine_dir)
        except Exception as qe:
            return _RecoveryAction(
                "fail",
                f"Crash-recovery: could not quarantine invalid {partition_dir}: {qe}. "
                "Manual intervention required."
            )
        try:
            os.replace(backup_dir, partition_dir)
        except Exception as restore_err:
            # Restore failed — try to un-quarantine.
            try:
                os.replace(quarantine_dir, partition_dir)
            except Exception:
                pass
            return _RecoveryAction(
                "fail",
                f"Crash-recovery: restore of {backup_dir} -> {partition_dir} failed: "
                f"{restore_err}. Manual intervention required."
            )
        logger.info(f"Crash-recovery complete: restored {partition_dir}.")
        # Remove quarantined invalid copy best-effort.
        try:
            _shutil.rmtree(quarantine_dir)
        except Exception as e:
            logger.warning(f"Could not remove quarantined copy {quarantine_dir}: {e}")
        return _RecoveryAction("skip", f"Restored valid backup to {partition_dir}.")

    # Case E: output invalid (or missing), backup invalid.
    logger.error(
        f"Crash-recovery (Case E): both canonical ({partition_dir}) and "
        f"backup ({backup_dir}) are invalid or missing. "
        "Preserving both for operator inspection."
    )
    return _RecoveryAction(
        "fail",
        f"Both canonical and backup are invalid/missing for {venue}/{symbol}/{date}. "
        "Manual inspection required."
    )


def build_replay_for_symbol(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    replay_root: Path,
    *,
    force: bool = False,
    schema_version: int = 0,
    price_scale: "Optional[int]" = None,
    qty_scale: "Optional[int]" = None,
    check_repartition_readiness: bool = False,
    require_complete_next_day: bool = False,
) -> dict:
    """
    Build replay store for a single venue/symbol/date.

    Skips partitions that already have a complete, checksum-valid manifest so
    that restarted runs make durable progress without rebuilding earlier work.
    Pass force=True to rebuild even when a valid partition already exists (use
    after raw data has been repaired or backfilled).

    Args:
        schema_version: 0 (default — legacy v0, unchanged production
            behavior; every existing caller that omits this argument keeps
            producing exactly today's output), 1 (the issue #20 Phase 5
            compact prototype schema), or 2 (the issue #20 Phase 7
            hierarchical-integrity candidate). The compact schemas are
            intended for development validation only and are not wired into
            a systemd unit or production config. Any other value raises
            immediately via ``ReplayWriter``'s own constructor check — never
            silently falls back to v0.
        price_scale / qty_scale: used by schema_version 1 and 2; forwarded to
            ``ReplayWriter`` (see its docstring for the derivation fallback).
        check_repartition_readiness: when True, refuse (return
            ``status="deferred"``, never publish) to build this partition
            until the NEXT UTC day's depth_v2 raw directory has at least
            one hourly file present (see
            ``check_depth_repartition_readiness`` for the exact rule and
            rationale: a depth_update whose exchange event time is late on
            ``date`` can be physically written into ``date+1``'s raw
            files by ordinary recorder receive-latency, and
            ``convert_day.py``'s reference route already scans D-1/D/D+1
            to assign every record to its true event-time day — see
            ``_stream_repartitioned_depth_records``, always applied to the
            depth_v2 channel regardless of this flag). Defaults to
            ``False`` for backward compatibility with existing
            single-day-only callers/test fixtures that never populate an
            adjacent-day raw directory; all operator/validation entry
            points (this module's CLI ``main()``, ``pipeline.daily_build``,
            and ``validation.validate_catalog_equivalence``) explicitly
            pass ``True``. Depth
            records themselves are ALWAYS repartitioned across D-1/D/D+1
            when present, independent of this flag — this flag controls
            only whether an ABSENT/not-yet-started D+1 blocks publication.
        require_complete_next_day: when readiness checking is enabled,
            require proof that D+1's final hour is closed. Canonical offline
            semantic validation enables this; the 01:00 production timer
            cannot because D+1 is still in progress.

    Returns:
        Status dict with counts and errors.
    """
    if schema_version not in (0, 1, 2):
        # Preserve the public fail-fast contract even though writer creation
        # now lives inside the staging-cleanup scope below.
        raise ValueError(
            f"Unsupported schema_version={schema_version!r} "
            "(supported: 0, 1, 2)"
        )

    status = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "status": "success",
        "depth_count": 0,
        "trade_count": 0,
        "errors": [],
    }

    import shutil as _shutil

    staging_dir = (
        replay_root / f"venue={venue}" / f"symbol={symbol}"
        / f".staging_{date}_{symbol}"
    )

    # ---------------------------------------------------------------
    # Crash-recovery: handle all possible backup/output state combinations
    # before doing anything else. This runs even when force=True: --force
    # means "rebuild even when a valid canonical partition exists", not
    # "delete recovery copies before a replacement is valid". Invalid or
    # ambiguous states (recovery.action == "fail") must still fail closed
    # under --force so a valid backup/canonical copy is never silently
    # destroyed. The normal publish() flow (backup <- canonical <- staging)
    # protects the current valid partition through the forced rebuild
    # without any separate pre-build backup deletion.
    # ---------------------------------------------------------------
    recovery = recover_partition_state(replay_root, venue, symbol, date)
    if recovery.action == "fail":
        status["status"] = "failed"
        status["errors"].append(recovery.message)
        logger.error(recovery.message)
        return status
    if recovery.action == "skip" and not force:
        # Partition is valid (possibly just restored from backup), but it is
        # reusable only for the schema the caller actually requested.
        manifest_path = (
            replay_root
            / f"venue={venue}"
            / f"symbol={symbol}"
            / f"date={date}"
            / "manifest.json"
        )
        try:
            with open(manifest_path) as manifest_file:
                existing_manifest = json.load(manifest_file)
        except Exception as exc:
            status["status"] = "failed"
            status["errors"].append(
                f"Could not read validated existing manifest "
                f"{manifest_path}: {exc}"
            )
            return status
        existing_schema_version = existing_manifest.get("schema_version", 0)
        if existing_schema_version != schema_version:
            status["status"] = "failed"
            status["errors"].append(
                f"Existing valid partition {venue}/{symbol}/{date} uses "
                f"schema_version={existing_schema_version!r}, but the caller "
                f"requested schema_version={schema_version!r}. Refusing to "
                "reuse or overwrite it implicitly; use force=True for an "
                "intentional replacement."
            )
            logger.error(status["errors"][-1])
            return status

        if schema_version == 2:
            # A v2 partition's replacement for the per-event payload hash is
            # its exact raw-source identity. Reuse therefore requires the same
            # readiness proof as a fresh build and a fresh, strict identity
            # scan of the currently selected raw files.
            if check_repartition_readiness:
                readiness_reason = check_depth_repartition_readiness(
                    venue,
                    symbol,
                    date,
                    data_root,
                    require_complete_next_day=require_complete_next_day,
                )
                if readiness_reason is not None:
                    status["status"] = "deferred"
                    status["errors"].append(readiness_reason)
                    logger.info(
                        f"Deferring reuse of {venue}/{symbol}/{date}: "
                        f"{readiness_reason}"
                    )
                    return status
            try:
                live_source_identity = compute_repartitioned_source_identity(
                    venue,
                    symbol,
                    date,
                    data_root,
                    include_record_counts=True,
                    strict=True,
                )
                validate_v2_source_identity(
                    live_source_identity,
                    venue,
                    symbol,
                    date,
                )
            except Exception as exc:
                status["status"] = "failed"
                status["errors"].append(
                    f"Cannot reuse schema_version=2 partition "
                    f"{venue}/{symbol}/{date}: live raw source identity could "
                    f"not be proven: {exc}"
                )
                logger.error(status["errors"][-1])
                return status
            if existing_manifest.get("source_identity") != live_source_identity:
                status["status"] = "failed"
                status["errors"].append(
                    f"Cannot reuse schema_version=2 partition "
                    f"{venue}/{symbol}/{date}: current selected raw source "
                    "identity differs from the published manifest. Refusing "
                    "silent reuse or automatic replacement."
                )
                logger.error(status["errors"][-1])
                return status

        logger.info(
            f"Skipping already-complete partition: {venue}/{symbol}/{date}"
        )
        status["status"] = "skipped"
        return status
    # action == "rebuild", or (action == "skip" and force) -- fall through to
    # build. In the force+skip case, recover_partition_state has already
    # resolved any crash-left backup/canonical ambiguity, and the current
    # valid canonical output (if any) will be moved into the backup slot by
    # publish() itself, then deleted only after the replacement validates.

    # issue #20 Phase 7 semantic-oracle correction: depth_v2 cross-day
    # event-time repartitioning readiness. The reference route
    # (convert_day.py) requires D-1/D/D+1's raw depth_v2 data to correctly
    # assign every record to its true UTC event-time day; a replay
    # partition built before D+1's raw data has even started being written
    # would silently omit any late-D event-time-skewed record that lands
    # under D+1. Defer rather than knowingly publishing an input-incomplete
    # partition. The programmatic default remains disabled for existing
    # single-day library fixtures; every operator and canonical validation
    # entry point enables it explicitly.
    readiness_reason = None
    if check_repartition_readiness:
        readiness_reason = check_depth_repartition_readiness(
            venue,
            symbol,
            date,
            data_root,
            require_complete_next_day=require_complete_next_day,
        )
    if readiness_reason is not None:
        status["status"] = "deferred"
        status["errors"].append(readiness_reason)
        logger.info(f"Deferring {venue}/{symbol}/{date}: {readiness_reason}")
        return status

    # Remove stale staging directory from a previous SIGKILL so it cannot be
    # confused with a successful previous build.
    if staging_dir.exists():
        logger.info(f"Removing stale staging dir: {staging_dir}")
        try:
            _shutil.rmtree(staging_dir)
        except Exception as exc:
            status["status"] = "failed"
            status["errors"].append(
                f"Failed to remove stale staging dir {staging_dir}: {exc}"
            )
            logger.error(status["errors"][-1])
            return status
        if staging_dir.exists():
            status["status"] = "failed"
            status["errors"].append(
                f"Failed to remove stale staging dir {staging_dir}; "
                "refusing to build on top of stale files."
            )
            logger.error(status["errors"][-1])
            return status

    writer: "ReplayWriter | None" = None
    pre_build_raw_identity = None
    try:
        writer = ReplayWriter(
            replay_root, venue, symbol, date,
            schema_version=schema_version,
            price_scale=price_scale,
            qty_scale=qty_scale,
            data_root=data_root,
        )

        # issue #20 Phase 7 review point 6 (time-of-check/time-of-use):
        # snapshot the selected raw files' identity BEFORE streaming begins,
        # so a change during the build window can be detected against the
        # second snapshot below. This is inside the cleanup scope because the
        # writer has already created its staging directory.
        if schema_version in (1, 2):
            pre_build_raw_identity = compute_repartitioned_source_identity(
                venue,
                symbol,
                date,
                data_root,
                strict=(schema_version == 2),
            )
            if schema_version == 2 and not pre_build_raw_identity.get("complete"):
                raise RuntimeError(
                    f"Cannot build schema_version=2 replay for "
                    f"{venue}/{symbol}/{date}: selected raw source identity "
                    "is incomplete before streaming"
                )
        # Stream depth records (issue #20 Phase 7: cross-day event-time
        # repartitioned, matching convert_day.py's reference rule exactly —
        # see _stream_repartitioned_depth_records's docstring. raw_index is
        # the repartitioned scan-order index (matching the reference's own
        # target_index semantics), NOT necessarily the record's ordinal
        # within its own physical source file.
        depth_batch = []
        for raw_index, raw_record, _source_date in _stream_repartitioned_depth_records(
            venue,
            symbol,
            date,
            data_root,
            strict=(schema_version == 2),
        ):
            raw_record = dict(raw_record)
            raw_record["raw_index"] = raw_index
            converted = _convert_depth_record(raw_record, venue, symbol, date)
            if converted:
                depth_batch.append(converted)
                if len(depth_batch) >= 5000:
                    writer.write_depth_batch(depth_batch)
                    depth_batch = []
        if depth_batch:
            writer.write_depth_batch(depth_batch)

        # Stream trade records
        trade_batch = []
        trade_records = (
            _stream_raw_records_strict(
                venue,
                symbol,
                "trade_v2",
                date,
                data_root,
            )
            if schema_version == 2
            else stream_raw_records(
                venue,
                symbol,
                "trade_v2",
                date,
                root=data_root,
            )
        )
        for raw_index, raw_record in enumerate(trade_records):
            raw_record = dict(raw_record)
            raw_record.setdefault("raw_index", raw_index)
            converted = _convert_trade_record(raw_record, venue, symbol, date)
            if converted:
                trade_batch.append(converted)
                if len(trade_batch) >= 5000:
                    writer.write_trades_batch(trade_batch)
                    trade_batch = []
        if trade_batch:
            writer.write_trades_batch(trade_batch)

        if schema_version in (1, 2):
            # Issue #20 Phase 5 correction: source identity must reflect the
            # EXACT data_root/channels this build actually consumed above
            # (depth_v2, trade_v2) — never independently recomputed by
            # ReplayWriter against the global config.DATA_ROOT, which would
            # silently record checksums from a different raw root than a
            # custom --data-root build actually used.
            #
            # schema_version=2 (issue #20 Phase 7 hierarchical-integrity
            # candidate) additionally requires per-file record_count/
            # record_range, so a replay event's raw_index can be mapped
            # back to its exact source file deterministically (see
            # stores.replay_writer.resolve_source_record) — this replaces
            # the removed per-event native_payload_hash.
            post_build_raw_identity = compute_repartitioned_source_identity(
                venue, symbol, date, data_root,
                include_record_counts=(schema_version == 2),
                strict=(schema_version == 2),
            )

            # issue #20 Phase 7 review point 6 (TOCTOU): compare the
            # pre-streaming snapshot against this post-streaming one
            # (path/sha256/size_bytes only — record_count/record_range are
            # v2-only and not part of the pre-build snapshot). Any
            # difference means the raw files were mutated WHILE this build
            # was streaming/converting them, so the manifest we are about
            # to write would describe raw bytes that do not match what was
            # actually read. Fail closed rather than publish a manifest
            # for different raw bytes than were streamed.
            def _identity_fingerprint(identity: dict) -> dict:
                return {
                    channel: sorted(
                        (e["path"], e["sha256"], e["size_bytes"])
                        for e in entries
                    )
                    for channel, entries in identity.get("channels", {}).items()
                }

            if _identity_fingerprint(pre_build_raw_identity) != _identity_fingerprint(post_build_raw_identity):
                raise RuntimeError(
                    f"Raw source files for {venue}/{symbol}/{date} changed "
                    "during this build (time-of-check/time-of-use hazard): "
                    "the pre-streaming and post-streaming raw file "
                    "checksums/sizes differ. Refusing to publish a manifest "
                    "that would describe different raw bytes than were "
                    "actually streamed/converted. Rebuild from a stable raw "
                    "snapshot."
                )

            writer.set_source_identity(post_build_raw_identity)

        # Load instrument metadata if available
        instrument_metadata = None
        try:
            exchangeinfo_records = list(
                stream_raw_records(venue, "EXCHANGEINFO", "exchangeinfo", date, root=data_root)
            )
            if exchangeinfo_records:
                last_record = exchangeinfo_records[-1]
                symbol_info = None
                for item in last_record.get("symbols", []):
                    if item.get("symbol") == symbol:
                        symbol_info = item
                        break
                symbol_info = symbol_info or {}
                instrument_metadata = {
                    "venue": venue,
                    "symbol": symbol,
                    "market_type": "spot" if "BINANCE_SPOT" in venue else "perpetual",
                    "instrument_id": (
                        f"{symbol}.BINANCE"
                        if "BINANCE_SPOT" in venue
                        else f"{symbol}-PERP.BINANCE"
                    ),
                    "raw_symbol": symbol,
                    "quote_asset": symbol_info.get("quoteAsset", "USDT"),
                    "base_asset": symbol_info.get("baseAsset", symbol.replace("USDT", "")),
                    # Preserve the raw exchangeInfo filters (PRICE_FILTER,
                    # LOT_SIZE, etc.) so downstream Nautilus instrument
                    # reconstruction (validation.replay_catalog_reconstruct's
                    # build_instruments()) derives the SAME price/size
                    # precision as the reference convert_day.py path,
                    # instead of silently falling back to
                    # converter.instruments._default_info()'s generic
                    # defaults. Not previously included — a pre-existing
                    # gap in the canonical builder's instrument metadata,
                    # independent of replay schema_version, fixed here
                    # because it otherwise blocks the canonical
                    # instrument-precision gate for ANY replay-based
                    # candidate (v0 or v1).
                    "filters": symbol_info.get("filters", []),
                }
        except Exception as e:
            logger.warning(f"Could not load instrument metadata for {venue}/{symbol}: {e}")

        # Finalize and publish
        writer.finalize_staging()
        writer.publish(instrument_metadata)

        status["depth_count"] = writer.depth_count
        status["trade_count"] = writer.trade_count

        logger.info(
            f"✓ Built replay: {venue}/{symbol}/{date} "
            f"({writer.depth_count} depth, {writer.trade_count} trades)"
        )

    except Exception as primary_error:
        status["status"] = "failed"
        status["errors"].append(str(primary_error))
        logger.error(
            f"Failed to build replay for {venue}/{symbol}/{date}: {primary_error}"
        )
        try:
            if writer is not None:
                writer.cleanup_staging()
        except Exception as cleanup_error:
            status["errors"].append(f"Staging cleanup also failed: {cleanup_error}")
            logger.error(
                f"Staging cleanup also failed for {venue}/{symbol}/{date}: "
                f"{cleanup_error}"
            )

    return status


def main():
    """CLI entry point for build_replay_store."""
    parser = argparse.ArgumentParser(
        description="Build replay_store from raw JSONL.zst data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m pipeline.build_replay_store --date 2026-06-15
  python -m pipeline.build_replay_store --date 2026-06-15 --symbols BTCUSDT,ETHUSDT
  python -m pipeline.build_replay_store --date 2026-06-15 --symbols all --data-root /path/to/raw --replay-root /path/to/replay
        """,
    )
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols",
        default="all",
        help="Comma-separated symbols or 'all' (default: all)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help=f"Data root (default: {DATA_ROOT})",
    )
    parser.add_argument(
        "--replay-root",
        type=Path,
        default=None,
        help=f"Replay root (default: {REPLAY_ROOT})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Rebuild partition even if it already has a valid complete manifest "
             "(use after raw data has been repaired or backfilled)",
    )
    parser.add_argument(
        "--schema-version",
        type=int,
        default=0,
        choices=(0, 1, 2),
        help="Replay schema version to build: 0 (default, production legacy "
             "layout), 1 (issue #20 Phase 5 compact prototype), or 2 (issue "
             "#20 Phase 7 hierarchical-integrity candidate — compact "
             "encoding plus a manifest-level traceability hierarchy "
             "replacing the per-event native_payload_hash column). 1 and 2 "
             "are for development validation only — not used by any "
             "systemd unit or production configuration.",
    )
    args = parser.parse_args()

    data_root = args.data_root or DATA_ROOT
    replay_root = args.replay_root or REPLAY_ROOT

    date_str = args.date

    # Parse symbols
    if args.symbols.lower() == "all":
        from pipeline.raw_manifest import scan_raw_coverage
        coverage = scan_raw_coverage(date_str, data_root)
        all_symbols = set()
        for venue_data in coverage["data"].values():
            all_symbols.update(venue_data.keys())
        symbols_to_build = sorted(all_symbols)
    else:
        symbols_to_build = [s.strip().upper() for s in args.symbols.split(",")]

    # Discover venues from raw data
    from pipeline.raw_manifest import scan_raw_coverage
    coverage = scan_raw_coverage(date_str, data_root)
    venues = coverage["venues"]

    if not venues:
        logger.error(f"No raw data found for {date_str}")
        sys.exit(1)

    # Build replay for each venue/symbol combination
    results = []
    for venue in venues:
        for symbol in symbols_to_build:
            if symbol in coverage["data"].get(venue, {}):
                result = build_replay_for_symbol(
                    venue, symbol, date_str, data_root, replay_root,
                    force=args.force,
                    schema_version=args.schema_version,
                    check_repartition_readiness=True,
                )
                results.append(result)

    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    deferred = sum(1 for r in results if r["status"] == "deferred")
    total_depth = sum(r.get("depth_count", 0) for r in results)
    total_trades = sum(r.get("trade_count", 0) for r in results)

    logger.info(
        f"Replay build complete: {successful} successful, {failed} failed, "
        f"{deferred} deferred (pending next-day raw availability for depth "
        f"cross-day repartitioning), {total_depth} depth records, "
        f"{total_trades} trade records"
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
