"""Validate old convert_day catalog output against replay-generated catalogs."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import (
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
    EMIT_DEPTH10_DEFAULT,
)
from converter.readers import stream_raw_records
from converter.spool import RawRecordSpool
from pipeline.build_replay_store import build_replay_for_symbol
from stores.replay_reader import ReplayReader
from validation.replay_catalog_reconstruct import generate_catalog_from_replay
from validation.catalog_compare import (
    compare_book_checkpoints_streaming,
    compare_continuity_diagnostics_semantic,
    compare_event_metadata_exhaustive,
    compare_fenced_ranges_digest,
    compare_instruments_semantic,
    compare_order_book_deltas_exhaustive,
    compare_order_book_depth10_exhaustive,
    compare_trade_ticks_exhaustive,
    iter_order_book_deltas_windowed,
    iter_order_book_depth10_windowed,
    iter_trade_ticks_windowed,
    load_instrument_ids,
    load_instruments,
    write_validation_report,
)

logger = logging.getLogger(__name__)

# Profiles that exercise the depth (OrderBookDeltas / Depth10) comparison path.
_DEPTH_PROFILES = ("full_l2", "depth_only", "depth10")

# Default window size for the bounded-memory windowed loaders used by the
# acceptance path (see iter_trade_ticks_windowed()/iter_order_book_deltas_windowed()
# in validation.catalog_compare for the boundary-safety design and its
# explicit "not a proven strict memory ceiling from time alone" caveat).
DEFAULT_WINDOW_NS = 3_600_000_000_000  # 1 hour


def _parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _instrument_id_for(venue: str, symbol: str) -> str:
    if "USDTF" in venue:
        return f"{symbol}-PERP.BINANCE"
    return f"{symbol}.BINANCE"


def _split_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _prepare_dir(path: Path, *, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"{path} already exists; use --overwrite to replace it")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _run_old_converter(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    old_catalog_root: Path,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "convert_day.py",
        "--date",
        date,
        "--staging",
        "--catalog-root",
        str(old_catalog_root),
        "--symbols",
        ",".join(symbols),
        "--venues",
        ",".join(venues),
        "--allow-partial-overwrite",
    ]
    env = os.environ.copy()
    env["CRYPTO_RECORDER_DATA_ROOT"] = str(data_root)
    result = subprocess.run(
        cmd,
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
    }


def _run_new_pipeline(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    replay_root: Path,
    new_catalog_root: Path,
    start: datetime,
    end: datetime,
    profile: str,
    overwrite: bool,
    emit_depth10: bool,
    depth10_interval_sec: float,
    derived_depth_snapshot_levels: int,
    schema_version: int = 0,
) -> dict[str, Any]:
    replay_results = []
    for venue in venues:
        for symbol in symbols:
            replay_results.append(
                build_replay_for_symbol(
                    venue, symbol, date, data_root, replay_root,
                    schema_version=schema_version,
                )
            )

    catalog_result = generate_catalog_from_replay(
        replay_root=replay_root,
        catalog_root=new_catalog_root,
        job_id="validation_new",
        symbols=symbols,
        venues=venues,
        start=start,
        end=end,
        profile=profile,
        overwrite=overwrite,
        emit_depth10=emit_depth10,
        depth10_interval_sec=depth10_interval_sec,
        derived_depth_snapshot_levels=derived_depth_snapshot_levels,
    )
    return {
        "replay_results": replay_results,
        "catalog_result": catalog_result,
        "catalog_path": str(new_catalog_root / "job_validation_new"),
    }


def _compare_depth_for_instrument(
    old_catalog_root: Path,
    new_catalog_path: Path,
    instrument_id: str,
    start_ns: int,
    end_ns: int,
    *,
    window_ns: int,
    emit_depth10: bool,
    levels: int,
) -> dict[str, Any]:
    """Compare OrderBookDeltas, book-state checkpoints, and (if enabled)
    Depth10 — all exhaustively, in bounded memory, and ALL gating `passed`
    (issue #20 follow-up correction: none of these may be marked
    non-gating or degraded to a full-day-list diagnostic).

    - `order_book_deltas`: compare_order_book_deltas_exhaustive() fed by
      iter_order_book_deltas_windowed().
    - `book_checkpoints`: compare_book_checkpoints_streaming() — the
      bounded-memory checkpoint reconstruction, fed by a SECOND pair of
      windowed delta iterators (checkpoints need their own independent
      traversal from the exhaustive delta comparison above, since a
      generator can only be consumed once; this doubles the on-disk read
      for the delta channel but keeps memory bounded for each pass). The
      full-day `load_order_book_deltas()`-based `compare_book_checkpoints()`
      is no longer used here at all.
    - `order_book_depth10`: compare_order_book_depth10_exhaustive() fed by
      iter_order_book_depth10_windowed() when `emit_depth10` is True and
      gates `passed`; when explicitly disabled, reported as intentionally
      skipped (not a failing-but-ignored comparison).
    """
    old_delta_stream = iter_order_book_deltas_windowed(
        old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    new_delta_stream = iter_order_book_deltas_windowed(
        new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    deltas_cmp = compare_order_book_deltas_exhaustive(old_delta_stream, new_delta_stream)

    old_delta_stream_for_checkpoints = iter_order_book_deltas_windowed(
        old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    new_delta_stream_for_checkpoints = iter_order_book_deltas_windowed(
        new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
    )
    checkpoints_cmp = compare_book_checkpoints_streaming(
        old_delta_stream_for_checkpoints, new_delta_stream_for_checkpoints, start_ns, end_ns, levels=levels
    )

    out: dict[str, Any] = {
        "order_book_deltas": deltas_cmp,
        "book_checkpoints": checkpoints_cmp,
    }
    if emit_depth10:
        old_depth10_stream = iter_order_book_depth10_windowed(
            old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
        )
        new_depth10_stream = iter_order_book_depth10_windowed(
            new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
        )
        out["order_book_depth10"] = compare_order_book_depth10_exhaustive(old_depth10_stream, new_depth10_stream)
    else:
        out["order_book_depth10"] = {"passed": True, "skipped": True, "reason": "emit_depth10 disabled"}

    out["passed"] = deltas_cmp["passed"] and checkpoints_cmp["passed"] and out["order_book_depth10"]["passed"]
    return out


def _load_old_convert_report(old_catalog_root: Path, date: str) -> dict[str, Any]:
    """Load convert_day.py's own report JSON (per-symbol continuity/fenced-
    range diagnostics live here — convert_day.py's Nautilus catalog output
    itself carries none of this). See convert_day.py's `_save_report()`:
    it writes to `catalog_root.parent / "convert_reports" / f"{date}.json"`,
    where `catalog_root` is exactly `old_catalog_root` as invoked by
    `_run_old_converter()` above."""
    report_path = old_catalog_root.parent / "convert_reports" / f"{date}.json"
    if not report_path.exists():
        return {}
    try:
        return json.loads(report_path.read_text())
    except (OSError, ValueError):
        return {}


def _compare_continuity_for_symbol(
    old_report: dict[str, Any], new_manifest: dict[str, Any], venue: str, symbol: str
) -> dict[str, Any]:
    """Compare snapshot-seed/resync/desync/fenced-range counts between the
    reference route's per-symbol depth report and the candidate route's
    manifest depth_diagnostics — see
    validation.catalog_compare.compare_continuity_diagnostics_semantic()
    for the field-name normalization this relies on."""
    key = f"{venue}/{symbol}"
    old_per_symbol = (old_report.get("per_symbol_depth") or {}).get(key, {})
    new_depth_diagnostics = new_manifest.get("depth_diagnostics") or {}
    if not old_per_symbol and not new_depth_diagnostics:
        return {
            "passed": True,
            "skipped": True,
            "reason": "no continuity diagnostics available on either side for this symbol",
        }
    return compare_continuity_diagnostics_semantic(old_per_symbol, new_depth_diagnostics)


def _compare_fenced_ranges_for_symbol(
    old_report: dict[str, Any], new_manifest: dict[str, Any], venue: str, symbol: str
) -> dict[str, Any]:
    """Compare the reference route's COMPLETE fenced-range collection
    (via its canonical count + SHA-256 digest, computed by convert_day.py
    over every fence — see converter.depth_phase2.canonical_fence_digest())
    against the candidate route's actual fenced-range list for this
    venue/symbol.

    Issue #20 follow-up correction: the reference route's report used to
    expose only up to 3 example fences per symbol, and this validator used
    to treat a candidate `extra_in_new` fence as expected/non-gating.
    Neither is true anymore: `canonical_count`/`canonical_digest` cover the
    reference's complete fence collection, so a candidate that has an
    extra fence the reference does not (or a difference beyond the 3rd
    fence) changes the digest and correctly fails via
    compare_fenced_ranges_digest()'s `passed`, with no separate
    "gating_passed" carve-out.
    """
    key = f"{venue}/{symbol}"
    old_entry = (old_report.get("per_symbol_fenced_ranges") or {}).get(key, {})
    old_canonical_count = int(old_entry.get("canonical_count", 0))
    old_canonical_digest = old_entry.get("canonical_digest", "")
    new_fences_all = new_manifest.get("fenced_ranges") or []
    new_fences_for_symbol = [
        f for f in new_fences_all if f.get("venue") == venue and f.get("symbol") == symbol
    ]
    return compare_fenced_ranges_digest(old_canonical_count, old_canonical_digest, new_fences_for_symbol)


# ---------------------------------------------------------------------------
# Raw-to-replay logical metadata comparison (quality/continuity, event-keyed)
# ---------------------------------------------------------------------------
#
# Issue #20 follow-up correction: the previous implementation collected a
# complete day of raw+replay quality_flags values into two Python lists and
# compared them as a multiset (compare_quality_flags_semantic()). That both
# violates the bounded-memory requirement for a complete production day AND
# can miss a quality flag (or any other per-event field) that MOVED from one
# event to another while the overall multiset of values stayed the same.
# This section replaces it with a streaming, event-identity-keyed comparison
# via compare_event_metadata_exhaustive().

_DEPTH_ACCEPTED_RECORD_TYPES = {"snapshot_seed", "depth_update"}
_TRADE_ACCEPTED_RECORD_TYPES = {"trade", "agg_trade"}

_DEPTH_COMPARE_FIELDS = (
    "channel",
    "stream_session_id",
    "session_seq",
    "raw_index",
    "record_type",
    "U",
    "u",
    "pu",
    "is_snapshot_seed",
    "is_depth_update",
    "is_sync_state",
    "is_desync",
    "is_resync",
    "quality_flags",
)
_TRADE_COMPARE_FIELDS = (
    "channel",
    "trade_stream_session_id",
    "trade_session_seq",
    "raw_index",
    "record_type",
    "quality_flags",
)


def _canonical_quality_flags(value: Any) -> Any:
    """Parse and re-serialize quality_flags deterministically (sorted
    keys) so two logically identical JSON payloads that merely differ in
    key order/whitespace compare equal, matching
    compare_quality_flags_semantic()'s existing normalization approach."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    try:
        return json.dumps(json.loads(value), sort_keys=True)
    except (TypeError, ValueError, json.JSONDecodeError):
        return value


def _normalize_raw_depth_record(rec: dict[str, Any]) -> dict[str, Any]:
    record_type = rec.get("record_type", "depth_update")
    sync_state = rec.get("sync_state")
    is_desync = bool(rec.get("is_desync", False) or sync_state == "desynced")
    is_resync = bool(rec.get("is_resync", False) or sync_state == "resync_required")
    return {
        "channel": "depth_v2",
        "stream_session_id": rec.get("stream_session_id"),
        "session_seq": rec.get("session_seq"),
        "raw_index": rec.get("raw_index"),
        "record_type": record_type,
        "U": None if rec.get("U") is None else str(rec.get("U")),
        "u": None if (rec.get("u") or rec.get("lastUpdateId")) is None else str(rec.get("u") or rec.get("lastUpdateId")),
        "pu": None if rec.get("pu") is None else str(rec.get("pu")),
        "is_snapshot_seed": record_type == "snapshot_seed",
        "is_depth_update": record_type == "depth_update",
        "is_sync_state": record_type == "sync_state",
        "is_desync": is_desync,
        "is_resync": is_resync,
        "quality_flags": _canonical_quality_flags(rec.get("quality_flags")),
    }


def _normalize_replay_depth_record(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "channel": "depth_v2",
        "stream_session_id": rec.get("stream_session_id"),
        "session_seq": rec.get("session_seq"),
        "raw_index": rec.get("raw_index"),
        "record_type": rec.get("record_type"),
        "U": rec.get("U"),
        "u": rec.get("u"),
        "pu": rec.get("pu"),
        "is_snapshot_seed": rec.get("is_snapshot_seed"),
        "is_depth_update": rec.get("is_depth_update"),
        "is_sync_state": rec.get("is_sync_state"),
        "is_desync": rec.get("is_desync"),
        "is_resync": rec.get("is_resync"),
        "quality_flags": _canonical_quality_flags(rec.get("quality_flags")),
    }


def _normalize_raw_trade_record(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "channel": "trade_v2",
        "trade_stream_session_id": rec.get("trade_stream_session_id"),
        "trade_session_seq": rec.get("trade_session_seq"),
        "raw_index": rec.get("raw_index"),
        "record_type": rec.get("record_type", "trade"),
        "quality_flags": _canonical_quality_flags(rec.get("quality_flags")),
    }


def _normalize_replay_trade_record(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "channel": "trade_v2",
        "trade_stream_session_id": rec.get("trade_stream_session_id"),
        "trade_session_seq": rec.get("trade_session_seq"),
        "raw_index": rec.get("raw_index"),
        "record_type": rec.get("record_type"),
        "quality_flags": _canonical_quality_flags(rec.get("quality_flags")),
    }


def _iter_sorted_raw_depth(data_root: Path, venue: str, symbol: str, date: str):
    """Stream depth_v2 raw records for one venue/symbol/date, filtered to
    only the record types the replay writer actually converts
    (`snapshot_seed`/`depth_update` — see pipeline/build_replay_store.py's
    `_convert_depth_record()`; other raw record types such as `sync_state`
    are intentionally never written to replay and would otherwise show up
    as spurious "extra on the raw side" mismatches), sorted into the
    canonical `(stream_session_id, session_seq, raw_index)` order via
    converter.spool.RawRecordSpool — an existing disk-backed bounded
    spool, reused here rather than sorting a full-day Python list in
    memory. The raw hourly files are not guaranteed to already be in this
    order across file boundaries, so sorting is required for a correct
    positional comparison against the replay side (which the replay-store
    contract already guarantees is delivered in this order)."""
    with RawRecordSpool(prefix="cr-validate-depth-") as spool:
        raw_index = 0
        for rec in stream_raw_records(venue, symbol, "depth_v2", date, root=data_root):
            if rec.get("record_type", "depth_update") not in _DEPTH_ACCEPTED_RECORD_TYPES:
                raw_index += 1
                continue
            session_id = int(rec.get("stream_session_id") or 0)
            session_seq = int(rec.get("session_seq") or 0)
            rec = dict(rec)
            rec["raw_index"] = raw_index
            spool.insert(rec, (session_id, session_seq, 0), raw_index)
            raw_index += 1
        spool.commit()
        for rec in spool.iter_records():
            yield _normalize_raw_depth_record(rec)


def _iter_sorted_raw_trades(data_root: Path, venue: str, symbol: str, date: str):
    """Same rationale as _iter_sorted_raw_depth(), for trade_v2 records
    (filtered to `trade`/`agg_trade`, matching
    pipeline/build_replay_store.py's `_convert_trade_record()`), sorted by
    `(trade_stream_session_id, trade_session_seq, raw_index)`."""
    with RawRecordSpool(prefix="cr-validate-trade-") as spool:
        raw_index = 0
        for rec in stream_raw_records(venue, symbol, "trade_v2", date, root=data_root):
            if rec.get("record_type", "trade") not in _TRADE_ACCEPTED_RECORD_TYPES:
                raw_index += 1
                continue
            session_id = int(rec.get("trade_stream_session_id") or 0)
            session_seq = int(rec.get("trade_session_seq") or 0)
            rec = dict(rec)
            rec["raw_index"] = raw_index
            spool.insert(rec, (session_id, session_seq, 0), raw_index)
            raw_index += 1
        spool.commit()
        for rec in spool.iter_records():
            yield _normalize_raw_trade_record(rec)


def _compare_raw_to_replay_metadata_for_symbol(
    data_root: Path, replay_root: Path, venue: str, symbol: str, date: str
) -> dict[str, Any]:
    """Bounded-memory, event-identity-keyed comparison of raw-vs-replay
    logical metadata (continuity IDs, sync/desync/resync state, and
    quality_flags) for both the depth_v2 and trade_v2 channels of one
    venue/symbol/date. Replaces the prior full-day-list multiset
    quality-flags comparison; see this module's header comment above
    `_DEPTH_ACCEPTED_RECORD_TYPES` for the full rationale.

    convert_day.py's Nautilus catalog output does not persist any of
    this per-event metadata at all (Nautilus's TradeTick/OrderBookDelta
    objects have no such fields), so there is no "old Nautilus catalog vs
    new Nautilus catalog" comparison available for it. The one place this
    metadata genuinely exists on both a reference and a candidate side is:
    the permanent raw source (`data_raw`) versus the replay_store the
    candidate pipeline builds from it — this proves the replay pipeline
    faithfully preserves per-event continuity/quality metadata, using a
    different "reference" (raw) than the rest of this validator
    (convert_day.py's catalog), documented explicitly here.
    """
    reader = ReplayReader(replay_root)

    old_depth_stream = _iter_sorted_raw_depth(data_root, venue, symbol, date)
    new_depth_stream = (_normalize_replay_depth_record(rec) for rec in reader.iter_depths(venue, symbol, date))
    depth_cmp = compare_event_metadata_exhaustive(
        old_depth_stream, new_depth_stream, compare_fields=_DEPTH_COMPARE_FIELDS
    )

    old_trade_stream = _iter_sorted_raw_trades(data_root, venue, symbol, date)
    new_trade_stream = (_normalize_replay_trade_record(rec) for rec in reader.iter_trades(venue, symbol, date))
    trade_cmp = compare_event_metadata_exhaustive(
        old_trade_stream, new_trade_stream, compare_fields=_TRADE_COMPARE_FIELDS
    )

    return {
        "depth": depth_cmp,
        "trades": trade_cmp,
        "passed": depth_cmp["passed"] and trade_cmp["passed"],
    }


def _read_new_manifest(new_catalog_path: Path) -> dict[str, Any]:
    manifest_path = new_catalog_path / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text())
    except (OSError, ValueError):
        return {}


def validate_catalog_equivalence(
    *,
    date: str,
    symbols: list[str],
    venues: list[str],
    data_root: Path,
    work_root: Path,
    old_catalog_root: Path,
    replay_root: Path,
    new_catalog_root: Path,
    profile: str,
    overwrite: bool,
    emit_depth10: bool = EMIT_DEPTH10_DEFAULT,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
    window_ns: int = DEFAULT_WINDOW_NS,
    schema_version: int = 0,
) -> dict[str, Any]:
    """Run the canonical semantic-equivalence gate between the reference
    (data_raw -> convert_day.py) catalog and the replay-reconstructed
    catalog for one date/symbol/venue set.

    Args:
        schema_version: replay schema version to build the candidate side
            with (0 default/legacy, or 1 for the issue #20 Phase 5 compact
            prototype). This lets the SAME canonical validator gate a v1
            replay build end-to-end (instruments/precision, exhaustive
            trades/deltas, book checkpoints, Depth10, fenced-range/
            continuity/quality-flag evidence) without a separate ad-hoc
            four-function comparison script.
    """
    start = _parse_date(date)
    end = start + timedelta(days=1)
    new_catalog_path = new_catalog_root / "job_validation_new"
    compares_trades = profile in ("trades_only", "full_l2")
    compares_depth = profile in _DEPTH_PROFILES
    report: dict[str, Any] = {
        "date": date,
        "symbols": symbols,
        "venues": venues,
        "profile": profile,
        "status": "failed",
        "old_catalog_root": str(old_catalog_root),
        "new_catalog_root": str(new_catalog_path),
        "replay_root": str(replay_root),
        "old_path": str(old_catalog_root),
        "new_path": str(new_catalog_path),
        "comparison": {},
        "diagnostics": {},
        "notes": [],
        "errors": [],
    }

    if profile not in ("trades_only", "full_l2"):
        report["status"] = "skipped"
        report["notes"].append(
            f"validate_catalog_equivalence supports trades_only and full_l2; got profile={profile}"
        )
        return report

    work_root.mkdir(parents=True, exist_ok=True)
    _prepare_dir(old_catalog_root, overwrite=overwrite)
    _prepare_dir(replay_root, overwrite=overwrite)
    _prepare_dir(new_catalog_root, overwrite=overwrite)

    old_result = _run_old_converter(
        date=date,
        symbols=symbols,
        venues=venues,
        data_root=data_root,
        old_catalog_root=old_catalog_root,
    )
    report["old_run"] = old_result
    if old_result["returncode"] != 0:
        report["errors"].append("old convert_day.py run failed")
        return report

    new_result = _run_new_pipeline(
        date=date,
        symbols=symbols,
        venues=venues,
        data_root=data_root,
        replay_root=replay_root,
        new_catalog_root=new_catalog_root,
        start=start,
        end=end,
        profile=profile,
        overwrite=overwrite,
        emit_depth10=emit_depth10,
        depth10_interval_sec=depth10_interval_sec,
        derived_depth_snapshot_levels=derived_depth_snapshot_levels,
        schema_version=schema_version,
    )
    report["new_run"] = new_result
    if new_result["catalog_result"].get("status") != "success":
        report["errors"].append("new replay-generated catalog run failed")
        return report

    expected_ids = sorted(_instrument_id_for(venue, symbol) for venue in venues for symbol in symbols)
    # Parallel (venue, symbol, instrument_id) triples — continuity/fenced-
    # range/quality-flag comparisons operate on venue+symbol, not
    # instrument_id, and need to be run alongside the per-instrument loop.
    venue_symbol_by_id: dict[str, tuple[str, str]] = {
        _instrument_id_for(venue, symbol): (venue, symbol) for venue in venues for symbol in symbols
    }

    # Instrument identity AND precision/increment comparison (issue #20
    # Phase 1 coverage-gap fix, now wired into the real acceptance path —
    # a wrong price_precision/tick-size on an otherwise-correctly-named
    # instrument was previously undetectable here).
    old_instruments = load_instruments(old_catalog_root)
    new_instruments = load_instruments(new_catalog_path)
    instrument_precision_cmp = compare_instruments_semantic(old_instruments, new_instruments)

    old_ids = load_instrument_ids(old_catalog_root)
    new_ids = load_instrument_ids(new_catalog_path)
    old_expected_ids = sorted(instrument_id for instrument_id in old_ids if instrument_id in expected_ids)
    new_expected_ids = sorted(instrument_id for instrument_id in new_ids if instrument_id in expected_ids)

    start_ns = int(start.timestamp() * 1_000_000_000)
    end_ns = int(end.timestamp() * 1_000_000_000)

    old_report = _load_old_convert_report(old_catalog_root, date)
    new_manifest = _read_new_manifest(new_catalog_path)

    comparison: dict[str, Any] = {
        "expected_instrument_ids": expected_ids,
        "old_instrument_ids": old_ids,
        "new_instrument_ids": new_ids,
        "instrument_ids_match": old_expected_ids == new_expected_ids == expected_ids,
        "instrument_precision": instrument_precision_cmp,
        "by_instrument": {},
    }

    all_passed = comparison["instrument_ids_match"] and instrument_precision_cmp["passed"]
    trades_all_passed = True
    deltas_all_passed = True
    depth10_all_passed = True
    checkpoints_all_passed = True
    continuity_all_passed = True
    fenced_ranges_all_passed = True
    quality_flags_all_passed = True

    for instrument_id in expected_ids:
        per_instrument: dict[str, Any] = {}
        venue, symbol = venue_symbol_by_id[instrument_id]

        if compares_trades:
            # Exhaustive, order-preserving, bounded-memory comparison —
            # the acceptance-gating trade comparison (issue #20 follow-up
            # correction). Fed by the windowed loader, never the full-day
            # load_trade_ticks() list loader, and never
            # compare_trade_ticks_semantic()'s sampled comparator.
            old_trade_stream = iter_trade_ticks_windowed(
                old_catalog_root, instrument_id, start_ns, end_ns, window_ns=window_ns
            )
            new_trade_stream = iter_trade_ticks_windowed(
                new_catalog_path, instrument_id, start_ns, end_ns, window_ns=window_ns
            )
            trades_cmp = compare_trade_ticks_exhaustive(old_trade_stream, new_trade_stream)
            per_instrument["trade_ticks"] = trades_cmp
            trades_all_passed = trades_all_passed and trades_cmp["passed"]
            all_passed = all_passed and trades_cmp["passed"]

        if compares_depth:
            depth_cmp = _compare_depth_for_instrument(
                old_catalog_root,
                new_catalog_path,
                instrument_id,
                start_ns,
                end_ns,
                window_ns=window_ns,
                emit_depth10=emit_depth10,
                levels=derived_depth_snapshot_levels,
            )
            per_instrument["order_book_deltas"] = depth_cmp["order_book_deltas"]
            per_instrument["order_book_depth10"] = depth_cmp["order_book_depth10"]
            per_instrument["book_checkpoints"] = depth_cmp["book_checkpoints"]
            deltas_all_passed = deltas_all_passed and depth_cmp["order_book_deltas"]["passed"]
            depth10_all_passed = depth10_all_passed and depth_cmp["order_book_depth10"]["passed"]
            checkpoints_all_passed = (
                checkpoints_all_passed and depth_cmp["book_checkpoints"]["passed"]
            )
            # order_book_deltas, book_checkpoints, and order_book_depth10
            # (when enabled) ALL gate `passed` here — see
            # _compare_depth_for_instrument()'s docstring; none of these
            # may be downgraded to a non-gating diagnostic.
            all_passed = all_passed and depth_cmp["passed"]

            continuity_cmp = _compare_continuity_for_symbol(old_report, new_manifest, venue, symbol)
            per_instrument["continuity_diagnostics"] = continuity_cmp
            continuity_all_passed = continuity_all_passed and continuity_cmp["passed"]
            all_passed = all_passed and continuity_cmp["passed"]

            fenced_ranges_cmp = _compare_fenced_ranges_for_symbol(old_report, new_manifest, venue, symbol)
            per_instrument["fenced_ranges"] = fenced_ranges_cmp
            fenced_ranges_all_passed = fenced_ranges_all_passed and fenced_ranges_cmp["passed"]
            all_passed = all_passed and fenced_ranges_cmp["passed"]

            metadata_cmp = _compare_raw_to_replay_metadata_for_symbol(data_root, replay_root, venue, symbol, date)
            per_instrument["raw_to_replay_metadata"] = metadata_cmp
            quality_flags_all_passed = quality_flags_all_passed and metadata_cmp["passed"]
            all_passed = all_passed and metadata_cmp["passed"]

        comparison["by_instrument"][instrument_id] = per_instrument

    # Aggregate, profile-shaped comparison block.
    if compares_trades:
        comparison["trade_ticks"] = {"passed": trades_all_passed}
    if compares_depth:
        comparison["order_book_deltas"] = {"passed": deltas_all_passed}
        comparison["order_book_depth10"] = {
            "passed": depth10_all_passed,
            "emitted": emit_depth10,
        }
        comparison["book_checkpoints"] = {"passed": checkpoints_all_passed}
        comparison["continuity_diagnostics"] = {"passed": continuity_all_passed}
        comparison["fenced_ranges"] = {"passed": fenced_ranges_all_passed}
        comparison["raw_to_replay_metadata"] = {"passed": quality_flags_all_passed}

    # Backward-compatible flat single-instrument trade fields. Field shape
    # intentionally changed (issue #20 follow-up correction): the exhaustive
    # comparator does not compute ts_min/ts_max/timestamp_range_match/
    # sample_mismatches the way the old sampled comparator did — those
    # fields are dropped here rather than faked.
    if compares_trades and len(expected_ids) == 1:
        only = comparison["by_instrument"][expected_ids[0]].get("trade_ticks")
        if only:
            comparison.update(
                {
                    "trade_count_old": only["trade_count_old"],
                    "trade_count_new": only["trade_count_new"],
                    "trade_count_match": only["trade_count_match"],
                    "positions_compared": only["positions_compared"],
                }
            )

    report["diagnostics"] = {
        "old_report": {
            "returncode": old_result["returncode"],
            "stdout_tail": old_result.get("stdout_tail", ""),
        },
        "new_manifest": new_manifest,
        "fenced_ranges": new_manifest.get("fenced_ranges", []),
        "equivalence_caveats": new_manifest.get("equivalence_caveats", []),
        "warnings": new_manifest.get("warnings", []),
    }

    report["comparison"] = comparison
    report["status"] = "passed" if all_passed else "failed"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare old convert_day.py catalog output with replay-generated trades_only output.",
    )
    parser.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    parser.add_argument("--symbols", required=True, help="Comma-separated raw symbols")
    parser.add_argument("--venues", required=True, help="Comma-separated venues")
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--old-catalog-root", type=Path, required=True)
    parser.add_argument("--replay-root", type=Path, required=True)
    parser.add_argument("--new-catalog-root", type=Path, required=True)
    parser.add_argument("--profile", choices=["trades_only", "full_l2"], default="trades_only")
    parser.add_argument(
        "--emit-depth10",
        dest="emit_depth10",
        action="store_true",
        default=EMIT_DEPTH10_DEFAULT,
        help="Compare derived OrderBookDepth10 (full_l2). Default: on.",
    )
    parser.add_argument(
        "--no-emit-depth10",
        dest="emit_depth10",
        action="store_false",
        help="Skip OrderBookDepth10 comparison (full_l2).",
    )
    parser.add_argument(
        "--depth10-interval-sec",
        type=float,
        default=DEPTH10_INTERVAL_SEC,
        help=f"Depth10 snapshot interval for the new pipeline (default: {DEPTH10_INTERVAL_SEC}).",
    )
    parser.add_argument(
        "--derived-depth-snapshot-levels",
        type=int,
        default=DERIVED_DEPTH_SNAPSHOT_LEVELS,
        help=f"Depth10 levels for the new pipeline (default: {DERIVED_DEPTH_SNAPSHOT_LEVELS}).",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=DEFAULT_WINDOW_NS / 3_600_000_000_000,
        help=(
            "Bounded-memory time window (in hours) used by the exhaustive "
            "trade/delta comparators' windowed catalog loaders. Default: 1 "
            "hour. Tune based on measured per-window RSS for the target "
            "production day (issue #20 Tier 3) — a fixed time window bounds "
            "query result size per window but is not by itself a proven "
            "strict event-count/RSS ceiling."
        ),
    )
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--schema-version",
        type=int,
        default=0,
        choices=(0, 1),
        help="Replay schema version to build the candidate side with: 0 "
             "(default, legacy) or 1 (issue #20 Phase 5 compact prototype, "
             "for development validation only).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    symbols = _split_csv(args.symbols)
    venues = _split_csv(args.venues)

    report = validate_catalog_equivalence(
        date=args.date,
        symbols=symbols,
        venues=venues,
        data_root=args.data_root,
        work_root=args.work_root,
        old_catalog_root=args.old_catalog_root,
        replay_root=args.replay_root,
        new_catalog_root=args.new_catalog_root,
        profile=args.profile,
        overwrite=args.overwrite,
        emit_depth10=args.emit_depth10,
        depth10_interval_sec=args.depth10_interval_sec,
        derived_depth_snapshot_levels=args.derived_depth_snapshot_levels,
        window_ns=int(args.window_hours * 3_600_000_000_000),
        schema_version=args.schema_version,
    )

    if args.report_path is not None:
        report_path = args.report_path
    elif args.profile == "full_l2":
        repo_root = Path(__file__).resolve().parent.parent
        symbol_tag = "-".join(symbols) if symbols else "ALL"
        report_path = (
            repo_root
            / "validation_reports"
            / f"full_l2_equivalence_{args.date}_{symbol_tag}.json"
        )
    else:
        report_path = args.work_root / f"catalog_equivalence_{args.date}.json"
    write_validation_report(report, report_path)

    print(f"Catalog equivalence status: {report['status']} (profile={report['profile']})")
    print(f"Report: {report_path}")
    comparison = report.get("comparison") or {}
    # NOTE: this summary print was previously stale (referenced
    # "trade_count_old"/"timestamp_range_match" keys that no longer exist
    # in the per-instrument comparison report shape — fixed here, not a
    # behavior change to the comparison logic itself, only to what is
    # printed to stdout).
    if "instrument_ids_match" in comparison:
        print(f"Instrument IDs match: {comparison['instrument_ids_match']}")
    if "instrument_precision" in comparison:
        print(f"Instrument precision match: {comparison['instrument_precision']['passed']}")
    for instrument_id, per_instrument in (comparison.get("by_instrument") or {}).items():
        print(f"--- {instrument_id} ---")
        for key, value in per_instrument.items():
            if isinstance(value, dict) and "passed" in value:
                print(f"  {key} passed: {value['passed']}")


    if report["status"] == "passed":
        return 0
    if report["status"] == "skipped":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
