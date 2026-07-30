"""
validation.replay_catalog_reconstruct — Internal replay -> Nautilus catalog
reconstruction helper.

**This module is validation-only tooling. It is NOT a supported downstream
runtime API and has no CLI entrypoint.** Its sole purpose is to let
``validation.validate_catalog_equivalence`` reconstruct a temporary Nautilus
``ParquetDataCatalog`` from ``replay_store`` data so it can be compared against
the reference ``convert_day.py`` output for semantic equivalence.

Per the CryptoRecorder/KovacsTrader ownership boundary
(see ``docs/ARCHITECTURE.md``), CryptoRecorder does not offer a general-purpose
consumer catalog-generation service. Any downstream repository that needs a
temporary Nautilus catalog reconstructed from replay_store data (e.g.
KovacsTrader) is expected to own that reconstruction itself; this module exists
only so the reference-vs-replay equivalence check keeps working.
"""
from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    from nautilus_trader.model.data import OrderBookDeltas, OrderBookDepth10, TradeTick
    from nautilus_trader.model.enums import AggressorSide
    from nautilus_trader.model.identifiers import InstrumentId, TradeId
    from nautilus_trader.model.objects import Price, Quantity
    from nautilus_trader.persistence.catalog import ParquetDataCatalog
    NAUTILUS_AVAILABLE = True
except ImportError:
    NAUTILUS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Nautilus not available; replay_catalog_reconstruct will not work")

from config import (
    DEPTH10_INTERVAL_SEC,
    DERIVED_DEPTH_SNAPSHOT_LEVELS,
    EMIT_DEPTH10_DEFAULT,
)
from converter.depth_phase2 import replay_records_to_depth_streaming
from converter.instruments import build_instruments
from converter.spool import ObjectSpool
from stores.replay_depth_adapter import iter_replay_depth_records
from stores.replay_reader import ReplayReader

logger = logging.getLogger(__name__)

WRITE_BATCH_SIZE = 5000

# Supported reconstruction profiles:
#   trades_only — instruments + TradeTick (validated equivalent path)
#   full_l2     — instruments + TradeTick + OrderBookDeltas (+ optional Depth10)
#   depth_only  — instruments + OrderBookDeltas (+ optional Depth10), no trades
#   depth10     — instruments + OrderBookDepth10 only
SUPPORTED_PROFILES = ("trades_only", "full_l2", "depth_only", "depth10")

# Documented equivalence caveats for the replay-based full_l2 reconstruction.
# See docs/FULL_L2_REPLAY_CATALOG_PLAN.md for the full equivalence boundary.
#
# issue #20 Phase 7: "UTC-boundary repartitioning of clock-skewed records is
# not applied" and "cross-day carry / synthetic opening snapshot is not
# reproduced (no prev/next repartitioning)" were REMOVED from this list only
# after implementation and evidence: depth_v2 raw records are now
# repartitioned across D-1/D/D+1 by canonical event time in
# ``pipeline.build_replay_store._stream_repartitioned_depth_records``,
# reusing convert_day.py's own reference rule
# (``converter.depth_phase2._spool_repartitioned_records``) exactly — proven
# via the full exhaustive `validate_catalog_equivalence` gate (TradeTicks,
# OrderBookDeltas, OrderBookDepth10, book checkpoints, continuity
# diagnostics, fenced ranges, raw-to-replay metadata — all 8 components) on
# BINANCE_SPOT/ADAUSDT for 2026-06-10, 2026-06-11 (the specific day that
# exposed a 47-event OrderBookDeltas gap before this correction — now
# old=new=1,071,997 exactly), and 2026-06-12, for both schema_version=0 and
# schema_version=2. See ``pipeline.build_replay_store.check_depth_repartition_readiness``
# for the readiness dependency: offline equivalence construction requires a
# closed full D+1 scope, while the production 01:00 build uses a narrower
# closed-first-hour operational policy documented there.
# ``sync_state``/``stream_lifecycle`` records are preserved by current replay
# schemas and the accepted ADA/BTC gates proved continuity and canonical
# fenced-range results exact; they are therefore not listed as caveats.
FULL_L2_CAVEATS = [
    "duplicate depth suppression relies on the replay builder, not the converter spool",
]


def _profile_write_flags(profile: str, emit_depth10: bool) -> tuple[bool, bool, bool]:
    """Resolve (writes_trades, writes_deltas, writes_depth10) for a profile."""
    if profile == "trades_only":
        return True, False, False
    if profile == "full_l2":
        return True, True, emit_depth10
    if profile == "depth_only":
        return False, True, emit_depth10
    if profile == "depth10":
        return False, False, True
    raise ValueError(f"Unsupported profile: {profile}")


def _parse_iso_datetime(iso_str: str) -> datetime:
    """Parse ISO 8601 UTC datetime string."""
    try:
        # Try parsing with timezone
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    except ValueError:
        raise ValueError(f"Invalid ISO 8601 datetime: {iso_str}")


def _date_range_from_window(start: datetime, end: datetime) -> list[str]:
    """Generate date strings touched by the half-open [start, end) window."""
    dates = []
    current = datetime.combine(start.date(), datetime.min.time(), tzinfo=timezone.utc)
    while current < end:
        dates.append(current.date().isoformat())
        current += timedelta(days=1)
    return dates


def _window_from_date(date_str: str) -> tuple[datetime, datetime]:
    """Return the UTC day window for YYYY-MM-DD."""
    try:
        start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Invalid date: {date_str}; expected YYYY-MM-DD") from exc
    return start, start + timedelta(days=1)


class ReplayTradeIdentifierError(ValueError):
    """A replay row cannot produce a semantically identified TradeTick."""


def _convert_trade_to_nautilus(
    trade: dict,
    instrument_id: InstrumentId,
    venue: str,
) -> Optional[TradeTick]:
    """Convert an identified replay trade record to Nautilus TradeTick.

    A missing identifier is a replay contract violation, not an ordinary
    invalid market value which may be counted and skipped. Raise so injected
    or historical anonymous rows cannot turn into a successful shorter
    catalog.
    """
    if not NAUTILUS_AVAILABLE:
        return None

    try:
        trade_id = trade.get("trade_id")
        if trade_id is None or str(trade_id) == "":
            trade_id = trade.get("agg_trade_id")
        if trade_id is None or str(trade_id) == "":
            raise ReplayTradeIdentifierError(
                "Replay trade row has no supported identifier "
                "(trade_id or agg_trade_id); refusing to reconstruct an "
                "anonymous TradeTick"
            )

        price = trade.get("price_str") or trade.get("price", 0)
        quantity = trade.get("quantity_str") or trade.get("quantity", 0)
        ts_ns = int(trade.get("ts_exchange_ns", 0))
        ts_recv_ns = int(trade.get("ts_receive_ns", ts_ns))

        if float(price) <= 0 or float(quantity) <= 0:
            return None

        side = AggressorSide.BUYER if not trade.get("buyer_maker", False) else AggressorSide.SELLER

        tick = TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(str(price)),
            size=Quantity.from_str(str(quantity)),
            aggressor_side=side,
            trade_id=TradeId(str(trade_id)),
            ts_event=ts_ns,
            ts_init=ts_recv_ns,
        )
        return tick
    except ReplayTradeIdentifierError:
        raise
    except Exception as e:
        logger.warning(f"Error converting trade: {e}")
        return None


def _date_shift(date_str: str, days: int) -> str:
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return (base + timedelta(days=days)).strftime("%Y-%m-%d")


def _write_depth_for_partition(
    *,
    reader: ReplayReader,
    venue: str,
    symbol: str,
    date: str,
    instrument,
    catalog,
    start_ns: int,
    end_ns: int,
    writes_deltas: bool,
    writes_depth10: bool,
    depth10_interval_sec: float,
    derived_depth_snapshot_levels: int,
    time_filter: str,
    batch_size: int = WRITE_BATCH_SIZE,
):
    """Replay one partition's depth through the shared engine and write to catalog.

    Reuses the validated ``converter.depth_phase2`` engine (via the replay
    adapter) so OrderBookDeltas / Depth10 semantics match the raw path exactly.
    Objects are buffered in disk-backed :class:`ObjectSpool`s (memory-bounded)
    and written in the same ``ts_init``-ordered batches as ``convert_day.py``.

    Returns ``(metrics, deltas_written, depth10_written, deltas_skipped,
    depth10_skipped)``.
    """
    iid = instrument.id
    price_prec = instrument.price_precision
    size_prec = instrument.size_precision

    def _ts(obj) -> int:
        return int(obj.ts_init) if time_filter == "ts_init" else int(obj.ts_event)

    def _in_window(obj) -> bool:
        ts = _ts(obj)
        return start_ns <= ts < end_ns

    deltas_skipped = 0
    depth10_skipped = 0

    with (
        ObjectSpool(prefix="cryptorecorder-gc-delta-") as deltas_spool,
        ObjectSpool(prefix="cryptorecorder-gc-depth10-") as depth10_spool,
    ):
        delta_ordinal = 0
        depth10_ordinal = 0

        def on_deltas_batch(batch):
            nonlocal delta_ordinal, deltas_skipped
            if not writes_deltas:
                return
            kept = [d for d in batch if _in_window(d)]
            deltas_skipped += len(batch) - len(kept)
            if kept:
                delta_ordinal = deltas_spool.insert_many(kept, start_ordinal=delta_ordinal)

        def on_depth10_batch(batch):
            nonlocal depth10_ordinal, depth10_skipped
            if not writes_depth10:
                return
            kept = [d for d in batch if _in_window(d)]
            depth10_skipped += len(batch) - len(kept)
            if kept:
                depth10_ordinal = depth10_spool.insert_many(kept, start_ordinal=depth10_ordinal)

        records = iter_replay_depth_records(reader.iter_depths(venue, symbol, date))

        # Cross-day carry recovery (mirrors convert_depth_v2_streaming()'s raw
        # carry mechanism exactly, reusing the same depth_phase2 helpers): if
        # the previous day's replay partition exists for this venue/symbol,
        # its depth rows are consumed transiently (never persisted/copied
        # into this partition) so a session that began on the prior day can
        # be recovered from its last snapshot_seed forward, matching the
        # reference's fenced-range/continuity behavior across a UTC day
        # boundary. If no such partition exists, carry_records stays None
        # and behavior is unchanged (no carry recovery attempted).
        prev_date = _date_shift(date, -1)
        carry_records = None
        if prev_date in set(reader.iter_dates(venue, symbol)):
            carry_records = iter_replay_depth_records(reader.iter_depths(venue, symbol, prev_date))

        metrics = replay_records_to_depth_streaming(
            records,
            venue,
            symbol,
            iid,
            price_prec,
            size_prec,
            on_deltas_batch=on_deltas_batch,
            on_depth10_batch=on_depth10_batch,
            batch_size=batch_size,
            emit_depth10=writes_depth10,
            depth10_interval_sec=depth10_interval_sec,
            derived_depth_snapshot_levels=derived_depth_snapshot_levels,
            carry_records=carry_records,
        )

        deltas_written = 0
        depth10_written = 0
        if writes_deltas and deltas_spool.count:
            deltas_spool.commit()
            for spool_batch in deltas_spool.iter_batches(batch_size):
                catalog.write_data(spool_batch)
                deltas_written += len(spool_batch)
        if writes_depth10 and depth10_spool.count:
            depth10_spool.commit()
            for spool_batch in depth10_spool.iter_batches(batch_size):
                catalog.write_data(spool_batch)
                depth10_written += len(spool_batch)

    return metrics, deltas_written, depth10_written, deltas_skipped, depth10_skipped


def _exchange_info_from_replay_metadata(symbol: str, metadata: Optional[dict]) -> dict[str, dict]:
    """Return exchangeInfo-shaped metadata if the replay partition provides it."""
    if not metadata:
        return {}
    if isinstance(metadata.get("exchange_info"), dict):
        return {symbol: metadata["exchange_info"]}
    if isinstance(metadata.get("filters"), list):
        return {symbol: metadata}
    return {}


def generate_catalog_from_replay(
    replay_root: Path,
    catalog_root: Path,
    job_id: str,
    symbols: list[str],
    venues: list[str],
    start: datetime,
    end: datetime,
    profile: str = "trades_only",
    overwrite: bool = False,
    *,
    emit_depth10: bool = EMIT_DEPTH10_DEFAULT,
    depth10_interval_sec: float = DEPTH10_INTERVAL_SEC,
    derived_depth_snapshot_levels: int = DERIVED_DEPTH_SNAPSHOT_LEVELS,
    time_filter: str = "ts_init",
) -> dict:
    """
    Reconstruct a temporary Nautilus catalog from replay_store.

    Validation-only helper: used exclusively by
    ``validation.validate_catalog_equivalence`` to compare replay-based
    reconstruction against the reference ``convert_day.py`` converter. Not a
    supported downstream runtime API — there is no CLI for this module.

    Args:
        replay_root: Path to replay_store
        catalog_root: Output path for the temporary catalog job
        job_id: Unique job identifier
        symbols: List of symbols to include
        venues: List of venues to include
        start: Start datetime (UTC)
        end: End datetime (UTC)
        profile: Catalog profile. One of SUPPORTED_PROFILES
            (trades_only, full_l2, depth_only, depth10).
        overwrite: Delete and recreate the job dir if it exists.
        emit_depth10: Whether full_l2/depth_only also emit OrderBookDepth10.
        depth10_interval_sec: Minimum interval between derived Depth10 snapshots.
        derived_depth_snapshot_levels: Levels per derived Depth10 snapshot (<=10).
        time_filter: Window filter field for catalog reads ('ts_init' or 'ts_event').

    Returns:
        Status dict with manifest
    """
    status = {
        "job_id": job_id,
        "status": "success",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "profile": profile,
        "symbols_requested": symbols,
        "venues_requested": venues,
        "requested_symbols": symbols,
        "requested_venues": venues,
        "time_filter": time_filter,
        "symbols_processed": [],
        "found_partitions": [],
        "missing_partitions": [],
        "date_partitions_scanned": [],
        "records_read": {
            "trades": 0,
            "depth": 0,
        },
        "records_written": {
            "trade_ticks": 0,
            "order_book_deltas": 0,
            "order_book_depth10": 0,
        },
        "records_skipped": {
            "outside_window": 0,
            "invalid_trade": 0,
            "depth_outside_window": 0,
        },
        "skipped_invalid_records": 0,
        "depth_diagnostics": {
            "raw_depth_records_read": 0,
            "snapshot_seeds": 0,
            "resyncs": 0,
            "desyncs": 0,
            "fenced_range_count": 0,
            "bad_lines": 0,
            "emit_depth10": emit_depth10,
            "depth10_interval_sec": depth10_interval_sec,
            "derived_depth_snapshot_levels": derived_depth_snapshot_levels,
        },
        "fenced_ranges": [],
        "caveats": list(FULL_L2_CAVEATS) if profile in ("full_l2", "depth_only", "depth10") else [],
        "warnings": [],
        "errors": [],
    }

    if not NAUTILUS_AVAILABLE:
        status["status"] = "failed"
        status["errors"].append("Nautilus not installed")
        logger.error("Nautilus not available for catalog reconstruction")
        return status

    if profile not in SUPPORTED_PROFILES:
        status["status"] = "failed"
        status["errors"].append(
            f"Unsupported profile: {profile}. Supported: {', '.join(SUPPORTED_PROFILES)}."
        )
        return status

    if time_filter not in ("ts_init", "ts_event"):
        status["status"] = "failed"
        status["errors"].append(
            f"Unsupported time_filter: {time_filter}. Use 'ts_init' or 'ts_event'."
        )
        return status

    writes_trades, writes_deltas, writes_depth10 = _profile_write_flags(profile, emit_depth10)

    try:
        reader = ReplayReader(replay_root)
        job_dir = catalog_root / f"job_{job_id}"
        if job_dir.exists():
            if not overwrite:
                status["status"] = "failed"
                status["errors"].append(
                    f"Catalog job already exists: {job_dir}. Use overwrite=True."
                )
                return status
            shutil.rmtree(job_dir)
        job_dir.mkdir(parents=True, exist_ok=True)
        catalog = ParquetDataCatalog(str(job_dir))

        # Determine date range
        dates = _date_range_from_window(start, end)
        if not dates:
            status["status"] = "failed"
            status["errors"].append("End time must be after start time")
            return status
        logger.info(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")

        start_ns = int(start.timestamp() * 1_000_000_000)
        end_ns = int(end.timestamp() * 1_000_000_000)
        instruments_written: set[str] = set()
        processed_symbols: set[str] = set()
        available_venues = set(reader.iter_venues())
        target_venues = venues or sorted(available_venues)

        for venue in target_venues:
            if venue not in available_venues:
                for symbol in symbols:
                    for date in dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "venue_missing",
                        })
                continue

            available_symbols = set(reader.iter_symbols(venue))
            target_symbols = symbols or sorted(available_symbols)
            for symbol in target_symbols:
                if symbol not in available_symbols:
                    for date in dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "symbol_missing",
                        })
                    continue

                available_dates = set(reader.iter_dates(venue, symbol))
                for date in dates:
                    if date not in available_dates:
                        status["missing_partitions"].append({
                            "venue": venue,
                            "symbol": symbol,
                            "date": date,
                            "reason": "date_missing",
                        })
                        continue

                    logger.info(f"Processing {venue}/{symbol}/{date}...")
                    partition_key = f"{venue}:{symbol}:{date}"
                    status["found_partitions"].append({
                        "venue": venue,
                        "symbol": symbol,
                        "date": date,
                    })
                    status["date_partitions_scanned"].append(partition_key)
                    symbol_key = f"{venue}:{symbol}"
                    if symbol_key not in processed_symbols:
                        status["symbols_processed"].append(symbol_key)
                        processed_symbols.add(symbol_key)

                    # Load instrument metadata
                    instrument_metadata = reader.load_instrument_metadata(
                        venue, symbol, date
                    )
                    if not instrument_metadata:
                        logger.warning(
                            f"No instrument metadata for {venue}/{symbol}/{date}; "
                            "using default Nautilus instrument settings"
                        )

                    exchange_info = _exchange_info_from_replay_metadata(
                        symbol, instrument_metadata
                    )
                    instruments = build_instruments(venue, [symbol], exchange_info)
                    if not instruments:
                        status["errors"].append(f"could not build instrument for {venue}/{symbol}")
                        continue
                    instrument = instruments[0]
                    instrument_id = instrument.id
                    instrument_key = str(instrument_id)
                    if instrument_key not in instruments_written:
                        catalog.write_data([instrument])
                        instruments_written.add(instrument_key)

                    # Stream and convert trades
                    if writes_trades:
                        # issue #20 Phase 7 semantic-oracle diagnosis fix:
                        # TradeTick objects must be buffered through a
                        # ts_init-sorted ObjectSpool before being written to
                        # the catalog, exactly like convert_day.py's own
                        # _write_object_spool() does and exactly like this
                        # same function already does for deltas/depth10 in
                        # _write_depth_for_partition() below. Writing
                        # directly from reader.iter_trades()'s physical
                        # replay order (sorted by
                        # (trade_stream_session_id, trade_session_seq,
                        # raw_index)) is CORRECT for that layer but is not
                        # chronological: trade_stream_session_id is a
                        # simple in-process counter (native_trades.py)
                        # that resets on every recorder process restart, so
                        # a session started late in the day after a restart
                        # can have a SMALLER session id than an
                        # earlier-in-the-day session from a longer-running
                        # process — session-key order is intentionally not
                        # wall-clock order (this is what the whole
                        # replay/convert_day stack relies on for per-session
                        # continuity/fencing). Nautilus's ParquetDataCatalog
                        # requires a stream to be ts_init-monotonic, so the
                        # FINAL write order must be re-sorted by ts_init —
                        # bounded memory via ObjectSpool's disk-backed
                        # SQLite `ORDER BY ts_init, ordinal` (see
                        # converter/spool.py), never a full-day in-RAM sort.
                        # TradeTicks have no sequential book-state
                        # dependency (unlike deltas), so re-sorting them by
                        # ts_init before writing changes only their
                        # on-disk/write order, never their values, counts,
                        # or any other content.
                        trade_ordinal = 0
                        with ObjectSpool(prefix="cryptorecorder-gc-trade-") as trade_spool:
                            for trade in reader.iter_trades(venue, symbol, date):
                                status["records_read"]["trades"] += 1
                                ts_init_ns = int(trade.get("ts_receive_ns") or trade.get("ts_exchange_ns", 0))

                                # Nautilus catalog bounded reads are based on ts_init.
                                if ts_init_ns < start_ns:
                                    status["records_skipped"]["outside_window"] += 1
                                    continue
                                if ts_init_ns >= end_ns:
                                    status["records_skipped"]["outside_window"] += 1
                                    continue

                                trade_tick = _convert_trade_to_nautilus(
                                    trade, instrument_id, venue
                                )
                                if trade_tick:
                                    trade_ordinal = trade_spool.insert_many(
                                        [trade_tick], start_ordinal=trade_ordinal
                                    )
                                    status["records_written"]["trade_ticks"] += 1
                                else:
                                    status["records_skipped"]["invalid_trade"] += 1
                                    status["skipped_invalid_records"] += 1
                            if trade_spool.count:
                                trade_spool.commit()
                                for spool_batch in trade_spool.iter_batches(5000):
                                    catalog.write_data(spool_batch)

                    # Depth records (OrderBookDeltas / OrderBookDepth10) via the
                    # shared, validated converter engine + replay adapter.
                    if writes_deltas or writes_depth10:
                        (
                            depth_metrics,
                            deltas_written,
                            depth10_written,
                            deltas_skipped,
                            depth10_skipped,
                        ) = _write_depth_for_partition(
                            reader=reader,
                            venue=venue,
                            symbol=symbol,
                            date=date,
                            instrument=instrument,
                            catalog=catalog,
                            start_ns=start_ns,
                            end_ns=end_ns,
                            writes_deltas=writes_deltas,
                            writes_depth10=writes_depth10,
                            depth10_interval_sec=depth10_interval_sec,
                            derived_depth_snapshot_levels=derived_depth_snapshot_levels,
                            time_filter=time_filter,
                        )
                        status["records_read"]["depth"] += depth_metrics.raw_record_count
                        status["records_written"]["order_book_deltas"] += deltas_written
                        status["records_written"]["order_book_depth10"] += depth10_written
                        status["records_skipped"]["depth_outside_window"] += (
                            deltas_skipped + depth10_skipped
                        )
                        diag = status["depth_diagnostics"]
                        diag["raw_depth_records_read"] += depth_metrics.raw_record_count
                        diag["snapshot_seeds"] += depth_metrics.snapshot_seed_count
                        diag["resyncs"] += depth_metrics.resync_count
                        diag["desyncs"] += depth_metrics.desync_events
                        diag["fenced_range_count"] += len(depth_metrics.fenced_ranges)
                        diag["bad_lines"] += depth_metrics.bad_lines
                        for fence in depth_metrics.fenced_ranges:
                            enriched = dict(fence)
                            enriched.setdefault("venue", venue)
                            enriched.setdefault("symbol", symbol)
                            enriched["date"] = date
                            status["fenced_ranges"].append(enriched)

                    logger.info(
                        f"Processed {venue}/{symbol}/{date}: "
                        f"trades={status['records_written']['trade_ticks']}, "
                        f"deltas={status['records_written']['order_book_deltas']}, "
                        f"depth10={status['records_written']['order_book_depth10']}"
                    )

        if writes_trades and status["records_written"]["trade_ticks"] == 0:
            status["warnings"].append(
                "No TradeTick records were written for the requested venues/symbols/window."
            )
        if writes_deltas and status["records_written"]["order_book_deltas"] == 0:
            status["warnings"].append(
                "No OrderBookDeltas records were written for the requested venues/symbols/window."
            )

        # Write manifest
        manifest = {
            "job_id": job_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "profile": profile,
            "requested_symbols": symbols,
            "requested_venues": venues,
            "symbols": status["symbols_processed"],
            "found_partitions": status["found_partitions"],
            "missing_partitions": status["missing_partitions"],
            "date_partitions_scanned": status["date_partitions_scanned"],
            "time_filter": time_filter,
            "time_window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
            "records_read": status["records_read"],
            "record_counts": status["records_written"],
            "records_skipped": status["records_skipped"],
            "skipped_invalid_records": status["skipped_invalid_records"],
            "instrument_count": len(instruments_written),
            "replay_source": str(replay_root),
            "depth_diagnostics": status["depth_diagnostics"],
            "fenced_ranges": status["fenced_ranges"],
            "equivalence_caveats": status["caveats"],
            "warnings": status["warnings"],
        }

        manifest_path = job_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(
            f"Catalog reconstructed: job_id={job_id}, profile={profile}, "
            f"symbols={len(status['symbols_processed'])}, "
            f"trades={status['records_written']['trade_ticks']}, "
            f"deltas={status['records_written']['order_book_deltas']}, "
            f"depth10={status['records_written']['order_book_depth10']}"
        )

    except Exception as e:
        status["status"] = "failed"
        status["errors"].append(str(e))
        logger.error(f"Failed to reconstruct catalog: {e}")

    return status
