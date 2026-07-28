"""
stores.replay_writer — Memory-bounded deterministic Parquet writing for replay_store.

Uses disk-backed SQLite spools (via converter.spool.RawRecordSpool) to avoid
retaining a full symbol/day in Python lists.  Records are written to Parquet
incrementally in bounded batches using pyarrow.parquet.ParquetWriter so that peak
RSS remains independent of the total record count.

Ordering contracts (unchanged from v0):
    depth:  (stream_session_id, session_seq, raw_index)
    trades: (trade_stream_session_id, trade_session_seq, raw_index)

Output format (unchanged from v0):
    replay_store/
      venue=<VENUE>/
        symbol=<SYMBOL>/
          date=<DATE>/
            depth.parquet
            trades.parquet
            instrument.json   (optional)
            manifest.json
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from converter.spool import RawRecordSpool
from .replay_schema import (
    DEPTH_REPLAY_SCHEMA,
    TRADE_REPLAY_SCHEMA,
    MANIFEST_SCHEMA,
    DEPTH_REPLAY_SCHEMA_V1,
    TRADE_REPLAY_SCHEMA_V1,
    FORMAT_VERSION_V1,
    SCHEMA_VERSION_V1,
    BUILDER_VERSION_V1,
    SUPPORTED_SCHEMA_VERSIONS,
    DEPTH_RECORD_TYPE_CODES,
    TRADE_RECORD_TYPE_CODES,
    pack_depth_flags,
    encode_fixed_point,
    encode_aggressor_side,
    normalized_decimal_scale,
)

logger = logging.getLogger(__name__)

# Number of spool rows read per Parquet row-group. Unchanged default for v0
# (and for v1 unless overridden) -- issue #20 Phase 7 measured format
# optimization uses larger, schema_version=1-only batch sizes below instead
# of changing this shared default, so v0's physical output is byte-for-byte
# unchanged.
_DEFAULT_PARQUET_BATCH = int(
    os.environ.get("CRYPTO_RECORDER_REPLAY_PARQUET_BATCH", "5000")
)

# ============================================================================
# issue #20 Phase 7: measured v1-only Parquet encoding profile
# ============================================================================
#
# Selected via a representative-symbol sweep (ADAUSDT/BTCUSDT/ETHUSDT spot,
# BTCUSDT/ETHUSDT/VELVETUSDT/LABUSDT futures -- the two highest-local-volume
# symbols and one of the five scale-corrected anomalous futures symbols),
# testing row-group targets (~64/128/256 MiB), ZSTD levels (3/6/9),
# dictionary on/off, DELTA_BINARY_PACKED for monotonic session/sequence/
# timestamp integer columns, and BYTE_STREAM_SPLIT for the int64 fixed-point
# mantissa columns (including the nested bids/asks list-of-struct mantissas).
# ZSTD level 6 + no dictionary + delta-encoded monotonic integers +
# byte-stream-split mantissas measured consistently smaller than every other
# tested combination across all 7 representative symbols (see
# docs/CHANGE_AUDIT.md for the full measured comparison table). This is a
# pure Parquet *physical encoding* change: every field's logical type,
# nullability, and semantic meaning are completely unchanged, so any reader
# using standard Parquet/Arrow decoding (unaware of these encoding choices)
# reads back byte-for-byte identical logical values.
#
# v0 is completely unaffected: none of these constants are read unless
# schema_version == 1.
V1_COMPRESSION_LEVEL = int(os.environ.get("CRYPTO_RECORDER_REPLAY_V1_ZSTD_LEVEL", "6"))

# Larger row groups measurably reduce size (row-group/dictionary-page
# overhead amortized over more rows), but batches are held fully in memory
# before being flushed as one row group -- so these are deliberately modest
# multiples of the v0 default (5000), not an aggressive byte-target-driven
# size (which would require knowing per-row serialized size in advance,
# violating the bounded/streaming design). Independently configurable so an
# operator can tune for a specific machine's RAM headroom without touching
# v0's batch size.
V1_DEPTH_PARQUET_BATCH = int(os.environ.get("CRYPTO_RECORDER_REPLAY_V1_DEPTH_BATCH", "20000"))
V1_TRADE_PARQUET_BATCH = int(os.environ.get("CRYPTO_RECORDER_REPLAY_V1_TRADE_BATCH", "50000"))

# Disabling dictionary encoding entirely (rather than per-column) measured
# smaller on every representative symbol, including for already-low-
# cardinality repeated string columns (e.g. trades' `market_type`, which
# only ever takes 2 distinct values) -- verified directly at scale
# (VELVETUSDT, 11.3M trade rows): `market_type` compressed to 49,578 bytes
# with dictionary disabled vs. 32,522 bytes with it enabled, a negligible
# ~17 KB difference against an overall ~8 MB smaller total file, because
# ZSTD's own repetition detection compresses a page of near-identical short
# strings almost as well as RLE-dictionary-then-ZSTD would. No column
# needed a manually-preserved dictionary exception.
V1_USE_DICTIONARY = False

# Monotonic (or near-monotonic within a session) integer/timestamp columns
# -- safe, lossless DELTA_BINARY_PACKED candidates per the Phase 7 sweep.
V1_DEPTH_COLUMN_ENCODING = {
    "session_seq": "DELTA_BINARY_PACKED",
    "raw_index": "DELTA_BINARY_PACKED",
    "ts_exchange_ns": "DELTA_BINARY_PACKED",
    "ts_receive_ns": "DELTA_BINARY_PACKED",
    # Nested list<struct> fixed-point mantissa columns -- BYTE_STREAM_SPLIT
    # measured a further ~20% reduction on top of delta-encoded integers
    # alone for large futures symbols (e.g. BINANCE_USDTF/BTCUSDT depth:
    # 325,308,871 -> 255,321,595 bytes, ~21.5% smaller).
    "bids.list.element.price_mantissa": "BYTE_STREAM_SPLIT",
    "bids.list.element.size_mantissa": "BYTE_STREAM_SPLIT",
    "asks.list.element.price_mantissa": "BYTE_STREAM_SPLIT",
    "asks.list.element.size_mantissa": "BYTE_STREAM_SPLIT",
}
V1_TRADE_COLUMN_ENCODING = {
    "trade_session_seq": "DELTA_BINARY_PACKED",
    "raw_index": "DELTA_BINARY_PACKED",
    "ts_exchange_ns": "DELTA_BINARY_PACKED",
    "ts_receive_ns": "DELTA_BINARY_PACKED",
    "price_mantissa": "BYTE_STREAM_SPLIT",
    "quantity_mantissa": "BYTE_STREAM_SPLIT",
}


def _derive_fixed_point_scales(
    venue: str, symbol: str, date: str, data_root: "Path | None" = None
) -> "tuple[int, int]":
    """Derive (price_scale, qty_scale) from date-specific Binance exchangeInfo
    filters (``PRICE_FILTER.tickSize`` / ``LOT_SIZE.stepSize`` /
    ``MARKET_LOT_SIZE.stepSize`` where present), per the checked-in Phase 3
    field/consumer/integrity matrix. Spot and futures are treated
    independently because ``load_exchange_info`` is looked up per ``venue``.

    Args:
        data_root: raw data root to read exchangeInfo from. Defaults to
            ``config.DATA_ROOT`` (via ``load_exchange_info``'s own default)
            for backward compatibility, but the canonical builder
            (``pipeline.build_replay_store.build_replay_for_symbol``) always
            passes its own ``data_root`` explicitly, so a custom
            ``--data-root`` build derives its scale from the SAME raw root
            it consumed for everything else.

    Raises ``ValueError`` with a clear message (never guesses a scale) if the
    required filters are not available for this venue/symbol/date — a v1
    build must not proceed without an exact, source-derived scale.
    """
    from converter.instruments import load_exchange_info, _get_filter, _precision_from_str

    exchange_info = load_exchange_info(venue, date, data_root=data_root)
    info = exchange_info.get(symbol)
    if info is None:
        raise ValueError(
            f"Cannot build schema_version=1 replay for {venue}/{symbol}/{date}: "
            "no exchangeInfo entry found for this venue/symbol/date. The v1 "
            "fixed-point encoding requires date-specific PRICE_FILTER.tickSize/"
            "LOT_SIZE.stepSize and refuses to guess a scale."
        )
    filters = info.get("filters", [])
    pf = _get_filter(filters, "PRICE_FILTER")
    lf = _get_filter(filters, "LOT_SIZE")
    mlf = _get_filter(filters, "MARKET_LOT_SIZE")
    tick_size = pf.get("tickSize")
    step_size = lf.get("stepSize")
    if not tick_size or not step_size:
        raise ValueError(
            f"Cannot build schema_version=1 replay for {venue}/{symbol}/{date}: "
            "exchangeInfo is missing PRICE_FILTER.tickSize or LOT_SIZE.stepSize."
        )
    price_scale = _precision_from_str(tick_size)
    qty_scale = _precision_from_str(step_size)
    market_step = mlf.get("stepSize")
    if market_step:
        qty_scale = max(qty_scale, _precision_from_str(market_step))
    return price_scale, qty_scale


def _project_depth_row_v1(row: dict, price_scale: int, qty_scale: int) -> dict:
    """Project a v0-shaped depth record dict (as produced by
    ``pipeline.build_replay_store._convert_depth_record`` — unchanged) down to
    the compact v1 physical row shape. Pure function; no I/O."""
    record_type = row.get("record_type", "depth_update")

    def _level(lv: dict) -> dict:
        return {
            "price_mantissa": encode_fixed_point(lv["price_str"], price_scale),
            "size_mantissa": encode_fixed_point(lv["size_str"], qty_scale),
        }

    hash_hex = row.get("native_payload_hash")
    return {
        "stream_session_id": int(row.get("stream_session_id", 0)),
        "session_seq": int(row.get("session_seq", 0)),
        "raw_index": int(row.get("raw_index", 0)),
        "record_type_code": DEPTH_RECORD_TYPE_CODES[record_type],
        "U": row.get("U"),
        "u": row.get("u"),
        "pu": row.get("pu"),
        "ts_exchange_ns": int(row.get("ts_exchange_ns") or 0),
        "ts_receive_ns": int(row.get("ts_receive_ns") or 0),
        "bids": [_level(lv) for lv in (row.get("bids") or [])],
        "asks": [_level(lv) for lv in (row.get("asks") or [])],
        "flags": pack_depth_flags(
            bool(row.get("is_snapshot_seed", False)),
            bool(row.get("is_depth_update", False)),
            bool(row.get("is_sync_state", False)),
            bool(row.get("is_desync", False)),
            bool(row.get("is_resync", False)),
        ),
        "quality_flags": row.get("quality_flags"),
        "native_payload_hash": bytes.fromhex(hash_hex) if hash_hex else None,
    }


def _project_trade_row_v1(row: dict, price_scale: int, qty_scale: int) -> dict:
    """Project a v0-shaped trade record dict (as produced by
    ``pipeline.build_replay_store._convert_trade_record`` — unchanged) down to
    the compact v1 physical row shape. Pure function; no I/O."""
    record_type = row.get("record_type", "trade")
    hash_hex = row.get("native_payload_hash")
    return {
        "trade_stream_session_id": int(row.get("trade_stream_session_id", 0)),
        "trade_session_seq": int(row.get("trade_session_seq", 0)),
        "raw_index": int(row.get("raw_index", 0)),
        "record_type_code": TRADE_RECORD_TYPE_CODES[record_type],
        "market_type": row.get("market_type", "spot"),
        "trade_id": row.get("trade_id"),
        "agg_trade_id": row.get("agg_trade_id"),
        "ts_exchange_ns": int(row.get("ts_exchange_ns") or 0),
        "ts_receive_ns": int(row.get("ts_receive_ns") or 0),
        "price_mantissa": encode_fixed_point(str(row["price_str"]), price_scale),
        "quantity_mantissa": encode_fixed_point(str(row["quantity_str"]), qty_scale),
        "buyer_maker": bool(row.get("buyer_maker", False)),
        "aggressor_side_code": encode_aggressor_side(row.get("aggressor_side")),
        "quality_flags": row.get("quality_flags"),
        "native_payload_hash": bytes.fromhex(hash_hex) if hash_hex else None,
    }


def _compute_sha256(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def validate_partition(partition_dir: Path) -> bool:
    """Return True only if partition_dir is a complete, checksum-valid,
    schema-version-valid replay partition.

    This is the single source of truth for partition validity, shared by
    ReplayWriter.publish() (post-publication validation) and
    pipeline.build_replay_store (skip-if-valid / crash-recovery checks) so
    that both call sites can never disagree about what "valid" means.

    Version-aware (issue #20 Phase 5 correction): a partition is valid only
    when EITHER
      - the manifest has no ``schema_version`` field (legacy v0) and the
        physical depth/trades Parquet files satisfy the legacy v0 schema; OR
      - the manifest has an explicit, supported ``schema_version`` and all
        required version-specific metadata/physical-schema checks for that
        version pass.
    An unsupported ``schema_version``, missing required v1 metadata, or a
    physical-schema mismatch for the declared version all return False —
    never treated as a valid, skippable partition.
    """
    manifest_path = partition_dir / "manifest.json"
    depth_path = partition_dir / "depth.parquet"
    trades_path = partition_dir / "trades.parquet"
    if not (manifest_path.exists() and depth_path.exists() and trades_path.exists()):
        return False
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        if manifest.get("status") != "complete":
            return False
        for key, path in (("depth_checksum", depth_path), ("trades_checksum", trades_path)):
            expected = manifest.get(key)
            if not expected:
                return False
            if _compute_sha256(path) != expected:
                logger.warning(f"Checksum mismatch for {partition_dir} ({key})")
                return False
        return _validate_schema_version_contract(partition_dir, manifest, depth_path, trades_path)
    except Exception as e:
        logger.warning(f"Partition validation failed for {partition_dir}: {e}")
        return False


def _validate_schema_version_contract(
    partition_dir: Path,
    manifest: dict,
    depth_path: Path,
    trades_path: Path,
) -> bool:
    """Version-specific completeness/physical-schema checks, split out of
    validate_partition() for clarity. Called only after status/checksum
    checks already passed."""
    if "schema_version" not in manifest:
        # Legacy v0: physical files must match the legacy v0 schema exactly
        # (catches a partition whose manifest was stripped of
        # schema_version but whose physical files are actually v1, or vice
        # versa — an unsupported/ambiguous state must fail, not be
        # silently accepted as v0).
        return (
            _schema_matches(depth_path, DEPTH_REPLAY_SCHEMA)
            and _schema_matches(trades_path, TRADE_REPLAY_SCHEMA)
        )

    version = manifest["schema_version"]
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        logger.warning(
            f"Unsupported schema_version={version!r} for {partition_dir} "
            f"(supported: {SUPPORTED_SCHEMA_VERSIONS})"
        )
        return False

    if version == 0:
        return (
            _schema_matches(depth_path, DEPTH_REPLAY_SCHEMA)
            and _schema_matches(trades_path, TRADE_REPLAY_SCHEMA)
        )

    if version == 1:
        if manifest.get("format_version") != FORMAT_VERSION_V1:
            logger.warning(
                f"Partition {partition_dir} has schema_version=1 but "
                f"format_version={manifest.get('format_version')!r} "
                f"(expected {FORMAT_VERSION_V1!r})"
            )
            return False
        if not manifest.get("builder_version"):
            logger.warning(f"Partition {partition_dir} (v1) missing builder_version")
            return False

        price_scale = manifest.get("price_scale")
        qty_scale = manifest.get("qty_scale")
        if not isinstance(price_scale, int) or isinstance(price_scale, bool) or price_scale < 0:
            logger.warning(f"Partition {partition_dir} (v1) has invalid price_scale: {price_scale!r}")
            return False
        if not isinstance(qty_scale, int) or isinstance(qty_scale, bool) or qty_scale < 0:
            logger.warning(f"Partition {partition_dir} (v1) has invalid qty_scale: {qty_scale!r}")
            return False

        encoding_profile = manifest.get("encoding_profile")
        if not isinstance(encoding_profile, dict):
            logger.warning(f"Partition {partition_dir} (v1) missing encoding_profile")
            return False
        for required_key in ("compression", "compression_level", "row_group_batch_size"):
            if required_key not in encoding_profile:
                logger.warning(
                    f"Partition {partition_dir} (v1) encoding_profile missing "
                    f"required key {required_key!r}"
                )
                return False

        return (
            _schema_matches(depth_path, DEPTH_REPLAY_SCHEMA_V1)
            and _schema_matches(trades_path, TRADE_REPLAY_SCHEMA_V1)
        )

    # Unreachable given the SUPPORTED_SCHEMA_VERSIONS check above, but fail
    # closed rather than silently accept.
    return False


def _schema_matches(parquet_path: Path, expected_schema: pa.Schema) -> bool:
    """Return True only if parquet_path's on-disk Arrow schema has exactly
    the same field names and types as expected_schema (order-independent;
    Parquet round-tripping may reorder metadata but must not change the
    logical field set)."""
    try:
        actual = pq.ParquetFile(parquet_path).schema_arrow
    except Exception as e:
        logger.warning(f"Could not read schema of {parquet_path}: {e}")
        return False
    actual_fields = {f.name: f.type for f in actual}
    expected_fields = {f.name: f.type for f in expected_schema}
    return actual_fields == expected_fields


def _write_channel_incremental(
    spool: RawRecordSpool,
    out_path: Path,
    schema: pa.Schema,
    parquet_batch_size: int,
    row_transform: "Optional[Callable[[dict], dict]]" = None,
    *,
    compression_level: int = 3,
    use_dictionary=True,
    column_encoding: "Optional[dict]" = None,
) -> "tuple[int, int, int]":
    """Write all spool records to out_path in bounded batches.

    ``row_transform``, when given, is applied to each record (one at a time,
    not buffered) before it joins the current batch — used to project a
    v0-shaped spooled record dict down to the compact v1 physical row shape
    without ever materializing more than one row-group's worth of rows.

    ``compression_level``/``use_dictionary``/``column_encoding`` default to
    the original, unchanged v0 behavior (ZSTD level 3, dictionary enabled,
    no explicit per-column encoding) — only ``finalize_staging()``'s
    schema_version=1 call site overrides these (issue #20 Phase 7 measured
    Parquet encoding profile; see the ``V1_*`` constants above), so v0's
    physical output is completely unaffected.

    Returns (record_count, ts_min_ns, ts_max_ns).
    An empty channel produces a schema-bearing empty Parquet file.
    """
    record_count = 0
    ts_min: "int | None" = None
    ts_max: "int | None" = None
    writer: "pq.ParquetWriter | None" = None

    def _new_writer() -> pq.ParquetWriter:
        kwargs: dict = dict(
            schema=schema,
            compression="zstd",
            compression_level=compression_level,
            use_dictionary=use_dictionary,
        )
        if column_encoding:
            kwargs["column_encoding"] = column_encoding
        return pq.ParquetWriter(str(out_path), **kwargs)

    try:
        batch: list = []
        for record in spool.iter_records():
            ts = int(record.get("ts_exchange_ns") or 0)
            row = row_transform(record) if row_transform is not None else record
            batch.append(row)
            if ts_min is None or ts < ts_min:
                ts_min = ts
            if ts_max is None or ts > ts_max:
                ts_max = ts
            record_count += 1
            if len(batch) >= parquet_batch_size:
                tbl = pa.Table.from_pylist(batch, schema=schema)
                if writer is None:
                    writer = _new_writer()
                writer.write_table(tbl)
                del tbl, batch
                batch = []

        # Flush remainder (or write empty schema-bearing file)
        tbl = pa.Table.from_pylist(batch, schema=schema)
        if writer is None:
            writer = _new_writer()
        writer.write_table(tbl)
        del tbl, batch
    finally:
        if writer is not None:
            writer.close()

    return record_count, (ts_min or 0), (ts_max or 0)


class ReplayWriter:
    """
    Memory-bounded writer for a single venue/symbol/date replay partition.

    Records are spooled to SQLite on disk immediately and read back in
    bounded batches during finalization so that peak RSS is proportional to
    parquet_batch_size, not to the total record count.

    Output layout (Hive-style, unchanged from v0):
        replay_store/
          venue=BINANCE_SPOT/
            symbol=BTCUSDT/
              date=2026-06-15/
                depth.parquet
                trades.parquet
                instrument.json
                manifest.json
    """

    def __init__(
        self,
        replay_root: Path,
        venue: str,
        symbol: str,
        date: str,
        *,
        parquet_batch_size: int = _DEFAULT_PARQUET_BATCH,
        schema_version: int = 0,
        price_scale: "Optional[int]" = None,
        qty_scale: "Optional[int]" = None,
        source_identity: "Optional[dict]" = None,
        data_root: "Optional[Path]" = None,
    ):
        """
        Args:
            schema_version: 0 (default, unchanged legacy physical layout —
                every existing caller that does not pass this argument keeps
                producing exactly today's v0 output) or 1 (the issue #20
                Phase 5 compact prototype schema). Any other value raises
                immediately — never silently falls back to v0.
            price_scale / qty_scale: only used when schema_version=1. If not
                given, derived automatically from date-specific Binance
                exchangeInfo filters (see ``_derive_fixed_point_scales``) the
                first time a schema_version=1 partition is finalized. Tests
                may pass these explicitly to avoid depending on on-disk
                exchangeInfo fixtures.
            source_identity: only used when schema_version=1. The caller
                (e.g. ``pipeline.build_replay_store.build_replay_for_symbol``)
                must compute this itself via
                ``pipeline.raw_manifest.compute_raw_source_identity()`` using
                the EXACT ``data_root`` and channels it actually streamed
                from, and pass the result here — ``ReplayWriter`` never
                independently recomputes source identity against the global
                ``config.DATA_ROOT`` (issue #20 Phase 5 correction: doing so
                could silently record checksums from a different raw root
                than the one actually consumed by this build, e.g. under a
                custom ``--data-root``). If omitted for a schema_version=1
                partition, the manifest honestly records
                ``source_identity`` as incomplete/not computed rather than
                guessing a root.
            data_root: raw data root used ONLY for automatic price/qty scale
                derivation (exchangeInfo lookup) when schema_version=1 and
                price_scale/qty_scale are not explicitly given. Defaults to
                ``config.DATA_ROOT`` for backward compatibility, but the
                canonical builder always passes its own ``data_root``
                explicitly so a custom ``--data-root`` build derives its
                scale from the exact same raw root it consumed for
                everything else — never a different, global default root.
        """
        if schema_version not in (0, 1):
            raise ValueError(
                f"Unsupported schema_version={schema_version!r} for ReplayWriter "
                "(supported: 0 (legacy), 1 (issue #20 Phase 5 compact prototype))"
            )
        self.replay_root = Path(replay_root)
        self.venue = venue
        self.symbol = symbol
        self.date = date
        self._parquet_batch_size = parquet_batch_size
        self.schema_version = schema_version
        self._price_scale = price_scale
        self._qty_scale = qty_scale
        self._source_identity = source_identity
        self._data_root = data_root

        # Running maximum observed normalized decimal scale (issue #20
        # Phase 7 correction), updated incrementally as each batch is
        # spooled — never by rescanning a partition. Only meaningful (and
        # only ever updated) for schema_version=1; kept at 0 for v0 so the
        # legacy path performs zero extra work. See write_depth_batch/
        # write_trades_batch and finalize_staging.
        self._observed_price_scale = 0
        self._observed_qty_scale = 0

        self.output_dir = (
            self.replay_root
            / f"venue={venue}"
            / f"symbol={symbol}"
            / f"date={date}"
        )
        self.staging_dir = self.output_dir.parent / f".staging_{date}_{symbol}"

        # Counters (compatible with existing callers)
        self.depth_count = 0
        self.trade_count = 0
        self._manifest: "dict[str, Any] | None" = None

        # Spool files live inside the staging directory so that a stale-staging
        # cleanup (shutil.rmtree on .staging_*) also removes orphaned SQLite
        # spools even when a previous SIGKILL/OOM prevented normal cleanup.
        self._spool_scratch_dir = self.staging_dir / "scratch"
        self._spool_scratch_dir.mkdir(parents=True, exist_ok=True)

        self._depth_spool: "RawRecordSpool | None" = None
        self._trade_spool: "RawRecordSpool | None" = None

    def set_source_identity(self, source_identity: dict) -> None:
        """Explicitly supply the raw source-identity dict for this
        partition (only meaningful for ``schema_version=1``). The caller
        must have computed this against the exact ``data_root``/channels it
        actually consumed (e.g. via
        ``pipeline.raw_manifest.compute_raw_source_identity()``) — see the
        constructor's ``source_identity`` docstring for why ``ReplayWriter``
        never computes this itself."""
        self._source_identity = source_identity

    # ------------------------------------------------------------------
    # Lazy spool init
    # ------------------------------------------------------------------

    def _get_depth_spool(self) -> RawRecordSpool:
        if self._depth_spool is None:
            self._depth_spool = RawRecordSpool(
                temp_dir=self._spool_scratch_dir,
                prefix="replay-depth-",
            )
        return self._depth_spool

    def _get_trade_spool(self) -> RawRecordSpool:
        if self._trade_spool is None:
            self._trade_spool = RawRecordSpool(
                temp_dir=self._spool_scratch_dir,
                prefix="replay-trade-",
            )
        return self._trade_spool

    # ------------------------------------------------------------------
    # Public write API (compatible signatures with v0)
    # ------------------------------------------------------------------

    def write_depth_batch(self, records: list) -> None:
        """Spool depth records to disk (O(batch) memory, not O(day))."""
        spool = self._get_depth_spool()
        track_scale = self.schema_version == 1
        for r in records:
            sort_key = (
                int(r.get("stream_session_id", 0)),
                int(r.get("session_seq", 0)),
                int(r.get("raw_index", 0)),
            )
            spool.insert(r, sort_key=sort_key, raw_index=int(r.get("raw_index", 0)))
            if track_scale:
                # Bounded incremental scan of this single record's levels
                # only (never a rescan of the spool/partition) — updates a
                # small running maximum, not a growing collection.
                for side in ("bids", "asks"):
                    for level in (r.get(side) or ()):
                        ps = normalized_decimal_scale(level["price_str"])
                        if ps > self._observed_price_scale:
                            self._observed_price_scale = ps
                        ss = normalized_decimal_scale(level["size_str"])
                        if ss > self._observed_qty_scale:
                            self._observed_qty_scale = ss
        self.depth_count += len(records)

    def write_trades_batch(self, records: list) -> None:
        """Spool trade records to disk (O(batch) memory, not O(day))."""
        spool = self._get_trade_spool()
        track_scale = self.schema_version == 1
        for r in records:
            sort_key = (
                int(r.get("trade_stream_session_id", 0)),
                int(r.get("trade_session_seq", 0)),
                int(r.get("raw_index", 0)),
            )
            spool.insert(r, sort_key=sort_key, raw_index=int(r.get("raw_index", 0)))
            if track_scale:
                ps = normalized_decimal_scale(r["price_str"])
                if ps > self._observed_price_scale:
                    self._observed_price_scale = ps
                qs = normalized_decimal_scale(r["quantity_str"])
                if qs > self._observed_qty_scale:
                    self._observed_qty_scale = qs
        self.trade_count += len(records)

    # ------------------------------------------------------------------
    # Finalization (incremental Parquet, no full-day lists)
    # ------------------------------------------------------------------

    def finalize_staging(self) -> "dict[str, Any]":
        """
        Write spooled records to staging Parquet files using bounded batches.

        The depth channel is fully written and its Parquet writer closed before
        the trade channel begins, so both channels are never simultaneously in
        memory.

        Returns:
            Manifest dict with counts, checksums, and timestamp range.
        """
        depth_path = self.staging_dir / "depth.parquet"
        trades_path = self.staging_dir / "trades.parquet"

        row_transform_depth = None
        row_transform_trade = None
        depth_schema = DEPTH_REPLAY_SCHEMA
        trade_schema = TRADE_REPLAY_SCHEMA
        declared_price_scale: "Optional[int]" = None
        declared_qty_scale: "Optional[int]" = None

        if self.schema_version == 1:
            # Only compute the declared (exchangeInfo-derived) scale when at
            # least one of price_scale/qty_scale was not explicitly supplied
            # by the caller — an explicit override (as tests use, precisely
            # to avoid depending on on-disk exchangeInfo fixtures) must never
            # trigger an exchangeInfo lookup it doesn't need.
            if self._price_scale is None or self._qty_scale is None:
                declared_price_scale, declared_qty_scale = _derive_fixed_point_scales(
                    self.venue, self.symbol, self.date, data_root=self._data_root
                )

            # issue #20 Phase 7 correction: the exchange's declared
            # PRICE_FILTER.tickSize/LOT_SIZE.stepSize/MARKET_LOT_SIZE.stepSize
            # is not always sufficient — real recorded values on a given day
            # can carry finer precision than the exchange's own declared
            # filters (observed directly on 5 real BINANCE_USDTF symbols on
            # 2026-06-11: e.g. declared PRICE_FILTER.tickSize scale 5 for
            # BTWUSDT vs. an actual observed depth/trade price scale of 6).
            # The automatically-derived scale is therefore
            # max(declared, observed-this-partition), never declared alone.
            #
            # An EXPLICITLY supplied scale (test fixtures, or any future
            # caller that wants full manual control) is never silently
            # enlarged to accommodate observed data — instead, observed data
            # exceeding an explicit override fails clearly, so a caller is
            # never surprised by silently-changed encoding behavior.
            if self._price_scale is not None:
                if self._observed_price_scale > self._price_scale:
                    raise ValueError(
                        f"Cannot build schema_version=1 replay for "
                        f"{self.venue}/{self.symbol}/{self.date}: explicitly "
                        f"supplied price_scale={self._price_scale} is "
                        f"insufficient — observed depth/trade price data "
                        f"requires scale >= {self._observed_price_scale} to "
                        "be represented exactly. Refusing to silently "
                        "enlarge an explicit override."
                    )
            else:
                self._price_scale = max(declared_price_scale, self._observed_price_scale)

            if self._qty_scale is not None:
                if self._observed_qty_scale > self._qty_scale:
                    raise ValueError(
                        f"Cannot build schema_version=1 replay for "
                        f"{self.venue}/{self.symbol}/{self.date}: explicitly "
                        f"supplied qty_scale={self._qty_scale} is "
                        f"insufficient — observed depth/trade size/quantity "
                        f"data requires scale >= {self._observed_qty_scale} "
                        "to be represented exactly. Refusing to silently "
                        "enlarge an explicit override."
                    )
            else:
                self._qty_scale = max(declared_qty_scale, self._observed_qty_scale)

            price_scale = self._price_scale
            qty_scale = self._qty_scale
            depth_schema = DEPTH_REPLAY_SCHEMA_V1
            trade_schema = TRADE_REPLAY_SCHEMA_V1
            row_transform_depth = lambda rec: _project_depth_row_v1(rec, price_scale, qty_scale)
            row_transform_trade = lambda rec: _project_trade_row_v1(rec, price_scale, qty_scale)

        # --- Depth ---
        depth_spool = self._get_depth_spool()
        depth_spool.commit()
        if self.schema_version == 1:
            depth_count, ts_d_min, ts_d_max = _write_channel_incremental(
                depth_spool, depth_path, depth_schema, V1_DEPTH_PARQUET_BATCH, row_transform_depth,
                compression_level=V1_COMPRESSION_LEVEL,
                use_dictionary=V1_USE_DICTIONARY,
                column_encoding=V1_DEPTH_COLUMN_ENCODING,
            )
        else:
            depth_count, ts_d_min, ts_d_max = _write_channel_incremental(
                depth_spool, depth_path, depth_schema, self._parquet_batch_size, row_transform_depth
            )
        depth_spool.close()
        self._depth_spool = None
        logger.info(f"Wrote staging depth: {depth_path} ({depth_count} records)")

        # --- Trades ---
        trade_spool = self._get_trade_spool()
        trade_spool.commit()
        if self.schema_version == 1:
            trade_count, ts_t_min, ts_t_max = _write_channel_incremental(
                trade_spool, trades_path, trade_schema, V1_TRADE_PARQUET_BATCH, row_transform_trade,
                compression_level=V1_COMPRESSION_LEVEL,
                use_dictionary=V1_USE_DICTIONARY,
                column_encoding=V1_TRADE_COLUMN_ENCODING,
            )
        else:
            trade_count, ts_t_min, ts_t_max = _write_channel_incremental(
                trade_spool, trades_path, trade_schema, self._parquet_batch_size, row_transform_trade
            )
        trade_spool.close()
        self._trade_spool = None
        logger.info(f"Wrote staging trades: {trades_path} ({trade_count} records)")

        # Sync authoritative counts
        self.depth_count = depth_count
        self.trade_count = trade_count

        nonzero_ts = [t for t in (ts_d_min, ts_d_max, ts_t_min, ts_t_max) if t != 0]
        ts_min = min(nonzero_ts) if nonzero_ts else 0
        ts_max = max(nonzero_ts) if nonzero_ts else 0

        depth_checksum = _compute_sha256(depth_path)
        trades_checksum = _compute_sha256(trades_path)

        # Remove the scratch directory — spools have been closed and deleted,
        # so it should now be empty.  An empty scratch/ must NOT appear in the
        # final published partition, and we must not publish while scratch
        # artifacts remain.
        if self._spool_scratch_dir.exists():
            # Verify that spool files are gone (they should be, given close() above)
            remaining = list(self._spool_scratch_dir.iterdir())
            if remaining:
                raise RuntimeError(
                    f"scratch dir {self._spool_scratch_dir} still contains "
                    f"files after spool close: {remaining}. "
                    "Refusing to publish — manual inspection required."
                )
            # Directory is empty; remove it.
            try:
                self._spool_scratch_dir.rmdir()
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot remove scratch dir {self._spool_scratch_dir}: {exc}. "
                    "Refusing to publish."
                ) from exc
            if self._spool_scratch_dir.exists():
                raise RuntimeError(
                    f"scratch dir {self._spool_scratch_dir} still exists after rmdir. "
                    "Refusing to publish."
                )

        manifest: dict = {
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date,
            "status": "complete",
            "depth_record_count": depth_count,
            "trade_record_count": trade_count,
            "ts_range_start_ns": ts_min,
            "ts_range_end_ns": ts_max,
            "depth_checksum": depth_checksum,
            "trades_checksum": trades_checksum,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }

        if self.schema_version == 1:
            # Explicit version identity (issue #20 Phase 5): a manifest with
            # no schema_version field is legacy v0 by definition, so v0 never
            # gains these keys — only schema_version=1 manifests carry them.
            manifest["format_version"] = FORMAT_VERSION_V1
            manifest["schema_version"] = SCHEMA_VERSION_V1
            manifest["builder_version"] = BUILDER_VERSION_V1
            manifest["encoding_profile"] = {
                "compression": "zstd",
                "compression_level": V1_COMPRESSION_LEVEL,
                # issue #20 Phase 7: depth and trades now use independently
                # tunable batch/row-group sizes (measured larger row groups
                # to be smaller); "row_group_batch_size" retained for
                # backward-compatible manifest-reader expectations
                # (validate_partition() only checks the key is present) and
                # set to the depth batch size, with the exact per-channel
                # values also recorded explicitly.
                "row_group_batch_size": V1_DEPTH_PARQUET_BATCH,
                "depth_row_group_batch_size": V1_DEPTH_PARQUET_BATCH,
                "trade_row_group_batch_size": V1_TRADE_PARQUET_BATCH,
                "use_dictionary": V1_USE_DICTIONARY,
                "depth_column_encoding": V1_DEPTH_COLUMN_ENCODING,
                "trade_column_encoding": V1_TRADE_COLUMN_ENCODING,
                "depth_schema_version": SCHEMA_VERSION_V1,
                "trade_schema_version": SCHEMA_VERSION_V1,
                # issue #20 Phase 7 correction: record the declared
                # (exchangeInfo-derived) and observed (streamed-record-scan)
                # scale components separately from the final selected
                # scale, so an anomalous partition (declared scale
                # insufficient for real recorded precision, or an explicit
                # override) remains explainable from the manifest alone.
                # declared_*_scale is None only when both price_scale and
                # qty_scale were supplied explicitly (no exchangeInfo
                # lookup was performed/needed).
                "price_scale_declared": declared_price_scale,
                "price_scale_observed": self._observed_price_scale,
                "qty_scale_declared": declared_qty_scale,
                "qty_scale_observed": self._observed_qty_scale,
            }
            manifest["price_scale"] = self._price_scale
            manifest["qty_scale"] = self._qty_scale
            if self._source_identity is not None:
                manifest["source_identity"] = self._source_identity
            else:
                # No source_identity was passed by the caller (issue #20
                # Phase 5 correction: ReplayWriter must never independently
                # recompute this against the global config.DATA_ROOT, since
                # that could silently record checksums from a different raw
                # root than the one actually consumed for this build, e.g.
                # under a custom --data-root). Record honestly as not
                # computed rather than guessing a root.
                manifest["source_identity"] = {
                    "channels": {},
                    "complete": False,
                    "missing_channels": ["depth_v2", "trade_v2"],
                    "error": "source_identity not supplied by caller",
                }

        self._manifest = manifest
        return manifest

    def publish(self, instrument_metadata: "Optional[dict]" = None) -> Path:
        """
        Atomically move staging to final output directory.

        A valid previously-published partition is overwritten only after
        staging is confirmed complete (manifest written).  An exception before
        os.replace keeps the existing published output intact.
        """
        if instrument_metadata:
            instrument_path = self.staging_dir / "instrument.json"
            with open(instrument_path, "w") as f:
                json.dump(instrument_metadata, f, indent=2)
            logger.info(f"Wrote instrument metadata: {instrument_path}")

        manifest = self._manifest
        if manifest is None:
            manifest = self.finalize_staging()

        manifest_path = self.staging_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Wrote manifest: {manifest_path}")

        # Atomic publication with backup/restore so the existing valid partition
        # is never lost if the replacement fails (I/O error, permissions, etc.).
        backup_dir = self.output_dir.parent / f".backup_{self.date}_{self.symbol}"
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)

        # Step 1: rename existing output to backup (if it exists).
        if self.output_dir.exists():
            if backup_dir.exists():
                shutil.rmtree(backup_dir)
            os.replace(self.output_dir, backup_dir)

        try:
            # Step 2: rename staging to canonical output.
            os.replace(self.staging_dir, self.output_dir)
        except Exception:
            # Canonical publication failed — restore backup so the last-known-
            # good partition is not lost.
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    logger.warning(
                        f"Publication failed; restored previous partition: {self.output_dir}"
                    )
                except Exception as restore_err:
                    logger.error(
                        f"Could not restore backup partition {backup_dir}: {restore_err}"
                    )
            raise

        # Step 3: canonical publication succeeded on the filesystem — but that
        # does not guarantee the published directory is a complete, valid
        # partition (missing/corrupt output under a non-standard filesystem
        # replace, truncated write, etc.). Validate before ever deleting the
        # last known-good backup.
        if not validate_partition(self.output_dir):
            logger.error(
                f"Post-publish validation failed for {self.output_dir}: "
                "missing/corrupt manifest, parquet files, or checksum mismatch. "
                "Quarantining invalid output and restoring backup if available."
            )
            quarantine_dir = self.output_dir.parent / f".quarantine_{self.date}_{self.symbol}"
            try:
                if quarantine_dir.exists():
                    shutil.rmtree(quarantine_dir)
                if self.output_dir.exists():
                    os.replace(self.output_dir, quarantine_dir)
            except Exception as qe:
                logger.error(
                    f"Could not quarantine invalid output {self.output_dir}: {qe}"
                )
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    logger.warning(
                        f"Restored previous valid partition after invalid publish: "
                        f"{self.output_dir}"
                    )
                except Exception as restore_err:
                    logger.error(
                        f"Could not restore backup partition {backup_dir} after "
                        f"invalid publish: {restore_err}"
                    )
            raise RuntimeError(
                f"Post-publish validation failed for {self.output_dir}; refusing "
                "to report success. Previous valid partition preserved/restored "
                "where possible; invalid output quarantined at "
                f"{quarantine_dir}."
            )

        # Step 4: new canonical partition is valid — delete the obsolete
        # backup on a best-effort basis. A failure here must NOT cause
        # publish() to raise or the build to fail, since the new partition is
        # already confirmed valid.
        if backup_dir.exists():
            try:
                shutil.rmtree(backup_dir)
            except Exception as backup_del_err:
                logger.warning(
                    f"Could not delete obsolete backup {backup_dir}: {backup_del_err}. "
                    "Leaving for startup cleanup; publication is still successful."
                )

        logger.info(f"Published replay data to: {self.output_dir}")
        return self.output_dir

    def cleanup_staging(self) -> None:
        """Remove staging directory and close/delete open spool files.

        Safe to call on error paths or after successful publish.
        """
        for spool_attr in ("_depth_spool", "_trade_spool"):
            spool = getattr(self, spool_attr)
            if spool is not None:
                try:
                    spool.close()
                except Exception:
                    pass
                setattr(self, spool_attr, None)
        if self.staging_dir.exists():
            try:
                shutil.rmtree(self.staging_dir)
            except Exception as exc:
                logger.error(f"Could not remove staging dir {self.staging_dir}: {exc}")
                raise RuntimeError(
                    f"cleanup_staging failed: staging dir {self.staging_dir} still exists"
                ) from exc
            if self.staging_dir.exists():
                raise RuntimeError(
                    f"cleanup_staging failed: staging dir {self.staging_dir} was not removed"
                )
