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
import stat
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
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
    DEPTH_REPLAY_SCHEMA_V2,
    TRADE_REPLAY_SCHEMA_V2,
    FORMAT_VERSION_V1,
    SCHEMA_VERSION_V1,
    BUILDER_VERSION_V1,
    FORMAT_VERSION_V2,
    SCHEMA_VERSION_V2,
    BUILDER_VERSION_V2,
    SUPPORTED_BUILDER_VERSIONS_V2,
    SUPPORTED_SCHEMA_VERSIONS,
    DEPTH_RECORD_TYPE_CODES,
    TRADE_RECORD_TYPE_CODES,
    pack_depth_flags,
    encode_fixed_point,
    encode_aggressor_side,
    normalized_decimal_scale,
)

logger = logging.getLogger(__name__)

# Number of spool rows read per Parquet row-group. This remains the unchanged
# v0 default; the compact schema_version=1/2 paths use the larger measured
# batch sizes below instead, so v0's physical output is byte-for-byte
# unchanged.
_DEFAULT_PARQUET_BATCH = int(
    os.environ.get("CRYPTO_RECORDER_REPLAY_PARQUET_BATCH", "5000")
)

# ============================================================================
# issue #20 Phase 7: measured compact v1/v2 Parquet encoding profile
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
# v0 is completely unaffected: these constants are read only for
# schema_version 1 or 2.
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
    required filters are not available for this venue/symbol/date — a compact
    v1/v2 build must not proceed without an exact, source-derived scale.
    """
    from converter.exchange_info import load_exchange_info, _get_filter, _precision_from_str

    exchange_info = load_exchange_info(venue, date, data_root=data_root)
    info = exchange_info.get(symbol)
    if info is None:
        raise ValueError(
            f"Cannot derive compact replay scales for {venue}/{symbol}/{date}: "
            "no exchangeInfo entry found for this venue/symbol/date. The v1/v2 "
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
            f"Cannot derive compact replay scales for {venue}/{symbol}/{date}: "
            "exchangeInfo is missing PRICE_FILTER.tickSize or LOT_SIZE.stepSize."
        )
    price_scale = _precision_from_str(tick_size)
    qty_scale = _precision_from_str(step_size)
    market_step = mlf.get("stepSize")
    if market_step:
        qty_scale = max(qty_scale, _precision_from_str(market_step))
    return price_scale, qty_scale


def _project_depth_row_v1(row: dict, price_scale: int, qty_scale: int, *, include_hash: bool = True) -> dict:
    """Project a v0-shaped depth record dict (as produced by
    ``pipeline.build_replay_store._convert_depth_record`` — unchanged) down to
    the shared compact v1/v2 fixed-point row shape. Pure function; no I/O.

    ``include_hash=False`` (used by schema_version=2, issue #20 Phase 7
    hierarchical-integrity candidate) omits the ``native_payload_hash`` key
    entirely from the returned dict, rather than setting it to ``None`` —
    the physical v2 schema has no such column at all, and
    ``pa.Table.from_pylist(rows, schema=...)`` requires every row dict to
    contain no keys outside the target schema.
    """
    record_type = row.get("record_type", "depth_update")

    def _level(lv: dict) -> dict:
        return {
            "price_mantissa": encode_fixed_point(lv["price_str"], price_scale),
            "size_mantissa": encode_fixed_point(lv["size_str"], qty_scale),
        }

    projected = {
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
    }
    if include_hash:
        hash_hex = row.get("native_payload_hash")
        projected["native_payload_hash"] = bytes.fromhex(hash_hex) if hash_hex else None
    return projected


def _project_trade_row_v1(row: dict, price_scale: int, qty_scale: int, *, include_hash: bool = True) -> dict:
    """Project a v0-shaped trade record dict (as produced by
    ``pipeline.build_replay_store._convert_trade_record`` — unchanged) down to
    the shared compact v1/v2 fixed-point row shape. Pure function; no I/O.

    ``include_hash=False`` — see ``_project_depth_row_v1``'s docstring.
    """
    record_type = row.get("record_type", "trade")
    projected = {
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
    }
    if include_hash:
        hash_hex = row.get("native_payload_hash")
        projected["native_payload_hash"] = bytes.fromhex(hash_hex) if hash_hex else None
    return projected


# ============================================================================
# issue #20 Phase 7 hierarchical-integrity candidate (schema_version=2):
# bounded per-block (Parquet row-group) integrity metadata, and a documented
# deterministic raw_index -> source-file mapping, replacing the removed
# per-event native_payload_hash column.
#
# Digest method (issue #20 Phase 7 review correction): block digests are
# computed from a CANONICAL, VECTORIZED encoding of the Arrow
# Table/RecordBatch backing each Parquet row-group — never by iterating
# Python row dicts and JSON-serializing them one at a time. Measured on the
# representative VELVETUSDT partition (11.4M trade rows / 426K depth rows,
# 2026-06-11): the previous per-row JSON approach took ~582s for a single
# validate_partition() call; this vectorized approach takes ~25s for the
# same trades file and ~9s for the same (nested bids/asks) depth file —
# roughly a 17x speedup. New manifests use ``arrow_canonical_v2``: a
# length-framed, round-trip-stable encoding which includes validity at every
# primitive/list/struct level, so null list vs. empty list and null struct vs.
# valid struct cannot collide. See ``_canon_array``/``_canon_table_hash``.
#
# Existing artifacts retain their recorded ``arrow_canonical_v1`` identity
# and are verified with the unchanged legacy algorithm. That method is
# deterministic for the historically produced non-null nested rows but is
# not a general injective Arrow serialization: strings/names are not length
# framed and nested validity is omitted. Exact physical-schema validation and
# complete-file SHA-256 are independent layers. The method recorded in each
# manifest selects the verifier; existing artifacts are never reinterpreted.
# ============================================================================

def _canon_array_v1(arr) -> bytes:
    """Legacy ``arrow_canonical_v1`` encoding retained for old manifests.

    This representation is deterministic for the previously accepted
    non-null nested replay data, but it does not encode list/struct validity
    or length-frame variable-width values. It is kept solely so existing v2
    artifacts remain auditable under their recorded digest-method identity.
    New manifests use ``arrow_canonical_v2`` below.

    Canonical byte encoding of one Arrow array/column,
    recursing into list/struct types — used to build a vectorized,
    round-trip-stable digest of an entire row-group's logical content
    without ever materializing individual Python row objects.

    Historical design within the produced replay-v2 physical-schema domain:
      - List columns (e.g. ``bids``/``asks``): the child ``values`` array is
        sliced to exactly this array's own logical extent and its offsets
        are normalized to start at 0, so two logically-identical list
        columns produce identical bytes regardless of any unrelated slicing
        applied earlier in the pipeline (e.g. across row-group boundaries).
        This is what makes the encoding round-trip-stable across a real
        Parquet write+read cycle for nested types, unlike hashing the raw
        Arrow IPC buffer bytes directly (measured to differ pre-write vs.
        post-read for list<struct> columns, even though logically
        identical — Arrow's physical buffer layout for nested types is not
        guaranteed IPC-stable across a Parquet round-trip).
      - Struct columns (e.g. one bid/ask level): each child field is
        recursed in schema-declared order, with the field name embedded in
        the encoding so a struct with reordered-but-renamed-identically
        fields cannot collide with a different schema.
      - Primitive columns: the validity (null) bitmap is encoded separately
        from the filled values (nulls filled with a fixed sentinel: 0 for
        numeric/int types, False for boolean, "" for strings) — this is
        what correctly distinguishes "logically null" from "the sentinel
        fill value occurring naturally," and is what makes a null-vs-value
        primitive change (or vice versa) detectable. List/struct validity is
        not represented by this legacy method.
      - Every column's canonical bytes are order-sensitive (row order is
        never permuted before encoding), so a reordering of rows always
        changes the resulting digest.
    """
    import pyarrow as _pa
    import pyarrow.compute as _pc

    arr = arr.combine_chunks() if hasattr(arr, "combine_chunks") else arr
    if _pa.types.is_dictionary(arr.type):
        # Defensive: decode dictionary-encoded columns to their logical
        # value type before canonicalizing, so the digest reflects logical
        # values only — never an incidental physical dictionary-encoding
        # choice (e.g. a low-cardinality column encoded differently by two
        # otherwise-identical writes).
        arr = arr.dictionary_decode()
    if _pa.types.is_list(arr.type) or _pa.types.is_large_list(arr.type):
        off_start = arr.offsets[0].as_py()
        off_end = arr.offsets[-1].as_py()
        offsets = arr.offsets.to_numpy(zero_copy_only=False).astype("int64")
        offsets = offsets - offsets[0]
        values = arr.values.slice(off_start, off_end - off_start)
        return offsets.tobytes() + b"|L|" + _canon_array_v1(values)
    if _pa.types.is_struct(arr.type):
        parts = []
        for i in range(arr.type.num_fields):
            fname = arr.type.field(i).name
            parts.append(fname.encode() + b"=" + _canon_array_v1(arr.field(i)))
        return b"{" + b",".join(parts) + b"}"
    validity = arr.is_valid().to_numpy(zero_copy_only=False).tobytes()
    if _pa.types.is_string(arr.type) or _pa.types.is_large_string(arr.type):
        filled = _pc.fill_null(arr, "")
        vals = b"\x00".join(v.encode() for v in filled.to_pylist())
    elif _pa.types.is_boolean(arr.type):
        filled = _pc.fill_null(arr, False)
        vals = filled.to_numpy(zero_copy_only=False).tobytes()
    else:
        filled = _pc.fill_null(arr, 0)
        vals = filled.to_numpy(zero_copy_only=False).tobytes()
    return validity + b"|V|" + vals


def _canon_table_hash_v1(table: "pa.Table") -> str:
    """Recompute the legacy ``arrow_canonical_v1`` digest."""
    digest = hashlib.sha256()
    for name in table.schema.names:
        digest.update(name.encode())
        digest.update(_canon_array_v1(table.column(name)))
    return digest.hexdigest()


def _canon_frame(label: bytes, payload: bytes) -> bytes:
    return (
        len(label).to_bytes(8, "big")
        + label
        + len(payload).to_bytes(8, "big")
        + payload
    )


def _canon_array(arr) -> bytes:
    """Injective canonical encoding for accepted replay Arrow values.

    ``arrow_canonical_v2`` length-frames every component and explicitly
    includes validity for primitive, list, and struct arrays. Consequently a
    null list differs from an empty list, a null struct differs from a valid
    struct with identical child buffers, and embedded separator bytes in
    strings or field names cannot create concatenation collisions.
    """
    import pyarrow as _pa
    import pyarrow.compute as _pc

    arr = arr.combine_chunks() if hasattr(arr, "combine_chunks") else arr
    if _pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    validity = arr.is_valid().to_numpy(zero_copy_only=False).tobytes()
    # Exact physical schema equality is validated independently before block
    # hashing. Avoid embedding Arrow's incidental nested child-name rendering
    # here because Parquet round trips may normalize it while preserving the
    # same validated logical schema and values.
    header = _canon_frame(b"validity", validity)

    if _pa.types.is_list(arr.type) or _pa.types.is_large_list(arr.type):
        off_start = arr.offsets[0].as_py()
        off_end = arr.offsets[-1].as_py()
        offsets = arr.offsets.to_numpy(zero_copy_only=False).astype("int64")
        offsets = offsets - offsets[0]
        values = arr.values.slice(off_start, off_end - off_start)
        return (
            _canon_frame(b"kind", b"list")
            + header
            + _canon_frame(b"offsets", offsets.tobytes())
            + _canon_frame(b"values", _canon_array(values))
        )
    if _pa.types.is_struct(arr.type):
        encoded = _canon_frame(b"kind", b"struct") + header
        for index in range(arr.type.num_fields):
            field = arr.type.field(index)
            encoded += _canon_frame(b"field-name", field.name.encode("utf-8"))
            encoded += _canon_frame(b"field-value", _canon_array(arr.field(index)))
        return encoded
    if _pa.types.is_string(arr.type) or _pa.types.is_large_string(arr.type):
        filled = _pc.fill_null(arr, "")
        values = b"".join(
            _canon_frame(b"string", value.encode("utf-8"))
            for value in filled.to_pylist()
        )
    elif _pa.types.is_boolean(arr.type):
        filled = _pc.fill_null(arr, False)
        values = filled.to_numpy(zero_copy_only=False).tobytes()
    else:
        filled = _pc.fill_null(arr, 0)
        values = filled.to_numpy(zero_copy_only=False).tobytes()
    return _canon_frame(b"kind", b"primitive") + header + _canon_frame(
        b"values", values
    )


def _canon_table_hash(table: "pa.Table") -> str:
    """Deterministic ``arrow_canonical_v2`` SHA-256 over one Arrow table,
    column-by-column in schema order, via ``_canon_array``. Bounded memory
    (proportional to one row-group's already-in-memory Table, never the
    full partition) and vectorized (no per-row Python object creation)."""
    digest = hashlib.sha256()
    digest.update(b"arrow_canonical_v2")
    for name in table.schema.names:
        digest.update(_canon_frame(b"column-name", name.encode("utf-8")))
        digest.update(_canon_frame(b"column-value", _canon_array(table.column(name))))
    return digest.hexdigest()


# Digest method identifier recorded in the manifest's integrity metadata —
# an explicit, versioned tag (never silently assumed) so any future digest
# method change is distinguishable from this one.
BLOCK_DIGEST_METHOD_V1 = "arrow_canonical_v1"
BLOCK_DIGEST_METHOD_V2 = "arrow_canonical_v2"


class _BlockIntegrityRecorder:
    """Accumulates one integrity-metadata entry per Parquet row-group
    ("block") as ``_write_channel_incremental`` flushes each batch — O(one
    hash object + a few scalars) per in-flight block, never O(block size)
    or O(partition size). Appends a bounded dict per block (not a per-event
    value) to ``self.blocks``."""

    def __init__(self, session_field: str):
        # first/last locator key is (session_field, raw_index). The content
        # digest remains the order-sensitive full-row integrity layer.
        self._session_field = session_field
        self.blocks: list[dict] = []

    def record(self, batch_index: int, table: "pa.Table") -> None:
        """Record one flushed row-group's integrity metadata from the
        already-constructed Arrow Table (the same Table object about to be
        passed to ``ParquetWriter.write_table`` — no separate
        re-materialization of rows)."""
        if table.num_rows == 0:
            return
        digest_hex = _canon_table_hash(table)
        session_col = table.column(self._session_field)
        raw_index_col = table.column("raw_index")
        self.blocks.append({
            "block_index": batch_index,
            "num_rows": table.num_rows,
            "first_key": [int(session_col[0].as_py()), int(raw_index_col[0].as_py())],
            "last_key": [int(session_col[-1].as_py()), int(raw_index_col[-1].as_py())],
            "sha256": digest_hex,
        })


def validate_v2_source_identity(
    source_identity: dict,
    venue: str,
    symbol: str,
    date: str,
) -> None:
    """Validate the complete, deterministic schema-v2 raw-source contract.

    Raises ``ValueError`` on the first incompatibility. Schema v2 removes the
    per-event payload hash, so a merely present dictionary is not sufficient:
    both required channels, their exact selected files, checksums, sizes, and
    contiguous contribution ranges must be well formed and scoped to this
    partition.
    """
    if not isinstance(source_identity, dict):
        raise ValueError("source_identity must be an object")

    required_top_keys = {
        "venue",
        "symbol",
        "date",
        "channels",
        "complete",
        "missing_channels",
    }
    if set(source_identity) != required_top_keys:
        raise ValueError(
            "source_identity keys must be exactly "
            f"{sorted(required_top_keys)!r}; found "
            f"{sorted(map(repr, source_identity))!r}"
        )
    for key, expected in (("venue", venue), ("symbol", symbol), ("date", date)):
        if source_identity.get(key) != expected:
            raise ValueError(
                f"source_identity.{key}={source_identity.get(key)!r} does not "
                f"match partition {key}={expected!r}"
            )
    try:
        parsed_target_date = datetime.strptime(date, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"partition date {date!r} is not canonical YYYY-MM-DD"
        ) from exc
    if parsed_target_date.strftime("%Y-%m-%d") != date:
        raise ValueError(f"partition date {date!r} is not canonical YYYY-MM-DD")

    channels = source_identity.get("channels")
    required_channels = {"depth_v2", "trade_v2"}
    if not isinstance(channels, dict) or set(channels) != required_channels:
        found = (
            sorted(map(repr, channels))
            if isinstance(channels, dict)
            else type(channels).__name__
        )
        raise ValueError(
            "source_identity.channels must contain exactly "
            f"{sorted(required_channels)!r}; found {found!r}"
        )

    if source_identity.get("complete") is not True:
        raise ValueError("source_identity.complete must be true")
    if source_identity.get("missing_channels") != []:
        raise ValueError("source_identity.missing_channels must be an empty list")

    allowed_depth_dates = {
        (parsed_target_date + timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in (-1, 0, 1)
    }
    for channel in sorted(required_channels):
        entries = channels[channel]
        if not isinstance(entries, list) or not entries:
            raise ValueError(
                f"source_identity channel {channel!r} must be a non-empty list"
            )

        paths: list[str] = []
        expected_range_start = 0
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"source_identity {channel}[{index}] must be an object"
                )
            required_entry_keys = {
                "path",
                "sha256",
                "size_bytes",
                "record_count",
                "record_range",
            }
            if channel == "depth_v2":
                required_entry_keys.add("source_date")
            if set(entry) != required_entry_keys:
                raise ValueError(
                    f"source_identity {channel}[{index}] keys must be exactly "
                    f"{sorted(required_entry_keys)!r}; found "
                    f"{sorted(map(repr, entry))!r}"
                )

            path_text = entry.get("path")
            if not isinstance(path_text, str) or not path_text:
                raise ValueError(
                    f"source_identity {channel}[{index}].path is invalid"
                )
            source_path = PurePosixPath(path_text)
            if (
                source_path.is_absolute()
                or "\\" in path_text
                or source_path.as_posix() != path_text
                or any(part in {"", ".", ".."} for part in source_path.parts)
            ):
                raise ValueError(
                    f"source_identity {channel}[{index}].path must be a "
                    f"canonical relative POSIX path: {path_text!r}"
                )
            path_parts = source_path.parts
            if (
                len(path_parts) != 5
                or path_parts[0] != venue
                or path_parts[1] != channel
                or path_parts[2] != symbol
                or not PurePosixPath(path_parts[-1]).match("*.jsonl*")
            ):
                raise ValueError(
                    f"source_identity {channel}[{index}].path is outside the "
                    f"requested partition/channel or is not a selected "
                    f"*.jsonl* input: {path_text!r}"
                )

            source_date = path_parts[3]
            if channel == "trade_v2":
                if source_date != date:
                    raise ValueError(
                        f"source_identity trade_v2[{index}] must belong to "
                        f"target date {date}, found {source_date!r}"
                    )
            else:
                if (
                    entry.get("source_date") != source_date
                    or source_date not in allowed_depth_dates
                ):
                    raise ValueError(
                        f"source_identity depth_v2[{index}] has invalid "
                        f"source_date/path date {entry.get('source_date')!r}/"
                        f"{source_date!r}"
                    )

            digest = entry.get("sha256")
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise ValueError(
                    f"source_identity {channel}[{index}].sha256 is invalid"
                )
            size_bytes = entry.get("size_bytes")
            if (
                not isinstance(size_bytes, int)
                or isinstance(size_bytes, bool)
                or size_bytes < 0
            ):
                raise ValueError(
                    f"source_identity {channel}[{index}].size_bytes is invalid"
                )
            record_count = entry.get("record_count")
            record_range = entry.get("record_range")
            if (
                not isinstance(record_count, int)
                or isinstance(record_count, bool)
                or record_count < 0
                or not isinstance(record_range, list)
                or len(record_range) != 2
                or any(
                    not isinstance(value, int) or isinstance(value, bool)
                    for value in record_range
                )
                or record_range[0] != expected_range_start
                or record_range[1] < record_range[0]
                or record_range[1] - record_range[0] != record_count
            ):
                raise ValueError(
                    f"source_identity {channel}[{index}] has an invalid or "
                    "non-contiguous record_count/record_range"
                )
            expected_range_start = record_range[1]
            paths.append(path_text)

        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ValueError(
                f"source_identity channel {channel!r} paths must be unique "
                "and deterministically sorted"
            )
        logical_variants: dict[str, list[str]] = {}
        for path_text in paths:
            source_path = PurePosixPath(path_text)
            name = source_path.name
            if name.endswith(".jsonl.zst"):
                logical_name = name[:-4]
            elif name.endswith(".jsonl.gz"):
                logical_name = name[:-3]
            elif name.endswith(".jsonl"):
                logical_name = name
            else:
                continue
            logical_key = (source_path.parent / logical_name).as_posix()
            logical_variants.setdefault(logical_key, []).append(path_text)
        conflicts = {
            logical_key: variants
            for logical_key, variants in logical_variants.items()
            if len(variants) > 1
        }
        if conflicts:
            raise ValueError(
                f"source_identity channel {channel!r} contains ambiguous "
                f"coexisting compression variants: {conflicts!r}"
            )


def resolve_source_record(source_identity: dict, channel: str, raw_index: int) -> "Optional[dict]":
    """Deterministic source-file/contribution mapping (issue #20 Phase 7
    hierarchical-integrity candidate, replacing the removed per-event
    native_payload_hash): given a partition's ``source_identity`` (as
    produced by ``pipeline.raw_manifest.compute_raw_source_identity`` with
    ``include_record_counts=True``) and a replay event's ``raw_index`` for a
    given ``channel`` (``"depth_v2"`` or ``"trade_v2"``), resolve:

      - venue/symbol/date (from ``source_identity``'s own top-level fields
        — always present since the Phase 7 review; a ``source_identity``
        computed by an older caller without those fields is rejected below
        rather than silently guessed);
      - the canonical, data_root-relative source-file path;
      - the 0-based ordinal within that file's contribution to this replay
        partition (see ``record_range``);
      - the channel itself (echoed back for a fully self-describing result).

    For a non-repartitioned channel, every parsed record contributes and this
    offset is also its parsed-record ordinal in the physical file. For the
    depth channel's D-1/D/D+1 event-time repartitioning, rejected or duplicate
    records do not contribute; the offset is therefore an ordinal among the
    file's accepted records, not necessarily its physical JSON-line ordinal.
    Resolving the exact physical depth line requires rescanning that file with
    the same filter/dedup rule. The file path and file SHA-256 remain exact.

    This function performs a bounded linear scan over the (small,
    per-partition) file list — never a per-event lookup structure — and
    returns ``None`` if ``raw_index`` is out of range, negative, or
    ``record_range`` was never computed for this ``source_identity``
    (e.g. it was built with ``include_record_counts=False``).

    Returns:
        {"venue": ..., "symbol": ..., "date": ..., "channel": ...,
         "path": "<posix-relative-file>", "contribution_ordinal": <int>}
        or ``None``.
    """
    if not isinstance(source_identity, dict):
        raise ValueError("source_identity must be a dict")
    for field in ("venue", "symbol", "date"):
        value = source_identity.get(field)
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError(
                f"source_identity has missing or invalid {field}: {value!r}"
            )
    try:
        parsed_date = datetime.strptime(source_identity["date"], "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"source_identity has invalid date: {source_identity['date']!r} "
            "(expected YYYY-MM-DD)"
        ) from exc
    if parsed_date.strftime("%Y-%m-%d") != source_identity["date"]:
        raise ValueError(
            f"source_identity has invalid date: {source_identity['date']!r} "
            "(expected YYYY-MM-DD)"
        )

    if raw_index < 0:
        return None
    entries = source_identity.get("channels", {}).get(channel, [])
    for entry in entries:
        record_range = entry.get("record_range")
        if record_range is None:
            return None
        start, end = record_range
        if start <= raw_index < end:
            return {
                "venue": source_identity.get("venue"),
                "symbol": source_identity.get("symbol"),
                "date": source_identity.get("date"),
                "channel": channel,
                "path": entry["path"],
                "contribution_ordinal": raw_index - start,
            }
    return None


def _compute_sha256(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def _fsync_regular_file(path: Path) -> None:
    """Durably flush one required publication file without following links."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"publication path is not a regular file: {path}")
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    """Durably flush directory entries used by atomic publication."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def validate_partition(partition_dir: Path) -> bool:
    """Return True only if partition_dir is a complete, checksum-valid,
    schema-version-valid replay partition. This is the ROUTINE validation
    tier (issue #20 Phase 7 review: routine vs. deep split) — called before
    atomic publication, during skip-if-valid checks, and during normal
    daily-build reconciliation. It is intentionally bounded to checks that
    are cheap at any partition size:

      - supported manifest/schema/builder contract (schema_version,
        format_version, builder_version, encoding_profile, price/qty scale);
      - expected files present (manifest.json, depth.parquet,
        trades.parquet);
      - Parquet readability and exact physical schema
        (``_schema_matches``, reads only Parquet FOOTER metadata, never row
        data);
      - manifest row counts and row-group metadata are structurally present
        (``integrity`` dict shape: source_identity/depth_blocks/
        trade_blocks keys and list types — never deserializes the blocks'
        row content);
      - COMPLETE-FILE SHA-256 checksums for depth.parquet/trades.parquet
        against the manifest's recorded ``depth_checksum``/
        ``trades_checksum`` — this alone already detects ANY corruption,
        deletion, insertion, or reordering of Parquet bytes (routine
        validation's primary defense; see the module-level security/
        integrity rationale note near ``verify_block_integrity``);
      - no staging/publication inconsistency (``status == "complete"``).

    It deliberately does NOT deserialize or JSON-serialize every logical
    row, and does NOT recompute per-block digests — that is the DEEP audit
    tier (``audit_partition_deep``), which is never invoked automatically
    by this function. This split exists because the previous all-in-one
    design (block-level re-verification inside every ``validate_partition``
    call) measured ~582s for a single VELVETUSDT-scale v2 partition (11.4M
    trade rows) even with a later vectorized digest — impractical for
    routine per-partition skip-valid checks across a multi-hundred-symbol
    universe. ``validate_partition``'s complete-file SHA-256 check alone
    remains equivalent in strength to what schema_version=0/1 partitions
    have always relied on, and to what routine callers actually need.

    This is the single source of truth for ROUTINE partition validity,
    shared by ReplayWriter.publish() (post-publication validation) and
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

    if version == 2:
        if manifest.get("format_version") != FORMAT_VERSION_V2:
            logger.warning(
                f"Partition {partition_dir} has schema_version=2 but "
                f"format_version={manifest.get('format_version')!r} "
                f"(expected {FORMAT_VERSION_V2!r})"
            )
            return False
        if manifest.get("builder_version") not in SUPPORTED_BUILDER_VERSIONS_V2:
            logger.warning(
                f"Partition {partition_dir} has schema_version=2 but "
                f"builder_version={manifest.get('builder_version')!r} "
                f"(expected one of {SUPPORTED_BUILDER_VERSIONS_V2!r})"
            )
            return False

        price_scale = manifest.get("price_scale")
        qty_scale = manifest.get("qty_scale")
        if not isinstance(price_scale, int) or isinstance(price_scale, bool) or price_scale < 0:
            logger.warning(f"Partition {partition_dir} (v2) has invalid price_scale: {price_scale!r}")
            return False
        if not isinstance(qty_scale, int) or isinstance(qty_scale, bool) or qty_scale < 0:
            logger.warning(f"Partition {partition_dir} (v2) has invalid qty_scale: {qty_scale!r}")
            return False

        encoding_profile = manifest.get("encoding_profile")
        if not isinstance(encoding_profile, dict):
            logger.warning(f"Partition {partition_dir} (v2) missing encoding_profile")
            return False
        for required_key in ("compression", "compression_level", "row_group_batch_size"):
            if required_key not in encoding_profile:
                logger.warning(
                    f"Partition {partition_dir} (v2) encoding_profile missing "
                    f"required key {required_key!r}"
                )
                return False

        # issue #20 Phase 7 hierarchical-integrity candidate: the manifest
        # must carry the traceability hierarchy that replaces the removed
        # per-event native_payload_hash column — never treated as valid
        # without it (no silent fallback to "no integrity metadata").
        integrity = manifest.get("integrity")
        if not isinstance(integrity, dict):
            logger.warning(f"Partition {partition_dir} (v2) missing integrity metadata")
            return False
        for required_key in (
            "hierarchy_version",
            "digest_method",
            "source_identity",
            "depth_blocks",
            "trade_blocks",
            "depth_checksum",
            "trades_checksum",
        ):
            if required_key not in integrity:
                logger.warning(
                    f"Partition {partition_dir} (v2) integrity metadata missing "
                    f"required key {required_key!r}"
                )
                return False
        if integrity.get("hierarchy_version") != 1:
            logger.warning(
                f"Partition {partition_dir} (v2) has unsupported integrity "
                f"hierarchy_version={integrity.get('hierarchy_version')!r} "
                "(expected 1)"
            )
            return False
        if integrity.get("digest_method") not in (
            BLOCK_DIGEST_METHOD_V1,
            BLOCK_DIGEST_METHOD_V2,
        ):
            logger.warning(
                f"Partition {partition_dir} (v2) has unsupported integrity "
                f"digest_method={integrity.get('digest_method')!r} "
                f"(expected one of "
                f"{(BLOCK_DIGEST_METHOD_V1, BLOCK_DIGEST_METHOD_V2)!r})"
            )
            return False
        source_identity = manifest.get("source_identity")
        if not isinstance(source_identity, dict):
            logger.warning(
                f"Partition {partition_dir} (v2) has invalid top-level "
                "source_identity"
            )
            return False
        expected_path_parts = (
            f"venue={manifest.get('venue')}",
            f"symbol={manifest.get('symbol')}",
            f"date={manifest.get('date')}",
        )
        actual_path_parts = (
            partition_dir.parent.parent.name,
            partition_dir.parent.name,
            partition_dir.name,
        )
        if actual_path_parts != expected_path_parts:
            logger.warning(
                f"Partition {partition_dir} (v2) manifest identity does not "
                f"match its Hive path: manifest={expected_path_parts!r}, "
                f"path={actual_path_parts!r}"
            )
            return False
        try:
            validate_v2_source_identity(
                source_identity,
                manifest.get("venue"),
                manifest.get("symbol"),
                manifest.get("date"),
            )
        except ValueError as exc:
            logger.warning(
                f"Partition {partition_dir} (v2) has invalid source_identity: "
                f"{exc}"
            )
            return False
        if integrity.get("source_identity") != source_identity:
            logger.warning(
                f"Partition {partition_dir} (v2) integrity.source_identity "
                "does not match top-level source_identity"
            )
            return False
        for checksum_key in ("depth_checksum", "trades_checksum"):
            if integrity.get(checksum_key) != manifest.get(checksum_key):
                logger.warning(
                    f"Partition {partition_dir} (v2) integrity.{checksum_key} "
                    f"does not match top-level {checksum_key}"
                )
                return False
        if (
            not isinstance(integrity.get("depth_blocks"), list)
            or not isinstance(integrity.get("trade_blocks"), list)
        ):
            logger.warning(f"Partition {partition_dir} (v2) integrity blocks must be lists")
            return False
        if not _block_metadata_shape_matches(
            depth_path,
            integrity["depth_blocks"],
            manifest.get("depth_record_count"),
        ):
            return False
        if not _block_metadata_shape_matches(
            trades_path,
            integrity["trade_blocks"],
            manifest.get("trade_record_count"),
        ):
            return False
        if not _raw_indices_resolve_to_source_identity(
            depth_path,
            source_identity["channels"]["depth_v2"],
        ):
            return False
        if not _raw_indices_resolve_to_source_identity(
            trades_path,
            source_identity["channels"]["trade_v2"],
        ):
            return False

        # Routine tier stops here: physical schema match plus the
        # structural checks above. Block-level digest re-verification is
        # the DEEP tier's responsibility (see audit_partition_deep) — never
        # performed inside this routine, always-called validity check.
        return (
            _schema_matches(depth_path, DEPTH_REPLAY_SCHEMA_V2)
            and _schema_matches(trades_path, TRADE_REPLAY_SCHEMA_V2)
        )

    # Unreachable given the SUPPORTED_SCHEMA_VERSIONS check above, but fail
    # closed rather than silently accept.
    return False


def _block_metadata_shape_matches(
    parquet_path: Path,
    blocks: list[dict],
    expected_record_count: Any,
) -> bool:
    """Validate the cheap structural block contract from Parquet metadata.

    This reads only the footer. Content digests remain an explicit deep-audit
    concern.
    """
    if (
        not isinstance(expected_record_count, int)
        or isinstance(expected_record_count, bool)
        or expected_record_count < 0
    ):
        logger.warning(
            f"{parquet_path}: invalid expected record count "
            f"{expected_record_count!r}"
        )
        return False
    parquet_file: "pq.ParquetFile | None" = None
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        nonempty_groups = [
            index
            for index in range(parquet_file.metadata.num_row_groups)
            if parquet_file.metadata.row_group(index).num_rows > 0
        ]
        empty_groups = [
            index
            for index in range(parquet_file.metadata.num_row_groups)
            if parquet_file.metadata.row_group(index).num_rows == 0
        ]
        allowed_trailing_empty = (
            not empty_groups
            or empty_groups == [parquet_file.metadata.num_row_groups - 1]
        )
        if not allowed_trailing_empty or len(nonempty_groups) != len(blocks):
            logger.warning(
                f"{parquet_path}: non-empty row-group layout does not match "
                f"integrity blocks (non-empty={nonempty_groups}, "
                f"empty={empty_groups}, blocks={len(blocks)})"
            )
            return False
    except Exception as exc:
        logger.warning(f"Could not inspect block metadata for {parquet_path}: {exc}")
        return False
    finally:
        if parquet_file is not None:
            parquet_file.close()

    total_rows = 0
    for index, block in enumerate(blocks):
        if not isinstance(block, dict):
            logger.warning(
                f"{parquet_path}: integrity block {index} is not an object"
            )
            return False
        num_rows = block.get("num_rows")
        if (
            block.get("block_index") != index
            or not isinstance(num_rows, int)
            or isinstance(num_rows, bool)
            or num_rows <= 0
        ):
            logger.warning(
                f"{parquet_path}: integrity block {index} has invalid "
                "block_index/num_rows"
            )
            return False
        for key_name in ("first_key", "last_key"):
            key = block.get(key_name)
            if (
                not isinstance(key, list)
                or len(key) != 2
                or any(
                    not isinstance(value, int) or isinstance(value, bool)
                    for value in key
                )
            ):
                logger.warning(
                    f"{parquet_path}: integrity block {index} has invalid "
                    f"{key_name}"
                )
                return False
        digest = block.get("sha256")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
        ):
            logger.warning(
                f"{parquet_path}: integrity block {index} has invalid sha256"
            )
            return False
        total_rows += num_rows

    if total_rows != expected_record_count:
        logger.warning(
            f"{parquet_path}: integrity block rows total {total_rows} does "
            f"not match manifest record count {expected_record_count}"
        )
        return False
    return True


def _raw_indices_resolve_to_source_identity(
    parquet_path: Path,
    source_entries: list[dict],
) -> bool:
    """Prove from bounded Parquet footer statistics that every replay
    ``raw_index`` falls inside the source identity's contiguous ranges.

    Source ranges describe parsed/accepted raw contributions, not replay row
    counts, so their terminal offset may legitimately exceed the replay row
    count when conversion filters non-event records. Equality is neither
    required nor expected.
    """
    terminal_offset = source_entries[-1]["record_range"][1]
    parquet_file: "pq.ParquetFile | None" = None
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        leaf_names = parquet_file.schema.names
        if "raw_index" not in leaf_names:
            logger.warning(f"{parquet_path}: missing raw_index column")
            return False
        raw_index_column = leaf_names.index("raw_index")
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            if row_group.num_rows == 0:
                continue
            statistics = row_group.column(raw_index_column).statistics
            if statistics is None or not statistics.has_min_max:
                logger.warning(
                    f"{parquet_path}: row group {row_group_index} lacks "
                    "raw_index min/max statistics; cannot prove source "
                    "resolution"
                )
                return False
            raw_min = int(statistics.min)
            raw_max = int(statistics.max)
            if raw_min < 0 or raw_max >= terminal_offset:
                logger.warning(
                    f"{parquet_path}: row group {row_group_index} raw_index "
                    f"range [{raw_min}, {raw_max}] is outside source identity "
                    f"range [0, {terminal_offset})"
                )
                return False
    except Exception as exc:
        logger.warning(
            f"Could not prove raw_index source resolution for "
            f"{parquet_path}: {exc}"
        )
        return False
    finally:
        if parquet_file is not None:
            parquet_file.close()
    return True


def audit_partition_deep(partition_dir: Path) -> "list[str]":
    """DEEP integrity audit tier (issue #20 Phase 7 review: routine vs.
    deep split). Explicitly requested — never invoked automatically by
    ``validate_partition``, skip-if-valid checks, or publication.

    In addition to everything ``validate_partition`` (routine tier) checks,
    this:
      - recomputes every logical block's digest via the vectorized
        ``_canon_table_hash`` (see its module-level docstring) and compares
        it against the manifest's recorded ``depth_blocks``/
        ``trade_blocks`` entries;
      - verifies exact block index, row count, and recorded first/last
        ``(session_seq, raw_index)`` locator keys; the order-sensitive
        content digest separately verifies row ordering;
      - localizes any block-level corruption to a specific row-group index
        and reports whether the mismatch was a row-count difference or a
        content/order difference.

    This works fully self-contained (no ``data_raw`` access). Raw-source
    verification (comparing the manifest's ``source_identity`` against live
    raw files) is a separate, explicitly raw-dependent operation, not
    performed here.

    Returns a list of human-readable problems (empty if the partition's
    routine validity AND every recorded block re-verify exactly). Only
    schema_version=2 partitions carry block-level integrity metadata to
    audit; for any other schema_version this returns a single explanatory
    problem string rather than silently reporting success.
    """
    problems: "list[str]" = []
    if not validate_partition(partition_dir):
        problems.append(
            f"{partition_dir}: validate_partition() (routine tier) reported "
            "invalid -- see logged warnings for the specific cause"
        )

    manifest_path = partition_dir / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception as e:
        problems.append(f"{partition_dir}: could not read/parse manifest.json: {e}")
        return problems

    if manifest.get("schema_version") != SCHEMA_VERSION_V2:
        problems.append(
            f"{partition_dir}: audit_partition_deep only performs "
            f"block-level re-verification for schema_version={SCHEMA_VERSION_V2} "
            f"partitions (found {manifest.get('schema_version')!r}); routine "
            "validation result (above, if any) still applies"
        )
        return problems

    integrity = manifest.get("integrity", {})
    depth_path = partition_dir / "depth.parquet"
    trades_path = partition_dir / "trades.parquet"
    digest_method = integrity.get("digest_method")
    problems.extend(
        verify_block_integrity(
            depth_path,
            integrity.get("depth_blocks", []),
            digest_method=digest_method,
        )
    )
    problems.extend(
        verify_block_integrity(
            trades_path,
            integrity.get("trade_blocks", []),
            digest_method=digest_method,
        )
    )
    return problems


def _schema_matches(parquet_path: Path, expected_schema: pa.Schema) -> bool:
    """Return True only if parquet_path's on-disk Arrow schema has exactly
    the same fields, in the same order, with identical types and
    nullability as expected_schema. Schema-level metadata is ignored
    because it is not part of the replay row contract."""
    parquet_file: "pq.ParquetFile | None" = None
    try:
        parquet_file = pq.ParquetFile(parquet_path)
        actual = parquet_file.schema_arrow
    except Exception as e:
        logger.warning(f"Could not read schema of {parquet_path}: {e}")
        return False
    finally:
        if parquet_file is not None:
            parquet_file.close()
    return actual.equals(expected_schema, check_metadata=False)


def verify_block_integrity(
    parquet_path: Path,
    blocks: "list[dict]",
    *,
    digest_method: str = BLOCK_DIGEST_METHOD_V2,
) -> "list[str]":
    """DEEP-tier helper: re-read ``parquet_path`` one Parquet row-group at a
    time (bounded memory: never more than one row-group's Table in flight,
    matching how ``_write_channel_incremental``/``_BlockIntegrityRecorder``
    produced this metadata originally) and verify every recorded block in
    ``blocks`` (issue #20 Phase 7 hierarchical-integrity candidate,
    schema_version=2) against the file's actual current contents, using the
    manifest-selected vectorized canonical digest (never per-row Python/JSON
    iteration — see the module-level note above ``_canon_array`` for the
    measured ~17x speedup and round-trip-stability rationale). Called only
    by the explicit deep-audit tier (``audit_partition_deep``) — never by
    routine ``validate_partition``.

    Detects, by construction (each failure mode maps to one of the checks
    below):
      - **changed replay value** — under ``arrow_canonical_v2``, the
        length-framed logical encoding includes primitive and nested
        validity, so value, null/non-null, and null/empty changes alter the
        digest. Existing ``arrow_canonical_v1`` manifests remain verifiable
        with their recorded legacy method; that older block layer does not
        claim collision-proof nested validity/string framing.
      - **missing/extra replay row** — ``num_rows`` for that block differs,
        or the file's total row-group count differs from ``len(blocks)``.
      - **reordered replay rows** — the canonical encoding is
        order-sensitive, so a reordering (even with all rows individually
        unchanged) changes the recomputed digest.
      - **damaged replay block / damaged completed Parquet file** — a
        corrupted/truncated row group either fails to read (surfaced as an
        error string) or produces a hash/row-count/key mismatch.

    Returns a list of human-readable problem descriptions — empty if every
    recorded block verifies exactly. Never raises for a data-content
    mismatch (that is the expected, reportable outcome); only a completely
    unreadable file surfaces as a single error string via a caught
    exception, since no block-level detail can be recovered from it.
    """
    digest_function = {
        BLOCK_DIGEST_METHOD_V1: _canon_table_hash_v1,
        BLOCK_DIGEST_METHOD_V2: _canon_table_hash,
    }.get(digest_method)
    if digest_function is None:
        return [
            f"{parquet_path}: unsupported block digest method "
            f"{digest_method!r}"
        ]
    if not isinstance(blocks, list):
        return [f"{parquet_path}: block metadata is not a list ({type(blocks).__name__!r}) -- malformed integrity metadata, refusing to verify"]

    problems: "list[str]" = []
    try:
        pf = pq.ParquetFile(parquet_path)
    except Exception as e:
        return [f"could not open {parquet_path}: {e}"]

    try:
        actual_row_groups = pf.metadata.num_row_groups
        nonempty_row_groups = [
            index
            for index in range(actual_row_groups)
            if pf.metadata.row_group(index).num_rows > 0
        ]
        empty_row_groups = [
            index
            for index in range(actual_row_groups)
            if pf.metadata.row_group(index).num_rows == 0
        ]
        allowed_trailing_empty = (
            not empty_row_groups
            or empty_row_groups == [actual_row_groups - 1]
        )
        if not allowed_trailing_empty or len(nonempty_row_groups) != len(blocks):
            problems.append(
                f"{parquet_path}: non-empty row-group layout mismatch "
                f"(non-empty={nonempty_row_groups}, empty={empty_row_groups}, "
                f"manifest blocks={len(blocks)}) -- a replay row-group was "
                "added, removed, split, or an empty group is not trailing"
            )

        for i, expected in enumerate(blocks):
            if not isinstance(expected, dict):
                problems.append(
                    f"{parquet_path} block {i}: block metadata is not a dict "
                    f"({type(expected).__name__!r})"
                )
                continue
            recorded_index = expected.get("block_index")
            if (
                not isinstance(recorded_index, int)
                or isinstance(recorded_index, bool)
                or recorded_index != i
            ):
                problems.append(
                    f"{parquet_path} block {i}: block_index mismatch "
                    f"(expected {i}, recorded {recorded_index!r})"
                )
            if i >= len(nonempty_row_groups):
                problems.append(f"{parquet_path} block {i}: missing row-group in file")
                continue
            row_group_index = nonempty_row_groups[i]
            try:
                rg_table = pf.read_row_group(row_group_index, use_threads=False)
            except Exception as e:
                problems.append(f"{parquet_path} block {i}: failed to read row-group: {e}")
                continue

            if "session_seq" in rg_table.schema.names:
                session_field = "session_seq"
            elif "trade_session_seq" in rg_table.schema.names:
                session_field = "trade_session_seq"
            else:
                problems.append(
                    f"{parquet_path} block {i}: no supported session field "
                    "for first_key/last_key verification"
                )
                continue

            if rg_table.num_rows == 0:
                problems.append(
                    f"{parquet_path} block {i}: empty row-group cannot match "
                    "recorded first_key/last_key"
                )
                continue

            session_col = rg_table.column(session_field)
            raw_index_col = rg_table.column("raw_index")
            actual_first_key = [
                int(session_col[0].as_py()),
                int(raw_index_col[0].as_py()),
            ]
            actual_last_key = [
                int(session_col[-1].as_py()),
                int(raw_index_col[-1].as_py()),
            ]
            if expected.get("first_key") != actual_first_key:
                problems.append(
                    f"{parquet_path} block {i}: first_key mismatch "
                    f"(expected {expected.get('first_key')!r}, "
                    f"found {actual_first_key!r})"
                )
            if expected.get("last_key") != actual_last_key:
                problems.append(
                    f"{parquet_path} block {i}: last_key mismatch "
                    f"(expected {expected.get('last_key')!r}, "
                    f"found {actual_last_key!r})"
                )

            expected_rows = expected.get("num_rows")
            if rg_table.num_rows != expected_rows:
                problems.append(
                    f"{parquet_path} block {i}: row count mismatch "
                    f"(expected {expected_rows!r}, found {rg_table.num_rows})"
                )
                continue
            actual_sha256 = digest_function(rg_table)
            if actual_sha256 != expected.get("sha256"):
                problems.append(
                    f"{parquet_path} block {i}: content checksum mismatch "
                    f"(expected {expected.get('sha256')!r}, "
                    f"computed {actual_sha256}) "
                    "-- a value changed, rows were reordered, or the block is damaged"
                )
    finally:
        pf.close()
    return problems


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
    block_recorder: "Optional[_BlockIntegrityRecorder]" = None,
) -> "tuple[int, int, int]":
    """Write all spool records to out_path in bounded batches.

    ``row_transform``, when given, is applied to each record (one at a time,
    not buffered) before it joins the current batch — used to project a
    v0-shaped spooled record dict down to the compact v1/v2 physical row
    shape without ever materializing more than one row-group's worth of
    rows.

    ``compression_level``/``use_dictionary``/``column_encoding`` default to
    the original, unchanged v0 behavior (ZSTD level 3, dictionary enabled,
    no explicit per-column encoding) — only ``finalize_staging()``'s
    schema_version=1/2 call sites override these (issue #20 Phase 7 measured
    Parquet encoding profile; see the ``V1_*`` constants above), so v0's
    physical output is completely unaffected.

    ``block_recorder``, when given (schema_version=2 only), is fed each
    flushed batch — i.e. each Parquet row-group — via
    ``_BlockIntegrityRecorder.record()``, exactly once per row-group, in
    write order. This produces the bounded, per-block (never per-event)
    integrity metadata that replaces the removed native_payload_hash column.

    Returns (record_count, ts_min_ns, ts_max_ns).
    An empty channel produces a schema-bearing empty Parquet file.
    """
    record_count = 0
    ts_min: "int | None" = None
    ts_max: "int | None" = None
    writer: "pq.ParquetWriter | None" = None
    block_index = 0

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
                if block_recorder is not None:
                    block_recorder.record(block_index, tbl)
                    block_index += 1
                del tbl, batch
                batch = []

        # Flush remainder (or write empty schema-bearing file)
        tbl = pa.Table.from_pylist(batch, schema=schema)
        if writer is None:
            writer = _new_writer()
        writer.write_table(tbl)
        if block_recorder is not None and batch:
            block_recorder.record(block_index, tbl)
            block_index += 1
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
                producing exactly today's v0 output), 1 (the issue #20
                Phase 5 compact prototype schema), or 2 (the issue #20
                Phase 7 hierarchical-integrity candidate). Any other value
                raises immediately — never silently falls back to v0.
            price_scale / qty_scale: used by schema_version 1 and 2. If not
                given, derived automatically from date-specific Binance
                exchangeInfo filters (see ``_derive_fixed_point_scales``) the
                first time a compact partition is finalized. Tests may pass
                these explicitly to avoid depending on on-disk exchangeInfo
                fixtures.
            source_identity: used by schema_version 1 and 2. The caller
                (e.g. ``pipeline.build_replay_store.build_replay_for_symbol``)
                must compute this itself via
                ``pipeline.raw_manifest.compute_raw_source_identity()`` using
                the EXACT ``data_root`` and channels it actually streamed
                from, and pass the result here — ``ReplayWriter`` never
                independently recomputes source identity against the global
                ``config.DATA_ROOT`` (issue #20 Phase 5 correction: doing so
                could silently record checksums from a different raw root
                than the one actually consumed by this build, e.g. under a
                custom ``--data-root``). If omitted for schema_version=1, the
                manifest honestly records ``source_identity`` as
                incomplete/not computed rather than guessing a root;
                schema_version=2 requires it and fails clearly if it is
                missing.
            data_root: raw data root used ONLY for automatic price/qty scale
                derivation (exchangeInfo lookup) for schema_version 1 or 2 when
                price_scale/qty_scale are not explicitly given. Defaults to
                ``config.DATA_ROOT`` for backward compatibility, but the
                canonical builder always passes its own ``data_root``
                explicitly so a custom ``--data-root`` build derives its
                scale from the exact same raw root it consumed for
                everything else — never a different, global default root.
        """
        if schema_version not in (0, 1, 2):
            raise ValueError(
                f"Unsupported schema_version={schema_version!r} for ReplayWriter "
                "(supported: 0 (legacy), 1 (issue #20 Phase 5 compact prototype), "
                "2 (issue #20 Phase 7 hierarchical-integrity candidate))"
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
        # only ever updated) for schema_version 1 or 2; kept at 0 for v0 so the
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
        partition (only meaningful for ``schema_version`` 1 or 2). The caller
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
        track_scale = self.schema_version in (1, 2)
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
        """Spool identified trade records to disk (O(batch), not O(day)).

        ``trade_id`` and ``agg_trade_id`` are the only replay identifiers
        reconstruction can publish as a Nautilus ``TradeId``. Reject a row
        which has neither before it reaches any physical replay schema. This
        protects direct ``ReplayWriter`` callers as well as the canonical raw
        normalizer and prevents a complete manifest from describing
        semantically unusable anonymous trades.
        """
        for r in records:
            identifiers = (r.get("trade_id"), r.get("agg_trade_id"))
            if not any(value is not None and str(value) != "" for value in identifiers):
                raise ValueError(
                    "Replay trade row has no supported identifier "
                    "(trade_id or agg_trade_id); refusing to publish an "
                    "anonymous trade"
                )

        spool = self._get_trade_spool()
        track_scale = self.schema_version in (1, 2)
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

        if self.schema_version in (1, 2):
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
                        f"Cannot build schema_version={self.schema_version} replay for "
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
                        f"Cannot build schema_version={self.schema_version} replay for "
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

            depth_block_recorder: "Optional[_BlockIntegrityRecorder]" = None
            trade_block_recorder: "Optional[_BlockIntegrityRecorder]" = None

            if self.schema_version == 1:
                depth_schema = DEPTH_REPLAY_SCHEMA_V1
                trade_schema = TRADE_REPLAY_SCHEMA_V1
                row_transform_depth = lambda rec: _project_depth_row_v1(rec, price_scale, qty_scale)
                row_transform_trade = lambda rec: _project_trade_row_v1(rec, price_scale, qty_scale)
            else:
                # schema_version == 2 (issue #20 Phase 7 hierarchical-
                # integrity candidate): identical fixed-point encoding, but
                # native_payload_hash is omitted from the physical row, and
                # per-block integrity metadata is captured as each row-group
                # is flushed, replacing it.
                if self._source_identity is None:
                    raise ValueError(
                        f"Cannot build schema_version=2 replay for "
                        f"{self.venue}/{self.symbol}/{self.date}: source_identity "
                        "is required for schema_version=2 (the manifest-level "
                        "traceability hierarchy that replaces the removed "
                        "per-event native_payload_hash column) but was not "
                        "supplied by the caller. Refusing to build without it "
                        "— never a silent fallback to unverifiable output."
                    )
                try:
                    validate_v2_source_identity(
                        self._source_identity,
                        self.venue,
                        self.symbol,
                        self.date,
                    )
                except ValueError as exc:
                    raise ValueError(
                        f"Cannot build schema_version=2 replay for "
                        f"{self.venue}/{self.symbol}/{self.date}: invalid "
                        f"source_identity: {exc}"
                    ) from exc
                depth_schema = DEPTH_REPLAY_SCHEMA_V2
                trade_schema = TRADE_REPLAY_SCHEMA_V2
                row_transform_depth = lambda rec: _project_depth_row_v1(rec, price_scale, qty_scale, include_hash=False)
                row_transform_trade = lambda rec: _project_trade_row_v1(rec, price_scale, qty_scale, include_hash=False)
                depth_block_recorder = _BlockIntegrityRecorder("session_seq")
                trade_block_recorder = _BlockIntegrityRecorder("trade_session_seq")

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
        elif self.schema_version == 2:
            depth_count, ts_d_min, ts_d_max = _write_channel_incremental(
                depth_spool, depth_path, depth_schema, V1_DEPTH_PARQUET_BATCH, row_transform_depth,
                compression_level=V1_COMPRESSION_LEVEL,
                use_dictionary=V1_USE_DICTIONARY,
                column_encoding=V1_DEPTH_COLUMN_ENCODING,
                block_recorder=depth_block_recorder,
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
        elif self.schema_version == 2:
            trade_count, ts_t_min, ts_t_max = _write_channel_incremental(
                trade_spool, trades_path, trade_schema, V1_TRADE_PARQUET_BATCH, row_transform_trade,
                compression_level=V1_COMPRESSION_LEVEL,
                use_dictionary=V1_USE_DICTIONARY,
                column_encoding=V1_TRADE_COLUMN_ENCODING,
                block_recorder=trade_block_recorder,
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
            # gains these keys. This branch writes the v1 values; the v2
            # branch below writes its own explicit version identity.
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

        elif self.schema_version == 2:
            # Explicit version identity, mirroring v1's contract exactly —
            # issue #20 Phase 7 hierarchical-integrity candidate.
            manifest["format_version"] = FORMAT_VERSION_V2
            manifest["schema_version"] = SCHEMA_VERSION_V2
            manifest["builder_version"] = BUILDER_VERSION_V2
            manifest["encoding_profile"] = {
                "compression": "zstd",
                "compression_level": V1_COMPRESSION_LEVEL,
                "row_group_batch_size": V1_DEPTH_PARQUET_BATCH,
                "depth_row_group_batch_size": V1_DEPTH_PARQUET_BATCH,
                "trade_row_group_batch_size": V1_TRADE_PARQUET_BATCH,
                "use_dictionary": V1_USE_DICTIONARY,
                "depth_column_encoding": V1_DEPTH_COLUMN_ENCODING,
                "trade_column_encoding": V1_TRADE_COLUMN_ENCODING,
                "depth_schema_version": SCHEMA_VERSION_V2,
                "trade_schema_version": SCHEMA_VERSION_V2,
                "price_scale_declared": declared_price_scale,
                "price_scale_observed": self._observed_price_scale,
                "qty_scale_declared": declared_qty_scale,
                "qty_scale_observed": self._observed_qty_scale,
            }
            manifest["price_scale"] = self._price_scale
            manifest["qty_scale"] = self._qty_scale
            # source_identity is mandatory for schema_version=2 (already
            # enforced above, before any Parquet was written) — never a
            # placeholder here.
            manifest["source_identity"] = self._source_identity
            # issue #20 Phase 7 hierarchical-integrity candidate: the
            # traceability hierarchy that replaces the removed per-event
            # native_payload_hash column (see stores/replay_schema.py's
            # module docstring near FORMAT_VERSION_V2 for the full design).
            manifest["integrity"] = {
                "hierarchy_version": 1,
                "digest_method": BLOCK_DIGEST_METHOD_V2,
                "source_identity": self._source_identity,
                "depth_blocks": depth_block_recorder.blocks if depth_block_recorder else [],
                "trade_blocks": trade_block_recorder.blocks if trade_block_recorder else [],
                "depth_checksum": depth_checksum,
                "trades_checksum": trades_checksum,
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
                f.flush()
            logger.info(f"Wrote instrument metadata: {instrument_path}")

        manifest = self._manifest
        if manifest is None:
            manifest = self.finalize_staging()

        # ParquetWriter.close() completes the files but does not make their
        # contents durable. Flush both closed data files before persisting a
        # manifest which claims the partition is complete. Instrument metadata
        # is part of the published set when present and receives the same
        # durability treatment.
        _fsync_regular_file(self.staging_dir / "depth.parquet")
        _fsync_regular_file(self.staging_dir / "trades.parquet")
        instrument_path = self.staging_dir / "instrument.json"
        if instrument_path.exists() or instrument_path.is_symlink():
            _fsync_regular_file(instrument_path)

        manifest_path = self.staging_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
            f.flush()
        _fsync_regular_file(manifest_path)
        logger.info(f"Wrote manifest: {manifest_path}")

        _fsync_directory(self.staging_dir)

        # Atomic publication with backup/restore so the existing valid partition
        # is never lost if the replacement fails (I/O error, permissions, etc.).
        backup_dir = self.output_dir.parent / f".backup_{self.date}_{self.symbol}"
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)

        new_installed = False
        try:
            # Step 1: rename existing output to backup (if it exists), then
            # durably record that parent-directory transition.
            if self.output_dir.exists():
                if backup_dir.exists() or backup_dir.is_symlink():
                    raise RuntimeError(
                        f"Ambiguous publication state: backup already exists at "
                        f"{backup_dir}. Run build-wide reconciliation while holding "
                        "the replay lifecycle lock; refusing to delete it."
                    )
                os.replace(self.output_dir, backup_dir)
                _fsync_directory(self.output_dir.parent)

            # Step 2: rename staging to canonical output and fsync its parent.
            os.replace(self.staging_dir, self.output_dir)
            new_installed = True
            _fsync_directory(self.output_dir.parent)
        except Exception:
            # Durability preparation/publication failed. Move a newly installed
            # candidate back to staging before restoring the prior canonical.
            if new_installed and self.output_dir.exists() and not self.staging_dir.exists():
                try:
                    os.replace(self.output_dir, self.staging_dir)
                    _fsync_directory(self.output_dir.parent)
                except Exception as unpublish_err:
                    logger.error(
                        f"Could not withdraw unpublished candidate {self.output_dir}: "
                        f"{unpublish_err}"
                    )
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    _fsync_directory(self.output_dir.parent)
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
            suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            quarantine_dir = self.output_dir.parent / (
                f".quarantine_{self.date}_{self.symbol}_invalid_publish.{suffix}.{os.getpid()}"
            )
            try:
                if quarantine_dir.exists():
                    raise RuntimeError(
                        f"quarantine destination already exists: {quarantine_dir}"
                    )
                if self.output_dir.exists():
                    os.replace(self.output_dir, quarantine_dir)
                    _fsync_directory(self.output_dir.parent)
            except Exception as qe:
                logger.error(
                    f"Could not quarantine invalid output {self.output_dir}: {qe}"
                )
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    _fsync_directory(self.output_dir.parent)
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
                _fsync_directory(self.output_dir.parent)
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
