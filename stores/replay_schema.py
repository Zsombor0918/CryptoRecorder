"""
stores.replay_schema — Parquet schema definitions for replay_store.

Defines deterministic normalized replay layer schemas for depth and trade events.
Uses Parquet nested structures for bids/asks lists.

Versioning (issue #20 Phase 5 — revised-plan phase numbering, see
docs/CHANGE_AUDIT.md): a manifest with no ``schema_version`` field is legacy
v0 (the original schemas below, unchanged, still fully supported). A manifest
carrying an explicit ``schema_version`` is dispatched to the matching
versioned schema; unsupported explicit versions must fail clearly rather than
being silently reinterpreted. v1 (``DEPTH_REPLAY_SCHEMA_V1`` /
``TRADE_REPLAY_SCHEMA_V1``) is the first compact prototype schema, built only
from compaction levers the checked-in Phase 3 field/consumer/integrity matrix
(``docs/IMPLEMENTATION_AUDIT.md``) explicitly approves:

  - ``venue``/``symbol``/``date`` (matrix: partition-constant "Yes") move to
    partition/manifest metadata instead of being repeated on every row.
  - ``record_type`` (depth: snapshot_seed/depth_update/sync_state; trade:
    trade/agg_trade) is stored as a small int8 enum code instead of a
    string.
  - The 5 depth boolean columns (``is_snapshot_seed``, ``is_depth_update``,
    ``is_sync_state``, ``is_desync``, ``is_resync``) are packed into a single
    int8 bitmask (matrix: "packed flags byte/enum" — proof of lossless
    round-trip is provided by tests/test_replay_schema_v1.py).
  - ``price``/``size``/``quantity`` float64 + lexical ``*_str`` columns are
    replaced by an exact fixed-point integer mantissa (int64) whose scale is
    derived from date-specific Binance ``PRICE_FILTER.tickSize`` /
    ``LOT_SIZE.stepSize`` (spot and futures independently) and recorded once
    per partition in the manifest — never a binary float intermediate.
  - ``native_payload_hash`` is stored as 32 raw bytes instead of a 64-character
    hex string (the Phase 2 Section 3 traceability design remains
    unimplemented/unresolved, so the hash itself is *retained*, only its
    physical encoding is compacted — see docs/IMPLEMENTATION_AUDIT.md).

Deliberately NOT compacted in v1 (deferred, per the matrix's own "pending
proof"/"benchmark-needed" caveats — not because they were overlooked):
``U``/``u``/``pu`` continuity ids, ``trade_id``/``agg_trade_id``,
``market_type``, and ``quality_flags`` all remain in their v0 lexical/JSON
representations.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

import pyarrow as pa

# ============================================================================
# Version constants
# ============================================================================

FORMAT_VERSION_V1 = 1
SCHEMA_VERSION_V1 = 1
BUILDER_VERSION_V1 = "cryptorecorder-replay-writer-v1.0.0"

# The only schema_version values ReplayReader/ReplayWriter know how to
# produce/consume. A manifest with schema_version outside this set (or an
# unrecognized value) must fail with a clear "found vs supported" error, not
# be silently misread.
SUPPORTED_SCHEMA_VERSIONS = (0, 1)

# ============================================================================
# v1 enum code maps (record_type / aggressor_side)
# ============================================================================

DEPTH_RECORD_TYPE_CODES = {"snapshot_seed": 0, "depth_update": 1, "sync_state": 2, "stream_lifecycle": 3}
DEPTH_RECORD_TYPE_CODES_REV = {v: k for k, v in DEPTH_RECORD_TYPE_CODES.items()}

TRADE_RECORD_TYPE_CODES = {"trade": 0, "agg_trade": 1}
TRADE_RECORD_TYPE_CODES_REV = {v: k for k, v in TRADE_RECORD_TYPE_CODES.items()}

AGGRESSOR_SIDE_CODES = {"BUY": 0, "SELL": 1}
AGGRESSOR_SIDE_CODES_REV = {v: k for k, v in AGGRESSOR_SIDE_CODES.items()}


def encode_aggressor_side(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    return AGGRESSOR_SIDE_CODES[value]


def decode_aggressor_side(code: Optional[int]) -> Optional[str]:
    if code is None:
        return None
    return AGGRESSOR_SIDE_CODES_REV[int(code)]


# ============================================================================
# v1 depth flag bitmask (packs the 5 v0 boolean columns into one int8)
# ============================================================================

_FLAG_IS_SNAPSHOT_SEED = 1 << 0
_FLAG_IS_DEPTH_UPDATE = 1 << 1
_FLAG_IS_SYNC_STATE = 1 << 2
_FLAG_IS_DESYNC = 1 << 3
_FLAG_IS_RESYNC = 1 << 4


def pack_depth_flags(
    is_snapshot_seed: bool,
    is_depth_update: bool,
    is_sync_state: bool,
    is_desync: bool,
    is_resync: bool,
) -> int:
    code = 0
    if is_snapshot_seed:
        code |= _FLAG_IS_SNAPSHOT_SEED
    if is_depth_update:
        code |= _FLAG_IS_DEPTH_UPDATE
    if is_sync_state:
        code |= _FLAG_IS_SYNC_STATE
    if is_desync:
        code |= _FLAG_IS_DESYNC
    if is_resync:
        code |= _FLAG_IS_RESYNC
    return code


def unpack_depth_flags(code: int) -> "tuple[bool, bool, bool, bool, bool]":
    code = int(code)
    return (
        bool(code & _FLAG_IS_SNAPSHOT_SEED),
        bool(code & _FLAG_IS_DEPTH_UPDATE),
        bool(code & _FLAG_IS_SYNC_STATE),
        bool(code & _FLAG_IS_DESYNC),
        bool(code & _FLAG_IS_RESYNC),
    )


# ============================================================================
# v1 exact fixed-point mantissa encode/decode (Decimal only, never float)
# ============================================================================

def encode_fixed_point(value_str: str, scale: int) -> int:
    """Convert an exact decimal string to an integer mantissa at ``scale``.

    Raises ``ValueError`` if ``value_str`` carries more fractional precision
    than ``scale`` allows (i.e. the value cannot be represented exactly at
    this scale) — this must never silently truncate. Never uses a binary
    float intermediate.
    """
    d = Decimal(value_str)
    scaled = d.scaleb(scale)
    if scaled != scaled.to_integral_value():
        raise ValueError(
            f"value {value_str!r} cannot be represented exactly at scale "
            f"{scale} (would lose precision)"
        )
    return int(scaled)


def decode_fixed_point(mantissa: int, scale: int) -> str:
    """Reconstruct the exact decimal string for ``mantissa`` at ``scale``.

    Always formatted with exactly ``scale`` fractional digits (matching the
    instrument's required precision), using Decimal arithmetic only.
    """
    d = Decimal(int(mantissa)).scaleb(-int(scale))
    if scale <= 0:
        return str(int(d))
    return f"{d:.{scale}f}"


# ============================================================================
# Depth Replay Schema
# ============================================================================

# Nested struct for bid/ask level. Floats are kept for feature convenience;
# string fields preserve the exact source value for catalog reconstruction.
_bid_ask_struct = pa.struct([
    pa.field("price", pa.float64(), nullable=False),
    pa.field("size", pa.float64(), nullable=False),
    pa.field("price_str", pa.string(), nullable=False),
    pa.field("size_str", pa.string(), nullable=False),
])

DEPTH_REPLAY_SCHEMA = pa.schema([
    # Core identifiers
    pa.field("venue", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("date", pa.string(), nullable=False),
    
    # Session & ordering
    pa.field("stream_session_id", pa.uint64(), nullable=False),
    pa.field("session_seq", pa.uint64(), nullable=False),
    pa.field("raw_index", pa.uint32(), nullable=False),
    
    # Record metadata
    pa.field("record_type", pa.string(), nullable=False),  # 'snapshot_seed', 'depth_update'
    pa.field("U", pa.string(), nullable=True),  # First update ID when available
    pa.field("u", pa.string(), nullable=True),  # Update ID (spot)
    pa.field("pu", pa.string(), nullable=True),  # Previous Update ID (futures)
    
    # Timestamps (nanoseconds)
    pa.field("ts_exchange_ns", pa.int64(), nullable=False),  # Exchange timestamp
    pa.field("ts_receive_ns", pa.int64(), nullable=False),  # Receive timestamp (or ts_init_ns)
    
    # Order book levels (nested list of structs)
    pa.field("bids", pa.list_(_bid_ask_struct), nullable=False),
    pa.field("asks", pa.list_(_bid_ask_struct), nullable=False),
    
    # Quality & diagnostics
    pa.field("is_snapshot_seed", pa.bool_(), nullable=False),
    pa.field("is_depth_update", pa.bool_(), nullable=False),
    pa.field("is_sync_state", pa.bool_(), nullable=False),
    pa.field("is_desync", pa.bool_(), nullable=False),
    pa.field("is_resync", pa.bool_(), nullable=False),
    pa.field("quality_flags", pa.string(), nullable=True),  # JSON-encoded diagnostic flags
    pa.field("native_payload_hash", pa.string(), nullable=True),  # SHA256 hex
])


# ============================================================================
# Depth Replay Schema — v1 (compact prototype, issue #20 Phase 5)
# ============================================================================

# venue/symbol/date removed (partition-constant, moved to manifest).
# record_type -> record_type_code (int8 enum). 5 bool columns -> flags (int8
# bitmask). price/size float64+str -> price_mantissa/size_mantissa (int64,
# scale recorded once per partition in the manifest). native_payload_hash
# hex string -> 32 raw bytes.
_bid_ask_struct_v1 = pa.struct([
    pa.field("price_mantissa", pa.int64(), nullable=False),
    pa.field("size_mantissa", pa.int64(), nullable=False),
])

DEPTH_REPLAY_SCHEMA_V1 = pa.schema([
    pa.field("stream_session_id", pa.uint64(), nullable=False),
    pa.field("session_seq", pa.uint64(), nullable=False),
    pa.field("raw_index", pa.uint32(), nullable=False),

    pa.field("record_type_code", pa.int8(), nullable=False),
    pa.field("U", pa.string(), nullable=True),
    pa.field("u", pa.string(), nullable=True),
    pa.field("pu", pa.string(), nullable=True),

    pa.field("ts_exchange_ns", pa.int64(), nullable=False),
    pa.field("ts_receive_ns", pa.int64(), nullable=False),

    pa.field("bids", pa.list_(_bid_ask_struct_v1), nullable=False),
    pa.field("asks", pa.list_(_bid_ask_struct_v1), nullable=False),

    pa.field("flags", pa.int8(), nullable=False),
    pa.field("quality_flags", pa.string(), nullable=True),
    pa.field("native_payload_hash", pa.binary(32), nullable=True),
])


# ============================================================================
# Trade Replay Schema
# ============================================================================

TRADE_REPLAY_SCHEMA = pa.schema([
    # Core identifiers
    pa.field("venue", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("date", pa.string(), nullable=False),
    
    # Session & ordering
    pa.field("trade_stream_session_id", pa.uint64(), nullable=False),
    pa.field("trade_session_seq", pa.uint64(), nullable=False),
    pa.field("raw_index", pa.uint32(), nullable=False),
    
    # Record metadata
    pa.field("record_type", pa.string(), nullable=False),  # 'trade', 'agg_trade'
    pa.field("market_type", pa.string(), nullable=False),  # 'spot' or 'futures'
    
    # Trade identifiers
    pa.field("trade_id", pa.string(), nullable=True),  # Spot trade_id
    pa.field("agg_trade_id", pa.string(), nullable=True),  # Futures agg_trade_id (use if trade_id null)
    
    # Timestamps (nanoseconds)
    pa.field("ts_exchange_ns", pa.int64(), nullable=False),  # Exchange timestamp
    pa.field("ts_receive_ns", pa.int64(), nullable=False),  # Receive timestamp (or ts_init_ns)
    
    # Trade details
    pa.field("price", pa.float64(), nullable=False),
    pa.field("quantity", pa.float64(), nullable=False),
    pa.field("price_str", pa.string(), nullable=False),
    pa.field("quantity_str", pa.string(), nullable=False),
    pa.field("buyer_maker", pa.bool_(), nullable=False),  # True if buyer is maker (taker is seller)
    pa.field("aggressor_side", pa.string(), nullable=True),  # 'BUY', 'SELL', or null if not reliably derivable
    
    # Quality & diagnostics
    pa.field("quality_flags", pa.string(), nullable=True),  # JSON-encoded diagnostic flags
    pa.field("native_payload_hash", pa.string(), nullable=True),  # SHA256 hex
])


# ============================================================================
# Trade Replay Schema — v1 (compact prototype, issue #20 Phase 5)
# ============================================================================

# venue/symbol/date removed (partition-constant, moved to manifest).
# market_type is kept as-is in v1: the matrix marks its partition-constancy
# as "pending an invariant proof" (not yet done), so it is deliberately NOT
# compacted in this prototype. trade_id/agg_trade_id similarly remain
# lexical strings (matrix: "benchmark-needed" before any numeric packing).
TRADE_REPLAY_SCHEMA_V1 = pa.schema([
    pa.field("trade_stream_session_id", pa.uint64(), nullable=False),
    pa.field("trade_session_seq", pa.uint64(), nullable=False),
    pa.field("raw_index", pa.uint32(), nullable=False),

    pa.field("record_type_code", pa.int8(), nullable=False),
    pa.field("market_type", pa.string(), nullable=False),

    pa.field("trade_id", pa.string(), nullable=True),
    pa.field("agg_trade_id", pa.string(), nullable=True),

    pa.field("ts_exchange_ns", pa.int64(), nullable=False),
    pa.field("ts_receive_ns", pa.int64(), nullable=False),

    pa.field("price_mantissa", pa.int64(), nullable=False),
    pa.field("quantity_mantissa", pa.int64(), nullable=False),
    pa.field("buyer_maker", pa.bool_(), nullable=False),
    pa.field("aggressor_side_code", pa.int8(), nullable=True),

    pa.field("quality_flags", pa.string(), nullable=True),
    pa.field("native_payload_hash", pa.binary(32), nullable=True),
])


# ============================================================================
# Instrument Metadata Schema
# ============================================================================

INSTRUMENT_SCHEMA = {
    """
    Per-symbol instrument metadata stored as instrument.json.
    Contains enough info for Nautilus catalog generation.
    """
    "venue": str,  # e.g., "BINANCE_SPOT"
    "symbol": str,  # e.g., "BTCUSDT"
    "market_type": str,  # "spot" or "perpetual"
    "instrument_id": str,  # Nautilus instrument_id if known
    "raw_symbol": str,  # Raw Binance symbol
    "price_precision": int,  # Decimal places for price
    "size_precision": int,  # Decimal places for quantity
    "quote_asset": str,  # e.g., "USDT"
    "base_asset": str,  # e.g., "BTC"
}


# ============================================================================
# Manifest Schema
# ============================================================================

MANIFEST_SCHEMA = {
    """
    Per-date manifest stored as manifest.json in each symbol partition.
    Aggregates statistics for validation and auditing.
    """
    "venue": str,
    "symbol": str,
    "date": str,  # YYYY-MM-DD
    "status": str,  # "complete", "partial", "failed"
    "depth_record_count": int,
    "trade_record_count": int,
    "ts_range_start_ns": int,  # Earliest timestamp
    "ts_range_end_ns": int,  # Latest timestamp
    "depth_checksum": str,  # SHA256 of depth.parquet
    "trades_checksum": str,  # SHA256 of trades.parquet
    "created_at_utc": str,  # ISO 8601
    "errors": list,  # Error messages, if any
}
