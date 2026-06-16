"""
stores.replay_schema — Parquet schema definitions for replay_store.

Defines deterministic normalized replay layer schemas for depth and trade events.
Uses Parquet nested structures for bids/asks lists.
"""
from __future__ import annotations

from typing import Any

import pyarrow as pa


# ============================================================================
# Depth Replay Schema
# ============================================================================

# Nested struct for bid/ask level: [price, size]
_bid_ask_struct = pa.struct([
    pa.field("price", pa.float64(), nullable=False),
    pa.field("size", pa.float64(), nullable=False),
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
    pa.field("buyer_maker", pa.bool_(), nullable=False),  # True if buyer is maker (taker is seller)
    pa.field("aggressor_side", pa.string(), nullable=True),  # 'BUY', 'SELL', or null if not reliably derivable
    
    # Quality & diagnostics
    pa.field("quality_flags", pa.string(), nullable=True),  # JSON-encoded diagnostic flags
    pa.field("native_payload_hash", pa.string(), nullable=True),  # SHA256 hex
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
