"""
stores.replay_reader — Streaming reader for replay_store Parquet data.

Handles iteration over replay partitions with memory-efficient streaming.

Versioning (issue #20 Phase 5): ``iter_depths``/``iter_trades`` dispatch on
each partition's manifest ``schema_version`` (absent -> legacy v0). Every row
yielded by either method has the EXACT same logical key/value shape
regardless of physical schema version — v1's compact physical columns
(record_type_code, packed flags, fixed-point mantissas, binary hash) are
decoded back into the same dict shape v0 always produced (record_type
string, is_snapshot_seed/is_depth_update/is_sync_state/is_desync/is_resync
booleans, price_str/size_str exact decimal strings, native_payload_hash hex
string, venue/symbol/date restored from partition args/manifest). Downstream
consumers (``stores.replay_depth_adapter``,
``validation.validate_catalog_equivalence``) require zero changes to support
v1 partitions. This decode logic is independent of, and does not import,
``convert_day.py``/``converter.depth_phase2`` — the reference route's own
depth-conversion logic is untouched by this file.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Generator, Optional

import pyarrow.parquet as pq

from .replay_schema import (
    SUPPORTED_SCHEMA_VERSIONS,
    DEPTH_RECORD_TYPE_CODES_REV,
    TRADE_RECORD_TYPE_CODES_REV,
    unpack_depth_flags,
    decode_fixed_point,
    decode_aggressor_side,
)

logger = logging.getLogger(__name__)


class ReplayReader:
    """
    Streaming reader for replay_store data.
    
    Supports iteration over venues, symbols, dates, and events.
    """

    def __init__(self, replay_root: Path):
        """
        Initialize reader for a replay_store directory.

        Args:
            replay_root: Path to replay_store root
        """
        self.replay_root = Path(replay_root)

    def iter_venues(self) -> Generator[str, None, None]:
        """Iterate over available venue directories."""
        venue_dirs = sorted(self.replay_root.glob("venue=*"))
        for venue_dir in venue_dirs:
            if venue_dir.is_dir():
                venue = venue_dir.name.split("=")[1]
                yield venue

    def iter_symbols(self, venue: str) -> Generator[str, None, None]:
        """Iterate over symbols for a given venue."""
        venue_dir = self.replay_root / f"venue={venue}"
        if not venue_dir.exists():
            return
        symbol_dirs = sorted(venue_dir.glob("symbol=*"))
        for symbol_dir in symbol_dirs:
            if symbol_dir.is_dir():
                symbol = symbol_dir.name.split("=")[1]
                yield symbol

    def iter_dates(self, venue: str, symbol: str) -> Generator[str, None, None]:
        """Iterate over dates for a given venue/symbol."""
        symbol_dir = self.replay_root / f"venue={venue}" / f"symbol={symbol}"
        if not symbol_dir.exists():
            return
        date_dirs = sorted(symbol_dir.glob("date=*"))
        for date_dir in date_dirs:
            if date_dir.is_dir():
                date = date_dir.name.split("=")[1]
                yield date

    def get_schema_version(self, venue: str, symbol: str, date: str) -> int:
        """Return the manifest's explicit ``schema_version``, or 0 (legacy)
        if the manifest is missing the field entirely (or the partition has
        no manifest at all — treated the same as legacy for dispatch
        purposes; callers that need existence should check
        ``load_manifest()`` separately). Raises ``ValueError`` naming the
        found and supported versions if an explicit version is present but
        unsupported — never silently misread.
        """
        manifest = self.load_manifest(venue, symbol, date)
        if not manifest or "schema_version" not in manifest:
            return 0
        version = manifest["schema_version"]
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"Unsupported replay schema_version={version!r} for "
                f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
            )
        return int(version)

    def iter_depths(
        self,
        venue: str,
        symbol: str,
        date: str,
    ) -> Generator[dict, None, None]:
        """
        Stream depth records from depth.parquet for given partition.

        Args:
            venue: Venue name
            symbol: Symbol name
            date: Date string (YYYY-MM-DD)

        Yields:
            Depth record dicts (same logical shape regardless of schema
            version — see module docstring).
        """
        depth_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "depth.parquet"
        )
        if not depth_path.exists():
            return

        version = self.get_schema_version(venue, symbol, date)
        try:
            parquet = pq.ParquetFile(depth_path)
            if version == 0:
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for record in batch.to_pylist():
                        if record is not None:
                            yield record
            elif version == 1:
                manifest = self.load_manifest(venue, symbol, date) or {}
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_depth_row_v1(row, price_scale, qty_scale)
            else:
                raise ValueError(
                    f"Unsupported replay schema_version={version!r} for "
                    f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
                )
        except Exception as e:
            logger.error(f"Error reading {depth_path}: {e}")

    def iter_trades(
        self,
        venue: str,
        symbol: str,
        date: str,
    ) -> Generator[dict, None, None]:
        """
        Stream trade records from trades.parquet for given partition.

        Args:
            venue: Venue name
            symbol: Symbol name
            date: Date string (YYYY-MM-DD)

        Yields:
            Trade record dicts (same logical shape regardless of schema
            version — see module docstring).
        """
        trades_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "trades.parquet"
        )
        if not trades_path.exists():
            return

        version = self.get_schema_version(venue, symbol, date)
        try:
            parquet = pq.ParquetFile(trades_path)
            if version == 0:
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for record in batch.to_pylist():
                        if record is not None:
                            yield record
            elif version == 1:
                manifest = self.load_manifest(venue, symbol, date) or {}
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_trade_row_v1(row, price_scale, qty_scale)
            else:
                raise ValueError(
                    f"Unsupported replay schema_version={version!r} for "
                    f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
                )
        except Exception as e:
            logger.error(f"Error reading {trades_path}: {e}")

    def load_instrument_metadata(
        self, venue: str, symbol: str, date: str
    ) -> Optional[dict]:
        """Load instrument.json metadata for a partition."""
        instrument_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "instrument.json"
        )
        if not instrument_path.exists():
            return None
        try:
            with open(instrument_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {instrument_path}: {e}")
            return None

    def load_manifest(self, venue: str, symbol: str, date: str) -> Optional[dict]:
        """Load manifest.json for a partition."""
        manifest_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "manifest.json"
        )
        if not manifest_path.exists():
            return None
        try:
            with open(manifest_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {manifest_path}: {e}")
            return None


def _decode_depth_row_v1(row: dict, price_scale: int, qty_scale: int) -> dict:
    """Decode one v1 physical depth row back to the exact v0 logical row
    shape. Independent of converter/depth_phase2.py and convert_day.py —
    uses only this module's own fixed-point/flag decode helpers."""
    record_type = DEPTH_RECORD_TYPE_CODES_REV[int(row["record_type_code"])]
    (
        is_snapshot_seed,
        is_depth_update,
        is_sync_state,
        is_desync,
        is_resync,
    ) = unpack_depth_flags(row["flags"])

    def _level(lv: dict) -> dict:
        price_str = decode_fixed_point(lv["price_mantissa"], price_scale)
        size_str = decode_fixed_point(lv["size_mantissa"], qty_scale)
        return {
            "price": float(price_str),
            "size": float(size_str),
            "price_str": price_str,
            "size_str": size_str,
        }

    hash_bytes = row.get("native_payload_hash")
    return {
        "stream_session_id": row["stream_session_id"],
        "session_seq": row["session_seq"],
        "raw_index": row["raw_index"],
        "record_type": record_type,
        "U": row.get("U"),
        "u": row.get("u"),
        "pu": row.get("pu"),
        "ts_exchange_ns": row["ts_exchange_ns"],
        "ts_receive_ns": row["ts_receive_ns"],
        "bids": [_level(lv) for lv in (row.get("bids") or [])],
        "asks": [_level(lv) for lv in (row.get("asks") or [])],
        "is_snapshot_seed": is_snapshot_seed,
        "is_depth_update": is_depth_update,
        "is_sync_state": is_sync_state,
        "is_desync": is_desync,
        "is_resync": is_resync,
        "quality_flags": row.get("quality_flags"),
        "native_payload_hash": hash_bytes.hex() if hash_bytes is not None else None,
    }


def _decode_trade_row_v1(row: dict, price_scale: int, qty_scale: int) -> dict:
    """Decode one v1 physical trade row back to the exact v0 logical row
    shape. Independent of converter/depth_phase2.py and convert_day.py —
    uses only this module's own fixed-point/enum decode helpers."""
    record_type = TRADE_RECORD_TYPE_CODES_REV[int(row["record_type_code"])]
    price_str = decode_fixed_point(row["price_mantissa"], price_scale)
    quantity_str = decode_fixed_point(row["quantity_mantissa"], qty_scale)
    hash_bytes = row.get("native_payload_hash")
    return {
        "trade_stream_session_id": row["trade_stream_session_id"],
        "trade_session_seq": row["trade_session_seq"],
        "raw_index": row["raw_index"],
        "record_type": record_type,
        "market_type": row.get("market_type"),
        "trade_id": row.get("trade_id"),
        "agg_trade_id": row.get("agg_trade_id"),
        "ts_exchange_ns": row["ts_exchange_ns"],
        "ts_receive_ns": row["ts_receive_ns"],
        "price": float(price_str),
        "quantity": float(quantity_str),
        "price_str": price_str,
        "quantity_str": quantity_str,
        "buyer_maker": row.get("buyer_maker"),
        "aggressor_side": decode_aggressor_side(row.get("aggressor_side_code")),
        "quality_flags": row.get("quality_flags"),
        "native_payload_hash": hash_bytes.hex() if hash_bytes is not None else None,
    }
