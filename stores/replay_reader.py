"""
stores.replay_reader — Streaming reader for replay_store Parquet data.

Handles iteration over replay partitions with memory-efficient streaming.

Versioning (issue #20 Phase 5): ``iter_depths``/``iter_trades`` dispatch on
each partition's manifest ``schema_version``. A historical manifest without
that field is accepted as legacy v0 only after both physical Parquet schemas
exactly match the recognized v0 contracts, including the legacy exact-string
fields. Compact fixed-point layouts always require an explicit supported
version. Every row
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
    SUPPORTED_BUILDER_VERSIONS_V2,
    DEPTH_RECORD_TYPE_CODES_REV,
    DEPTH_REPLAY_SCHEMA,
    DEPTH_REPLAY_SCHEMA_V1,
    DEPTH_REPLAY_SCHEMA_V2,
    FORMAT_VERSION_V1,
    FORMAT_VERSION_V2,
    SCHEMA_VERSION_V1,
    SCHEMA_VERSION_V2,
    SUPPORTED_SCHEMA_VERSIONS,
    TRADE_REPLAY_SCHEMA,
    TRADE_REPLAY_SCHEMA_V1,
    TRADE_REPLAY_SCHEMA_V2,
    TRADE_RECORD_TYPE_CODES_REV,
    decode_aggressor_side,
    decode_fixed_point,
    unpack_depth_flags,
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

    def _partition_paths(
        self, venue: str, symbol: str, date: str
    ) -> tuple[Path, Path, Path, Path]:
        partition_dir = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
        )
        return (
            partition_dir,
            partition_dir / "manifest.json",
            partition_dir / "depth.parquet",
            partition_dir / "trades.parquet",
        )

    @staticmethod
    def _read_manifest_required(manifest_path: Path) -> dict:
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"Replay manifest is required for schema dispatch but is "
                f"missing: {manifest_path}"
            )
        try:
            with open(manifest_path) as manifest_file:
                manifest = json.load(manifest_file)
        except Exception as exc:
            raise ValueError(
                f"Replay manifest is unreadable or invalid JSON: "
                f"{manifest_path}: {exc}"
            ) from exc
        if not isinstance(manifest, dict):
            raise ValueError(
                f"Replay manifest must contain a JSON object for schema "
                f"dispatch: {manifest_path}"
            )
        return manifest

    @staticmethod
    def _physical_schema(path: Path):
        if not path.is_file():
            raise FileNotFoundError(
                f"Replay schema dispatch requires both channel files; missing: {path}"
            )
        try:
            return pq.read_schema(path)
        except Exception as exc:
            raise ValueError(
                f"Replay Parquet schema is unreadable for schema dispatch: "
                f"{path}: {exc}"
            ) from exc

    def _partition_contract(
        self, venue: str, symbol: str, date: str
    ) -> tuple[int, dict]:
        """Return a decoder only after manifest and physical schemas agree."""
        _, manifest_path, depth_path, trades_path = self._partition_paths(
            venue, symbol, date
        )
        manifest = self._read_manifest_required(manifest_path)
        historical_v0 = "schema_version" not in manifest
        version = 0 if historical_v0 else manifest["schema_version"]
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version not in SUPPORTED_SCHEMA_VERSIONS
        ):
            raise ValueError(
                f"Unsupported replay schema_version={version!r} for "
                f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
            )

        expected_schemas = {
            0: (DEPTH_REPLAY_SCHEMA, TRADE_REPLAY_SCHEMA),
            SCHEMA_VERSION_V1: (DEPTH_REPLAY_SCHEMA_V1, TRADE_REPLAY_SCHEMA_V1),
            SCHEMA_VERSION_V2: (DEPTH_REPLAY_SCHEMA_V2, TRADE_REPLAY_SCHEMA_V2),
        }
        expected_depth, expected_trades = expected_schemas[int(version)]
        actual_depth = self._physical_schema(depth_path)
        actual_trades = self._physical_schema(trades_path)
        qualifier = (
            "historical manifest without schema_version"
            if historical_v0
            else f"manifest-declared schema_version={version}"
        )
        if not actual_depth.equals(expected_depth, check_metadata=False):
            raise ValueError(
                f"Replay depth physical schema contradicts {qualifier} for "
                f"{venue}/{symbol}/{date}; compact layouts require an explicit "
                "supported schema_version and are never decoded as legacy v0"
            )
        if not actual_trades.equals(expected_trades, check_metadata=False):
            raise ValueError(
                f"Replay trade physical schema contradicts {qualifier} for "
                f"{venue}/{symbol}/{date}; compact layouts require an explicit "
                "supported schema_version and are never decoded as legacy v0"
            )

        if version in (SCHEMA_VERSION_V1, SCHEMA_VERSION_V2):
            expected_format = (
                FORMAT_VERSION_V1
                if version == SCHEMA_VERSION_V1
                else FORMAT_VERSION_V2
            )
            if manifest.get("format_version") != expected_format:
                raise ValueError(
                    f"Replay manifest format_version={manifest.get('format_version')!r} "
                    f"contradicts schema_version={version}; expected {expected_format}"
                )
            for field in ("price_scale", "qty_scale"):
                value = manifest.get(field)
                if (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or value < 0
                ):
                    raise ValueError(
                        f"Replay manifest {field}={value!r} is invalid for "
                        f"schema_version={version}"
                    )
            if version == SCHEMA_VERSION_V1:
                builder = manifest.get("builder_version")
                if not isinstance(builder, str) or not builder:
                    raise ValueError(
                        "Replay manifest builder_version is required for schema_version=1"
                    )
            elif manifest.get("builder_version") not in SUPPORTED_BUILDER_VERSIONS_V2:
                raise ValueError(
                    f"Replay manifest builder_version={manifest.get('builder_version')!r} "
                    "contradicts schema_version=2; expected one of "
                    f"{SUPPORTED_BUILDER_VERSIONS_V2!r}"
                )
        return int(version), manifest

    def get_schema_version(self, venue: str, symbol: str, date: str) -> int:
        """Return the manifest/physical-schema-confirmed decoder version.

        A missing ``schema_version`` is accepted only for an exact historical
        v0 layout. Missing/malformed manifests, unsupported versions, and any
        declared-version/physical-schema contradiction fail clearly.
        """
        version, _ = self._partition_contract(venue, symbol, date)
        return version

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
        partition_dir = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
        )
        depth_path = partition_dir / "depth.parquet"
        if not depth_path.exists():
            if partition_dir.exists():
                raise FileNotFoundError(
                    f"Replay partition exists but required depth channel is "
                    f"missing: {depth_path}"
                )
            return

        version, manifest = self._partition_contract(venue, symbol, date)
        parquet = None
        try:
            parquet = pq.ParquetFile(depth_path)
            if version == 0:
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for record in batch.to_pylist():
                        if record is not None:
                            yield record
            elif version == 1:
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_depth_row_v1(row, price_scale, qty_scale, venue, symbol, date)
            elif version == 2:
                # V2 physically removes native_payload_hash. Preserve that
                # contract in the logical row by omitting the key rather than
                # fabricating a compatibility None value.
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_depth_row_v1(row, price_scale, qty_scale, venue, symbol, date, include_hash=False)
            else:
                raise ValueError(
                    f"Unsupported replay schema_version={version!r} for "
                    f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
                )
        except Exception as e:
            logger.error(f"Error reading {depth_path}: {e}")
            raise
        finally:
            if parquet is not None:
                parquet.close()

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
        partition_dir = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
        )
        trades_path = partition_dir / "trades.parquet"
        if not trades_path.exists():
            if partition_dir.exists():
                raise FileNotFoundError(
                    f"Replay partition exists but required trade channel is "
                    f"missing: {trades_path}"
                )
            return

        version, manifest = self._partition_contract(venue, symbol, date)
        parquet = None
        try:
            parquet = pq.ParquetFile(trades_path)
            if version == 0:
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for record in batch.to_pylist():
                        if record is not None:
                            yield record
            elif version == 1:
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_trade_row_v1(row, price_scale, qty_scale, venue, symbol, date)
            elif version == 2:
                # issue #20 Phase 7 hierarchical-integrity candidate -- see
                # the matching comment in iter_depths above: the key is
                # genuinely omitted, never a fake None.
                price_scale = manifest["price_scale"]
                qty_scale = manifest["qty_scale"]
                for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                    for row in batch.to_pylist():
                        if row is not None:
                            yield _decode_trade_row_v1(row, price_scale, qty_scale, venue, symbol, date, include_hash=False)
            else:
                raise ValueError(
                    f"Unsupported replay schema_version={version!r} for "
                    f"{venue}/{symbol}/{date} (supported: {SUPPORTED_SCHEMA_VERSIONS})"
                )
        except Exception as e:
            logger.error(f"Error reading {trades_path}: {e}")
            raise
        finally:
            if parquet is not None:
                parquet.close()

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


def _decode_depth_row_v1(row: dict, price_scale: int, qty_scale: int, venue: str, symbol: str, date: str, *, include_hash: bool = True) -> dict:
    """Decode one v1/v2 physical depth row back to its logical row shape
    (including venue/symbol/date, which v0 rows carry per-row and v1/v2
    physically omit, restoring them from the partition identity passed by
    the caller — never physically re-added to v1/v2 storage). Independent
    of converter/depth_phase2.py and convert_day.py — uses only this
    module's own fixed-point/flag decode helpers.

    ``include_hash`` defaults to True for v1. When False for v2, the returned
    dict omits ``native_payload_hash`` rather than inventing a value for a
    field which does not exist in that schema.
    """
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
    result = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
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
    }
    if include_hash:
        result["native_payload_hash"] = hash_bytes.hex() if hash_bytes is not None else None
    return result


def _decode_trade_row_v1(row: dict, price_scale: int, qty_scale: int, venue: str, symbol: str, date: str, *, include_hash: bool = True) -> dict:
    """Decode one v1/v2 physical trade row back to its logical row shape
    (including venue/symbol/date, restored from the partition identity
    passed by the caller). Independent of converter/depth_phase2.py and
    convert_day.py — uses only this module's own fixed-point/enum decode
    helpers.

    ``include_hash`` — see ``_decode_depth_row_v1``'s docstring.
    """
    record_type = TRADE_RECORD_TYPE_CODES_REV[int(row["record_type_code"])]
    price_str = decode_fixed_point(row["price_mantissa"], price_scale)
    quantity_str = decode_fixed_point(row["quantity_mantissa"], qty_scale)
    hash_bytes = row.get("native_payload_hash")
    result = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
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
    }
    if include_hash:
        result["native_payload_hash"] = hash_bytes.hex() if hash_bytes is not None else None
    return result
