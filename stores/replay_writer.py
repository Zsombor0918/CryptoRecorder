"""
stores.replay_writer — Deterministic Parquet writing for replay_store.

Handles sorting, staging, and atomic publish for replay data integrity.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .replay_schema import DEPTH_REPLAY_SCHEMA, TRADE_REPLAY_SCHEMA, MANIFEST_SCHEMA

logger = logging.getLogger(__name__)


class ReplayWriter:
    """
    Writes deterministic normalized Parquet replay data with staging/publish pattern.
    
    Output layout (Hive-style partitions):
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
    ):
        """
        Initialize writer for a single venue/symbol/date partition.

        Args:
            replay_root: Base replay_store directory
            venue: Venue name (e.g., 'BINANCE_SPOT')
            symbol: Symbol name (e.g., 'BTCUSDT')
            date: Date string (e.g., '2026-06-15')
        """
        self.replay_root = Path(replay_root)
        self.venue = venue
        self.symbol = symbol
        self.date = date

        # Final output directory (Hive-style)
        self.output_dir = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
        )

        # Staging directory (temporary)
        self.staging_dir = self.output_dir.parent / f".staging_{date}_{symbol}"

        # Ensure staging exists
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        # Data accumulation
        self.depth_batches: list[dict] = []
        self.trade_batches: list[dict] = []
        self.depth_count = 0
        self.trade_count = 0
        self._manifest: dict[str, Any] | None = None

    def write_depth_batch(self, records: list[dict]) -> None:
        """
        Accumulate depth records (will be sorted and written at finalize).

        Args:
            records: List of depth record dicts
        """
        self.depth_batches.extend(records)
        self.depth_count += len(records)

    def write_trades_batch(self, records: list[dict]) -> None:
        """
        Accumulate trade records (will be sorted and written at finalize).

        Args:
            records: List of trade record dicts
        """
        self.trade_batches.extend(records)
        self.trade_count += len(records)

    def _sort_records(self, records: list[dict], is_depth: bool = True) -> list[dict]:
        """
        Sort records deterministically by (stream_session_id, session_seq, raw_index).
        """
        if is_depth:
            return sorted(
                records,
                key=lambda r: (r["stream_session_id"], r["session_seq"], r["raw_index"]),
            )
        else:  # trades
            return sorted(
                records,
                key=lambda r: (r["trade_stream_session_id"], r["trade_session_seq"], r["raw_index"]),
            )

    def _records_to_table(
        self, records: list[dict], schema: pa.Schema
    ) -> pa.Table:
        """Convert list of dicts to PyArrow Table."""
        if not records:
            return pa.table({}, schema=schema)
        return pa.Table.from_pylist(records, schema=schema)

    def _compute_sha256(self, file_path: Path) -> str:
        """Compute SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def finalize_staging(self) -> dict[str, Any]:
        """
        Write sorted records to staging directory.

        Returns:
            Manifest dict with counts and metadata.
        """
        # Sort records deterministically
        sorted_depth = self._sort_records(self.depth_batches, is_depth=True)
        sorted_trades = self._sort_records(self.trade_batches, is_depth=False)

        # Convert to Parquet tables
        depth_table = self._records_to_table(sorted_depth, DEPTH_REPLAY_SCHEMA)
        trades_table = self._records_to_table(sorted_trades, TRADE_REPLAY_SCHEMA)

        # Write to staging with ZSTD compression
        depth_path = self.staging_dir / "depth.parquet"
        trades_path = self.staging_dir / "trades.parquet"

        pq.write_table(
            depth_table,
            depth_path,
            compression="zstd",
            compression_level=3,
        )
        pq.write_table(
            trades_table,
            trades_path,
            compression="zstd",
            compression_level=3,
        )

        logger.info(
            f"Wrote staging: {depth_path} ({self.depth_count} records), "
            f"{trades_path} ({self.trade_count} records)"
        )

        # Compute checksums
        depth_checksum = self._compute_sha256(depth_path)
        trades_checksum = self._compute_sha256(trades_path)

        # Get timestamp ranges
        ts_depth_range = self._get_timestamp_range(sorted_depth, is_depth=True)
        ts_trades_range = self._get_timestamp_range(sorted_trades, is_depth=False)
        ts_min = min(ts_depth_range[0], ts_trades_range[0]) if sorted_depth or sorted_trades else 0
        ts_max = max(ts_depth_range[1], ts_trades_range[1]) if sorted_depth or sorted_trades else 0

        # Create manifest
        manifest = {
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date,
            "status": "complete",
            "depth_record_count": self.depth_count,
            "trade_record_count": self.trade_count,
            "ts_range_start_ns": ts_min,
            "ts_range_end_ns": ts_max,
            "depth_checksum": depth_checksum,
            "trades_checksum": trades_checksum,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }

        self._manifest = manifest
        return manifest

    def _get_timestamp_range(
        self, records: list[dict], is_depth: bool = True
    ) -> tuple[int, int]:
        """Get min/max timestamp from records."""
        if not records:
            return (0, 0)
        if is_depth:
            ts_field = "ts_exchange_ns"
        else:
            ts_field = "ts_exchange_ns"
        timestamps = [r.get(ts_field, 0) for r in records]
        return (min(timestamps), max(timestamps))

    def publish(self, instrument_metadata: Optional[dict] = None) -> Path:
        """
        Atomically move staging to final output directory.
        Write instrument.json and manifest.json.

        Args:
            instrument_metadata: Optional instrument metadata dict

        Returns:
            Path to published directory
        """
        # Write instrument.json if provided
        if instrument_metadata:
            instrument_path = self.staging_dir / "instrument.json"
            with open(instrument_path, "w") as f:
                json.dump(instrument_metadata, f, indent=2)
            logger.info(f"Wrote instrument metadata: {instrument_path}")

        manifest = self._manifest
        if manifest is None:
            manifest = self.finalize_staging()

        # Write manifest.json
        manifest_path = self.staging_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Wrote manifest: {manifest_path}")

        # Atomically publish the completed partition directory.
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self.staging_dir, self.output_dir)

        logger.info(f"Published replay data to: {self.output_dir}")
        return self.output_dir
