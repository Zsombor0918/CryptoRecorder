"""
stores.feature_writer — Feature Parquet writer with timeframe aggregation.

Writes feature data with staging/publish pattern.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .feature_schema import FEATURE_SCHEMA_CORE_V1

logger = logging.getLogger(__name__)


class FeatureWriter:
    """
    Writes feature data to Parquet with timeframe-based partitioning.
    
    Output layout (Hive-style):
        feature_store/
          timeframe=100ms/
            venue=BINANCE_SPOT/
              symbol=BTCUSDT/
                date=2026-06-15.parquet
          timeframe=1s/
            venue=BINANCE_SPOT/
              symbol=BTCUSDT/
                date=2026-06-15.parquet
    """

    def __init__(
        self,
        feature_root: Path,
        timeframe: str,
        venue: str,
        symbol: str,
        date: str,
    ):
        """
        Initialize feature writer for a single timeframe/venue/symbol/date.

        Args:
            feature_root: Base feature_store directory
            timeframe: Timeframe name (e.g., '100ms', '1s', '1m')
            venue: Venue name (e.g., 'BINANCE_SPOT')
            symbol: Symbol name (e.g., 'BTCUSDT')
            date: Date string (e.g., '2026-06-15')
        """
        self.feature_root = Path(feature_root)
        self.timeframe = timeframe
        self.venue = venue
        self.symbol = symbol
        self.date = date

        # Output directory (Hive-style)
        self.output_dir = (
            self.feature_root / f"timeframe={timeframe}" / f"venue={venue}" / f"symbol={symbol}"
        )
        self.output_file = self.output_dir / f"{date}.parquet"

        # Staging directory
        self.staging_dir = self.output_dir.parent / f".staging_{date}_{symbol}"
        self.staging_file = self.staging_dir / f"{date}.parquet"

        # Ensure staging exists
        self.staging_dir.mkdir(parents=True, exist_ok=True)

        # Data accumulation
        self.features: list[dict] = []
        self.feature_count = 0

    def write_feature_batch(self, records: list[dict]) -> None:
        """
        Accumulate feature records (will be sorted and written at finalize).

        Args:
            records: List of feature record dicts
        """
        self.features.extend(records)
        self.feature_count += len(records)

    def _records_to_table(self, records: list[dict]) -> pa.Table:
        """Convert list of dicts to PyArrow Table."""
        if not records:
            return pa.table({}, schema=FEATURE_SCHEMA_CORE_V1)
        return pa.Table.from_pylist(records, schema=FEATURE_SCHEMA_CORE_V1)

    def _compute_sha256(self, file_path: Path) -> str:
        """Compute SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def finalize_staging(self) -> dict[str, Any]:
        """
        Write sorted feature records to staging directory.

        Returns:
            Manifest dict with counts and metadata.
        """
        # Sort by timestamp
        sorted_features = sorted(self.features, key=lambda r: r.get("timestamp_ns", 0))

        # Convert to Parquet table
        feature_table = self._records_to_table(sorted_features)

        # Write to staging with ZSTD compression
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            feature_table,
            self.staging_file,
            compression="zstd",
            compression_level=3,
        )

        logger.info(
            f"Wrote feature staging: {self.staging_file} ({self.feature_count} records)"
        )

        # Compute checksum
        checksum = self._compute_sha256(self.staging_file)

        # Get timestamp range
        timestamps = [r.get("timestamp_ns", 0) for r in sorted_features]
        ts_min = min(timestamps) if timestamps else 0
        ts_max = max(timestamps) if timestamps else 0

        # Create manifest
        manifest = {
            "timeframe": self.timeframe,
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date,
            "status": "complete",
            "record_count": self.feature_count,
            "ts_range_start_ns": ts_min,
            "ts_range_end_ns": ts_max,
            "checksum": checksum,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }

        return manifest

    def publish(self, manifest: Optional[dict] = None) -> Path:
        """
        Atomically move staging to final output directory.

        Args:
            manifest: Optional manifest dict

        Returns:
            Path to published file
        """
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Move staging file to final location
        if self.staging_file.exists():
            if self.output_file.exists():
                self.output_file.unlink()
            self.staging_file.rename(self.output_file)
            logger.info(f"Published feature file: {self.output_file}")

        # Write manifest if provided
        if manifest:
            manifest_path = self.output_dir / f"{self.date}.manifest.json"
            with open(manifest_path, "w") as f:
                json.dump(manifest, f, indent=2)
            logger.info(f"Wrote feature manifest: {manifest_path}")

        # Clean up staging
        if self.staging_dir.exists():
            import shutil
            shutil.rmtree(self.staging_dir)

        return self.output_file
