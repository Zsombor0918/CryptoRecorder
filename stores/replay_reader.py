"""
stores.replay_reader — Streaming reader for replay_store Parquet data.

Handles iteration over replay partitions with memory-efficient streaming.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Generator, Optional

import pyarrow.parquet as pq

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
            Depth record dicts
        """
        depth_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "depth.parquet"
        )
        if not depth_path.exists():
            return

        try:
            parquet = pq.ParquetFile(depth_path)
            for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                for record in batch.to_pylist():
                    if record is not None:
                        yield record
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
            Trade record dicts
        """
        trades_path = (
            self.replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "trades.parquet"
        )
        if not trades_path.exists():
            return

        try:
            parquet = pq.ParquetFile(trades_path)
            for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
                for record in batch.to_pylist():
                    if record is not None:
                        yield record
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
