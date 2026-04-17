"""
Parquet catalog converter - RAW JSONL to Nautilus ParquetDataCatalog.
Converts previous day's data from raw logs to Nautilus format for backtesting.

Usage:
    python convert_yesterday.py                  # Convert yesterday
    python convert_yesterday.py --date 2024-01-15  # Convert specific date
"""
import asyncio
import gzip
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
import zstandard as zstd
import argparse

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: Please install pandas and pyarrow: pip install pandas pyarrow")
    exit(1)

from config import (
    META_ROOT,
    STATE_ROOT,
    DATA_ROOT,
    NAUTILUS_CATALOG_ROOT,
    CONVERTER_BATCH_SIZE,
)

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class RawRecord:
    """Parsed raw record from JSONL"""
    venue: str
    symbol: str
    channel: str
    ts_recv_ns: int
    ts_event_ms: Optional[int]
    payload: dict


@dataclass
class TradeTick:
    """Converted trade tick for Nautilus"""
    symbol: str
    venue: str
    ts_event_ns: int
    ts_init_ns: int
    price: float
    quantity: float
    side: str


@dataclass
class OrderBookLevel:
    """L2 order book level"""
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    """Reconstructed L2 order book state"""
    symbol: str
    venue: str
    ts_event_ms: int
    ts_recv_ns: int
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]


# ============================================================================
# File I/O
# ============================================================================

class RawDataReader:
    """Read compressed JSONL files from raw data storage."""
    
    @staticmethod
    def read_files_for_date(venue: str, symbol: str, channel: str, date: datetime) -> Generator[RawRecord, None, None]:
        """Generator that yields records from all files for a given date."""
        date_str = date.strftime("%Y-%m-%d")
        day_path = DATA_ROOT / venue / channel / symbol / date_str
        
        if not day_path.exists():
            logger.debug(f"Directory not found: {day_path}")
            return
        
        # Iterate over all hourly files
        for file_path in sorted(day_path.glob("*.jsonl*")):
            yield from RawDataReader._read_file(file_path)
    
    @staticmethod
    def _read_file(file_path: Path) -> Generator[RawRecord, None, None]:
        """Read single compressed file and yield records."""
        try:
            if file_path.suffix == '.zst':
                with zstd.open(file_path, 'rt', errors='ignore') as f:
                    for line in f:
                        if line.strip():
                            yield RawDataReader._parse_line(line)
            elif file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', errors='ignore') as f:
                    for line in f:
                        if line.strip():
                            yield RawDataReader._parse_line(line)
            else:
                with open(file_path, 'r', errors='ignore') as f:
                    for line in f:
                        if line.strip():
                            yield RawDataReader._parse_line(line)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
    
    @staticmethod
    def _parse_line(line: str) -> RawRecord:
        """Parse JSON line into RawRecord."""
        data = json.loads(line)
        return RawRecord(
            venue=data.get('venue'),
            symbol=data.get('symbol'),
            channel=data.get('channel'),
            ts_recv_ns=data.get('ts_recv_ns'),
            ts_event_ms=data.get('ts_event_ms'),
            payload=data.get('payload', {})
        )


# ============================================================================
# Data Conversion
# ============================================================================

class TradeConverter:
    """Convert raw trades to Nautilus TradeTicks."""
    
    @staticmethod
    def convert(record: RawRecord) -> Optional[TradeTick]:
        """Convert a trade record."""
        try:
            payload = record.payload
            price = float(payload.get('price', 0))
            quantity = float(payload.get('quantity', 0))
            side = payload.get('side', 'BUY')
            
            # Use ts_event_ms if available, else ts_recv_ns
            ts_event_ns = record.ts_event_ms * 1_000_000 if record.ts_event_ms else record.ts_recv_ns
            
            return TradeTick(
                symbol=record.symbol,
                venue=record.venue,
                ts_event_ns=ts_event_ns,
                ts_init_ns=record.ts_recv_ns,
                price=price,
                quantity=quantity,
                side=side,
            )
        except Exception as e:
            logger.error(f"Error converting trade: {e}")
            return None


class BookReconstructor:
    """Reconstruct L2 order book from delta updates."""
    
    def __init__(self, symbol: str, venue: str):
        self.symbol = symbol
        self.venue = venue
        self.bids: Dict[float, float] = {}  # price -> size
        self.asks: Dict[float, float] = {}  # price -> size
        self.last_update_id = None
        self.gaps = 0
    
    def load_snapshot(self, record: RawRecord) -> OrderBookSnapshot:
        """Load a snapshot and reset book state."""
        payload = record.payload
        
        # Load bids and asks from snapshot
        bids = {float(p): float(s) for p, s in payload.get('bids', [])}
        asks = {float(p): float(s) for p, s in payload.get('asks', [])}
        
        self.bids = bids
        self.asks = asks
        self.last_update_id = payload.get('lastUpdateId')
        
        # Return snapshot
        bid_levels = sorted([OrderBookLevel(p, s) for p, s in bids.items()], key=lambda x: -x.price)
        ask_levels = sorted([OrderBookLevel(p, s) for p, s in asks.items()], key=lambda x: x.price)
        
        return OrderBookSnapshot(
            symbol=self.symbol,
            venue=self.venue,
            ts_event_ms=record.ts_event_ms or 0,
            ts_recv_ns=record.ts_recv_ns,
            bids=bid_levels,
            asks=ask_levels,
        )
    
    def apply_delta(self, record: RawRecord) -> OrderBookSnapshot:
        """Apply delta update and return current state."""
        payload = record.payload
        
        # Apply bid deltas
        for price_str, size_str in payload.get('bids', []):
            price = float(price_str)
            size = float(size_str)
            
            if size == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = size
        
        # Apply ask deltas
        for price_str, size_str in payload.get('asks', []):
            price = float(price_str)
            size = float(size_str)
            
            if size == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size
        
        # Return current state
        bid_levels = sorted([OrderBookLevel(p, s) for p, s in self.bids.items()], key=lambda x: -x.price)[:10]
        ask_levels = sorted([OrderBookLevel(p, s) for p, s in self.asks.items()], key=lambda x: x.price)[:10]
        
        return OrderBookSnapshot(
            symbol=self.symbol,
            venue=self.venue,
            ts_event_ms=record.ts_event_ms or 0,
            ts_recv_ns=record.ts_recv_ns,
            bids=bid_levels,
            asks=ask_levels,
        )


# ============================================================================
# Parquet Writer
# ============================================================================

class ParquetWriter:
    """Write data to Nautilus ParquetDataCatalog."""
    
    def __init__(self):
        NAUTILUS_CATALOG_ROOT.mkdir(parents=True, exist_ok=True)
    
    def write_trades(self, trades: List[TradeTick], date_str: str) -> None:
        """Write trades to Parquet."""
        if not trades:
            return
        
        df = pd.DataFrame([
            {
                'symbol': t.symbol,
                'venue': t.venue,
                'ts_event_ns': t.ts_event_ns,
                'ts_init_ns': t.ts_init_ns,
                'price': t.price,
                'quantity': t.quantity,
                'side': t.side,
            }
            for t in trades
        ])
        
        output_file = NAUTILUS_CATALOG_ROOT / f"trades_{date_str}.parquet"
        df.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"Wrote {len(df)} trades to {output_file}")
    
    def write_depth(self, snapshots: List[OrderBookSnapshot], date_str: str) -> None:
        """Write depth snapshots to Parquet."""
        if not snapshots:
            return
        
        records = []
        for snap in snapshots:
            for i, bid in enumerate(snap.bids[:5]):
                records.append({
                    'symbol': snap.symbol,
                    'venue': snap.venue,
                    'ts_event_ms': snap.ts_event_ms,
                    'bid_price_' + str(i): bid.price,
                    'bid_size_' + str(i): bid.size,
                })
            for i, ask in enumerate(snap.asks[:5]):
                if i < len(records):
                    records[-1]['ask_price_' + str(i)] = ask.price
                    records[-1]['ask_size_' + str(i)] = ask.size
        
        if records:
            df = pd.DataFrame(records)
            output_file = NAUTILUS_CATALOG_ROOT / f"depth_{date_str}.parquet"
            df.to_parquet(output_file, compression='snappy', index=False)
            logger.info(f"Wrote {len(df)} depth snapshots to {output_file}")


# ============================================================================
# Main Converter
# ============================================================================

async def convert_date(date: datetime) -> Dict:
    """Convert a day's worth of data."""
    logger.info(f"Converting data for {date.date()}...")
    
    date_str = date.strftime("%Y-%m-%d")
    report = {
        'date': date_str,
        'timestamp': datetime.utcnow().isoformat(),
        'venues': {},
        'total_trades': 0,
        'total_depth_snapshots': 0,
        'gaps_detected': 0,
    }
    
    writer = ParquetWriter()
    
    # Get universe for this date
    universe_file = META_ROOT / "universe" / "BINANCE_SPOT" / f"{date_str}.json"
    if not universe_file.exists():
        logger.warning(f"Universe file not found for {date_str}")
        return report
    
    with open(universe_file) as f:
        universe = json.load(f)
    
    # Process each venue
    for venue in universe['BINANCE_SPOT'] if 'BINANCE_SPOT' in universe else []:
        for channel in ['trade', 'depth', 'snapshot']:
            pass  # Placeholder for processing
    
    # Save report
    report_file = STATE_ROOT / "convert_reports" / f"{date_str}.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Conversion report saved to {report_file}")
    return report


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Convert raw Binance data to Nautilus Parquet")
    parser.add_argument('--date', type=str, help='Date to convert (YYYY-MM-DD), defaults to yesterday')
    args = parser.parse_args()
    
    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        date = datetime.utcnow() - timedelta(days=1)
    
    logger.info(f"Starting converter for {date.date()}")
    report = await convert_date(date)
    logger.info(f"Conversion complete: {json.dumps(report, indent=2)}")


if __name__ == '__main__':
    asyncio.run(main())
