"""
JSONL Record Parsing and Decompression

Utilities for reading, decompressing, and parsing raw JSONL records from storage.
"""
import json
import gzip
import logging
from pathlib import Path
from typing import Generator, List, Dict, Any, Optional
from dataclasses import dataclass
import zstandard as zstd
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class RawRecord:
    """Parsed record from JSONL storage."""
    venue: str
    symbol: str
    channel: str
    ts_recv_ns: int
    ts_event_ms: Optional[int]
    sequence_number: Optional[int]
    update_id: Optional[int]
    final_update_id: Optional[int]
    payload: Dict[str, Any]


@dataclass
class ExchangeInfo:
    """Instrument metadata from exchangeInfo API."""
    venue: str
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    price_precision: int
    qty_precision: int
    min_notional: float
    min_qty: float
    max_qty: float


class JSONLReader:
    """Read and decompress JSONL files from storage."""
    
    SUPPORTED_COMPRESSIONS = {'.zst', '.gz', '.jsonl'}
    
    @staticmethod
    def read_file(file_path: Path) -> Generator[RawRecord, None, None]:
        """
        Read a single JSONL file (any compression format).
        
        Args:
            file_path: Path to .jsonl, .jsonl.zst, or .jsonl.gz file
            
        Yields:
            RawRecord instances
        """
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.debug(f"Reading file: {file_path}")
        
        try:
            if file_path.suffix == '.zst':
                with zstd.open(file_path, 'rt', errors='ignore') as f:
                    yield from JSONLReader._parse_lines(f)
            elif file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', errors='ignore') as f:
                    yield from JSONLReader._parse_lines(f)
            else:
                with open(file_path, 'r', errors='ignore') as f:
                    yield from JSONLReader._parse_lines(f)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
    
    @staticmethod
    def _parse_lines(file_obj) -> Generator[RawRecord, None, None]:
        """Parse lines from file object."""
        line_num = 0
        for line in file_obj:
            line_num += 1
            if not line.strip():
                continue
            
            try:
                yield JSONLReader._parse_line(line)
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error at line {line_num}: {e}")
            except Exception as e:
                logger.warning(f"Parse error at line {line_num}: {e}")
    
    @staticmethod
    def _parse_line(line: str) -> RawRecord:
        """Parse single JSON line into RawRecord."""
        data = json.loads(line.strip())
        
        return RawRecord(
            venue=data.get('venue'),
            symbol=data.get('symbol'),
            channel=data.get('channel'),
            ts_recv_ns=data.get('ts_recv_ns'),
            ts_event_ms=data.get('ts_event_ms'),
            sequence_number=data.get('sequence_number'),
            update_id=data.get('update_id'),
            final_update_id=data.get('final_update_id'),
            payload=data.get('payload', {})
        )
    
    @staticmethod
    def read_directory(dir_path: Path) -> Generator[RawRecord, None, None]:
        """
        Read all JSONL files in directory (recursive).
        
        Args:
            dir_path: Directory containing .jsonl files
            
        Yields:
            RawRecord instances from all files in sorted order
        """
        if not dir_path.exists():
            logger.warning(f"Directory not found: {dir_path}")
            return
        
        # Get all JSONL files, sorted
        files = sorted(
            dir_path.glob('**/*.jsonl*'),
            key=lambda p: p.name
        )
        
        logger.info(f"Found {len(files)} files in {dir_path}")
        
        for file_path in files:
            yield from JSONLReader.read_file(file_path)
    
    @staticmethod
    def read_date_range(
        data_root: Path,
        venue: str,
        symbol: str,
        channel: str,
        start_date: datetime,
        end_date: datetime
    ) -> Generator[RawRecord, None, None]:
        """
        Read all records for date range.
        
        Args:
            data_root: Root data directory
            venue: Venue name (e.g., 'BINANCE_SPOT')
            symbol: Symbol name (e.g., 'BTCUSDT')
            channel: Channel name (e.g., 'depth', 'trade')
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Yields:
            RawRecord instances
        """
        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            day_path = data_root / venue / channel / symbol / date_str
            
            if day_path.exists():
                logger.debug(f"Reading {date_str}: {day_path}")
                yield from JSONLReader.read_directory(day_path)
            else:
                logger.debug(f"No data for {date_str}")
            
            current = current.replace(day=current.day + 1)


class InstrumentMetadataParser:
    """Parse exchange info metadata from JSON files."""
    
    @staticmethod
    def load_exchangeinfo(file_path: Path) -> Dict[str, ExchangeInfo]:
        """
        Load exchangeInfo JSON and parse into ExchangeInfo objects.
        
        Args:
            file_path: Path to exchangeInfo JSON file
            
        Returns:
            Dict mapping symbol -> ExchangeInfo
        """
        result = {}
        
        if not file_path.exists():
            logger.warning(f"ExchangeInfo file not found: {file_path}")
            return result
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            venue = file_path.stem.split('_')[1]  # e.g., BINANCE_SPOT_20240115.json
            
            # Parse symbols
            for symbol_data in data.get('symbols', []):
                symbol = symbol_data.get('symbol')
                if not symbol:
                    continue
                
                # Get precision from filters
                price_precision = 8
                qty_precision = 8
                for filter_obj in symbol_data.get('filters', []):
                    if filter_obj.get('filterType') == 'PRICE_FILTER':
                        price_precision = len(str(filter_obj.get('tickSize', '')).rstrip('0').split('.')[-1])
                    elif filter_obj.get('filterType') == 'LOT_SIZE':
                        qty_precision = len(str(filter_obj.get('stepSize', '')).rstrip('0').split('.')[-1])
                
                info = ExchangeInfo(
                    venue=venue,
                    symbol=symbol,
                    status=symbol_data.get('status', 'TRADING'),
                    base_asset=symbol_data.get('baseAsset', ''),
                    quote_asset=symbol_data.get('quoteAsset', ''),
                    price_precision=price_precision,
                    qty_precision=qty_precision,
                    min_notional=float(symbol_data.get('notional', {}).get('minNotional', 0)),
                    min_qty=float(symbol_data.get('filters', [{}])[1].get('minQty', 0)),
                    max_qty=float(symbol_data.get('filters', [{}])[1].get('maxQty', 0)),
                )
                result[symbol] = info
            
            logger.info(f"Loaded {len(result)} instruments from {file_path}")
        except Exception as e:
            logger.error(f"Error parsing exchangeInfo: {e}")
        
        return result
    
    @staticmethod
    def load_universe(file_path: Path) -> List[str]:
        """
        Load universe JSON (list of selected symbols).
        
        Args:
            file_path: Path to universe JSON file
            
        Returns:
            List of symbol strings
        """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # May be list or dict with venue keys
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Return first venue's symbols
                for key in data.values():
                    if isinstance(key, list):
                        return key
            
            return []
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            return []


class RecordFilter:
    """Filter and query RawRecord streams."""
    
    @staticmethod
    def by_channel(
        records: Generator[RawRecord, None, None],
        channel: str
    ) -> Generator[RawRecord, None, None]:
        """Filter records by channel."""
        for record in records:
            if record.channel == channel:
                yield record
    
    @staticmethod
    def by_symbol(
        records: Generator[RawRecord, None, None],
        symbols: set
    ) -> Generator[RawRecord, None, None]:
        """Filter records by symbol."""
        for record in records:
            if record.symbol in symbols:
                yield record
    
    @staticmethod
    def by_time_range(
        records: Generator[RawRecord, None, None],
        start_ns: int,
        end_ns: int
    ) -> Generator[RawRecord, None, None]:
        """Filter records by timestamp range."""
        for record in records:
            if start_ns <= record.ts_recv_ns <= end_ns:
                yield record
    
    @staticmethod
    def dedup_by_sequence(
        records: Generator[RawRecord, None, None]
    ) -> Generator[RawRecord, None, None]:
        """Deduplicate records by sequence number (for L2 updates)."""
        seen = {}
        
        for record in records:
            # Use (venue, symbol, update_id/sequence) as key
            key = (record.venue, record.symbol, record.update_id or record.sequence_number)
            
            if key not in seen:
                seen[key] = True
                yield record
