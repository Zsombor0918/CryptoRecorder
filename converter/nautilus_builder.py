"""
Nautilus Data Object Building

Convert raw records to NautilusTrader compatible data structures.
"""
import logging
from typing import Optional, Dict, List
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class NautilusTradeTick:
    """NautilusTrader-compatible trade tick."""
    instrument_id: str
    ts_event_ns: int
    ts_init_ns: int
    price: float
    size: float
    direction: str  # "BUY" or "SELL"
    side: str  # "B" or "S"
    trade_id: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for Parquet export."""
        return {
            'instrument_id': self.instrument_id,
            'ts_event_ns': self.ts_event_ns,
            'ts_init_ns': self.ts_init_ns,
            'price': self.price,
            'size': self.size,
            'direction': self.direction,
            'side': self.side,
            'trade_id': self.trade_id,
        }


@dataclass
class NautilusOrderBookDelta:
    """NautilusTrader order book delta."""
    instrument_id: str
    ts_event_ns: int
    ts_init_ns: int
    side: str  # "BID" or "ASK"
    price_level: float
    size_level: float
    action: str  # "ADD", "UPDATE", "DELETE"
    reason: int  # 0=normal, 1=restart, 2=checksum, 3=gap
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for Parquet export."""
        return {
            'instrument_id': self.instrument_id,
            'ts_event_ns': self.ts_event_ns,
            'ts_init_ns': self.ts_init_ns,
            'side': self.side,
            'price_level': self.price_level,
            'size_level': self.size_level,
            'action': self.action,
            'reason': self.reason,
        }


@dataclass
class NautilusOrderBookDepth10:
    """NautilusTrader L2 order book snapshot."""
    instrument_id: str
    ts_event_ns: int
    ts_init_ns: int
    
    # Top 10 bids and asks
    bid_prices: List[float]
    bid_sizes: List[float]
    ask_prices: List[float]
    ask_sizes: List[float]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for Parquet export."""
        record = {
            'instrument_id': self.instrument_id,
            'ts_event_ns': self.ts_event_ns,
            'ts_init_ns': self.ts_init_ns,
        }
        
        # Add bid levels (bid_price_0, bid_size_0, etc.)
        for i in range(min(len(self.bid_prices), 10)):
            record[f'bid_price_{i}'] = self.bid_prices[i]
            record[f'bid_size_{i}'] = self.bid_sizes[i]
        
        # Add ask levels
        for i in range(min(len(self.ask_prices), 10)):
            record[f'ask_price_{i}'] = self.ask_prices[i]
            record[f'ask_size_{i}'] = self.ask_sizes[i]
        
        return record


class InstrumentIDBuilder:
    """Build Nautilus instrument IDs from raw symbols."""
    
    # Binance venue mapping
    VENUE_MAP = {
        'BINANCE_SPOT': 'BINANCE',
        'BINANCE_USDTF': 'BINANCE',
    }
    
    @staticmethod
    def build_instrument_id(venue: str, symbol: str) -> str:
        """
        Build Nautilus instrument ID.
        
        Examples:
            venue='BINANCE_SPOT', symbol='BTCUSDT' -> 'BTCUSDT.BINANCE'
            venue='BINANCE_USDTF', symbol='BTCUSDT' -> 'BTCUSDT-PERP.BINANCE'
        
        Args:
            venue: Raw venue name
            symbol: Raw symbol name
            
        Returns:
            Nautilus instrument_id
        """
        binance_venue = InstrumentIDBuilder.VENUE_MAP.get(venue, 'BINANCE')
        
        if venue == 'BINANCE_USDTF':
            # Futures: add -PERP suffix
            if not symbol.endswith('-PERP'):
                return f"{symbol}-PERP.{binance_venue}"
            else:
                return f"{symbol}.{binance_venue}"
        else:
            # Spot: no suffix
            return f"{symbol}.{binance_venue}"


class TradeTickBuilder:
    """Convert raw trade records to NautilusTradeTick."""
    
    @staticmethod
    def build(
        venue: str,
        symbol: str,
        ts_recv_ns: int,
        ts_event_ms: Optional[int],
        payload: Dict
    ) -> Optional[NautilusTradeTick]:
        """
        Build NautilusTradeTick from raw trade record.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            ts_recv_ns: Receive timestamp (nanoseconds)
            ts_event_ms: Event timestamp (milliseconds, may be None)
            payload: Raw trade payload
            
        Returns:
            NautilusTradeTick or None if parsing fails
        """
        try:
            instrument_id = InstrumentIDBuilder.build_instrument_id(venue, symbol)
            
            price = float(payload.get('price', 0))
            quantity = float(payload.get('quantity', 0))
            side = payload.get('side', 'BUY')  # BUY or SELL
            trade_id = str(payload.get('trade_id', ''))
            
            # Determine direction (buy/sell)
            if side.upper() == 'BUY':
                direction = "BUY"
                side_code = "B"
            else:
                direction = "SELL"
                side_code = "S"
            
            # Use event timestamp if available, else receive timestamp
            ts_event_ns = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            
            return NautilusTradeTick(
                instrument_id=instrument_id,
                ts_event_ns=ts_event_ns,
                ts_init_ns=ts_recv_ns,
                price=price,
                size=quantity,
                direction=direction,
                side=side_code,
                trade_id=trade_id,
            )
        except Exception as e:
            logger.error(f"Error building trade tick: {e}")
            return None


class OrderBookDeltaBuilder:
    """Convert raw L2 delta records to NautilusOrderBookDelta."""
    
    @staticmethod
    def build_from_deltas(
        venue: str,
        symbol: str,
        ts_recv_ns: int,
        ts_event_ms: Optional[int],
        payload: Dict,
        has_gap: bool = False
    ) -> List[NautilusOrderBookDelta]:
        """
        Build NautilusOrderBookDelta list from raw delta payload.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            ts_recv_ns: Receive timestamp (nanoseconds)
            ts_event_ms: Event timestamp (milliseconds)
            payload: Raw depth delta payload
            has_gap: Whether a gap was detected in sequence
            
        Returns:
            List of NautilusOrderBookDelta
        """
        result = []
        instrument_id = InstrumentIDBuilder.build_instrument_id(venue, symbol)
        ts_event_ns = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
        
        # Reason code: 1=gap detected, 0=normal
        reason = 1 if has_gap else 0
        
        try:
            # Process bids
            for price_str, size_str in payload.get('bids', []):
                price = float(price_str)
                size = float(size_str)
                
                action = "DELETE" if size == 0 else "UPDATE"
                
                delta = NautilusOrderBookDelta(
                    instrument_id=instrument_id,
                    ts_event_ns=ts_event_ns,
                    ts_init_ns=ts_recv_ns,
                    side="BID",
                    price_level=price,
                    size_level=size,
                    action=action,
                    reason=reason,
                )
                result.append(delta)
            
            # Process asks
            for price_str, size_str in payload.get('asks', []):
                price = float(price_str)
                size = float(size_str)
                
                action = "DELETE" if size == 0 else "UPDATE"
                
                delta = NautilusOrderBookDelta(
                    instrument_id=instrument_id,
                    ts_event_ns=ts_event_ns,
                    ts_init_ns=ts_recv_ns,
                    side="ASK",
                    price_level=price,
                    size_level=size,
                    action=action,
                    reason=reason,
                )
                result.append(delta)
        
        except Exception as e:
            logger.error(f"Error building order book deltas: {e}")
        
        return result


class OrderBookSnapshotBuilder:
    """Convert reconstructed order book to NautilusOrderBookDepth10."""
    
    @staticmethod
    def build(
        venue: str,
        symbol: str,
        ts_recv_ns: int,
        ts_event_ms: int,
        bids: List[tuple],  # [(price, size), ...]
        asks: List[tuple]   # [(price, size), ...]
    ) -> NautilusOrderBookDepth10:
        """
        Build NautilusOrderBookDepth10 from order book state.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            ts_recv_ns: Receive timestamp (ns)
            ts_event_ms: Event timestamp (ms)
            bids: List of (price, size) tuples (best first)
            asks: List of (price, size) tuples (best first)
            
        Returns:
            NautilusOrderBookDepth10
        """
        instrument_id = InstrumentIDBuilder.build_instrument_id(venue, symbol)
        ts_event_ns = ts_event_ms * 1_000_000
        
        # Extract prices and sizes
        bid_prices = [float(b[0]) for b in bids[:10]]
        bid_sizes = [float(b[1]) for b in bids[:10]]
        ask_prices = [float(a[0]) for a in asks[:10]]
        ask_sizes = [float(a[1]) for a in asks[:10]]
        
        # Pad to 10 levels if needed
        while len(bid_prices) < 10:
            bid_prices.append(0.0)
            bid_sizes.append(0.0)
        while len(ask_prices) < 10:
            ask_prices.append(0.0)
            ask_sizes.append(0.0)
        
        return NautilusOrderBookDepth10(
            instrument_id=instrument_id,
            ts_event_ns=ts_event_ns,
            ts_init_ns=ts_recv_ns,
            bid_prices=bid_prices[:10],
            bid_sizes=bid_sizes[:10],
            ask_prices=ask_prices[:10],
            ask_sizes=ask_sizes[:10],
        )


class NautilusInstrument:
    """Nautilus Instrument representation."""
    
    def __init__(
        self,
        instrument_id: str,
        isin: str,
        mro: List[str],
        price_precision: int = 8,
        size_precision: int = 8,
        price_increment: float = 0.01,
        size_increment: float = 0.00000001,
        maker_fee: float = 0.001,
        taker_fee: float = 0.001,
        ts_event_ns: int = None,
        ts_init_ns: int = None,
    ):
        """
        Initialize Nautilus Instrument.
        
        Args:
            instrument_id: e.g., 'BTCUSDT.BINANCE'
            isin: ISO name (e.g., 'BINANCE-BTCUSDT')
            mro: Multi-leg instrument representation
            price_precision: Number of decimal places for pricing
            size_precision: Number of decimal places for sizing
            price_increment: Minimum price movement
            size_increment: Minimum size movement
            maker_fee: Maker fee rate
            taker_fee: Taker fee rate
            ts_event_ns: Event timestamp
            ts_init_ns: Init timestamp
        """
        self.instrument_id = instrument_id
        self.isin = isin
        self.mro = mro  # [instrument_id]
        self.price_precision = price_precision
        self.size_precision = size_precision
        self.price_increment = price_increment
        self.size_increment = size_increment
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.ts_event_ns = ts_event_ns or 0
        self.ts_init_ns = ts_init_ns or 0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for Parquet export."""
        return {
            'instrument_id': self.instrument_id,
            'isin': self.isin,
            'mro': str(self.mro),
            'price_precision': self.price_precision,
            'size_precision': self.size_precision,
            'price_increment': self.price_increment,
            'size_increment': self.size_increment,
            'maker_fee': self.maker_fee,
            'taker_fee': self.taker_fee,
            'ts_event_ns': self.ts_event_ns,
            'ts_init_ns': self.ts_init_ns,
        }
