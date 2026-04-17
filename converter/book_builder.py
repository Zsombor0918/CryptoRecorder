"""
Order Book Reconstruction from Deltas

Reconstructs L2 order book state from streaming delta updates with gap detection.
"""
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from decimal import Decimal

logger = logging.getLogger(__name__)


@dataclass
class OrderBookLevel:
    """Single level in order book."""
    price: float
    size: float
    
    def __lt__(self, other):
        return self.price < other.price


@dataclass
class OrderBookSnapshot:
    """Complete order book state at a point in time."""
    symbol: str
    venue: str
    ts_event_ms: int
    ts_recv_ns: int
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)
    has_gap: bool = False
    gap_from_update_id: Optional[int] = None
    gap_to_update_id: Optional[int] = None
    last_update_id: int = 0


class LocalOrderBook:
    """
    Maintain local order book state from Binance delta updates.
    
    Handles both Spot and Futures L2 depth updates with gap detection.
    """
    
    def __init__(
        self,
        symbol: str,
        venue: str,
        depth: int = 20
    ):
        self.symbol = symbol
        self.venue = venue
        self.depth = depth  # Keep top N levels (5, 10, 20, etc.)
        
        # Order book state
        self.bids: Dict[float, float] = {}  # price -> size
        self.asks: Dict[float, float] = {}  # price -> size
        
        # Sequence tracking
        self.last_update_id = 0
        self.last_update_id_u = 0  # For strict ordering
        
        # Gap detection
        self.gaps_detected = 0
        self.last_gap_at_update_id = None
    
    def load_snapshot(
        self,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        last_update_id: int
    ) -> None:
        """
        Load a fresh snapshot (REST API response).
        
        Args:
            bids: [(price, size), ...]
            asks: [(price, size), ...]
            last_update_id: lastUpdateId from snapshot
        """
        self.bids = {float(p): float(s) for p, s in bids if float(s) > 0}
        self.asks = {float(p): float(s) for p, s in asks if float(s) > 0}
        self.last_update_id = last_update_id
        self.last_update_id_u = last_update_id
        
        logger.debug(
            f"{self.symbol}: Loaded snapshot with "
            f"{len(self.bids)} bids, {len(self.asks)} asks, "
            f"lastUpdateId={last_update_id}"
        )
    
    def apply_delta(
        self,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        update_id: int,
        final_update_id: int
    ) -> bool:
        """
        Apply a delta update to the order book.
        
        Args:
            bids: [(price_str, size_str), ...]
            asks: [(price_str, size_str), ...]
            update_id: u field (update ID)
            final_update_id: U field (final update ID)
            
        Returns:
            True if applied successfully, False if gap detected
        """
        # Check for gaps (strict ordering)
        expected_update_id = self.last_update_id + 1
        
        if update_id != expected_update_id:
            # Gap detected
            gap_size = update_id - expected_update_id
            self.gaps_detected += 1
            self.last_gap_at_update_id = update_id
            
            logger.warning(
                f"{self.symbol}: Gap detected! Expected u={expected_update_id}, "
                f"got u={update_id}, gap_size={gap_size}"
            )
            return False
        
        try:
            # Apply bid deltas
            for price_str, size_str in bids:
                price = float(price_str)
                size = float(size_str)
                
                if size == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = size
            
            # Apply ask deltas
            for price_str, size_str in asks:
                price = float(price_str)
                size = float(size_str)
                
                if size == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = size
            
            # Update sequence
            self.last_update_id = final_update_id
            self.last_update_id_u = update_id
            
            return True
            
        except Exception as e:
            logger.error(f"{self.symbol}: Error applying delta: {e}")
            return False
    
    def get_snapshot(
        self,
        ts_event_ms: int,
        ts_recv_ns: int,
        levels: Optional[int] = None
    ) -> OrderBookSnapshot:
        """
        Get current order book state as snapshot.
        
        Args:
            ts_event_ms: Event timestamp in milliseconds
            ts_recv_ns: Receive timestamp in nanoseconds
            levels: Number of levels to include (default: self.depth)
            
        Returns:
            OrderBookSnapshot
        """
        if levels is None:
            levels = self.depth
        
        # Sort and take top levels
        bid_levels = sorted(
            [OrderBookLevel(p, s) for p, s in self.bids.items()],
            key=lambda x: -x.price
        )[:levels]
        
        ask_levels = sorted(
            [OrderBookLevel(p, s) for p, s in self.asks.items()],
            key=lambda x: x.price
        )[:levels]
        
        return OrderBookSnapshot(
            symbol=self.symbol,
            venue=self.venue,
            ts_event_ms=ts_event_ms,
            ts_recv_ns=ts_recv_ns,
            bids=bid_levels,
            asks=ask_levels,
            last_update_id=self.last_update_id,
            has_gap=False,  # Set flag if gap was detected
        )
    
    def get_state_for_export(self) -> Dict:
        """Get book state as dict for debugging."""
        return {
            'symbol': self.symbol,
            'venue': self.venue,
            'last_update_id': self.last_update_id,
            'last_update_id_u': self.last_update_id_u,
            'gaps_detected': self.gaps_detected,
            'bid_count': len(self.bids),
            'ask_count': len(self.asks),
            'top_bid': max(self.bids.keys()) if self.bids else None,
            'top_ask': min(self.asks.keys()) if self.asks else None,
        }
    
    def reset(self) -> None:
        """Reset book to empty state."""
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.last_update_id_u = 0
        self.gaps_detected = 0
        self.last_gap_at_update_id = None


class BookReconstructor:
    """
    Reconstruct order books from raw records.
    
    Manages LocalOrderBook instances and handles snapshot loading + delta application.
    """
    
    def __init__(self, depth: int = 20):
        self.depth = depth
        self.books: Dict[Tuple[str, str], LocalOrderBook] = {}  # (venue, symbol) -> book
    
    def get_or_create_book(self, venue: str, symbol: str) -> LocalOrderBook:
        """Get or create book for (venue, symbol)."""
        key = (venue, symbol)
        if key not in self.books:
            self.books[key] = LocalOrderBook(symbol, venue, depth=self.depth)
        return self.books[key]
    
    def process_snapshot_record(
        self,
        venue: str,
        symbol: str,
        ts_event_ms: int,
        ts_recv_ns: int,
        payload: Dict
    ) -> OrderBookSnapshot:
        """
        Process a snapshot record and update book state.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            ts_event_ms: Event time in ms
            ts_recv_ns: Receive time in ns
            payload: Raw payload from record
            
        Returns:
            OrderBookSnapshot after loading
        """
        book = self.get_or_create_book(venue, symbol)
        
        bids = payload.get('bids', [])
        asks = payload.get('asks', [])
        last_update_id = payload.get('lastUpdateId', 0)
        
        book.load_snapshot(bids, asks, last_update_id)
        
        return book.get_snapshot(ts_event_ms, ts_recv_ns)
    
    def process_delta_record(
        self,
        venue: str,
        symbol: str,
        ts_event_ms: int,
        ts_recv_ns: int,
        payload: Dict
    ) -> Tuple[OrderBookSnapshot, bool]:
        """
        Process a delta record and update book state.
        
        Args:
            venue: Venue name
            symbol: Symbol name
            ts_event_ms: Event time in ms
            ts_recv_ns: Receive time in ns
            payload: Raw payload from record
            
        Returns:
            (OrderBookSnapshot, gap_detected)
        """
        book = self.get_or_create_book(venue, symbol)
        
        bids = payload.get('bids', [])
        asks = payload.get('asks', [])
        u = payload.get('u')  # update ID
        U = payload.get('U')  # final update ID
        
        success = book.apply_delta(bids, asks, u, U)
        
        snapshot = book.get_snapshot(ts_event_ms, ts_recv_ns)
        snapshot.has_gap = not success
        
        if not success:
            snapshot.gap_from_update_id = book.last_update_id_u
            snapshot.gap_to_update_id = u
        
        return snapshot, not success
    
    def get_book_state(self, venue: str, symbol: str) -> Optional[Dict]:
        """Get book state dict for debugging."""
        key = (venue, symbol)
        if key in self.books:
            return self.books[key].get_state_for_export()
        return None
    
    def reset_book(self, venue: str, symbol: str) -> None:
        """Reset a specific book."""
        key = (venue, symbol)
        if key in self.books:
            self.books[key].reset()
