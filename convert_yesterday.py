"""
Parquet catalog converter – RAW JSONL to Nautilus ParquetDataCatalog.

Reads raw trade and depth JSONL(.zst) files for a given date, converts
trades to a single Parquet file, reconstructs approximate L2 Depth-10
snapshots from cryptofeed-normalised deltas (no REST snapshots needed),
and writes them to NAUTILUS_CATALOG_ROOT.

Usage:
    python convert_yesterday.py                      # Convert yesterday
    python convert_yesterday.py --date 2026-04-17    # Convert specific date
"""
import argparse
import gzip
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set

import zstandard as zstd

try:
    import pandas as pd
    import pyarrow  # noqa: F401 – presence check
except ImportError:
    print("ERROR: pip install pandas pyarrow")
    exit(1)

from config import (
    DATA_ROOT,
    META_ROOT,
    NAUTILUS_CATALOG_ROOT,
    STATE_ROOT,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Depth-10 snapshot interval (seconds).  One snapshot per symbol per interval.
# ---------------------------------------------------------------------------
DEPTH_SNAPSHOT_INTERVAL_SEC: float = 1.0


# ===================================================================
# Data classes  (public names – used by validate_converter_e2e.py)
# ===================================================================

@dataclass
class RawRecord:
    """Parsed raw record from JSONL."""
    venue: str
    symbol: str
    channel: str
    ts_recv_ns: int
    ts_event_ms: Optional[int]
    payload: dict


@dataclass
class TradeTick:
    """Converted trade tick for Nautilus."""
    symbol: str
    venue: str
    ts_event_ns: int
    ts_init_ns: int
    price: float
    quantity: float
    side: str
    trade_id: Optional[str] = None


@dataclass
class OrderBookLevel:
    """Single price level."""
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    """Reconstructed Depth-10 order-book state."""
    symbol: str
    venue: str
    ts_event_ms: int
    ts_recv_ns: int
    bids: List[OrderBookLevel] = field(default_factory=list)
    asks: List[OrderBookLevel] = field(default_factory=list)


# ===================================================================
# File I/O – streaming reader
# ===================================================================

class RawDataReader:
    """Read JSONL / JSONL.zst / JSONL.gz files from raw data storage."""

    @staticmethod
    def read_files_for_date(
        venue: str, symbol: str, channel: str, date: datetime,
    ) -> Generator[RawRecord, None, None]:
        """Yield records from all hourly files for *date*."""
        date_str = date.strftime("%Y-%m-%d")
        day_path = DATA_ROOT / venue / channel / symbol / date_str
        if not day_path.exists():
            return
        for file_path in sorted(day_path.glob("*.jsonl*")):
            yield from RawDataReader._read_file(file_path)

    @staticmethod
    def _read_file(file_path: Path) -> Generator[RawRecord, None, None]:
        try:
            if file_path.suffix == ".zst":
                opener = lambda: zstd.open(file_path, "rt", errors="ignore")
            elif file_path.suffix == ".gz":
                opener = lambda: gzip.open(file_path, "rt", errors="ignore")
            else:
                opener = lambda: open(file_path, "r", errors="ignore")

            with opener() as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield RawDataReader._parse_line(line)
                    except (json.JSONDecodeError, KeyError):
                        pass
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")

    @staticmethod
    def _parse_line(line: str) -> RawRecord:
        d = json.loads(line)
        return RawRecord(
            venue=d.get("venue", ""),
            symbol=d.get("symbol", ""),
            channel=d.get("channel", ""),
            ts_recv_ns=d.get("ts_recv_ns", 0),
            ts_event_ms=d.get("ts_event_ms"),
            payload=d.get("payload", {}),
        )


# ===================================================================
# Trade conversion
# ===================================================================

class TradeConverter:
    @staticmethod
    def convert(record: RawRecord) -> Optional[TradeTick]:
        try:
            p = record.payload
            price = float(p.get("price", 0))
            qty = float(p.get("quantity", 0))
            side = p.get("side", "buy")
            ts_event_ns = (
                record.ts_event_ms * 1_000_000
                if record.ts_event_ms
                else record.ts_recv_ns
            )
            return TradeTick(
                symbol=record.symbol,
                venue=record.venue,
                ts_event_ns=ts_event_ns,
                ts_init_ns=record.ts_recv_ns,
                price=price,
                quantity=qty,
                side=side,
                trade_id=p.get("trade_id"),
            )
        except Exception as e:
            logger.debug(f"trade convert error: {e}")
            return None


# ===================================================================
# Book reconstruction (delta-only, no REST snapshots)
# ===================================================================

class BookReconstructor:
    """Maintain an in-memory L2 book from delta updates.

    Starts with an empty book and applies each delta in order.
    This gives an *approximate* book — deterministic replay would
    require the initial REST snapshot + Binance update-ID chain.
    """

    def __init__(self, symbol: str, venue: str):
        self.symbol = symbol
        self.venue = venue
        self.bids: Dict[float, float] = {}  # price → size
        self.asks: Dict[float, float] = {}

    def apply_delta(self, record: RawRecord) -> OrderBookSnapshot:
        """Apply a delta and return current Depth-10 state."""
        payload = record.payload

        for price_s, size_s in payload.get("bids", []):
            price, size = float(price_s), float(size_s)
            if size == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = size

        for price_s, size_s in payload.get("asks", []):
            price, size = float(price_s), float(size_s)
            if size == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = size

        bid_levels = sorted(
            (OrderBookLevel(p, s) for p, s in self.bids.items()),
            key=lambda x: -x.price,
        )[:10]
        ask_levels = sorted(
            (OrderBookLevel(p, s) for p, s in self.asks.items()),
            key=lambda x: x.price,
        )[:10]

        return OrderBookSnapshot(
            symbol=self.symbol,
            venue=self.venue,
            ts_event_ms=record.ts_event_ms or 0,
            ts_recv_ns=record.ts_recv_ns,
            bids=bid_levels,
            asks=ask_levels,
        )


# ===================================================================
# Parquet writer
# ===================================================================

class ParquetWriter:
    """Write trades / depth Parquet files into a catalog directory."""

    def __init__(self, root: Optional[Path] = None):
        self.root = root or NAUTILUS_CATALOG_ROOT
        self.root.mkdir(parents=True, exist_ok=True)

    def write_trades(self, trades: List[TradeTick], date_str: str) -> int:
        """Write trade ticks to a single sorted Parquet file.  Returns row count."""
        if not trades:
            return 0
        df = pd.DataFrame(
            {
                "symbol": [t.symbol for t in trades],
                "venue": [t.venue for t in trades],
                "ts_event_ns": [t.ts_event_ns for t in trades],
                "ts_init_ns": [t.ts_init_ns for t in trades],
                "price": [t.price for t in trades],
                "quantity": [t.quantity for t in trades],
                "side": [t.side for t in trades],
                "trade_id": [t.trade_id for t in trades],
            }
        )
        df.sort_values("ts_event_ns", inplace=True)
        out = self.root / f"trades_{date_str}.parquet"
        df.to_parquet(out, compression="snappy", index=False)
        logger.info(f"Wrote {len(df)} trades → {out}")
        return len(df)

    def write_depth(self, snapshots: List[OrderBookSnapshot], date_str: str) -> int:
        """Write Depth-10 snapshots to Parquet.  Returns row count."""
        if not snapshots:
            return 0

        rows: List[dict] = []
        for snap in snapshots:
            row: dict = {
                "symbol": snap.symbol,
                "venue": snap.venue,
                "ts_event_ms": snap.ts_event_ms,
                "ts_recv_ns": snap.ts_recv_ns,
            }
            for i, bid in enumerate(snap.bids[:10]):
                row[f"bid_price_{i}"] = bid.price
                row[f"bid_size_{i}"] = bid.size
            for i, ask in enumerate(snap.asks[:10]):
                row[f"ask_price_{i}"] = ask.price
                row[f"ask_size_{i}"] = ask.size
            rows.append(row)

        df = pd.DataFrame(rows)
        df.sort_values("ts_event_ms", inplace=True, na_position="first")
        out = self.root / f"depth_{date_str}.parquet"
        df.to_parquet(out, compression="snappy", index=False)
        logger.info(f"Wrote {len(df)} depth snapshots → {out}")
        return len(df)


# ===================================================================
# Symbol discovery from raw data on disk
# ===================================================================

def _discover_symbols(date_str: str) -> Dict[str, Set[str]]:
    """Return {venue: {symbol, …}} that have trade or depth data for *date_str*."""
    result: Dict[str, Set[str]] = {}
    if not DATA_ROOT.exists():
        return result
    for venue_dir in DATA_ROOT.iterdir():
        if not venue_dir.is_dir():
            continue
        venue = venue_dir.name
        syms: Set[str] = set()
        for channel_dir in venue_dir.iterdir():
            if not channel_dir.is_dir() or channel_dir.name == "exchangeinfo":
                continue
            for symbol_dir in channel_dir.iterdir():
                if symbol_dir.is_dir() and (symbol_dir / date_str).is_dir():
                    syms.add(symbol_dir.name)
        if syms:
            result[venue] = syms
    return result


# ===================================================================
# Main conversion
# ===================================================================

def convert_date(date: datetime) -> Dict:
    """Convert one day's raw data → Parquet.  Returns a report dict."""
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"Converting data for {date_str} …")

    symbols_by_venue = _discover_symbols(date_str)
    if not symbols_by_venue:
        logger.warning(f"No raw data found for {date_str}")
        return {"date": date_str, "status": "no_data"}

    writer = ParquetWriter()

    all_trades: List[TradeTick] = []
    all_depth: List[OrderBookSnapshot] = []
    ts_min: Optional[int] = None
    ts_max: Optional[int] = None
    venue_reports: Dict[str, dict] = {}

    for venue, symbols in sorted(symbols_by_venue.items()):
        v_trades = 0
        v_depth = 0

        # ---- trades (stream per symbol) ----
        for symbol in sorted(symbols):
            for rec in RawDataReader.read_files_for_date(venue, symbol, "trade", date):
                tick = TradeConverter.convert(rec)
                if tick is None:
                    continue
                all_trades.append(tick)
                v_trades += 1
                ts = tick.ts_event_ns
                if ts_min is None or ts < ts_min:
                    ts_min = ts
                if ts_max is None or ts > ts_max:
                    ts_max = ts

        # ---- depth (stream per symbol, emit 1-sec snapshots) ----
        for symbol in sorted(symbols):
            book = BookReconstructor(symbol, venue)
            last_emit_ms: Optional[int] = None
            interval_ms = int(DEPTH_SNAPSHOT_INTERVAL_SEC * 1000)

            for rec in RawDataReader.read_files_for_date(venue, symbol, "depth", date):
                snap = book.apply_delta(rec)
                ts_ms = snap.ts_event_ms
                if ts_ms == 0:
                    continue
                # emit at most one snapshot per interval
                if last_emit_ms is None or (ts_ms - last_emit_ms) >= interval_ms:
                    if snap.bids and snap.asks:
                        all_depth.append(snap)
                        v_depth += 1
                        last_emit_ms = ts_ms

        venue_reports[venue] = {
            "symbols": sorted(symbols),
            "trades": v_trades,
            "depth_snapshots": v_depth,
        }

    # ---- write Parquet ----
    n_trades = writer.write_trades(all_trades, date_str)
    n_depth = writer.write_depth(all_depth, date_str)

    # ---- report ----
    report = {
        "date": date_str,
        "timestamp": datetime.now(tz=None).isoformat(),
        "total_trades": n_trades,
        "total_depth_snapshots": n_depth,
        "venues": venue_reports,
        "ts_range": {
            "min_ns": ts_min,
            "max_ns": ts_max,
        },
        "status": "ok" if (n_trades + n_depth) > 0 else "empty",
    }

    report_file = STATE_ROOT / "convert_reports" / f"{date_str}.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"Report → {report_file}")
    return report


# ===================================================================
# CLI
# ===================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert raw Binance JSONL data to Nautilus Parquet",
    )
    ap.add_argument(
        "--date",
        type=str,
        help="Date to convert (YYYY-MM-DD). Default: yesterday.",
    )
    args = ap.parse_args()

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        date = datetime.utcnow() - timedelta(days=1)

    report = convert_date(date)
    logger.info(
        f"Done: {report.get('total_trades', 0)} trades, "
        f"{report.get('total_depth_snapshots', 0)} depth snapshots"
    )


if __name__ == "__main__":
    main()
