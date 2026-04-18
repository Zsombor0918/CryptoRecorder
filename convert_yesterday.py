"""
Nautilus-native Parquet catalog converter.

Reads raw trade and depth JSONL(.zst) files for a given UTC date,
builds Nautilus Instrument objects from exchangeInfo, converts trades
to TradeTick, reconstructs approximate L2 Depth-10 snapshots from
cryptofeed-normalised deltas, and writes everything into a
ParquetDataCatalog at NAUTILUS_CATALOG_ROOT.

Usage:
    python convert_yesterday.py                      # yesterday UTC
    python convert_yesterday.py --date 2026-04-17    # specific date
    python convert_yesterday.py --date 2026-04-17 --staging  # atomic via staging dir
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple

import zstandard as zstd

from config import (
    DATA_ROOT,
    META_ROOT,
    NAUTILUS_CATALOG_ROOT,
    STATE_ROOT,
)

# ---------------------------------------------------------------------------
# Nautilus imports
# ---------------------------------------------------------------------------
from nautilus_trader.model.data import BookOrder, OrderBookDepth10, TradeTick
from nautilus_trader.model.enums import AggressorSide, OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, TradeId, Venue
from nautilus_trader.model.instruments import CryptoPerpetual, CurrencyPair
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEPTH_SNAPSHOT_INTERVAL_SEC: float = 1.0
WRITE_BATCH_SIZE: int = 5000            # max objects per catalog.write_data()
BINANCE_VENUE = Venue("BINANCE")

# Pad BookOrder lists to exactly 10 levels
_ZERO_BID = lambda: BookOrder(side=OrderSide.BUY, price=Price.from_str("0"), size=Quantity.from_str("0"), order_id=0)
_ZERO_ASK = lambda: BookOrder(side=OrderSide.SELL, price=Price.from_str("0"), size=Quantity.from_str("0"), order_id=0)


# ===================================================================
# Helpers
# ===================================================================

def _precision_from_str(s: str) -> int:
    """Count decimal precision from a Binance filter string like '0.01000000'."""
    s = s.rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _get_filter(filters: list, filter_type: str) -> dict:
    """Find a filter dict by type from Binance exchangeInfo filters list."""
    for f in filters:
        if f.get("filterType") == filter_type:
            return f
    return {}


# ===================================================================
# Streaming raw-file reader
# ===================================================================

def stream_raw_records(
    venue: str, symbol: str, channel: str, date_str: str,
) -> Generator[dict, None, None]:
    """Yield parsed dicts from JSONL/JSONL.zst/JSONL.gz files (streaming)."""
    day_path = DATA_ROOT / venue / channel / symbol / date_str
    if not day_path.exists():
        return
    for file_path in sorted(day_path.glob("*.jsonl*")):
        try:
            if file_path.suffix == ".zst":
                opener = lambda p=file_path: zstd.open(p, "rt", errors="ignore")
            elif file_path.suffix == ".gz":
                opener = lambda p=file_path: gzip.open(p, "rt", errors="ignore")
            else:
                opener = lambda p=file_path: open(p, "r", errors="ignore")

            with opener() as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass  # counted at call site
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")


# ===================================================================
# Universe resolution (A3)
# ===================================================================

def resolve_universe(date_str: str) -> Dict[str, List[str]]:
    """Return {venue: [symbol, ...]} for the given date.

    Tries universe files first, falls back to scanning raw dirs.
    """
    result: Dict[str, List[str]] = {}
    for venue in ("BINANCE_SPOT", "BINANCE_USDTF"):
        ufile = META_ROOT / "universe" / venue / f"{date_str}.json"
        if ufile.exists():
            try:
                syms = json.loads(ufile.read_text())
                if isinstance(syms, list) and syms:
                    result[venue] = syms
                    continue
            except Exception:
                pass
        # Fallback: scan raw directories
        syms = _discover_symbols_from_disk(venue, date_str)
        if syms:
            result[venue] = sorted(syms)
    return result


def _discover_symbols_from_disk(venue: str, date_str: str) -> Set[str]:
    syms: Set[str] = set()
    venue_dir = DATA_ROOT / venue
    if not venue_dir.exists():
        return syms
    for ch_dir in venue_dir.iterdir():
        if not ch_dir.is_dir() or ch_dir.name == "exchangeinfo":
            continue
        for sym_dir in ch_dir.iterdir():
            if sym_dir.is_dir() and (sym_dir / date_str).is_dir():
                syms.add(sym_dir.name)
    return syms


# ===================================================================
# ExchangeInfo loading
# ===================================================================

def load_exchange_info(venue: str, date_str: str) -> Dict[str, dict]:
    """Load exchangeInfo and return {symbol_str: info_dict} for requested symbols."""
    info_dir = DATA_ROOT / venue / "exchangeinfo" / "EXCHANGEINFO" / date_str
    if not info_dir.exists():
        return {}

    sym_map: Dict[str, dict] = {}
    # Read the newest file for this date
    files = sorted(info_dir.glob("*.jsonl*"), reverse=True)
    for fpath in files:
        try:
            if fpath.suffix == ".zst":
                opener = lambda p=fpath: zstd.open(p, "rt", errors="ignore")
            else:
                opener = lambda p=fpath: open(p, "r", errors="ignore")
            with opener() as fh:
                for line in fh:
                    d = json.loads(line.strip())
                    for s in d.get("symbols", []):
                        sym_map[s["symbol"]] = s
                    if sym_map:
                        return sym_map
        except Exception:
            continue
    return sym_map


# ===================================================================
# Instrument building (A4)
# ===================================================================

def build_instruments(
    venue: str,
    symbols: List[str],
    exchange_info: Dict[str, dict],
) -> List:
    """Build Nautilus Instrument objects from exchangeInfo.

    Spot → CurrencyPair (BTCUSDT.BINANCE)
    Futures → CryptoPerpetual (BTCUSDT-PERP.BINANCE)
    """
    instruments = []
    is_futures = "USDTF" in venue

    for raw_sym in symbols:
        info = exchange_info.get(raw_sym)
        if info is None:
            logger.debug(f"No exchangeInfo for {raw_sym}, using defaults")
            info = _default_info(raw_sym)

        try:
            inst = _build_one_instrument(raw_sym, info, is_futures)
            instruments.append(inst)
        except Exception as e:
            logger.warning(f"Failed to build instrument for {raw_sym}: {e}")

    return instruments


def _default_info(sym: str) -> dict:
    """Minimal fallback exchangeInfo when we have none."""
    return {
        "symbol": sym,
        "baseAsset": sym.replace("USDT", ""),
        "quoteAsset": "USDT",
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.00001000", "minQty": "0.00001000"},
            {"filterType": "NOTIONAL", "minNotional": "5.00000000"},
        ],
    }


def _build_one_instrument(raw_sym: str, info: dict, is_futures: bool):
    base_str = info.get("baseAsset", raw_sym.replace("USDT", ""))
    quote_str = info.get("quoteAsset", "USDT")
    base = Currency.from_str(base_str)
    quote = Currency.from_str(quote_str)

    pf = _get_filter(info.get("filters", []), "PRICE_FILTER")
    lf = _get_filter(info.get("filters", []), "LOT_SIZE")
    nf = _get_filter(info.get("filters", []), "NOTIONAL")

    tick_size = pf.get("tickSize", "0.01000000")
    step_size = lf.get("stepSize", "0.00001000")
    min_qty = lf.get("minQty", "0.00001000")
    min_notional = nf.get("minNotional", "5.00000000")

    price_prec = _precision_from_str(tick_size)
    size_prec = _precision_from_str(step_size)

    # Ensure tick_size/step_size are trimmed to their precision
    tick_str = f"{float(tick_size):.{price_prec}f}"
    step_str = f"{float(step_size):.{size_prec}f}"
    min_qty_str = f"{float(min_qty):.{size_prec}f}"
    min_not_str = f"{float(min_notional):.{_precision_from_str(min_notional)}f}"

    if is_futures:
        iid = InstrumentId(Symbol(f"{raw_sym}-PERP"), BINANCE_VENUE)
        return CryptoPerpetual(
            instrument_id=iid,
            raw_symbol=Symbol(raw_sym),
            base_currency=base,
            quote_currency=quote,
            settlement_currency=quote,
            is_inverse=False,
            price_precision=price_prec,
            size_precision=size_prec,
            price_increment=Price.from_str(tick_str),
            size_increment=Quantity.from_str(step_str),
            multiplier=Quantity.from_str("1"),
            lot_size=None,
            max_quantity=None,
            min_quantity=Quantity.from_str(min_qty_str),
            max_notional=None,
            min_notional=Money(float(min_notional), quote),
            max_price=None,
            min_price=Price.from_str(tick_str),
            margin_init=None,
            margin_maint=None,
            maker_fee=None,
            taker_fee=None,
            ts_event=0,
            ts_init=0,
        )
    else:
        iid = InstrumentId(Symbol(raw_sym), BINANCE_VENUE)
        return CurrencyPair(
            instrument_id=iid,
            raw_symbol=Symbol(raw_sym),
            base_currency=base,
            quote_currency=quote,
            price_precision=price_prec,
            size_precision=size_prec,
            price_increment=Price.from_str(tick_str),
            size_increment=Quantity.from_str(step_str),
            lot_size=None,
            max_quantity=None,
            min_quantity=Quantity.from_str(min_qty_str),
            max_notional=None,
            min_notional=Money(float(min_notional), quote),
            max_price=None,
            min_price=Price.from_str(tick_str),
            margin_init=None,
            margin_maint=None,
            maker_fee=None,
            taker_fee=None,
            ts_event=0,
            ts_init=0,
        )


def instrument_id_for(raw_sym: str, venue: str) -> InstrumentId:
    """Resolve InstrumentId from raw Binance symbol + venue tag."""
    if "USDTF" in venue:
        return InstrumentId(Symbol(f"{raw_sym}-PERP"), BINANCE_VENUE)
    return InstrumentId(Symbol(raw_sym), BINANCE_VENUE)


# ===================================================================
# Trade conversion (A5)
# ===================================================================

def convert_trades(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[TradeTick], int]:
    """Stream-convert raw trade records to Nautilus TradeTick.

    Returns (tick_list, bad_line_count).  tick_list may be large;
    caller should batch-write.
    """
    ticks: List[TradeTick] = []
    bad = 0

    for rec in stream_raw_records(venue, symbol, "trade", date_str):
        try:
            payload = rec.get("payload", {})
            price_str = payload.get("price", "0")
            qty_str = payload.get("quantity", "0")
            side_raw = payload.get("side", "buy")
            trade_id_raw = payload.get("trade_id")

            ts_event_ms = rec.get("ts_event_ms")
            ts_recv_ns = rec.get("ts_recv_ns", 0)
            ts_event = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            ts_init = ts_recv_ns

            aggressor = AggressorSide.BUYER if side_raw == "buy" else AggressorSide.SELLER
            tid = TradeId(str(trade_id_raw)) if trade_id_raw else TradeId("0")

            tick = TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(price_str),
                size=Quantity.from_str(qty_str),
                aggressor_side=aggressor,
                trade_id=tid,
                ts_event=ts_event,
                ts_init=ts_init,
            )
            ticks.append(tick)
        except Exception:
            bad += 1

    return ticks, bad


# ===================================================================
# Depth-10 reconstruction (A6)
# ===================================================================

class BookReconstructor:
    """In-memory L2 book from delta updates → OrderBookDepth10 snapshots."""

    def __init__(self, instrument_id: InstrumentId, price_prec: int, size_prec: int):
        self.instrument_id = instrument_id
        self.price_prec = price_prec
        self.size_prec = size_prec
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    def apply_delta(self, rec: dict) -> None:
        payload = rec.get("payload", {})
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

    def snapshot(self, ts_event: int, ts_init: int) -> Optional[OrderBookDepth10]:
        """Build an OrderBookDepth10 from current state.

        Returns None if we don't have both bids and asks.
        """
        if not self.bids or not self.asks:
            return None

        top_bids = sorted(self.bids.items(), key=lambda x: -x[0])[:10]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:10]

        bid_orders = [
            BookOrder(
                side=OrderSide.BUY,
                price=Price.from_str(f"{p:.{self.price_prec}f}"),
                size=Quantity.from_str(f"{s:.{self.size_prec}f}"),
                order_id=0,
            )
            for p, s in top_bids
        ]
        ask_orders = [
            BookOrder(
                side=OrderSide.SELL,
                price=Price.from_str(f"{p:.{self.price_prec}f}"),
                size=Quantity.from_str(f"{s:.{self.size_prec}f}"),
                order_id=0,
            )
            for p, s in top_asks
        ]

        # Pad to exactly 10
        while len(bid_orders) < 10:
            bid_orders.append(_ZERO_BID())
        while len(ask_orders) < 10:
            ask_orders.append(_ZERO_ASK())

        return OrderBookDepth10(
            instrument_id=self.instrument_id,
            bids=bid_orders,
            asks=ask_orders,
            bid_counts=[1] * 10,
            ask_counts=[1] * 10,
            flags=0,
            sequence=0,
            ts_event=ts_event,
            ts_init=ts_init,
        )


def convert_depth(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[OrderBookDepth10], int]:
    """Stream-convert raw depth deltas → 1-second OrderBookDepth10 snapshots."""
    book = BookReconstructor(instrument_id, price_prec, size_prec)
    snapshots: List[OrderBookDepth10] = []
    bad = 0
    interval_ns = int(DEPTH_SNAPSHOT_INTERVAL_SEC * 1e9)
    last_emit_ns: Optional[int] = None

    for rec in stream_raw_records(venue, symbol, "depth", date_str):
        try:
            book.apply_delta(rec)
            ts_event_ms = rec.get("ts_event_ms")
            ts_recv_ns = rec.get("ts_recv_ns", 0)
            ts_event = ts_event_ms * 1_000_000 if ts_event_ms else ts_recv_ns
            ts_init = ts_recv_ns

            if ts_event == 0:
                continue

            if last_emit_ns is None or (ts_event - last_emit_ns) >= interval_ns:
                snap = book.snapshot(ts_event, ts_init)
                if snap is not None:
                    snapshots.append(snap)
                    last_emit_ns = ts_event
        except Exception:
            bad += 1

    return snapshots, bad


# ===================================================================
# Catalog housekeeping (idempotency)
# ===================================================================

def _purge_catalog_data(catalog_root: Path, instruments) -> None:
    """Remove existing Parquet files for the given instruments.

    This allows re-running the converter for the same date without
    'non-disjoint intervals' errors from the Nautilus catalog writer.
    """
    data_dir = catalog_root / "data"
    if not data_dir.exists():
        return

    purged = 0
    for inst in instruments:
        iid_str = str(inst.id)
        # Data dirs: trade_tick/{IID}/, order_book_depths/{IID}/
        for subdir in ("trade_tick", "order_book_depths"):
            inst_dir = data_dir / subdir / iid_str
            if inst_dir.exists():
                for pf in inst_dir.glob("*.parquet"):
                    pf.unlink()
                    purged += 1

    # Purge all instrument metadata parquets (cheap to regenerate)
    # Nautilus stores these in data/{class_name}/{IID}/ (e.g. crypto_perpetual/, currency_pair/)
    for cls_name in ("crypto_perpetual", "currency_pair"):
        cls_dir = data_dir / cls_name
        if cls_dir.exists():
            for iid_dir in cls_dir.iterdir():
                if iid_dir.is_dir():
                    for pf in iid_dir.glob("*.parquet"):
                        pf.unlink()
                        purged += 1

    if purged:
        logger.info(f"Purged {purged} existing Parquet files for idempotent re-write")


# ===================================================================
# Main conversion logic (A1, A7)
# ===================================================================

def convert_date(
    date: datetime,
    catalog_root: Optional[Path] = None,
    staging: bool = False,
) -> Dict:
    """Convert one UTC day's raw data → Nautilus ParquetDataCatalog.

    If staging=True, writes to a temp dir first, then atomically
    replaces the real catalog on success.
    """
    t0 = time.time()
    date_str = date.strftime("%Y-%m-%d")
    logger.info(f"Converting data for {date_str} …")

    target_root = catalog_root or NAUTILUS_CATALOG_ROOT
    if staging:
        staging_dir = Path(str(target_root) + f".staging.{os.getpid()}")
        work_root = staging_dir
    else:
        work_root = target_root

    work_root.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(work_root))

    # ── universe ──
    universe = resolve_universe(date_str)
    if not universe:
        logger.warning(f"No raw data found for {date_str}")
        return _make_report(date_str, t0, status="no_data")

    # ── exchangeInfo ──
    einfo_spot = load_exchange_info("BINANCE_SPOT", date_str)
    einfo_fut = load_exchange_info("BINANCE_USDTF", date_str)

    # ── build instruments ──
    all_instruments = []
    for venue, syms in universe.items():
        einfo = einfo_fut if "USDTF" in venue else einfo_spot
        insts = build_instruments(venue, syms, einfo)
        all_instruments.extend(insts)

    # ── purge existing catalog data for these instruments (idempotency) ──
    if not staging:
        _purge_catalog_data(work_root, all_instruments)

    if all_instruments:
        catalog.write_data(all_instruments)
        logger.info(f"Wrote {len(all_instruments)} instruments")

    # Build lookup: (venue, raw_sym) → (instrument_id, price_prec, size_prec)
    inst_map: Dict[Tuple[str, str], Tuple[InstrumentId, int, int]] = {}
    for inst in all_instruments:
        raw = str(inst.raw_symbol)
        if isinstance(inst, CryptoPerpetual):
            venue_tag = "BINANCE_USDTF"
        else:
            venue_tag = "BINANCE_SPOT"
        inst_map[(venue_tag, raw)] = (inst.id, inst.price_precision, inst.size_precision)

    # ── convert per venue/symbol ──
    total_trades = 0
    total_depth = 0
    total_bad = 0
    venue_reports: Dict[str, dict] = {}
    ts_min: Optional[int] = None
    ts_max: Optional[int] = None
    has_futures = "BINANCE_USDTF" in universe

    for venue, symbols in sorted(universe.items()):
        v_trades = 0
        v_depth = 0

        for symbol in sorted(symbols):
            key = (venue, symbol)
            if key not in inst_map:
                logger.warning(f"No instrument for {venue}/{symbol}, skipping")
                continue
            iid, pp, sp = inst_map[key]

            # ── trades ──
            ticks, bad_t = convert_trades(venue, symbol, date_str, iid, pp, sp)
            total_bad += bad_t
            if ticks:
                # Catalog requires monotonic ts_init; sort before writing
                ticks.sort(key=lambda t: t.ts_init)
                for i in range(0, len(ticks), WRITE_BATCH_SIZE):
                    catalog.write_data(ticks[i : i + WRITE_BATCH_SIZE])
                v_trades += len(ticks)
                # Track timestamps
                first_ts = ticks[0].ts_event
                last_ts = ticks[-1].ts_event
                if ts_min is None or first_ts < ts_min:
                    ts_min = first_ts
                if ts_max is None or last_ts > ts_max:
                    ts_max = last_ts

            # ── depth ──
            snaps, bad_d = convert_depth(venue, symbol, date_str, iid, pp, sp)
            total_bad += bad_d
            if snaps:
                snaps.sort(key=lambda s: s.ts_init)
                for i in range(0, len(snaps), WRITE_BATCH_SIZE):
                    catalog.write_data(snaps[i : i + WRITE_BATCH_SIZE])
                v_depth += len(snaps)
                first_ts = snaps[0].ts_event
                last_ts = snaps[-1].ts_event
                if ts_min is None or first_ts < ts_min:
                    ts_min = first_ts
                if ts_max is None or last_ts > ts_max:
                    ts_max = last_ts

        total_trades += v_trades
        total_depth += v_depth
        venue_reports[venue] = {
            "symbols": sorted(symbols),
            "trades_written": v_trades,
            "depth_snapshots_written": v_depth,
        }

    # ── staging → atomic rename ──
    if staging and total_trades + total_depth > 0:
        if target_root.exists():
            backup = Path(str(target_root) + ".bak")
            if backup.exists():
                shutil.rmtree(backup)
            target_root.rename(backup)
        staging_dir.rename(target_root)
        logger.info(f"Staging → {target_root} (atomic rename)")
    elif staging:
        # No data written, clean up staging
        if staging_dir.exists():
            shutil.rmtree(staging_dir)

    # ── report ──
    elapsed = time.time() - t0
    report = {
        "date": date_str,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "runtime_seconds": round(elapsed, 2),
        "instruments_written": len(all_instruments),
        "total_trades_written": total_trades,
        "total_depth_snapshots_written": total_depth,
        "bad_lines": total_bad,
        "futures_enabled": has_futures,
        "venues": venue_reports,
        "ts_range": {"min_ns": ts_min, "max_ns": ts_max},
        "catalog_root": str(target_root),
        "status": "ok" if (total_trades + total_depth) > 0 else "empty",
    }

    report_file = STATE_ROOT / "convert_reports" / f"{date_str}.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(json.dumps(report, indent=2, default=str))
    logger.info(f"Report → {report_file}")

    logger.info(
        f"Done: {total_trades} trades, {total_depth} depth snapshots, "
        f"{len(all_instruments)} instruments, {total_bad} bad lines in {elapsed:.1f}s"
    )
    return report


def _make_report(date_str: str, t0: float, **kwargs) -> dict:
    report = {"date": date_str, "runtime_seconds": round(time.time() - t0, 2), **kwargs}
    rp = STATE_ROOT / "convert_reports" / f"{date_str}.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(report, indent=2, default=str))
    return report


# ===================================================================
# CLI (A1)
# ===================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Convert raw Binance JSONL → Nautilus ParquetDataCatalog",
    )
    ap.add_argument(
        "--date", type=str,
        help="Date to convert (YYYY-MM-DD). Default: yesterday UTC.",
    )
    ap.add_argument(
        "--staging", action="store_true",
        help="Write to staging dir, then atomically rename on success.",
    )
    args = ap.parse_args()

    if args.date:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        date = datetime.now(tz=timezone.utc) - timedelta(days=1)

    convert_date(date, staging=args.staging)


if __name__ == "__main__":
    main()
