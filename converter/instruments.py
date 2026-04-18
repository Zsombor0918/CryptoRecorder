"""
converter.instruments — Build Nautilus Instrument objects from Binance exchangeInfo.

Spot  → CurrencyPair   (e.g. BTCUSDT.BINANCE)
Futures → CryptoPerpetual (e.g. BTCUSDT-PERP.BINANCE)
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

import zstandard as zstd

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual, CurrencyPair
from nautilus_trader.model.objects import Currency, Money, Price, Quantity

from config import DATA_ROOT

logger = logging.getLogger(__name__)

BINANCE_VENUE = Venue("BINANCE")


# ── helpers ──────────────────────────────────────────────────────────

def _precision_from_str(s: str) -> int:
    """Count decimal precision from a Binance filter string like '0.01000000'."""
    s = s.rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _get_filter(filters: list, filter_type: str) -> dict:
    for f in filters:
        if f.get("filterType") == filter_type:
            return f
    return {}


# ── exchangeInfo loading ─────────────────────────────────────────────

def load_exchange_info(venue: str, date_str: str) -> Dict[str, dict]:
    """Load exchangeInfo and return ``{symbol_str: info_dict}``."""
    info_dir = DATA_ROOT / venue / "exchangeinfo" / "EXCHANGEINFO" / date_str
    if not info_dir.exists():
        return {}

    sym_map: Dict[str, dict] = {}
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


# ── instrument ID helpers ────────────────────────────────────────────

def instrument_id_for(raw_sym: str, venue: str) -> InstrumentId:
    """Resolve InstrumentId from raw Binance symbol + venue tag."""
    if "USDTF" in venue:
        return InstrumentId(Symbol(f"{raw_sym}-PERP"), BINANCE_VENUE)
    return InstrumentId(Symbol(raw_sym), BINANCE_VENUE)


# ── build instruments ────────────────────────────────────────────────

def build_instruments(
    venue: str,
    symbols: List[str],
    exchange_info: Dict[str, dict],
) -> List:
    """Build Nautilus Instrument objects.

    Returns list of CurrencyPair / CryptoPerpetual.
    """
    instruments = []
    is_futures = "USDTF" in venue

    for raw_sym in symbols:
        info = exchange_info.get(raw_sym)
        if info is None:
            logger.debug(f"No exchangeInfo for {raw_sym}, using defaults")
            info = _default_info(raw_sym)

        try:
            inst = _build_one(raw_sym, info, is_futures)
            instruments.append(inst)
        except Exception as e:
            logger.warning(f"Failed to build instrument for {raw_sym}: {e}")

    return instruments


def _default_info(sym: str) -> dict:
    """Minimal fallback exchangeInfo."""
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


def _build_one(raw_sym: str, info: dict, is_futures: bool):
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
