"""
converter.trades — Convert raw ``trade_v2`` JSONL records → Nautilus TradeTick.

Records are sorted by ``(trade_stream_session_id, trade_session_seq)`` so
replay determinism depends only on committed canonical order, never on file
timestamp coincidence.

Exchange trade IDs are preserved as diagnostic ``TradeId`` metadata but do
not affect ordering.
"""
from __future__ import annotations

from decimal import Decimal
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity

from converter.readers import stream_raw_records
from converter.spool import RawRecordSpool

logger = logging.getLogger(__name__)


def _trade_sort_key(rec: dict) -> Tuple[int, int]:
    """Sort by committed canonical trade order."""
    return (
        int(rec.get("trade_stream_session_id", 0)),
        int(rec.get("trade_session_seq", 0)),
    )


def _native_payload(rec: dict) -> dict:
    payload = rec.get("native_payload")
    return payload if isinstance(payload, dict) else {}


def _as_decimal(value: object, *, field: str) -> Decimal:
    if value is None:
        raise ValueError(f"missing trade {field}")
    return Decimal(str(value))


def _trade_id_for_report(rec: dict) -> object:
    native = _native_payload(rec)
    trade_id = rec.get("exchange_trade_id")
    return trade_id if trade_id is not None else native.get("t")


def _first_trade_id_for_report(rec: dict) -> object:
    native = _native_payload(rec)
    trade_id = rec.get("first_trade_id")
    return trade_id if trade_id is not None else native.get("f")


def _last_trade_id_for_report(rec: dict) -> object:
    native = _native_payload(rec)
    trade_id = rec.get("last_trade_id")
    return trade_id if trade_id is not None else native.get("l")


def _trade_report_example(
    *,
    venue: str,
    symbol: str,
    rec: dict,
) -> Dict[str, object]:
    return {
        "venue": venue,
        "symbol": rec.get("symbol", symbol),
        "ts_event_ms": rec.get("ts_event_ms"),
        "price": rec.get("price"),
        "quantity": rec.get("quantity"),
        "exchange_trade_id": _trade_id_for_report(rec),
        "first_trade_id": _first_trade_id_for_report(rec),
        "last_trade_id": _last_trade_id_for_report(rec),
    }


DEFAULT_CONVERTER_BATCH_SIZE = 5000


def convert_trades(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[TradeTick], int, Optional[int], Optional[int]]:
    """Stream-convert raw trade_v2 records to Nautilus TradeTick.

    Returns ``(tick_list, bad_line_count, first_ts_ns, last_ts_ns)``.
    """
    ticks, bad, first_ts, last_ts, _ = convert_trades_with_diagnostics(
        venue,
        symbol,
        date_str,
        instrument_id,
        price_prec,
        size_prec,
    )
    return ticks, bad, first_ts, last_ts


def convert_trades_with_diagnostics(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
) -> Tuple[List[TradeTick], int, Optional[int], Optional[int], Dict[str, Any]]:
    """Stream-convert raw trade_v2 records and return diagnostics.

    Returns ``(tick_list, bad_line_count, first_ts_ns, last_ts_ns, diagnostics)``
    where diagnostics includes:
      - ``raw_record_count``: all parsed JSON records from trade_v2 files
      - ``raw_trade_record_count``: parsed records with record_type == "trade"
      - ``raw_lifecycle_record_count``: parsed lifecycle records
      - ``ticks_written``: number of TradeTick objects produced
      - ``zero_size_trade_skipped``: trade records intentionally skipped
        before Nautilus object construction because raw quantity is zero
      - ``bad_lines_by_exception_type``: breakdown of exceptions by type
      - ``bad_lines_by_record_type``: breakdown of bad lines by record type
      - ``bad_line_examples``: first 20 examples with details
    """
    ticks: List[TradeTick] = []

    def collect(batch: List[TradeTick]) -> None:
        ticks.extend(batch)

    bad, first_ts, last_ts, diagnostics = convert_trades_streaming(
        venue,
        symbol,
        date_str,
        instrument_id,
        price_prec,
        size_prec,
        on_ticks_batch=collect,
        batch_size=DEFAULT_CONVERTER_BATCH_SIZE,
    )
    return ticks, bad, first_ts, last_ts, diagnostics


def convert_trades_streaming(
    venue: str,
    symbol: str,
    date_str: str,
    instrument_id: InstrumentId,
    price_prec: int,
    size_prec: int,
    *,
    on_ticks_batch: Callable[[List[TradeTick]], None],
    batch_size: int = DEFAULT_CONVERTER_BATCH_SIZE,
    temp_dir: str | None = None,
) -> Tuple[int, Optional[int], Optional[int], Dict[str, Any]]:
    """Convert raw trade_v2 records and emit bounded TradeTick batches.

    Returns ``(bad_line_count, first_ts_ns, last_ts_ns, diagnostics)``.
    """
    batch_size = max(1, int(batch_size))
    bad = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    bad_lines_by_exception_type: Dict[str, int] = {}
    bad_lines_by_record_type: Dict[str, int] = {}
    bad_line_examples: List[Dict[str, Any]] = []
    zero_size_trade_skipped = 0
    zero_size_trade_examples: List[Dict[str, Any]] = []
    raw_trade_record_count = 0
    raw_lifecycle_record_count = 0
    ticks_written = 0
    batch: List[TradeTick] = []

    def flush_batch() -> None:
        if not batch:
            return
        on_ticks_batch(list(batch))
        batch.clear()

    with RawRecordSpool(temp_dir=temp_dir, prefix="cryptorecorder-trades-") as spool:
        for raw_index, rec in enumerate(stream_raw_records(venue, symbol, "trade_v2", date_str)):
            item = dict(rec)
            item["record_type"] = item.get("record_type", "trade")
            sort1, sort2 = _trade_sort_key(item)
            spool.insert(item, (sort1, sort2, raw_index), raw_index)
        spool.commit()

        for rec in spool.iter_records():
            try:
                record_type = rec.get("record_type", "trade")
                # Skip lifecycle markers — they are metadata, not trade ticks
                if record_type != "trade":
                    if record_type == "trade_stream_lifecycle":
                        raw_lifecycle_record_count += 1
                    continue
                raw_trade_record_count += 1

                price_str = rec.get("price")
                qty_str = rec.get("quantity")
                is_buyer_maker = rec.get("is_buyer_maker", False)
                exchange_trade_id = _trade_id_for_report(rec)

                qty = _as_decimal(qty_str, field="quantity")
                if qty == 0:
                    zero_size_trade_skipped += 1
                    if len(zero_size_trade_examples) < 20:
                        zero_size_trade_examples.append(
                            _trade_report_example(venue=venue, symbol=symbol, rec=rec)
                        )
                    continue

                ts_event_ms = rec.get("ts_event_ms")
                ts_trade_ms = rec.get("ts_trade_ms")
                ts_recv_ns = rec.get("ts_recv_ns", 0)

                # Prefer trade time, then event time, then recv time
                if ts_trade_ms:
                    ts_event = int(ts_trade_ms) * 1_000_000
                elif ts_event_ms:
                    ts_event = int(ts_event_ms) * 1_000_000
                else:
                    ts_event = int(ts_recv_ns)
                ts_init = int(ts_recv_ns)

                # is_buyer_maker=True means the buyer was the maker,
                # so the taker (aggressor) is the seller
                aggressor = (
                    AggressorSide.SELLER if is_buyer_maker else AggressorSide.BUYER
                )
                tid = TradeId(str(exchange_trade_id)) if exchange_trade_id else TradeId("0")

                tick = TradeTick(
                    instrument_id=instrument_id,
                    price=Price.from_str(str(price_str)),
                    size=Quantity.from_str(str(qty_str)),
                    aggressor_side=aggressor,
                    trade_id=tid,
                    ts_event=ts_event,
                    ts_init=ts_init,
                )
                batch.append(tick)
                ticks_written += 1
                if len(batch) >= batch_size:
                    flush_batch()

                if first_ts is None:
                    first_ts = ts_event
                last_ts = ts_event
            except Exception as exc:
                bad += 1
                exc_type = type(exc).__name__
                record_type = (
                    rec.get("record_type", "unknown") if "rec" in locals() else "unknown"
                )

                # Track counts by exception type and record type
                bad_lines_by_exception_type[exc_type] = (
                    bad_lines_by_exception_type.get(exc_type, 0) + 1
                )
                bad_lines_by_record_type[record_type] = (
                    bad_lines_by_record_type.get(record_type, 0) + 1
                )

                # Keep first 20 examples (compact format)
                if len(bad_line_examples) < 20 and "rec" in locals():
                    example = _trade_report_example(venue=venue, symbol=symbol, rec=rec)
                    example.update(
                        {
                            "record_type": record_type,
                            "ts_trade_ms": rec.get("ts_trade_ms"),
                            "ts_recv_ns": rec.get("ts_recv_ns"),
                            "exception_type": exc_type,
                            "exception_message": str(exc)[:100],
                            "record_keys": list(rec.keys()) if isinstance(rec, dict) else [],
                        }
                    )
                    bad_line_examples.append(example)

        flush_batch()

    diagnostics: Dict[str, Any] = {
        "raw_record_count": spool.count,
        "raw_trade_record_count": raw_trade_record_count,
        "raw_lifecycle_record_count": raw_lifecycle_record_count,
        "ticks_written": ticks_written,
        "zero_size_trade_skipped": zero_size_trade_skipped,
        "zero_size_trade_examples": zero_size_trade_examples,
        "bad_lines_by_exception_type": bad_lines_by_exception_type,
        "bad_lines_by_record_type": bad_lines_by_record_type,
        "bad_line_examples": bad_line_examples,
    }
    return bad, first_ts, last_ts, diagnostics
