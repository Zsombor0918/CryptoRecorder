"""
pipeline.build_replay_store — Daily replay store builder from raw data.

Converts raw JSONL.zst data to normalized deterministic Parquet replay_store.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from config import DATA_ROOT, REPLAY_ROOT
from converter.readers import stream_raw_records
from stores.replay_writer import ReplayWriter

logger = logging.getLogger(__name__)

# Suppress verbose library logs
logging.getLogger("pyarrow").setLevel(logging.WARNING)


def _to_ns_from_ms(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value) * 1_000_000
    except (TypeError, ValueError):
        return None


def _event_ts_ns(raw_record: dict) -> int:
    return (
        _to_ns_from_ms(raw_record.get("ts_event_ms"))
        or _to_ns_from_ms(raw_record.get("exchange_ts_ms"))
        or _to_ns_from_ms(raw_record.get("ts_trade_ms"))
        or int(raw_record.get("ts_exchange_ns") or raw_record.get("ts_recv_ns") or 0)
    )


def _trade_event_ts_ns(raw_record: dict) -> int:
    return (
        _to_ns_from_ms(raw_record.get("ts_trade_ms"))
        or _to_ns_from_ms(raw_record.get("ts_event_ms"))
        or _to_ns_from_ms(raw_record.get("exchange_ts_ms"))
        or int(raw_record.get("ts_exchange_ns") or raw_record.get("ts_recv_ns") or 0)
    )


def _receive_ts_ns(raw_record: dict) -> int:
    return int(
        raw_record.get("ts_receive_ns")
        or raw_record.get("ts_recv_ns")
        or _event_ts_ns(raw_record)
    )


def _native_payload_hash(raw_record: dict) -> str | None:
    payload = raw_record.get("native_payload") or raw_record.get("payload")
    if payload is None:
        return None
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _as_optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _decimal_pair_to_level(level: object) -> dict:
    price = level[0]  # type: ignore[index]
    size = level[1]  # type: ignore[index]
    price_str = str(price)
    size_str = str(size)
    return {
        "price": float(price_str),
        "size": float(size_str),
        "price_str": price_str,
        "size_str": size_str,
    }


def _convert_depth_record(raw_record: dict, venue: str, symbol: str, date: str) -> Optional[dict]:
    """
    Convert raw depth record to replay schema.
    
    Raw schema expected:
        {
            "stream_session_id": uint64,
            "session_seq": uint64,
            "raw_index": uint32,
            "snapshot_seed": {...},  or
            "depth_update": {...},
            ...
        }
    """
    try:
        record_type = raw_record.get("record_type", "depth_update")
        if record_type not in {"snapshot_seed", "depth_update"}:
            return None

        session_id = raw_record.get("stream_session_id", 0)
        session_seq = raw_record.get("session_seq", 0)
        raw_index = raw_record.get("raw_index", 0)

        payload = raw_record.get("payload") or {}
        bids = raw_record.get("bids", payload.get("bids", []))
        asks = raw_record.get("asks", payload.get("asks", []))

        # Parse bids/asks if they're strings (JSON-encoded)
        if isinstance(bids, str):
            bids = json.loads(bids)
        if isinstance(asks, str):
            asks = json.loads(asks)

        bids_struct = [_decimal_pair_to_level(b) for b in bids]
        asks_struct = [_decimal_pair_to_level(a) for a in asks]

        # Determine flags
        is_snapshot = record_type == "snapshot_seed"
        is_update = record_type == "depth_update"
        sync_state = raw_record.get("sync_state")
        is_sync_state = record_type == "sync_state"
        is_desync = bool(raw_record.get("is_desync", False) or sync_state == "desynced")
        is_resync = bool(raw_record.get("is_resync", False) or sync_state == "resync_required")

        # Quality flags (JSON-encoded)
        quality_flags = raw_record.get("quality_flags")
        if quality_flags and isinstance(quality_flags, dict):
            quality_flags = json.dumps(quality_flags)

        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "stream_session_id": session_id,
            "session_seq": session_seq,
            "raw_index": raw_index,
            "record_type": record_type,
            "U": _as_optional_str(raw_record.get("U")),
            "u": _as_optional_str(raw_record.get("u") or raw_record.get("lastUpdateId")),
            "pu": _as_optional_str(raw_record.get("pu")),
            "ts_exchange_ns": _event_ts_ns(raw_record),
            "ts_receive_ns": _receive_ts_ns(raw_record),
            "bids": bids_struct,
            "asks": asks_struct,
            "is_snapshot_seed": is_snapshot,
            "is_depth_update": is_update,
            "is_sync_state": is_sync_state,
            "is_desync": is_desync,
            "is_resync": is_resync,
            "quality_flags": quality_flags,
            "native_payload_hash": raw_record.get("native_payload_hash") or _native_payload_hash(raw_record),
        }
    except Exception as e:
        logger.warning(f"Error converting depth record for {venue}/{symbol}: {e}")
        return None


def _convert_trade_record(raw_record: dict, venue: str, symbol: str, date: str) -> Optional[dict]:
    """
    Convert raw trade record to replay schema.
    
    Raw schema expected:
        {
            "trade_stream_session_id": uint64,
            "trade_session_seq": uint64,
            "raw_index": uint32,
            "market_type": "spot" or "futures",
            "trade_id": str,  or
            "agg_trade_id": str,
            ...
        }
    """
    try:
        session_id = raw_record.get("trade_stream_session_id", 0)
        session_seq = raw_record.get("trade_session_seq", 0)
        raw_index = raw_record.get("raw_index", 0)
        market_type = raw_record.get("market_type", "spot")
        record_type = raw_record.get("record_type", "trade")
        if record_type not in {"trade", "agg_trade"}:
            return None

        # Trade IDs
        trade_id = raw_record.get("trade_id") or raw_record.get("exchange_trade_id")
        agg_trade_id = raw_record.get("agg_trade_id")

        # Trade details
        price_str = str(raw_record.get("price", "0"))
        quantity_str = str(raw_record.get("quantity", "0"))
        price = float(price_str)
        quantity = float(quantity_str)
        buyer_maker = bool(raw_record.get("is_buyer_maker", raw_record.get("buyer_maker", False)))
        aggressor_side = raw_record.get("aggressor_side")

        # Quality flags
        quality_flags = raw_record.get("quality_flags")
        if quality_flags and isinstance(quality_flags, dict):
            quality_flags = json.dumps(quality_flags)

        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "trade_stream_session_id": session_id,
            "trade_session_seq": session_seq,
            "raw_index": raw_index,
            "record_type": record_type,
            "market_type": market_type,
            "trade_id": _as_optional_str(trade_id),
            "agg_trade_id": _as_optional_str(agg_trade_id),
            "ts_exchange_ns": _trade_event_ts_ns(raw_record),
            "ts_receive_ns": _receive_ts_ns(raw_record),
            "price": price,
            "quantity": quantity,
            "price_str": price_str,
            "quantity_str": quantity_str,
            "buyer_maker": buyer_maker,
            "aggressor_side": aggressor_side,
            "quality_flags": quality_flags,
            "native_payload_hash": raw_record.get("native_payload_hash") or _native_payload_hash(raw_record),
        }
    except Exception as e:
        logger.warning(f"Error converting trade record for {venue}/{symbol}: {e}")
        return None


def build_replay_for_symbol(
    venue: str,
    symbol: str,
    date: str,
    data_root: Path,
    replay_root: Path,
) -> dict:
    """
    Build replay store for a single venue/symbol/date.

    Returns:
        Status dict with counts and errors.
    """
    status = {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "status": "success",
        "depth_count": 0,
        "trade_count": 0,
        "errors": [],
    }

    writer = ReplayWriter(replay_root, venue, symbol, date)

    try:
        # Stream depth records
        depth_batch = []
        for raw_index, raw_record in enumerate(stream_raw_records(
            venue, symbol, "depth_v2", date, root=data_root
        )):
            raw_record = dict(raw_record)
            raw_record.setdefault("raw_index", raw_index)
            converted = _convert_depth_record(raw_record, venue, symbol, date)
            if converted:
                depth_batch.append(converted)
                if len(depth_batch) >= 5000:
                    writer.write_depth_batch(depth_batch)
                    depth_batch = []
        if depth_batch:
            writer.write_depth_batch(depth_batch)

        # Stream trade records
        trade_batch = []
        for raw_index, raw_record in enumerate(stream_raw_records(
            venue, symbol, "trade_v2", date, root=data_root
        )):
            raw_record = dict(raw_record)
            raw_record.setdefault("raw_index", raw_index)
            converted = _convert_trade_record(raw_record, venue, symbol, date)
            if converted:
                trade_batch.append(converted)
                if len(trade_batch) >= 5000:
                    writer.write_trades_batch(trade_batch)
                    trade_batch = []
        if trade_batch:
            writer.write_trades_batch(trade_batch)

        # Load instrument metadata if available
        instrument_metadata = None
        try:
            exchangeinfo_records = list(
                stream_raw_records(venue, "EXCHANGEINFO", "exchangeinfo", date, root=data_root)
            )
            if exchangeinfo_records:
                last_record = exchangeinfo_records[-1]
                symbol_info = None
                for item in last_record.get("symbols", []):
                    if item.get("symbol") == symbol:
                        symbol_info = item
                        break
                symbol_info = symbol_info or {}
                instrument_metadata = {
                    "venue": venue,
                    "symbol": symbol,
                    "market_type": "spot" if "BINANCE_SPOT" in venue else "perpetual",
                    "instrument_id": (
                        f"{symbol}.BINANCE"
                        if "BINANCE_SPOT" in venue
                        else f"{symbol}-PERP.BINANCE"
                    ),
                    "raw_symbol": symbol,
                    "quote_asset": symbol_info.get("quoteAsset", "USDT"),
                    "base_asset": symbol_info.get("baseAsset", symbol.replace("USDT", "")),
                }
        except Exception as e:
            logger.warning(f"Could not load instrument metadata for {venue}/{symbol}: {e}")

        # Finalize and publish
        writer.finalize_staging()
        writer.publish(instrument_metadata)

        status["depth_count"] = writer.depth_count
        status["trade_count"] = writer.trade_count

        logger.info(
            f"✓ Built replay: {venue}/{symbol}/{date} "
            f"({writer.depth_count} depth, {writer.trade_count} trades)"
        )

    except Exception as e:
        status["status"] = "failed"
        status["errors"].append(str(e))
        logger.error(f"✗ Failed to build replay for {venue}/{symbol}/{date}: {e}")

    return status


def main():
    """CLI entry point for build_replay_store."""
    parser = argparse.ArgumentParser(
        description="Build replay_store from raw JSONL.zst data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m pipeline.build_replay_store --date 2026-06-15
  python -m pipeline.build_replay_store --date 2026-06-15 --symbols BTCUSDT,ETHUSDT
  python -m pipeline.build_replay_store --date 2026-06-15 --symbols all --data-root /path/to/raw --replay-root /path/to/replay
        """,
    )
    parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
    parser.add_argument(
        "--symbols",
        default="all",
        help="Comma-separated symbols or 'all' (default: all)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help=f"Data root (default: {DATA_ROOT})",
    )
    parser.add_argument(
        "--replay-root",
        type=Path,
        default=None,
        help=f"Replay root (default: {REPLAY_ROOT})",
    )
    args = parser.parse_args()

    data_root = args.data_root or DATA_ROOT
    replay_root = args.replay_root or REPLAY_ROOT

    date_str = args.date

    # Parse symbols
    if args.symbols.lower() == "all":
        from pipeline.raw_manifest import scan_raw_coverage
        coverage = scan_raw_coverage(date_str, data_root)
        all_symbols = set()
        for venue_data in coverage["data"].values():
            all_symbols.update(venue_data.keys())
        symbols_to_build = sorted(all_symbols)
    else:
        symbols_to_build = [s.strip().upper() for s in args.symbols.split(",")]

    # Discover venues from raw data
    from pipeline.raw_manifest import scan_raw_coverage
    coverage = scan_raw_coverage(date_str, data_root)
    venues = coverage["venues"]

    if not venues:
        logger.error(f"No raw data found for {date_str}")
        sys.exit(1)

    # Build replay for each venue/symbol combination
    results = []
    for venue in venues:
        for symbol in symbols_to_build:
            if symbol in coverage["data"].get(venue, {}):
                result = build_replay_for_symbol(venue, symbol, date_str, data_root, replay_root)
                results.append(result)

    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    total_depth = sum(r.get("depth_count", 0) for r in results)
    total_trades = sum(r.get("trade_count", 0) for r in results)

    logger.info(
        f"Replay build complete: {successful} successful, {failed} failed, "
        f"{total_depth} depth records, {total_trades} trade records"
    )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    sys.exit(main())
