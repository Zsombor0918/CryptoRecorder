"""Audit replay_store partitions for counts, checksums, ordering, and precision fields."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from config import REPLAY_ROOT
from stores.replay_reader import ReplayReader


def _split_csv(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def _sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _nested_level_fields_present(parquet: pq.ParquetFile, column_name: str) -> bool:
    try:
        field = parquet.schema_arrow.field(column_name)
    except KeyError:
        return False
    value_type = getattr(field.type, "value_type", None)
    if value_type is None:
        return False
    nested_names = set(getattr(value_type, "names", []))
    return {"price_str", "size_str"}.issubset(nested_names)


def _init_file_report(path: Path, key_fields: list[str], null_fields: list[str]) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "row_count": 0,
        "empty": True,
        "sorted": True,
        "duplicate_sequence_key_count": 0,
        "min_ts_exchange_ns": None,
        "max_ts_exchange_ns": None,
        "min_ts_receive_ns": None,
        "max_ts_receive_ns": None,
        "null_ratio": {name: None for name in null_fields},
        "schema_columns": [],
        "errors": [],
        "_key_fields": key_fields,
        "_null_fields": null_fields,
        "_null_counts": {name: 0 for name in null_fields},
    }


def _finalize_file_report(report: dict[str, Any]) -> dict[str, Any]:
    row_count = report["row_count"]
    for name, count in report.pop("_null_counts").items():
        report["null_ratio"][name] = (count / row_count) if row_count else None
    report.pop("_key_fields", None)
    report.pop("_null_fields", None)
    report["empty"] = row_count == 0
    return report


def _audit_parquet_file(
    path: Path,
    *,
    key_fields: list[str],
    null_fields: list[str],
    timestamp_fields: tuple[str, str] = ("ts_exchange_ns", "ts_receive_ns"),
) -> dict[str, Any]:
    report = _init_file_report(path, key_fields, null_fields)
    if not path.exists():
        return _finalize_file_report(report)

    try:
        parquet = pq.ParquetFile(path)
        report["row_count"] = parquet.metadata.num_rows
        report["schema_columns"] = list(parquet.schema_arrow.names)
        previous_key: tuple[Any, ...] | None = None
        seen_keys: set[tuple[Any, ...]] = set()
        ts_exchange, ts_receive = timestamp_fields

        for batch in parquet.iter_batches(batch_size=5000, use_threads=False):
            rows = batch.to_pylist()
            for row in rows:
                key = tuple(row.get(field) for field in key_fields)
                if previous_key is not None and key < previous_key:
                    report["sorted"] = False
                if key in seen_keys:
                    report["duplicate_sequence_key_count"] += 1
                seen_keys.add(key)
                previous_key = key

                exchange_value = row.get(ts_exchange)
                receive_value = row.get(ts_receive)
                if exchange_value is not None:
                    current_min = report["min_ts_exchange_ns"]
                    current_max = report["max_ts_exchange_ns"]
                    report["min_ts_exchange_ns"] = (
                        exchange_value if current_min is None else min(current_min, exchange_value)
                    )
                    report["max_ts_exchange_ns"] = (
                        exchange_value if current_max is None else max(current_max, exchange_value)
                    )
                if receive_value is not None:
                    current_min = report["min_ts_receive_ns"]
                    current_max = report["max_ts_receive_ns"]
                    report["min_ts_receive_ns"] = (
                        receive_value if current_min is None else min(current_min, receive_value)
                    )
                    report["max_ts_receive_ns"] = (
                        receive_value if current_max is None else max(current_max, receive_value)
                    )
                for field in null_fields:
                    if row.get(field) is None:
                        report["_null_counts"][field] += 1
    except Exception as exc:
        report["errors"].append(str(exc))

    return _finalize_file_report(report)


def audit_replay_partition(
    replay_root: Path,
    *,
    venue: str,
    symbol: str,
    date: str,
) -> dict[str, Any]:
    partition = replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    manifest_path = partition / "manifest.json"
    instrument_path = partition / "instrument.json"
    depth_path = partition / "depth.parquet"
    trades_path = partition / "trades.parquet"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception as exc:
            manifest = {"errors": [str(exc)]}

    depth = _audit_parquet_file(
        depth_path,
        key_fields=["stream_session_id", "session_seq", "raw_index"],
        null_fields=["U", "u", "pu"],
    )
    trades = _audit_parquet_file(
        trades_path,
        key_fields=["trade_stream_session_id", "trade_session_seq", "raw_index"],
        null_fields=["trade_id", "agg_trade_id", "price_str", "quantity_str"],
    )

    if depth_path.exists():
        try:
            depth_parquet = pq.ParquetFile(depth_path)
            depth["level_exact_fields_present"] = (
                _nested_level_fields_present(depth_parquet, "bids")
                and _nested_level_fields_present(depth_parquet, "asks")
            )
        except Exception as exc:
            depth["level_exact_fields_present"] = False
            depth["errors"].append(str(exc))
    else:
        depth["level_exact_fields_present"] = False

    depth_checksum = _sha256(depth_path)
    trades_checksum = _sha256(trades_path)
    manifest_depth_count = manifest.get("depth_record_count")
    manifest_trade_count = manifest.get("trade_record_count")

    return {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "partition": str(partition),
        "partition_exists": partition.exists(),
        "instrument_exists": instrument_path.exists(),
        "manifest_exists": manifest_path.exists(),
        "manifest_status": manifest.get("status"),
        "manifest_counts": {
            "depth_record_count": manifest_depth_count,
            "trade_record_count": manifest_trade_count,
        },
        "parquet_counts": {
            "depth_record_count": depth["row_count"],
            "trade_record_count": trades["row_count"],
        },
        "manifest_count_match": {
            "depth": manifest_depth_count == depth["row_count"],
            "trades": manifest_trade_count == trades["row_count"],
        },
        "checksum_match": {
            "depth": bool(depth_checksum and depth_checksum == manifest.get("depth_checksum")),
            "trades": bool(trades_checksum and trades_checksum == manifest.get("trades_checksum")),
        },
        "depth": depth,
        "trades": trades,
    }


def audit_replay_store(
    *,
    replay_root: Path,
    date: str,
    symbols: list[str],
    venues: list[str],
) -> dict[str, Any]:
    reader = ReplayReader(replay_root)
    if venues == ["ALL"]:
        venues = list(reader.iter_venues())
    if symbols == ["ALL"]:
        discovered: set[str] = set()
        for venue in venues:
            discovered.update(reader.iter_symbols(venue))
        symbols = sorted(discovered)

    partitions = []
    missing = []
    for venue in venues:
        available_symbols = set(reader.iter_symbols(venue))
        for symbol in symbols:
            if symbol not in available_symbols:
                missing.append({"venue": venue, "symbol": symbol, "date": date, "reason": "symbol_missing"})
                continue
            available_dates = set(reader.iter_dates(venue, symbol))
            if date not in available_dates:
                missing.append({"venue": venue, "symbol": symbol, "date": date, "reason": "date_missing"})
                continue
            partitions.append(
                audit_replay_partition(
                    replay_root,
                    venue=venue,
                    symbol=symbol,
                    date=date,
                )
            )

    return {
        "replay_root": str(replay_root),
        "date": date,
        "symbols": symbols,
        "venues": venues,
        "missing_partitions": missing,
        "partitions": partitions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit replay_store partitions.")
    parser.add_argument("--replay-root", type=Path, default=REPLAY_ROOT)
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols or all")
    parser.add_argument("--venues", required=True, help="Comma-separated venues or all")
    parser.add_argument("--report-path", type=Path, default=None)
    args = parser.parse_args()

    report = audit_replay_store(
        replay_root=args.replay_root,
        date=args.date,
        symbols=["ALL"] if args.symbols.lower() == "all" else _split_csv(args.symbols),
        venues=["ALL"] if args.venues.lower() == "all" else _split_csv(args.venues),
    )
    payload = json.dumps(report, indent=2, default=str)
    if args.report_path:
        args.report_path.parent.mkdir(parents=True, exist_ok=True)
        args.report_path.write_text(payload)
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
