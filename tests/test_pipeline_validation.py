from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from converter.readers import stream_raw_records
from converter.instruments import build_instruments
from validation.audit_feature_store import audit_feature_store
from validation.audit_replay_store import audit_replay_store
from pipeline.build_feature_store import build_features_for_symbol
from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.generate_catalog import (
    _date_range_from_window,
    _parse_iso_datetime,
    _window_from_date,
    generate_catalog_from_replay,
)
from validation.validate_catalog_equivalence import validate_catalog_equivalence
from validation.catalog_compare import compare_trade_ticks_semantic, load_trade_ticks


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _sample_raw_root(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    date = "2026-06-12"
    venue = "BINANCE_SPOT"
    symbol = "ADAUSDT"
    base_ts_ms = 1_781_222_400_000
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 2,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 2,
                "ts_event_ms": base_ts_ms,
                "U": 11,
                "u": 12,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1700", "100.0"]],
                    "asks": [["0.1710", "200.0"]],
                },
            },
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 1,
                "ts_event_ms": base_ts_ms + 1,
                "U": 9,
                "u": 10,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1690", "150.0"]],
                    "asks": [["0.1720", "250.0"]],
                },
            },
        ],
    )
    _write_jsonl(
        root / venue / "trade_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "heartbeat",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 0,
                "ts_recv_ns": base_ts_ms * 1_000_000,
                "ts_event_ms": base_ts_ms,
            },
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 2,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 20,
                "ts_event_ms": base_ts_ms,
                "ts_trade_ms": base_ts_ms,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 102,
                "native_payload": {"t": 102},
            },
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 10,
                "ts_event_ms": base_ts_ms + 1,
                "ts_trade_ms": base_ts_ms + 1,
                "price": "0.17060000",
                "quantity": "35.20000000",
                "is_buyer_maker": True,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )
    return root


def _sample_cross_day_feature_raw_root(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    date = "2026-06-12"
    venue = "BINANCE_SPOT"
    symbol = "ADAUSDT"
    day_start_ms = 1_781_222_400_000
    records = [
        ("prev", day_start_ms - 1_000, 1),
        ("in_first", day_start_ms, 2),
        ("in_third", day_start_ms + 120_000, 3),
        ("next", day_start_ms + 86_400_000, 4),
    ]
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": seq,
                "ts_recv_ns": ts_ms * 1_000_000 + seq,
                "ts_event_ms": ts_ms,
                "U": 100 + seq,
                "u": 100 + seq,
                "pu": 99 + seq,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1700", "100.0"]],
                    "asks": [["0.1710", "200.0"]],
                },
            }
            for _, ts_ms, seq in records
        ],
    )
    _write_jsonl(
        root / venue / "trade_v2" / symbol / date / "2026-06-12T00.jsonl",
        [
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": seq,
                "ts_recv_ns": ts_ms * 1_000_000 + seq,
                "ts_event_ms": ts_ms,
                "ts_trade_ms": ts_ms,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 1000 + seq,
            }
            for _, ts_ms, seq in records
        ],
    )
    return root


def test_pipeline_cli_help_does_not_touch_default_data_roots() -> None:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("CRYPTO_RECORDER_")
    }
    for module in (
        "pipeline.build_replay_store",
        "pipeline.build_feature_store",
        "validation.audit_feature_store",
        "validation.audit_replay_store",
        "pipeline.generate_catalog",
        "validation.validate_catalog_equivalence",
        "pipeline.daily_build",
    ):
        result = subprocess.run(
            [sys.executable, "-m", module, "--help"],
            cwd=Path(__file__).resolve().parent.parent,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr


def test_generate_catalog_help_lists_supported_profiles() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.generate_catalog", "--help"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "trades_only" in result.stdout
    assert "full_l2" in result.stdout
    assert "depth_only" in result.stdout
    assert "depth10" in result.stdout


def test_docs_do_not_reference_convert_day_symbols_until_supported() -> None:
    docs = "\n".join(
        path.read_text()
        for path in (Path(__file__).resolve().parent.parent / "docs").glob("*.md")
    )
    # convert_day.py now supports --symbols, so docs may use it intentionally.
    help_result = subprocess.run(
        [sys.executable, "convert_day.py", "--help"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    assert help_result.returncode == 0
    if "convert_day.py --date" in docs and "--symbols" in docs:
        assert "--symbols" in help_result.stdout


def test_stream_raw_records_uses_custom_root(tmp_path: Path) -> None:
    custom_root = tmp_path / "custom_raw"
    _write_jsonl(
        custom_root / "BINANCE_SPOT" / "trade_v2" / "ADAUSDT" / "2026-06-12" / "x.jsonl",
        [{"ok": True}],
    )

    records = list(
        stream_raw_records(
            "BINANCE_SPOT",
            "ADAUSDT",
            "trade_v2",
            "2026-06-12",
            root=custom_root,
        )
    )

    assert records == [{"ok": True}]


def test_replay_build_writes_sorted_manifested_partition(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"

    result = build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    assert result["status"] == "success"
    assert result["depth_count"] == 2
    assert result["trade_count"] == 2
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "date=2026-06-12"
    manifest = json.loads((partition / "manifest.json").read_text())
    assert manifest["depth_record_count"] == 2
    assert manifest["trade_record_count"] == 2
    assert manifest["depth_checksum"]
    assert manifest["trades_checksum"]

    trades = pq.ParquetFile(partition / "trades.parquet").read().to_pylist()
    assert [row["trade_session_seq"] for row in trades] == [1, 2]
    assert [row["price_str"] for row in trades] == ["0.17060000", "0.17070000"]
    assert [row["quantity_str"] for row in trades] == ["35.20000000", "30.90000000"]
    depths = pq.ParquetFile(partition / "depth.parquet").read().to_pylist()
    assert [row["session_seq"] for row in depths] == [1, 2]
    assert depths[0]["U"] == "9"
    assert depths[0]["bids"][0]["price_str"] == "0.1690"
    assert depths[0]["bids"][0]["size_str"] == "150.0"


def test_replay_store_audit_reports_counts_checksums_and_ordering(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    report = audit_replay_store(
        replay_root=replay_root,
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
    )

    assert report["missing_partitions"] == []
    partition = report["partitions"][0]
    assert partition["instrument_exists"] is False
    assert partition["manifest_count_match"] == {"depth": True, "trades": True}
    assert partition["checksum_match"] == {"depth": True, "trades": True}
    assert partition["depth"]["sorted"] is True
    assert partition["trades"]["sorted"] is True
    assert partition["depth"]["level_exact_fields_present"] is True
    assert partition["depth"]["null_ratio"]["U"] == 0
    assert partition["trades"]["null_ratio"]["price_str"] == 0
    assert partition["trades"]["null_ratio"]["quantity_str"] == 0


def test_generate_catalog_date_range_uses_exclusive_end() -> None:
    dates = _date_range_from_window(
        _parse_iso_datetime("2026-06-12T00:00:00Z"),
        _parse_iso_datetime("2026-06-13T00:00:00Z"),
    )
    assert dates == ["2026-06-12"]
    start, end = _window_from_date("2026-06-12")
    assert start == _parse_iso_datetime("2026-06-12T00:00:00Z")
    assert end == _parse_iso_datetime("2026-06-13T00:00:00Z")


def test_generate_catalog_from_replay_writes_readable_trades_catalog(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    output_root = tmp_path / "catalog_jobs"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    result = generate_catalog_from_replay(
        replay_root,
        output_root,
        "smoke",
        ["ADAUSDT"],
        ["BINANCE_SPOT"],
        _parse_iso_datetime("2026-06-12T00:00:00Z"),
        _parse_iso_datetime("2026-06-13T00:00:00Z"),
        profile="trades_only",
    )

    assert result["status"] == "success"
    assert result["time_filter"] == "ts_init"
    assert result["records_read"]["trades"] == 2
    assert result["found_partitions"] == [
        {"venue": "BINANCE_SPOT", "symbol": "ADAUSDT", "date": "2026-06-12"}
    ]
    assert result["missing_partitions"] == []
    assert result["records_written"]["trade_ticks"] == 2
    catalog_root = output_root / "job_smoke"
    catalog = ParquetDataCatalog(str(catalog_root))
    instruments = catalog.instruments()
    assert [str(instrument.id) for instrument in instruments] == ["ADAUSDT.BINANCE"]
    ticks = catalog.trade_ticks(
        instrument_ids=["ADAUSDT.BINANCE"],
        start=1_781_222_400_000_000_000,
        end=1_781_222_400_001_000_000,
    )
    assert len(ticks) == 2
    assert str(ticks[0].price) == "0.17060000"
    assert str(ticks[0].size) == "35.20000000"


def test_generate_catalog_cli_date_shortcut(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    output_root = tmp_path / "catalog_jobs"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline.generate_catalog",
            "--input",
            str(replay_root),
            "--symbols",
            "ADAUSDT",
            "--venues",
            "BINANCE_SPOT",
            "--date",
            "2026-06-12",
            "--output",
            str(output_root),
            "--job-id",
            "date_shortcut",
            "--overwrite",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((output_root / "job_date_shortcut" / "manifest.json").read_text())
    assert manifest["time_window"]["start"] == "2026-06-12T00:00:00+00:00"
    assert manifest["time_window"]["end"] == "2026-06-13T00:00:00+00:00"
    assert manifest["time_filter"] == "ts_init"
    assert manifest["records_read"]["trades"] == 2
    assert manifest["record_counts"]["trade_ticks"] == 2


def test_generate_catalog_fixed_job_id_and_overwrite(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    output_root = tmp_path / "catalog_jobs"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )
    kwargs = dict(
        replay_root=replay_root,
        catalog_root=output_root,
        job_id="validation_new",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        start=_parse_iso_datetime("2026-06-12T00:00:00Z"),
        end=_parse_iso_datetime("2026-06-13T00:00:00Z"),
        profile="trades_only",
    )

    first = generate_catalog_from_replay(**kwargs)
    second = generate_catalog_from_replay(**kwargs)
    third = generate_catalog_from_replay(**kwargs, overwrite=True)

    assert first["status"] == "success"
    assert (output_root / "job_validation_new").exists()
    assert second["status"] == "failed"
    assert "already exists" in second["errors"][0]
    assert third["status"] == "success"


def _write_ticks_catalog(catalog_root: Path, ticks: list[TradeTick]) -> None:
    instrument = build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]
    catalog = ParquetDataCatalog(str(catalog_root))
    catalog.write_data([instrument])
    catalog.write_data(ticks)


def test_trades_only_semantic_comparison_with_synthetic_catalogs(tmp_path: Path) -> None:
    instrument = build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]
    ticks = [
        TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("0.17060000"),
            size=Quantity.from_str("35.20000000"),
            aggressor_side=AggressorSide.SELLER,
            trade_id=TradeId("101"),
            ts_event=1_781_222_400_001_000_000,
            ts_init=1_781_222_400_000_000_010,
        ),
        TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("0.17070000"),
            size=Quantity.from_str("30.90000000"),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId("102"),
            ts_event=1_781_222_400_000_000_000,
            ts_init=1_781_222_400_000_000_020,
        ),
    ]
    old_root = tmp_path / "old_catalog"
    new_root = tmp_path / "new_catalog"
    _write_ticks_catalog(old_root, ticks)
    _write_ticks_catalog(new_root, list(ticks))

    old_ticks = load_trade_ticks(old_root, "ADAUSDT.BINANCE")
    new_ticks = load_trade_ticks(new_root, "ADAUSDT.BINANCE")
    comparison = compare_trade_ticks_semantic(old_ticks, new_ticks)

    assert comparison["passed"] is True
    assert comparison["trade_count_match"] is True
    assert comparison["timestamp_range_match"] is True
    assert comparison["sample_mismatches"] == []


def test_feature_build_clamps_to_utc_day_and_remains_sparse(tmp_path: Path) -> None:
    raw_root = _sample_cross_day_feature_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    feature_root = tmp_path / "features"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )

    result = build_features_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        ["1m"],
        replay_root,
        feature_root,
    )

    assert result["status"] == "success"
    assert result["timeframes_processed"]["1m"] == 2
    path = feature_root / "timeframe=1m" / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / "2026-06-12.parquet"
    table = pq.ParquetFile(path).read()
    timestamps = table.column("timestamp_ns").to_pylist()
    day_start_ns = 1_781_222_400_000_000_000
    day_end_ns = day_start_ns + 86_400_000_000_000
    assert table.num_rows == 2
    assert all(day_start_ns <= ts < day_end_ns for ts in timestamps)
    assert timestamps == [
        day_start_ns + 60_000_000_000 - 1,
        day_start_ns + 180_000_000_000 - 1,
    ]


def test_feature_store_audit_reports_sparse_utc_day_output(tmp_path: Path) -> None:
    raw_root = _sample_cross_day_feature_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    feature_root = tmp_path / "features"
    build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
    )
    build_features_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        ["1m"],
        replay_root,
        feature_root,
    )

    report = audit_feature_store(
        feature_root=feature_root,
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        timeframes=["1m"],
    )
    item = report["files"][0]
    assert item["actual_row_count"] == 2
    assert item["expected_dense_row_count"] == 1440
    assert item["outside_date_rows"] == 0
    assert item["duplicate_timestamp_count"] == 0
    assert item["missing_windows_count_if_dense"] == 1438
    assert "return_1s" in item["all_null_columns"]


def test_validator_skips_unsupported_profiles(tmp_path: Path) -> None:
    # full_l2 is now implemented and validated elsewhere; the validator still
    # short-circuits profiles it has no convert_day reference for (depth10).
    report = validate_catalog_equivalence(
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
        data_root=tmp_path / "raw",
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old",
        replay_root=tmp_path / "replay",
        new_catalog_root=tmp_path / "new",
        profile="depth10",
        overwrite=True,
    )
    assert report["status"] == "skipped"


@pytest.mark.realdata
def test_real_data_catalog_equivalence_when_enabled(tmp_path: Path) -> None:
    real_root = os.environ.get("CRYPTO_RECORDER_REAL_DATA_ROOT")
    if not real_root:
        pytest.skip("Set CRYPTO_RECORDER_REAL_DATA_ROOT to run real-data equivalence")
    report = validate_catalog_equivalence(
        date=os.environ.get("CRYPTO_RECORDER_REAL_DATA_DATE", "2026-06-12"),
        symbols=[os.environ.get("CRYPTO_RECORDER_REAL_DATA_SYMBOL", "ADAUSDT")],
        venues=[os.environ.get("CRYPTO_RECORDER_REAL_DATA_VENUE", "BINANCE_SPOT")],
        data_root=Path(real_root),
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old_catalog",
        replay_root=tmp_path / "replay_store",
        new_catalog_root=tmp_path / "new_catalog",
        profile="trades_only",
        overwrite=True,
    )
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
