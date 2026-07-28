"""
Focused regression tests for pipeline.daily_build.

Covers the false-success bug: previously, when zero raw partitions were
found for a date (no raw data / no eligible symbols), the replay-build
status computed `successful == len(results)` as `0 == 0` -> True, which was
reported as "success" with exit code 0. This must instead be an explicit
"no_data" status with a nonzero exit code, distinct from a genuine "success"
(all eligible partitions built), a "partial" failure (some symbols succeeded
and some failed), and a "failed" status (partitions were attempted but zero
succeeded).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import daily_build


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _write_symbol_raw_data(root: Path, venue: str, symbol: str, date: str) -> None:
    base_ts_ms = 1_781_222_400_000
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000,
                "ts_event_ms": base_ts_ms,
                "U": 1,
                "u": 2,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [["0.1700", "100.0"]],
                    "asks": [["0.1710", "200.0"]],
                },
            },
        ],
    )
    _write_jsonl(
        root / venue / "trade_v2" / symbol / date / f"{date}T00.jsonl",
        [
            {
                "record_type": "trade",
                "venue": venue,
                "market_type": "spot",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 10,
                "ts_event_ms": base_ts_ms,
                "ts_trade_ms": base_ts_ms,
                "price": "0.17070000",
                "quantity": "30.90000000",
                "is_buyer_maker": False,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )


def _write_exchangeinfo_raw_data(root: Path, venue: str, date: str) -> None:
    """Write a raw exchangeinfo partition: data_raw/<venue>/exchangeinfo/
    EXCHANGEINFO/<date>/... — metadata only, never a market symbol."""
    _write_jsonl(
        root / venue / "exchangeinfo" / "EXCHANGEINFO" / date / f"{date}T00.jsonl",
        [{"record_type": "exchangeinfo_snapshot", "venue": venue, "symbols": []}],
    )


DATE = "2026-06-12"
VENUE = "BINANCE_SPOT"


def test_no_raw_data_reports_no_data_status_and_nonzero_exit(tmp_path: Path) -> None:
    """An empty (but existing) data_root must produce an explicit 'no_data'
    report status, not 'success', and main() must return nonzero."""
    data_root = tmp_path / "raw"
    data_root.mkdir()
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    assert raw_result["symbol_count"] == 0

    replay_result = daily_build.run_build_replay_store(DATE, [], data_root, replay_root)
    assert replay_result["status"] == "no_data"
    assert replay_result["symbols_total"] == 0

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "no_data"
    assert report["status"] != "success"


def test_no_raw_data_main_exits_nonzero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "raw"
    data_root.mkdir()
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"

    monkeypatch.setattr(
        "sys.argv",
        [
            "daily_build.py",
            "--date", DATE,
            "--data-root", str(data_root),
            "--replay-root", str(replay_root),
            "--report-root", str(report_root),
        ],
    )
    exit_code = daily_build.main()
    assert exit_code != 0

    report_path = report_root / f"daily_build_{DATE}.json"
    report = json.loads(report_path.read_text())
    assert report["status"] == "no_data"


def test_all_success_reports_success_status(tmp_path: Path) -> None:
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_symbol_raw_data(data_root, VENUE, "ADAUSDT", DATE)
    _write_symbol_raw_data(data_root, VENUE, "BTCUSDT", DATE)

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    replay_result = daily_build.run_build_replay_store(
        DATE, ["ADAUSDT", "BTCUSDT"], data_root, replay_root
    )
    assert replay_result["status"] == "success"
    assert replay_result["symbols_total"] == 2
    assert replay_result["symbols_processed"] == 2

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "success"


def test_partial_failure_preserves_partial_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When some symbols succeed and some fail, the report status must be
    'partial' (not 'success' and not 'no_data')."""
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_symbol_raw_data(data_root, VENUE, "ADAUSDT", DATE)
    _write_symbol_raw_data(data_root, VENUE, "BTCUSDT", DATE)

    import pipeline.build_replay_store as build_replay_store_module

    real_build = build_replay_store_module.build_replay_for_symbol

    def _fake_build(venue, symbol, date, data_root_, replay_root_):
        if symbol == "BTCUSDT":
            return {
                "venue": venue,
                "symbol": symbol,
                "date": date,
                "status": "failed",
                "depth_count": 0,
                "trade_count": 0,
                "errors": ["simulated failure"],
            }
        return real_build(venue, symbol, date, data_root_, replay_root_)

    monkeypatch.setattr(build_replay_store_module, "build_replay_for_symbol", _fake_build)

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    replay_result = daily_build.run_build_replay_store(
        DATE, ["ADAUSDT", "BTCUSDT"], data_root, replay_root
    )
    assert replay_result["status"] == "partial"
    assert replay_result["symbols_processed"] == 1
    assert replay_result["symbols_total"] == 2

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "partial"
    assert report["errors"] == ["simulated failure"]


def test_all_partitions_failed_reports_failed_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When partitions were attempted but every single one failed (zero
    successful, nonzero attempted), the report status must be the distinct
    'failed' status — not 'partial' (implies some success) and not 'no_data'
    (implies nothing was eligible)."""
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_symbol_raw_data(data_root, VENUE, "ADAUSDT", DATE)
    _write_symbol_raw_data(data_root, VENUE, "BTCUSDT", DATE)

    import pipeline.build_replay_store as build_replay_store_module

    def _fake_build_all_fail(venue, symbol, date, data_root_, replay_root_):
        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "status": "failed",
            "depth_count": 0,
            "trade_count": 0,
            "errors": [f"simulated failure for {symbol}"],
        }

    monkeypatch.setattr(
        build_replay_store_module, "build_replay_for_symbol", _fake_build_all_fail
    )

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    replay_result = daily_build.run_build_replay_store(
        DATE, ["ADAUSDT", "BTCUSDT"], data_root, replay_root
    )
    assert replay_result["status"] == "failed"
    assert replay_result["symbols_processed"] == 0
    assert replay_result["symbols_total"] == 2

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "failed"
    assert report["status"] not in ("partial", "no_data", "success")
    assert len(report["errors"]) == 2


# ---------------------------------------------------------------------------
# exchangeinfo-only dates must never be treated as market-symbol replay work
# (Codex review finding #1).
# ---------------------------------------------------------------------------

def test_exchangeinfo_only_date_reports_no_data(tmp_path: Path) -> None:
    """A date with only a raw exchangeinfo partition (no depth_v2/trade_v2)
    must report 'no_data' and attempt zero replay partitions — EXCHANGEINFO
    must never be derived/attempted as a market symbol. Issue #20 Phase 7
    correction: pipeline.raw_manifest.scan_raw_coverage() now excludes
    EXCHANGEINFO at the source, so it never appears in raw_result['data']
    at all (previously it appeared there and was filtered out only by a
    downstream ELIGIBLE_REPLAY_CHANNELS check)."""
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_exchangeinfo_raw_data(data_root, VENUE, DATE)

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    assert "EXCHANGEINFO" not in raw_result["data"].get(VENUE, {})

    all_symbols = sorted(
        {s for venue_data in raw_result["data"].values() for s in venue_data}
    )
    replay_result = daily_build.run_build_replay_store(
        DATE, all_symbols, data_root, replay_root
    )

    assert replay_result["status"] == "no_data"
    assert replay_result["symbols_total"] == 0
    assert replay_result["results"] == []

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "no_data"


def test_exchangeinfo_main_exits_nonzero_with_no_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_exchangeinfo_raw_data(data_root, VENUE, DATE)

    monkeypatch.setattr(
        "sys.argv",
        [
            "daily_build.py",
            "--date", DATE,
            "--data-root", str(data_root),
            "--replay-root", str(replay_root),
            "--report-root", str(report_root),
        ],
    )
    exit_code = daily_build.main()
    assert exit_code != 0

    report_path = report_root / f"daily_build_{DATE}.json"
    report = json.loads(report_path.read_text())
    assert report["status"] == "no_data"


def test_exchangeinfo_plus_one_valid_symbol_only_attempts_valid_symbol(
    tmp_path: Path,
) -> None:
    """A date with both an exchangeinfo partition and one valid depth/trade
    symbol must attempt only the valid symbol; EXCHANGEINFO must never be
    attempted, and the eligible symbol's success must still be reported.
    Issue #20 Phase 7 correction: EXCHANGEINFO is now excluded by
    scan_raw_coverage() itself, so it never appears in raw_result['data']
    in the first place."""
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_exchangeinfo_raw_data(data_root, VENUE, DATE)
    _write_symbol_raw_data(data_root, VENUE, "ADAUSDT", DATE)

    raw_result = daily_build.run_raw_manifest(DATE, data_root)
    all_symbols = sorted(
        {s for venue_data in raw_result["data"].values() for s in venue_data}
    )
    assert "EXCHANGEINFO" not in all_symbols
    assert "ADAUSDT" in all_symbols

    replay_result = daily_build.run_build_replay_store(
        DATE, all_symbols, data_root, replay_root
    )

    assert replay_result["status"] == "success"
    assert replay_result["symbols_total"] == 1
    assert replay_result["symbols_processed"] == 1
    assert [r["symbol"] for r in replay_result["results"]] == ["ADAUSDT"]

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "success"


def test_explicit_symbol_filtering_cannot_build_exchangeinfo(tmp_path: Path) -> None:
    """Even if a caller explicitly requests --symbols EXCHANGEINFO, it must
    never be attempted as a replay build, because it has no depth_v2/trade_v2
    channel coverage."""
    data_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    report_root = tmp_path / "reports"
    _write_exchangeinfo_raw_data(data_root, VENUE, DATE)
    _write_symbol_raw_data(data_root, VENUE, "ADAUSDT", DATE)

    raw_result = daily_build.run_raw_manifest(DATE, data_root)

    replay_result = daily_build.run_build_replay_store(
        DATE, ["EXCHANGEINFO"], data_root, replay_root
    )
    assert replay_result["status"] == "no_data"
    assert replay_result["symbols_total"] == 0
    assert replay_result["results"] == []

    report = daily_build.generate_daily_report(
        DATE, data_root, replay_root, report_root, raw_result, replay_result, 1.0
    )
    assert report["status"] == "no_data"
