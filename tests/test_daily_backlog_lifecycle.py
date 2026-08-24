"""Bounded backlog ordering, outcome, policy, and reporting tests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline import daily_build


VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"


def _raw(root: Path, date: str, *, depth: bool = True, trades: bool = True) -> None:
    if depth:
        path = root / VENUE / "depth_v2" / SYMBOL / date / f"{date}T00.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n")
    if trades:
        path = root / VENUE / "trade_v2" / SYMBOL / date / f"{date}T00.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n")


def _install_fake_builder(monkeypatch, replay_root: Path, calls: list[str], outcomes=None):
    outcomes = outcomes or {}

    def fake(venue, symbol, date, data_root, replay_root_arg, **kwargs):
        calls.append(date)
        outcome = outcomes.get(date)
        partition = replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
        if outcome is None:
            outcome = "skipped_valid" if partition.exists() else "built"
        if outcome == "built":
            partition.mkdir(parents=True, exist_ok=True)
        status = "skipped" if outcome == "skipped_valid" else (
            "deferred" if outcome == "deferred_not_ready" else (
                "success" if outcome == "built" else "failed"
            )
        )
        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "status": status,
            "outcome": outcome,
            "depth_count": 10 if outcome in daily_build.SUCCESS_OUTCOMES else 0,
            "trade_count": 3 if outcome in daily_build.SUCCESS_OUTCOMES else 0,
            "errors": [] if outcome in daily_build.SUCCESS_OUTCOMES else [outcome],
        }

    monkeypatch.setattr("pipeline.build_replay_store.build_replay_for_symbol", fake)
    monkeypatch.setattr("stores.replay_writer.validate_partition", lambda path: path.exists())
    monkeypatch.setattr("pipeline.replay_lifecycle.validate_partition", lambda path: path.exists())


def _run(tmp_path: Path, **kwargs):
    return daily_build.run_backlog(
        newest_date=kwargs.pop("newest_date", "2026-01-03"),
        backlog_days=kwargs.pop("backlog_days", 3),
        max_build_dates=kwargs.pop("max_build_dates", 3),
        schema_version=kwargs.pop("schema_version", 2),
        data_root=tmp_path / "raw",
        replay_root=tmp_path / "replay",
        report_root=tmp_path / "reports",
        **kwargs,
    )


def test_oldest_incomplete_dates_first_and_max_build_dates(monkeypatch, tmp_path: Path) -> None:
    for date in ("2026-01-01", "2026-01-02", "2026-01-03"):
        _raw(tmp_path / "raw", date)
    calls: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)

    report, exit_code = _run(tmp_path, max_build_dates=2)

    assert calls == ["2026-01-01", "2026-01-02"]
    assert report["dates_selected_for_build"] == ["2026-01-01", "2026-01-02"]
    assert report["built_count"] == 2
    assert report["deferred_count"] == 1
    assert exit_code != 0


def test_valid_date_skips_without_consuming_slot(monkeypatch, tmp_path: Path) -> None:
    for date in ("2026-01-01", "2026-01-02", "2026-01-03"):
        _raw(tmp_path / "raw", date)
    valid = tmp_path / "replay" / f"venue={VENUE}" / f"symbol={SYMBOL}" / "date=2026-01-01"
    valid.mkdir(parents=True)
    calls: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)

    report, _ = _run(tmp_path, max_build_dates=1)

    assert calls == ["2026-01-01", "2026-01-02"]
    assert report["skipped_valid_count"] == 1
    assert report["built_count"] == 1
    assert report["deferred_count"] == 1


@pytest.mark.parametrize("backlog,max_dates", [(0, 1), (-1, 1), (32, 1), (1, 0), (1, 32)])
def test_unreasonable_bounds_rejected(tmp_path: Path, backlog: int, max_dates: int) -> None:
    with pytest.raises(ValueError):
        _run(tmp_path, backlog_days=backlog, max_build_dates=max_dates)


def test_failed_historical_date_is_rediscovered(monkeypatch, tmp_path: Path) -> None:
    _raw(tmp_path / "raw", "2026-01-03")
    calls: list[str] = []
    _install_fake_builder(
        monkeypatch,
        tmp_path / "replay",
        calls,
        outcomes={"2026-01-03": "failed"},
    )
    first, first_code = _run(tmp_path, backlog_days=1)
    second, second_code = _run(tmp_path, backlog_days=1)
    assert calls == ["2026-01-03", "2026-01-03"]
    assert first["failed_count"] == second["failed_count"] == 1
    assert first_code != 0 and second_code != 0


def test_missing_trade_is_distinct_and_nonzero(monkeypatch, tmp_path: Path) -> None:
    _raw(tmp_path / "raw", "2026-01-03", trades=False)
    calls: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)
    report, exit_code = _run(tmp_path, backlog_days=1)
    assert calls == []
    assert report["missing_count"] == 1
    assert report["failed_count"] == 0
    assert exit_code != 0


@pytest.mark.parametrize(
    "outcome,count_field",
    [
        ("deferred_not_ready", "deferred_count"),
        ("source_changed_rebuild_required", "source_changed_count"),
        ("incompatible_schema_rebuild_required", "incompatible_schema_count"),
    ],
)
def test_nonfinal_outcomes_are_distinct_and_nonzero(
    monkeypatch, tmp_path: Path, outcome: str, count_field: str
) -> None:
    _raw(tmp_path / "raw", "2026-01-03")
    calls: list[str] = []
    _install_fake_builder(
        monkeypatch,
        tmp_path / "replay",
        calls,
        outcomes={"2026-01-03": outcome},
    )
    report, exit_code = _run(tmp_path, backlog_days=1)
    assert report[count_field] == 1
    assert exit_code != 0


def test_report_counts_match_partition_results_and_are_atomic(monkeypatch, tmp_path: Path) -> None:
    _raw(tmp_path / "raw", "2026-01-03")
    calls: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)
    report, exit_code = _run(tmp_path, backlog_days=1)
    assert exit_code == 0
    assert report["total_eligible_count"] == report["built_count"] == 1
    date_report = json.loads((tmp_path / "reports" / "daily_build_2026-01-03.json").read_text())
    assert date_report["built_count"] == len(date_report["partition_results"])
    assert not list((tmp_path / "reports").glob(".*.tmp"))


def test_report_write_failure_cannot_return_success(monkeypatch, tmp_path: Path) -> None:
    _raw(tmp_path / "raw", "2026-01-03")
    calls: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)
    monkeypatch.setattr(
        daily_build,
        "atomic_write_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("report disk full")),
    )
    with pytest.raises(OSError, match="report disk full"):
        _run(tmp_path, backlog_days=1)


def test_reconciliation_failure_writes_atomic_invocation_report(tmp_path: Path) -> None:
    unknown = tmp_path / "replay" / "unexpected"
    unknown.parent.mkdir(parents=True)
    unknown.write_text("unsafe")

    report, exit_code = _run(tmp_path, backlog_days=1)

    assert exit_code != 0
    assert report["process_exit_classification"] == "reconciliation_failure"
    assert report["dates_inspected"] == []
    paths = list((tmp_path / "reports").glob("replay_backlog_*.json"))
    assert len(paths) == 1
    persisted = json.loads(paths[0].read_text())
    assert persisted["final_status"] == "failed"
    assert "unknown" in persisted["errors"][0]


def test_date_orchestration_exception_reports_and_stops(monkeypatch, tmp_path: Path) -> None:
    for date in ("2026-01-02", "2026-01-03"):
        _raw(tmp_path / "raw", date)
    calls: list[str] = []

    def fail_builder(venue, symbol, date, *args, **kwargs):
        calls.append(date)
        raise RuntimeError("injected builder crash")

    monkeypatch.setattr(
        "pipeline.build_replay_store.build_replay_for_symbol", fail_builder
    )
    report, exit_code = _run(tmp_path, backlog_days=2)

    assert exit_code != 0
    assert calls == ["2026-01-02"]
    assert report["dates_inspected"] == ["2026-01-02"]
    assert report["failed_count"] == 1
    date_report = json.loads(
        (tmp_path / "reports" / "daily_build_2026-01-02.json").read_text()
    )
    assert date_report["partition_results"][0]["outcome"] == "failed"
    assert "injected builder crash" in date_report["errors"][0]


def test_default_cli_contract_is_schema2_and_bounded() -> None:
    args = daily_build._parser().parse_args(["--date", "2026-01-03"])
    assert args.schema_version == 2
    assert 1 <= args.backlog_days <= 31
    assert 1 <= args.max_build_dates <= 31


def test_explicit_venue_scope_excludes_other_venues(monkeypatch, tmp_path: Path) -> None:
    _raw(tmp_path / "raw", "2026-01-03")
    other = "BINANCE_USDTF"
    for channel in ("depth_v2", "trade_v2"):
        path = tmp_path / "raw" / other / channel / SYMBOL / "2026-01-03" / "00.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n")
    calls: list[str] = []
    observed_venues: list[str] = []
    _install_fake_builder(monkeypatch, tmp_path / "replay", calls)
    original = __import__("pipeline.build_replay_store", fromlist=["build_replay_for_symbol"])
    fake = original.build_replay_for_symbol

    def capture(venue, *args, **kwargs):
        observed_venues.append(venue)
        return fake(venue, *args, **kwargs)

    monkeypatch.setattr("pipeline.build_replay_store.build_replay_for_symbol", capture)
    report, exit_code = _run(
        tmp_path,
        backlog_days=1,
        venues=[VENUE],
    )
    assert exit_code == 0
    assert observed_venues == [VENUE]
    assert report["selected_venues"] == [VENUE]
