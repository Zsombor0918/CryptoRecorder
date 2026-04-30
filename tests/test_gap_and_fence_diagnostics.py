from __future__ import annotations

import convert_day as convert_day_mod


def test_depth_gap_thresholds_are_counted_separately() -> None:
    stats = convert_day_mod._gap_counts([
        0,
        2_000_000_000,
        8_000_000_000,
        70_000_000_000,
    ])

    assert stats["max_gap_sec"] == 62.0
    assert stats["gap_count_over_1s"] == 3
    assert stats["gap_count_over_5s"] == 2
    assert stats["gap_count_over_60s"] == 1


def test_gap_scan_sorts_timestamps_to_avoid_file_rotation_false_gap() -> None:
    stats = convert_day_mod._gap_counts([
        3_000_000_000,
        1_000_000_000,
        2_000_000_000,
    ])

    assert stats["max_gap_sec"] == 1.0
    assert stats["gap_count_over_1s"] == 0


def test_trade_gap_diagnostics_are_informational(monkeypatch) -> None:
    def fake_stream_raw_records(venue, symbol, channel, date_str):
        if channel == "depth_v2":
            yield {"record_type": "depth_update", "ts_event_ms": 1_000}
            yield {"record_type": "depth_update", "ts_event_ms": 1_100}
        else:
            yield {"record_type": "trade", "ts_trade_ms": 1_000}
            yield {"record_type": "trade", "ts_trade_ms": 70_000}

    monkeypatch.setattr(convert_day_mod, "stream_raw_records", fake_stream_raw_records)

    diag = convert_day_mod._build_gap_diagnostics("BINANCE_SPOT", "BTCUSDT", "2026-04-21", [])

    assert diag["max_trade_gap_sec"] == 69.0
    assert diag["trade_gap_informational"] is True
    assert diag["depth_gap_count_over_60s"] == 0


def test_shutdown_boundary_is_low_severity() -> None:
    summary = convert_day_mod._summarize_fences([
        {"reason": "websocket_closed", "recovered": False},
    ])

    assert summary["fenced_ranges_low"] == 1
    assert summary["fenced_ranges_high"] == 0


def test_continuity_break_unrecovered_is_high_severity() -> None:
    summary = convert_day_mod._summarize_fences([
        {"reason": "continuity_break", "recovered": False},
    ])

    assert summary["fenced_ranges_high"] == 1
    assert summary["unrecovered_fences"] == 1
