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


def test_lifecycle_boundaries_are_not_real_unrecovered_fences() -> None:
    summary = convert_day_mod._summarize_fences([
        {"reason": "bootstrap", "recovered": True},
        {"reason": "websocket_closed", "recovered": False},
    ])

    assert summary["bootstrap_fences"] == 1
    assert summary["shutdown_fences"] == 1
    assert summary["reconnect_fences"] == 0
    assert summary["real_desync_fences"] == 0
    assert summary["unrecovered_real_fences"] == 0
    assert summary["unrecovered_fences"] == 0
    assert summary["fenced_ranges_low"] == 2
    assert summary["fenced_ranges_high"] == 0


def test_utc_day_rollover_fence_is_lifecycle_not_real_desync() -> None:
    summary = convert_day_mod._summarize_fences([
        {"reason": "utc_day_rollover", "recovered": True},
    ])

    assert summary["utc_day_rollover_fences"] == 1
    assert summary["real_desync_fences"] == 0
    assert summary["unrecovered_real_fences"] == 0
    assert summary["fenced_ranges_low"] == 1


def test_reconnect_boundary_is_separate_from_shutdown() -> None:
    summary = convert_day_mod._summarize_fences([
        {
            "reason": "websocket_closed",
            "recovered": False,
            "start_ts_ns": 1_000,
            "end_ts_ns": 2_000,
            "closed_by_session_change": True,
        },
    ])

    assert summary["reconnect_fences"] == 1
    assert summary["shutdown_fences"] == 0
    assert summary["unrecovered_real_fences"] == 0


def test_continuity_break_unrecovered_is_high_severity() -> None:
    summary = convert_day_mod._summarize_fences([
        {"reason": "continuity_break", "recovered": False},
    ])

    assert summary["fenced_ranges_high"] == 1
    assert summary["real_desync_fences"] == 1
    assert summary["unrecovered_real_fences"] == 1
    assert summary["unrecovered_fences"] == 1


def test_initial_startup_boundary_is_not_reconnect(monkeypatch) -> None:
    def fake_stream_raw_records(venue, symbol, channel, date_str):
        if channel != "depth_v2":
            return
        yield {
            "record_type": "stream_lifecycle",
            "event": "session_start",
            "reason": "startup_or_reconnect",
            "stream_session_id": 1,
            "ts_recv_ns": 1_000,
        }
        yield {
            "record_type": "stream_lifecycle",
            "event": "session_end",
            "reason": "websocket_closed",
            "stream_session_id": 1,
            "ts_recv_ns": 2_000,
        }

    monkeypatch.setattr(convert_day_mod, "stream_raw_records", fake_stream_raw_records)

    diag = convert_day_mod._build_gap_diagnostics("BINANCE_SPOT", "BTCUSDT", "2026-04-21", [])

    assert diag["session_boundary_gap_count"] == 2
    assert diag["shutdown_boundary_gap_count"] == 1
    assert diag["reconnect_boundary_gap_count"] == 0


def test_later_session_start_counts_one_reconnect_boundary() -> None:
    counts = convert_day_mod._classify_lifecycle_boundaries([
        {"event": "session_start", "reason": "startup_or_reconnect", "stream_session_id": 1},
        {"event": "session_end", "reason": "websocket_closed", "stream_session_id": 1},
        {"event": "session_start", "reason": "startup_or_reconnect", "stream_session_id": 2},
    ])

    assert counts["session_boundary_gap_count"] == 3
    assert counts["shutdown_boundary_gap_count"] == 0
    assert counts["reconnect_boundary_gap_count"] == 1


def test_top_real_gap_offenders_use_depth_not_trade() -> None:
    trade_only_gap = {
        "max_depth_update_gap_sec": 0.2,
        "depth_gap_count_over_1s": 0,
        "depth_gap_count_over_5s": 0,
        "depth_gap_count_over_60s": 0,
        "max_trade_gap_sec": 120.0,
        "max_depth10_gap_sec": 0.0,
    }
    depth_gap = {
        "max_depth_update_gap_sec": 7.5,
        "depth_gap_count_over_1s": 1,
        "depth_gap_count_over_5s": 1,
        "depth_gap_count_over_60s": 0,
        "max_trade_gap_sec": 1.0,
        "max_depth10_gap_sec": 7.5,
    }

    candidates = [
        entry
        for entry in [
            convert_day_mod._real_gap_offender_entry("BINANCE_SPOT/TRADEGAP", trade_only_gap),
            convert_day_mod._real_gap_offender_entry("BINANCE_SPOT/DEPTHGAP", depth_gap),
        ]
        if entry is not None
    ]

    assert convert_day_mod._top_real_gap_offenders(candidates) == [
        {
            "symbol": "BINANCE_SPOT/DEPTHGAP",
            "max_depth_update_gap_sec": 7.5,
            "depth_gap_count_over_1s": 1,
            "depth_gap_count_over_5s": 1,
            "depth_gap_count_over_60s": 0,
            "max_depth10_gap_sec": 7.5,
        }
    ]
