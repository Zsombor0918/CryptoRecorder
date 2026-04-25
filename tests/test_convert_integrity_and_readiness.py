"""
test_convert_integrity_and_readiness.py

Tests for:
- per_symbol_trade diagnostics (will_create_tradetick, raw counts)
- per_symbol_depth diagnostics (will_create_l2, raw counts, new metrics fields)
- readiness_classification section (full_ready / l2_ready / trade_only / not_ready)
- conversion_integrity by-venue breakdowns
- partial overwrite guard (refuse and allow paths, catalog never purged on refuse)
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest
from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
from nautilus_trader.model.enums import BookAction, OrderSide
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import convert_day as convert_day_mod
from converter.depth_phase2 import Phase2ReplayMetrics


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_dep_metrics(
    *,
    raw_record_count: int = 0,
    depth_update_record_count: int = 0,
    sync_state_record_count: int = 0,
    stream_lifecycle_record_count: int = 0,
    snapshot_seed_count: int = 0,
    desync_events: int = 0,
    resync_count: int = 0,
    deltas: int = 0,
    depth10: int = 0,
    first_ts_ns: int | None = None,
    last_ts_ns: int | None = None,
) -> Phase2ReplayMetrics:
    m = Phase2ReplayMetrics(
        snapshot_seed_count=snapshot_seed_count,
        delta_events_written=deltas,
        depth10_written=depth10,
        desync_events=desync_events,
        resync_count=resync_count,
        first_ts_ns=first_ts_ns,
        last_ts_ns=last_ts_ns,
    )
    m.raw_record_count = raw_record_count
    m.depth_update_record_count = depth_update_record_count
    m.sync_state_record_count = sync_state_record_count
    m.stream_lifecycle_record_count = stream_lifecycle_record_count
    return m


def _snapshot_deltas_batch(instrument) -> OrderBookDeltas:
    ts_e, ts_i = 1_000_000_000, 1_000_000_100
    return OrderBookDeltas(instrument.id, [
        OrderBookDelta.clear(instrument.id, 100, ts_e, ts_i),
        OrderBookDelta(
            instrument.id,
            BookAction.UPDATE,
            BookOrder(OrderSide.BUY, Price.from_str("100.0"), Quantity.from_str("1.0"), 0),
            flags=32 | 128,
            sequence=100,
            ts_event=ts_e,
            ts_init=ts_i,
        ),
    ])


def _no_ticks_diag() -> dict:
    return {
        "raw_record_count": 0,
        "raw_trade_record_count": 0,
        "raw_lifecycle_record_count": 0,
        "ticks_written": 0,
    }


def _lifecycle_only_diag() -> dict:
    return {
        "raw_record_count": 3,
        "raw_trade_record_count": 0,
        "raw_lifecycle_record_count": 3,
        "ticks_written": 0,
    }


def _trade_diag(n: int = 5) -> dict:
    return {
        "raw_record_count": n + 1,
        "raw_trade_record_count": n,
        "raw_lifecycle_record_count": 1,
        "ticks_written": n,
    }


# ---------------------------------------------------------------------------
# per_symbol_trade diagnostics
# ---------------------------------------------------------------------------

class TestPerSymbolTradeDiagnostics:
    """per_symbol_trade fields correctly reflect lifecycle-only vs real trades."""

    def _setup_minimal(self, monkeypatch, tmp_path, instrument, trade_diag, ticks=None):
        if ticks is None:
            ticks = []
        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: (ticks, 0, None, None, trade_diag),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([], [], Phase2ReplayMetrics()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: set(),
        )

    def test_lifecycle_only_futures_will_create_tradetick_false(
        self, monkeypatch, tmp_path: Path
    ):
        instrument = TestInstrumentProvider.btcusdt_binance()
        self._setup_minimal(monkeypatch, tmp_path, instrument, _lifecycle_only_diag())
        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        entry = report["per_symbol_trade"]["BINANCE_SPOT/BTCUSDT"]
        assert entry["will_create_tradetick"] is False
        assert entry["raw_trade_record_count"] == 0
        assert entry["raw_lifecycle_record_count"] == 3
        assert entry["ticks_written"] == 0
        assert entry["first_trade_ts_ns"] is None
        assert entry["last_trade_ts_ns"] is None

    def test_symbol_with_trades_will_create_tradetick_true(
        self, monkeypatch, tmp_path: Path
    ):
        from nautilus_trader.model.data import TradeTick
        from nautilus_trader.model.enums import AggressorSide
        from nautilus_trader.model.identifiers import TradeId

        instrument = TestInstrumentProvider.btcusdt_binance()
        diag = _trade_diag(3)
        # Build a minimal TradeTick
        tick = TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("50000.0"),
            size=Quantity.from_str("1.0"),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId("1"),
            ts_event=1_000_000_000,
            ts_init=1_000_000_100,
        )
        self._setup_minimal(
            monkeypatch, tmp_path, instrument, diag, ticks=[tick, tick, tick]
        )
        # Override diag to reflect actual timestamp
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: (
                [tick, tick, tick],
                0,
                1_000_000_000,
                1_000_000_000,
                diag,
            ),
        )
        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        entry = report["per_symbol_trade"]["BINANCE_SPOT/BTCUSDT"]
        assert entry["will_create_tradetick"] is True
        assert entry["raw_trade_record_count"] == 3
        assert entry["ticks_written"] == 3
        assert entry["first_trade_ts_ns"] == 1_000_000_000
        assert entry["last_trade_ts_ns"] == 1_000_000_000


# ---------------------------------------------------------------------------
# per_symbol_depth diagnostics
# ---------------------------------------------------------------------------

class TestPerSymbolDepthDiagnostics:
    """per_symbol_depth fields reflect raw scan counts and conversion outcomes."""

    def test_depth_symbol_with_deltas_will_create_l2_true(
        self, monkeypatch, tmp_path: Path
    ):
        instrument = TestInstrumentProvider.btcusdt_binance()
        batch = _snapshot_deltas_batch(instrument)
        metrics = _make_dep_metrics(
            raw_record_count=10,
            depth_update_record_count=7,
            sync_state_record_count=2,
            stream_lifecycle_record_count=1,
            snapshot_seed_count=1,
            deltas=2,
            first_ts_ns=1_000_000_000,
            last_ts_ns=2_000_000_000,
        )

        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: ([], 0, None, None, _no_ticks_diag()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([batch, batch], [], metrics),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: {"BINANCE_SPOT/BTCUSDT"},
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        entry = report["per_symbol_depth"]["BINANCE_SPOT/BTCUSDT"]
        assert entry["will_create_l2"] is True
        assert entry["deltas_written"] == 2
        assert entry["depth10_written"] == 0
        assert entry["raw_record_count"] == 10
        assert entry["depth_update_record_count"] == 7
        assert entry["sync_state_record_count"] == 2
        assert entry["stream_lifecycle_record_count"] == 1
        assert entry["snapshot_seed_count"] == 1
        assert entry["first_depth_ts_ns"] == 1_000_000_000
        assert entry["last_depth_ts_ns"] == 2_000_000_000

    def test_depth_symbol_without_deltas_will_create_l2_false(
        self, monkeypatch, tmp_path: Path
    ):
        instrument = TestInstrumentProvider.btcusdt_binance()
        metrics = _make_dep_metrics(
            raw_record_count=3,
            stream_lifecycle_record_count=3,
        )

        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: ([], 0, None, None, _no_ticks_diag()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([], [], metrics),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: set(),
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        entry = report["per_symbol_depth"]["BINANCE_SPOT/BTCUSDT"]
        assert entry["will_create_l2"] is False
        assert entry["deltas_written"] == 0
        assert entry["stream_lifecycle_record_count"] == 3


# ---------------------------------------------------------------------------
# readiness_classification
# ---------------------------------------------------------------------------

class TestReadinessClassification:
    """readiness_classification section buckets are populated honestly."""

    def _make_report(self, monkeypatch, tmp_path, trade_diag, ticks, deltas, depth_metrics):
        """Run convert_date for BTCUSDT with given mocked outputs."""
        instrument = TestInstrumentProvider.btcusdt_binance()
        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: (ticks, 0, None, None, trade_diag),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: (deltas, [], depth_metrics),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: set(),
        )
        return convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )

    def test_full_ready_when_trade_and_l2(self, monkeypatch, tmp_path: Path):
        from nautilus_trader.model.data import TradeTick
        from nautilus_trader.model.enums import AggressorSide
        from nautilus_trader.model.identifiers import TradeId
        instrument = TestInstrumentProvider.btcusdt_binance()
        tick = TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("50000.0"),
            size=Quantity.from_str("1.0"),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId("1"),
            ts_event=1_000_000_000,
            ts_init=1_000_000_100,
        )
        batch = _snapshot_deltas_batch(instrument)
        report = self._make_report(
            monkeypatch, tmp_path,
            trade_diag=_trade_diag(1),
            ticks=[tick],
            deltas=[batch],
            depth_metrics=_make_dep_metrics(deltas=1, raw_record_count=2, depth_update_record_count=1),
        )
        rc = report["readiness_classification"]
        assert rc["full_ready_count"] == 1
        assert rc["l2_ready_count"] == 0
        assert rc["trade_only_count"] == 0
        assert rc["not_ready_count"] == 0
        assert len(rc["full_ready"]) == 1
        assert "BINANCE_SPOT" in rc["by_venue"]
        assert rc["by_venue"]["BINANCE_SPOT"]["full_ready_count"] == 1

    def test_l2_ready_when_depth_only(self, monkeypatch, tmp_path: Path):
        instrument = TestInstrumentProvider.btcusdt_binance()
        batch = _snapshot_deltas_batch(instrument)
        report = self._make_report(
            monkeypatch, tmp_path,
            trade_diag=_no_ticks_diag(),
            ticks=[],
            deltas=[batch],
            depth_metrics=_make_dep_metrics(deltas=1, raw_record_count=2, depth_update_record_count=1),
        )
        rc = report["readiness_classification"]
        assert rc["full_ready_count"] == 0
        assert rc["l2_ready_count"] == 1
        assert len(rc["l2_ready"]) == 1

    def test_trade_only_when_ticks_no_depth(self, monkeypatch, tmp_path: Path):
        from nautilus_trader.model.data import TradeTick
        from nautilus_trader.model.enums import AggressorSide
        from nautilus_trader.model.identifiers import TradeId
        instrument = TestInstrumentProvider.btcusdt_binance()
        tick = TradeTick(
            instrument_id=instrument.id,
            price=Price.from_str("50000.0"),
            size=Quantity.from_str("1.0"),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId("1"),
            ts_event=1_000_000_000,
            ts_init=1_000_000_100,
        )
        report = self._make_report(
            monkeypatch, tmp_path,
            trade_diag=_trade_diag(1),
            ticks=[tick],
            deltas=[],
            depth_metrics=Phase2ReplayMetrics(),
        )
        rc = report["readiness_classification"]
        assert rc["trade_only_count"] == 1
        assert len(rc["trade_only"]) == 1

    def test_not_ready_when_no_data(self, monkeypatch, tmp_path: Path):
        instrument = TestInstrumentProvider.btcusdt_binance()
        report = self._make_report(
            monkeypatch, tmp_path,
            trade_diag=_lifecycle_only_diag(),
            ticks=[],
            deltas=[],
            depth_metrics=Phase2ReplayMetrics(),
        )
        rc = report["readiness_classification"]
        assert rc["not_ready_count"] == 1
        assert len(rc["not_ready"]) == 1


# ---------------------------------------------------------------------------
# Partial overwrite guard
# ---------------------------------------------------------------------------

class TestPartialOverwriteGuard:
    """Guard refuses conversion when raw depth coverage is too low."""

    def _monkeypatch_single_symbol(self, monkeypatch, tmp_path, raw_depth_set):
        instrument = TestInstrumentProvider.btcusdt_binance()
        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: ([], 0, None, None, _no_ticks_diag()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([], [], Phase2ReplayMetrics()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda universe, date_str, channel, record_type: (
                raw_depth_set if channel == "depth_v2" else set()
            ),
        )
        # Threshold: refuse when 1+ expected symbol and ratio < 80%
        monkeypatch.setattr(convert_day_mod, "OVERWRITE_DEPTH_REFUSE_MIN_EXPECTED_SYMBOLS", 1)
        monkeypatch.setattr(convert_day_mod, "OVERWRITE_DEPTH_REFUSE_MIN_RATIO", 0.8)

    def test_refuses_by_default_when_coverage_below_threshold(
        self, monkeypatch, tmp_path: Path
    ):
        self._monkeypatch_single_symbol(monkeypatch, tmp_path, raw_depth_set=set())
        purge_called = []
        monkeypatch.setattr(
            convert_day_mod,
            "purge_catalog_date_range",
            lambda *a, **kw: purge_called.append(True),
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=tmp_path / "cat",
            emit_depth10=False,
            allow_partial_overwrite=False,
        )
        assert report["status"] == "refused_partial_raw_depth"
        assert purge_called == [], "Catalog must NOT be purged when refusing"
        assert report["conversion_integrity"]["warnings"]

    def test_allows_when_flag_set(self, monkeypatch, tmp_path: Path):
        self._monkeypatch_single_symbol(monkeypatch, tmp_path, raw_depth_set=set())
        purge_called = []
        monkeypatch.setattr(
            convert_day_mod,
            "purge_catalog_date_range",
            lambda *a, **kw: purge_called.append(True),
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=tmp_path / "cat",
            emit_depth10=False,
            allow_partial_overwrite=True,
        )
        # Should NOT refuse
        assert report["status"] != "refused_partial_raw_depth"
        # Purge WAS called because conversion proceeded
        assert purge_called, "Catalog should be purged when allow_partial_overwrite=True"

    def test_no_refuse_when_coverage_meets_threshold(
        self, monkeypatch, tmp_path: Path
    ):
        # If raw_depth_symbols == expected, ratio == 1.0, no refusal
        self._monkeypatch_single_symbol(
            monkeypatch, tmp_path, raw_depth_set={"BINANCE_SPOT/BTCUSDT"}
        )
        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=tmp_path / "cat",
            emit_depth10=False,
            allow_partial_overwrite=False,
        )
        assert report.get("status") != "refused_partial_raw_depth"

    def test_staging_never_triggers_refuse(self, monkeypatch, tmp_path: Path):
        """Staging mode skips overwrite logic entirely."""
        self._monkeypatch_single_symbol(monkeypatch, tmp_path, raw_depth_set=set())
        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=tmp_path / "cat",
            staging=True,
            emit_depth10=False,
            allow_partial_overwrite=False,
        )
        # staging=True means overwrite_enabled=False → no refusal
        assert report.get("status") != "refused_partial_raw_depth"


# ---------------------------------------------------------------------------
# conversion_integrity by-venue breakdowns
# ---------------------------------------------------------------------------

class TestConversionIntegrityByVenue:
    """conversion_integrity includes required by-venue breakdowns."""

    def test_by_venue_keys_present(self, monkeypatch, tmp_path: Path):
        instrument = TestInstrumentProvider.btcusdt_binance()
        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: ([], 0, None, None, _no_ticks_diag()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([], [], Phase2ReplayMetrics()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: set(),
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        ci = report["conversion_integrity"]
        required_keys = [
            "date_converted",
            "catalog_root_written",
            "staging",
            "emit_depth10",
            "expected_symbols_total",
            "expected_symbols_by_venue",
            "raw_trade_symbols_by_venue",
            "raw_depth_symbols_by_venue",
            "converted_trade_symbols_by_venue",
            "converted_depth_symbols_by_venue",
            "converted_depth10_symbols_by_venue",
            "missing_raw_trade_symbols_by_venue",
            "missing_raw_depth_symbols_by_venue",
            "missing_converted_trade_symbols_by_venue",
            "missing_converted_depth_symbols_by_venue",
            "missing_converted_depth10_symbols_by_venue",
        ]
        for key in required_keys:
            assert key in ci, f"Missing conversion_integrity key: {key}"

        assert "BINANCE_SPOT" in ci["expected_symbols_by_venue"]
        assert ci["expected_symbols_by_venue"]["BINANCE_SPOT"] == ["BTCUSDT"]
        assert ci["missing_raw_depth_symbols_by_venue"]["BINANCE_SPOT"] == ["BTCUSDT"]
        assert ci["missing_converted_trade_symbols_by_venue"]["BINANCE_SPOT"] == ["BTCUSDT"]

    def test_backward_compat_flat_lists_present(self, monkeypatch, tmp_path: Path):
        """Old flat-list keys are still present for backward compatibility."""
        instrument = TestInstrumentProvider.btcusdt_binance()
        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: ([], 0, None, None, _no_ticks_diag()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "convert_depth_v2",
            lambda *a, **kw: ([], [], Phase2ReplayMetrics()),
        )
        monkeypatch.setattr(
            convert_day_mod,
            "_symbols_with_raw_record_type",
            lambda *a, **kw: set(),
        )

        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21), catalog_root=tmp_path / "cat", emit_depth10=False,
        )
        ci = report["conversion_integrity"]
        # Backward-compat flat keys
        for key in ("raw_depth_symbols", "raw_trade_symbols",
                    "converted_trade_symbols", "converted_order_book_delta_symbols",
                    "converted_order_book_depth_symbols",
                    "missing_depth_after_convert", "missing_trade_after_convert",
                    "overwrite_enabled"):
            assert key in ci, f"Missing backward-compat key: {key}"
