"""
Integration tests for the converter pipeline (deterministic native).

These tests exercise the converter modules end-to-end using fixture data
on disk, verifying that raw trade_v2 and depth_v2 JSONL records convert
to correctly-typed Nautilus objects with canonical ordering.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest
from nautilus_trader.model.data import OrderBookDeltas, TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import convert_day as convert_day_mod
from converter.depth_phase2 import Phase2ReplayMetrics, convert_depth_v2
from converter.trades import convert_trades


# ── helpers ─────────────────────────────────────────────────────────────


def _write_jsonl(path: Path, records: list[dict]) -> None:
    """Write JSONL records to a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


def _make_depth_snapshot(
    session_id: int, session_seq: int, ts_recv_ns: int, bids: list, asks: list
) -> dict:
    return {
        "record_type": "snapshot_seed",
        "stream_session_id": session_id,
        "session_seq": session_seq,
        "raw_index": 0,
        "ts_recv_ns": ts_recv_ns,
        "lastUpdateId": 100,
        "payload": {
            "bids": bids,
            "asks": asks,
        },
    }


def _make_depth_update(
    session_id: int,
    session_seq: int,
    raw_index: int,
    ts_recv_ns: int,
    first_update_id: int,
    last_update_id: int,
    bids: list,
    asks: list,
) -> dict:
    return {
        "record_type": "depth_update",
        "stream_session_id": session_id,
        "session_seq": session_seq,
        "raw_index": raw_index,
        "ts_recv_ns": ts_recv_ns,
        "U": first_update_id,
        "u": last_update_id,
        "payload": {
            "bids": bids,
            "asks": asks,
        },
    }


def _make_trade(
    session_id: int,
    session_seq: int,
    ts_recv_ns: int,
    price: str,
    quantity: str,
    is_buyer_maker: bool,
    exchange_trade_id: int,
    ts_trade_ms: int | None = None,
) -> dict:
    return {
        "record_type": "trade",
        "trade_stream_session_id": session_id,
        "trade_session_seq": session_seq,
        "ts_recv_ns": ts_recv_ns,
        "price": price,
        "quantity": quantity,
        "is_buyer_maker": is_buyer_maker,
        "exchange_trade_id": exchange_trade_id,
        "ts_trade_ms": ts_trade_ms or (ts_recv_ns // 1_000_000),
        "market_type": "spot",
    }


def _make_lifecycle(session_id: int, ts_recv_ns: int, event: str) -> dict:
    return {
        "record_type": "trade_stream_lifecycle",
        "trade_stream_session_id": session_id,
        "ts_recv_ns": ts_recv_ns,
        "event": event,
    }


# ── trade converter integration ─────────────────────────────────────────


class TestTradeConverterIntegration:
    """Integration tests for converter.trades.convert_trades."""

    def test_canonical_ordering_across_sessions(self, tmp_path: Path, monkeypatch):
        """Trades from multiple sessions sort by (session_id, session_seq)."""
        date_str = "2026-04-21"
        venue = "BINANCE_SPOT"
        symbol = "BTCUSDT"

        # Session 2 trade written first on disk, session 1 trade second
        records = [
            _make_trade(2, 1, 2_000_000_000, "50100.0", "0.5", True, 9002),
            _make_trade(1, 1, 1_000_000_000, "50000.0", "1.0", False, 9001),
            _make_trade(1, 2, 1_100_000_000, "50050.0", "0.3", True, 9003),
        ]
        data_dir = tmp_path / venue / "trade_v2" / symbol / date_str
        _write_jsonl(data_dir / "2026-04-21T00.jsonl", records)

        monkeypatch.setattr(
            "converter.readers.DATA_ROOT", tmp_path,
        )

        instrument = TestInstrumentProvider.btcusdt_binance()
        ticks, bad, first_ts, last_ts = convert_trades(
            venue, symbol, date_str, instrument.id,
            instrument.price_precision, instrument.size_precision,
        )

        assert bad == 0
        assert len(ticks) == 3
        # Session 1 comes first, then session 2
        assert float(ticks[0].price) == 50000.0
        assert float(ticks[1].price) == 50050.0
        assert float(ticks[2].price) == 50100.0

    def test_lifecycle_markers_excluded(self, tmp_path: Path, monkeypatch):
        """Lifecycle markers are not converted to TradeTick."""
        date_str = "2026-04-21"
        venue = "BINANCE_SPOT"
        symbol = "BTCUSDT"

        records = [
            _make_lifecycle(1, 500_000_000, "connected"),
            _make_trade(1, 1, 1_000_000_000, "50000.0", "1.0", False, 9001),
            _make_lifecycle(1, 1_500_000_000, "disconnected"),
        ]
        data_dir = tmp_path / venue / "trade_v2" / symbol / date_str
        _write_jsonl(data_dir / "2026-04-21T00.jsonl", records)

        monkeypatch.setattr("converter.readers.DATA_ROOT", tmp_path)

        instrument = TestInstrumentProvider.btcusdt_binance()
        ticks, bad, _, _ = convert_trades(
            venue, symbol, date_str, instrument.id,
            instrument.price_precision, instrument.size_precision,
        )

        assert len(ticks) == 1
        assert bad == 0

    def test_aggressor_side_mapping(self, tmp_path: Path, monkeypatch):
        """is_buyer_maker True → SELLER aggressor, False → BUYER."""
        date_str = "2026-04-21"
        venue = "BINANCE_SPOT"
        symbol = "BTCUSDT"

        records = [
            _make_trade(1, 1, 1_000_000_000, "50000.0", "1.0", True, 1001),
            _make_trade(1, 2, 1_100_000_000, "50010.0", "0.5", False, 1002),
        ]
        data_dir = tmp_path / venue / "trade_v2" / symbol / date_str
        _write_jsonl(data_dir / "2026-04-21T00.jsonl", records)

        monkeypatch.setattr("converter.readers.DATA_ROOT", tmp_path)

        instrument = TestInstrumentProvider.btcusdt_binance()
        ticks, _, _, _ = convert_trades(
            venue, symbol, date_str, instrument.id,
            instrument.price_precision, instrument.size_precision,
        )

        assert ticks[0].aggressor_side == AggressorSide.SELLER
        assert ticks[1].aggressor_side == AggressorSide.BUYER


# ── depth converter integration ──────────────────────────────────────────


class TestDepthConverterIntegration:
    """Integration tests for converter.depth_phase2.convert_depth_v2."""

    def test_snapshot_plus_update_produces_deltas(self, tmp_path: Path, monkeypatch):
        """A snapshot seed followed by a valid update yields OrderBookDeltas."""
        date_str = "2026-04-21"
        venue = "BINANCE_SPOT"
        symbol = "BTCUSDT"

        records = [
            _make_depth_snapshot(
                1, 1, 1_000_000_000,
                bids=[["50000.0", "1.0"]], asks=[["50001.0", "2.0"]],
            ),
            _make_depth_update(
                1, 2, 0, 1_100_000_000,
                first_update_id=101, last_update_id=101,
                bids=[["50000.0", "1.5"]], asks=[],
            ),
        ]
        data_dir = tmp_path / venue / "depth_v2" / symbol / date_str
        _write_jsonl(data_dir / "2026-04-21T00.jsonl", records)

        monkeypatch.setattr("converter.readers.DATA_ROOT", tmp_path)

        instrument = TestInstrumentProvider.btcusdt_binance()
        deltas, depth10s, metrics = convert_depth_v2(
            venue, symbol, date_str, instrument.id,
            instrument.price_precision, instrument.size_precision,
            emit_depth10=False,
        )

        assert len(deltas) >= 1
        assert len(depth10s) == 0
        assert metrics.snapshot_seed_count >= 1
        # Every item is an OrderBookDeltas batch
        assert all(isinstance(d, OrderBookDeltas) for d in deltas)

    def test_depth10_emitted_when_enabled(self, tmp_path: Path, monkeypatch):
        """When emit_depth10=True, OrderBookDepth10 snapshots are produced."""
        date_str = "2026-04-21"
        venue = "BINANCE_SPOT"
        symbol = "BTCUSDT"

        records = [
            _make_depth_snapshot(
                1, 1, 1_000_000_000,
                bids=[["50000.0", "1.0"]], asks=[["50001.0", "2.0"]],
            ),
            _make_depth_update(
                1, 2, 0, 1_100_000_000,
                first_update_id=101, last_update_id=101,
                bids=[["50000.0", "1.5"]], asks=[],
            ),
        ]
        data_dir = tmp_path / venue / "depth_v2" / symbol / date_str
        _write_jsonl(data_dir / "2026-04-21T00.jsonl", records)

        monkeypatch.setattr("converter.readers.DATA_ROOT", tmp_path)

        instrument = TestInstrumentProvider.btcusdt_binance()
        deltas, depth10s, metrics = convert_depth_v2(
            venue, symbol, date_str, instrument.id,
            instrument.price_precision, instrument.size_precision,
            emit_depth10=True,
            depth10_interval_sec=0,  # emit on every update
        )

        assert len(deltas) >= 1
        assert len(depth10s) >= 1


# ── convert_day integration ──────────────────────────────────────────────


class TestConvertDayIntegration:
    """Integration tests for convert_day.convert_date end-to-end."""

    def test_deterministic_native_report_shape(self, monkeypatch, tmp_path: Path):
        """convert_date produces a report with architecture=deterministic_native."""
        from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
        from nautilus_trader.model.enums import BookAction, OrderSide
        from nautilus_trader.model.objects import Price, Quantity

        instrument = TestInstrumentProvider.btcusdt_binance()

        def _fake_snapshot_deltas():
            ts_e, ts_i = 1_000_000_000, 1_000_000_100
            return OrderBookDeltas(instrument.id, [
                OrderBookDelta.clear(instrument.id, 100, ts_e, ts_i),
                OrderBookDelta(
                    instrument.id, BookAction.UPDATE,
                    BookOrder(OrderSide.BUY, Price.from_str("100.0"),
                              Quantity.from_str("1.0"), 0),
                    flags=32 | 128, sequence=100,
                    ts_event=ts_e, ts_init=ts_i,
                ),
            ])

        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info",
                            lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: (
                [],
                0,
                None,
                None,
                {
                    "raw_record_count": 0,
                    "raw_trade_record_count": 0,
                    "raw_lifecycle_record_count": 0,
                    "ticks_written": 0,
                },
            ),
        )
        monkeypatch.setattr(convert_day_mod, "convert_depth_v2",
                            lambda *a, **kw: (
                                [_fake_snapshot_deltas()],
                                [],
                                Phase2ReplayMetrics(
                                    snapshot_seed_count=1,
                                    delta_events_written=1,
                                    first_ts_ns=1_000_000_000,
                                    last_ts_ns=1_000_000_000,
                                ),
                            ))

        catalog_root = tmp_path / "catalog"
        report = convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=catalog_root,
            emit_depth10=False,
        )

        assert report["architecture"] == "deterministic_native"
        assert report["status"] == "ok"
        assert "total_trades_written" in report
        assert "total_order_book_deltas_written" in report
        # No Phase 1 fields
        assert "phase" not in report
        assert "depth_mode" not in report
        assert "crossed_book_events_total" not in report

    def test_catalog_queryable_after_convert(self, monkeypatch, tmp_path: Path):
        """After convert_date, the Nautilus catalog is queryable."""
        from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
        from nautilus_trader.model.enums import BookAction, OrderSide
        from nautilus_trader.model.objects import Price, Quantity

        instrument = TestInstrumentProvider.btcusdt_binance()

        ts_e, ts_i = 1_000_000_000, 1_000_000_100
        deltas_batch = OrderBookDeltas(instrument.id, [
            OrderBookDelta.clear(instrument.id, 100, ts_e, ts_i),
            OrderBookDelta(
                instrument.id, BookAction.UPDATE,
                BookOrder(OrderSide.BUY, Price.from_str("100.0"),
                          Quantity.from_str("1.0"), 0),
                flags=32 | 128, sequence=100,
                ts_event=ts_e, ts_init=ts_i,
            ),
        ])

        monkeypatch.setattr(convert_day_mod, "resolve_universe",
                            lambda ds: {"BINANCE_SPOT": ["BTCUSDT"]})
        monkeypatch.setattr(convert_day_mod, "load_exchange_info",
                            lambda v, ds: {})
        monkeypatch.setattr(convert_day_mod, "build_instruments",
                            lambda v, s, e: [instrument])
        monkeypatch.setattr(
            convert_day_mod,
            "convert_trades_with_diagnostics",
            lambda *a, **kw: (
                [],
                0,
                None,
                None,
                {
                    "raw_record_count": 0,
                    "raw_trade_record_count": 0,
                    "raw_lifecycle_record_count": 0,
                    "ticks_written": 0,
                },
            ),
        )
        monkeypatch.setattr(convert_day_mod, "convert_depth_v2",
                            lambda *a, **kw: (
                                [deltas_batch], [],
                                Phase2ReplayMetrics(
                                    snapshot_seed_count=1,
                                    delta_events_written=1,
                                    first_ts_ns=ts_e,
                                    last_ts_ns=ts_e,
                                ),
                            ))

        catalog_root = tmp_path / "catalog"
        convert_day_mod.convert_date(
            datetime(2026, 4, 21),
            catalog_root=catalog_root,
            emit_depth10=False,
        )

        catalog = ParquetDataCatalog(str(catalog_root))
        result_deltas = catalog.order_book_deltas(
            instrument_ids=[instrument.id], batched=True,
        )
        assert len(result_deltas) >= 1
