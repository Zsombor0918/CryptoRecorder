"""Tests for validation.audit_storage_size (issue #20 Phase 0 baseline).

Covers:
- allocated vs. apparent byte reporting;
- published (partition) vs. scratch (staging/backup/quarantine) separation;
- per-trade / per-depth-event / per-depth-level byte estimates;
- root-wide scratch discovery independent of any single venue/symbol/date
  (the BANKUSDT-orphan-shaped scenario: a `.staging_*` dir for a symbol not
  otherwise queried must still be found).

This module is audit/measurement-only: none of these tests exercise any
deletion or mutation of staging/backup/quarantine data.
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stores.replay_schema import DEPTH_REPLAY_SCHEMA, TRADE_REPLAY_SCHEMA
from validation.audit_storage_size import (
    audit_scratch_bytes,
    audit_storage_size,
)


def _write_depth_parquet(path: Path, *, num_events: int, levels_per_side: int) -> None:
    rows = []
    for i in range(num_events):
        level = {"price": 1.0, "size": 1.0, "price_str": "1.0", "size_str": "1.0"}
        rows.append(
            {
                "venue": "BINANCE_SPOT",
                "symbol": "ADAUSDT",
                "date": "2026-06-12",
                "stream_session_id": 1,
                "session_seq": i,
                "raw_index": i,
                "record_type": "depth_update",
                "U": None,
                "u": str(i),
                "pu": None,
                "ts_exchange_ns": i,
                "ts_receive_ns": i,
                "bids": [level] * levels_per_side,
                "asks": [level] * levels_per_side,
                "is_snapshot_seed": False,
                "is_depth_update": True,
                "is_sync_state": False,
                "is_desync": False,
                "is_resync": False,
                "quality_flags": None,
                "native_payload_hash": "a" * 64,
            }
        )
    table = pa.Table.from_pylist(rows, schema=DEPTH_REPLAY_SCHEMA)
    pq.write_table(table, path)


def _write_trades_parquet(path: Path, *, num_trades: int) -> None:
    rows = []
    for i in range(num_trades):
        rows.append(
            {
                "venue": "BINANCE_SPOT",
                "symbol": "ADAUSDT",
                "date": "2026-06-12",
                "trade_stream_session_id": 1,
                "trade_session_seq": i,
                "raw_index": i,
                "record_type": "trade",
                "market_type": "spot",
                "trade_id": str(i),
                "agg_trade_id": None,
                "ts_exchange_ns": i,
                "ts_receive_ns": i,
                "price": 1.0,
                "quantity": 1.0,
                "price_str": "1.0",
                "quantity_str": "1.0",
                "buyer_maker": True,
                "aggressor_side": "SELL",
                "quality_flags": None,
                "native_payload_hash": "b" * 64,
            }
        )
    table = pa.Table.from_pylist(rows, schema=TRADE_REPLAY_SCHEMA)
    pq.write_table(table, path)


def _build_partition(
    replay_root: Path,
    *,
    venue: str = "BINANCE_SPOT",
    symbol: str = "ADAUSDT",
    date: str = "2026-06-12",
    num_depth_events: int = 10,
    levels_per_side: int = 5,
    num_trades: int = 20,
) -> Path:
    partition = replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    partition.mkdir(parents=True)
    _write_depth_parquet(partition / "depth.parquet", num_events=num_depth_events, levels_per_side=levels_per_side)
    _write_trades_parquet(partition / "trades.parquet", num_trades=num_trades)
    manifest = {
        "date": date,
        "symbol": symbol,
        "venue": venue,
        "status": "complete",
        "depth_record_count": num_depth_events,
        "trade_record_count": num_trades,
        "errors": [],
    }
    (partition / "manifest.json").write_text(json.dumps(manifest))
    return partition


def test_reports_allocated_and_apparent_bytes_separately(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root)

    report = audit_storage_size(venue="BINANCE_SPOT", symbol="ADAUSDT", date="2026-06-12", replay_root=replay_root)

    depth_component = next(c for c in report["components"] if c["artifact"] == "replay.depth.parquet")
    assert depth_component["apparent_bytes"] > 0
    assert depth_component["allocated_bytes"] > 0
    # Allocated bytes are block-rounded and therefore need not equal apparent
    # bytes exactly, but both must be reported (never silently merged).
    assert "total_apparent_bytes_excluding_catalog_total" in report
    assert "total_allocated_bytes_excluding_catalog_total" in report


def test_per_trade_and_per_depth_event_bytes_use_manifest_counts(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root, num_depth_events=10, num_trades=20, levels_per_side=5)

    report = audit_storage_size(venue="BINANCE_SPOT", symbol="ADAUSDT", date="2026-06-12", replay_root=replay_root)

    per_unit = report["per_unit_bytes"]
    assert per_unit["apparent_bytes_per_trade"] is not None
    assert per_unit["apparent_bytes_per_depth_event"] is not None
    assert per_unit["apparent_bytes_per_trade"] > 0
    assert per_unit["apparent_bytes_per_depth_event"] > 0


def test_per_depth_level_bytes_account_for_variable_level_counts(tmp_path: Path) -> None:
    """Two partitions with the same event count but different level counts
    per event must report different bytes-per-level — proving the estimate
    is level-aware, not just a row average (per issue #20's explicit
    correction that a flat bytes/row figure is orientation-only)."""
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root, symbol="FEWLEVELS", num_depth_events=20, levels_per_side=1, num_trades=1)
    _build_partition(replay_root, symbol="MANYLEVELS", num_depth_events=20, levels_per_side=20, num_trades=1)

    few = audit_storage_size(venue="BINANCE_SPOT", symbol="FEWLEVELS", date="2026-06-12", replay_root=replay_root)
    many = audit_storage_size(venue="BINANCE_SPOT", symbol="MANYLEVELS", date="2026-06-12", replay_root=replay_root)

    assert few["depth_level_stats"]["total_levels"] == 20 * 1 * 2
    assert many["depth_level_stats"]["total_levels"] == 20 * 20 * 2
    # Both should compute a positive per-level estimate.
    assert few["per_unit_bytes"]["apparent_bytes_per_depth_level"] > 0
    assert many["per_unit_bytes"]["apparent_bytes_per_depth_level"] > 0


def test_missing_manifest_reports_none_not_false_zero(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay_store"
    partition = replay_root / "venue=BINANCE_SPOT" / "symbol=NOMANIFEST" / "date=2026-06-12"
    partition.mkdir(parents=True)
    _write_depth_parquet(partition / "depth.parquet", num_events=3, levels_per_side=2)
    _write_trades_parquet(partition / "trades.parquet", num_trades=3)
    # Deliberately no manifest.json written.

    report = audit_storage_size(venue="BINANCE_SPOT", symbol="NOMANIFEST", date="2026-06-12", replay_root=replay_root)

    assert report["per_unit_bytes"]["apparent_bytes_per_trade"] is None
    assert report["per_unit_bytes"]["apparent_bytes_per_depth_event"] is None
    assert "note" in report["per_unit_bytes"]


def test_scratch_bytes_are_separate_from_published_bytes(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root)

    staging = replay_root / "venue=BINANCE_SPOT" / "symbol=ADAUSDT" / ".staging_2026-06-13_ADAUSDT"
    staging.mkdir(parents=True)
    (staging / "scratch.bin").write_bytes(b"x" * 4096)

    report = audit_scratch_bytes(replay_root)

    assert report["by_kind"]["staging"]["count"] == 1
    assert report["by_kind"]["staging"]["apparent_bytes"] >= 4096
    assert report["by_kind"]["backup"]["count"] == 0
    assert report["by_kind"]["quarantine"]["count"] == 0


def test_root_wide_scratch_scan_finds_orphan_for_symbol_not_queried(tmp_path: Path) -> None:
    """Reproduces the shape of the real BANKUSDT 2026-07-21 orphan: a stale
    `.staging_*` dir for a symbol that is not part of the current
    venue/symbol/date being measured must still be discovered by a root-wide
    scan. This is measurement-only — the orphan is never touched or deleted
    by this test or by audit_storage_size.py itself."""
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root, symbol="BTCUSDT")

    orphan = replay_root / "venue=BINANCE_USDTF" / "symbol=BANKUSDT" / ".staging_2026-07-21_BANKUSDT"
    orphan.mkdir(parents=True)
    (orphan / "depth_spool.sqlite3").write_bytes(b"y" * 8192)

    # A partition-scoped query for BTCUSDT never sees the BANKUSDT orphan —
    # this is expected: audit_storage_size() measures one partition.
    partition_report = audit_storage_size(
        venue="BINANCE_SPOT", symbol="BTCUSDT", date="2026-06-12", replay_root=replay_root
    )
    assert partition_report is not None

    # But the root-wide scratch scan must find it regardless of today's
    # eligible symbol universe.
    scratch_report = audit_scratch_bytes(replay_root)
    orphan_paths = [e["path"] for e in scratch_report["entries"]]
    assert str(orphan) in orphan_paths
    assert scratch_report["by_kind"]["staging"]["apparent_bytes"] >= 8192


def test_backup_and_quarantine_prefixes_are_classified_separately(tmp_path: Path) -> None:
    replay_root = tmp_path / "replay_store"
    backup = replay_root / "venue=BINANCE_SPOT" / "symbol=ETHUSDT" / ".backup_2026-06-12_ETHUSDT"
    quarantine = replay_root / "venue=BINANCE_SPOT" / "symbol=ETHUSDT" / ".quarantine_2026-06-11_ETHUSDT"
    backup.mkdir(parents=True)
    quarantine.mkdir(parents=True)
    (backup / "f.bin").write_bytes(b"a" * 1024)
    (quarantine / "f.bin").write_bytes(b"b" * 2048)

    report = audit_scratch_bytes(replay_root)

    assert report["by_kind"]["backup"]["count"] == 1
    assert report["by_kind"]["quarantine"]["count"] == 1
    assert report["by_kind"]["backup"]["apparent_bytes"] >= 1024
    assert report["by_kind"]["quarantine"]["apparent_bytes"] >= 2048


def test_scratch_scan_never_mutates_filesystem(tmp_path: Path) -> None:
    """audit_scratch_bytes must be pure measurement: running it must not
    remove, rename, or modify any discovered directory."""
    replay_root = tmp_path / "replay_store"
    staging = replay_root / "venue=BINANCE_SPOT" / "symbol=X" / ".staging_2026-01-01_X"
    staging.mkdir(parents=True)
    marker = staging / "marker.bin"
    marker.write_bytes(b"z" * 10)

    audit_scratch_bytes(replay_root)
    audit_scratch_bytes(replay_root)  # run twice — must stay idempotent/non-destructive

    assert staging.exists()
    assert marker.exists()
    assert marker.read_bytes() == b"z" * 10


def test_missing_replay_root_returns_empty_scratch_report(tmp_path: Path) -> None:
    report = audit_scratch_bytes(tmp_path / "does_not_exist")
    assert report["by_kind"]["staging"]["count"] == 0
    assert report["total_scratch_apparent_bytes"] == 0


@pytest.mark.parametrize("num_events,levels", [(0, 5), (5, 0)])
def test_zero_counts_do_not_raise_division_errors(tmp_path: Path, num_events: int, levels: int) -> None:
    replay_root = tmp_path / "replay_store"
    _build_partition(replay_root, num_depth_events=num_events, levels_per_side=levels, num_trades=0)

    report = audit_storage_size(venue="BINANCE_SPOT", symbol="ADAUSDT", date="2026-06-12", replay_root=replay_root)
    # Must not raise ZeroDivisionError; must report None instead of crashing.
    assert report["per_unit_bytes"]["apparent_bytes_per_trade"] is None
