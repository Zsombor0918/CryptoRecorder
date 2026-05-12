from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas
from nautilus_trader.model.enums import BookAction, OrderSide
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider

import convert_day as convert_day_mod
from converter.depth_phase2 import Phase2ReplayMetrics


def _parquet_name(date_str: str) -> str:
    return (
        f"{date_str}T00-00-00-000000000Z_"
        f"{date_str}T23-59-59-000000000Z.parquet"
    )


def _metadata_name() -> str:
    return (
        "1970-01-01T00-00-00-000000000Z_"
        "1970-01-01T00-00-00-000000000Z.parquet"
    )


def _write_fake_parquet(
    root: Path,
    catalog_type: str,
    instrument_id: str,
    filename: str,
    payload: bytes,
) -> Path:
    path = root / "data" / catalog_type / instrument_id / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _snapshot_deltas(instrument) -> OrderBookDeltas:
    ts_event = int(
        datetime(2026, 4, 21, tzinfo=timezone.utc).timestamp() * 1_000_000_000
    )
    ts_init = ts_event + 100
    deltas = [
        OrderBookDelta.clear(instrument.id, 100, ts_event, ts_init),
        OrderBookDelta(
            instrument.id,
            BookAction.UPDATE,
            BookOrder(
                side=OrderSide.BUY,
                price=Price.from_str("100.0"),
                size=Quantity.from_str("1.0"),
                order_id=0,
            ),
            flags=32 | 128,
            sequence=100,
            ts_event=ts_event,
            ts_init=ts_init,
        ),
    ]
    return OrderBookDeltas(instrument.id, deltas)


def _no_trade_diag() -> dict[str, int]:
    return {
        "raw_record_count": 0,
        "raw_trade_record_count": 0,
        "raw_lifecycle_record_count": 0,
        "trade_ticks_written": 0,
    }


def _patch_single_symbol_depth_conversion(monkeypatch) -> None:
    instrument = TestInstrumentProvider.btcusdt_binance()
    monkeypatch.setattr(
        convert_day_mod,
        "resolve_universe",
        lambda date_str: {"BINANCE_SPOT": ["BTCUSDT"]},
    )
    monkeypatch.setattr(convert_day_mod, "load_exchange_info", lambda venue, date_str: {})
    monkeypatch.setattr(
        convert_day_mod,
        "build_instruments",
        lambda venue, syms, einfo: [instrument],
    )
    monkeypatch.setattr(
        convert_day_mod,
        "convert_trades_with_diagnostics",
        lambda *args, **kwargs: ([], 0, None, None, _no_trade_diag()),
    )
    monkeypatch.setattr(
        convert_day_mod,
        "convert_depth_v2",
        lambda *args, **kwargs: (
            [_snapshot_deltas(instrument)],
            [],
            Phase2ReplayMetrics(
                snapshot_seed_count=1,
                delta_events_written=1,
                first_ts_ns=1_000_000_000,
                last_ts_ns=1_000_000_000,
            ),
        ),
    )
    monkeypatch.setattr(
        convert_day_mod,
        "_symbols_with_raw_record_type",
        lambda *args, **kwargs: {"BINANCE_SPOT/BTCUSDT"},
    )


def test_staged_publish_preserves_other_days_and_replaces_target_day(tmp_path: Path) -> None:
    live_root = tmp_path / "catalog"
    staging_root = tmp_path / "staging"
    iid = "BTCUSDT.BINANCE"

    day20 = _write_fake_parquet(
        live_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-20"),
        b"keep-day-20",
    )
    day21 = _write_fake_parquet(
        live_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-21"),
        b"keep-day-21",
    )
    target_live = _write_fake_parquet(
        live_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-22"),
        b"old-target-day",
    )
    target_metadata = _write_fake_parquet(
        live_root,
        "currency_pair",
        iid,
        _metadata_name(),
        b"old-metadata",
    )
    sentinel = live_root / "KEEP_ME.txt"
    sentinel.write_text("preserve")

    staged_target = _write_fake_parquet(
        staging_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-22"),
        b"new-target-day",
    )
    staged_metadata = _write_fake_parquet(
        staging_root,
        "currency_pair",
        iid,
        _metadata_name(),
        b"new-metadata",
    )

    summary = convert_day_mod._publish_staged_catalog_for_date(
        staging_dir=staging_root,
        target_root=live_root,
        target_date_str="2026-04-22",
        staged_files=[staged_target, staged_metadata],
    )

    assert day20.read_bytes() == b"keep-day-20"
    assert day21.read_bytes() == b"keep-day-21"
    assert target_live.read_bytes() == b"new-target-day"
    assert target_metadata.read_bytes() == b"new-metadata"
    assert sentinel.read_text() == "preserve"
    assert summary["replaced_live_parquet_count"] == 2
    assert summary["published_staged_parquet_count"] == 2
    assert not list(tmp_path.glob("catalog.bak*"))


def test_staging_validation_rejects_parquet_outside_requested_day(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _ReadableCatalog:
        def __init__(self, root: str):
            self.root = root

        def instruments(self):
            return []

    staging_root = tmp_path / "staging"
    _write_fake_parquet(
        staging_root,
        "trade_tick",
        "BTCUSDT.BINANCE",
        _parquet_name("2026-04-23"),
        b"wrong-day",
    )
    _write_fake_parquet(
        staging_root,
        "currency_pair",
        "BTCUSDT.BINANCE",
        _metadata_name(),
        b"metadata",
    )
    monkeypatch.setattr(convert_day_mod, "ParquetDataCatalog", _ReadableCatalog)

    with pytest.raises(convert_day_mod.StagingValidationError, match="outside requested UTC date"):
        convert_day_mod._validate_staging_catalog(staging_root, "2026-04-22")


def test_validation_failure_does_not_modify_live_catalog(monkeypatch, tmp_path: Path) -> None:
    _patch_single_symbol_depth_conversion(monkeypatch)
    live_root = tmp_path / "catalog"
    existing = _write_fake_parquet(
        live_root,
        "order_book_deltas",
        "BTCUSDT.BINANCE",
        _parquet_name("2026-04-21"),
        b"existing-live",
    )
    monkeypatch.setattr(
        convert_day_mod,
        "_validate_staging_catalog",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            convert_day_mod.StagingValidationError("synthetic validation failure")
        ),
    )

    report = convert_day_mod.convert_date(
        datetime(2026, 4, 21),
        catalog_root=live_root,
        staging=True,
        emit_depth10=False,
    )

    assert report["status"] == "staging_validation_failed"
    assert "synthetic validation failure" in report["staging_error"]
    assert existing.read_bytes() == b"existing-live"


def test_publish_failure_rolls_back_live_catalog(monkeypatch, tmp_path: Path) -> None:
    live_root = tmp_path / "catalog"
    staging_root = tmp_path / "staging"
    iid = "BTCUSDT.BINANCE"
    keep_older = _write_fake_parquet(
        live_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-21"),
        b"older-day",
    )
    replace_target = _write_fake_parquet(
        live_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-22"),
        b"old-target",
    )
    replace_metadata = _write_fake_parquet(
        live_root,
        "currency_pair",
        iid,
        _metadata_name(),
        b"old-metadata",
    )
    staged_target = _write_fake_parquet(
        staging_root,
        "trade_tick",
        iid,
        _parquet_name("2026-04-22"),
        b"new-target",
    )
    staged_metadata = _write_fake_parquet(
        staging_root,
        "currency_pair",
        iid,
        _metadata_name(),
        b"new-metadata",
    )

    real_replace = convert_day_mod.os.replace
    publish_calls = {"count": 0}

    def _flaky_replace(src, dst):
        src_path = Path(src)
        dst_path = Path(dst)
        if src_path.is_relative_to(staging_root) and dst_path.is_relative_to(live_root):
            publish_calls["count"] += 1
            if publish_calls["count"] == 2:
                raise OSError("simulated staged publish failure")
        return real_replace(src, dst)

    monkeypatch.setattr(convert_day_mod.os, "replace", _flaky_replace)

    with pytest.raises(convert_day_mod.StagingPublishError, match="rolled back"):
        convert_day_mod._publish_staged_catalog_for_date(
            staging_dir=staging_root,
            target_root=live_root,
            target_date_str="2026-04-22",
            staged_files=[staged_target, staged_metadata],
        )

    assert keep_older.read_bytes() == b"older-day"
    assert replace_target.read_bytes() == b"old-target"
    assert replace_metadata.read_bytes() == b"old-metadata"
    assert staged_target.read_bytes() == b"new-target"
    assert staged_metadata.read_bytes() == b"new-metadata"
    assert not list(tmp_path.glob("catalog.publish-backup.*"))


def test_normal_staging_never_replaces_whole_catalog_root(monkeypatch, tmp_path: Path) -> None:
    _patch_single_symbol_depth_conversion(monkeypatch)
    live_root = tmp_path / "catalog"
    sentinel = live_root / "root-sentinel.txt"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("keep-root")

    prior_day = _write_fake_parquet(
        live_root,
        "trade_tick",
        "BTCUSDT.BINANCE",
        _parquet_name("2026-04-20"),
        b"keep-prior-day",
    )

    report = convert_day_mod.convert_date(
        datetime(2026, 4, 21),
        catalog_root=live_root,
        staging=True,
        emit_depth10=False,
    )

    assert report["status"] == "ok"
    assert sentinel.read_text() == "keep-root"
    assert prior_day.read_bytes() == b"keep-prior-day"
    assert report["staging_publication"]["published_staged_parquet_count"] > 0
    assert not list(tmp_path.glob("catalog.bak*"))


def test_convert_day_help_exits_successfully() -> None:
    result = subprocess.run(
        [sys.executable, str(Path(convert_day_mod.__file__)), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--allow-partial-overwrite" in result.stdout
