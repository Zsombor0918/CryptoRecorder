"""End-to-end integration tests proving validate_catalog_equivalence()'s REAL
orchestration path uses the GATING, bounded-memory comparators for book
checkpoints, Depth10, fenced ranges (complete-collection digest), and
raw-to-replay logical metadata — and that every one of them contributes to
the final `report["status"]`.

Two test strategies are used, matched to what each scenario needs:

1. Catalog-content scenarios (trades, deltas, checkpoints, Depth10,
   instrument precision) monkeypatch the build steps (`_run_old_converter`,
   `_run_new_pipeline`, `_prepare_dir`) to no-ops so each test can construct
   fully controlled real Nautilus catalogs directly via
   `ParquetDataCatalog.write_data()`, then calls the real, unmodified
   `validate_catalog_equivalence()`.
2. Fenced-range and raw-to-replay-metadata scenarios use real convert_day.py
   report JSON / replay manifest fixtures (fenced ranges) or the REAL raw
   JSONL -> real replay-store pipeline (metadata), monkeypatching only the
   `_iter_sorted_raw_depth` raw-side generator for the one specific
   corruption under test — the replay (candidate) side always comes from
   the real, unmodified build.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from nautilus_trader.model.data import (
    BookOrder,
    OrderBookDelta,
    OrderBookDeltas,
    OrderBookDepth10,
    TradeTick,
)
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

import validation.validate_catalog_equivalence as vce
from converter.depth_phase2 import canonical_fence_digest
from converter.instruments import build_instruments

DATE = "2026-06-12"
VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
N_TRADES = 150  # > the legacy sampled comparator's default sample_count=100
N_DELTAS = 30
_DAY_START_NS = int(datetime.strptime(DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)


def _instrument():
    return build_instruments(VENUE, [SYMBOL], {})[0]


def _tick(instrument, *, trade_id: str, ts: int, price: str = "1.0000") -> TradeTick:
    absolute_ts = _DAY_START_NS + ts
    return TradeTick(
        instrument_id=instrument.id,
        price=Price.from_str(price),
        size=Quantity.from_str("1.0"),
        aggressor_side=AggressorSide.BUYER,
        trade_id=TradeId(trade_id),
        ts_event=absolute_ts,
        ts_init=absolute_ts,
    )


def _baseline_ticks(instrument) -> list[TradeTick]:
    return [_tick(instrument, trade_id=str(i), ts=i) for i in range(N_TRADES)]


def _baseline_deltas(instrument) -> list[OrderBookDeltas]:
    out = []
    for i in range(N_DELTAS):
        side = OrderSide.BUY if i % 2 == 0 else OrderSide.SELL
        price = f"{100 - i}.0" if side == OrderSide.BUY else f"{200 + i}.0"
        order = BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str("1.0"), order_id=0)
        absolute_ts = _DAY_START_NS + i
        out.append(
            OrderBookDeltas(
                instrument.id,
                [OrderBookDelta(instrument.id, BookAction.UPDATE, order, flags=0, sequence=i, ts_event=absolute_ts, ts_init=absolute_ts)],
            )
        )
    return out


def _depth10(instrument, *, sequence: int, ts: int, bid_price: str = "100.0") -> OrderBookDepth10:
    absolute_ts = _DAY_START_NS + ts
    bids = [BookOrder(side=OrderSide.BUY, price=Price.from_str(bid_price if i == 0 else f"{100 - i}.0"), size=Quantity.from_str("1.0"), order_id=0) for i in range(10)]
    asks = [BookOrder(side=OrderSide.SELL, price=Price.from_str(f"{101 + i}.0"), size=Quantity.from_str("1.0"), order_id=0) for i in range(10)]
    return OrderBookDepth10(
        instrument_id=instrument.id, bids=bids, asks=asks,
        bid_counts=[1] * 10, ask_counts=[1] * 10, flags=0, sequence=sequence,
        ts_event=absolute_ts, ts_init=absolute_ts,
    )


def _write_catalog(root: Path, instrument, ticks, deltas, depth10s=None) -> None:
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([instrument])
    if ticks:
        catalog.write_data(ticks)
    if deltas:
        catalog.write_data(deltas)
    if depth10s:
        catalog.write_data(depth10s)


def _write_old_report(old_catalog_root: Path, *, per_symbol_depth=None, per_symbol_fenced_ranges=None) -> None:
    report = {
        "date": DATE,
        "per_symbol_depth": per_symbol_depth
        or {f"{VENUE}/{SYMBOL}": {"snapshot_seed_count": 1, "resync_count": 0, "desync_events": 0, "fenced_ranges": 0}},
        "per_symbol_fenced_ranges": per_symbol_fenced_ranges or {f"{VENUE}/{SYMBOL}": {"canonical_count": 0, "canonical_digest": canonical_fence_digest([])}},
    }
    report_dir = old_catalog_root.parent / "convert_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"{DATE}.json").write_text(json.dumps(report))


def _write_new_manifest(new_catalog_path: Path, *, depth_diagnostics=None, fenced_ranges=None) -> None:
    manifest = {
        "depth_diagnostics": depth_diagnostics
        or {"snapshot_seeds": 1, "resyncs": 0, "desyncs": 0, "fenced_range_count": 0},
        "fenced_ranges": fenced_ranges or [],
    }
    new_catalog_path.mkdir(parents=True, exist_ok=True)
    (new_catalog_path / "manifest.json").write_text(json.dumps(manifest))


def _run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    old_ticks,
    new_ticks,
    old_deltas,
    new_deltas,
    old_depth10s=None,
    new_depth10s=None,
    old_per_symbol_depth=None,
    new_depth_diagnostics=None,
    old_per_symbol_fenced_ranges=None,
    new_fenced_ranges=None,
    new_instrument=None,
    emit_depth10=False,
) -> dict:
    instrument = _instrument()
    old_catalog_root = tmp_path / "old_catalog"
    new_catalog_root = tmp_path / "new_catalog"
    new_catalog_path = new_catalog_root / "job_validation_new"

    _write_catalog(old_catalog_root, instrument, old_ticks, old_deltas, old_depth10s)
    _write_catalog(new_catalog_path, new_instrument or instrument, new_ticks, new_deltas, new_depth10s)
    _write_old_report(old_catalog_root, per_symbol_depth=old_per_symbol_depth, per_symbol_fenced_ranges=old_per_symbol_fenced_ranges)
    _write_new_manifest(new_catalog_path, depth_diagnostics=new_depth_diagnostics, fenced_ranges=new_fenced_ranges)

    monkeypatch.setattr(vce, "_run_old_converter", lambda **kw: {"cmd": [], "returncode": 0, "stdout_tail": "", "stderr_tail": ""})
    monkeypatch.setattr(
        vce, "_run_new_pipeline",
        lambda **kw: {"replay_results": [], "catalog_result": {"status": "success"}, "catalog_path": str(new_catalog_path)},
    )
    monkeypatch.setattr(vce, "_prepare_dir", lambda path, *, overwrite: None)
    # Neither raw data_raw nor a real replay_store exists for these
    # synthetic-catalog tests, so the raw-to-replay metadata comparison
    # trivially sees 0 records on both sides and passes — it is exercised
    # separately below via the real-pipeline tests.

    return vce.validate_catalog_equivalence(
        date=DATE, symbols=[SYMBOL], venues=[VENUE],
        data_root=tmp_path / "data_raw", work_root=tmp_path / "work",
        old_catalog_root=old_catalog_root, replay_root=tmp_path / "replay_store",
        new_catalog_root=new_catalog_root, profile="full_l2", overwrite=True,
        emit_depth10=emit_depth10,
    )


def _instrument_id(instrument) -> str:
    return str(instrument.id)


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------


def test_baseline_identical_streams_pass_through_real_orchestration(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    ticks = _baseline_ticks(instrument)
    deltas = _baseline_deltas(instrument)
    report = _run(monkeypatch, tmp_path, old_ticks=list(ticks), new_ticks=list(ticks), old_deltas=list(deltas), new_deltas=list(deltas))
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["book_checkpoints"]["passed"] is True


# ---------------------------------------------------------------------------
# 1. Trade mismatch outside the old 100 sampled positions
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_trade_mismatch_beyond_legacy_sample_positions(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)
    corrupt_position = 110
    new_ticks[corrupt_position] = _tick(instrument, trade_id=str(corrupt_position), ts=corrupt_position, price="9.9999")
    report = _run(monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[])
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_orchestration_fails_on_reordered_trades(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)
    new_ticks[60] = _tick(instrument, trade_id="61", ts=60)
    new_ticks[61] = _tick(instrument, trade_id="60", ts=61)
    report = _run(monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[])
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_orchestration_fails_on_reordered_commutative_deltas(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_deltas = _baseline_deltas(instrument)
    new_deltas = _baseline_deltas(instrument)

    def _delta_at(ts: int, *, sequence: int) -> OrderBookDeltas:
        side = OrderSide.BUY if sequence % 2 == 0 else OrderSide.SELL
        price = f"{100 - sequence}.0" if side == OrderSide.BUY else f"{200 + sequence}.0"
        order = BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str("1.0"), order_id=0)
        absolute_ts = _DAY_START_NS + ts
        return OrderBookDeltas(instrument.id, [OrderBookDelta(instrument.id, BookAction.UPDATE, order, flags=0, sequence=sequence, ts_event=absolute_ts, ts_init=absolute_ts)])

    new_deltas[10] = _delta_at(10, sequence=11)
    new_deltas[11] = _delta_at(11, sequence=10)
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=old_deltas, new_deltas=new_deltas)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["order_book_deltas"]["passed"] is False


def test_real_orchestration_fails_on_extra_trade(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument) + [_tick(instrument, trade_id="extra", ts=N_TRADES)]
    report = _run(monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[])
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_orchestration_fails_on_missing_trade(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)[:-1]
    report = _run(monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[])
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_orchestration_fails_on_instrument_precision_mismatch(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    corrupted_instrument = build_instruments(VENUE, [SYMBOL], {SYMBOL: {"filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.00010000"}]}})[0]
    assert corrupted_instrument.price_precision != instrument.price_precision
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[], new_instrument=corrupted_instrument)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    assert report["comparison"]["instrument_precision"]["passed"] is False


def test_real_orchestration_fails_on_continuity_mismatch(monkeypatch, tmp_path: Path) -> None:
    old_per_symbol_depth = {f"{VENUE}/{SYMBOL}": {"snapshot_seed_count": 1, "resync_count": 2, "desync_events": 1, "fenced_ranges": 0}}
    new_depth_diagnostics = {"snapshot_seeds": 1, "resyncs": 0, "desyncs": 1, "fenced_range_count": 0}
    report = _run(
        monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[],
        old_per_symbol_depth=old_per_symbol_depth, new_depth_diagnostics=new_depth_diagnostics,
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


# ---------------------------------------------------------------------------
# Book-checkpoint gating
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_book_checkpoint_mismatch(monkeypatch, tmp_path: Path) -> None:
    """A genuine book-state value difference (not merely a reorder) that
    manifests at the reconstructed checkpoints; book_checkpoints must gate
    `passed`, never be marked non-gating."""
    instrument = _instrument()
    clear = OrderBookDelta.clear(instrument.id, 1, _DAY_START_NS, _DAY_START_NS)
    old_order = BookOrder(side=OrderSide.BUY, price=Price.from_str("100.0"), size=Quantity.from_str("1.0"), order_id=0)
    new_order = BookOrder(side=OrderSide.BUY, price=Price.from_str("100.0"), size=Quantity.from_str("2.0"), order_id=0)
    old_deltas = [OrderBookDeltas(instrument.id, [clear, OrderBookDelta(instrument.id, BookAction.UPDATE, old_order, flags=0, sequence=1, ts_event=_DAY_START_NS, ts_init=_DAY_START_NS)])]
    new_deltas = [OrderBookDeltas(instrument.id, [clear, OrderBookDelta(instrument.id, BookAction.UPDATE, new_order, flags=0, sequence=1, ts_event=_DAY_START_NS, ts_init=_DAY_START_NS)])]
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=old_deltas, new_deltas=new_deltas)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["book_checkpoints"]["passed"] is False
    assert "gating" not in report["comparison"]["by_instrument"][iid]["book_checkpoints"]
    assert report["comparison"]["book_checkpoints"] == {"passed": False}


# ---------------------------------------------------------------------------
# Depth10 gating
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_enabled_depth10_mismatch(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_depth10s = [_depth10(instrument, sequence=1, ts=1)]
    new_depth10s = [_depth10(instrument, sequence=1, ts=1, bid_price="999.0")]
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[], old_depth10s=old_depth10s, new_depth10s=new_depth10s, emit_depth10=True)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["order_book_depth10"]["passed"] is False
    assert "gating" not in report["comparison"]["by_instrument"][iid]["order_book_depth10"]


def test_real_orchestration_fails_on_depth10_reorder(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_depth10s = [_depth10(instrument, sequence=1, ts=1, bid_price="100.0"), _depth10(instrument, sequence=2, ts=2, bid_price="200.0")]
    new_depth10s = [_depth10(instrument, sequence=1, ts=1, bid_price="200.0"), _depth10(instrument, sequence=2, ts=2, bid_price="100.0")]
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[], old_depth10s=old_depth10s, new_depth10s=new_depth10s, emit_depth10=True)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_depth10_intentionally_skipped_when_disabled(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_depth10s = [_depth10(instrument, sequence=1, ts=1)]
    new_depth10s = [_depth10(instrument, sequence=1, ts=1, bid_price="999.0")]  # would fail if compared
    report = _run(monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[], old_depth10s=old_depth10s, new_depth10s=new_depth10s, emit_depth10=False)
    iid = _instrument_id(instrument)
    depth10_result = report["comparison"]["by_instrument"][iid]["order_book_depth10"]
    assert depth10_result["skipped"] is True
    assert depth10_result["passed"] is True


# ---------------------------------------------------------------------------
# Fenced ranges — complete-collection digest, gating on any difference
# ---------------------------------------------------------------------------


def _fence(n: int, *, reason: str = "resync_required") -> dict:
    return {"venue": VENUE, "symbol": SYMBOL, "start_ts_ns": n * 1000, "end_ts_ns": n * 1000 + 500, "severity": "high", "reason": reason}


def test_real_orchestration_fails_on_fenced_range_mismatch_after_first_three(monkeypatch, tmp_path: Path) -> None:
    """The first 3 fences match; only the 4th differs. A truncated
    3-example reference comparison would have passed this; the complete
    canonical digest must not."""
    reference_fences = [_fence(1), _fence(2), _fence(3), _fence(4)]
    candidate_fences = [_fence(1), _fence(2), _fence(3), _fence(4, reason="different_reason")]
    old_per_symbol_fenced_ranges = {
        f"{VENUE}/{SYMBOL}": {"canonical_count": len(reference_fences), "canonical_digest": canonical_fence_digest(reference_fences)}
    }
    report = _run(
        monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[],
        old_per_symbol_fenced_ranges=old_per_symbol_fenced_ranges, new_fenced_ranges=candidate_fences,
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(_instrument())
    assert report["comparison"]["by_instrument"][iid]["fenced_ranges"]["passed"] is False
    assert report["comparison"]["by_instrument"][iid]["fenced_ranges"]["digest_match"] is False


def test_real_orchestration_fails_on_extra_candidate_fenced_range(monkeypatch, tmp_path: Path) -> None:
    """A candidate fence the reference does not have is a semantic
    difference and must fail — extra_in_new/count mismatch is never
    treated as expected or non-gating."""
    reference_fences = [_fence(1), _fence(2), _fence(3)]
    candidate_fences = reference_fences + [_fence(99)]
    old_per_symbol_fenced_ranges = {
        f"{VENUE}/{SYMBOL}": {"canonical_count": len(reference_fences), "canonical_digest": canonical_fence_digest(reference_fences)}
    }
    report = _run(
        monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[],
        old_per_symbol_fenced_ranges=old_per_symbol_fenced_ranges, new_fenced_ranges=candidate_fences,
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(_instrument())
    fenced_cmp = report["comparison"]["by_instrument"][iid]["fenced_ranges"]
    assert fenced_cmp["passed"] is False
    assert fenced_cmp["count_match"] is False


def test_real_orchestration_passes_when_fenced_ranges_completely_match(monkeypatch, tmp_path: Path) -> None:
    reference_fences = [_fence(1), _fence(2), _fence(3), _fence(4)]
    old_per_symbol_fenced_ranges = {
        f"{VENUE}/{SYMBOL}": {"canonical_count": len(reference_fences), "canonical_digest": canonical_fence_digest(reference_fences)}
    }
    report = _run(
        monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=[], new_deltas=[],
        old_per_symbol_fenced_ranges=old_per_symbol_fenced_ranges, new_fenced_ranges=list(reference_fences),
    )
    iid = _instrument_id(_instrument())
    assert report["comparison"]["by_instrument"][iid]["fenced_ranges"]["passed"] is True


# ---------------------------------------------------------------------------
# Real-pipeline raw-to-replay metadata scenarios
# ---------------------------------------------------------------------------


def _write_clean_raw_day(data_root: Path) -> None:
    base = 1_781_222_400_000

    def _jsonl(path: Path, records: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

    _jsonl(
        data_root / VENUE / "depth_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {"record_type": "snapshot_seed", "venue": VENUE, "symbol": SYMBOL, "stream_session_id": 1, "session_seq": 0,
             "ts_recv_ns": base * 1_000_000 + 1, "ts_event_ms": base, "lastUpdateId": 100,
             "payload": {"bids": [["0.1700", "100.0"]], "asks": [["0.1710", "200.0"]]}},
            {"record_type": "depth_update", "venue": VENUE, "symbol": SYMBOL, "stream_session_id": 1, "session_seq": 1,
             "ts_recv_ns": base * 1_000_000 + 2_000_000_000, "ts_event_ms": base + 2_000,
             "U": 101, "u": 105, "pu": None, "quality_flags": {"gap": False},
             "payload": {"bids": [["0.1700", "120.0"]], "asks": [["0.1710", "180.0"]]}},
            {"record_type": "depth_update", "venue": VENUE, "symbol": SYMBOL, "stream_session_id": 1, "session_seq": 2,
             "ts_recv_ns": base * 1_000_000 + 4_000_000_000, "ts_event_ms": base + 4_000,
             "U": 106, "u": 110, "pu": None, "quality_flags": {"gap": True, "severity": "low"},
             "payload": {"bids": [["0.1698", "40.0"]], "asks": [["0.1712", "70.0"]]}},
        ],
    )
    _jsonl(
        data_root / VENUE / "trade_v2" / SYMBOL / DATE / "2026-06-12T00.jsonl",
        [
            {"record_type": "trade", "venue": VENUE, "market_type": "spot", "symbol": SYMBOL,
             "trade_stream_session_id": 1, "trade_session_seq": 1, "ts_recv_ns": base * 1_000_000 + 10,
             "ts_event_ms": base, "ts_trade_ms": base, "price": "0.17060000", "quantity": "35.20000000",
             "is_buyer_maker": True, "exchange_trade_id": 101, "native_payload": {"t": 101}},
            {"record_type": "trade", "venue": VENUE, "market_type": "spot", "symbol": SYMBOL,
             "trade_stream_session_id": 1, "trade_session_seq": 2, "ts_recv_ns": base * 1_000_000 + 20,
             "ts_event_ms": base, "ts_trade_ms": base, "price": "0.17070000", "quantity": "30.90000000",
             "is_buyer_maker": False, "exchange_trade_id": 102, "native_payload": {"t": 102}},
        ],
    )


def _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path: Path, corrupt_fn) -> dict:
    """Runs the REAL convert_day.py + REAL replay build against a clean raw
    fixture, but monkeypatches `_iter_sorted_raw_depth` to apply `corrupt_fn`
    to the list of normalized old-side depth records before yielding them —
    the replay (candidate) side is always the real, unmodified build."""
    data_root = tmp_path / "data_raw"
    _write_clean_raw_day(data_root)

    real_iter_sorted_raw_depth = vce._iter_sorted_raw_depth

    def _patched(data_root_arg, venue, symbol, date):
        records = list(real_iter_sorted_raw_depth(data_root_arg, venue, symbol, date))
        yield from corrupt_fn(records)

    monkeypatch.setattr(vce, "_iter_sorted_raw_depth", _patched)

    return vce.validate_catalog_equivalence(
        date=DATE, symbols=[SYMBOL], venues=[VENUE], data_root=data_root,
        work_root=tmp_path / "work", old_catalog_root=tmp_path / "old_catalog",
        replay_root=tmp_path / "replay_store", new_catalog_root=tmp_path / "new_catalog",
        profile="full_l2", overwrite=True,
    )


def test_real_pipeline_fails_when_quality_flag_moved_to_wrong_event_same_multiset(monkeypatch, tmp_path: Path) -> None:
    """Swap quality_flags between the two depth_update records: the overall
    multiset of flag values is unchanged, but each is now attached to the
    wrong event — a pure multiset comparison would miss this."""
    def _swap_flags(records: list[dict]) -> list[dict]:
        records = [dict(r) for r in records]
        depth_updates = [i for i, r in enumerate(records) if r["record_type"] == "depth_update"]
        assert len(depth_updates) == 2
        i, j = depth_updates
        records[i]["quality_flags"], records[j]["quality_flags"] = records[j]["quality_flags"], records[i]["quality_flags"]
        return records

    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, _swap_flags)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = f"{SYMBOL}.BINANCE"
    depth_cmp = report["comparison"]["by_instrument"][iid]["raw_to_replay_metadata"]["depth"]
    assert depth_cmp["passed"] is False


def test_real_pipeline_fails_on_changed_U_u_pu(monkeypatch, tmp_path: Path) -> None:
    def _corrupt_uupu(records: list[dict]) -> list[dict]:
        records = [dict(r) for r in records]
        for r in records:
            if r["record_type"] == "depth_update":
                r["u"] = "999999"
                break
        return records

    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, _corrupt_uupu)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_pipeline_fails_on_changed_sync_desync_resync_state(monkeypatch, tmp_path: Path) -> None:
    def _corrupt_state(records: list[dict]) -> list[dict]:
        records = [dict(r) for r in records]
        for r in records:
            if r["record_type"] == "depth_update":
                r["is_desync"] = not r["is_desync"]
                break
        return records

    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, _corrupt_state)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_pipeline_fails_on_missing_diagnostic_event(monkeypatch, tmp_path: Path) -> None:
    def _drop_one(records: list[dict]) -> list[dict]:
        return records[:-1]

    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, _drop_one)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = f"{SYMBOL}.BINANCE"
    depth_cmp = report["comparison"]["by_instrument"][iid]["raw_to_replay_metadata"]["depth"]
    assert depth_cmp["count_match"] is False


def test_real_pipeline_fails_on_extra_diagnostic_event(monkeypatch, tmp_path: Path) -> None:
    def _add_one(records: list[dict]) -> list[dict]:
        extra = dict(records[-1])
        extra["raw_index"] = 9999
        return records + [extra]

    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, _add_one)
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)


def test_real_pipeline_passes_on_unmodified_clean_day(monkeypatch, tmp_path: Path) -> None:
    """Sanity baseline for the real-pipeline metadata tests: an unmodified
    clean day must pass the raw-to-replay metadata comparison."""
    report = _run_real_pipeline_with_corrupted_old_depth(monkeypatch, tmp_path, lambda records: records)
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)


# ---------------------------------------------------------------------------
# Regression guards
# ---------------------------------------------------------------------------


def test_regression_production_path_uses_exhaustive_not_sampled_or_multiset_comparators() -> None:
    forbidden_names = (
        "compare_trade_ticks_semantic",
        "compare_order_book_deltas_semantic",
        "compare_depth10_semantic",
        "compare_book_checkpoints",
        "compare_fenced_ranges_semantic",
        "compare_quality_flags_semantic",
        "load_trade_ticks",
        "load_order_book_deltas",
        "load_order_book_depth10",
    )
    for name in forbidden_names:
        assert not hasattr(vce, name), (
            f"validation.validate_catalog_equivalence must not import '{name}' — "
            f"the acceptance path must use only the exhaustive/streaming/gating "
            f"comparators, per the issue #20 follow-up correction"
        )

    required_names = (
        "compare_trade_ticks_exhaustive",
        "compare_order_book_deltas_exhaustive",
        "compare_order_book_depth10_exhaustive",
        "compare_book_checkpoints_streaming",
        "iter_trade_ticks_windowed",
        "iter_order_book_deltas_windowed",
        "iter_order_book_depth10_windowed",
        "compare_instruments_semantic",
        "load_instruments",
        "compare_continuity_diagnostics_semantic",
        "compare_fenced_ranges_digest",
        "compare_event_metadata_exhaustive",
    )
    for name in required_names:
        assert hasattr(vce, name), (
            f"validation.validate_catalog_equivalence must import '{name}' to wire it into the real acceptance path"
        )


def test_regression_gating_comparators_are_actually_invoked_during_a_real_run(monkeypatch, tmp_path: Path) -> None:
    calls = {"trades": 0, "deltas": 0, "checkpoints": 0}
    real_trades_fn = vce.compare_trade_ticks_exhaustive
    real_deltas_fn = vce.compare_order_book_deltas_exhaustive
    real_checkpoints_fn = vce.compare_book_checkpoints_streaming

    def _spy_trades(*a, **kw):
        calls["trades"] += 1
        return real_trades_fn(*a, **kw)

    def _spy_deltas(*a, **kw):
        calls["deltas"] += 1
        return real_deltas_fn(*a, **kw)

    def _spy_checkpoints(*a, **kw):
        calls["checkpoints"] += 1
        return real_checkpoints_fn(*a, **kw)

    monkeypatch.setattr(vce, "compare_trade_ticks_exhaustive", _spy_trades)
    monkeypatch.setattr(vce, "compare_order_book_deltas_exhaustive", _spy_deltas)
    monkeypatch.setattr(vce, "compare_book_checkpoints_streaming", _spy_checkpoints)

    instrument = _instrument()
    ticks = _baseline_ticks(instrument)
    deltas = _baseline_deltas(instrument)
    report = _run(monkeypatch, tmp_path, old_ticks=list(ticks), new_ticks=list(ticks), old_deltas=list(deltas), new_deltas=list(deltas))

    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
    assert calls["trades"] > 0
    assert calls["deltas"] > 0
    assert calls["checkpoints"] > 0
