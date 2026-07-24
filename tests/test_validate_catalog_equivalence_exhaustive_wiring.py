"""End-to-end integration tests proving validate_catalog_equivalence()'s REAL
orchestration path — not the comparator helper functions called directly —
uses the exhaustive, order-preserving, bounded-memory oracle and the
instrument/continuity/fenced-range/quality-flags comparisons, and that every
one of them actually gates the final `report["status"]`.

Strategy: the build steps (`_run_old_converter`, `_run_new_pipeline`,
`_prepare_dir`) are monkeypatched to no-ops so each test can construct fully
controlled "old" (reference) and "new" (candidate) Nautilus catalogs plus a
matching old convert_day.py report and new replay manifest directly — this
gives deterministic control over exact trade/delta positions (needed to
place a difference beyond the legacy sampler's 100 positions, or to reorder
specific commutative deltas) while still exercising every line of
`validate_catalog_equivalence()`'s real comparison/aggregation code, exactly
as the installed CLI would. Only the four thin, pre-existing data-quality
collector helpers (`_collect_quality_flags_from_raw/_replay`) are stubbed per
test, since raw/replay quality_flags fixtures are orthogonal to what these
tests are proving.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from nautilus_trader.model.data import BookOrder, OrderBookDelta, OrderBookDeltas, TradeTick
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

import validation.validate_catalog_equivalence as vce
from converter.instruments import build_instruments

DATE = "2026-06-12"
VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
N_TRADES = 150  # > the legacy sampled comparator's default sample_count=100
N_DELTAS = 30
# validate_catalog_equivalence() only queries events within [start_ns, end_ns)
# for the requested UTC date — event timestamps must fall inside that
# window, not near-epoch, or the windowed loaders correctly filter them out.
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
    """Non-conflicting BUY/SELL updates at independent price levels — any
    two of these can be reordered without changing the eventual book state
    (the "commutative-looking" scenario)."""
    out = []
    for i in range(N_DELTAS):
        side = OrderSide.BUY if i % 2 == 0 else OrderSide.SELL
        price = f"{100 - i}.0" if side == OrderSide.BUY else f"{200 + i}.0"
        order = BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str("1.0"), order_id=0)
        absolute_ts = _DAY_START_NS + i
        out.append(
            OrderBookDeltas(
                instrument.id,
                [
                    OrderBookDelta(
                        instrument.id,
                        BookAction.UPDATE,
                        order,
                        flags=0,
                        sequence=i,
                        ts_event=absolute_ts,
                        ts_init=absolute_ts,
                    )
                ],
            )
        )
    return out


def _write_catalog(root: Path, instrument, ticks, deltas) -> None:
    catalog = ParquetDataCatalog(str(root))
    catalog.write_data([instrument])
    if ticks:
        catalog.write_data(ticks)
    if deltas:
        catalog.write_data(deltas)


def _write_old_report(old_catalog_root: Path, *, per_symbol_depth=None, per_symbol_fenced_ranges=None) -> None:
    report = {
        "date": DATE,
        "per_symbol_depth": per_symbol_depth
        or {f"{VENUE}/{SYMBOL}": {"snapshot_seed_count": 1, "resync_count": 0, "desync_events": 0, "fenced_ranges": 0}},
        "per_symbol_fenced_ranges": per_symbol_fenced_ranges or {},
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
    old_per_symbol_depth=None,
    new_depth_diagnostics=None,
    old_per_symbol_fenced_ranges=None,
    new_fenced_ranges=None,
    old_quality_flags=None,
    new_quality_flags=None,
    new_instrument=None,
) -> dict:
    instrument = _instrument()
    old_catalog_root = tmp_path / "old_catalog"
    new_catalog_root = tmp_path / "new_catalog"
    new_catalog_path = new_catalog_root / "job_validation_new"

    _write_catalog(old_catalog_root, instrument, old_ticks, old_deltas)
    _write_catalog(new_catalog_path, new_instrument or instrument, new_ticks, new_deltas)
    _write_old_report(
        old_catalog_root,
        per_symbol_depth=old_per_symbol_depth,
        per_symbol_fenced_ranges=old_per_symbol_fenced_ranges,
    )
    _write_new_manifest(new_catalog_path, depth_diagnostics=new_depth_diagnostics, fenced_ranges=new_fenced_ranges)

    monkeypatch.setattr(
        vce,
        "_run_old_converter",
        lambda **kw: {"cmd": [], "returncode": 0, "stdout_tail": "", "stderr_tail": ""},
    )
    monkeypatch.setattr(
        vce,
        "_run_new_pipeline",
        lambda **kw: {
            "replay_results": [],
            "catalog_result": {"status": "success"},
            "catalog_path": str(new_catalog_path),
        },
    )
    # Don't wipe the catalogs/reports we just pre-built.
    monkeypatch.setattr(vce, "_prepare_dir", lambda path, *, overwrite: None)
    monkeypatch.setattr(
        vce,
        "_collect_quality_flags_from_raw",
        lambda data_root, venue, symbol, date: old_quality_flags if old_quality_flags is not None else [],
    )
    monkeypatch.setattr(
        vce,
        "_collect_quality_flags_from_replay",
        lambda replay_root, venue, symbol, date: new_quality_flags if new_quality_flags is not None else [],
    )

    return vce.validate_catalog_equivalence(
        date=DATE,
        symbols=[SYMBOL],
        venues=[VENUE],
        data_root=tmp_path / "data_raw",
        work_root=tmp_path / "work",
        old_catalog_root=old_catalog_root,
        replay_root=tmp_path / "replay_store",
        new_catalog_root=new_catalog_root,
        profile="full_l2",
        overwrite=True,
    )


def _instrument_id(instrument) -> str:
    return str(instrument.id)


# ---------------------------------------------------------------------------
# Baseline: everything identical must pass end-to-end through the real path.
# ---------------------------------------------------------------------------


def test_baseline_identical_streams_pass_through_real_orchestration(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    ticks = _baseline_ticks(instrument)
    deltas = _baseline_deltas(instrument)
    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=list(ticks),
        new_ticks=list(ticks),
        old_deltas=list(deltas),
        new_deltas=list(deltas),
        old_quality_flags=["ok"],
        new_quality_flags=["ok"],
    )
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["trade_ticks"]["passed"] is True
    assert report["comparison"]["by_instrument"][iid]["order_book_deltas"]["passed"] is True
    assert report["comparison"]["instrument_precision"]["passed"] is True


# ---------------------------------------------------------------------------
# 1. Trade mismatch outside the old 100 sampled positions
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_trade_mismatch_beyond_legacy_sample_positions(
    monkeypatch, tmp_path: Path
) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)
    corrupt_position = 110  # beyond compare_trade_ticks_semantic's default sample_count=100
    new_ticks[corrupt_position] = _tick(
        instrument, trade_id=str(corrupt_position), ts=corrupt_position, price="9.9999"
    )

    report = _run(
        monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[]
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    trades_cmp = report["comparison"]["by_instrument"][iid]["trade_ticks"]
    assert trades_cmp["passed"] is False
    assert any(m["position"] == corrupt_position for m in trades_cmp["position_mismatches"])


# ---------------------------------------------------------------------------
# 2. Reordered trades
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_reordered_trades(monkeypatch, tmp_path: Path) -> None:
    """Nautilus's ParquetDataCatalog enforces monotonically increasing
    `ts_init` at write time (it raises ValueError otherwise), so a literal
    object swap between two ticks cannot be written to a real catalog at
    all — true "same timestamps, different arrival order" reordering
    cannot be represented on disk. What a real bug of this shape actually
    looks like at the catalog level is: the *content* (trade_id/price) at
    two adjacent timestamp slots is swapped relative to the reference,
    while timestamps remain monotonic — this is the representable,
    real-catalog form of "reordering" and is exactly what this test
    constructs and what the exhaustive positional comparator must catch."""
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)
    new_ticks[60] = _tick(instrument, trade_id="61", ts=60)  # content from position 61 at ts=60
    new_ticks[61] = _tick(instrument, trade_id="60", ts=61)  # content from position 60 at ts=61

    report = _run(
        monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[]
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    trades_cmp = report["comparison"]["by_instrument"][iid]["trade_ticks"]
    assert trades_cmp["passed"] is False
    assert any(m["position"] in (60, 61) for m in trades_cmp["position_mismatches"])


# ---------------------------------------------------------------------------
# 3. Reordered commutative-looking depth deltas
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_reordered_commutative_deltas(monkeypatch, tmp_path: Path) -> None:
    """Same real-catalog monotonic-`ts_init` constraint as the trade-
    reordering test above: swap the *content* (side/price/sequence) of two
    independent, non-conflicting BUY/SELL updates between adjacent
    timestamp slots, rather than swapping whole objects. Applying either
    ordering to the book yields the identical final state (independent
    price levels), which is exactly the "commutative-looking" scenario the
    exhaustive comparator — unlike deterministic book-state checkpoints —
    must still catch as a positional divergence."""
    instrument = _instrument()
    old_deltas = _baseline_deltas(instrument)
    new_deltas = _baseline_deltas(instrument)

    def _delta_at(ts: int, *, sequence: int) -> OrderBookDeltas:
        side = OrderSide.BUY if sequence % 2 == 0 else OrderSide.SELL
        price = f"{100 - sequence}.0" if side == OrderSide.BUY else f"{200 + sequence}.0"
        order = BookOrder(side=side, price=Price.from_str(price), size=Quantity.from_str("1.0"), order_id=0)
        absolute_ts = _DAY_START_NS + ts
        return OrderBookDeltas(
            instrument.id,
            [
                OrderBookDelta(
                    instrument.id, BookAction.UPDATE, order, flags=0,
                    sequence=sequence, ts_event=absolute_ts, ts_init=absolute_ts,
                )
            ],
        )

    new_deltas[10] = _delta_at(10, sequence=11)  # content from index 11 at ts=10
    new_deltas[11] = _delta_at(11, sequence=10)  # content from index 10 at ts=11

    report = _run(
        monkeypatch, tmp_path, old_ticks=[], new_ticks=[], old_deltas=old_deltas, new_deltas=new_deltas
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    deltas_cmp = report["comparison"]["by_instrument"][iid]["order_book_deltas"]
    assert deltas_cmp["passed"] is False
    assert deltas_cmp["position_mismatches"]
    # The reordering does not change the final book state, so the
    # non-gating checkpoint diagnostic legitimately still matches — this is
    # exactly why the exhaustive comparison (not checkpoints) must gate.
    assert report["comparison"]["by_instrument"][iid]["book_checkpoints"]["passed"] is True


# ---------------------------------------------------------------------------
# 4. Extra / missing events
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_extra_trade(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument) + [_tick(instrument, trade_id="extra", ts=N_TRADES)]

    report = _run(
        monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[]
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["trade_ticks"]["trade_count_match"] is False


def test_real_orchestration_fails_on_missing_trade(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    old_ticks = _baseline_ticks(instrument)
    new_ticks = _baseline_ticks(instrument)[:-1]

    report = _run(
        monkeypatch, tmp_path, old_ticks=old_ticks, new_ticks=new_ticks, old_deltas=[], new_deltas=[]
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(instrument)
    assert report["comparison"]["by_instrument"][iid]["trade_ticks"]["trade_count_match"] is False


# ---------------------------------------------------------------------------
# 5. Instrument precision/increment mismatch
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_instrument_precision_mismatch(monkeypatch, tmp_path: Path) -> None:
    instrument = _instrument()
    corrupted_instrument = build_instruments(
        VENUE,
        [SYMBOL],
        {SYMBOL: {"filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.00010000"}]}},
    )[0]
    assert corrupted_instrument.price_precision != instrument.price_precision, (
        "sanity check: the corrupted instrument must actually have a "
        "different price_precision, or this test proves nothing"
    )

    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=[],
        new_ticks=[],
        old_deltas=[],
        new_deltas=[],
        new_instrument=corrupted_instrument,
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    assert report["comparison"]["instrument_precision"]["passed"] is False
    assert report["comparison"]["instrument_precision"]["precision_mismatches"]


# ---------------------------------------------------------------------------
# 6. Continuity mismatch
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_continuity_mismatch(monkeypatch, tmp_path: Path) -> None:
    old_per_symbol_depth = {
        f"{VENUE}/{SYMBOL}": {"snapshot_seed_count": 1, "resync_count": 2, "desync_events": 1, "fenced_ranges": 0}
    }
    new_depth_diagnostics = {"snapshot_seeds": 1, "resyncs": 0, "desyncs": 1, "fenced_range_count": 0}

    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=[],
        new_ticks=[],
        old_deltas=[],
        new_deltas=[],
        old_per_symbol_depth=old_per_symbol_depth,
        new_depth_diagnostics=new_depth_diagnostics,
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(_instrument())
    continuity_cmp = report["comparison"]["by_instrument"][iid]["continuity_diagnostics"]
    assert continuity_cmp["passed"] is False
    assert "resync_count" in continuity_cmp["field_mismatches"]


# ---------------------------------------------------------------------------
# 7. Fenced-range mismatch
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_fenced_range_mismatch(monkeypatch, tmp_path: Path) -> None:
    old_per_symbol_fenced_ranges = {
        f"{VENUE}/{SYMBOL}": {
            "fenced_ranges": 1,
            "examples": [
                {
                    "venue": VENUE,
                    "symbol": SYMBOL,
                    "start_ts_ns": 100,
                    "end_ts_ns": 200,
                    "reason": "resync_required",
                }
            ],
        }
    }
    # New side reports the same fenced_range_count so continuity passes,
    # but does not reproduce the specific fence content — the candidate's
    # per-fence list is missing the reference example.
    new_depth_diagnostics = {"snapshot_seeds": 1, "resyncs": 0, "desyncs": 0, "fenced_range_count": 1}

    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=[],
        new_ticks=[],
        old_deltas=[],
        new_deltas=[],
        old_per_symbol_fenced_ranges=old_per_symbol_fenced_ranges,
        new_depth_diagnostics=new_depth_diagnostics,
        new_fenced_ranges=[],  # deliberately empty — missing the reference example
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(_instrument())
    fenced_cmp = report["comparison"]["by_instrument"][iid]["fenced_ranges"]
    assert fenced_cmp["gating_passed"] is False
    assert fenced_cmp["missing_in_new"]


# ---------------------------------------------------------------------------
# 8. Quality-state mismatch
# ---------------------------------------------------------------------------


def test_real_orchestration_fails_on_quality_flags_mismatch(monkeypatch, tmp_path: Path) -> None:
    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=[],
        new_ticks=[],
        old_deltas=[],
        new_deltas=[],
        old_quality_flags=['{"gap": true}', None, None],
        new_quality_flags=[None, None, None],  # replay lost the gap flag
    )
    assert report["status"] == "failed", json.dumps(report, indent=2, default=str)
    iid = _instrument_id(_instrument())
    quality_cmp = report["comparison"]["by_instrument"][iid]["quality_flags"]
    assert quality_cmp["passed"] is False
    assert quality_cmp["mismatches"]


# ---------------------------------------------------------------------------
# Regression guard: the production path must not be switchable back to the
# sampled/multiset comparators or full-day list loaders without this test
# failing.
# ---------------------------------------------------------------------------


def test_regression_production_path_uses_exhaustive_not_sampled_or_multiset_comparators() -> None:
    """Static guard: validate_catalog_equivalence.py must import and be able
    to call the exhaustive comparators and windowed loaders, and must NOT
    import the sampled trade comparator, the multiset delta comparator, or
    the full-day list loaders that produced them. If a future change
    "switches back" to the old functions by re-adding these imports, this
    test fails immediately without needing to run a full scenario."""
    forbidden_names = (
        "compare_trade_ticks_semantic",
        "compare_order_book_deltas_semantic",
        "load_trade_ticks",
    )
    for name in forbidden_names:
        assert not hasattr(vce, name), (
            f"validation.validate_catalog_equivalence must not import "
            f"'{name}' — the acceptance path must use the exhaustive/"
            f"windowed comparators only, per the issue #20 follow-up "
            f"correction"
        )

    required_names = (
        "compare_trade_ticks_exhaustive",
        "compare_order_book_deltas_exhaustive",
        "iter_trade_ticks_windowed",
        "iter_order_book_deltas_windowed",
        "compare_instruments_semantic",
        "load_instruments",
        "compare_continuity_diagnostics_semantic",
        "compare_fenced_ranges_semantic",
        "compare_quality_flags_semantic",
    )
    for name in required_names:
        assert hasattr(vce, name), (
            f"validation.validate_catalog_equivalence must import '{name}' "
            f"to wire it into the real acceptance path"
        )


def test_regression_exhaustive_comparators_are_actually_invoked_during_a_real_run(
    monkeypatch, tmp_path: Path
) -> None:
    """Beyond the static import guard above, prove the exhaustive
    comparators are genuinely *called* (not merely importable-but-unused)
    during a real validate_catalog_equivalence() invocation, by wrapping
    them with call-counting spies before running the baseline scenario."""
    calls = {"trades": 0, "deltas": 0}
    real_trades_fn = vce.compare_trade_ticks_exhaustive
    real_deltas_fn = vce.compare_order_book_deltas_exhaustive

    def _spy_trades(*args, **kwargs):
        calls["trades"] += 1
        return real_trades_fn(*args, **kwargs)

    def _spy_deltas(*args, **kwargs):
        calls["deltas"] += 1
        return real_deltas_fn(*args, **kwargs)

    monkeypatch.setattr(vce, "compare_trade_ticks_exhaustive", _spy_trades)
    monkeypatch.setattr(vce, "compare_order_book_deltas_exhaustive", _spy_deltas)

    instrument = _instrument()
    ticks = _baseline_ticks(instrument)
    deltas = _baseline_deltas(instrument)
    report = _run(
        monkeypatch,
        tmp_path,
        old_ticks=list(ticks),
        new_ticks=list(ticks),
        old_deltas=list(deltas),
        new_deltas=list(deltas),
    )

    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
    assert calls["trades"] > 0, "compare_trade_ticks_exhaustive was never called during a real run"
    assert calls["deltas"] > 0, "compare_order_book_deltas_exhaustive was never called during a real run"
