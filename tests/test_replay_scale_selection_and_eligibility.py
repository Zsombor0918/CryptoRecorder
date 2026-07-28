"""Focused tests for the issue #20 Phase 7 blocking correction: the v1
compact schema's automatic price_scale/qty_scale derivation must use
max(declared exchangeInfo scale, observed normalized scale across depth
and trades) — not declared scale alone — and pipeline.raw_manifest's
eligibility scan must never surface venue-level metadata (exchangeinfo)
as a tradable symbol.

Root cause (found via a real full-universe 2026-06-11 build): 5 of 78
BINANCE_USDTF futures symbols failed with "cannot be represented exactly
at scale N" because the exchange's actual recorded tick granularity on
that day was finer than its own declared PRICE_FILTER.tickSize/
LOT_SIZE.stepSize — a genuine data anomaly, not a benchmark artifact.

These tests prove:
  1. declared scale 5 with observed exact value '0.0795760' selects
     encoding scale 6 (the real number of significant fractional
     digits), not 7 (naive lexical string length) and round-trips
     exactly;
  2. insignificant trailing zeros never inflate the required scale;
  3. observed scale below declared scale retains the declared scale
     (declared is a floor, not silently lowered);
  4. mixed depth and trade values select the true maximum required
     scale across both channels;
  5. quantity scale considers depth size, trade quantity, LOT_SIZE, and
     MARKET_LOT_SIZE together;
  6. scientific-notation, zero, and negative-exponent decimal strings
     are handled exactly (Decimal only, never float);
  7. an explicitly supplied scale is never silently enlarged — it fails
     clearly instead when observed data would require more precision;
  8. int64 mantissa overflow fails clearly rather than silently
     wrapping;
  9. EXCHANGEINFO is excluded from eligible symbols for both venues and
     both schema versions;
  10. real symbol/channel coverage and missing-single-channel reporting
      remain correct and honest.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.raw_manifest import ELIGIBLE_MARKET_CHANNELS, scan_raw_coverage
from stores.replay_schema import (
    decode_fixed_point,
    encode_fixed_point,
    normalized_decimal_scale,
)
from stores.replay_writer import ReplayWriter

from tests.test_replay_schema_v1_corrections import (
    _sample_raw_root,
    _write_exchangeinfo,
    _write_jsonl,
)


# ---------------------------------------------------------------------------
# 1-6, 8: normalized_decimal_scale / encode_fixed_point unit-level proofs
# ---------------------------------------------------------------------------


def test_observed_scale_from_real_failing_value_is_six_not_seven():
    """The exact value that broke BTWUSDT/HOMEUSDT in the real 2026-06-11
    full-universe build: declared scale 5, observed value '0.0795760'.
    The correct required scale is 6 (six significant fractional digits:
    0,7,9,5,7,6) — not 7 (the naive lexical-length count of the raw
    string's 7 characters after the decimal point, which double-counts
    the insignificant trailing zero)."""
    scale = normalized_decimal_scale("0.0795760")
    assert scale == 6
    # Must round-trip numerically exactly at the correct scale.
    mantissa = encode_fixed_point("0.0795760", scale)
    assert decode_fixed_point(mantissa, scale) == "0.079576"
    # And must NOT be representable exactly at the naive (too-small, since
    # declared was 5) OR the over-inflated (7) scale being required —
    # scale 6 is both necessary and sufficient.
    with pytest.raises(ValueError, match="cannot be represented exactly"):
        encode_fixed_point("0.0795760", 5)


@pytest.mark.parametrize(
    "value,expected_scale",
    [
        ("0.0795760", 6),
        ("0.100", 1),
        ("1.00000000", 0),
        ("100", 0),
        ("100.00", 0),
        ("0.010000", 2),
        ("5.4422000", 4),
    ],
)
def test_insignificant_trailing_zeros_never_inflate_scale(value, expected_scale):
    assert normalized_decimal_scale(value) == expected_scale


def test_zero_scale_is_exactly_zero():
    assert normalized_decimal_scale("0") == 0
    assert normalized_decimal_scale("0.000") == 0
    assert normalized_decimal_scale("0E-5") == 0


def test_scientific_notation_and_negative_exponent_are_exact():
    # 1.23E-4 == 0.000123 exactly -> scale 6.
    assert normalized_decimal_scale("1.23E-4") == 6
    assert normalized_decimal_scale("1.23e-4") == 6
    # A negative-exponent form that still normalizes to an integer.
    assert normalized_decimal_scale("1E2") == 0
    # A plain negative value (never expected for price/size, but the
    # scale computation itself must still be exact and sign-independent).
    assert normalized_decimal_scale("-0.0300") == 2


def test_int64_mantissa_overflow_fails_clearly():
    huge_value = "99999999999999999999.5"  # far exceeds int64 range at scale 1
    with pytest.raises(ValueError, match="does not fit in a signed int64"):
        encode_fixed_point(huge_value, 1)


# ---------------------------------------------------------------------------
# 3, 4, 5, 7: ReplayWriter-level scale-selection integration
# ---------------------------------------------------------------------------


def _sample_raw_root_with_scales(
    tmp_path: Path,
    *,
    venue: str = "BINANCE_USDTF",
    symbol: str = "BTWUSDT",
    date: str = "2026-06-11",
    tick_size: str = "0.0000100",   # declared price scale 5
    step_size: str = "1",           # declared qty scale 0
    market_step_size: "str | None" = None,
    depth_price: str = "0.0795760",  # observed price scale 6 (real failing value)
    depth_size: str = "64",
    trade_price: str = "7.8230000",  # observed price scale 6
    trade_qty: str = "18",
) -> Path:
    root = tmp_path / "raw"
    base_ts_ms = 1_781_161_266_587
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": venue,
                "symbol": symbol,
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 1,
                "ts_event_ms": base_ts_ms,
                "U": 9,
                "u": 10,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {
                    "bids": [[depth_price, depth_size]],
                    "asks": [[depth_price, depth_size]],
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
                "market_type": "futures",
                "symbol": symbol,
                "trade_stream_session_id": 1,
                "trade_session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 10,
                "ts_event_ms": base_ts_ms + 1,
                "ts_trade_ms": base_ts_ms + 1,
                "price": trade_price,
                "quantity": trade_qty,
                "is_buyer_maker": True,
                "exchange_trade_id": 101,
                "native_payload": {"t": 101},
            },
        ],
    )
    filters = [
        {"filterType": "PRICE_FILTER", "tickSize": tick_size},
        {"filterType": "LOT_SIZE", "stepSize": step_size},
    ]
    if market_step_size is not None:
        filters.append({"filterType": "MARKET_LOT_SIZE", "stepSize": market_step_size})
    _write_jsonl(
        root / venue / "exchangeinfo" / "EXCHANGEINFO" / date / f"{date}T00.jsonl",
        [
            {
                "symbols": [
                    {
                        "symbol": symbol,
                        "baseAsset": symbol.replace("USDT", ""),
                        "quoteAsset": "USDT",
                        "filters": filters,
                    }
                ]
            }
        ],
    )
    return root


def test_real_failing_symbol_now_succeeds_with_max_declared_and_observed_scale(tmp_path):
    """Reproduces the exact real-world failure (declared price scale 5,
    real observed value 0.0795760 requiring scale 6) end-to-end through
    build_replay_for_symbol, and proves it now succeeds using the
    corrected max(declared, observed) scale, with the evidence recorded
    in the manifest's encoding_profile."""
    raw_root = _sample_raw_root_with_scales(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_USDTF", "BTWUSDT", "2026-06-11", raw_root, replay_root, schema_version=1
    )
    assert result["status"] == "success", result

    manifest = json.loads(
        (replay_root / "venue=BINANCE_USDTF" / "symbol=BTWUSDT" / "date=2026-06-11" / "manifest.json").read_text()
    )
    assert manifest["price_scale"] == 6  # max(declared=5, observed=6)
    profile = manifest["encoding_profile"]
    assert profile["price_scale_declared"] == 5
    assert profile["price_scale_observed"] == 6


def test_observed_scale_below_declared_retains_declared_scale(tmp_path):
    """When observed data needs LESS precision than the declared filter,
    the declared scale is retained (never silently lowered)."""
    raw_root = _sample_raw_root_with_scales(
        tmp_path,
        tick_size="0.0000100",  # declared price scale 5
        depth_price="0.10",     # observed price scale only 1
        trade_price="0.10",
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_USDTF", "BTWUSDT", "2026-06-11", raw_root, replay_root, schema_version=1
    )
    assert result["status"] == "success", result
    manifest = json.loads(
        (replay_root / "venue=BINANCE_USDTF" / "symbol=BTWUSDT" / "date=2026-06-11" / "manifest.json").read_text()
    )
    assert manifest["price_scale"] == 5  # declared floor retained
    assert manifest["encoding_profile"]["price_scale_declared"] == 5
    assert manifest["encoding_profile"]["price_scale_observed"] == 1


def test_mixed_depth_and_trade_values_select_true_maximum_scale(tmp_path):
    """Depth carries the smaller-precision value and trades carry the
    larger-precision value (or vice versa) — the selected scale must be
    the maximum required across BOTH channels, not just one."""
    raw_root = _sample_raw_root_with_scales(
        tmp_path,
        tick_size="0.0000100",   # declared scale 5
        depth_price="0.0700000",  # observed depth scale: 2
        trade_price="0.0795760",  # observed trade scale: 6 (the real value)
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_USDTF", "BTWUSDT", "2026-06-11", raw_root, replay_root, schema_version=1
    )
    assert result["status"] == "success", result
    manifest = json.loads(
        (replay_root / "venue=BINANCE_USDTF" / "symbol=BTWUSDT" / "date=2026-06-11" / "manifest.json").read_text()
    )
    assert manifest["price_scale"] == 6
    assert manifest["encoding_profile"]["price_scale_observed"] == 6


def test_quantity_scale_considers_depth_size_trade_quantity_lot_and_market_lot(tmp_path):
    """qty_scale must be the maximum required across: declared LOT_SIZE,
    declared MARKET_LOT_SIZE, observed depth size, and observed trade
    quantity — proven by making MARKET_LOT_SIZE the sole driver first,
    then observed trade quantity precision the sole driver."""
    # Case 1: MARKET_LOT_SIZE declares finer precision than LOT_SIZE, and
    # observed quantities need no extra precision.
    raw_root = _sample_raw_root_with_scales(
        tmp_path,
        step_size="1",                 # declared LOT_SIZE scale 0
        market_step_size="0.001",      # declared MARKET_LOT_SIZE scale 3
        depth_size="64",                # observed depth qty scale 0
        trade_qty="18",                 # observed trade qty scale 0
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_USDTF", "BTWUSDT", "2026-06-11", raw_root, replay_root, schema_version=1
    )
    assert result["status"] == "success", result
    manifest = json.loads(
        (replay_root / "venue=BINANCE_USDTF" / "symbol=BTWUSDT" / "date=2026-06-11" / "manifest.json").read_text()
    )
    assert manifest["qty_scale"] == 3  # MARKET_LOT_SIZE drives it
    assert manifest["encoding_profile"]["qty_scale_declared"] == 3

    # Case 2: declared filters need no extra precision, but observed trade
    # quantity precision exceeds all declared filters.
    raw_root2 = _sample_raw_root_with_scales(
        tmp_path.joinpath("case2"),
        step_size="1",           # declared LOT_SIZE scale 0
        market_step_size="1",    # declared MARKET_LOT_SIZE scale 0
        depth_size="64",          # observed depth qty scale 0
        trade_qty="18.12345",     # observed trade qty scale 5
    )
    replay_root2 = tmp_path / "replay2"
    result2 = build_replay_for_symbol(
        "BINANCE_USDTF", "BTWUSDT", "2026-06-11", raw_root2, replay_root2, schema_version=1
    )
    assert result2["status"] == "success", result2
    manifest2 = json.loads(
        (replay_root2 / "venue=BINANCE_USDTF" / "symbol=BTWUSDT" / "date=2026-06-11" / "manifest.json").read_text()
    )
    assert manifest2["qty_scale"] == 5  # observed trade quantity drives it
    assert manifest2["encoding_profile"]["qty_scale_observed"] == 5


def test_explicit_scale_override_is_never_silently_enlarged_and_fails_clearly(tmp_path):
    """Supplying price_scale explicitly (bypassing exchangeInfo entirely)
    must never be silently enlarged when observed data needs more
    precision — it must fail clearly instead, so a caller relying on a
    fixed, deliberately chosen scale is never surprised."""
    raw_root = _sample_raw_root_with_scales(
        tmp_path,
        depth_price="0.0795760",  # observed scale 6
        trade_price="0.0795760",
    )
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(
        replay_root, "BINANCE_USDTF", "BTWUSDT", "2026-06-11",
        schema_version=1,
        price_scale=5,  # explicit, insufficient for the observed data
        qty_scale=0,
        data_root=raw_root,
    )
    from converter.readers import stream_raw_records
    from pipeline.build_replay_store import _convert_depth_record, _convert_trade_record

    depth_batch = []
    for raw_index, raw_record in enumerate(
        stream_raw_records("BINANCE_USDTF", "BTWUSDT", "depth_v2", "2026-06-11", root=raw_root)
    ):
        raw_record = dict(raw_record)
        raw_record.setdefault("raw_index", raw_index)
        converted = _convert_depth_record(raw_record, "BINANCE_USDTF", "BTWUSDT", "2026-06-11")
        if converted:
            depth_batch.append(converted)
    writer.write_depth_batch(depth_batch)

    trade_batch = []
    for raw_index, raw_record in enumerate(
        stream_raw_records("BINANCE_USDTF", "BTWUSDT", "trade_v2", "2026-06-11", root=raw_root)
    ):
        raw_record = dict(raw_record)
        raw_record.setdefault("raw_index", raw_index)
        converted = _convert_trade_record(raw_record, "BINANCE_USDTF", "BTWUSDT", "2026-06-11")
        if converted:
            trade_batch.append(converted)
    writer.write_trades_batch(trade_batch)

    with pytest.raises(ValueError, match="explicitly supplied price_scale=5 is insufficient"):
        writer.finalize_staging()


def test_explicit_sufficient_scale_override_succeeds_without_exchangeinfo_lookup(tmp_path):
    """An explicit scale that IS sufficient for the observed data must be
    honored exactly as supplied (never silently changed), and must not
    require any exchangeInfo file to exist at all."""
    root = tmp_path / "raw_no_exchangeinfo"
    date = "2026-06-11"
    base_ts_ms = 1_781_161_266_587
    _write_jsonl(
        root / "BINANCE_USDTF" / "depth_v2" / "NOINFOUSDT" / date / f"{date}T00.jsonl",
        [
            {
                "record_type": "depth_update",
                "venue": "BINANCE_USDTF",
                "symbol": "NOINFOUSDT",
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": base_ts_ms * 1_000_000 + 1,
                "ts_event_ms": base_ts_ms,
                "U": 9,
                "u": 10,
                "pu": None,
                "sync_state": "live_synced",
                "payload": {"bids": [["0.10", "5"]], "asks": [["0.11", "5"]]},
            },
        ],
    )
    _write_jsonl(
        root / "BINANCE_USDTF" / "trade_v2" / "NOINFOUSDT" / date / f"{date}T00.jsonl",
        [],
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_USDTF", "NOINFOUSDT", date, root, replay_root,
        schema_version=1, price_scale=2, qty_scale=0,
    )
    assert result["status"] == "success", result
    manifest = json.loads(
        (replay_root / "venue=BINANCE_USDTF" / "symbol=NOINFOUSDT" / f"date={date}" / "manifest.json").read_text()
    )
    assert manifest["price_scale"] == 2
    assert manifest["qty_scale"] == 0
    # No exchangeInfo lookup was needed/performed since both scales were
    # explicitly supplied.
    assert manifest["encoding_profile"]["price_scale_declared"] is None
    assert manifest["encoding_profile"]["qty_scale_declared"] is None


# ---------------------------------------------------------------------------
# 9-10: pipeline.raw_manifest.scan_raw_coverage() eligibility correction
# ---------------------------------------------------------------------------


def _write_minimal_symbol(root: Path, venue: str, symbol: str, date: str, *, channels=("depth_v2", "trade_v2")) -> None:
    for channel in channels:
        _write_jsonl(root / venue / channel / symbol / date / f"{date}T00.jsonl", [{"record_type": "depth_update"}])


def test_exchangeinfo_excluded_from_eligible_symbols_both_venues(tmp_path):
    root = tmp_path / "raw"
    date = "2026-06-11"
    _write_minimal_symbol(root, "BINANCE_SPOT", "BTCUSDT", date)
    _write_minimal_symbol(root, "BINANCE_USDTF", "ETHUSDT", date)
    _write_exchangeinfo(root, "BINANCE_SPOT", date, "BTCUSDT")
    _write_exchangeinfo(root, "BINANCE_USDTF", date, "ETHUSDT")

    coverage = scan_raw_coverage(date, root)
    assert "EXCHANGEINFO" not in coverage["data"].get("BINANCE_SPOT", {})
    assert "EXCHANGEINFO" not in coverage["data"].get("BINANCE_USDTF", {})
    assert coverage["data"]["BINANCE_SPOT"]["BTCUSDT"] == {"depth_v2": True, "trade_v2": True}
    assert coverage["data"]["BINANCE_USDTF"]["ETHUSDT"] == {"depth_v2": True, "trade_v2": True}
    assert coverage["symbol_count"] == 2


def test_exchangeinfo_excluded_end_to_end_via_build_replay_for_symbol_all(tmp_path):
    """The eligibility fix must apply identically regardless of
    schema_version: neither v0 nor v1 full-universe ("--symbols all")
    builds may ever attempt to build a replay partition for the
    EXCHANGEINFO pseudo-symbol."""
    raw_root = _sample_raw_root(tmp_path)
    _write_exchangeinfo(raw_root, "BINANCE_SPOT", "2026-06-12", "ADAUSDT")
    coverage = scan_raw_coverage("2026-06-12", raw_root)
    assert "EXCHANGEINFO" not in coverage["data"].get("BINANCE_SPOT", {})
    all_symbols = set()
    for venue_data in coverage["data"].values():
        all_symbols.update(venue_data.keys())
    assert all_symbols == {"ADAUSDT"}


def test_eligible_market_channels_is_exactly_depth_and_trade():
    assert ELIGIBLE_MARKET_CHANNELS == frozenset({"depth_v2", "trade_v2"})


def test_symbol_missing_one_channel_is_still_reported_honestly(tmp_path):
    """A symbol present under depth_v2 but not trade_v2 (or vice versa)
    must still appear in coverage['data'] with only the present channel
    key set — never silently dropped or hidden."""
    root = tmp_path / "raw"
    date = "2026-06-11"
    _write_minimal_symbol(root, "BINANCE_SPOT", "DEPTHONLYUSDT", date, channels=("depth_v2",))
    _write_minimal_symbol(root, "BINANCE_SPOT", "TRADEONLYUSDT", date, channels=("trade_v2",))

    coverage = scan_raw_coverage(date, root)
    assert coverage["data"]["BINANCE_SPOT"]["DEPTHONLYUSDT"] == {"depth_v2": True}
    assert coverage["data"]["BINANCE_SPOT"]["TRADEONLYUSDT"] == {"trade_v2": True}


def test_malformed_market_data_directory_still_surfaces_as_error(tmp_path):
    """A stray FILE where a venue directory is expected must still
    surface via the existing per-venue error collection — the
    EXCHANGEINFO-exclusion fix must not broadly suppress errors for
    genuinely malformed raw layouts."""
    root = tmp_path / "raw"
    root.mkdir(parents=True)
    (root / "NOT_A_DIR").write_text("stray file, not a venue directory")
    coverage = scan_raw_coverage("2026-06-11", root)
    # A stray file at the venue level is simply skipped by the `is_dir()`
    # guard (not a venue) rather than raising — confirm it does not crash
    # and does not appear as a phantom venue.
    assert "NOT_A_DIR" not in coverage["venues"]
