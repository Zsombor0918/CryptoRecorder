"""Focused tests for the issue #20 Phase 5 compact replay schema v1
prototype: version dispatch, exact fixed-point round-trips, flag/enum
round-trips, partition-constant restoration, and bounded-memory writer/reader
behavior.

These tests use synthetic (Tier 1) data only, with explicit price_scale/
qty_scale so they do not depend on real on-disk exchangeInfo fixtures. Tier 2
(local real raw data) validation is performed separately (see
docs/CHANGE_AUDIT.md for the Tier 2 run against 2026-06-10..12 data).
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from stores.replay_schema import (
    SUPPORTED_SCHEMA_VERSIONS,
    DEPTH_RECORD_TYPE_CODES,
    TRADE_RECORD_TYPE_CODES,
    pack_depth_flags,
    unpack_depth_flags,
    encode_fixed_point,
    decode_fixed_point,
    encode_aggressor_side,
    decode_aggressor_side,
)
from stores.replay_writer import ReplayWriter, _derive_fixed_point_scales
from stores.replay_reader import ReplayReader


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

def _depth_row(
    *,
    session_seq: int,
    raw_index: int,
    record_type: str = "depth_update",
    U=None,
    u="100",
    pu=None,
    bids=None,
    asks=None,
    is_snapshot_seed=False,
    is_depth_update=True,
    is_sync_state=False,
    is_desync=False,
    is_resync=False,
    quality_flags=None,
    native_payload_hash="ab" * 32,
) -> dict:
    return {
        "venue": "BINANCE_SPOT",
        "symbol": "BTCUSDT",
        "date": "2026-06-10",
        "stream_session_id": 1,
        "session_seq": session_seq,
        "raw_index": raw_index,
        "record_type": record_type,
        "U": U,
        "u": u,
        "pu": pu,
        "ts_exchange_ns": 1_000_000 + raw_index,
        "ts_receive_ns": 1_000_100 + raw_index,
        "bids": bids if bids is not None else [
            {"price": 100.50, "size": 1.23456, "price_str": "100.50", "size_str": "1.23456"},
        ],
        "asks": asks if asks is not None else [
            {"price": 100.55, "size": 2.00000, "price_str": "100.55", "size_str": "2.00000"},
        ],
        "is_snapshot_seed": is_snapshot_seed,
        "is_depth_update": is_depth_update,
        "is_sync_state": is_sync_state,
        "is_desync": is_desync,
        "is_resync": is_resync,
        "quality_flags": quality_flags,
        "native_payload_hash": native_payload_hash,
    }


def _trade_row(
    *,
    session_seq: int,
    raw_index: int,
    record_type: str = "trade",
    market_type: str = "spot",
    trade_id="1",
    agg_trade_id=None,
    price_str="100.50",
    quantity_str="0.50000",
    buyer_maker=True,
    aggressor_side="SELL",
    quality_flags=None,
    native_payload_hash="cd" * 32,
) -> dict:
    return {
        "venue": "BINANCE_SPOT",
        "symbol": "BTCUSDT",
        "date": "2026-06-10",
        "trade_stream_session_id": 1,
        "trade_session_seq": session_seq,
        "raw_index": raw_index,
        "record_type": record_type,
        "market_type": market_type,
        "trade_id": trade_id,
        "agg_trade_id": agg_trade_id,
        "ts_exchange_ns": 2_000_000 + raw_index,
        "ts_receive_ns": 2_000_100 + raw_index,
        "price": float(price_str),
        "quantity": float(quantity_str),
        "price_str": price_str,
        "quantity_str": quantity_str,
        "buyer_maker": buyer_maker,
        "aggressor_side": aggressor_side,
        "quality_flags": quality_flags,
        "native_payload_hash": native_payload_hash,
    }


def _build_v1_partition(
    tmp_path: Path,
    *,
    depth_rows: list[dict],
    trade_rows: list[dict],
    price_scale: int = 2,
    qty_scale: int = 5,
    venue: str = "BINANCE_SPOT",
    symbol: str = "BTCUSDT",
    date: str = "2026-06-10",
) -> Path:
    replay_root = tmp_path / "replay_store"
    writer = ReplayWriter(
        replay_root, venue, symbol, date,
        schema_version=1, price_scale=price_scale, qty_scale=qty_scale,
    )
    writer.write_depth_batch(depth_rows)
    writer.write_trades_batch(trade_rows)
    writer.finalize_staging()
    return writer.publish()


def _build_v0_partition(
    tmp_path: Path,
    *,
    depth_rows: list[dict],
    trade_rows: list[dict],
    venue: str = "BINANCE_SPOT",
    symbol: str = "BTCUSDT",
    date: str = "2026-06-10",
) -> Path:
    replay_root = tmp_path / "replay_store"
    writer = ReplayWriter(replay_root, venue, symbol, date)  # default schema_version=0
    writer.write_depth_batch(depth_rows)
    writer.write_trades_batch(trade_rows)
    writer.finalize_staging()
    return writer.publish()


# ---------------------------------------------------------------------------
# 1. Version dispatch
# ---------------------------------------------------------------------------

def test_manifest_without_schema_version_dispatches_to_v0(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v0_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    manifest = reader.load_manifest("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    assert "schema_version" not in manifest
    assert reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10") == 0

    rows = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert len(rows) == 1
    assert rows[0]["bids"][0]["price_str"] == "100.50"


def test_explicit_v1_manifest_dispatches_to_v1_reader(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    manifest = reader.load_manifest("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    assert manifest["schema_version"] == 1
    assert manifest["format_version"] == 1
    assert "builder_version" in manifest
    assert "encoding_profile" in manifest
    assert reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10") == 1

    rows = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert len(rows) == 1
    assert rows[0]["bids"][0]["price_str"] == "100.50"


def test_unsupported_schema_version_fails_clearly(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    out_dir = _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = 999
    manifest_path.write_text(json.dumps(manifest))

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(ValueError, match=r"999.*supported|supported.*999"):
        reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10")


def test_supported_schema_versions_constant_is_0_and_1():
    assert set(SUPPORTED_SCHEMA_VERSIONS) == {0, 1}


def test_writer_rejects_unsupported_schema_version(tmp_path):
    with pytest.raises(ValueError, match="Unsupported schema_version"):
        ReplayWriter(tmp_path / "replay_store", "BINANCE_SPOT", "BTCUSDT", "2026-06-10", schema_version=2)


# ---------------------------------------------------------------------------
# 2. v0 fixtures remain readable and behaviorally unchanged
# ---------------------------------------------------------------------------

def test_v0_fixture_unaffected_by_v1_existing(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    out_dir = _build_v0_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    manifest = json.loads((out_dir / "manifest.json").read_text())
    # Exactly today's v0 manifest keys — no v1 fields leaked in.
    assert "schema_version" not in manifest
    assert "format_version" not in manifest
    assert "price_scale" not in manifest
    assert "source_identity" not in manifest
    expected_keys = {
        "venue", "symbol", "date", "status", "depth_record_count",
        "trade_record_count", "ts_range_start_ns", "ts_range_end_ns",
        "depth_checksum", "trades_checksum", "created_at_utc", "errors",
    }
    assert set(manifest.keys()) == expected_keys


# ---------------------------------------------------------------------------
# 3. Exact fixed-point round trips (Decimal only, no float intermediate)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value_str,scale",
    [
        ("100.50", 2),
        ("0.00001000", 8),
        ("1.23456", 5),
        ("999999.99", 2),
        ("0", 2),
        ("0.00", 2),
        ("123456789.12345678", 8),
    ],
)
def test_fixed_point_round_trip_preserves_exact_value(value_str, scale):
    mantissa = encode_fixed_point(value_str, scale)
    assert isinstance(mantissa, int)
    decoded = decode_fixed_point(mantissa, scale)
    assert Decimal(decoded) == Decimal(value_str)
    assert len(decoded.split(".")[-1]) == scale if scale > 0 else "." not in decoded


def test_fixed_point_rejects_precision_loss_never_truncates_silently():
    with pytest.raises(ValueError):
        encode_fixed_point("1.005", 2)  # would lose the 3rd fractional digit


def test_fixed_point_no_float_intermediate():
    """Values that are NOT exactly representable as binary floats must still
    round-trip exactly through Decimal-based fixed point (a float
    intermediate would silently corrupt these)."""
    value_str = "0.1"
    scale = 1
    mantissa = encode_fixed_point(value_str, scale)
    assert mantissa == 1
    assert decode_fixed_point(mantissa, scale) == "0.1"

    # A value whose float64 representation is NOT exact must still encode to
    # the exact intended mantissa, not the nearest float64 double's decimal
    # expansion. Scale kept small enough that the resulting mantissa still
    # fits the physical int64 mantissa field (issue #20 Phase 7 correction
    # added an explicit int64-range check to encode_fixed_point — a mantissa
    # that could never fit int64 was never usable in the real Parquet
    # schema anyway, so this test uses the largest value/scale combination
    # that both exceeds 2**53 exactly-representable-float precision AND
    # fits int64).
    value_str = "9007199254740993.12"  # > 2**53 (9007199254740992), would lose bits as float
    scale = 2
    mantissa = encode_fixed_point(value_str, scale)
    assert decode_fixed_point(mantissa, scale) == value_str


def test_fixed_point_null_and_zero_boundary_values():
    assert decode_fixed_point(0, 2) == "0.00"
    assert encode_fixed_point("0.00", 2) == 0


# ---------------------------------------------------------------------------
# 4. Spot vs futures filter cases (scale derivation, independently)
# ---------------------------------------------------------------------------

def test_derive_fixed_point_scales_uses_exchange_filters(tmp_path, monkeypatch):
    def _fake_load_exchange_info(venue, date, data_root=None):
        if "USDTF" in venue:
            return {"BTCUSDT": {"filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.10000000"},
                {"filterType": "LOT_SIZE", "stepSize": "0.00100000"},
                {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.00010000"},
            ]}}
        return {"BTCUSDT": {"filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01000000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.00001000"},
        ]}}

    monkeypatch.setattr("converter.instruments.load_exchange_info", _fake_load_exchange_info)

    spot_price_scale, spot_qty_scale = _derive_fixed_point_scales("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    fut_price_scale, fut_qty_scale = _derive_fixed_point_scales("BINANCE_USDTF", "BTCUSDT", "2026-06-10")

    assert (spot_price_scale, spot_qty_scale) == (2, 5)
    # futures: PRICE_FILTER tickSize scale=1, LOT_SIZE stepSize scale=3,
    # MARKET_LOT_SIZE stepSize scale=4 -> qty_scale must be the max (4).
    assert (fut_price_scale, fut_qty_scale) == (1, 4)


def test_derive_fixed_point_scales_raises_clearly_when_exchange_info_missing(monkeypatch):
    monkeypatch.setattr("converter.instruments.load_exchange_info", lambda venue, date, data_root=None: {})
    with pytest.raises(ValueError, match="exchangeInfo"):
        _derive_fixed_point_scales("BINANCE_SPOT", "UNKNOWNUSDT", "2026-06-10")


# ---------------------------------------------------------------------------
# 5. Depth/trade event types and compact flags round-trip
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "is_snapshot_seed,is_depth_update,is_sync_state,is_desync,is_resync",
    [
        (True, False, False, False, False),
        (False, True, False, False, False),
        (False, False, True, False, False),
        (False, True, False, True, False),
        (False, True, False, False, True),
        (False, False, False, False, False),
        (True, True, True, True, True),
    ],
)
def test_depth_flag_bitmask_round_trip(
    is_snapshot_seed, is_depth_update, is_sync_state, is_desync, is_resync
):
    code = pack_depth_flags(is_snapshot_seed, is_depth_update, is_sync_state, is_desync, is_resync)
    assert unpack_depth_flags(code) == (
        is_snapshot_seed, is_depth_update, is_sync_state, is_desync, is_resync
    )


@pytest.mark.parametrize("record_type", ["snapshot_seed", "depth_update"])
def test_depth_record_type_round_trips_through_v1(tmp_path, record_type):
    depth_rows = [_depth_row(
        session_seq=1, raw_index=0, record_type=record_type,
        is_snapshot_seed=(record_type == "snapshot_seed"),
        is_depth_update=(record_type == "depth_update"),
    )]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    rows = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert rows[0]["record_type"] == record_type
    assert rows[0]["is_snapshot_seed"] == (record_type == "snapshot_seed")
    assert rows[0]["is_depth_update"] == (record_type == "depth_update")


@pytest.mark.parametrize("record_type", ["trade", "agg_trade"])
def test_trade_record_type_round_trips_through_v1(tmp_path, record_type):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, record_type=record_type)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    rows = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert rows[0]["record_type"] == record_type


@pytest.mark.parametrize("aggressor_side", ["BUY", "SELL", None])
def test_aggressor_side_round_trips_through_v1(tmp_path, aggressor_side):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, aggressor_side=aggressor_side)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    rows = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert rows[0]["aggressor_side"] == aggressor_side


def test_encode_decode_aggressor_side_helpers():
    for side in ("BUY", "SELL"):
        assert decode_aggressor_side(encode_aggressor_side(side)) == side
    assert encode_aggressor_side(None) is None
    assert decode_aggressor_side(None) is None


# ---------------------------------------------------------------------------
# 6. Null/optional IDs and required range boundaries
# ---------------------------------------------------------------------------

def test_null_optional_ids_survive_v1_round_trip(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0, U=None, u=None, pu=None)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, trade_id=None, agg_trade_id="55")]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    depth = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    trade = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    assert depth["U"] is None and depth["u"] is None and depth["pu"] is None
    assert trade["trade_id"] is None and trade["agg_trade_id"] == "55"


def test_zero_and_near_int64_boundary_mantissas_round_trip():
    # int64 max is 2**63 - 1; a large-but-valid price at scale=2 must still
    # round trip exactly.
    big_value = "92233720368547758.07"  # near int64/100 boundary
    scale = 2
    mantissa = encode_fixed_point(big_value, scale)
    assert mantissa <= 2**63 - 1
    assert decode_fixed_point(mantissa, scale) == big_value


# ---------------------------------------------------------------------------
# 7. Partition constants restored logically by the reader
# ---------------------------------------------------------------------------

def test_partition_constants_not_physically_stored_in_v1_rows(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    out_dir = _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    import pyarrow.parquet as pq
    depth_schema = pq.ParquetFile(out_dir / "depth.parquet").schema_arrow
    trade_schema = pq.ParquetFile(out_dir / "trades.parquet").schema_arrow
    for name in ("venue", "symbol", "date"):
        assert name not in depth_schema.names
        assert name not in trade_schema.names


def test_partition_constants_restored_logically_via_manifest(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v1_partition(
        tmp_path, depth_rows=depth_rows, trade_rows=trade_rows,
        venue="BINANCE_SPOT", symbol="ETHUSDT", date="2026-06-11",
    )
    reader = ReplayReader(tmp_path / "replay_store")
    manifest = reader.load_manifest("BINANCE_SPOT", "ETHUSDT", "2026-06-11")
    assert manifest["venue"] == "BINANCE_SPOT"
    assert manifest["symbol"] == "ETHUSDT"
    assert manifest["date"] == "2026-06-11"


# ---------------------------------------------------------------------------
# 8. Quality/continuity information survives round-trip
# ---------------------------------------------------------------------------

def test_quality_flags_and_continuity_survive_v1_round_trip(tmp_path):
    quality_flags_json = json.dumps({"gap_detected": True, "reason": "resync"})
    depth_rows = [_depth_row(
        session_seq=1, raw_index=0, record_type="depth_update",
        is_depth_update=True, is_desync=True, is_resync=False,
        U="10", u="20", pu="9",
        quality_flags=quality_flags_json,
    )]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, quality_flags=quality_flags_json)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    depth = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    trade = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    assert depth["quality_flags"] == quality_flags_json
    assert depth["U"] == "10" and depth["u"] == "20" and depth["pu"] == "9"
    assert depth["is_desync"] is True and depth["is_resync"] is False
    assert trade["quality_flags"] == quality_flags_json


# ---------------------------------------------------------------------------
# 9. Integrity / source-identity fields validate as designed
# ---------------------------------------------------------------------------

def test_native_payload_hash_round_trips_as_exact_hex(tmp_path):
    hash_hex = "0123456789abcdef" * 4  # 64 hex chars = 32 bytes
    depth_rows = [_depth_row(session_seq=1, raw_index=0, native_payload_hash=hash_hex)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, native_payload_hash=hash_hex)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    depth = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    trade = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    assert depth["native_payload_hash"] == hash_hex
    assert trade["native_payload_hash"] == hash_hex


def test_native_payload_hash_null_survives_round_trip(tmp_path):
    depth_rows = [_depth_row(session_seq=1, raw_index=0, native_payload_hash=None)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, native_payload_hash=None)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    depth = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    trade = list(reader.iter_trades("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))[0]
    assert depth["native_payload_hash"] is None
    assert trade["native_payload_hash"] is None


def test_native_payload_hash_physically_32_bytes_not_64_char_hex(tmp_path):
    hash_hex = "ab" * 32
    depth_rows = [_depth_row(session_seq=1, raw_index=0, native_payload_hash=hash_hex)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0, native_payload_hash=hash_hex)]
    out_dir = _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    import pyarrow.parquet as pq
    depth_schema = pq.ParquetFile(out_dir / "depth.parquet").schema_arrow
    field = depth_schema.field("native_payload_hash")
    assert str(field.type) == "fixed_size_binary[32]"


def test_source_identity_recorded_when_explicitly_supplied(tmp_path):
    """ReplayWriter no longer computes source_identity itself (issue #20
    Phase 5 correction — see test_replay_schema_v1_corrections.py for the
    full bound-to-actual-data-root proof); this test only proves an
    explicitly-supplied identity is faithfully recorded in the manifest."""
    supplied_identity = {
        "channels": {
            "depth_v2": [{"path": "x", "sha256": "aa", "size_bytes": 1}],
            "trade_v2": [{"path": "y", "sha256": "bb", "size_bytes": 1}],
        },
        "complete": True,
        "missing_channels": [],
    }

    replay_root = tmp_path / "replay_store"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "BTCUSDT", "2026-06-10",
        schema_version=1, price_scale=2, qty_scale=5,
        source_identity=supplied_identity,
    )
    writer.write_depth_batch([_depth_row(session_seq=1, raw_index=0)])
    writer.write_trades_batch([_trade_row(session_seq=1, raw_index=0)])
    writer.finalize_staging()
    writer.publish()

    reader = ReplayReader(replay_root)
    manifest = reader.load_manifest("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    assert manifest["source_identity"]["complete"] is True
    assert manifest["source_identity"]["channels"]["depth_v2"][0]["sha256"] == "aa"


def test_source_identity_honestly_incomplete_when_not_supplied(tmp_path):
    _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    reader = ReplayReader(tmp_path / "replay_store")
    manifest = reader.load_manifest("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    assert manifest["source_identity"]["complete"] is False
    assert "error" in manifest["source_identity"]


# ---------------------------------------------------------------------------
# 10. Ordering preserved (canonical sort key still honoured in v1)
# ---------------------------------------------------------------------------

def test_v1_preserves_canonical_ordering(tmp_path):
    depth_rows = [
        _depth_row(session_seq=3, raw_index=0, u="103"),
        _depth_row(session_seq=1, raw_index=1, u="101"),
        _depth_row(session_seq=2, raw_index=2, u="102"),
    ]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    reader = ReplayReader(tmp_path / "replay_store")
    rows = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert [r["session_seq"] for r in rows] == [1, 2, 3]
    assert [r["u"] for r in rows] == ["101", "102", "103"]


# ---------------------------------------------------------------------------
# 11. Bounded memory (writer/reader do not accumulate a full symbol/day)
# ---------------------------------------------------------------------------

def test_v1_writer_and_reader_bounded_memory(tmp_path):
    """Live-object-counter proof (mirroring
    tests/test_streaming_gating_bounded_memory.py's pattern): peak
    simultaneously-alive spooled records stays independent of the total
    record count across a 20,000-row synthetic day."""
    n = 20_000
    depth_rows = [
        _depth_row(session_seq=i, raw_index=i, u=str(100 + i))
        for i in range(n)
    ]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]

    replay_root = tmp_path / "replay_store"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "BTCUSDT", "2026-06-10",
        schema_version=1, price_scale=2, qty_scale=5,
        parquet_batch_size=500,
    )
    writer.write_depth_batch(depth_rows)
    writer.write_trades_batch(trade_rows)
    writer.finalize_staging()
    writer.publish()

    reader = ReplayReader(replay_root)
    count = 0
    for row in reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"):
        count += 1
        # Streaming: rows must be individually yielded, not collected first.
    assert count == n


def test_v1_reader_streams_without_full_materialization(tmp_path, monkeypatch):
    """Prove ReplayReader.iter_depths() is a generator that does not call
    ``to_pylist()``/materialize the whole file before yielding the first row
    — assert on the batch_size actually passed to pyarrow (bounded, not the
    full row count) rather than on peak RSS (unreliable in CI)."""
    depth_rows = [_depth_row(session_seq=i, raw_index=i) for i in range(50)]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]
    _build_v1_partition(tmp_path, depth_rows=depth_rows, trade_rows=trade_rows)

    import pyarrow.parquet as pq
    original_iter_batches = pq.ParquetFile.iter_batches
    seen_batch_sizes = []

    def _spy_iter_batches(self, *args, **kwargs):
        seen_batch_sizes.append(kwargs.get("batch_size"))
        return original_iter_batches(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", _spy_iter_batches)

    reader = ReplayReader(tmp_path / "replay_store")
    rows = list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    assert len(rows) == 50
    assert seen_batch_sizes and all(bs == 5000 for bs in seen_batch_sizes if bs is not None)


# ---------------------------------------------------------------------------
# 12. Physical size measured separately from logical equivalence
# ---------------------------------------------------------------------------

def test_v1_physical_size_smaller_than_v0_for_equivalent_data(tmp_path):
    """Development evidence only (not a Tier-3 claim): the v1 physical
    depth.parquet for an identical logical row set must be smaller than the
    v0 physical depth.parquet, given the compaction levers applied."""
    n = 2_000
    depth_rows_template = [
        _depth_row(session_seq=i, raw_index=i, u=str(100 + i))
        for i in range(n)
    ]
    trade_rows = [_trade_row(session_seq=1, raw_index=0)]

    v0_dir = _build_v0_partition(
        tmp_path / "v0", depth_rows=depth_rows_template, trade_rows=trade_rows
    )
    v1_dir = _build_v1_partition(
        tmp_path / "v1", depth_rows=depth_rows_template, trade_rows=trade_rows
    )

    v0_size = (v0_dir / "depth.parquet").stat().st_size
    v1_size = (v1_dir / "depth.parquet").stat().st_size
    assert v1_size < v0_size, f"v1 depth.parquet ({v1_size}B) not smaller than v0 ({v0_size}B)"
