"""Fail-closed replay trade identifier normalization.

These tests pin the unchanged reference converter's top-level/native
precedence and the two Binance native event shapes consumed by replay builds.
"""
from __future__ import annotations

import json
from pathlib import Path

from nautilus_trader.model.identifiers import InstrumentId
import pytest

from converter.trades import _trade_id_for_report
from pipeline.build_replay_store import (
    ReplayTradeIdentifierError,
    _convert_trade_record,
    build_replay_for_symbol,
)
from stores.replay_reader import ReplayReader
from stores.replay_schema import (
    BUILDER_VERSION_V1,
    BUILDER_VERSION_V2,
    BUILDER_VERSION_V2_LEGACY,
)
from stores.replay_writer import ReplayWriter, validate_partition
from validation.replay_catalog_reconstruct import (
    ReplayTradeIdentifierError as ReconstructionIdentifierError,
    _convert_trade_to_nautilus,
)


VENUE = "BINANCE_USDTF"
SYMBOL = "BTWUSDT"
DATE = "2026-06-11"


def _raw_trade(**overrides: object) -> dict:
    row = {
        "schema_version": 2,
        "record_type": "trade",
        "venue": VENUE,
        "market_type": "futures",
        "symbol": SYMBOL,
        "channel": "trade_v2",
        "trade_stream_session_id": 2,
        "trade_session_seq": 1,
        "raw_index": 0,
        "ts_recv_ns": 1_781_161_289_171_761_749,
        "ts_event_ms": 1_781_161_289_840,
        "ts_trade_ms": 1_781_161_289_840,
        "price": "0.0794300",
        "quantity": "332",
        "is_buyer_maker": True,
        "exchange_trade_id": 30_738_028,
        "first_trade_id": None,
        "last_trade_id": None,
        "native_payload": {
            "e": "trade",
            "E": 1_781_161_289_840,
            "T": 1_781_161_289_840,
            "s": SYMBOL,
            "t": 30_738_028,
            "p": "0.0794300",
            "q": "332",
            "X": "MARKET",
            "m": True,
        },
    }
    row.update(overrides)
    return row


def _convert(row: dict) -> dict:
    converted = _convert_trade_record(row, VENUE, SYMBOL, DATE)
    assert converted is not None
    return converted


def _write_raw_trade(root: Path, row: dict) -> None:
    path = root / VENUE / "trade_v2" / SYMBOL / DATE / f"{DATE}T00.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row) + "\n")


def _write_raw_depth(root: Path) -> None:
    path = root / VENUE / "depth_v2" / SYMBOL / DATE / f"{DATE}T00.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "record_type": "snapshot_seed",
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_recv_ns": 1_781_161_266_587_615_001,
                "last_update_id": 1,
                "bids": [["0.0794300", "332"]],
                "asks": [["0.0794400", "100"]],
            }
        )
        + "\n"
    )


def test_top_level_exchange_trade_id_is_preserved_exactly() -> None:
    row = _raw_trade(exchange_trade_id="0030738028")
    converted = _convert(row)

    assert converted["trade_id"] == "0030738028"
    assert converted["agg_trade_id"] is None
    assert converted["trade_id"] == str(_trade_id_for_report(row))


def test_null_top_level_identifiers_recover_native_trade_t_exactly() -> None:
    row = _raw_trade(exchange_trade_id=None)
    converted = _convert(row)

    assert converted["trade_id"] == "30738028"
    assert converted["agg_trade_id"] is None
    assert converted["trade_id"] == str(_trade_id_for_report(row))


def test_exact_preserved_btw_legacy_futures_shape_recovers_trade_id() -> None:
    row = _raw_trade(
        exchange_trade_id=None,
        first_trade_id=None,
        last_trade_id=None,
        native_payload={
            "e": "trade",
            "E": 1_781_161_289_840,
            "T": 1_781_161_289_840,
            "s": "BTWUSDT",
            "t": 30_738_028,
            "p": "0.0794300",
            "q": "332",
            "X": "MARKET",
            "m": True,
        },
    )

    converted = _convert(row)

    assert converted["trade_id"] == "30738028"
    assert converted["price_str"] == "0.0794300"
    assert converted["quantity_str"] == "332"


def test_native_aggtrade_recovers_reference_compatible_aggregate_id() -> None:
    row = _raw_trade(
        exchange_trade_id=None,
        native_payload={
            "e": "aggTrade",
            "a": 987_654,
            "p": "0.0794300",
            "q": "332",
            "m": True,
        },
    )
    converted = _convert(row)

    assert converted["trade_id"] is None
    assert converted["agg_trade_id"] == "987654"
    # The recorder's canonical normalized aggTrade shape puts native `a` in
    # exchange_trade_id; that is the value the unchanged converter consumes.
    canonical_reference_row = dict(row, exchange_trade_id=987_654)
    assert converted["agg_trade_id"] == str(
        _trade_id_for_report(canonical_reference_row)
    )


def test_top_level_identifiers_precede_native_payload_like_reference() -> None:
    row = _raw_trade(
        exchange_trade_id=111,
        native_payload={"e": "trade", "t": 222},
    )
    converted = _convert(row)
    assert converted["trade_id"] == "111"
    assert converted["trade_id"] == str(_trade_id_for_report(row))

    already_normalized = _raw_trade(
        trade_id="preferred-normalized",
        exchange_trade_id=111,
        native_payload={"e": "trade", "t": 222},
    )
    assert _convert(already_normalized)["trade_id"] == "preferred-normalized"


@pytest.mark.parametrize(
    ("identifier", "expected"),
    [(30_738_028, "30738028"), ("0030738028", "0030738028")],
)
def test_integer_and_string_identifiers_preserve_exact_value(
    identifier: object,
    expected: str,
) -> None:
    converted = _convert(_raw_trade(exchange_trade_id=identifier))
    assert converted["trade_id"] == expected


def test_missing_every_supported_identifier_fails_clearly() -> None:
    row = _raw_trade(
        trade_id=None,
        agg_trade_id=None,
        exchange_trade_id=None,
        native_payload={"e": "trade"},
    )

    with pytest.raises(
        ReplayTradeIdentifierError,
        match="no reconstructable identifier",
    ):
        _convert_trade_record(row, VENUE, SYMBOL, DATE)


def test_failed_identifier_normalization_preserves_existing_atomic_partition(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    _write_raw_trade(raw_root, _raw_trade(exchange_trade_id=30_738_028))
    first = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=0,
    )
    assert first["status"] == "success"
    partition = (
        replay_root / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    )
    original_files = {
        path.name: path.read_bytes()
        for path in partition.iterdir()
        if path.is_file()
    }

    _write_raw_trade(
        raw_root,
        _raw_trade(
            trade_id=None,
            agg_trade_id=None,
            exchange_trade_id=None,
            native_payload={"e": "trade"},
        ),
    )
    failed = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=0,
        force=True,
    )

    assert failed["status"] == "failed"
    assert "no reconstructable identifier" in failed["errors"][0]
    assert {
        path.name: path.read_bytes()
        for path in partition.iterdir()
        if path.is_file()
    } == original_files
    assert not (
        partition.parent / f".staging_{DATE}_{SYMBOL}"
    ).exists()


def test_identifier_fallback_does_not_change_scale_selection(tmp_path: Path) -> None:
    manifests = []
    for name, row in (
        (
            "top",
            _raw_trade(exchange_trade_id=30_738_028, price="0.0795760"),
        ),
        (
            "native",
            _raw_trade(exchange_trade_id=None, price="0.0795760"),
        ),
    ):
        replay_root = tmp_path / name
        writer = ReplayWriter(
            replay_root,
            VENUE,
            SYMBOL,
            DATE,
            schema_version=1,
            price_scale=6,
            qty_scale=0,
        )
        writer.write_depth_batch([])
        writer.write_trades_batch([_convert(row)])
        manifests.append(writer.finalize_staging())
        writer.cleanup_staging()

    assert manifests[0]["builder_version"] == BUILDER_VERSION_V1
    assert manifests[1]["builder_version"] == BUILDER_VERSION_V1
    assert manifests[0]["price_scale"] == manifests[1]["price_scale"] == 6
    assert manifests[0]["qty_scale"] == manifests[1]["qty_scale"] == 0
    assert (
        manifests[0]["encoding_profile"]["price_scale_observed"]
        == manifests[1]["encoding_profile"]["price_scale_observed"]
        == 6
    )
    assert (
        manifests[0]["encoding_profile"]["qty_scale_observed"]
        == manifests[1]["encoding_profile"]["qty_scale_observed"]
        == 0
    )


@pytest.mark.parametrize("schema_version", [0, 1, 2])
def test_writer_rejects_anonymous_trade_for_every_physical_schema(
    tmp_path: Path,
    schema_version: int,
) -> None:
    writer = ReplayWriter(
        tmp_path / str(schema_version),
        VENUE,
        SYMBOL,
        DATE,
        schema_version=schema_version,
        price_scale=6 if schema_version else None,
        qty_scale=0 if schema_version else None,
    )
    anonymous = _convert(_raw_trade())
    anonymous["trade_id"] = None
    anonymous["agg_trade_id"] = None

    with pytest.raises(ValueError, match="no supported identifier"):
        writer.write_trades_batch([anonymous])
    writer.cleanup_staging()


def test_reconstruction_emits_recovered_trade_id() -> None:
    converted = _convert(_raw_trade(exchange_trade_id=None))
    tick = _convert_trade_to_nautilus(
        converted,
        InstrumentId.from_str("BTWUSDT-PERP.BINANCE"),
        VENUE,
    )

    assert tick is not None
    assert str(tick.trade_id) == "30738028"


def test_reconstruction_oracle_rejects_injected_missing_identifier() -> None:
    converted = _convert(_raw_trade())
    converted["trade_id"] = None
    converted["agg_trade_id"] = None

    with pytest.raises(
        ReconstructionIdentifierError,
        match="refusing to reconstruct an anonymous TradeTick",
    ):
        _convert_trade_to_nautilus(
            converted,
            InstrumentId.from_str("BTWUSDT-PERP.BINANCE"),
            VENUE,
        )


def test_current_builder_versions_change_without_physical_schema_bump() -> None:
    assert BUILDER_VERSION_V1 == "cryptorecorder-replay-writer-v1.2.1"
    assert BUILDER_VERSION_V2 == "cryptorecorder-replay-writer-v2.0.1"


def test_legacy_v2_builder_remains_readable_but_is_not_reused_as_current(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    replay_root = tmp_path / "replay"
    _write_raw_depth(raw_root)
    _write_raw_trade(raw_root, _raw_trade())
    built = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=2,
        price_scale=6,
        qty_scale=0,
    )
    assert built["status"] == "success"
    partition = (
        replay_root / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"
    )
    manifest_path = partition / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["builder_version"] = BUILDER_VERSION_V2_LEGACY
    manifest_path.write_text(json.dumps(manifest))

    assert validate_partition(partition) is True
    rows = list(ReplayReader(replay_root).iter_trades(VENUE, SYMBOL, DATE))
    assert [row["trade_id"] for row in rows] == ["30738028"]

    reuse = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=2,
        price_scale=6,
        qty_scale=0,
    )
    assert reuse["status"] == "failed"
    assert "different normalization semantics" in reuse["errors"][0]
