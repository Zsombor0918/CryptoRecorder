"""Production schema/source replacement policy tests on real Parquet fixtures."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.build_replay_store import build_replay_for_symbol


DATE = "2026-01-03"
VENUE = "BINANCE_SPOT"


def _append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        handle.write(json.dumps(record) + "\n")


def _raw(root: Path, symbol: str, *, trade_id: int = 1) -> tuple[Path, Path]:
    depth = root / VENUE / "depth_v2" / symbol / DATE / f"{DATE}T00.jsonl"
    trade = root / VENUE / "trade_v2" / symbol / DATE / f"{DATE}T00.jsonl"
    _append(
        depth,
        {
            "record_type": "depth_update",
            "stream_session_id": 1,
            "session_seq": 1,
            "ts_event_ms": 1_767_398_400_000,
            "ts_recv_ns": 1_767_398_400_000_000_000,
            "U": 1,
            "u": 1,
            "payload": {"bids": [["1.00", "2.0"]], "asks": [["1.01", "3.0"]]},
        },
    )
    _append(
        trade,
        {
            "record_type": "trade",
            "market_type": "spot",
            "trade_stream_session_id": 1,
            "trade_session_seq": trade_id,
            "ts_trade_ms": 1_767_398_400_000 + trade_id,
            "ts_recv_ns": 1_767_398_400_000_000_000 + trade_id,
            "price": "1.00",
            "quantity": "2.0",
            "exchange_trade_id": trade_id,
        },
    )
    return depth, trade


def _build_v2(raw: Path, replay: Path, symbol: str, **kwargs):
    return build_replay_for_symbol(
        VENUE,
        symbol,
        DATE,
        raw,
        replay,
        schema_version=2,
        price_scale=2,
        qty_scale=1,
        **kwargs,
    )


def test_matching_v2_source_skips_and_source_change_requires_policy(tmp_path: Path) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, "ADAUSDT")
    assert _build_v2(raw, replay, "ADAUSDT")["outcome"] == "built"
    assert _build_v2(raw, replay, "ADAUSDT")["outcome"] == "skipped_valid"

    _append(
        trade,
        {
            "record_type": "trade",
            "market_type": "spot",
            "trade_stream_session_id": 1,
            "trade_session_seq": 2,
            "ts_trade_ms": 1_767_398_400_002,
            "ts_recv_ns": 1_767_398_400_000_000_002,
            "price": "1.00",
            "quantity": "2.0",
            "exchange_trade_id": 2,
        },
    )
    refused = _build_v2(raw, replay, "ADAUSDT")
    assert refused["outcome"] == "source_changed_rebuild_required"
    replaced = _build_v2(raw, replay, "ADAUSDT", rebuild_source_changed=True)
    assert replaced["outcome"] == "built"
    assert replaced["trade_count"] == 2


def test_incompatible_legacy_requires_explicit_partition_scoped_replacement(tmp_path: Path) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _raw(raw, "ADAUSDT")
    _raw(raw, "BTCUSDT")
    for symbol in ("ADAUSDT", "BTCUSDT"):
        assert build_replay_for_symbol(VENUE, symbol, DATE, raw, replay, schema_version=0)["outcome"] == "built"

    refused = _build_v2(raw, replay, "ADAUSDT")
    assert refused["outcome"] == "incompatible_schema_rebuild_required"
    replaced = _build_v2(raw, replay, "ADAUSDT", replace_incompatible=True)
    assert replaced["outcome"] == "built"
    ada_manifest = json.loads((replay / f"venue={VENUE}" / "symbol=ADAUSDT" / f"date={DATE}" / "manifest.json").read_text())
    btc_manifest = json.loads((replay / f"venue={VENUE}" / "symbol=BTCUSDT" / f"date={DATE}" / "manifest.json").read_text())
    assert ada_manifest["schema_version"] == 2
    assert btc_manifest.get("schema_version", 0) == 0


def test_corrupt_partition_is_never_skipped_or_implicitly_rebuilt(tmp_path: Path) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _raw(raw, "ADAUSDT")
    assert _build_v2(raw, replay, "ADAUSDT")["outcome"] == "built"
    depth = replay / f"venue={VENUE}" / "symbol=ADAUSDT" / f"date={DATE}" / "depth.parquet"
    depth.write_bytes(b"corrupt")
    result = _build_v2(
        raw,
        replay,
        "ADAUSDT",
        rebuild_source_changed=True,
        replace_incompatible=True,
    )
    assert result["outcome"] == "failed"
    assert depth.read_bytes() == b"corrupt"
