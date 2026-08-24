"""Focused fail-closed tests for replay reader and schema-v2 source identity."""
from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

import stores.replay_reader as replay_reader_module
from pipeline.build_replay_store import (
    build_replay_for_symbol,
    compute_depth_repartitioned_source_identity,
)
from pipeline.raw_manifest import compute_raw_source_identity
from stores.replay_reader import ReplayReader
from stores.replay_writer import (
    ReplayWriter,
    resolve_source_record,
    validate_partition,
    validate_v2_source_identity,
)
from tests.test_replay_schema_v1 import (
    _build_v0_partition,
    _build_v1_partition,
    _depth_row,
    _trade_row,
)
from tests.test_replay_schema_v1_corrections import (
    _sample_raw_root,
    _write_jsonl,
)
from tests.test_replay_hierarchical_integrity_v2 import _build_v2_partition


VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
DATE = "2026-06-12"


@pytest.mark.parametrize("manifest_state", ["missing", "invalid_json"])
def test_compact_reader_rejects_missing_or_unreadable_manifest(
    tmp_path: Path,
    manifest_state: str,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    manifest_path = partition / "manifest.json"
    if manifest_state == "missing":
        manifest_path.unlink()
        expected_error = FileNotFoundError
    else:
        manifest_path.write_text("{not-json")
        expected_error = ValueError

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(expected_error):
        reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10")
    with pytest.raises(expected_error):
        list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))


def test_existing_manifest_without_schema_version_remains_legitimate_v0(
    tmp_path: Path,
) -> None:
    _build_v0_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    reader = ReplayReader(tmp_path / "replay_store")
    assert reader.get_schema_version(
        "BINANCE_SPOT", "BTCUSDT", "2026-06-10"
    ) == 0
    assert len(
        list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))
    ) == 1


def _rewrite_manifest_version(partition: Path, version) -> None:
    manifest_path = partition / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if version is None:
        manifest.pop("schema_version", None)
    else:
        manifest["schema_version"] = version
    manifest_path.write_text(json.dumps(manifest))


def test_reader_rejects_v1_physical_schema_when_version_removed(
    tmp_path: Path,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    _rewrite_manifest_version(partition, None)

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(ValueError, match="never decoded as legacy v0"):
        reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10")


def test_reader_rejects_v2_physical_schema_when_version_removed(
    tmp_path: Path,
) -> None:
    partition, _ = _build_v2_partition(tmp_path)
    _rewrite_manifest_version(partition, None)

    reader = ReplayReader(tmp_path / "replay")
    with pytest.raises(ValueError, match="never decoded as legacy v0"):
        reader.get_schema_version("BINANCE_SPOT", "ADAUSDT", "2026-06-12")


@pytest.mark.parametrize("declared_version", [1, 2])
def test_reader_rejects_compact_manifest_over_legacy_physical_files(
    tmp_path: Path,
    declared_version: int,
) -> None:
    partition = _build_v0_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    _rewrite_manifest_version(partition, declared_version)

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(ValueError, match="physical schema contradicts"):
        reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10")


def test_reader_rejects_manifest_declaring_v0_over_compact_files(
    tmp_path: Path,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    _rewrite_manifest_version(partition, 0)

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(ValueError, match="schema_version=0"):
        list(reader.iter_depths("BINANCE_SPOT", "BTCUSDT", "2026-06-10"))


@pytest.mark.parametrize("invalid_version", [999, "2", True])
def test_reader_rejects_unsupported_or_malformed_explicit_version(
    tmp_path: Path,
    invalid_version,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    _rewrite_manifest_version(partition, invalid_version)

    reader = ReplayReader(tmp_path / "replay_store")
    with pytest.raises(ValueError, match="Unsupported replay schema_version"):
        reader.get_schema_version("BINANCE_SPOT", "BTCUSDT", "2026-06-10")


@pytest.mark.parametrize(
    ("file_name", "reader_method"),
    [
        ("depth.parquet", "iter_depths"),
        ("trades.parquet", "iter_trades"),
    ],
)
def test_reader_rejects_missing_channel_in_existing_partition(
    tmp_path: Path,
    file_name: str,
    reader_method: str,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    (partition / file_name).unlink()
    reader = ReplayReader(tmp_path / "replay_store")

    with pytest.raises(FileNotFoundError, match="partition exists"):
        list(
            getattr(reader, reader_method)(
                "BINANCE_SPOT", "BTCUSDT", "2026-06-10"
            )
        )


def test_reader_keeps_genuinely_absent_partition_as_empty_iteration(
    tmp_path: Path,
) -> None:
    reader = ReplayReader(tmp_path / "replay_store")
    assert list(reader.iter_depths(VENUE, SYMBOL, DATE)) == []
    assert list(reader.iter_trades(VENUE, SYMBOL, DATE)) == []


def test_reader_propagates_mid_iteration_parquet_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partition = _build_v1_partition(
        tmp_path,
        depth_rows=[_depth_row(session_seq=1, raw_index=0)],
        trade_rows=[_trade_row(session_seq=1, raw_index=0)],
    )
    real_parquet = pq.ParquetFile(partition / "depth.parquet")
    closed = {"value": False}

    class ExplodingParquet:
        def iter_batches(self, *args, **kwargs):
            yield next(real_parquet.iter_batches(*args, **kwargs))
            raise OSError("injected mid-file read failure")

        def close(self):
            closed["value"] = True
            real_parquet.close()

    monkeypatch.setattr(
        replay_reader_module.pq,
        "ParquetFile",
        lambda _path: ExplodingParquet(),
    )

    rows = ReplayReader(tmp_path / "replay_store").iter_depths(
        "BINANCE_SPOT", "BTCUSDT", "2026-06-10"
    )
    assert next(rows)["raw_index"] == 0
    with pytest.raises(OSError, match="injected mid-file read failure"):
        next(rows)
    assert closed["value"] is True


def _valid_source_identity() -> dict:
    return {
        "venue": VENUE,
        "symbol": SYMBOL,
        "date": DATE,
        "channels": {
            "depth_v2": [
                {
                    "path": (
                        f"{VENUE}/depth_v2/{SYMBOL}/{DATE}/"
                        f"{DATE}T00.jsonl"
                    ),
                    "sha256": "a" * 64,
                    "size_bytes": 10,
                    "source_date": DATE,
                    "record_count": 2,
                    "record_range": [0, 2],
                }
            ],
            "trade_v2": [
                {
                    "path": (
                        f"{VENUE}/trade_v2/{SYMBOL}/{DATE}/"
                        f"{DATE}T00.jsonl.zst"
                    ),
                    "sha256": "b" * 64,
                    "size_bytes": 20,
                    "record_count": 1,
                    "record_range": [0, 1],
                }
            ],
        },
        "complete": True,
        "missing_channels": [],
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda identity: identity.update(complete=False),
        lambda identity: identity.update(missing_channels=["trade_v2"]),
        lambda identity: identity.update(symbol="ETHUSDT"),
        lambda identity: identity["channels"]["depth_v2"][0].update(
            record_range=[1, 3]
        ),
        lambda identity: identity["channels"]["depth_v2"][0].update(
            path="/absolute/input.jsonl"
        ),
        lambda identity: identity["channels"]["depth_v2"][0].update(
            path=(
                f"{VENUE}/depth_v2/{SYMBOL}/{DATE}/nested/"
                f"{DATE}T00.jsonl"
            )
        ),
        lambda identity: identity["channels"]["trade_v2"][0].update(
            sha256="not-a-digest"
        ),
        lambda identity: identity["channels"]["depth_v2"][0].update(
            source_date="2026-06-10"
        ),
    ],
)
def test_v2_source_identity_rejects_incomplete_or_malformed_contract(
    mutation,
) -> None:
    identity = copy.deepcopy(_valid_source_identity())
    mutation(identity)
    with pytest.raises(ValueError):
        validate_v2_source_identity(identity, VENUE, SYMBOL, DATE)


def test_v2_writer_rejects_present_but_incomplete_source_identity(
    tmp_path: Path,
) -> None:
    identity = _valid_source_identity()
    identity["complete"] = False
    writer = ReplayWriter(
        tmp_path / "replay",
        VENUE,
        SYMBOL,
        DATE,
        schema_version=2,
        price_scale=4,
        qty_scale=1,
        source_identity=identity,
    )
    writer.write_depth_batch([])
    writer.write_trades_batch([])
    with pytest.raises(ValueError, match="source_identity.complete"):
        writer.finalize_staging()
    writer.cleanup_staging()


def test_source_identity_uses_only_files_selected_by_raw_reader(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    depth_dir = raw_root / VENUE / "depth_v2" / SYMBOL / DATE
    trade_dir = raw_root / VENUE / "trade_v2" / SYMBOL / DATE
    valid_depth = depth_dir / f"{DATE}T00.jsonl"
    valid_trade = trade_dir / f"{DATE}T00.jsonl"
    _write_jsonl(
        valid_depth,
        [
            {
                "record_type": "depth_update",
                "stream_session_id": 1,
                "session_seq": 1,
                "ts_event_ms": 1,
            }
        ],
    )
    _write_jsonl(valid_trade, [{"record_type": "trade"}])
    _write_jsonl(depth_dir / "sidecar.txt", [{"ts_event_ms": 1}])
    _write_jsonl(trade_dir / "sidecar.txt", [{"record_type": "trade"}])

    depth_identity = compute_depth_repartitioned_source_identity(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        include_record_counts=True,
    )
    trade_identity = compute_raw_source_identity(
        VENUE,
        SYMBOL,
        DATE,
        ["trade_v2"],
        data_root=raw_root,
        include_record_counts=True,
    )

    assert [entry["path"] for entry in depth_identity["channels"]["depth_v2"]] == [
        valid_depth.relative_to(raw_root).as_posix()
    ]
    assert depth_identity["channels"]["depth_v2"][0]["record_range"] == [0, 1]
    assert [entry["path"] for entry in trade_identity["channels"]["trade_v2"]] == [
        valid_trade.relative_to(raw_root).as_posix()
    ]
    assert trade_identity["channels"]["trade_v2"][0]["record_range"] == [0, 1]


def test_v2_source_identity_rejects_persisted_compression_variants() -> None:
    identity = _valid_source_identity()
    first = identity["channels"]["trade_v2"][0]
    first["path"] = (
        f"{VENUE}/trade_v2/{SYMBOL}/{DATE}/{DATE}T00.jsonl"
    )
    identity["channels"]["trade_v2"].append(
        {
            "path": f"{first['path']}.zst",
            "sha256": "c" * 64,
            "size_bytes": 21,
            "record_count": 1,
            "record_range": [1, 2],
        }
    )

    with pytest.raises(ValueError, match="compression variants"):
        validate_v2_source_identity(identity, VENUE, SYMBOL, DATE)


def test_v2_partition_rejects_replay_raw_index_outside_source_ranges(
    tmp_path: Path,
) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "success"
    partition = (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={DATE}"
    )
    manifest_path = partition / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for identity in (
        manifest["source_identity"],
        manifest["integrity"]["source_identity"],
    ):
        trade_entry = identity["channels"]["trade_v2"][0]
        trade_entry["record_count"] = 0
        trade_entry["record_range"] = [0, 0]
    manifest_path.write_text(json.dumps(manifest))

    assert validate_partition(partition) is False


def test_v2_source_ranges_cover_filtered_raw_gaps_not_replay_row_count(
    tmp_path: Path,
) -> None:
    raw_root = _sample_raw_root(tmp_path)
    trade_path = next(
        (raw_root / VENUE / "trade_v2" / SYMBOL / DATE).glob("*.jsonl")
    )
    original_records = [
        json.loads(line)
        for line in trade_path.read_text().splitlines()
        if line
    ]
    _write_jsonl(
        trade_path,
        [{"record_type": "not_a_trade_event"}, *original_records],
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "success"
    partition = (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={DATE}"
    )
    manifest = json.loads((partition / "manifest.json").read_text())
    trade_entry = manifest["source_identity"]["channels"]["trade_v2"][0]

    assert trade_entry["record_count"] == 2
    assert trade_entry["record_range"] == [0, 2]
    assert manifest["trade_record_count"] == 1
    trade_row = list(
        ReplayReader(replay_root).iter_trades(VENUE, SYMBOL, DATE)
    )[0]
    assert trade_row["raw_index"] == 1
    assert resolve_source_record(
        manifest["source_identity"], "trade_v2", 1
    )["contribution_ordinal"] == 1
    assert validate_partition(partition) is True


@pytest.mark.parametrize("channel", ["depth_v2", "trade_v2"])
def test_strict_v2_build_rejects_coexisting_compression_variants(
    tmp_path: Path,
    channel: str,
) -> None:
    raw_root = _sample_raw_root(tmp_path)
    plain_path = next(
        (raw_root / VENUE / channel / SYMBOL / DATE).glob("*.jsonl")
    )
    compressed_path = Path(f"{plain_path}.gz")
    with open(plain_path, "rb") as source, gzip.open(
        compressed_path, "wb"
    ) as destination:
        destination.write(source.read())

    result = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        tmp_path / "replay",
        schema_version=2,
    )

    assert result["status"] == "failed"
    assert any("compression variants" in error for error in result["errors"])


def test_prebuild_identity_failure_cleans_created_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline.build_replay_store as build_module

    def fail_identity(*_args, **_kwargs):
        raise OSError("injected identity failure")

    monkeypatch.setattr(
        build_module,
        "compute_repartitioned_source_identity",
        fail_identity,
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        tmp_path / "raw",
        replay_root,
        schema_version=2,
    )

    assert result["status"] == "failed"
    assert any("injected identity failure" in error for error in result["errors"])
    staging = (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f".staging_{DATE}_{SYMBOL}"
    )
    assert not staging.exists()


@pytest.mark.parametrize("channel", ["depth_v2", "trade_v2"])
def test_v2_build_fails_closed_on_corrupt_selected_raw_file(
    tmp_path: Path,
    channel: str,
) -> None:
    raw_root = _sample_raw_root(tmp_path)
    corrupt_path = (
        raw_root
        / VENUE
        / channel
        / SYMBOL
        / DATE
        / f"{DATE}T99.jsonl.zst"
    )
    corrupt_path.write_bytes(b"not-a-zstandard-frame")
    replay_root = tmp_path / "replay"

    result = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=2,
    )

    assert result["status"] == "failed"
    assert any(
        "selected raw input" in error or "source identity" in error
        for error in result["errors"]
    )
    assert not (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={DATE}"
    ).exists()


def test_v2_build_propagates_transient_midstream_raw_read_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pipeline.build_replay_store as build_module

    raw_root = _sample_raw_root(tmp_path)
    real_strict_stream = build_module._stream_raw_records_strict

    def transient_failure(venue, symbol, channel, date, data_root):
        for record in real_strict_stream(
            venue, symbol, channel, date, data_root
        ):
            yield record
            if channel == "trade_v2":
                raise OSError("injected transient streaming read failure")

    monkeypatch.setattr(
        build_module,
        "_stream_raw_records_strict",
        transient_failure,
    )
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=2,
    )

    assert result["status"] == "failed"
    assert any(
        "injected transient streaming read failure" in error
        for error in result["errors"]
    )
    assert not (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={DATE}"
    ).exists()


def test_v2_reuse_requires_live_source_identity_equality(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    first = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert first["status"] == "success"
    partition = (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={DATE}"
    )
    preserved_checksum = json.loads(
        (partition / "manifest.json").read_text()
    )["trades_checksum"]

    unchanged = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert unchanged["status"] == "skipped"

    trade_path = next(
        (raw_root / VENUE / "trade_v2" / SYMBOL / DATE).glob("*.jsonl")
    )
    with open(trade_path, "a") as trade_file:
        trade_file.write("\n")
    changed = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert changed["status"] == "failed"
    assert any("differs" in error for error in changed["errors"])
    assert json.loads(
        (partition / "manifest.json").read_text()
    )["trades_checksum"] == preserved_checksum


def test_v2_reuse_applies_requested_readiness_policy(tmp_path: Path) -> None:
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    first = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw_root, replay_root, schema_version=2
    )
    assert first["status"] == "success"

    reused = build_replay_for_symbol(
        VENUE,
        SYMBOL,
        DATE,
        raw_root,
        replay_root,
        schema_version=2,
        check_repartition_readiness=True,
        require_complete_next_day=True,
    )

    assert reused["status"] == "deferred"
    assert reused["errors"]
