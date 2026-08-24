"""Focused tests for path-sanitized semantic-gate artifact identities."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from stores.replay_schema import BUILDER_VERSION_V2
from validation.artifact_identity import (
    IDENTITY_SCHEMA,
    canonical_identity_document_bytes,
    build_artifact_identity,
    hash_tree,
    identity_document_sha256,
    main,
    validate_artifact_identity_document,
    verify_artifact_inputs,
    write_identity_exclusive,
)

DATE = "2026-06-11"
VENUE = "BINANCE_SPOT"
SYMBOL = "ADAUSDT"
INSTRUMENT_ID = "ADAUSDT.BINANCE"


def _source_identity(date: str) -> dict:
    return {
        "venue": VENUE,
        "symbol": SYMBOL,
        "date": date,
        "channels": {
            "depth_v2": [
                {
                    "path": f"{VENUE}/depth_v2/{SYMBOL}/{date}/depth.jsonl.zst",
                    "sha256": "1" * 64,
                    "size_bytes": 11,
                    "record_count": 2,
                    "record_range": [0, 2],
                    "source_date": date,
                }
            ],
            "trade_v2": [
                {
                    "path": f"{VENUE}/trade_v2/{SYMBOL}/{date}/trades.jsonl.zst",
                    "sha256": "2" * 64,
                    "size_bytes": 13,
                    "record_count": 3,
                    "record_range": [0, 3],
                }
            ],
        },
        "complete": True,
        "missing_channels": [],
    }


def _replay_manifest(date: str, identity: dict) -> dict:
    return {
        "venue": VENUE,
        "symbol": SYMBOL,
        "date": date,
        "status": "complete",
        "schema_version": 2,
        "format_version": 2,
        "builder_version": BUILDER_VERSION_V2,
        "source_identity": identity,
        "integrity": {
            "source_identity": identity,
            "depth_blocks": [],
            "trade_blocks": [],
        },
        "created_at_utc": "2026-07-30T00:00:00+00:00",
    }


def _partition(replay_root: Path, date: str) -> Path:
    return (
        replay_root
        / f"venue={VENUE}"
        / f"symbol={SYMBOL}"
        / f"date={date}"
    )


def _write_fixture(root: Path, *, carry: bool = True) -> tuple[dict, dict]:
    data_root = root / "raw"
    data_root.mkdir(parents=True)
    replay_root = root / "replay"
    replay_root.mkdir()

    target_identity = _source_identity(DATE)
    target_partition = _partition(replay_root, DATE)
    target_partition.mkdir(parents=True)
    (target_partition / "manifest.json").write_text(
        json.dumps(_replay_manifest(DATE, target_identity))
    )

    previous_date = (
        datetime.strptime(DATE, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    if carry:
        carry_identity = _source_identity(previous_date)
        carry_partition = _partition(replay_root, previous_date)
        carry_partition.mkdir(parents=True)
        (carry_partition / "manifest.json").write_text(
            json.dumps(_replay_manifest(previous_date, carry_identity))
        )

    reference_catalog = root / "reference_catalog"
    (reference_catalog / "data" / "trade_tick").mkdir(parents=True)
    (reference_catalog / "data" / "trade_tick" / "part.parquet").write_bytes(
        b"reference-catalog-bytes"
    )
    reference_report_path = root / "convert_reports" / f"{DATE}.json"
    reference_report_path.parent.mkdir()
    key = f"{VENUE}/{SYMBOL}"
    reference_report_path.write_text(
        json.dumps(
            {
                "date": DATE,
                "architecture": "deterministic_native",
                "status": "ok",
                "catalog_root": str(reference_catalog),
                "symbols_processed": {VENUE: [SYMBOL]},
                "per_symbol_trade": {key: {"count": 3}},
                "per_symbol_depth": {key: {"count": 2}},
                "per_symbol_fenced_ranges": {key: {"canonical_count": 0}},
                "staging_publication": {
                    "live_catalog_root": str(reference_catalog),
                    "published_live_parquets": [
                        str(
                            reference_catalog
                            / "data"
                            / "trade_tick"
                            / "part.parquet"
                        )
                    ],
                },
            }
        )
    )

    candidate_catalog = root / "candidate_catalog"
    (candidate_catalog / "data" / "trade_tick").mkdir(parents=True)
    (candidate_catalog / "data" / "trade_tick" / "part.parquet").write_bytes(
        b"candidate-catalog-bytes"
    )
    candidate_manifest_path = candidate_catalog / "manifest.json"
    candidate_manifest_path.write_text(
        json.dumps(
            {
                "profile": "full_l2",
                "requested_symbols": [SYMBOL],
                "requested_venues": [VENUE],
                "symbols": [f"{VENUE}:{SYMBOL}"],
                "found_partitions": [
                    {"venue": VENUE, "symbol": SYMBOL, "date": DATE}
                ],
                "missing_partitions": [],
                "time_filter": "ts_init",
                "time_window": {
                    "start": f"{DATE}T00:00:00+00:00",
                    "end": "2026-06-12T00:00:00+00:00",
                },
                "replay_source": str(replay_root),
                "record_counts": {
                    "trade_ticks": 3,
                    "order_book_deltas": 2,
                    "order_book_depth10": 1,
                },
            }
        )
    )

    config = {
        "date": DATE,
        "venue": VENUE,
        "symbol": SYMBOL,
        "instrument_id": INSTRUMENT_ID,
        "data_root": str(data_root),
        "replay_root": str(replay_root),
        "reference_catalog_root": str(reference_catalog),
        "reference_report_path": str(reference_report_path),
        "candidate_catalog_root": str(candidate_catalog),
        "candidate_reconstruction_manifest_path": str(candidate_manifest_path),
        "carry": {
            "kind": "replay_manifest" if carry else "no_carry_prelisting",
            "date": previous_date,
        },
    }
    return config, target_identity


def _patch_raw_identity(
    monkeypatch: pytest.MonkeyPatch,
    target_identity: dict,
    *,
    previous_has_records: bool = False,
) -> None:
    from pipeline import build_replay_store

    monkeypatch.setattr(
        build_replay_store,
        "compute_repartitioned_source_identity",
        lambda *args, **kwargs: copy.deepcopy(target_identity),
    )
    monkeypatch.setattr(
        build_replay_store,
        "replay_partition_has_source_records",
        lambda *args, **kwargs: previous_has_records,
    )


def test_tree_hash_is_root_independent_length_framed_and_fail_closed(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    for root in (first, second):
        (root / "nested").mkdir(parents=True)
        (root / "nested" / "a").write_bytes(b"bc")
        (root / "other").write_bytes(b"d")

    assert hash_tree(first) == hash_tree(second)

    (second / "nested" / "a").write_bytes(b"b")
    (second / "other").write_bytes(b"cd")
    assert hash_tree(first)["sha256"] != hash_tree(second)["sha256"]

    (first / "link").symlink_to(first / "other")
    with pytest.raises(ValueError, match="symlink"):
        hash_tree(first)


def test_identity_is_path_sanitized_and_relocation_independent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_a, target_identity = _write_fixture(tmp_path / "machine-a")
    _patch_raw_identity(monkeypatch, target_identity)
    identity_a = build_artifact_identity(config_a)

    config_b, _ = _write_fixture(tmp_path / "machine-b")
    identity_b = build_artifact_identity(config_b)

    assert identity_a == identity_b
    assert canonical_identity_document_bytes(identity_a) == canonical_identity_document_bytes(
        identity_b
    )
    assert identity_document_sha256(identity_a) == identity_document_sha256(identity_b)
    assert identity_a["identity_schema"] == IDENTITY_SCHEMA
    encoded = json.dumps(identity_a)
    assert str(tmp_path) not in encoded
    assert "/machine-a/" not in encoded
    assert identity_a["scope"]["source_identity_sha256"] == identity_a["source"]["sha256"]
    assert (
        identity_a["scope"]["candidate_identity_sha256"]
        == identity_a["candidate"]["sha256"]
    )

    identity_b["contracts"]["candidate_reconstruction"]["profile"] = "trades_only"
    with pytest.raises(ValueError):
        identity_document_sha256(identity_b)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("status", "failed"),
        ("schema_version", 1),
        ("format_version", 1),
        ("builder_version", "wrong-builder"),
    ],
)
def test_identity_rejects_wrong_target_replay_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    bad_value: object,
) -> None:
    config, target_identity = _write_fixture(tmp_path)
    _patch_raw_identity(monkeypatch, target_identity)
    manifest_path = _partition(Path(config["replay_root"]), DATE) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest[field] = bad_value
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="replay manifest contract mismatch"):
        build_artifact_identity(config)


def test_identity_rejects_incomplete_or_changed_target_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, target_identity = _write_fixture(tmp_path)
    incomplete = copy.deepcopy(target_identity)
    incomplete["complete"] = False
    _patch_raw_identity(monkeypatch, incomplete)

    with pytest.raises(ValueError, match="complete"):
        build_artifact_identity(config)

    changed = copy.deepcopy(target_identity)
    changed["channels"]["trade_v2"][0]["sha256"] = "3" * 64
    _patch_raw_identity(monkeypatch, changed)
    with pytest.raises(ValueError, match="does not exactly match"):
        build_artifact_identity(config)


def test_explicit_no_carry_marker_probes_source_and_rejects_stale_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, target_identity = _write_fixture(tmp_path, carry=False)
    _patch_raw_identity(monkeypatch, target_identity)

    identity = build_artifact_identity(config)
    assert "no_carry_prelisting_marker" in identity["candidate"]["components"]
    assert identity["contracts"]["carry"]["result"] == "not_applicable_pre_listing"
    assert str(config["data_root"]) not in json.dumps(identity)

    _patch_raw_identity(
        monkeypatch,
        target_identity,
        previous_has_records=True,
    )
    with pytest.raises(ValueError, match="source records exist"):
        build_artifact_identity(config)

    _patch_raw_identity(monkeypatch, target_identity)
    carry_partition = _partition(
        Path(config["replay_root"]),
        config["carry"]["date"],
    )
    carry_partition.mkdir(parents=True)
    with pytest.raises(ValueError, match="replay partition exists"):
        build_artifact_identity(config)


def test_identity_document_detects_tampering_and_final_recheck_detects_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, target_identity = _write_fixture(tmp_path)
    _patch_raw_identity(monkeypatch, target_identity)
    identity = build_artifact_identity(config)

    tampered = copy.deepcopy(identity)
    tampered["source"]["components"]["reference_report"]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="source composite"):
        validate_artifact_identity_document(tampered)

    verify_artifact_inputs(config, identity)
    candidate_data = (
        Path(config["candidate_catalog_root"])
        / "data"
        / "trade_tick"
        / "part.parquet"
    )
    candidate_data.write_bytes(b"mutated")
    with pytest.raises(ValueError, match="candidate"):
        verify_artifact_inputs(config, identity)


def test_identity_cli_and_writer_refuse_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, target_identity = _write_fixture(tmp_path)
    _patch_raw_identity(monkeypatch, target_identity)
    config_path = tmp_path / "identity-config.json"
    config_path.write_text(json.dumps(config))
    out_path = tmp_path / "identity.json"

    assert main(["--config", str(config_path), "--out", str(out_path)]) == 0
    original = out_path.read_bytes()
    assert main(["--config", str(config_path), "--out", str(out_path)]) == 2
    assert out_path.read_bytes() == original

    with pytest.raises(FileExistsError):
        write_identity_exclusive(
            out_path,
            json.loads(original),
        )
