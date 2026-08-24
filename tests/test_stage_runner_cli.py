"""Tests for ``validation.stage_runner_cli`` comparison subcommands and
report aggregation, run against small real on-disk Nautilus catalogs.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from converter.instruments import build_instruments
from stores.replay_schema import BUILDER_VERSION_V2
from validation.artifact_identity import (
    COMPOSITE_HASH_ALGORITHM,
    IDENTITY_SCHEMA,
    JSON_HASH_ALGORITHM,
    TREE_HASH_ALGORITHM,
    artifact_binding_summary,
    composite_hash,
    load_artifact_identity,
)
from validation.stage_runner_cli import (
    _SUBCOMMANDS,
    _cmd_checkpoints,
    _cmd_continuity,
    _cmd_deltas,
    _cmd_depth10,
    _cmd_fences,
    _cmd_integrity,
    _cmd_metadata,
    _cmd_report,
    _cmd_trades,
    main,
)


def _instrument():
    return build_instruments("BINANCE_SPOT", ["ADAUSDT"], {})[0]


def _write_trade_catalog(catalog_root: Path, instrument, count: int, *, corrupt_index: int | None = None) -> None:
    catalog = ParquetDataCatalog(str(catalog_root))
    catalog.write_data([instrument])
    ticks = []
    for i in range(count):
        price = "9999.0000" if i == corrupt_index else "1.0000"
        ticks.append(
            TradeTick(
                instrument_id=instrument.id,
                price=Price.from_str(price),
                size=Quantity.from_str("1"),
                aggressor_side=AggressorSide.BUYER,
                trade_id=TradeId(str(i)),
                ts_event=i * 1_000,
                ts_init=i * 1_000,
            )
        )
    catalog.write_data(ticks)


def test_trades_subcommand_matches_on_identical_catalogs(tmp_path: Path) -> None:
    instrument = _instrument()
    old_root = tmp_path / "old"
    new_root = tmp_path / "new" / "job_validation_new"
    count = 2_000
    _write_trade_catalog(old_root, instrument, count)
    _write_trade_catalog(new_root, instrument, count)

    config = {
        "old_catalog_root": str(old_root),
        "new_catalog_path": str(new_root),
        "instrument_ids": [str(instrument.id)],
        "start_ns": 0,
        "end_ns": count * 1_000,
    }
    result = _cmd_trades(config)

    assert result["passed"] is True
    assert result["by_instrument"][str(instrument.id)]["trade_count_match"] is True


def test_trades_subcommand_detects_near_end_mismatch(tmp_path: Path) -> None:
    """A single injected mismatch near the END of a large stream must be
    caught — proving exhaustiveness is preserved (no early-exit/sampling
    that would miss a late divergence)."""
    instrument = _instrument()
    old_root = tmp_path / "old"
    new_root = tmp_path / "new" / "job_validation_new"
    count = 50_000
    corrupt_index = count - 3  # near the very end
    _write_trade_catalog(old_root, instrument, count)
    _write_trade_catalog(new_root, instrument, count, corrupt_index=corrupt_index)

    config = {
        "old_catalog_root": str(old_root),
        "new_catalog_path": str(new_root),
        "instrument_ids": [str(instrument.id)],
        "start_ns": 0,
        "end_ns": count * 1_000,
    }
    result = _cmd_trades(config)

    assert result["passed"] is False
    per_instrument = result["by_instrument"][str(instrument.id)]
    assert per_instrument["passed"] is False
    mismatch_positions = [m["position"] for m in per_instrument["position_mismatches"]]
    assert corrupt_index in mismatch_positions


def test_trades_subcommand_detects_missing_trailing_event(tmp_path: Path) -> None:
    """One stream missing its final event (length divergence at the very
    end) must be detected, not just interior positional mismatches."""
    instrument = _instrument()
    old_root = tmp_path / "old"
    new_root = tmp_path / "new" / "job_validation_new"
    count = 3_000
    _write_trade_catalog(old_root, instrument, count)
    _write_trade_catalog(new_root, instrument, count - 1)  # missing the last trade

    config = {
        "old_catalog_root": str(old_root),
        "new_catalog_path": str(new_root),
        "instrument_ids": [str(instrument.id)],
        "start_ns": 0,
        "end_ns": count * 1_000,
    }
    result = _cmd_trades(config)

    assert result["passed"] is False
    per_instrument = result["by_instrument"][str(instrument.id)]
    assert per_instrument["trade_count_match"] is False
    assert per_instrument["first_length_divergence_position"] == count - 1


def test_deltas_subcommand_delegates_to_bounded_exhaustive_comparator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import catalog_compare

    monkeypatch.setattr(
        catalog_compare,
        "iter_order_book_deltas_bounded",
        lambda *args, **kwargs: iter(["delta"]),
    )
    monkeypatch.setattr(
        catalog_compare,
        "compare_order_book_deltas_exhaustive",
        lambda old, new: {
            "passed": list(old) == list(new) == ["delta"],
            "positions_compared": 1,
        },
    )
    result = _cmd_deltas(
        {
            "old_catalog_root": str(tmp_path / "old"),
            "new_catalog_path": str(tmp_path / "new"),
            "instrument_ids": ["ADAUSDT.BINANCE"],
            "start_ns": 0,
            "end_ns": 10,
        }
    )

    assert result["stage"] == "deltas"
    assert result["passed"] is True


def test_depth10_and_checkpoints_are_separate_stage_functions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import catalog_compare

    monkeypatch.setattr(
        catalog_compare,
        "iter_order_book_depth10_bounded",
        lambda *args, **kwargs: iter(["depth"]),
    )
    monkeypatch.setattr(
        catalog_compare,
        "compare_order_book_depth10_exhaustive",
        lambda old, new: {
            "passed": list(old) == list(new) == ["depth"],
            "positions_compared": 1,
        },
    )
    monkeypatch.setattr(
        catalog_compare,
        "iter_order_book_deltas_bounded",
        lambda *args, **kwargs: iter(["delta"]),
    )
    monkeypatch.setattr(
        catalog_compare,
        "compare_book_checkpoints_streaming",
        lambda old, new, start, end, levels: {
            "passed": list(old) == list(new) == ["delta"],
            "checkpoint_count": 7,
        },
    )
    config = {
        "old_catalog_root": str(tmp_path / "old"),
        "new_catalog_path": str(tmp_path / "new"),
        "instrument_ids": ["ADAUSDT.BINANCE"],
        "start_ns": 0,
        "end_ns": 10,
        "emit_depth10": True,
        "derived_depth_snapshot_levels": 10,
    }

    depth10 = _cmd_depth10(config)
    checkpoints = _cmd_checkpoints(config)

    assert depth10["stage"] == "depth10"
    assert depth10["passed"] is True
    assert checkpoints["stage"] == "checkpoints"
    assert checkpoints["passed"] is True


def test_continuity_and_fence_stages_use_complete_persisted_diagnostics(
    tmp_path: Path,
) -> None:
    from converter.depth_phase2 import canonical_fence_digest

    old_catalog = tmp_path / "old_catalog"
    old_catalog.mkdir()
    report_dir = tmp_path / "convert_reports"
    report_dir.mkdir()
    date = "2026-06-11"
    key = "BINANCE_SPOT/ADAUSDT"
    (report_dir / f"{date}.json").write_text(
        json.dumps(
            {
                "per_symbol_depth": {
                    key: {
                        "snapshot_seed_count": 1,
                        "resync_count": 0,
                        "desync_events": 0,
                        "fenced_ranges": 0,
                    }
                },
                "per_symbol_fenced_ranges": {
                    key: {
                        "canonical_count": 0,
                        "canonical_digest": canonical_fence_digest([]),
                    }
                },
            }
        )
    )
    new_catalog = tmp_path / "new_catalog"
    new_catalog.mkdir()
    (new_catalog / "manifest.json").write_text(
        json.dumps(
            {
                "depth_diagnostics": {
                    "snapshot_seeds": 1,
                    "resyncs": 0,
                    "desyncs": 0,
                    "fenced_range_count": 0,
                },
                "fenced_ranges": [],
            }
        )
    )
    config = {
        "date": date,
        "old_catalog_root": str(old_catalog),
        "new_catalog_path": str(new_catalog),
        "venue_symbols": [
            {
                "venue": "BINANCE_SPOT",
                "symbol": "ADAUSDT",
                "instrument_id": "ADAUSDT.BINANCE",
            }
        ],
    }

    continuity = _cmd_continuity(config)
    fences = _cmd_fences(config)

    assert continuity["passed"] is True
    assert continuity["by_instrument"]["ADAUSDT.BINANCE"].get("skipped") is not True
    assert fences["passed"] is True
    assert fences["by_instrument"]["ADAUSDT.BINANCE"]["digest_match"] is True


def test_metadata_stage_gates_live_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pipeline import build_replay_store
    from stores import replay_reader
    from validation import validate_catalog_equivalence

    identity = {
        "venue": "BINANCE_SPOT",
        "symbol": "ADAUSDT",
        "date": "2026-06-11",
        "channels": {"depth_v2": [{"path": "depth"}], "trade_v2": [{"path": "trade"}]},
        "complete": True,
        "missing_channels": [],
    }
    manifest = {
        "status": "complete",
        "schema_version": 2,
        "source_identity": identity,
        "integrity": {"source_identity": identity},
    }
    monkeypatch.setattr(
        validate_catalog_equivalence,
        "_compare_raw_to_replay_metadata_for_symbol",
        lambda *args: {
            "depth": {"passed": True},
            "trades": {"passed": True},
            "passed": True,
        },
    )
    monkeypatch.setattr(
        build_replay_store,
        "compute_repartitioned_source_identity",
        lambda *args, **kwargs: identity,
    )
    monkeypatch.setattr(
        build_replay_store,
        "check_depth_repartition_readiness",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        replay_reader.ReplayReader,
        "load_manifest",
        lambda self, venue, symbol, date: manifest,
    )

    result = _cmd_metadata(
        {
            "data_root": str(tmp_path / "raw"),
            "replay_root": str(tmp_path / "replay"),
            "date": "2026-06-11",
            "expected_schema_version": 2,
            "venue_symbols": [
                {
                    "venue": "BINANCE_SPOT",
                    "symbol": "ADAUSDT",
                    "instrument_id": "ADAUSDT.BINANCE",
                }
            ],
        }
    )

    source = result["by_instrument"]["ADAUSDT.BINANCE"]["source_identity"]
    assert result["passed"] is True
    assert source["recorded_matches_live"] is True
    assert source["integrity_matches_recorded"] is True


def test_integrity_stage_runs_routine_and_deep_audits(tmp_path: Path) -> None:
    from pipeline.build_replay_store import build_replay_for_symbol
    from tests.test_replay_schema_v1_corrections import _sample_raw_root

    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_SPOT",
        "ADAUSDT",
        "2026-06-12",
        raw_root,
        replay_root,
        schema_version=2,
    )
    assert result["status"] == "success"

    audit = _cmd_integrity(
        {
            "replay_root": str(replay_root),
            "date": "2026-06-12",
            "expected_schema_version": 2,
            "venue_symbols": [
                {
                    "venue": "BINANCE_SPOT",
                    "symbol": "ADAUSDT",
                    "instrument_id": "ADAUSDT.BINANCE",
                }
            ],
        }
    )

    per_instrument = audit["by_instrument"]["ADAUSDT.BINANCE"]
    assert audit["passed"] is True
    assert per_instrument["routine_valid"] is True
    assert per_instrument["deep_problem_count"] == 0


_SOURCE_COMPONENTS = {
    "reference_catalog_tree": {
        "algorithm": TREE_HASH_ALGORITHM,
        "file_count": 1,
        "total_bytes": 1,
        "sha256": "1" * 64,
    },
    "reference_report": {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": 1,
        "sha256": "2" * 64,
    },
    "raw_target_source_identity": {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": 1,
        "sha256": "3" * 64,
    },
}
_CANDIDATE_COMPONENTS = {
    "candidate_catalog_tree": {
        "algorithm": TREE_HASH_ALGORITHM,
        "file_count": 1,
        "total_bytes": 1,
        "sha256": "4" * 64,
    },
    "candidate_reconstruction_manifest": {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": 1,
        "sha256": "5" * 64,
    },
    "target_replay_manifest": {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": 1,
        "sha256": "6" * 64,
    },
    "no_carry_prelisting_marker": {
        "algorithm": JSON_HASH_ALGORITHM,
        "normalized_size_bytes": 1,
        "sha256": "7" * 64,
    },
}
_SCOPE = {
    "date": "2026-06-11",
    "venue": "BINANCE_SPOT",
    "symbol": "ADAUSDT",
    "instrument_id": "ADAUSDT.BINANCE",
    "source_identity_sha256": composite_hash("source", _SOURCE_COMPONENTS),
    "candidate_identity_sha256": composite_hash(
        "candidate", _CANDIDATE_COMPONENTS
    ),
}


def _write_artifact_identity(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "identity_schema": IDENTITY_SCHEMA,
                "scope": _SCOPE,
                "source": {
                    "algorithm": COMPOSITE_HASH_ALGORITHM,
                    "sha256": _SCOPE["source_identity_sha256"],
                    "components": _SOURCE_COMPONENTS,
                },
                "candidate": {
                    "algorithm": COMPOSITE_HASH_ALGORITHM,
                    "sha256": _SCOPE["candidate_identity_sha256"],
                    "components": _CANDIDATE_COMPONENTS,
                },
                "contracts": {
                    "reference_report": {
                        "date": "2026-06-11",
                        "architecture": "deterministic_native",
                        "status": "ok",
                    },
                    "candidate_reconstruction": {
                        "profile": "full_l2",
                        "time_filter": "ts_init",
                        "date": "2026-06-11",
                    },
                    "target_replay": {
                        "date": "2026-06-11",
                        "status": "complete",
                        "schema_version": 2,
                        "format_version": 2,
                        "builder_version": BUILDER_VERSION_V2,
                        "source_identity_complete": True,
                    },
                    "carry": {
                        "kind": "no_carry_prelisting",
                        "result": "not_applicable_pre_listing",
                        "date": "2026-06-10",
                        "venue": "BINANCE_SPOT",
                        "symbol": "ADAUSDT",
                    },
                    "raw_target_source_identity": {
                        "complete": True,
                        "channel_file_counts": {
                            "depth_v2": 1,
                            "trade_v2": 1,
                        },
                    },
                },
            }
        )
    )


def _identity_config(identity_path: Path) -> dict:
    return {
        "artifact_identity_path": str(identity_path),
        "scope": _SCOPE,
        "profile": "full_l2",
    }


def _bound_fragment(
    identity_path: Path,
    *,
    stage: str,
    passed: bool,
) -> dict:
    config = _identity_config(identity_path)
    identity = load_artifact_identity(identity_path)
    return {
        "stage": stage,
        "passed": passed,
        "scope": _SCOPE,
        "artifact_binding": artifact_binding_summary(identity, config),
    }


def _write_artifact_identity_variant(path: Path, *, candidate_byte: str) -> dict:
    _write_artifact_identity(path)
    document = json.loads(path.read_text())
    document["candidate"]["components"]["candidate_catalog_tree"]["sha256"] = (
        candidate_byte * 64
    )
    candidate_sha = composite_hash(
        "candidate", document["candidate"]["components"]
    )
    document["candidate"]["sha256"] = candidate_sha
    document["scope"]["candidate_identity_sha256"] = candidate_sha
    path.write_text(json.dumps(document))
    return document


def test_report_subcommand_aggregates_fragments_and_fails_if_any_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    frag_a = tmp_path / "a.json"
    frag_b = tmp_path / "b.json"
    frag_a.write_text(json.dumps(_bound_fragment(identity_path, stage="trades", passed=True)))
    frag_b.write_text(json.dumps(_bound_fragment(identity_path, stage="deltas", passed=False)))

    result = _cmd_report(
        {
            **_identity_config(identity_path),
            "fragment_paths": [str(frag_a), str(frag_b)],
        }
    )

    assert result["status"] == "failed"
    assert result["passed"] is False
    assert len(result["fragments"]) == 2


def test_report_subcommand_passes_when_all_fragments_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    frag_a = tmp_path / "a.json"
    frag_b = tmp_path / "b.json"
    frag_a.write_text(json.dumps(_bound_fragment(identity_path, stage="trades", passed=True)))
    frag_b.write_text(json.dumps(_bound_fragment(identity_path, stage="deltas", passed=True)))

    result = _cmd_report(
        {
            **_identity_config(identity_path),
            "fragment_paths": [str(frag_a), str(frag_b)],
        }
    )

    assert result["status"] == "passed"
    assert result["passed"] is True


def test_report_subcommand_fails_closed_when_required_stage_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    fragment = tmp_path / "trades.json"
    fragment.write_text(
        json.dumps(_bound_fragment(identity_path, stage="trades", passed=True))
    )

    result = _cmd_report(
        {
            **_identity_config(identity_path),
            "fragment_paths": [str(fragment)],
            "required_stages": ["trades", "deltas"],
        }
    )

    assert result["status"] == "failed"
    assert result["missing_stages"] == ["deltas"]
    assert result["stage_set_matches"] is False


def test_report_subcommand_rejects_cross_artifact_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    fragment = tmp_path / "trades.json"
    wrong_scope = {**_SCOPE, "symbol": "ETHUSDT"}
    wrong_fragment = _bound_fragment(identity_path, stage="trades", passed=True)
    wrong_fragment["scope"] = wrong_scope
    fragment.write_text(json.dumps(wrong_fragment))

    result = _cmd_report(
        {
            **_identity_config(identity_path),
            "fragment_paths": [str(fragment)],
            "required_stages": ["trades"],
        }
    )

    assert result["passed"] is False
    assert result["scope_mismatches"] == [
        {"stage": "trades", "actual_scope": wrong_scope}
    ]


def test_report_rejects_same_labels_with_different_artifact_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_a = tmp_path / "identity-a.json"
    identity_b = tmp_path / "identity-b.json"
    _write_artifact_identity(identity_a)
    document_b = _write_artifact_identity_variant(identity_b, candidate_byte="8")
    config_b = {
        "artifact_identity_path": str(identity_b),
        "scope": document_b["scope"],
        "profile": "full_l2",
    }
    foreign_fragment = tmp_path / "foreign.json"
    foreign_fragment.write_text(
        json.dumps(
            {
                "stage": "trades",
                "passed": True,
                "scope": document_b["scope"],
                "artifact_binding": artifact_binding_summary(
                    load_artifact_identity(identity_b), config_b
                ),
            }
        )
    )

    result = _cmd_report(
        {
            **_identity_config(identity_a),
            "fragment_paths": [str(foreign_fragment)],
            "required_stages": ["trades"],
        }
    )

    assert result["passed"] is False
    assert result["binding_mismatches"]


def test_report_rejects_mixed_fragments_from_two_artifact_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_a = tmp_path / "identity-a.json"
    identity_b = tmp_path / "identity-b.json"
    _write_artifact_identity(identity_a)
    document_b = _write_artifact_identity_variant(identity_b, candidate_byte="9")
    config_b = {
        "artifact_identity_path": str(identity_b),
        "scope": document_b["scope"],
        "profile": "full_l2",
    }
    trade = tmp_path / "trade.json"
    delta = tmp_path / "delta.json"
    trade.write_text(
        json.dumps(_bound_fragment(identity_a, stage="trades", passed=True))
    )
    delta.write_text(
        json.dumps(
            {
                "stage": "deltas",
                "passed": True,
                "scope": document_b["scope"],
                "artifact_binding": artifact_binding_summary(
                    load_artifact_identity(identity_b), config_b
                ),
            }
        )
    )

    result = _cmd_report(
        {
            **_identity_config(identity_a),
            "fragment_paths": [str(trade), str(delta)],
            "required_stages": ["trades", "deltas"],
        }
    )

    assert result["passed"] is False
    assert result["binding_mismatches"] == [
        {
            "stage": "deltas",
            "identity_document_sha256": artifact_binding_summary(
                load_artifact_identity(identity_b), config_b
            )["identity_document_sha256"],
        }
    ]


def test_report_rejects_identity_document_changed_after_fragment_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    fragment = tmp_path / "trades.json"
    fragment.write_text(
        json.dumps(_bound_fragment(identity_path, stage="trades", passed=True))
    )
    changed = _write_artifact_identity_variant(identity_path, candidate_byte="a")

    result = _cmd_report(
        {
            "artifact_identity_path": str(identity_path),
            "scope": changed["scope"],
            "profile": "full_l2",
            "fragment_paths": [str(fragment)],
            "required_stages": ["trades"],
        }
    )

    assert result["passed"] is False
    assert result["binding_mismatches"]


def test_stage_fails_when_artifact_mutates_after_handler_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(_identity_config(identity_path)))
    out_path = tmp_path / "out.json"
    verify_calls = 0
    handler_calls = 0

    def _verify(*args):
        nonlocal verify_calls
        verify_calls += 1
        if verify_calls == 2:
            raise ValueError("artifact changed after stage")

    def _handler(config):
        nonlocal handler_calls
        handler_calls += 1
        return {"stage": "trades", "passed": True}

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", _verify)
    monkeypatch.setitem(_SUBCOMMANDS, "trades", _handler)

    assert main(
        ["trades", "--config", str(config_path), "--out", str(out_path)]
    ) == 1
    fragment = json.loads(out_path.read_text())
    assert fragment["passed"] is False
    assert "artifact changed after stage" in fragment["error"]
    assert handler_calls == 1
    assert verify_calls == 2


def test_cli_returns_nonzero_and_writes_scoped_failed_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    config_path = tmp_path / "config.json"
    out_path = tmp_path / "out.json"
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    config_path.write_text(
        json.dumps(
            {
                "scope": _SCOPE,
                "artifact_identity_path": str(identity_path),
            }
        )
    )
    monkeypatch.setitem(
        _SUBCOMMANDS,
        "trades",
        lambda config: {"stage": "trades", "passed": False},
    )

    exit_code = main(
        [
            "trades",
            "--config",
            str(config_path),
            "--out",
            str(out_path),
        ]
    )

    fragment = json.loads(out_path.read_text())
    assert exit_code == 1
    assert fragment["passed"] is False
    assert fragment["scope"] == _SCOPE


def test_cli_rejects_scope_not_bound_to_artifact_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "artifact_identity_path": str(identity_path),
                "scope": {**_SCOPE, "symbol": "ETHUSDT"},
            }
        )
    )
    out_path = tmp_path / "out.json"
    called = False

    def _handler(config):
        nonlocal called
        called = True
        return {"stage": "trades", "passed": True}

    monkeypatch.setitem(_SUBCOMMANDS, "trades", _handler)
    assert main(
        ["trades", "--config", str(config_path), "--out", str(out_path)]
    ) == 1
    assert called is False
    fragment = json.loads(out_path.read_text())
    assert fragment["passed"] is False
    assert "does not exactly match" in fragment["error"]


def test_cli_refuses_existing_output_before_running_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    monkeypatch.setattr(artifact_identity, "verify_artifact_inputs", lambda *args: None)
    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "artifact_identity_path": str(identity_path),
                "scope": _SCOPE,
            }
        )
    )
    out_path = tmp_path / "out.json"
    out_path.write_text("preserve-me")

    def _unexpected(config):
        raise AssertionError("handler must not run")

    monkeypatch.setitem(_SUBCOMMANDS, "trades", _unexpected)
    assert main(
        ["trades", "--config", str(config_path), "--out", str(out_path)]
    ) == 2
    assert out_path.read_text() == "preserve-me"


def test_report_cli_performs_final_artifact_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from validation import artifact_identity

    identity_path = tmp_path / "identity.json"
    _write_artifact_identity(identity_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "artifact_identity_path": str(identity_path),
                "scope": _SCOPE,
            }
        )
    )
    out_path = tmp_path / "out.json"
    calls: list[tuple[dict, dict]] = []
    monkeypatch.setattr(
        artifact_identity,
        "verify_artifact_inputs",
        lambda config, identity: calls.append((config, identity)),
    )
    monkeypatch.setitem(
        _SUBCOMMANDS,
        "report",
        lambda config: {"stage": "report", "passed": True},
    )

    assert main(
        ["report", "--config", str(config_path), "--out", str(out_path)]
    ) == 0
    assert len(calls) == 2
    assert calls[0][1]["scope"] == _SCOPE
