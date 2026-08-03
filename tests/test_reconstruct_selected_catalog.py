"""Supported selected-catalog API/CLI contract and publication safety."""
from __future__ import annotations

import json
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.reconstruct_selected_catalog import (
    MANIFEST_VERSION,
    SelectedCatalogError,
    SelectedCatalogRequest,
    _new_job_claim,
    _normalize_request,
    _parser,
    _preflight,
    _remove_claim,
    reconstruct_selected_catalog,
)
from tests.test_replay_catalog_reconstruct import (
    DATE,
    INSTRUMENT_ID,
    SYMBOL,
    VENUE,
    WINDOW_END,
    WINDOW_START,
    _sample_raw_root,
    _write_jsonl,
)
from validation.catalog_compare import (
    load_order_book_deltas,
    load_order_book_depth10,
    load_trade_ticks,
)


def _request(tmp_path: Path, replay_root: Path, **changes) -> SelectedCatalogRequest:
    values = {
        "replay_root": replay_root,
        "venues": [VENUE],
        "symbols": [SYMBOL],
        "start": WINDOW_START,
        "end": WINDOW_END,
        "output_root": tmp_path / "jobs",
        "job_id": "safe-job_1",
        "profile": "full_l2",
        "overwrite": False,
    }
    values.update(changes)
    Path(values["output_root"]).mkdir(parents=True, exist_ok=True)
    return SelectedCatalogRequest(**values)


def _build_supported_replay(tmp_path: Path) -> Path:
    raw = _sample_raw_root(tmp_path)
    _write_jsonl(
        raw / VENUE / "exchangeinfo" / "EXCHANGEINFO" / DATE / f"{DATE}T00.jsonl",
        [{"symbols": [{
            "symbol": SYMBOL,
            "baseAsset": "ADA",
            "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1"},
                {"filterType": "NOTIONAL", "minNotional": "5.0"},
            ],
        }]}],
    )
    replay = tmp_path / "replay"
    result = build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw, replay, schema_version=2,
        price_scale=4, qty_scale=1,
    )
    assert result["status"] == "success"
    return replay


def _fake_preflight() -> dict:
    inventory = [{
        "venue": VENUE,
        "symbol": SYMBOL,
        "date": DATE,
        "roles": ["target"],
        "relative_path": f"venue={VENUE}/symbol={SYMBOL}/date={DATE}",
    }]
    return {"target_dates": [DATE], "partitions": inventory, "inventory_sha256": "a" * 64}


def _fake_engine(*, fail: bool = False):
    def generate(replay_root, catalog_root, job_id, symbols, venues, start, end, **kwargs):
        if fail:
            return {"status": "failed", "errors": ["injected failure"]}
        catalog = Path(catalog_root) / f"job_{job_id}"
        catalog.mkdir()
        (catalog / "data.parquet").write_bytes(b"catalog")
        return {
            "status": "success",
            "errors": [],
            "warnings": [],
            "missing_partitions": [],
            "found_partitions": [{"venue": venues[0], "symbol": symbols[0], "date": DATE}],
            "records_written": {
                "trade_ticks": 1,
                "order_book_deltas": 2,
                "order_book_depth10": 1,
            },
            "partition_record_counts": [{
                "venue": venues[0], "symbol": symbols[0], "date": DATE,
                "trade_ticks": 1, "order_book_deltas": 2, "order_book_depth10": 1,
            }],
        }
    return SimpleNamespace(generate_catalog_from_replay=generate)


def _patch_fast_success(monkeypatch) -> None:
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._preflight", lambda request: _fake_preflight())
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._rehash_preflight", lambda request, original: original)
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._load_engine", lambda: _fake_engine())
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._repository_commit", lambda: "1" * 40)


def test_cli_requires_every_mandatory_argument() -> None:
    parser = _parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])
    args = parser.parse_args([
        "--replay-root", "/replay", "--venues", VENUE, "--symbols", SYMBOL,
        "--start", "2026-06-12T00:00:00Z", "--end", "2026-06-13T00:00:00Z",
        "--output-root", "/jobs", "--job-id", "job", "--profile", "full_l2",
    ])
    assert args.venues == [VENUE]
    assert args.symbols == [SYMBOL]


@pytest.mark.parametrize("field,value", [("venues", []), ("symbols", [])])
def test_empty_selections_fail(tmp_path: Path, field: str, value: list[str]) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    request = _request(tmp_path, replay, **{field: value})
    with pytest.raises(SelectedCatalogError, match="at least one"):
        _normalize_request(request)


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"start": "2026-06-12T00:00:00", "end": "2026-06-13T00:00:00Z"}, "UTC offset"),
        ({"start": WINDOW_END, "end": WINDOW_START}, "strictly before"),
        ({"start": WINDOW_START, "end": WINDOW_START}, "strictly before"),
        ({"venues": [VENUE, VENUE]}, "duplicate venue"),
        ({"symbols": [SYMBOL, SYMBOL]}, "duplicate symbol"),
        ({"profile": "depth_only"}, "unsupported profile"),
    ],
)
def test_request_contract_rejects_ambiguity(tmp_path: Path, changes: dict, match: str) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    with pytest.raises(SelectedCatalogError, match=match):
        _normalize_request(_request(tmp_path, replay, **changes))


@pytest.mark.parametrize(
    "job_id", ["/absolute", "..", "a..b", "a/b", "a\\b", "", ".hidden/child"]
)
def test_job_id_path_escape_fails(tmp_path: Path, job_id: str) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    with pytest.raises(SelectedCatalogError, match="job_id"):
        _normalize_request(_request(tmp_path, replay, job_id=job_id))


def test_symlink_output_root_and_final_job_fail(tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)
    with pytest.raises(SelectedCatalogError, match="symlink"):
        _normalize_request(_request(tmp_path, replay, output_root=linked))

    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "safe-job_1").symlink_to(real, target_is_directory=True)
    with pytest.raises(SelectedCatalogError, match="unsafe"):
        reconstruct_selected_catalog(request=_request(
            tmp_path, replay, output_root=jobs, overwrite=True
        ))


def test_missing_requested_partition_fails_before_publication(tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    request = _request(tmp_path, replay)
    with pytest.raises(SelectedCatalogError, match="missing"):
        reconstruct_selected_catalog(request=request)
    assert not (request.output_root / request.job_id).exists()


def test_invalid_instrument_checksum_and_format_fail_preflight(tmp_path: Path) -> None:
    replay = _build_supported_replay(tmp_path)
    request = _request(tmp_path, replay, profile="trades_only")
    partition = replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"

    instrument_path = partition / "instrument.json"
    original_instrument = instrument_path.read_text()
    instrument_path.unlink()
    with pytest.raises(SelectedCatalogError, match="missing or unsafe"):
        _preflight(_normalize_request(request))
    instrument_path.write_text(original_instrument)

    instrument_path.write_text("{}")
    with pytest.raises(SelectedCatalogError, match="instrument metadata"):
        _preflight(_normalize_request(request))
    instrument_path.write_text(original_instrument)

    manifest_path = partition / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["depth_checksum"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(SelectedCatalogError, match="routine validation"):
        _preflight(_normalize_request(request))


def test_unsupported_declared_schema_format_fails(tmp_path: Path) -> None:
    replay = _build_supported_replay(tmp_path)
    request = _request(tmp_path, replay, profile="trades_only")
    manifest_path = replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = 99
    manifest["format_version"] = 99
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(SelectedCatalogError, match="routine validation"):
        _preflight(_normalize_request(request))


def test_safe_job_manifest_binds_replay_and_catalog_hashes(tmp_path: Path) -> None:
    replay = _build_supported_replay(tmp_path)
    result = reconstruct_selected_catalog(request=_request(tmp_path, replay))
    manifest = json.loads((result / "job_manifest.json").read_text())
    assert result == tmp_path / "jobs" / "safe-job_1"
    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["status"] == "complete"
    assert manifest["normalized_request"]["interval"] == "[start,end)"
    assert len(manifest["target_replay_partitions"]) == 1
    consumed = manifest["target_replay_partitions"][0]
    assert consumed["files"]["manifest.json"]["sha256"]
    assert consumed["files"]["depth.parquet"]["sha256"]
    assert consumed["files"]["trades.parquet"]["sha256"]
    assert consumed["files"]["instrument.json"]["sha256"]
    assert manifest["consumed_partition_inventory_digest"]["sha256"]
    assert manifest["catalog_tree_digest"]["sha256"]
    assert load_trade_ticks(result / "catalog", INSTRUMENT_ID)
    assert load_order_book_deltas(result / "catalog", INSTRUMENT_ID)
    assert load_order_book_depth10(result / "catalog", INSTRUMENT_ID)


def test_trades_only_secondary_profile_is_supported(tmp_path: Path) -> None:
    replay = _build_supported_replay(tmp_path)
    result = reconstruct_selected_catalog(request=_request(
        tmp_path, replay, profile="trades_only", job_id="trades-only"
    ))
    manifest = json.loads((result / "job_manifest.json").read_text())
    assert manifest["profile"] == "trades_only"
    assert manifest["record_counts"]["trade_ticks"] == 2
    assert manifest["record_counts"]["order_book_deltas"] == 0


def test_explicit_scope_and_end_exclusive_are_forwarded(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    captured = {}
    engine = _fake_engine()
    original = engine.generate_catalog_from_replay

    def spy(*args, **kwargs):
        captured["symbols"] = args[3]
        captured["venues"] = args[4]
        captured["start"] = args[5]
        captured["end"] = args[6]
        return original(*args, **kwargs)

    engine.generate_catalog_from_replay = spy
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._preflight", lambda request: _fake_preflight())
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._rehash_preflight", lambda request, original: original)
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._load_engine", lambda: engine)
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._repository_commit", lambda: "1" * 40)
    reconstruct_selected_catalog(request=_request(tmp_path, replay))
    assert captured == {
        "symbols": [SYMBOL], "venues": [VENUE], "start": WINDOW_START, "end": WINDOW_END,
    }


def test_failure_preserves_evidence_and_never_publishes(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._preflight", lambda request: _fake_preflight())
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._load_engine", lambda: _fake_engine(fail=True))
    request = _request(tmp_path, replay)
    with pytest.raises(SelectedCatalogError, match="injected failure"):
        reconstruct_selected_catalog(request=request)
    assert not (request.output_root / request.job_id).exists()
    failed = list(request.output_root.glob(".failed_safe-job_1_*"))
    assert len(failed) == 1
    assert json.loads((failed[0] / "failure.json").read_text())["status"] == "failed"


def test_artifact_mutation_is_detected_before_publication(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._preflight", lambda request: _fake_preflight())
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._load_engine", lambda: _fake_engine())
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._rehash_preflight",
        lambda request, original: (_ for _ in ()).throw(SelectedCatalogError("identity changed")),
    )
    request = _request(tmp_path, replay)
    with pytest.raises(SelectedCatalogError, match="identity changed"):
        reconstruct_selected_catalog(request=request)
    assert not (request.output_root / request.job_id).exists()


def test_existing_job_requires_explicit_exact_overwrite(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    request = _request(tmp_path, replay)
    final = reconstruct_selected_catalog(request=request)
    sibling = request.output_root / "keep-me"
    sibling.write_text("unchanged")
    with pytest.raises(SelectedCatalogError, match="already exists"):
        reconstruct_selected_catalog(request=request)
    replaced = reconstruct_selected_catalog(request=_request(tmp_path, replay, overwrite=True))
    assert replaced == final
    assert sibling.read_text() == "unchanged"
    assert json.loads((replaced / "job_manifest.json").read_text())["status"] == "complete"


def test_preflight_records_preceding_carry_and_multiday_scope(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    for date in ("2026-06-11", "2026-06-12", "2026-06-13"):
        (replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={date}").mkdir(parents=True)
    normalized = _normalize_request(_request(
        tmp_path,
        replay,
        start="2026-06-12T12:00:00Z",
        end="2026-06-14T00:00:00Z",
    ))
    seen = []

    def inventory(request, venue, symbol, date, roles):
        seen.append((venue, symbol, date, sorted(roles)))
        return {
            "venue": venue,
            "symbol": symbol,
            "date": date,
            "roles": sorted(roles),
            "files": {"instrument.json": {"sha256": "a" * 64}},
        }

    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._partition_inventory", inventory)
    result = _preflight(normalized)
    assert result["target_dates"] == ["2026-06-12", "2026-06-13"]
    assert seen == [
        (VENUE, SYMBOL, "2026-06-11", ["preceding_carry"]),
        (VENUE, SYMBOL, "2026-06-12", ["preceding_carry", "target"]),
        (VENUE, SYMBOL, "2026-06-13", ["target"]),
    ]


def test_missing_optional_dependency_has_actionable_guidance(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    monkeypatch.setattr("pipeline.reconstruct_selected_catalog._preflight", lambda request: _fake_preflight())
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._load_engine",
        lambda: (_ for _ in ()).throw(SelectedCatalogError(
            "selected reconstruction dependencies are required; "
            "uv sync --frozen --no-default-groups --extra reconstruction"
        )),
    )
    with pytest.raises(SelectedCatalogError, match="uv sync --frozen"):
        reconstruct_selected_catalog(request=_request(tmp_path, replay))


def test_same_job_concurrent_invocation_is_rejected(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    started = threading.Event()
    release = threading.Event()
    base_engine = _fake_engine()

    def generate(*args, **kwargs):
        started.set()
        assert release.wait(timeout=5)
        return base_engine.generate_catalog_from_replay(*args, **kwargs)

    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._preflight",
        lambda request: _fake_preflight(),
    )
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._rehash_preflight",
        lambda request, original: original,
    )
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._load_engine",
        lambda: SimpleNamespace(generate_catalog_from_replay=generate),
    )
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._repository_commit",
        lambda: "1" * 40,
    )
    request = _request(tmp_path, replay)
    result: list[Path] = []
    errors: list[BaseException] = []

    def first_invocation() -> None:
        try:
            result.append(reconstruct_selected_catalog(request=request))
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    worker = threading.Thread(target=first_invocation)
    worker.start()
    assert started.wait(timeout=5)
    with pytest.raises(SelectedCatalogError, match="already claimed"):
        reconstruct_selected_catalog(request=request)
    release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert errors == []
    assert result == [request.output_root / request.job_id]
    assert not (request.output_root / f".claim_{request.job_id}").exists()


def test_different_job_claims_are_independent(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    first = _normalize_request(_request(tmp_path, replay, job_id="first"))
    claim = _new_job_claim(first)
    try:
        second = reconstruct_selected_catalog(
            request=_request(tmp_path, replay, job_id="second")
        )
        assert second.name == "second"
        assert claim.path.exists()
    finally:
        _remove_claim(claim)


def test_non_overwrite_rechecks_before_publication(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    request = _request(tmp_path, replay)
    engine = _fake_engine()
    original = engine.generate_catalog_from_replay

    def concurrent_completion(*args, **kwargs):
        final = request.output_root / request.job_id
        final.mkdir()
        (final / "job_manifest.json").write_text(json.dumps({
            "manifest_version": MANIFEST_VERSION,
            "job_id": request.job_id,
            "status": "complete",
        }))
        (final / "owner-marker").write_text("other invocation")
        return original(*args, **kwargs)

    _patch_fast_success(monkeypatch)
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._load_engine",
        lambda: SimpleNamespace(generate_catalog_from_replay=concurrent_completion),
    )
    with pytest.raises(SelectedCatalogError, match="already exists"):
        reconstruct_selected_catalog(request=request)
    final = request.output_root / request.job_id
    assert (final / "owner-marker").read_text() == "other invocation"
    assert not list(request.output_root.glob(".replaced_*"))


def test_handled_failure_releases_exact_claim(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._preflight",
        lambda request: _fake_preflight(),
    )
    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._load_engine",
        lambda: _fake_engine(fail=True),
    )
    request = _request(tmp_path, replay)
    with pytest.raises(SelectedCatalogError, match="injected failure"):
        reconstruct_selected_catalog(request=request)
    assert not (request.output_root / f".claim_{request.job_id}").exists()


def test_crash_claim_residue_blocks_mutation(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    request = _request(tmp_path, replay)
    claim = _new_job_claim(_normalize_request(request))

    with pytest.raises(SelectedCatalogError, match="manual recovery"):
        reconstruct_selected_catalog(request=request)
    assert claim.path.exists()
    assert set(path.name for path in claim.path.iterdir()) == {
        "claim.json",
    }


def test_unsafe_claim_symlink_fails_closed(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    request = _request(tmp_path, replay)
    outside = tmp_path / "outside-claim"
    outside.mkdir()
    claim_path = request.output_root / f".claim_{request.job_id}"
    claim_path.symlink_to(outside, target_is_directory=True)
    with pytest.raises(SelectedCatalogError, match="claim is unsafe"):
        reconstruct_selected_catalog(request=request)
    assert claim_path.is_symlink()


def test_overwrite_exchange_failure_rolls_back_exact_job(monkeypatch, tmp_path: Path) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    request = _request(tmp_path, replay)
    final = reconstruct_selected_catalog(request=request)
    marker = final / "old-marker"
    marker.write_text("preserve old job")
    original_claim_state = __import__(
        "pipeline.reconstruct_selected_catalog", fromlist=["_claim_state"]
    )._claim_state

    def fail_after_exchange(claim, state, **updates):
        if state == "jobs_exchanged":
            raise OSError("injected state-write failure")
        return original_claim_state(claim, state, **updates)

    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog._claim_state", fail_after_exchange
    )
    with pytest.raises(SelectedCatalogError, match="injected state-write failure"):
        reconstruct_selected_catalog(
            request=_request(tmp_path, replay, overwrite=True)
        )
    assert marker.read_text() == "preserve old job"
    assert json.loads((final / "job_manifest.json").read_text())["status"] == "complete"
    assert not list(request.output_root.glob(".replaced_*"))


def test_backup_cleanup_failure_is_success_with_manifest_warning(
    monkeypatch, tmp_path: Path,
) -> None:
    replay = tmp_path / "replay"
    replay.mkdir()
    _patch_fast_success(monkeypatch)
    request = _request(tmp_path, replay)
    final = reconstruct_selected_catalog(request=request)
    original_rmtree = shutil.rmtree

    def fail_backup_cleanup(path, *args, **kwargs):
        if Path(path).name.startswith(".replaced_"):
            raise OSError("injected obsolete-backup cleanup failure")
        return original_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(
        "pipeline.reconstruct_selected_catalog.shutil.rmtree", fail_backup_cleanup
    )
    replaced = reconstruct_selected_catalog(
        request=_request(tmp_path, replay, overwrite=True)
    )
    assert replaced == final
    manifest = json.loads((final / "job_manifest.json").read_text())
    assert manifest["status"] == "complete"
    assert any("obsolete backup cleanup" in warning for warning in manifest["warnings"])
    assert len(list(request.output_root.glob(".replaced_*"))) == 1
    assert not (request.output_root / f".claim_{request.job_id}").exists()
