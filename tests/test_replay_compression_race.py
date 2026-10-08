"""Schema-v2 representation transitions are retried only at partition scope."""
from __future__ import annotations

import json
import gzip
from pathlib import Path

import pytest
import zstandard as zstd

import pipeline.build_replay_store as builder
import pipeline.raw_manifest as raw_manifest
from stores.replay_writer import validate_partition
from tests.test_replay_build_policy import DATE, VENUE, _raw

SYMBOL = "ADAUSDT"


def _compress(path: Path, *, remove_plain: bool = True, corrupt: bool = False) -> Path:
    replacement = Path(f"{path}.zst")
    replacement.write_bytes(
        b"corrupt-zstd" if corrupt else zstd.ZstdCompressor().compress(path.read_bytes())
    )
    if remove_plain:
        path.unlink()
    return replacement


def _build(raw: Path, replay: Path) -> dict:
    return builder.build_replay_for_symbol(
        VENUE, SYMBOL, DATE, raw, replay, schema_version=2,
        price_scale=2, qty_scale=1,
    )


def _partition(replay: Path) -> Path:
    return replay / f"venue={VENUE}" / f"symbol={SYMBOL}" / f"date={DATE}"


def _assert_clean(replay: Path) -> None:
    symbol_dir = _partition(replay).parent
    assert not list(symbol_dir.glob(".staging_*"))
    assert not list(symbol_dir.glob(".backup_*"))


@pytest.mark.parametrize("phase", ["pre_identity", "stream", "post_identity"])
def test_exact_compression_transition_restarts_whole_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    monkeypatch.setattr(builder.time, "sleep", lambda _seconds: None)
    attempts: list[int] = []
    real_attempt = builder._build_replay_for_symbol_locked

    def count_attempts(*args, **kwargs):
        attempts.append(1)
        return real_attempt(*args, **kwargs)

    monkeypatch.setattr(builder, "_build_replay_for_symbol_locked", count_attempts)

    if phase == "pre_identity":
        real_hash = raw_manifest._sha256_file

        def during_hash(path):
            if path == trade and trade.exists():
                _compress(trade)
            return real_hash(path)

        monkeypatch.setattr(raw_manifest, "_sha256_file", during_hash)
    elif phase == "stream":
        real_stream = builder._stream_raw_records_strict

        def during_stream(venue, symbol, channel, date, data_root, **kwargs):
            if channel == "trade_v2" and trade.exists():
                _compress(trade)
            yield from real_stream(venue, symbol, channel, date, data_root, **kwargs)

        monkeypatch.setattr(builder, "_stream_raw_records_strict", during_stream)
    else:
        real_identity = builder.compute_repartitioned_source_identity
        calls = 0

        def during_post_identity(*args, **kwargs):
            nonlocal calls
            calls += 1
            result = real_identity(*args, **kwargs)
            if calls == 2 and trade.exists():
                _compress(trade)
            return result

        monkeypatch.setattr(builder, "compute_repartitioned_source_identity", during_post_identity)

    result = _build(raw, replay)
    assert result["outcome"] == "built", result
    assert len(attempts) == 2
    partition = _partition(replay)
    assert validate_partition(partition)
    manifest = json.loads((partition / "manifest.json").read_text())
    assert result["depth_count"] == 1
    assert result["trade_count"] == 1
    assert [entry["path"] for entry in manifest["source_identity"]["channels"]["trade_v2"]] == [
        f"{VENUE}/trade_v2/{SYMBOL}/{DATE}/{DATE}T00.jsonl.zst"
    ]
    _assert_clean(replay)


def test_persistent_dual_variants_exhaust_exact_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    _compress(trade, remove_plain=False)
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)
    result = _build(raw, replay)
    assert result["outcome"] == "failed"
    assert "3 attempts" in result["errors"][0]
    assert str(trade) in result["errors"][0]
    assert waits == [0.2, 0.2]
    assert not _partition(replay).exists()
    _assert_clean(replay)


def test_multiple_compressed_alternatives_fail_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    _compress(trade)
    with gzip.open(f"{trade}.gz", "wb") as handle:
        handle.write(b"{}\n")
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)
    result = _build(raw, replay)
    assert result["outcome"] == "failed"
    assert "compression variants" in result["errors"][0]
    assert not waits
    assert not _partition(replay).exists()


@pytest.mark.parametrize("replacement", ["absent", "corrupt", "io_error"])
def test_unproven_disappearance_and_io_fail_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    real_hash = raw_manifest._sha256_file
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)

    def fail_hash(path):
        if path == trade and trade.exists():
            if replacement == "io_error":
                raise OSError("injected I/O failure")
            if replacement == "corrupt":
                _compress(trade, corrupt=True)
            else:
                trade.unlink()
        return real_hash(path)

    monkeypatch.setattr(raw_manifest, "_sha256_file", fail_hash)
    result = _build(raw, replay)
    assert result["outcome"] == "failed"
    assert not waits
    assert not _partition(replay).exists()
    _assert_clean(replay)


def test_same_path_content_mutation_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    real_stream = builder._stream_raw_records_strict
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)

    def mutate_stream(venue, symbol, channel, date, data_root, **kwargs):
        yield from real_stream(venue, symbol, channel, date, data_root, **kwargs)
        if channel == "trade_v2":
            with trade.open("a") as handle:
                handle.write("{}\n")

    monkeypatch.setattr(builder, "_stream_raw_records_strict", mutate_stream)
    result = _build(raw, replay)
    assert result["outcome"] == "failed"
    assert "checksums/sizes differ" in result["errors"][0]
    assert not waits
    assert not _partition(replay).exists()
    _assert_clean(replay)


def test_decoded_change_during_compression_is_not_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    real_stream = builder._stream_raw_records_strict
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)

    def change_then_compress(venue, symbol, channel, date, data_root, **kwargs):
        if channel == "trade_v2" and trade.exists():
            altered = json.loads(trade.read_text())
            altered["price"] = "9.00"
            Path(f"{trade}.zst").write_bytes(
                zstd.ZstdCompressor().compress((json.dumps(altered) + "\n").encode())
            )
            trade.unlink()
        yield from real_stream(venue, symbol, channel, date, data_root, **kwargs)

    monkeypatch.setattr(builder, "_stream_raw_records_strict", change_then_compress)
    result = _build(raw, replay)
    assert result["outcome"] == "failed"
    assert "decoded content differs" in result["errors"][0]
    assert not waits
    assert not _partition(replay).exists()
    _assert_clean(replay)


def test_live_reuse_identity_transition_retries_then_requires_source_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    _depth, trade = _raw(raw, SYMBOL)
    assert _build(raw, replay)["outcome"] == "built"
    real_hash = raw_manifest._sha256_file
    waits: list[float] = []
    monkeypatch.setattr(builder.time, "sleep", waits.append)

    def during_reuse(path):
        if path == trade and trade.exists():
            _compress(trade)
        return real_hash(path)

    monkeypatch.setattr(raw_manifest, "_sha256_file", during_reuse)
    result = _build(raw, replay)
    assert result["outcome"] == "source_changed_rebuild_required"
    assert waits == [0.2]
    assert validate_partition(_partition(replay))
    _assert_clean(replay)


def test_adjacent_day_depth_hour_transition_is_reinventoried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw, replay = tmp_path / "raw", tmp_path / "replay"
    depth, _trade = _raw(raw, SYMBOL)
    next_day = "2026-01-04"
    adjacent = raw / VENUE / "depth_v2" / SYMBOL / next_day / f"{next_day}T14.jsonl"
    adjacent.parent.mkdir(parents=True)
    record = json.loads(depth.read_text())
    record["session_seq"] = 2
    record["U"] = 2
    record["u"] = 2
    adjacent.write_text(json.dumps(record) + "\n")
    real_hash = raw_manifest._sha256_file
    monkeypatch.setattr(builder.time, "sleep", lambda _seconds: None)

    def during_hash(path):
        if path == adjacent and adjacent.exists():
            _compress(adjacent)
        return real_hash(path)

    monkeypatch.setattr(raw_manifest, "_sha256_file", during_hash)
    result = _build(raw, replay)
    assert result["outcome"] == "built", result
    assert result["depth_count"] == 2
    manifest = json.loads((_partition(replay) / "manifest.json").read_text())
    assert any(entry["path"].endswith(f"{next_day}T14.jsonl.zst")
               for entry in manifest["source_identity"]["channels"]["depth_v2"])
    assert validate_partition(_partition(replay))
    _assert_clean(replay)
