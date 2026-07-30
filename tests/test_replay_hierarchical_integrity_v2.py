"""Focused tests for the issue #20 Phase 7 hierarchical-integrity candidate
(schema_version=2): replaces the per-event ``native_payload_hash`` column
(measured at ~54% of the optimized-v1 candidate's total size) with the
manifest-level traceability hierarchy documented as "planned" in
docs/IMPLEMENTATION_AUDIT.md's Phase 2 Section 3:

  1. raw file/chunk identity + SHA-256 checksum per partition, extended with
     a per-file record_count/record_range so a replay event's raw_index can
     be mapped back to its exact source file deterministically;
  2. bounded per-block (Parquet row-group) first/last sort key, row count,
     and SHA-256 checksum of the block's canonical row content;
  3. the existing whole-file depth_checksum/trades_checksum (unchanged);
  4. a documented, deterministic raw_index -> source-file mapping method
     (``stores.replay_writer.resolve_source_record``).

Contract proof performed before writing this candidate (recorded in
docs/CHANGE_AUDIT.md): zero internal consumers anywhere in the codebase read
the per-event native_payload_hash *value* — writer/reader only round-trip it,
and validation/audit_replay_store.py's null-ratio checks explicitly exclude
it (only ``U``/``u``/``pu`` for depth and
``trade_id``/``agg_trade_id``/``price_str``/``quantity_str`` for trades are
tracked there). No validation/*.py comparator reads it either. This file's
tests assume that finding and focus on proving the replacement hierarchy is
sound, not re-litigating the consumer inventory.

These tests prove:
  - the v2 manifest carries a complete, structurally valid integrity
    hierarchy (source_identity with record_count/record_range,
    depth_blocks/trade_blocks with first/last key + row count + sha256,
    whole-file checksums);
  - ``resolve_source_record`` deterministically maps a raw_index back to its
    exact source file and contribution ordinal, across multiple compressed
    source files, both channels, multiple sessions, and file boundaries;
  - manifest source paths are canonical/POSIX-relative to data_root, never
    absolute and never carrying mtime/inode metadata;
  - ``verify_block_integrity`` detects: a changed value, a missing row, an
    extra row, reordered rows, and a damaged/truncated block;
  - two independent builds of the same input produce byte-identical
    manifests (except the observational ``created_at_utc`` field) and
    checksums — the deterministic-rebuild proof;
  - ``native_payload_hash`` is physically absent from the v2 schema AND the
    canonical v2 LOGICAL row genuinely omits the key (never a fabricated
    ``None``) -- no speculative compatibility adapter is provided since the
    consumer inventory found no current consumer that requires one;
  - replay-side deep integrity verification remains fully self-contained
    after ``data_raw`` is removed;
  - a time-of-check/time-of-use raw-file change during a build fails the
    build rather than publishing a manifest for different raw bytes;
  - v0 and v1 partitions remain readable exactly as before (no regression);
  - ``validate_partition`` rejects a v2 manifest missing integrity metadata,
    and ``ReplayWriter`` refuses to build schema_version=2 without an
    explicit ``source_identity``.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.build_replay_store import build_replay_for_symbol
from pipeline.raw_manifest import compute_raw_source_identity
from stores.replay_reader import ReplayReader
from stores.replay_schema import DEPTH_REPLAY_SCHEMA_V2, TRADE_REPLAY_SCHEMA_V2
from stores.replay_writer import (
    BLOCK_DIGEST_METHOD_V1,
    BLOCK_DIGEST_METHOD_V2,
    ReplayWriter,
    _canon_array,
    _canon_table_hash_v1,
    audit_partition_deep,
    resolve_source_record,
    validate_partition,
    verify_block_integrity,
)
from validation.audit_replay_store import audit_replay_store

from tests.test_replay_schema_v1_corrections import _sample_raw_root, _write_jsonl


def _manifest_path(replay_root: Path, venue: str, symbol: str, date: str) -> Path:
    return replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}" / "manifest.json"


def _partition_dir(replay_root: Path, venue: str, symbol: str, date: str) -> Path:
    return replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"


# ---------------------------------------------------------------------------
# 1. Manifest carries a complete, structurally valid integrity hierarchy
# ---------------------------------------------------------------------------


def test_v2_manifest_carries_complete_integrity_hierarchy(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "success", result

    manifest = json.loads(_manifest_path(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12").read_text())
    assert manifest["schema_version"] == 2
    integrity = manifest["integrity"]
    assert integrity["digest_method"] == BLOCK_DIGEST_METHOD_V2
    assert integrity["hierarchy_version"] == 1
    assert integrity["depth_checksum"] == manifest["depth_checksum"]
    assert integrity["trades_checksum"] == manifest["trades_checksum"]

    # Bounded per-block metadata: at least one block per non-empty channel,
    # each with the required structural fields.
    assert len(integrity["depth_blocks"]) >= 1
    assert len(integrity["trade_blocks"]) >= 1
    for block in integrity["depth_blocks"] + integrity["trade_blocks"]:
        assert set(block.keys()) == {"block_index", "num_rows", "first_key", "last_key", "sha256"}
        assert isinstance(block["sha256"], str) and len(block["sha256"]) == 64

    # Raw source identity, extended with record_count/record_range.
    source_identity = integrity["source_identity"]
    assert source_identity["complete"] is True
    for channel in ("depth_v2", "trade_v2"):
        entries = source_identity["channels"][channel]
        assert len(entries) >= 1
        for entry in entries:
            assert "record_count" in entry
            assert "record_range" in entry
            assert entry["record_range"][1] - entry["record_range"][0] == entry["record_count"]


def test_canonical_v2_distinguishes_null_list_from_empty_list() -> None:
    level = pa.struct(
        [
            pa.field("price_mantissa", pa.int64(), nullable=False),
            pa.field("size_mantissa", pa.int64(), nullable=False),
        ]
    )
    list_type = pa.list_(level)

    assert _canon_array(pa.array([None], type=list_type)) != _canon_array(
        pa.array([[]], type=list_type)
    )


def test_canonical_v2_distinguishes_null_struct_from_valid_struct() -> None:
    struct_type = pa.struct([pa.field("value", pa.int64(), nullable=True)])

    assert _canon_array(pa.array([None], type=struct_type)) != _canon_array(
        pa.array([{"value": None}], type=struct_type)
    )


def test_canonical_v2_length_frames_strings_while_v1_remains_auditable() -> None:
    left = pa.table({"value": pa.array(["a\x00b", "c"])})
    right = pa.table({"value": pa.array(["a", "b\x00c"])})

    # The legacy collision is recorded rather than silently reinterpreted.
    assert _canon_table_hash_v1(left) == _canon_table_hash_v1(right)
    assert _canon_array(left.column("value")) != _canon_array(right.column("value"))
    assert BLOCK_DIGEST_METHOD_V1 != BLOCK_DIGEST_METHOD_V2


def test_v2_physical_schema_omits_native_payload_hash_column(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    depth_schema = pq.ParquetFile(pdir / "depth.parquet").schema_arrow
    trade_schema = pq.ParquetFile(pdir / "trades.parquet").schema_arrow
    assert "native_payload_hash" not in depth_schema.names
    assert "native_payload_hash" not in trade_schema.names
    assert set(depth_schema.names) == set(DEPTH_REPLAY_SCHEMA_V2.names)
    assert set(trade_schema.names) == set(TRADE_REPLAY_SCHEMA_V2.names)


def test_replay_audit_reports_v2_fixed_point_fields_as_exact(tmp_path):
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
    assert result["status"] == "success", result

    report = audit_replay_store(
        replay_root=replay_root,
        date="2026-06-12",
        symbols=["ADAUSDT"],
        venues=["BINANCE_SPOT"],
    )

    partition = report["partitions"][0]
    assert partition["schema_version"] == 2
    assert partition["depth"]["level_exact_fields_present"] is True
    assert partition["depth"]["level_exact_encoding"] == "fixed_point_mantissa"
    assert partition["trades"]["null_ratio"]["price_mantissa"] == 0
    assert partition["trades"]["null_ratio"]["quantity_mantissa"] == 0
    assert "price_str" not in partition["trades"]["null_ratio"]
    assert "quantity_str" not in partition["trades"]["null_ratio"]


def test_v2_logical_row_native_payload_hash_key_is_genuinely_omitted_not_fake_none(tmp_path):
    """issue #20 Phase 7 review point 1: the canonical v2 logical row must
    NOT expose native_payload_hash=None to imitate the v1 row shape -- that
    would be a fake compatibility value indistinguishable from "a hash was
    computed and is legitimately None". The key must be genuinely absent."""
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    trades = list(reader.iter_trades("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    assert len(depths) > 0 and len(trades) > 0
    for row in depths + trades:
        assert "native_payload_hash" not in row


# ---------------------------------------------------------------------------
# 2. Deterministic raw_index -> source-file mapping
# ---------------------------------------------------------------------------


def test_resolve_source_record_maps_files_deterministically(tmp_path):
    root = tmp_path / "raw"
    venue, symbol, date = "BINANCE_SPOT", "MULTIFILEUSDT", "2026-06-12"
    # Two raw files, sorted-filename order matters (matches
    # stream_raw_records's sorted glob) -- 3 records in file "00", 2 in "01".
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [{"record_type": "depth_update", "n": i} for i in range(3)],
    )
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T01.jsonl",
        [{"record_type": "depth_update", "n": i} for i in range(2)],
    )
    identity = compute_raw_source_identity(
        venue, symbol, date, ["depth_v2"], data_root=root, include_record_counts=True
    )
    entries = identity["channels"]["depth_v2"]
    assert len(entries) == 2
    assert entries[0]["path"].endswith("T00.jsonl")
    assert entries[0]["record_count"] == 3
    assert entries[0]["record_range"] == [0, 3]
    assert entries[1]["record_count"] == 2
    assert entries[1]["record_range"] == [3, 5]

    # raw_index 0, 1, 2 -> file 00; raw_index 3, 4 -> file 01.
    for raw_index in (0, 1, 2):
        assert resolve_source_record(identity, "depth_v2", raw_index)["path"] == entries[0]["path"]
    for raw_index in (3, 4):
        assert resolve_source_record(identity, "depth_v2", raw_index)["path"] == entries[1]["path"]
    # Out of range -> None, never a wrong guess.
    assert resolve_source_record(identity, "depth_v2", 5) is None
    assert resolve_source_record(identity, "depth_v2", -1) is None


def test_resolve_source_record_exact_mapping_multi_file_multi_channel_compressed(tmp_path):
    """issue #20 Phase 7 review point 2: prove EXACT source-record mapping
    (not merely raw_index->file): venue/channel/symbol/date, the canonical
    relative source-file path, and the exact contribution ordinal. Covers
    multiple compressed source files (.gz and .zst), both depth and trades,
    multiple "sessions" (represented here as separate files, matching how
    the recorder actually rotates raw files), and exact file-boundary
    (first/last record of each file) resolution."""
    import gzip
    import zstandard as zstd

    root = tmp_path / "raw"
    venue, symbol, date = "BINANCE_SPOT", "BOUNDARYUSDT", "2026-06-12"

    def _write_compressed_jsonl(path: Path, records: list, *, fmt: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = "\n".join(json.dumps(r) for r in records) + "\n"
        if fmt == "gz":
            with gzip.open(path, "wt") as f:
                f.write(payload)
        elif fmt == "zst":
            cctx = zstd.ZstdCompressor()
            with open(path, "wb") as f:
                with cctx.stream_writer(f) as compressor:
                    compressor.write(payload.encode("utf-8"))
        else:
            path.write_text(payload)

    # depth_v2: 3 files (2 "sessions" worth) -- .jsonl, .gz, .zst -- with
    # 4, 3, and 2 records respectively (non-uniform sizes to make boundary
    # arithmetic unambiguous).
    _write_compressed_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [{"record_type": "depth_update", "n": i} for i in range(4)], fmt="plain",
    )
    _write_compressed_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T01.jsonl.gz",
        [{"record_type": "depth_update", "n": i} for i in range(3)], fmt="gz",
    )
    _write_compressed_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T02.jsonl.zst",
        [{"record_type": "depth_update", "n": i} for i in range(2)], fmt="zst",
    )
    # trade_v2: 2 files -- .zst and .gz -- with 5 and 1 records.
    _write_compressed_jsonl(
        root / venue / "trade_v2" / symbol / date / f"{date}T00.jsonl.zst",
        [{"record_type": "trade", "n": i} for i in range(5)], fmt="zst",
    )
    _write_compressed_jsonl(
        root / venue / "trade_v2" / symbol / date / f"{date}T01.jsonl.gz",
        [{"record_type": "trade", "n": i} for i in range(1)], fmt="gz",
    )

    identity = compute_raw_source_identity(
        venue, symbol, date, ["depth_v2", "trade_v2"], data_root=root, include_record_counts=True
    )
    assert identity["venue"] == venue
    assert identity["symbol"] == symbol
    assert identity["date"] == date

    depth_entries = identity["channels"]["depth_v2"]
    assert [e["record_count"] for e in depth_entries] == [4, 3, 2]
    assert [e["record_range"] for e in depth_entries] == [[0, 4], [4, 7], [7, 9]]

    trade_entries = identity["channels"]["trade_v2"]
    assert [e["record_count"] for e in trade_entries] == [5, 1]
    assert [e["record_range"] for e in trade_entries] == [[0, 5], [5, 6]]

    # Depth: exercise every file boundary (first and last raw_index of each
    # file), for both plain and compressed files.
    expectations = [
        (0, depth_entries[0]["path"], 0),   # first record, file 0
        (3, depth_entries[0]["path"], 3),   # last record, file 0
        (4, depth_entries[1]["path"], 0),   # first record, file 1 (.gz)
        (6, depth_entries[1]["path"], 2),   # last record, file 1 (.gz)
        (7, depth_entries[2]["path"], 0),   # first record, file 2 (.zst)
        (8, depth_entries[2]["path"], 1),   # last record, file 2 (.zst)
    ]
    for raw_index, expected_path, expected_ordinal in expectations:
        record = resolve_source_record(identity, "depth_v2", raw_index)
        assert record is not None, raw_index
        assert record["venue"] == venue
        assert record["symbol"] == symbol
        assert record["date"] == date
        assert record["channel"] == "depth_v2"
        assert record["path"] == expected_path
        assert record["contribution_ordinal"] == expected_ordinal
    # Trades: exercise the .zst -> .gz boundary specifically.
    assert resolve_source_record(identity, "trade_v2", 4) == {
        "venue": venue, "symbol": symbol, "date": date, "channel": "trade_v2",
        "path": trade_entries[0]["path"], "contribution_ordinal": 4,
    }
    assert resolve_source_record(identity, "trade_v2", 5) == {
        "venue": venue, "symbol": symbol, "date": date, "channel": "trade_v2",
        "path": trade_entries[1]["path"], "contribution_ordinal": 0,
    }

    # Out of range / negative -> None, never a wrong guess.
    assert resolve_source_record(identity, "depth_v2", 9) is None
    assert resolve_source_record(identity, "depth_v2", -1) is None
    assert resolve_source_record(identity, "trade_v2", 6) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("venue", None),
        ("venue", ""),
        ("symbol", None),
        ("symbol", " "),
        ("date", None),
        ("date", "2026/06/12"),
    ],
)
def test_resolve_source_record_rejects_missing_or_malformed_partition_identity(
    field, value
):
    identity = {
        "venue": "BINANCE_SPOT",
        "symbol": "ADAUSDT",
        "date": "2026-06-12",
        "channels": {"depth_v2": []},
    }
    identity[field] = value

    with pytest.raises(ValueError, match=field):
        resolve_source_record(identity, "depth_v2", 0)


def test_source_identity_paths_are_canonical_relative_posix_no_machine_metadata(tmp_path):
    """issue #20 Phase 7 review point 3: manifest source paths must be
    canonical, POSIX-relative to data_root -- never absolute, and never
    carrying mtime/inode/owner or other machine-specific metadata, so
    independent machines / Syncthing-synced copies of the same raw bytes
    produce/consume identical source_identity dicts regardless of local
    absolute path or OS path separator conventions."""
    root = tmp_path / "raw"
    venue, symbol, date = "BINANCE_SPOT", "PATHUSDT", "2026-06-12"
    _write_jsonl(
        root / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [{"record_type": "depth_update", "n": 0}],
    )
    identity = compute_raw_source_identity(venue, symbol, date, ["depth_v2"], data_root=root)
    entry = identity["channels"]["depth_v2"][0]
    assert entry["path"] == f"{venue}/depth_v2/{symbol}/{date}/{date}T00.jsonl"
    assert not entry["path"].startswith("/")
    assert str(root) not in entry["path"]
    assert set(entry.keys()) == {"path", "sha256", "size_bytes"}  # no mtime/inode/owner keys



# ---------------------------------------------------------------------------
# 3. Block-integrity verification: proves detection of every required
#    corruption/tampering case by directly manipulating a real v2 parquet
#    file and re-running verify_block_integrity against the original,
#    untouched manifest block metadata.
# ---------------------------------------------------------------------------


def _build_v2_partition(tmp_path: Path, *, n_depth_batches: int = 1):
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "success", result
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    manifest = json.loads((pdir / "manifest.json").read_text())
    return pdir, manifest


def test_verify_block_integrity_passes_for_untouched_file(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    problems = verify_block_integrity(pdir / "depth.parquet", depth_blocks)
    assert problems == []


def test_legacy_canonical_v1_manifest_blocks_remain_verifiable(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    blocks = json.loads(json.dumps(manifest["integrity"]["depth_blocks"]))
    parquet = pq.ParquetFile(pdir / "depth.parquet")
    try:
        nonempty = [
            index
            for index in range(parquet.metadata.num_row_groups)
            if parquet.metadata.row_group(index).num_rows > 0
        ]
        for block, row_group_index in zip(blocks, nonempty):
            block["sha256"] = _canon_table_hash_v1(
                parquet.read_row_group(row_group_index)
            )
    finally:
        parquet.close()

    assert (
        verify_block_integrity(
            pdir / "depth.parquet",
            blocks,
            digest_method=BLOCK_DIGEST_METHOD_V1,
        )
        == []
    )


@pytest.mark.parametrize(
    ("file_name", "blocks_key", "metadata_key", "problem_text"),
    [
        ("depth.parquet", "depth_blocks", "block_index", "block_index mismatch"),
        ("depth.parquet", "depth_blocks", "first_key", "first_key mismatch"),
        ("trades.parquet", "trade_blocks", "last_key", "last_key mismatch"),
    ],
)
def test_verify_block_integrity_rejects_forged_block_locator_metadata(
    tmp_path, file_name, blocks_key, metadata_key, problem_text
):
    pdir, manifest = _build_v2_partition(tmp_path)
    blocks = json.loads(json.dumps(manifest["integrity"][blocks_key]))
    assert blocks
    if metadata_key == "block_index":
        blocks[0][metadata_key] += 1
    else:
        blocks[0][metadata_key][1] += 1

    problems = verify_block_integrity(pdir / file_name, blocks)

    assert any(problem_text in problem for problem in problems)


def test_verify_block_integrity_detects_changed_value(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    depth_path = pdir / "depth.parquet"

    tbl = pq.read_table(depth_path)
    rows = tbl.to_pylist()
    assert len(rows) >= 1
    # Change one value in the first row (a fixed-point mantissa) -- a
    # semantically real "changed replay value" corruption.
    rows[0] = dict(rows[0])
    rows[0]["ts_exchange_ns"] = rows[0]["ts_exchange_ns"] + 1

    import pyarrow as pa
    tampered_tbl = pa.Table.from_pylist(rows, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=len(rows))

    problems = verify_block_integrity(depth_path, depth_blocks)
    assert any("checksum mismatch" in p for p in problems)


def test_verify_block_integrity_detects_missing_row(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    depth_path = pdir / "depth.parquet"

    tbl = pq.read_table(depth_path)
    rows = tbl.to_pylist()
    assert len(rows) >= 2
    rows_missing = rows[:-1]  # drop the last row

    import pyarrow as pa
    tampered_tbl = pa.Table.from_pylist(rows_missing, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=max(1, len(rows_missing)))

    problems = verify_block_integrity(depth_path, depth_blocks)
    assert any("row count mismatch" in p for p in problems)


def test_verify_block_integrity_detects_extra_row(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    depth_path = pdir / "depth.parquet"

    tbl = pq.read_table(depth_path)
    rows = tbl.to_pylist()
    rows_extra = rows + [dict(rows[-1])]  # duplicate the last row (extra row)

    import pyarrow as pa
    tampered_tbl = pa.Table.from_pylist(rows_extra, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=len(rows_extra))

    problems = verify_block_integrity(depth_path, depth_blocks)
    assert any("row count mismatch" in p for p in problems)


def test_verify_block_integrity_detects_reordered_rows(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    depth_path = pdir / "depth.parquet"

    tbl = pq.read_table(depth_path)
    rows = tbl.to_pylist()
    assert len(rows) >= 2
    reordered = list(reversed(rows))
    assert reordered != rows  # sanity: an actual reordering occurred

    import pyarrow as pa
    tampered_tbl = pa.Table.from_pylist(reordered, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=len(reordered))

    problems = verify_block_integrity(depth_path, depth_blocks)
    assert any("checksum mismatch" in p for p in problems)


def test_verify_block_integrity_detects_damaged_block(tmp_path):
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    depth_path = pdir / "depth.parquet"

    # Truncate the file to simulate a damaged/corrupted completed Parquet
    # file -- must surface as a problem, not silently pass.
    original = depth_path.read_bytes()
    depth_path.write_bytes(original[: len(original) // 2])

    problems = verify_block_integrity(depth_path, depth_blocks)
    assert len(problems) >= 1


def test_verify_block_integrity_detects_wrong_manifest_source_identity_via_mismatched_blocks(tmp_path):
    """A manifest recording the WRONG (e.g. swapped from a different
    partition) block metadata must be detected as a mismatch against the
    real file's actual blocks -- proving "incorrect source identity"
    (a manifest that doesn't match its own file) is caught."""
    pdir_a, manifest_a = _build_v2_partition(tmp_path / "a")
    pdir_b, manifest_b = _build_v2_partition(tmp_path / "b")
    # Use partition A's real depth.parquet file but partition B's recorded
    # block metadata (simulating a manifest that was copied from/associated
    # with the wrong file).
    problems = verify_block_integrity(pdir_a / "depth.parquet", manifest_b["integrity"]["depth_blocks"])
    # Since both partitions build identical ADAUSDT fixture data, blocks may
    # coincidentally match; to make the mismatch unambiguous, tamper A's file
    # first so it can never match either manifest by chance.
    tbl = pq.read_table(pdir_a / "depth.parquet")
    rows = tbl.to_pylist()
    rows[0] = dict(rows[0])
    rows[0]["raw_index"] = rows[0]["raw_index"] + 1000
    import pyarrow as pa
    pq.write_table(pa.Table.from_pylist(rows, schema=tbl.schema), pdir_a / "depth.parquet", row_group_size=len(rows))
    problems = verify_block_integrity(pdir_a / "depth.parquet", manifest_b["integrity"]["depth_blocks"])
    assert len(problems) >= 1


# ---------------------------------------------------------------------------
# 4. Deterministic rebuild: two independent builds of the same input produce
#    byte-identical manifests (except created_at_utc) and checksums.
# ---------------------------------------------------------------------------


def test_v2_deterministic_rebuild_identical_manifests_and_checksums(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root_a = tmp_path / "replay_a"
    replay_root_b = tmp_path / "replay_b"

    result_a = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root_a, schema_version=2
    )
    result_b = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root_b, schema_version=2
    )
    assert result_a["status"] == "success" and result_b["status"] == "success"

    manifest_a = json.loads(_manifest_path(replay_root_a, "BINANCE_SPOT", "ADAUSDT", "2026-06-12").read_text())
    manifest_b = json.loads(_manifest_path(replay_root_b, "BINANCE_SPOT", "ADAUSDT", "2026-06-12").read_text())

    # Exclude only the explicitly-observational timestamp field.
    manifest_a.pop("created_at_utc", None)
    manifest_b.pop("created_at_utc", None)
    assert manifest_a == manifest_b

    # Whole-file checksums must also be byte-identical between independent
    # builds (proves the Parquet output itself is deterministic, not just
    # the manifest).
    assert manifest_a["depth_checksum"] == manifest_b["depth_checksum"]
    assert manifest_a["trades_checksum"] == manifest_b["trades_checksum"]


# ---------------------------------------------------------------------------
# 5. Fail-closed contracts: no silent fallback
# ---------------------------------------------------------------------------


def test_v2_requires_explicit_source_identity(tmp_path):
    """ReplayWriter must refuse to build schema_version=2 without an
    explicit source_identity -- never a silent placeholder like v1's
    "not supplied by caller" fallback (v2's whole traceability hierarchy
    depends on a real source_identity being present)."""
    replay_root = tmp_path / "replay"
    writer = ReplayWriter(
        replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12",
        schema_version=2, price_scale=4, qty_scale=1,
    )
    writer.write_depth_batch([{
        "stream_session_id": 1, "session_seq": 1, "raw_index": 0,
        "record_type": "depth_update", "U": None, "u": None, "pu": None,
        "ts_exchange_ns": 1, "ts_receive_ns": 1,
        "bids": [], "asks": [],
        "is_snapshot_seed": False, "is_depth_update": True, "is_sync_state": False,
        "is_desync": False, "is_resync": False,
        "quality_flags": None, "native_payload_hash": None,
    }])
    writer.write_trades_batch([])
    with pytest.raises(ValueError, match="source_identity is required"):
        writer.finalize_staging()


def test_validate_partition_rejects_v2_missing_integrity_metadata(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    manifest_path = pdir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    del manifest["integrity"]
    manifest_path.write_text(json.dumps(manifest))
    assert validate_partition(pdir) is False


def test_validate_partition_rejects_unpinned_or_inconsistent_v2_integrity_metadata(
    tmp_path,
):
    pdir, manifest = _build_v2_partition(tmp_path)
    manifest_path = pdir / "manifest.json"

    def _mutated_manifest():
        return json.loads(json.dumps(manifest))

    mutations = []

    wrong_builder = _mutated_manifest()
    wrong_builder["builder_version"] = "cryptorecorder-replay-writer-v2.future"
    mutations.append(wrong_builder)

    wrong_hierarchy = _mutated_manifest()
    wrong_hierarchy["integrity"]["hierarchy_version"] = 2
    mutations.append(wrong_hierarchy)

    wrong_digest = _mutated_manifest()
    wrong_digest["integrity"]["digest_method"] = "unknown_digest"
    mutations.append(wrong_digest)

    wrong_checksum_copy = _mutated_manifest()
    wrong_checksum_copy["integrity"]["depth_checksum"] = "0" * 64
    mutations.append(wrong_checksum_copy)

    wrong_source_identity = _mutated_manifest()
    wrong_source_identity["integrity"]["source_identity"]["symbol"] = "ETHUSDT"
    mutations.append(wrong_source_identity)

    wrong_block_index = _mutated_manifest()
    wrong_block_index["integrity"]["depth_blocks"][0]["block_index"] = 1
    mutations.append(wrong_block_index)

    wrong_block_rows = _mutated_manifest()
    wrong_block_rows["integrity"]["trade_blocks"][0]["num_rows"] += 1
    mutations.append(wrong_block_rows)

    malformed_block_digest = _mutated_manifest()
    malformed_block_digest["integrity"]["depth_blocks"][0]["sha256"] = "not-a-digest"
    mutations.append(malformed_block_digest)

    for mutated in mutations:
        manifest_path.write_text(json.dumps(mutated))
        assert validate_partition(pdir) is False


def test_schema_match_rejects_field_reordering_and_nullability_changes(tmp_path):
    from stores.replay_writer import _schema_matches

    expected = pa.schema([
        pa.field("first", pa.int64(), nullable=False),
        pa.field("second", pa.string(), nullable=True),
    ])
    expected_table = pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), pa.array(["value"], type=pa.string())],
        schema=expected,
    )

    reordered_path = tmp_path / "reordered.parquet"
    reordered = expected_table.select(["second", "first"])
    pq.write_table(reordered, reordered_path)
    assert _schema_matches(reordered_path, expected) is False

    nullable_path = tmp_path / "nullable.parquet"
    nullable_schema = pa.schema([
        pa.field("first", pa.int64(), nullable=True),
        pa.field("second", pa.string(), nullable=True),
    ])
    nullable_table = pa.Table.from_arrays(
        [
            expected_table.column("first"),
            expected_table.column("second"),
        ],
        schema=nullable_schema,
    )
    pq.write_table(nullable_table, nullable_path)
    assert _schema_matches(nullable_path, expected) is False


def test_both_modes_reject_missing_integrity_metadata(tmp_path):
    """Both the routine and deep tiers must fail closed (never silently
    pass) when a v2 manifest is missing its integrity metadata entirely."""
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    manifest_path = pdir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    del manifest["integrity"]
    manifest_path.write_text(json.dumps(manifest))

    assert validate_partition(pdir) is False
    problems = audit_partition_deep(pdir)
    assert len(problems) >= 1


def test_both_modes_reject_malformed_integrity_metadata(tmp_path):
    """Both tiers must fail closed on a structurally malformed (wrong-type)
    integrity dict, not merely a missing one."""
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    manifest_path = pdir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["integrity"]["depth_blocks"] = "not-a-list"  # malformed type
    manifest_path.write_text(json.dumps(manifest))

    assert validate_partition(pdir) is False
    problems = audit_partition_deep(pdir)
    assert len(problems) >= 1


def test_routine_validation_detects_ordinary_file_corruption_via_complete_checksum(tmp_path):
    """Routine validate_partition() must still catch ordinary Parquet
    corruption/truncation/deletion/insertion/reordering through the
    unchanged complete-file SHA-256 mechanism -- this is routine
    validation's primary, cheap defense (see its docstring's security/
    integrity rationale)."""
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_path = pdir / "depth.parquet"
    assert validate_partition(pdir) is True

    # Truncate the file WITHOUT updating the manifest's recorded checksum
    # -- simulates ordinary corruption (bad disk sector, partial write,
    # accidental truncation) that a routine caller must still catch cheaply.
    original = depth_path.read_bytes()
    depth_path.write_bytes(original[: len(original) // 2])

    assert validate_partition(pdir) is False


def test_publish_and_skip_valid_call_routine_not_deep(tmp_path, monkeypatch):
    """Prove that the actual production call chain -- ReplayWriter.publish()
    (post-publish check) and pipeline.build_replay_store's skip-if-valid
    check -- calls the ROUTINE validate_partition(), never the deep tier,
    by monkeypatching audit_partition_deep to raise if invoked and
    confirming both a build and a repeat (skip-valid) build still succeed."""
    import stores.replay_writer as replay_writer_module

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("audit_partition_deep must not be called by the routine build/skip-valid path")

    monkeypatch.setattr(replay_writer_module, "audit_partition_deep", _fail_if_called)

    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result1 = build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    assert result1["status"] == "success"

    # Second call over the same (now valid) partition exercises the
    # skip-if-valid path (pipeline.build_replay_store's recover_partition_state
    # -> validate_partition), which must also avoid the deep tier.
    result2 = build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    assert result2["status"] == "skipped"


def test_self_contained_reconstruction_requires_neither_raw_nor_deep_audit(tmp_path):
    """Reconstruction via ReplayReader must work with data_raw removed AND
    without ever invoking the deep audit tier -- routine validation (or no
    validation at all) is sufficient for read access."""
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    shutil.rmtree(raw_root)

    reader = ReplayReader(replay_root)
    depths = list(reader.iter_depths("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    trades = list(reader.iter_trades("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    assert len(depths) > 0 and len(trades) > 0

    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    assert validate_partition(pdir) is True  # routine only, no deep audit call


def test_validate_partition_accepts_valid_v2(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2)
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    assert validate_partition(pdir) is True


def test_routine_validate_partition_does_not_catch_stale_block_checksum(tmp_path):
    """issue #20 Phase 7 review (routine/deep split): ROUTINE
    validate_partition() intentionally does NOT re-verify block-level
    checksums (that would require per-row-group Arrow reads at a cost this
    tier is designed to avoid) -- it only re-verifies the COMPLETE-FILE
    SHA-256. Proves that when a value is changed, valid Parquet is
    rewritten, and the complete-file checksum is recomputed and updated to
    match, ROUTINE validate_partition() reports the partition valid (True)
    -- this exact scenario is what DEEP audit_partition_deep() exists to
    catch instead (see the paired test below).
    """
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_path = pdir / "depth.parquet"

    assert validate_partition(pdir) is True

    # 1. Change one logical replay value, preserving the EXACT physical
    #    schema (read via ParquetFile.read_row_group, matching the real
    #    production read path -- NOT pq.read_table, which infers spurious
    #    hive-partition columns from the venue=.../symbol=.../date=...
    #    directory structure and would corrupt the schema comparison).
    pf = pq.ParquetFile(depth_path)
    tbl = pf.read_row_group(0, use_threads=False)
    for i in range(1, pf.metadata.num_row_groups):
        tbl = pa.concat_tables([tbl, pf.read_row_group(i, use_threads=False)])
    rows = tbl.to_pylist()
    assert len(rows) >= 1
    rows[0] = dict(rows[0])
    rows[0]["ts_exchange_ns"] = rows[0]["ts_exchange_ns"] + 1

    tampered_tbl = pa.Table.from_pylist(rows, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=len(rows))

    # 2. Recompute and update the COMPLETE-FILE checksum in the manifest.
    from stores.replay_writer import _compute_sha256
    manifest_path = pdir / "manifest.json"
    current_manifest = json.loads(manifest_path.read_text())
    new_depth_checksum = _compute_sha256(depth_path)
    assert new_depth_checksum != current_manifest["depth_checksum"]
    current_manifest["depth_checksum"] = new_depth_checksum
    current_manifest["integrity"]["depth_checksum"] = new_depth_checksum
    manifest_path.write_text(json.dumps(current_manifest))

    # 3. ROUTINE validate_partition() does not catch this (by design).
    assert validate_partition(pdir) is True


def test_deep_audit_catches_changed_value_via_stale_block_checksum(tmp_path):
    """issue #20 Phase 7 review point 2 (mandatory proof, deep tier): the
    DEEP audit (``audit_partition_deep``)
    must catch a changed replay value via block-checksum re-verification
    even when the complete-file checksum in the manifest was recomputed to
    match the tampered file -- exactly the scenario ROUTINE validation
    (previous test) cannot catch by design.
    """
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_path = pdir / "depth.parquet"

    assert audit_partition_deep(pdir) == []

    pf = pq.ParquetFile(depth_path)
    tbl = pf.read_row_group(0, use_threads=False)
    for i in range(1, pf.metadata.num_row_groups):
        tbl = pa.concat_tables([tbl, pf.read_row_group(i, use_threads=False)])
    rows = tbl.to_pylist()
    assert len(rows) >= 1
    rows[0] = dict(rows[0])
    rows[0]["ts_exchange_ns"] = rows[0]["ts_exchange_ns"] + 1

    tampered_tbl = pa.Table.from_pylist(rows, schema=tbl.schema)
    pq.write_table(tampered_tbl, depth_path, row_group_size=len(rows))

    from stores.replay_writer import _compute_sha256
    manifest_path = pdir / "manifest.json"
    current_manifest = json.loads(manifest_path.read_text())
    new_depth_checksum = _compute_sha256(depth_path)
    assert new_depth_checksum != current_manifest["depth_checksum"]
    current_manifest["depth_checksum"] = new_depth_checksum
    current_manifest["integrity"]["depth_checksum"] = new_depth_checksum
    # depth_blocks left stale on purpose.
    manifest_path.write_text(json.dumps(current_manifest))

    # ROUTINE mode misses this (whole-file checksum was recomputed)...
    assert validate_partition(pdir) is True
    # ...but DEEP mode catches it via the stale block checksum.
    problems = audit_partition_deep(pdir)
    assert len(problems) >= 1
    assert any("checksum mismatch" in p for p in problems)


# ---------------------------------------------------------------------------
# 7. Fully self-contained validation after data_raw is no longer present
# ---------------------------------------------------------------------------


def test_replay_integrity_verification_self_contained_after_raw_removed(tmp_path):
    """issue #20 Phase 7 review point 4: build v2 from raw, validate it,
    then remove the raw root entirely and prove replay validation/
    reconstruction-support still passes using ONLY the manifest/block/
    complete-file checksums stored inside replay_store -- data_raw
    availability must never be a hard dependency for replay-side integrity
    verification."""
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"
    result = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "success"
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")

    # Validate while raw is still present (baseline).
    assert validate_partition(pdir) is True
    assert audit_partition_deep(pdir) == []

    # Reconstruct depth/trade rows while raw is present (baseline for
    # comparison after raw is removed).
    reader = ReplayReader(replay_root)
    depths_before = list(reader.iter_depths("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    trades_before = list(reader.iter_trades("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))

    # Remove the raw root entirely -- data_raw is no longer locally present.
    shutil.rmtree(raw_root)
    assert not raw_root.exists()

    # Replay-side integrity verification must still fully pass -- it never
    # touches data_raw.
    assert validate_partition(pdir) is True
    assert audit_partition_deep(pdir) == []

    # Reconstruction from replay alone must produce identical results.
    reader_after = ReplayReader(replay_root)
    depths_after = list(reader_after.iter_depths("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    trades_after = list(reader_after.iter_trades("BINANCE_SPOT", "ADAUSDT", "2026-06-12"))
    assert depths_after == depths_before
    assert trades_after == trades_before

    # Source verification (cross-checking source_identity against live raw
    # bytes) is explicitly a SEPARATE, raw-dependent operation that must be
    # reported as unavailable (never falsely "passed") when raw is absent
    # -- compute_raw_source_identity reports complete=False/missing_channels
    # rather than raising or fabricating a match.
    unavailable_identity = compute_raw_source_identity(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", ["depth_v2", "trade_v2"],
        data_root=raw_root, include_record_counts=True,
    )
    assert unavailable_identity["complete"] is False
    assert set(unavailable_identity["missing_channels"]) == {"depth_v2", "trade_v2"}


# ---------------------------------------------------------------------------
# 8. Checksum framing behavior: order-sensitive and deterministic within the
#    replay schema domain.
# ---------------------------------------------------------------------------


def test_block_checksum_distinguishes_adjacent_string_splits_without_nul(tmp_path):
    """The v1 digest's NUL separator distinguishes ordinary adjacent string
    splits in replay data. This deliberately does not claim a general
    collision-proof serialization for arbitrary strings containing NUL; the
    persisted digest method cannot be changed without a new version."""
    import pyarrow as pa
    from stores.replay_writer import _canon_table_hash

    # "ab" + "c" vs "a" + "bc" collide under naive concatenation but not
    # under the persisted v1 encoding's NUL separator.
    schema = pa.schema([pa.field("x", pa.string())])
    tbl_a = pa.Table.from_pylist([{"x": "ab"}, {"x": "c"}], schema=schema)
    tbl_b = pa.Table.from_pylist([{"x": "a"}, {"x": "bc"}], schema=schema)

    assert _canon_table_hash(tbl_a) != _canon_table_hash(tbl_b)


def test_block_checksum_is_order_sensitive_and_channel_scoped(tmp_path):
    """Block checksums cover: row count (via num_rows), ordering keys
    (first_key/last_key), and the full canonical row content (including
    nested bids/asks) in order -- proven end-to-end via a real v2 build."""
    pdir, manifest = _build_v2_partition(tmp_path)
    depth_blocks = manifest["integrity"]["depth_blocks"]
    for block in depth_blocks:
        assert block["first_key"][1] <= block["last_key"][1]  # raw_index non-decreasing within a block
    # depth and trade blocks must never be cross-comparable/interchangeable
    # -- verifying depth blocks against the trades file must fail loudly.
    trade_blocks = manifest["integrity"]["trade_blocks"]
    problems = verify_block_integrity(pdir / "depth.parquet", trade_blocks)
    assert len(problems) >= 1


# ---------------------------------------------------------------------------
# 9. Time-of-check/time-of-use: publication must fail if raw changes mid-build
# ---------------------------------------------------------------------------


def test_toctou_raw_file_change_during_build_fails_the_build(tmp_path, monkeypatch):
    """issue #20 Phase 7 review point 6: if a raw source file changes after
    the pre-build source_identity snapshot but before the post-build one,
    the build must fail closed rather than publish a manifest describing
    different raw bytes than were actually streamed.

    issue #20 Phase 7 cross-day repartitioning correction: depth_v2 source
    identity is now computed via
    ``pipeline.build_replay_store.compute_repartitioned_source_identity``
    (not directly via ``pipeline.raw_manifest.compute_raw_source_identity``,
    which is now only used internally for the unaffected trade_v2 channel)
    -- this test patches that higher-level entry point directly so the
    tamper timing is correct regardless of which raw_manifest helper any
    given channel uses internally.
    """
    raw_root = _sample_raw_root(tmp_path)
    replay_root = tmp_path / "replay"

    import pipeline.build_replay_store as build_replay_store_module
    real_compute = build_replay_store_module.compute_repartitioned_source_identity
    call_count = {"n": 0}
    depth_file = next((raw_root / "BINANCE_SPOT" / "depth_v2" / "ADAUSDT" / "2026-06-12").iterdir())

    def _tampering_compute(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            # Mutate the raw depth file between the pre-build (call 1) and
            # post-build (call 2) source_identity snapshots.
            with open(depth_file, "a") as f:
                f.write("\n")
        return real_compute(*args, **kwargs)

    monkeypatch.setattr(build_replay_store_module, "compute_repartitioned_source_identity", _tampering_compute)

    result = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root, schema_version=2
    )
    assert result["status"] == "failed"
    assert any("time-of-check" in e.lower() or "toctou" in e.lower() or "changed during this build" in e.lower() for e in result["errors"])
    # No partition must have been published for the tampered build.
    pdir = _partition_dir(replay_root, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")
    assert not (pdir / "manifest.json").exists()


# ---------------------------------------------------------------------------
# 10. v0/v1 regression: unaffected by v2's introduction
# ---------------------------------------------------------------------------


def test_v0_and_v1_still_build_and_validate_unaffected_by_v2(tmp_path):
    raw_root = _sample_raw_root(tmp_path)
    replay_root_v0 = tmp_path / "replay_v0"
    replay_root_v1 = tmp_path / "replay_v1"
    result_v0 = build_replay_for_symbol("BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root_v0)
    result_v1 = build_replay_for_symbol(
        "BINANCE_SPOT", "ADAUSDT", "2026-06-12", raw_root, replay_root_v1, schema_version=1
    )
    assert result_v0["status"] == "success"
    assert result_v1["status"] == "success"
    assert validate_partition(_partition_dir(replay_root_v0, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")) is True
    assert validate_partition(_partition_dir(replay_root_v1, "BINANCE_SPOT", "ADAUSDT", "2026-06-12")) is True

    manifest_v0 = json.loads(_manifest_path(replay_root_v0, "BINANCE_SPOT", "ADAUSDT", "2026-06-12").read_text())
    manifest_v1 = json.loads(_manifest_path(replay_root_v1, "BINANCE_SPOT", "ADAUSDT", "2026-06-12").read_text())
    assert "schema_version" not in manifest_v0
    assert manifest_v1["schema_version"] == 1
    assert "integrity" not in manifest_v1  # v1 never gains v2's integrity field
