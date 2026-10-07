"""
pipeline.raw_manifest — Raw data coverage scanning for daily manifest.

Scans raw directory to determine available venues, symbols, and channels.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import re
from pathlib import Path
from typing import Optional

import zstandard as zstd

from config import DATA_ROOT

logger = logging.getLogger(__name__)

# Raw channels that make a directory entry a genuine tradable market-data
# symbol partition. Everything else under a venue (currently only
# "exchangeinfo", whose single pseudo-"symbol" directory is literally named
# "EXCHANGEINFO") is venue-level metadata, never a tradable symbol — it must
# never be surfaced as a candidate venue/symbol partition by
# ``scan_raw_coverage``, for either schema_version. This is intentionally a
# small, explicit allow-list (not a broad "ignore anything unfamiliar" rule):
# any future new raw channel must be added here deliberately before its
# directories can ever be treated as symbols, and any genuinely malformed
# market-data directory (e.g. a stray file where a symbol dir was expected)
# still surfaces via the existing per-venue error collection below rather
# than being silently hidden.
ELIGIBLE_MARKET_CHANNELS = frozenset({"depth_v2", "trade_v2"})


class RawRepresentationTransition(RuntimeError):
    """A selected plain hourly file is being replaced by one compressed sibling."""

    def __init__(self, logical_path: Path, detail: str) -> None:
        self.logical_path = logical_path
        super().__init__(f"raw representation transition at {logical_path}: {detail}")


class ReplayRawInventory(dict):
    """Selected paths plus attempt-local plain-byte baselines (not manifest data)."""

    def __init__(self) -> None:
        super().__init__()
        self.plain_hashes: dict[Path, str] = {}


def _logical_raw_path(path: Path) -> Path:
    name = path.name
    if name.endswith(".jsonl.zst"):
        return path.with_name(name[:-4])
    if name.endswith(".jsonl.gz"):
        return path.with_name(name[:-3])
    return path


def _is_exact_hour(path: Path) -> bool:
    """Limit transition recovery to an hourly file in its own UTC-day directory."""
    return re.fullmatch(
        rf"{re.escape(path.parent.name)}T(?:[01][0-9]|2[0-3])\.jsonl",
        path.name,
    ) is not None


def _validate_compressed_replacement(
    path: Path, *, expected_plain_sha256: str | None = None,
) -> None:
    """Prove a sole replacement is a closed, readable compressed file."""
    before = (path.stat().st_size, _sha256_file(path))
    opener = zstd.open if path.suffix == ".zst" else gzip.open
    decoded_digest = hashlib.sha256()
    with opener(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            decoded_digest.update(chunk)
    after = (path.stat().st_size, _sha256_file(path))
    if before != after:
        raise RuntimeError(f"compressed replacement changed while validating: {path}")
    if expected_plain_sha256 is not None and decoded_digest.hexdigest() != expected_plain_sha256:
        raise RuntimeError(f"decoded content differs from selected plain source: {path}")


def _selected_raw_files(channel_dir: Path) -> tuple[Path, ...]:
    files = tuple(_iter_consumed_raw_files(channel_dir))
    variants: dict[Path, list[Path]] = {}
    for path in files:
        variants.setdefault(_logical_raw_path(path), []).append(path)
    for logical, paths in variants.items():
        if len(paths) < 2:
            continue
        if (len(paths) == 2 and _is_exact_hour(logical) and logical in paths and
                sum(path.suffix in (".zst", ".gz") for path in paths) == 1):
            raise RawRepresentationTransition(logical, "compression variants: plain and compressed siblings coexist")
        _assert_no_compression_variants(list(paths), context=str(channel_dir))
    return files


def select_replay_raw_inventory(
    venue: str, symbol: str, date: str, data_root: Path,
) -> ReplayRawInventory:
    """Select exactly the schema-v2 inputs for one replay partition attempt."""
    from datetime import date as calendar_date, timedelta

    day = calendar_date.fromisoformat(date)
    inventory = ReplayRawInventory()
    for source_date in ((day - timedelta(days=1)).isoformat(), date,
                        (day + timedelta(days=1)).isoformat()):
        directory = data_root / venue / "depth_v2" / symbol / source_date
        inventory[("depth_v2", source_date)] = _selected_raw_files(directory)
    directory = data_root / venue / "trade_v2" / symbol / date
    inventory[("trade_v2", date)] = _selected_raw_files(directory)
    return inventory


def capture_plain_raw_hashes(
    inventory: ReplayRawInventory, identity: dict,
    venue: str, symbol: str, date: str, data_root: Path,
) -> None:
    """Retain decoded-byte baselines for plain inputs, including excluded depth."""
    known = {
        data_root / entry["path"]: entry["sha256"]
        for entries in identity["channels"].values() for entry in entries
        if entry["path"].endswith(".jsonl")
    }
    for paths in inventory.values():
        for path in paths:
            if path.suffix != ".jsonl":
                continue
            if path in known:
                inventory.plain_hashes[path] = known[path]
                continue
            try:
                inventory.plain_hashes[path] = _sha256_file(path)
            except FileNotFoundError as exc:
                check_selected_raw_disappearance(
                    inventory, path, venue, symbol, date, data_root
                )
                raise RuntimeError(f"selected raw input disappeared: {path}") from exc


def verify_plain_raw_hashes(
    inventory: ReplayRawInventory,
    venue: str, symbol: str, date: str, data_root: Path,
) -> None:
    """Keep same-path mutations of otherwise excluded plain inputs fail-closed."""
    for path, expected in inventory.plain_hashes.items():
        try:
            current = _sha256_file(path)
        except FileNotFoundError as exc:
            check_selected_raw_disappearance(
                inventory, path, venue, symbol, date, data_root
            )
            raise RuntimeError(f"selected raw input disappeared: {path}") from exc
        if current != expected:
            raise RuntimeError(f"selected raw content changed during build: {path}")


def assert_replay_inventory_unchanged(
    selected: dict[tuple[str, str], tuple[Path, ...]],
    venue: str, symbol: str, date: str, data_root: Path,
) -> None:
    """Reject drift; retry only an exact plain-to-compressed replacement."""
    current = select_replay_raw_inventory(venue, symbol, date, data_root)
    if current == selected:
        return
    changed = [(key, old, current[key]) for key, old in selected.items()
               if old != current[key]]
    if len(changed) == 1:
        _key, old, new = changed[0]
        removed = set(old) - set(new)
        added = set(new) - set(old)
        if len(removed) == len(added) == 1:
            plain, compressed = next(iter(removed)), next(iter(added))
            if (plain.suffix == ".jsonl" and _is_exact_hour(plain) and
                    compressed in (Path(f"{plain}.zst"), Path(f"{plain}.gz"))):
                _validate_compressed_replacement(
                    compressed,
                    expected_plain_sha256=getattr(selected, "plain_hashes", {}).get(plain),
                )
                raise RawRepresentationTransition(plain, f"now represented by {compressed.name}")
    raise RuntimeError(f"selected raw inventory changed for {venue}/{symbol}/{date}; refusing mixed source evidence")


def check_selected_raw_disappearance(
    selected: dict[tuple[str, str], tuple[Path, ...]],
    missing: Path, venue: str, symbol: str, date: str, data_root: Path,
) -> None:
    """Raise a transition only when ENOENT has the exact valid replacement."""
    if (missing.suffix != ".jsonl" or not _is_exact_hour(missing) or
            not any(missing in files for files in selected.values())):
        return
    assert_replay_inventory_unchanged(selected, venue, symbol, date, data_root)


def _iter_consumed_raw_files(channel_dir: Path) -> "list[Path]":
    """Return raw files in the exact deterministic order consumed by
    ``converter.readers.stream_raw_records``.

    Source identity must never describe a broader directory listing than the
    build actually reads. In particular, regular diagnostic/sidecar files are
    not replay inputs merely because they happen to live beside the JSONL
    files.
    """
    return [
        path
        for path in sorted(channel_dir.glob("*.jsonl*"))
        if path.is_file()
    ]


def _assert_no_compression_variants(
    files: "list[Path]",
    *,
    context: str,
) -> None:
    """Reject simultaneous plain/compressed siblings of one logical JSONL.

    The raw reader would consume every matching sibling. For schema-v2
    provenance that layout is ambiguous (and commonly means compression is
    still in progress), so strict identity collection must fail closed.
    """
    variants: dict[str, list[str]] = {}
    for path in files:
        name = path.name
        if name.endswith(".jsonl.zst"):
            logical_name = name[:-4]
        elif name.endswith(".jsonl.gz"):
            logical_name = name[:-3]
        elif name.endswith(".jsonl"):
            logical_name = name
        else:
            continue
        variants.setdefault(logical_name, []).append(name)
    conflicts = {
        logical_name: sorted(names)
        for logical_name, names in variants.items()
        if len(names) > 1
    }
    if conflicts:
        raise RuntimeError(
            f"Ambiguous coexisting raw compression variants in {context}: "
            f"{conflicts!r}"
        )


def _sha256_file(path: Path) -> str:
    """Stream a file through SHA-256 in bounded (64 KiB) chunks — never reads
    a whole raw file into memory at once."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _count_parsed_records(path: Path) -> int:
    """Count successfully-JSON-parsed lines in one raw file, streaming line
    by line (bounded memory, one line in flight at a time) — mirrors
    ``converter.readers.stream_raw_records``'s own per-file opener/parse
    logic exactly, so the resulting count matches the number of records
    that file actually contributes to the enumerated ``raw_index`` stream
    (malformed lines that ``stream_raw_records`` silently skips are not
    counted here either, for the same reason)."""
    count = 0
    if path.suffix == ".zst":
        opener = lambda: zstd.open(path, "rt", errors="ignore")
    elif path.suffix == ".gz":
        opener = lambda: gzip.open(path, "rt", errors="ignore")
    else:
        opener = lambda: open(path, "r", errors="ignore")
    with opener() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError:
                continue
            count += 1
    return count


def compute_raw_source_identity(
    venue: str,
    symbol: str,
    date_str: str,
    channels: "list[str]",
    data_root: Optional[Path] = None,
    *,
    include_record_counts: bool = False,
    strict: bool = False,
    inventory: Optional[dict[tuple[str, str], tuple[Path, ...]]] = None,
) -> dict:
    """Record per-file identity (path + SHA-256 + size) for the raw files that
    back one venue/symbol/date/channel set.

    This is the minimal, best-effort "source-identity/checksum information"
    the issue #20 Phase 2 traceability design (docs/IMPLEMENTATION_AUDIT.md,
    "Traceability design") requires new replay manifests to carry (item 1 of
    its planned hierarchy: "Raw file/chunk identity + SHA-256 checksum,
    recorded per partition in the manifest").

    ``include_record_counts`` (issue #20 Phase 7 hierarchical-integrity
    candidate, schema_version=2 only): when True, also computes each file's
    ``record_count`` (number of successfully-parsed JSON lines, matching
    exactly how many records that file contributes to the enumerated
    ``raw_index`` stream — see ``_count_parsed_records``) and a cumulative,
    deterministic ``record_range`` ``[start, end)`` per file, in the same
    sorted-filename order ``converter.readers.stream_raw_records`` reads
    them in. This is what makes
    ``stores.replay_writer.resolve_source_record`` possible: a bounded (one
    entry per file, not per event) mapping from a replay event's
    ``raw_index`` back to its exact source raw file and contribution
    ordinal. For this non-repartitioned helper the contribution ordinal is
    also the parsed-record ordinal in the file. Adds one
    additional bounded, streaming read pass per raw file (never loads a
    whole file into memory) — left ``False`` by default so schema_version=1
    (and any other caller) pays zero extra I/O cost for this.

    ``strict=True`` makes any selected ``*.jsonl*`` file that cannot be
    checksummed or decoded an immediate error. The schema-v2 builder uses
    this mode so a reader-side log-and-continue cannot be mistaken for a
    complete source snapshot. In best-effort mode, such a failure still marks
    the channel/source identity incomplete even when another file succeeded.

    Checksum scope (issue #20 Phase 7 review — precise, not left implicit):
      - ``sha256``/``size_bytes`` cover the raw file's ON-DISK bytes exactly
        as stored (i.e. the COMPRESSED bytes for ``.zst``/``.gz`` files, not
        the decompressed record stream) — this is what ``_sha256_file``
        streams. It detects any change to the stored artifact itself
        (recompression with different settings included), independent of
        whether the decoded record content also changed.
      - ``record_count``/``record_range`` describe only the DECOMPRESSED,
        successfully decoded JSON-line count/order (via
        ``_count_parsed_records``). They localize missing/extra-record count
        changes but do not detect a same-count value edit; the stored-byte
        SHA-256 is the content-integrity layer. Recompressing identical
        decoded content can change ``sha256`` without changing the count.
      - ``path`` is always POSIX-relative to ``data_root`` (via
        ``Path.relative_to`` + ``.as_posix()``) — never absolute, and never
        includes mtime/inode/owner or any other machine-specific metadata.
        This is required so that independent machines and Syncthing-synced
        copies of the same raw bytes at different local paths (or different
        OS path separators) produce and consume byte-identical
        ``source_identity`` dicts.

    Returns:
        {
            "venue": "<venue>", "symbol": "<symbol>", "date": "<date_str>",
            "channels": {"depth_v2": [{"path": "<posix-relative-file>",
                                        "sha256": "<hex>",
                                        "size_bytes": <int>,
                                        "record_count": <int>,       # only if include_record_counts
                                        "record_range": [<start>, <end>]}, ...], ...},
            "complete": bool,  # True only if every requested channel had at
                                # least one readable selected raw file and no
                                # selected file failed identity collection
            "missing_channels": [<channel names missing usable complete input>],
        }
    """
    if data_root is None:
        data_root = DATA_ROOT
    data_root = Path(data_root)

    result: dict = {
        "venue": venue,
        "symbol": symbol,
        "date": date_str,
        "channels": {},
        "complete": True,
        "missing_channels": [],
    }
    for channel in channels:
        channel_dir = data_root / venue / channel / symbol / date_str
        entries: "list[dict]" = []
        cumulative = 0
        channel_failed = False
        if channel_dir.exists():
            selected_files = (
                inventory[(channel, date_str)] if inventory is not None
                else _iter_consumed_raw_files(channel_dir)
            )
            if strict:
                _assert_no_compression_variants(
                    selected_files,
                    context=f"{venue}/{channel}/{symbol}/{date_str}",
                )
            for fpath in selected_files:
                try:
                    entry = {
                        # POSIX-relative, canonical, machine-independent —
                        # never an absolute path, mtime, inode, or owner.
                        "path": fpath.relative_to(data_root).as_posix(),
                        "sha256": _sha256_file(fpath),
                        "size_bytes": fpath.stat().st_size,
                    }
                    if include_record_counts:
                        record_count = _count_parsed_records(fpath)
                        entry["record_count"] = record_count
                        entry["record_range"] = [cumulative, cumulative + record_count]
                        cumulative += record_count
                    entries.append(entry)
                except FileNotFoundError as exc:
                    if strict and inventory is not None:
                        check_selected_raw_disappearance(
                            inventory, fpath, venue, symbol, date_str, data_root
                        )
                    channel_failed = True
                    if strict:
                        raise RuntimeError(f"Could not read selected raw input {fpath}: {exc}") from exc
                except Exception as exc:
                    channel_failed = True
                    logger.warning(f"Could not checksum raw file {fpath}: {exc}")
                    if strict:
                        raise RuntimeError(
                            f"Could not read selected raw input {fpath} while "
                            f"computing source identity for "
                            f"{venue}/{symbol}/{date_str}/{channel}: {exc}"
                        ) from exc
        result["channels"][channel] = entries
        if channel_failed or not entries:
            result["complete"] = False
            if channel not in result["missing_channels"]:
                result["missing_channels"].append(channel)
    return result


def scan_raw_coverage(
    date_str: str,
    data_root: Optional[Path] = None,
) -> dict:
    """
    Scan raw data directory for available venues/symbols/channels on a given date.

    Only ``depth_v2``/``trade_v2`` directory entries can ever contribute a
    tradable venue/symbol partition to the returned ``data`` mapping (see
    ``ELIGIBLE_MARKET_CHANNELS``) — venue-level metadata channels (currently
    only ``exchangeinfo``, whose sole "symbol" directory is literally named
    "EXCHANGEINFO") are scanned on disk (so genuinely malformed directories
    still surface as errors below) but are never treated as symbols. A
    symbol present under only one of ``depth_v2``/``trade_v2`` is still
    reported honestly in ``data`` (with only that one channel key set to
    True) — this function does not silently drop or hide partial channel
    coverage; callers that require both channels decide that policy
    themselves (e.g. ``pipeline.daily_build``).

    Args:
        date_str: Date string (YYYY-MM-DD)
        data_root: Optional custom data_root. If None, uses config.DATA_ROOT.

    Returns:
        Dict with structure:
        {
            "date": "2026-06-15",
            "venues": ["BINANCE_SPOT", "BINANCE_USDTF"],
            "data": {
                "BINANCE_SPOT": {
                    "BTCUSDT": {"depth_v2": True, "trade_v2": True},
                    "ETHUSDT": {"depth_v2": True, "trade_v2": True},
                },
                "BINANCE_USDTF": {...}
            },
            "symbol_count": 2,
            "errors": [],
        }
    """
    if data_root is None:
        data_root = DATA_ROOT

    result = {
        "date": date_str,
        "venues": [],
        "data": {},
        "symbol_count": 0,
        "errors": [],
    }

    data_root = Path(data_root)
    if not data_root.exists():
        result["errors"].append(f"data_root does not exist: {data_root}")
        return result

    try:
        # List venues (directories like BINANCE_SPOT, BINANCE_USDTF)
        venue_dirs = sorted([d for d in data_root.iterdir() if d.is_dir()])

        for venue_dir in venue_dirs:
            venue = venue_dir.name
            result["venues"].append(venue)
            result["data"][venue] = {}

            try:
                # List channels (depth_v2, trade_v2, exchangeinfo)
                for channel_dir in sorted(venue_dir.iterdir()):
                    if not channel_dir.is_dir():
                        continue

                    channel = channel_dir.name
                    if channel not in ELIGIBLE_MARKET_CHANNELS:
                        # Venue-level metadata (e.g. "exchangeinfo"), not a
                        # market-data channel — its directory entries (e.g.
                        # the single "EXCHANGEINFO" pseudo-symbol) must never
                        # be surfaced as a tradable venue/symbol partition.
                        continue

                    # List symbols for this channel/date
                    for symbol_dir in sorted(channel_dir.iterdir()):
                        if not symbol_dir.is_dir():
                            continue

                        symbol = symbol_dir.name

                        # Check if date exists for this symbol/channel
                        date_dir = symbol_dir / date_str
                        if date_dir.exists():
                            # Record this symbol/channel combination
                            if symbol not in result["data"][venue]:
                                result["data"][venue][symbol] = {}
                            result["data"][venue][symbol][channel] = True

            except Exception as e:
                result["errors"].append(f"Error scanning venue {venue}: {e}")
                logger.error(f"Error scanning venue {venue}: {e}")

        # Count unique symbols across all venues
        all_symbols = set()
        for venue_data in result["data"].values():
            all_symbols.update(venue_data.keys())
        result["symbol_count"] = len(all_symbols)

        logger.info(
            f"Raw coverage scan for {date_str}: {len(result['venues'])} venues, "
            f"{result['symbol_count']} symbols"
        )

    except Exception as e:
        result["errors"].append(f"Critical error scanning raw_manifest: {e}")
        logger.error(f"Critical error scanning raw_manifest: {e}")

    return result
