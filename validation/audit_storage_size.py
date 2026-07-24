"""validation.audit_storage_size — measure on-disk size of CryptoRecorder artifacts.

Audit-only (no build/transform). Reports the bytes used by the replay_store /
generated catalog for a single venue/symbol/date so storage growth can be
tracked honestly instead of guessed.

This is Phase 0 of the issue #20 compact-replay-storage plan
(docs/IMPLEMENTATION_AUDIT.md / docs/FULL_L2_REPLAY_CATALOG_PLAN.md): it
establishes a reproducible baseline measurement *before* any schema change,
distinguishing:

- published replay bytes (the canonical `depth.parquet` / `trades.parquet`
  under `venue=.../symbol=.../date=...`);
- staging/backup/quarantine bytes (temporary artifacts that must never be
  counted as published output);
- allocated (actual disk blocks used) vs. apparent (logical file length)
  bytes, since sparse/compressed filesystems can differ significantly;
- per-trade / per-depth-event / per-depth-level byte estimates, since a
  single "bytes per row" average is misleading when depth events carry a
  varying number of book levels.

Example::

    python -m validation.audit_storage_size \\
      --venue BINANCE_SPOT --symbol ADAUSDT --date 2026-06-12 \\
      --replay-root /tmp/cr-full-l2/replay_store \\
      --catalog-root /tmp/cr-full-l2/new_catalog/job_validation_new \\
      --json

Root-wide staging/backup/quarantine scan (independent of any single
venue/symbol/date; measurement only, never deletes anything)::

    python -m validation.audit_storage_size --replay-root /data/.../replay_store --scratch-only --json
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Directory-name prefixes that mark temporary (never-published) replay
# artifacts. Kept as a single source of truth so the "published vs scratch"
# split here and any future root-wide lifecycle tooling (issue #20 Phase 6/11)
# agree on what counts as scratch.
_SCRATCH_PREFIXES: tuple[str, ...] = (".staging_", ".backup_", ".quarantine_")


def _file_apparent_bytes(path: Path) -> int:
    """Logical file length (st_size) — what `ls -l` / Python `len(data)` report."""
    return path.stat().st_size


def _file_allocated_bytes(path: Path) -> int:
    """Actual disk blocks used (st_blocks * 512), independent of the logical
    length. On filesystems with block-size rounding, sparse regions, or
    compression, this can differ meaningfully from apparent bytes — the size
    acceptance gate in issue #20 must report both, not just one."""
    try:
        return path.stat().st_blocks * 512
    except AttributeError:
        # st_blocks is POSIX-only; fall back to apparent size on platforms
        # that don't expose it (e.g. some non-POSIX filesystems).
        return path.stat().st_size


def _measure(path: Path) -> dict[str, int]:
    """Recursively sum apparent and allocated bytes for a file or directory.
    Returns zeros (not an error) for a missing path — many artifacts this
    function is asked to measure (backup, quarantine, catalog) are
    legitimately absent most of the time."""
    if not path.exists():
        return {"apparent_bytes": 0, "allocated_bytes": 0}
    if path.is_file():
        return {
            "apparent_bytes": _file_apparent_bytes(path),
            "allocated_bytes": _file_allocated_bytes(path),
        }
    apparent = 0
    allocated = 0
    for p in path.rglob("*"):
        if p.is_file():
            apparent += _file_apparent_bytes(p)
            allocated += _file_allocated_bytes(p)
    return {"apparent_bytes": apparent, "allocated_bytes": allocated}


def _dir_bytes(path: Path) -> int:
    """Backward-compatible apparent-bytes-only helper (pre-Phase-0 callers)."""
    return _measure(path)["apparent_bytes"]


def _human(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TiB"


def _replay_partition(replay_root: Path, venue: str, symbol: str, date: str) -> Path:
    return replay_root / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"


def _load_manifest(partition_dir: Path) -> Optional[dict[str, Any]]:
    manifest_path = partition_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(f"Could not read manifest at {manifest_path}: {exc}")
        return None


def _count_depth_levels(depth_parquet: Path) -> Optional[dict[str, int]]:
    """Count total bid+ask levels across all depth rows, using pyarrow if
    available. Returns None (not zero) when pyarrow is unavailable or the
    file cannot be read, so callers never silently report a false zero."""
    if not depth_parquet.exists():
        return {"depth_events": 0, "total_levels": 0, "bid_levels": 0, "ask_levels": 0}
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.warning(
            "pyarrow not installed — cannot compute per-depth-level byte "
            "estimates; install pyarrow to get this breakdown."
        )
        return None

    total_bid_levels = 0
    total_ask_levels = 0
    depth_events = 0
    try:
        parquet_file = pq.ParquetFile(depth_parquet)
        for batch in parquet_file.iter_batches(columns=["bids", "asks"], batch_size=5000):
            bids_col = batch.column("bids")
            asks_col = batch.column("asks")
            depth_events += len(batch)
            for i in range(len(batch)):
                bid_val = bids_col[i]
                ask_val = asks_col[i]
                total_bid_levels += 0 if bid_val is None else len(bid_val)
                total_ask_levels += 0 if ask_val is None else len(ask_val)
    except Exception as exc:  # noqa: BLE001 - audit tool must not crash on a
        # malformed/corrupt file; report the failure instead of aborting the
        # whole size report.
        logger.warning(f"Could not read depth levels from {depth_parquet}: {exc}")
        return None

    return {
        "depth_events": depth_events,
        "total_levels": total_bid_levels + total_ask_levels,
        "bid_levels": total_bid_levels,
        "ask_levels": total_ask_levels,
    }


def _find_scratch_dirs(replay_root: Path) -> list[Path]:
    """Root-wide scan for `.staging_*` / `.backup_*` / `.quarantine_*`
    directories under any `venue=*/symbol=*/` path. This is measurement-only
    (per docs/IMPLEMENTATION_AUDIT.md and the issue #20 plan) — it never
    deletes anything; a future lifecycle tool decides active/recoverable/
    abandoned/unknown classification before any mutation.

    Deliberately root-wide and independent of any single day's eligible
    symbol universe: a `.staging_*` orphan for a symbol not present in
    today's build (e.g. the known BANKUSDT 2026-07-21 case) would never be
    found by a scan scoped to the current build's venue/symbol only."""
    if not replay_root.exists():
        return []
    found: list[Path] = []
    for venue_dir in replay_root.glob("venue=*"):
        if not venue_dir.is_dir():
            continue
        for symbol_dir in venue_dir.glob("symbol=*"):
            if not symbol_dir.is_dir():
                continue
            for entry in symbol_dir.iterdir():
                if entry.is_dir() and entry.name.startswith(_SCRATCH_PREFIXES):
                    found.append(entry)
    return found


def audit_scratch_bytes(replay_root: Path) -> dict[str, Any]:
    """Report staging/backup/quarantine bytes across the *entire* replay_store,
    separately from any single partition's published bytes. This never
    mutates or deletes anything — it is measurement only."""
    scratch_dirs = _find_scratch_dirs(replay_root)
    by_kind: dict[str, dict[str, int]] = {
        "staging": {"apparent_bytes": 0, "allocated_bytes": 0, "count": 0},
        "backup": {"apparent_bytes": 0, "allocated_bytes": 0, "count": 0},
        "quarantine": {"apparent_bytes": 0, "allocated_bytes": 0, "count": 0},
    }
    entries: list[dict[str, Any]] = []
    for entry in scratch_dirs:
        if entry.name.startswith(".staging_"):
            kind = "staging"
        elif entry.name.startswith(".backup_"):
            kind = "backup"
        else:
            kind = "quarantine"
        measured = _measure(entry)
        by_kind[kind]["apparent_bytes"] += measured["apparent_bytes"]
        by_kind[kind]["allocated_bytes"] += measured["allocated_bytes"]
        by_kind[kind]["count"] += 1
        entries.append({"path": str(entry), "kind": kind, **measured})

    return {
        "replay_root": str(replay_root),
        "by_kind": by_kind,
        "entries": entries,
        "total_scratch_apparent_bytes": sum(v["apparent_bytes"] for v in by_kind.values()),
        "total_scratch_allocated_bytes": sum(v["allocated_bytes"] for v in by_kind.values()),
        "note": (
            "Measurement only — never deletes or mutates staging/backup/"
            "quarantine directories. Root-wide scan across all venues/"
            "symbols/dates, independent of any single day's eligible "
            "symbol universe."
        ),
    }


def audit_storage_size(
    *,
    venue: str,
    symbol: str,
    date: str,
    replay_root: Optional[Path] = None,
    catalog_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Collect byte sizes for the replay/catalog artifacts of one partition.

    Reports allocated *and* apparent bytes for every component, plus
    per-trade / per-depth-event / per-depth-level byte estimates derived from
    the partition's manifest record counts and (if pyarrow is available) an
    exact depth-level count. These per-unit figures are the basis for the
    issue #20 Tier-3 representative-day size report — a single "bytes per
    replay row" average is explicitly flagged there as orientation-only,
    since depth events carry a varying number of levels.
    """
    components: list[dict[str, Any]] = []
    manifest: Optional[dict[str, Any]] = None
    level_stats: Optional[dict[str, int]] = None

    depth_bytes: Optional[dict[str, int]] = None
    trades_bytes: Optional[dict[str, int]] = None

    if replay_root is not None:
        partition = _replay_partition(replay_root, venue, symbol, date)
        depth = partition / "depth.parquet"
        trades = partition / "trades.parquet"
        depth_bytes = _measure(depth)
        trades_bytes = _measure(trades)
        components.append({"artifact": "replay.depth.parquet", "path": str(depth), **depth_bytes})
        components.append({"artifact": "replay.trades.parquet", "path": str(trades), **trades_bytes})
        manifest = _load_manifest(partition)
        level_stats = _count_depth_levels(depth)

    if catalog_root is not None:
        data_root = catalog_root / "data"
        base = data_root if data_root.exists() else catalog_root
        for sub in ("trade_tick", "order_book_deltas", "order_book_depths", "order_book_depth10"):
            target = base / sub
            if target.exists():
                components.append({"artifact": f"catalog.{sub}", "path": str(target), **_measure(target)})
        components.append({"artifact": "catalog.total", "path": str(catalog_root), **_measure(catalog_root)})

    total_apparent = sum(
        c["apparent_bytes"] for c in components if not c["artifact"].endswith(".total")
    )
    total_allocated = sum(
        c["allocated_bytes"] for c in components if not c["artifact"].endswith(".total")
    )

    per_unit: dict[str, Any] = {}
    if manifest is not None and depth_bytes is not None and trades_bytes is not None:
        depth_record_count = manifest.get("depth_record_count")
        trade_record_count = manifest.get("trade_record_count")

        if trade_record_count:
            per_unit["apparent_bytes_per_trade"] = trades_bytes["apparent_bytes"] / trade_record_count
            per_unit["allocated_bytes_per_trade"] = trades_bytes["allocated_bytes"] / trade_record_count
        else:
            per_unit["apparent_bytes_per_trade"] = None
            per_unit["allocated_bytes_per_trade"] = None

        if depth_record_count:
            per_unit["apparent_bytes_per_depth_event"] = depth_bytes["apparent_bytes"] / depth_record_count
            per_unit["allocated_bytes_per_depth_event"] = depth_bytes["allocated_bytes"] / depth_record_count
        else:
            per_unit["apparent_bytes_per_depth_event"] = None
            per_unit["allocated_bytes_per_depth_event"] = None

        if level_stats is not None and level_stats["total_levels"]:
            per_unit["apparent_bytes_per_depth_level"] = (
                depth_bytes["apparent_bytes"] / level_stats["total_levels"]
            )
            per_unit["allocated_bytes_per_depth_level"] = (
                depth_bytes["allocated_bytes"] / level_stats["total_levels"]
            )
        else:
            per_unit["apparent_bytes_per_depth_level"] = None
            per_unit["allocated_bytes_per_depth_level"] = None
            if level_stats is None:
                per_unit["depth_level_note"] = (
                    "pyarrow unavailable or depth.parquet unreadable — "
                    "per-depth-level bytes could not be computed"
                )
    else:
        per_unit = {
            "apparent_bytes_per_trade": None,
            "allocated_bytes_per_trade": None,
            "apparent_bytes_per_depth_event": None,
            "allocated_bytes_per_depth_event": None,
            "apparent_bytes_per_depth_level": None,
            "allocated_bytes_per_depth_level": None,
            "note": "No manifest found for this partition — per-unit bytes unavailable.",
        }

    return {
        "venue": venue,
        "symbol": symbol,
        "date": date,
        "components": components,
        "total_bytes_excluding_catalog_total": total_apparent,
        "total_apparent_bytes_excluding_catalog_total": total_apparent,
        "total_allocated_bytes_excluding_catalog_total": total_allocated,
        "manifest_record_counts": {
            "depth_record_count": (manifest or {}).get("depth_record_count"),
            "trade_record_count": (manifest or {}).get("trade_record_count"),
        },
        "depth_level_stats": level_stats,
        "per_unit_bytes": per_unit,
        "note": (
            "Single venue/symbol/date measurement. Extrapolating to the full "
            "universe is a rough estimate, not a benchmark — liquidity varies "
            "widely across symbols. Per-unit bytes are orientation-only "
            "since a single 'bytes per row' average hides the fact that "
            "depth events carry a varying number of book levels."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit on-disk size of CryptoRecorder artifacts.")
    parser.add_argument("--venue", required=False, help="required unless --scratch-only")
    parser.add_argument("--symbol", required=False, help="required unless --scratch-only")
    parser.add_argument("--date", required=False, help="UTC date YYYY-MM-DD; required unless --scratch-only")
    parser.add_argument("--replay-root", type=Path, default=None)
    parser.add_argument("--catalog-root", type=Path, default=None, help="A generated job_* catalog root")
    parser.add_argument(
        "--scratch-only",
        action="store_true",
        help=(
            "Only report root-wide staging/backup/quarantine bytes across "
            "the entire --replay-root (independent of --venue/--symbol/"
            "--date). Measurement only; never deletes or mutates anything."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.scratch_only:
        if args.replay_root is None:
            parser.error("--scratch-only requires --replay-root")
        report = audit_scratch_bytes(args.replay_root)
        if args.json:
            print(json.dumps(report, indent=2))
            return 0
        print(f"Scratch audit (root-wide): {report['replay_root']}")
        print(f"{'kind':<12} {'count':>6} {'apparent':>12} {'allocated':>12}")
        print("-" * 44)
        for kind, stats in report["by_kind"].items():
            print(
                f"{kind:<12} {stats['count']:>6} "
                f"{_human(stats['apparent_bytes']):>12} {_human(stats['allocated_bytes']):>12}"
            )
        print("-" * 44)
        print(f"\n{report['note']}")
        return 0

    if not (args.venue and args.symbol and args.date):
        parser.error("--venue/--symbol/--date are required unless --scratch-only is used")

    report = audit_storage_size(
        venue=args.venue,
        symbol=args.symbol,
        date=args.date,
        replay_root=args.replay_root,
        catalog_root=args.catalog_root,
    )

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"Storage audit: {args.venue}/{args.symbol}/{args.date}")
    print(f"{'artifact':<28} {'apparent':>12} {'allocated':>12}")
    print("-" * 54)
    for component in report["components"]:
        print(
            f"{component['artifact']:<28} "
            f"{_human(component['apparent_bytes']):>12} {_human(component['allocated_bytes']):>12}"
        )
    print("-" * 54)
    print(
        f"{'sum (excl catalog.total)':<28} "
        f"{_human(report['total_apparent_bytes_excluding_catalog_total']):>12} "
        f"{_human(report['total_allocated_bytes_excluding_catalog_total']):>12}"
    )

    per_unit = report["per_unit_bytes"]
    print("\nPer-unit bytes (orientation only — depth events vary in level count):")
    for key in (
        "apparent_bytes_per_trade",
        "apparent_bytes_per_depth_event",
        "apparent_bytes_per_depth_level",
    ):
        value = per_unit.get(key)
        formatted = f"{value:.2f}" if isinstance(value, (int, float)) else "n/a"
        print(f"  {key:<34} {formatted}")

    print(f"\n{report['note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
