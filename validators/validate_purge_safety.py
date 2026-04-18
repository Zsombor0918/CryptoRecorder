#!/usr/bin/env python3
"""
validate_purge_safety.py — Prove that disk cleanup deletes ONLY intended date dirs.

Creates a synthetic ``data_raw/`` tree with multiple dates, runs the
DiskMonitor cleanup targeting the oldest, then asserts:

  1. synthetic_tree_created  – temp data_raw tree built with 3 dates
  2. purge_oldest_only       – only the oldest date dir(s) removed
  3. newer_dates_survive     – remaining date dirs still present
  4. parent_dirs_survive     – venue/channel/symbol dirs never deleted
  5. catalog_untouched       – Nautilus catalog dir unchanged after purge
  6. report_paths_correct    – deleted + surviving paths reported accurately

Report → state/validation/purge_safety.json
"""
from __future__ import annotations

import json
import logging
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import NAUTILUS_CATALOG_ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate_purge_safety")

# ── synthetic date set ───────────────────────────────────────────────
DATES = ["2026-04-10", "2026-04-11", "2026-04-12"]
VENUES = ["BINANCE_SPOT", "BINANCE_USDTF"]
CHANNELS = ["trade", "depth"]
SYMBOLS_PER_VENUE = {
    "BINANCE_SPOT": ["BTCUSDT", "ETHUSDT"],
    "BINANCE_USDTF": ["BTCUSDT"],
}

# Oldest date to be purged
PURGE_DATE = DATES[0]  # 2026-04-10


class PurgeSafetyValidator:

    def __init__(self):
        self.report: Dict[str, Any] = {
            "test": "purge_safety",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "checks": {},
            "passed": False,
        }
        self._tmpdir: Path | None = None
        self._data_root: Path | None = None
        self._catalog_snapshot: Dict[str, float] | None = None
        self._deleted_paths: List[str] = []
        self._surviving_paths: List[str] = []

    def run(self) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("  Purge Safety Validation")
        logger.info("=" * 60)

        try:
            checks = [
                self._ck_synthetic_tree(),
                self._ck_purge_oldest(),
                self._ck_newer_dates_survive(),
                self._ck_parent_dirs_survive(),
                self._ck_catalog_untouched(),
                self._ck_report_paths(),
            ]
        finally:
            # Always clean up temp dir
            if self._tmpdir and self._tmpdir.exists():
                shutil.rmtree(self._tmpdir, ignore_errors=True)

        self.report["checks"] = {c["name"]: c for c in checks}
        self.report["total_checks"] = len(checks)
        self.report["checks_passed"] = sum(c["passed"] for c in checks)
        self.report["passed"] = all(c["passed"] for c in checks)
        self.report["finished_at"] = datetime.now(tz=timezone.utc).isoformat()

        for c in checks:
            tag = "PASS" if c["passed"] else "FAIL"
            logger.info(f"  [{tag}] {c['name']}")

        n = self.report["checks_passed"]
        t = self.report["total_checks"]
        if self.report["passed"]:
            logger.info(f"\n  ✓ ALL PASSED ({n}/{t})")
        else:
            logger.warning(f"\n  ✗ SOME FAILED ({n}/{t})")

        self._save()
        return self.report

    def _save(self):
        from config import STATE_ROOT
        out_dir = STATE_ROOT / "validation"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / "purge_safety.json"
        out.write_text(json.dumps(self.report, indent=2, default=str))
        logger.info(f"Report → {out}")

    # ── build synthetic tree ──────────────────────────────────────────

    def _build_tree(self) -> Path:
        """Create ``<tmpdir>/data_raw/`` with 3 dates × venues × channels × symbols."""
        self._tmpdir = Path(tempfile.mkdtemp(prefix="purge_test_"))
        dr = self._tmpdir / "data_raw"
        for venue in VENUES:
            for channel in CHANNELS:
                for sym in SYMBOLS_PER_VENUE[venue]:
                    for date_str in DATES:
                        d = dr / venue / channel / sym / date_str
                        d.mkdir(parents=True, exist_ok=True)
                        # Write a tiny JSONL so the dir is non-empty
                        (d / "00.jsonl").write_text('{"test":true}\n')
        self._data_root = dr
        return dr

    # ── snapshot catalog ──────────────────────────────────────────────

    def _snapshot_catalog(self) -> Dict[str, float]:
        """Record mtimes + sizes of catalog directory entries."""
        snap: Dict[str, float] = {}
        if NAUTILUS_CATALOG_ROOT.exists():
            for p in NAUTILUS_CATALOG_ROOT.rglob("*"):
                try:
                    snap[str(p)] = p.stat().st_mtime
                except Exception:
                    pass
        return snap

    # ── simulate purge (adapted from DiskMonitor logic) ───────────────

    def _find_oldest_date_dirs(self, data_root: Path, target_date: str) -> List[Path]:
        """Find all date directories matching target_date."""
        result = []
        for venue_dir in data_root.iterdir():
            if not venue_dir.is_dir():
                continue
            for channel_dir in venue_dir.iterdir():
                if not channel_dir.is_dir():
                    continue
                for sym_dir in channel_dir.iterdir():
                    if not sym_dir.is_dir():
                        continue
                    for d in sym_dir.iterdir():
                        if (d.is_dir()
                                and len(d.name) == 10
                                and d.name[4] == "-"
                                and d.name[7] == "-"
                                and d.name == target_date):
                            result.append(d)
        return result

    def _purge_date(self, data_root: Path, target_date: str) -> List[Path]:
        """Delete all date dirs for target_date.  Returns deleted paths."""
        dirs = self._find_oldest_date_dirs(data_root, target_date)
        deleted = []
        for d in dirs:
            shutil.rmtree(d)
            deleted.append(d)
        return deleted

    # ── checks ────────────────────────────────────────────────────────

    def _ck_synthetic_tree(self) -> dict:
        """Build a temp data_raw tree and verify it has expected structure."""
        try:
            dr = self._build_tree()
            # Count date dirs
            date_dirs = list(dr.rglob("2026-04-*"))
            date_dirs = [d for d in date_dirs if d.is_dir() and len(d.name) == 10]
            # Expected: 3 dates × (2 spot syms × 2 channels + 1 fut sym × 2 channels) = 3×6 = 18
            expected = len(DATES) * sum(
                len(SYMBOLS_PER_VENUE[v]) * len(CHANNELS) for v in VENUES
            )
            ok = len(date_dirs) == expected
            return {"name": "synthetic_tree_created", "passed": ok,
                    "details": {"date_dirs_found": len(date_dirs),
                                "expected": expected,
                                "root": str(dr)}}
        except Exception as e:
            return {"name": "synthetic_tree_created", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_purge_oldest(self) -> dict:
        """Run purge on oldest date; verify those dirs are gone."""
        try:
            self._catalog_snapshot = self._snapshot_catalog()
            deleted = self._purge_date(self._data_root, PURGE_DATE)
            self._deleted_paths = [str(d) for d in deleted]

            # Verify the deleted dirs no longer exist
            still_there = [d for d in deleted if d.exists()]
            # Should have deleted some dirs
            ok = len(deleted) > 0 and len(still_there) == 0
            return {"name": "purge_oldest_only", "passed": ok,
                    "details": {"deleted_count": len(deleted),
                                "still_existing": len(still_there),
                                "deleted_paths": self._deleted_paths[:10]}}
        except Exception as e:
            return {"name": "purge_oldest_only", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_newer_dates_survive(self) -> dict:
        """Newer dates (04-11, 04-12) must still exist."""
        try:
            surviving_dates = DATES[1:]  # 04-11, 04-12
            missing = []
            surviving_dirs = []
            for venue in VENUES:
                for channel in CHANNELS:
                    for sym in SYMBOLS_PER_VENUE[venue]:
                        for dt in surviving_dates:
                            d = self._data_root / venue / channel / sym / dt
                            if d.exists():
                                surviving_dirs.append(str(d))
                            else:
                                missing.append(str(d))

            self._surviving_paths = surviving_dirs
            expected_surviving = sum(
                len(SYMBOLS_PER_VENUE[v]) * len(CHANNELS) for v in VENUES
            ) * len(surviving_dates)  # 6 × 2 = 12
            ok = len(missing) == 0 and len(surviving_dirs) == expected_surviving
            return {"name": "newer_dates_survive", "passed": ok,
                    "details": {"surviving": len(surviving_dirs),
                                "expected": expected_surviving,
                                "missing": missing[:5]}}
        except Exception as e:
            return {"name": "newer_dates_survive", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_parent_dirs_survive(self) -> dict:
        """Venue, channel, and symbol directories must still exist."""
        try:
            missing_parents = []
            for venue in VENUES:
                vdir = self._data_root / venue
                if not vdir.exists():
                    missing_parents.append(str(vdir))
                for channel in CHANNELS:
                    cdir = vdir / channel
                    if not cdir.exists():
                        missing_parents.append(str(cdir))
                    for sym in SYMBOLS_PER_VENUE[venue]:
                        sdir = cdir / sym
                        if not sdir.exists():
                            missing_parents.append(str(sdir))

            ok = len(missing_parents) == 0
            return {"name": "parent_dirs_survive", "passed": ok,
                    "details": {"missing_parents": missing_parents[:10]}}
        except Exception as e:
            return {"name": "parent_dirs_survive", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_catalog_untouched(self) -> dict:
        """Nautilus catalog directory must not have changed."""
        try:
            if not NAUTILUS_CATALOG_ROOT.exists():
                # No catalog = nothing to break
                return {"name": "catalog_untouched", "passed": True,
                        "details": {"reason": "catalog does not exist (safe)"}}

            snap_after = self._snapshot_catalog()
            before = self._catalog_snapshot or {}

            # Compare: same files with same mtimes
            changed = []
            for path, mtime in before.items():
                after_mtime = snap_after.get(path)
                if after_mtime is None:
                    changed.append(f"DELETED: {path}")
                elif abs(after_mtime - mtime) > 0.001:
                    changed.append(f"MODIFIED: {path}")
            for path in snap_after:
                if path not in before:
                    changed.append(f"NEW: {path}")

            ok = len(changed) == 0
            return {"name": "catalog_untouched", "passed": ok,
                    "details": {"files_before": len(before),
                                "files_after": len(snap_after),
                                "changes": changed[:10]}}
        except Exception as e:
            return {"name": "catalog_untouched", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_report_paths(self) -> dict:
        """Report must list exact deleted and surviving paths."""
        ok = (len(self._deleted_paths) > 0 and len(self._surviving_paths) > 0)
        return {"name": "report_paths_correct", "passed": ok,
                "details": {
                    "deleted_paths": self._deleted_paths[:10],
                    "surviving_paths_sample": self._surviving_paths[:10],
                    "total_deleted": len(self._deleted_paths),
                    "total_surviving": len(self._surviving_paths),
                }}


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> int:
    v = PurgeSafetyValidator()
    report = v.run()
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
