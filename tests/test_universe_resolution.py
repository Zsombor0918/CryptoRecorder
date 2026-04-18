#!/usr/bin/env python3
"""
tests/test_universe_resolution.py — Prove converter.universe handles both formats.

Scenarios:
  1. dict format (recorder writes):  {"BINANCE_SPOT": [...], "BINANCE_USDTF": [...], "timestamp": "...", "top_count": 50}
  2. list format (legacy):           ["BTCUSDT", "ETHUSDT"]
  3. empty / missing file → disk scan fallback
  4. corrupt JSON → fallback

Usage:
    python tests/test_universe_resolution.py
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATE_STR = "2099-01-01"
SPOT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
FUT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


# ── helpers ──────────────────────────────────────────────────────────

def _setup_dirs(tmpdir: Path):
    """Create meta/ and data_raw/ directory structure."""
    (tmpdir / "meta" / "universe" / "BINANCE_SPOT").mkdir(parents=True)
    (tmpdir / "meta" / "universe" / "BINANCE_USDTF").mkdir(parents=True)
    (tmpdir / "data_raw").mkdir(parents=True)


def _write_meta(tmpdir: Path, venue: str, content):
    """Write universe meta file."""
    p = tmpdir / "meta" / "universe" / venue / f"{DATE_STR}.json"
    p.write_text(json.dumps(content, indent=2))


def _create_disk_dirs(tmpdir: Path, venue: str, symbols: list):
    """Create data_raw dirs for disk scan fallback."""
    for sym in symbols:
        for ch in ("trade", "depth"):
            d = tmpdir / "data_raw" / venue / ch / sym / DATE_STR
            d.mkdir(parents=True, exist_ok=True)
            # Need at least one file for scanner to find something
            (d / "00.jsonl").write_text("{}\n")


def _patch_config(tmpdir: Path):
    """Monkey-patch config paths for test."""
    import config
    config.DATA_ROOT = tmpdir / "data_raw"
    config.META_ROOT = tmpdir / "meta"
    # Reload universe module so it picks up patched config
    import importlib
    import converter.universe
    importlib.reload(converter.universe)


# ── tests ────────────────────────────────────────────────────────────

def test_dict_format():
    """Recorder format: dict with venue keys — must extract correct list."""
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        _patch_config(tmpdir)

        # Recorder writes same dict to both files
        recorder_dict = {
            "BINANCE_SPOT": SPOT_SYMBOLS,
            "BINANCE_USDTF": FUT_SYMBOLS,
            "timestamp": "2099-01-01T00:00:00",
            "top_count": 50,
        }
        _write_meta(tmpdir, "BINANCE_SPOT", recorder_dict)
        _write_meta(tmpdir, "BINANCE_USDTF", recorder_dict)

        from converter.universe import resolve_universe
        result = resolve_universe(DATE_STR)

        assert "BINANCE_SPOT" in result, f"BINANCE_SPOT missing: {result}"
        assert "BINANCE_USDTF" in result, f"BINANCE_USDTF missing: {result}"
        assert result["BINANCE_SPOT"] == SPOT_SYMBOLS, f"Spot mismatch: {result['BINANCE_SPOT']}"
        assert result["BINANCE_USDTF"] == FUT_SYMBOLS, f"Fut mismatch: {result['BINANCE_USDTF']}"
        print("  [PASS] dict_format")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_list_format():
    """Legacy format: plain list of symbols."""
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        _patch_config(tmpdir)

        _write_meta(tmpdir, "BINANCE_SPOT", SPOT_SYMBOLS)
        _write_meta(tmpdir, "BINANCE_USDTF", FUT_SYMBOLS)

        from converter.universe import resolve_universe
        result = resolve_universe(DATE_STR)

        assert "BINANCE_SPOT" in result, f"BINANCE_SPOT missing: {result}"
        assert "BINANCE_USDTF" in result, f"BINANCE_USDTF missing: {result}"
        assert result["BINANCE_SPOT"] == SPOT_SYMBOLS
        assert result["BINANCE_USDTF"] == FUT_SYMBOLS
        print("  [PASS] list_format")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_disk_fallback():
    """No meta file → falls back to disk scan."""
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        _patch_config(tmpdir)

        # No meta files, but create disk dirs
        _create_disk_dirs(tmpdir, "BINANCE_SPOT", SPOT_SYMBOLS)
        _create_disk_dirs(tmpdir, "BINANCE_USDTF", FUT_SYMBOLS)

        from converter.universe import resolve_universe
        result = resolve_universe(DATE_STR)

        assert "BINANCE_SPOT" in result, f"BINANCE_SPOT missing: {result}"
        assert set(result["BINANCE_SPOT"]) == set(SPOT_SYMBOLS)
        assert set(result["BINANCE_USDTF"]) == set(FUT_SYMBOLS)
        print("  [PASS] disk_fallback")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_corrupt_json_fallback():
    """Corrupt JSON → falls back to disk scan."""
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        _patch_config(tmpdir)

        # Write corrupt files
        p = tmpdir / "meta" / "universe" / "BINANCE_SPOT" / f"{DATE_STR}.json"
        p.write_text("{not valid json!!")

        # Create disk dirs for fallback
        _create_disk_dirs(tmpdir, "BINANCE_SPOT", ["BTCUSDT"])

        from converter.universe import resolve_universe
        result = resolve_universe(DATE_STR)

        assert "BINANCE_SPOT" in result
        assert "BTCUSDT" in result["BINANCE_SPOT"]
        print("  [PASS] corrupt_json_fallback")
        return True
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ── main ─────────────────────────────────────────────────────────────

def main() -> int:
    print("test_universe_resolution")
    results = []
    for fn in [test_dict_format, test_list_format, test_disk_fallback, test_corrupt_json_fallback]:
        try:
            results.append(fn())
        except Exception as e:
            print(f"  [FAIL] {fn.__name__}: {e}")
            results.append(False)

    passed = sum(results)
    total = len(results)
    print(f"\n  {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
