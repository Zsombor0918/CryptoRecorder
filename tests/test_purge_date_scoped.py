#!/usr/bin/env python3
"""
tests/test_purge_date_scoped.py — Prove that catalog purge is date-scoped.

Creates a fake Nautilus catalog structure with TWO dates for the same
instrument, purges ONE date, and verifies the other date's files survive.

Checks:
  1. two_dates_setup        – both dates' parquet files exist
  2. purge_target_only      – purged files are for target date only
  3. other_date_survives    – other date's parquet files still exist
  4. instrument_meta_purged – instrument metadata re-generated (purged)
  5. non_matching_untouched – files for instruments NOT in purge list survive

Usage:
    python tests/test_purge_date_scoped.py
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── helpers ──────────────────────────────────────────────────────────

def _make_parquet(path: Path):
    """Create a fake parquet file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1_FAKE")


def _build_fake_catalog(tmpdir: Path) -> Path:
    """Build catalog with 2 dates for same instrument + 1 other instrument.

    Layout:
      data/trade_tick/BTCUSDT.BINANCE/
        2026-04-17T00-00-00-000000000Z_2026-04-17T23-59-59-000000000Z.parquet  (day 17)
        2026-04-18T00-00-00-000000000Z_2026-04-18T23-59-59-000000000Z.parquet  (day 18)
      data/order_book_depths/BTCUSDT.BINANCE/
        2026-04-17T00-00-00-000000000Z_2026-04-17T23-59-59-000000000Z.parquet  (day 17)
        2026-04-18T00-00-00-000000000Z_2026-04-18T23-59-59-000000000Z.parquet  (day 18)
      data/trade_tick/ETHUSDT.BINANCE/
        2026-04-17T00-00-00-000000000Z_2026-04-17T23-59-59-000000000Z.parquet  (day 17, different instrument)
      data/currency_pair/BTCUSDT.BINANCE/
        1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet  (instrument metadata)
    """
    cat = tmpdir / "catalog"

    # BTCUSDT trades — two dates
    for dt in ("2026-04-17", "2026-04-18"):
        fn = f"{dt}T00-00-00-000000000Z_{dt}T23-59-59-000000000Z.parquet"
        _make_parquet(cat / "data" / "trade_tick" / "BTCUSDT.BINANCE" / fn)
        _make_parquet(cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE" / fn)

    # ETHUSDT trades — only day 17 (different instrument, should NOT be touched)
    fn17 = "2026-04-17T00-00-00-000000000Z_2026-04-17T23-59-59-000000000Z.parquet"
    _make_parquet(cat / "data" / "trade_tick" / "ETHUSDT.BINANCE" / fn17)

    # Instrument metadata
    meta_fn = "1970-01-01T00-00-00-000000000Z_1970-01-01T00-00-00-000000000Z.parquet"
    _make_parquet(cat / "data" / "currency_pair" / "BTCUSDT.BINANCE" / meta_fn)
    _make_parquet(cat / "data" / "currency_pair" / "ETHUSDT.BINANCE" / meta_fn)

    return cat


# ── tests ────────────────────────────────────────────────────────────

def run_tests() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="purge_test_"))
    try:
        cat = _build_fake_catalog(tmpdir)
        results = []

        # Check 1: Both dates exist
        btc_trades = list((cat / "data" / "trade_tick" / "BTCUSDT.BINANCE").glob("*.parquet"))
        btc_depth = list((cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE").glob("*.parquet"))
        ok1 = len(btc_trades) == 2 and len(btc_depth) == 2
        print(f"  [{'PASS' if ok1 else 'FAIL'}] two_dates_setup (trades={len(btc_trades)}, depth={len(btc_depth)})")
        results.append(ok1)

        # Purge day 17 for BTCUSDT only
        from converter.catalog import purge_catalog_date_range

        # Simulate InstrumentId as string (the function uses str(iid))
        class FakeIID:
            def __init__(self, s):
                self._s = s
            def __str__(self):
                return self._s

        purged = purge_catalog_date_range(
            cat,
            [FakeIID("BTCUSDT.BINANCE")],
            "2026-04-17",
        )

        # Check 2: Purged files are for day 17 only
        remaining_trades = list((cat / "data" / "trade_tick" / "BTCUSDT.BINANCE").glob("*.parquet"))
        remaining_depth = list((cat / "data" / "order_book_depths" / "BTCUSDT.BINANCE").glob("*.parquet"))
        ok2 = purged >= 2  # at least trade + depth for day 17
        print(f"  [{'PASS' if ok2 else 'FAIL'}] purge_target_only (purged={purged})")
        results.append(ok2)

        # Check 3: Day 18 files survive
        day18_names = [f.name for f in remaining_trades + remaining_depth]
        ok3 = all("2026-04-18" in n for n in day18_names) and len(remaining_trades) == 1 and len(remaining_depth) == 1
        print(f"  [{'PASS' if ok3 else 'FAIL'}] other_date_survives (remaining={day18_names})")
        results.append(ok3)

        # Check 4: Instrument metadata purged for BTCUSDT (cheap to regenerate)
        btc_meta = list((cat / "data" / "currency_pair" / "BTCUSDT.BINANCE").glob("*.parquet"))
        ok4 = len(btc_meta) == 0
        print(f"  [{'PASS' if ok4 else 'FAIL'}] instrument_meta_purged (btc meta remaining={len(btc_meta)})")
        results.append(ok4)

        # Check 5: ETHUSDT files untouched (not in purge instrument list)
        eth_trades = list((cat / "data" / "trade_tick" / "ETHUSDT.BINANCE").glob("*.parquet"))
        eth_meta = list((cat / "data" / "currency_pair" / "ETHUSDT.BINANCE").glob("*.parquet"))
        ok5 = len(eth_trades) == 1 and len(eth_meta) == 1
        print(f"  [{'PASS' if ok5 else 'FAIL'}] non_matching_untouched (eth_trades={len(eth_trades)}, eth_meta={len(eth_meta)})")
        results.append(ok5)

        passed = sum(results)
        total = len(results)
        print(f"\n  {passed}/{total} passed")
        return 0 if passed == total else 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(run_tests())
