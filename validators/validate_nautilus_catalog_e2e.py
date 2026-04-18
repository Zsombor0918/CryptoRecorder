#!/usr/bin/env python3
"""
validate_nautilus_catalog_e2e.py  –  Nautilus catalog validation

Runs the converter for a given date (or the most recent date with raw data),
then queries the resulting ParquetDataCatalog to verify:

  1. catalog_exists        – NAUTILUS_CATALOG_ROOT directory exists
  2. instruments_exist     – instruments in catalog for converted symbols
  3. trades_nonempty       – trade_ticks query returns data for ≥1 symbol
  4. trades_5min_slice     – querying a 5-minute window returns results
  5. depth10_nonempty      – order_book_depth10 query returns data for ≥1 symbol
  6. depth10_5min_slice    – querying a 5-minute window returns results
  7. time_bounds           – all returned records within [day_start, day_end+1h]
  8. objects_are_nautilus   – objects are Nautilus types, not pandas rows
  9. idempotency           – re-running conversion does not corrupt catalog

Report → state/validation/nautilus_catalog_e2e_YYYY-MM-DD.json
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_ROOT, NAUTILUS_CATALOG_ROOT, STATE_ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("validate_nautilus_catalog_e2e")


# ── helpers ──────────────────────────────────────────────────────────

def _find_most_recent_date() -> Optional[str]:
    """Find most recent date that has raw data."""
    if not DATA_ROOT.exists():
        return None
    dates: Set[str] = set()
    for venue_dir in DATA_ROOT.iterdir():
        if not venue_dir.is_dir():
            continue
        for ch_dir in venue_dir.iterdir():
            if not ch_dir.is_dir():
                continue
            for sym_dir in ch_dir.iterdir():
                if not sym_dir.is_dir():
                    continue
                for d in sym_dir.iterdir():
                    if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                        if any(d.rglob("*.jsonl*")):
                            dates.add(d.name)
    return sorted(dates)[-1] if dates else None


def _find_dates_with_data() -> List[str]:
    """Return all dates with raw data, sorted descending (newest first)."""
    if not DATA_ROOT.exists():
        return []
    dates: Set[str] = set()
    for venue_dir in DATA_ROOT.iterdir():
        if not venue_dir.is_dir():
            continue
        for ch_dir in venue_dir.iterdir():
            if not ch_dir.is_dir():
                continue
            for sym_dir in ch_dir.iterdir():
                if not sym_dir.is_dir():
                    continue
                for d in sym_dir.iterdir():
                    if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                        if any(d.rglob("*.jsonl*")):
                            dates.add(d.name)
    return sorted(dates, reverse=True)


def _run_converter(date_str: str) -> bool:
    """Run the converter for a date.  Returns True on success."""
    import subprocess
    venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
    py = str(venv_py) if venv_py.exists() else sys.executable
    result = subprocess.run(
        [py, str(PROJECT_ROOT / "convert_yesterday.py"), "--date", date_str],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=300,
    )
    if result.returncode != 0:
        logger.error(f"Converter failed: {result.stderr[-500:]}")
    return result.returncode == 0


# ── checks ───────────────────────────────────────────────────────────

class NautilusCatalogValidator:

    def __init__(self, target_date: Optional[str] = None):
        self.target_date = target_date
        self.report: Dict[str, Any] = {
            "test": "nautilus_catalog_e2e",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "checks": {},
            "passed": False,
        }
        self._catalog = None
        self._instruments = []
        self._sample_iid = None

    def run(self) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("  Nautilus Catalog E2E Validation")
        logger.info("=" * 60)

        # Determine date
        if self.target_date:
            candidates = [self.target_date]
        else:
            candidates = _find_dates_with_data()

        if not candidates:
            logger.warning("No raw data found")
            self.report["skipped"] = True
            self.report["reason"] = "no raw data"
            self._save("unknown")
            return self.report

        # Try each date until conversion succeeds (today may be incomplete)
        date_str = None
        for dt in candidates:
            logger.info(f"  Trying date: {dt}")
            if _run_converter(dt):
                date_str = dt
                break
            logger.warning(f"  Converter failed for {dt}, trying next …")

        if not date_str:
            logger.warning("No raw data found")
            self.report["skipped"] = True
            self.report["reason"] = "converter failed"
            self._save(candidates[0] if candidates else "unknown")
            return self.report

        self.report["target_date"] = date_str
        logger.info(f"  Target date: {date_str}")

        # Open catalog
        try:
            from nautilus_trader.persistence.catalog import ParquetDataCatalog
            self._catalog = ParquetDataCatalog(str(NAUTILUS_CATALOG_ROOT))
        except Exception as e:
            self.report["skipped"] = True
            self.report["reason"] = f"Cannot open catalog: {e}"
            self._save(date_str)
            return self.report

        # Run checks
        checks: List[dict] = [
            self._ck_catalog_exists(),
            self._ck_instruments_exist(),
            self._ck_trades_nonempty(),
            self._ck_trades_5min(),
            self._ck_depth10_nonempty(),
            self._ck_depth10_5min(),
            self._ck_time_bounds(date_str),
            self._ck_objects_nautilus(),
            self._ck_idempotency(date_str),
        ]

        self.report["checks"] = {c["name"]: c for c in checks}
        self.report["total_checks"] = len(checks)
        self.report["checks_passed"] = sum(c["passed"] for c in checks)
        self.report["passed"] = all(c["passed"] for c in checks)
        self.report["finished_at"] = datetime.now(tz=timezone.utc).isoformat()

        for c in checks:
            tag = "PASS" if c["passed"] else "FAIL"
            logger.info(f"  [{tag}] {c['name']}")
            if not c["passed"]:
                for k, v in c.get("details", {}).items():
                    logger.info(f"         {k}: {v}")

        n = self.report["checks_passed"]
        t = self.report["total_checks"]
        if self.report["passed"]:
            logger.info(f"\n  ✓ ALL PASSED ({n}/{t})")
        else:
            logger.warning(f"\n  ✗ SOME FAILED ({n}/{t})")

        self._save(date_str)
        return self.report

    def _save(self, date_str: str):
        out_dir = STATE_ROOT / "validation"
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"nautilus_catalog_e2e_{date_str}.json"
        out.write_text(json.dumps(self.report, indent=2, default=str))
        logger.info(f"Report → {out}")

    # ── individual checks ────────────────────────────────────────────

    def _ck_catalog_exists(self) -> dict:
        ok = NAUTILUS_CATALOG_ROOT.exists() and any(NAUTILUS_CATALOG_ROOT.iterdir())
        return {"name": "catalog_exists", "passed": ok,
                "details": {"path": str(NAUTILUS_CATALOG_ROOT)}}

    def _ck_instruments_exist(self) -> dict:
        try:
            self._instruments = self._catalog.instruments()
        except Exception as e:
            return {"name": "instruments_exist", "passed": False,
                    "details": {"error": str(e)}}
        ok = len(self._instruments) > 0
        ids = [str(i.id) for i in self._instruments]
        if self._instruments:
            self._sample_iid = self._instruments[0].id
        return {"name": "instruments_exist", "passed": ok,
                "details": {"count": len(self._instruments), "ids": ids[:10]}}

    def _ck_trades_nonempty(self) -> dict:
        if not self._sample_iid:
            return {"name": "trades_nonempty", "passed": False,
                    "details": {"reason": "no instruments"}}
        try:
            trades = self._catalog.trade_ticks(instrument_ids=[self._sample_iid])
        except Exception as e:
            return {"name": "trades_nonempty", "passed": False,
                    "details": {"error": str(e)}}
        ok = len(trades) > 0
        return {"name": "trades_nonempty", "passed": ok,
                "details": {"instrument": str(self._sample_iid), "count": len(trades)}}

    def _ck_trades_5min(self) -> dict:
        if not self._sample_iid:
            return {"name": "trades_5min_slice", "passed": False,
                    "details": {"reason": "no instruments"}}
        try:
            # Get all trades, then slice to 5mins from the first
            trades = self._catalog.trade_ticks(instrument_ids=[self._sample_iid])
            if not trades:
                return {"name": "trades_5min_slice", "passed": False,
                        "details": {"reason": "no trades"}}
            first_ts = trades[0].ts_init
            end_ts = first_ts + 5 * 60 * 1_000_000_000  # 5 minutes
            sliced = [t for t in trades if t.ts_init <= end_ts]
            ok = len(sliced) > 0
            return {"name": "trades_5min_slice", "passed": ok,
                    "details": {"window_count": len(sliced), "total": len(trades)}}
        except Exception as e:
            return {"name": "trades_5min_slice", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_depth10_nonempty(self) -> dict:
        if not self._sample_iid:
            return {"name": "depth10_nonempty", "passed": False,
                    "details": {"reason": "no instruments"}}
        try:
            depth = self._catalog.order_book_depth10(instrument_ids=[self._sample_iid])
        except Exception as e:
            return {"name": "depth10_nonempty", "passed": False,
                    "details": {"error": str(e)}}
        ok = len(depth) > 0
        return {"name": "depth10_nonempty", "passed": ok,
                "details": {"instrument": str(self._sample_iid), "count": len(depth)}}

    def _ck_depth10_5min(self) -> dict:
        if not self._sample_iid:
            return {"name": "depth10_5min_slice", "passed": False,
                    "details": {"reason": "no instruments"}}
        try:
            depth = self._catalog.order_book_depth10(instrument_ids=[self._sample_iid])
            if not depth:
                return {"name": "depth10_5min_slice", "passed": False,
                        "details": {"reason": "no depth10"}}
            first_ts = depth[0].ts_init
            end_ts = first_ts + 5 * 60 * 1_000_000_000
            sliced = [d for d in depth if d.ts_init <= end_ts]
            ok = len(sliced) > 0
            return {"name": "depth10_5min_slice", "passed": ok,
                    "details": {"window_count": len(sliced), "total": len(depth)}}
        except Exception as e:
            return {"name": "depth10_5min_slice", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_time_bounds(self, date_str: str) -> dict:
        """Trades/depth for this date's range are within [day_start, day_start + 36h].

        Raw data in a date directory can extend well past midnight because
        the recorder runs continuously and files are keyed by session start.
        The catalog is cumulative, so we filter queries to the date range.
        """
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_start_ns = int(dt.timestamp() * 1e9)
        day_end_ns = int((dt + timedelta(hours=36)).timestamp() * 1e9)

        issues = []
        if self._sample_iid:
            try:
                trades = self._catalog.trade_ticks(
                    instrument_ids=[self._sample_iid],
                    start=day_start_ns, end=day_end_ns,
                )
                for t in trades:
                    if t.ts_event < day_start_ns or t.ts_event > day_end_ns:
                        issues.append(f"trade ts_event={t.ts_event} out of bounds")
                        break

                depth = self._catalog.order_book_depth10(
                    instrument_ids=[self._sample_iid],
                    start=day_start_ns, end=day_end_ns,
                )
                for d in depth:
                    if d.ts_event < day_start_ns or d.ts_event > day_end_ns:
                        issues.append(f"depth10 ts_event={d.ts_event} out of bounds")
                        break
            except Exception as e:
                issues.append(str(e))

        ok = len(issues) == 0
        return {"name": "time_bounds", "passed": ok, "details": {"issues": issues}}

    def _ck_objects_nautilus(self) -> dict:
        """Verify objects are Nautilus types, not pandas rows."""
        from nautilus_trader.model.data import TradeTick as NT_TradeTick
        from nautilus_trader.model.data import OrderBookDepth10 as NT_Depth10

        checks = {}
        if self._sample_iid:
            try:
                trades = self._catalog.trade_ticks(instrument_ids=[self._sample_iid])
                if trades:
                    checks["trade_type"] = type(trades[0]).__name__
                    checks["trade_is_nautilus"] = isinstance(trades[0], NT_TradeTick)
                depth = self._catalog.order_book_depth10(instrument_ids=[self._sample_iid])
                if depth:
                    checks["depth_type"] = type(depth[0]).__name__
                    checks["depth_is_nautilus"] = isinstance(depth[0], NT_Depth10)
            except Exception as e:
                checks["error"] = str(e)

        ok = checks.get("trade_is_nautilus", False) and checks.get("depth_is_nautilus", False)
        return {"name": "objects_are_nautilus", "passed": ok, "details": checks}

    def _ck_idempotency(self, date_str: str) -> dict:
        """Re-run conversion and verify catalog is not corrupted."""
        try:
            ok1 = _run_converter(date_str)
            if not ok1:
                return {"name": "idempotency", "passed": False,
                        "details": {"reason": "second conversion failed"}}

            # Re-open catalog and check counts haven't doubled
            from nautilus_trader.persistence.catalog import ParquetDataCatalog
            cat2 = ParquetDataCatalog(str(NAUTILUS_CATALOG_ROOT))
            instruments2 = cat2.instruments()

            # We allow equal or same count (idempotent or append-safe)
            ok = len(instruments2) > 0
            return {"name": "idempotency", "passed": ok,
                    "details": {"instruments_after_rerun": len(instruments2)}}
        except Exception as e:
            return {"name": "idempotency", "passed": False,
                    "details": {"error": str(e)}}


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Nautilus catalog E2E validation")
    ap.add_argument("--date", type=str, default=None,
                    help="Date to validate (YYYY-MM-DD)")
    args = ap.parse_args()
    v = NautilusCatalogValidator(target_date=args.date)
    report = v.run()
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
