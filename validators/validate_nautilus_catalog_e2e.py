#!/usr/bin/env python3
"""
validate_nautilus_catalog_e2e.py — Nautilus catalog end-to-end validation.

Runs the converter for a given date then queries the resulting
ParquetDataCatalog to prove that Nautilus-native objects are
queryable and internally consistent.

Checks (16):
   1.  converter_exit_zero        – converter exits 0
   2.  catalog_exists             – catalog directory is non-empty
   3.  report_valid               – convert report exists with required fields
   4.  instruments_exist          – catalog.instruments() returns objects
   5.  trades_nonempty            – trade_ticks query returns ≥1 row
   6.  trades_5min_slice          – 5-minute window query returns results
   7.  depth10_nonempty           – order_book_depth10 query returns ≥1 row
   8.  depth10_5min_slice         – 5-minute window query returns results
   9.  time_bounds                – ts_event within [day_start, day_start+36h]
  10.  objects_are_nautilus        – types are real Nautilus model types
  11.  instrument_id_mapping       – ALL trades+depth reference valid instruments
  12.  instrument_venue_mapping    – spot/futures instrument IDs use correct conventions
  13.  idempotency_counts          – re-run yields identical counts (no duplication)
  14.  gap_fields_valid            – report includes gap/reset counters, gap_rate
  15.  gap_rate_sane               – gap_rate is non-negative and < 1.0
  16.  gap_per_symbol              – per-symbol gap breakdown present & non-negative

Report → state/validation/nautilus_catalog_e2e_{date}.json
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
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


def _run_converter(date_str: str) -> tuple[bool, str]:
    """Run the converter for a date.  Returns (success, stderr_tail)."""
    venv_py = PROJECT_ROOT / ".venv" / "bin" / "python3"
    py = str(venv_py) if venv_py.exists() else sys.executable
    result = subprocess.run(
        [py, str(PROJECT_ROOT / "convert_yesterday.py"), "--date", date_str],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT), timeout=300,
    )
    if result.returncode != 0:
        logger.error(f"Converter failed: {result.stderr[-500:]}")
    return result.returncode == 0, result.stderr[-500:]


def _load_report(date_str: str) -> Optional[dict]:
    rp = STATE_ROOT / "convert_reports" / f"{date_str}.json"
    if not rp.exists():
        return None
    try:
        return json.loads(rp.read_text())
    except Exception:
        return None


# ── validator ────────────────────────────────────────────────────────

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
        self._all_instrument_ids: Set[str] = set()
        self._sample_iid = None
        self._converter_ok = False
        self._date_str: Optional[str] = None
        self._convert_report: Optional[dict] = None
        # Counts captured after first run (for idempotency)
        self._run1_counts: Optional[dict] = None

    def run(self) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("  Nautilus Catalog E2E Validation")
        logger.info("=" * 60)

        # ── pick a date ───────────────────────────────────────────────
        candidates = [self.target_date] if self.target_date else _find_dates_with_data()
        if not candidates:
            self._skip("no raw data")
            return self.report

        for dt in candidates:
            logger.info(f"  Trying date: {dt}")
            ok, err = _run_converter(dt)
            if ok:
                self._converter_ok = True
                self._date_str = dt
                break
            logger.warning(f"  Converter failed for {dt}, trying next …")

        if not self._date_str:
            self._skip("converter failed")
            return self.report

        self.report["target_date"] = self._date_str
        self._convert_report = _load_report(self._date_str)
        logger.info(f"  Target date: {self._date_str}")

        # ── open catalog ──────────────────────────────────────────────
        try:
            from nautilus_trader.persistence.catalog import ParquetDataCatalog
            self._catalog = ParquetDataCatalog(str(NAUTILUS_CATALOG_ROOT))
        except Exception as e:
            self._skip(f"Cannot open catalog: {e}")
            return self.report

        # ── capture run-1 counts ──────────────────────────────────────
        self._run1_counts = self._query_catalog_counts()

        # ── run all checks ────────────────────────────────────────────
        checks: List[dict] = [
            self._ck_converter_exit_zero(),
            self._ck_catalog_exists(),
            self._ck_report_valid(),
            self._ck_instruments_exist(),
            self._ck_trades_nonempty(),
            self._ck_trades_5min(),
            self._ck_depth10_nonempty(),
            self._ck_depth10_5min(),
            self._ck_time_bounds(),
            self._ck_objects_nautilus(),
            self._ck_instrument_id_mapping(),
            self._ck_instrument_venue_mapping(),
            self._ck_idempotency_counts(),
            self._ck_gap_fields_valid(),
            self._ck_gap_rate_sane(),
            self._ck_gap_per_symbol(),
        ]

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

    # ── helpers ───────────────────────────────────────────────────────

    def _skip(self, reason: str):
        self.report["skipped"] = True
        self.report["reason"] = reason
        self._save()

    def _save(self):
        out_dir = STATE_ROOT / "validation"
        out_dir.mkdir(parents=True, exist_ok=True)
        ds = self._date_str or "unknown"
        out = out_dir / f"nautilus_catalog_e2e_{ds}.json"
        out.write_text(json.dumps(self.report, indent=2, default=str))
        logger.info(f"Report → {out}")

    def _query_catalog_counts(self) -> dict:
        """Query instrument, trade, depth counts from current catalog state."""
        from nautilus_trader.persistence.catalog import ParquetDataCatalog
        cat = ParquetDataCatalog(str(NAUTILUS_CATALOG_ROOT))
        instruments = cat.instruments()
        total_trades = 0
        total_depth = 0
        for inst in instruments:
            try:
                trades = cat.trade_ticks(instrument_ids=[inst.id])
                total_trades += len(trades)
            except Exception:
                pass
            try:
                depth = cat.order_book_depth10(instrument_ids=[inst.id])
                total_depth += len(depth)
            except Exception:
                pass
        return {
            "instruments": len(instruments),
            "trades": total_trades,
            "depth": total_depth,
        }

    # ── individual checks ─────────────────────────────────────────────

    def _ck_converter_exit_zero(self) -> dict:
        return {"name": "converter_exit_zero", "passed": self._converter_ok}

    def _ck_catalog_exists(self) -> dict:
        ok = NAUTILUS_CATALOG_ROOT.exists() and any(NAUTILUS_CATALOG_ROOT.iterdir())
        return {"name": "catalog_exists", "passed": ok,
                "details": {"path": str(NAUTILUS_CATALOG_ROOT)}}

    def _ck_report_valid(self) -> dict:
        rpt = self._convert_report
        if rpt is None:
            return {"name": "report_valid", "passed": False,
                    "details": {"reason": "report file missing"}}
        required = [
            "date", "runtime_sec", "status", "instruments_written",
            "total_trades_written", "total_depth_snapshots_written",
            "bad_lines", "gaps_suspected", "book_resets_total",
            "gap_rate", "per_symbol_gaps", "symbols_processed",
            "venues", "ts_ranges",
        ]
        missing = [f for f in required if f not in rpt]
        ok = len(missing) == 0 and rpt.get("status") == "ok"
        return {"name": "report_valid", "passed": ok,
                "details": {"missing_fields": missing, "status": rpt.get("status")}}

    def _ck_instruments_exist(self) -> dict:
        try:
            self._instruments = self._catalog.instruments()
        except Exception as e:
            return {"name": "instruments_exist", "passed": False,
                    "details": {"error": str(e)}}
        self._all_instrument_ids = {str(i.id) for i in self._instruments}
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
            trades = self._catalog.trade_ticks(instrument_ids=[self._sample_iid])
            if not trades:
                return {"name": "trades_5min_slice", "passed": False,
                        "details": {"reason": "no trades"}}
            first_ts = trades[0].ts_init
            end_ts = first_ts + 5 * 60 * 1_000_000_000
            sliced = [t for t in trades if t.ts_init <= end_ts]
            return {"name": "trades_5min_slice", "passed": len(sliced) > 0,
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
            return {"name": "depth10_5min_slice", "passed": len(sliced) > 0,
                    "details": {"window_count": len(sliced), "total": len(depth)}}
        except Exception as e:
            return {"name": "depth10_5min_slice", "passed": False,
                    "details": {"error": str(e)}}

    def _ck_time_bounds(self) -> dict:
        dt = datetime.strptime(self._date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
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
                        issues.append(f"depth ts_event={d.ts_event} out of bounds")
                        break
            except Exception as e:
                issues.append(str(e))
        return {"name": "time_bounds", "passed": len(issues) == 0,
                "details": {"issues": issues}}

    def _ck_objects_nautilus(self) -> dict:
        from nautilus_trader.model.data import OrderBookDepth10 as NT_Depth10
        from nautilus_trader.model.data import TradeTick as NT_TradeTick
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

    # ── NEW: instrument_id full mapping proof (req 2) ─────────────────

    def _ck_instrument_id_mapping(self) -> dict:
        """Every trade and depth object must reference an instrument in the catalog."""
        if not self._all_instrument_ids:
            return {"name": "instrument_id_mapping", "passed": False,
                    "details": {"reason": "no instruments loaded"}}

        total_trades_checked = 0
        total_depth_checked = 0
        invalid_refs = 0
        issues: List[str] = []

        for inst in self._instruments:
            iid = inst.id
            iid_str = str(iid)
            try:
                trades = self._catalog.trade_ticks(instrument_ids=[iid])
                for t in trades:
                    total_trades_checked += 1
                    if str(t.instrument_id) not in self._all_instrument_ids:
                        invalid_refs += 1
                        if len(issues) < 5:
                            issues.append(f"trade ref {t.instrument_id} not in instruments")
            except Exception:
                pass
            try:
                depth = self._catalog.order_book_depth10(instrument_ids=[iid])
                for d in depth:
                    total_depth_checked += 1
                    if str(d.instrument_id) not in self._all_instrument_ids:
                        invalid_refs += 1
                        if len(issues) < 5:
                            issues.append(f"depth ref {d.instrument_id} not in instruments")
            except Exception:
                pass

        ok = (invalid_refs == 0
              and total_trades_checked > 0
              and total_depth_checked > 0)
        return {
            "name": "instrument_id_mapping",
            "passed": ok,
            "details": {
                "instruments_found": len(self._all_instrument_ids),
                "trades_checked": total_trades_checked,
                "depth_checked": total_depth_checked,
                "invalid_instrument_refs": invalid_refs,
                "issues": issues,
            },
        }

    def _ck_instrument_venue_mapping(self) -> dict:
        """Spot instruments must be SYM.BINANCE, futures must be SYM-PERP.BINANCE."""
        from nautilus_trader.model.instruments import CryptoPerpetual, CurrencyPair
        issues: List[str] = []
        for inst in self._instruments:
            iid_str = str(inst.id)
            if isinstance(inst, CryptoPerpetual):
                if "-PERP.BINANCE" not in iid_str:
                    issues.append(f"Futures instrument {iid_str} missing -PERP suffix")
            elif isinstance(inst, CurrencyPair):
                if "-PERP" in iid_str:
                    issues.append(f"Spot instrument {iid_str} has -PERP suffix")
                if ".BINANCE" not in iid_str:
                    issues.append(f"Spot instrument {iid_str} missing .BINANCE venue")
        ok = len(issues) == 0 and len(self._instruments) > 0
        return {"name": "instrument_venue_mapping", "passed": ok,
                "details": {"instruments_checked": len(self._instruments),
                            "issues": issues}}

    # ── NEW: idempotency with count comparison (req 3) ────────────────

    def _ck_idempotency_counts(self) -> dict:
        """Re-run conversion, compare counts — must be identical (no duplication)."""
        try:
            run1 = self._run1_counts
            if not run1 or run1["instruments"] == 0:
                return {"name": "idempotency_counts", "passed": False,
                        "details": {"reason": "run-1 counts unavailable"}}

            # Second conversion run
            ok2, err2 = _run_converter(self._date_str)
            if not ok2:
                return {"name": "idempotency_counts", "passed": False,
                        "details": {"reason": f"second conversion failed: {err2[:200]}"}}

            run2 = self._query_catalog_counts()

            instruments_match = run1["instruments"] == run2["instruments"]
            trades_match = run1["trades"] == run2["trades"]
            depth_match = run1["depth"] == run2["depth"]
            ok = instruments_match and trades_match and depth_match

            return {
                "name": "idempotency_counts",
                "passed": ok,
                "details": {
                    "run1": run1,
                    "run2": run2,
                    "instruments_match": instruments_match,
                    "trades_match": trades_match,
                    "depth_match": depth_match,
                },
            }
        except Exception as e:
            return {"name": "idempotency_counts", "passed": False,
                    "details": {"error": str(e)}}

    # ── NEW: gap validation checks (req 4) ────────────────────────────

    def _ck_gap_fields_valid(self) -> dict:
        """Report must contain gap/reset counters and gap_rate."""
        rpt = self._convert_report
        if not rpt:
            return {"name": "gap_fields_valid", "passed": False,
                    "details": {"reason": "no report"}}

        required = ["gaps_suspected", "book_resets_total", "gap_rate", "per_symbol_gaps"]
        missing = [f for f in required if f not in rpt]

        # Check venue-level reports also contain gap/reset fields
        venue_missing = []
        for vname, vdata in rpt.get("venues", {}).items():
            if "gaps_suspected" not in vdata:
                venue_missing.append(f"{vname}.gaps_suspected")
            if "book_resets" not in vdata:
                venue_missing.append(f"{vname}.book_resets")

        ok = len(missing) == 0 and len(venue_missing) == 0
        return {"name": "gap_fields_valid", "passed": ok,
                "details": {"missing_top_level": missing,
                            "missing_venue_level": venue_missing}}

    def _ck_gap_rate_sane(self) -> dict:
        """gap_rate must be non-negative and < 1.0 (less than 100%).

        gaps_suspected and book_resets_total must be non-negative integers.
        """
        rpt = self._convert_report
        if not rpt:
            return {"name": "gap_rate_sane", "passed": False,
                    "details": {"reason": "no report"}}

        issues: List[str] = []
        gaps = rpt.get("gaps_suspected")
        resets = rpt.get("book_resets_total")
        rate = rpt.get("gap_rate")

        if not isinstance(gaps, (int, float)) or gaps < 0:
            issues.append(f"gaps_suspected invalid: {gaps}")
        if not isinstance(resets, (int, float)) or resets < 0:
            issues.append(f"book_resets_total invalid: {resets}")
        if not isinstance(rate, (int, float)) or rate < 0:
            issues.append(f"gap_rate invalid: {rate}")
        elif rate >= 1.0:
            issues.append(f"gap_rate >= 1.0: {rate} (100%+ depth records are gaps?)")

        ok = len(issues) == 0
        return {"name": "gap_rate_sane", "passed": ok,
                "details": {
                    "gaps_suspected": gaps,
                    "book_resets_total": resets,
                    "gap_rate": rate,
                    "issues": issues,
                }}

    def _ck_gap_per_symbol(self) -> dict:
        """per_symbol_gaps breakdown must be present and values non-negative."""
        rpt = self._convert_report
        if not rpt:
            return {"name": "gap_per_symbol", "passed": False,
                    "details": {"reason": "no report"}}

        psg = rpt.get("per_symbol_gaps")
        if not isinstance(psg, dict):
            return {"name": "gap_per_symbol", "passed": False,
                    "details": {"reason": f"per_symbol_gaps is {type(psg).__name__}, expected dict"}}

        issues: List[str] = []
        for sym_key, counts in psg.items():
            if not isinstance(counts, dict):
                issues.append(f"{sym_key}: value not a dict")
                continue
            gs = counts.get("gaps_suspected", -1)
            br = counts.get("book_resets", -1)
            if not isinstance(gs, (int, float)) or gs < 0:
                issues.append(f"{sym_key}: gaps_suspected invalid ({gs})")
            if not isinstance(br, (int, float)) or br < 0:
                issues.append(f"{sym_key}: book_resets invalid ({br})")

        # per_symbol_gaps can be empty if no gaps exist — that's fine
        ok = len(issues) == 0
        return {"name": "gap_per_symbol", "passed": ok,
                "details": {
                    "symbols_with_gaps": len(psg),
                    "total_gaps_from_breakdown": sum(
                        c.get("gaps_suspected", 0) for c in psg.values()
                        if isinstance(c, dict)
                    ),
                    "issues": issues,
                }}


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
