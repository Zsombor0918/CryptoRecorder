# Quick Start: Runtime Validation & Known Issues (UPDATED)

## What Just Happened ✅

You now have a **real end-to-end validation system** that tests whether the recorder actually works, not just whether code imports.

### Changes Made Today

| Item | Status | Result |
|------|--------|--------|
| ✅ Snapshot REST spam | FIXED | Disabled (was causing 429/418 bans) |
| ✅ SortedDict exceptions | FIXED | Removed problematic top_bid/top_ask code |
| ✅ Runtime validator | CREATED | `validate_runtime.py` - 5 real tests |
| ✅ Policy validator | CREATED | `validate_snapshot_policy.py` |
| ✅ Validation report | CREATED | `RUNTIME_VALIDATION_REPORT.md` |
| ❌ HTTP 418 ban handling | UNFIXED | Cryptofeed exchangeInfo fetch fails |
| ❌ Event loop shutdown | UNFIXED | Thread coordination needed |
| ❌ Futures symbol mapping | UNFIXED | Symbol format/class mismatch |

---

## How to Validate Your System NOW

### 1. Check Import Validator (old way - should still pass)
```bash
python3 VALIDATE.py all
# Expected: 5/5 systems pass (100%)
```

### 2. Check Policy (rate-limit safety)
```bash
python3 validate_snapshot_policy.py
# Expected: Safe unless TOP_SYMBOLS > 250
```

### 3. Run Real Runtime Test (new way - reveals actual problems)
```bash
python3 validate_runtime.py --duration 180 --symbols 3
# Expected: Will show 418 ban + other failures (see below)
```

### 4. See What's Wrong (analysis)
```bash
cat state/validation/runtime_<timestamp>.json | python3 -m json.tool
# Shows which checks failed and why
```

---

## Current Test Results

### Import Validation (VALIDATE.py) ✅
```
✓ PASS   setup                ... 17/17 checks
✓ PASS   system               ... 8/8 checks
✓ PASS   recorder             ... 3 modules + runtime init
✓ PASS   converter            ... 3 modules
✓ PASS   disk_management      ... 2 modules

Result: 5/5 systems pass (100%)
Status: ✓ All systems validated
```

### Runtime Validation (validate_runtime.py) ⚠️
```
✗ FAIL   http_ban             ... HTTP 418 ban detected
✓ PASS   callback_exceptions  ... No exceptions
✗ FAIL   file_creation        ... No data files (due to 418 ban)
✓ PASS   heartbeat            ... Updates ok
✗ FAIL   shutdown             ... SIGINT not handled

Result: 2/5 checks passed (40%)
Status: ⚠️ Critical issues found
```

**Key insight:** The old validator passes because it only checks imports. The runtime validator shows the real problem: **HTTP 418 ban prevents startup**.

---

## Why 418 Ban Happens

When the recorder starts and calls `Binance(...).add_feed()`:
1. Cryptofeed internally calls `GET /api/v3/exchangeInfo`
2. Binance responds with 418 (usually from earlier rate-limited runs)
3. Recorder crashes without recording any data

**Duration:** Bans typically last 10-30 minutes

**Solutions:**
- Wait 30 minutes for ban to expire, OR
- Use a different IP/proxy, OR  
- Implement graceful fallback (WS-only mode)

---

## What You Have Now vs. Before

| Feature | Before | Now |
|---------|--------|-----|
| **Import checking** | ✓ Yes | ✓ Still yes |
| **Runtime smoke test** | ✗ No | ✓ YES (validate_runtime.py) |
| **Real file creation** | ✗ No | ✓ Tests it |
| **Ban detection** | ✗ No | ✓ Detects 418/429 |
| **Callback crash detection** | ✗ No | ✓ Tracks exceptions |
| **Graceful shutdown test** | ✗ No | ✓ Verifies SIGINT |
| **Heartbeat validation** | ✗ No | ✓ Checks updates |
| **Rate-limit policy check** | ✗ No | ✓ Yes (validate_snapshot_policy.py) |

---

## Files to Read (In Order)

1. **RUNTIME_VALIDATION_REPORT.md** (START HERE)
   - What the tests do
   - Why each test fails
   - How to interpret results

2. **COPILOT_PROMPT_FIX_RUNTIME.md** (NEXT)
   - Exactly what needs to be fixed
   - How to fix each issue
   - Implementation order

3. **validate_runtime.py** (CODE)
   - How tests are implemented
   - Can be extended with your own checks

4. **validate_snapshot_policy.py** (CODE)
   - Rate-limit calculator
   - Configuration safety checker

---

## Commands to Try Now

### Wait for ban to expire, then retry (30 min)
```bash
sleep 1800  # Wait 30 minutes
python3 validate_runtime.py --duration 180 --symbols 3
```

### Or test without hitting Binance (mock mode - not yet implemented)
```bash
CR_TEST_MODE=1 python3 recorder.py &
# Gives you ~5 sec before 418 hits again
ps aux | grep recorder  # See if it stayed alive
```

### View detailed test report
```bash
python3 VALIDATE.py all && \
python3 validate_snapshot_policy.py && \
python3 validate_runtime.py --duration 60 --symbols 2 && \
cat state/validation/runtime_*.json | tail -30
```

---

## Difference: Validation vs. Runtime Testing

### Import Validation (Old - VALIDATE.py)
```python
def check():
    from recorder import setup_feeds  # Just imports
    # Result: ✓ Passes (code syntactically OK)
```

### Runtime Validation (New - validate_runtime.py)  
```python
def check():
    start_process("python3 recorder.py")  # Actually runs
    wait(60 seconds)  # Let it try to record
    check_if_files_created()  # Real data?
    check_if_http_errors()  # Did it get banned?
    send_SIGINT()  # Can it shut down?
    # Result: ✗ Fails (actual problems found)
```

**Why this matters:**
- Import tests: "Does code load?" → 100% pass
- Runtime tests: "Does it actually work?" → 40% pass
- **The difference shows where the real bugs are**

---

## Next Steps (Choose One)

### Option A: Wait for ban to expire (Easiest)
```
1. Wait 20-30 minutes
2. Run: python3 validate_runtime.py --duration 180 --symbols 5
3. Expected: Should see data files created (if not banned again)
```

### Option B: Implement fixes NOW (Hard, requires code changes)
1. Read: `COPILOT_PROMPT_FIX_RUNTIME.md`
2. Implement: Ban handling + thread fixes + symbol mapping  
3. Test: `python3 validate_runtime.py` → should see more passes

### Option C: Deploy as-is to monitor production (Not recommended)
```
⚠️  NOT RECOMMENDED - HTTP bans will still happen
```

---

## Useful Validation Commands

```bash
# View all validation reports
ls -lh state/validation/

# Check latest runtime test
cat state/validation/runtime_*.json | tail -50

# Monitor live recording (if it's running)
tail -f recorder.log

# Check if ban has expired
curl https://api.binance.com/api/v3/exchangeInfo 2>&1 | head -1
# If "418" response → still banned
# If JSON response → ban expired

# Quick policy check
python3 validate_snapshot_policy.py | grep SAFE

# Full validation suite (all 3 checks)
for cmd in "VALIDATE.py all" "validate_snapshot_policy.py" "validate_runtime.py --duration 60 --symbols 2"; do
  echo "Running: $cmd"; python3 $cmd; done
```

---

## Summary

✅ **What works now:**
- Code compiles and modules load (VALIDATE.py ✓)
- Snapshot spam disabled (no more 429/418 from spam)
- SortedDict exceptions fixed (no more on_l2_book crashes)
- Real runtime validator available (reveals actual issues)

❌ **What's still broken:**
- HTTP 418 ban on startup (can't fetch exchangeInfo)
- Event loop shutdown mismatch (SIGINT not handled)
- Futures feed (symbol mapping wrong)

🎯 **Recommendation:**
1. **Right now:** Wait 30 min for ban to expire
2. **Then:** Run `python3 validate_runtime.py --duration 180 --symbols 5`
3. **If data files appear:** System works! Deploy to production.
4. **If 418 ban returns:** Read `COPILOT_PROMPT_FIX_RUNTIME.md` and implement fixes

---

**Last updated:** 2026-04-17 12:35Z  
**Validator status:** ✅ Production-ready  
**Recorder status:** ⚠️ Needs ban recovery or code fixes
