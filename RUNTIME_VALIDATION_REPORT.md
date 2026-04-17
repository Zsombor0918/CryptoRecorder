# Runtime Validation Report - CryptoRecorder E2E Testing

**Date:** 2026-04-17  
**Status:** ⚠️ **CRITICAL ISSUES IDENTIFIED**

---

## Executive Summary

The validation system reveals **3 critical runtime issues** preventing correct operation:

| Issue | Severity | Status | Impact |
|-------|----------|--------|--------|
| **HTTP 418 Ban at Startup** | 🔴 CRITICAL | Unfixed | Recorder cannot fetch any data |
| **Event Loop Shutdown Mismatch** | 🔴 CRITICAL | Unfixed | Ungraceful termination, coroutine errors |
| **Futures Symbol Mapping** | 🔴 CRITICAL | Unfixed | Futures feed cannot initialize |
| ~~Snapshot REST Spam~~ | ⚠️ HIGH | **FIXED** | Disabled in this session |
| ~~SortedDict.items() Crash~~ | ⚠️ HIGH | **FIXED** | top_bid/top_ask removed |

---

## What Runtime Validator Actually Tests

The `validate_runtime.py` performs **real execution tests**, not just import checks:

1. **HTTP Ban Detection**
   - Searches logs for HTTP 429 (rate limited) and 418 (IP banned)
   - If present → FAIL (cannot operate)

2. **Callback Exception Monitoring**
   - Tracks if `on_l2_book` or `on_trade` throw exceptions
   - If exceptions > 0 → FAIL (data loss)

3. **File Creation Verification**
   - Checks if `data_raw/**/*.jsonl.zst` files are created
   - Validates files are non-empty
   - If not → FAIL (no data recorded)

4. **Heartbeat Consistency**
   - Verifies `state/heartbeat.json` updates at least twice
   - Confirms background monitoring runs

5. **Graceful Shutdown**
   - Sends SIGINT after test period
   - Verifies clean exit (code 0)
   - Checks for event loop / coroutine errors in logs

---

## Current Test Results

```
Runtime Validation (60 sec, 2 symbols)
├─ ✗ FAIL   HTTP 418 ban detected
├─ ✓ PASS   No callback exceptions  
├─ ✗ FAIL   No data files created
├─ ✓ PASS   Heartbeat updates correctly
└─ ✗ FAIL   Shutdown SIGINT not handled

Result: 2/5 checks passed (40%)
```

---

## Why Tests Fail

### Issue #1: HTTP 418 Ban at Startup (CRITICAL)

**What happens:**
```
ERROR: BINANCE: Failed to parse symbol information: 418 Client Error: 
I'm a teapot for url: https://api.binance.com/api/v3/exchangeInfo
```

**Root cause:**  
- When `Binance()` feed initializes, cryptofeed calls `/api/v3/exchangeInfo` internally
- This fetch is happening while a ban is already in place (likely from earlier test runs)
- Or the exchangeInfo fetch itself is somehow triggering the ban

**Impact:**
- Spot feed cannot initialize
- No L2_BOOK or TRADES channels subscribe
- No data files created

**Why the ban persists:**
- Binance IP bans last ~10-30 minutes
- Our multiple failed test runs in quick succession triggered the ban
- Need to either:
  - Wait for ban to expire, OR
  - Use a proxy/different IP, OR
  - Implement healthcheck to detect ban and backoff

### Issue #2: Event Loop Mismatch on Shutdown (CRITICAL)

**What happens:**
```
Process did not shutdown after SIGINT
```

**Root cause:**
- FeedHandler runs in separate thread with its own asyncio event loop
- StorageManager's writer tasks are in the main asyncio loop
- On SIGINT → shutdown tries to gather futures from different loops
- Cross-loop future operations fail

**Impact:**
- SIGINT doesn't gracefully flush writer queues
- Process hangs or requires kill -9
- Potential data loss

**Fix needed:**
- Either run feedhandler in main loop, OR
- Ensure shutdown doesn't mix thread loops and main loop futures

### Issue #3: Futures Feed Initialization Fails (CRITICAL)

**What happens:**
```
ERROR: Failed to initialize Binance Futures: BTCUSDT is not supported on BINANCE_FUTURES
```

**Root cause:**
- Symbol format mismatch between what we pass and what cryptofeed expects
- Or wrong exchange class for USDT-M perpetuals

**Impact:**
- Futures feed doesn't initialize
- Only spot is recorded

**Fix needed:**
- Verify correct cryptofeed exchange class for Binance USDT-M
- Verify symbol format (might need transformation)

---

## How to Use Runtime Validator

### Run 3-minute smoke test:
```bash
python3 validate_runtime.py --duration 180 --symbols 5
```

### Expected output (once issues are fixed):
```
✓ PASS   http_ban                  No HTTP bans detected
✓ PASS   callback_exceptions       No callback exceptions
✓ PASS   file_creation             5 data files created, total size: 2.4MB
✓ PASS   heartbeat                 heartbeat.json updates correctly
✓ PASS   shutdown                  Graceful shutdown - no errors

Result: 5/5 checks passed (100%)
Status: ✓ SUCCESS
```

### View report:
```bash
cat state/validation/runtime_<timestamp>.json | python3 -m json.tool
```

---

## Snapshot Policy Validation

The `validate_snapshot_policy.py` checks if snapshot strategy is rate-limit safe:

```bash
python3 validate_snapshot_policy.py
```

**Current config:**
- `TOP_SYMBOLS: 50`
- `SNAPSHOT_INTERVAL_SEC: 600`
- Estimated weight: 25/min (safe, well below 3000/min limit)

**But:** Snapshots are **DISABLED** now to prevent spam, since filesystem is already banned.

---

## Next Steps to Fix

### Immediate (Today)

1. **Check if Binance ban has expired** (10-30 min wait, or use different IP):
   ```bash
   curl https://api.binance.com/api/v3/exchangeInfo 2>&1 | head -5
   # If you get 418 teapot → still banned
   ```

2. **Run runtime validator again in 30 minutes:**
   ```bash
   python3 validate_runtime.py --duration 180 --symbols 3
   ```

### Medium-term (This week)

1. **Fix event loop shutdown:**
   - Restructure FeedHandler initialization to avoid cross-thread futures
   - Or use a simpler thread sync mechanism (threading.Event)

2. **Fix Futures symbol mapping:**
   - Verify correct cryptofeed exchange class
   - Test symbol format with cryptofeed directly

3. **Re-enable snapshots safely:**
   - Use rate limiter with token bucket
   - Implement gap-only snapshots by default

### Production Hardening

1. **Add ban detection:**
   ```python
   if "418" in response or "429" in response:
       enter_backoff_mode(duration=600)  # 10 min backoff
   ```

2. **Implement graceful degradation:**
   - If ban detected → stop REST calls, keep WS running
   - If WS broken → wait for recovery, don't spam

3. **Persistent heartbeat:**
   - Always write `state/heartbeat.json`
   - Include ban status and recovery ETA

---

## Validation Checklist for Production

Before deploying to production, run:

```bash
# 1. Policy check (before startup)
python3 validate_snapshot_policy.py  # Must show SAFE

# 2. Runtime test (with test config)
python3 validate_runtime.py --duration 300 --symbols 10  # Must pass 5/5

# 3. Converter E2E (optional, once data exists)
python3 validate_converter_e2e.py --date $(date +%Y-%m-%d)

# 4. All validators integrated
python3 VALIDATE.py all  # Must show GREEN
```

---

## How These Tests Differ from Import Validators

| Aspect | Old VALIDATE.py | New Runtime Validator |
|--------|-----------------|----------------------|
| **What it tests** | Can modules import? | Does it actually work? |
| **Data capture** | No | Yes (checks files created) |
| **Rate limits** | No | Yes (detects bans) |
| **Callback errors** | No | Yes (monitors exceptions) |
| **Shutdown** | No | Yes (verifies SIGINT cleanup) |
| **Heartbeat** | No | Yes (verifies updates) |
| **Execution time** | 30 seconds | 180+ seconds |
| **Real data** | No | Yes (creates actual files) |

---

## Files Modified This Session

| File | Change | Purpose |
|------|--------|---------|
| `recorder.py` | Disabled snapshot task + top_bid/ask | Prevent REST spam & SortedDict exceptions |
| `validate_runtime.py` | NEW | Real e2e validation tester |
| `validate_snapshot_policy.py` | NEW | Rate-limit safety checker |
| `VALIDATE.py` | (unchanged) | Can be integrated with new validators |

---

## Summary

✅ **Fixed:**
- Snapshot REST spam (disabled)
- SortedDict.items() callback exceptions (removed problematic code)

❌ **Still needs fixing:**
- HTTP 418 ban at startup (requires investigation + IP recovery wait)
- Event loop shutdown mismatch (requires refactoring)
- Futures symbol mapping (requires verification)

📊 **New validation:**
- `validate_runtime.py` - real smoke testing
- `validate_snapshot_policy.py` - rate-limit safety checking
- Can now distinguish between "code loads" and "code actually works"

🎯 **Next validation run:** After 30-min ban recovery, run:
```bash
python3 validate_runtime.py --duration 180 --symbols 3
# Expected: Should see data files and no bans if IP recovered
```

