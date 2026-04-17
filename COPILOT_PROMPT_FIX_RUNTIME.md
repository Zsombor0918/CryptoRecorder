# Copilot Prompt: Complete CryptoRecorder Runtime Fixes

## Context

We have a Binance market data recorder (cryptofeed-based, WSL2, Python 3.12). 

**Current status:**
- ✅ Validation system created (import tests work 100%)
- ✅ Runtime validator created (reveals actual problems)
- ✅ Quick fixes applied (snapshot spam disabled, SortedDict exceptions removed)
- ❌ 3 critical runtime issues remain unfixed:
  1. HTTP 418 ban at startup (cryptofeed exchangeInfo fetch)
  2. Event loop shutdown mismatch (thread vs main loop)
  3. Futures symbol mapping broken (exchange class or format issue)

**Test results:**
- Old VALIDATE.py: 5/5 import checks pass ✓
- New validate_runtime.py: 2/5 execution checks pass ⚠️
- Root cause: cryptofeed integration issues, not code bugs

---

## Your Task: Fix All 3 Runtime Issues

### Issue 1: HTTP 418 Ban at Startup

**Problem:**
```
ERROR: BINANCE: Failed to parse symbol information: 418 Client Error: 
I'm a teapot for url: https://api.binance.com/api/v3/exchangeInfo
```

When `Binance(...).add_feed()` is called, cryptofeed internally calls `/api/v3/exchangeInfo`.
This request is triggering Binance to return 418 (IP banned status).

**Required fix:**

1. Add ban detection and graceful fallback:
   - If exchangeInfo returns 418 → log warning, but **do not crash**
   - Continue with WS-only recording (no exchangeInfo metadata)
   - Set backoff timer (10-30 min) to retry exchangeInfo

2. Implement in `setup_feeds()`:
   ```python
   try:
       feed_handler.add_feed(Binance(...))
   except Exception as e:
       if "418" in str(e):
           logger.warning(f"HTTP 418 ban detected - continuing with WS-only recording")
           # Add flag: exchangeinfo_available = False
           # Continue (do not re-raise)
       else:
           raise
   ```

3. Modify health_monitor.heartbeat to include:
   ```json
   {
     "exchangeinfo_available": false,
     "ban_detected": true,
     "Ban_recovery_ETA": "2026-04-17T12:50:00Z"
   }
   ```

4. Later: If needed, implement REST call with exponential backoff, but only after WS stabilizes.

---

### Issue 2: Event Loop Mismatch on Shutdown

**Problem:**
```
Process did not shutdown after SIGINT
# Or: ValueError: The future belongs to a different loop
# Or: RuntimeError: coroutine was never awaited
```

**Root cause:**
- FeedHandler runs in thread with its own loop (line 516 in recorder.py)
- StorageManager tasks run in main asyncio loop
- shutdown() tries to gather futures from both loops → crash

**Required fix:**

1. **Option A (Recommended): Run FeedHandler in main thread**
   - Check if cryptofeed supports `start_loop=False` or similar
   - Or use `concurrent.futures.ThreadPoolExecutor` with proper lifecycle

2. **Option B: Sync shutdown between loops**
   - Add explicit threading.Event to signal FeedHandler thread to stop:
   ```python
   feed_stop_event = threading.Event()
   
   def _run_feed_handler_blocking(feed_handler, stop_event):
       while not stop_event.is_set():
           feed_handler.run(timeout=1)  # If cryptofeed supports timeout
   
   async def shutdown():
       feed_stop_event.set()  # Signal thread
       feed_thread.join(timeout=5)  # Wait for thread
       # Then shutdown storage in main loop
       await storage_manager.shutdown()
   ```

3. **Test:** `python3 recorder.py` → run 30 sec → SIGINT → must exit code 0 with no errors

---

### Issue 3: Futures Symbol Mapping Broken

**Problem:**
```
ERROR: Failed to initialize Binance Futures: BTCUSDT is not supported on BINANCE_FUTURES
```

**Root cause:**
- Spot uses: `Symbol(base='BTC', quote='USDT')`
- Futures uses: raw string `'BTCUSDT'`
- Probably cryptofeed expects a different format/exchange class for Binance USDT-M perpetuals

**Required fix:**

1. **Verify cryptofeed class:**
   - Check cryptofeed docs: is it `BinanceFutures` or something else for Binance USDT-M?
   - Try: `python3 -c "from cryptofeed.exchanges import BinanceFutures; print(BinanceFutures.__doc__)"`

2. **Test symbol format directly:**
   ```python
   from cryptofeed.symbols import Symbol
   from cryptofeed.exchanges import BinanceFutures
   
   # Try both formats
   sym1 = Symbol('BTC', 'USDT')
   sym2 = 'BTCUSDT'
   
   # Check which one BinanceFutures accepts
   try:
       BinanceFutures.normalize_symbols([sym1])
   except:
       pass
   try:
       BinanceFutures.normalize_symbols([sym2])
   except:
       pass
   ```

3. **Fix in recorder.py line ~480:**
   ```python
   futures_symbols_filtered = filter_symbols(universe['BINANCE_USDTF'])
   
   # Try: convert to Symbol objects like Spot does
   futures_symbols = [Symbol(s.replace('USDT', ''), 'USDT') for s in futures_symbols_filtered]
   
   # OR: keep raw strings but verify cryptofeed accepts them
   # futures_symbols = futures_symbols_filtered
   ```

4. **Add validation:**
   ```python
   if len(futures_symbols) == 0:
       logger.warning("No valid Futures symbols - Futures feed disabled")
       # Continue without Futures
   else:
       # Add feed
   ```

---

## Implementation Order

### Step 1: Fix Issue #3 (Futures) - Easiest
- Edit line ~480 in recorder.py
- Test with small symbol count
- Log what format works

### Step 2: Fix Issue #1 (418 Ban) - Medium
- Wrap `add_feed()` calls in try/except
- Detect 418 and log gracefully
- Continue with WS-only if REST fails

### Step 3: Fix Issue #2 (Shutdown) - Hardest
- Refactor thread/loop coordination
- Add proper signal handling between threads
- Test SIGINT cleanup thoroughly

---

## Validation After Each Fix

After each fix, run:
```bash
python3 validate_runtime.py --duration 120 --symbols 3
```

Expected progression:
- After Fix #1: Still 418, but check logs for graceful handling
- After Fix #2: Better shutdown, but still 418 from startup
- After Fix #3: Full e2e success (if IP not banned)

---

## Final Validation Checklist

Once all 3 fixes are deployed:

```bash
# Check HTTP ban status (or wait if needed)
curl https://api.binance.com/api/v3/exchangeInfo 2>&1 | head -1

# Run full runtime validation
python3 validate_runtime.py --duration 300 --symbols 5

# Expected output:
# ✓ PASS   http_ban
# ✓ PASS   callback_exceptions
# ✓ PASS   file_creation
# ✓ PASS   heartbeat
# ✓ PASS   shutdown

# If all pass: Integration test with VALIDATE.py
python3 VALIDATE.py all
```

---

## Code Context Reference

**Relevant files:**
- `recorder.py` lines 451-495 (feed initialization)
- `recorder.py` lines 516-550 (feed handler threading)
- `recorder.py` lines 575-605 (shutdown sequence)
- `storage.py` lines 250-268 (storage shutdown)

**Key functions to modify:**
- `setup_feeds(universe)` → add try/except for 418
- `_run_feed_handler_blocking()` → add threading.Event coordination
- `shutdown()` → ensure no cross-loop futures

---

## Delivered Artifacts

✅ Already created for you:
1. `validate_runtime.py` - Real e2e validator
2. `validate_snapshot_policy.py` - Rate-limit safety
3. `RUNTIME_VALIDATION_REPORT.md` - Detailed analysis
4. `recorder.py` quick fixes (snapshot disabled, SortedDict removed)

❌ Still needed from you (Copilot):
1. Fix Futures symbol mapping (Issue #3)
2. Add graceful 418 handling (Issue #1)
3. Fix shutdown threading (Issue #2)

---

## Questions for You (Copilot)

Before implementing, confirm:
1. Should I keep FeedHandler in thread or move to main thread?
2. Should 418 errors be retried with backoff or left as one-time failure?
3. For Futures, is it BinanceFutures or a different class for USDT-M?

