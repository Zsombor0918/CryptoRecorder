# Tests

Unit tests for CryptoRecorder. Run with pytest.

## Running Tests

Activate the project virtualenv first, then prefer `python -m pytest` so the
tests run with the same interpreter that has the project dependencies installed.

```bash
# All tests
python -m pytest tests/

# Specific test file
python -m pytest tests/test_depth_deterministic.py

# With verbose output
python -m pytest tests/ -v

# Stop on first failure
python -m pytest tests/ -x
```

## Test Files

| File | What it tests |
|------|---------------|
| `test_depth_deterministic.py` | Deterministic depth ordering, session_seq, futures continuity, depth10 default |
| `test_trade_deterministic.py` | Trade canonical ordering, aggressor mapping, lifecycle exclusion, tagged union schema |
| `test_converter_integration.py` | End-to-end converter pipeline (trade_v2 → TradeTick, depth_v2 → OrderBookDeltas) |
| `test_convert_day_phase2.py` | convert_date report shape and catalog output |
| `test_futures_support_precheck.py` | REST-based futures exchange info validation |
| `test_purge_date_scoped.py` | Catalog date-scoped purge logic |
| `test_heartbeat_coverage_fields.py` | Heartbeat coverage field consistency |
| `test_universe_resolution.py` | Universe file parsing |
| `test_universe_selection_quality.py` | Symbol selection logic |

## Writing Tests

Tests should:
1. Be self-contained (use `tmp_path` fixture or temp directories)
2. Not require network access
3. Not require the recorder to be running
4. Use monkeypatch for module-level dependencies

## Operational Tests

For live recorder/converter tests, use `scripts/` instead:
- `scripts/smoke_test.py` — Quick 3-minute recorder test
- `scripts/acceptance_test.py` — Full pipeline test
