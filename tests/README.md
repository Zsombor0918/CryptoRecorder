# Tests

Unit tests for CryptoRecorder. Run with pytest.

## Running Tests

```bash
# All tests
pytest tests/

# Specific test file
pytest tests/test_bookbuilder.py

# With verbose output
pytest tests/ -v

# Stop on first failure
pytest tests/ -x
```

## Test Files

| File | What it tests |
|------|---------------|
| `test_bookbuilder.py` | BookBuilder class and crossed-book handling |
| `test_depth_reconstruction_phase1.py` | L2 book reconstruction from deltas |
| `test_purge_date_scoped.py` | Catalog date-scoped purge logic |
| `test_heartbeat_coverage_fields.py` | Heartbeat coverage field consistency |
| `test_universe_resolution.py` | Universe file parsing |
| `test_universe_selection_quality.py` | Symbol selection logic |
| `test_futures_support_precheck.py` | Futures symbol support checking |
| `test_spot_feed_resilience.py` | Spot feed error handling |

## Test Fixtures

Test fixtures are in `tests/fixtures/`:
- `crossed_depth_musdt_perp.jsonl` — Example crossed book data for testing

## Writing Tests

Tests should:
1. Be self-contained (use temp directories, clean up after)
2. Not require network access
3. Not require the recorder to be running
4. Use fixtures for test data

Example:
```python
def test_something() -> None:
    tmpdir = Path(tempfile.mkdtemp())
    try:
        # Test logic here
        assert result == expected
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
```

## Operational Tests

For live recorder/converter tests, use `scripts/` instead:
- `scripts/smoke_test.py` — Quick recorder test
- `scripts/acceptance_test.py` — Full pipeline test
