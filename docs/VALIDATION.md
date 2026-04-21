# Validation

CryptoRecorder has a clear separation between validation, tests, and operational checks.

## Quick Reference

| What | Command | When |
|------|---------|------|
| Setup validation | `python validate.py` | After cloning/setup |
| Unit tests | `pytest tests/` | After code changes |
| Smoke test | `python scripts/smoke_test.py` | Verify recorder works |
| Full acceptance | `python scripts/acceptance_test.py` | Release readiness |

## Setup Validation (`validate.py`)

Run this after cloning the repo or setting up on a new machine:

```bash
python validate.py          # Full validation
python validate.py --quick  # Quick dependency check only
```

Checks:
- Python version (3.10+)
- Dependencies installed (cryptofeed, nautilus_trader, etc.)
- Project structure (directories exist)
- Configuration loads correctly
- Core modules can be imported

## Unit Tests (`tests/`)

Run with pytest:

```bash
pytest tests/           # All tests
pytest tests/ -v        # Verbose output
pytest tests/ -x        # Stop on first failure
pytest tests/test_bookbuilder.py  # Specific file
```

Tests cover:
- Book reconstruction logic
- Crossed-book handling
- Date-scoped catalog purging
- Heartbeat fields
- Universe resolution

## Operational Scripts (`scripts/`)

### Smoke Test

Quick 3-minute recorder test:

```bash
python scripts/smoke_test.py              # 3 minutes
python scripts/smoke_test.py --runtime 60 # 1 minute
```

Checks:
- Recorder starts and runs
- Raw files are created
- Heartbeat is written
- No rate limit errors
- Clean shutdown

### Acceptance Test

Full pipeline test (recorder → converter → catalog):

```bash
python scripts/acceptance_test.py              # Full test (10 min)
python scripts/acceptance_test.py --runtime 300 # 5 minutes
python scripts/acceptance_test.py --skip-recorder # Test converter only
python scripts/acceptance_test.py --depth-mode phase2 --skip-recorder
```

Checks:
- Recorder works with 50 symbols
- Converter produces valid output
- Catalog is queryable
- No crossed-book snapshots
- In Phase 2 mode, `OrderBookDeltas` are queryable and fenced ranges are reported

## Reports

All validation/test results are saved to `state/`:

| File | Content |
|------|---------|
| `state/smoke_test_results.json` | Smoke test results |
| `state/acceptance_test_results.json` | Acceptance test results |
| `state/smoke_test.log` | Recorder output from smoke test |

## Quality Thresholds

Phase 1 enforces these thresholds:

| Metric | Threshold | Meaning |
|--------|-----------|---------|
| crossed_rate | < 0.1% | Crossed events very rare |
| queue_drops | 0 | No drops in normal operation |
| rate_limit_hits | 0 | No 429/418 errors |

## Crossed-Book Handling

Crossed-book detection is a first-class quality concern:

1. **During conversion:** When the reconstructed book becomes crossed (best_bid >= best_ask),
   the converter resets the book and does NOT emit a crossed snapshot.

2. **In reports:** The converter report includes:
   - `crossed_book_events_total`: Total crossed events
   - `crossed_rate`: crossed_events / total_depth_snapshots

3. **Final guarantee:** The catalog contains no crossed-book snapshots.
