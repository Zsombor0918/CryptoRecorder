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
- Dependencies installed (nautilus_trader, aiohttp, zstandard, etc.)
- Project structure (directories exist)
- Configuration loads correctly
- Core modules can be imported

## Unit Tests (`tests/`)

Run with pytest:

```bash
pytest tests/              # All tests
pytest tests/ -v           # Verbose output
pytest tests/ -x           # Stop on first failure
pytest tests/test_depth_deterministic.py   # Depth ordering/session tests
pytest tests/test_trade_deterministic.py   # Trade ordering/schema tests
pytest tests/test_converter_integration.py # Converter pipeline tests
```

Tests cover:
- Deterministic depth ordering by `(session_id, session_seq)`
- Committed-only session_seq allocation (no gaps from lifecycle/rejects)
- Futures U/u/pu continuity enforcement and fencing
- Reconnect session boundary handling
- Depth10 off by default
- Trade canonical ordering and aggressor mapping
- Lifecycle marker exclusion from TradeTick output
- Spot vs futures tagged union schema decoding
- Converter integration (trade_v2 → TradeTick, depth_v2 → OrderBookDeltas)
- convert_date report shape and catalog queryability
- REST-based futures support precheck
- Date-scoped catalog purging
- Heartbeat field coverage
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
- Raw files created (depth_v2 + trade_v2)
- Heartbeat written with `architecture: deterministic_native`
- No rate limit errors
- Clean shutdown

### Acceptance Test

Full pipeline test (recorder → converter → catalog):

```bash
python scripts/acceptance_test.py              # Full test (10 min)
python scripts/acceptance_test.py --runtime 300 # 5 minutes
python scripts/acceptance_test.py --skip-recorder # Test converter only
python scripts/acceptance_test.py --emit-depth10  # Also check derived depth10
```

Checks:
- Recorder works with 50 symbols (both depth_v2 and trade_v2 channels)
- Converter produces valid output with `architecture: deterministic_native`
- Catalog is queryable (instruments, OrderBookDeltas, TradeTick)
- Fenced ranges reported in convert report

## Reports

All validation/test results are saved to `state/`:

| File | Content |
|------|---------|
| `state/smoke_test_results.json` | Smoke test results |
| `state/acceptance_test_results.json` | Acceptance test results |
| `state/smoke_test.log` | Recorder output from smoke test |

## Quality Metrics

The deterministic native pipeline tracks these quality indicators:

| Metric | Where | Meaning |
|--------|-------|---------|
| `fenced_ranges_total` | Convert report | Ranges excluded from deterministic replay |
| `desync_events` | Convert report / heartbeat | Times continuity was lost |
| `resync_count` | Convert report / heartbeat | Successful re-synchronizations |
| `snapshot_seed_count` | Convert report | REST snapshots used to seed replay |
| `queue_drop_total` | Heartbeat | WebSocket messages dropped due to backpressure |
| `instruments_with_no_data` | Convert report | Instruments defined but missing raw data |
