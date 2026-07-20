# Validation

CryptoRecorder has a clear separation between validation, tests, and operational checks.

## Quick Reference

| What | Command | When |
|------|---------|------|
| Setup validation | `python validate.py` | After cloning/setup |
| Unit tests | `pytest tests/` | After code changes |
| Smoke test | `python scripts/smoke_test.py` | Verify recorder works |
| Full acceptance | `python scripts/acceptance_test.py` | Release readiness |
| Replay partition audit | `python -m validation.audit_replay_store` | After building `replay_store` |
| Old-vs-new semantic equivalence | `python -m validation.validate_catalog_equivalence` | Validate `replay_store` against `convert_day.py` |
| Change-compliance audit | `python -m validation.audit_change_compliance` | Before every commit (pre-commit hook) |

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
- Depth10 enabled by default
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

## Replay Store Validation (`validation/`)

These commands validate `replay_store` — the recorder + replay-store output
contract handed off to downstream repositories. They are non-mutating audits;
none of them modify `data_raw/`, production `replay_store`, or `/etc` files.

### Replay partition audit

Checks schema, sort order, checksum, and null-ratio invariants for one or more
already-built `replay_store` partitions:

```bash
python -m validation.audit_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --replay-root ./replay_store
```

Use `--symbols all` / `--venues all` to audit every partition present for a
date. Pass `--report-path` to write the JSON report to a file instead of
stdout.

### Old-vs-new semantic equivalence

Compares `replay_store` (rebuilt into a temporary Nautilus catalog via the
internal `validation.replay_catalog_reconstruct` helper — there is no
`generate_catalog` product CLI) against the legacy `convert_day.py` catalog
for the same date/symbol set:

```bash
python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --old-catalog-root /tmp/cryptorecorder-equivalence/old_catalog \
  --replay-root /tmp/cryptorecorder-equivalence/replay_store \
  --new-catalog-root /tmp/cryptorecorder-equivalence/new_catalog \
  --profile trades_only \
  --overwrite
```

`--profile` accepts `trades_only`, `full_l2`, `depth_only`, or `depth10`. The
`full_l2` profile is validated on the ADAUSDT single-day smoke; broader
top50/multi-day validation is pending — see
[FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md).

See [DAILY_BUILD_PIPELINE.md](DAILY_BUILD_PIPELINE.md) `## Local Testing
Workflow (temp-root smoke)` for the full end-to-end build-then-validate
recipe using temporary roots.

### Change-compliance audit

Enforces the mandatory change-audit rules in `AGENTS.md` Section 6 (run
automatically by the pre-commit hook on staged changes):

```bash
python -m validation.audit_change_compliance --staged   # staged changes only
python -m validation.audit_change_compliance --base main # full branch diff vs main
```

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
