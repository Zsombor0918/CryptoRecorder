# Scripts

Operational scripts for testing and running CryptoRecorder.

## Available Scripts

### smoke_test.py
Quick 3-minute recorder smoke test. Use after making changes to verify nothing broke.

```bash
python scripts/smoke_test.py              # Default 3 minutes
python scripts/smoke_test.py --runtime 60 # 1 minute
```

### acceptance_test.py
Full pipeline acceptance test (recorder → converter → catalog validation).

```bash
python scripts/acceptance_test.py              # Full test (10 min recorder)
python scripts/acceptance_test.py --runtime 300 # 5 minute recorder
python scripts/acceptance_test.py --skip-recorder # Test converter only
```

## When to Use What

| Scenario | Command |
|----------|---------|
| After cloning/setup | `python validate.py` |
| After code changes | `pytest tests/` |
| Verify recorder works | `python scripts/smoke_test.py` |
| Full pipeline check | `python scripts/acceptance_test.py` |

## Output

All scripts write results to `state/`:

- `state/smoke_test.log` — Recorder output from smoke test
- `state/smoke_test_results.json` — Smoke test results
- `state/acceptance_test_results.json` — Full test results
