# Scripts

Operational scripts for testing and running CryptoRecorder.

These are **thin operator wrappers only**. No importable business logic lives
here. All pipeline and validation commands use `python -m pipeline.*` or
`python -m validation.*`.

## Available Scripts

### smoke_test.py
Quick 3-minute raw recorder smoke test. It does not validate replay_store,
feature_store, or Nautilus catalog generation.

```bash
python scripts/smoke_test.py              # Default 3 minutes
python scripts/smoke_test.py --runtime 60 # 1 minute
```

### acceptance_test.py
Legacy converter acceptance test (recorder → `convert_day.py` → Nautilus
catalog validation). It does not exercise the replay_store → full_l2 catalog
path; for that, use `python -m validation.validate_catalog_equivalence
--profile full_l2` (validated on the ADAUSDT smoke, broader validation pending).

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
| Legacy converter check | `python scripts/acceptance_test.py` |
| Audit replay store | `python -m validation.audit_replay_store --date YYYY-MM-DD ...` |
| Audit feature store | `python -m validation.audit_feature_store --date YYYY-MM-DD ...` |
| Compare old vs new catalog | `python -m validation.validate_catalog_equivalence --date YYYY-MM-DD ...` |
| Build replay store | `python -m pipeline.build_replay_store --date YYYY-MM-DD ...` |
| Build feature store | `python -m pipeline.build_feature_store --date YYYY-MM-DD ...` |
| Generate catalog | `python -m pipeline.generate_catalog --date YYYY-MM-DD ...` |

## Output

All scripts write results to `state/`:

- `state/smoke_test.log` — Recorder output from smoke test
- `state/smoke_test_results.json` — Smoke test results
- `state/acceptance_test_results.json` — Full test results
