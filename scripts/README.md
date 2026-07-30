# Scripts

Operational scripts for testing and running CryptoRecorder.

These are **thin operator wrappers only**. No importable business logic lives
here. All pipeline and validation commands use `python -m pipeline.*` or
`python -m validation.*`.

## Available Scripts

### smoke_test.py
Quick 3-minute raw recorder smoke test. It does not validate replay_store or
Nautilus catalog generation.

```bash
python scripts/smoke_test.py              # Default 3 minutes
python scripts/smoke_test.py --runtime 60 # 1 minute
```

### acceptance_test.py
Legacy converter acceptance test (recorder → `convert_day.py` → Nautilus
catalog validation). It does not exercise the internal replay full-L2
reconstruction path; for that, use `python -m validation.validate_catalog_equivalence
--profile full_l2` (validated on the ADAUSDT smoke, broader validation pending).

```bash
python scripts/acceptance_test.py              # Full test (10 min recorder)
python scripts/acceptance_test.py --runtime 300 # 5 minute recorder
python scripts/acceptance_test.py --skip-recorder # Test converter only
```

### run_under_cgroup.sh

Runs one validation command in a transient user-systemd scope with an explicit
memory ceiling, zero swap, persistent sampled peak/`memory.events` evidence,
and no retry. Use a fresh unit name and output directory for every substantial
stage; existing evidence is never overwritten.

```bash
scripts/run_under_cgroup.sh 10G validation_runs/ada-trades cr-ada-trades -- \
  python -m validation.stage_runner_cli trades \
    --config validation_runs/ada-trades/config.json \
    --out validation_runs/ada-trades/result.json
```

The wrapper returns the wrapped command's exit code. A non-zero exit, OOM
event, missing fragment, or failed fragment stops a serial gate.

## When to Use What

| Scenario | Command |
|----------|---------|
| After cloning/setup | `python validate.py` |
| After code changes | `pytest tests/` |
| Verify recorder works | `python scripts/smoke_test.py` |
| Legacy converter check | `python scripts/acceptance_test.py` |
| Audit replay store | `python -m validation.audit_replay_store --date YYYY-MM-DD ...` |
| Compare old vs new catalog | `python -m validation.validate_catalog_equivalence --date YYYY-MM-DD ...` |
| Cap one semantic stage | `scripts/run_under_cgroup.sh 10G OUTPUT_DIR UNIT -- COMMAND...` |
| Build replay store | `python -m pipeline.build_replay_store --date YYYY-MM-DD ...` |
| Run daily build (replay-only) | `python -m pipeline.daily_build --date YYYY-MM-DD ...` |

## Output

The Python smoke and acceptance scripts write their operator results to
`state/`:

- `state/smoke_test.log` — Recorder output from smoke test
- `state/smoke_test_results.json` — Smoke test results
- `state/acceptance_test_results.json` — Full test results

`run_under_cgroup.sh` instead writes its memory samples, final
`memory.events`, and exit/peak summary to the output directory explicitly
supplied as its second argument. The wrapped command controls its own result
fragment and logs.
