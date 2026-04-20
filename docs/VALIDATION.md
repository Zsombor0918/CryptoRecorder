# Validation

`VALIDATE.py` is the master validation entrypoint.

Preferred validator modules now use the short names under `validators/`:

- `validators/system.py`
- `validators/runtime.py`
- `validators/scale.py`
- `validators/nautilus_catalog.py`
- `validators/purge_safety.py`
- `validators/converter.py`

Legacy `validate_*.py` filenames are still present so older commands and
references do not break.

## Modes

```bash
python VALIDATE.py system
python VALIDATE.py runtime
python VALIDATE.py scale
python VALIDATE.py nautilus
python VALIDATE.py purge
python VALIDATE.py converter
python VALIDATE.py all
python VALIDATE.py accept
```

## Intent by mode

- `system`: local environment and structure sanity
- `runtime`: live recorder smoke validation
- `scale`: heavier live acceptance run
- `nautilus`: conversion + catalog checks
- `purge`: date-scoped purge safety checks
- `converter`: legacy alias that delegates to `nautilus`
- `all`: quick multi-check suite
- `accept`: broader acceptance suite

## Reports

Validation outputs are written under `state/`, including per-mode reports and
an aggregated master report.

## Notes

- Keep validation expectations aligned with current Phase 1 behavior.
- Avoid relying on fragile exact check counts in external docs.
- Treat `runtime` as tolerant of graceful degradation, and `scale` as the stricter operational gate.
