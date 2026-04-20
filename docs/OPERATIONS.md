# Operations

## Common commands

```bash
source .venv/bin/activate
python recorder.py
python VALIDATE.py all
python convert_day.py --date YYYY-MM-DD
```

## Service mode

Systemd units are in `systemd/`.

```bash
sudo systemctl restart cryptofeed-recorder
sudo systemctl status cryptofeed-recorder
journalctl -u cryptofeed-recorder -f
```

## Important runtime files

- `state/heartbeat.json`
- `state/startup_coverage.json`
- `state/convert_reports/YYYY-MM-DD.json`
- `state/master_validation_report.json`
- `recorder.log`

## Coverage terminology

Startup and runtime reporting uses these terms:

- `candidate_pool`: ranked symbols considered for a venue
- `pre_filter_rejected`: symbols rejected before recorder startup
- `selected`: symbols passed from universe selection into startup
- `runtime_dropped`: selected symbols that fail during feed initialization
- `active`: symbols successfully recording

## Failure handling expectations

- Unsupported symbols should be logged and skipped.
- Startup should continue with survivors.
- Futures may be degraded if support is limited, but recorder should stay up.
- Approximate L2 remains expected in Phase 1; deterministic replay is not promised.

## Conversion operations

Primary CLI:

```bash
python convert_day.py --date YYYY-MM-DD [--staging]
```

Compatibility CLI (legacy name):

```bash
python convert_yesterday.py --date YYYY-MM-DD [--staging]
```

## Validation operations

`VALIDATE.py` is the stable front door:

```bash
python VALIDATE.py system
python VALIDATE.py runtime
python VALIDATE.py nautilus
python VALIDATE.py all
```

Preferred validator module names are the short files under `validators/`.
Legacy `validate_*.py` files remain for compatibility.
