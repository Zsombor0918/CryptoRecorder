# Operations

## Quick Reference

| Task | Command |
|------|---------|
| Start recorder | `python recorder.py` |
| Convert a day | `python convert_day.py --date YYYY-MM-DD` |
| Setup validation | `python validate.py` |
| Run tests | `pytest tests/` |
| Smoke test | `python scripts/smoke_test.py` |
| Full acceptance | `python scripts/acceptance_test.py` |

## Service Mode

Systemd units are in `systemd/`.

```bash
# Control recorder service
sudo systemctl start crypto-recorder
sudo systemctl stop crypto-recorder
sudo systemctl restart crypto-recorder
sudo systemctl status crypto-recorder

# View logs
journalctl -u crypto-recorder -f
```

## Important Runtime Files

| File | Description |
|------|-------------|
| `state/heartbeat.json` | Live recorder status (architecture=deterministic_native) |
| `state/startup_coverage.json` | Startup symbol coverage |
| `state/convert_reports/YYYY-MM-DD.json` | Conversion reports |
| `recorder.log` | Recorder log file |

Report timestamps use Hungary local time (`Europe/Budapest`).
Day-scoped dates in file names remain UTC.

## Coverage Terminology

Startup and runtime reporting uses these terms:

- `candidate_pool`: ranked symbols considered for a venue
- `pre_filter_rejected`: symbols rejected before recorder startup
- `selected`: symbols passed from universe selection into startup
- `runtime_dropped`: selected symbols that fail during feed initialization
- `active`: symbols successfully recording

## Failure Handling

- Unsupported symbols are logged and skipped
- Startup continues with surviving symbols
- Futures support is validated via REST exchangeInfo
- Depth sync lifecycle handles reconnects deterministically (desync → resync)

## Conversion

```bash
# Convert yesterday (default)
python convert_day.py

# Convert specific date
python convert_day.py --date 2026-04-20

# Convert with staging (atomic rename on success)
python convert_day.py --date 2026-04-20 --staging

# Enable optional derived depth10
python convert_day.py --date 2026-04-20 --emit-depth10
```

## Validation & Testing

```bash
# Check setup (run on new machine)
python validate.py

# Run unit tests
pytest tests/

# Quick recorder test (3 minutes)
python scripts/smoke_test.py

# Full pipeline test (10 minutes)
python scripts/acceptance_test.py
```

See [VALIDATION.md](VALIDATION.md) for details.
