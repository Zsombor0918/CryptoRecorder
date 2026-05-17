# Operations

## Quick Reference

| Task | Command |
|------|---------|
| Start recorder | `python recorder.py` |
| Convert a day | `python convert_day.py --date YYYY-MM-DD --staging` |
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

## Writer Backpressure

Recorder storage uses one writer queue per `venue/symbol/channel`.
`depth_v2` is protected first: by default it waits for writer capacity instead
of dropping records. `trade_v2` keeps a bounded timeout and may drop under
sustained pressure.

Important environment knobs:

```bash
CRYPTO_RECORDER_DEPTH_WRITER_QUEUE_MAX_SIZE=20000
CRYPTO_RECORDER_TRADE_WRITER_QUEUE_MAX_SIZE=5000
CRYPTO_RECORDER_WRITER_BATCH_SIZE=1000
CRYPTO_RECORDER_WRITER_FLUSH_INTERVAL_SEC=5.0
CRYPTO_RECORDER_DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC=0
CRYPTO_RECORDER_TRADE_WRITER_ENQUEUE_TIMEOUT_SEC=1.0
CRYPTO_RECORDER_DEPTH_BLOCK_WARN_INTERVAL_SEC=10.0
CRYPTO_RECORDER_DEPTH_BLOCK_ALERT_SEC=30.0
CRYPTO_RECORDER_WRITER_TELEMETRY_LOG_INTERVAL_SEC=60
CRYPTO_RECORDER_WRITER_COMPRESSION_WORKERS=1
```

Use `state/heartbeat.json` → `writer_queue_telemetry` to inspect pressure:

- `top_pressure_writers`: symbols/channels causing queue pressure
- `queue_size` / `queue_high_watermark`: current and peak backlog
- `drop_count`: lossy queue drops, normally trade-side
- `blocked` / `current_block_sec`: depth writers waiting for capacity
- `compression`: queued/active/completed/failed background compression work

Hourly rotation closes old files quickly and queues compression in the
background, so compression should not block the active ingest path. Any
compression failures are left as uncompressed `.jsonl` files and surfaced in
heartbeat telemetry.

## Conversion

```bash
# Convert yesterday UTC using the safe staged publish flow
python convert_day.py --staging

# Convert specific date using the safe staged publish flow
python convert_day.py --date 2026-04-20 --staging

# Direct non-staging conversion writes into the live catalog immediately
python convert_day.py --date 2026-04-20

# Enable optional derived depth10
python convert_day.py --date 2026-04-20 --staging --emit-depth10
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
