# Implementation Audit

## Summary

This repo currently has a validated v0 replay/feature foundation.
It does not yet have validated `replay_store -> full_l2 Nautilus catalog` generation.
Old `convert_day.py` remains the validated full-L2 path.

Current paths:

```text
data_raw -> convert_day.py -> Nautilus catalog
  validated full-L2 path

data_raw -> replay_store
  implemented v0

replay_store -> feature_store
  implemented v0, UTC-day clamped, sparse windows

replay_store -> generate_catalog --profile trades_only
  implemented and semantically validated for a real ADAUSDT spot day

replay_store -> generate_catalog --profile full_l2
  deferred
```

## What Works

Replay store:

- Builds Hive-style partitions under `venue=.../symbol=.../date=...`.
- Writes `depth.parquet`, `trades.parquet`, `instrument.json`, and `manifest.json`.
- Sorts depth records by `(stream_session_id, session_seq, raw_index)`.
- Sorts trade records by `(trade_stream_session_id, trade_session_seq, raw_index)`.
- Preserves exact trade `price_str` / `quantity_str`.
- Preserves exact depth level `price_str` / `size_str`.
- Preserves depth continuity fields `U`, `u`, and `pu` where raw provides them.
- Writes SHA256 checksums after Parquet staging files are written.
- Publishes completed staging directories atomically.
- Supports custom temp roots through `stream_raw_records(..., root=...)`.

Feature store:

- Writes one Parquet file per timeframe/venue/symbol/date.
- Supports `100ms`, `1s`, and `1m` windows.
- Defaults to UTC-day clamping: `[date 00:00:00 UTC, next day 00:00:00 UTC)`.
- Uses sparse output: empty windows are skipped.
- Computes current core v1 fields from latest depth and trades inside each window.
- Leaves simplified/deferred fields null where not implemented.

Catalog generation:

- `generate_catalog` supports `trades_only`.
- It uses exact replay strings for Nautilus `Price` and `Quantity`.
- It supports deterministic `--job-id` and safe `--overwrite`.
- It rejects non-implemented profiles by CLI choices.

Validation:

- `pipeline.validate_catalog_equivalence` builds old and new catalogs and compares semantic TradeTick equality.
- It compares instrument IDs, counts, timestamp ranges, first/last/sample trades, price, size, side, `trade_id`, `ts_event`, and `ts_init`.
- It writes JSON reports and exits nonzero on failed comparison.

## Tested

Automated tests cover:

- CLI help without touching unwritable default `/data` roots.
- Custom raw root reading.
- Replay manifest counts and sorted records.
- Exact replay decimal preservation.
- Depth `U/u/pu` preservation.
- `generate_catalog` exclusive end behavior.
- Nautilus readability for generated trades-only catalogs.
- Deterministic `--job-id` and `--overwrite`.
- Synthetic trades-only catalog semantic comparison.
- Feature UTC-day clamp and sparse output.
- Feature audit report fields.
- Explicit skipped full-L2 validation status.
- Real-data equivalence behind `pytest.mark.realdata`.

Last full local suite:

```text
172 passed, 5 skipped
```

## Smoke-Tested

Real local validation was run for:

```text
date: 2026-06-12
venue: BINANCE_SPOT
symbol: ADAUSDT
profile: trades_only
```

Result:

```text
status: passed
instrument_ids_match: true
old trades: 124457
new trades: 124457
timestamp_range_match: true
sample_mismatches: 0
```

Feature audit for `2026-06-12`, `BINANCE_SPOT/ADAUSDT`, `1m`:

```text
actual_row_count: 1428
expected_dense_row_count: 1440
outside_date_rows: 0
duplicate_timestamp_count: 0
missing_windows_count_if_dense: 12
all-null examples: return_1s, return_5s, return_10s, return_30s, return_1m
```

This confirms the feature store is UTC-day clamped and sparse. It does not force a dense full-day grid.

Report path from the local run:

```text
/tmp/cryptorecorder-equivalence-smoke-20260616/catalog_equivalence_2026-06-12.json
```

## Deferred

Replay-based full-L2 catalog generation is not implemented.

Deferred path:

```text
data_raw -> replay_store -> generate_catalog --profile full_l2
```

Future full-L2 validation must compare:

- instruments;
- TradeTick count and sampled equality;
- OrderBookDeltas count;
- first, last, and sampled deltas;
- action, side, price, size, order_id, flags, and sequence where applicable;
- reconstructed book checkpoints;
- top 10 bid/ask equality at checkpoints;
- gap/fenced range report equality or documented acceptable differences;
- optional OrderBookDepth10 semantic equality if emitted.

## Known Limitations

- `ReplayWriter` still accumulates one symbol/date in memory before writing.
- Feature aggregation still loads one symbol/date of replay depth/trade records into memory.
- Feature output is sparse, not dense; missing windows are expected unless dense mode is implemented later.
- Return and volatility fields are mostly simplified/null in v0.
- OFI/order-pressure advanced fields are deferred.
- The real-data equivalence test is gated by environment variables and is not part of normal CI.

## Reproduce One-Day Smoke

```bash
BASE=/tmp/cryptorecorder-replay-feature-validation
rm -rf "$BASE"
mkdir -p "$BASE"

python -m pipeline.build_replay_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --data-root ./data_raw \
  --replay-root "$BASE/replay_store"

python -m pipeline.build_feature_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --timeframes 1m \
  --replay-root "$BASE/replay_store" \
  --feature-root "$BASE/feature_store"

python -m pipeline.generate_catalog \
  --input "$BASE/replay_store" \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --start 2026-06-12T00:00:00Z \
  --end 2026-06-13T00:00:00Z \
  --output "$BASE/catalog_jobs" \
  --profile trades_only \
  --job-id validation_new \
  --overwrite
```

## Run Old-vs-New Validation

```bash
python -m pipeline.validate_catalog_equivalence \
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

## Audit Feature Output

```bash
python -m pipeline.audit_feature_store \
  --feature-root /tmp/cryptorecorder-replay-feature-validation/feature_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --timeframes 1m,1s,100ms
```

The audit reports dense expected counts, actual row counts, date-bound violations, duplicate timestamps, null ratios, all-null columns, `quality_ok=false` count, and crossed-book totals.

## Next Milestone

Do not expand feature engineering before the full-L2 correctness milestone.

Next milestone:

```text
data_raw -> convert_day.py
semantically matches
data_raw -> replay_store -> generate_catalog --profile full_l2
```

Only after that should the project consider replay-store raw archival policy or broad production replacement of the old converter path.
