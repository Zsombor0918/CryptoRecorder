# Feature Store

## Current Status

The feature store is implemented as a v0 analysis layer built from `replay_store`.
It writes Parquet files with ZSTD compression and Hive-style path components:

```text
feature_store/
  timeframe=1m/
    venue=BINANCE_SPOT/
      symbol=ADAUSDT/
        2026-06-12.parquet
        2026-06-12.manifest.json
```

Important limitation: the current feature builder loads one symbol/date of replay depth and trade records into memory before aggregating windows. It is useful for validation and small runs, but it is not yet proven memory-safe for full top50 production. Benchmark RSS before scaling. The next implementation step is streaming/windowed aggregation.

## Window Behavior

The production default is UTC-day clamped and sparse:

- `--date YYYY-MM-DD` clamps input records to `[YYYY-MM-DD 00:00:00 UTC, next day 00:00:00 UTC)`.
- `timeframe=1m` means one row per one-minute window that contains at least one depth or trade record.
- `timeframe=1s` means one row per one-second window that contains data.
- `timeframe=100ms` means one row per 100ms window that contains data.
- Empty windows are skipped; the feature store does not currently force a dense full-day grid.
- `timestamp_ns` is the end of the feature window minus one nanosecond.

Dense UTC-day row counts are audit references, not guaranteed output counts:

- `1m`: 1440 possible windows
- `1s`: 86400 possible windows
- `100ms`: 864000 possible windows

`pipeline.build_feature_store` also exposes `--window-mode observed` for diagnostics. The default remains `utc_day`; both modes are sparse.

## Inputs

Features are computed from replay partitions:

```text
replay_store/venue=.../symbol=.../date=.../depth.parquet
replay_store/venue=.../symbol=.../date=.../trades.parquet
```

Replay records are sorted by their committed stream ordering. Feature calculations must not use future data, future labels, future returns, strategy outcomes, MAE/MFE, or future slippage.

## Written Schema

The writer currently uses `stores.feature_schema.FEATURE_SCHEMA_CORE_V1`. These are the fields actually written:

```text
venue: string
symbol: string
timestamp_ns: int64
timeframe: string
quality_ok: bool

best_bid: float64
best_ask: float64
mid_price: float64
spread: float64
spread_bps: float64

top1_bid_size: float64
top1_ask_size: float64

top5_bid_notional: float64
top5_ask_notional: float64
top10_bid_notional: float64
top10_ask_notional: float64
top50_bid_notional: float64
top50_ask_notional: float64

imbalance_top1: float64
imbalance_top5: float64
imbalance_top10: float64
imbalance_top50: float64

trade_count: int64
buy_volume: float64
sell_volume: float64
total_volume: float64
net_trade_flow: float64
signed_trade_volume: float64
aggressive_buy_ratio: float64
aggressive_sell_ratio: float64
large_trade_count: int64

return_1s: float64
return_5s: float64
return_10s: float64
return_30s: float64
return_1m: float64
realized_vol_1m: float64
high_low_range_1m: float64
jump_score: float64

depth_update_count: int64
trade_update_count: int64
update_rate: float64
dropped_gap_count: int64
reconnect_count: int64
crossed_book_count: int64
stale_book_seconds: float64
missing_trade_seconds: float64
latency_ms_mean: float64
latency_ms_p95: float64
```

Do not rely on fields that are not listed here. `FEATURE_SCHEMA_ADVANCED` exists in code as a deferred schema definition, but `FeatureWriter` currently writes only the core v1 schema above.

## Field Meanings

`timestamp_ns` is the feature window timestamp in UTC nanoseconds. `timeframe` is the requested aggregation interval, such as `100ms`, `1s`, or `1m`.

`best_bid`, `best_ask`, `mid_price`, `spread`, and `spread_bps` come from the latest depth record in the window when available.

`top1_*_size` is the size at the best bid/ask. `top5_*_notional`, `top10_*_notional`, and `top50_*_notional` are cumulative price * size notionals from the latest depth record.

`imbalance_top1`, `imbalance_top5`, `imbalance_top10`, and `imbalance_top50` are bid-side ratios over bid plus ask liquidity/notional for the same level group.

Trade-flow fields come from trades inside the window. `buyer_maker=False` is counted as aggressive buy volume; `buyer_maker=True` is counted as aggressive sell volume.

The return and volatility fields are currently placeholders or simplified values where the implementation does not yet maintain longer historical windows. Treat null values as expected in v0.

Quality fields summarize update counts, sequence gaps, crossed books, stale periods, missing trade periods, and receive-vs-exchange latency.

## CLI

```bash
python -m pipeline.build_feature_store \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --timeframes 1m \
  --replay-root /tmp/test_replay \
  --feature-root /tmp/test_features
```

Audit an existing feature store:

```bash
python -m validation.audit_feature_store \
  --feature-root /tmp/test_features \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --timeframes 1m,1s,100ms \
  --report-path /tmp/test_features/audit_2026-06-12.json
```

The audit reports actual row counts, expected dense row counts, min/max timestamps, rows outside the requested UTC day, duplicate timestamps, null ratios, all-null columns, `quality_ok=false` counts, and crossed-book counts.

## Validation Notes

The v0 feature store is not a replacement for the Nautilus catalog. It is an AI/selection layer. The current validated full-L2 path remains:

```text
data_raw -> convert_day.py -> full_l2 Nautilus catalog
```

The implemented replay-based catalog path is currently:

```text
data_raw -> replay_store -> generate_catalog --profile trades_only
```
