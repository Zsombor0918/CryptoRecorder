# Full-L2 Replay Catalog Plan

## Goal

Implement and validate:

```text
data_raw -> replay_store -> generate_catalog --profile full_l2
```

so it is semantically equivalent to the current validated path:

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
```

Do not replace `convert_day.py` until equivalence is proven.

## Current Status

Implemented:

- `data_raw -> replay_store`
- `replay_store -> feature_store`
- `replay_store -> generate_catalog --profile trades_only`
- trades-only old-vs-new semantic validation

Deferred:

- replay-based `OrderBookDeltas`
- replay-based `OrderBookDepth10`
- replay-based `full_l2` profile

## Semantics To Preserve

The replay-based full-L2 generator must reproduce the old converter behavior:

- depth records sorted by `(stream_session_id, session_seq, raw_index)`;
- exact `Decimal` book state;
- Binance spot `U/u` continuity semantics;
- Binance futures `pu` continuity semantics;
- fenced ranges for desync/resync gaps;
- `OrderBookDeltas` output;
- optional `OrderBookDepth10` derived from the replayed book state;
- `TradeTick` output using exact replay price/quantity strings;
- instrument output matching old `convert_day.py`;
- reports comparable to old converter reports.

## Preferred Implementation

### A. Best Path

Extract a shared depth replay core from `converter/depth_phase2.py`.

Both callers should use the same implementation:

```text
convert_day.py raw reader -> shared depth replay core -> Nautilus output
replay_store reader -> shared depth replay core -> Nautilus output
```

This minimizes semantic drift.

### B. Acceptable Path

Create an adapter that maps replay_store depth rows back into the normalized
record shape expected by `converter/depth_phase2.py`.

This is acceptable if extracting the core is too invasive for the first
milestone, but the adapter must preserve exact price/size strings and continuity
fields.

### C. Avoid

Do not write a second independent depth-to-Nautilus implementation from scratch
unless reuse is impossible. Two independent full-L2 implementations would make
equivalence bugs harder to find and easier to reintroduce.

## Replay Fields Required

Depth replay rows must preserve:

- `stream_session_id`
- `session_seq`
- `raw_index`
- `record_type`
- `U`
- `u`
- `pu`
- `ts_exchange_ns`
- `ts_receive_ns`
- exact nested bid/ask `price_str`
- exact nested bid/ask `size_str`
- `is_snapshot_seed`
- `is_depth_update`
- `is_sync_state`
- `is_desync`
- `is_resync`
- `quality_flags`
- `native_payload_hash`

Trade replay rows must preserve:

- `trade_stream_session_id`
- `trade_session_seq`
- `raw_index`
- `trade_id` / `agg_trade_id`
- `ts_exchange_ns`
- `ts_receive_ns`
- `price_str`
- `quantity_str`
- `buyer_maker`
- `aggressor_side` when available

The string fields are authoritative for catalog reconstruction. Float fields are
for feature convenience only.

## Validation Target

For the same date, venues, symbols, and time window:

```text
old = data_raw -> convert_day.py
new = data_raw -> replay_store -> generate_catalog --profile full_l2
```

Compare semantically, not byte-for-byte.

Required comparisons:

- instrument IDs;
- `TradeTick` count;
- first, last, and deterministic sampled trades;
- trade `price`, `size`, `aggressor_side`, `trade_id`, `ts_event`, `ts_init`;
- `OrderBookDeltas` count;
- first, last, and deterministic sampled deltas;
- delta `action`, `side`, `price`, `size`, `order_id`, `flags`, `sequence`,
  `ts_event`, and `ts_init` where applicable;
- reconstructed book checkpoints after sampled delta ranges;
- top 10 bid/ask equality at checkpoints;
- `OrderBookDepth10` count and sampled equality if emitted;
- fenced ranges, gaps, desync/resync diagnostics, or documented acceptable
  differences where old reports are not directly comparable.

## Test Plan

Normal CI:

- keep `full_l2` replay test skipped with a clear reason until implemented;
- unit-test the replay-row adapter or shared replay core with synthetic depth;
- ensure `convert_day.py` still does not depend on replay_store.

Real-data gated tests:

```bash
pytest -m realdata
```

or:

```bash
python -m pipeline.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --profile full_l2 \
  --overwrite
```

This command should remain skipped/unsupported until the full-L2 implementation
lands.

## Acceptance Criteria

The milestone is complete only when:

1. `generate_catalog --profile full_l2` exists and writes a Nautilus-readable catalog.
2. It reuses the old converter semantics directly or through a thin replay adapter.
3. TradeTick equivalence still passes.
4. OrderBookDeltas equivalence passes for synthetic fixtures and real ADAUSDT smoke.
5. Depth10 equivalence passes if Depth10 is emitted.
6. Date/window filtering is explicit and documented.
7. Reports make missing partitions, skipped records, and fenced ranges visible.
8. The old `convert_day.py` path remains available and unchanged for production fallback.
