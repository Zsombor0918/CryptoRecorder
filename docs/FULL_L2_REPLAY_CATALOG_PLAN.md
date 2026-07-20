# Full-L2 Replay Catalog Plan

## Goal

Implement and validate:

```text
data_raw -> replay_store -> validation.replay_catalog_reconstruct --profile full_l2
```

(validation-only, no CLI; invoked only by `validation.validate_catalog_equivalence`)

so it is semantically equivalent to the current validated path:

```text
data_raw -> convert_day.py -> Nautilus full-L2 catalog
```

Do not replace `convert_day.py` until equivalence is proven.

> **Status (updated)**: `full_l2` is **implemented** and reuses the shared depth
> engine in `converter/depth_phase2.py` (via the `stores/replay_depth_adapter.py`
> adapter). It is **semantically validated on the ADAUSDT single-day smoke**
> against `convert_day.py` (trades, OrderBookDeltas, OrderBookDepth10, and book
> checkpoints all match). `convert_day.py` remains the production reference until
> broader top50/multi-day validation passes; `v2.0.0` is **not** declared. There
> is no second independent depth converter.

## Current Status

Implemented:

- `data_raw -> replay_store`
- `replay_store -> validation.replay_catalog_reconstruct --profile trades_only` (validation-only, no CLI)
- trades-only old-vs-new semantic validation
- **`replay_store -> validation.replay_catalog_reconstruct --profile full_l2`** (shared depth engine)
- **replay-based `OrderBookDeltas`** (validated on ADAUSDT smoke)
- **replay-based `OrderBookDepth10`** (validated on ADAUSDT smoke)
- **full-L2 old-vs-new semantic validation** (`validate_catalog_equivalence
  --profile full_l2`)

CryptoRecorder does not build a feature-store, label-store, or
general-purpose consumer catalog from replay_store (removed, issue #17).

Pending (NOT done):

- broader `full_l2` validation across the top50 universe and multiple days
- `v2.0.0` declaration (gated on the broader validation above)

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
new = data_raw -> replay_store -> validation.replay_catalog_reconstruct --profile full_l2
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

- `full_l2` is implemented; the synthetic full-L2 equivalence test runs in CI
  (`tests/test_catalog_equivalence_full_l2.py`,
  `tests/test_catalog_equivalence.py::test_full_l2_validator_matches_convert_day_on_clean_synthetic_day`);
- unit-test the replay-row adapter and shared replay core with synthetic depth
  (`tests/test_replay_depth_adapter.py`, `tests/test_replay_catalog_reconstruct.py`);
- ensure `convert_day.py` still does not depend on replay_store
  (`tests/test_semantic_equivalence.py::test_convert_day_remains_legacy_full_l2_entrypoint`).

Real-data gated tests:

```bash
pytest -m realdata
```

or:

```bash
python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 \
  --symbols ADAUSDT \
  --venues BINANCE_SPOT \
  --data-root ./data_raw \
  --work-root /tmp/cryptorecorder-equivalence \
  --profile full_l2 \
  --overwrite
```

This command is supported and **passes on the ADAUSDT 2026-06-12 smoke** (see the
Real-Data Result below). It remains gated (off by default in CI) because it needs
local raw market data.

## Real-Data Result (ADAUSDT smoke)

```
2026-06-12 BINANCE_SPOT/ADAUSDT full_l2 (replay vs convert_day.py):
  trade_ticks         old 124457   new 124457   range match  0 mismatches
  order_book_deltas   old 1231284  new 1231284  range match  0 mismatches
  order_book_depth10  old 71341    new 71341    range match  0 mismatches
  book checkpoints    7/7 match, no crossed books
  status              passed
```

This is a **single-symbol, single-day** result. It does not by itself prove
universe-wide equivalence.

## Equivalence Boundary (caveats)

Reproducible by the replay full-L2 path (matches `convert_day.py`):

- snapshot-seed deltas, live depth deltas, derived Depth10;
- continuity fences, session resets, clean single-session bootstrap days.

Documented acceptable differences (the replay v0 path does not reproduce these
old-converter internals byte-for-byte; comparison is semantic, not byte-equal):

- `sync_state` fenced-range bookkeeping;
- carry synthetic snapshots across partition boundaries;
- repartitioned boundary records;
- duplicate-suppression details.

Days dominated by these internals may diverge; the ADAUSDT smoke day did not.

## Acceptance Criteria

Milestone-complete checklist (✅ = done for the ADAUSDT smoke milestone):

1. ✅ `validation.replay_catalog_reconstruct --profile full_l2` exists (validation-only, no CLI) and writes a Nautilus-readable temporary catalog.
2. ✅ It reuses the old converter semantics through the shared depth engine + thin replay adapter.
3. ✅ TradeTick equivalence passes (synthetic + ADAUSDT smoke).
4. ✅ OrderBookDeltas equivalence passes for synthetic fixtures and real ADAUSDT smoke.
5. ✅ Depth10 equivalence passes when Depth10 is emitted (ADAUSDT smoke).
6. ✅ Date/window filtering is explicit and documented (`--date`/`--start`/`--end`, `--time-filter`).
7. ✅ Reports make missing partitions, skipped records, and fenced ranges visible (`depth_diagnostics`/`fenced_ranges`/`caveats`).
8. ✅ The old `convert_day.py` path remains available and unchanged for production fallback.

Still open before `v2.0.0`:

- ⬜ broader validation across the top50 universe;
- ⬜ multi-day validation.
