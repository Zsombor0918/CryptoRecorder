# State File Schemas

These schemas document stable operational fields used by tooling and operators.
They are interface notes, not a strict JSON Schema contract.

## `state/heartbeat.json`

Top-level fields:

- `timestamp` (ISO-8601 with Hungary local offset, `Europe/Budapest`)
- `uptime_seconds`
- `total_symbols`
- `spot_symbols_active`
- `futures_symbols_active`
- `spot_symbols_requested`
- `futures_symbols_requested`
- `spot_symbols_dropped`
- `futures_symbols_dropped`
- `spot_symbols_dropped_list`
- `futures_symbols_dropped_list`
- `spot_coverage_ratio`
- `futures_coverage_ratio`
- `total_messages`
- `total_gaps`
- `total_reconnects`
- `queue_drop_total`
- `queue_drop_by_writer`
- `futures_enabled`
- `futures_disabled_reason`
- `architecture` — always `"deterministic_native"`
- `trade_health` — trade_v2 ingest diagnostics by venue
- `by_venue`

Notes:

- Human-facing report timestamps use Hungary local time with DST-aware offset
  (`+01:00` or `+02:00` depending on the date).
- `spot_symbols_dropped*` / `futures_symbols_dropped*` summarize startup
  `runtime_dropped` symbols, not the full universe `candidate_pool`.

`trade_health` is a map keyed by venue (e.g. `BINANCE_SPOT`, `BINANCE_USDTF`) containing venue-level ingest diagnostics:

- `ws_message_count` — total WebSocket messages received
- `parsed_trade_count` — trade records successfully parsed and committed
- `skipped_message_count` — messages skipped (validation or processing errors)
- `skip_reasons` — map of skip reason → count
- `lifecycle_only_sessions` — stream sessions with zero trade records
- `reconnect_count` — number of stream reconnections
- `last_close_reason` — most recent WebSocket close reason
- `sample_payload_shape` — example of first parsed message structure (diagnostic)
- `subscribed_symbols` / `subscribed_symbol_count` — native trade stream subscription coverage
- `per_symbol_parsed_trade_count` — parsed trade counts keyed by raw Binance symbol
- `stream_count`, `first_5_streams`, `url`, `url_length` — shard subscription/connect details
- `task_started`, `task_done`, `task_cancelled`, `connect_attempt_count`, `connected_once` — shard lifecycle details
- `first_message_seen_at`, `last_message_seen_at`, `last_exception` — liveness diagnostics for silent or failing trade shards
- `warnings` / `warning_count` — trade-ingest warnings, such as high-liquidity futures with active depth but zero parsed trades
- `shards` — if connection sharding is enabled, per-shard diagnostics with same structure

Empty if no trade recorder is running.

`by_venue` is a map keyed by venue (e.g. `BINANCE_SPOT`, `BINANCE_USDTF`) containing per-symbol objects with:

- `venue`
- `symbol`
- `message_count`
- `last_ts_event`
- `last_update_id`
- `prev_update_id`
- `gap_count`
- `sync_state`
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `last_heartbeat`

## `state/startup_coverage.json`

Startup audit summary with top-level `timestamp`, `warnings`, and nested
per-venue `spot` / `futures` sections.

`timestamp` uses Hungary local time (`Europe/Budapest`). The date-scoped file
names and conversion target dates elsewhere in the pipeline still stay on UTC.

Per-venue fields:

- `venue`
- `requested_raw`, `requested_count`
- `selected_raw`, `selected_count`
- `candidate_pool`
- `pre_filter_rejected_count`, `pre_filter_rejected_sample`
- `runtime_dropped_count`
- `active_raw`, `active_count`
- `coverage_ratio`
- `warnings`

Futures-specific fields:

- `candidate_pool_raw_count`
- `candidate_pool_after_sanity_count`
- `candidate_pool_after_support_check_count`
- `support_precheck_available`
- `support_precheck_error`
- `support_precheck_rejected_count`
- `support_precheck_rejected_sample`

## `state/convert_reports/YYYY-MM-DD.json`

Per-day converter report.

Core fields:

- `date`
- `timestamp`
- `runtime_sec`
- `status` — `ok`, `empty`, or `no_data`
- `architecture` — always `"deterministic_native"`
- `instruments_written`
- `total_trades_written`
- `total_order_book_deltas_written`
- `total_depth10_written`
- `total_derived_depth_snapshots_written`
- `full_depth_source` — currently `"OrderBookDeltas"`
- `derived_depth_snapshot_type` — currently `"OrderBookDepth10"`
- `derived_depth_snapshot_levels`
- `requested_depth_snapshot_levels`
- `requested_depth_snapshot_levels_applied`
- `snapshot_seed_limit` — Binance REST snapshot seed depth, not catalog snapshot depth
- `bad_lines`
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `fenced_ranges_total`
- `fenced_ranges_low`
- `fenced_ranges_medium`
- `fenced_ranges_high`
- `unrecovered_fences`
- `gap_warning_counts`
- `per_symbol_fenced_ranges`
- `per_symbol_gap_diagnostics`
- `data_presence`
- `futures_enabled`
- `symbols_processed`
- `venues`
- `ts_ranges` (`trade`, `order_book_deltas`, `order_book_depths` start/end nanoseconds)
- `catalog_root`

`status` meanings:

- `ok`: converted trade and/or depth data was written
- `empty`: raw inputs resolved but no trade/depth output was produced
- `no_data`: no raw data was found for the requested date

`venues` is keyed by venue and contains:

- `symbols`
- `trades_written`
- `delta_events_written`
- `depth10_written`
- `snapshot_seed_count`
- `resync_count`
- `desync_events`
- `fenced_ranges`

`data_presence` tracks which instruments have actual data:

- `instruments_defined`: Total instruments from exchangeInfo
- `instruments_with_trades`: Instruments with ≥1 TradeTick
- `instruments_with_depth`: Instruments with ≥1 OrderBookDeltas
- `instruments_with_no_data`: Instruments with neither
- `no_data_list`: List of instruments with no data (up to 20)

`per_symbol_fenced_ranges` maps `"VENUE/SYMBOL"` to:

- `fenced_ranges`: Count of intentionally excluded ranges
- `fenced_ranges_low`
- `fenced_ranges_medium`
- `fenced_ranges_high`
- `unrecovered_fences`
- `examples`: Up to 3 sample fenced ranges with session/time/reason metadata

`per_symbol_gap_diagnostics` maps `"VENUE/SYMBOL"` to:

- `max_depth_update_gap_sec`
- `depth_gap_count_over_1s`
- `depth_gap_count_over_5s`
- `depth_gap_count_over_60s`
- `max_trade_gap_sec` (informational; trade inactivity is not an L2 failure)
- `max_depth10_gap_sec`
- `session_boundary_gap_count`
- `shutdown_boundary_gap_count`
- `reconnect_boundary_gap_count`

`ts_ranges` is the authoritative indication of actual temporal coverage.

`timestamp` is the report creation time in Hungary local time
(`Europe/Budapest`), not the UTC trading day boundary.

## Raw Record Schemas

### depth_v2 records

All depth_v2 records have `record_type` and `stream_session_id`.

**snapshot_seed:**
```json
{
  "record_type": "snapshot_seed",
  "stream_session_id": 1,
  "session_seq": 1,
  "raw_index": 0,
  "ts_recv_ns": 1713400000000000000,
  "last_update_id": 12345,
  "bids": [["50000.00", "1.5"], ...],
  "asks": [["50001.00", "2.0"], ...]
}
```

**depth_update:**
```json
{
  "record_type": "depth_update",
  "stream_session_id": 1,
  "session_seq": 2,
  "raw_index": 0,
  "ts_recv_ns": 1713400001000000000,
  "first_update_id": 12346,
  "last_update_id": 12346,
  "bids": [["50000.00", "1.6"]],
  "asks": []
}
```

**sync_state / stream_lifecycle:** metadata records with `session_seq` (for sync_state) or without (for lifecycle).

### trade_v2 records

**trade (spot):**
```json
{
  "record_type": "trade",
  "market_type": "spot",
  "trade_stream_session_id": 1,
  "trade_session_seq": 1,
  "ts_recv_ns": 1713400000000000000,
  "ts_trade_ms": 1713400000000,
  "exchange_trade_id": 987654,
  "price": "50000.00",
  "quantity": "0.5",
  "is_buyer_maker": false,
  "buyer_order_id": 111,
  "seller_order_id": 222,
  "best_match_flag": true,
  "native_payload": { ... }
}
```

**trade (futures):**
```json
{
  "record_type": "trade",
  "market_type": "futures",
  "trade_stream_session_id": 1,
  "trade_session_seq": 1,
  "ts_recv_ns": 1713400000000000000,
  "ts_trade_ms": 1713400000000,
  "exchange_trade_id": 987654,
  "price": "50000.00",
  "quantity": "0.5",
  "is_buyer_maker": true,
  "first_trade_id": 100,
  "last_trade_id": 105,
  "native_payload": { ... }
}
```

**trade_stream_lifecycle:**
```json
{
  "record_type": "trade_stream_lifecycle",
  "trade_stream_session_id": 1,
  "ts_recv_ns": 1713400000000000000,
  "event": "connected"
}
```
Lifecycle markers do NOT consume `trade_session_seq`.
