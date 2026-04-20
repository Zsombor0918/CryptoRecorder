# State File Schemas (Phase 1)

These schemas document stable operational fields used by tooling and operators.
They are interface notes, not a strict JSON Schema contract.

## `state/heartbeat.json`

Top-level fields:

- `timestamp` (UTC ISO-8601)
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
- `snapshot_mode`
- `by_venue`

Notes:

- `snapshot_mode` is expected to be `"disabled"` in Phase 1.
- `spot_symbols_dropped*` / `futures_symbols_dropped*` summarize startup
  `runtime_dropped` symbols, not the full universe `candidate_pool`.
- `last_update_id` is reserved for future richer sequencing support and is
  typically `null` in the current Phase 1 pipeline.

`by_venue` is a map keyed by venue (for example `BINANCE_SPOT`, `BINANCE_USDTF`) containing per-symbol objects with:

- `venue`
- `symbol`
- `message_count`
- `last_ts_event`
- `last_update_id`
- `gap_count`
- `last_heartbeat`

## `state/startup_coverage.json`

Startup audit summary with top-level `timestamp`, `warnings`, and nested
per-venue `spot` / `futures` sections.

Per-venue fields:

- `venue`
- `requested_raw`, `requested_count`
- `selected_raw`, `selected_count`
- `candidate_pool`
- `pre_filter_rejected_count`, `pre_filter_rejected_sample`
- `runtime_dropped_raw`, `runtime_dropped_cf`, `runtime_dropped_count`
- `active_raw`, `active_cf`, `active_count`
- `coverage_ratio`
- `warnings`

Compatibility aliases still emitted:

- `filtered_raw`, `filtered_cf`, `filtered_count`
- `dropped_raw`, `dropped_cf`, `dropped_count`
- `dropped_all_raw`, `dropped_all_cf`
- `rejected_pre_filter_count`, `rejected_pre_filter_sample`

Futures-specific fields:

- `candidate_pool_raw_count`
- `candidate_pool_after_sanity_count`
- `candidate_pool_after_support_check_count`
- `support_precheck_available`
- `support_precheck_error`
- `support_precheck_rejected_count`
- `support_precheck_rejected_sample`

Top-level convenience summary fields may also be present, for example
`spot_candidate_pool`, `futures_candidate_pool`,
`spot_pre_filter_rejected_count`, and `futures_support_precheck_rejected_count`.

## `state/convert_reports/YYYY-MM-DD.json`

Per-day converter report.

Core fields:

- `date`
- `timestamp`
- `runtime_sec`
- `status`
- `instruments_written`
- `total_trades_written`
- `total_depth_snapshots_written`
- `bad_lines`
- `gaps_suspected`
- `book_resets_total`
- `gap_rate`
- `per_symbol_gaps`
- `futures_enabled`
- `symbols_processed`
- `venues`
- `ts_ranges` (`trade` and `depth` start/end nanoseconds)
- `catalog_root`

`status` meanings:

- `ok`: converted trade and/or depth data was written
- `empty`: raw inputs resolved but no trade/depth output was produced
- `no_data`: no raw data was found for the requested date

`venues` is keyed by venue and contains:

- `symbols`
- `trades_written`
- `depth_snapshots_written`
- `gaps_suspected`
- `book_resets`

`ts_ranges` is the authoritative indication of actual temporal coverage in converted raw inputs.

`per_symbol_gaps` is a small diagnostic map of the worst gap offenders, not a
complete per-symbol dump of every converted instrument.
