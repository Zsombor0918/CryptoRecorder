# Architecture

## Pipeline

1. `binance_universe.py` builds a ranked daily universe for spot and futures.
2. `recorder.py` starts `cryptofeed` with the selected symbols.
3. `storage.py` writes append-only raw files under `data_raw/`.
4. `health_monitor.py` publishes `state/heartbeat.json`.
5. Recorder startup writes `state/startup_coverage.json`.
6. `convert_day.py` converts a UTC date into Nautilus catalog output.

## Universe and startup flow

The preferred startup vocabulary is:

- `candidate_pool`: ranked symbols considered for a venue
- `pre_filter_rejected`: symbols rejected before recorder startup because they fail sanity or known-friction filters
- `selected`: symbols handed from universe selection into recorder startup
- `runtime_dropped`: selected symbols lost during feed initialization / compatibility stripping
- `active`: symbols that actually start recording

Compatibility fields from earlier iterations are still emitted where existing
tests and tooling rely on them.

## Raw storage layout

`data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl(.zst)`

Channels:
- `trade`
- `depth`
- `exchangeinfo`

## Conversion model

- Trades are converted to Nautilus `TradeTick`.
- Depth deltas are replayed into an approximate book and emitted as `OrderBookDepth10` snapshots.
- Instruments are built from `exchangeinfo` data.
- Conversion reports are written to `state/convert_reports/YYYY-MM-DD.json`.

## Phase 1 design boundaries

- Approximate L2 reconstruction only.
- No deterministic replay with Binance `U/u/pu` sequencing.
- No recurring REST depth snapshot polling.
- Deterministic replay remains future roadmap work.

## Reliability model

- Bad/unsupported symbols are rejected or dropped while startup continues.
- Venue-level graceful degradation is preserved: surviving active symbols continue recording.
- Queue-drop metrics, heartbeat state, and startup coverage are surfaced for operators.
