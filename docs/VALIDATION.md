# Validation

CryptoRecorder validation is organized into clear layers that match the pipeline stages.

## Validation Layers

### A. Recorder Validation
Validates that the recorder is running correctly and producing raw data.

**Covered by:** `runtime.py`, `scale.py`

| Check | Description |
|-------|-------------|
| raw_files_nonempty | JSONL files created in data_raw/ |
| schema_fields | Records have venue/symbol/channel/ts_recv_ns |
| ts_recv_monotonic | Timestamps non-decreasing per file |
| heartbeat_updated | Heartbeat shows uptime and messages |
| queue_drops | Zero queue drops |
| no_429_418 | No rate limit bans |
| clean_shutdown | Clean exit, no async pathology |

### B. Converter Validation
Validates that raw data converts correctly to Nautilus catalog.

**Covered by:** `nautilus_catalog.py`

| Check | Description |
|-------|-------------|
| converter_exit_zero | Converter exits 0 |
| catalog_exists | Catalog directory non-empty |
| report_valid | Convert report has required fields |
| instruments_exist | catalog.instruments() returns objects |
| trades_nonempty | trade_ticks query returns data |
| depth10_nonempty | order_book_depth10 query returns data |
| time_bounds | Timestamps within expected range |
| objects_are_nautilus | Types are real Nautilus model types |
| instrument_id_mapping | All data references valid instruments |
| idempotency_counts | Re-run yields identical counts |

### C. Catalog Quality Validation
Validates that the catalog meets quality thresholds.

**Covered by:** `nautilus_catalog.py`

| Check | Description |
|-------|-------------|
| crossed_book_spot | Spot snapshots have positive spread |
| crossed_book_futures | Futures snapshots have positive spread |
| crossed_rate_threshold | crossed_rate < 0.1% |
| gap_rate_sane | gap_rate < 100% |
| gap_per_symbol | Per-symbol gap breakdown valid |
| data_presence | Data presence fields consistent |

### D. Infrastructure Validation
Validates system setup and safety.

**Covered by:** `system.py`, `purge_safety.py`

| Check | Description |
|-------|-------------|
| Dependencies | All imports available |
| Configuration | Config loads correctly |
| Directories | Required directories exist |
| Purge safety | Disk cleanup deletes only intended files |

## CLI Usage

`VALIDATE.py` is the master validation entrypoint.

```bash
python VALIDATE.py system      # Infrastructure checks
python VALIDATE.py runtime     # 3-min recorder smoke test
python VALIDATE.py scale       # 10-min 50/50 acceptance test
python VALIDATE.py nautilus    # Converter + catalog validation
python VALIDATE.py purge       # Purge safety proof
python VALIDATE.py all         # system + runtime + nautilus + purge
python VALIDATE.py accept      # Full acceptance suite including scale
```

## Reports

Validation outputs are written to `state/`:

| File | Content |
|------|---------|
| `validation_report.json` | System validation results |
| `runtime_report.json` | Runtime smoke test results |
| `scale_50_50_report.json` | Scale test results |
| `validation/nautilus_catalog_e2e_{date}.json` | Catalog validation results |
| `validation/purge_safety.json` | Purge safety results |

## Crossed-Book Handling

Crossed-book detection is a first-class quality concern:

1. **During conversion:** When the reconstructed book becomes crossed (best_bid >= best_ask),
   the converter logs the event, resets the book, and does NOT emit a crossed snapshot.

2. **In reports:** The converter report includes:
   - `crossed_book_events_total`: Total crossed events during reconstruction
   - `crossed_rate`: crossed_events / total_depth_snapshots
   - `per_symbol_crossed_books`: Per-symbol breakdown with examples

3. **In validation:** The validator checks:
   - `crossed_rate_threshold`: crossed_rate must be < 0.1%
   - `crossed_book_spot`: No crossed snapshots in catalog (inspected)
   - `crossed_book_futures`: No crossed snapshots in catalog (inspected)

**Phase 1 guarantee:** The final catalog contains no crossed-book snapshots.
Crossed events during reconstruction trigger resets, not catalog writes.

## Data Presence Reporting

The converter tracks which instruments have actual data:

| Field | Description |
|-------|-------------|
| `instruments_defined` | Total instruments from exchangeInfo |
| `instruments_with_trades` | Instruments with ≥1 TradeTick |
| `instruments_with_depth` | Instruments with ≥1 OrderBookDepth10 |
| `instruments_with_both` | Instruments with both trades and depth |
| `instruments_with_no_data` | Instruments with neither (no raw data for date) |
| `no_data_list` | List of instruments with no data (up to 20) |

This helps distinguish between:
- Instruments that were defined but had no trading activity
- Instruments that failed to record
- Instruments with partial data (trades only, or depth only)

## Phase 1 Scope

Validation expectations are aligned with Phase 1 behavior:

**Acceptable:**
- Approximate L2 reconstruction (not deterministic)
- Gap detection via timestamp heuristic
- Some instruments with no data (market was inactive)

**Not acceptable:**
- Crossed snapshots in catalog
- Queue drops during normal operation
- Rate limit bans (429/418)
- Async pathology during shutdown
