# Binance Market Data Recorder

24/7 market-data recording pipeline for Binance Spot and USDT-M Futures.
Captures **L2 order-book deltas** and **trades** via cryptofeed, stores them as
append-only JSONL with hourly rotation and zstd compression, and converts
daily to Parquet for later NautilusTrader backtesting.

---

## Quick Start

```bash
cd ~/CryptoRecorder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
sudo apt-get install -y build-essential python3-dev g++

python recorder.py                       # start recording (Ctrl-C to stop)
python VALIDATE.py all                   # quick suite: system + runtime + converter
python VALIDATE.py accept               # full DoD: system + runtime + scale + converter
python convert_yesterday.py              # convert yesterday's raw data → Parquet
```

---

## Pipeline

```
Binance WS (cryptofeed)
  ├── L2 book deltas  (depth_interval=100ms)
  └── Trade events
        │
        ▼
  recorder.py  ──→  data_raw/{VENUE}/{channel}/{SYMBOL}/{date}/{hour}.jsonl.zst
        │
        ▼  (daily, or on demand)
  convert_yesterday.py  ──→  ~/nautilus_data/catalog/trades_YYYY-MM-DD.parquet
                              ~/nautilus_data/catalog/depth_YYYY-MM-DD.parquet
```

### What Is Recorded

| Channel | Source | Content |
|---------|--------|---------|
| `depth` | cryptofeed `L2_BOOK` delta | `{venue, symbol, channel, ts_recv_ns, ts_event_ms, sequence_number, payload: {bids, asks}}` |
| `trade` | cryptofeed `TRADES` | `{venue, symbol, channel, ts_recv_ns, ts_event_ms, payload: {price, quantity, side, trade_id}}` |
| `exchangeinfo` | REST (every 6 h) | Full `exchangeInfo` JSON (symbol filters, tick sizes, status) |

### What Is *Not* Recorded (by design)

- **Periodic REST depth snapshots** — removed because polling 50+ symbols
  triggers HTTP 429/418 bans.  The recorder relies on cryptofeed's own
  internal snapshot + delta management.
- **Raw Binance `depthUpdate` frames** — cryptofeed normalises the data,
  so `U`/`u`/`pu` update-ID fields are not preserved.  This means the
  recorded data supports **approximate** L2 reconstruction only.

---

## Raw Storage Format

- **Format**: append-only JSONL, one line per event
- **Rotation**: hourly (configurable via `ROTATION_INTERVAL_MIN`)
- **Compression**: on rotation and on normal shutdown, `.jsonl` files are
  compressed to `.jsonl.zst` (zstandard level 3); the uncompressed source
  is deleted only after successful compression
- **Layout**: `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{YYYY-MM-DDTHH}.jsonl.zst`

> **Note**: If the process is killed (SIGKILL / power failure), the active
> hourly file may remain as uncompressed `.jsonl`.  A normal SIGINT/SIGTERM
> shutdown compresses all open files before exiting.

---

## L2 Book Reconstruction

The raw depth records are **cryptofeed-normalised L2 deltas** (changed price
levels per update).  The daily converter (`convert_yesterday.py`) replays
deltas through a `BookReconstructor` to produce **Depth-10 snapshots**
(one per symbol per second) written to Parquet.

This is sufficient for:

- **Approximate L2 replay** — reconstruct bid/ask depth at any point by
  replaying deltas from the start of the file.
- **Spread / mid-price analytics** — derived from reconstructed book state.

### Limitations (Phase 1)

- The book starts empty — the first few snapshots may be incomplete until
  enough deltas have been received.
- Gaps from reconnects are not detected or repaired.
- This is **approximate** reconstruction, not bit-exact Binance replay.

### Determinism Roadmap (Phase 2)

For bit-exact L2 replay, the following would be needed:

1. Capture raw Binance `depthUpdate` frames with `U`/`u`/`pu` intact
2. Fetch a single REST snapshot per symbol at startup (rate-limit aware)
3. Store `last_update_id` in heartbeat for gap detection on reconnect

---

## Converter

```bash
python convert_yesterday.py --date YYYY-MM-DD
```

Reads raw JSONL (plain or zstd-compressed) for the given date, produces:

- `trades_YYYY-MM-DD.parquet` — all trades sorted by `ts_event_ns`
  (columns: symbol, venue, ts_event_ns, ts_init_ns, price, quantity, side, trade_id)
- `depth_YYYY-MM-DD.parquet` — Depth-10 snapshots at ~1 s interval
  (columns: symbol, venue, ts_event_ms, ts_recv_ns, bid_price_0..9, bid_size_0..9,
  ask_price_0..9, ask_size_0..9)

Output is plain Parquet (snappy compression), written to `~/nautilus_data/catalog/`.
These are **not** a NautilusTrader `ParquetDataCatalog` structure — they are
flat files that can be loaded with `pd.read_parquet()` and transformed for
Nautilus ingestion in a separate step.

A conversion report is written to `state/convert_reports/YYYY-MM-DD.json`.

---

## Futures Behaviour

- Futures are attempted at startup with the top-N USDT-M perpetual symbols.
- If cryptofeed cannot resolve a symbol, the recorder strips it and retries.
- If all futures symbols fail, futures are **disabled** with a warning.
- The heartbeat includes `futures_enabled` and `futures_disabled_reason`.
- The 3-min smoke test accepts degraded mode (futures disabled) as a pass.
- The 10-min scale acceptance test **requires** futures to be actively recording.

---

## Queue Drop Monitoring

Each async writer tracks dropped records (when the queue is full).  Drops are:

- Logged per occurrence
- Aggregated in `heartbeat.json` as `queue_drop_total` and `queue_drop_by_writer`
- Checked by validators: the runtime smoke test requires zero drops; the
  scale test allows up to 50 drops (configurable threshold).

---

## Validation

```bash
python VALIDATE.py system      # imports, config, directory structure (~5 s)
python VALIDATE.py runtime     # 3-min live smoke-test (12 checks)
python VALIDATE.py scale       # 10-min 50/50 acceptance test (11 checks)
python VALIDATE.py converter   # end-to-end raw → Parquet conversion check
python VALIDATE.py all         # system + runtime + converter (quick suite)
python VALIDATE.py accept      # system + runtime + scale + converter (full DoD)
```

Reports are written to `state/`:

| File | Contents |
|------|----------|
| `state/validation_report.json` | System check results |
| `state/runtime_report.json` | Runtime smoke-test |
| `state/scale_50_50_report.json` | Scale acceptance test |
| `state/converter_e2e_report.json` | Converter E2E |
| `state/master_validation_report.json` | Aggregated pass/fail |

### Runtime Smoke-Test Checks (12)

`no_429_418` · `no_callback_errors` · `raw_files_nonempty` ·
`heartbeat_updated` · `clean_shutdown` · `no_async_pathology` ·
`schema_fields` · `ts_recv_monotonic` · `symbol_no_dash` ·
`futures_status` · `raw_files_compressed` · `queue_drops`

### Scale 50/50 Acceptance Checks (11)

`no_429_418` · `no_callback_errors` · `spot_depth_symbols` ·
`futures_depth_symbols` · `heartbeat_total_msgs` · `futures_enabled` ·
`queue_drops` · `reconnect_count` · `no_reconnect_storm` ·
`clean_shutdown` · `raw_files_compressed`

---

## Operations

### Paths

| What | Location |
|------|----------|
| Raw data | `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{hour}.jsonl.zst` |
| Parquet output | `~/nautilus_data/catalog/` |
| Universe cache | `meta/universe/{VENUE}/{YYYY-MM-DD}.json` |
| Heartbeat | `state/heartbeat.json` |
| Recorder log | `recorder.log` |
| Systemd units | `systemd/` |

### Disk Cleanup

- Soft limit **400 GB** — warning logged
- Hard limit **480 GB** — oldest **date directories** under `data_raw/` are
  deleted automatically (never the catalog or venue/channel structure)
- Raw retention default: **7 days**
- Cleanup target: **350 GB**

### Useful Commands

```bash
tail -f recorder.log                                    # live log
find data_raw -name '*.jsonl.zst' -newermt 'today' | wc -l  # today's files
du -sh data_raw/ ~/nautilus_data/                       # disk usage
cat state/heartbeat.json | python3 -m json.tool         # heartbeat
```

---

## Configuration

Edit `config.py` or override with environment variables:

| Setting | Default | Env Override |
|---------|---------|-------------|
| Symbols per venue | 50 | `CRYPTO_RECORDER_TOP_SYMBOLS` |
| Depth update interval | 100 ms | — |
| File rotation | 60 min | — |
| ExchangeInfo refresh | 6 hours | — |
| Heartbeat interval | 30 s | — |
| Shutdown watchdog | 120 s | `CRYPTO_RECORDER_WATCHDOG_SEC` |

---

## Project Files

```
recorder.py               Main recorder (cryptofeed, single asyncio loop)
config.py                 All constants and paths
storage.py                Hourly file rotation, zstd compression, async writers
health_monitor.py         Heartbeat, per-symbol stats, queue drop tracking
disk_monitor.py           Disk usage checks, automatic date-dir cleanup
binance_universe.py       Top-N symbol selection by 24h volume
convert_yesterday.py      Daily raw → Parquet converter (approximate L2)
converter/                Book reconstruction & Nautilus builders
VALIDATE.py               Unified validation entry point
validate_system.py        Dependency & config checks
validate_runtime.py       3-min live smoke-test (12 checks)
validate_scale_50_50.py   10-min 50/50 scale acceptance (11 checks)
validate_converter_e2e.py Converter pipeline E2E
systemd/                  Service & timer units for production
```

---

## References

- [Binance WebSocket Streams](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)
- [cryptofeed](https://github.com/bmoscon/cryptofeed) (v2.4.1)
- [NautilusTrader](https://nautilustrader.io/)
