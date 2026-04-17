# Binance Market Data Recorder

24/7 market-data recording pipeline for Binance Spot and USDT-M Futures.
Captures **L2 order-book deltas** and **trades** via cryptofeed, stores them as
append-only JSONL (hourly rotation), and converts daily to Parquet for
NautilusTrader backtesting.

**Validated**: `python VALIDATE.py all` passes 100 % (system + 3-min runtime
smoke-test + converter E2E).

---

## Quick Start

```bash
cd ~/CryptoRecorder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # cryptofeed, aiohttp, zstandard, pandas, pyarrow
sudo apt-get install -y build-essential python3-dev g++   # needed to compile yapic.json / uvloop

python recorder.py                       # start recording (Ctrl-C to stop)
python VALIDATE.py all                   # run full validation suite
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
  recorder.py  ──→  data_raw/{VENUE}/{channel}/{SYMBOL}/{date}/{hour}.jsonl
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

---

## L2 Book Reconstruction

The raw depth records are **cryptofeed-normalised L2 deltas** (changed price
levels per update).  The daily converter (`convert_yesterday.py`) replays
deltas through a `BookReconstructor` to produce **top-of-book snapshots**
written to Parquet (`depth_YYYY-MM-DD.parquet`).

This is sufficient for:

- **Approximate L2 replay** — reconstruct bid/ask depth at any point in the
  day by replaying deltas from the start of the file.
- **Depth-N snapshots** — the converter emits top-5 levels per side.
- **Spread / mid-price analytics** — derived from the reconstructed book.

### L2 Book Determinism Roadmap

For **bit-exact** Binance L2 replay (matching the exchange's own sequence),
the following fields would need to be captured from the raw WebSocket frames
*before* cryptofeed normalises them:

| Field | Purpose | Status |
|-------|---------|--------|
| `U` / `u` (first/last update ID) | Detect gaps: `U == prev_u + 1` | Not yet captured (cryptofeed strips these) |
| `pu` (previous update ID, futures) | Chain verification for USDT-M streams | Not yet captured |
| Raw `depthUpdate` JSON | Preserves exact Binance format for replay | Normalised by cryptofeed |
| Initial REST snapshot (`/depth?limit=1000`) | Needed once per symbol at start-up for deterministic book init | Removed (causes 429) |

**Action items for deterministic replay:**

1. Add a thin pre-cryptofeed WebSocket tap that logs raw `depthUpdate` frames
   with `U`/`u`/`pu` intact, OR patch cryptofeed callbacks to forward these.
2. Fetch a single REST snapshot per symbol at process start (rate-limit aware),
   then never again.
3. Store `last_update_id` in the heartbeat so gap detection can trigger a
   one-time re-snapshot on reconnect.

Until these are implemented the recorded data supports **approximate** L2
reconstruction — not gap-proven deterministic replay.

---

## Validation

```bash
python VALIDATE.py system      # imports, config, directory structure
python VALIDATE.py runtime     # 3-min live smoke-test (launches recorder subprocess)
python VALIDATE.py converter   # end-to-end raw → Parquet conversion check
python VALIDATE.py all         # all of the above
```

Reports are written to `state/`:

| File | Contents |
|------|----------|
| `state/validation_report.json` | System check results |
| `state/runtime_report.json` | Runtime smoke-test (9 checks) |
| `state/converter_e2e_report.json` | Converter E2E (7 checks) |
| `state/master_validation_report.json` | Aggregated pass/fail |

### Runtime Checks (9)

`no_429_418` · `no_callback_errors` · `raw_files_nonempty` ·
`heartbeat_updated` · `clean_shutdown` · `schema_fields` ·
`ts_recv_monotonic` · `symbol_no_dash` · `futures_status`

---

## Operations

### Paths

| What | Location |
|------|----------|
| Raw data | `data_raw/{VENUE}/{channel}/{SYMBOL}/{YYYY-MM-DD}/{hour}.jsonl` |
| Nautilus catalog | `~/nautilus_data/catalog/` |
| Universe cache | `meta/universe/{VENUE}/{YYYY-MM-DD}.json` |
| Heartbeat | `state/heartbeat.json` |
| Recorder log | `recorder.log` |
| Systemd units | `systemd/` |

### Disk Retention

- Soft limit **400 GB** — warning logged
- Hard limit **480 GB** — oldest data cleaned automatically
- Raw retention default: **7 days** (converter will not delete newer data)
- Cleanup target: **350 GB**

### Useful Commands

```bash
# Live log
tail -f recorder.log

# Today's file count
find data_raw -name '*.jsonl' -newermt 'today' | wc -l

# Disk usage
du -sh data_raw/ ~/nautilus_data/

# Heartbeat
cat state/heartbeat.json | python3 -m json.tool
```

---

## Configuration

Edit `config.py` or override with environment variables:

| Setting | Default | Env Override |
|---------|---------|-------------|
| Symbols per venue | 50 | `CRYPTO_RECORDER_TOP_SYMBOLS` |
| Depth update interval | 100 ms (`DEPTH_INTERVAL_MS`) | — |
| File rotation | 60 min | — |
| ExchangeInfo refresh | 6 hours | — |
| Heartbeat interval | 30 s | — |

Futures are attempted at startup; if cryptofeed cannot resolve a symbol the
recorder strips it and retries.  If all futures symbols fail, futures are
disabled with a warning flag in the heartbeat.

---

## Project Files

```
recorder.py               Main recorder (cryptofeed, single asyncio loop)
config.py                 All constants and paths
storage.py                Hourly file rotation + JSONL writer
health_monitor.py         Heartbeat, per-symbol stats
disk_monitor.py           Disk usage checks, automatic cleanup
binance_universe.py       Top-N symbol selection by 24h volume
convert_yesterday.py      Daily raw → Parquet converter
converter/                Book reconstruction & Nautilus builders
VALIDATE.py               Unified validation entry point
validate_system.py        Dependency & config checks
validate_runtime.py       3-min live smoke-test
validate_converter_e2e.py Converter pipeline E2E
systemd/                  Service & timer units for production
```

---

## References

- [Binance WebSocket Streams](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)
- [cryptofeed](https://github.com/bmoscon/cryptofeed) (v2.4.1)
- [NautilusTrader](https://nautilustrader.io/)
