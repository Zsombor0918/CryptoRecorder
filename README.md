# Binance Market Data Recorder

A robust, 24/7 market data recording pipeline for Binance Spot and Futures that captures L2 order book updates, trades, and snapshots for deterministic backtesting with NautilusTrader.

## Features

- **Real-time L2 Order Book Recording**: Captures depthUpdate events at 100ms frequency
- **Trade Stream Recording**: Records all trades with timestamps and sides
- **REST Snapshots**: Periodic (10-minute) REST API snapshots for fast checkpoint recovery
- **Instrument Metadata**: Daily metadata capture (symbol filters, price precision, status)
- **Gap Detection & Resync**: Detects missing updates and automatically re-syncs from REST snapshots
- **50 Symbol Universe**: Tracks top 50 USDT trading pairs across Spot and Futures
- **Reliable Storage**: JSONL append-only format with hourly rotation and zstandard compression
- **Nautilus Conversion**: Daily batch conversion to ParquetDataCatalog format
- **Disk Management**: Configurable retention and automatic cleanup

## Architecture

```
┌─────────────────────────────────────────────────────┐
│          Binance WebSocket Streams (24/7)          │
│   - L2 Book Updates (@100ms per symbol)            │
│   - Trade Events                                    │
└────────────┬────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────┐
│      Main Recorder (recorder.py)                    │
│  - Validates & deduplicates messages               │
│  - Maintains L2 book state                         │
│  - Detects gaps & triggers resyncs                 │
└────────────┬────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────┐
│    File Rotation & Storage (hourly rotation)       │
│  Location: ~/data_raw/{venue}/{channel}/{symbol}  │
│  Format: JSONL (zstd compressed)                   │
│  Size: ~500GB budget, 30-day retention             │
└─────────────────────────────────────────────────────┘
             │
             ▼  (Daily @00:30 UTC)
┌─────────────────────────────────────────────────────┐
│  Nautilus Converter (converter.py)                 │
│  - Reconstructs L2 book state                     │
│  - Generates ParquetDataCatalog                   │
│  Location: ~/data_nautilus/                       │
└─────────────────────────────────────────────────────┘
```

## Installation

### 1. Clone and Setup

```bash
cd ~/services/CryptoRecorder
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Install Build Tools (if needed)

```bash
sudo apt-get install build-essential python3.12-dev
```

## Configuration

Edit `config.yaml` to customize:

```yaml
recorder:
  venues:
    - binance_spot
    - binance_usdtm
  
  # Top 50 USDT symbols
  symbols:
    binance_spot:
      - BTCUSDT
      - ETHUSDT
      # ... 48 more
  
  # Channel configuration
  depth_interval: "100ms"          # L2 update frequency
  snapshot_interval_seconds: 600   # REST snapshots every 10 min
  metadata_interval_seconds: 3600  # Metadata daily
  
  # Storage settings
  storage:
    base_path: ~/data_raw
    compression: zstd
    rotation_minutes: 60
    retention_days: 30
    disk_limit_gb: 500
```

## Running the Recorder

### Quick Start

```bash
source .venv/bin/activate
python main.py
```

This will:
1. Start WebSocket streams for all 50 symbols on both Spot and Futures
2. Record L2 updates and trades in real-time
3. Run REST snapshot service (background)
4. Convert yesterday's data daily at 00:30 UTC

### Running Individual Components

**Just the recorder (no conversion):**
```bash
python recorder.py
```

**Just fetch snapshots:**
```bash
python snapshot_service.py
```

**Convert historical data:**
```bash
python converter.py
```

## Data Format

### Raw Message Structure (JSONL)

```json
{
  "venue": "BINANCE_SPOT",
  "symbol": "BTCUSDT",
  "channel": "L2_BOOK",
  "ts_recv_ns": 1704067200000000000,
  "ts_event": 1704067200000,
  "sequence_number": 12345,
  "update_id_first": 100,
  "update_id_last": 105,
  "payload": {
    "bids": [["45000.00", "1.5"], ["44999.00", "2.0"]],
    "asks": [["45001.00", "1.2"], ["45002.00", "0.8"]],
    "top_bid": "45000.00",
    "top_ask": "45001.00"
  }
}
```

### Directory Structure

```
~/data_raw/
├── binance_spot/
│   ├── L2_BOOK/
│   │   ├── BTCUSDT/
│   │   │   ├── 2024-01-15/
│   │   │   │   ├── 2024-01-15T00.jsonl.zst
│   │   │   │   ├── 2024-01-15T01.jsonl.zst
│   │   │   │   └── ...
│   ├── TRADES/
│   │   └── BTCUSDT/
│   │       └── ...
│   └── SNAPSHOT/
│       └── BTCUSDT/
│           └── ...
├── binance_usdtm/
│   └── ...
│
~/data_nautilus/  # Converted Parquet files
├── l2_book_BINANCE_SPOT_20240115.parquet
├── trades_BINANCE_SPOT_20240115.parquet
├── l2_book_BINANCE_USDTM_20240115.parquet
└── trades_BINANCE_USDTM_20240115.parquet
```

## Gap Detection & Recovery

The recorder automatically handles gaps in the update sequence:

1. **Detection**: Maintains `last_update_id` per symbol
   - If `U > last_u + 1`, gap detected
   - Logs warning and increments gap counter

2. **Recovery**: Triggered on gap
   - Fetches fresh REST snapshot
   - Resets L2 book
   - Resumes applying WS updates from correct sequence

3. **Monitoring**: Check gap stats
   ```python
   print(recorder.gaps_detected)  # {(venue, symbol): count}
   ```

## Performance & Storage

### Typical Data Volumes (per day, 50 symbols)

| Metric | Spot | Futures | Total |
|--------|------|---------|-------|
| L2 Updates | ~4.3GB | ~4.3GB | 8.6GB |
| Trades | ~150MB | ~150MB | 300MB |
| Snapshots | ~20MB | ~20MB | 40MB |
| **Daily Total** | **~4.5GB** | **~4.5GB** | **~9GB** |

### Storage Recommendations

- **30-day retention**: ~270GB raw data
- **90-day retention**: ~810GB (exceeds 500GB budget)
- **Cleanup policy**: Oldest data deleted when disk usage > 450GB threshold

### Compression Ratios

- L2 deltas: 8-12x (many repetitive prices)
- Trades: 6-10x (structured JSON)
- Overall: 7-10x compression with zstd

## Monitoring

### Log File

```bash
tail -f recorder.log
```

Key metrics to monitor:
- Gap frequency per symbol
- File rotation counts
- REST API latency
- Storage usage

### Check Recording Status

```bash
# Count recent messages per symbol
find ~/data_raw/binance_spot/L2_BOOK -type f -name "*.zst" | wc -l

# Check disk usage
du -sh ~/data_raw ~/data_nautilus

# Verify latest data timestamp
zstdcat ~/data_raw/binance_spot/L2_BOOK/BTCUSDT/2024-01-15/2024-01-15T23.jsonl.zst | jq '.ts_recv_ns' | tail -1
```

## Backtesting with Nautilus

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog

catalog = ParquetDataCatalog(path="~/data_nautilus")

# Query recorded data
bars = catalog.query_bars(
    instrument_id="BTCUSDT.BINANCE",
    bar_type="1H-BID",
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

quotes = catalog.query_l2_books(
    instrument_id="BTCUSDT.BINANCE",
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)

trades = catalog.query_trades(
    instrument_id="BTCUSDT.BINANCE",
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)
```

## Troubleshooting

### Issue: "x86_64-linux-gnu-g++ not found"

**Solution**: Install build tools
```bash
sudo apt-get install build-essential python3.12-dev
```

### Issue: WebSocket Connection Drops

**Solution**: Recorder auto-reconnects. Check logs:
```bash
grep "reconnect\|error" recorder.log | tail -20
```

### Issue: Disk Space Low

**Solution**: Check retention policy in config.yaml
```bash
# Manual cleanup
find ~/data_raw -type f -mtime +30 -delete
```

### Issue: Gaps in Data

**Solution**: Normal for high-frequency data. Gap detection automatically handles this.
Check gap stats:
```bash
grep "Gap detected" recorder.log | wc -l
```

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Adding New Symbols

Edit `config.yaml`:
```yaml
symbols:
  binance_spot:
    - BTCUSDT
    - ETHUSDT
    - NEW_SYMBOL_USDT  # Add here
```

### Custom Conversion Logic

Edit `converter.py` to customize Nautilus output format.

## Performance Tuning

1. **Reduce symbol count** if disk/bandwidth limited
2. **Increase rotation interval** (hourly → 4 hourly) for fewer files
3. **Lower retention** if disk space critical
4. **Disable snapshots** if not needed for faster sync

## References

- [Binance API Docs](https://binance-docs.github.io/apidocs/)
- [Binance WebSocket Streams](https://binance-docs.github.io/apidocs/spot/en/#web-socket-streams)
- [cryptofeed Documentation](https://github.com/bmoscon/cryptofeed)
- [NautilusTrader Documentation](https://nautilustrader.io/)

## License

See LICENSE file

## Support

For issues or questions:
1. Check the logs: `recorder.log`
2. Review config settings in `config.yaml`
3. Verify Binance API is accessible: `curl https://api.binance.com/api/v3/ping`

---

**Last Updated**: 2024-01-15  
**Version**: 1.0.0
