# Project Status

**Version:** `v1.1.0-dev`
**Last updated:** 2026-06-17

This document is the single source of truth for **what is validated** vs **what is
deferred** in CryptoRecorder. Keep it honest. Do not promote a deferred item to
validated without recorded evidence.

---

## Validated (works today)

- **Recorder** — Binance spot + USDT-M futures ingestion to `data_raw/`
  (deterministic-native depth_v2 + trade_v2). Stable.
- **`data_raw/ → convert_day.py → Nautilus full-L2 catalog`** — the reference
  production conversion path. This is the byte-for-byte validated path and must not
  regress.
- **`data_raw/ → replay_store`** (v0) — normalized deterministic Parquet replay layer
  (`stores/replay_*`, `pipeline/build_replay_store.py`).
- **`replay_store → feature_store`** (v0) — sparse, UTC-day-clamped feature rows
  (`stores/feature_*`, `pipeline/build_feature_store.py`).
- **`replay_store → generate_catalog --profile trades_only`** — produces a Nautilus
  catalog of trades that matches the reference converter for trades.

### Recorded validation evidence

**Trades-only catalog equivalence**

```
2026-06-12 BINANCE_SPOT/ADAUSDT trades_only:
  old trades            124457
  new trades            124457
  timestamp range match true
  sample mismatches     0
```

**Feature store audit**

```
2026-06-12 BINANCE_SPOT/ADAUSDT 1m:
  actual rows           1428
  expected dense rows   1440
  outside-date rows     0
  duplicate timestamps  0
  missing dense windows 12
```

The feature store is intentionally **sparse**: missing dense windows correspond to
minutes with no qualifying replay activity, not to data loss.

---

## Deferred (NOT done — do not claim otherwise)

- **`replay_store → generate_catalog --profile full_l2`** — the full order-book (L2)
  reconstruction path. Deferred until validated for semantic equivalence against
  `convert_day.py`. See [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md).
  This is the gate for `v2.0.0`.
- **Syncthing archive / backup** — `ARCHIVE_DAYS_ROOT` is a **placeholder** env path
  only. No archive code exists.
- **Label store** — `LABEL_ROOT` is a **placeholder** env path only. No label/target
  generation exists.
- **Import / restore tooling** — not implemented.

---

## Current recommended production behavior

1. Run the **recorder** continuously (`recorder.py`).
2. Convert the previous UTC day with **`convert_day.py --staging`** (the validated
   full-L2 path) shortly after 00:00 UTC.
3. Build the **replay store** and **feature store** for the previous day via
   `pipeline.daily_build` (replay then features).
4. For backtests needing trades, use `generate_catalog --profile trades_only`.
5. Do **not** rely on `full_l2` catalog generation from the replay store — use
   `convert_day.py` for full order-book catalogs until full-L2 is validated.

See [DEPLOYMENT.md](DEPLOYMENT.md) and [LINUX_SERVER.md](LINUX_SERVER.md) for the
service layout that runs these steps.
