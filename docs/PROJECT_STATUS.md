# Project Status

**Version:** `v1.1.0-dev`
**Last updated:** 2026-07-20

This document is the single source of truth for **what is validated** vs **what is
deferred** in CryptoRecorder. Keep it honest. Do not promote a deferred item to
validated without recorded evidence.

---

## Validated (works today)

- **Recorder** — Binance spot + USDT-M futures ingestion to `data_raw/`
  (deterministic-native depth_v2 + trade_v2). Stable.
- **Disk monitoring (fail-safe measurement)** — `disk_monitor.py` never reports a
  failed/timed-out directory-size scan as zero. Failures fall back to a persisted
  last-known-good value marked `stale`, or `null` if none exists;
  `state/disk_usage.json` exposes `monitoring_health` (healthy/degraded/unhealthy)
  and alerts; automatic cleanup fails closed on any untrusted `data_raw`
  measurement; filesystem capacity is reported independently via
  `shutil.disk_usage()`. See `docs/ARCHITECTURE.md` and `docs/OPERATIONS.md`.
  Covered by `tests/test_disk_monitor_fail_safe.py` and
  `tests/test_disk_monitor_cleanup.py`. Real-server verification (post-deploy
  log/report inspection) is pending — see deployment checklist in the PR.
- **`data_raw/ → convert_day.py → Nautilus full-L2 catalog`** — the reference
  production conversion path. This is the byte-for-byte validated path and must not
  regress.
- **`data_raw/ → replay_store`** (v0) — normalized deterministic Parquet replay layer
  (`stores/replay_*`, `pipeline/build_replay_store.py`). This is the stable
  external contract consumed by downstream repositories (e.g. KovacsTrader).
  CryptoRecorder does not build a feature/label layer or a general-purpose
  consumer catalog from it. `ReplayWriter` is memory-bounded (SQLite spool +
  incremental Parquet write); the service restart loop has been fixed
  (`Restart=no`); production RAM measurement against the DEXEUSDT partition is
  still pending on the production server.
- **`replay_store → validation.replay_catalog_reconstruct`** (validation-only,
  no CLI) — internal helper used only by
  `validation.validate_catalog_equivalence` to reconstruct a temporary Nautilus
  catalog from replay_store for equivalence checking. Supports `trades_only`
  (matches the reference converter for trades) and `full_l2` (full order-book
  reconstruction via the shared depth engine). **Semantically validated on the
  ADAUSDT smoke** against `convert_day.py`; broader top50/multi-day validation is
  still pending (see Deferred). This is the `v2.0.0` gate and `v2.0.0` is **not**
  declared. This helper is not a supported downstream runtime API.

### Recorded validation evidence

**Trades-only catalog equivalence**

```
2026-06-12 BINANCE_SPOT/ADAUSDT trades_only:
  old trades            124457
  new trades            124457
  timestamp range match true
  sample mismatches     0
```

**Full-L2 catalog equivalence (ADAUSDT smoke)**

```
2026-06-12 BINANCE_SPOT/ADAUSDT full_l2 (replay vs convert_day.py):
  trade_ticks         old 124457   new 124457   range match  0 mismatches
  order_book_deltas   old 1231284  new 1231284  range match  0 mismatches
  order_book_depth10  old 71341    new 71341    range match  0 mismatches
  book checkpoints    7/7 match, no crossed books
  status              passed
```

Report: `validation_reports/full_l2_equivalence_2026-06-12_ADAUSDT.json` (local,
gitignored). Reproduce with `python -m validation.validate_catalog_equivalence
--date 2026-06-12 --symbols ADAUSDT --venues BINANCE_SPOT --profile full_l2`.
This is a **single-symbol single-day smoke**, not a universe benchmark.

---

## Deferred (NOT done — do not claim otherwise)

- **Broader `full_l2` validation (top50 + multi-day)** — the replay full-L2
  reconstruction path is implemented and **passes the ADAUSDT single-day smoke**
  against `convert_day.py` (see evidence above), but it is **not** yet validated
  across the top50 universe or multiple days. `v2.0.0` remains **ungated** until
  that wider validation passes.
  See [FULL_L2_REPLAY_CATALOG_PLAN.md](FULL_L2_REPLAY_CATALOG_PLAN.md).
- **Syncthing archive / backup** — `ARCHIVE_DAYS_ROOT` is a **placeholder** env path
  only. No archive code exists.
- **Import / restore tooling** — not implemented.

CryptoRecorder does not own a feature-store or label-store subsystem (removed,
issue #17); `FEATURE_ROOT`, `LABEL_ROOT`, and `CATALOG_JOBS_ROOT` no longer
exist in `config.py`. That responsibility belongs to downstream consumer
repositories (e.g. KovacsTrader).

---

## Current recommended production behavior

1. Run the **recorder** continuously (`recorder.py`).
2. Convert the previous UTC day with **`convert_day.py --staging`** (the validated
   full-L2 path) shortly after 00:00 UTC.
3. Build the **replay store** for the previous day via `pipeline.daily_build`.
4. For downstream consumers needing a temporary Nautilus catalog rebuilt from
   replay_store for validation purposes, use
   `python -m validation.validate_catalog_equivalence --profile trades_only`
   (there is no product-facing `generate_catalog` CLI).
5. The replay `full_l2` reconstruction path passes the ADAUSDT single-day smoke, but
   `convert_day.py` remains the **production reference** for full order-book
   catalogs until broader top50/multi-day validation passes.

See [OPERATIONS.md](OPERATIONS.md) for the service layout that runs these steps.
