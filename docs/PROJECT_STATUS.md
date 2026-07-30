# Project Status

**Version:** `v1.1.0-dev`
**Last updated:** 2026-07-30

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
  ADAUSDT smoke with additional completed high-volume BTCUSDT schema-v2
  representative-day evidence** against `convert_day.py`; broader
  top50/multi-day validation is still pending (see Deferred). This is the
  `v2.0.0` gate and `v2.0.0` is **not** declared. This helper is not a supported
  downstream runtime API.

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

**Full-L2 catalog equivalence (BTCUSDT schema-v2 representative day)**

```
2026-06-11 BINANCE_SPOT/BTCUSDT full_l2 (schema-v2 replay vs convert_day.py):
  reference construction                         passed (preserved)
  replay construction/catalog reconstruction      passed (preserved)
  trade_ticks        old 3418712   new 3418712    exact, 0 mismatches
  order_book_deltas  old 30009655  new 30009655   exact, 0 mismatches
  order_book_depth10 old 84066     new 84066       exact, 0 mismatches
  book checkpoints   7/7 canonical hashes match
  continuity         seeds 7/7, resyncs 0/0, desyncs 0/0, fences 25/25
  fenced ranges      25/25, canonical digest match
  raw/replay metadata depth 846430/846430, trades 3419004/3419004, exact
  live source identity matches manifest + integrity copy (25 depth, 24 trade files)
  status             passed
```

The accepted external report has SHA-256
`69c4466d1a6cb4206110f07def6f9d9c2b751a65f6923bd270ac16956668c281`.
A compact, path-sanitized local summary is generated at
`validation_reports/issue20_phase7_btcusdt_spot_2026-06-11.json` (SHA-256
`aa3d116b269355e267058491562245a8e4f3e0add3182e653d48e59fbbf61782`);
`validation_reports/` is contractually gitignored. Catalogs, replay
partitions, raw data, large logs, and cgroup sample streams remain external.
The report reuses the preserved passing reference, replay, and trade
fragments; the hardened reader reran only deltas and produced the same passing
fragment SHA-256 as the accepted Round 5 result. Every new substantial stage
ran serially in its own 10 GiB cgroup with zero OOM events.

This is strong high-volume, single-symbol/single-day local development
evidence. It does **not** satisfy or narrow the broader top50/multi-day
`v2.0.0` gate, does not make schema v2 a production default, and does not
replace `convert_day.py` as the production reference.

**Full-L2 catalog equivalence (BTWUSDT corrected schema-v2 futures case)**

```
2026-06-11 BINANCE_USDTF/BTWUSDT full_l2:
  replay trade rows       1371217 (all identified; 45 zero-quantity)
  trade_ticks             old 1371172   new 1371172   exhaustive exact
  order_book_deltas       old 11507066  new 11507066  exhaustive exact
  order_book_depth10      old 40398     new 40398     exhaustive exact
  book checkpoints        7/7 canonical hashes match
  continuity/fences       exact (61 seeds, 3 resyncs, 0 desyncs, 249 fences)
  raw/replay metadata     exact (350157 depth, 1371217 trades)
  source identity         current raw = manifest = integrity copy
  routine/deep integrity  passed
  status                  passed
```

This result corrects the replay normalization defect exposed by the preserved
first BTW attempt: its raw native `trade.t` identifiers are now recovered
exactly, while anonymous trades fail before replay publication. The preserved
reference catalog was reused; only replay/candidate artifacts were rebuilt in
a fresh external directory. Peak cgroup memory was 2,371,661,824 bytes with
zero OOM/OOM-kill. Final report SHA-256:
`2ae29713f09dd10988566c10c3bb040ec55a0252d936a5e60e08032295af4d85`.

This is representative local development evidence for one futures
symbol/day, not a completed representative matrix, top50/full-universe,
multi-day, Tier-3, or production validation. The old v2.0.0 spot reports
remain valid historical evidence for their exact artifacts, but final
checkpoint evidence under the corrected v2.0.1 builder will require
intentional spot replay/candidate revalidation; those large spot cases were
not rebuilt in this correction round.

---

## Deferred (NOT done — do not claim otherwise)

- **Broader `full_l2` validation (top50 + multi-day)** — the replay full-L2
  reconstruction path is implemented and **passes the ADAUSDT single-day smoke
  plus the BTCUSDT schema-v2 representative day** against `convert_day.py` (see
  evidence above), but it is **not** yet validated across the top50 universe or
  multiple days. `v2.0.0` remains **ungated** until that wider validation passes.
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
5. The replay `full_l2` reconstruction path passes the ADAUSDT smoke and the
   BTCUSDT schema-v2 representative day, but `convert_day.py` remains the
   **production reference** for full order-book catalogs until broader
   top50/multi-day validation passes.

See [OPERATIONS.md](OPERATIONS.md) for the service layout that runs these steps.
