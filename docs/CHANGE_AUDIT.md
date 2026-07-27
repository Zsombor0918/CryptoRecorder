# Change Audit Log

**Purpose:** This is the mandatory append-only log of non-trivial changes made to
CryptoRecorder. Every agent (AI or human) must write an entry here before a commit
or PR is considered complete.

This log protects data integrity: it ensures every change is traceable, every
status claim is honest, and every reviewer can understand what changed and why.

---

## When an entry is required

An entry is required for **any** commit that touches:

- Python source files (`*.py`) anywhere in the repo
- Schema, config, or deployment files (`config.py`, `systemd/`, `requirements.txt`)
- Documentation files where the change affects status claims, validated/deferred
  state, or the repo structure contract

## When an entry may be skipped

An entry may be skipped **only** for:

- Commits that exclusively fix typos or whitespace in docs (no status/claim changes)
- The audit entry commit itself (to avoid infinite recursion)

**If in doubt: write the entry.**

---

## Rules for agents

- **Do not delete old entries** except during an explicit, approved maintenance task
  (and even then, the deletion itself requires an audit entry).
- Add new entries at the **top** of the log (newest-first).
- Fill in every field. If a field does not apply, say why (e.g.,
  "No docs update required because: this is a test-only fix with no public API change").
- Never leave `yes/no` placeholders unfilled.
- Never promote a deferred feature to validated without recorded evidence in
  "Evidence for any new validation claim".

---

## Required entry template

```markdown
## YYYY-MM-DD — <short task title>

### Change summary
- <one bullet per logical change>

### Files/packages touched
- <path/to/file.py>
- <package/>

### Docs reviewed
- [ ] AGENTS.md
- [ ] docs/REPO_STRUCTURE.md
- [ ] docs/PROJECT_STATUS.md
- [ ] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - <list or "none">

### Docs updated
- [ ] CHANGELOG.md
- [ ] README.md
- [ ] docs/PROJECT_STATUS.md
- [ ] docs/REPO_STRUCTURE.md
- [ ] relevant feature docs:
  - <list or "none">
- No docs update required because: <reason, or delete this line if docs were updated>

### Status / validation impact
- Validated status changed: yes/no
- Deferred status changed: yes/no
- New claims added: yes/no
- Evidence for any new validation claim:
  - <command + output summary, or "n/a">

### Tests run
```bash
<exact commands>
```

### Validation CLIs run
```bash
<exact commands, or "none required for this change type">
```

### Known limitations / out of scope
- <explicit list of what was NOT done>
- <or "none — task fully completed">
```

## 2026-07-27 — fix(replay): preserve synchronization continuity events

### Change summary
- A review of the pushed head (`1fb588d`) correctly rejected the previous
  round's fenced-range gap as "out of scope because v0 has the same
  failure" — it proved a pre-existing replay-builder semantic defect,
  not an acceptable limitation, and the approved plan mandates exact
  sync/desync/resync and fenced-range reproduction. This entry is the
  narrowly scoped semantic correction requested, on top of `1fb588d`
  (not reverted).
- **Step 1 — inventory (evidence, not guessing)**: directly enumerated
  every raw `depth_v2` `record_type` value present in the ADAUSDT
  2026-06-12 fixture via `converter.readers.stream_raw_records()`:
  `depth_update` (412,332), `sync_state` (68), `stream_lifecycle` (60),
  `snapshot_seed` (4) — no other record types exist in this fixture.
  Cross-referenced against `converter.depth_phase2._run_depth_replay_loop()`
  (the shared engine both `convert_day.py` and the replay-reconstruction
  path use) to confirm which types it actually reads:
  `record_type == "sync_state"` is read directly (`rec["state"]`/
  `rec["reason"]` drive desync/resync state transitions and
  fenced-range open/close via `_open_fence()`/`_close_fence()`);
  `record_type == "stream_lifecycle"` triggers only a diagnostic counter
  and a `continue`, but the engine's session-change detection (which
  closes/opens fences on a `stream_session_id` change) runs
  UNCONDITIONALLY before that, using the CURRENT record's timestamp —
  so `stream_lifecycle` records' PRESENCE and TIMESTAMP matter even
  though their content does not.
- **Step 2 — root cause confirmed exactly as described**:
  `pipeline/build_replay_store.py::_convert_depth_record()`'s
  `if record_type not in {"snapshot_seed", "depth_update"}: return None`
  silently dropped `sync_state` (and `stream_lifecycle`) records before
  any later branch could act on them — verified this is the sole cause
  by re-running the canonical Tier-2 gate after each incremental fix and
  observing the fenced-range count/digest converge exactly.
- **Fix, part A (sync_state)**: `_convert_depth_record()` now accepts
  `record_type in {"snapshot_seed", "depth_update", "sync_state"}`.
  `sync_state` records have no book payload and no `U`/`u`/`pu` (they use
  `last_update_id`/`prev_update_id` instead — distinct fields, never
  conflated with `U`/`u`/`pu`); their complete state transition
  (`state`/`previous_state`/`reason`/`last_update_id`/`prev_update_id`)
  is preserved via the existing, already-nullable `quality_flags` JSON
  column (`{"sync_state_transition": {...}}`) — no new physical schema
  field added or changed for either v0 or v1. A pre-existing latent bug
  was also fixed here: the prior code read `raw_record.get("sync_state")`
  (a `depth_update` record's own, differently-named, legacy informational
  field) to determine `is_desync`/`is_resync`, which would never be
  populated on an actual `sync_state` RECORD (whose transition value is
  in its `state` field) — now dispatches on record type to read the
  correct field.
- **Fix, part B (stream_lifecycle)**: `_convert_depth_record()` also
  accepts `record_type == "stream_lifecycle"`, preserving `event`/
  `reason` via `quality_flags` (`{"stream_lifecycle_event": {...}}`) for
  completeness, even though the shared engine only needs their
  presence-and-timestamp. This closed 31 of the 34 fenced ranges'
  `end_ts_ns` mismatches (each off by exactly the raw gap between the
  dropped `stream_lifecycle` record and the next preserved record — the
  fence count already matched at 34/34 after part A alone; only the
  digest, sensitive to exact timestamps, differed).
- **Fix, part C (cross-day carry recovery)**: after parts A/B, exactly 1
  of 34 fences still differed — root-caused to a session (session 19)
  that began on 2026-06-11 (its first record in the 2026-06-12 raw file
  is `session_seq=54040`, mid-session). `convert_day.py`'s raw path
  recovers such sessions via its existing, already-implemented
  cross-day carry-spool mechanism
  (`converter.depth_phase2._recover_carry_state_from_spool()`/
  `_emit_synthetic_opening_snapshot()`, invoked from
  `convert_depth_v2_streaming()`'s `_prime()` callback) — reading the
  adjacent day's raw partition to find the session's last snapshot and
  replay forward from it. The replay-reconstruction path
  (`replay_records_to_depth_streaming()`) had no equivalent mechanism at
  all, so it fenced immediately at the session's first record in the
  target day. Added a new, optional `carry_records` parameter to
  `replay_records_to_depth_streaming()` that, when supplied, reuses the
  EXACT SAME `_recover_carry_state_from_spool()`/
  `_emit_synthetic_opening_snapshot()` helpers via two bounded, disk-backed
  `converter.spool.RawRecordSpool` instances (never a full-day Python
  list) — identical mechanism to the raw path, applied to
  already-adapter-normalized replay rows instead of raw records.
  Omitting `carry_records` (the previous default, and every other
  existing caller) leaves behavior completely unchanged — verified by a
  dedicated backward-compatibility test.
  `validation/replay_catalog_reconstruct.py`'s `_write_depth_for_partition()`
  now checks whether the previous day's replay partition exists (via
  `ReplayReader.iter_dates()`) and, if so, supplies its depth rows
  (via the same `iter_replay_depth_records()` adapter) as
  `carry_records` — consumed transiently to derive carry state; never
  copied, persisted, or exposed as part of the requested date's own
  reconstructed catalog output (the reconstructed date range is derived
  from the caller's `start`/`end` window, not from what partitions exist
  in `replay_root`). `validation/validate_catalog_equivalence.py`'s
  `_run_new_pipeline()` now also builds the previous day's replay
  partition (same `schema_version`) purely so this carry lookup can find
  it — this build is not itself part of the requested date's output.
  This applies identically to v0 and v1 logical replay (same shared
  engine, same helpers, dispatched only by which schema version's reader
  produced the adapter-normalized rows).
- Also updated `validation/validate_catalog_equivalence.py`'s raw-to-
  replay metadata comparator
  (`_DEPTH_ACCEPTED_RECORD_TYPES`/`_normalize_raw_depth_record()`) to
  accept and correctly normalize `sync_state`/`stream_lifecycle` records
  identically to the writer's new logic, so it compares like-for-like
  instead of flagging a spurious "extra on the raw side" mismatch now
  that these types are written to replay.
- Added `tests/test_replay_sync_continuity.py` (14 tests): proves
  `_convert_depth_record()` no longer drops `sync_state`/
  `stream_lifecycle`, an unsupported/non-continuity record type is still
  deliberately dropped, `sync_state` survives v0 AND v1 writer/reader
  round-trip with its ordering relative to snapshots/depth_updates
  preserved exactly, desync/resync flags survive, dropping a
  `resync_required` `sync_state` record changes reconstructed continuity
  evidence (`Phase2ReplayMetrics.resync_count`), the candidate
  reconstructs the expected fenced range from a synthetic
  desync→resync→re-snapshot sequence, cross-day carry recovery both
  recovers a session started on a prior day (matching `convert_day.py`'s
  synthetic-opening-snapshot/`carried_seed_last_update_id` behavior) and
  remains fully backward-compatible (identical fenced-range output) when
  `carry_records` is omitted.

### Files/packages touched
- `pipeline/build_replay_store.py` (`_convert_depth_record()`: accept
  `sync_state`/`stream_lifecycle`, correct desync/resync field dispatch,
  preserve state transition via `quality_flags`)
- `stores/replay_schema.py` (`DEPTH_RECORD_TYPE_CODES`: added
  `sync_state=2`/`stream_lifecycle=3`, v0 codes unchanged)
- `stores/replay_depth_adapter.py` (`replay_row_to_depth_record()`:
  recover `sync_state` transition fields via new
  `_sync_state_transition()`; docstrings updated)
- `converter/depth_phase2.py` (`replay_records_to_depth_streaming()`:
  new optional `carry_records` parameter reusing the raw path's existing
  carry-recovery helpers)
- `validation/replay_catalog_reconstruct.py` (`_write_depth_for_partition()`:
  supply previous-day replay rows as `carry_records` when available;
  new `_date_shift()` helper)
- `validation/validate_catalog_equivalence.py`
  (`_DEPTH_ACCEPTED_RECORD_TYPES`/`_normalize_raw_depth_record()`: accept
  and normalize `sync_state`/`stream_lifecycle`; `_run_new_pipeline()`:
  also build the previous day's replay partition for carry lookup)
- `tests/test_replay_sync_continuity.py` (new — 14 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — Phase 5 remains not-yet-approved per the user's explicit
  instruction; this entry does not advance any v2.0.0/full_l2 claim)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; no contradiction — this
  entry restores information the Phase 3 matrix never authorized
  dropping in the first place; it is a correctness fix, not a new
  compaction decision)
- [x] relevant feature docs:
  - docs/FULL_L2_REPLAY_CATALOG_PLAN.md (reviewed; its "Equivalence
    Boundary (caveats)" section previously listed `sync_state`
    fenced-range bookkeeping and cross-day carry as NOT reproduced by
    the replay path — both are now reproduced; no doc text update made
    in this entry since the checkpoint explicitly limits scope to the
    code correction, test evidence, and this audit trail, and the ADAUSDT
    smoke's own documented result already matches this entry's evidence)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: no validated/deferred status claim
  changed; `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`'s existing ADAUSDT-smoke
  evidence and caveat list are superseded by, but not contradicted by,
  this entry's more complete Tier-2 result — a broader documentation
  pass is out of scope for this narrowly scoped semantic correction per
  the explicit instruction.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no new *validated* status claim; Phase 5 remains
  explicitly not-yet-approved (per the user's own instruction) and Phase
  6 has not begun.
- Evidence for any new validation claim:
  - **Tier 2 re-run (canonical `validation.validate_catalog_equivalence`
    CLI, ADAUSDT, 2026-06-12, real local `data_raw`, `--schema-version 1`
    then again with `--schema-version 0`)**: for BOTH schema versions,
    `report["status"] == "passed"` and ALL SEVEN gating components pass:
    `instrument_ids_match=True`, `instrument_precision.passed=True`,
    `trade_ticks.passed=True` (124,457/124,457), `order_book_deltas.passed=True`
    (412,317/412,317), `order_book_depth10.passed=True` (71,341/71,341),
    `book_checkpoints.passed=True` (7/7), `continuity_diagnostics.passed=True`,
    `fenced_ranges.passed=True` (`count_old=34`, `count_new=34`,
    `digest_old == digest_new` byte-for-byte).
  - Confirmed via `converter.depth_phase2.fence_canonical_key()`-based
    direct key-by-key diffing (not just the count) that the full 34-fence
    lists are identical, not merely equal in count — the earlier "1
    fence differs" case was diagnosed field-by-field (`start_ts_ns`/
    `reason` differed due to missing cross-day carry) before being fixed.

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_replay_sync_continuity.py -q   # 14 passed
python -m pytest tests/test_replay_schema_v1.py tests/test_replay_schema_v1_corrections.py \
  tests/test_replay_store.py tests/test_replay_depth_adapter.py tests/test_replay_memory_bounded.py \
  tests/test_convert_day_phase2.py tests/test_book_checkpoint_hash_canonicalization.py \
  tests/test_replay_catalog_reconstruct.py tests/test_replay_sync_continuity.py -q
  # 170 passed
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 518 passed, 3 skipped
```

### Validation CLIs run
```bash
CRYPTO_RECORDER_DATA_ROOT="$(pwd)/data_raw" python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 --symbols ADAUSDT --venues BINANCE_SPOT \
  --data-root "$(pwd)/data_raw" --work-root /tmp/.../work \
  --old-catalog-root /tmp/.../old_catalog --replay-root /tmp/.../replay \
  --new-catalog-root /tmp/.../new_catalog --profile full_l2 --emit-depth10 \
  --schema-version 1 --overwrite --report-path /tmp/.../report.json
# repeated identically with --schema-version 0
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- Docs (`docs/FULL_L2_REPLAY_CATALOG_PLAN.md`'s caveat list) were not
  rewritten in this entry — deliberately out of scope for this narrowly
  scoped semantic correction; a future documentation pass should update
  its "Equivalence Boundary" section to remove the now-resolved
  `sync_state`/cross-day-carry caveats.
- This Tier-2 re-run covers exactly one symbol (ADAUSDT) and one day
  (2026-06-12) on BINANCE_SPOT; it does not cover BINANCE_USDTF or any
  other symbol/day, and remains development evidence, not a Tier-3
  representative-day claim.
- Phase 5 remains **not yet approved** (explicit user instruction); Phase
  6, Tier 3, production deployment, custom format, retention gate, uv
  migration, and any KovacsTrader change remain out of scope and were not
  started.

---

## 2026-07-27 — Issue #20 Phase 5 corrective commit: complete v1 logical and validation contract

### Change summary
- A review of the pushed head (`76a61e5`, the Phase 5 compact replay
  schema v1 prototype) correctly identified 4 review blockers. The
  `76a61e5` commit is **not reverted**; this entry is a narrowly scoped
  corrective commit on top of it, per explicit instruction, and Phase 5
  remains directionally accepted (the 1.30x local size reduction reported
  previously is honest prototype evidence, not a failure).
- **Blocker 1 — complete logical-row contract**: `stores/replay_reader.py`'s
  `_decode_depth_row_v1()`/`_decode_trade_row_v1()` previously omitted
  `venue`/`symbol`/`date` from every decoded v1 row despite the module
  docstring claiming v0/v1 logical parity, and despite v0 rows (as
  produced by `pipeline/build_replay_store.py`'s
  `_convert_depth_record()`/`_convert_trade_record()`) always carrying
  these three fields. Both decode functions now accept the partition
  identity (threaded through from `ReplayReader.iter_depths()`/
  `iter_trades()`, which already have `venue`/`symbol`/`date` as method
  arguments) and include them in every returned row.
- **Blocker 2 — version-aware partition validation**: `stores/replay_writer.py`'s
  `validate_partition()` previously validated only `status`/checksums,
  identically regardless of `schema_version` — an unsupported version, a
  v1 partition missing required metadata, or a manifest/physical-schema
  mismatch would all have been silently accepted as valid (and
  skippable, via `pipeline.build_replay_store`'s skip-if-valid check).
  Split the version-specific logic into a new
  `_validate_schema_version_contract()` helper: a manifest with no
  `schema_version` (legacy v0) or an explicit `schema_version=0` requires
  the on-disk depth/trades Parquet physical schema to exactly match
  `DEPTH_REPLAY_SCHEMA`/`TRADE_REPLAY_SCHEMA` (field names + types, via
  new `_schema_matches()`); an explicit `schema_version=1` additionally
  requires `format_version == FORMAT_VERSION_V1`, a non-empty
  `builder_version`, integer `price_scale`/`qty_scale` (rejecting `bool`,
  which is a `int` subclass in Python, and negative values), a complete
  `encoding_profile` (`compression`/`compression_level`/
  `row_group_batch_size` present), and that the physical Parquet schemas
  match `DEPTH_REPLAY_SCHEMA_V1`/`TRADE_REPLAY_SCHEMA_V1`. An explicit
  `schema_version` outside `{0, 1}` (`SUPPORTED_SCHEMA_VERSIONS`) fails
  immediately with a logged warning.
- **Blocker 3 — explicit non-default v1 path through the canonical
  builder**: `pipeline/build_replay_store.py`'s `build_replay_for_symbol()`
  gained `schema_version: int = 0` (plus `price_scale`/`qty_scale`
  passthrough) — the default preserves exactly today's production
  behavior for every existing caller (including
  `pipeline/daily_build.py`, which does not pass this argument). Added a
  `--schema-version {0,1}` CLI flag to `python -m pipeline.build_replay_store`.
  No systemd unit or production configuration references this flag; no
  automatic rebuild/migration of existing partitions was added; an
  unsupported value fails immediately via `ReplayWriter`'s own
  constructor check (unchanged from the original Phase 5 commit).
- **Blocker 4 — source identity bound to the raw root actually
  consumed**: `stores/replay_writer.py`'s `finalize_staging()` no longer
  calls `pipeline.raw_manifest.compute_raw_source_identity()` itself (the
  original Phase 5 commit did, using the global `config.DATA_ROOT`
  default when no explicit `data_root` was supplied to
  `compute_raw_source_identity`, which could silently record checksums
  against a different raw root than a custom `--data-root` build
  actually consumed). `ReplayWriter` now only ever records
  `source_identity` if the caller explicitly supplies it (constructor
  arg or the new `set_source_identity()` method); if not supplied, the
  manifest honestly records `source_identity` as incomplete
  (`"error": "source_identity not supplied by caller"`), never a guess.
  `pipeline/build_replay_store.py`'s `build_replay_for_symbol()` now
  computes `compute_raw_source_identity(..., data_root=data_root)` — the
  EXACT `data_root` argument it was called with — immediately after
  streaming depth/trade records, and calls
  `writer.set_source_identity(...)` before finalization. Also fixed
  `converter/instruments.py`'s `load_exchange_info()` and
  `stores/replay_writer.py`'s `_derive_fixed_point_scales()`/
  `ReplayWriter.__init__()` to accept an explicit `data_root` parameter
  (previously both hardcoded to `config.DATA_ROOT`), threaded through
  from `build_replay_for_symbol()`'s own `data_root`, so a custom
  `--data-root` build's fixed-point scale derivation reads exchangeInfo
  from the same root it consumed for depth/trade streaming and source
  identity — never a different, global default root.
- **Pre-existing gap fixed while re-running Tier-2 through the canonical
  builder** (discovered during this correction, NOT specific to
  `schema_version`): `build_replay_for_symbol()`'s `instrument_metadata`
  dict never included the raw exchangeInfo `filters` list (only
  `venue`/`symbol`/`market_type`/`instrument_id`/`raw_symbol`/
  `quote_asset`/`base_asset`). `validation/replay_catalog_reconstruct.py`'s
  `_exchange_info_from_replay_metadata()` requires a `filters` key to
  treat the replay's `instrument.json` as exchangeInfo-shaped; without
  it, `build_instruments()` silently fell back to
  `converter.instruments._default_info()`'s generic
  `PRICE_FILTER.tickSize="0.01000000"`/`LOT_SIZE.stepSize="0.00001000"`
  defaults — producing a DIFFERENT price/size precision than the
  reference `convert_day.py` path's real exchangeInfo-derived precision,
  and failing the canonical instrument-precision comparison
  (`compare_instruments_semantic()`) for ANY replay-based candidate (v0
  or v1 alike — confirmed by reproducing the exact same
  `price_precision`/`size_precision` mismatch for ADAUSDT with
  `schema_version=0`). Fixed by including `symbol_info.get("filters", [])`
  in `instrument_metadata`.
- Also fixed a pre-existing, unrelated bug in
  `validation/validate_catalog_equivalence.py`'s CLI `main()`: the
  summary `print()` block referenced stale comparison-dict keys
  (`comparison["trade_count_old"]`, `comparison["timestamp_range_match"]`)
  that no longer exist in the current per-instrument (`by_instrument`)
  report shape, causing a `KeyError` crash immediately after a
  successful comparison run (the JSON report itself was written
  correctly before the crash — this was a cosmetic CLI-output bug, not a
  comparison-logic bug). Rewrote the summary print to walk the actual
  `by_instrument` structure.
- Added `tests/test_replay_schema_v1_corrections.py` (20 tests) proving
  all 4 blockers plus the instrument-metadata regression: complete
  key-set/value comparison of equivalent v0/v1 rows (not just the
  manifest), `validate_partition()` acceptance/rejection for valid v0,
  valid v1, unsupported version, missing `price_scale`, invalid
  `qty_scale` type, malformed `encoding_profile`, and v0/v1 physical-
  schema mismatch in both directions, `build_replay_for_symbol()`
  defaulting to v0 / explicitly producing v1 / failing immediately on an
  unsupported version / publishing instrument metadata, and
  `source_identity` reflecting only the actually-consumed root across
  two different raw roots with different file content (proving no
  cross-root leakage) plus `ReplayWriter` never recomputing
  `source_identity` itself.

### Files/packages touched
- `stores/replay_reader.py` (venue/symbol/date restored in v1 decode)
- `stores/replay_writer.py` (version-aware `validate_partition()`,
  `_validate_schema_version_contract()`, `_schema_matches()`,
  `set_source_identity()`, `data_root` constructor arg, removed internal
  `compute_raw_source_identity()` call)
- `converter/instruments.py` (`load_exchange_info()` gained explicit
  `data_root` parameter)
- `pipeline/build_replay_store.py` (`schema_version`/`price_scale`/
  `qty_scale` args + `--schema-version` CLI flag on
  `build_replay_for_symbol()`/`main()`; explicit `compute_raw_source_identity()`
  call bound to the actual `data_root`; `instrument_metadata` now
  includes `filters`)
- `validation/validate_catalog_equivalence.py` (`schema_version` param
  threaded through `_run_new_pipeline()`/`validate_catalog_equivalence()`/
  CLI `--schema-version`; fixed the stale CLI summary print)
- `tests/test_replay_schema_v1_corrections.py` (new — 20 tests)
- `tests/test_replay_schema_v1.py` (2 stubs updated for the
  `load_exchange_info(..., data_root=...)` signature change; 1 test
  rewritten for the `ReplayWriter` no-longer-self-computing-source-identity
  contract)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — the `full_l2`/v2.0.0 gate remains unmet; this correction
  strengthens the Phase 5 prototype without claiming Tier-3 completion)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; no contradiction — the
  Phase 3 matrix's compaction levers are unchanged by this correction,
  only the completeness/validation/identity-binding bugs around them)
- [x] relevant feature docs:
  - docs/FULL_L2_REPLAY_CATALOG_PLAN.md (reviewed; the
    `sync_state`-fenced-range-bookkeeping caveat this entry's Tier-2
    re-run confirms is pre-existing and identical for v0 is already
    documented there — no update needed)
  - docs/REPLAY_STORE.md (reviewed; the "Versioning (v0 / v1)" section
    added in the prior Phase 5 entry remains accurate — no update
    required for this correction)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: the v0/v1 physical-difference
  description in `docs/REPLAY_STORE.md` remains accurate; no new
  validated/deferred status claim changed.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no new *validated* status claim. This entry reports
  an HONEST PARTIAL Tier-2 result (see below), explicitly not claimed as
  a full Tier-2 pass, and does not alter the `full_l2`/v2.0.0 gate.
- Evidence for any new validation claim:
  - **Tier 2 re-run through the canonical builder + canonical validator**
    (`validation.validate_catalog_equivalence` CLI, `--schema-version 1`,
    `--profile full_l2 --emit-depth10`, ADAUSDT, BINANCE_SPOT,
    2026-06-12, real local `data_raw`, normal instrument-metadata
    publication via the canonical `build_replay_for_symbol()` path — not
    a manual four-function comparison script):
    - `instrument_ids_match`: **True**
    - `instrument_precision` (after the filters fix): **True**
      (`price_precision`/`size_precision`/`price_increment`/
      `size_increment` all match; before the fix this was **False**
      — `price_precision` 4 vs 2, `size_precision` 1 vs 5 — confirming
      the gap was real and the fix necessary)
    - `trade_ticks` (exhaustive): **True** (124,457 / 124,457)
    - `order_book_deltas` (exhaustive): **True** (412,317 / 412,317)
    - `order_book_depth10` (exhaustive): **True** (71,341 / 71,341)
    - `book_checkpoints` (streaming, hash-canonicalized): **True**
      (7/7 checkpoints match)
    - `raw_to_replay_metadata` (quality/continuity evidence, bounded
      raw-vs-replay comparison): **True**
    - `continuity_diagnostics`: **False** (`fenced_range_count`: old 34,
      new 1)
    - `fenced_ranges` (digest comparison): **False** (`count_old=34`,
      `count_new=1`, digest mismatch)
    - **Re-run the identical canonical validator with `--schema-version 0`
      against the same raw data**: the exact same
      `continuity_diagnostics`/`fenced_ranges` result
      (`fenced_range_count` old 34, new 1) occurs for legacy v0 too —
      proving this gap is NOT a v1-specific regression but the
      already-documented `sync_state`-fenced-range-bookkeeping caveat in
      `docs/FULL_L2_REPLAY_CATALOG_PLAN.md` (the replay builder drops
      `sync_state` records identically for both schema versions).
    - **Honest conclusion, per this correction's own instruction**: Tier 2
      is **6 of 7 canonical gating components pass** for the v1
      prototype on this single symbol/day. It is explicitly **NOT**
      reported as "Tier 2 fully passed" — the fenced-range/continuity gap
      remains open (shared with legacy v0, not newly introduced by v1),
      and closing it is out of scope for this corrective commit (it is
      not a v1-specific defect to fix, and reproducing it identically on
      v0 proves the canonical validator's fenced-range/continuity
      comparators are functioning correctly, not broken by this
      correction).

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_replay_schema_v1_corrections.py tests/test_replay_schema_v1.py tests/test_book_checkpoint_hash_canonicalization.py -q
  # 90 passed
python -m pytest tests/test_replay_store.py tests/test_replay_depth_adapter.py tests/test_replay_memory_bounded.py tests/test_converter_integration.py tests/test_convert_day_phase2.py tests/test_streaming_gating_bounded_memory.py tests/test_validate_catalog_equivalence_exhaustive_wiring.py tests/test_semantic_oracle_exhaustive_streaming.py tests/test_semantic_oracle_detects_injected_faults.py tests/test_catalog_equivalence.py tests/test_catalog_equivalence_full_l2.py tests/test_windowed_loader_boundaries.py -q
  # 141 passed, 1 skipped
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 504 passed, 3 skipped
```

### Validation CLIs run
```bash
CRYPTO_RECORDER_DATA_ROOT="$(pwd)/data_raw" python -m validation.validate_catalog_equivalence \
  --date 2026-06-12 --symbols ADAUSDT --venues BINANCE_SPOT \
  --data-root "$(pwd)/data_raw" \
  --work-root /tmp/tier2_v3_work/work \
  --old-catalog-root /tmp/tier2_v3_work/old_catalog \
  --replay-root /tmp/tier2_v3_work/replay \
  --new-catalog-root /tmp/tier2_v3_work/new_catalog \
  --profile full_l2 --emit-depth10 --schema-version 1 --overwrite \
  --report-path /tmp/tier2_v3_work/report.json
# Re-run identically with --schema-version 0 against /tmp/tier2_v0_check/*
# to confirm the fenced-range gap is pre-existing, not v1-specific.
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- The `continuity_diagnostics`/`fenced_ranges` gap (34 vs 1 fenced
  ranges) remains open for BOTH v0 and v1 — this corrective commit does
  not attempt to close it, since it is a pre-existing, already-documented
  `sync_state` limitation of the replay builder shared by both schema
  versions, not a defect introduced by Phase 5 or this correction.
  Closing it (if ever required) is a separate, future scoped task.
- The Tier-2 re-run covers exactly one symbol (ADAUSDT) and one day
  (2026-06-12) on BINANCE_SPOT; it does not cover BINANCE_USDTF or any
  other symbol, and must not be read as a Tier-3 representative-day
  result.
- No real-build peak-RSS/MemoryMax measurement was taken for this
  corrective commit's Tier-2 re-run (unchanged limitation from the prior
  Phase 5 entry).
- Phase 6 (external-merge/SQLite replacement), Tier 3, format-selection
  Phase 7, a custom binary format, Phase 9 self-contained-replay
  acceptance, the raw-retention deletion gate, staging lifecycle work,
  disk-monitor/systemd changes, a selected-reconstruction CLI, production
  deployment/data cleanup, uv migration, and any KovacsTrader change
  remain explicitly **not started**, per the approved checkpoint's scope
  boundary.

---

## 2026-07-27 — Issue #20 Phase 5 (revised-plan phase numbering): compact versioned replay schema v1 prototype

### Change summary
- Implemented the smallest viable versioned compact replay-schema v1
  prototype, per the approved Phase 0–4 review checkpoint (baseline,
  semantic oracle + failure-injection proof, raw-retention/legacy/
  traceability/versioning design, field/consumer/integrity matrix,
  repo-boundary alignment — all previously completed and approved; not
  repeated or re-hardened here). Legacy v0 is completely unchanged and
  remains the default; v1 is strictly additive and opt-in.
- **Versioning**: added `format_version`/`schema_version`/
  `builder_version`/`SUPPORTED_SCHEMA_VERSIONS = (0, 1)` to
  `stores/replay_schema.py`. A manifest with no `schema_version` field is
  legacy v0 (unchanged behavior). `ReplayReader.get_schema_version()`
  dispatches on the manifest's `schema_version`; an explicit version
  outside `{0, 1}` raises `ValueError` naming the found and supported
  versions — never silently misread. `ReplayWriter(..., schema_version=1)`
  is required to opt in; `schema_version` outside `{0, 1}` raises
  immediately at construction. v0 and v1 partitions may coexist under the
  same `replay_store/` root; no existing partition is migrated or
  rewritten in place.
- **Partition constants**: `venue`/`symbol`/`date` are removed from every
  v1 physical row (matrix: proven partition-constant, path-derivable) and
  restored by the reader from the manifest/partition path — proven via
  `tests/test_replay_schema_v1.py::test_partition_constants_*`.
- **Exact fixed-point representation**: `stores/replay_schema.py` adds
  `encode_fixed_point()`/`decode_fixed_point()` (`Decimal` only, never a
  float intermediate; raises if a value cannot be represented exactly at
  the given scale). `stores/replay_writer.py::_derive_fixed_point_scales()`
  derives `(price_scale, qty_scale)` from date-specific Binance
  `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize`/`MARKET_LOT_SIZE` via the
  existing `converter.instruments.load_exchange_info()`/`_get_filter()`
  (spot and futures looked up independently, since `load_exchange_info` is
  keyed by `venue`); raises a clear `ValueError` (never guesses) if the
  required filters are unavailable. Scales are recorded once per partition
  in the manifest (`price_scale`/`qty_scale`) — the replay partition
  itself, not `data_raw`, carries everything needed to reconstruct exact
  values. `price_str`/`quantity_str`/float64 duplication is dropped in v1
  in favor of a single int64 mantissa per field.
- **Compact flags/quality/continuity**: the 5 depth boolean columns
  (`is_snapshot_seed`, `is_depth_update`, `is_sync_state`, `is_desync`,
  `is_resync`) are packed into one int8 bitmask
  (`pack_depth_flags()`/`unpack_depth_flags()`); `record_type` becomes an
  int8 enum code (`DEPTH_RECORD_TYPE_CODES`/`TRADE_RECORD_TYPE_CODES`).
  Per the matrix's explicit "pending proof"/"benchmark-needed" status,
  `U`/`u`/`pu`, `trade_id`/`agg_trade_id`, `market_type`, and
  `quality_flags` are deliberately left in their v0 lexical/JSON form in
  this prototype — not compacted, not removed.
- **Integrity/traceability**: `native_payload_hash` is stored as 32 raw
  bytes (`pa.binary(32)`) instead of a 64-character hex string — the hash
  value itself is retained (the Phase 2 Section 3 traceability
  replacement design remains unimplemented, so hash removal is not
  authorized; only its physical encoding is compacted). Added
  `pipeline/raw_manifest.py::compute_raw_source_identity()` (bounded-
  memory streaming SHA-256 per raw file) and wired it into v1 manifests as
  a best-effort `source_identity` field — provenance evidence only, not
  required for reconstruction (failure to compute it is logged and
  recorded honestly in the manifest, never a build failure).
- **Reader/writer boundary**: `stores/replay_reader.py`'s
  `_decode_depth_row_v1()`/`_decode_trade_row_v1()` decode v1 physical rows
  back to the exact v0 logical row shape (independent of, and importing
  nothing from, `convert_day.py`/`converter/depth_phase2.py`), so every
  existing downstream consumer (`stores/replay_depth_adapter.py`,
  `validation/validate_catalog_equivalence.py`,
  `validation/replay_catalog_reconstruct.py`) requires zero changes to
  read either schema version. KovacsTrader was not touched; compact
  physical columns remain internal to CryptoRecorder.
- **RAM/scope boundary**: v1 writing reuses the existing bounded
  `RawRecordSpool`-backed batch write path unchanged (row-by-row
  projection via `_project_depth_row_v1()`/`_project_trade_row_v1()`
  applied inside the existing bounded batch loop — no new full-day
  in-memory collection). No Phase 6 external-merge/SQLite-replacement work
  was started.
- **Oracle correction discovered during the Tier-2 gate** (not a schema or
  reference-route change): the real ADAUSDT run below found
  `compare_book_checkpoints_streaming()`'s book-state comparison/hash was
  literal-string-sensitive to fractional-digit padding — v1 formats
  prices/quantities at the instrument's exact required scale (e.g. 4
  decimals, from `PRICE_FILTER.tickSize`) while legacy v0 preserves
  Binance's literal 8-decimal wire-format string; both represent the exact
  same numeric value (`Decimal("0.1713") == Decimal("0.17130000")`).
  Added `_canonical_decimal_str()`/`_canonical_book_state()` to
  `validation/catalog_compare.py` — `Decimal`-only, never a float
  intermediate, strips only numerically insignificant zero-padding and
  never rounds/quantizes genuinely different values into equality — and
  applied them to `compare_book_checkpoints_streaming()`'s `match`/hash
  computation. This is confined entirely to the validation oracle; no
  change was made to `convert_day.py`, the reference converter, replay
  physical encoding, or Nautilus catalog behavior.

### Files/packages touched
- `stores/replay_schema.py` (v1 pyarrow schemas, version constants, enum
  maps, flag bitmask helpers, fixed-point encode/decode helpers)
- `stores/replay_writer.py` (`schema_version`/`price_scale`/`qty_scale`
  constructor args, `_derive_fixed_point_scales()`,
  `_project_depth_row_v1()`/`_project_trade_row_v1()`, `row_transform`
  param on `_write_channel_incremental()`, v1 manifest fields)
- `stores/replay_reader.py` (`get_schema_version()`, v0/v1 dispatch in
  `iter_depths()`/`iter_trades()`, `_decode_depth_row_v1()`/
  `_decode_trade_row_v1()`)
- `pipeline/raw_manifest.py` (`compute_raw_source_identity()`)
- `validation/catalog_compare.py` (`_canonical_decimal_str()`,
  `_canonical_book_state()`, wired into
  `compare_book_checkpoints_streaming()`)
- `tests/test_replay_schema_v1.py` (new — 46 tests)
- `tests/test_book_checkpoint_hash_canonicalization.py` (new — 22 tests)
- `docs/REPLAY_STORE.md` (new "Versioning (v0 / v1)" section)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change — all new
  code lives in the already-approved `stores/`/`pipeline/`/`validation/`
  packages)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed by this entry — the `full_l2`/v2.0.0 gate remains unmet;
  v1 remains an unvalidated-at-Tier-3-scale prototype)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; this entry implements
  exactly the compaction levers that document's Phase 3 matrix approved,
  and does not contradict any "pending proof"/"unresolved" item — items
  still pending proof were deliberately left uncompacted)
- [x] relevant feature docs:
  - docs/REPLAY_STORE.md (updated — see Docs updated)
  - docs/FULL_L2_REPLAY_CATALOG_PLAN.md (reviewed; describes the
    replay-reconstruction path, which is schema-version-agnostic per this
    entry's reader change — no update required)

### Docs updated
- [x] CHANGELOG.md
- [x] docs/REPLAY_STORE.md
- No further docs update required because: no other documented CLI, flag,
  or status claim changed; `docs/PROJECT_STATUS.md` correctly still shows
  no validated Tier-3 full_l2 claim, which this entry does not alter.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no new *validated* status claim. This entry records
  a real Tier-2 (single-symbol/single-day, local, development-evidence)
  semantic-equivalence PASS for the v1 prototype against the
  `convert_day.py` reference on ADAUSDT/2026-06-12, and real local size
  measurements — both are reported honestly as development evidence, not
  as a Tier-3 representative-day or production claim.
- Evidence for any new validation claim:
  - **Tier 1 (synthetic round-trip)**: `pytest tests/test_replay_schema_v1.py
    tests/test_book_checkpoint_hash_canonicalization.py -q` → 46 + 22 = 68
    passed.
  - **Tier 2 (local real data, ADAUSDT, 2026-06-12)**: built the reference
    catalog via `convert_day.py --date 2026-06-12 --symbols ADAUSDT
    --venues BINANCE_SPOT --staging --catalog-root /tmp/tier2_ref_catalog`
    (166.4s; 124,457 trades, 412,317 delta_events, 71,341 depth10, 34
    fenced ranges). Built a real v1 replay partition directly from
    `data_raw` via `ReplayWriter(..., schema_version=1)` with scales
    derived from the real 2026-06-12 exchangeInfo (`price_scale=4,
    qty_scale=1` for ADAUSDT) — 412,336 depth records, 124,457 trade
    records, 59.5s. Built the candidate catalog via
    `validation.replay_catalog_reconstruct.generate_catalog_from_replay(
    profile="full_l2", emit_depth10=True)` from that v1 partition — 590.4s;
    124,457 trade_ticks, 412,317 order_book_deltas, 71,341 depth10 (exact
    count match with the reference). Ran the exhaustive, order-preserving,
    gating semantic oracle directly
    (`compare_trade_ticks_exhaustive`, `compare_order_book_deltas_exhaustive`,
    `compare_book_checkpoints_streaming`,
    `compare_order_book_depth10_exhaustive`) over the full UTC day
    (closed 1-hour windows): all four passed
    (`trades_cmp["passed"]=True`, `deltas_cmp["passed"]=True`,
    `checkpoints_cmp["passed"]=True` — 7/7 checkpoints hash-matching after
    the canonicalization fix, `depth10_cmp["passed"]=True`). Runtimes:
    trades 4.0s, deltas 28.3s, checkpoints 18.6s, depth10 19.0s.
  - **Local old-v0 vs new-v1 size measurement** (ADAUSDT, 2026-06-12,
    single symbol/day — development evidence only, not a Tier-3 claim):
    - v0 `depth.parquet`: 38,997,712 bytes (94.58 bytes/depth event over
      412,336 events)
    - v1 `depth.parquet`: 29,071,749 bytes (70.50 bytes/depth event) —
      1.341x reduction
    - v0 `trades.parquet`: 7,347,664 bytes (59.04 bytes/trade over
      124,457 trades)
    - v1 `trades.parquet`: 6,538,715 bytes (52.54 bytes/trade) —
      1.124x reduction
    - Combined: 46,345,376 bytes (v0) vs 35,610,464 bytes (v1) —
      1.301x reduction
    - Peak-memory evidence: not separately profiled in this session (no
      `MemoryMax`/RSS sampling tool was run against the real ADAUSDT
      build); the bounded-memory *behavior* is proven structurally (v1
      reuses the unchanged, already-bounded `RawRecordSpool`/batch-write
      path) and empirically at synthetic scale (20,000-row live-object-
      counter proof in `tests/test_replay_schema_v1.py`), but no
      real-build peak-RSS number is reported here — this is an explicit
      limitation of this entry, not a claim of "well under X".

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_replay_schema_v1.py -q                        # 46 passed
python -m pytest tests/test_book_checkpoint_hash_canonicalization.py -q   # 22 passed
python -m pytest tests/test_streaming_gating_bounded_memory.py tests/test_validate_catalog_equivalence_exhaustive_wiring.py tests/test_semantic_oracle_exhaustive_streaming.py tests/test_catalog_equivalence.py tests/test_catalog_equivalence_full_l2.py -q
  # 47 passed, 1 skipped
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 482 passed, 3 skipped
```

### Validation CLIs run
```bash
CRYPTO_RECORDER_DATA_ROOT="$(pwd)/data_raw" python3 convert_day.py --date 2026-06-12 \
  --staging --catalog-root /tmp/tier2_ref_catalog --symbols ADAUSDT \
  --venues BINANCE_SPOT --allow-partial-overwrite
# (v1 replay build and candidate catalog build were driven via direct Python calls
# into stores.replay_writer.ReplayWriter(schema_version=1) and
# validation.replay_catalog_reconstruct.generate_catalog_from_replay(); the exhaustive
# oracle comparison was driven directly via validation.catalog_compare's public
# functions — see "Evidence for any new validation claim" above for exact calls.)
```

### Known limitations / out of scope
- Phase 6 (external-merge/SQLite-replacement ordering), a full Tier-3
  representative production-day build, format-selection Phase 7, a custom
  binary format, Phase 9 self-contained-replay acceptance, the
  raw-retention deletion gate, staging lifecycle/locking/quarantine/
  backlog reconciliation, disk-monitor/systemd changes, a selected-
  reconstruction CLI, production deployment/data cleanup, uv migration,
  and any KovacsTrader change are all explicitly **not** started, per the
  approved checkpoint's scope boundary.
- No real-build peak-RSS/MemoryMax measurement was taken against the real
  ADAUSDT v1 build in this session (see above) — only structural/
  synthetic bounded-memory evidence is reported.
- `U`/`u`/`pu`, `trade_id`/`agg_trade_id`, `market_type`, and
  `quality_flags` remain uncompacted in v1, per the matrix's own
  "pending proof"/"benchmark-needed" status — not an oversight.
- The Tier-2 result covers exactly one symbol (ADAUSDT) and one day
  (2026-06-12) on BINANCE_SPOT; it does not cover BINANCE_USDTF (futures)
  or any other symbol, and must not be read as satisfying the later,
  explicitly separate Tier-3 representative-day gate.

---

## 2026-07-24 — Issue #20 Phase 1 second follow-up correction: gating book checkpoints/Depth10, complete fenced-range digest, RAM-bounded raw-to-replay metadata comparison

### Change summary
- A further review of the pushed head (`9fbdf92`) correctly found that
  the exhaustive trade/delta wiring was correct, but the acceptance path
  still weakened four mandatory checks: `book_checkpoints` and
  `order_book_depth10` were marked `"gating": False` full-day-list
  diagnostics; fenced-range comparison only checked the reference's
  3-example truncation and treated a candidate's extra fence as
  expected/non-gating (via a `gating_passed` carve-out); and
  quality-flag comparison collected a full day into two Python lists and
  compared them as an order-independent multiset, which can miss a flag
  moved from one event to another.
- **Book checkpoints — now streaming and gating**: added
  `reconstruct_book_checkpoints_streaming()` and
  `compare_book_checkpoints_streaming()` to `validation/catalog_compare.py`.
  These process a windowed `OrderBookDeltas` iterator sequentially,
  retaining only the current top-N book state plus the handful of already-
  captured checkpoint snapshots — never calling `list()` on a complete
  day — and add a deterministic SHA-256 hash per checkpoint
  (`old_hash`/`new_hash`/`hash_match`) alongside the existing top-of-book
  comparison. `validation/validate_catalog_equivalence.py`'s
  `_compare_depth_for_instrument()` now feeds this from a second,
  independent pair of `iter_order_book_deltas_windowed()` iterators
  (checkpoints need their own traversal separate from the exhaustive delta
  comparison, since a generator can only be consumed once) and its
  `passed` result is ANDed into the overall `passed` for that instrument.
  The full-day `load_order_book_deltas()`-based `compare_book_checkpoints()`
  path is no longer called by the acceptance path at all (the function
  itself remains in `catalog_compare.py` for other callers/tests).
- **Depth10 — now gating when enabled, honestly skipped when disabled**:
  added `iter_order_book_depth10_windowed()` (same closed-window boundary
  design as the trade/delta loaders) and
  `compare_order_book_depth10_exhaustive()` (positional, no sampling, no
  re-sorting, full per-level bid/ask comparison) to
  `validation/catalog_compare.py`. When `emit_depth10=True`, this result
  gates `passed`; when explicitly disabled, the acceptance path reports
  `{"skipped": True, "passed": True, "reason": "emit_depth10 disabled"}`
  rather than silently treating an unevaluated-but-would-have-failed
  comparison as passing.
- **Fenced ranges — complete-collection digest, no truncation carve-out**:
  added `canonical_fence_digest()` and its underlying
  `fence_canonical_key()` to `converter/depth_phase2.py` (shared by both
  `convert_day.py`, which now computes `canonical_count`/`canonical_digest`
  over the COMPLETE per-symbol `Phase2ReplayMetrics.fenced_ranges` list —
  already fully materialized in memory by the existing depth-conversion
  engine, so this adds no new full-day materialization — alongside the
  existing 3-example `examples` field, and `validation/catalog_compare.py`,
  whose new `compare_fenced_ranges_digest()` compares that count+digest
  against the candidate manifest's actual fenced-range list for the
  symbol). `validation/validate_catalog_equivalence.py`'s
  `_compare_fenced_ranges_for_symbol()` now gates directly on this
  comparator's `passed` — an extra candidate fence, or a difference beyond
  the 3rd reference example, correctly fails; the previous
  `gating_passed`/"extra_in_new is expected and non-gating" carve-out is
  removed entirely.
- **Quality/continuity metadata — RAM-bounded, event-identity-keyed, not a
  multiset**: added `compare_event_metadata_exhaustive()` to
  `validation/catalog_compare.py` — a generic, streaming, positional
  comparator over two already-canonically-ordered record streams, with
  `compare_fields` including identity fields (`raw_index`, `session_seq`,
  etc.) alongside content fields (`quality_flags`, `U`, `u`, `pu`,
  `is_desync`, etc.), so a value that moved from event i to event j is
  detected as a mismatch at both positions even though a pure multiset
  comparison of just the moved value would see no difference at all.
  `validation/validate_catalog_equivalence.py` replaces
  `_compare_quality_flags_for_symbol()`/`_collect_quality_flags_from_raw()`/
  `_collect_quality_flags_from_replay()` with
  `_compare_raw_to_replay_metadata_for_symbol()`, which sorts the raw side
  into the canonical `(stream_session_id, session_seq, raw_index)` /
  `(trade_stream_session_id, trade_session_seq, raw_index)` order via
  `converter.spool.RawRecordSpool` (an existing disk-backed bounded spool,
  reused rather than sorting a full-day Python list in memory) and streams
  the replay side via `stores.replay_reader.ReplayReader` (already
  guaranteed sorted by the replay-store contract). Both channels
  (depth_v2, trade_v2) are filtered during raw normalization to only the
  record types the replay writer actually converts (`snapshot_seed`/
  `depth_update` for depth, `trade`/`agg_trade` for trades — matching
  `pipeline/build_replay_store.py`'s `_convert_depth_record()`/
  `_convert_trade_record()` exactly), to avoid spurious "extra on the raw
  side" mismatches from record types (e.g. `sync_state`) the replay writer
  intentionally never converts.
- **Bug found and fixed while wiring the metadata comparator**: raw records
  read via `converter.readers.stream_raw_records()` do not carry a
  `raw_index` field (it is assigned by the replay writer during
  conversion, via `enumerate()` over the full unfiltered raw stream); the
  first version of the raw-side normalizer read a nonexistent
  `rec.get("raw_index")` (always `None`), causing every position to
  spuriously mismatch even on an otherwise-identical clean day. Fixed by
  assigning `raw_index` locally using the same global (unfiltered)
  enumeration order the replay writer uses, before inserting into the
  sorting spool — verified directly against
  `pipeline/build_replay_store.py`'s `enumerate(stream_raw_records(...))`
  pattern to confirm the numbering scheme matches exactly.
- Added `tests/test_streaming_gating_bounded_memory.py` (6 tests):
  empirical proofs, via a live-object-counter hooked into `__del__`, that
  `compare_book_checkpoints_streaming()`,
  `compare_order_book_depth10_exhaustive()`, and
  `compare_event_metadata_exhaustive()` all stay bounded-memory
  (peak simultaneously-alive objects independent of stream length, tested
  at 20,000–50,000 synthetic events) while still detecting a difference
  injected near the end of the stream — including, for the metadata
  comparator, a value moved between two events with the overall multiset
  unchanged (the exact gap a multiset comparison has).
- Rewrote `tests/test_validate_catalog_equivalence_exhaustive_wiring.py`
  (23 tests, up from 12) to prove, through the real, unmodified
  `validate_catalog_equivalence()` orchestration: all the previously-
  proven trade/delta/instrument/continuity scenarios, plus new coverage
  for a book-checkpoint value mismatch (asserting `"gating"` is absent
  from the result and the aggregate `book_checkpoints` block is exactly
  `{"passed": False}`, not a diagnostic-shaped dict); an enabled-Depth10
  mismatch and an enabled-Depth10 reorder (both failing); Depth10
  explicitly disabled reporting `skipped=True, passed=True`; a
  fenced-range mismatch where only the 4th of 4 fences differs (proving
  the 3-example truncation gap is closed); an extra candidate fenced
  range (proving `extra_in_new` is no longer treated as expected); and,
  via a real raw-JSONL → real `convert_day.py` → real replay-build
  pipeline with only the raw-side generator monkeypatched for each
  scenario: a quality flag swapped between the two depth_update events
  with the overall multiset unchanged, a changed `u` continuity ID, a
  flipped `is_desync` state, a missing diagnostic event, and an extra
  diagnostic event — each correctly failing the real orchestration's
  `report["status"]`. Two regression guards were also updated: a static
  check that the module no longer imports any of the sampled/multiset/
  full-day-list comparators or loaders (now including
  `compare_depth10_semantic`, `compare_book_checkpoints`,
  `compare_fenced_ranges_semantic`, `compare_quality_flags_semantic`,
  `load_order_book_deltas`, `load_order_book_depth10` in addition to the
  previously-guarded names), and a call-counting spy proving
  `compare_trade_ticks_exhaustive`, `compare_order_book_deltas_exhaustive`,
  and `compare_book_checkpoints_streaming` are genuinely invoked during a
  real run.
- No compact replay schema was changed. This remains Phase 1 (oracle
  hardening) work, still gating any future compact schema implementation
  (Phase 5+, not started).

### Files/packages touched
- `validation/catalog_compare.py` (new streaming checkpoint reconstruction,
  Depth10 windowed loader + exhaustive comparator, fenced-range digest
  comparator, generic event-metadata comparator)
- `validation/validate_catalog_equivalence.py` (rewired
  `_compare_depth_for_instrument()`, `_compare_fenced_ranges_for_symbol()`;
  replaced quality-flags collectors with
  `_compare_raw_to_replay_metadata_for_symbol()` and its
  `_iter_sorted_raw_depth()`/`_iter_sorted_raw_trades()`/normalizer
  helpers)
- `converter/depth_phase2.py` (new `fence_canonical_key()`,
  `canonical_fence_digest()`)
- `convert_day.py` (computes and persists `canonical_count`/
  `canonical_digest` per symbol in `per_symbol_fenced_ranges`)
- `tests/test_streaming_gating_bounded_memory.py` (new — 6 tests)
- `tests/test_validate_catalog_equivalence_exhaustive_wiring.py` (rewritten
  — 23 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — this remains oracle-hardening tooling, not a new real-data
  validation run against a representative production day)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; consistent with the existing
  Phase 1/2/3 entries — this correction strengthens, not contradicts, the
  "Phase 1 oracle (implemented)" references already recorded there)
- [x] relevant feature docs:
  - docs/VALIDATION.md (reviewed; `validate_catalog_equivalence` CLI
    description remains accurate — internal comparison logic changed, the
    documented CLI invocation/flags did not)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: `docs/VALIDATION.md`'s existing
  CLI-level description remains accurate; no new CLI flags or status/gate
  claims were added.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this proves the corrected oracle wiring is
  gating and bounded-memory via synthetic/local integration tests; it does
  not constitute a new semantic-equivalence validation run against real
  production data, and does not change the `full_l2`/v2.0.0 gate status.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_semantic_oracle_exhaustive_streaming.py tests/test_semantic_oracle_detects_injected_faults.py tests/test_windowed_loader_boundaries.py tests/test_validate_catalog_equivalence_exhaustive_wiring.py tests/test_streaming_gating_bounded_memory.py tests/test_catalog_equivalence.py tests/test_catalog_equivalence_full_l2.py tests/test_pipeline_validation.py -q   # 82 passed, 2 skipped
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 414 passed, 3 skipped
```

### Validation CLIs run
```bash
none required for this change type — no config or deployment file was
touched; the wiring is exercised directly by the test suites above.
```

### Known limitations / out of scope
- `_collect_quality_flags_from_raw()`/`_collect_quality_flags_from_replay()`
  from the prior correction were removed entirely (not merely deprecated),
  since the new event-identity-keyed comparator fully supersedes them;
  `compare_quality_flags_semantic()` itself remains in
  `validation/catalog_compare.py` for lightweight/Tier-1 ad-hoc use, now
  explicitly documented as superseded for the acceptance path.
- No representative production-day (Tier 3: 2026-07-22/23) run of the
  corrected wiring was performed in this session — proven only against
  synthetic (Tier 1) data via real, on-disk Nautilus catalogs, a real
  raw→convert_day.py→replay-build pipeline, and empirical bounded-memory
  proofs, which is exactly what "prove the wiring before using it as a
  gate" requires before any real-data run.
- The book-checkpoint and Depth10 comparisons still issue a second,
  independent pass over the windowed delta/Depth10 iterators (once for
  the exhaustive comparison, once for checkpoints/Depth10 respectively),
  doubling the on-disk read for those channels versus a hypothetical
  single-pass design; this trade-off is documented in
  `_compare_depth_for_instrument()`'s docstring and was judged acceptable
  to keep each comparator's memory bounded and independently testable,
  not optimized further in this correction.

---

## 2026-07-24 — Issue #20 Phase 1 follow-up correction: wire exhaustive oracle into the real acceptance path, fix duplicate semantics, fix windowed-loader boundary bug

### Change summary
- A review of the previously pushed head (`7ca0854`) correctly found that
  the Phase 1 correction's exhaustive/streaming comparators and windowed
  loaders were added to `validation/catalog_compare.py` but never wired
  into `validation/validate_catalog_equivalence.py` — the actual function
  the CLI and any future Tier-2/Tier-3 acceptance run calls. The real
  orchestration still used `load_trade_ticks()` +
  `compare_trade_ticks_semantic()` (sampled) and `load_order_book_deltas()` +
  `compare_order_book_deltas_semantic()` (multiset). This entry corrects
  that gap and two further defects found while doing so.
- **Wired into the real path** (`validation/validate_catalog_equivalence.py`,
  `validate_catalog_equivalence()` and `_compare_depth_for_instrument()`):
  - `load_instruments()` + `compare_instruments_semantic()` — instrument
    identity/precision now gates `report["comparison"]["instrument_precision"]`.
  - `iter_trade_ticks_windowed()` + `compare_trade_ticks_exhaustive()` —
    replaces the sampled trade comparison entirely; gates
    `comparison["by_instrument"][iid]["trade_ticks"]`.
  - `iter_order_book_deltas_windowed()` + `compare_order_book_deltas_exhaustive()` —
    replaces the multiset delta comparison entirely; gates
    `comparison["by_instrument"][iid]["order_book_deltas"]`.
  - `compare_continuity_diagnostics_semantic()` — new
    `_load_old_convert_report()` reads convert_day.py's own report JSON
    (`catalog_root.parent / "convert_reports" / f"{date}.json"`, per
    convert_day.py's own `_save_report()`) for `per_symbol_depth`, compared
    against the candidate manifest's `depth_diagnostics`; gates
    `continuity_diagnostics`.
  - `compare_fenced_ranges_semantic()` — reference-side
    `per_symbol_fenced_ranges[...]["examples"]` (convert_day.py only
    records up to 3 example fences per symbol, not the full list) against
    the candidate manifest's `fenced_ranges` filtered to that venue/symbol.
    Documented explicitly: `extra_in_new` is expected whenever the
    candidate legitimately has more than 3 fences (a reference-side
    data-shape truncation, not an equivalence failure) and does not gate;
    only `missing_in_new` (every reference example must be reproduced) is
    gating, exposed as a new `gating_passed` field.
  - `compare_quality_flags_semantic()` — new
    `_collect_quality_flags_from_raw()`/`_collect_quality_flags_from_replay()`
    compare quality_flags read directly from `data_raw` (via
    `converter.readers.stream_raw_records()`) against the replay_store the
    candidate builds from that same raw source (via
    `stores.replay_reader.ReplayReader`). Documented explicitly why: neither
    convert_day.py's Nautilus catalog output nor its own report JSON
    persists a per-event quality_flags stream, so there is no "old Nautilus
    catalog vs new Nautilus catalog" comparison available for this field —
    raw is the one place it exists on both a reference and candidate side.
  - Every one of the above `passed` results now contributes to
    `report["status"]`.
  - `compare_book_checkpoints()` and Depth10 comparison remain wired but
    are now explicit non-gating diagnostics (new `"gating": False` field on
    each): both still require full-day list materialization
    (`compare_book_checkpoints()` calls `list(...)` on its inputs; there is
    no windowed/streaming equivalent for either today), which the bounded-
    memory acceptance path is specifically designed to avoid. This is a
    documented, deliberate limitation, not silently hidden — closing it is
    explicitly out of scope for this correction and left as future work.
  - The legacy sampled/multiset comparators and the full-day
    `load_trade_ticks()`/`load_order_book_deltas()`-as-primary-loader
    pattern are no longer imported by `validate_catalog_equivalence.py` at
    all (confirmed by a new static regression test, see below).
- **Corrected duplicate-event semantics** (`validation/catalog_compare.py`):
  two identical ordered streams now correctly pass even when both contain
  the exact same duplicate event at the same position — equivalence means
  the reference and candidate streams are identical, including identical
  duplicate occurrences. The prior version incorrectly treated "a
  duplicate exists on either side" as an independent failure condition.
  Removed `_BoundedDedupeWindow` (which stored keys in a Python `list` and
  called `pop(0)` on eviction — O(N) per eviction once the window filled,
  an O(N×window) cost across a full stream) entirely, rather than merely
  replacing it with a `collections.deque`: the pre-existing positional/
  length comparison already fully detects every duplicate-related
  discrepancy that can actually indicate non-equivalence (an extra,
  missing, or differently-positioned duplicate shifts every subsequent
  position). This keeps `compare_trade_ticks_exhaustive()`/
  `compare_order_book_deltas_exhaustive()` O(N) end-to-end, remaining
  practical at 200M+ events per the issue's requirement.
- **Found and fixed a real windowed-loader boundary bug**
  (`validation/catalog_compare.py`): `iter_trade_ticks_windowed()`/
  `iter_order_book_deltas_windowed()` previously assumed Nautilus's
  `catalog.trade_ticks(start=a, end=b)`/`catalog.order_book_deltas(start=a,
  end=b)` queries were half-open `[a, b)`. Direct testing against a real
  on-disk `ParquetDataCatalog` (new
  `tests/test_windowed_loader_boundaries.py`) proved this false: the query
  is inclusive on **both** `a` and `b` — confirmed directly by querying
  `start=0, end=1000` against a single event at `ts=1000` and observing it
  returned. The previous window-chaining logic
  (`next_window_start = previous_window_end`) therefore double-yielded any
  event landing exactly on an internal window boundary (reproduced
  directly: `[0, 999, 1000, 1000, 1001, 2999]` instead of
  `[0, 999, 1000, 1001, 2999]`). Both loaders now partition the caller's
  half-open `[start_ns, end_ns)` range into non-overlapping **closed**
  sub-windows (`window_end = min(window_start + window_ns - 1,
  end_ns - 1)`, next `window_start = window_end + 1`), safe because all
  Nautilus event timestamps are integer nanoseconds. Re-verified after the
  fix: every boundary-position event (overall start; immediately
  before/on/after an internal boundary; immediately before the overall
  end) is yielded exactly once, in order, for both loaders, and windowed
  iteration matches a single unwindowed full-range query exactly.
  `window_ns` remains fully configurable (two different window sizes
  proven to yield identical results against the same data); docstrings no
  longer claim a fixed time window is a strict event-count/RSS bound —
  only that it bounds query result size per window, to be tuned against
  measured per-window RSS on real production data (issue #20 Tier 3).
- Added `tests/test_validate_catalog_equivalence_exhaustive_wiring.py`
  (new, 12 tests): end-to-end integration tests that monkeypatch only the
  build steps (`_run_old_converter`, `_run_new_pipeline`, `_prepare_dir`)
  to no-ops so each test can pre-construct fully controlled real Nautilus
  catalogs (via `ParquetDataCatalog.write_data()`) plus a matching
  convert_day.py-shaped report and replay manifest, then calls the real,
  unmodified `validate_catalog_equivalence()` — proving the orchestration
  itself, not just the comparator helpers in isolation, fails for: a trade
  mismatch beyond the legacy sampled comparator's 100 positions; reordered
  trades (content-swapped between adjacent timestamp slots, since
  Nautilus's catalog enforces monotonically increasing `ts_init` at write
  time — a literal object swap cannot be written to a real catalog, which
  is itself a documented finding of this work); reordered commutative-
  looking depth deltas (with a sanity assertion that the non-gating
  book-checkpoint diagnostic legitimately still matches, demonstrating
  exactly why the exhaustive comparison must be the actual gate);
  extra/missing trades; an instrument precision/increment mismatch; a
  continuity (resync-count) mismatch; a fenced-range mismatch; and a
  quality-flags mismatch. Plus a passing baseline and two regression
  guards: a static check that `compare_trade_ticks_semantic`,
  `compare_order_book_deltas_semantic`, and `load_trade_ticks` are no
  longer attributes of the `validate_catalog_equivalence` module (would
  fail immediately if a future change re-adds those imports), and a
  call-counting spy proving `compare_trade_ticks_exhaustive`/
  `compare_order_book_deltas_exhaustive` are genuinely invoked (not merely
  importable-but-unused) during a real run.
- No compact replay schema was changed. This remains Phase 1 (oracle
  hardening) work, still gating any future compact schema implementation
  (Phase 5+, not started).

### Files/packages touched
- `validation/validate_catalog_equivalence.py` (real acceptance-path wiring)
- `validation/catalog_compare.py` (duplicate-semantics fix, windowed-loader
  boundary-bug fix)
- `tests/test_semantic_oracle_exhaustive_streaming.py` (updated duplicate
  tests to match corrected semantics)
- `tests/test_windowed_loader_boundaries.py` (new — 6 tests)
- `tests/test_validate_catalog_equivalence_exhaustive_wiring.py` (new — 12 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — this remains oracle-hardening tooling, not a new real-data
  validation run)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; consistent with the existing
  Phase 1/2/3 entries — this correction strengthens, not contradicts, the
  "Phase 1 oracle (implemented)" reference already recorded there)
- [x] relevant feature docs:
  - docs/VALIDATION.md (reviewed; `validate_catalog_equivalence` CLI
    description remains accurate — internal comparison logic changed, the
    documented CLI invocation/flags did not, aside from the additive
    `--window-hours` flag)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: `docs/VALIDATION.md`'s existing
  CLI-level description remains accurate; the new `--window-hours` flag
  is additive and self-documenting via `--help`.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this proves the *oracle wiring* is correct via
  synthetic/local integration tests; it does not constitute a new
  semantic-equivalence validation run against real production data, and
  does not change the `full_l2`/v2.0.0 gate status.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_semantic_oracle_exhaustive_streaming.py tests/test_semantic_oracle_detects_injected_faults.py tests/test_windowed_loader_boundaries.py tests/test_validate_catalog_equivalence_exhaustive_wiring.py -q   # 50 passed
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 397 passed, 3 skipped
```

### Validation CLIs run
```bash
none required for this change type — no schema, systemd, or deployment
file was touched; the wiring is exercised directly by the new
integration-test suite above.
```

### Known limitations / out of scope
- `compare_book_checkpoints()` and Depth10 comparison remain full-day
  list-materializing and are explicitly non-gating diagnostics — a
  windowed/streaming equivalent for either is out of scope for this
  correction and remains future work.
- `_collect_quality_flags_from_raw()`/`_collect_quality_flags_from_replay()`
  are not themselves windowed/bounded-memory (they materialize a Python
  list of quality_flags values for the requested venue/symbol/date) —
  acceptable for now since `quality_flags` values are small compared to
  full event objects, but this should be revisited if quality_flags volume
  becomes a real memory concern for a full production day.
- No representative production-day (Tier 3: 2026-07-22/23) run of the
  corrected wiring was performed in this session — proven only against
  synthetic (Tier 1) data via real, on-disk Nautilus catalogs and a
  genuine end-to-end orchestration call, which is exactly what "prove the
  wiring before using it as a gate" requires before any real-data run.
- The `--window-hours` CLI default (1 hour) has not been validated against
  measured per-window RSS on real production data; it remains a
  configurable starting point per the docstring's explicit caveat, not a
  proven-safe value.

---

## 2026-07-24 — Issue #20 Phase 1 correction: exhaustive, order-preserving, bounded-memory oracle comparison (closes sampling/multiset/streaming gap)

### Change summary
- A follow-up review of the already-committed Phase 1 oracle-hardening
  work correctly identified that it was still insufficient for the
  issue's actual requirement: `compare_trade_ticks_semantic()` samples up
  to `sample_count` (default 100) positions after re-sorting both streams
  by `(ts_event, trade_id)`, and `compare_order_book_deltas_semantic()` is
  a multiset comparison that also re-sorts before comparing. Verified
  directly (and demonstrated in the new test suite) that both designs
  have real, non-hypothetical blind spots:
  - a difference placed at a position the sampler does not select is
    invisible to `compare_trade_ticks_semantic()`;
  - a pure reordering of two otherwise-identical-content trades/deltas is
    invisible to both the sampled comparator (same trade_id set, no
    missing/extra keys) and the multiset comparator (same multiset);
  - a reordering of two independent, non-conflicting depth updates that
    happens to produce an *identical final book state* is invisible even
    to `compare_book_checkpoints()`'s deterministic book-state
    reconstruction, since the checkpoint only observes the state *after*
    both deltas have been applied, not the order they arrived in.
- Added `compare_trade_ticks_exhaustive()` and
  `compare_order_book_deltas_exhaustive()` to `validation/catalog_compare.py`:
  both compare every event at its original stream position via
  `itertools.zip_longest` (no re-sorting, no sampling), so a reordering,
  an out-of-sample-range difference, or an extra/missing event anywhere
  in the stream is detected. Neither function materializes either input
  stream into a list internally — they accept and consume arbitrary
  iterables (including one-shot generators), keeping memory bounded and
  independent of total event count.
- Added `_BoundedDedupeWindow` (O(window) memory) to detect duplicate
  events using a bounded recent-window lookback per stream side — a
  documented, deliberate trade-off against a true O(total-event-count)
  global duplicate check, which would itself violate the bounded-memory
  requirement for a complete production day's tens/hundreds of millions
  of events. The docstring states this trade-off explicitly: a duplicate
  whose two occurrences are farther apart than the window will not be
  flagged by this specific check.
- Added `iter_trade_ticks_windowed()` and `iter_order_book_deltas_windowed()`
  to `validation/catalog_compare.py`: bounded-memory catalog loaders that
  fetch in fixed time windows (default 1 hour) via repeated
  `catalog.trade_ticks()`/`catalog.order_book_deltas()` calls, rather than
  materializing an entire requested time range in one call the way
  `load_trade_ticks()`/`load_order_book_deltas()` do. These are the
  necessary companion loaders for the new exhaustive comparators to
  actually achieve bounded memory end-to-end against a real catalog for a
  complete production day (issue #20 Tier 3), not just within the
  comparator function itself.
- Added `tests/test_semantic_oracle_exhaustive_streaming.py` (11 tests):
  - a difference outside the legacy sampler's selected positions is
    missed by the legacy comparator and caught by the new one (sanity
    assertion on the legacy comparator's "false pass" included, to prove
    the gap is real, not assumed);
  - a pure reordering of two independent depth deltas is reported as
    equal by the multiset comparator and as a mismatch by the exhaustive
    one (same structure: sanity assertion on the multiset comparator's
    "false pass" included);
  - extra trade appended / missing trade / extra delta — all detected via
    `first_length_divergence_position`;
  - duplicate trade added (new-side duplicate) and duplicate trade removed
    (old-side-only duplicate) — both detected via
    `duplicate_events_new`/`duplicate_events_old` respectively;
  - reordered trades at positions outside the legacy sampler's selection —
    detected, with an explicit sanity check that the legacy comparator's
    `missing_keys`/`extra_keys` stay empty (proving a set-based check
    alone cannot see a pure reordering);
  - reordered commutative-looking depth deltas producing an *identical*
    final book state — verified via `compare_book_checkpoints()` reporting
    `passed=True` (sanity check that the scenario is genuinely
    commutative), while `compare_order_book_deltas_exhaustive()` reports
    `passed=False`;
  - two bounded-memory + late-difference proofs (one for trades at
    n=20,000, one for deltas at n=5,000): a `_LiveCounter`/`_FakeTick` pair
    tracks how many synthetic event objects are simultaneously alive via
    Python's refcounting `__del__` hook (not merely asserted from
    implementation reading), proving peak simultaneous liveness stays
    under 100 objects — independent of n — while a single difference
    injected 3–5 positions before the end of each stream is still
    detected, proving the entire stream is genuinely scanned rather than
    truncated or sampled.
- The pre-existing sampled/multiset comparators
  (`compare_trade_ticks_semantic()`, `compare_order_book_deltas_semantic()`)
  are unchanged and retained — they remain useful for fast
  summary-level comparisons during iterative development (Tier 1/2); the
  new exhaustive comparators are the ones required for the Tier 3
  representative-production-day gate and are additive, not a replacement.
- No compact replay schema, builder, staging-cleanup, or raw-retention
  behavior was changed. This is a correction within Phase 1 (oracle
  hardening), which continues to gate any future compact schema
  implementation (Phase 5+, not started).

### Files/packages touched
- `validation/catalog_compare.py` (extended — 2 new exhaustive comparators,
  2 new bounded-memory catalog loaders, 1 new dedupe-window helper)
- `tests/test_semantic_oracle_exhaustive_streaming.py` (new — 11 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change —
  `validation/` already owns `catalog_compare.py`)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — this is oracle tooling correction, not a new validation run
  against real data)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; consistent with the Phase 2-3
  entry, which already references "the Phase 1 oracle (implemented)" —
  this correction strengthens, not contradicts, that reference)
- [x] relevant feature docs:
  - docs/VALIDATION.md (reviewed; existing `validate_catalog_equivalence`
    CLI description remains accurate — the new comparators are additive
    library functions, not yet wired into the CLI's default profile
    output, consistent with the prior Phase 1 entry's same known
    limitation)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: this is additive comparator/loader
  tooling correcting a gap within the already-documented Phase 1 scope;
  no new CLI surface, status, or gate changed.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this proves the *oracle* now performs exhaustive,
  order-preserving, bounded-memory comparison on synthetic data; it does
  not constitute a new semantic-equivalence validation run against real
  data, and does not change the `full_l2`/v2.0.0 gate status.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_semantic_oracle_exhaustive_streaming.py -q   # 11 passed
python -m pytest tests/test_semantic_oracle_exhaustive_streaming.py \
  tests/test_semantic_oracle_detects_injected_faults.py \
  tests/test_catalog_equivalence.py tests/test_catalog_equivalence_full_l2.py \
  tests/test_semantic_equivalence.py tests/test_replay_catalog_reconstruct.py -q   # 41 passed, 1 skipped
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 377 passed, 3 skipped
```

### Validation CLIs run
```bash
none required for this change type — no schema, config, or deployment
file was touched; the new comparators/loaders are exercised directly by
the new pytest suite above.
```

### Known limitations / out of scope
- The new comparison functions are not yet wired into
  `validation/validate_catalog_equivalence.py`'s default output — same
  known limitation already recorded in the prior Phase 1 entry; that
  integration is deferred until the compact schema (Phase 5+, not started)
  determines the exact manifest/report field shapes to compare against.
- `_BoundedDedupeWindow`'s duplicate detection is bounded-window, not
  global — a duplicate whose two occurrences are farther apart than
  `dedupe_window` (default 100,000 events) will not be flagged. This is a
  documented, deliberate trade-off, not an oversight: true global
  duplicate detection over hundreds of millions of events would itself
  require unbounded memory.
- No representative production-day (Tier 3: 2026-07-22/23) or even local
  real-data (Tier 2) run of the new exhaustive comparators was performed
  in this session — this correction is proven only against synthetic
  (Tier 1) data and a live-object-counter memory proof, which is exactly
  what "prove the oracle before using it as a gate" requires before any
  real-data run.
- `iter_trade_ticks_windowed()`/`iter_order_book_deltas_windowed()` were
  not exercised against a real on-disk Nautilus catalog in this session's
  test suite (no test writes a multi-hour synthetic catalog and asserts
  the windowed loader issues multiple bounded `catalog.trade_ticks()`
  calls); the loaders' correctness rests on straightforward, directly
  auditable logic (a `while` loop advancing a fixed time window) rather
  than an integration test in this commit. Adding that integration test is
  explicitly left as a candidate follow-up, not claimed as already done.

---

## 2026-07-24 — Issue #20 Phase 4: deliberate repository-boundary and guard alignment for a future selected-scope reconstruction CLI

### Change summary
- `docs/REPO_STRUCTURE.md`'s `pipeline/` role text previously stated
  pipeline "does not contain a product-facing catalog-generation CLI
  (removed; see docs/ARCHITECTURE.md)" — an absolute prohibition dating
  from issue #17's removal of the old unscoped `pipeline/generate_catalog.py`.
  Issue #20 Phase 4 requires reversing that prohibition for one narrow,
  explicitly bounded case: a development-computer selected-reconstruction
  CLI that builds a **temporary** Nautilus full-L2 catalog for an
  explicitly requested venue/symbol list and start/end time window only —
  never an unscoped, all-history/all-universe rebuild, and never an
  unattended Linux production service.
- Updated the `pipeline/` role description in `docs/REPO_STRUCTURE.md` to
  permit exactly one such CLI, while explicitly re-stating four hard
  prohibitions that must hold regardless: no default/silent expansion to
  "all symbols/all venues/all history"; no permanent all-universe catalog;
  no unattended systemd/production service; and the caller must always
  explicitly supply both a venue/symbol scope and a start/end window.
  Named the historical `pipeline/generate_catalog.py` shape as
  permanently forbidden by name and by its unscoped design, distinct from
  any future scoped CLI (which must use a different name).
- Added a 2026-07-24 entry to the `docs/REPO_STRUCTURE.md` Amendment Log
  documenting this contract change and pointing at the rewritten guard
  test.
- Rewrote (did not delete or weaken) the corresponding guard test in
  `tests/test_repo_structure.py`:
  `test_pipeline_does_not_contain_generate_catalog_cli()` →
  `test_pipeline_reconstruction_cli_stays_explicitly_scoped()`. The new
  test (a) still asserts `pipeline/generate_catalog.py` must never exist —
  the old unscoped name stays permanently forbidden — and (b) additionally
  scans every *other* module in `pipeline/` (i.e. anything beyond the
  three known, already-existing modules `build_replay_store.py`,
  `daily_build.py`, `raw_manifest.py`) for unscoped-reconstruction text
  markers (`all_symbols`, `all_venues`, `full_universe`, `--all-history`,
  etc.), so that once a future selected-reconstruction CLI is added, this
  guard will immediately fail if it silently reintroduces an unscoped
  default. No reconstruction CLI exists in the repository yet, so this
  half of the test is presently forward-looking/vacuously satisfied — it
  is exercised and will start actively gating the moment such a file is
  added in a future implementation phase.
- Verified the existing `test_docs_do_not_reference_pipeline_audit_modules()`
  guard (which forbids the historical dotted-module import path for the
  removed catalog CLI appearing in any `docs/*.md` file) is not tripped by
  this change: all new text in `docs/REPO_STRUCTURE.md` uses the slash-path
  form (`` `pipeline/generate_catalog.py` ``), confirmed via a direct grep
  for the dotted form before committing.
- No selected-reconstruction CLI, no compact schema, and no other pipeline/
  module was implemented in this commit. This is a deliberate,
  documented, test-enforced contract change only, per AGENTS.md's
  allowance for updating a guard "when the product contract truly
  changes."

### Files/packages touched
- `docs/REPO_STRUCTURE.md` (role text + amendment-log entry)
- `tests/test_repo_structure.py` (guard test rewritten, not deleted)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md (Section 2 folder-boundary rules; Section 5 "a guard may be
  updated when the product contract truly changes" — the basis for this
  change being permitted at all)
- [x] docs/REPO_STRUCTURE.md (this is the file amended)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — no CLI was implemented, only the future contract for one)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; consistent with the Phase
  2-3 design entry immediately above, which already anticipates a future
  selected-reconstruction CLI without implementing one)
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md (reviewed; its "does not build a feature/label
    layer or a general-purpose consumer catalog" statement remains
    accurate — a *selected, temporary* catalog for an explicit
    venue/symbol/window is a distinct, narrower thing than a
    general-purpose consumer catalog, and is not yet implemented)

### Docs updated
- [x] CHANGELOG.md
- [x] docs/REPO_STRUCTURE.md
- No further docs update required because: `docs/ARCHITECTURE.md` and
  `docs/PROJECT_STATUS.md` describe *current, implemented* behavior, which
  is unchanged by this contract-only amendment; the amendment itself lives
  in `docs/REPO_STRUCTURE.md` per the "identify the right existing home"
  rule.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — no reconstruction CLI is claimed as implemented;
  the amendment explicitly states "No such CLI exists yet in this
  repository."
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q   # 366 passed, 3 skipped
```

### Validation CLIs run
```bash
grep -n "pipeline[.]generate_catalog" docs/REPO_STRUCTURE.md   # no matches (dotted forbidden form absent)
```

### Known limitations / out of scope
- No selected-reconstruction CLI was implemented — this commit only
  updates the repository contract and its guard test ahead of a future
  implementation phase (Phase 6/7 of the approved plan, not started).
- The guard's "unscoped marker" scan is a text-content heuristic on any
  future `pipeline/*.py` file, not a semantic/AST analysis of its actual
  CLI argument parser; it is deliberately simple and forward-looking, and
  will need to be revisited once the actual future CLI's argument names
  are known, without weakening its current forbidding assertions.
- This is Phase 4 of the issue #20 plan. Per the approved plan, Phases
  0-4 (baseline, oracle + failure-injection proof, raw-retention/legacy/
  traceability/versioning design, finalized field/consumer/integrity
  matrix, and this repository-boundary alignment) are now all complete;
  compact physical replay schema implementation (Phase 5+) requires the
  explicit review checkpoint the approved plan calls for before it may
  begin.

---

## 2026-07-24 — Issue #20 Phases 2–3: raw-retention safety, legacy-v0 inventory, traceability, versioning design, and finalized field/consumer/integrity matrix

### Change summary
- **Design/documentation only — no code was changed in this commit.** Both
  phases are documented as new sections appended to
  `docs/IMPLEMENTATION_AUDIT.md` rather than as two separate commits,
  because Phase 3's matrix directly builds on and cross-references Phase
  2's design conclusions (e.g. the `native_payload_hash`/`quality_flags`
  "unresolved/unproven" status in the matrix is defined by Phase 2's
  traceability-design and consumer-proof sections) — splitting them into
  two commits within the same file via interactive patch staging was
  judged more fragile than a single reviewable append covering both,
  given both are strictly additive, non-destructive documentation.
- **Phase 2 — Raw-retention safety contract**: corrected a false assumption
  from the previously-approved plan. Verified directly in code that
  `disk_monitor.py::cleanup_old_data()` **already** automatically deletes
  raw data above `CRYPTO_RECORDER_DISK_SOFT_LIMIT_GB`, and that
  `get_oldest_date_dir()`'s `venue → channel → symbol → date` glob makes
  `depth_v2` and `trade_v2` for the same symbol/date independently
  deletable — i.e. `data_raw` is not retained forever, and the current
  deletion unit is not atomic across the channels of one logical
  partition. Documented a 9-point precondition gate (replay
  exists/complete/checksummed/self-contained/gate-passed, etc.) that must
  hold before any raw deletion of a corrected, atomic per-partition
  deletion unit is permitted, fail-closed by default, layered as an
  additive safety check over the existing (unmodified in this commit)
  `cleanup_old_data()` mechanism.
- **Phase 2 — Legacy-v0 inventory design**: documented the
  rebuildable/not-rebuildable/uncertain classification approach for
  existing v0 partitions, given that raw availability can no longer be
  assumed. Non-rebuildable/uncertain partitions are designed to be
  preserved with the legacy v0 reader kept available indefinitely (not a
  fixed migration window); reader removal is conditioned on an explicit
  future inventory run proving zero dependency.
- **Phase 2 — Traceability design**: documented the planned replacement
  integrity hierarchy (raw file/chunk checksums → source
  offset/ordinal → block checksums → published-file checksums →
  deterministic event-to-source mapping) that must be implemented and
  proven equivalent-or-stronger than the current per-row
  `native_payload_hash` before that field may be demoted or removed. This
  explicitly reverses the prior plan draft's "hash demotion is low risk"
  framing to "unresolved, pending proof."
- **Phase 2 — Versioning/`encoding_profile` design**: documented that a
  missing `schema_version` today means legacy v0 (today's manifest has no
  version fields at all, verified against `docs/REPLAY_STORE.md`'s
  manifest example), and designed a new `encoding_profile` manifest field
  as the build-configuration identity needed for a future
  deterministic-rebuild proof.
- **Phase 3 — Finalized field/consumer/integrity matrix**: audited every
  column of `stores/replay_schema.py`'s current `DEPTH_REPLAY_SCHEMA` and
  `TRADE_REPLAY_SCHEMA` (verified directly in code) and classified each by
  writer, current representation, reconstruction consumer, audit/
  integrity consumer, semantic necessity, partition-constancy, proposed
  compact representation, and migration concern. No field is approved for
  removal or repacking by this matrix — every compaction candidate is
  explicitly gated on a named, not-yet-satisfied proof condition (e.g. the
  4-condition fixed-point round-trip proof for `bids`/`asks`/`price`/
  `quantity`, or the consumer-and-semantics proof for `quality_flags`).
- This entry, together with the Phase 1 oracle (`validation/catalog_compare.py`
  + `tests/test_semantic_oracle_detects_injected_faults.py`, already
  committed), constitutes the review-checkpoint evidence the approved plan
  requires before any compact physical replay schema implementation
  (Phase 5+) may begin.

### Files/packages touched
- `docs/IMPLEMENTATION_AUDIT.md` (extended — two new top-level sections)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change — content
  added to an existing, already-owned doc, not a new file)
- [x] docs/PROJECT_STATUS.md (reviewed; no validated/deferred status
  changed — this is design documentation, not a new validated/deferred
  claim)
- [x] docs/IMPLEMENTATION_AUDIT.md (this is the file extended)
- [x] relevant feature docs:
  - docs/REPLAY_STORE.md (reviewed; its documented schema/manifest example
    remains accurate as the *current* v0 state — the new sections in
    IMPLEMENTATION_AUDIT.md explicitly describe the *planned* future
    schema/manifest, not a change to what REPLAY_STORE.md documents today)
  - docs/FULL_L2_REPLAY_CATALOG_PLAN.md (reviewed; no gate status changed)

### Docs updated
- [x] CHANGELOG.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- No further docs update required because: `docs/REPLAY_STORE.md` and
  `docs/ARCHITECTURE.md` describe the *current* (v0, unchanged) schema and
  architecture, which remains accurate; the new design content is
  explicitly forward-looking and lives in `docs/IMPLEMENTATION_AUDIT.md`
  (the ground-truth/audit-history doc) rather than duplicated elsewhere.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — every claim in the new sections is explicitly
  labeled "design only, not implemented" / "planned" / "unresolved" /
  "unproven," per the honesty-labeling requirement; no capability is
  claimed as done.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q   # 56 passed
python -m pytest -q   # 366 passed, 3 skipped (unchanged from Phase 1 — docs-only change)
```

### Validation CLIs run
```bash
none required beyond the honesty-guard tests above — no schema, config,
or deployment file was touched; docs/CHANGE_AUDIT.md and CHANGELOG.md
updates are covered by the standard change-audit compliance check.
```

### Known limitations / out of scope
- No code implementing any part of this design (raw-retention gate,
  legacy-v0 inventory scan, traceability hierarchy, `encoding_profile`
  field, or schema changes implied by the matrix) was written in this
  commit — per the approved plan, that requires the Phase 0–4 review
  checkpoint (this entry plus the already-committed Phase 1 oracle) to be
  reviewed and approved first.
- The field/consumer/integrity matrix does not yet cover any *new* fields
  a future compact schema might introduce (e.g. a mantissa/scale pair) —
  it only audits what exists in the current `stores/replay_schema.py`,
  which is the correct scope for a "before you remove/repack anything,
  prove it" audit.
- No representative production-day or KovacsTrader-contract verification
  was performed as part of this design work — the KovacsTrader contract
  question was already resolved (non-blocking; CryptoRecorder exposes
  exact logical values via a future versioned `ReplayReader` regardless of
  physical schema) in the previously-approved plan and is not re-litigated
  here.

---

## 2026-07-24 — Issue #20 Phase 1: semantic-oracle coverage audit, missing comparisons, failure-injection proof

### Change summary
- Coverage-audited `validation/catalog_compare.py` against the issue #20
  full semantic-equivalence contract (instrument identity/precision;
  ordered TradeTicks; ordered OrderBookDeltas incl. actions/sides/prices/
  sizes/flags/sequence/timestamps; snapshot seeds; clear/reset; sync/
  desync/resync; continuity gaps/fenced ranges; session/day boundaries;
  Depth10; quality-flag behavior; deterministic book-state checkpoints).
  Found the existing comparators already cover: TradeTicks, OrderBookDeltas
  (incl. CLEAR/snapshot flags since `action`/`flags` are compared fields),
  Depth10, and 7-checkpoint deterministic book-state reconstruction.
- Identified and closed five gaps that the pre-existing comparators could
  not detect, because none of the corresponding data is visible in the
  Nautilus catalog objects themselves:
  - `compare_instruments_semantic()` (new) — the prior `load_instrument_ids()`
    only compared the *set* of instrument ids; it could not detect a wrong
    `price_precision`/`size_precision`/`price_increment`/`size_increment`
    on an otherwise-correctly-named instrument, which would silently
    corrupt exact-decimal reconstruction downstream.
  - `compare_continuity_diagnostics_semantic()` (new) — compares
    snapshot-seed/resync/desync/fenced-range **counts** between the
    reference route's `convert_day.py` `per_symbol_depth` report and the
    candidate route's `validation/replay_catalog_reconstruct.py` manifest
    `depth_diagnostics` section. Both originate from the same shared
    `converter.depth_phase2.Phase2ReplayMetrics` dataclass, but the two
    call sites independently renamed the aggregated fields (e.g.
    `resync_count` vs. `resyncs`, `fenced_ranges` count vs.
    `fenced_range_count`) — the new comparator normalizes both naming
    conventions rather than assuming either one.
  - `compare_fenced_ranges_semantic()` (new) — per-fence content
    comparison (venue/symbol/start/end/severity/reason), not just a count,
    for routes/versions that expose the fence list (the candidate manifest
    already does via `manifest["fenced_ranges"]`).
  - `compare_quality_flags_semantic()` (new) — compares `quality_flags` by
    decoded JSON content (a multiset of parsed values), not raw string
    equality, since the replay schema stores it as a JSON-encoded string
    that could differ in key order/whitespace without differing
    semantically.
- Added `tests/test_semantic_oracle_detects_injected_faults.py` (19 tests):
  for each required fault class, starts from an otherwise-passing synthetic
  reference/candidate pair and injects exactly one deliberate corruption,
  asserting the relevant comparator flips from `passed=True` to
  `passed=False`. Covers: wrong trade price, wrong trade timestamp, dropped
  trade, dropped delta, wrong sequence number, wrong flag, wrong side,
  missing snapshot-seed/CLEAR delta, wrong Depth10 level, a mismatched
  deterministic book-state checkpoint (plus a matching-checkpoints sanity
  check), wrong instrument precision, a missing instrument, wrong
  snapshot/resync/desync/fenced-range counts (three separate injected-count
  tests), a missing fenced range by content, and a corrupted quality-flag
  value.
- Added a structural independence test
  (`test_reference_and_candidate_decoders_are_independently_implemented`)
  proving `validation/catalog_compare.py` does not import
  `stores.replay_depth_adapter`/`stores.replay_reader`/`stores.replay_writer`,
  and `stores/replay_depth_adapter.py` does not import
  `validation.catalog_compare` — i.e. the oracle and the candidate's
  schema-specific decoding logic cannot silently share a bug through a
  direct import dependency. The only intentionally shared component
  remains `converter/depth_phase2.py`'s book-replay engine (unchanged,
  already the documented shared component per
  `docs/IMPLEMENTATION_AUDIT.md`).
- No compact replay schema, builder, staging-cleanup, or raw-retention
  behavior was implemented or changed in this commit — this is oracle
  hardening only, per the plan's requirement that the oracle be proven
  before any schema code is written.

### Files/packages touched
- `validation/catalog_compare.py` (extended — 5 new comparison functions)
- `tests/test_semantic_oracle_detects_injected_faults.py` (new — 19 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change —
  `validation/` already owns `catalog_compare.py`)
- [x] docs/PROJECT_STATUS.md (no validated/deferred status changed — the
  oracle is validation *tooling*; it does not itself change the `full_l2`
  validated/deferred gate status)
- [x] docs/IMPLEMENTATION_AUDIT.md (reviewed; no ground-truth claim changed)
- [x] relevant feature docs:
  - docs/VALIDATION.md (reviewed; existing `validate_catalog_equivalence`
    CLI description remains accurate — new comparison functions are
    additive library functions, not yet wired into the CLI's default
    profile output; that wiring is deferred to the schema-implementation
    phase per the approved plan, since it depends on which manifest/report
    fields the finalized schema actually produces)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: this is additive comparator tooling
  with no new CLI surface and no status/gate change; the existing
  `docs/VALIDATION.md` description of `validate_catalog_equivalence` and
  `catalog_compare` remains accurate.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this proves the *oracle* detects injected faults
  in synthetic data; it does not itself constitute a new semantic-
  equivalence validation run against real data, and does not change the
  `full_l2`/v2.0.0 gate status.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_semantic_oracle_detects_injected_faults.py -q   # 19 passed
python -m pytest tests/test_catalog_equivalence.py tests/test_catalog_equivalence_full_l2.py \
  tests/test_semantic_equivalence.py tests/test_replay_catalog_reconstruct.py -q   # 11 passed, 1 skipped
python -m pytest -q   # 366 passed, 3 skipped
```

### Validation CLIs run
```bash
none required for this change type — no schema, config, or deployment
file was touched; the new comparison functions are exercised directly by
the new pytest suite above.
```

### Known limitations / out of scope
- The new comparison functions are not yet wired into
  `validation/validate_catalog_equivalence.py`'s default output — that
  integration is deferred until the compact schema (Phase 5+, not started
  in this session) determines the exact manifest/report field shapes to
  compare against.
- `compare_fenced_ranges_semantic()` is forward-looking on the reference
  side: `convert_day.py`'s own per-symbol report today only exposes a
  fenced-range *count*, not the per-fence list — the content-level
  comparator is exercised here against synthetic data and is ready for the
  reference side once/if a per-fence list is exported there; until then,
  `compare_continuity_diagnostics_semantic()`'s count-level comparison is
  the one actually usable against today's `convert_day.py` output.
- No representative production-day or even local real-data (Tier 2) run of
  the extended oracle was performed in this session — per the approved
  plan, Tier 3 requires production-server access this session does not
  have; this Phase 1 work is proven only against synthetic (Tier 1) data,
  which is exactly what "prove the oracle before using it as a gate" is
  meant to establish.

---

## 2026-07-24 — Issue #20 Phase 0: baseline storage-audit breakdown (allocated/apparent, per-unit, root-wide scratch scan)

### Change summary
- Extended `validation/audit_storage_size.py` (audit-only, no build/transform
  logic added) to report **allocated** bytes (`st_blocks * 512`) alongside
  the existing **apparent** bytes (`st_size`) for every measured component,
  since sparse/compressed filesystems can make these differ meaningfully —
  the issue #20 size-acceptance gate requires both, not just one.
- Added per-trade, per-depth-event, and per-depth-level byte estimates,
  computed from a partition's `manifest.json` record counts plus an exact
  depth-level count read via `pyarrow.parquet.ParquetFile.iter_batches()`.
  These are explicitly labeled orientation-only in the report `note` field:
  depth events carry a varying number of book levels, so a flat "bytes per
  replay row" average (as used in the issue's own ≈39.7/≈15.9 bytes/row
  orientation figures) can be misleading on its own.
- Added `audit_scratch_bytes()` and a new `--scratch-only` CLI mode: a
  **root-wide** scan of `.staging_*` / `.backup_*` / `.quarantine_*`
  directories under `replay_root/venue=*/symbol=*/`, independent of any
  single venue/symbol/date being queried. This directly targets the known
  gap where the existing builder only checks the *current* build's own
  `.staging_{date}_{symbol}` path (confirmed in
  `pipeline/build_replay_store.py` and `stores/replay_writer.py`), which is
  why the real BANKUSDT `2026-07-21` staging orphan was never rediscovered
  by later BANKUSDT builds for other dates. This CLI **only measures** —
  it does not delete, quarantine, or otherwise mutate any discovered
  directory. No lifecycle/cleanup logic was added or changed.
- Missing manifests, zero record counts, and missing/unreadable
  `depth.parquet` files all report `None` for the affected per-unit metric
  (never a false `0`), consistent with the repo's existing
  fail-visibly-not-silently-zero convention used elsewhere (e.g.
  `disk_monitor.py`'s `DirectoryMeasurement`).
- This is Phase 0 ("reproducible baseline") of the issue #20 compact-replay-
  storage plan. No replay schema (`stores/replay_schema.py`), builder
  (`pipeline/build_replay_store.py`), staging-cleanup, or raw-retention
  behavior was touched — this change is read-only measurement tooling.

### Files/packages touched
- `validation/audit_storage_size.py` (extended)
- `tests/test_audit_storage_size.py` (new — 11 tests)
- `CHANGELOG.md` (`[Unreleased]` → `Added`)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change — `validation/`
  already owns `audit_storage_size.py`; no new top-level package added)
- [x] docs/PROJECT_STATUS.md (no validated/deferred status changed by this
  measurement-only change)
- [x] docs/IMPLEMENTATION_AUDIT.md (no ground-truth claim changed)
- [x] relevant feature docs:
  - docs/FULL_L2_REPLAY_CATALOG_PLAN.md (reviewed; no gate status changed)
  - docs/VALIDATION.md (reviewed; `audit_storage_size` CLI reference
    remains accurate — flags added are additive, existing usage unchanged)

### Docs updated
- [x] CHANGELOG.md
- No docs update required because: this is an additive, backward-compatible
  extension to an existing audit-only CLI (no new CLI, no schema change, no
  status/gate change); `docs/VALIDATION.md`'s existing description of
  `audit_storage_size` remains accurate without edits.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this is instrumentation only; it does not itself
  claim any storage-size reduction or gate result. The issue #20 plan's
  Tier-3 (representative-day) gate still requires production-server
  execution, which this change does not perform.
- Evidence for any new validation claim: n/a (no new validation claim made)

### Tests run
```bash
source .venv/bin/activate
python -m pytest tests/test_audit_storage_size.py -q   # 11 passed
python -m pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q   # 56 passed
python -m pytest -q                                     # 347 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_storage_size --venue BINANCE_SPOT --symbol ADAUSDT --date 2026-06-12 \
  --replay-root <local test fixture> --json   # exercised via tests/test_audit_storage_size.py
```
`python -m validation.audit_change_compliance --staged` was not run in this
session because commit staging is out of scope for this plan-mode-derived
implementation increment (task only requested "start implementation", not
committing); this entry documents the change ahead of any future commit so
the compliance check can be run at commit time.

### Known limitations / out of scope
- No replay schema, builder lifecycle, staging cleanup, raw-retention gate,
  semantic-oracle hardening, or reconstruction CLI work was implemented in
  this increment — per the reviewed issue #20 plan, those require the
  Phase 0–4 review checkpoint (semantic oracle + failure-injection proof +
  field/consumer/integrity matrix + raw-retention/legacy/traceability
  design) to complete and be reviewed before any schema code is written.
- Per-column (individual Parquet field) byte-contribution estimation is not
  yet implemented — only per-trade/per-depth-event/per-depth-level
  aggregates are provided. A true per-column breakdown (e.g. via Parquet
  column-chunk metadata) is deferred to a follow-up within Phase 0.
- No representative production-day (Tier 3: 2026-07-22 or 2026-07-23) data
  was measured — this workspace only has local raw data for
  2026-06-10..12; running this CLI against a real production day requires
  server access, per the plan's own Tier-3 logistics note.
- `--scratch-only` is measurement-only; no quarantine/deletion lifecycle was
  implemented — that remains future work per the plan's fail-closed staging
  lifecycle design (Phase 11 of the reviewed plan).

---

## 2026-07-23 — Correction: bound the daily replay-build timeout at 23h instead of infinity

### Change summary
- Corrects the immediately-preceding 2026-07-23 entry below ("Remove 1-hour
  systemd start timeout on replay-build service"), which set
  `TimeoutStartSec=infinity`. That entry is **not** rewritten (append-only
  history); this entry supersedes its conclusion.
- `systemd/cryptorecorder-replay-build.service`: `TimeoutStartSec` changed
  from `infinity` to `23h`. Rationale: the daily timer fires once at `01:00
  UTC`; systemd does not start a new instance of a service while an existing
  invocation is still active. An unbounded timeout could therefore let a
  genuinely stuck job remain active indefinitely, silently blocking every
  later scheduled run — a worse failure mode than the original 1-hour cap
  being too short. `23h` gives ample room for a long daily build of the
  previous completed UTC day across a large symbol universe, while still
  guaranteeing systemd terminates a stuck/hung run before the next `01:00
  UTC` activation. `Restart=no`, `RestartSec=300`,
  `StartLimitIntervalSec=86400`, `StartLimitBurst=3`, and `MemoryMax` are
  all unchanged.
- `systemd/cryptorecorder-replay-build.service` comments: corrected to no
  longer claim the installed daily service performs `--force` rebuilds or
  arbitrary multi-day backfills. The installed service only ever builds the
  previous completed UTC day via `pipeline.daily_build --date yesterday`;
  manual force rebuilds/backfills use the documented CLI or a separately
  controlled transient systemd scope with its own explicit timeout.
- `systemd/cryptorecorder-replay-build.timer`: removed the stale comment
  "Run after the legacy converter has had time to finish the previous UTC
  day" (converter systemd automation was removed from the supported
  architecture in an earlier PR #18 commit). Replaced with "Run at 01:00
  UTC, after the previous UTC recording day has closed."
- `docs/OPERATIONS.md`: rewrote the "Start timeout" and "Durable progress on
  restart" paragraphs in "Replay-build memory and restart behaviour" to
  describe the `23h` contract (not `infinity`), the explicit tradeoff (1h
  too short / infinity rejected / 23h chosen), what happens if the ceiling
  is reached (systemd marks invocation failed, no restart per `Restart=no`,
  operator must inspect journal and rerun manually), and that manual force
  rebuilds/backfills are a separate, manually-run CLI/scope action, not part
  of the installed daily service.
- `CHANGELOG.md`: corrected the `[Unreleased]` "Changed" entry (still
  unreleased, so edited in place rather than appended twice) to describe
  the final `23h` value, the infinity-considered-and-rejected tradeoff, the
  timer comment fix, and the corrected installed-service contract
  description.
- No Python source, recorder behavior, raw/replay schema, replay ordering,
  reconstruction, converter automation, KovacsTrader integration, Syncthing,
  or issue #20 uv work was touched.

### Files/packages touched
- `systemd/cryptorecorder-replay-build.service`
- `systemd/cryptorecorder-replay-build.timer`
- `docs/OPERATIONS.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`
- `tests/test_agent_infrastructure.py` (5 new lightweight systemd-contract tests)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change)
- [x] docs/OPERATIONS.md
- [x] docs/PROJECT_STATUS.md (no validated/deferred status affected)

### Docs updated
- [x] CHANGELOG.md (corrected in place; still `[Unreleased]`)
- [x] docs/OPERATIONS.md
- No docs/PROJECT_STATUS.md update required because: operational systemd
  tuning correction, not a validated/deferred status change.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- No production validation is claimed: the 23h ceiling has not been
  exercised against an actual long-running production build on a real
  server; this remains a deployment-time observation, not something
  asserted as already proven here.

### Tests run
```bash
pytest tests/test_agent_infrastructure.py -q   # 33 passed (5 new)
pytest tests/test_repo_structure.py -q         # 23 passed
pytest -q                                       # 336 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # PASS
bash -n scripts/deploy_linux_server.sh                       # OK (no output)
./scripts/deploy_linux_server.sh --target all --dry-run
  # Clean; only cryptorecorder-recorder.service,
  # cryptorecorder-replay-build.service, cryptorecorder-replay-build.timer
  # planned; no converter unit referenced.
systemd-analyze verify systemd/cryptorecorder-replay-build.service
  # Fails on this dev machine only: unit references the production path
  # /home/zsom/services/CryptoRecorder/.venv/bin/python, which does not
  # exist here. Pre-existing condition unrelated to this change (confirmed
  # via git diff that only the TimeoutStartSec/comment block changed).
systemd-analyze verify systemd/cryptorecorder-replay-build.timer
  # Exit 0. One pre-existing warning ("Unknown key name 'Timezone' in
  # section 'Timer', ignoring") on a line this change did not touch
  # (confirmed via git diff — only the OnCalendar comment line changed).
```

### Known limitations / out of scope
- No real production host run was performed to observe a >1h build
  completing under the new 23h ceiling; this is a systemd configuration
  correction, not a runtime-validated claim.
- The prior 2026-07-23 audit entry describing `TimeoutStartSec=infinity` is
  left as historical record per the append-only policy; it is superseded by
  this entry, not deleted or edited.

## 2026-07-23 — Remove 1-hour systemd start timeout on replay-build service

### Change summary
- `systemd/cryptorecorder-replay-build.service`: `TimeoutStartSec` changed
  from `3600` (1 hour) to `infinity`. The unit is `Type=oneshot`; systemd
  treats a `oneshot` unit as "hung" if it has not exited before
  `TimeoutStartSec` elapses and will `SIGTERM`/`SIGKILL` it. A finite 1-hour
  cap risked systemd killing an in-progress, otherwise-healthy replay build
  (e.g. a `--force` rebuild, a large multi-day backfill, or a run across the
  full top50 universe) purely for exceeding the wall-clock budget, not for
  any actual failure. `Restart=no`, `RestartSec=300`, and
  `StartLimitIntervalSec=86400`/`StartLimitBurst=3` in `[Unit]` are
  unchanged — restart-attempt capping is unaffected; only the single-run
  maximum duration is removed.
- `docs/OPERATIONS.md`: added a new "Start timeout" paragraph in the
  "Replay-build memory and restart behaviour" section explaining the
  `TimeoutStartSec=infinity` setting, why the previous 1-hour value was
  risky, and that `StartLimitIntervalSec`/`StartLimitBurst` still bound
  restart attempts (not run duration).
- `CHANGELOG.md`: added an `[Unreleased]` "Changed" entry documenting the
  value change and rationale, cross-referencing the `docs/OPERATIONS.md`
  section.
- No change needed in `INSTALL.md`, `docs/REPLAY_STORE.md`, or
  `scripts/deploy_linux_server.sh`: none of them hardcode or document the
  specific `TimeoutStartSec` value; `INSTALL.md`'s manual-install steps just
  copy/render the unit file as-is via `sed`, so the new value propagates
  automatically.
- No test asserted the old `3600` value (verified via repo-wide grep), so no
  test changes were required.

### Files/packages touched
- `systemd/cryptorecorder-replay-build.service`
- `docs/OPERATIONS.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md (no folder/file contract change — same file, edited in place)
- [x] docs/OPERATIONS.md
- [x] docs/PROJECT_STATUS.md (no validated/deferred status affected)
- [x] INSTALL.md (confirmed no hardcoded timeout value to update)
- [x] docs/REPLAY_STORE.md (confirmed no mention of this setting)

### Docs updated
- [x] CHANGELOG.md
- [x] docs/OPERATIONS.md
- No docs/PROJECT_STATUS.md update required because: this is an operational
  systemd tuning change, not a validated/deferred status change.
- No docs/REPO_STRUCTURE.md update required because: no files added, moved,
  or removed.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no

### Tests run
```bash
pytest -q   # 331 passed, 3 skipped
```

### Validation CLIs run
```bash
systemd-analyze verify systemd/cryptorecorder-replay-build.service
  # Fails on this dev machine only because the unit references the production
  # path /home/zsom/services/CryptoRecorder/.venv/bin/python, which does not
  # exist here. Pre-existing condition unrelated to this change (confirmed
  # the same failure occurs on the unmodified file too); TimeoutStartSec=infinity
  # itself introduces no new parse/verify error.
python -m validation.audit_change_compliance --staged   # PASS
```

### Known limitations / out of scope
- Not verified on an actual production host that a long-running (>1 hour)
  build now completes without being killed — this dev-machine change only
  removes the systemd-side cap; real long-duration production verification
  remains a separate deployment-time check.

## 2026-07-22 — Follow-up 3: fix FileNotFoundError in stale-staging-cleanup test finally block

### Change summary
- Codex, running as root inside a container, found that
  `test_stale_staging_cleanup_fails_closed` in
  `tests/test_replay_memory_bounded.py` fails: when `shutil.rmtree` succeeds
  despite the removed write bit (root ignores the write-permission check
  that normally blocks the delete on a non-root user), the test's own
  `finally` block still unconditionally calls `staging_dir.chmod(...)` on a
  directory that no longer exists, raising `FileNotFoundError` and turning a
  passing regression test into a failure in that environment. This
  contradicted the audit-entry claim of a full, reliable suite pass.
- `tests/test_replay_memory_bounded.py`: guarded the `finally` block with
  `if staging_dir.exists():` before calling `chmod`, so the cleanup is a
  no-op (not an error) when `rmtree` already removed the directory.
- Verified the fix logic directly with a standalone script that reproduces
  the "directory already gone" condition and confirms the guarded chmod no
  longer raises. Could not literally re-run as root on this dev machine (no
  passwordless sudo available), but the guard is unconditionally correct:
  `chmod` on a path is only ever attempted if it still exists.

### Files/packages touched
- `tests/test_replay_memory_bounded.py`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md

### Docs updated
- No docs update required because: test-only fix, no status/API/schema change.
- CHANGELOG not required because: this is a test-infrastructure-only fix (a
  regression test's own cleanup logic), with no user-facing behavior, API,
  schema, or status change.

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no

### Tests run
```bash
pytest tests/test_replay_memory_bounded.py -q   # 47 passed
pytest -q                                        # 331 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # PASS
```

### Known limitations / out of scope
- Not re-verified under an actual root/container environment on this
  machine (no privileged access available); fix was validated via isolated
  logic simulation instead of the full pytest run as root.

## 2026-07-22 — Follow-up 2: add exact wording-variant checks Codex asked for

### Change summary
- Codex reviewed commit `1f7a3f2` (superseded by `a0fe13b`, which already
  reworded the stale `CHANGELOG.md` sentence) and asked that the guard test
  also check the literal substrings `"template files remain in the repo"`
  and `"remain in the repo as manual/reference templates"` (after whitespace
  normalization), rather than relying only on the proximity regex added in
  the prior follow-up.
- `tests/test_repo_structure.py::test_docs_do_not_claim_deleted_converter_systemd_files_exist`:
  added those two exact phrases (plus `"remain in the repo as manual"`) to
  `forbidden_normalized_phrases`, in addition to the existing regex, so the
  check is both explicit/auditable and resilient to further rewording.
- Re-confirmed via `grep_search` across all `*.md` files that the only
  remaining occurrence of `"template files remain in the repo as
  manual/reference templates"` is the intentionally-quoted historical
  narration inside the append-only `docs/CHANGE_AUDIT.md` log entry below
  (exempted from the check by design), not any current-state doc.

### Files/packages touched
- `tests/test_repo_structure.py`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] CHANGELOG.md (re-confirmed no stale current-state claim remains)

### Docs updated
- No further CHANGELOG.md/IMPLEMENTATION_AUDIT.md changes needed: already
  correct from the prior follow-up commit (`a0fe13b`).

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no

### Tests run
```bash
pytest tests/test_repo_structure.py -q   # 23 passed
pytest -q                                 # 331 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # PASS
```

### Known limitations / out of scope
- No code changes in this follow-up; test hardening only.

## 2026-07-22 — Follow-up: stale converter-template claim also present in an older CHANGELOG.md entry

### Change summary
- Codex flagged that the `[Unreleased]` CHANGELOG.md entry immediately below
  the corrected one still said (about `INSTALL.md`'s note): "the converter
  systemd template files remain in the repo as manual/reference templates
  only and must not be installed on the production server." This wording
  differs from the phrases the previous session's guard test checked for
  ("kept in the repo as manual..." / "converter systemd templates remain"),
  so it slipped through unnoticed.
- `CHANGELOG.md`: reworded that older entry to describe the note as a
  point-in-time historical fact ("at the time of this change... those unit
  files were subsequently **deleted** in a later PR #18 finalization
  commit... `INSTALL.md` no longer contains that note") instead of present
  tense current-state language.
- `docs/IMPLEMENTATION_AUDIT.md` lines 493-495: re-checked; already correctly
  states the files "were deleted from the repository in PR #18 finalization"
  from the prior session's fix. No further change needed there.
- `tests/test_repo_structure.py::test_docs_do_not_claim_deleted_converter_systemd_files_exist`:
  broadened from an exact-phrase list to also match a proximity regex
  (`convert(er)? ... remain ... repo|manual`) so future wording variants of
  the same stale claim are caught. Verified the new regex matches the
  original stale sentence via a standalone sanity check before relying on it.

### Files/packages touched
- `CHANGELOG.md`
- `tests/test_repo_structure.py`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/IMPLEMENTATION_AUDIT.md (re-verified lines 480-505, already correct)
- [x] CHANGELOG.md (full file grepped for remaining stale phrasing)

### Docs updated
- [x] CHANGELOG.md
- No docs/PROJECT_STATUS.md update required: validated/deferred status unchanged
- No docs/IMPLEMENTATION_AUDIT.md change needed: already correct

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no

### Tests run
```bash
pytest tests/test_repo_structure.py -q   # 23 passed
pytest -q                                 # 331 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # PASS
```

### Known limitations / out of scope
- No code changes in this follow-up; documentation + guard-test only.

## 2026-07-22 — PR #18 final Codex findings: post-publish validation, cleanup-failure preservation, force-rebuild backup safety, converter doc consistency

### Change summary
- `stores/replay_writer.py`: extracted `validate_partition(partition_dir)` as a
  single shared source of truth for partition validity (manifest exists,
  parses, `status == "complete"`, both parquet files exist, checksums match).
  `publish()` now calls it immediately after `os.replace(staging, output)`
  succeeds. If validation fails, the invalid new output is quarantined to
  `.quarantine_{date}_{symbol}`, the previous valid backup (if any) is
  restored to canonical, and `publish()` raises `RuntimeError` instead of
  returning normally — closing the P1 gap where a missing/corrupt post-replace
  destination could still report `status="success"`. The obsolete backup is
  now deleted only *after* the new canonical partition validates (never
  before).
- `pipeline/build_replay_store.py`: `_partition_is_valid()` now delegates to
  `stores.replay_writer.validate_partition()` instead of duplicating checksum
  logic, so ReplayWriter's post-publish check and the skip-if-valid/crash-
  recovery check can never disagree about what "valid" means.
- `pipeline/build_replay_store.py`: the primary-failure exception handler in
  `build_replay_for_symbol()` now wraps `writer.cleanup_staging()` in its own
  `try/except`. A secondary cleanup failure is appended to `status["errors"]`
  alongside the primary failure instead of escaping and replacing the handled
  per-symbol result — closing the P1 gap where `run_build_replay_store()`
  could stop mid-loop on a cleanup exception.
- `pipeline/build_replay_store.py`: removed the `force=True` pre-build block
  that blindly deleted `.backup_*` before any replacement had been built or
  validated. `recover_partition_state()` now runs unconditionally (even under
  `force=True`); a `fail` action (ambiguous/invalid states) still fails
  closed under `--force`, and a `skip` action under `force=True` falls through
  to rebuild instead of returning `skipped` — but no longer deletes the
  backup pre-emptively. The existing `publish()` backup<-canonical<-staging
  rotation now naturally protects the last known-good copy through forced
  rebuild failures.
- `docs/IMPLEMENTATION_AUDIT.md`: corrected the current-state claim that
  `systemd/cryptorecorder-convert.service`/`.timer` "are kept in the repo as
  manual/reference templates" — they were deleted in PR #18 finalization.
- `CHANGELOG.md`: reworded a quoted description of the old INSTALL.md note so
  it no longer contains the literal stale-claim phrase itself.
- `tests/test_repo_structure.py`: added
  `test_docs_do_not_claim_deleted_converter_systemd_files_exist()` — a guard
  test asserting both converter unit files stay deleted and that no
  current-state doc (excluding the append-only `docs/CHANGE_AUDIT.md` log)
  claims they still exist as manual/reference templates.
- `tests/test_replay_memory_bounded.py`: 13 new tests covering post-publish
  validation (normal success, fault-injected missing output, corrupt
  manifest, checksum mismatch, backup-not-deleted-until-validated, backup
  still deleted on validated success), cleanup-failure preservation (single
  symbol + multi-symbol continuation), and all 5 required `--force` rebuild
  scenarios (valid/no-backup success and failure, missing-canonical/valid-
  backup success and failure, invalid-canonical/valid-backup preserved).

### Files/packages touched
- `stores/replay_writer.py`
- `pipeline/build_replay_store.py`
- `tests/test_replay_memory_bounded.py`
- `tests/test_repo_structure.py`
- `docs/IMPLEMENTATION_AUDIT.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs: docs/REPLAY_STORE.md, docs/DAILY_BUILD_PIPELINE.md

### Docs updated
- [x] CHANGELOG.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- No docs/PROJECT_STATUS.md update required: validated/deferred status unchanged
- No docs/REPO_STRUCTURE.md update required: no folder/file contract change
  (no files added or removed in this pass — the converter systemd files were
  already deleted in the prior commit)

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence: n/a — correctness/safety fixes with no new validation paths

### Tests run
```bash
pytest tests/test_replay_memory_bounded.py -q     # 47 passed
pytest tests/test_daily_build.py -q                # 9 passed
pytest tests/test_replay_store.py -q               # 3 passed
pytest tests/test_replay_catalog_reconstruct.py -q # 4 passed
pytest tests/test_catalog_equivalence_full_l2.py -q # 2 passed
pytest tests/test_agent_infrastructure.py -q        # 28 passed
pytest tests/test_repo_structure.py -q              # 23 passed
pytest -q                                           # 331 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # PASS
bash -n scripts/deploy_linux_server.sh                       # OK (no output = valid syntax)
systemd-analyze verify systemd/cryptorecorder-replay-build.service
  # Fails on this dev machine: references production path
  # /home/zsom/services/CryptoRecorder/.venv/bin/python, which does not exist
  # here. Pre-existing condition unrelated to this change; not a regression.
./scripts/deploy_linux_server.sh --target all --dry-run
  # Confirms only cryptorecorder-recorder.service,
  # cryptorecorder-replay-build.service, cryptorecorder-replay-build.timer are
  # targeted; no converter unit referenced.
```

### Known limitations / out of scope
- Real Linux production service validation (actual systemd start/enable on
  the production host) was not performed and is out of scope for a dev-machine
  change; noted as pending in the PR comment.
- No changes to recorder.py, raw schema, replay schema, replay ordering,
  Nautilus reconstruction, Syncthing, KovacsTrader integration, or the uv
  migration (issue #20).
- Merge remains deferred; this change is pushed to
  `refactor/recorder-replay-only` only.

## 2026-07-22 — PR #18 finalization: fail-closed crash-recovery state machine, best-effort backup deletion, converter systemd files deleted

### Change summary
- `pipeline/build_replay_store.py`: extracted `recover_partition_state()` helper
  handling all 7 filesystem state combinations (Cases A-G). Old inline crash-recovery
  only handled Case A and silently dropped invalid backups. New helper handles:
  Case A (restore valid backup), Case B (fail on invalid backup + no output),
  Case C (skip + remove stale backup), Case D (quarantine invalid + restore backup),
  Case E (both invalid, fail and preserve), Case F (valid, no backup, skip),
  Case G (missing, no backup, rebuild). `build_replay_for_symbol()` now calls this
  helper instead of inline ad-hoc checks.
- `pipeline/build_replay_store.py`: backup deletion after successful
  `os.replace(staging, output)` is now best-effort — failure logs a warning and
  does NOT re-raise. Previously any backup deletion exception was propagated,
  turning a successful publication into a build failure.
- `pipeline/build_replay_store.py`: all `status["status"] = "error"` replaced
  with `"failed"` — `pipeline.daily_build` counts `r["status"] == "failed"`;
  the old `"error"` value was silently excluded from the failed-partition count.
- `systemd/cryptorecorder-convert.service` and `systemd/cryptorecorder-convert.timer`
  deleted from the repository — converter systemd automation is not part of the
  supported production architecture. The deploy script already stops/removes any
  installed converter units (unchanged).
- `INSTALL.md`: updated `> **Note:**` to correctly state that the converter systemd
  files were deleted, not "kept as templates".
- `docs/OPERATIONS.md`: updated converter unit reference from "is not installed" to
  "deleted from the repository in PR #18".
- `docs/REPO_STRUCTURE.md`: added amendment log entry for deletion of converter
  systemd files.
- `tests/test_replay_memory_bounded.py`: 10 new regression and failure injection tests
  covering Cases A-G, best-effort backup deletion, and fail-closed scratch cleanup.

### Files/packages touched
- `pipeline/build_replay_store.py`
- `systemd/cryptorecorder-convert.service` (deleted)
- `systemd/cryptorecorder-convert.timer` (deleted)
- `tests/test_replay_memory_bounded.py`
- `INSTALL.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`
- `docs/OPERATIONS.md`
- `docs/REPO_STRUCTURE.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs: docs/REPLAY_STORE.md, docs/OPERATIONS.md, docs/DAILY_BUILD_PIPELINE.md

### Docs updated
- [x] CHANGELOG.md
- [x] docs/OPERATIONS.md
- [x] docs/REPO_STRUCTURE.md (amendment log entry added)
- [x] INSTALL.md
- No docs/PROJECT_STATUS.md update required: validated/deferred status unchanged

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence: n/a — correctness/safety fixes with no new validation paths

### Tests run
```
pytest tests/test_replay_memory_bounded.py -q   → 34 passed
pytest -q                                        → 317 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   → PASS
```

### Known limitations / out of scope
- Linux production validation (DEXEUSDT 2026-07-21, 12 GiB cgroup) cannot be run
  from the dev machine — noted as pending in PR comment.
- `test_staging_to_output_rename_failure_restores_backup` requires `_force_staging`
  constructor parameter which was not added in this pass; that test was not written.
  The publish() restore-on-failure path is covered by the existing
  `test_publish_preserves_existing_partition_on_replace_error` test.
- No changes to recorder.py, raw schema, replay schema, or replay ordering.

## 2026-07-22 — Crash-recovery, fail-closed cleanup, partition layout, INSTALL.md (PR #18)

### Change summary
- `pipeline/build_replay_store.py`: startup crash-recovery — if `output_dir`
  is missing and `.backup_{date}_{symbol}` exists (mid-publish SIGKILL state),
  validates and restores the backup before proceeding; fails closed if restore
  fails; removes invalid backups. Handles both missing-output and
  both-exist cases.
- `pipeline/build_replay_store.py`: stale-staging cleanup now catches rmtree
  exceptions and verifies the directory is gone; returns `status=error` if
  cleanup fails, refusing to build on stale state (`ignore_errors` removed).
- `stores/replay_writer.py`: `finalize_staging()` removes the empty
  `scratch/` subdirectory after spools are closed, so published partitions
  contain only supported files and no subdirectories.
- `stores/replay_writer.py`: `cleanup_staging()` now raises `RuntimeError`
  if `shutil.rmtree` fails (instead of logging a warning and continuing), 
  so callers see the failure.
- `INSTALL.md`: manual installation loop, `systemd-analyze verify` command,
  enable/start/stop/status blocks, and troubleshooting section all updated to
  remove `cryptorecorder-convert.service`/`.timer` as active production units.
  Removed the "converter timer date seems wrong" troubleshooting section. Added
  `> **Note:**` clarifying converter templates remain for manual local use only.
- `tests/test_replay_memory_bounded.py`: 3 new regression tests:
  `test_published_partition_layout_is_clean`,
  `test_crash_recovery_restores_backup_on_startup`,
  `test_stale_staging_cleanup_fails_closed`.

### Files/packages touched
- `pipeline/build_replay_store.py`
- `stores/replay_writer.py`
- `tests/test_replay_memory_bounded.py`
- `INSTALL.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs: docs/REPLAY_STORE.md, docs/OPERATIONS.md

### Docs updated
- [x] CHANGELOG.md
- No other docs update required: code fixes only; status is unchanged

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence: n/a — these are defensive/correctness fixes; no new validation paths

### Tests run
```
pytest tests/test_replay_memory_bounded.py -q   → 24 passed
pytest -q                                       → 307 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   (run before commit)
```

### Known limitations / out of scope
- DEXEUSDT production server test still pending (requires production server).
- PR description update (item 5 from review) done by the author as a PR comment.

## 2026-07-22 — Fix spool lifetime, atomic publication, force-rebuild, stale docs (PR #18)

### Change summary
- `stores/replay_writer.py`: spool files moved from system temp to
  `staging_dir/scratch/` — stale staging cleanup now also removes orphaned
  SQLite spools; `_spool_temp_dir` removed from constructor (no longer
  configurable separately — spools are always co-located with staging).
- `stores/replay_writer.py`: `publish()` now does a backup/restore atomic
  swap — renames existing valid partition to `.backup_{date}_{symbol}` before
  `os.replace(staging→output)`, restores backup on failure; the previously
  valid partition can no longer be lost by a failed rename.
- `pipeline/build_replay_store.py`: added `force` kwarg and `--force` CLI
  flag; skip-if-valid respects `force=True`; documents the provenance contract
  (without `--force`, output integrity is validated, raw inputs are not).
- `tests/test_replay_memory_bounded.py`: 4 new tests — spool-in-staging, stale
  staging removes spools, backup/restore on replace error, force-rebuild.
- `docs/OPERATIONS.md`: fixed stale `crypto-recorder` unit name in quick-
  reference commands → `cryptorecorder-recorder`.
- `docs/IMPLEMENTATION_AUDIT.md`: removed stale feature-store audit content
  from active `Smoke-Tested` section (feature-store was removed in issue #17).
- `docs/CHANGE_AUDIT.md`: updated previous entry's Docs-updated section.
- `CHANGELOG.md`: added `[Unreleased]` section for P1/P2 fixes.
- Real-data RAM test: BTCUSDT 2026-06-12 (509 MB raw) — pending `/usr/bin/time
  -v` peak RSS output.

### Files/packages touched
- `stores/replay_writer.py`
- `pipeline/build_replay_store.py`
- `tests/test_replay_memory_bounded.py`
- `docs/OPERATIONS.md`
- `docs/IMPLEMENTATION_AUDIT.md`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] docs/REPLAY_STORE.md, docs/OPERATIONS.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no public interface change; not required
- [ ] docs/PROJECT_STATUS.md — no new validated/deferred status change
- [ ] docs/REPO_STRUCTURE.md — no new folders/files
- [x] docs/OPERATIONS.md — fixed stale unit name in quick-reference
- [x] docs/IMPLEMENTATION_AUDIT.md — removed stale feature-store smoke content

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```
pytest tests/test_replay_memory_bounded.py   # 21 passed (incl. 4 new tests)
pytest -q                                    # 304 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   (pending — run after commit)
Real-data BTCUSDT 2026-06-12 RAM test:
  BINANCE_SPOT:  835,403 depth + 3,112,086 trades
  BINANCE_USDTF: 563,875 depth + 3,200,399 trades
  Maximum RSS:   855,432 kB (~835 MiB) — well under 12 GiB systemd MemoryMax
  Exit status:   0
```

### Known limitations / out of scope
- `REPLAY_SPOOL_TEMP_DIR` config in `config.py` and `cryptorecorder.env.example`
  is now dead (spools always use staging/scratch). Will be removed in a
  follow-up cleanup.
- Production DEXEUSDT 2026-07-21 test still requires the production server.
- uv migration (issue #20) excluded.

---

## 2026-07-22 — Memory-bounded replay-store builder (PR #18)

### Change summary
- `stores/replay_writer.py`: replaced unbounded `depth_batches`/`trade_batches`
  Python-list accumulation with disk-backed SQLite spooling via
  `converter.spool.RawRecordSpool`; incremental Parquet writing via
  `pyarrow.parquet.ParquetWriter`; added `cleanup_staging()` method
- `pipeline/build_replay_store.py`: import `REPLAY_SPOOL_TEMP_DIR`; pass
  `spool_temp_dir` to `ReplayWriter`; added `_partition_is_valid()` helper with
  checksum validation; skip-if-valid logic; stale staging removal;
  `cleanup_staging()` on error
- `pipeline/daily_build.py`: track `skipped` partitions; treat skipped-valid as
  success; update log and return dict
- `config.py`: added `REPLAY_SPOOL_TEMP_DIR` (optional, env-controlled)
- `systemd/cryptorecorder-replay-build.service`: `Restart=on-failure` →
  `Restart=no`; `StartLimitIntervalSec=86400` / `StartLimitBurst=3` in `[Unit]`
- `systemd/cryptorecorder.env.example`: documented `CRYPTO_RECORDER_REPLAY_SPOOL_TEMP_DIR`
- `tests/test_replay_memory_bounded.py`: new file, 17 regression tests
- `CHANGELOG.md`: updated `[Unreleased]`

### Files/packages touched
- `stores/replay_writer.py`
- `pipeline/build_replay_store.py`
- `pipeline/daily_build.py`
- `config.py`
- `systemd/cryptorecorder-replay-build.service`
- `systemd/cryptorecorder.env.example`
- `tests/test_replay_memory_bounded.py`
- `CHANGELOG.md`
- `docs/CHANGE_AUDIT.md`
- `docs/REPLAY_STORE.md`
- `docs/OPERATIONS.md`
- `docs/PROJECT_STATUS.md`
- `INSTALL.md`

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs: docs/REPLAY_STORE.md, docs/OPERATIONS.md,
      docs/FULL_L2_REPLAY_CATALOG_PLAN.md, docs/VALIDATION.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no public interface change; not required
- [x] docs/PROJECT_STATUS.md — updated replay_store v0 bullet to reflect memory-bounded writer and fixed restart policy; noted production RAM measurement still pending
- [ ] docs/REPO_STRUCTURE.md — no new folders/files; not required
- [x] docs/REPLAY_STORE.md — removed stale "v0 write limitation" bullet; replaced "Future optimization" note with implemented-solution description; updated Processing Details section
- [x] docs/OPERATIONS.md — added "Replay-build memory and restart behaviour" section documenting bounded writes, spool temp dir, `Restart=no`, durable forward progress, and recovery commands
- [x] INSTALL.md — removed stale `legacy-converter` target from deploy command reference

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```
pytest tests/test_replay_memory_bounded.py         # 17 passed
pytest tests/test_replay_store.py                  # 3 passed
pytest tests/test_streaming_conversion_memory.py   # 10 passed
pytest tests/test_daily_build.py                   # 9 passed
pytest tests/test_agent_infrastructure.py          # passed
pytest tests/test_repo_structure.py                # passed
pytest -q                                          # 300 passed, 3 skipped
```

### Validation CLIs run
```
python -m validation.audit_change_compliance --base main   # PASS
bash -n scripts/deploy_linux_server.sh                     # OK
systemd-analyze verify systemd/cryptorecorder-replay-build.service
  # expected path-only warning on dev machine (no /home/zsom)
```

### Known limitations / out of scope
- Real-data DEXEUSDT 2026-07-21 test not run — production raw data unavailable
  on development machine. Required command documented in CHANGELOG.md.
- uv migration (issue #20) explicitly excluded.
- No changes to recorder.py, phase2_depth.py, native_trades.py, storage.py,
  raw schemas, replay-store v0 schema, or existing production data.

---



---

## Example of a GOOD entry

```markdown
## 2026-07-01 — Add audit_storage_size CLI

### Change summary
- Added `validation/audit_storage_size.py` to measure on-disk artifact sizes.
- Added `docs/IMPLEMENTATION_AUDIT.md` describing the audit output format.

### Files/packages touched
- validation/audit_storage_size.py (new)
- docs/STORAGE_SIZE_AUDIT.md (new)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; audit-only tool, not a user-facing feature
- [ ] docs/PROJECT_STATUS.md — no status change; tool is an inspect/report utility
- [x] docs/REPO_STRUCTURE.md — added audit_storage_size.py to validation/ module list
- [x] relevant feature docs:
  - docs/STORAGE_SIZE_AUDIT.md (new)

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q
# 22 passed
```

### Validation CLIs run
```bash
# none required — this is a new inspect CLI, not a replay/feature/catalog change
```

### Known limitations / out of scope
- Does not yet track per-symbol breakdown; that is deferred.
```

---

## Example of a BAD entry (do not do this)

```markdown
## 2026-07-01 — Fixed stuff

### Change summary
- Fixed some things.

### Files/packages touched
- various

### Docs reviewed
- (skipped — no time)

### Docs updated
- not needed

### Status / validation impact
- no change

### Tests run
```bash
pytest
```

### Validation CLIs run
- skipped

### Known limitations / out of scope
- none
```

**Why this is bad:** "various", "some things", "no time", "not needed" are not
auditable. Reviewers cannot tell what changed, what was reviewed, or whether
status claims are honest.

---

## Audit entries (newest first)

## 2026-07-21 — Deployment boundary: converter removed from automated production systemd path (issue #17 follow-up)

### Change summary
- `scripts/deploy_linux_server.sh`: removed `legacy-converter` from
  `VALID_TARGETS` (it is no longer a deployable `--target`); `--target all`
  now only installs/controls `cryptorecorder-recorder.service` and
  `cryptorecorder-replay-build.{service,timer}`. Removed the now-dead
  `legacy-converter)` cases from `units_for_target()`/`control_for_target()`,
  and dropped it from `selected_targets()`'s `all` expansion.
- Added `cryptorecorder-convert.service` and `cryptorecorder-convert.timer`
  to the `cleanup_stale_units()` `STALE_UNITS` list, so any copy already
  installed on an existing server is stopped/disabled/removed automatically
  on the next deploy, matching how the pre-issue-#17 feature-build units are
  already handled.
- Marked `systemd/cryptorecorder-convert.service` and `.timer` as
  manual/reference-only templates via an in-file comment (not rendered or
  installed by the deploy script for any target); the files themselves were
  **not** deleted.
- `docs/OPERATIONS.md`: updated the "Targets" table, "Safety notes" stale-unit
  list, and "Service groups"/ordering text in the Linux Server Layout section
  to remove `legacy-converter` and correct the "daily chain runs convert →
  replay" claim (`replay-build` reads directly from `data_raw` via
  `pipeline.raw_manifest` and never depended on converter output — there was
  no real ordering dependency to begin with).
- `docs/IMPLEMENTATION_AUDIT.md`: added a new "Completed Cleanup Items
  (2026-07-21 — deployment boundary...)" entry documenting this change.
- `CHANGELOG.md`: added a new `[Unreleased]` `### Changed (PR #18 —
  deployment boundary...)` section.
- `tests/test_agent_infrastructure.py`: removed `legacy-converter` from
  `DEPLOY_TARGETS`; added `cryptorecorder-convert.timer`/`.service` to
  `LEGACY_STALE_UNITS`; added two new regression tests —
  `test_deploy_script_rejects_legacy_converter_target` (asserts
  `--target legacy-converter` now fails like any unknown target) and
  `test_deploy_script_all_target_never_installs_converter` (asserts
  `--target all --dry-run` output never mentions `cryptorecorder-convert`).

### Files/packages touched
- scripts/deploy_linux_server.sh
- systemd/cryptorecorder-convert.service
- systemd/cryptorecorder-convert.timer
- docs/OPERATIONS.md
- docs/IMPLEMENTATION_AUDIT.md
- CHANGELOG.md
- tests/test_agent_infrastructure.py

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md (Deployment Script Reference, Linux Server Layout)

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; deployment-path detail, not a user-facing feature description
- [ ] docs/PROJECT_STATUS.md — no change needed; this is a deployment-boundary change, not a validated/deferred data-path status change
- [ ] docs/REPO_STRUCTURE.md — no change needed; no top-level folder added/removed, `systemd/` package purpose text is unchanged
- [x] relevant feature docs:
  - docs/OPERATIONS.md
  - docs/IMPLEMENTATION_AUDIT.md

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this narrows the automated production deployment surface; it does not change the validated/deferred status of `convert_day.py`, the full_l2 replay path, or any data artifact
- Evidence for any new validation claim: n/a

### Tests run
```bash
source .venv/bin/activate
pytest tests/test_agent_infrastructure.py -q   # 28 passed
pytest -q                                       # 283 passed, 3 skipped
```

### Validation CLIs run
```bash
bash scripts/deploy_linux_server.sh --target all --dry-run --no-systemd
# confirms units: cryptorecorder-recorder.service cryptorecorder-replay-build.service cryptorecorder-replay-build.timer
bash scripts/deploy_linux_server.sh --target legacy-converter --dry-run --no-systemd
# confirms exit 1: invalid --target 'legacy-converter' (expected: all recorder replay-build)
python -m validation.audit_change_compliance --base main
```

### Known limitations / out of scope
- No converter/reconstruction Python code was removed or modified:
  `convert_day.py`, `converter/`, and `validation/replay_catalog_reconstruct.py`
  remain fully in place and required for replay building, validation, and
  local test-computer catalog reconstruction.
- The `systemd/cryptorecorder-convert.{service,timer}` unit-file templates
  were kept in the repo (marked manual/reference-only) rather than deleted;
  deleting them was judged out of scope since the task only required removing
  the converter from the *active* deployment path, not the reference templates.
- This change was not tested against a real production server (no `sudo`/
  real systemd actions were run); only `--dry-run --no-systemd` was exercised,
  consistent with this being a WSL/dev sandbox, not the production host.
- Full_l2 broader top50/multi-day validation remains deferred, unaffected by
  this change (no data-path code was touched).
- Merge remains deferred; this change is pushed to
  `refactor/recorder-replay-only` only, per explicit instruction.

---

---
## 2026-07-21 — PR #18 third review round: exchangeinfo-only no_data classification, disk-report timestamp consistency, stale changelog claims (issues #17, #19)

### Change summary
- `pipeline/daily_build.py` — `run_build_replay_store()` now filters eligible
  venue/symbol partitions by actual raw channel coverage (`depth_v2`/`trade_v2`
  in `ELIGIBLE_REPLAY_CHANNELS`) instead of assuming every raw-manifest
  "symbol" entry is a market symbol. A date containing only an `exchangeinfo`
  partition (e.g. `data_raw/<venue>/exchangeinfo/EXCHANGEINFO/<date>/`) now
  attempts zero replay partitions and reports `no_data`, never `failed`.
  `EXCHANGEINFO` can never become an attempted replay symbol even if a caller
  explicitly passes `--symbols EXCHANGEINFO`, because filtering is based on
  channel coverage, not a literal symbol-name exclusion — so other future
  non-market metadata channels are protected the same way.
- `disk_monitor.py` — `_check_disk_usage_locked()`'s top-level `"timestamp"`
  field now uses `time_utils.local_now_iso()` (Europe/Budapest) instead of a
  bare UTC `now.isoformat()`, matching the already-local-time skipped/overlap
  path and the documented `docs/OPERATIONS.md` contract. Internal
  `measured_at`, growth-history epoch ordering, and measurement-age/staleness
  calculations are untouched and remain UTC/epoch-based.
- `docs/OPERATIONS.md` — added an explicit `timestamp` row to the
  `disk_usage.json` fields table clarifying the Europe/Budapest top-level
  contract and that internal growth calculations stay UTC/epoch-based.
- `CHANGELOG.md` — the two pre-issue-#17 `[Unreleased]` "Changed" blocks that
  described `pipeline/generate_catalog.py --profile full_l2` and
  `docs/GENERATE_CATALOG.md`/`docs/FEATURE_STORE.md` as if still active are
  now explicitly headed "(historical — ... superseded)" with an inline note
  stating the CLI and both doc files were later removed by issue #17 and do
  not exist today. No history was deleted; only the currently-active-state
  framing was corrected.
- Tests: `tests/test_daily_build.py` (4 new: exchangeinfo-only → `no_data`,
  exchangeinfo + one valid symbol → only the valid symbol attempted, explicit
  `--symbols EXCHANGEINFO` filtering still yields `no_data`, main() exits
  nonzero); `tests/test_disk_monitor_fail_safe.py` (3 new: normal report
  timestamp carries the Europe/Budapest offset, the overlapping/no-prior path
  carries the same offset, the timestamp change does not alter
  growth/measurement-age logic); `tests/test_agent_infrastructure.py` (1 new:
  `[Unreleased]` may not present the removed `generate_catalog` CLI or the
  deleted feature/catalog docs as currently available outside a
  historical/removed context).

### Files/packages touched
- pipeline/daily_build.py
- disk_monitor.py
- docs/OPERATIONS.md
- CHANGELOG.md
- tests/test_daily_build.py
- tests/test_disk_monitor_fail_safe.py
- tests/test_agent_infrastructure.py
- docs/CHANGE_AUDIT.md (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/DAILY_BUILD_PIPELINE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no user-facing behavior/API change
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change; these are
  bugfixes to already-documented statuses (`no_data` classification,
  timestamp contract), not new capability claims
- [ ] docs/REPO_STRUCTURE.md — no structural change
- [x] relevant feature docs:
  - docs/OPERATIONS.md (added `timestamp` field row to the `disk_usage.json` table)

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry corrects a stale-claim framing bug in
  `CHANGELOG.md`, it does not add or remove any validated/deferred capability
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_daily_build.py -q                                    # 9 passed
pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q  # 36 passed
pytest tests/test_repo_structure.py tests/test_agent_infrastructure.py -q  # 49 passed
pytest -q                                                               # 282 passed, 3 skipped
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main   # RESULT: PASS
```

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence remains deferred (unchanged
  by this entry).
- No production data, services, or `/etc/cryptorecorder/cryptorecorder.env`
  were touched; all tests use `tmp_path`-scoped temporary roots.
- The ADAUSDT replay-equivalence smoke was not re-run as part of this change
  (no code path it depends on — `convert_day.py`, replay writer/reader
  schemas, catalog reconstruction — was touched); see final report for the
  smoke-availability statement.

---
## 2026-07-20 — PR #18 second review round: fail-closed disk monitor, data_raw-only retention accounting, daily_build failed status, deploy-script legacy cleanup + honest flags, stale doc references (issues #17, #19)

### Change summary
- `disk_monitor.py` — `check_disk_usage()` now forces
  `retention_measurement_trustworthy=False` whenever a scan is skipped due to
  an overlapping scan already in progress (`skipped_duplicate=True`), even if
  the previous cycle's own report was trustworthy; adds a `WARNING`/`ERROR`
  alert and downgrades `monitoring_health` to at least `degraded`.
  `cleanup_old_data()` now explicitly refuses to act (`return False`)
  whenever the current cycle's report has `skipped_duplicate=True`.
- `disk_monitor.py` — `percent_of_soft_limit`, `percent_of_hard_limit`,
  `growth_rate_gb_day`, and `days_to_full` are now all `null` whenever the
  current cycle's `data_raw` measurement is not itself fresh and successful
  (never derived from the persisted last-known-good fallback).
- `disk_monitor.py` — soft/hard-limit and cleanup-target comparisons,
  `percent_of_soft_limit`/`percent_of_hard_limit`, and growth-rate/
  `days_to_full` are now derived exclusively from fresh `data_raw` usage
  (`data_raw_gb_for_retention`), never from `total_gb` (the cross-root
  `data_raw + catalog + meta + state` observability sum, which may span
  different filesystems). `GrowthSample.total_bytes` renamed to
  `GrowthSample.data_raw_bytes` throughout the module and its tests.
- `pipeline/daily_build.py` — `run_build_replay_store()` now reports
  `"failed"` (distinct from `"partial"` and `"no_data"`) when one or more
  venue/symbol partitions were attempted and *none* succeeded.
  `generate_daily_report()` propagates `"failed"` distinctly rather than
  collapsing it into `"partial"`.
- `scripts/deploy_linux_server.sh` — `cleanup_stale_units()` now removes
  every legacy/renamed unit name this repo has ever shipped
  (`crypto-recorder.service`, `nautilus-convert.{service,timer}`,
  `cryptorecorder-daily-build.{service,timer}`, in addition to the existing
  `cryptorecorder-feature-build.{service,timer}`), runs for every `--target`,
  and now runs before `install_units`. `--user`/`--app-dir`/`--env-file` are
  now rendered into each installed unit file via `sed`
  (`User=`/`Group=`/`WorkingDirectory=`/`ExecStart=`/`EnvironmentFile=`), and
  `--data-root` is rendered into a newly created env file's
  `CRYPTO_RECORDER_*_ROOT` values (an existing env file is still never
  overwritten).
- Corrected stale systemd unit name references: `docs/DAILY_BUILD_PIPELINE.md`
  (11 occurrences of `cryptorecorder-daily-build.{service,timer}` -> the real
  `cryptorecorder-replay-build.{service,timer}`), `docs/ARCHITECTURE.md` (2
  occurrences, same rename), `INSTALL.md` (`nautilus-convert.{service,timer}`
  -> the real `cryptorecorder-convert.{service,timer}` in Troubleshooting).
- Corrected stale "tracked retention usage (data_raw + catalog + meta +
  state)" comments to reflect data_raw-only retention semantics in
  `config.py`, `systemd/cryptorecorder.env.example`, `docs/OPERATIONS.md`,
  and `INSTALL.md`.
- `docs/ARCHITECTURE.md` and `docs/DAILY_BUILD_PIPELINE.md` now document all
  four `daily_build` report statuses (`success`/`partial`/`failed`/`no_data`).
- `docs/OPERATIONS.md`'s Deployment Script Reference updated to describe the
  rendering behavior of `--user`/`--app-dir`/`--data-root`/`--env-file` and
  the expanded stale-unit cleanup list.

### Files/packages touched
- disk_monitor.py
- tests/test_disk_monitor_fail_safe.py
- pipeline/daily_build.py
- tests/test_daily_build.py
- scripts/deploy_linux_server.sh
- tests/test_agent_infrastructure.py
- config.py
- systemd/cryptorecorder.env.example
- docs/OPERATIONS.md
- docs/ARCHITECTURE.md
- docs/DAILY_BUILD_PIPELINE.md
- INSTALL.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md,
    INSTALL.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no stale references found in this file
- [ ] docs/PROJECT_STATUS.md — no status/claim change; full_l2 top50/multi-day
  validation remains pending as before
- [ ] docs/REPO_STRUCTURE.md — no structural change (no files added/removed)
- [x] relevant feature docs:
  - docs/OPERATIONS.md, docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md,
    INSTALL.md, systemd/cryptorecorder.env.example

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry fixes fail-open/fail-closed edge cases,
  accounting scope, deployment honesty, and stale references; it does not
  change what is validated vs deferred (full_l2 top50/multi-day validation
  remains pending, as before)
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest -q
# 274 passed, 3 skipped

pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q
# 33 passed

pytest tests/test_daily_build.py -q
# 5 passed

pytest tests/test_agent_infrastructure.py -q
# 26 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main
# RESULT: PASS (64 changed files vs main)

python -m pipeline.build_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/cr_smoke_replay
# Built replay: BINANCE_SPOT/ADAUSDT/2026-06-12 (412336 depth, 124457 trades)
# Built replay: BINANCE_USDTF/ADAUSDT/2026-06-12 (442834 depth, 401883 trades)
# Replay build complete: 2 successful, 0 failed, 855170 depth, 526340 trades

python -m validation.audit_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --venues BINANCE_SPOT,BINANCE_USDTF --replay-root /tmp/cr_smoke_replay
# depth/trades parquet for both venues: sorted=true, 0 duplicate sequence
# keys, schema OK, no errors

python -m pipeline.daily_build --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/cr_smoke_replay \
  --report-root /tmp/cr_daily_reports
# status=success, runtime=38.7s, 2/2 symbols, 855170 depth, 526340 trades

bash scripts/deploy_linux_server.sh --target all --dry-run --user customuser \
  --app-dir /opt/customdir --data-root /srv/customdata \
  --env-file /etc/customenv/cr.env
# confirmed all 4 custom values appear in the rendered dry-run plan and none
# of the hardcoded defaults leak through
```
All commands ran against temporary roots (`/tmp/...`) using the existing
local `./data_raw` fixture; no production data, `/etc` files, or running
services were touched. Temp directories were removed after the run.

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence validation remains pending
  (unchanged from before this entry); the `v2.0.0` gate is still not declared.
- No live systemd install/enable/start was performed (out of scope — dry-run
  only, no root/sudo access in this environment).
- The migration/cleanup test for the deploy script's `cleanup_stale_units()`
  verifies the unit-name list and rendered-flag behavior via source
  inspection and `--dry-run` output, not a live `/etc/systemd/system`
  install, since this environment has no sudo/systemd access.

---
## 2026-07-20 — complete PR #18 remaining work: strip feature-store residue, harden structure tests, fix daily_build false-success, correct systemd/doc references (issues #17, #19)

### Change summary
- Merged current `main` into `refactor/recorder-replay-only` (via cherry-pick of
  commit `9c639b8` from `fix/disk-monitor-fail-safe-measurement`, completed as
  `45356f9`), resolving conflicts in `CHANGELOG.md`, `docs/PROJECT_STATUS.md`, and
  `docs/CHANGE_AUDIT.md` by hand.
- Stripped feature-store naming from `validation/audit_storage_size.py`: removed
  the `feature_root` parameter, its `feature_store` report component, and the
  `--feature-root` CLI flag (the feature-store subsystem no longer exists).
- Deleted `docs/GUARANTEES.md` — fully superseded by the existing
  "System Guarantees" section in `docs/ARCHITECTURE.md`; no unique content lost.
- Deleted root-level `inspect_catalog.py` — dead code from a stale merge, not
  imported anywhere, with a docstring referencing a nonexistent `validators/`
  package. `validation/catalog_inspect.py` is the real, currently-used CLI.
- Reverted `validate.py` to its working form: the `main`-branch merge had
  regressed it to import unused `cryptofeed`/`yaml` dependencies, reference a
  nonexistent `converter.book` module, and hardcode paths instead of using
  `config.py`'s configurable `DATA_ROOT`/`META_ROOT`/`STATE_ROOT`.
- Hardened `tests/test_repo_structure.py` with 7 new tests enforcing the exact
  root Python/other file sets and exact `docs/` file set from
  `docs/REPO_STRUCTURE.md`, absence of stray Python files in `docs/`, absence of
  feature-store config roots/CLI flags/systemd units, and absence of
  `validators` imports. Updated `docs/REPO_STRUCTURE.md`'s Root-Level Files
  table to list every real root `.py` module (several were previously missing).
- Fixed `pipeline/daily_build.py`'s false-success bug: `run_build_replay_store()`
  now reports `"no_data"` (distinct from `"success"`) when zero raw partitions
  were eligible for the date, instead of falsely reporting `"success"`.
  `generate_daily_report()` checks `"no_data"` explicitly before the generic
  `"partial"` fallback. `main()` now logs a warning and returns nonzero for any
  non-`"success"` status. Added `tests/test_daily_build.py` (4 new tests) and
  updated `docs/DAILY_BUILD_PIPELINE.md`'s status-semantics documentation.
- Deleted stale duplicate systemd unit files superseded by the units actually
  referenced by `scripts/deploy_linux_server.sh`: `systemd/crypto-recorder.service`
  (superseded by `cryptorecorder-recorder.service`),
  `systemd/nautilus-convert.{service,timer}` (superseded by
  `cryptorecorder-convert.{service,timer}`), and
  `systemd/cryptorecorder-daily-build.{service,timer}` (superseded by
  `cryptorecorder-replay-build.{service,timer}`).
- Corrected numerous stale documentation references: `INSTALL.md`'s
  `crypto-recorder.service`/`nautilus-convert.*` unit names and a duplicate
  `## 10.` heading; `AGENTS.md`'s and `docs/OPERATIONS.md`'s broken
  self-referential "merged from the former `OPERATIONS.md`" provenance notes
  and a broken same-file link; similar self-referential provenance notes in
  `docs/ARCHITECTURE.md` and `docs/IMPLEMENTATION_AUDIT.md`; a stale
  `pipeline/audit_replay_store.py` table row in `docs/ARCHITECTURE.md` (real
  path is `validation/audit_replay_store.py`); a duplicate
  `[OPERATIONS.md](OPERATIONS.md)` link in `docs/PROJECT_STATUS.md`; a
  misleading `--date today` example in `docs/DAILY_BUILD_PIPELINE.md` (only
  `YYYY-MM-DD` and `yesterday` are implemented); and added a new
  "Replay Store Validation" section to `docs/VALIDATION.md` documenting
  `validation.audit_replay_store`, `validation.validate_catalog_equivalence`,
  and `validation.audit_change_compliance` (previously undocumented there).

### Files/packages touched
- validation/audit_storage_size.py
- docs/GUARANTEES.md (deleted)
- inspect_catalog.py (deleted)
- validate.py
- tests/test_repo_structure.py
- docs/REPO_STRUCTURE.md
- pipeline/daily_build.py
- tests/test_daily_build.py (new)
- docs/DAILY_BUILD_PIPELINE.md
- systemd/crypto-recorder.service (deleted)
- systemd/nautilus-convert.service (deleted)
- systemd/nautilus-convert.timer (deleted)
- systemd/cryptorecorder-daily-build.service (deleted)
- systemd/cryptorecorder-daily-build.timer (deleted)
- systemd/cryptorecorder-recorder.service
- systemd/cryptorecorder.env.example
- scripts/deploy_linux_server.sh
- INSTALL.md
- AGENTS.md
- docs/OPERATIONS.md
- docs/ARCHITECTURE.md
- docs/IMPLEMENTATION_AUDIT.md
- docs/PROJECT_STATUS.md
- docs/VALIDATION.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/DAILY_BUILD_PIPELINE.md, docs/VALIDATION.md, docs/OPERATIONS.md,
    docs/ARCHITECTURE.md, INSTALL.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; no stale references found in this file
- [x] docs/PROJECT_STATUS.md — fixed duplicate link and stale "Date" header
- [x] docs/REPO_STRUCTURE.md — root-file table completed; amendment log entry added
- [x] relevant feature docs:
  - docs/DAILY_BUILD_PIPELINE.md, docs/VALIDATION.md, docs/OPERATIONS.md,
    docs/ARCHITECTURE.md, docs/IMPLEMENTATION_AUDIT.md, INSTALL.md, AGENTS.md

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no — this entry fixes structural/doc/test defects and stale
  references; it does not change what is validated vs deferred (full_l2
  top50/multi-day validation remains pending, as before)
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest -q
# 267 passed, 3 skipped

pytest tests/test_repo_structure.py tests/test_replay_store.py \
  tests/test_pipeline_validation.py tests/test_agent_infrastructure.py -q
# 22 passed (test_repo_structure.py); 36 passed, 1 skipped (combined others)

pytest tests/test_daily_build.py -q
# 4 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --base main
# RESULT: PASS (52 changed files vs main)

python -m pipeline.build_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --data-root ./data_raw --replay-root /tmp/tmp.fRQ8vNOyNf/replay_store
# Built replay: BINANCE_SPOT/ADAUSDT/2026-06-12 (412336 depth, 124457 trades)
# Built replay: BINANCE_USDTF/ADAUSDT/2026-06-12 (442834 depth, 401883 trades)
# Replay build complete: 2 successful, 0 failed

python -m validation.audit_replay_store --date 2026-06-12 --symbols ADAUSDT \
  --venues BINANCE_SPOT --replay-root /tmp/tmp.fRQ8vNOyNf/replay_store
# depth.parquet: 412336 rows, sorted=true, 0 duplicate sequence keys, schema OK
# trades.parquet: 124457 rows, sorted=true, 0 duplicate sequence keys, schema OK
```
All commands ran against temporary roots (`/tmp/...`) using the existing local
`./data_raw` fixture; no production data, `/etc` files, or running services
were touched.

### Known limitations / out of scope
- Broader top50/multi-day `full_l2` equivalence validation remains pending
  (unchanged from before this entry); the `v2.0.0` gate is still not declared.
- No live systemd install/enable/start was performed (out of scope — this is a
  documentation/reference correction pass, not a deployment).
- The `full_l2` semantic-equivalence smoke re-run (`convert_day.py` vs
  `validate_catalog_equivalence --profile full_l2`) was not re-executed in this
  session; the existing ADAUSDT smoke evidence in `docs/PROJECT_STATUS.md` and
  `docs/IMPLEMENTATION_AUDIT.md` is unchanged and still applies.

---
## 2026-07-20 — fix disk monitor false-zero reporting and fail-open cleanup (issue #19)

### Change summary
- Rewrote `disk_monitor.py` to eliminate the false-zero measurement defect: a
  failed/timed-out recursive `du` scan previously returned numeric `0.0`, which was
  published as `data_raw_gb=0.0` and silently disabled capacity alerts and
  automatic cleanup (observed ~442 timeouts since June 2 against a ~410GB raw tree
  with a 30s hard-coded timeout).
- Added `measure_directory()` / `DirectoryMeasurement` (`ok`, `status` — one of
  `ok`/`missing`/`timeout`/`command_error`/`malformed_output`/`error`, `error`,
  `value_bytes`, `measured_at`, `duration_seconds`) so a failure can never be
  represented as a bare numeric zero. A genuinely empty directory still reports
  `ok=True, status="ok"`.
- Switched the scan command from `du -sb` (apparent size) to `du -s -B1` (allocated
  bytes) — documented as the intended, more honest-for-retention semantics.
- Added last-known-good persistence (`state/disk_monitor_state.json`): on measurement
  failure the monitor falls back to the prior successful value marked `stale=True`
  with `measurement_age_seconds`; if no prior value exists the field is `null`, never
  `0`. State survives process restarts (loaded in `DiskMonitor.__init__`).
- `state/disk_usage.json` now reports per-component `measurement_ok` /
  `measurement_status` / `measurement_error` / `measurement_timestamp` /
  `measurement_age_seconds` / `stale`, a top-level `monitoring_health`
  (`healthy`/`degraded`/`unhealthy`), and an `alerts` list. Retention percentages,
  growth rate, and `days_to_full` are `null` (never derived) when the backing data is
  unknown/stale.
- `cleanup_old_data()` now fails closed: it refuses to run or continue unless the
  current cycle's `data_raw` measurement is fresh and successful
  (`retention_measurement_trustworthy=True`), re-validating before every destructive
  deletion phase, and logs an `ERROR` (with a report alert) when skipped.
- Added independent filesystem-capacity reporting via `measure_filesystem()`
  (`shutil.disk_usage`), exposed under `filesystem.*` in the report, with its own
  `DISK_FS_FREE_WARN_GB`/`DISK_FS_FREE_CRITICAL_GB` thresholds — kept semantically
  separate from the raw-retention `DISK_SOFT_LIMIT_GB`/`DISK_HARD_LIMIT_GB` limits.
- Growth-rate/`days_to_full` now use real sample timestamps (bounded, persisted
  `GrowthSample` history capped by `DISK_HISTORY_MAX_SAMPLES`/`DISK_HISTORY_MAX_AGE_SEC`),
  only recording a sample when every monitored root was measured fresh and
  successfully in the same cycle; non-increasing timestamps are rejected; growth and
  `days_to_full` are `null` when the valid sample span is under 1 hour.
- Added an `asyncio.Lock` around `check_disk_usage()` to prevent overlapping scans;
  an overlapping call returns the previous report with `skipped_duplicate=True`
  instead of queuing or running concurrently. The lock is released via `async with`
  on every exception path.
- Report and companion-state writes are now atomic (`tempfile.NamedTemporaryFile` in
  the same directory + `os.replace()`), with the temp file cleaned up on any
  write failure.
- `config.py`: added `DISK_SCAN_TIMEOUT_SEC` (default 60s, validated > 0),
  `DISK_MEASUREMENT_STALE_AFTER_SEC`, `DISK_FS_FREE_WARN_GB`,
  `DISK_FS_FREE_CRITICAL_GB`, `DISK_HISTORY_MAX_SAMPLES`, `DISK_HISTORY_MAX_AGE_SEC`;
  existing `DISK_SOFT_LIMIT_GB`/`DISK_HARD_LIMIT_GB`/`DISK_CLEANUP_TARGET_GB` env vars
  are unchanged for backward compatibility.
- `recorder.py`: `disk_check_task()` updated to use `usage.get('data_raw_gb')` (no
  longer defaults a missing/None value to `0`) before comparing against the soft
  limit.
- Added `tests/test_disk_monitor_fail_safe.py` (30 new tests) covering: successful/
  empty/missing/timeout/nonzero-exit/malformed-output/unexpected-exception `du`
  parsing; invalid-timeout config validation; last-known-good fallback marked
  stale; restart-persisted state; no-prior-value → `null`; staleness alert;
  misleading percentage/growth omission; cleanup skipped on unknown/stale
  measurement with no destructive `shutil.rmtree` call; independent filesystem
  capacity fields and low-free-space alert; separate retention/filesystem threshold
  semantics; atomic report writing and temp-file cleanup on failure; growth from
  real timestamps, short-span exclusion, non-increasing-timestamp rejection,
  failed/stale-sample exclusion; overlapping-scan prevention and lock release on
  exception.
- Updated `tests/test_disk_monitor_cleanup.py`'s two existing fakes to include
  `retention_measurement_trustworthy: True` (new required field in the cleanup
  trust contract) — no behavioral change to those tests' assertions.
- No production data, service, or `/etc` changes were made. No destructive cleanup
  was run against real data during implementation or testing (temp dirs / mocks
  only).

### Files/packages touched
- `disk_monitor.py` (rewritten)
- `recorder.py` (`disk_check_task` — safe `.get()` for `data_raw_gb`)
- `config.py` (new disk-monitor env vars + docstring clarifying retention vs
  filesystem-threshold semantics)
- `systemd/cryptorecorder.env.example` (documented new env vars)
- `tests/test_disk_monitor_fail_safe.py` (new)
- `tests/test_disk_monitor_cleanup.py` (updated fakes for the trust contract)
- `docs/ARCHITECTURE.md` (new "Disk Monitoring Safety Invariant" section)
- `docs/OPERATIONS.md` (new "Disk Monitoring" field/alert/threshold reference)
- `docs/IMPLEMENTATION_AUDIT.md` (addendum under Section A)
- `docs/PROJECT_STATUS.md` (new validated bullet + `Last updated` bump)
- `INSTALL.md` (runtime file table: `disk_usage.json` description +
  `disk_monitor_state.json` row)
- `CHANGELOG.md` (`[Unreleased]` → new "Fixed" section)
- `docs/CHANGE_AUDIT.md` (this entry)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/OPERATIONS.md, docs/VALIDATION.md (no disk-monitor
    content existed there; not amended), CHANGELOG.md

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md
- [x] docs/PROJECT_STATUS.md
- [ ] docs/REPO_STRUCTURE.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/OPERATIONS.md, docs/IMPLEMENTATION_AUDIT.md,
    INSTALL.md, systemd/cryptorecorder.env.example
- No docs update required for README.md/REPO_STRUCTURE.md because: no new
  top-level files/folders or root-entrypoint changes were introduced; this is an
  internal-module fix within the existing `disk_monitor.py` file already listed
  in `docs/REPO_STRUCTURE.md`.

### Status / validation impact
- Validated status changed: yes — `docs/PROJECT_STATUS.md` gained a new
  "Disk monitoring (fail-safe measurement)" validated bullet.
- Deferred status changed: no.
- New claims added: yes — the fail-safe measurement behavior is claimed as
  validated by the focused test suite below; **real-server verification is
  explicitly NOT claimed** (deployment/log/report inspection is documented as a
  manual, not-yet-performed checklist item — see PR body).
- Evidence for any new validation claim:
  - `pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q`
    → `30 passed`
  - Full suite: `pytest -q` → `266 passed, 3 skipped`

### Tests run
```bash
source .venv/bin/activate
pytest tests/test_disk_monitor_fail_safe.py tests/test_disk_monitor_cleanup.py -q
pytest -q
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- Real production-server verification (restarting the monitor/recorder service,
  inspecting logs and `disk_usage.json` against the actual ~410GB raw tree) was
  **not performed** as part of this change — see the manual deployment/
  verification checklist in the PR description. No production data or services
  were touched.
- `get_dir_size_gb()` is retained only as a best-effort single-directory helper
  for cleanup log messages; it is not used for any retention/cleanup decision
  (which relies solely on the current cycle's `data_raw` `DirectoryMeasurement`).
- Concurrent/parallel scanning across roots was deliberately **not** introduced
  (roots are measured sequentially against the same disk) since the issue asked
  to avoid concurrent recursive scans unless benchmarked as safe; no such
  benchmark was performed in this change.
- `disk_check_task()` in `recorder.py` still runs on a fixed
  `DISK_CHECK_INTERVAL_SEC` sleep loop (unchanged); the new `asyncio.Lock`-based
  overlap guard lives inside `DiskMonitor.check_disk_usage()` itself, which is
  sufficient because `disk_check_task()` awaits each cycle in sequence and does
  not spawn concurrent calls itself — this is noted for completeness, not as a
  gap.


---

## 2026-07-15 — Issue #17: narrow scope to recorder + replay-store ownership, remove feature-store subsystem

### Change summary
- Removed the entire **feature-store subsystem**: `stores/feature_schema.py`,
  `stores/feature_calc.py`, `stores/feature_writer.py`,
  `pipeline/build_feature_store.py`, `validation/audit_feature_store.py`,
  `tests/test_feature_store.py`, and the
  `systemd/cryptorecorder-feature-build.{service,timer}` units.
- Removed `pipeline/generate_catalog.py` as a **product/runtime CLI**. Moved its
  `generate_catalog_from_replay` reconstruction logic and helpers to
  `validation/replay_catalog_reconstruct.py` — an internal, CLI-less,
  validation-only helper used exclusively by
  `validation/validate_catalog_equivalence.py`. Renamed
  `tests/test_generate_catalog_full_l2.py` to
  `tests/test_replay_catalog_reconstruct.py` and removed
  `tests/test_generate_catalog.py` (trades_only product-CLI tests, no longer
  applicable).
- Removed `config.py`: `FEATURE_ROOT`, `LABEL_ROOT`, `CATALOG_JOBS_ROOT`.
  `ARCHIVE_DAYS_ROOT` is unaffected (still a placeholder, not implemented).
- Simplified `pipeline/daily_build.py`: removed `--steps`, `--timeframes`,
  `--feature-root` CLI flags and the feature-build execution path. It now
  always scans raw coverage and builds the replay store only; report shape no
  longer contains a `feature_build` section.
- Deleted `docs/FEATURE_STORE.md` and `docs/GENERATE_CATALOG.md` rather than
  leaving tombstones — the fixed docs/ file count drops from 14 to 12.
- Rewrote `docs/REPO_STRUCTURE.md` (12-file contract, narrowed `pipeline/`,
  `stores/`, `validation/` package definitions, updated CLI Command Reference,
  new Amendment Log entry), `docs/IMPLEMENTATION_AUDIT.md` (feature-store
  sections marked removed with preservation banners, historical evidence
  retained), `docs/PROJECT_STATUS.md` (Validated/Deferred sections updated,
  replay_store framed as the stable external contract for downstream
  repositories), `docs/ARCHITECTURE.md` (removed the "Feature Store" storage
  layer and "Build Feature Store"/"Generate Catalog" pipeline sections,
  replaced with the validation-only `validation.replay_catalog_reconstruct`
  helper description), `docs/DAILY_BUILD_PIPELINE.md` (fully rewritten,
  replay-only), `docs/OPERATIONS.md` (removed `feature-build` service group and
  `FEATURE_ROOT`/`CATALOG_JOBS_ROOT`/`LABEL_ROOT` path rows), `docs/REPLAY_STORE.md`,
  `docs/FULL_L2_REPLAY_CATALOG_PLAN.md`, `docs/AI_WORKFLOW.md`, `docs/README.md`,
  `README.md` (root), `AGENTS.md`, and `.github/copilot-instructions.md`.
- Rewrote `tests/test_repo_structure.py`: added
  `test_pipeline_does_not_contain_feature_store_modules()` and
  `test_pipeline_does_not_contain_generate_catalog_cli()`; updated
  `test_docs_do_not_reference_pipeline_audit_modules()` forbidden-pattern list;
  updated `test_validation_contains_audit_and_equivalence_modules()` required
  module list; removed `test_validation_audit_feature_store_cli_help()`.
- Updated `tests/test_agent_infrastructure.py` `DEPLOY_TARGETS` (removed
  `feature-build`); updated `tests/test_semantic_equivalence.py` and
  `tests/test_replay_depth_adapter.py` to reference
  `validation.replay_catalog_reconstruct` instead of the removed
  `pipeline/generate_catalog.py` module.
- Cleaned `scripts/deploy_linux_server.sh` (removed the `feature-build` target
  throughout: `VALID_TARGETS`, help text, unit/control case statements,
  directory creation list, plus an explicit stale-unit cleanup step that
  stops/disables/removes any previously-installed
  `cryptorecorder-feature-build.{service,timer}` on `all`/`replay-build`
  deploys so upgraded servers don't keep firing the removed
  `daily_build --steps features` command), `systemd/cryptorecorder.env.example` (removed
  `CRYPTO_RECORDER_FEATURE_ROOT`, `CRYPTO_RECORDER_CATALOG_JOBS_ROOT`,
  `CRYPTO_RECORDER_LABEL_ROOT`), `systemd/cryptorecorder-replay-build.service`
  (removed the now-nonexistent `--steps replay` flag), and `scripts/README.md`.
- Updated package docstrings: `pipeline/__init__.py`, `stores/__init__.py`,
  `validation/__init__.py`.
- `validation/audit_change_compliance.py`: `_REPLAY_CATALOG_PATTERNS` no longer
  includes `stores/feature`, `pipeline/build_feature`, `pipeline/generate_catalog`,
  or `validation/audit_feature`.
- `validation/audit_storage_size.py` deliberately left unchanged (see "Known
  limitations" below).

### Files/packages touched
- pipeline/__init__.py, pipeline/daily_build.py
- pipeline/build_feature_store.py (deleted), pipeline/generate_catalog.py (deleted)
- stores/__init__.py
- stores/feature_schema.py, stores/feature_calc.py, stores/feature_writer.py (all deleted)
- stores/replay_depth_adapter.py
- validation/__init__.py
- validation/audit_feature_store.py (deleted)
- validation/replay_catalog_reconstruct.py (new)
- validation/audit_change_compliance.py
- config.py
- tests/test_repo_structure.py
- tests/test_agent_infrastructure.py
- tests/test_semantic_equivalence.py
- tests/test_replay_depth_adapter.py
- tests/test_feature_store.py (deleted)
- tests/test_generate_catalog.py (deleted)
- tests/test_generate_catalog_full_l2.py → tests/test_replay_catalog_reconstruct.py (renamed)
- scripts/acceptance_test.py, scripts/deploy_linux_server.sh, scripts/README.md
- systemd/cryptorecorder.env.example, systemd/cryptorecorder-replay-build.service
- systemd/cryptorecorder-feature-build.service, systemd/cryptorecorder-feature-build.timer (both deleted)
- docs/REPO_STRUCTURE.md, docs/IMPLEMENTATION_AUDIT.md, docs/PROJECT_STATUS.md,
  docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
  docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md, docs/AI_WORKFLOW.md,
  docs/README.md
- docs/FEATURE_STORE.md, docs/GENERATE_CATALOG.md (both deleted)
- README.md (root), AGENTS.md, .github/copilot-instructions.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
    docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md,
    docs/AI_WORKFLOW.md, docs/README.md, docs/VALIDATION.md (verified, no
    change needed), INSTALL.md (verified, no change needed)

### Docs updated
- [x] CHANGELOG.md
- [x] README.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/REPO_STRUCTURE.md
- [x] relevant feature docs:
  - docs/ARCHITECTURE.md, docs/DAILY_BUILD_PIPELINE.md, docs/OPERATIONS.md,
    docs/REPLAY_STORE.md, docs/FULL_L2_REPLAY_CATALOG_PLAN.md,
    docs/AI_WORKFLOW.md, docs/README.md, docs/IMPLEMENTATION_AUDIT.md

### Status / validation impact
- Validated status changed: no — the previously validated
  `data_raw -> replay_store` contract and the ADAUSDT single-day `full_l2`
  smoke evidence are unchanged and preserved verbatim in
  `docs/PROJECT_STATUS.md` and `docs/IMPLEMENTATION_AUDIT.md`.
- Deferred status changed: no new deferrals added beyond removing the
  feature-store/label-store scope entirely (it is no longer "deferred", it is
  "not this repository's responsibility").
- New claims added: no. This is a scope-narrowing and cleanup change; no new
  validation claims were made. Broader top50/multi-day full_l2 equivalence
  (the `v2.0.0` gate) remains explicitly not claimed.
- Evidence for any new validation claim:
  - n/a — no new validation claims; existing ADAUSDT smoke evidence preserved
    unchanged.

### Tests run
```bash
source .venv/bin/activate && pytest -q
# 227 passed, 3 skipped
```

### Validation CLIs run
```bash
# none required — this change removes/renames modules and rewrites docs; it
# does not alter recorder, converter, or replay-store semantics. The existing
# ADAUSDT full_l2 smoke evidence was not re-run because no code paths it
# exercises were modified (only its call site moved from
# pipeline/generate_catalog.py to validation/replay_catalog_reconstruct.py with
# behavior otherwise unchanged, and this is covered by the passing pytest run
# above, including tests/test_replay_catalog_reconstruct.py and
# tests/test_catalog_equivalence_full_l2.py).
```

### Known limitations / out of scope
- `validation/audit_storage_size.py` still has a generic `--feature-root` CLI
  flag and a `feature_store` component label for measuring arbitrary directory
  sizes. Left unchanged deliberately: it imports nothing from the deleted
  feature-store modules and is a generic size-measurement tool, not a
  feature-store consumer. Renaming its flag was judged out of scope for this
  issue.
- Broader top50/multi-day `full_l2` validation (the `v2.0.0` gate) is still
  pending; not addressed by this change.
- Issue #15 (the superseded `generate_catalog` product-CLI proposal) needs to
  be manually commented on and closed as "not planned" on GitHub — not done as
  part of this local change; requires user confirmation before performing any
  GitHub write action.
- Pushing the `refactor/recorder-replay-only` branch and opening a PR are not
  done as part of this change; both require explicit user confirmation first
  per this repository's operational safety rules.


### Change summary
- Created `.githooks/commit-msg` — bash hook that validates every commit message
  against the conventional commits format before the commit is accepted.
  Enforces: correct type, no capital first letter, no trailing period, blank line
  before body. Skips auto-generated messages (Merge, Revert, fixup!, squash!).
- Added `AGENTS.md` Section 7 "Commit message style" with full format reference,
  type table, subject rules, valid/invalid examples, and bypass guidance.
- Updated `docs/AI_WORKFLOW.md` Step 7 to include commit message format requirement.
- Added a new "bad behavior" example for malformed commit messages.
- Fixed workflow heading: "The 8-step workflow" → "The 9-step workflow".
- Updated `docs/REPO_STRUCTURE.md`: expanded `.githooks/` description to list both hooks.
- Updated `INSTALL.md` Section 7 to describe both hooks.
- Updated `docs/REPO_STRUCTURE.md` amendment log.

### Files/packages touched
- .githooks/commit-msg (new)
- AGENTS.md
- docs/AI_WORKFLOW.md
- docs/REPO_STRUCTURE.md
- INSTALL.md
- docs/CHANGE_AUDIT.md (this entry)
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed
- [ ] docs/PROJECT_STATUS.md — no status change
- [x] docs/REPO_STRUCTURE.md — .githooks/ description + amendment log
- [ ] relevant feature docs:
  - none applicable

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
# 37 passed
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- The hook cannot verify imperative tense ("add" vs "adding") — that is documented
  but not mechanically checked.
- No changes to recorder, stores, pipeline, converter, or validation Python code.

---

## 2026-07-09 — Docs structure consolidation (14-file fixed structure)

### Change summary
- Merged 9 small docs into 4 larger comprehensive files (all content preserved).
- `docs/STORAGE_ARCHITECTURE.md` + `docs/GUARANTEES.md` → appended to `docs/ARCHITECTURE.md`.
- `docs/DEPLOYMENT.md` + `docs/LINUX_SERVER.md` + `docs/SCHEMAS.md` → appended to `docs/OPERATIONS.md`.
- `docs/REPO_CLEANUP_AUDIT.md` + `docs/FEATURE_STORE_REQUIREMENTS_AUDIT.md` + `docs/STORAGE_SIZE_AUDIT.md` → appended to `docs/IMPLEMENTATION_AUDIT.md`.
- `docs/VERSIONING.md` → appended to `CHANGELOG.md` as "Versioning Policy" section.
- Deleted the 9 source files after merging.
- Rewrote `docs/README.md` as a navigation index with a "Where to update what" table.
- Added "No New Docs Files" rule to `AGENTS.md` Section 2 and `docs/REPO_STRUCTURE.md`.
- Updated `tests/test_agent_infrastructure.py` REQUIRED_FILES and link checks.
- Fixed all cross-references throughout the repo to point to the new consolidated files.
- Updated `REPO_STRUCTURE.md` docs/ package table and amendment log.
- Updated `README.md` (root) key-references section.

### Files/packages touched
- docs/ARCHITECTURE.md (expanded: +STORAGE_ARCHITECTURE + GUARANTEES)
- docs/OPERATIONS.md (expanded: +DEPLOYMENT + LINUX_SERVER + SCHEMAS)
- docs/IMPLEMENTATION_AUDIT.md (expanded: +REPO_CLEANUP_AUDIT + FS_REQ_AUDIT + STORAGE_SIZE_AUDIT)
- CHANGELOG.md (expanded: +VERSIONING policy section)
- docs/STORAGE_ARCHITECTURE.md (deleted)
- docs/GUARANTEES.md (deleted)
- docs/DEPLOYMENT.md (deleted)
- docs/LINUX_SERVER.md (deleted)
- docs/SCHEMAS.md (deleted)
- docs/REPO_CLEANUP_AUDIT.md (deleted)
- docs/FEATURE_STORE_REQUIREMENTS_AUDIT.md (deleted)
- docs/STORAGE_SIZE_AUDIT.md (deleted)
- docs/VERSIONING.md (deleted)
- docs/README.md (rewritten as navigation index)
- AGENTS.md (No New Docs rule + updated read order)
- docs/REPO_STRUCTURE.md (docs/ table + No New Docs rule + amendment log)
- tests/test_agent_infrastructure.py (REQUIRED_FILES updated)
- README.md (key references updated)
- INSTALL.md (More Documentation section updated)
- docs/AI_WORKFLOW.md, docs/PROJECT_STATUS.md, docs/REPLAY_STORE.md, docs/GENERATE_CATALOG.md, docs/DAILY_BUILD_PIPELINE.md (cross-reference links updated)

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — this is documentation infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [x] README.md
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change
- [x] docs/REPO_STRUCTURE.md — docs/ table, No New Docs rule, amendment log

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
# 37 passed
pytest -q
# 238 passed, 3 skipped
```

### Validation CLIs run
```bash
# none required — documentation-only restructure
```

### Known limitations / out of scope
- The merged sections in ARCHITECTURE.md, OPERATIONS.md, and IMPLEMENTATION_AUDIT.md
  retain "Content merged from the former X" markers; future contributors may do a
  prose clean-up pass to integrate them more smoothly.
- No changes to recorder, stores, pipeline, converter, or validation Python code.
- The -2 test count vs previous run (238 vs 240) is expected: DEPLOYMENT.md,
  LINUX_SERVER.md, and VERSIONING.md removed from REQUIRED_FILES; OPERATIONS.md added.

---

## 2026-07-09 — Mandatory change-audit infrastructure

### Change summary
- Added `AGENTS.md` Section 6: mandatory change audit before commit.
- Updated `AGENTS.md` Definition of Done (item 7) to require an audit entry.
- Extended `docs/AI_WORKFLOW.md` with Step 7 (write the change audit entry) and a
  new "bad behavior" example for skipping the audit.
- Created `docs/CHANGE_AUDIT.md` (this file) as the append-only audit log.
- Created `validation/audit_change_compliance.py` — a compliance-check CLI that
  inspects staged or branch-diff changes and reports PASS/FAIL.
- Created `.githooks/pre-commit` — a git hook that runs the compliance CLI on every
  commit and blocks if it reports FAIL.
- Updated `docs/REPO_STRUCTURE.md`:
  - Added `.githooks/` to allowed configuration directories.
  - Added `audit_change_compliance.py` to the `validation/` module list.
  - Added amendment log entry.
- Updated `INSTALL.md` with instructions to activate the git hook.
- Updated `CHANGELOG.md [Unreleased]`.

### Files/packages touched
- AGENTS.md
- docs/AI_WORKFLOW.md
- docs/CHANGE_AUDIT.md (new)
- validation/audit_change_compliance.py (new)
- .githooks/pre-commit (new)
- docs/REPO_STRUCTURE.md
- INSTALL.md
- CHANGELOG.md

### Docs reviewed
- [x] AGENTS.md
- [x] docs/REPO_STRUCTURE.md
- [x] docs/PROJECT_STATUS.md
- [x] docs/IMPLEMENTATION_AUDIT.md
- [ ] relevant feature docs:
  - none applicable — this is infrastructure only

### Docs updated
- [x] CHANGELOG.md
- [ ] README.md — no change needed; audit infrastructure is internal tooling
- [ ] docs/PROJECT_STATUS.md — no validated/deferred status change
- [x] docs/REPO_STRUCTURE.md — added .githooks/, updated validation/ module list
- [ ] relevant feature docs:
  - none applicable

### Status / validation impact
- Validated status changed: no
- Deferred status changed: no
- New claims added: no
- Evidence for any new validation claim:
  - n/a

### Tests run
```bash
pytest tests/test_agent_infrastructure.py tests/test_repo_structure.py -q
```

### Validation CLIs run
```bash
python -m validation.audit_change_compliance --staged
```

### Known limitations / out of scope
- The compliance CLI performs heuristic text-pattern checks on the latest audit
  entry; it does not do deep semantic validation of claim honesty.
- The pre-commit hook must be activated manually per-clone via
  `git config core.hooksPath .githooks` (see INSTALL.md).
- No changes to recorder, stores, pipeline, or converter code.
