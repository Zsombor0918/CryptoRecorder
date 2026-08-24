"""Disk-backed temporary spools for memory-bounded conversion."""
from __future__ import annotations

import heapq
import json
import os
import pickle
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence, Tuple


def _spool_dir(temp_dir: str | Path | None = None) -> str | None:
    raw = temp_dir or os.environ.get("CRYPTO_RECORDER_CONVERTER_TEMP_DIR")
    if raw is None:
        return None
    path = Path(raw).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _temp_db_path(prefix: str, temp_dir: str | Path | None = None) -> Path:
    fh = tempfile.NamedTemporaryFile(
        prefix=prefix,
        suffix=".sqlite3",
        dir=_spool_dir(temp_dir),
        delete=False,
    )
    path = Path(fh.name)
    fh.close()
    return path


def _session_key(record: dict) -> str:
    session_id = record.get("stream_session_id", record.get("trade_stream_session_id"))
    return "" if session_id is None else str(session_id)


_DEFAULT_RUN_BYTES = 64 * 1024 * 1024  # 64 MiB
_DEFAULT_FAN_IN = 16


class RawRecordSpool:
    """Bounded-memory external-merge-sorted record spool (issue #20 Phase 6,
    corrected).

    Replaces the prior SQLite-backed implementation (a single on-disk
    B-tree with 3 secondary indexes storing the complete JSON payload text
    per row — "scratch-inefficient" per the approved plan's correction
    #13) with a genuine bounded external merge sort, in two respects:

    1. **Byte-budgeted buffering, not record-count buffering.** Each
       inserted record is serialized immediately
       (``pickle.dumps(record, protocol=HIGHEST_PROTOCOL)``); only the
       resulting bytes (never the live Python object graph) are retained
       in the in-memory buffer. The buffer is flushed to a new sorted,
       disk-backed run file once its *accumulated serialized byte size*
       reaches ``CRYPTO_RECORDER_SPOOL_RUN_BYTES`` (default 64 MiB) — not
       once a record *count* threshold is hit. This is the only
       guarantee that actually bounds RAM on a 16 GB machine: raw depth
       records vary hugely in size (a `depth_update` with a large
       nested `bids`/`asks` array can be orders of magnitude larger than
       a `sync_state` record), so a fixed record count does not bound
       bytes. A single record whose own serialized size already exceeds
       the budget is still accepted — it becomes its own one-record run,
       flushed immediately after being buffered, so an oversized record
       never blocks or grows the buffer without bound.

    2. **Bounded fan-in hierarchical (multi-pass) merge, not a flat
       k-way merge over every run file.** A flat ``heapq.merge`` across
       *all* run files opens (and holds one buffered item for) one file
       per run — memory and file-descriptor usage would be proportional
       to the number of runs, which is unbounded for large inputs. This
       implementation instead merges runs in a tournament of bounded
       passes: each pass takes at most ``CRYPTO_RECORDER_SPOOL_FAN_IN``
       run files (default 16) at a time, merges them via ``heapq.merge``
       into one new output run file, and repeats until a single fully
       sorted run remains. Each merge pass therefore opens at most
       ``fan_in`` input file handles plus 1 output file handle
       (``fan_in + 1`` total) — never proportional to the total number
       of runs. The merge-down is performed once (lazily, on first
       query) and its single resulting run file is cached and reused for
       every subsequent query, so repeated query passes also never
       exceed that same small descriptor bound.

    Each intermediate merge output is written to a temporary
    ``*.run.part`` file which is flushed and closed *before* being
    atomically renamed (``os.replace``, a single filesystem rename) to
    its final ``*.run`` path — a reader can therefore never observe a
    partially written merge output under its final name. If
    ``os.replace`` itself fails (e.g. disk full, cross-device, permission
    error), the still-fully-written-and-closed ``*.run.part`` file is
    treated as a partial output and removed, exactly like a failure
    during writing.

    **Transactional ownership/state model** (corrected): ``self._run_paths``
    is updated to reflect a completed merge — replacing the batch's
    input paths with the new merged output path — only *after*
    ``_merge_batch`` has returned successfully (i.e. after the output is
    fully written, fsync'd, closed, and atomically renamed into place).
    If any batch or merge pass raises partway through a multi-pass
    reduction, ``self._run_paths`` therefore still reflects exactly the
    complete, valid, retryable set of sorted runs from immediately
    before that failing batch — every already-completed batch from this
    same call remains reflected (its output tracked, its inputs no
    longer part of ``self._run_paths``), and the failing batch's own
    inputs are left untouched on disk and still listed. A subsequent
    call (e.g. the next query) automatically retries from that exact
    state — no manual reset is required, since ``self._merged`` is only
    ever set ``True`` once every run has been folded down to one. Every
    run file this spool ever creates (initial flushed runs and every
    merge output, including a ``*.run.part`` that fails before rename)
    is also recorded in ``self._owned_paths``, an append-only ownership
    set consulted by ``close()`` — so ``close()`` reliably removes every
    file this spool ever created, including any intermediate output
    already logically superseded but that happened to fail to unlink,
    and any partial output left behind by a caught failure. Broader
    process-kill/SIGKILL/unknown crash discovery of stray temp files
    *after this process exits* is explicitly out of scope here and
    remains deferred to the already-planned Phase 11 (per the approved
    plan).

    Inserting additional records after a query (which triggers the
    lazy merge-and-cache) is still supported, exactly like the original
    SQLite-backed implementation (where every query was a live view of
    the current table contents): ``insert()`` unconditionally invalidates
    the cached merged state (``self._merged = False``), and the merge is
    lazily redone (folding in the already-merged run alongside any newly
    buffered/flushed records) the next time a query is made — so later
    records are never silently ignored.

    The public interface (``insert``/``commit``/``iter_records``/
    ``first_record``/``has_record_before``/``max_record``/``close``,
    context-manager support, ``.count``, ``.path``) and every method's
    external, observable behavior are UNCHANGED from the prior
    implementation and from the original SQLite-backed implementation
    before it — this is purely an internal storage/ordering mechanism
    correction. Every existing caller (``convert_day.py``'s raw
    repartition/carry spools, ``stores/replay_writer.py``'s
    write-batching, ``stores/replay_depth_adapter.py``'s replay-side
    canonical resort, ``validation/validate_catalog_equivalence.py``'s
    raw metadata sort) requires zero changes.

    ``first_record``/``has_record_before``/``max_record`` stream the
    (lazily merged, then cached) single sorted run — these are called at
    most a small, bounded number of times per partition build (never once
    per record), so trading index lookup speed for zero extra
    memory/index-maintenance cost is the correct tradeoff here (long
    runtime is acceptable; unbounded memory or unbounded descriptors are
    not).
    """

    def __init__(self, *, temp_dir: str | Path | None = None, prefix: str = "cryptorecorder-raw-"):
        # A lightweight marker file (mirrors the prior SQLite-file
        # lifecycle exactly: created at construction under the configured
        # temp dir, removed at close()) — proves CRYPTO_RECORDER_CONVERTER_TEMP_DIR
        # plumbing without requiring a single physical backing file, since
        # this implementation may create zero-or-more run files depending
        # on how many records are actually inserted.
        self.path = _temp_db_path(prefix, temp_dir)
        self._temp_dir = _spool_dir(temp_dir)
        self._prefix = prefix
        self._run_bytes_budget = max(
            1, int(os.environ.get("CRYPTO_RECORDER_SPOOL_RUN_BYTES", str(_DEFAULT_RUN_BYTES)))
        )
        self._fan_in = max(2, int(os.environ.get("CRYPTO_RECORDER_SPOOL_FAN_IN", str(_DEFAULT_FAN_IN))))
        self._buffer: list[Tuple[Tuple[int, int, int, int], str, str, bytes]] = []
        self._buffer_bytes = 0
        self._run_paths: list[Path] = []
        # Append-only record of every run file this spool has ever
        # created (initial flushed runs and every merge output,
        # including a ``.run.part`` that failed before its rename) — the
        # safety net ``close()`` uses to guarantee every owned file is
        # removed, independent of ``self._run_paths``'s current logical
        # state.
        self._owned_paths: set[Path] = set()
        self._merged = False
        self.count = 0
        self._closed = False

    def __enter__(self) -> "RawRecordSpool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def insert(self, record: dict, sort_key: Tuple[int, int, int], raw_index: int) -> None:
        key = (int(sort_key[0]), int(sort_key[1]), int(sort_key[2]), int(raw_index))
        record_type = str(record.get("record_type", ""))
        session_key = _session_key(record)
        payload = pickle.dumps(record, protocol=pickle.HIGHEST_PROTOCOL)
        self._buffer.append((key, record_type, session_key, payload))
        self._buffer_bytes += len(payload)
        self.count += 1
        # Any new record invalidates a previously cached fully merged
        # single run: a later query must fold this (and any other new)
        # record in, exactly like the original SQLite-backed
        # implementation where every query was a live view of the
        # current table contents. Never silently ignored.
        self._merged = False
        # Flushing whenever accumulated bytes reach the budget — even
        # for a single, individually oversized record — bounds memory
        # unconditionally: an oversized record becomes its own
        # one-record run immediately, rather than blocking or growing
        # the buffer without limit.
        if self._buffer_bytes >= self._run_bytes_budget:
            self._flush_run()

    def _flush_run(self) -> None:
        if not self._buffer:
            return
        self._buffer.sort(key=lambda item: item[0])
        fh = tempfile.NamedTemporaryFile(
            prefix=self._prefix,
            suffix=".run",
            dir=self._temp_dir,
            delete=False,
        )
        run_path = Path(fh.name)
        self._owned_paths.add(run_path)
        try:
            for item in self._buffer:
                pickle.dump(item, fh, protocol=pickle.HIGHEST_PROTOCOL)
        finally:
            fh.close()
        self._run_paths.append(run_path)
        self._buffer = []
        self._buffer_bytes = 0

    def commit(self) -> None:
        self._flush_run()

    @staticmethod
    def _iter_run_file(path: Path) -> Iterator[Tuple[Tuple[int, int, int, int], str, str, bytes]]:
        with open(path, "rb") as fh:
            while True:
                try:
                    yield pickle.load(fh)
                except EOFError:
                    return

    def _merge_batch(self, batch: list[Path]) -> Path:
        """Merge at most ``fan_in`` sorted run files into one new sorted
        run file, written atomically: the output is fully written and
        durably closed under a temporary ``*.run.part`` name, then
        atomically renamed (``os.replace``) to its final ``*.run`` name.
        On any failure, the partial output is removed and the exception
        propagates; the input ``batch`` files are left untouched (the
        caller only unlinks them after this method returns successfully).
        """
        tmp_fh = tempfile.NamedTemporaryFile(
            prefix=self._prefix,
            suffix=".run.part",
            dir=self._temp_dir,
            delete=False,
        )
        tmp_path = Path(tmp_fh.name)
        self._owned_paths.add(tmp_path)
        try:
            iterators = [self._iter_run_file(p) for p in batch]
            for item in heapq.merge(*iterators, key=lambda entry: entry[0]):
                pickle.dump(item, tmp_fh, protocol=pickle.HIGHEST_PROTOCOL)
            tmp_fh.flush()
            os.fsync(tmp_fh.fileno())
        except BaseException:
            tmp_fh.close()
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        else:
            tmp_fh.close()
        final_path = Path(str(tmp_path)[: -len(".part")])
        try:
            os.replace(tmp_path, final_path)
        except BaseException:
            # os.replace() itself failed: the fully written, closed
            # `.run.part` file is a partial output exactly like a
            # mid-write failure — remove it and propagate.
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        self._owned_paths.add(final_path)
        return final_path

    def _ensure_single_run(self) -> None:
        """Reduce ``self._run_paths`` to at most one fully sorted run
        file via bounded fan-in hierarchical (multi-pass) merging, then
        cache the result so subsequent calls are no-ops. Each pass opens
        at most ``fan_in`` input files plus 1 output file — never
        proportional to the total number of runs.

        Any pending buffered records are flushed first, so records
        inserted since the last merge (whether or not they triggered an
        automatic budget-flush) are always folded in — a later query
        never silently misses them.

        Processes one bounded batch at a time (rather than whole
        "levels"), reassigning ``self._run_paths`` only immediately
        after each individual batch's merge has fully and successfully
        completed (write + fsync + close + atomic rename). This keeps
        ``self._run_paths`` a complete, valid, retryable set of sorted
        runs at every point — if a later batch (or a later pass over the
        shrinking run list) raises, every already-completed batch from
        this same call remains correctly reflected, and the exception
        propagates with ``self._merged`` still ``False`` so the next
        query call automatically retries from that exact state.
        """
        self._flush_run()
        if self._merged:
            return
        while len(self._run_paths) > 1:
            batch = self._run_paths[: self._fan_in]
            merged_path = self._merge_batch(batch)
            # Commit the new logical state only now that the merge for
            # this batch has fully and durably succeeded: the batch's
            # inputs are atomically replaced by the single merged output
            # in one assignment, so a reader of ``self._run_paths`` (e.g.
            # a failure handler after a caught exception) never observes
            # a partial in-between state.
            self._run_paths = [merged_path] + self._run_paths[len(batch) :]
            # Only now, after the new state is committed, remove the
            # superseded input files from disk. Any file that fails to
            # unlink here (anything other than already-gone) is still in
            # ``self._owned_paths`` and will be cleaned up by ``close()``;
            # it is no longer part of ``self._run_paths``, so it can
            # never be double-processed.
            for p in batch:
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
        self._merged = True

    def _iter_sorted(self) -> Iterator[Tuple[Tuple[int, int, int, int], str, str, dict]]:
        """Stream the (lazily merged, then cached) single fully sorted
        run file, unpickling each record only as it is yielded — bounding
        memory to a single in-flight record at a time."""
        self._ensure_single_run()
        if not self._run_paths:
            return
        for key, rt, sk, payload in self._iter_run_file(self._run_paths[0]):
            yield key, rt, sk, pickle.loads(payload)

    def iter_records(
        self,
        *,
        record_type: str | None = None,
        session_id: object | None = None,
        min_sort_key: Tuple[int, int, int] | None = None,
    ) -> Iterator[dict]:
        session_key = None if session_id is None else str(session_id)
        min_key: Tuple[int, int, int] | None = None
        if min_sort_key is not None:
            min_key = (int(min_sort_key[0]), int(min_sort_key[1]), int(min_sort_key[2]))
        for key, rt, sk, record in self._iter_sorted():
            if record_type is not None and rt != record_type:
                continue
            if session_key is not None and sk != session_key:
                continue
            if min_key is not None and key[:3] < min_key:
                continue
            yield record

    def first_record(self, *, record_type: str | None = None) -> Optional[dict]:
        for key, rt, sk, record in self._iter_sorted():
            if record_type is not None and rt != record_type:
                continue
            return record
        return None

    def has_record_before(self, record_type: str, sort_key: Tuple[int, int, int]) -> bool:
        target = (int(sort_key[0]), int(sort_key[1]), int(sort_key[2]))
        for key, rt, sk, record in self._iter_sorted():
            if key[:3] >= target:
                break
            if rt == record_type:
                return True
        return False

    def max_record(
        self,
        *,
        record_type: str,
        session_id: object,
        first_tie: bool = False,
    ) -> Optional[dict]:
        session_key = str(session_id)
        best_cmp_key: Optional[tuple] = None
        best_record: Optional[dict] = None
        for key, rt, sk, record in self._iter_sorted():
            if rt != record_type or sk != session_key:
                continue
            # Mirrors the prior SQL ORDER BY exactly:
            #   first_tie=True:  sort1 DESC, sort2 DESC, sort3 ASC,  raw_index ASC  LIMIT 1
            #   first_tie=False: sort1 DESC, sort2 DESC, sort3 DESC, raw_index DESC LIMIT 1
            # i.e. maximize (sort1, sort2) always; among ties, minimize
            # (sort3, raw_index) for first_tie, else maximize them too.
            if first_tie:
                cmp_key = (key[0], key[1], -key[2], -key[3])
            else:
                cmp_key = key
            if best_cmp_key is None or cmp_key > best_cmp_key:
                best_cmp_key = cmp_key
                best_record = record
        return best_record

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Union of the current logical run set and every run file ever
        # created by this spool: guarantees removal of every owned file,
        # including intermediate merge outputs already logically
        # superseded, and any partial output left behind by a caught
        # failure (e.g. a ``.run.part`` from a failed ``os.replace``).
        for run_path in set(self._run_paths) | self._owned_paths:
            try:
                run_path.unlink()
            except FileNotFoundError:
                pass
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class DedupeSet:
    """SQLite-backed set for large duplicate suppression keys."""

    def __init__(self, *, temp_dir: str | Path | None = None, prefix: str = "cryptorecorder-dedupe-"):
        self.path = _temp_db_path(prefix, temp_dir)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("CREATE TABLE keys (key TEXT PRIMARY KEY)")
        self._closed = False

    def __enter__(self) -> "DedupeSet":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def add(self, key: Sequence[object]) -> bool:
        payload = json.dumps(list(key), separators=(",", ":"), default=str, ensure_ascii=False)
        try:
            self.conn.execute("INSERT INTO keys (key) VALUES (?)", (payload,))
            return True
        except sqlite3.IntegrityError:
            return False

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.conn.close()
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class ObjectSpool:
    """SQLite-backed spool for Nautilus objects sorted by ts_init."""

    def __init__(self, *, temp_dir: str | Path | None = None, prefix: str = "cryptorecorder-objects-"):
        self.path = _temp_db_path(prefix, temp_dir)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute(
            """
            CREATE TABLE objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_init INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                payload BLOB NOT NULL
            )
            """
        )
        self.conn.execute("CREATE INDEX objects_order_idx ON objects (ts_init, ordinal)")
        self.count = 0
        self._closed = False

    def __enter__(self) -> "ObjectSpool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def insert(self, obj: object, ordinal: int) -> None:
        self.conn.execute(
            "INSERT INTO objects (ts_init, ordinal, payload) VALUES (?, ?, ?)",
            (
                int(getattr(obj, "ts_init")),
                int(ordinal),
                sqlite3.Binary(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)),
            ),
        )
        self.count += 1

    def insert_many(self, objects: Sequence[object], *, start_ordinal: int) -> int:
        ordinal = start_ordinal
        rows = []
        for obj in objects:
            rows.append(
                (
                    int(getattr(obj, "ts_init")),
                    ordinal,
                    sqlite3.Binary(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)),
                )
            )
            ordinal += 1
        if rows:
            self.conn.executemany(
                "INSERT INTO objects (ts_init, ordinal, payload) VALUES (?, ?, ?)",
                rows,
            )
            self.count += len(rows)
        return ordinal

    def commit(self) -> None:
        self.conn.commit()

    def iter_batches(self, batch_size: int) -> Iterator[list[object]]:
        cursor = self.conn.execute(
            "SELECT payload FROM objects ORDER BY ts_init, ordinal"
        )
        batch: list[object] = []
        for (payload,) in cursor:
            batch.append(pickle.loads(payload))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.conn.close()
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


class TimestampSpool:
    """SQLite-backed timestamp spool for sorted gap diagnostics."""

    def __init__(self, *, temp_dir: str | Path | None = None, prefix: str = "cryptorecorder-ts-"):
        self.path = _temp_db_path(prefix, temp_dir)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute(
            """
            CREATE TABLE timestamps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL
            )
            """
        )
        self.conn.execute("CREATE INDEX timestamps_ts_idx ON timestamps (ts)")
        self.count = 0
        self._closed = False

    def __enter__(self) -> "TimestampSpool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def insert(self, ts: int) -> None:
        self.conn.execute("INSERT INTO timestamps (ts) VALUES (?)", (int(ts),))
        self.count += 1

    def insert_many(self, timestamps: Sequence[int]) -> None:
        rows = [(int(ts),) for ts in timestamps]
        if rows:
            self.conn.executemany("INSERT INTO timestamps (ts) VALUES (?)", rows)
            self.count += len(rows)

    def commit(self) -> None:
        self.conn.commit()

    def gap_counts(self) -> dict[str, float | int]:
        cursor = self.conn.execute("SELECT ts FROM timestamps ORDER BY ts")
        previous: Optional[int] = None
        max_gap = 0.0
        over_1s = 0
        over_5s = 0
        over_60s = 0
        for (ts,) in cursor:
            current = int(ts)
            if previous is not None:
                gap = (current - previous) / 1_000_000_000
                if gap > max_gap:
                    max_gap = gap
                if gap > 1.0:
                    over_1s += 1
                if gap > 5.0:
                    over_5s += 1
                if gap > 60.0:
                    over_60s += 1
            previous = current
        return {
            "max_gap_sec": round(max_gap, 6),
            "gap_count_over_1s": over_1s,
            "gap_count_over_5s": over_5s,
            "gap_count_over_60s": over_60s,
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.conn.close()
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
