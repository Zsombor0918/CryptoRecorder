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


class RawRecordSpool:
    """Bounded-memory external-merge-sorted record spool (issue #20 Phase 6).

    Replaces the prior SQLite-backed implementation (a single on-disk
    B-tree with 3 secondary indexes storing the complete JSON payload text
    per row — "scratch-inefficient" but disk-backed/RAM-bounded, per the
    approved plan's correction #13) with a genuine external merge sort:
    incoming records are buffered in memory only up to a bounded run size
    (``CRYPTO_RECORDER_SPOOL_RUN_SIZE``, default 20000 records), each run
    is sorted in memory and flushed to its own disk-backed pickle run
    file, and the fully sorted output is produced by a bounded k-way merge
    (``heapq.merge``) that holds only one buffered record per run in
    memory at a time — peak resident memory is O(run_size), never
    O(total record count), regardless of how many records are spooled.

    The public interface (``insert``/``commit``/``iter_records``/
    ``first_record``/``has_record_before``/``max_record``/``close``,
    context-manager support, ``.count``, ``.path``) and every method's
    external behavior are UNCHANGED from the prior implementation — this
    is purely an internal storage/ordering mechanism replacement. Every
    existing caller (``convert_day.py``'s raw repartition/carry spools,
    ``stores/replay_writer.py``'s write-batching, ``stores/
    replay_depth_adapter.py``'s replay-side canonical resort,
    ``validation/validate_catalog_equivalence.py``'s raw metadata sort)
    requires zero changes.

    ``first_record``/``has_record_before``/``max_record`` re-merge the run
    files with a fresh streaming pass per call (no persistent index) —
    these are called at most a small, bounded number of times per
    partition build (never once per record), so trading index lookup
    speed for zero extra memory/index-maintenance cost is the correct
    tradeoff here (long runtime is acceptable; unbounded memory is not).
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
        self._run_size = max(1, int(os.environ.get("CRYPTO_RECORDER_SPOOL_RUN_SIZE", "20000")))
        self._buffer: list[Tuple[Tuple[int, int, int, int], str, str, dict]] = []
        self._run_paths: list[Path] = []
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
        self._buffer.append((key, record_type, session_key, record))
        self.count += 1
        if len(self._buffer) >= self._run_size:
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
        try:
            for item in self._buffer:
                pickle.dump(item, fh, protocol=pickle.HIGHEST_PROTOCOL)
        finally:
            fh.close()
        self._run_paths.append(run_path)
        self._buffer = []

    def commit(self) -> None:
        self._flush_run()

    @staticmethod
    def _iter_run_file(path: Path) -> Iterator[Tuple[Tuple[int, int, int, int], str, str, dict]]:
        with open(path, "rb") as fh:
            while True:
                try:
                    yield pickle.load(fh)
                except EOFError:
                    return

    def _iter_sorted(self) -> Iterator[Tuple[Tuple[int, int, int, int], str, str, dict]]:
        """Bounded-memory k-way merge across all sorted run files.

        ``heapq.merge`` pulls at most one buffered item per run file at a
        time, so peak memory here is O(number of runs), not O(total
        record count).
        """
        if not self._run_paths:
            return
        iterators = [self._iter_run_file(p) for p in self._run_paths]
        yield from heapq.merge(*iterators, key=lambda item: item[0])

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
        for run_path in self._run_paths:
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
