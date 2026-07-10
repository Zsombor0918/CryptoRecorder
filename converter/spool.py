"""Disk-backed temporary spools for memory-bounded conversion."""
from __future__ import annotations

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
    """SQLite-backed JSON record spool ordered by canonical sort keys."""

    def __init__(self, *, temp_dir: str | Path | None = None, prefix: str = "cryptorecorder-raw-"):
        self.path = _temp_db_path(prefix, temp_dir)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            """
            CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sort1 INTEGER NOT NULL,
                sort2 INTEGER NOT NULL,
                sort3 INTEGER NOT NULL,
                raw_index INTEGER NOT NULL,
                record_type TEXT NOT NULL,
                session_key TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX records_order_idx ON records (sort1, sort2, sort3, raw_index)"
        )
        self.conn.execute(
            "CREATE INDEX records_session_idx ON records (session_key, sort1, sort2, sort3, raw_index)"
        )
        self.conn.execute(
            "CREATE INDEX records_type_idx ON records (record_type, sort1, sort2, sort3, raw_index)"
        )
        self.count = 0
        self._closed = False

    def __enter__(self) -> "RawRecordSpool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def insert(self, record: dict, sort_key: Tuple[int, int, int], raw_index: int) -> None:
        payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        self.conn.execute(
            """
            INSERT INTO records
                (sort1, sort2, sort3, raw_index, record_type, session_key, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(sort_key[0]),
                int(sort_key[1]),
                int(sort_key[2]),
                int(raw_index),
                str(record.get("record_type", "")),
                _session_key(record),
                payload,
            ),
        )
        self.count += 1

    def commit(self) -> None:
        self.conn.commit()

    def iter_records(
        self,
        *,
        record_type: str | None = None,
        session_id: object | None = None,
        min_sort_key: Tuple[int, int, int] | None = None,
    ) -> Iterator[dict]:
        clauses: list[str] = []
        params: list[Any] = []
        if record_type is not None:
            clauses.append("record_type = ?")
            params.append(record_type)
        if session_id is not None:
            clauses.append("session_key = ?")
            params.append(str(session_id))
        if min_sort_key is not None:
            clauses.append("(sort1, sort2, sort3) >= (?, ?, ?)")
            params.extend([int(min_sort_key[0]), int(min_sort_key[1]), int(min_sort_key[2])])
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cursor = self.conn.execute(
            f"""
            SELECT payload FROM records
            {where}
            ORDER BY sort1, sort2, sort3, raw_index
            """,
            params,
        )
        for (payload,) in cursor:
            yield json.loads(payload)

    def first_record(self, *, record_type: str | None = None) -> Optional[dict]:
        clauses: list[str] = []
        params: list[Any] = []
        if record_type is not None:
            clauses.append("record_type = ?")
            params.append(record_type)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        row = self.conn.execute(
            f"""
            SELECT payload FROM records
            {where}
            ORDER BY sort1, sort2, sort3, raw_index
            LIMIT 1
            """,
            params,
        ).fetchone()
        return json.loads(row[0]) if row else None

    def has_record_before(self, record_type: str, sort_key: Tuple[int, int, int]) -> bool:
        row = self.conn.execute(
            """
            SELECT 1 FROM records
            WHERE record_type = ?
              AND (sort1, sort2, sort3) < (?, ?, ?)
            LIMIT 1
            """,
            (record_type, int(sort_key[0]), int(sort_key[1]), int(sort_key[2])),
        ).fetchone()
        return row is not None

    def max_record(
        self,
        *,
        record_type: str,
        session_id: object,
        first_tie: bool = False,
    ) -> Optional[dict]:
        order_by = (
            "sort1 DESC, sort2 DESC, sort3 ASC, raw_index ASC"
            if first_tie
            else "sort1 DESC, sort2 DESC, sort3 DESC, raw_index DESC"
        )
        row = self.conn.execute(
            f"""
            SELECT payload FROM records
            WHERE record_type = ? AND session_key = ?
            ORDER BY {order_by}
            LIMIT 1
            """,
            (record_type, str(session_id)),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.conn.close()
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
