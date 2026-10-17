"""
Storage management: file rotation, compression, and writing.
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import stat
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import zstandard as zstd

from config import (
    DATA_ROOT,
    DEPTH_BLOCK_ALERT_SEC,
    DEPTH_BLOCK_WARN_INTERVAL_SEC,
    DEPTH_V2_CHANNEL,
    DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC,
    DEPTH_WRITER_QUEUE_MAX_SIZE,
    QUEUE_MAX_SIZE,
    ROTATION_INTERVAL_MIN,
    TRADE_V2_CHANNEL,
    TRADE_WRITER_ENQUEUE_TIMEOUT_SEC,
    TRADE_WRITER_QUEUE_MAX_SIZE,
    WRITER_BATCH_SIZE,
    WRITER_COMPRESSION_WORKERS,
    WRITER_COMPRESSION_SHUTDOWN_TIMEOUT_SEC,
    WRITER_FLUSH_INTERVAL_SEC,
    WRITER_TELEMETRY_LOG_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


@contextmanager
def _raw_directory_lock(directory: Path):
    """Serialize allocation with publication, never the compression work itself."""
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _file_stamp(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class CompressionManager:
    """Runs file compression away from the writer rotation path."""

    def __init__(self, worker_count: int = WRITER_COMPRESSION_WORKERS):
        self.worker_count = max(1, int(worker_count))
        self.queue: asyncio.Queue[Optional[Path]] = asyncio.Queue()
        self.tasks: list[asyncio.Task] = []
        self.active = 0
        self.completed = 0
        self.failed = 0
        self.last_error: Optional[str] = None
        self.completed_files: dict[Path, dict] = {}
        self.coverage_errors: list[str] = []
        self._started = False
        self._closed = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        for index in range(self.worker_count):
            self.tasks.append(
                asyncio.create_task(self._worker(index + 1), name=f"compressor-{index + 1}")
            )

    async def enqueue(self, file_path: Path) -> None:
        """Queue a closed JSONL file for compression."""
        if self._closed:
            raise RuntimeError(f"compression manager closed: {file_path}")
        if not file_path.exists() or file_path.stat().st_size <= 0:
            return
        await self.start()
        await self.queue.put(file_path)

    async def _worker(self, worker_id: int) -> None:
        while True:
            file_path = await self.queue.get()
            try:
                if file_path is None:
                    return
                self.active += 1
                try:
                    await self._compress_file(file_path)
                    self.completed += 1
                    logger.debug("Compressed rotated file: %s", file_path)
                except Exception as exc:
                    self.failed += 1
                    self.last_error = f"{file_path}: {exc}"
                    logger.error("Error compressing rotated file %s: %s", file_path, exc, exc_info=True)
                finally:
                    self.active -= 1
            finally:
                self.queue.task_done()

    async def _compress_file(self, file_path: Path) -> None:
        """Compress file with zstd. Only removes source on success."""
        loop = asyncio.get_running_loop()

        def compress() -> dict:
            from recording_coverage import file_identity, _fsync_dir

            dst = Path(f"{file_path}.zst")
            pending = file_path.with_name(f".compress-{file_path.stem}-{uuid.uuid4().hex}.pending")
            published = False
            pending_created = False
            try:
                source_fd = os.open(file_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(source_fd, "rb") as f_in:
                    fcntl.flock(f_in, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    before = os.fstat(f_in.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                        raise RuntimeError(f"unsafe compression source: {file_path}")
                    os.fsync(f_in.fileno())
                    if os.path.lexists(dst) or os.path.lexists(f"{file_path}.gz"):
                        raise FileExistsError(f"compression destination already exists: {dst}")
                    # The pending name cannot match any historical *.jsonl* reader.
                    with pending.open("xb") as raw_out:
                        pending_created = True
                        digest = hashlib.sha256()
                        with zstd.ZstdCompressor(level=3, write_checksum=True).stream_writer(raw_out, closefd=False) as f_out:
                            for chunk in iter(lambda: f_in.read(128 * 1024), b""):
                                digest.update(chunk)
                                f_out.write(chunk)
                        raw_out.flush()
                        os.fsync(raw_out.fileno())
                    decoded = hashlib.sha256()
                    with zstd.open(pending, "rb") as check:
                        for chunk in iter(lambda: check.read(128 * 1024), b""):
                            decoded.update(chunk)
                    if decoded.digest() != digest.digest():
                        raise RuntimeError(f"compression verification failed: {file_path}")
                    identity = file_identity(pending)
                    with _raw_directory_lock(file_path.parent):
                        if (_file_stamp(before) != _file_stamp(os.fstat(f_in.fileno())) or
                                _file_stamp(before) != _file_stamp(file_path.lstat())):
                            raise RuntimeError(f"compression source changed: {file_path}")
                        if os.path.lexists(f"{file_path}.gz"):
                            raise FileExistsError(f"compressed alternative already exists: {file_path}")
                        # link() is atomic and refuses every pre-existing destination,
                        # including dangling symlinks; replace()/wb would destroy it.
                        os.link(pending, dst)
                        published = True
                        _fsync_dir(dst.parent)
                        pending.unlink()
                        _fsync_dir(dst.parent)
                    # Retain the existing single-link coverage invariant. The
                    # potentially large verification read holds no directory lock.
                    if file_identity(dst) != identity:
                        raise RuntimeError(f"published compression identity mismatch: {dst}")
                    with _raw_directory_lock(file_path.parent):
                        if _file_stamp(before) != _file_stamp(file_path.lstat()):
                            raise RuntimeError(f"compression source changed: {file_path}")
                        file_path.unlink()
                        _fsync_dir(dst.parent)
                    return identity
            except BaseException:
                # After publication retain crash/dual-representation evidence.
                # Before publication only this invocation's disposable output is removed.
                if pending_created and not published:
                    pending.unlink()
                    _fsync_dir(pending.parent)
                raise

        identity = await loop.run_in_executor(None, compress)
        from recording_coverage import MAX_RAW_FILES
        if len(self.completed_files) < MAX_RAW_FILES:
            self.completed_files[Path(f"{file_path}.zst")] = identity
        elif not self.coverage_errors:
            self.coverage_errors.append("compressed-file evidence bound exceeded")

    async def shutdown(
        self,
        timeout_sec: float = WRITER_COMPRESSION_SHUTDOWN_TIMEOUT_SEC,
    ) -> None:
        """Wait for queued compression, then stop workers."""
        if not self._started:
            self._closed = True
            return
        timed_out = False
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            timed_out = True
            self.failed += self.queue.qsize() + self.active
            self.last_error = (
                f"compression shutdown timed out after {timeout_sec:.1f}s "
                f"(queued={self.queue.qsize()} active={self.active})"
            )
            logger.error(self.last_error)
        self._closed = True
        if timed_out:
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()
                except asyncio.QueueEmpty:
                    break
            self.tasks.clear()
            return

        for _ in self.tasks:
            await self.queue.put(None)
        results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                self.failed += 1
                self.last_error = f"compression worker failed: {result}"
        self.tasks.clear()

    def get_telemetry(self) -> Dict[str, Any]:
        return {
            "queued": self.queue.qsize(),
            "active": self.active,
            "completed": self.completed,
            "failed": self.failed,
            "last_error": self.last_error,
            "worker_count": self.worker_count,
        }


class FileRotator:
    """Manages hourly file rotation and queues compression off the hot path."""

    def __init__(self, compression_manager: Optional[CompressionManager] = None):
        self.current_files: Dict[str, Tuple[Path, float]] = {}
        self.file_handles: Dict[str, object] = {}
        self.lock = asyncio.Lock()
        self.compression_manager = compression_manager
        self.closure_errors: list[str] = []
        self.touched_files: set[Path] = set()
        self.preexisting_files: set[Path] = set()

    def get_file_key(self, venue: str, symbol: str, channel: str) -> str:
        """Generate unique key for a file."""
        return f"{venue}:{symbol}:{channel}"

    def _parts_from_key(self, key: str) -> Optional[Tuple[str, str, str]]:
        parts = key.split(":", 2)
        if len(parts) != 3:
            return None
        return parts[0], parts[1], parts[2]

    def get_file_path(self, venue: str, symbol: str, channel: str) -> Path:
        """Generate file path for venue/channel/symbol."""
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%Y-%m-%dT%H")

        path = DATA_ROOT / venue / channel / symbol / date_str / f"{hour_str}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    async def should_rotate(self, key: str) -> bool:
        """Check if file should rotate based on time or UTC path boundary."""
        async with self.lock:
            if key not in self.current_files:
                return True

            file_path, creation_time = self.current_files[key]
            key_parts = self._parts_from_key(key)
            if key_parts is not None:
                venue, symbol, channel = key_parts
                canonical = self.get_file_path(venue, symbol, channel)
                if (file_path.parent != canonical.parent or
                        file_path.name.split(".", 1)[0] != canonical.stem):
                    return True

            elapsed_min = (time.time() - creation_time) / 60
            return elapsed_min >= ROTATION_INTERVAL_MIN

    async def rotate_file(self, key: str, compress: bool = True) -> Optional[Path]:
        """Close current file and queue compression after releasing the lock.

        Returns the closed .jsonl path only when compression could not be queued
        or completed by the synchronous fallback.
        """
        file_to_compress: Optional[Path] = None
        async with self.lock:
            if key not in self.current_files:
                return None

            file_path, _ = self.current_files[key]

            if key in self.file_handles:
                try:
                    try:
                        self.file_handles[key].flush()
                        os.fsync(self.file_handles[key].fileno())
                    finally:
                        self.file_handles[key].close()
                except Exception as exc:
                    self.closure_errors.append(f"file close failed: {file_path}: {exc}")
                    logger.warning("Error closing file %s: %s", file_path, exc)
                del self.file_handles[key]

            del self.current_files[key]

            if compress and file_path.exists() and file_path.stat().st_size > 0:
                file_to_compress = file_path

        if file_to_compress is None:
            return None

        try:
            if self.compression_manager is not None:
                await self.compression_manager.enqueue(file_to_compress)
                logger.debug("Queued rotated file for compression: %s", file_to_compress)
            else:
                await CompressionManager(worker_count=1)._compress_file(file_to_compress)
                logger.debug("Rotated and compressed: %s", file_to_compress)
        except Exception as exc:
            self.closure_errors.append(f"compression scheduling failed: {file_to_compress}: {exc}")
            logger.error("Error scheduling compression for %s: %s", file_to_compress, exc, exc_info=True)
            return file_to_compress
        return None

    async def get_file_handle(self, venue: str, symbol: str, channel: str):
        """Get or create file handle for writing."""
        key = self.get_file_key(venue, symbol, channel)

        if await self.should_rotate(key):
            await self.rotate_file(key)

        async with self.lock:
            if key not in self.file_handles:
                canonical = self.get_file_path(venue, symbol, channel)
                from recording_coverage import _fsync_dir, _safe_path
                _safe_path(canonical.parent, directory=True)
                with _raw_directory_lock(canonical.parent):
                    pattern = re.compile(re.escape(canonical.stem) + r"(?:\.segment-([0-9]{6}))?\.jsonl(?:\.zst|\.gz)?")
                    ordinals = []
                    # Keep removed/empty paths reserved within this rotator as well.
                    names = {p.name for p in canonical.parent.iterdir()} | {
                        p.name for p in self.touched_files if p.parent == canonical.parent}
                    for name in names:
                        if not name.startswith(canonical.stem):
                            continue
                        match = pattern.fullmatch(name)
                        if match is None or match.group(1) == "000000":
                            raise RuntimeError(f"unknown raw hour artifact: {name}")
                        existing = canonical.parent / name
                        if os.path.lexists(existing):
                            _safe_path(existing)
                        ordinals.append(int(match.group(1) or 0))
                    ordinal = max(ordinals) + 1 if ordinals else 0
                    if ordinal > 999999:
                        raise RuntimeError(f"raw segment ordinal exhausted: {canonical}")
                    file_path = (canonical if ordinal == 0 else
                                 canonical.with_name(f"{canonical.stem}.segment-{ordinal:06d}.jsonl"))
                    handle = open(file_path, "x", encoding="utf-8")
                    try:
                        _fsync_dir(file_path.parent)
                    except BaseException:
                        handle.close()
                        raise
                self.file_handles[key] = handle
                self.current_files[key] = (file_path, time.time())
                from recording_coverage import MAX_RAW_FILES
                if len(self.touched_files) < MAX_RAW_FILES:
                    self.touched_files.add(file_path)
                elif "touched-file evidence bound exceeded" not in self.closure_errors:
                    self.closure_errors.append("touched-file evidence bound exceeded")

            return self.file_handles[key]

    async def close_all(self, compress: bool = True) -> int:
        """Close all open handles and queue final compression.

        Returns the number of files closed without immediate compression
        scheduling failure.
        """
        keys = list(self.current_files.keys())
        queued = 0
        failed = 0
        for key in keys:
            result = await self.rotate_file(key, compress=compress)
            if result is None:
                queued += 1
            else:
                failed += 1
        if compress:
            logger.info("Shutdown compression queued: %d files, %d scheduling failures", queued, failed)
        return queued


def _queue_size_for_channel(channel: str) -> int:
    if channel == DEPTH_V2_CHANNEL:
        return DEPTH_WRITER_QUEUE_MAX_SIZE
    if channel == TRADE_V2_CHANNEL:
        return TRADE_WRITER_QUEUE_MAX_SIZE
    return QUEUE_MAX_SIZE


def _enqueue_timeout_for_channel(channel: str) -> float:
    if channel == DEPTH_V2_CHANNEL:
        return DEPTH_WRITER_ENQUEUE_TIMEOUT_SEC
    if channel == TRADE_V2_CHANNEL:
        return TRADE_WRITER_ENQUEUE_TIMEOUT_SEC
    return TRADE_WRITER_ENQUEUE_TIMEOUT_SEC


class AsyncWriter:
    """Writes records to disk with async queuing and batching."""

    def __init__(self, venue: str, symbol: str, channel: str, rotator: FileRotator):
        self.venue = venue
        self.symbol = symbol
        self.channel = channel
        self.rotator = rotator

        self.queue_max_size = _queue_size_for_channel(channel)
        self.enqueue_timeout_sec = _enqueue_timeout_for_channel(channel)
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=self.queue_max_size)
        self.running = True
        self.write_count = 0
        self.write_errors = 0
        self.drop_count = 0
        self.enqueued_count = 0
        self.queue_high_watermark = 0
        self.last_flush_time = time.time()
        self.blocked_since_monotonic: Optional[float] = None
        self.last_block_started_ts: Optional[float] = None
        self.last_block_ended_ts: Optional[float] = None
        self.max_block_sec = 0.0

    @property
    def writer_id(self) -> str:
        return f"{self.venue}/{self.symbol}/{self.channel}"

    def _mark_enqueued(self) -> None:
        self.enqueued_count += 1
        self.queue_high_watermark = max(self.queue_high_watermark, self.queue.qsize())

    def _record_drop(self, reason: str) -> None:
        self.drop_count += 1
        logger.warning(
            "Queue full for %s, dropping record (reason=%s total_drops=%d)",
            self.writer_id,
            reason,
            self.drop_count,
        )

    async def enqueue(self, record: dict) -> None:
        """Enqueue a record for writing using channel-specific backpressure."""
        if not self.running:
            raise RuntimeError(f"writer already stopped: {self.writer_id}")
        if self.channel == DEPTH_V2_CHANNEL:
            await self._enqueue_depth(record)
            return
        await self._enqueue_lossy(record)

    async def _enqueue_lossy(self, record: dict) -> None:
        try:
            if self.enqueue_timeout_sec <= 0:
                self.queue.put_nowait(record)
            else:
                await asyncio.wait_for(self.queue.put(record), timeout=self.enqueue_timeout_sec)
            self._mark_enqueued()
        except (asyncio.TimeoutError, asyncio.QueueFull):
            self._record_drop("enqueue_timeout")

    async def _enqueue_depth(self, record: dict) -> None:
        start = time.monotonic()
        deadline = (
            start + self.enqueue_timeout_sec
            if self.enqueue_timeout_sec and self.enqueue_timeout_sec > 0
            else None
        )
        was_blocked = self.queue.full()
        if was_blocked:
            self.blocked_since_monotonic = start
            self.last_block_started_ts = time.time()
        try:
            while True:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._record_drop("depth_enqueue_timeout")
                        return
                    wait_timeout = min(DEPTH_BLOCK_WARN_INTERVAL_SEC, remaining)
                else:
                    wait_timeout = DEPTH_BLOCK_WARN_INTERVAL_SEC

                try:
                    await asyncio.wait_for(self.queue.put(record), timeout=wait_timeout)
                    self._mark_enqueued()
                    return
                except asyncio.TimeoutError:
                    now = time.monotonic()
                    elapsed = now - start
                    if self.blocked_since_monotonic is None:
                        self.blocked_since_monotonic = start
                        self.last_block_started_ts = time.time() - elapsed
                    self.max_block_sec = max(self.max_block_sec, elapsed)
                    was_blocked = True
                    log_level = logging.ERROR if elapsed >= DEPTH_BLOCK_ALERT_SEC else logging.WARNING
                    logger.log(
                        log_level,
                        "Depth writer queue blocked for %.1fs: %s queue=%d/%d high=%d",
                        elapsed,
                        self.writer_id,
                        self.queue.qsize(),
                        self.queue_max_size,
                        self.queue_high_watermark,
                    )
        finally:
            if was_blocked or self.blocked_since_monotonic is not None:
                elapsed = time.monotonic() - start
                self.max_block_sec = max(self.max_block_sec, elapsed)
                self.last_block_ended_ts = time.time()
                self.blocked_since_monotonic = None

    async def writer_task(self) -> None:
        """Background task that flushes queue to disk."""
        batch: list[dict] = []

        while self.running:
            try:
                try:
                    record = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=WRITER_FLUSH_INTERVAL_SEC,
                    )
                    batch.append(record)
                    self._drain_queue_into(batch)
                except asyncio.TimeoutError:
                    pass

                should_flush = (
                    len(batch) >= WRITER_BATCH_SIZE
                    or (time.time() - self.last_flush_time) >= WRITER_FLUSH_INTERVAL_SEC
                )

                if should_flush and batch:
                    await self._write_batch(batch)
                    batch = []
                    self.last_flush_time = time.time()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.write_errors += 1
                logger.error("Error in writer task for %s: %s", self.writer_id, exc, exc_info=True)
                await asyncio.sleep(1)

        self._drain_queue_into(batch, limit=None)
        if batch:
            await self._write_batch(batch)

    def _drain_queue_into(self, batch: list[dict], limit: Optional[int] = WRITER_BATCH_SIZE) -> None:
        while limit is None or len(batch) < limit:
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break

    async def _write_batch(self, batch: list[dict]) -> None:
        """Write batch of records to file."""
        try:
            handle = await self.rotator.get_file_handle(self.venue, self.symbol, self.channel)
            handle.writelines(json.dumps(record) + "\n" for record in batch)
            handle.flush()
            self.write_count += len(batch)
        except Exception as exc:
            self.write_errors += 1
            logger.error("Error writing batch for %s: %s", self.writer_id, exc, exc_info=True)

    async def shutdown(self) -> None:
        """Stop the writer task and flush remaining data."""
        self.running = False

    def get_telemetry(self) -> Dict[str, Any]:
        now_mono = time.monotonic()
        current_block_sec = (
            max(0.0, now_mono - self.blocked_since_monotonic)
            if self.blocked_since_monotonic is not None
            else 0.0
        )
        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "channel": self.channel,
            "queue_size": self.queue.qsize(),
            "queue_max_size": self.queue_max_size,
            "queue_high_watermark": self.queue_high_watermark,
            "queue_utilization": (
                round(self.queue.qsize() / self.queue_max_size, 6)
                if self.queue_max_size
                else 0.0
            ),
            "drop_count": self.drop_count,
            "enqueued_count": self.enqueued_count,
            "write_count": self.write_count,
            "write_errors": self.write_errors,
            "blocked": self.blocked_since_monotonic is not None,
            "current_block_sec": round(current_block_sec, 3),
            "max_block_sec": round(max(self.max_block_sec, current_block_sec), 3),
            "last_block_started_ts": self.last_block_started_ts,
            "last_block_ended_ts": self.last_block_ended_ts,
        }


class StorageManager:
    """Manages all file writers, rotation, and background compression."""

    def __init__(self):
        self.compression_manager = CompressionManager()
        self.rotator = FileRotator(compression_manager=self.compression_manager)
        self.writers: Dict[str, AsyncWriter] = {}
        self.writer_tasks: Dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()
        self._last_telemetry_log_time = 0.0
        self._closing = False
        self._inflight = 0

    async def get_writer(self, venue: str, symbol: str, channel: str) -> AsyncWriter:
        """Get or create a writer for venue/symbol/channel."""
        key = self.rotator.get_file_key(venue, symbol, channel)

        async with self.lock:
            if key not in self.writers:
                writer = AsyncWriter(venue, symbol, channel, self.rotator)
                self.writers[key] = writer

                task = asyncio.create_task(writer.writer_task())
                self.writer_tasks[key] = task

            return self.writers[key]

    async def write_record(self, venue: str, symbol: str, channel: str, record: dict) -> None:
        """Enqueue a record for writing."""
        if self._closing:
            raise RuntimeError("storage already closing")
        self._inflight += 1
        try:
            writer = await self.get_writer(venue, symbol, channel)
            await writer.enqueue(record)
        finally:
            self._inflight -= 1

    async def shutdown(self) -> dict:
        """Shutdown all writers, flush queues, close files, and drain compression."""
        logger.info("Shutting down storage manager...")
        self._closing = True
        errors = []
        deadline = time.monotonic() + 10
        while self._inflight and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        if self._inflight:
            errors.append("enqueue operations did not stop")

        for writer in self.writers.values():
            await writer.shutdown()

        if self.writer_tasks:
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*self.writer_tasks.values(), return_exceptions=True),
                    timeout=10,
                )
                errors.extend(f"writer task failed: {result}" for result in results if isinstance(result, BaseException))
            except asyncio.TimeoutError:
                errors.append("writer tasks timed out")
                logger.warning("Writer tasks did not finish in 10s - cancelling")
                for task in self.writer_tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self.writer_tasks.values(), return_exceptions=True)

        await self.rotator.close_all(compress=True)
        await self.compression_manager.shutdown()
        errors.extend(self.rotator.closure_errors)
        errors.extend(self.compression_manager.coverage_errors)
        writers = self.get_writer_telemetry()["writers"]
        compression = self.compression_manager.get_telemetry()
        result = {
            "queues_drained": not self._inflight and not errors and all(
                w["queue_size"] == 0 and w["enqueued_count"] == w["write_count"]
                and w["write_errors"] == 0 and w["drop_count"] == 0
                for w in writers.values()),
            "files_closed": not self.rotator.file_handles and not self.rotator.current_files and not self.rotator.closure_errors,
            "compression_drained": compression["queued"] == 0 and compression["active"] == 0 and compression["failed"] == 0 and compression["last_error"] is None,
            "writers": writers, "compression": compression, "errors": errors,
        }
        logger.info("Storage manager shutdown complete")
        return result

    def coverage_raw_files(self) -> dict:
        """Hash final session-touched market files only after storage shutdown.

        No raw marker or raw record is written. Ambiguous variants and failures
        leave the session untrusted. Hashing is streamed and bounded.
        """
        from recording_coverage import CoverageError, MAX_RAW_FILES, _safe_path
        if len(self.rotator.touched_files) > MAX_RAW_FILES:
            raise CoverageError("session raw inventory bound exceeded")
        files = {}
        for plain in sorted(self.rotator.touched_files):
            relative = plain.relative_to(DATA_ROOT)
            if relative.parts[1] not in {DEPTH_V2_CHANNEL, TRADE_V2_CHANNEL}:
                continue
            if plain.exists() or plain.is_symlink() or Path(f"{plain}.gz").exists():
                raise CoverageError(f"uncompressed/ambiguous session raw file: {plain}")
            compressed = Path(f"{plain}.zst")
            _safe_path(compressed)
            identity = self.compression_manager.completed_files.get(compressed)
            if identity is None or compressed.stat().st_size != identity["size_bytes"]:
                raise CoverageError(f"missing compression identity: {compressed}")
            files[compressed.relative_to(DATA_ROOT).as_posix()] = identity
        return files

    def coverage_unowned_files(self) -> list[str]:
        """Legacy API: exclusive allocation no longer inherits existing raw bytes."""
        return sorted(Path(f"{p}.zst").relative_to(DATA_ROOT).as_posix()
                      for p in self.rotator.preexisting_files
                      if p.relative_to(DATA_ROOT).parts[1] in {DEPTH_V2_CHANNEL, TRADE_V2_CHANNEL})

    def get_write_counts(self) -> Dict[str, int]:
        """Get write counts for all writers (for monitoring)."""
        return {key: writer.write_count for key, writer in self.writers.items()}

    def get_drop_counts(self) -> Dict[str, int]:
        """Get drop counts for all writers (for monitoring)."""
        return {key: writer.drop_count for key, writer in self.writers.items()}

    def get_total_drops(self) -> int:
        """Total number of dropped records across all writers."""
        return sum(writer.drop_count for writer in self.writers.values())

    def get_writer_telemetry(self, *, log_summary: bool = False) -> Dict[str, Any]:
        writers = {
            key: writer.get_telemetry()
            for key, writer in sorted(self.writers.items())
        }
        queued_records = sum(item["queue_size"] for item in writers.values())
        total_drops = sum(item["drop_count"] for item in writers.values())
        depth_blocked = sum(
            1
            for item in writers.values()
            if item["channel"] == DEPTH_V2_CHANNEL and item["blocked"]
        )
        top_pressure = self._top_pressure_writers(writers)
        telemetry = {
            "writers": writers,
            "totals": {
                "writer_count": len(writers),
                "queued_records": queued_records,
                "total_drops": total_drops,
                "depth_blocked_writer_count": depth_blocked,
            },
            "top_pressure_writers": top_pressure,
            "compression": self.compression_manager.get_telemetry(),
        }
        if log_summary:
            self._maybe_log_pressure_summary(telemetry)
        return telemetry

    def _top_pressure_writers(self, writers: Dict[str, Dict[str, Any]], limit: int = 10) -> list[Dict[str, Any]]:
        def score(item: tuple[str, Dict[str, Any]]) -> tuple[float, float, float, int]:
            _, data = item
            block_score = data["current_block_sec"] if data["blocked"] else 0.0
            return (
                block_score,
                float(data["drop_count"]),
                float(data["queue_utilization"]),
                int(data["queue_high_watermark"]),
            )

        selected: list[Dict[str, Any]] = []
        for key, data in sorted(writers.items(), key=score, reverse=True):
            if (
                data["queue_size"] <= 0
                and data["queue_high_watermark"] <= 0
                and data["drop_count"] <= 0
                and not data["blocked"]
            ):
                continue
            entry = {"key": key}
            entry.update(data)
            selected.append(entry)
            if len(selected) >= limit:
                break
        return selected

    def _maybe_log_pressure_summary(self, telemetry: Dict[str, Any]) -> None:
        if WRITER_TELEMETRY_LOG_INTERVAL_SEC <= 0:
            return
        now = time.monotonic()
        if now - self._last_telemetry_log_time < WRITER_TELEMETRY_LOG_INTERVAL_SEC:
            return
        totals = telemetry["totals"]
        compression = telemetry["compression"]
        has_pressure = (
            totals["queued_records"] > 0
            or totals["total_drops"] > 0
            or totals["depth_blocked_writer_count"] > 0
            or compression["queued"] > 0
            or compression["active"] > 0
            or compression["failed"] > 0
        )
        if not has_pressure:
            return
        self._last_telemetry_log_time = now

        top = telemetry["top_pressure_writers"][:5]
        top_text = "; ".join(
            (
                f"{item['key']} q={item['queue_size']}/{item['queue_max_size']} "
                f"high={item['queue_high_watermark']} drops={item['drop_count']} "
                f"block={item['current_block_sec']:.1f}s"
            )
            for item in top
        ) or "none"
        log_level = (
            logging.WARNING
            if totals["total_drops"] > 0 or totals["depth_blocked_writer_count"] > 0 or compression["failed"] > 0
            else logging.INFO
        )
        logger.log(
            log_level,
            "Writer pressure summary: queued=%d drops=%d depth_blocked=%d "
            "compression queued=%d active=%d failed=%d top=%s",
            totals["queued_records"],
            totals["total_drops"],
            totals["depth_blocked_writer_count"],
            compression["queued"],
            compression["active"],
            compression["failed"],
            top_text,
        )
