"""
Storage management: file rotation, compression, and writing.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
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
            logger.warning("Compression manager closed; leaving uncompressed file: %s", file_path)
            return
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

        def compress() -> None:
            dst = Path(f"{file_path}.zst")
            with open(file_path, "rb") as f_in:
                with zstd.open(str(dst), "wb", cctx=zstd.ZstdCompressor(level=3)) as f_out:
                    f_out.write(f_in.read())
            os.remove(file_path)

        await loop.run_in_executor(None, compress)

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
        await asyncio.gather(*self.tasks, return_exceptions=True)
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
                if file_path != self.get_file_path(venue, symbol, channel):
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
                    self.file_handles[key].close()
                except Exception as exc:
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
                file_path = self.get_file_path(venue, symbol, channel)
                handle = open(file_path, "a", encoding="utf-8")
                self.file_handles[key] = handle
                self.current_files[key] = (file_path, time.time())

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
        writer = await self.get_writer(venue, symbol, channel)
        await writer.enqueue(record)

    async def shutdown(self) -> None:
        """Shutdown all writers, flush queues, close files, and drain compression."""
        logger.info("Shutting down storage manager...")

        for writer in self.writers.values():
            await writer.shutdown()

        if self.writer_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.writer_tasks.values(), return_exceptions=True),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                logger.warning("Writer tasks did not finish in 10s - cancelling")
                for task in self.writer_tasks.values():
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self.writer_tasks.values(), return_exceptions=True)

        await self.rotator.close_all(compress=True)
        await self.compression_manager.shutdown()
        logger.info("Storage manager shutdown complete")

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
