"""
Storage management: file rotation, compression, and writing.
"""
import asyncio
import gzip
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import zstandard as zstd

from config import (
    DATA_ROOT,
    COMPRESSION_FORMAT,
    ROTATION_INTERVAL_MIN,
    QUEUE_MAX_SIZE,
    WRITER_BATCH_SIZE,
    WRITER_FLUSH_INTERVAL_SEC,
)

logger = logging.getLogger(__name__)


class FileRotator:
    """Manages hourly file rotation with compression."""
    
    def __init__(self):
        self.current_files: Dict[str, Tuple[Path, float]] = {}  # key -> (file_path, creation_time)
        self.file_handles: Dict[str, object] = {}  # key -> file handle (if open)
        self.lock = asyncio.Lock()
        self._shutting_down = False
    
    def get_file_key(self, venue: str, symbol: str, channel: str) -> str:
        """Generate unique key for a file."""
        return f"{venue}:{symbol}:{channel}"
    
    def get_file_path(self, venue: str, symbol: str, channel: str) -> Path:
        """Generate file path for venue/channel/symbol."""
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%Y-%m-%dT%H")
        
        path = DATA_ROOT / venue / channel / symbol / date_str / f"{hour_str}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    
    async def should_rotate(self, key: str) -> bool:
        """Check if file should be rotated based on time."""
        async with self.lock:
            if key not in self.current_files:
                return True
            
            _, creation_time = self.current_files[key]
            elapsed_min = (time.time() - creation_time) / 60
            return elapsed_min >= ROTATION_INTERVAL_MIN
    
    async def rotate_file(self, key: str, compress: bool = True) -> None:
        """Close current file and optionally compress it."""
        async with self.lock:
            if key not in self.current_files:
                return
            
            file_path, _ = self.current_files[key]
            
            # Close file handle
            if key in self.file_handles:
                try:
                    self.file_handles[key].close()
                except Exception as e:
                    logger.warning(f"Error closing file {file_path}: {e}")
                del self.file_handles[key]
            
            # Compress file (skip during shutdown to avoid blocking)
            if compress and not self._shutting_down and file_path.exists():
                try:
                    await self._compress_file(file_path)
                    logger.debug(f"Rotated and compressed: {file_path}")
                except Exception as e:
                    logger.error(f"Error compressing file {file_path}: {e}")
            
            del self.current_files[key]
    
    async def _compress_file(self, file_path: Path) -> None:
        """Compress file with zstd."""
        loop = asyncio.get_event_loop()
        
        def compress():
            try:
                with open(file_path, 'rb') as f_in:
                    with zstd.open(f"{file_path}.zst", 'wb', cctx=zstd.ZstdCompressor(level=3)) as f_out:
                        f_out.write(f_in.read())
                os.remove(file_path)
            except Exception as e:
                logger.error(f"Compression failed for {file_path}: {e}")
                raise
        
        await loop.run_in_executor(None, compress)
    
    async def get_file_handle(self, venue: str, symbol: str, channel: str):
        """Get or create file handle for writing."""
        key = self.get_file_key(venue, symbol, channel)
        
        # Check if rotation needed
        if await self.should_rotate(key):
            await self.rotate_file(key)
        
        async with self.lock:
            # Create new file handle if needed
            if key not in self.file_handles:
                file_path = self.get_file_path(venue, symbol, channel)
                handle = open(file_path, 'a')
                self.file_handles[key] = handle
                self.current_files[key] = (file_path, time.time())
            
            return self.file_handles[key]
    
    async def close_all(self) -> None:
        """Close all open file handles.  Compression is skipped during shutdown."""
        self._shutting_down = True
        keys = list(self.current_files.keys())
        for key in keys:
            await self.rotate_file(key, compress=False)


class AsyncWriter:
    """Writes records to disk with async queuing and batching."""
    
    def __init__(self, venue: str, symbol: str, channel: str, rotator: FileRotator):
        self.venue = venue
        self.symbol = symbol
        self.channel = channel
        self.rotator = rotator
        
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self.running = True
        self.write_count = 0
        self.last_flush_time = time.time()
    
    async def enqueue(self, record: dict) -> None:
        """Enqueue a record for writing (non-blocking with backpressure)."""
        try:
            await asyncio.wait_for(
                self.queue.put(record),
                timeout=1.0
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Queue full for {self.venue}/{self.symbol}/{self.channel}, dropping record"
            )
    
    async def writer_task(self) -> None:
        """Background task that flushes queue to disk."""
        batch = []
        
        while self.running:
            try:
                # Try to get a record with timeout for periodic flushing
                try:
                    record = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=WRITER_FLUSH_INTERVAL_SEC
                    )
                    batch.append(record)
                except asyncio.TimeoutError:
                    # Timeout occurred, flush what we have
                    pass
                
                # Write batch if it reaches size or time interval
                should_flush = (
                    len(batch) >= WRITER_BATCH_SIZE or
                    (time.time() - self.last_flush_time) >= WRITER_FLUSH_INTERVAL_SEC
                )
                
                if should_flush and batch:
                    await self._write_batch(batch)
                    batch = []
                    self.last_flush_time = time.time()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Error in writer task for {self.venue}/{self.symbol}/{self.channel}: {e}",
                    exc_info=True
                )
                await asyncio.sleep(1)
        
        # Drain remaining queue items (non-blocking)
        while not self.queue.empty():
            try:
                batch.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            await self._write_batch(batch)
    
    async def _write_batch(self, batch: list) -> None:
        """Write batch of records to file."""
        try:
            handle = await self.rotator.get_file_handle(self.venue, self.symbol, self.channel)
            
            for record in batch:
                line = json.dumps(record) + '\n'
                handle.write(line)
            
            handle.flush()
            self.write_count += len(batch)
            
        except Exception as e:
            logger.error(f"Error writing batch: {e}", exc_info=True)
    
    async def shutdown(self) -> None:
        """Stop the writer task and flush remaining data."""
        self.running = False


class StorageManager:
    """Manages all file writers and rotation."""
    
    def __init__(self):
        self.rotator = FileRotator()
        self.writers: Dict[str, AsyncWriter] = {}  # key -> AsyncWriter
        self.writer_tasks: Dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()
    
    async def get_writer(self, venue: str, symbol: str, channel: str) -> AsyncWriter:
        """Get or create a writer for venue/symbol/channel."""
        key = self.rotator.get_file_key(venue, symbol, channel)
        
        async with self.lock:
            if key not in self.writers:
                writer = AsyncWriter(venue, symbol, channel, self.rotator)
                self.writers[key] = writer
                
                # Start background writer task
                task = asyncio.create_task(writer.writer_task())
                self.writer_tasks[key] = task
            
            return self.writers[key]
    
    async def write_record(self, venue: str, symbol: str, channel: str, record: dict) -> None:
        """Enqueue a record for writing."""
        writer = await self.get_writer(venue, symbol, channel)
        await writer.enqueue(record)
    
    async def shutdown(self) -> None:
        """Shutdown all writers → flush → close files (no compression)."""
        logger.info("Shutting down storage manager...")
        
        # 1. Signal all writers to stop accepting new records
        for writer in self.writers.values():
            await writer.shutdown()
        
        # 2. Wait for writer tasks to drain queues (with hard timeout)
        if self.writer_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.writer_tasks.values(),
                                   return_exceptions=True),
                    timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Writer tasks did not finish in 10s – cancelling")
                for t in self.writer_tasks.values():
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*self.writer_tasks.values(),
                                     return_exceptions=True)
        
        # 3. Close file handles (compression skipped during shutdown)
        await self.rotator.close_all()
        logger.info("Storage manager shutdown complete")
    
    def get_write_counts(self) -> Dict[str, int]:
        """Get write counts for all writers (for monitoring)."""
        return {key: writer.write_count for key, writer in self.writers.items()}
