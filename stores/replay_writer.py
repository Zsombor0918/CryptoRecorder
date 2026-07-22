"""
stores.replay_writer — Memory-bounded deterministic Parquet writing for replay_store.

Uses disk-backed SQLite spools (via converter.spool.RawRecordSpool) to avoid
retaining a full symbol/day in Python lists.  Records are written to Parquet
incrementally in bounded batches using pyarrow.parquet.ParquetWriter so that peak
RSS remains independent of the total record count.

Ordering contracts (unchanged from v0):
    depth:  (stream_session_id, session_seq, raw_index)
    trades: (trade_stream_session_id, trade_session_seq, raw_index)

Output format (unchanged from v0):
    replay_store/
      venue=<VENUE>/
        symbol=<SYMBOL>/
          date=<DATE>/
            depth.parquet
            trades.parquet
            instrument.json   (optional)
            manifest.json
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from converter.spool import RawRecordSpool
from .replay_schema import DEPTH_REPLAY_SCHEMA, TRADE_REPLAY_SCHEMA, MANIFEST_SCHEMA

logger = logging.getLogger(__name__)

# Number of spool rows read per Parquet row-group.
_DEFAULT_PARQUET_BATCH = int(
    os.environ.get("CRYPTO_RECORDER_REPLAY_PARQUET_BATCH", "5000")
)


def _compute_sha256(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(65536), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def validate_partition(partition_dir: Path) -> bool:
    """Return True only if partition_dir is a complete, checksum-valid replay partition.

    This is the single source of truth for partition validity, shared by
    ReplayWriter.publish() (post-publication validation) and
    pipeline.build_replay_store (skip-if-valid / crash-recovery checks) so
    that both call sites can never disagree about what "valid" means.
    """
    manifest_path = partition_dir / "manifest.json"
    depth_path = partition_dir / "depth.parquet"
    trades_path = partition_dir / "trades.parquet"
    if not (manifest_path.exists() and depth_path.exists() and trades_path.exists()):
        return False
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        if manifest.get("status") != "complete":
            return False
        for key, path in (("depth_checksum", depth_path), ("trades_checksum", trades_path)):
            expected = manifest.get(key)
            if not expected:
                return False
            if _compute_sha256(path) != expected:
                logger.warning(f"Checksum mismatch for {partition_dir} ({key})")
                return False
        return True
    except Exception as e:
        logger.warning(f"Partition validation failed for {partition_dir}: {e}")
        return False


def _write_channel_incremental(
    spool: RawRecordSpool,
    out_path: Path,
    schema: pa.Schema,
    parquet_batch_size: int,
) -> "tuple[int, int, int]":
    """Write all spool records to out_path in bounded batches.

    Returns (record_count, ts_min_ns, ts_max_ns).
    An empty channel produces a schema-bearing empty Parquet file.
    """
    record_count = 0
    ts_min: "int | None" = None
    ts_max: "int | None" = None
    writer: "pq.ParquetWriter | None" = None

    try:
        batch: list = []
        for record in spool.iter_records():
            batch.append(record)
            ts = int(record.get("ts_exchange_ns") or 0)
            if ts_min is None or ts < ts_min:
                ts_min = ts
            if ts_max is None or ts > ts_max:
                ts_max = ts
            record_count += 1
            if len(batch) >= parquet_batch_size:
                tbl = pa.Table.from_pylist(batch, schema=schema)
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(out_path),
                        schema=schema,
                        compression="zstd",
                        compression_level=3,
                    )
                writer.write_table(tbl)
                del tbl, batch
                batch = []

        # Flush remainder (or write empty schema-bearing file)
        tbl = pa.Table.from_pylist(batch, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(
                str(out_path),
                schema=schema,
                compression="zstd",
                compression_level=3,
            )
        writer.write_table(tbl)
        del tbl, batch
    finally:
        if writer is not None:
            writer.close()

    return record_count, (ts_min or 0), (ts_max or 0)


class ReplayWriter:
    """
    Memory-bounded writer for a single venue/symbol/date replay partition.

    Records are spooled to SQLite on disk immediately and read back in
    bounded batches during finalization so that peak RSS is proportional to
    parquet_batch_size, not to the total record count.

    Output layout (Hive-style, unchanged from v0):
        replay_store/
          venue=BINANCE_SPOT/
            symbol=BTCUSDT/
              date=2026-06-15/
                depth.parquet
                trades.parquet
                instrument.json
                manifest.json
    """

    def __init__(
        self,
        replay_root: Path,
        venue: str,
        symbol: str,
        date: str,
        *,
        parquet_batch_size: int = _DEFAULT_PARQUET_BATCH,
    ):
        self.replay_root = Path(replay_root)
        self.venue = venue
        self.symbol = symbol
        self.date = date
        self._parquet_batch_size = parquet_batch_size

        self.output_dir = (
            self.replay_root
            / f"venue={venue}"
            / f"symbol={symbol}"
            / f"date={date}"
        )
        self.staging_dir = self.output_dir.parent / f".staging_{date}_{symbol}"

        # Counters (compatible with existing callers)
        self.depth_count = 0
        self.trade_count = 0
        self._manifest: "dict[str, Any] | None" = None

        # Spool files live inside the staging directory so that a stale-staging
        # cleanup (shutil.rmtree on .staging_*) also removes orphaned SQLite
        # spools even when a previous SIGKILL/OOM prevented normal cleanup.
        self._spool_scratch_dir = self.staging_dir / "scratch"
        self._spool_scratch_dir.mkdir(parents=True, exist_ok=True)

        self._depth_spool: "RawRecordSpool | None" = None
        self._trade_spool: "RawRecordSpool | None" = None

    # ------------------------------------------------------------------
    # Lazy spool init
    # ------------------------------------------------------------------

    def _get_depth_spool(self) -> RawRecordSpool:
        if self._depth_spool is None:
            self._depth_spool = RawRecordSpool(
                temp_dir=self._spool_scratch_dir,
                prefix="replay-depth-",
            )
        return self._depth_spool

    def _get_trade_spool(self) -> RawRecordSpool:
        if self._trade_spool is None:
            self._trade_spool = RawRecordSpool(
                temp_dir=self._spool_scratch_dir,
                prefix="replay-trade-",
            )
        return self._trade_spool

    # ------------------------------------------------------------------
    # Public write API (compatible signatures with v0)
    # ------------------------------------------------------------------

    def write_depth_batch(self, records: list) -> None:
        """Spool depth records to disk (O(batch) memory, not O(day))."""
        spool = self._get_depth_spool()
        for r in records:
            sort_key = (
                int(r.get("stream_session_id", 0)),
                int(r.get("session_seq", 0)),
                int(r.get("raw_index", 0)),
            )
            spool.insert(r, sort_key=sort_key, raw_index=int(r.get("raw_index", 0)))
        self.depth_count += len(records)

    def write_trades_batch(self, records: list) -> None:
        """Spool trade records to disk (O(batch) memory, not O(day))."""
        spool = self._get_trade_spool()
        for r in records:
            sort_key = (
                int(r.get("trade_stream_session_id", 0)),
                int(r.get("trade_session_seq", 0)),
                int(r.get("raw_index", 0)),
            )
            spool.insert(r, sort_key=sort_key, raw_index=int(r.get("raw_index", 0)))
        self.trade_count += len(records)

    # ------------------------------------------------------------------
    # Finalization (incremental Parquet, no full-day lists)
    # ------------------------------------------------------------------

    def finalize_staging(self) -> "dict[str, Any]":
        """
        Write spooled records to staging Parquet files using bounded batches.

        The depth channel is fully written and its Parquet writer closed before
        the trade channel begins, so both channels are never simultaneously in
        memory.

        Returns:
            Manifest dict with counts, checksums, and timestamp range.
        """
        depth_path = self.staging_dir / "depth.parquet"
        trades_path = self.staging_dir / "trades.parquet"

        # --- Depth ---
        depth_spool = self._get_depth_spool()
        depth_spool.commit()
        depth_count, ts_d_min, ts_d_max = _write_channel_incremental(
            depth_spool, depth_path, DEPTH_REPLAY_SCHEMA, self._parquet_batch_size
        )
        depth_spool.close()
        self._depth_spool = None
        logger.info(f"Wrote staging depth: {depth_path} ({depth_count} records)")

        # --- Trades ---
        trade_spool = self._get_trade_spool()
        trade_spool.commit()
        trade_count, ts_t_min, ts_t_max = _write_channel_incremental(
            trade_spool, trades_path, TRADE_REPLAY_SCHEMA, self._parquet_batch_size
        )
        trade_spool.close()
        self._trade_spool = None
        logger.info(f"Wrote staging trades: {trades_path} ({trade_count} records)")

        # Sync authoritative counts
        self.depth_count = depth_count
        self.trade_count = trade_count

        nonzero_ts = [t for t in (ts_d_min, ts_d_max, ts_t_min, ts_t_max) if t != 0]
        ts_min = min(nonzero_ts) if nonzero_ts else 0
        ts_max = max(nonzero_ts) if nonzero_ts else 0

        depth_checksum = _compute_sha256(depth_path)
        trades_checksum = _compute_sha256(trades_path)

        # Remove the scratch directory — spools have been closed and deleted,
        # so it should now be empty.  An empty scratch/ must NOT appear in the
        # final published partition, and we must not publish while scratch
        # artifacts remain.
        if self._spool_scratch_dir.exists():
            # Verify that spool files are gone (they should be, given close() above)
            remaining = list(self._spool_scratch_dir.iterdir())
            if remaining:
                raise RuntimeError(
                    f"scratch dir {self._spool_scratch_dir} still contains "
                    f"files after spool close: {remaining}. "
                    "Refusing to publish — manual inspection required."
                )
            # Directory is empty; remove it.
            try:
                self._spool_scratch_dir.rmdir()
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot remove scratch dir {self._spool_scratch_dir}: {exc}. "
                    "Refusing to publish."
                ) from exc
            if self._spool_scratch_dir.exists():
                raise RuntimeError(
                    f"scratch dir {self._spool_scratch_dir} still exists after rmdir. "
                    "Refusing to publish."
                )

        manifest: dict = {
            "venue": self.venue,
            "symbol": self.symbol,
            "date": self.date,
            "status": "complete",
            "depth_record_count": depth_count,
            "trade_record_count": trade_count,
            "ts_range_start_ns": ts_min,
            "ts_range_end_ns": ts_max,
            "depth_checksum": depth_checksum,
            "trades_checksum": trades_checksum,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": [],
        }
        self._manifest = manifest
        return manifest

    def publish(self, instrument_metadata: "Optional[dict]" = None) -> Path:
        """
        Atomically move staging to final output directory.

        A valid previously-published partition is overwritten only after
        staging is confirmed complete (manifest written).  An exception before
        os.replace keeps the existing published output intact.
        """
        if instrument_metadata:
            instrument_path = self.staging_dir / "instrument.json"
            with open(instrument_path, "w") as f:
                json.dump(instrument_metadata, f, indent=2)
            logger.info(f"Wrote instrument metadata: {instrument_path}")

        manifest = self._manifest
        if manifest is None:
            manifest = self.finalize_staging()

        manifest_path = self.staging_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Wrote manifest: {manifest_path}")

        # Atomic publication with backup/restore so the existing valid partition
        # is never lost if the replacement fails (I/O error, permissions, etc.).
        backup_dir = self.output_dir.parent / f".backup_{self.date}_{self.symbol}"
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)

        # Step 1: rename existing output to backup (if it exists).
        if self.output_dir.exists():
            if backup_dir.exists():
                shutil.rmtree(backup_dir)
            os.replace(self.output_dir, backup_dir)

        try:
            # Step 2: rename staging to canonical output.
            os.replace(self.staging_dir, self.output_dir)
        except Exception:
            # Canonical publication failed — restore backup so the last-known-
            # good partition is not lost.
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    logger.warning(
                        f"Publication failed; restored previous partition: {self.output_dir}"
                    )
                except Exception as restore_err:
                    logger.error(
                        f"Could not restore backup partition {backup_dir}: {restore_err}"
                    )
            raise

        # Step 3: canonical publication succeeded on the filesystem — but that
        # does not guarantee the published directory is a complete, valid
        # partition (missing/corrupt output under a non-standard filesystem
        # replace, truncated write, etc.). Validate before ever deleting the
        # last known-good backup.
        if not validate_partition(self.output_dir):
            logger.error(
                f"Post-publish validation failed for {self.output_dir}: "
                "missing/corrupt manifest, parquet files, or checksum mismatch. "
                "Quarantining invalid output and restoring backup if available."
            )
            quarantine_dir = self.output_dir.parent / f".quarantine_{self.date}_{self.symbol}"
            try:
                if quarantine_dir.exists():
                    shutil.rmtree(quarantine_dir)
                if self.output_dir.exists():
                    os.replace(self.output_dir, quarantine_dir)
            except Exception as qe:
                logger.error(
                    f"Could not quarantine invalid output {self.output_dir}: {qe}"
                )
            if backup_dir.exists() and not self.output_dir.exists():
                try:
                    os.replace(backup_dir, self.output_dir)
                    logger.warning(
                        f"Restored previous valid partition after invalid publish: "
                        f"{self.output_dir}"
                    )
                except Exception as restore_err:
                    logger.error(
                        f"Could not restore backup partition {backup_dir} after "
                        f"invalid publish: {restore_err}"
                    )
            raise RuntimeError(
                f"Post-publish validation failed for {self.output_dir}; refusing "
                "to report success. Previous valid partition preserved/restored "
                "where possible; invalid output quarantined at "
                f"{quarantine_dir}."
            )

        # Step 4: new canonical partition is valid — delete the obsolete
        # backup on a best-effort basis. A failure here must NOT cause
        # publish() to raise or the build to fail, since the new partition is
        # already confirmed valid.
        if backup_dir.exists():
            try:
                shutil.rmtree(backup_dir)
            except Exception as backup_del_err:
                logger.warning(
                    f"Could not delete obsolete backup {backup_dir}: {backup_del_err}. "
                    "Leaving for startup cleanup; publication is still successful."
                )

        logger.info(f"Published replay data to: {self.output_dir}")
        return self.output_dir

    def cleanup_staging(self) -> None:
        """Remove staging directory and close/delete open spool files.

        Safe to call on error paths or after successful publish.
        """
        for spool_attr in ("_depth_spool", "_trade_spool"):
            spool = getattr(self, spool_attr)
            if spool is not None:
                try:
                    spool.close()
                except Exception:
                    pass
                setattr(self, spool_attr, None)
        if self.staging_dir.exists():
            try:
                shutil.rmtree(self.staging_dir)
            except Exception as exc:
                logger.error(f"Could not remove staging dir {self.staging_dir}: {exc}")
                raise RuntimeError(
                    f"cleanup_staging failed: staging dir {self.staging_dir} still exists"
                ) from exc
            if self.staging_dir.exists():
                raise RuntimeError(
                    f"cleanup_staging failed: staging dir {self.staging_dir} was not removed"
                )
