#!/usr/bin/env python
"""
Disk usage monitoring and estimation module.

Tracks:
  - Total size of data_raw/
  - Total size of catalog/ (Nautilus Parquet)
  - Total size of meta/ and state/
  - 24h rolling growth rate
  - Days to full at current growth rate
"""
import asyncio
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from collections import deque

logger = logging.getLogger(__name__)


class DiskMonitor:
    """Monitor disk usage and growth."""
    
    def __init__(self, config):
        """
        Args:
            config: Config module with paths and disk limits
        """
        self.config = config
        self.data_root = config.DATA_ROOT
        self.meta_root = config.META_ROOT
        self.state_root = config.STATE_ROOT
        self.catalog_root = config.NAUTILUS_CATALOG_ROOT
        
        # Thresholds
        self.soft_limit_gb = config.DISK_SOFT_LIMIT_GB
        self.hard_limit_gb = config.DISK_HARD_LIMIT_GB
        self.cleanup_target_gb = config.DISK_CLEANUP_TARGET_GB
        
        # Usage history (last 288 samples = 48 hours at 10min interval)
        self.usage_history = deque(maxlen=288)
        
        # State directory
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.usage_log_file = self.state_root / "disk_usage.json"
    
    def get_dir_size_mb(self, path: Path) -> float:
        """Get directory size in MB using du."""
        if not path.exists():
            return 0.0
        
        try:
            result = subprocess.run(
                ['du', '-sb', str(path)],
                capture_output=True,
                timeout=30,
                text=True
            )
            
            if result.returncode == 0:
                size_bytes = int(result.stdout.split()[0])
                return size_bytes / (1024 * 1024)
            return 0.0
        except Exception as e:
            logger.warning(f"Could not get size of {path}: {e}")
            return 0.0
    
    def get_dir_size_gb(self, path: Path) -> float:
        """Get directory size in GB."""
        return self.get_dir_size_mb(path) / 1024
    
    async def check_disk_usage(self) -> Dict:
        """
        Check total disk usage.
        
        Returns:
            Dict with usage breakdown
        """
        # Get sizes using executor to avoid blocking
        loop = asyncio.get_event_loop()
        
        data_raw_gb = await loop.run_in_executor(
            None,
            self.get_dir_size_gb,
            self.data_root
        )
        catalog_gb = await loop.run_in_executor(
            None,
            self.get_dir_size_gb,
            self.catalog_root
        )
        meta_gb = await loop.run_in_executor(
            None,
            self.get_dir_size_gb,
            self.meta_root
        )
        state_gb = await loop.run_in_executor(
            None,
            self.get_dir_size_gb,
            self.state_root
        )
        
        total_gb = data_raw_gb + catalog_gb + meta_gb + state_gb
        
        usage = {
            "timestamp": datetime.now().isoformat(),
            "data_raw_gb": round(data_raw_gb, 2),
            "catalog_gb": round(catalog_gb, 2),
            "meta_gb": round(meta_gb, 2),
            "state_gb": round(state_gb, 2),
            "total_gb": round(total_gb, 2),
            "percent_of_soft_limit": round(total_gb / self.soft_limit_gb * 100, 1),
            "percent_of_hard_limit": round(total_gb / self.hard_limit_gb * 100, 1),
        }
        
        # Add to history
        self.usage_history.append(usage)
        
        # Log alerts
        if usage["total_gb"] >= self.hard_limit_gb:
            logger.critical(
                f"DISK CRITICAL: {usage['total_gb']}GB >= "
                f"{self.hard_limit_gb}GB hard limit!"
            )
        elif usage["total_gb"] >= self.soft_limit_gb:
            logger.warning(
                f"DISK WARNING: {usage['total_gb']}GB >= "
                f"{self.soft_limit_gb}GB soft limit"
            )
        
        return usage
    
    def get_growth_rate(self) -> Optional[float]:
        """
        Calculate 24h growth rate in GB/day.
        
        Returns:
            GB/day or None if insufficient history
        """
        if len(self.usage_history) < 2:
            return None
        
        # Need at least 144 samples (24 hours at 10min interval)
        if len(self.usage_history) < 144:
            return None
        
        # Calculate growth from oldest to newest
        oldest = self.usage_history[0]
        newest = self.usage_history[-1]
        
        time_diff_hours = 24  # Fixed for 24h estimate
        size_diff_gb = newest["total_gb"] - oldest["total_gb"]
        
        if size_diff_gb < 0:
            # Cleanup happened, use positive growth
            return 0.0
        
        return size_diff_gb / time_diff_hours * 24  # Convert to per-day
    
    def get_days_to_full(self, growth_rate_gb_day: float) -> Optional[float]:
        """
        Estimate days to reach hard limit at current growth rate.
        
        Args:
            growth_rate_gb_day: GB/day
            
        Returns:
            Days to full or None if unknown
        """
        if len(self.usage_history) == 0:
            return None
        
        if growth_rate_gb_day <= 0:
            return None
        
        current_gb = self.usage_history[-1]["total_gb"]
        available_gb = self.hard_limit_gb - current_gb
        
        return available_gb / growth_rate_gb_day
    
    async def write_usage_report(self, usage: Dict) -> None:
        """Write usage to disk for monitoring."""
        try:
            growth_rate = self.get_growth_rate()
            days_to_full = self.get_days_to_full(growth_rate) if growth_rate else None
            
            report = {
                **usage,
                "growth_rate_gb_day": round(growth_rate, 2) if growth_rate else None,
                "days_to_full": round(days_to_full, 1) if days_to_full else None,
                "alert": None
            }
            
            # Add alert if needed
            if usage["total_gb"] >= self.hard_limit_gb:
                report["alert"] = "CRITICAL: Hard limit exceeded"
            elif usage["total_gb"] >= self.soft_limit_gb * 0.95:
                report["alert"] = "WARNING: Approaching soft limit"
            elif days_to_full and days_to_full < 7:
                report["alert"] = f"WARNING: Full in {days_to_full:.1f} days"
            
            with open(self.usage_log_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.debug(f"Disk usage report: {usage['total_gb']}GB ({usage['percent_of_soft_limit']}% of soft limit)")
        except Exception as e:
            logger.error(f"Error writing usage report: {e}")
    
    async def get_oldest_date_dir(self) -> Optional[Path]:
        """Get oldest date directory in data_raw/."""
        try:
            venue_dirs = list(self.data_root.glob("*/"))
            date_dirs = set()
            
            for venue_dir in venue_dirs:
                channel_dirs = list(venue_dir.glob("*/"))
                for channel_dir in channel_dirs:
                    symbol_dirs = list(channel_dir.glob("*/"))
                    for symbol_dir in symbol_dirs:
                        date_subdirs = [
                            d for d in symbol_dir.glob("*/")
                            if d.is_dir() and len(d.name) == 10 and d.name[4] == '-'
                        ]
                        date_dirs.update(date_subdirs)
            
            if not date_dirs:
                return None
            
            # Return oldest date directory
            oldest = min(date_dirs, key=lambda x: x.name)
            return oldest.parent.parent.parent  # Go up to venue level
        except Exception as e:
            logger.warning(f"Could not find oldest date dir: {e}")
            return None
    
    async def cleanup_old_data(self) -> bool:
        """
        Delete oldest data directories if disk > soft limit.
        
        Returns:
            True if cleanup performed, False otherwise
        """
        usage = await self.check_disk_usage()
        
        if usage["total_gb"] <= self.soft_limit_gb:
            logger.debug("Disk usage within limits, no cleanup needed")
            return False
        
        logger.info(
            f"Disk usage {usage['total_gb']}GB > soft limit {self.soft_limit_gb}GB, "
            f"cleaning up oldest data..."
        )
        
        # Delete oldest date directories until we hit cleanup target
        deleted_count = 0
        max_attempts = 10
        
        while usage["total_gb"] > self.cleanup_target_gb and deleted_count < max_attempts:
            oldest_dir = await self.get_oldest_date_dir()
            
            if not oldest_dir:
                logger.warning("Could not find old directories to delete")
                break
            
            try:
                dir_size_gb = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.get_dir_size_gb,
                    oldest_dir
                )
                
                logger.info(f"Deleting {oldest_dir.name} ({dir_size_gb:.1f}GB)...")
                
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    shutil.rmtree,
                    oldest_dir
                )
                
                deleted_count += 1
                
                # Re-check usage
                usage = await self.check_disk_usage()
                logger.info(f"After cleanup: {usage['total_gb']}GB")
            except Exception as e:
                logger.error(f"Error deleting {oldest_dir}: {e}")
                break
        
        logger.info(
            f"Cleanup complete: deleted {deleted_count} date directories, "
            f"current size: {usage['total_gb']}GB"
        )
        
        return deleted_count > 0
    
    async def disk_check_task(self) -> None:
        """Background task for periodic disk checks."""
        from config import DISK_CHECK_INTERVAL_SEC
        
        logger.info("Disk monitor starting...")
        
        while True:
            try:
                usage = await self.check_disk_usage()
                await self.write_usage_report(usage)
                
                # Cleanup if needed
                if usage["total_gb"] > self.soft_limit_gb:
                    await self.cleanup_old_data()
                
            except Exception as e:
                logger.error(f"Error in disk monitor: {e}", exc_info=True)
            
            await asyncio.sleep(DISK_CHECK_INTERVAL_SEC)
    
    async def shutdown(self) -> None:
        """Shutdown disk monitor and save final state."""
        logger.info("Disk monitor shutting down...")
        try:
            # Final usage report
            usage = await self.check_disk_usage()
            await self.write_usage_report(usage)
            logger.info(f"Final disk usage: {usage['total_gb']:.1f}GB")
        except Exception as e:
            logger.error(f"Error during disk monitor shutdown: {e}")
        logger.info("Disk monitor shutdown complete")
