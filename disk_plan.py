#!/usr/bin/env python
"""
disk_plan.py - Generate recommended disk retention policy.

Based on measured growth rate, provides recommendations for:
  - Soft limit (warn threshold)
  - Cleanup target (minimum retained)
  - Raw retention days
  - Expected monthly/yearly capacity

Usage:
    python disk_plan.py [--days N]  # N days of historical data to analyze

Output:
    Prints recommended config settings
    Writes analysis to state/disk_plan_report.json
"""
import asyncio
import json
import logging
import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DiskPlanner:
    """Generate disk retention recommendations."""
    
    def __init__(self, capacity_gb: int = 500, analysis_days: int = 30):
        """
        Args:
            capacity_gb: Total disk capacity (500 GB)
            analysis_days: Days of historical data to analyze
        """
        self.capacity_gb = capacity_gb
        self.analysis_days = analysis_days
        self.state_root = Path(__file__).parent / "state"
        self.usage_log = self.state_root / "disk_usage.json"
    
    def get_measured_growth_rate(self) -> float:
        """
        Get measured growth rate from disk_usage.json.
        
        Returns:
            GB/day
        """
        if not self.usage_log.exists():
            logger.warning(f"No usage history at {self.usage_log}")
            return 10.0  # Default estimate: 10 GB/day
        
        try:
            with open(self.usage_log) as f:
                latest = json.load(f)
            
            growth = latest.get("growth_rate_gb_day")
            if growth is not None:
                return growth
        except Exception as e:
            logger.warning(f"Could not read usage history: {e}")
        
        return 10.0
    
    def generate_plan(self, measured_daily_gb: float = None) -> Dict:
        """
        Generate retention plan.
        
        Args:
            measured_daily_gb: Measured daily growth in GB (uses historical if None)
            
        Returns:
            Dict with recommendations
        """
        if measured_daily_gb is None:
            measured_daily_gb = self.get_measured_growth_rate()
        
        logger.info(f"Measured growth rate: {measured_daily_gb:.2f} GB/day")
        
        # Calculate metrics
        daily_gb = measured_daily_gb
        weekly_gb = daily_gb * 7
        monthly_gb = daily_gb * 30
        yearly_gb = daily_gb * 365
        
        # Conservative buffer: 4× weekly growth
        buffer_gb = weekly_gb * 4
        
        # Soft limit: 80% of capacity
        soft_limit_gb = int(self.capacity_gb * 0.80)
        
        # Hard limit: 95% of capacity (safety margin)
        hard_limit_gb = int(self.capacity_gb * 0.95)
        
        # Cleanup target: maintain 70% of capacity (leaves room for spikes)
        cleanup_target_gb = int(self.capacity_gb * 0.70)
        
        # Raw data retention: how many days can we keep?
        # Raw data = ~80% of total (catalog is ~20%)
        raw_data_capacity_gb = cleanup_target_gb * 0.80
        raw_retention_days = int(raw_data_capacity_gb / daily_gb)
        
        # Recommended retention window (conservative)
        recommended_retention_days = min(raw_retention_days, 60)  # Cap at 60 days
        
        plan = {
            "timestamp": datetime.now().isoformat(),
            "capacity_gb": self.capacity_gb,
            "measured_growth": {
                "daily_gb": round(daily_gb, 2),
                "weekly_gb": round(weekly_gb, 2),
                "monthly_gb": round(monthly_gb, 2),
                "yearly_gb": round(yearly_gb, 2),
            },
            "buffers_and_safety": {
                "weekly_growth_multiplier": 4,
                "buffer_gb": round(buffer_gb, 2),
                "recommended_min_free_gb": round(buffer_gb * 1.5, 2),
            },
            "recommended_config": {
                "DISK_SOFT_LIMIT_GB": soft_limit_gb,
                "DISK_HARD_LIMIT_GB": hard_limit_gb,
                "DISK_CLEANUP_TARGET_GB": cleanup_target_gb,
                "RAW_RETENTION_DAYS": recommended_retention_days,
            },
            "utilization_scenarios": {
                "at_soft_limit": {
                    "capacity_used_gb": soft_limit_gb,
                    "capacity_used_percent": 80,
                    "action": "Begin cleanup to cleanup_target"
                },
                "at_cleanup_target": {
                    "capacity_used_gb": cleanup_target_gb,
                    "capacity_used_percent": 70,
                    "action": "Safe operating point"
                },
                "at_hard_limit": {
                    "capacity_used_gb": hard_limit_gb,
                    "capacity_used_percent": 95,
                    "action": "Critical - emergency cleanup"
                }
            },
            "retention_analysis": {
                "raw_data_daily_gb": round(daily_gb * 0.80, 2),
                "catalog_daily_gb": round(daily_gb * 0.20, 2),
                "retention_at_cleanup_target": {
                    "days": recommended_retention_days,
                    "raw_data_gb_retained": round(daily_gb * 0.80 * recommended_retention_days, 2),
                    "catalog_retained": "Kept for ~90 days (not cleaned)"
                }
            },
            "recommendations": [
                f"Use measured growth rate of {daily_gb:.1f} GB/day",
                f"Set soft limit to {soft_limit_gb}GB (80% of {self.capacity_gb}GB)",
                f"Cleanup to {cleanup_target_gb}GB target (70% capacity)",
                f"Retain {recommended_retention_days} days of raw data",
                f"Expected monthly growth: {monthly_gb:.0f}GB",
                f"Expected yearly growth: {yearly_gb:.0f}GB",
                "Monitor disk_usage.json for actual growth trends",
                "Adjust retention settings if actual growth differs from measured",
            ]
        }
        
        return plan
    
    def validate_plan(self, plan: Dict) -> bool:
        """Validate that plan is sensible."""
        config = plan["recommended_config"]
        
        # Soft < Hard < Capacity
        if not (config["DISK_SOFT_LIMIT_GB"] < config["DISK_HARD_LIMIT_GB"] < self.capacity_gb):
            logger.error("Invalid limits: soft >= hard or hard >= capacity")
            return False
        
        # Cleanup target < soft
        if config["DISK_CLEANUP_TARGET_GB"] >= config["DISK_SOFT_LIMIT_GB"]:
            logger.error("Cleanup target must be less than soft limit")
            return False
        
        # Retention days makes sense
        if config["RAW_RETENTION_DAYS"] < 1:
            logger.error("Retention days must be >= 1")
            return False
        
        logger.info("Plan validation passed ✓")
        return True
    
    def print_plan(self, plan: Dict) -> None:
        """Pretty-print the plan."""
        print("\n" + "=" * 70)
        print("DISK RETENTION PLAN")
        print("=" * 70)
        
        print(f"\nCapacity: {self.capacity_gb}GB")
        print(f"Measured growth: {plan['measured_growth']['daily_gb']} GB/day")
        print(f"               {plan['measured_growth']['weekly_gb']} GB/week")
        print(f"               {plan['measured_growth']['monthly_gb']} GB/month")
        
        print("\n" + "-" * 70)
        print("RECOMMENDED CONFIG.PY SETTINGS:")
        print("-" * 70)
        
        for key, value in plan["recommended_config"].items():
            print(f"{key:<30} = {value}")
        
        print("\n" + "-" * 70)
        print("UTILIZATION POINTS:")
        print("-" * 70)
        
        for scenario, details in plan["utilization_scenarios"].items():
            print(f"\n{scenario.upper()}:")
            print(f"  Used: {details['capacity_used_gb']}GB ({details['capacity_used_percent']}%)")
            print(f"  Action: {details['action']}")
        
        print("\n" + "-" * 70)
        print("RETENTION ANALYSIS:")
        print("-" * 70)
        
        ra = plan["retention_analysis"]
        print(f"Raw data daily growth: {ra['raw_data_daily_gb']}GB")
        print(f"Catalog daily growth: {ra['catalog_daily_gb']}GB")
        print(f"At cleanup target ({plan['recommended_config']['DISK_CLEANUP_TARGET_GB']}GB):")
        print(f"  - {ra['retention_at_cleanup_target']['days']} days of raw data retained")
        print(f"  - {ra['retention_at_cleanup_target']['raw_data_gb_retained']}GB raw data")
        
        print("\n" + "-" * 70)
        print("RECOMMENDATIONS:")
        print("-" * 70)
        
        for i, rec in enumerate(plan["recommendations"], 1):
            print(f"{i}. {rec}")
        
        print("\n" + "=" * 70 + "\n")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate disk retention plan")
    parser.add_argument('--capacity', type=int, default=500, help='Disk capacity in GB')
    parser.add_argument('--days', type=int, default=30, help='Analysis period in days')
    parser.add_argument('--measured', type=float, help='Override measured growth (GB/day)')
    args = parser.parse_args()
    
    planner = DiskPlanner(capacity_gb=args.capacity, analysis_days=args.days)
    
    # Generate plan
    plan = planner.generate_plan(measured_daily_gb=args.measured)
    
    # Validate
    if not planner.validate_plan(plan):
        return 1
    
    # Print
    planner.print_plan(plan)
    
    # Save
    report_file = planner.state_root / "disk_plan_report.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(plan, f, indent=2)
    logger.info(f"Plan saved to {report_file}")
    
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
