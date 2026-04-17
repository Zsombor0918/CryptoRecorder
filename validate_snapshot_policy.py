#!/usr/bin/env python3
"""
Snapshot policy validator - determine if snapshot strategy will avoid bans.

Binance Rate Limits (Spot WebSocket):
- Weight system: most endpoint 1 weight
- 6000 weight limit per minute
- /api/v3/depth endpoint: costs 1-5 weight depending on limit parameter
"""

import json
import logging
from pathlib import Path
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnapshotPolicyValidator:
    """Validate snapshot strategy safety."""
    
    # Binance rate limits
    BINANCE_WEIGHT_LIMIT_PER_MIN = 6000
    DEPTH_REQUEST_WEIGHT = 5  # assumes worst case (limit=50)
    SAFE_THRESHOLD_PERCENT = 50  # use only 50% of limit
    
    def __init__(self):
        """Initialize validator."""
        self.project_root = Path(__file__).parent
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load config from config.py."""
        try:
            import sys
            sys.path.insert(0, str(self.project_root))
            import config
            return {
                'TOP_SYMBOLS': config.TOP_SYMBOLS,
                'SNAPSHOT_INTERVAL_SEC': config.SNAPSHOT_INTERVAL_SEC,
                'SNAPSHOT_MODE': getattr(config, 'SNAPSHOT_MODE', 'periodic'),
            }
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}
    
    def check_periodic_snapshot_safety(self) -> Dict:
        """Check if periodic snapshot strategy is safe."""
        symbols = self.config.get('TOP_SYMBOLS', 50)
        interval = self.config.get('SNAPSHOT_INTERVAL_SEC', 600)
        
        # Estimate requests per minute
        requests_per_interval = symbols
        minutes_per_interval = interval / 60
        requests_per_minute = requests_per_interval / minutes_per_interval
        
        # Estimate weight per minute
        weight_per_minute = requests_per_minute * self.DEPTH_REQUEST_WEIGHT
        
        # Safe limit
        safe_limit = self.BINANCE_WEIGHT_LIMIT_PER_MIN * (self.SAFE_THRESHOLD_PERCENT / 100)
        
        is_safe = weight_per_minute <= safe_limit
        usage_percent = (weight_per_minute / safe_limit) * 100
        
        return {
            'strategy': 'periodic',
            'symbols': symbols,
            'interval_sec': interval,
            'requests_per_minute': requests_per_minute,
            'weight_per_minute': weight_per_minute,
            'safe_limit': safe_limit,
            'usage_percent': usage_percent,
            'is_safe': is_safe,
            'recommendation': (
                'SAFE' if is_safe else 
                'UNSAFE - reduce TOP_SYMBOLS or increase SNAPSHOT_INTERVAL_SEC or use SNAPSHOT_MODE=on_gap_only'
            )
        }
    
    def check_gap_only_safety(self) -> Dict:
        """Check if gap-only snapshot strategy is safe."""
        return {
            'strategy': 'on_gap_only',
            'weight_per_minute': 0,  # Only on gaps
            'safe_limit': self.BINANCE_WEIGHT_LIMIT_PER_MIN * (self.SAFE_THRESHOLD_PERCENT / 100),
            'is_safe': True,
            'recommendation': 'SAFE - snapshots only on data gaps, minimal REST calls'
        }
    
    def generate_report(self) -> Dict:
        """Generate full snapshot policy report."""
        report = {
            'timestamp': __import__('datetime').datetime.now().isoformat(),
            'binance_limits': {
                'weight_limit_per_minute': self.BINANCE_WEIGHT_LIMIT_PER_MIN,
                'safe_threshold_percent': self.SAFE_THRESHOLD_PERCENT,
                'safe_limit': self.BINANCE_WEIGHT_LIMIT_PER_MIN * (self.SAFE_THRESHOLD_PERCENT / 100),
            },
            'current_config': self.config,
            'strategies': {
                'periodic': self.check_periodic_snapshot_safety(),
                'gap_only': self.check_gap_only_safety(),
            }
        }
        
        # Determine recommendation
        current_mode = self.config.get('SNAPSHOT_MODE', 'periodic')
        if current_mode == 'periodic':
            periodic_safe = report['strategies']['periodic']['is_safe']
            if not periodic_safe:
                report['critical_recommendation'] = (
                    f"⚠️  CRITICAL: Current periodic snapshot strategy is UNSAFE and will cause 429/418 bans.\n"
                    f"   Immediately change to: SNAPSHOT_MODE='on_gap_only' or reduce TOP_SYMBOLS"
                )
        
        return report
    
    def print_report(self, report: Dict):
        """Print formatted report."""
        print("\n" + "=" * 70)
        print("SNAPSHOT POLICY VALIDATION")
        print("=" * 70)
        
        print(f"\nBinance Rate Limits:")
        print(f"  Weight limit:     {report['binance_limits']['weight_limit_per_minute']}/min")
        print(f"  Safe threshold:   {report['binance_limits']['safe_threshold_percent']}% = {report['binance_limits']['safe_limit']:.0f} weight/min")
        
        print(f"\nCurrent Configuration:")
        for key, value in report['current_config'].items():
            print(f"  {key}: {value}")
        
        print(f"\nPeriodic Strategy (every {report['current_config'].get('SNAPSHOT_INTERVAL_SEC', '?')}s):")
        periodic = report['strategies']['periodic']
        print(f"  Symbols:          {periodic['symbols']}")
        print(f"  Requests/min:     {periodic['requests_per_minute']:.1f}")
        print(f"  Weight/min:       {periodic['weight_per_minute']:.0f}")
        print(f"  Safe limit:       {periodic['safe_limit']:.0f}")
        print(f"  Usage:            {periodic['usage_percent']:.1f}%")
        print(f"  Status:           {'✓ SAFE' if periodic['is_safe'] else '✗ UNSAFE'}")
        print(f"  Recommendation:   {periodic['recommendation']}")
        
        print(f"\nGap-Only Strategy:")
        gap = report['strategies']['gap_only']
        print(f"  Weight/min:       {gap['weight_per_minute']:.0f} (minimal)")
        print(f"  Status:           ✓ SAFE")
        print(f"  Recommendation:   {gap['recommendation']}")
        
        if 'critical_recommendation' in report:
            print(f"\n⚠️  CRITICAL:")
            print(f"  {report['critical_recommendation']}")
        
        print("\n" + "=" * 70 + "\n")


def main():
    """Main entry point."""
    validator = SnapshotPolicyValidator()
    report = validator.generate_report()
    
    # Save report
    report_file = validator.project_root / "state" / "validation" / "snapshot_policy.json"
    report_file.parent.mkdir(parents=True, exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Report saved: {report_file}")
    
    # Print formatted report
    validator.print_report(report)


if __name__ == '__main__':
    main()
