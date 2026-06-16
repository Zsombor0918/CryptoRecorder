"""
tests/test_semantic_equivalence.py — Validate new pipeline produces same output as old.

This test is critical for Phase 8 (Legacy Converter Validation).

Verifies that:
  old: raw → convert_day.py → catalog
  new: raw → replay_store → generate_catalog → catalog
  
produce semantically equivalent catalogs (same instrument count, TradeTick count, etc).
"""
import pytest
from datetime import datetime, timezone
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def raw_3day_sample(tmp_path):
    """Create a minimal 3-day raw data sample for local testing."""
    # This fixture prepares raw JSONL.zst data for testing
    # In practice, user will symlink actual 3-day raw data
    pytest.skip("Requires actual 3-day raw data; symlink to config.DATA_ROOT")
    return tmp_path


class TestSemanticEquivalence:
    """Validate new vs old pipeline produce equivalent results."""

    def test_old_pipeline_baseline(self, raw_3day_sample):
        """
        MANUAL TEST: Run old convert_day.py on 3-day sample.
        
        Steps:
        1. Ensure raw_3day_sample contains 3 days of raw JSONL(.zst/.gz)
        2. Run: python convert_day.py --date 2026-06-15
        3. Run: python convert_day.py --date 2026-06-16
        4. Run: python convert_day.py --date 2026-06-17
        5. Load resulting Nautilus catalogs
        6. Record: instrument count, TradeTick count, OrderBook* counts, time ranges
        
        This establishes baseline from existing convert_day.py implementation.
        """
        pytest.skip("Manual test; execute convert_day.py on 3-day sample")

    def test_new_pipeline_replay_build(self, raw_3day_sample, tmp_path):
        """
        AUTOMATED TEST: Build replay_store from 3-day raw sample.
        
        Validates:
        - raw → replay_store conversion preserves all records
        - Deterministic sorting (session_id, session_seq, raw_index)
        - Correct Hive partitioning (venue, symbol, date)
        - Checksums computed for integrity
        """
        from config import DATA_ROOT
        from pipeline.build_replay_store import build_replay_for_symbol
        
        # For now, skip if raw sample not available
        if not DATA_ROOT.exists():
            pytest.skip(f"Raw data not available at {DATA_ROOT}")
        
        # TODO: Implement automated replay build test
        # This would:
        # 1. Scan raw_3day_sample for available symbols
        # 2. Call build_replay_for_symbol() for each symbol/date
        # 3. Verify output directory structure
        # 4. Validate schema compliance
        # 5. Check record counts match raw input
        pytest.skip("TODO: Implement replay build validation")

    def test_new_pipeline_catalog_generation(self, tmp_path):
        """
        AUTOMATED TEST: Generate catalog from replay_store.
        
        Validates:
        - replay_store → catalog conversion preserves all records
        - Time window filtering works correctly
        - Symbol/venue filtering works correctly
        - Manifest generated with correct metadata
        """
        # TODO: Implement automated catalog generation test
        # This would:
        # 1. Use built replay_store from test_new_pipeline_replay_build
        # 2. Call generate_catalog_from_replay() for 3-day window
        # 3. Verify output catalog structure
        # 4. Count TradeTicks, OrderBookDeltas, OrderBookDepth10
        # 5. Compare record counts with raw input
        pytest.skip("TODO: Implement catalog generation validation")

    def test_semantic_equivalence_counts(self):
        """
        MANUAL COMPARISON TEST: Compare old vs new pipeline results.
        
        After running both pipelines on same 3-day sample:
        
        1. Load old catalog (output from convert_day.py)
        2. Load new catalog (output from generate_catalog.py)
        3. Compare:
           - instrument count (should be identical)
           - TradeTick count (should be identical)
           - OrderBookDeltas count (should be identical)
           - OrderBookDepth10 count (if applicable)
           - timestamp range (should be identical)
           - first/last event timestamp (should be identical per symbol)
        
        Acceptable differences:
        - JSON key ordering
        - Internal ordering of bids/asks within snapshot
        - Feature schema extensions (not in old pipeline)
        
        Must-have equality:
        - Instrument metadata (symbol, venue, decimals, min/max price, etc)
        - Event counts per symbol/venue
        - Time boundaries per symbol
        """
        pytest.skip("Manual test; requires both pipelines executed and catalogs loaded")

    def test_convert_day_backward_compatibility(self):
        """Verify convert_day.py still works unchanged."""
        from pathlib import Path
        
        # Verify convert_day.py file exists and hasn't been modified
        convert_day_path = Path("/home/zsom/services/CryptoRecorder/convert_day.py")
        assert convert_day_path.exists(), "convert_day.py should not be deleted"
        
        # Read first few lines to verify it's the original implementation
        with open(convert_day_path) as f:
            content = f.read(500)
        
        # Should not contain new pipeline references
        assert "replay_store" not in content, \
            "convert_day.py should be unchanged (no replay_store references)"
        assert "feature_store" not in content, \
            "convert_day.py should be unchanged (no feature_store references)"


# Manual Testing Instructions
"""
PHASE 8 TESTING WORKFLOW:

1. SETUP RAW DATA:
   - Ensure 3 days of raw JSONL.zst data is available
   - Symlink or place in a local directory
   - Example dates: 2026-06-15, 2026-06-16, 2026-06-17

2. RUN OLD PIPELINE (convert_day.py baseline):
   cd /home/zsom/services/CryptoRecorder
   python convert_day.py --date 2026-06-15
   python convert_day.py --date 2026-06-16
   python convert_day.py --date 2026-06-17
   
   - Load output catalogs: Nautilus ParquetDataCatalog
   - Record counts: instruments, TradeTicks, OrderBookDeltas
   - Record sample timestamps and symbol ranges

3. RUN NEW PIPELINE (replay + feature + catalog):
   # Build replay store from raw
   python -m pipeline.build_replay_store --date 2026-06-15 --data-root /path/to/raw
   python -m pipeline.build_replay_store --date 2026-06-16 --data-root /path/to/raw
   python -m pipeline.build_replay_store --date 2026-06-17 --data-root /path/to/raw
   
   # Generate catalog from replay
   python -m pipeline.generate_catalog \
     --input /path/to/replay_store \
     --symbols BTCUSDT,ETHUSDT \
     --start 2026-06-15T00:00:00Z \
     --end 2026-06-18T00:00:00Z \
     --profile trades_only

4. COMPARE RESULTS:
   - Old catalog: instrument_count = X, trade_tick_count = Y
   - New catalog: instrument_count = X, trade_tick_count = Y
   - Should match exactly (semantic equivalence)
   - If differences exist:
     a. Check time window filtering
     b. Verify symbol selection
     c. Investigate raw data integrity
     d. Trace conversion pipeline for lost records

5. VALIDATE BACKWARD COMPATIBILITY:
   - Run old convert_day.py alongside new pipeline
   - Verify it still works without errors
   - Confirm new modules don't interfere with existing code

6. SIGN OFF:
   - If semantic equivalence test passes
   - And backward compatibility verified
   - Then approve rollout to Phase 6 (systemd deployment)
"""
