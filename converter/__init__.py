"""
converter — Transform raw JSONL data → Nautilus ParquetDataCatalog.

Modules:
  readers       Streaming JSONL/zst/gz decompression
  instruments   Build Nautilus Instrument objects from exchangeInfo
  trades        Raw trade_v2 records → TradeTick
  depth_phase2  Deterministic depth_v2 replay → OrderBookDeltas / OrderBookDepth10
  catalog       Idempotent catalog writing & purge helpers
  universe      Universe resolution from meta/ or disk fallback
"""
