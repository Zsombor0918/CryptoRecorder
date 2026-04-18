"""
converter — Transform raw JSONL data → Nautilus ParquetDataCatalog.

Modules:
  readers      Streaming JSONL/zst/gz decompression
  instruments  Build Nautilus Instrument objects from exchangeInfo
  trades       Raw trade records → TradeTick
  book         L2 delta reconstruction → OrderBookDepth10 snapshots
  catalog      Idempotent catalog writing & purge helpers
  report       Conversion report generation
  universe     Universe resolution from meta/ or disk fallback
"""
