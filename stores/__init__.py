"""
stores — Data storage modules for the replay layer.

This package contains storage schemas, readers, and writers for:
- replay_store: normalized deterministic Parquet replay layer (the stable
  external contract consumed by downstream repositories, e.g. KovacsTrader)

CryptoRecorder does not own a feature-store or label-store layer; those
responsibilities belong to downstream consumer repositories.
"""
