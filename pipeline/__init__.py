"""
pipeline — Build and transform data artifacts.

This package contains only build/transform commands:
- daily_build: Main daily orchestrator (raw manifest + replay build + report)
- build_replay_store: Replay store builder from raw data
- raw_manifest: Raw data coverage scanning

Audit and equivalence commands live in the validation/ package, not here.
There is no feature-store builder and no product-facing catalog-generation
CLI in this package (see docs/ARCHITECTURE.md for the recorder + replay-store
ownership boundary).
"""
