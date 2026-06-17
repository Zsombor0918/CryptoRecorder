"""
pipeline — Build and transform data artifacts.

This package contains only build/transform commands:
- daily_build: Main daily orchestrator (replay + features + reports)
- build_replay_store: Replay store builder from raw data
- build_feature_store: Feature store builder from replay data
- generate_catalog: On-demand Nautilus catalog generation from replay
- raw_manifest: Raw data coverage scanning

Audit and equivalence commands live in the validation/ package, not here.
"""
