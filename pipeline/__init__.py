"""
pipeline — Build and transform data artifacts.

This package contains only build/transform commands:
- daily_build: Main daily orchestrator (raw manifest + replay build + report)
- build_replay_store: Replay store builder from raw data
- raw_manifest: Raw data coverage scanning
- reconstruct_selected_catalog: Supported, explicitly selected development-
  computer replay -> temporary Nautilus catalog boundary

Audit and equivalence commands live in the validation/ package, not here.
There is no feature-store builder or persistent catalog service. Selected
catalog reconstruction requires explicit venue, symbol, time, output, and job
scope (see docs/ARCHITECTURE.md for the recorder + replay-store boundary).
"""
