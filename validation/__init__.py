"""
validation — Inspect, audit, and compare data artifacts.

This is the single general validation package. It contains:
- audit_replay_store: Audit replay_store partitions
- validate_catalog_equivalence: Compare old convert_day vs new replay catalogs
- replay_catalog_reconstruct: Internal, CLI-less helper that reconstructs a
  temporary Nautilus catalog from replay_store for equivalence checking only
  (not a supported downstream runtime API)
- catalog_compare: Semantic TradeTick comparison utilities
- catalog_inspect: Nautilus catalog instrument inspector
- phase2_report: Convert report JSON validator

Build and transform commands live in the pipeline/ package, not here.
"""
