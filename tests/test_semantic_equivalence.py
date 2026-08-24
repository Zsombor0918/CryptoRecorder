"""Truthful semantic-equivalence guards for the replay/catalog milestone."""
from __future__ import annotations

from pathlib import Path


def test_convert_day_remains_legacy_full_l2_entrypoint() -> None:
    """The validated full-L2 path must stay independent from replay_store v0."""
    convert_day = Path(__file__).resolve().parent.parent / "convert_day.py"
    content = convert_day.read_text()

    assert convert_day.exists()
    assert "pipeline.build_replay_store" not in content
    assert "validation.replay_catalog_reconstruct" not in content
    assert "ReplayReader" not in content


def test_replay_full_l2_catalog_generation_is_implemented() -> None:
    """full_l2 is no longer deferred: validation.replay_catalog_reconstruct
    supports it and the profile write-flags must emit OrderBookDeltas (and
    depth10 when enabled). This is a validation-only helper, not a product CLI."""
    from validation.replay_catalog_reconstruct import SUPPORTED_PROFILES, _profile_write_flags

    assert "full_l2" in SUPPORTED_PROFILES
    writes_trades, writes_deltas, writes_depth10 = _profile_write_flags("full_l2", True)
    assert (writes_trades, writes_deltas, writes_depth10) == (True, True, True)
    # full_l2 with depth10 disabled still writes trades + deltas.
    assert _profile_write_flags("full_l2", False) == (True, True, False)
