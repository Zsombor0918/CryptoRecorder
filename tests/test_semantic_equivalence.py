"""Truthful semantic-equivalence guards for the replay/catalog milestone."""
from __future__ import annotations

from pathlib import Path

import pytest


def test_convert_day_remains_legacy_full_l2_entrypoint() -> None:
    """The validated full-L2 path must stay independent from replay_store v0."""
    convert_day = Path(__file__).resolve().parent.parent / "convert_day.py"
    content = convert_day.read_text()

    assert convert_day.exists()
    assert "pipeline.build_replay_store" not in content
    assert "pipeline.generate_catalog" not in content
    assert "ReplayReader" not in content


def test_replay_full_l2_catalog_generation_is_deferred() -> None:
    pytest.skip("generate_catalog full_l2 is deferred; convert_day.py remains the full-L2 path")
