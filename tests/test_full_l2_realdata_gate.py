"""Real-data v2.0.0 gate: convert_day.py vs replay ``full_l2`` semantic equivalence.

This test only runs when ``CRYPTO_RECORDER_REAL_DATA_ROOT`` points at a local
``data_raw`` tree. It is the authoritative gate referenced by
``docs/FULL_L2_REPLAY_CATALOG_PLAN.md``: until it passes against real recorded
data, the ``replay_store -> full_l2`` path stays "synthetic validated, real-data
validation pending" and must NOT be called v2.0.0.

Run it with, e.g.::

    CRYPTO_RECORDER_REAL_DATA_ROOT=./data_raw \
    CRYPTO_RECORDER_REAL_DATA_DATE=2026-06-12 \
    CRYPTO_RECORDER_REAL_DATA_SYMBOL=ADAUSDT \
    CRYPTO_RECORDER_REAL_DATA_VENUE=BINANCE_SPOT \
    pytest -m realdata tests/test_full_l2_realdata_gate.py -s
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from validation.validate_catalog_equivalence import validate_catalog_equivalence


@pytest.mark.realdata
def test_full_l2_real_data_equivalence_when_enabled(tmp_path: Path) -> None:
    real_root = os.environ.get("CRYPTO_RECORDER_REAL_DATA_ROOT")
    if not real_root:
        pytest.skip("Set CRYPTO_RECORDER_REAL_DATA_ROOT to run the full_l2 real-data gate")

    date = os.environ.get("CRYPTO_RECORDER_REAL_DATA_DATE", "2026-06-12")
    symbol = os.environ.get("CRYPTO_RECORDER_REAL_DATA_SYMBOL", "ADAUSDT")
    venue = os.environ.get("CRYPTO_RECORDER_REAL_DATA_VENUE", "BINANCE_SPOT")

    report = validate_catalog_equivalence(
        date=date,
        symbols=[symbol],
        venues=[venue],
        data_root=Path(real_root),
        work_root=tmp_path / "work",
        old_catalog_root=tmp_path / "old_catalog",
        replay_root=tmp_path / "replay_store",
        new_catalog_root=tmp_path / "new_catalog",
        profile="full_l2",
        overwrite=True,
    )

    # Always surface the diagnostics so a failure is fully explained, not just red.
    print(json.dumps(report, indent=2, default=str))
    assert report["status"] != "skipped", "full_l2 must not be skipped on real data"
    assert report["status"] == "passed", json.dumps(report, indent=2, default=str)
