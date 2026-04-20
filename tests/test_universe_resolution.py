from __future__ import annotations

import importlib
import json
import shutil
import tempfile
from pathlib import Path

DATE_STR = "2099-01-01"
SPOT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
FUT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def _setup_dirs(tmpdir: Path) -> None:
    (tmpdir / "meta" / "universe" / "BINANCE_SPOT").mkdir(parents=True)
    (tmpdir / "meta" / "universe" / "BINANCE_USDTF").mkdir(parents=True)
    (tmpdir / "data_raw").mkdir(parents=True)


def _write_meta(tmpdir: Path, venue: str, content) -> None:
    path = tmpdir / "meta" / "universe" / venue / f"{DATE_STR}.json"
    path.write_text(json.dumps(content, indent=2))


def _create_disk_dirs(tmpdir: Path, venue: str, symbols: list[str]) -> None:
    for sym in symbols:
        for channel in ("trade", "depth"):
            day_dir = tmpdir / "data_raw" / venue / channel / sym / DATE_STR
            day_dir.mkdir(parents=True, exist_ok=True)
            (day_dir / "00.jsonl").write_text("{}\n")


def _patch_config(tmpdir: Path):
    import config
    import converter.universe

    original_data_root = config.DATA_ROOT
    original_meta_root = config.META_ROOT
    config.DATA_ROOT = tmpdir / "data_raw"
    config.META_ROOT = tmpdir / "meta"
    importlib.reload(converter.universe)
    return config, converter.universe, original_data_root, original_meta_root


def _restore_config(config_module, universe_module, original_data_root, original_meta_root) -> None:
    config_module.DATA_ROOT = original_data_root
    config_module.META_ROOT = original_meta_root
    importlib.reload(universe_module)


def test_dict_format() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        config_module, universe_module, old_data_root, old_meta_root = _patch_config(tmpdir)
        try:
            recorder_dict = {
                "BINANCE_SPOT": SPOT_SYMBOLS,
                "BINANCE_USDTF": FUT_SYMBOLS,
                "timestamp": "2099-01-01T00:00:00",
                "top_count": 50,
                "candidate_pool_size": 120,
                "filter_version": "v3_futures_support_precheck",
                "selection_metadata": {
                    "BINANCE_SPOT": {
                        "selected_count": len(SPOT_SYMBOLS),
                        "configured_candidate_pool_size": 120,
                    },
                    "BINANCE_USDTF": {
                        "selected_count": len(FUT_SYMBOLS),
                        "configured_candidate_pool_size": 200,
                    },
                },
            }
            _write_meta(tmpdir, "BINANCE_SPOT", recorder_dict)
            _write_meta(tmpdir, "BINANCE_USDTF", recorder_dict)

            result = universe_module.resolve_universe(DATE_STR)
            assert result["BINANCE_SPOT"] == SPOT_SYMBOLS
            assert result["BINANCE_USDTF"] == FUT_SYMBOLS
        finally:
            _restore_config(config_module, universe_module, old_data_root, old_meta_root)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_list_format() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        config_module, universe_module, old_data_root, old_meta_root = _patch_config(tmpdir)
        try:
            _write_meta(tmpdir, "BINANCE_SPOT", SPOT_SYMBOLS)
            _write_meta(tmpdir, "BINANCE_USDTF", FUT_SYMBOLS)

            result = universe_module.resolve_universe(DATE_STR)
            assert result["BINANCE_SPOT"] == SPOT_SYMBOLS
            assert result["BINANCE_USDTF"] == FUT_SYMBOLS
        finally:
            _restore_config(config_module, universe_module, old_data_root, old_meta_root)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_disk_fallback() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        config_module, universe_module, old_data_root, old_meta_root = _patch_config(tmpdir)
        try:
            _create_disk_dirs(tmpdir, "BINANCE_SPOT", SPOT_SYMBOLS)
            _create_disk_dirs(tmpdir, "BINANCE_USDTF", FUT_SYMBOLS)

            result = universe_module.resolve_universe(DATE_STR)
            assert set(result["BINANCE_SPOT"]) == set(SPOT_SYMBOLS)
            assert set(result["BINANCE_USDTF"]) == set(FUT_SYMBOLS)
        finally:
            _restore_config(config_module, universe_module, old_data_root, old_meta_root)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_corrupt_json_fallback() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="univ_test_"))
    try:
        _setup_dirs(tmpdir)
        config_module, universe_module, old_data_root, old_meta_root = _patch_config(tmpdir)
        try:
            path = tmpdir / "meta" / "universe" / "BINANCE_SPOT" / f"{DATE_STR}.json"
            path.write_text("{not valid json!!")
            _create_disk_dirs(tmpdir, "BINANCE_SPOT", ["BTCUSDT"])

            result = universe_module.resolve_universe(DATE_STR)
            assert "BTCUSDT" in result["BINANCE_SPOT"]
        finally:
            _restore_config(config_module, universe_module, old_data_root, old_meta_root)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
