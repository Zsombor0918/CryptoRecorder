"""Fail-closed validation for the authoritative uv dependency environments.

This command is read-only: it checks the committed lock, the active Python
environment, dependency separation, imports, and CLI boundaries.  Environment
creation remains an explicit ``uv sync --frozen`` operator action so this
validator never owns or removes arbitrary directories.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import os
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_NAUTILUS_VERSION = "1.225.0"
ENVIRONMENT_KINDS = ("production", "reconstruction", "development")
PRODUCTION_DISTRIBUTIONS = ("aiohttp", "numpy", "pyarrow", "zstandard")
RECONSTRUCTION_DISTRIBUTIONS = ("nautilus-trader",)
DEVELOPMENT_DISTRIBUTIONS = ("pytest", "pytest-asyncio")
PRODUCTION_IMPORTS = (
    "recorder",
    "disk_monitor",
    "pipeline.replay_lifecycle",
    "pipeline.daily_build",
    "pipeline.build_replay_store",
    "stores.replay_reader",
    "stores.replay_writer",
    "validation.audit_replay_store",
)
PRODUCTION_HELP_MODULES = (
    "pipeline.daily_build",
    "pipeline.build_replay_store",
    "pipeline.reconstruct_selected_catalog",
    "validation.audit_replay_store",
)
RECONSTRUCTION_IMPORTS = (
    "convert_day",
    "pipeline.reconstruct_selected_catalog",
    "validation.replay_catalog_reconstruct",
    "validation.validate_catalog_equivalence",
)


class DependencyEnvironmentError(RuntimeError):
    """The active environment does not satisfy its selected contract."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _distribution_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _run(command: Sequence[str], *, env: dict[str, str] | None = None) -> None:
    result = subprocess.run(
        list(command),
        cwd=REPOSITORY_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise DependencyEnvironmentError(
            f"command failed ({result.returncode}): {' '.join(command)}: {detail}"
        )


def _validate_project_contract() -> dict:
    pyproject_path = REPOSITORY_ROOT / "pyproject.toml"
    lock_path = REPOSITORY_ROOT / "uv.lock"
    version_path = REPOSITORY_ROOT / "VERSION"
    if not pyproject_path.is_file() or not lock_path.is_file() or not version_path.is_file():
        raise DependencyEnvironmentError("pyproject.toml, uv.lock, and VERSION must exist")
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    if project.get("tool", {}).get("uv", {}).get("package") is not False:
        raise DependencyEnvironmentError("CryptoRecorder must remain a non-packaged uv project")
    if project.get("tool", {}).get("uv", {}).get("default-groups") != []:
        raise DependencyEnvironmentError("uv default-groups must be empty")
    if "build-system" in project:
        raise DependencyEnvironmentError("the virtual project must not declare a build backend")
    if project.get("project", {}).get("requires-python") != ">=3.12,<3.15":
        raise DependencyEnvironmentError("supported Python range must be >=3.12,<3.15")
    if project.get("project", {}).get("version") == version_path.read_text().strip():
        raise DependencyEnvironmentError("VERSION must not be duplicated as project metadata")
    return project


def _new_external_root(path: Path) -> Path:
    path = path.expanduser()
    if path.is_symlink() or path.exists():
        raise DependencyEnvironmentError("smoke root must be a new non-symlink path")
    absolute = path.absolute()
    ancestor = absolute.parent
    while ancestor != ancestor.parent:
        if ancestor.is_symlink():
            raise DependencyEnvironmentError("smoke-root parent is missing or unsafe")
        ancestor = ancestor.parent
    resolved = path.resolve(strict=False)
    if resolved in {Path("/"), REPOSITORY_ROOT, REPOSITORY_ROOT / ".venv"}:
        raise DependencyEnvironmentError("unsafe smoke root")
    try:
        resolved.relative_to(REPOSITORY_ROOT)
    except ValueError:
        pass
    else:
        raise DependencyEnvironmentError("smoke root must be outside the repository")
    parent = resolved.parent
    if (
        parent.is_symlink()
        or not parent.is_dir()
        or parent.stat().st_uid != os.getuid()
    ):
        raise DependencyEnvironmentError("smoke-root parent is missing or unsafe")
    resolved.mkdir(mode=0o700)
    return resolved


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def run_production_schema_v2_smoke(root: Path) -> dict:
    """Build and validate one tiny synthetic partition under a new external root."""
    root = _new_external_root(root)
    raw = root / "raw"
    replay = root / "replay"
    venue, symbol, date = "BINANCE_SPOT", "ADAUSDT", "2026-06-12"
    base_ms = int(
        datetime(2026, 6, 12, tzinfo=timezone.utc).timestamp() * 1_000
    )
    _write_jsonl(
        raw / venue / "depth_v2" / symbol / date / f"{date}T00.jsonl",
        [
            {
                "record_type": "snapshot_seed", "venue": venue, "symbol": symbol,
                "stream_session_id": 1, "session_seq": 0,
                "ts_recv_ns": base_ms * 1_000_000 + 1,
                "ts_event_ms": base_ms, "lastUpdateId": 100,
                "payload": {"bids": [["0.1700", "100.0"]], "asks": [["0.1710", "200.0"]]},
            },
            {
                "record_type": "depth_update", "venue": venue, "symbol": symbol,
                "stream_session_id": 1, "session_seq": 1,
                "ts_recv_ns": base_ms * 1_000_000 + 2_000_000,
                "ts_event_ms": base_ms + 2, "U": 101, "u": 101, "pu": None,
                "payload": {"bids": [["0.1700", "101.0"]], "asks": []},
            },
        ],
    )
    _write_jsonl(
        raw / venue / "trade_v2" / symbol / date / f"{date}T00.jsonl",
        [{
            "record_type": "trade", "venue": venue, "market_type": "spot",
            "symbol": symbol, "trade_stream_session_id": 1, "trade_session_seq": 1,
            "ts_recv_ns": base_ms * 1_000_000 + 10, "ts_event_ms": base_ms,
            "ts_trade_ms": base_ms, "price": "0.1705", "quantity": "2.0",
            "is_buyer_maker": False, "exchange_trade_id": 123,
            "native_payload": {"e": "trade", "t": 123},
        }],
    )
    _write_jsonl(
        raw / venue / "exchangeinfo" / "EXCHANGEINFO" / date / f"{date}T00.jsonl",
        [{"symbols": [{
            "symbol": symbol, "baseAsset": "ADA", "quoteAsset": "USDT",
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1"},
                {"filterType": "NOTIONAL", "minNotional": "5.0"},
            ],
        }]}],
    )

    from pipeline.build_replay_store import build_replay_for_symbol
    from stores.replay_reader import ReplayReader
    from stores.replay_writer import audit_partition_deep, validate_partition

    result = build_replay_for_symbol(
        venue, symbol, date, raw, replay,
        schema_version=2, price_scale=4, qty_scale=1,
        check_repartition_readiness=False,
    )
    partition = replay / f"venue={venue}" / f"symbol={symbol}" / f"date={date}"
    if result.get("status") != "success" or not validate_partition(partition):
        raise DependencyEnvironmentError(f"synthetic schema-v2 build failed: {result}")
    problems = audit_partition_deep(partition)
    if problems:
        raise DependencyEnvironmentError(f"synthetic deep integrity failed: {problems}")
    trades = list(ReplayReader(replay).iter_trades(venue, symbol, date))
    if len(trades) != 1 or not (trades[0].get("trade_id") or trades[0].get("agg_trade_id")):
        raise DependencyEnvironmentError("synthetic replay contains an anonymous trade")
    return {
        "status": "passed",
        "root": str(root),
        "partition": str(partition),
        "schema_version": 2,
        "depth_record_count": result["depth_count"],
        "trade_record_count": result["trade_count"],
        "routine_validation": "passed",
        "deep_integrity": "passed",
        "anonymous_trade_rows": 0,
    }


def validate_environment(kind: str, *, uv_bin: str = "uv") -> dict:
    if kind not in ENVIRONMENT_KINDS:
        raise DependencyEnvironmentError(f"unsupported environment kind: {kind}")
    if not ((3, 12) <= sys.version_info[:2] < (3, 15)):
        raise DependencyEnvironmentError("Python must satisfy >=3.12,<3.15")
    project = _validate_project_contract()
    lock_path = REPOSITORY_ROOT / "uv.lock"
    lock_before = _sha256(lock_path)
    _run([uv_bin, "lock", "--check"])
    lock_after = _sha256(lock_path)
    if lock_before != lock_after:
        raise DependencyEnvironmentError("uv.lock changed during read-only validation")

    selection = ["--no-default-groups"]
    if kind in {"reconstruction", "development"}:
        selection.extend(["--extra", "reconstruction"])
    if kind == "development":
        selection.extend(["--group", "dev"])
    exact_env = os.environ.copy()
    exact_env["UV_PROJECT_ENVIRONMENT"] = str(Path(sys.prefix).resolve())
    _run(
        [uv_bin, "sync", "--check", "--frozen", *selection],
        env=exact_env,
    )

    required = list(PRODUCTION_DISTRIBUTIONS)
    forbidden: list[str] = []
    if kind in {"reconstruction", "development"}:
        required.extend(RECONSTRUCTION_DISTRIBUTIONS)
    else:
        forbidden.extend(RECONSTRUCTION_DISTRIBUTIONS)
    if kind == "development":
        required.extend(DEVELOPMENT_DISTRIBUTIONS)
    else:
        forbidden.extend(DEVELOPMENT_DISTRIBUTIONS)

    versions = {name: _distribution_version(name) for name in required + forbidden}
    missing = [name for name in required if versions[name] is None]
    present_forbidden = [name for name in forbidden if versions[name] is not None]
    if missing:
        raise DependencyEnvironmentError(f"required distributions are missing: {missing}")
    if present_forbidden:
        raise DependencyEnvironmentError(
            f"forbidden distributions are installed for {kind}: {present_forbidden}"
        )
    if kind in {"reconstruction", "development"} and (
        versions["nautilus-trader"] != EXPECTED_NAUTILUS_VERSION
    ):
        raise DependencyEnvironmentError(
            "nautilus-trader must be exactly " + EXPECTED_NAUTILUS_VERSION
        )

    for module_name in PRODUCTION_IMPORTS:
        importlib.import_module(module_name)
    for module_name in PRODUCTION_HELP_MODULES:
        _run([sys.executable, "-m", module_name, "--help"])

    if kind == "production":
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from pipeline.reconstruct_selected_catalog import "
                    "SelectedCatalogError, _load_engine; "
                    "\ntry: _load_engine()"
                    "\nexcept SelectedCatalogError as exc: print(exc); raise SystemExit(0)"
                    "\nraise SystemExit(1)"
                ),
            ],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        guidance = "uv sync --frozen --no-default-groups --extra reconstruction"
        if result.returncode != 0 or guidance not in (result.stdout + result.stderr):
            raise DependencyEnvironmentError(
                "missing reconstruction extra did not produce canonical actionable guidance"
            )
    else:
        for module_name in RECONSTRUCTION_IMPORTS:
            importlib.import_module(module_name)
        _run([sys.executable, "-m", "pipeline.reconstruct_selected_catalog", "--help"])
        _run([sys.executable, "convert_day.py", "--help"])

    dependencies = project["project"]["dependencies"]
    reconstruction = project["project"]["optional-dependencies"]["reconstruction"]
    dev = project["dependency-groups"]["dev"]
    return {
        "status": "passed",
        "environment_kind": kind,
        "environment_path": str(Path(sys.prefix).absolute()),
        "python_version": sys.version.split()[0],
        "python_executable": str(Path(sys.executable).absolute()),
        "uv_version": subprocess.run(
            [uv_bin, "--version"], capture_output=True, text=True, check=True
        ).stdout.strip(),
        "lock_sha256_before": lock_before,
        "lock_sha256_after": lock_after,
        "lock_unchanged": True,
        "sync_selection": ["--frozen", *selection],
        "exact_locked_environment": True,
        "declared": {
            "production": dependencies,
            "reconstruction": reconstruction,
            "development": dev,
        },
        "installed_contract_versions": {
            name: versions[name] for name in required
        },
        "forbidden_absent": forbidden,
        "imports_checked": list(PRODUCTION_IMPORTS) + (
            list(RECONSTRUCTION_IMPORTS) if kind != "production" else []
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate one active frozen uv dependency environment without mutation."
    )
    parser.add_argument("--kind", choices=ENVIRONMENT_KINDS, required=True)
    parser.add_argument("--uv-bin", default="uv")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument(
        "--production-smoke-root",
        type=Path,
        help="new external directory for one tiny schema-v2 build/integrity smoke",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = validate_environment(args.kind, uv_bin=args.uv_bin)
        if args.production_smoke_root is not None:
            if args.kind != "production":
                raise DependencyEnvironmentError(
                    "--production-smoke-root is valid only with --kind production"
                )
            report["schema_v2_smoke"] = run_production_schema_v2_smoke(
                args.production_smoke_root
            )
    except (DependencyEnvironmentError, OSError, subprocess.SubprocessError) as exc:
        print(f"dependency environment validation failed: {exc}", file=sys.stderr)
        return 1
    encoded = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.json_output is not None:
        output = args.json_output.resolve()
        if output.is_symlink() or output.exists():
            print("dependency environment validation failed: output must be a new regular path", file=sys.stderr)
            return 1
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
