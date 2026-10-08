"""Unsigned, fail-closed physical recording-scope evidence (no market continuity claim).

All recorder processes for a dataset must use this protocol and retain its ledger.
A lifetime recorder lock prevents overlap. A separate ledger lock serializes start/
final publication with retirement-backed replay attempts. Relative raw paths and exact hashes bind portable offline bundles. Recorder
starts additionally bind the authoritative ledger to its local raw-root identity.
"""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import socket
import stat
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = 1
MAX_RECORDS = 4096
MAX_BYTES = 64 * 1024 * 1024
MAX_RAW_FILES = 250_000
ID = re.compile(r"^[0-9a-f]{32}$")
SHA = re.compile(r"^[0-9a-f]{64}$")
SYMBOL = re.compile(r"^[A-Z0-9]{2,40}$")
VENUES = {"BINANCE_SPOT", "BINANCE_USDTF"}


class CoverageError(ValueError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    try:
        result = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        if result.isoformat(timespec="microseconds").replace("+00:00", "Z") != value:
            raise ValueError("noncanonical time")
        return result
    except (TypeError, ValueError) as exc:
        raise CoverageError("invalid canonical UTC time") from exc


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _safe_path(path: Path, *, directory=False) -> None:
    for parent in [*reversed(path.absolute().parents), path.absolute()]:
        info = parent.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise CoverageError(f"symlink evidence/input: {parent}")
    info = path.lstat()
    if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        raise CoverageError(f"wrong file type: {path}")
    if not directory and info.st_nlink != 1:
        raise CoverageError(f"multiply linked input: {path}")


def file_identity(path: Path) -> dict:
    _safe_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        before = os.fstat(handle.fileno())
        h = hashlib.sha256()
        while chunk := handle.read(128 * 1024):
            h.update(chunk)
        after = os.fstat(handle.fileno())
    current = path.lstat()
    key = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if key(before) != key(after) or key(after) != key(current):
        raise CoverageError(f"input changed while hashing: {path}")
    return {"size_bytes": before.st_size, "sha256": h.hexdigest()}


def _read(path: Path) -> tuple[dict, dict]:
    _safe_path(path)
    if path.stat().st_size > MAX_BYTES:
        raise CoverageError("coverage record size bound exceeded")
    before = file_identity(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise CoverageError("coverage input changed file type")
        payload = json.load(handle, object_pairs_hook=_unique_keys)
    if file_identity(path) != before or not isinstance(payload, dict):
        raise CoverageError("coverage record changed or is not an object")
    return payload, before


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CoverageError("duplicate JSON key")
        result[key] = value
    return result


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish(path: Path, payload: dict, *, replace=False) -> None:
    _safe_path(path.parent, directory=True)
    temp = path.parent / f".pending-{uuid.uuid4().hex}"
    # Interrupted writes deliberately remain visible and block evidence use.
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        if os.fstat(handle.fileno()).st_size > MAX_BYTES:
            raise CoverageError("coverage record byte bound exceeded")
        os.fsync(handle.fileno())
    if replace:
        if path.exists() or path.is_symlink():
            _safe_path(path)
        os.replace(temp, path)
    else:
        # Atomic no-replace publication using same-filesystem link, then unlink.
        os.link(temp, path, follow_symlinks=False)
        # Keep the pending hard link until publication is durably synced. A
        # write/link/fsync failure therefore cannot expose acceptable evidence.
        _fsync_dir(path.parent)
        temp.unlink()
        # This is the final operation: after a crash an unsynced pending-unlink
        # may reappear and conservatively block the ledger, never certify it.
        return
    _fsync_dir(path.parent)


@contextlib.contextmanager
def ledger_lock(root: Path, *, exclusive=False, recorder=False):
    _safe_path(root, directory=True)
    root_info = root.stat()
    if root_info.st_uid != os.getuid() or root_info.st_mode & 0o022:
        raise CoverageError("unsafe coverage directory ownership/mode")
    path = root / (".recorder.lock" if recorder else ".ledger.lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise CoverageError("unsafe coverage lock")
        fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        yield
    except BlockingIOError as exc:
        raise CoverageError("coverage ledger/recorder busy; retry next run") from exc
    finally:
        os.close(fd)


def normalize_universe(universe: dict) -> dict:
    if not isinstance(universe, dict) or not set(universe) <= VENUES:
        raise CoverageError("invalid venue universe")
    result = {}
    for venue, symbols in sorted(universe.items()):
        if not isinstance(symbols, list) or len(symbols) > 1000:
            raise CoverageError("invalid symbol universe")
        if any(not isinstance(s, str) or not SYMBOL.fullmatch(s) for s in symbols):
            raise CoverageError("invalid symbol")
        if len(symbols) != len(set(symbols)):
            raise CoverageError("duplicate selected symbol")
        result[venue] = sorted(symbols)
    return result


def root_identity(data_root: Path) -> str:
    _safe_path(data_root, directory=True)
    info = data_root.stat()
    return digest({"path": str(data_root.absolute()), "device": info.st_dev, "inode": info.st_ino})


def _retirement_inventory(venue: str, symbol: str, date: str, data_root: Path):
    from pipeline.raw_manifest import select_replay_raw_inventory
    inventory = select_replay_raw_inventory(venue, symbol, date, data_root)
    # Ordinary selection ignores non-files, including dangling symlinks. For
    # absence proof that would silently turn an unsafe input into "no input".
    for channel, source_date in inventory:
        directory = data_root / venue / channel / symbol / source_date
        if os.path.lexists(directory):
            _safe_path(directory, directory=True)
            for index, path in enumerate(directory.iterdir()):
                if index >= 72:
                    raise CoverageError("raw hourly inventory bound exceeded")
                _safe_path(path)
                if path not in inventory[(channel, source_date)]:
                    raise CoverageError("unknown raw input in retirement scope")
    return inventory


def snapshot(root: Path) -> dict:
    _safe_path(root, directory=True)
    records = {}
    size = 0
    for index, path in enumerate(root.iterdir()):
        if index >= MAX_RECORDS:
            raise CoverageError("coverage ledger scan bound exceeded")
        if path.name in {".ledger.lock", ".recorder.lock"}:
            _safe_path(path)
            continue
        if path.name not in {"head.json"} and not re.fullmatch(r"[0-9a-f]{32}\.(start|final)\.json", path.name):
            raise CoverageError(f"unknown/interrupted coverage entry: {path.name}")
        payload, identity = _read(path)
        size += identity["size_bytes"]
        if size > MAX_BYTES:
            raise CoverageError("coverage ledger byte bound exceeded")
        info = path.stat()
        records[path.name] = {"payload": payload, **identity, "file_key": [info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns]}
    return records


def validate_chain(records: dict, binding: str) -> list[dict]:
    if not isinstance(records, dict) or len(records) > MAX_RECORDS:
        raise CoverageError("invalid embedded record inventory")
    for name, entry in records.items():
        if not isinstance(entry, dict) or set(entry) != {"payload", "sha256", "size_bytes", "file_key"}:
            raise CoverageError("invalid embedded record shape")
        encoded = json.dumps(entry["payload"], sort_keys=True, separators=(",", ":")).encode()
        if len(encoded) != entry["size_bytes"] or hashlib.sha256(encoded).hexdigest() != entry["sha256"]:
            raise CoverageError("embedded marker fingerprint mismatch")
        if name != "head.json" and not re.fullmatch(r"[0-9a-f]{32}\.(start|final)\.json", name):
            raise CoverageError("invalid embedded logical marker path")
        if not isinstance(entry["file_key"], list) or len(entry["file_key"]) != 4 or any(type(k) is not int for k in entry["file_key"]):
            raise CoverageError("invalid marker file identity")
    if not records:
        return []
    if "head.json" not in records:
        raise CoverageError("missing ledger head")
    head = records["head.json"]["payload"]
    if set(head) != {"contract_version", "count", "session_id", "start_sha256", "data_root_identity", "recorder_root_binding"}:
        raise CoverageError("invalid ledger head")
    if not isinstance(binding, str) or not SHA.fullmatch(binding) or not isinstance(head["recorder_root_binding"], str) or not SHA.fullmatch(head["recorder_root_binding"]):
        raise CoverageError("invalid dataset/local-root identity")
    if type(head["contract_version"]) is not int or head["contract_version"] != VERSION or head["data_root_identity"] != binding:
        raise CoverageError("ledger version/root mismatch")
    count = head["count"]
    if type(count) is not int or not 1 <= count <= MAX_RECORDS:
        raise CoverageError("invalid session count")
    starts = [entry["payload"] for name, entry in records.items() if name.endswith(".start.json")]
    if len(starts) != count:
        raise CoverageError("missing/extra session start")
    starts.sort(key=lambda entry: entry.get("sequence", -1))
    previous = None
    for i, start in enumerate(starts, 1):
        required = {"contract_version", "session_id", "sequence", "previous", "data_root_identity", "started_at", "universe", "universe_digest", "hostname", "pid", "repository_sha", "application_version", "status"}
        if set(start) != required or type(start["contract_version"]) is not int or start["contract_version"] != VERSION or start["status"] != "open":
            raise CoverageError("invalid session start contract")
        sid = start["session_id"]
        if not isinstance(sid, str) or not ID.fullmatch(sid) or type(start["sequence"]) is not int or start["sequence"] != i:
            raise CoverageError("invalid session identity/order")
        if records.get(f"{sid}.start.json", {}).get("payload") != start:
            raise CoverageError("session filename mismatch")
        if start["data_root_identity"] != binding or start["previous"] != previous:
            raise CoverageError("unknown session gap/root mismatch")
        universe = normalize_universe(start["universe"])
        if universe != start["universe"] or digest(universe) != start["universe_digest"]:
            raise CoverageError("universe digest mismatch")
        started = parse_time(start["started_at"])
        if i > 1 and started < parse_time(starts[i-2]["started_at"]):
            raise CoverageError("reversed session times")
        previous = {"session_id": sid, "sha256": records[f"{sid}.start.json"]["sha256"]}
        final_name = f"{sid}.final.json"
        if final_name in records:
            validate_final(start, records[final_name]["payload"])
            if records[final_name]["payload"]["start_sha256"] != records[f"{sid}.start.json"]["sha256"]:
                raise CoverageError("final/start fingerprint mismatch")
    if head["session_id"] != previous["session_id"] or head["start_sha256"] != previous["sha256"]:
        raise CoverageError("ledger tail/head mismatch")
    for name in records:
        if name.endswith(".final.json") and f"{name[:32]}.start.json" not in records:
            raise CoverageError("orphan final record")
    return starts


def validate_final(start: dict, final: dict) -> None:
    required = {"contract_version", "session_id", "start_sha256", "status", "ingestion_stopped_at", "finalized_at", "shutdown_reason", "producers_stopped", "storage", "raw_files", "unowned_raw_files", "errors"}
    if set(final) != required or type(final["contract_version"]) is not int or final["contract_version"] != VERSION or final["session_id"] != start["session_id"]:
        raise CoverageError("invalid final contract")
    if final["status"] not in {"closed_clean", "failed"}:
        raise CoverageError("invalid closure status")
    if type(final["producers_stopped"]) is not bool or not isinstance(final["shutdown_reason"], str) or not final["shutdown_reason"]:
        raise CoverageError("invalid producer/shutdown metadata")
    if not parse_time(start["started_at"]) <= parse_time(final["ingestion_stopped_at"]) <= parse_time(final["finalized_at"]):
        raise CoverageError("reversed closure times")
    if not isinstance(final["errors"], list) or not isinstance(final["raw_files"], dict) or len(final["raw_files"]) > MAX_RAW_FILES:
        raise CoverageError("invalid closure files/errors")
    if not isinstance(final["unowned_raw_files"], list) or len(set(final["unowned_raw_files"])) != len(final["unowned_raw_files"]) or any(path not in final["raw_files"] for path in final["unowned_raw_files"]):
        raise CoverageError("invalid preexisting raw inventory")
    if not isinstance(final["start_sha256"], str) or not SHA.fullmatch(final["start_sha256"]):
        raise CoverageError("invalid start fingerprint")
    storage = final["storage"]
    if not isinstance(storage, dict) or set(storage) != {"queues_drained", "files_closed", "compression_drained", "writers", "compression", "errors"}:
        raise CoverageError("invalid storage closure")
    if not isinstance(storage["writers"], dict) or not isinstance(storage["compression"], dict) or not isinstance(storage["errors"], list):
        raise CoverageError("invalid storage diagnostics")
    for writer in storage["writers"].values():
        for key in ("enqueued_count", "write_count", "drop_count", "write_errors", "queue_size"):
            if type(writer.get(key)) is not int or writer[key] < 0:
                raise CoverageError("invalid writer counts")
    if final["status"] == "closed_clean":
        if final["producers_stopped"] is not True or final["errors"] or storage["errors"] or any(storage[k] is not True for k in ("queues_drained", "files_closed", "compression_drained")):
            raise CoverageError("contradictory clean closure")
        compression = storage["compression"]
        if any(type(compression.get(k)) is not int or compression[k] != 0 for k in ("queued", "active", "failed")) or compression.get("last_error") is not None:
            raise CoverageError("unclean compression")
        for writer in storage["writers"].values():
            if writer["enqueued_count"] != writer["write_count"] or any(writer[k] for k in ("drop_count", "write_errors", "queue_size")):
                raise CoverageError("unclean writer")
    for path, identity in final["raw_files"].items():
        parts = Path(path).parts
        if len(parts) != 5 or Path(path).is_absolute() or any(p in {".", ".."} for p in parts) or str(Path(path)) != path:
            raise CoverageError("invalid logical raw path")
        venue, channel, symbol, date, name = parts
        if venue not in VENUES or channel not in {"depth_v2", "trade_v2"} or symbol not in start["universe"].get(venue, []):
            raise CoverageError("raw file outside selected universe")
        datetime.strptime(date, "%Y-%m-%d")
        if not re.fullmatch(re.escape(date) + r"T(?:[01][0-9]|2[0-3])\.jsonl\.zst", name):
            raise CoverageError("raw file is not final compressed hour")
        if not isinstance(identity, dict) or set(identity) != {"sha256", "size_bytes"} or not isinstance(identity["sha256"], str) or not SHA.fullmatch(identity["sha256"]) or type(identity["size_bytes"]) is not int or identity["size_bytes"] <= 0:
            raise CoverageError("invalid raw identity")


class RecordingSession:
    def __init__(self, root: Path, data_root: Path, universe: dict, *, repository_sha=None, application_version=None):
        self.root, self.data_root = Path(root), Path(data_root)
        self.root.mkdir(parents=True, exist_ok=True)
        _fsync_dir(self.root.parent)
        self._owner = ledger_lock(self.root, exclusive=True, recorder=True)
        self._owner.__enter__()
        try:
            with ledger_lock(self.root, exclusive=True):
                records = snapshot(self.root)
                local_binding = root_identity(self.data_root)
                if records:
                    head = records.get("head.json", {}).get("payload", {})
                    if head.get("recorder_root_binding") != local_binding:
                        raise CoverageError("recorder ledger belongs to a different local raw root")
                    binding = head.get("data_root_identity")
                else:
                    binding = digest({"dataset_id": uuid.uuid4().hex})
                chain = validate_chain(records, binding)
                sid = uuid.uuid4().hex
                self.start = {
                    "contract_version": VERSION, "session_id": sid, "sequence": len(chain)+1,
                    "previous": ({"session_id": chain[-1]["session_id"], "sha256": records[f"{chain[-1]['session_id']}.start.json"]["sha256"]} if chain else None),
                    "data_root_identity": binding, "started_at": utc_now(),
                    "universe": normalize_universe(universe), "hostname": socket.gethostname(), "pid": os.getpid(),
                    "repository_sha": repository_sha, "application_version": application_version, "status": "open",
                }
                self.start["universe_digest"] = digest(self.start["universe"])
                _publish(self.root / f"{sid}.start.json", self.start)
                self.start_sha = file_identity(self.root / f"{sid}.start.json")["sha256"]
                _publish(self.root / "head.json", {"contract_version": VERSION, "count": len(chain)+1, "session_id": sid, "start_sha256": self.start_sha, "data_root_identity": binding, "recorder_root_binding": local_binding}, replace=True)
        except BaseException:
            self.release()
            raise

    def finish(self, storage: dict, raw_files: dict, *, producers_stopped: bool, ingestion_stopped_at: str, errors: list[str], shutdown_reason: str, unowned_raw_files: list[str] | None = None):
        final = {
            "contract_version": VERSION, "session_id": self.start["session_id"], "start_sha256": self.start_sha,
            "status": "failed", "ingestion_stopped_at": ingestion_stopped_at, "finalized_at": utc_now(),
            "shutdown_reason": shutdown_reason, "producers_stopped": producers_stopped,
            "storage": storage, "raw_files": raw_files, "unowned_raw_files": list(unowned_raw_files or []), "errors": list(errors),
        }
        candidate = {**final, "status": "closed_clean"}
        try:
            validate_final(self.start, candidate)
            final = candidate
        except (CoverageError, KeyError, TypeError) as exc:
            final["errors"].append(str(exc))
        with ledger_lock(self.root, exclusive=True):
            _publish(self.root / f"{self.start['session_id']}.final.json", final)
        return final

    def release(self):
        self._owner.__exit__(None, None, None)


def _scope_owner(records: dict, chain: list[dict], venue: str, symbol: str, date: str, *, complete_next_day=False) -> tuple[dict, dict, datetime]:
    boundary = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    horizon = boundary + (timedelta(days=1) if complete_next_day else timedelta(hours=1))
    candidates = [s for s in chain if symbol in s["universe"].get(venue, []) and parse_time(s["started_at"]) < horizon]
    if not candidates:
        raise CoverageError("no owning selected session")
    owner = candidates[-1]
    final_entry = records.get(f"{owner['session_id']}.final.json")
    if final_entry is None:
        raise CoverageError("selected session remains open")
    final = final_entry["payload"]
    if final["status"] != "closed_clean" or final["start_sha256"] != records[f"{owner['session_id']}.start.json"]["sha256"]:
        raise CoverageError("session closure untrusted")
    if parse_time(final["finalized_at"]) > boundary - timedelta(hours=1):
        raise CoverageError("closure too late to prove retirement")
    successors = chain[owner["sequence"]:]
    if not successors or parse_time(successors[0]["started_at"]) >= boundary:
        raise CoverageError("no timely successor excluding retired symbol")
    previous = owner
    for successor in successors:
        if parse_time(successor["started_at"]) >= horizon:
            break
        previous_final = records.get(f"{previous['session_id']}.final.json", {}).get("payload")
        if previous_final is None or previous_final["status"] != "closed_clean" or previous_final["start_sha256"] != records[f"{previous['session_id']}.start.json"]["sha256"] or parse_time(previous_final["finalized_at"]) > parse_time(successor["started_at"]):
            raise CoverageError("unknown/open/overlapping session handoff")
        if parse_time(successor["started_at"]) < horizon and symbol in successor["universe"].get(venue, []):
            raise CoverageError("later session selected retired symbol")
        previous = successor
    return owner, final, horizon


def retirement_proof(root: Path, data_root: Path, venue: str, symbol: str, date: str, *, complete_next_day=False) -> dict:
    records = snapshot(root)
    chain = validate_chain(records, records.get("head.json", {}).get("payload", {}).get("data_root_identity"))
    owner, final, horizon = _scope_owner(records, chain, venue, symbol, date, complete_next_day=complete_next_day)
    if datetime.now(timezone.utc) < horizon:
        raise CoverageError("readiness horizon has not elapsed")
    # Require the target and every selected adjacent source to have exactly one
    # immutable owner. A shared-hour rewrite or pre-feature day stays deferred.
    inventory = _retirement_inventory(venue, symbol, date, data_root)
    raw = {}
    for entries in inventory.values():
        for file_path in entries:
            path = file_path.relative_to(data_root).as_posix()
            if path not in final["raw_files"]:
                raise CoverageError("raw input lacks exact retiring-session ownership")
            if path in final["unowned_raw_files"]:
                raise CoverageError("raw input predates owning session")
            if any(path in records.get(f"{other['session_id']}.final.json", {}).get("payload", {}).get("raw_files", {}) for other in chain if other != owner):
                raise CoverageError("raw input has multiple session owners")
            identity = file_identity(data_root / path)
            if identity != final["raw_files"][path]:
                raise CoverageError("raw input disagrees with closure evidence")
            raw[path] = identity
    if not raw or not any(f"/depth_v2/{symbol}/{date}/" in p for p in raw) or not any(f"/trade_v2/{symbol}/{date}/" in p for p in raw):
        raise CoverageError("required target-day raw inputs absent")
    proof = {"contract_version": VERSION, "mode": "clean_retirement_evidence", "venue": venue, "symbol": symbol, "date": date, "session_id": owner["session_id"], "data_root_identity": owner["data_root_identity"], "horizon": horizon.isoformat(timespec="microseconds").replace("+00:00", "Z"), "raw_files": raw, "records": records}
    proof["sha256"] = digest(proof)
    validate_provenance(proof, venue, symbol, date)
    return proof


def validate_provenance(proof: dict, venue: str, symbol: str, date: str) -> None:
    required = {"contract_version", "mode", "venue", "symbol", "date", "session_id", "data_root_identity", "horizon", "raw_files", "records", "sha256"}
    if not isinstance(proof, dict) or set(proof) != required or proof["contract_version"] != VERSION or proof["mode"] != "clean_retirement_evidence" or (proof["venue"], proof["symbol"], proof["date"]) != (venue, symbol, date):
        raise CoverageError("invalid readiness provenance")
    if proof["sha256"] != digest({k: v for k, v in proof.items() if k != "sha256"}):
        raise CoverageError("readiness provenance digest mismatch")
    chain = validate_chain(proof["records"], proof["data_root_identity"])
    boundary = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    horizon = parse_time(proof["horizon"])
    if horizon not in {boundary + timedelta(hours=1), boundary + timedelta(days=1)}:
        raise CoverageError("invalid readiness horizon")
    owner, final, expected_horizon = _scope_owner(proof["records"], chain, venue, symbol, date, complete_next_day=(horizon == boundary + timedelta(days=1)))
    if owner["session_id"] != proof["session_id"] or expected_horizon != horizon:
        raise CoverageError("readiness owner/horizon mismatch")
    if owner is None or symbol not in owner["universe"].get(venue, []):
        raise CoverageError("readiness owner mismatch")
    final = proof["records"][f"{owner['session_id']}.final.json"]["payload"]
    if final["status"] != "closed_clean" or not proof["raw_files"] or any(final["raw_files"].get(p) != i for p, i in proof["raw_files"].items()):
        raise CoverageError("readiness closure/raw mismatch")
    if set(proof["raw_files"]) & set(final["unowned_raw_files"]):
        raise CoverageError("readiness attempts retroactive raw certification")
    from datetime import date as calendar_date
    target = calendar_date.fromisoformat(date)
    for path in proof["raw_files"]:
        v, channel, sym, raw_date, name = Path(path).parts
        allowed_dates = {(target + timedelta(days=offset)).isoformat() for offset in (-1, 0, 1)} if channel == "depth_v2" else {date}
        if v != venue or sym != symbol or channel not in {"depth_v2", "trade_v2"} or raw_date not in allowed_dates:
            raise CoverageError("readiness raw source scope mismatch")


def recheck_proof(proof: dict, root: Path, data_root: Path) -> None:
    if snapshot(root) != proof["records"]:
        raise CoverageError("coverage evidence changed during replay attempt")
    inventory = _retirement_inventory(proof["venue"], proof["symbol"], proof["date"], data_root)
    current = {e.relative_to(data_root).as_posix(): file_identity(e) for entries in inventory.values() for e in entries}
    if current != proof["raw_files"]:
        raise CoverageError("retirement raw inventory changed during replay attempt")
