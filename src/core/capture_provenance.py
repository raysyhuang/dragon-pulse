"""Fail-closed validation for supplied Tushare daily_basic capture attestations."""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DATE_RE = re.compile(r"^\d{8}$")
_RECEIPT_RE = re.compile(r"^daily_basic_(\d{8})\.capture\.json$")
_SNAPSHOT_RE = re.compile(r"^daily_basic_(\d{8})\.csv$")
_GRADES = {"OBSERVED_CAPTURE", "TRUSTED_HISTORICAL_ASSUMPTION"}
_TRUSTED_CAVEAT = "historical_tushare_trusted_assumption"


class CaptureProvenanceError(ValueError):
    """Raised when a supplied capture attestation cannot be trusted as stated."""


@dataclass(frozen=True)
class CaptureAttestation:
    receipt_path: Path
    snapshot_path: Path
    snapshot_file: str
    snapshot_sha256: str
    captured_at: str
    provenance_grade: str
    caveat: str | None
    raw_response_file: str | None
    raw_response_sha256: str | None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_relpath(value: object, prefix: str | None = None) -> bool:
    if not isinstance(value, str) or not value:
        return False
    candidate = PurePosixPath(value)
    return (
        value == candidate.as_posix()
        and not candidate.is_absolute()
        and "." not in candidate.parts
        and ".." not in candidate.parts
        and not any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
        and (prefix is None or value.startswith(prefix))
    )


def _reject(message: str) -> None:
    raise CaptureProvenanceError(f"capture provenance validation failed: {message}")


def _safe_file(path: Path, root: Path, what: str) -> None:
    try:
        path.relative_to(root)
    except ValueError:
        _reject(f"{what} resolves outside allowed root: {path}")
    current = path
    while current != root:
        if current.is_symlink():
            _reject(f"symlinked {what} is forbidden: {path}")
        current = current.parent
    if not path.is_file():
        _reject(f"missing {what}: {path}")


def _load_receipt(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise CaptureProvenanceError(f"capture provenance validation failed: unparseable receipt {path}: {exc}") from exc
    if not isinstance(payload, dict):
        _reject(f"receipt must be an object: {path}")
    return payload


def validate_capture_attestations(
    sources_dir: str | Path,
    snapshots: list[Path] | tuple[Path, ...],
    *,
    receipts_dir: str | Path | None = None,
    raw_root: str | Path | None = None,
    output_relative: bool = False,
) -> tuple[CaptureAttestation, ...]:
    """Validate one capture receipt for every supplied snapshot, fail closed on evidence errors.

    Input receipts live beside their snapshots. Copied bundle receipts live in
    ``attestations/`` and use output-relative ``sources/...`` snapshot references.
    """
    source_dir = Path(sources_dir)
    receipt_dir = Path(receipts_dir) if receipts_dir is not None else source_dir
    raw_dir = Path(raw_root) if raw_root is not None else source_dir
    if not source_dir.is_dir() or source_dir.is_symlink():
        _reject(f"sources directory is missing or unsafe: {source_dir}")
    if not receipt_dir.is_dir() or receipt_dir.is_symlink():
        _reject(f"receipt directory is missing or unsafe: {receipt_dir}")
    if not raw_dir.is_dir() or raw_dir.is_symlink():
        _reject(f"raw root is missing or unsafe: {raw_dir}")
    source_root = source_dir.resolve()
    receipt_root = receipt_dir.resolve()
    raw_root_path = raw_dir.resolve()
    source_lexical_root = Path(os.path.abspath(source_dir))

    records: list[CaptureAttestation] = []
    for snapshot in snapshots:
        supplied_snapshot = Path(snapshot)
        snapshot = supplied_snapshot
        if snapshot.is_absolute():
            snapshot = Path(os.path.abspath(snapshot))
        else:
            lexical_snapshot = Path(os.path.abspath(snapshot))
            try:
                relative_snapshot = lexical_snapshot.relative_to(source_lexical_root)
            except ValueError:
                relative_snapshot = snapshot
            snapshot = Path(os.path.abspath(source_root / relative_snapshot))
        _safe_file(snapshot, source_root, "snapshot")
        match = _SNAPSHOT_RE.fullmatch(snapshot.name)
        if match is None:
            _reject(f"snapshot filename must be daily_basic_YYYYMMDD.csv: {snapshot.name}")
        day = match.group(1)
        receipt_name = f"daily_basic_{day}.capture.json"
        receipt_path = receipt_root / receipt_name
        _safe_file(receipt_path, receipt_root, "capture receipt")
        payload = _load_receipt(receipt_path)

        if payload.get("schema_version") != 1:
            _reject(f"receipt schema_version must be literal 1: {receipt_name}")
        if payload.get("provider") != "tushare":
            _reject(f"receipt provider must be literal tushare: {receipt_name}")
        if payload.get("endpoint") != "daily_basic":
            _reject(f"receipt endpoint must be literal daily_basic: {receipt_name}")
        if payload.get("requested_trade_date") != day or not _DATE_RE.fullmatch(day):
            _reject(f"receipt requested_trade_date must match filename date: {receipt_name}")
        expected_snapshot_file = f"sources/{snapshot.name}" if output_relative else snapshot.name
        if payload.get("snapshot_file") != expected_snapshot_file:
            _reject(f"receipt snapshot_file must match supplied snapshot: {receipt_name}")
        digest = payload.get("snapshot_sha256")
        if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
            _reject(f"receipt snapshot_sha256 must be lowercase SHA-256: {receipt_name}")
        if sha256_file(snapshot) != digest:
            _reject(f"snapshot hash mismatch: {receipt_name}")
        captured_at = payload.get("captured_at")
        if not isinstance(captured_at, str):
            _reject(f"receipt captured_at must be ISO UTC timestamp: {receipt_name}")
        try:
            parsed = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
        except ValueError:
            _reject(f"receipt captured_at must be ISO UTC timestamp: {receipt_name}")
        if not captured_at.endswith("Z") or parsed.utcoffset() is None:
            _reject(f"receipt captured_at must be ISO UTC timestamp: {receipt_name}")
        try:
            requested_trade_date = datetime.strptime(day, "%Y%m%d").date()
        except ValueError:
            _reject(f"receipt requested_trade_date must be a valid calendar date: {receipt_name}")
        if parsed.date() < requested_trade_date:
            _reject(f"receipt captured_at predates requested_trade_date: {receipt_name}")
        grade = payload.get("provenance_grade")
        if grade not in _GRADES:
            _reject(f"receipt provenance_grade is invalid or PIT_CAPTURE_VERIFIED: {receipt_name}")
        caveat = payload.get("caveat")
        raw_file = payload.get("raw_response_file")
        raw_digest = payload.get("raw_response_sha256")
        if grade == "TRUSTED_HISTORICAL_ASSUMPTION":
            if caveat != _TRUSTED_CAVEAT:
                _reject(f"trusted receipt caveat must be {_TRUSTED_CAVEAT}: {receipt_name}")
            if raw_file is not None or raw_digest is not None:
                _reject(f"trusted receipt must not claim observed raw response: {receipt_name}")
        else:
            if caveat is not None:
                _reject(f"observed receipt must not carry trusted-history caveat: {receipt_name}")
            if not _safe_relpath(raw_file, "raw/") or len(PurePosixPath(raw_file).parts) != 2:
                _reject(f"receipt raw_response_file must be a flat safe path below raw/: {receipt_name}")
            if not isinstance(raw_digest, str) or not _SHA256_RE.fullmatch(raw_digest):
                _reject(f"receipt raw_response_sha256 must be lowercase SHA-256: {receipt_name}")
            raw_path = raw_root_path / raw_file
            _safe_file(raw_path, raw_root_path, "raw response")
            if sha256_file(raw_path) != raw_digest:
                _reject(f"raw response hash mismatch: {receipt_name}")
        records.append(CaptureAttestation(receipt_path, supplied_snapshot, expected_snapshot_file, digest, captured_at, grade, caveat if isinstance(caveat, str) else None, raw_file if isinstance(raw_file, str) else None, raw_digest if isinstance(raw_digest, str) else None))

    return tuple(records)
