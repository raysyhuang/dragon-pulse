"""Fail-closed validation for immutable point-in-time universe bundles."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import cast

from src.core.capture_provenance import CaptureProvenanceError, validate_capture_attestations


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_REQUIRED_SCHEDULE_COLUMNS = {
    "as_of_date",
    "ticker",
    "listed_on_or_before",
    "delisted_after",
    "source_file",
    "source_sha256",
}


class PitBundleValidationError(ValueError):
    """Raised when a PIT bundle does not meet its manifest integrity contract."""


@dataclass(frozen=True)
class PitBundle:
    """Validated PIT schedule and its immutable manifest identity."""

    path: Path
    manifest: Mapping[str, object]
    bundle_id: str
    composite_sha256: str
    pit_grade: bool
    as_of_dates: tuple[str, ...]
    schedule: tuple[Mapping[str, str], ...]


def sha256_file(path: str | Path) -> str:
    """Return the SHA-256 digest of a file without loading it all into memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def composite_sha256(hashes: dict[str, str]) -> str:
    """Return the canonical manifest hash over sorted ``path  digest`` lines."""
    payload = "".join(f"{path}  {digest}\n" for path, digest in sorted(hashes.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_safe_path(relpath: str) -> bool:
    candidate = Path(relpath)
    canonical = PurePosixPath(relpath).as_posix()
    return (
        bool(relpath)
        and not candidate.is_absolute()
        and ".." not in candidate.parts
        and relpath == canonical
        and not any(
            character.isspace() or unicodedata.category(character) in {"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"}
            for character in relpath
        )
    )


def _parse_iso_date(value: str) -> date | None:
    if not _ISO_DATE_RE.fullmatch(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _has_symlinked_component(candidate: Path, root: Path) -> bool:
    while candidate != root:
        if candidate.is_symlink():
            return True
        candidate = candidate.parent
    return False


def _is_within_root(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _failures_text(failures: list[str]) -> str:
    return "PIT bundle validation failed:\n- " + "\n- ".join(failures)


def _is_flat_ancillary_path(relpath: str, directory: str, suffix: str | None = None) -> bool:
    parts = PurePosixPath(relpath).parts
    return len(parts) == 2 and parts[0] == directory and (suffix is None or parts[1].endswith(suffix))


# A manifest nesting limit safely remains far below Python's recursion limit.
_MAX_FREEZE_DEPTH = 100


def _freeze(value: object, depth: int = 0) -> object:
    if depth > _MAX_FREEZE_DEPTH:
        raise PitBundleValidationError(f"PIT bundle manifest nesting exceeds safe depth {_MAX_FREEZE_DEPTH}")
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item, depth + 1) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item, depth + 1) for item in value)
    return value


def validate_pit_bundle(bundle_dir: str | Path) -> PitBundle:
    """Validate a self-contained PIT bundle and return its date/ticker-sorted schedule."""
    path = Path(bundle_dir)
    if path.is_symlink():
        raise PitBundleValidationError(f"PIT bundle directory must not be a symlink: {path}")
    path = path.resolve()
    manifest_path = path / "manifest.json"
    if not path.is_dir():
        raise PitBundleValidationError(f"PIT bundle directory is missing: {path}")
    if not manifest_path.is_file():
        raise PitBundleValidationError(f"PIT bundle manifest is missing: {manifest_path}")
    if manifest_path.is_symlink():
        raise PitBundleValidationError(f"PIT bundle manifest must not be a symlink: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise PitBundleValidationError(f"PIT bundle manifest is unparseable: {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise PitBundleValidationError("PIT bundle manifest must be an object")

    failures: list[str] = []
    if manifest.get("pit_grade") is not True:
        failures.append("manifest.pit_grade must be literal true")
    if not isinstance(manifest.get("bundle_id"), str) or not manifest["bundle_id"]:
        failures.append("manifest.bundle_id must be a non-empty string")

    as_of_dates = manifest.get("as_of_dates")
    if not isinstance(as_of_dates, list) or not as_of_dates or not all(isinstance(value, str) and value for value in as_of_dates):
        failures.append("manifest.as_of_dates must be a non-empty list of strings")
        as_of_dates = []
    elif len(set(as_of_dates)) != len(as_of_dates):
        failures.append("manifest.as_of_dates must not contain duplicates")
    elif any(_parse_iso_date(value) is None for value in as_of_dates):
        failures.append("manifest.as_of_dates must use ISO YYYY-MM-DD dates")

    hashes = manifest.get("hashes")
    if not isinstance(hashes, dict) or not hashes:
        failures.append("manifest.hashes must be a non-empty object")
        hashes = {}
    else:
        for relpath, digest in hashes.items():
            if not isinstance(relpath, str) or not _is_safe_path(relpath):
                failures.append(f"invalid manifest hash path: {relpath}")
            if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
                failures.append(f"invalid SHA-256 manifest hash for {relpath}")

    if "universe_schedule.csv" not in hashes:
        failures.append("manifest is missing hash for universe_schedule.csv")
    source_hashes = {relpath: digest for relpath, digest in hashes.items() if isinstance(relpath, str) and relpath.startswith("sources/")}
    attestation_hashes = {relpath: digest for relpath, digest in hashes.items() if isinstance(relpath, str) and relpath.startswith("attestations/")}
    raw_hashes = {relpath: digest for relpath, digest in hashes.items() if isinstance(relpath, str) and relpath.startswith("raw/")}
    if not source_hashes:
        failures.append("manifest must hash at least one raw source under sources/")
    for relpath in source_hashes:
        if not _is_flat_ancillary_path(relpath, "sources") or PurePosixPath(relpath).name == "manifest.json":
            failures.append(f"manifest source path must be flat and not named manifest.json: {relpath}")
    for relpath in attestation_hashes:
        if not _is_flat_ancillary_path(relpath, "attestations", ".capture.json") or PurePosixPath(relpath).name == "manifest.json":
            failures.append(f"manifest attestation path must be flat capture receipt and not named manifest.json: {relpath}")
    for relpath in raw_hashes:
        if not _is_flat_ancillary_path(relpath, "raw") or PurePosixPath(relpath).name == "manifest.json":
            failures.append(f"manifest raw path must be flat and not named manifest.json: {relpath}")
    if any(
        relpath != "universe_schedule.csv"
        and not (isinstance(relpath, str) and (relpath.startswith("sources/") or relpath.startswith("attestations/") or relpath.startswith("raw/")))
        for relpath in hashes
    ):
        failures.append("manifest.hashes may contain only universe_schedule.csv and flat sources/, attestations/, or raw/ files")

    actual_files = {
        item.relative_to(path).as_posix()
        for item in path.rglob("*")
        if item.is_file() and item != manifest_path
    }
    for item in path.rglob("*"):
        if item.is_symlink():
            failures.append(f"symlinked bundle payload is forbidden: {item.relative_to(path).as_posix()}")
    for relpath in sorted(actual_files - set(hashes)):
        if relpath.startswith("sources/"):
            failures.append(f"unhashed raw source: {relpath}")
        elif relpath.startswith("attestations/") or relpath.startswith("raw/"):
            failures.append(f"unhashed ancillary payload: {relpath}")
        else:
            failures.append(f"unlisted PIT bundle file: {relpath}")
    for relpath, expected in sorted(hashes.items()):
        if not isinstance(relpath, str) or not _is_safe_path(relpath):
            continue
        candidate = path / relpath
        resolved_candidate = candidate.resolve()
        if _has_symlinked_component(candidate, path):
            failures.append(f"symlinked manifest payload is forbidden: {relpath}")
        elif not _is_within_root(resolved_candidate, path):
            failures.append(f"manifest payload resolves outside bundle root: {relpath}")
        elif not candidate.is_file():
            failures.append(f"missing manifest-listed file: {relpath}")
        else:
            try:
                actual_hash = sha256_file(candidate)
            except OSError as exc:
                failures.append(f"unable to hash manifest-listed file: {relpath}: {exc}")
            else:
                if actual_hash != expected:
                    failures.append(f"hash mismatch for {relpath}")

    composite = composite_sha256(hashes)
    if manifest.get("composite_sha256") != composite:
        failures.append("composite hash mismatch")

    schedule_path = path / "universe_schedule.csv"
    schedule: list[dict[str, str]] = []
    referenced_source_hashes: set[str] = set()
    if schedule_path.is_file() and "universe_schedule.csv" in hashes:
        try:
            with schedule_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None or not _REQUIRED_SCHEDULE_COLUMNS.issubset(reader.fieldnames):
                    failures.append("universe_schedule.csv is missing required columns")
                else:
                    seen: set[tuple[str, str]] = set()
                    for row_number, row in enumerate(reader, start=2):
                        entry = {column: (row.get(column) or "").strip() for column in _REQUIRED_SCHEDULE_COLUMNS}
                        if not all(entry[column] for column in _REQUIRED_SCHEDULE_COLUMNS - {"delisted_after"}):
                            failures.append(f"schedule row {row_number} has blank required fields")
                            continue
                        identity = (entry["as_of_date"], entry["ticker"])
                        if identity in seen:
                            failures.append(f"duplicate schedule identity: {identity[0]}, {identity[1]}")
                        seen.add(identity)
                        if entry["as_of_date"] not in as_of_dates:
                            failures.append(f"schedule date outside manifest.as_of_dates: {entry['as_of_date']}")
                        as_of_date = _parse_iso_date(entry["as_of_date"])
                        listed_on_or_before = _parse_iso_date(entry["listed_on_or_before"])
                        delisted_after = (
                            _parse_iso_date(entry["delisted_after"])
                            if entry["delisted_after"]
                            else None
                        )
                        if as_of_date is None or listed_on_or_before is None or (
                            entry["delisted_after"] and delisted_after is None
                        ):
                            failures.append(f"schedule row {row_number} dates must use ISO YYYY-MM-DD")
                        elif listed_on_or_before > as_of_date:
                            failures.append(f"schedule row {row_number} ticker was not listed as of {entry['as_of_date']}")
                        elif delisted_after is not None and delisted_after <= as_of_date:
                            failures.append(f"schedule row {row_number} ticker was already delisted as of {entry['as_of_date']}")
                        source_file = entry["source_file"]
                        if not _is_safe_path(source_file) or not source_file.startswith("sources/"):
                            failures.append(f"schedule source is not under sources/: {source_file}")
                        elif PurePosixPath(source_file).name == "manifest.json":
                            failures.append(f"schedule source must not be named manifest.json: {source_file}")
                        elif source_file not in source_hashes:
                            failures.append(f"schedule source missing manifest hash: {source_file}")
                        elif entry["source_sha256"] != source_hashes[source_file]:
                            failures.append(f"schedule source digest does not match manifest hash: {source_file}")
                        else:
                            referenced_source_hashes.add(source_file)
                        schedule.append(entry)
        except (OSError, UnicodeDecodeError, csv.Error) as exc:
            failures.append(f"unparseable universe_schedule.csv: {exc}")

    if not schedule:
        failures.append("schedule must not be empty")
    elif {entry["as_of_date"] for entry in schedule} != set(as_of_dates):
        failures.append("schedule dates do not exactly match manifest.as_of_dates (missing schedule dates or undeclared dates)")
    for relpath in sorted(set(source_hashes) - referenced_source_hashes):
        failures.append(f"unreferenced manifest source hash: {relpath}")

    if attestation_hashes:
        try:
            attestations = validate_capture_attestations(
                path / "sources",
                [path / relpath for relpath in sorted(source_hashes)],
                receipts_dir=path / "attestations",
                raw_root=path,
                output_relative=True,
            )
        except CaptureProvenanceError as exc:
            failures.append(str(exc))
        else:
            if {f"attestations/{item.receipt_path.name}" for item in attestations} != set(attestation_hashes):
                failures.append("manifest attestations must contain exactly one receipt per source")
            expected_raw = {item.raw_response_file for item in attestations if item.raw_response_file is not None}
            if expected_raw != set(raw_hashes):
                failures.append("manifest raw files must exactly match observed receipt references")
            all_observed = all(item.provenance_grade == "OBSERVED_CAPTURE" for item in attestations)
            expected_grade = "OBSERVED_CAPTURE" if all_observed else "TRUSTED_HISTORICAL_ASSUMPTION"
            if manifest.get("capture_provenance_grade") != expected_grade:
                failures.append("manifest.capture_provenance_grade does not match receipt grades")
            expected_caveat = None if all_observed else "trusted history is not independently capture-proven"
            if manifest.get("capture_provenance_caveat") != expected_caveat:
                failures.append("manifest.capture_provenance_caveat does not match supplied evidence")
    elif raw_hashes:
        failures.append("raw payloads require capture attestations")

    if failures:
        raise PitBundleValidationError(_failures_text(failures))

    return PitBundle(
        path=path,
        manifest=cast(Mapping[str, object], _freeze(manifest)),
        bundle_id=manifest["bundle_id"],
        composite_sha256=composite,
        pit_grade=True,
        as_of_dates=tuple(sorted(as_of_dates)),
        schedule=tuple(
            cast(Mapping[str, str], _freeze(row))
            for row in sorted(schedule, key=lambda row: (row["as_of_date"], row["ticker"]))
        ),
    )
