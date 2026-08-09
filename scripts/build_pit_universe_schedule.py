#!/usr/bin/env python3
"""Build an immutable, validator-accepted PIT universe bundle from supplied CSVs.

This builder intentionally performs no provider calls.  Its PIT claim is limited to
preserving and validating the supplied dated snapshots; authenticity/completeness of
those historical snapshots is an upstream provenance responsibility.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.core.capture_provenance import CaptureAttestation, CaptureProvenanceError, validate_capture_attestations

REQUIRED_COLUMNS = ("ts_code", "circ_mv", "list_date", "delist_date")
TICKER_PATTERN = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)")
SCHEDULE_COLUMNS = (
    "as_of_date",
    "ticker",
    "listed_on_or_before",
    "delisted_after",
    "source_file",
    "source_sha256",
)


class BuildError(ValueError):
    """Raised for a supplied snapshot that cannot support a PIT schedule."""


def _parse_date(value: str, field: str, source: Path, row_number: int) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise BuildError(f"{source}: row {row_number} {field} must be ISO YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise BuildError(f"{source}: row {row_number} {field} must be ISO YYYY-MM-DD")
    return parsed


def _parse_as_of_dates(value: str) -> list[tuple[str, date]]:
    values = value.split(",")
    if not values or any(not item or item.strip() != item for item in values):
        raise BuildError("--as-of-dates must be a comma-separated non-empty list of ISO YYYY-MM-DD dates")
    parsed = [(item, _parse_date(item, "as-of date", Path("--as-of-dates"), 0)) for item in values]
    if len({item for item, _ in parsed}) != len(parsed):
        raise BuildError("--as-of-dates must not contain duplicates")
    return parsed


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _composite(hashes: dict[str, str]) -> str:
    payload = "".join(f"{path}  {digest}\n" for path, digest in sorted(hashes.items()))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _git_commit(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise BuildError("unable to determine builder git commit") from exc


def _git_tree_dirty(repo_root: Path) -> bool:
    try:
        return bool(
            subprocess.check_output(
                ["git", "-C", str(repo_root), "status", "--porcelain"], text=True, stderr=subprocess.DEVNULL
            )
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise BuildError("unable to determine builder git tree status") from exc


def _read_eligible_snapshot(source: Path, as_of_text: str, as_of: date) -> list[tuple[str, float, str, str]]:
    if not source.is_file() or source.is_symlink():
        raise BuildError(f"missing required snapshot: {source}")
    try:
        with source.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fields = reader.fieldnames
            if fields is None or any(column not in fields for column in REQUIRED_COLUMNS) or len(fields) != len(set(fields)):
                raise BuildError(f"{source}: required columns are {', '.join(REQUIRED_COLUMNS)}")
            seen: set[str] = set()
            eligible: list[tuple[str, float, str, str]] = []
            for row_number, row in enumerate(reader, start=2):
                ticker = (row.get("ts_code") or "").strip()
                if not ticker:
                    raise BuildError(f"{source}: row {row_number} missing ticker")
                if not TICKER_PATTERN.fullmatch(ticker):
                    raise BuildError(f"{source}: row {row_number} ticker must be canonical A-share ts_code")
                if ticker in seen:
                    raise BuildError(f"{source}: row {row_number} duplicate ticker: {ticker}")
                seen.add(ticker)
                cap_text = (row.get("circ_mv") or "").strip()
                try:
                    cap = float(cap_text)
                except ValueError as exc:
                    raise BuildError(f"{source}: row {row_number} circ_mv must be numeric and positive") from exc
                if not math.isfinite(cap) or cap <= 0:
                    raise BuildError(f"{source}: row {row_number} circ_mv must be numeric and positive")
                listed_text = (row.get("list_date") or "").strip()
                if not listed_text:
                    raise BuildError(f"{source}: row {row_number} list_date is required")
                listed = _parse_date(listed_text, "list_date", source, row_number)
                delisted_text = (row.get("delist_date") or "").strip()
                delisted = _parse_date(delisted_text, "delist_date", source, row_number) if delisted_text else None
                if listed > as_of or (delisted is not None and delisted <= as_of):
                    raise BuildError(f"{source}: row {row_number} source snapshot eligibility contradiction as of {as_of_text}")
                eligible.append((ticker, cap, listed_text, delisted_text))
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise BuildError(f"unable to parse snapshot {source}: {exc}") from exc
    if not eligible:
        raise BuildError(f"{source}: no eligible candidates as of {as_of_text}")
    return sorted(eligible, key=lambda item: (-item[1], item[0]))


def build_bundle(sources_dir: Path, output: Path, as_of_dates: list[tuple[str, date]], universe_n: int, source_label: str) -> dict[str, object]:
    if output.exists() or output.is_symlink():
        raise BuildError(f"output already exists: {output}")
    if not sources_dir.is_dir() or sources_dir.is_symlink():
        raise BuildError(f"sources directory is missing or unsafe: {sources_dir}")

    selected: list[tuple[str, str, str, str, Path]] = []
    snapshots = [(as_of_text, sources_dir / f"daily_basic_{as_of.strftime('%Y%m%d')}.csv") for as_of_text, as_of in as_of_dates]
    for _, source in snapshots:
        if not source.is_file() or source.is_symlink():
            raise BuildError(f"missing required snapshot: {source}")
    try:
        attestations = validate_capture_attestations(sources_dir, [source for _, source in snapshots])
    except CaptureProvenanceError as exc:
        raise BuildError(str(exc)) from exc
    attestation_by_source = {attestation.snapshot_path: attestation for attestation in attestations}
    for (as_of_text, as_of), (_, source) in zip(as_of_dates, snapshots):
        eligible = _read_eligible_snapshot(source, as_of_text, as_of)
        if len(eligible) < universe_n:
            raise BuildError(f"{source}: fewer eligible candidates ({len(eligible)}) than universe_n ({universe_n})")
        selected.extend((as_of_text, ticker, listed, delisted, source) for ticker, _, listed, delisted in eligible[:universe_n])

    output_parent = output.parent
    output_parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}.tmp-", dir=output_parent))
    try:
        copied_sources = temporary / "sources"
        copied_sources.mkdir()
        copied_attestations = temporary / "attestations"
        copied_attestations.mkdir()
        copied_raw = temporary / "raw"
        copied_raw.mkdir()
        hashes: dict[str, str] = {}
        source_rels: dict[Path, tuple[str, str]] = {}
        for _, source in snapshots:
            destination = copied_sources / source.name
            shutil.copyfile(source, destination)
            relpath = f"sources/{source.name}"
            digest = _sha256(destination)
            hashes[relpath] = digest
            source_rels[source] = (relpath, digest)
            attestation: CaptureAttestation = attestation_by_source[source]
            receipt_payload = json.loads(attestation.receipt_path.read_text(encoding="utf-8"))
            receipt_payload["snapshot_file"] = relpath
            receipt_destination = copied_attestations / attestation.receipt_path.name
            receipt_destination.write_text(json.dumps(receipt_payload, sort_keys=True) + "\n", encoding="utf-8")
            hashes[f"attestations/{receipt_destination.name}"] = _sha256(receipt_destination)
            if attestation.raw_response_file is not None:
                raw_source = sources_dir / attestation.raw_response_file
                raw_destination = copied_raw / Path(attestation.raw_response_file).name
                shutil.copyfile(raw_source, raw_destination)
                hashes[f"raw/{raw_destination.name}"] = _sha256(raw_destination)

        schedule_path = temporary / "universe_schedule.csv"
        with schedule_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=SCHEDULE_COLUMNS)
            writer.writeheader()
            for as_of_text, ticker, listed, delisted, source in sorted(selected, key=lambda item: (item[0], item[1])):
                source_file, source_hash = source_rels[source]
                writer.writerow(
                    {
                        "as_of_date": as_of_text,
                        "ticker": ticker,
                        "listed_on_or_before": listed,
                        "delisted_after": delisted,
                        "source_file": source_file,
                        "source_sha256": source_hash,
                    }
                )
        hashes["universe_schedule.csv"] = _sha256(schedule_path)
        as_of_texts = [value for value, _ in as_of_dates]
        repo_root = Path(__file__).resolve().parents[1]
        commit = _git_commit(repo_root)
        tree_dirty = _git_tree_dirty(repo_root)
        manifest: dict[str, object] = {
            "bundle_id": f"pit-universe-{as_of_texts[0]}-to-{as_of_texts[-1]}-n{universe_n}",
            "pit_grade": True,
            "as_of_dates": as_of_texts,
            "universe_n": universe_n,
            "source_label": source_label,
            "provenance": {
                "source_label": source_label,
                "snapshot_contract": "supplied daily_basic_YYYYMMDD.csv; no provider calls",
                "historical_membership_authenticity": "not established by this builder",
            },
            "capture_provenance_grade": (
                "OBSERVED_CAPTURE"
                if all(item.provenance_grade == "OBSERVED_CAPTURE" for item in attestations)
                else "TRUSTED_HISTORICAL_ASSUMPTION"
            ),
            "capture_provenance_caveat": (
                None
                if all(item.provenance_grade == "OBSERVED_CAPTURE" for item in attestations)
                else "trusted history is not independently capture-proven"
            ),
            "builder_git_commit": commit,
            "builder_tree_dirty": tree_dirty,
            "hashes": hashes,
            "composite_sha256": _composite(hashes),
        }
        (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, output)
        return manifest
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--as-of-dates", required=True)
    parser.add_argument("--universe-n", required=True, type=_positive_int)
    parser.add_argument("--source-label", required=True)
    args = parser.parse_args(argv)
    if not args.source_label.strip():
        parser.error("--source-label must be non-empty")
    try:
        manifest = build_bundle(
            args.sources_dir,
            args.output,
            _parse_as_of_dates(args.as_of_dates),
            args.universe_n,
            args.source_label.strip(),
        )
    except BuildError as exc:
        parser.error(str(exc))
    print(json.dumps({"output": str(args.output), "bundle_id": manifest["bundle_id"], "composite_sha256": manifest["composite_sha256"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
