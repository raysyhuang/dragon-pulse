#!/usr/bin/env python3
"""Fail-closed freezer for the legacy top-1 paper ledger.

This is an inventory of files already saved by the paper tracker.  It never calls
providers, never derives a pick from an execution watchlist, and never repairs a
missing native top1-paper artifact.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
import sys
import secrets
from collections import Counter
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
EVIDENCE_GRADE = "LEGACY_NON_PIT_BASELINE_INVENTORY"
PROMOTION_STATUS = "NON_EXECUTION_NON_PROMOTABLE"
NATIVE_KIND = "top1_paper_watchlist"


class BaselineInventoryError(ValueError):
    """Raised when ledger or artifact evidence cannot be safely inventoried."""


class BaselineInventoryDurabilityUncertainError(BaselineInventoryError):
    """The artifact was linked, but output-directory durability is unknown."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


def _after_anchored_open(kind: str) -> None:
    """Deterministic test seam; production deliberately does nothing."""


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


class _AnchoredFile:
    """A regular file and its nofollow-opened parent, held until validation ends."""

    def __init__(self, *, parent_fd: int, fd: int, display: Path):
        self.parent_fd = parent_fd
        self.fd = fd
        self.display = display

    def close(self) -> None:
        first_error: OSError | None = None
        for fd in (self.fd, self.parent_fd):
            try:
                os.close(fd)
            except OSError as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def read_stable_bytes(self, label: str) -> bytes:
        try:
            before = os.fstat(self.fd)
            if not stat.S_ISREG(before.st_mode):
                raise BaselineInventoryError(f"{label} must be a regular non-symlink file: {self.display}")
            os.lseek(self.fd, 0, os.SEEK_SET)
            chunks: list[bytes] = []
            while True:
                chunk = os.read(self.fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            after = os.fstat(self.fd)
        except OSError as exc:
            raise BaselineInventoryError(f"cannot snapshot {label}: {self.display}") from exc
        if _stat_identity(before) != _stat_identity(after):
            raise BaselineInventoryError(f"{label} changed while being read: {self.display}")
        return b"".join(chunks)


class _AnchoredDirectory:
    def __init__(self, *, fd: int, display: Path):
        self.fd = fd
        self.display = display

    def close(self) -> None:
        os.close(self.fd)


def _open_absolute_directory(path: Path, *, create: bool = False,
                             missing_ok: bool = False) -> _AnchoredDirectory | None:
    """Walk an absolute lexical path beneath root without ever following a link."""
    absolute = _absolute_lexical(path)
    fd = os.open(absolute.anchor, _DIRECTORY_FLAGS)
    try:
        for part in absolute.parts[1:]:
            try:
                next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, dir_fd=fd)
                next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = next_fd
    except FileNotFoundError:
        os.close(fd)
        if missing_ok:
            return None
        raise BaselineInventoryError(f"cannot anchor declared directory: {absolute}") from None
    except OSError as exc:
        os.close(fd)
        raise BaselineInventoryError(f"cannot anchor declared directory: {absolute}") from exc
    return _AnchoredDirectory(fd=fd, display=absolute)


def _open_regular(parent_fd: int, leaf: str, display: Path, *, missing_ok: bool = False) -> _AnchoredFile | None:
    fd: int | None = None
    try:
        fd = os.open(leaf, _FILE_FLAGS, dir_fd=parent_fd)
        value = os.fstat(fd)
    except OSError as exc:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if missing_ok and isinstance(exc, FileNotFoundError):
            return None
        raise BaselineInventoryError(f"cannot open regular non-symlink file: {display}") from exc
    if not stat.S_ISREG(value.st_mode):
        os.close(fd)
        raise BaselineInventoryError(f"file must be a regular non-symlink file: {display}")
    return _AnchoredFile(parent_fd=parent_fd, fd=fd, display=display)


def _open_absolute_regular(path: Path) -> _AnchoredFile:
    absolute = _absolute_lexical(path)
    parent = _open_absolute_directory(absolute.parent)
    assert parent is not None
    try:
        anchored = _open_regular(parent.fd, absolute.name, absolute)
        assert anchored is not None
        return anchored
    except Exception:
        parent.close()
        raise


def _open_relative_regular(root: _AnchoredDirectory, relative: Path) -> _AnchoredFile | None:
    parent_fd = os.dup(root.fd)
    try:
        for part in relative.parts[:-1]:
            next_fd = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            os.close(parent_fd)
            parent_fd = next_fd
        return _open_regular(parent_fd, relative.name, root.display / relative, missing_ok=True)
    except FileNotFoundError:
        os.close(parent_fd)
        return None
    except Exception:
        os.close(parent_fd)
        raise


def _sha256_file(path: Path) -> str:
    anchored = _open_absolute_regular(path)
    try:
        return _sha256_bytes(anchored.read_stable_bytes("file"))
    finally:
        anchored.close()


def _require_date(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 10:
        raise BaselineInventoryError(f"{field} must be YYYY-MM-DD")
    try:
        # Lexical preservation is intentional, but reject impossible calendar dates.
        from datetime import date
        date.fromisoformat(value)
    except ValueError as exc:
        raise BaselineInventoryError(f"{field} must be YYYY-MM-DD") from exc
    return value


def _validate_row(row: Any, line_number: int) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise BaselineInventoryError(f"ledger line {line_number} must be an object")
    scan_date = _require_date(row.get("scan_date"), f"ledger line {line_number}.scan_date")
    required = ("regime", "n_live_picks", "top1", "top2", "evaluated", "results")
    if any(field not in row for field in required):
        raise BaselineInventoryError(f"ledger line {line_number} lacks required tracker fields")
    if row["regime"] is not None and not isinstance(row["regime"], str):
        raise BaselineInventoryError(f"ledger line {line_number}.regime must be a string or null")
    if "top1" not in row or (row["top1"] is not None and not isinstance(row["top1"], dict)):
        raise BaselineInventoryError(f"ledger line {line_number}.top1 must be an object or null")
    if "top2" not in row or not isinstance(row["top2"], list):
        raise BaselineInventoryError(f"ledger line {line_number}.top2 must be a list")
    if "n_live_picks" in row and (isinstance(row["n_live_picks"], bool) or not isinstance(row["n_live_picks"], int) or row["n_live_picks"] < 0):
        raise BaselineInventoryError(f"ledger line {line_number}.n_live_picks must be a non-negative integer")
    if "evaluated" in row and not isinstance(row["evaluated"], bool):
        raise BaselineInventoryError(f"ledger line {line_number}.evaluated must be boolean")
    if "results" in row and row["results"] is not None and not isinstance(row["results"], dict):
        raise BaselineInventoryError(f"ledger line {line_number}.results must be an object or null")
    if "native_artifact_path" in row and not isinstance(row["native_artifact_path"], str):
        raise BaselineInventoryError(f"ledger line {line_number}.native_artifact_path must be a string")
    count, top1, top2 = row["n_live_picks"], row["top1"], row["top2"]
    if len(top2) != min(count, 2) or (count == 0 and top1 is not None) or (count > 0 and (top1 is None or not top2 or _canonical_json(top1) != _canonical_json(top2[0]))):
        raise BaselineInventoryError(f"ledger line {line_number} has contradictory pick fields")
    if not row["evaluated"] and row["results"] is not None:
        raise BaselineInventoryError(f"ledger line {line_number}.results must be null while unevaluated")
    if row["evaluated"]:
        results = row["results"]
        if not isinstance(results, dict) or not isinstance(results.get("legs"), list) or not results["legs"]:
            raise BaselineInventoryError(f"ledger line {line_number}.results must contain legs while evaluated")
        if row["top1"] is not None:
            ticker = row["top1"].get("ticker")
            if not isinstance(ticker, str) or not ticker:
                raise BaselineInventoryError(f"ledger line {line_number}.top1.ticker must be a non-empty string while evaluated")
            top1_legs = [leg for leg in results["legs"]
                         if isinstance(leg, dict) and isinstance(leg.get("rank"), int)
                         and not isinstance(leg.get("rank"), bool) and leg["rank"] == 0]
            if len(top1_legs) != 1 or top1_legs[0].get("ticker") != ticker:
                raise BaselineInventoryError(f"ledger line {line_number}.results rank-0 leg must bind to top1.ticker")
    return row


def _read_ledger(ledger: _AnchoredFile) -> tuple[list[dict[str, Any]], bytes]:
    rows: list[dict[str, Any]] = []
    identities: set[str] = set()
    try:
        ledger_bytes = ledger.read_stable_bytes("ledger")
        lines = ledger_bytes.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise BaselineInventoryError(f"ledger is not UTF-8: {ledger.display}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BaselineInventoryError(f"malformed JSON at ledger line {line_number}") from exc
        row = _validate_row(raw, line_number)
        identity = row["scan_date"]
        if identity in identities:
            raise BaselineInventoryError(f"duplicate ledger identity: {identity}")
        identities.add(identity)
        rows.append(row)
    return rows, ledger_bytes


def _absolute_lexical(path: Path) -> Path:
    """Make a path absolute without resolving (and thereby hiding) symlinks."""
    return Path(os.path.abspath(path))


def _reject_symlink_components(path: Path) -> None:
    """Reject a symlink at any existing lexical component below filesystem root."""
    absolute = _absolute_lexical(path)
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        try:
            current.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise BaselineInventoryError(f"cannot inspect declared path component: {current}") from exc
        if current.is_symlink():
            raise BaselineInventoryError(f"declared path must not contain a symlink: {current}")


def _reject_symlinks_from(root: Path, relative: Path = Path(".")) -> None:
    root = _absolute_lexical(root)
    _reject_symlink_components(root)
    for part in relative.parts:
        root /= part
        _reject_symlink_components(root)


def _safe_root(root: Path) -> Path:
    root = _absolute_lexical(root)
    _reject_symlinks_from(root)
    return root


def _explicit_relative_path(pointer: str) -> Path:
    candidate = Path(pointer)
    if candidate.is_absolute() or ".." in candidate.parts or not pointer or candidate == Path("."):
        raise BaselineInventoryError("native_artifact_path must be a non-empty relative contained path")
    return candidate


def _artifact_candidates(row: dict[str, Any]) -> list[Path]:
    if "native_artifact_path" in row:
        return [_explicit_relative_path(row["native_artifact_path"])]
    date = row["scan_date"]
    return [Path(date) / f"top1_paper_watchlist_{date}.json"]


def _resolve_artifact(row: dict[str, Any], roots: list[_AnchoredDirectory], source_tiers: dict[str, str]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    relative_candidates = _artifact_candidates(row)
    found: list[tuple[_AnchoredDirectory, Path, _AnchoredFile]] = []
    for root in roots:
        for relative in relative_candidates:
            artifact = _open_relative_regular(root, relative)
            if artifact is not None:
                _after_anchored_open("artifact")
                found.append((root, relative, artifact))
    if len(found) > 1:
        for _, _, artifact in found:
            artifact.close()
        raise BaselineInventoryError(f"ambiguous native artifact for {row['scan_date']}")
    if not found:
        return ({"artifact_root": None, "artifact_path": None, "artifact_sha256": None, "source_tier": None,
                 "resolvable": False, "status": "MISSING_NATIVE_ARTIFACT",
                 "reason": "no deterministic native top1_paper_watchlist artifact found"}, [])
    root, relative, artifact = found[0]
    try:
        artifact_bytes = artifact.read_stable_bytes("native artifact")
        payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BaselineInventoryError(f"native artifact must be valid UTF-8 JSON: {artifact.display}") from exc
    finally:
        artifact.close()
    if (not isinstance(payload, dict) or payload.get("date") != row["scan_date"]
            or payload.get("sleeve") != "top1_paper" or payload.get("paper_only") is not True
            or payload.get("status") != "PAPER_TRACK_ONLY" or "regime" not in payload
            or payload["regime"] != row["regime"]
            or _canonical_json(payload.get("top1")) != _canonical_json(row["top1"])
            or _canonical_json(payload.get("top2")) != _canonical_json(row["top2"])):
        raise BaselineInventoryError(f"native artifact has incompatible top1-paper identity: {root.display / relative}")
    relative_display = relative.as_posix()
    digest = _sha256_bytes(artifact_bytes)
    tier = source_tiers.get(NATIVE_KIND, "UNSPECIFIED_SOURCE_TIER")
    evidence = {"root": str(root.display), "path": relative_display, "sha256": digest}
    return ({"artifact_root": str(root.display), "artifact_path": relative_display, "artifact_sha256": digest, "source_tier": tier,
             "resolvable": True, "status": "RESOLVED_NATIVE_ARTIFACT",
             "reason": "saved native top1_paper_watchlist artifact"}, [evidence])


def _accounting(row: dict[str, Any]) -> dict[str, int]:
    result = {"selected": 1 if row["top1"] is not None else 0,
              "filled": 0, "no_fill": 0, "censored": 0, "unknown": 0}
    if row["top1"] is None:
        return result
    results = row.get("results")
    legs = results.get("legs") if isinstance(results, dict) else None
    if not isinstance(legs, list):
        result["unknown"] = 1
        return result
    top1_legs = [leg for leg in legs if isinstance(leg, dict) and leg.get("rank") == 0]
    if len(top1_legs) != 1:
        result["unknown"] = 1
        return result
    leg = top1_legs[0]
    filled, reason = leg.get("filled"), leg.get("reason")
    if filled is True:
        result["filled"] = 1
    elif filled is False and isinstance(reason, str) and reason.startswith("no_fill"):
        result["no_fill"] = 1
    elif isinstance(reason, str) and reason.startswith("censored"):
        result["censored"] = 1
    else:
        result["unknown"] = 1
    return result


def _git_advisory(project_root: Path) -> dict[str, Any]:
    def run(*args: str) -> str | None:
        completed = subprocess.run(["git", *args], cwd=project_root, text=True, capture_output=True, check=False)
        return completed.stdout.strip() if completed.returncode == 0 else None
    commit = run("rev-parse", "HEAD")
    status = run("status", "--porcelain")
    return {"commit": commit, "dirty": None if status is None else bool(status), "advisory_only": True}


def _portable_manifest_view(document: dict[str, Any]) -> dict[str, Any]:
    """Return the exact environment-independent representation covered by manifest_sha256."""
    return {
        "schema_version": document["schema_version"],
        "manifest_identity": document["manifest_identity"],
        "as_of": document["as_of"],
        "generated": document["generated"],
        "evidence_grade": document["evidence_grade"],
        "promotion_status": document["promotion_status"],
        "capture_provenance_caveat": document["capture_provenance_caveat"],
        "inputs": {
            "ledger": {"sha256": document["inputs"]["ledger"]["sha256"]},
            "artifact_evidence": document["inputs"]["artifact_evidence"],
        },
        "rows": document["rows"],
        "summary": document["summary"],
    }


def _write_new_atomically(output: Path, parent: _AnchoredDirectory, encoded: bytes) -> None:
    temporary_name = f".{output.name}.{secrets.token_hex(16)}.tmp"
    try:
        fd = os.open(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600, dir_fd=parent.fd)
    except FileExistsError:
        # A cryptographic random collision is harmless; fail closed rather than re-resolve paths.
        raise BaselineInventoryError(f"cannot safely create temporary inventory: {output}") from None
    except OSError as exc:
        raise BaselineInventoryError(f"cannot safely create temporary inventory: {output}") from exc
    primary_error: BaseException | None = None
    try:
        written = 0
        while written < len(encoded):
            written += os.write(fd, encoded[written:])
        os.fsync(fd)
        try:
            os.close(fd)
        finally:
            fd = -1
        try:
            os.link(temporary_name, output.name, src_dir_fd=parent.fd, dst_dir_fd=parent.fd, follow_symlinks=False)
        except FileExistsError:
            raise FileExistsError(f"refusing to overwrite existing inventory: {output}") from None
        try:
            os.fsync(parent.fd)
        except OSError as exc:
            raise BaselineInventoryDurabilityUncertainError(
                f"artifact may exist at {output}; durability uncertain; reconcile manually") from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_error: OSError | None = None
        if fd >= 0:
            try:
                os.close(fd)
            except OSError as exc:
                cleanup_error = exc
        try:
            os.unlink(temporary_name, dir_fd=parent.fd)
        except FileNotFoundError:
            pass
        except OSError as exc:
            if cleanup_error is None:
                cleanup_error = exc
        if primary_error is None and cleanup_error is not None:
            raise cleanup_error


def freeze_baseline(*, ledger_path: str | Path, artifact_roots: list[str | Path], output_path: str | Path,
                    as_of: str, source_tiers: dict[str, str] | None = None) -> dict[str, Any]:
    """Validate then atomically create an immutable legacy inventory artifact."""
    ledger = _absolute_lexical(Path(ledger_path))
    output = _absolute_lexical(Path(output_path))
    _reject_symlink_components(ledger)
    _reject_symlink_components(output.parent)
    as_of = _require_date(as_of, "as_of")
    expected_name = f"baseline_inventory_{as_of}.json"
    if output.name != expected_name:
        raise BaselineInventoryError(f"output filename must be {expected_name}")
    source_tiers = dict(source_tiers or {})
    if any(not isinstance(key, str) or not isinstance(value, str) or not value for key, value in source_tiers.items()):
        raise BaselineInventoryError("source tier mapping must contain non-empty string keys and values")
    root_paths = [_safe_root(Path(root)) for root in artifact_roots]
    if len(set(root_paths)) != len(root_paths):
        raise BaselineInventoryError("artifact roots must be unique")
    ledger_file = _open_absolute_regular(ledger)
    output_parent: _AnchoredDirectory | None = None
    roots: list[_AnchoredDirectory] = []
    try:
        _after_anchored_open("ledger")
        output_parent = _open_absolute_directory(output.parent, create=True)
        assert output_parent is not None
        _after_anchored_open("output_parent")
        for root_path in root_paths:
            anchored = _open_absolute_directory(root_path, missing_ok=True)
            if anchored is not None:
                roots.append(anchored)
        rows, ledger_bytes = _read_ledger(ledger_file)
        inventory_rows: list[dict[str, Any]] = []
        artifact_evidence: list[dict[str, str]] = []
        for row in rows:
            artifact, evidence = _resolve_artifact(row, roots, source_tiers)
            artifact.pop("artifact_root")
            account = _accounting(row)
            inventory_rows.append({"scan_date": row["scan_date"], "identity": row["scan_date"],
                                   "ledger_record_sha256": _sha256_bytes(_canonical_json(row)),
                                   "regime": row.get("regime") if isinstance(row.get("regime"), str) and row.get("regime") else "UNKNOWN",
                                   "accounting": account, **artifact})
            artifact_evidence.extend(evidence)
        status_counts = Counter(record["status"] for record in inventory_rows)
        regime_counts = Counter(record["regime"] for record in inventory_rows)
        accounting = {key: sum(record["accounting"][key] for record in inventory_rows)
                      for key in ("selected", "filled", "no_fill", "censored", "unknown")}
        project_root = Path(__file__).resolve().parent.parent
        artifact_evidence.sort(key=lambda item: (item["root"], item["path"]))
        portable_artifact_evidence = [{"path": item["path"], "sha256": item["sha256"]} for item in artifact_evidence]
        document: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "manifest_identity": {
                "algorithm": "sha256",
                "canonicalization": "UTF-8 JSON with ensure_ascii=false, lexicographically sorted object keys, and separators ',' and ':'",
                "hash_covered_fields": [
                    "schema_version", "manifest_identity", "as_of", "generated", "evidence_grade", "promotion_status",
                    "capture_provenance_caveat", "inputs.ledger.sha256", "inputs.artifact_evidence[].path",
                    "inputs.artifact_evidence[].sha256", "rows", "summary",
                ],
                "excluded_advisory_fields": ["generator", "origin_advisory"],
                "interpretation": "Self-consistent legacy inventory hash, not an independent source-authenticity proof.",
            },
            "as_of": as_of,
            "generated": as_of,
            "evidence_grade": EVIDENCE_GRADE,
            "promotion_status": PROMOTION_STATUS,
            "capture_provenance_caveat": "Artifact capture provenance is not retroactively established; this inventory is legacy, non-PIT, non-execution, and non-promotable.",
            "inputs": {"ledger": {"sha256": _sha256_bytes(ledger_bytes)},
                       "artifact_evidence": portable_artifact_evidence},
            "generator": {"source_path": "scripts/freeze_top1_baseline.py", "source_sha256": _sha256_file(Path(__file__)),
                          "git": _git_advisory(project_root), "advisory_only": True},
            "rows": inventory_rows,
            "summary": {"ledger_rows": len(inventory_rows), "artifact_status_counts": dict(sorted(status_counts.items())),
                        "accounting": accounting, "regime_counts": dict(sorted(regime_counts.items()))},
            "origin_advisory": {
                "inputs": {"ledger_path": str(ledger.absolute()), "artifact_roots": [str(root) for root in root_paths],
                           "artifact_evidence": artifact_evidence, "advisory_only": True},
            },
        }
        document["manifest_sha256"] = _sha256_bytes(_canonical_json(_portable_manifest_view(document)))
        encoded = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        _write_new_atomically(output, output_parent, encoded)
        return document
    finally:
        for root in roots:
            root.close()
        if output_parent is not None:
            output_parent.close()
        ledger_file.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze legacy top1 paper ledger evidence without reconstruction")
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--artifact-root", action="append", default=[], type=Path,
                        help="Declared root containing saved top1-paper artifacts; repeatable")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--source-tier", action="append", default=[], metavar="KIND=TIER")
    args = parser.parse_args()
    tiers: dict[str, str] = {}
    for item in args.source_tier:
        kind, separator, tier = item.partition("=")
        if not separator or not kind or not tier or kind in tiers:
            parser.error("--source-tier must be unique KIND=TIER")
        tiers[kind] = tier
    try:
        document = freeze_baseline(ledger_path=args.ledger, artifact_roots=args.artifact_root,
                                   output_path=args.output, as_of=args.as_of, source_tiers=tiers)
    except (BaselineInventoryError, FileExistsError) as exc:
        parser.error(str(exc))
    print(json.dumps({"output": str(args.output), "manifest_sha256": document["manifest_sha256"],
                      "summary": document["summary"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
