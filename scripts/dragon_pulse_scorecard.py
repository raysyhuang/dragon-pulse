#!/usr/bin/env python3
"""Fail-honest PAPER-ONLY scorecard intake for the Dragon Pulse top-1 ledger.

This is a read-only reporter.  It deliberately does not evaluate positions, fetch
market data, inspect configuration/secrets, or write production artifacts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
NOT_EMITTED = "NOT_EMITTED_CANNOT_VERIFY"


class ScorecardValidationError(ValueError):
    """An input was malformed or contradicted an explicit scorecard contract."""


def _parse_date(value: object, field: str) -> date:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise ScorecardValidationError(f"{field} must be a YYYY-MM-DD string")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ScorecardValidationError(f"{field} must be a YYYY-MM-DD string") from exc


def _read_json(path: Path, label: str) -> tuple[Any, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ScorecardValidationError(f"cannot read {label}: {path}") from exc
    try:
        return json.loads(raw), hashlib.sha256(raw).hexdigest()
    except json.JSONDecodeError as exc:
        raise ScorecardValidationError(f"malformed JSON in {label}: {path}") from exc


def _artifact(path: Path, digest: str) -> dict[str, str]:
    return {"path": str(path), "sha256": digest}


def _read_ledger(path: Path, as_of: date) -> tuple[list[dict[str, Any]], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ScorecardValidationError(f"cannot read ledger: {path}") from exc
    rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for line_number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ScorecardValidationError(f"malformed ledger JSON at line {line_number}") from exc
        if not isinstance(row, dict):
            raise ScorecardValidationError(f"ledger row {line_number} must be an object")
        for field in ("scan_date", "evaluated", "results", "top1", "top2"):
            if field not in row:
                raise ScorecardValidationError(f"ledger row {line_number} missing {field}")
        scan_date = _parse_date(row["scan_date"], f"ledger row {line_number}.scan_date")
        if scan_date > as_of:
            raise ScorecardValidationError(f"ledger row {line_number} is after --as-of")
        if row["scan_date"] in seen_dates:
            raise ScorecardValidationError(f"duplicate ledger scan_date {row['scan_date']}")
        seen_dates.add(row["scan_date"])
        if not isinstance(row["evaluated"], bool):
            raise ScorecardValidationError(f"ledger row {line_number}.evaluated must be boolean")
        if not isinstance(row["top2"], list) or not isinstance(row["top1"], (dict, type(None))):
            raise ScorecardValidationError(f"ledger row {line_number} has invalid pick fields")
        if row["evaluated"] and (not isinstance(row["results"], dict) or not isinstance(row["results"].get("legs"), list)):
            raise ScorecardValidationError(f"evaluated ledger row {line_number} requires results.legs")
        if not row["evaluated"] and row["results"] is not None:
            raise ScorecardValidationError(f"pending ledger row {line_number} must have null results")
        rows.append(row)
    return rows, hashlib.sha256(raw).hexdigest()


def _read_summary(path: Path, evaluated_count: int) -> tuple[dict[str, Any], str]:
    payload, digest = _read_json(path, "summary")
    if not isinstance(payload, dict):
        raise ScorecardValidationError("summary must be a JSON object")
    if "evaluated_days" in payload:
        if not isinstance(payload["evaluated_days"], int) or isinstance(payload["evaluated_days"], bool):
            raise ScorecardValidationError("summary.evaluated_days must be an integer")
        if payload["evaluated_days"] != evaluated_count:
            raise ScorecardValidationError("summary.evaluated_days contradicts ledger")
    return payload, digest


def _validate_primary_payload(payload: Any, label: str, expected_date: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ScorecardValidationError(f"{label} must be a JSON object")
    if payload.get("date") != expected_date:
        raise ScorecardValidationError(f"{label}.date must equal --as-of")
    if not isinstance(payload.get("picks"), list):
        raise ScorecardValidationError(f"{label}.picks must be a list")
    for field in ("regime",):
        if field in payload and payload[field] is not None and not isinstance(payload[field], str):
            raise ScorecardValidationError(f"{label}.{field} must be a string or null")
    for field in ("universe_size", "downloaded", "download_failed"):
        if field in payload and (not isinstance(payload[field], int) or isinstance(payload[field], bool)):
            raise ScorecardValidationError(f"{label}.{field} must be an integer")
    for field in ("generated_utc", "download_health"):
        if field in payload and payload[field] is not None and not isinstance(payload[field], str):
            raise ScorecardValidationError(f"{label}.{field} must be a string or null")
    if "provider_provenance" in payload and not isinstance(payload["provider_provenance"], (str, dict, list, type(None))):
        raise ScorecardValidationError(f"{label}.provider_provenance must be literal JSON")
    return payload


def _primary_intake(primary_dir: Path | None, as_of: str) -> tuple[str, dict[str, Any], dict[str, dict[str, str]], Any]:
    if primary_dir is None:
        return "PRIMARY_ARTIFACT_UNAVAILABLE", {"availability": "UNAVAILABLE", "missing_reason": "primary directory not supplied", "watchlist": None, "scan": None}, {}, NOT_EMITTED
    if not primary_dir.is_dir():
        return "PRIMARY_ARTIFACT_UNAVAILABLE", {"availability": "UNAVAILABLE", "missing_reason": "primary directory does not exist", "watchlist": None, "scan": None}, {}, NOT_EMITTED

    expected = {
        "watchlist": primary_dir / f"execution_watchlist_{as_of}.json",
        "scan": primary_dir / f"scan_results_{as_of}.json",
    }
    missing = [name for name, path in expected.items() if not path.is_file()]
    if missing:
        wrong_date = sorted(primary_dir.glob("execution_watchlist_*.json")) + sorted(primary_dir.glob("scan_results_*.json"))
        if wrong_date:
            raise ScorecardValidationError("wrong-date primary artifact(s) supplied; refusing substitution")
        return "PRIMARY_ARTIFACT_UNAVAILABLE", {"availability": "UNAVAILABLE", "missing_reason": f"missing current primary artifact(s): {', '.join(missing)}", "watchlist": None, "scan": None}, {}, NOT_EMITTED

    watchlist, watch_hash = _read_json(expected["watchlist"], "execution watchlist")
    scan, scan_hash = _read_json(expected["scan"], "scan results")
    watchlist = _validate_primary_payload(watchlist, "execution_watchlist", as_of)
    scan = _validate_primary_payload(scan, "scan_results", as_of)
    for field in ("regime", "universe_size", "picks"):
        if field in watchlist and field in scan and watchlist[field] != scan[field]:
            raise ScorecardValidationError(f"primary artifacts contradict on {field}")
    provenance_values = [payload["provider_provenance"] for payload in (watchlist, scan) if "provider_provenance" in payload]
    if len(provenance_values) == 2 and provenance_values[0] != provenance_values[1]:
        raise ScorecardValidationError("primary artifacts contradict on provider_provenance")
    provenance = provenance_values[0] if provenance_values else NOT_EMITTED
    intake = {
        "availability": "AVAILABLE_CURRENT_DATE",
        "missing_reason": None,
        "watchlist": {key: watchlist[key] for key in ("date", "regime", "universe_size", "picks") if key in watchlist},
        "scan": {key: scan[key] for key in ("date", "generated_utc", "regime", "universe_size", "downloaded", "download_failed", "download_health", "picks") if key in scan},
    }
    hashes = {"execution_watchlist": _artifact(expected["watchlist"], watch_hash), "scan_results": _artifact(expected["scan"], scan_hash)}
    return "READY", intake, hashes, provenance


def build_scorecard(ledger_path: Path, summary_path: Path, as_of: str, primary_dir: Path | None = None) -> dict[str, Any]:
    """Build a deterministic, read-only report.  All dates derive from ``as_of``."""
    as_of_date = _parse_date(as_of, "--as-of")
    rows, ledger_hash = _read_ledger(Path(ledger_path), as_of_date)
    evaluated = [row for row in rows if row["evaluated"]]
    _, summary_hash = _read_summary(Path(summary_path), len(evaluated))
    latest = max((_parse_date(row["scan_date"], "ledger.scan_date") for row in evaluated), default=None)
    freshness = {
        "latest_evaluated_scan_date": latest.isoformat() if latest else None,
        "staleness_calendar_days": (as_of_date - latest).days if latest else None,
        "status": "FRESH_AS_OF" if latest == as_of_date else ("STALE" if latest else "NO_EVALUATED_ROWS"),
    }
    primary_status, primary, primary_hashes, provenance = _primary_intake(primary_dir, as_of)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_status": primary_status,
        "as_of": as_of,
        "labels": {"paper_only": True, "pnl": "NON_PNL", "promotion": "NON_PROMOTABLE"},
        "ledger": {"rows": {"total": len(rows), "evaluated": len(evaluated), "pending": len(rows) - len(evaluated)}},
        "evaluation_freshness": freshness,
        "primary_artifacts": primary,
        "provider_provenance": provenance,
        "source_artifacts": {"ledger": _artifact(Path(ledger_path), ledger_hash), "summary": _artifact(Path(summary_path), summary_hash), **primary_hashes},
    }


def render_human(report: dict[str, Any]) -> str:
    freshness = report["evaluation_freshness"]
    rows = report["ledger"]["rows"]
    primary = report["primary_artifacts"]
    return "\n".join((
        f"Dragon Pulse scorecard | PAPER ONLY / NON-P&L / NON-PROMOTABLE | as-of {report['as_of']}",
        f"Ledger: total={rows['total']} evaluated={rows['evaluated']} pending={rows['pending']}",
        f"Evaluation freshness: {freshness['status']} latest_evaluated_scan_date={freshness['latest_evaluated_scan_date']} staleness_calendar_days={freshness['staleness_calendar_days']}",
        f"Current primary scan: {primary['availability']}" + (f" ({primary['missing_reason']})" if primary["missing_reason"] else ""),
        f"Provider provenance: {report['provider_provenance']}",
        f"Report status: {report['report_status']}",
    ))


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only fail-honest Dragon Pulse PAPER-ONLY scorecard")
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--as-of", required=True, help="YYYY-MM-DD; no implicit current date")
    parser.add_argument("--primary-dir", type=Path, help="directory containing current-date primary JSON artifacts")
    parser.add_argument("--human", action="store_true", help="print concise 17:15 cron text instead of JSON")
    args = parser.parse_args()
    try:
        report = build_scorecard(args.ledger, args.summary, args.as_of, args.primary_dir)
    except ScorecardValidationError as exc:
        parser.error(str(exc))
    print(render_human(report) if args.human else json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
