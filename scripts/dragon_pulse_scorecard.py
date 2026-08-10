#!/usr/bin/env python3
"""Fail-honest PAPER-ONLY scorecard intake for the Dragon Pulse top-1 ledger.

This is a read-only reporter.  It deliberately does not evaluate positions, fetch
market data, inspect configuration/secrets, or write production artifacts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from datetime import date, datetime
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


def _reject_json_constant(value: str) -> None:
    raise ScorecardValidationError(f"non-finite JSON value {value!r} is not allowed")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ScorecardValidationError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def _reject_nonfinite_values(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ScorecardValidationError("non-finite JSON value is not allowed")
    if isinstance(value, dict):
        for item in value.values():
            _reject_nonfinite_values(item)
    elif isinstance(value, list):
        for item in value:
            _reject_nonfinite_values(item)


def _strict_json_loads(raw: bytes, label: str) -> Any:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScorecardValidationError(f"malformed UTF-8 in {label}") from exc
    try:
        value = json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_reject_json_constant)
    except ScorecardValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise ScorecardValidationError(f"malformed JSON in {label}") from exc
    _reject_nonfinite_values(value)
    return value


def _read_json(path: Path, label: str) -> tuple[Any, str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ScorecardValidationError(f"cannot read {label}: {path}") from exc
    return _strict_json_loads(raw, f"{label}: {path}"), hashlib.sha256(raw).hexdigest()


def _artifact(path: Path, digest: str) -> dict[str, str]:
    return {"path": str(path), "sha256": digest}


def _read_ledger(path: Path, as_of: date) -> tuple[list[dict[str, Any]], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ScorecardValidationError(f"cannot read ledger: {path}") from exc
    rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ScorecardValidationError("malformed UTF-8 in ledger") from exc
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = _strict_json_loads(line.encode("utf-8"), f"ledger JSON at line {line_number}")
        except ScorecardValidationError as exc:
            raise ScorecardValidationError(str(exc)) from exc
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
        if row["evaluated"]:
            _validate_evaluated_ledger_row(row, line_number)
        else:
            _validate_pending_ledger_row(row, line_number)
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


def _nonnegative_int(value: object, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ScorecardValidationError(f"{field} must be a nonnegative integer")


def _finite_number(value: object, field: str, *, nullable: bool = False) -> None:
    if value is None and nullable:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ScorecardValidationError(f"{field} must be a finite number" + (" or null" if nullable else ""))


def _nonempty_string(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ScorecardValidationError(f"{field} must be a nonempty string")


def _validate_pending_ledger_row(row: dict[str, Any], line_number: int) -> None:
    prefix = f"pending ledger row {line_number}"
    if row["results"] is not None:
        raise ScorecardValidationError(f"{prefix} must have null results")
    if row["top1"] is None and row["top2"] == []:
        return
    if not isinstance(row["top1"], dict) or not isinstance(row["top2"], list) or not row["top2"]:
        raise ScorecardValidationError(f"{prefix} requires either no picks or a top1 object and nonempty top2 list")
    _nonempty_string(row["top1"].get("ticker"), f"{prefix}.top1.ticker")
    for index, pick in enumerate(row["top2"]):
        pick_prefix = f"{prefix}.top2[{index}]"
        if not isinstance(pick, dict):
            raise ScorecardValidationError(f"{pick_prefix} must be an object")
        _nonempty_string(pick.get("ticker"), f"{pick_prefix}.ticker")
    if row["top1"]["ticker"] != row["top2"][0]["ticker"]:
        raise ScorecardValidationError(f"{prefix}.top1 ticker must equal top2[0] ticker")


def _validate_evaluated_ledger_row(row: dict[str, Any], line_number: int) -> None:
    prefix = f"evaluated ledger row {line_number}"
    if not isinstance(row["top1"], dict) or not isinstance(row["top2"], list) or not row["top2"]:
        raise ScorecardValidationError(f"{prefix} requires top1 object and nonempty top2 list")
    results = row["results"]
    if not isinstance(results, dict) or not isinstance(results.get("legs"), list) or not results["legs"]:
        raise ScorecardValidationError(f"{prefix} requires results with nonempty legs")
    for index, leg in enumerate(results["legs"]):
        leg_prefix = f"{prefix}.results.legs[{index}]"
        if not isinstance(leg, dict):
            raise ScorecardValidationError(f"{leg_prefix} must be an object")
        _nonnegative_int(leg.get("rank"), f"{leg_prefix}.rank")
        _nonempty_string(leg.get("ticker"), f"{leg_prefix}.ticker")
        _parse_date(leg.get("entry_date"), f"{leg_prefix}.entry_date")
        _parse_date(leg.get("exit_date"), f"{leg_prefix}.exit_date")
        _finite_number(leg.get("ret_pct"), f"{leg_prefix}.ret_pct")
        if not isinstance(leg.get("filled"), bool):
            raise ScorecardValidationError(f"{leg_prefix}.filled must be boolean")
        _nonempty_string(leg.get("reason"), f"{leg_prefix}.reason")
    for field in ("top1_ret_pct", "top2_ret_pct"):
        _finite_number(results.get(field), f"{prefix}.results.{field}")
    _finite_number(results.get("csi300_ret_pct"), f"{prefix}.results.csi300_ret_pct", nullable=True)
    _parse_date(results.get("entry_date"), f"{prefix}.results.entry_date")
    _parse_date(results.get("exit_date"), f"{prefix}.results.exit_date")


def _parse_utc_timestamp(value: object, field: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z?", value) is None:
        raise ScorecardValidationError(f"{field} must be a UTC ISO timestamp")
    try:
        datetime.fromisoformat(value.removesuffix("Z") + ("+00:00" if value.endswith("Z") else ""))
    except ValueError as exc:
        raise ScorecardValidationError(f"{field} must be a UTC ISO timestamp") from exc


def _validate_primary_payload(payload: Any, label: str, expected_date: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ScorecardValidationError(f"{label} must be a JSON object")
    _parse_date(payload.get("date"), f"{label}.date")
    if payload.get("date") != expected_date:
        raise ScorecardValidationError(f"{label}.date must equal --as-of")
    _nonempty_string(payload.get("regime"), f"{label}.regime")
    _nonnegative_int(payload.get("universe_size"), f"{label}.universe_size")
    if not isinstance(payload.get("picks"), list):
        raise ScorecardValidationError(f"{label}.picks must be a list")
    if label == "scan_results":
        _parse_utc_timestamp(payload.get("generated_utc"), f"{label}.generated_utc")
        if not isinstance(payload.get("regime_detail"), dict):
            raise ScorecardValidationError(f"{label}.regime_detail must be an object")
        for field in ("downloaded", "download_failed", "signals_total"):
            _nonnegative_int(payload.get(field), f"{label}.{field}")
        _nonempty_string(payload.get("download_health"), f"{label}.download_health")
        if "circuit_breaker" not in payload or (payload["circuit_breaker"] is not None and not isinstance(payload["circuit_breaker"], bool)):
            raise ScorecardValidationError(f"{label}.circuit_breaker must be boolean or null")
        if not isinstance(payload.get("errors"), list):
            raise ScorecardValidationError(f"{label}.errors must be a list")
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
