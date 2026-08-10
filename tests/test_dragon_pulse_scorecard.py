"""Contract tests for the fail-honest Dragon Pulse paper scorecard."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import math
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "dragon_pulse_scorecard.py"
_MODULE = None


def _load_module():
    global _MODULE
    if _MODULE is None:
        spec = importlib.util.spec_from_file_location("dragon_pulse_scorecard", SCRIPT)
        assert spec is not None and spec.loader is not None
        _MODULE = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_MODULE)
    return _MODULE


def _row(date: str, evaluated: bool) -> dict:
    row = {
        "scan_date": date,
        "regime": "bull",
        "n_live_picks": 1,
        "top1": {"ticker": "600000.SH"},
        "top2": [{"ticker": "600000.SH"}],
        "evaluated": evaluated,
        "results": None,
    }
    if not evaluated:
        row.update(top1=None, top2=[])
        return row
    row["results"] = {
        "legs": [{
            "rank": 0, "ticker": "600000.SH", "entry_date": date, "exit_date": date,
            "ret_pct": 0.0, "filled": True, "reason": "target",
        }],
        "top1_ret_pct": 0.0, "top2_ret_pct": 0.0, "csi300_ret_pct": None,
        "entry_date": date, "exit_date": date,
    }
    return row


def _tracker_pending_row(date: str = "2026-08-10") -> dict:
    """Representative `top1_paper_track.py record` output with two live picks."""
    first = {
        "ticker": "600000.SH", "name_cn": "浦发银行", "score": 91.5,
        "entry_price": 10.0, "max_entry_price": 10.2, "stop_loss": 9.5,
        "target_1": 11.0, "holding_period": 5,
    }
    second = {
        "ticker": "000001.SZ", "name_cn": "平安银行", "score": 90.0,
        "entry_price": 11.0, "max_entry_price": 11.2, "stop_loss": 10.5,
        "target_1": 12.0, "holding_period": 5,
    }
    return {
        "scan_date": date, "regime": "bull", "n_live_picks": 2,
        "top1": first, "top2": [first, second], "evaluated": False, "results": None,
    }


def _write_inputs(tmp_path: Path, rows: list[dict]) -> tuple[Path, Path]:
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"evaluated_days": sum(row["evaluated"] for row in rows)}), encoding="utf-8")
    return ledger, summary


def _primary(tmp_path: Path, date: str = "2026-08-10", *, provenance: object = ...):
    directory = tmp_path / "primary"
    directory.mkdir()
    watchlist = {"date": date, "regime": "bear", "universe_size": 997, "picks": []}
    scan = {"date": date, "generated_utc": f"{date}T00:04:14.123456Z", "regime": "bear", "regime_detail": {},
            "universe_size": 997, "downloaded": 997, "download_failed": 0, "download_health": "ok",
            "circuit_breaker": False, "signals_total": 0, "picks": [], "errors": []}
    if provenance is not ...:
        scan["provider_provenance"] = provenance
    (directory / f"execution_watchlist_{date}.json").write_text(json.dumps(watchlist), encoding="utf-8")
    (directory / f"scan_results_{date}.json").write_text(json.dumps(scan), encoding="utf-8")
    return directory


def _report(tmp_path: Path, rows: list[dict], *, as_of="2026-08-10", primary=None):
    ledger, summary = _write_inputs(tmp_path, rows)
    return _load_module().build_scorecard(ledger, summary, as_of, primary)


def test_stale_ledger_is_distinct_from_current_primary_and_uses_explicit_as_of(tmp_path):
    primary = _primary(tmp_path)
    report = _report(tmp_path, [_row("2026-07-02", True), _row("2026-08-09", False)], primary=primary)

    assert report["schema_version"] == 1
    assert report["labels"] == {"paper_only": True, "pnl": "NON_PNL", "promotion": "NON_PROMOTABLE"}
    assert report["as_of"] == "2026-08-10"
    assert report["ledger"]["rows"] == {"total": 2, "evaluated": 1, "pending": 1}
    assert report["evaluation_freshness"] == {"latest_evaluated_scan_date": "2026-07-02", "staleness_calendar_days": 39, "status": "STALE"}
    assert report["primary_artifacts"]["availability"] == "AVAILABLE_CURRENT_DATE"
    assert report["primary_artifacts"]["scan"]["date"] == "2026-08-10"
    assert report["provider_provenance"] == "NOT_EMITTED_CANNOT_VERIFY"
    assert "STALE" in _load_module().render_human(report)


def test_tracker_record_pending_selection_shape_is_accepted(tmp_path):
    report = _report(tmp_path, [_tracker_pending_row()])

    assert report["ledger"]["rows"] == {"total": 1, "evaluated": 0, "pending": 1}


@pytest.mark.parametrize("mutate", [
    lambda row: row.update(results={}),
    lambda row: row.update(top1=None),
    lambda row: row.update(top2=[]),
    lambda row: row.update(top1={"ticker": "600000.SH"}, top2=[{"ticker": "000001.SZ"}]),
    lambda row: row.update(top2=[{"ticker": ""}]),
    lambda row: row.update(top2=["not-an-object"]),
])
def test_pending_rows_reject_mixed_or_malformed_selection_shapes(tmp_path, mutate):
    row = _tracker_pending_row()
    mutate(row)

    with pytest.raises(_load_module().ScorecardValidationError):
        _report(tmp_path, [row])


def test_fresh_ledger_and_literal_provider_provenance_are_reported_without_fallback_claim(tmp_path):
    provenance = {"primary": "literal-primary", "backup": "literal-backup"}
    report = _report(tmp_path, [_row("2026-08-10", True)], primary=_primary(tmp_path, provenance=provenance))

    assert report["evaluation_freshness"] == {"latest_evaluated_scan_date": "2026-08-10", "staleness_calendar_days": 0, "status": "FRESH_AS_OF"}
    assert report["provider_provenance"] == provenance
    assert "fallback" not in json.dumps(report).lower()


def test_missing_primary_is_truthful_nonfatal_unavailable_report(tmp_path):
    report = _report(tmp_path, [_row("2026-08-10", True)], primary=tmp_path / "absent")

    assert report["report_status"] == "PRIMARY_ARTIFACT_UNAVAILABLE"
    assert report["primary_artifacts"] == {
        "availability": "UNAVAILABLE", "missing_reason": "primary directory does not exist",
        "watchlist": None, "scan": None,
    }


def test_wrong_date_primary_artifacts_fail_closed(tmp_path):
    primary = _primary(tmp_path, date="2026-08-09")
    with pytest.raises(_load_module().ScorecardValidationError, match="wrong-date"):
        _report(tmp_path, [_row("2026-08-10", True)], primary=primary)


def test_hashes_cover_all_supplied_source_artifacts(tmp_path):
    primary = _primary(tmp_path)
    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    report = _load_module().build_scorecard(ledger, summary, "2026-08-10", primary)

    hashes = report["source_artifacts"]
    assert hashes["ledger"]["sha256"] == hashlib.sha256(ledger.read_bytes()).hexdigest()
    assert hashes["summary"]["sha256"] == hashlib.sha256(summary.read_bytes()).hexdigest()
    assert hashes["execution_watchlist"]["sha256"] == hashlib.sha256((primary / "execution_watchlist_2026-08-10.json").read_bytes()).hexdigest()
    assert hashes["scan_results"]["sha256"] == hashlib.sha256((primary / "scan_results_2026-08-10.json").read_bytes()).hexdigest()


@pytest.mark.parametrize("ledger_text, summary_text", [
    ("{not-json}\n", "{}"),
    (json.dumps({"scan_date": "2026-08-10", "evaluated": True, "results": None}) + "\n", "{}"),
    (json.dumps(_row("2026-08-10", True)) + "\n", "[]"),
])
def test_malformed_or_contradictory_inputs_fail_closed(tmp_path, ledger_text, summary_text):
    ledger = tmp_path / "ledger.jsonl"
    summary = tmp_path / "summary.json"
    ledger.write_text(ledger_text, encoding="utf-8")
    summary.write_text(summary_text, encoding="utf-8")

    with pytest.raises(_load_module().ScorecardValidationError):
        _load_module().build_scorecard(ledger, summary, "2026-08-10", None)


def test_primary_payload_date_mismatch_fails_closed(tmp_path):
    primary = _primary(tmp_path)
    path = primary / "scan_results_2026-08-10.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["date"] = "2026-08-09"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(_load_module().ScorecardValidationError, match="date"):
        _report(tmp_path, [_row("2026-08-10", True)], primary=primary)


def test_dates_must_use_literal_extended_iso_format(tmp_path):
    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])

    with pytest.raises(_load_module().ScorecardValidationError, match="YYYY-MM-DD"):
        _load_module().build_scorecard(ledger, summary, "20260810")


def test_duplicate_summary_key_fails_closed(tmp_path):
    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    summary.write_bytes(b'{"evaluated_days": 1, "evaluated_days": 1}')

    with pytest.raises(_load_module().ScorecardValidationError, match="duplicate"):
        _load_module().build_scorecard(ledger, summary, "2026-08-10")


@pytest.mark.parametrize("bad_json", [b'{"value": NaN}', b'{"value": Infinity}', b'{"value": -Infinity}'])
def test_nonfinite_json_is_rejected_for_ledger_and_primary(tmp_path, bad_json):
    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    ledger.write_bytes(bad_json + b"\n")
    with pytest.raises(_load_module().ScorecardValidationError, match="non-finite"):
        _load_module().build_scorecard(ledger, summary, "2026-08-10")

    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    primary = _primary(tmp_path)
    (primary / "scan_results_2026-08-10.json").write_bytes(bad_json)
    with pytest.raises(_load_module().ScorecardValidationError, match="non-finite"):
        _load_module().build_scorecard(ledger, summary, "2026-08-10", primary)


def test_malformed_utf8_and_truncated_primary_fail_closed(tmp_path):
    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    summary.write_bytes(b'{"evaluated_days": "\xff"}')
    with pytest.raises(_load_module().ScorecardValidationError, match="UTF-8"):
        _load_module().build_scorecard(ledger, summary, "2026-08-10")

    ledger, summary = _write_inputs(tmp_path, [_row("2026-08-10", True)])
    primary = _primary(tmp_path)
    (primary / "scan_results_2026-08-10.json").write_bytes(b'{"date": "2026-08-10"')
    with pytest.raises(_load_module().ScorecardValidationError, match="malformed JSON"):
        _load_module().build_scorecard(ledger, summary, "2026-08-10", primary)


@pytest.mark.parametrize("field", ["regime_detail", "downloaded", "download_failed", "download_health", "circuit_breaker", "signals_total", "errors"])
def test_scan_requires_each_minimal_producer_field(tmp_path, field):
    primary = _primary(tmp_path)
    path = primary / "scan_results_2026-08-10.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload[field]
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(_load_module().ScorecardValidationError, match=field):
        _report(tmp_path, [_row("2026-08-10", True)], primary=primary)


@pytest.mark.parametrize("mutate", [
    lambda row: row.update(top1=None),
    lambda row: row.update(top2=[]),
    lambda row: row.update(results={"legs": []}),
    lambda row: row["results"]["legs"][0].update(ret_pct=math.nan),
    lambda row: row["results"]["legs"][0].update(entry_date="not-a-date"),
])
def test_empty_or_invalid_evaluated_ledger_rows_fail_closed(tmp_path, mutate):
    row = _row("2026-08-10", True)
    mutate(row)

    with pytest.raises(_load_module().ScorecardValidationError):
        _report(tmp_path, [row])


def test_real_ledger_artifact_remains_accepted():
    root = Path(__file__).resolve().parents[1]
    report = _load_module().build_scorecard(
        root / "outputs/top1_paper/ledger.jsonl", root / "outputs/top1_paper/summary.json", "2026-08-10"
    )

    assert report["ledger"]["rows"] == {"total": 35, "evaluated": 35, "pending": 0}
