"""Safety tests for the ChiNext timing paper sleeve.

An external audit found the sleeve had no timing-specific tests while demanding them of
everything else. These cover the properties whose failure would be silent: lookahead,
backfill-versus-forward labelling, ledger integrity under restatement, plaintext
credentials, unbounded queries, and divergence from the study implementation.

All offline. No provider call.
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
import sys

import numpy as np
import pandas as pd
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


sleeve = _load("chinext_timing_paper_sleeve")
study = _load("edge_test_trend_timing")


def _full_row(**over):
    row = {k: 0 for k in sleeve.ROW_SCHEMA}
    row["trade_date"] = "2026-01-05"
    row.update(over)
    return row


def synthetic(n: int = 900, seed: int = 7) -> pd.DataFrame:
    """A price path with genuine trend and reversal, so the signal actually toggles."""
    rng = np.random.default_rng(seed)
    drift = np.concatenate([np.full(n // 3, 0.0012), np.full(n // 3, -0.0015),
                            np.full(n - 2 * (n // 3), 0.0008)])
    ret = drift + rng.normal(0, 0.015, n)
    close = 1000 * np.exp(np.cumsum(ret))
    gap = 1 + rng.normal(0, 0.004, n)          # overnight gap -> open != close
    return pd.DataFrame({"trade_date": pd.bdate_range("2015-01-01", periods=n),
                         "open": close / gap, "close": close})


# --------------------------------------------------------------------------------------
# Credential and query safety
# --------------------------------------------------------------------------------------


def test_provider_url_is_https_never_plaintext():
    """The token travels in the POST body; over http it is sent in cleartext, and a
    scheduled job turns occasional exposure into daily exposure."""
    src = (ROOT / "scripts" / "chinext_timing_paper_sleeve.py").read_text()
    assert "http://api.tushare.pro" not in src
    assert "https://api.tushare.pro" in src


def test_entry_day_uses_open_to_close_not_full_close_to_close():
    """The executable-fill guard: on a switching session the sleeve must not credit the
    full close-to-close move, which would assume trading at the close it just observed."""
    src = (ROOT / "scripts" / "chinext_timing_paper_sleeve.py").read_text()
    assert "entry_adj" in src and "c / o - 1" in src


def test_query_window_is_closed_never_open_ended():
    """A far-future or open end date makes the query non-reproducible."""
    src = (ROOT / "scripts" / "chinext_timing_paper_sleeve.py").read_text()
    assert "20991231" not in src, "query end date must not be an open-ended sentinel"
    assert 'strftime("%Y%m%d")' in src


# --------------------------------------------------------------------------------------
# The rule itself
# --------------------------------------------------------------------------------------


def test_no_lookahead_position_is_the_prior_sessions_signal():
    rows = sleeve.build_rows(synthetic())
    for prev, cur in zip(rows, rows[1:]):
        assert cur["position_held_today"] == prev["signal_today"], (
            f"{cur['trade_date']}: position must equal the PRIOR session's signal")


def test_position_next_session_equals_todays_signal():
    for r in sleeve.build_rows(synthetic()):
        assert r["position_next_session"] == r["signal_today"]


def test_signal_toggles_in_the_fixture():
    """Guards the tests above from passing vacuously on a never-changing signal."""
    sigs = {r["signal_today"] for r in sleeve.build_rows(synthetic())}
    assert sigs == {True, False}


def test_sleeve_matches_the_study_implementation():
    """The live sleeve and the backtest must be the same rule, or forward evidence
    cannot be compared with the historical result."""
    df = synthetic()
    rows = sleeve.build_rows(df)
    cur = study.equity_curve(df, sleeve.FAST, sleeve.SLOW, fill="next_open",
                             cash_annual=sleeve.CASH_ANNUAL)
    bt = {d.strftime("%Y-%m-%d"): (float(p), float(n))
          for d, p, n in zip(cur["date"], cur["pos"], cur["net"])}
    compared = 0
    for r in rows:
        if r["trade_date"] not in bt:
            continue
        compared += 1
        pos, net = bt[r["trade_date"]]
        assert bool(pos) == r["position_held_today"]
        # the study carries no cash yield; add it to compare like for like.
        assert r["net_return"] == pytest.approx(net, abs=1e-8)
    assert compared > 500


# --------------------------------------------------------------------------------------
# Evidence labelling
# --------------------------------------------------------------------------------------


def test_rows_before_inception_are_backfill_not_forward_paper():
    df = synthetic()
    rows = sleeve.build_rows(df, inception=None)
    assert {r["record_origin"] for r in rows} == {"BACKFILLED_FROM_HISTORY"}


def test_inception_boundary_splits_backfill_from_forward():
    """The inception session is the last OBSERVED backfill row, so it is itself backfill.
    Only sessions appended AFTER it were tracked forward rather than reconstructed."""
    df = synthetic()
    rows_all = sleeve.build_rows(df)
    cut = rows_all[len(rows_all) // 2]["trade_date"]
    rows = sleeve.build_rows(df, inception=cut)
    at_cut = [r for r in rows if r["trade_date"] == cut]
    before = [r for r in rows if r["trade_date"] < cut]
    after = [r for r in rows if r["trade_date"] > cut]
    assert before and after and at_cut
    assert {r["record_origin"] for r in before} == {"BACKFILLED_FROM_HISTORY"}
    assert at_cut[0]["record_origin"] == "BACKFILLED_FROM_HISTORY", (
        "the inception session was reconstructed, not tracked forward")
    assert {r["record_origin"] for r in after} == {"FORWARD_PAPER"}


def test_every_row_carries_non_promotable_labels():
    for r in sleeve.build_rows(synthetic())[:50]:
        assert r["evidence_label"] == "RESEARCH_ONLY_NON_BINDING"
        assert r["execution_status"] == "PAPER_ONLY_NO_ORDERS"


# --------------------------------------------------------------------------------------
# Ledger integrity
# --------------------------------------------------------------------------------------


def test_restated_values_for_a_recorded_date_are_detected():
    """A date-only dedupe would silently keep the old row and drop a restatement."""
    rows = sleeve.build_rows(synthetic())
    prior = dict(rows[10])
    fresh = dict(rows[10]);  fresh["close"] = prior["close"] * 1.05
    assert "close" in sleeve.conflicts(prior, fresh)


def test_identical_rows_are_not_flagged_as_conflicts():
    rows = sleeve.build_rows(synthetic())
    assert sleeve.conflicts(dict(rows[10]), dict(rows[10])) == []


@pytest.mark.parametrize("field", ["signal_today", "position_held_today", "net_return"])
def test_conflicts_cover_every_decision_field(field):
    rows = sleeve.build_rows(synthetic())
    prior = dict(rows[20])
    fresh = dict(rows[20])
    fresh[field] = (not prior[field]) if isinstance(prior[field], bool) else prior[field] + 1
    assert field in sleeve.conflicts(prior, fresh)


def test_corrupt_ledger_with_conflicting_duplicates_is_refused(tmp_path, monkeypatch):
    led = tmp_path / "ledger.jsonl"
    a = _full_row(close=1.0)
    b = _full_row(close=2.0)
    led.write_text(json.dumps(a) + "\n" + json.dumps(b) + "\n")
    monkeypatch.setattr(sleeve, "LEDGER", led)
    with pytest.raises(SystemExit, match="conflicting"):
        sleeve.existing_rows()


def test_duplicate_identical_rows_are_tolerated(tmp_path, monkeypatch):
    led = tmp_path / "ledger.jsonl"
    row = _full_row(close=1.0)
    led.write_text(json.dumps(row) + "\n" + json.dumps(row) + "\n")
    monkeypatch.setattr(sleeve, "LEDGER", led)
    assert set(sleeve.existing_rows()) == {"2026-01-05"}


# --------------------------------------------------------------------------------------
# Recovery, schema, and fail-closed provider paths
#
# Added after an audit found: a false multi-file atomicity claim, conflict checks that a
# truncated row could evade, and malformed/empty provider responses that still wrote.
# --------------------------------------------------------------------------------------


def test_partial_ledger_row_is_refused(tmp_path, monkeypatch):
    """A truncated row must not be accepted; missing fields would skip comparison."""
    led = tmp_path / "l.jsonl"
    led.write_text(json.dumps({"trade_date": "2026-01-05", "close": 1.0}) + "\n")
    monkeypatch.setattr(sleeve, "LEDGER", led)
    with pytest.raises(SystemExit, match="row schema"):
        sleeve.existing_rows()


def test_full_schema_row_is_accepted(tmp_path, monkeypatch):
    led = tmp_path / "l.jsonl"
    led.write_text(json.dumps(_full_row()) + "\n")
    monkeypatch.setattr(sleeve, "LEDGER", led)
    assert set(sleeve.existing_rows()) == {"2026-01-05"}


def test_missing_field_counts_as_a_conflict_not_a_skip():
    fresh = _full_row()
    partial = {k: v for k, v in fresh.items() if k != "net_return"}
    assert "net_return" in sleeve.conflicts(partial, fresh)


@pytest.mark.parametrize("field", ["sleeve", "evidence_label", "execution_status",
                                   "record_origin", "fill_convention", "paper_equity"])
def test_conflicts_cover_provenance_and_labelling_fields(field):
    """An audit found these omitted: a row could be relabelled without detection."""
    a = _full_row()
    b = _full_row(**{field: "TAMPERED"})
    assert field in sleeve.conflicts(a, b)


def test_capture_digest_is_excluded_from_conflicts():
    """It legitimately differs between runs; including it would flag every rerun."""
    a = _full_row(capture_sha256="aa")
    b = _full_row(capture_sha256="bb")
    assert sleeve.conflicts(a, b) == []


def test_marker_disagreeing_with_the_ledger_fails_closed(tmp_path, monkeypatch):
    """A crash between the three file writes must be detectable on the next run."""
    led = tmp_path / "l.jsonl"
    led.write_text(json.dumps(_full_row()) + "\n")
    marker = tmp_path / "state.json"
    marker.write_text(json.dumps({"ledger": {"rows": 99, "latest": "2026-02-02"}}))
    monkeypatch.setattr(sleeve, "LEDGER", led)
    monkeypatch.setattr(sleeve, "MARKER", marker)
    with pytest.raises(SystemExit, match="did not complete"):
        sleeve.check_marker()


def test_marker_agreeing_with_the_ledger_passes(tmp_path, monkeypatch):
    led = tmp_path / "l.jsonl"
    led.write_text(json.dumps(_full_row()) + "\n")
    marker = tmp_path / "state.json"
    monkeypatch.setattr(sleeve, "LEDGER", led)
    monkeypatch.setattr(sleeve, "MARKER", marker)
    marker.write_text(json.dumps({"ledger": sleeve.ledger_state()}))
    sleeve.check_marker()


def test_durability_contract_is_stated_not_overclaimed():
    """The comment previously claimed the three files could not land partially. They can."""
    src = (ROOT / "scripts" / "chinext_timing_paper_sleeve.py").read_text()
    assert "CANNOT be replaced in one atomic step" in src
    assert "cannot land partially after a crash" not in src


@pytest.mark.parametrize("payload", [
    {"code": 0, "data": {"items": [], "fields": ["trade_date"]}},      # missing open/close
    {"code": 0, "data": {"items": "notalist", "fields": []}},
    {"code": 0},                                                       # no data key
    {"code": 40001, "msg": "bad token"},
    ["not", "a", "dict"],
])
def test_malformed_provider_response_raises_provider_error(monkeypatch, payload):
    class R:
        content = json.dumps(payload).encode()
    monkeypatch.setattr(sleeve.requests, "post", lambda *a, **k: R())
    monkeypatch.setenv("TUSHARE_TOKEN", "x")
    with pytest.raises(sleeve.ProviderError):
        sleeve.fetch_index()


def test_empty_but_valid_calendar_fails_closed(monkeypatch):
    """Previously this became UNKNOWN_CALENDAR, then appended and exited success."""
    class R:
        @staticmethod
        def json():
            return {"code": 0, "data": {"items": [], "fields": ["cal_date", "is_open"]}}
    monkeypatch.setattr(sleeve.requests, "post", lambda *a, **k: R())
    with pytest.raises(sleeve.ProviderError, match="no open sessions"):
        sleeve.latest_expected_session("token")


def test_main_checks_the_marker_before_any_provider_call(monkeypatch):
    """The guard must run in main(), not merely exist. Placed before the network so a
    corrupt ledger cannot be masked by an unrelated provider failure."""
    called = {"marker": False, "network": False}

    def boom():
        called["marker"] = True
        raise SystemExit("SENTINEL: marker checked")

    def net(*a, **k):
        called["network"] = True
        raise AssertionError("provider was called before the marker check")

    monkeypatch.setenv("TUSHARE_TOKEN", "x")
    monkeypatch.setattr(sleeve, "check_marker", boom)
    monkeypatch.setattr(sleeve.requests, "post", net)
    monkeypatch.setattr(sys, "argv", ["sleeve"])
    with pytest.raises(SystemExit, match="SENTINEL"):
        sleeve.main()
    assert called["marker"] and not called["network"]
