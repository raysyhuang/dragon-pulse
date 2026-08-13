"""Offline positive-control liveness contract for immutable bundle replay."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

from tests.golden_bundle_fixture import make_golden_mr_bundle


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLDEN_DATE = "2025-12-31"
GOLDEN_TICKER = "000001.SZ"
GOLDEN_BUNDLE_ID = "golden-mr-liveness-v1"
GOLDEN_COMPOSITE_SHA256 = "0ee44bd368557301769c0f535020110e40b7563d9909163cd4d7c475b6010e51"


def _run_golden(tmp_path):
    bundle = make_golden_mr_bundle(tmp_path)
    out_dir = tmp_path / "out"
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/backtest_1yr.py",
            "--input-bundle",
            str(bundle),
            "--config",
            "config/experiments/mr_a0_baseline.yaml",
            "--engines",
            "mr_only",
            "--start",
            GOLDEN_DATE,
            "--end",
            GOLDEN_DATE,
            "--top-n",
            "1",
            "--out-dir",
            str(out_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    return out_dir


def test_bundle_mode_cli_golden_mr_liveness(tmp_path):
    """A sealed local bundle must replay one known MR pick without providers."""
    out_dir = _run_golden(tmp_path)

    summary = json.loads((out_dir / "backtest_summary.json").read_text(encoding="utf-8"))
    with (out_dir / "backtest_detail.csv").open(newline="", encoding="utf-8") as handle:
        detail = list(csv.DictReader(handle))
    with (out_dir / "backtest_daily.csv").open(newline="", encoding="utf-8") as handle:
        daily = list(csv.DictReader(handle))

    assert summary["input_mode"] == "bundle"
    assert summary["bundle_id"] == GOLDEN_BUNDLE_ID
    assert summary["bundle_composite_sha256"] == GOLDEN_COMPOSITE_SHA256
    assert summary["pit_grade"] is False
    assert summary["total_picks"] == 1
    assert daily == [
        {
            **daily[0],
            "date": GOLDEN_DATE,
            "picks": "1",
            "eligible_count": "1",
            "input_mode": "bundle",
            "bundle_id": GOLDEN_BUNDLE_ID,
            "bundle_composite_sha256": GOLDEN_COMPOSITE_SHA256,
            "pit_grade": "False",
        }
    ]
    assert len(detail) == 1
    assert detail[0]["date"] == GOLDEN_DATE
    assert detail[0]["ticker"] == GOLDEN_TICKER
    assert detail[0]["engine"] == "mean_reversion"
    assert detail[0]["input_mode"] == "bundle"
    assert detail[0]["bundle_composite_sha256"] == GOLDEN_COMPOSITE_SHA256


def test_daily_breadth_column_is_blank_when_acceptance_is_off(tmp_path):
    """Only the acceptance paths measure breadth; the legacy top-N path must not crash.

    The column was added for the breadth-gate study and initially referenced a
    variable that only the acceptance branch binds, so `--acceptance-mode off`
    died with UnboundLocalError. Blank is the honest value here — reporting 0.0
    would put an unmeasured number on the record.
    """
    out_dir = _run_golden(tmp_path)

    with (out_dir / "backtest_daily.csv").open(newline="", encoding="utf-8") as handle:
        daily = list(csv.DictReader(handle))

    assert daily, "golden replay produced no daily rows"
    for row in daily:
        assert "breadth_above_sma20" in row
        assert row["breadth_above_sma20"] == ""


def test_sealed_bundle_replay_needs_no_provider_and_no_env(tmp_path, monkeypatch):
    """Bundle mode must resolve its calendar offline, from the bundle's own receipt.

    Acceptance is not "the test passes without a token" — it is that a sealed
    replay completes with TUSHARE_TOKEN unset AND .env unreachable AND the
    network refused, using the manifest-hashed CSI300 price file as the session
    receipt. The earlier calendar fix traded the offline guarantee for accuracy
    and nobody noticed, because every dev machine has a token in .env.
    """
    import socket

    bundle = make_golden_mr_bundle(tmp_path)
    out_dir = tmp_path / "out"

    env = dict(os.environ)
    env.pop("TUSHARE_TOKEN", None)
    env["HOME"] = str(tmp_path)          # nothing to fall back to
    env["DP_TEST_NO_NETWORK"] = "1"

    # Prove the receipt is what gets used: block sockets in-process too.
    def _no_network(*_a, **_kw):
        raise AssertionError("bundle mode attempted a network call")

    monkeypatch.setattr(socket, "create_connection", _no_network)

    completed = subprocess.run(
        [sys.executable, "scripts/backtest_1yr.py", "--input-bundle", str(bundle),
         "--config", "config/experiments/mr_a0_baseline.yaml", "--engines", "mr_only",
         "--start", GOLDEN_DATE, "--end", GOLDEN_DATE, "--top-n", "1",
         "--out-dir", str(out_dir)],
        cwd=PROJECT_ROOT, capture_output=True, text=True, check=False, env=env,
    )

    assert completed.returncode == 0, completed.stderr
    assert "TUSHARE_TOKEN required" not in completed.stderr
    assert (out_dir / "backtest_daily.csv").exists()


def test_bundle_calendar_comes_from_the_receipt_not_a_weekday_guess(tmp_path):
    """The sessions used must be exactly those in the hashed CSI300 file."""
    import importlib.util
    from datetime import date

    bundle = make_golden_mr_bundle(tmp_path)
    spec = importlib.util.spec_from_file_location("bt", PROJECT_ROOT / "scripts" / "backtest_1yr.py")
    bt = importlib.util.module_from_spec(spec)
    saved = sys.argv[:]
    sys.argv = ["bt"]
    try:
        spec.loader.exec_module(bt)
    except SystemExit:
        pass
    finally:
        sys.argv = saved

    import csv as _csv
    with (bundle / "prices" / "000300.SH.csv").open(newline="", encoding="utf-8") as fh:
        receipt = {row[next(iter(row))] for row in _csv.DictReader(fh)}

    days = bt.get_cn_trading_days(date(1900, 1, 1), date(2100, 1, 1), bundle_dir=bundle)

    assert {d.isoformat() for d in days} == receipt
