"""Offline positive-control liveness contract for immutable bundle replay."""

from __future__ import annotations

import csv
import json
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
