"""Signal generation must replay offline, not just results and the ledger.

The published bundle hashed prices, calendar and outputs, which made it look
complete. It was not: --universe-mode point_in_time rebuilt the membership
schedule from a live provider on every run, and membership decides which signals
can exist at all. get_top_n_cn_by_market_cap_asof answers a historical query with
what the provider believes today, so a clean-checkout replay could silently
produce a different universe and therefore different picks.

These tests pin the three frozen inputs that make signal generation reproducible:
membership schedule, trade_cal receipt, price cache.
"""

import csv
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.offline_replay_fixture import MINI_TICKER, make_offline_replay_fixture

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN = PROJECT_ROOT / "outputs" / "backtest" / "pit5y_final"
SCHEDULE = RUN / "pit_membership_schedule.json"
CALENDAR = RUN / "trade_cal_receipt.json"
SNAPSHOTS = RUN / "snapshots"

needs_artifacts = pytest.mark.skipif(
    not (SCHEDULE.exists() and CALENDAR.exists() and SNAPSHOTS.is_dir()),
    reason="published replay artifacts not present in this checkout",
)


def _offline_env(tmp_path):
    site = tmp_path / "nonet"
    site.mkdir(exist_ok=True)
    (site / "sitecustomize.py").write_text(
        "import socket\n"
        "def _b(*a, **k):\n"
        "    raise RuntimeError('NETWORK_BLOCKED_BY_TEST')\n"
        "socket.create_connection = _b\n"
        "socket.socket.connect = _b\n"
        "socket.socket.connect_ex = _b\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("TUSHARE_TOKEN", None)
    env["HOME"] = str(tmp_path)
    env["PYTHONPATH"] = str(site)
    return env


def test_the_network_blocker_is_live(tmp_path):
    """Positive control: without it, the offline test could pass for the wrong reason."""
    probe = subprocess.run(
        [sys.executable, "-c",
         "import socket; socket.create_connection(('example.com', 80), timeout=2)"],
        capture_output=True, text=True, env=_offline_env(tmp_path), timeout=60,
    )
    assert probe.returncode != 0 and "NETWORK_BLOCKED_BY_TEST" in probe.stderr


@needs_artifacts
def test_signals_rebuild_offline_from_frozen_schedule_calendar_and_cache(tmp_path):
    """The acceptance gate: no token, no network, frozen inputs, signals still emitted."""
    out = tmp_path / "out"
    completed = subprocess.run(
        [sys.executable, "scripts/backtest_1yr.py",
         "--start", "2025-06-02", "--end", "2025-06-30",
         "--universe-mode", "point_in_time", "--engines", "alpha",
         "--acceptance-mode", "live_equivalent", "--label", "offline_e2e",
         "--out-dir", str(out),
         "--price-snapshot-dir", str(SNAPSHOTS),
         "--pit-schedule-in", str(SCHEDULE),
         "--calendar-in", str(CALENDAR)],
        cwd=PROJECT_ROOT, capture_output=True, text=True, env=_offline_env(tmp_path), timeout=1800,
    )

    assert "NETWORK_BLOCKED_BY_TEST" not in completed.stderr, "replay attempted a network call"
    assert "TUSHARE_TOKEN" not in completed.stderr, "replay demanded a provider token"
    assert completed.returncode == 0, completed.stderr[-2000:]

    daily = out / "backtest_daily_offline_e2e.csv"
    assert daily.exists()
    rows = list(csv.DictReader(daily.open()))
    assert rows, "no sessions replayed"
    assert sum(int(r["picks"]) for r in rows) > 0, "offline replay produced no signals"


@needs_artifacts
def test_frozen_schedule_declares_its_provenance_honestly():
    """Freezing makes a replay reproducible; it does not make membership correct."""
    data = json.loads(SCHEDULE.read_text(encoding="utf-8"))

    assert data["artifact"] == "DP_PIT_MEMBERSHIP_SCHEDULE"
    assert data["provenance"] == "TRUSTED_HISTORICAL_ASSUMPTION"
    assert "provider-CURRENT" in data["provenance_note"]
    assert data["rebalances"] and all(r["members"] for r in data["rebalances"])


@needs_artifacts
def test_a_schedule_with_an_empty_rebalance_is_rejected(tmp_path):
    """Fail closed: a silently empty rebalance would shrink the universe unnoticed."""
    import importlib.util

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

    broken = json.loads(SCHEDULE.read_text(encoding="utf-8"))
    broken["rebalances"] = [dict(broken["rebalances"][0])]
    broken["rebalances"][0]["members"] = []
    path = tmp_path / "broken.json"
    path.write_text(json.dumps(broken), encoding="utf-8")

    with pytest.raises(RuntimeError, match="no members"):
        bt.load_pit_schedule(path)


# ---------------------------------------------------------------------------
# Mandatory gate: runs on every PR, no dependency on the release archive.
# ---------------------------------------------------------------------------

def _run_offline(tmp_path, *, schedule, calendar, snapshots, start, end, label):
    return subprocess.run(
        [sys.executable, "scripts/backtest_1yr.py",
         "--start", start, "--end", end,
         "--universe-mode", "point_in_time", "--engines", "mr_only",
         "--config", "config/experiments/mr_a0_baseline.yaml",
         "--acceptance-mode", "off", "--top-n", "1",
         "--label", label, "--out-dir", str(tmp_path / f"out_{label}"),
         "--price-snapshot-dir", str(snapshots),
         "--pit-schedule-in", str(schedule),
         "--calendar-in", str(calendar)],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
        env=_offline_env(tmp_path), timeout=900,
    )


def test_fixture_offline_replay_rebuilds_signals_with_no_provider(tmp_path):
    """THE GATE. Frozen schedule + calendar + mini cache, no token, no network.

    Unconditional: it must not skip because the 39MB release archive is absent,
    since a skipped acceptance gate reads exactly like a passing one.
    """
    fx = make_offline_replay_fixture(tmp_path)

    r = _run_offline(tmp_path, schedule=fx["schedule"], calendar=fx["calendar"],
                     snapshots=fx["snapshots"], start="2025-12-31", end="2025-12-31",
                     label="fixture_ok")

    assert "NETWORK_BLOCKED_BY_TEST" not in r.stderr, "replay attempted a network call"
    assert "TUSHARE_TOKEN" not in r.stderr, "replay demanded a provider token"
    assert r.returncode == 0, r.stderr[-2000:]
    assert "no provider call" in r.stderr, "frozen schedule was not the source of membership"

    daily = tmp_path / "out_fixture_ok" / "backtest_daily_fixture_ok.csv"
    assert daily.exists()
    rows = list(csv.DictReader(daily.open()))
    assert sum(int(x["picks"]) for x in rows) > 0, "offline replay produced no signals"


def test_missing_schedule_fails_closed(tmp_path):
    fx = make_offline_replay_fixture(tmp_path)

    r = _run_offline(tmp_path, schedule=fx["root"] / "absent.json", calendar=fx["calendar"],
                     snapshots=fx["snapshots"], start="2025-12-31", end="2025-12-31",
                     label="no_sched")

    assert r.returncode != 0, "a missing schedule must not fall back to a provider"
    assert "NETWORK_BLOCKED_BY_TEST" not in r.stderr


def test_empty_rebalance_fails_closed(tmp_path):
    """A silently empty rebalance would shrink the universe with no visible error."""
    fx = make_offline_replay_fixture(tmp_path)
    broken = json.loads(fx["schedule"].read_text())
    broken["rebalances"][0]["members"] = []
    bad = fx["root"] / "empty.json"
    bad.write_text(json.dumps(broken), encoding="utf-8")

    r = _run_offline(tmp_path, schedule=bad, calendar=fx["calendar"],
                     snapshots=fx["snapshots"], start="2025-12-31", end="2025-12-31",
                     label="empty_reb")

    assert r.returncode != 0
    assert "no members" in r.stderr


def test_wrong_artifact_type_fails_closed(tmp_path):
    """Hash/identity mismatch: a file at the right path is not the right artifact."""
    fx = make_offline_replay_fixture(tmp_path)
    imposter = fx["root"] / "imposter.json"
    imposter.write_text(json.dumps({"artifact": "SOMETHING_ELSE", "rebalances": [
        {"date": "2025-01-01", "members": [MINI_TICKER]}]}), encoding="utf-8")

    r = _run_offline(tmp_path, schedule=imposter, calendar=fx["calendar"],
                     snapshots=fx["snapshots"], start="2025-12-31", end="2025-12-31",
                     label="imposter")

    assert r.returncode != 0
    assert "not a PIT membership schedule" in r.stderr


def test_corrupt_calendar_receipt_fails_closed(tmp_path):
    fx = make_offline_replay_fixture(tmp_path)
    bad = fx["root"] / "bad_cal.json"
    bad.write_text(json.dumps({"code": 0, "data": {"fields": ["wrong"], "items": []}}), encoding="utf-8")

    r = _run_offline(tmp_path, schedule=fx["schedule"], calendar=bad,
                     snapshots=fx["snapshots"], start="2025-12-31", end="2025-12-31",
                     label="bad_cal")

    assert r.returncode != 0
    assert "unexpected schema" in r.stderr
