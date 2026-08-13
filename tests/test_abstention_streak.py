"""A run of empty sessions must be visible — silence and breakage look alike.

These lock the counter's two judgement calls: a missing artifact is a skip (the
scanner did not run), and a day with picks is a break (the scanner did run and
found something).
"""


import importlib
import json
from pathlib import Path
from unittest.mock import patch

from src.core.io import count_abstention_streak
from src.core.message_format import abstention_note


def _write_scan(root: Path, date: str, picks: int) -> None:
    day = root / date
    day.mkdir(parents=True, exist_ok=True)
    (day / f"scan_results_{date}.json").write_text(
        json.dumps({"date": date, "picks": [{"ticker": f"{i}"} for i in range(picks)]}),
        encoding="utf-8",
    )


def test_streak_counts_consecutive_empty_sessions(tmp_path):
    for date in ("2026-07-01", "2026-07-02", "2026-07-03"):
        _write_scan(tmp_path, date, picks=0)

    streak, since = count_abstention_streak("2026-07-03", root_dir=str(tmp_path))

    assert streak == 3
    assert since == "2026-07-01"


def test_streak_breaks_on_a_session_with_picks(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)
    _write_scan(tmp_path, "2026-07-02", picks=2)
    _write_scan(tmp_path, "2026-07-03", picks=0)

    streak, since = count_abstention_streak("2026-07-03", root_dir=str(tmp_path))

    assert streak == 1
    assert since == "2026-07-03"


def test_latest_session_with_picks_reports_no_streak(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)
    _write_scan(tmp_path, "2026-07-02", picks=1)

    assert count_abstention_streak("2026-07-02", root_dir=str(tmp_path)) == (0, None)


def test_missing_and_unreadable_artifacts_are_skipped_not_breaks(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)
    (tmp_path / "2026-07-02").mkdir()  # scanner never ran: no artifact at all
    corrupt = tmp_path / "2026-07-03"
    corrupt.mkdir()
    (corrupt / "scan_results_2026-07-03.json").write_text("{not json", encoding="utf-8")
    _write_scan(tmp_path, "2026-07-06", picks=0)

    streak, since = count_abstention_streak("2026-07-06", root_dir=str(tmp_path))

    assert streak == 2
    assert since == "2026-07-01"


def test_future_dates_and_non_date_dirs_are_ignored(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)
    _write_scan(tmp_path, "2026-07-09", picks=3)  # after asof — must not break the run
    (tmp_path / "backtest").mkdir()

    streak, since = count_abstention_streak("2026-07-01", root_dir=str(tmp_path))

    assert (streak, since) == (1, "2026-07-01")


def test_current_picks_overrides_disk_for_todays_session(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)
    # Today's artifact is not on disk yet when the alert is composed.
    streak, since = count_abstention_streak(
        "2026-07-02", root_dir=str(tmp_path), current_picks=0
    )

    assert (streak, since) == (2, "2026-07-01")


def test_current_picks_nonzero_breaks_the_run(tmp_path):
    _write_scan(tmp_path, "2026-07-01", picks=0)

    assert count_abstention_streak(
        "2026-07-02", root_dir=str(tmp_path), current_picks=1
    ) == (0, None)


def test_note_stays_silent_below_two_sessions():
    assert abstention_note(0, None) == ""
    assert abstention_note(1, "2026-07-03") == ""


def test_note_renders_count_and_start_date():
    note = abstention_note(29, "2026-07-03")

    assert "连续第 29 个交易日无选股" in note
    assert "07-03" in note


def test_scan_alert_carries_the_streak_line(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    monkeypatch.delenv("DRAGON_PULSE_SKIP_TELEGRAM", raising=False)

    for date in ("2026-08-10", "2026-08-11"):
        _write_scan(Path("outputs"), date, picks=0)

    # Exercise the alert builder directly: cmd_scan pulls in the scanner
    # pipeline, and the message layout does not depend on it.
    scan_cmd = importlib.import_module("src.commands.scan")
    empty_result = {
        "date": "2026-08-12",
        "regime": "bear",
        "regime_detail": {
            "acceptance_mode": "normal",
            "day_quality_score": 0,
            "acceptance_eligible_count": 0,
            "market_breadth_pct_above_sma20": 0.715,
        },
        "universe_size": 998,
        "downloaded": 998,
        "download_failed": 0,
        "download_health": "ok",
        "circuit_breaker": None,
        "signals_total": 0,
        "picks": [],
        "errors": [],
    }

    with patch(
        "src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}
    ) as send_alert:
        scan_cmd._send_scan_alert(empty_result)

    message = send_alert.call_args.kwargs["message"]
    assert "今日无选股" in message
    assert "连续第 3 个交易日无选股" in message
