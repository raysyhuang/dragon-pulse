import argparse
import importlib
from pathlib import Path
from unittest.mock import patch


def test_scan_sends_preopen_telegram_when_not_skipped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    monkeypatch.delenv("DRAGON_PULSE_SKIP_TELEGRAM", raising=False)

    scan_cmd = importlib.import_module("src.commands.scan")
    fake_result = {
        "date": "2026-05-29",
        "regime": "bull",
        "regime_detail": {
            "acceptance_mode": "normal",
            "day_quality_score": 88,
            "acceptance_eligible_count": 2,
            "market_breadth_pct_above_sma20": 0.61,
        },
        "universe_size": 996,
        "downloaded": 996,
        "download_failed": 0,
        "download_health": "ok",
        "circuit_breaker": None,
        "signals_total": 12,
        "picks": [
            {
                "ticker": "600026.SH",
                "name_cn": "中远海能",
                "score": 93.0,
                "entry_price": 8.12,
                "max_entry_price": 8.36,
                "stop_loss": 7.71,
                "target_1": 8.94,
                "holding_period": 5,
                "reason_summary": "rsi2_oversold=100",
            }
        ],
        "errors": [],
    }

    with patch("src.core.config.load_config", return_value={}) as load_config, patch(
        "src.pipelines.scanner.run_scan", return_value=fake_result
    ) as run_scan, patch("src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}) as send_alert:
        rc = scan_cmd.cmd_scan(argparse.Namespace(config="config/default.yaml", date=None))

    assert rc == 0
    load_config.assert_called_once_with("config/default.yaml")
    run_scan.assert_called_once_with({}, asof_date=None)
    assert Path("outputs/2026-05-29/execution_watchlist_2026-05-29.json").exists()
    assert Path("outputs/2026-05-29/scan_results_2026-05-29.json").exists()
    send_alert.assert_called_once()
    assert "龙脉扫描 — 2026-05-29" in send_alert.call_args.kwargs["message"]


def test_scan_skip_env_suppresses_preopen_telegram(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setenv("DRAGON_PULSE_SKIP_TELEGRAM", "1")

    scan_cmd = importlib.import_module("src.commands.scan")
    fake_result = {
        "date": "2026-05-29",
        "regime": "bull",
        "regime_detail": {},
        "universe_size": 996,
        "downloaded": 996,
        "download_failed": 0,
        "download_health": "ok",
        "circuit_breaker": None,
        "signals_total": 0,
        "picks": [],
        "errors": [],
    }

    with patch("src.core.config.load_config", return_value={}), patch(
        "src.pipelines.scanner.run_scan", return_value=fake_result
    ), patch("src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}) as send_alert:
        rc = scan_cmd.cmd_scan(argparse.Namespace(config="config/default.yaml", date=None))

    assert rc == 0
    send_alert.assert_not_called()
