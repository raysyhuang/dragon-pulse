import argparse
import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


def load_morning_check_module():
    module_path = Path(__file__).parent.parent / "scripts" / "morning_check.py"
    spec = importlib.util.spec_from_file_location("morning_check_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_fetch_open_prices_reads_snapshot_once_and_filters_zero_opens(monkeypatch):
    module = load_morning_check_module()

    fake_ak = types.SimpleNamespace()
    fake_ak.stock_zh_a_spot_em = MagicMock(return_value=pd.DataFrame([
        {"代码": "600000", "今开": 10.25},
        {"代码": "000001", "今开": 0},
        {"代码": "300001", "今开": 18.88},
    ]))
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    prices = module.fetch_open_prices(["600000.SH", "000001.SZ", "300001.SZ"])

    assert prices == {
        "600000.SH": 10.25,
        "300001.SZ": 18.88,
    }
    fake_ak.stock_zh_a_spot_em.assert_called_once()


def test_send_open_pending_alert_writes_marker_and_sends_message(tmp_path, monkeypatch):
    module = load_morning_check_module()

    date_str = "2026-04-14"
    out_dir = tmp_path / "outputs" / date_str
    out_dir.mkdir(parents=True)
    watchlist_path = out_dir / f"execution_watchlist_{date_str}.json"
    watchlist_path.write_text(json.dumps({
        "date": date_str,
        "regime": "bull",
        "universe_size": 996,
        "picks": [
            {"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.2,
             "max_entry_price": 10.35, "stop_loss": 9.8, "target_1": 10.9,
             "holding_period": 3, "score": 92, "reason_summary": "rsi2_oversold=100"},
            {"ticker": "000001.SZ", "name_cn": "平安银行", "entry_price": 12.3,
             "stop_loss": 11.5, "target_1": 13.1, "holding_period": 3, "score": 85},
        ],
    }), encoding="utf-8")
    pending_marker = out_dir / ".morning_open_pending_sent"

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")

    with patch("src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}) as send_alert:
        sent = module.send_open_pending_alert(
            today_str=date_str,
            date_str=date_str,
            watchlist_path=watchlist_path,
            picks=[],
            pending_marker=pending_marker,
        )

    assert sent is True
    assert pending_marker.exists()
    send_alert.assert_called_once()
    kwargs = send_alert.call_args.kwargs
    msg = kwargs["message"]
    assert "[PENDING]" in msg
    assert "Score: 92" in msg
    assert "Stop:" in msg
    assert "T1:" in msg
    assert "rsi2_oversold=100" in msg
    assert "Opening prices not yet available" in msg


def test_main_returns_zero_when_open_prices_are_missing(tmp_path, monkeypatch):
    module = load_morning_check_module()

    date_str = "2026-04-14"
    out_dir = tmp_path / "outputs" / date_str
    out_dir.mkdir(parents=True)
    (out_dir / f"execution_watchlist_{date_str}.json").write_text(json.dumps({
        "date": date_str,
        "regime": "bull",
        "picks": [
            {"ticker": "600000.SH", "name_cn": "浦发银行", "entry_price": 10.2},
        ],
    }), encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    with patch.object(module.argparse.ArgumentParser, "parse_args", return_value=argparse.Namespace(
        date=date_str,
        picks_file=None,
        max_gap_up=3.0,
        max_gap_down=5.0,
        dry_run=False,
    )), patch.object(module, "fetch_open_prices", return_value={}), patch.object(
        module, "send_open_pending_alert", return_value=True
    ) as send_pending:
        rc = module.main()

    assert rc == 0
    send_pending.assert_called_once()
