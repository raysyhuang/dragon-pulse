import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


def load_northbound_module():
    module_path = Path(__file__).parent.parent / "scripts" / "northbound_paper_sleeve.py"
    spec = importlib.util.spec_from_file_location("northbound_paper_sleeve_test_module", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_picks_filters_rank_and_stop_risk(monkeypatch):
    module = load_northbound_module()
    hsgt = pd.DataFrame([
        {"ts_code": "600001.SH", "name": "北向A", "rank": 1, "net_amount": 100.0, "amount": 500.0},
        {"ts_code": "600002.SH", "name": "北向B", "rank": 6, "net_amount": 200.0, "amount": 600.0},
        {"ts_code": "000001.SZ", "name": "北向C", "rank": 2, "net_amount": -50.0, "amount": 300.0},
    ])
    monkeypatch.setattr(module, "fetch_hsgt_top10_latest", lambda asof_date: ("20260626", hsgt, "ok"))

    idx = pd.date_range("2026-05-01", periods=30, freq="D")
    good_df = pd.DataFrame({"Open": 100, "High": 102.5, "Low": 97.5, "Close": 100, "Volume": 1_000_000}, index=idx)
    # ATR=1, close=100 -> 1.1%, outside risk band.
    low_risk_df = pd.DataFrame({"Open": 100, "High": 100.5, "Low": 99.5, "Close": 100, "Volume": 1_000_000}, index=idx)
    monkeypatch.setattr(module, "_fetch_ohlcv", lambda tickers, start, end: {"600001.SH": good_df, "000001.SZ": low_risk_df})
    monkeypatch.setattr(module, "get_cn_basic_info", lambda tickers, provider_config=None: {"600001.SH": {"name_cn": "北向A"}})
    monkeypatch.setattr(module, "fetch_holding_delta_map", lambda source_date: ({"600001.SH": {
        "holding_delta_vol": 10_000.0,
        "holding_delta_ratio": 0.02,
        "holding_delta_source": "eastmoney_hsgt_stock_statistics",
    }}, "ok"))

    picks, meta = module.build_picks("2026-06-29", top_n=5)

    assert meta["source_trade_date"] == "2026-06-26"
    assert [p.ticker for p in picks] == ["600001.SH"]
    pick = picks[0]
    assert pick.max_entry_price == 102.0
    assert pick.stop_loss == 94.5
    assert pick.target_1 == 110.5
    assert pick.paper_only if hasattr(pick, "paper_only") else True
    assert pick.to_dict()["paper_only"] is True
    assert pick.to_dict()["status"] == "PAPER_TRACK_ONLY"
    assert pick.to_dict()["holding_delta_vol"] == 10_000.0
    assert pick.to_dict()["northbound_flow_confirmed"] is True
    assert pick.to_dict()["promotion_eligible"] is False
    assert meta["holding_delta_status"] == "ok"


def test_save_watchlist_writes_separate_paper_artifact(tmp_path, monkeypatch):
    module = load_northbound_module()
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    pick = module.PaperPick(
        ticker="600001.SH", name_cn="北向A", rank=1, signal_date="2026-06-29", source_trade_date="2026-06-26",
        entry_price=100.0, max_entry_price=102.0, stop_loss=94.5, target_1=110.5, target_2=118.0,
        holding_period=5, score=120.0, stop_risk_pct=5.5, net_amount=100.0, amount=500.0,
        holding_delta_vol=10_000.0, holding_delta_ratio=0.02, holding_delta_source="eastmoney_hsgt_stock_statistics",
    )

    path = module.save_watchlist("2026-06-29", [pick], {"source_status": "ok"})
    data = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == "northbound_paper_watchlist_2026-06-29.json"
    assert data["sleeve"] == "northbound_active_riskband_paper"
    assert data["paper_only"] is True
    assert data["picks"][0]["engine"] == "northbound_active_riskband_paper"
    assert data["picks"][0]["paper_only"] is True
    assert data["picks"][0]["holding_delta_source"] == "eastmoney_hsgt_stock_statistics"


def test_send_preopen_alert_labels_paper_only(monkeypatch, tmp_path):
    module = load_northbound_module()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    pick = module.PaperPick(
        ticker="600001.SH", name_cn="北向A", rank=1, signal_date="2026-06-29", source_trade_date="2026-06-26",
        entry_price=100.0, max_entry_price=102.0, stop_loss=94.5, target_1=110.5, target_2=118.0,
        holding_period=5, score=120.0, stop_risk_pct=5.5,
        holding_delta_vol=10_000.0,
    )

    with patch("src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}) as send_alert:
        module.send_preopen_alert("2026-06-29", [pick], {"source_trade_date": "2026-06-26"}, tmp_path / "x.json")

    send_alert.assert_called_once()
    msg = send_alert.call_args.kwargs["message"]
    assert "PAPER ONLY" in msg
    assert "不进实盘龙脉排名" in msg
    assert "北向A" in msg
    assert "北向持股变化" in msg
    assert "forward paper" in msg


def test_source_probe_records_top10_and_holding_delta_status(tmp_path, monkeypatch):
    module = load_northbound_module()
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    hsgt = pd.DataFrame([
        {"ts_code": "600001.SH", "name": "北向A", "rank": 1, "net_amount": None, "amount": 500.0},
        {"ts_code": "600002.SH", "name": "北向B", "rank": 6, "net_amount": None, "amount": 600.0},
    ])
    monkeypatch.setattr(module, "fetch_hsgt_top10_latest", lambda date_str, lookback_days=10: ("20260626", hsgt, "ok"))
    monkeypatch.setattr(module, "fetch_holding_delta_map", lambda source_date: ({"600001.SH": {"holding_delta_vol": 123.0}}, "ok"))

    path = module.save_source_probe("2026-06-29")
    data = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == "northbound_source_probe_2026-06-29.json"
    assert data["hsgt_top10_latest_trade_date"] == "2026-06-26"
    assert data["hsgt_top10_rows"] == 2
    assert data["hsgt_top10_rank_le_5_rows"] == 1
    assert data["hsgt_top10_net_amount_non_null"] == 0
    assert data["holding_delta_status"] == "ok"
    assert data["holding_delta_rows"] == 1


def test_true_atr14_includes_gap_risk():
    module = load_northbound_module()
    idx = pd.date_range("2026-05-01", periods=20, freq="D")
    close = pd.Series([100.0] * 19 + [110.0], index=idx)
    high = pd.Series([101.0] * 19 + [111.0], index=idx)
    low = pd.Series([99.0] * 19 + [109.0], index=idx)

    atr = module._compute_true_atr14(high, low, close)

    # Last bar's high-low range is only 2, but true range captures the 100→110 gap.
    assert atr > 2.0
