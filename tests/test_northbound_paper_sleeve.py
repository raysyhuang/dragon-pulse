import importlib.util
import json
import re
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
    # Header must equal the alert title so AlertManager does not repeat it.
    first_line = re.sub(r"</?[^>]+>", "", msg.lstrip().split("\n", 1)[0]).strip()
    assert first_line == send_alert.call_args.kwargs["title"]
    # Raw exception text stays in the artifact, never in the Telegram body.
    assert "NoneType" not in msg


def test_send_preopen_alert_hides_raw_exception_text(monkeypatch, tmp_path):
    module = load_northbound_module()
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "chat")
    meta = {
        "source_trade_date": "2026-06-26",
        "data_quality": "active_rank_only",
        "holding_delta_status": (
            "eastmoney_holding_delta_parser_error: TypeError: "
            "'NoneType' object is not subscriptable"
        ),
        "data_gaps": [
            "latest hsgt_top10 trade_date 2026-06-26 is older than asof 2026-06-29",
            "hsgt_top10 net_amount/buy/sell are not populated; active-rank turnover only",
        ],
    }

    with patch("src.core.alerts.AlertManager.send_alert", return_value={"telegram": True}) as send_alert:
        module.send_preopen_alert("2026-06-29", [], meta, tmp_path / "x.json")

    msg = send_alert.call_args.kwargs["message"]
    assert "TypeError" not in msg and "NoneType" not in msg
    assert "解析失败" in msg
    assert "榜单日期滞后于今日" in msg


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
    assert data["data_quality"] == "flow_confirmed_but_lagged"
    assert data["flow_confirmed"] is True
    assert any("net_amount" in gap for gap in data["data_gaps"])


def test_load_regime_stamp_fails_open_on_non_object_json(tmp_path, monkeypatch):
    module = load_northbound_module()
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    path = tmp_path / "outputs" / "2026-06-26" / "regime_2026-06-26.json"
    path.parent.mkdir(parents=True)
    path.write_text("[]", encoding="utf-8")

    stamp = module.load_regime_stamp("2026-06-26")

    assert stamp["regime_at_signal"] is None
    assert stamp["breadth_at_signal"] is None
    assert stamp["regime_stamp_status"].startswith("regime_artifact_unavailable:")


def test_holding_delta_parser_error_is_distinguished_from_provider_unavailability(monkeypatch):
    module = load_northbound_module()
    fake_akshare = types.SimpleNamespace(
        stock_hsgt_stock_statistics_em=lambda **kwargs: (_ for _ in ()).throw(
            TypeError("'NoneType' object is not subscriptable")
        )
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_akshare)

    holdings, status = module.fetch_holding_delta_map("2026-07-15")

    assert holdings == {}
    assert status.startswith("eastmoney_holding_delta_parser_error: TypeError:")
    assert "NoneType" in status


def test_true_atr14_includes_gap_risk():
    module = load_northbound_module()
    idx = pd.date_range("2026-05-01", periods=20, freq="D")
    close = pd.Series([100.0] * 19 + [110.0], index=idx)
    high = pd.Series([101.0] * 19 + [111.0], index=idx)
    low = pd.Series([99.0] * 19 + [109.0], index=idx)

    atr = module._compute_true_atr14(high, low, close)

    # Last bar's high-low range is only 2, but true range captures the 100→110 gap.
    assert atr > 2.0


def test_data_quality_marks_active_rank_only_when_flow_missing():
    module = load_northbound_module()

    quality = module.assess_data_quality(
        asof_date="2026-06-29",
        source_trade_date="2026-06-26",
        source_status="ok",
        net_amount_non_null=0,
        holding_delta_status="eastmoney_holding_delta_unavailable: TypeError",
        holding_delta_rows=0,
    )

    assert quality["data_quality"] == "active_rank_only"
    assert quality["flow_confirmed"] is False
    assert "成交活跃" in quality["label"]
    assert any("active-rank turnover only" in gap for gap in quality["data_gaps"])
    assert any("older than" in gap for gap in quality["data_gaps"])


def test_build_picks_meta_exposes_data_quality_gaps(monkeypatch):
    module = load_northbound_module()
    hsgt = pd.DataFrame([
        {"ts_code": "600001.SH", "name": "北向A", "rank": 1, "net_amount": None, "amount": 500.0},
    ])
    monkeypatch.setattr(module, "fetch_hsgt_top10_latest", lambda asof_date: ("20260626", hsgt, "ok"))
    idx = pd.date_range("2026-05-01", periods=30, freq="D")
    good_df = pd.DataFrame({"Open": 100, "High": 102.5, "Low": 97.5, "Close": 100, "Volume": 1_000_000}, index=idx)
    monkeypatch.setattr(module, "_fetch_ohlcv", lambda tickers, start, end: {"600001.SH": good_df})
    monkeypatch.setattr(module, "get_cn_basic_info", lambda tickers, provider_config=None: {"600001.SH": {"name_cn": "北向A"}})
    monkeypatch.setattr(module, "fetch_holding_delta_map", lambda source_date: ({}, "eastmoney_holding_delta_unavailable: TypeError"))
    monkeypatch.setattr(module, "load_regime_stamp", lambda market_date: {
        "regime_at_signal": "bear",
        "breadth_at_signal": 0.3656,
        "regime_market_date": market_date,
        "regime_stamp_source": "saved_regime_artifact",
        "regime_stamp_status": "ok",
    })

    picks, meta = module.build_picks("2026-06-29", top_n=5)

    assert len(picks) == 1
    assert meta["data_quality"] == "active_rank_only"
    assert meta["flow_confirmed"] is False
    assert any("net_amount" in gap for gap in meta["data_gaps"])
    assert meta["regime_at_signal"] == "bear"
    assert meta["breadth_at_signal"] == 0.3656
    pick = picks[0].to_dict()
    assert pick["northbound_flow_confirmed"] is False
    assert pick["regime_at_signal"] == "bear"
    assert pick["breadth_at_signal"] == 0.3656
