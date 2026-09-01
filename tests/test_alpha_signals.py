import pandas as pd
from unittest.mock import patch

from scripts.alpha_tournament import build_config, detail_hash, _load_summary_metrics
from src.pipelines.funnel import build_engine_candidates
from src.signals.alpha_candidates import (
    score_accumulation_breakout_alpha,
    score_limitup_continuation_alpha,
    score_rs_pullback_alpha,
    score_sniper_breakout_alpha,
)


def _trend_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.055 for i in range(n)]
    # Put the last bar into a controlled pullback from the recent high.
    close[-2] = 19.05
    close[-1] = 18.85
    high = [c * 1.015 for c in close]
    low = [c * 0.985 for c in close]
    volume = [6_000_000] * n
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume}, index=idx)


def _breakout_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.04 for i in range(n)]
    close[-2] = 16.40
    close[-1] = 16.62
    high = [c * 1.01 for c in close]
    low = [c * 0.98 for c in close]
    # make prior high close enough for a near-breakout without a blowoff day
    high[-21:-1] = [16.55] * 20
    volume = [5_000_000] * (n - 6) + [4_000_000, 3_900_000, 3_800_000, 3_700_000, 3_600_000, 8_000_000]
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume}, index=idx)


def _flat_csi(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.DataFrame({"close": [100.0] * n}, index=idx)


def _limitup_continuation_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.035 for i in range(n)]
    close[-6] = 15.00
    close[-5] = 16.35  # near 10% impulse
    close[-4] = 16.15
    close[-3] = 16.28
    close[-2] = 16.22
    close[-1] = 16.55
    high = [c * 1.012 for c in close]
    low = [c * 0.985 for c in close]
    volume = [5_500_000] * n
    volume[-5] = 12_000_000
    volume[-1] = 7_500_000
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume}, index=idx)


def _accumulation_breakout_df(n: int = 170) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    close = [10 + i * 0.035 for i in range(n)]
    for j in range(n - 25, n - 1):
        close[j] = 15.2 + (j - (n - 25)) * 0.015
    close[-1] = 15.72
    high = [c * 1.008 for c in close]
    low = [c * 0.988 for c in close]
    high[-61:-1] = [15.85] * 60
    volume = [6_000_000] * n
    for j in range(n - 11, n - 1):
        volume[j] = 4_500_000
    volume[-1] = 9_000_000
    obv = list(range(n))
    return pd.DataFrame({"open": close, "high": high, "low": low, "close": close, "volume": volume, "obv": obv}, index=idx)


def test_rs_pullback_alpha_emits_research_signal_with_no_chase_ceiling():
    signal = score_rs_pullback_alpha(
        ticker="600118.SH",
        df=_trend_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=70,
        max_entry_pct=0.02,
    )

    assert signal is not None
    assert signal.subtype == "rs_pullback_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.02, 2)
    assert signal.holding_period == 5
    assert signal.target_1 > signal.entry_price > signal.stop_loss


def test_rs_pullback_alpha_refuses_when_benchmark_history_is_insufficient():
    signal = score_rs_pullback_alpha(
        ticker="600118.SH",
        df=_trend_df(),
        regime="choppy",
        csi300_df=_flat_csi().tail(60),
        regimes=("bull", "choppy"),
        score_floor=0,
    )

    assert signal is None


def test_rs_pullback_alpha_refuses_star_market_ticker():
    signal = score_rs_pullback_alpha(
        ticker="688006.SH",
        df=_trend_df(),
        regime="choppy",
        csi300_df=_flat_csi(),
        regimes=("bull", "choppy"),
        score_floor=0,
    )

    assert signal is None


def test_build_engine_candidates_refuses_star_market_for_every_engine():
    candidates = build_engine_candidates(
        feat_items=[("688006.SH", _breakout_df(), {"rsi_2": 50})],
        regime="bull",
        config={
            "universe": {"exclude_star_market": True},
            "mean_reversion": {"enabled": False},
            "sniper": {"enabled": False},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["sniper_breakout"],
                "sniper_breakout": {
                    "enabled": True,
                    "regimes": ["bull"],
                    "score_floor": 0,
                    "min_adv_cny": 0,
                },
            },
        },
        csi300_df=_flat_csi(),
    )

    assert candidates == []


def test_sniper_breakout_alpha_emits_research_signal_with_mas_style_ceiling():
    signal = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=_breakout_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        max_entry_pct=0.03,
        min_adv_cny=70_000_000,
    )

    assert signal is not None
    assert signal.subtype == "sniper_breakout_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.03, 2)
    assert signal.target_1 > signal.entry_price > signal.stop_loss
    assert set(["rs", "breakout", "volume", "trend", "not_exhausted"]).issubset(signal.components)


def test_sniper_breakout_alpha_requires_real_relative_strength():
    weak_csi = pd.DataFrame({"close": _breakout_df()["close"].values}, index=_breakout_df().index)

    signal = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=_breakout_df(),
        regime="bull",
        csi300_df=weak_csi,
        score_floor=60,
        min_adv_cny=70_000_000,
    )

    assert signal is None


def test_sniper_breakout_alpha_blocks_near_limit_up_exhaustion():
    df = _breakout_df()
    df.loc[df.index[-2], "close"] = 15.10
    df.loc[df.index[-1], "close"] = 16.55

    signal = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=70_000_000,
    )

    assert signal is None


def test_sniper_breakout_alpha_can_require_low_stop_risk():
    df = _breakout_df()

    passes_band = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=70_000_000,
        stop_atr_mult=1.6,
        max_stop_risk_pct=5.0,
    )
    assert passes_band is not None

    too_wide = score_sniper_breakout_alpha(
        ticker="601698.SH",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=70_000_000,
        stop_atr_mult=1.6,
        max_stop_risk_pct=3.0,
    )
    assert too_wide is None


def test_limitup_continuation_alpha_emits_theme_momentum_signal():
    signal = score_limitup_continuation_alpha(
        ticker="002999.SZ",
        df=_limitup_continuation_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=60_000_000,
    )

    assert signal is not None
    assert signal.subtype == "limitup_continuation_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.02, 2)
    assert {"rs", "continuation", "impulse_freshness", "not_chasing"}.issubset(signal.components)


def test_limitup_continuation_alpha_blocks_excessive_atr_risk():
    df = _limitup_continuation_df()
    df.loc[df.index[-14:], "high"] = df.loc[df.index[-14:], "close"] * 1.08
    df.loc[df.index[-14:], "low"] = df.loc[df.index[-14:], "close"] * 0.92

    signal = score_limitup_continuation_alpha(
        ticker="002999.SZ",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=60_000_000,
        max_atr_pct=6.5,
    )

    assert signal is None


def test_limitup_continuation_alpha_can_require_stop_risk_band():
    df = _limitup_continuation_df()

    passes_band = score_limitup_continuation_alpha(
        ticker="002999.SZ",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=60_000_000,
        stop_atr_mult=1.15,
        min_stop_risk_pct=1.0,
        max_stop_risk_pct=4.0,
    )
    assert passes_band is not None

    too_tight = score_limitup_continuation_alpha(
        ticker="002999.SZ",
        df=df,
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=60_000_000,
        stop_atr_mult=1.15,
        min_stop_risk_pct=6.5,
        max_stop_risk_pct=7.5,
    )
    assert too_tight is None


def test_accumulation_breakout_alpha_emits_dryup_expansion_signal():
    signal = score_accumulation_breakout_alpha(
        ticker="002999.SZ",
        df=_accumulation_breakout_df(),
        regime="bull",
        csi300_df=_flat_csi(),
        score_floor=60,
        min_adv_cny=60_000_000,
    )

    assert signal is not None
    assert signal.subtype == "accumulation_breakout_alpha"
    assert signal.max_entry_price == round(signal.entry_price * 1.025, 2)
    assert {"rs", "breakout", "accumulation", "not_extended"}.issubset(signal.components)


def test_build_engine_candidates_keeps_alpha_disabled_by_default():
    feat_items = [("600118.SH", _trend_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={"mean_reversion": {"rsi2_max": 5, "score_floor": 65}},
        csi300_df=_flat_csi(),
    )

    assert [engine for engine, _ in candidates] == []


def test_build_engine_candidates_can_disable_mean_reversion_for_alpha_core():
    feat_items = [("600118.SH", _trend_df(), {"rsi_2": 1})]

    with patch("src.pipelines.funnel.score_mean_reversion") as mock_mr:
        candidates = build_engine_candidates(
            feat_items=feat_items,
            regime="bull",
            config={"mean_reversion": {"enabled": False, "rsi2_max": 5, "score_floor": 65}},
            csi300_df=_flat_csi(),
        )

    assert candidates == []
    assert not mock_mr.called


def test_build_engine_candidates_adds_alpha_when_research_flag_enabled():
    feat_items = [("600118.SH", _trend_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={
            "mean_reversion": {"rsi2_max": 5, "score_floor": 65},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["rs_pullback"],
                "rs_pullback": {"enabled": True, "score_floor": 70, "max_entry_pct": 0.02},
            },
        },
        csi300_df=_flat_csi(),
    )

    assert [(engine, getattr(sig, "subtype")) for engine, sig in candidates] == [
        ("alpha_rs_pullback", "rs_pullback_alpha")
    ]


def test_build_engine_candidates_requires_per_alpha_engine_enable_flag():
    feat_items = [("601698.SH", _breakout_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={
            "mean_reversion": {"enabled": False},
            "sniper": {"enabled": False},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["sniper_breakout"],
                "sniper_breakout": {
                    "enabled": False,
                    "regimes": ["bull"],
                    "score_floor": 60,
                    "max_entry_pct": 0.03,
                    "min_adv_cny": 70_000_000,
                },
            },
        },
        csi300_df=_flat_csi(),
    )

    assert candidates == []


def test_build_engine_candidates_can_replace_rs_with_a_share_sniper_alpha():
    feat_items = [("601698.SH", _breakout_df(), {"rsi_2": 50})]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={
            "mean_reversion": {"enabled": False},
            "sniper": {"enabled": False},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["sniper_breakout"],
                "sniper_breakout": {
                    "enabled": True,
                    "regimes": ["bull"],
                    "score_floor": 60,
                    "max_entry_pct": 0.03,
                    "min_adv_cny": 70_000_000,
                },
            },
        },
        csi300_df=_flat_csi(),
    )

    assert [(engine, getattr(sig, "subtype")) for engine, sig in candidates] == [
        ("alpha_sniper_breakout", "sniper_breakout_alpha")
    ]


def test_build_engine_candidates_can_run_new_tournament_sleeves():
    feat_items = [
        ("002999.SZ", _limitup_continuation_df(), {"rsi_2": 50}),
        ("300999.SZ", _accumulation_breakout_df(), {"rsi_2": 50}),
    ]

    candidates = build_engine_candidates(
        feat_items=feat_items,
        regime="bull",
        config={
            "mean_reversion": {"enabled": False},
            "sniper": {"enabled": False},
            "alpha_candidates": {
                "enabled": True,
                "engines": ["limitup_continuation", "accumulation_breakout"],
                "limitup_continuation": {"enabled": True, "score_floor": 60, "min_adv_cny": 60_000_000},
                "accumulation_breakout": {"enabled": True, "score_floor": 60, "min_adv_cny": 60_000_000},
            },
        },
        csi300_df=_flat_csi(),
    )

    subtypes = {getattr(sig, "subtype") for _engine, sig in candidates}
    assert "limitup_continuation_alpha" in subtypes
    assert "accumulation_breakout_alpha" in subtypes


def test_alpha_tournament_limitup_acceptance_profile_is_research_only_and_top1():
    base = {
        "mean_reversion": {"enabled": True},
        "sniper": {"enabled": True},
        "book_size": {
            "breadth_floor": 0.15,
            "max_per_sector": 2,
            "bull": {"max_picks": 2, "min_score": 90},
            "bear": {"max_picks": 0, "min_score": 999},
            "choppy": {"max_picks": 0, "min_score": 999},
        },
        "acceptance": {
            "dq_full_threshold": 55,
            "dq_selective_threshold": 20,
            "max_full": 2,
            "max_selective": 2,
        },
        "alpha_candidates": {
            "enabled": True,
            "engines": ["rs_pullback"],
            "rs_pullback": {"enabled": True, "score_floor": 90},
            "limitup_continuation": {"enabled": False, "score_floor": 78},
        },
    }
    variant = {
        "engines": ["limitup_continuation"],
        "overrides": {"limitup_continuation": {"score_floor": 70}},
        "root_overrides": {
            "book_size": {"bull": {"max_picks": 1, "min_score": 70}, "max_per_sector": 1},
            "acceptance": {"dq_full_threshold": 45, "dq_selective_threshold": 15, "max_full": 1, "max_selective": 1},
        },
    }

    cfg = build_config(base, "limitup_continuation_acceptance_top1", variant)

    assert cfg["mean_reversion"]["enabled"] is False
    assert cfg["sniper"]["enabled"] is False
    assert cfg["alpha_candidates"]["engines"] == ["limitup_continuation"]
    assert cfg["alpha_candidates"]["rs_pullback"]["score_floor"] == 90
    assert cfg["alpha_candidates"]["limitup_continuation"]["enabled"] is True
    assert cfg["alpha_candidates"]["limitup_continuation"]["score_floor"] == 70
    assert cfg["book_size"]["bull"] == {"max_picks": 1, "min_score": 70}
    assert cfg["book_size"]["bear"] == {"max_picks": 0, "min_score": 999}
    assert cfg["book_size"]["max_per_sector"] == 1
    assert cfg["acceptance"]["max_full"] == 1
    assert cfg["acceptance"]["dq_selective_threshold"] == 15


def test_alpha_tournament_sniper_mas_profile_is_research_only_and_live_gated():
    from scripts.alpha_tournament import VARIANTS

    base = {
        "mean_reversion": {"enabled": True},
        "sniper": {"enabled": True},
        "book_size": {"bull": {"max_picks": 2, "min_score": 90}},
        "acceptance": {"max_full": 2, "max_selective": 2},
        "alpha_candidates": {"enabled": True, "engines": ["rs_pullback"], "rs_pullback": {"enabled": True, "score_floor": 90}},
    }

    cfg = build_config(base, "sniper_breakout_mas_top1", VARIANTS["sniper_breakout_mas_top1"])

    assert cfg["mean_reversion"]["enabled"] is False
    assert cfg["sniper"]["enabled"] is False
    assert cfg["alpha_candidates"]["enabled"] is True
    assert cfg["alpha_candidates"]["engines"] == ["sniper_breakout"]
    assert cfg["alpha_candidates"]["rs_pullback"]["score_floor"] == 90
    assert cfg["alpha_candidates"]["sniper_breakout"]["enabled"] is True
    assert cfg["alpha_candidates"]["sniper_breakout"]["score_floor"] == 70
    assert cfg["alpha_candidates"]["sniper_breakout"]["max_entry_pct"] == 0.03
    assert cfg["book_size"]["bull"] == {"max_picks": 1, "min_score": 70}
    assert cfg["acceptance"]["max_full"] == 1


def test_alpha_tournament_dp_sniper_lowrisk_profile_keeps_research_gate():
    from scripts.alpha_tournament import VARIANTS

    base = {
        "mean_reversion": {"enabled": True},
        "sniper": {"enabled": True},
        "book_size": {"bull": {"max_picks": 2, "min_score": 90}},
        "acceptance": {"max_full": 2, "max_selective": 2},
        "alpha_candidates": {"enabled": True, "engines": ["rs_pullback"], "rs_pullback": {"enabled": True, "score_floor": 90}},
    }

    cfg = build_config(base, "dp_sniper_breakout_lowrisk_rr_top1", VARIANTS["dp_sniper_breakout_lowrisk_rr_top1"])

    assert cfg["mean_reversion"]["enabled"] is False
    assert cfg["sniper"]["enabled"] is False
    assert cfg["alpha_candidates"]["engines"] == ["sniper_breakout"]
    assert cfg["alpha_candidates"]["sniper_breakout"]["max_stop_risk_pct"] == 4.75
    assert cfg["alpha_candidates"]["sniper_breakout"]["stop_atr_mult"] == 1.6
    assert cfg["book_size"]["bull"] == {"max_picks": 1, "min_score": 70}
    assert cfg["acceptance"]["max_selective"] == 1


def test_default_config_declares_northbound_paper_sleeve_as_parallel_feed_only():
    from src.core.config import load_config

    cfg = load_config("config/default.yaml")
    sleeve_cfg = cfg["external_paper_sleeves"]["sleeves"]["northbound_active_riskband_paper"]

    assert cfg["external_paper_sleeves"]["enabled"] is True
    assert sleeve_cfg["enabled"] is True
    assert sleeve_cfg["status"] == "parallel_paper_feed_only"
    assert "parallel paper feed" in sleeve_cfg["rule"]
    assert "51.98% WR" in sleeve_cfg["promotion_blockers"][0]
    assert sleeve_cfg["artifacts"]["bracket_summary_csv"].endswith("northbound_bracket_summary.csv")
    assert sleeve_cfg["artifacts"]["note"] == "references/northbound-active-riskband-paper-2026-06.md"


def test_alpha_tournament_summary_metrics_and_hash_are_machine_readable(tmp_path):
    summary_path = tmp_path / "backtest_summary.json"
    summary_path.write_text(
        """{
          "total_picks": 7,
          "entry_skipped_no_chase": 1,
          "pnl_win_rate": 0.5714,
          "target_hit_rate": 0.2857,
          "true_expectancy_pct": 2.34,
          "weighted_expectancy_pct": 2.1,
          "max_drawdown_pct": 8.9,
          "cumulative_pnl_pct": 14.2,
          "final_equity_multiple": 1.14,
          "price_data_fingerprint": "abc123",
          "price_snapshot_mode": "read_write",
          "price_snapshot_dir": "/tmp/snap",
          "download_bad_ticker_count": 2,
          "download_bad_tickers": ["000001.SZ", "000002.SZ"]
        }""",
        encoding="utf-8",
    )
    detail_path = tmp_path / "backtest_detail.csv"
    detail_path.write_text("ticker,pnl_pct\n000001.SZ,1.2\n", encoding="utf-8")

    metrics = _load_summary_metrics(summary_path, "ignored log text")

    assert metrics["picks"] == 7
    assert metrics["skipped"] == 1
    assert metrics["win_rate_pct"] == 57.1
    assert metrics["target_hit_rate_pct"] == 28.6
    assert metrics["true_expectancy_pct"] == 2.34
    assert metrics["equity_multiple"] == 1.14
    assert metrics["price_data_fingerprint"] == "abc123"
    assert metrics["price_snapshot_mode"] == "read_write"
    assert metrics["price_snapshot_dir"] == "/tmp/snap"
    assert metrics["download_bad_ticker_count"] == 2
    assert metrics["download_bad_tickers"] == ["000001.SZ", "000002.SZ"]
    actual_hash = detail_hash(detail_path)
    assert actual_hash == detail_hash(detail_path)
    assert actual_hash is not None
    assert len(actual_hash) == 64
