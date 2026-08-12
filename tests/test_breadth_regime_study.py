"""Invariants for the breadth-gate study.

The study exists to decide whether production changes, so its own arithmetic has
to be auditable: no lookahead, variants that really are supersets/subsets of the
baseline gate, and an overlay that reduces to buy-and-hold when it is always long
and costs are zero.
"""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def study():
    spec = importlib.util.spec_from_file_location(
        "breadth_regime_study", PROJECT_ROOT / "scripts" / "breadth_regime_study.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["breadth_regime_study"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def synthetic():
    n = 400
    dates = pd.bdate_range("2024-01-01", periods=n)
    rng = np.random.default_rng(0)
    close = pd.Series(100 * np.cumprod(1 + rng.normal(0.0004, 0.012, n)), index=dates)
    breadth = pd.Series(np.clip(rng.normal(0.5, 0.15, n), 0, 1), index=dates)
    return pd.DataFrame({"close": close}), breadth


def test_breadth_or_is_a_superset_and_and_is_a_subset(study, synthetic):
    px, breadth = synthetic
    sig = study.gate_signals(px, 20, 50, breadth, 0.60)

    assert (sig["breadth_or"] >= sig["baseline"]).all()
    assert (sig["breadth_and"] <= sig["baseline"]).all()


def test_breadth_or_equals_the_production_rule_with_an_extra_door(study, synthetic):
    """bull iff close>SMA_s and (SMA_s>SMA_l or breadth>=B)."""
    px, breadth = synthetic
    close = px["close"]
    sma_s, sma_l = close.rolling(20).mean(), close.rolling(50).mean()
    expected = (close > sma_s) & ((sma_s > sma_l) | (breadth >= 0.60))

    sig = study.gate_signals(px, 20, 50, breadth, 0.60)

    assert sig["breadth_or"].equals(expected.astype(bool))


def test_position_is_held_the_session_after_the_signal(study, synthetic):
    px, breadth = synthetic
    sig = study.gate_signals(px, 20, 50, breadth, 0.60)

    d = study.run_overlay(px, sig["baseline"], 5)

    assert d["pos"].iloc[0] == 0.0  # nothing is known before the first close
    for i in (5, 100, 399):
        assert bool(d["pos"].iloc[i]) == bool(sig["baseline"].iloc[i - 1])


def test_always_long_at_zero_cost_reproduces_buy_and_hold(study, synthetic):
    px, _ = synthetic
    always = pd.Series(True, index=px.index)

    d = study.run_overlay(px, always, 0)

    bh = px["close"].pct_change()
    assert np.allclose(d["stratret"].values[2:], bh.values[2:], atol=1e-12)


def test_costs_are_charged_on_every_switch(study, synthetic):
    px, _ = synthetic
    flip = pd.Series([i % 2 == 0 for i in range(len(px))], index=px.index)

    free = study.run_overlay(px, flip, 0)["stratret"].sum()
    charged = study.run_overlay(px, flip, 10)["stratret"].sum()

    assert charged < free


def test_marginal_days_counts_only_variant_only_sessions(study, synthetic):
    px, breadth = synthetic
    sig = study.gate_signals(px, 20, 50, breadth, 0.60)

    mg = study.marginal_days(px, sig["baseline"], sig["breadth_or"])

    expected = int(
        (sig["breadth_or"] & ~sig["baseline"]).astype(bool).shift(1, fill_value=False).sum()
    )
    assert mg["n_extra_days"] == expected


def test_reentry_lead_is_zero_when_the_variant_matches_the_baseline(study, synthetic):
    px, breadth = synthetic
    sig = study.gate_signals(px, 20, 50, breadth, 0.60)

    leads = study.reentry_lead(px.index, sig["baseline"], sig["baseline"])

    assert leads and all(e["lead_sessions"] == 0 for e in leads)


def test_reentry_lead_measures_sessions_opened_early(study):
    dates = pd.bdate_range("2024-01-01", periods=10)
    base = pd.Series([False] * 6 + [True] * 4, index=dates)
    variant = pd.Series([False] * 3 + [True] * 7, index=dates)

    leads = study.reentry_lead(dates, base, variant)

    assert leads == [
        {"baseline_entry": "2024-01-09", "variant_entry": "2024-01-04", "lead_sessions": 3}
    ]


def test_metrics_reports_drawdown_and_exposure(study):
    ret = np.array([0.01, -0.02, 0.03, -0.01])
    pos = pd.Series([1.0, 1.0, 0.0, 1.0])

    m = study.metrics(ret, pos)

    assert m["maxdd"] > 0
    assert m["expo"] == pytest.approx(75.0)
    assert m["switch"] == 2
