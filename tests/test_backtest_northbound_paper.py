import importlib.util
import math
import sys
from pathlib import Path

import pandas as pd


def load_module():
    module_path = Path(__file__).parent.parent / "scripts" / "backtest_northbound_paper.py"
    spec = importlib.util.spec_from_file_location("backtest_northbound_paper_test_module", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_profit_factor_edge_cases():
    module = load_module()

    assert math.isinf(module._profit_factor(pd.Series([1.0, 2.0])))
    assert module._profit_factor(pd.Series([-1.0, -2.0])) == 0.0
    assert module._profit_factor(pd.Series([2.0, -1.0, -1.0])) == 1.0


def test_variant_masks_and_top1_lowrisk_are_signal_time_only():
    module = load_module()
    df = pd.DataFrame([
        {"trade_date": 20250102, "ts_code": "000001.SZ", "rank": 1, "cons_fill": True, "stop_risk": 5.0, "change": 1.0, "ret20": 1.0, "ret60": 1.0},
        {"trade_date": 20250102, "ts_code": "000002.SZ", "rank": 2, "cons_fill": True, "stop_risk": 4.2, "change": -1.0, "ret20": -1.0, "ret60": -1.0},
        {"trade_date": 20250103, "ts_code": "000003.SZ", "rank": 6, "cons_fill": True, "stop_risk": 5.0, "change": 1.0, "ret20": 1.0, "ret60": 1.0},
        {"trade_date": 20250103, "ts_code": "000004.SZ", "rank": 1, "cons_fill": False, "stop_risk": 5.0, "change": 1.0, "ret20": 1.0, "ret60": 1.0},
        {"trade_date": 20250104, "ts_code": "000005.SZ", "rank": 1, "cons_fill": True, "stop_risk": 3.5, "change": -1.0, "ret20": 1.0, "ret60": 1.0},
    ])

    masks = module.add_variant_masks(df)

    assert masks["nb_top5"].tolist() == [True, True, False, False, True]
    assert masks["top5_lowrisk_4_7"].tolist() == [True, True, False, False, False]
    assert masks["top5_lowrisk_lt4"].tolist() == [False, False, False, False, True]
    # Per-day low-risk top1 picks the lower stop-risk name on 2025-01-02.
    assert masks["top5_lowrisk_4_7_top1_lowrisk"].tolist() == [False, True, False, False, False]


def test_split_summaries_uses_date_boundary_not_same_day_overlap():
    module = load_module()
    df = pd.DataFrame([
        {"trade_date": 20250101, "bracket_ret": 1.0, "bracket_reason": "time"},
        {"trade_date": 20250102, "bracket_ret": -1.0, "bracket_reason": "stop"},
        {"trade_date": 20250102, "bracket_ret": 2.0, "bracket_reason": "target"},
        {"trade_date": 20250103, "bracket_ret": 3.0, "bracket_reason": "target"},
    ])

    splits = pd.DataFrame(module.split_summaries(df, pd.Series([True] * 4), "x"))
    train = splits[splits["split"] == "train_70pct"].iloc[0]
    holdout = splits[splits["split"] == "holdout_30pct"].iloc[0]

    assert train["end"] < holdout["start"]
