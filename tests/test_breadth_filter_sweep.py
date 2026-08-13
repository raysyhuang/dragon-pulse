"""Guards for the breadth-filter sweep.

The sweep decides whether a gate change reaches production, and its whole
validity rests on one property: each filtered arm must be a strict subset of the
baseline picks, joined to breadth that was knowable on the signal day. A silent
join failure would drop trades and flatter every filtered arm at once.
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def sweep():
    spec = importlib.util.spec_from_file_location(
        "breadth_filter_pit_sweep", PROJECT_ROOT / "scripts" / "breadth_filter_pit_sweep.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["breadth_filter_pit_sweep"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def trades():
    return pd.DataFrame(
        {
            "ticker": ["A", "B", "C", "D"],
            "date": ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"],
            "pnl_pct": [1.0, -2.0, 3.0, -1.0],
        }
    )


@pytest.fixture
def breadth():
    return pd.Series(
        {"2026-01-05": 0.30, "2026-01-06": 0.45, "2026-01-07": 0.60, "2026-01-08": 0.75}
    )


def test_attach_breadth_maps_each_trade_to_its_signal_day(sweep, trades, breadth):
    out = sweep.attach_breadth(trades, breadth)

    assert list(out["_breadth"]) == [0.30, 0.45, 0.60, 0.75]


def test_attach_breadth_refuses_a_broken_join(sweep, trades, breadth):
    with pytest.raises(ValueError, match="join is broken"):
        sweep.attach_breadth(trades, breadth.drop("2026-01-07"))


def test_attach_breadth_normalises_timestamp_dates(sweep, trades, breadth):
    stamped = trades.assign(date=pd.to_datetime(trades["date"]))

    out = sweep.attach_breadth(stamped, breadth)

    assert not out["_breadth"].isna().any()


def test_every_filtered_arm_is_a_subset_of_baseline(sweep, trades, breadth):
    joined = sweep.attach_breadth(trades, breadth)
    baseline = set(zip(joined["ticker"], joined["date"]))

    for _label, thr in sweep.build_cuts(breadth, [], [0.25, 0.5, 0.75]):
        kept = joined[joined["_breadth"] >= thr]
        assert set(zip(kept["ticker"], kept["date"])) <= baseline


def test_higher_cutoffs_keep_no_more_trades(sweep, trades, breadth):
    joined = sweep.attach_breadth(trades, breadth)

    counts = [len(joined[joined["_breadth"] >= t]) for t in (0.30, 0.45, 0.60, 0.75)]

    assert counts == sorted(counts, reverse=True)


def test_build_cuts_reads_quantiles_off_the_scanned_distribution(sweep, breadth):
    cuts = dict(sweep.build_cuts(breadth, [0.5], [0.0, 1.0]))

    assert cuts["abs50"] == 0.50
    assert cuts["q0"] == pytest.approx(0.30)
    assert cuts["q100"] == pytest.approx(0.75)


def test_quantiles_use_all_scanned_sessions_not_just_traded_ones(sweep, trades):
    # Two extra quiet sessions the engine skipped: they must still shape the cut.
    scanned = pd.Series(
        {
            "2026-01-05": 0.30,
            "2026-01-06": 0.45,
            "2026-01-07": 0.60,
            "2026-01-08": 0.75,
            "2026-01-09": 0.10,
            "2026-01-12": 0.15,
        }
    )

    traded_only = dict(sweep.build_cuts(scanned[:4], [], [0.5]))["q50"]
    all_sessions = dict(sweep.build_cuts(scanned, [], [0.5]))["q50"]

    assert all_sessions < traded_only
