"""Independent adversarial verification for Task 3 (cross-sectional replay accounting).

Invariants were fixed in a pre-push attack catalogue written from the Task 3 spec on a
separate machine; only the binding to the real dataclasses was added afterwards. The
invariants were not revised to match what the implementation happens to do.

Ranked by how badly a miss corrupts a downstream result: conservation first, because a
broken partition silently inflates every performance number computed on top of it.
"""

from __future__ import annotations

import copy
import math
from datetime import date

import pytest

from src.core.xsec_replay import (
    Bar,
    Outcome,
    ReplayInput,
    ReplayStatus,
    TickerReplay,
    replay_cross_section,
)

SIGNAL = date(2026, 1, 5)
NEXT = date(2026, 1, 6)
EXIT = date(2026, 1, 9)


def bar(day: date = NEXT, o=10.0, h=11.0, l=9.5, c=10.5, v=1000.0) -> Bar:
    return Bar(day=day, open=o, high=h, low=l, close=c, volume=v)


def flat(day: date, price: float, v: float = 1000.0) -> Bar:
    """A valid bar sitting at one price. Needed because o=99 with default h/l is an
    impossible OHLC - their validator was right to reject my first fixture."""
    return Bar(day=day, open=price, high=price, low=price, close=price, volume=v)


def leg(ticker="600000.SH", *, nxt=..., ext=..., capacity=True,
        signal: date = SIGNAL) -> TickerReplay:
    return TickerReplay(
        ticker=ticker,
        signal_date=signal,
        next_session=bar(NEXT, o=10.0) if nxt is ... else nxt,
        exit_session=flat(EXIT, 11.0) if ext is ... else ext,
        capacity_available=capacity,
    )


def run(legs, cap=1e9, cost_bps=1.0):   # costs must be > 0: the core forbids a cost-free replay
    return replay_cross_section(
        ReplayInput(selected=tuple(legs), max_entry_cap=cap, total_cost_bps=cost_bps)
    )


def status_of(result, ticker) -> ReplayStatus:
    return next(o.status for o in result.outcomes if o.ticker == ticker)


def assert_rejected(legs, expect: str | tuple[str, ...], **kw):
    """Rejection must be typed and cite the intended reason. A bare KeyError/TypeError
    escaping the module means fail-closed was incidental, not deliberate."""
    with pytest.raises((ValueError, TypeError)) as exc:
        run(legs, **kw)
    msg = str(exc.value).lower()
    wanted = (expect,) if isinstance(expect, str) else expect
    assert any(w.lower() in msg for w in wanted), (
        f"rejected, but not for the intended reason.\n  wanted one of: {wanted}\n  got: {exc.value}"
    )


# --------------------------------------------------------------------------------------
# A. Conservation — the partition must be exact
# --------------------------------------------------------------------------------------


def test_A1_partition_reconciles_exactly():
    result = run([
        leg("A"),                                   # filled
        leg("B", nxt=flat(NEXT, 99.0)),            # no-fill (over cap)
        leg("C", nxt=None),                         # censored missing entry
        leg("D", ext=None),                         # censored missing exit
        leg("E", capacity=False),                   # capacity censored
    ], cap=50.0)
    t = result.totals
    assert t.selected == 5
    assert t.selected == t.filled + t.no_fill + t.censored, (
        f"partition does not reconcile: {t}"
    )
    assert t.capacity_censored <= t.censored, "capacity rows must not double-count"


def test_A2_every_selected_ticker_appears_exactly_once():
    tickers = ["A", "B", "C", "D", "E"]
    result = run([
        leg("A"), leg("B", nxt=flat(NEXT, 99.0)), leg("C", nxt=None),
        leg("D", ext=None), leg("E", capacity=False),
    ], cap=50.0)
    seen = [o.ticker for o in result.outcomes]
    assert sorted(seen) == sorted(tickers), f"names dropped or duplicated: {seen}"
    assert len(seen) == len(set(seen)), "a ticker appeared twice"
    assert len(result.outcomes) == result.totals.selected


def test_A4_empty_selection_is_zeroed_not_a_crash():
    result = run([])
    t = result.totals
    assert result.outcomes == ()
    assert (t.selected, t.filled, t.no_fill, t.censored) == (0, 0, 0, 0)


def test_A5_duplicate_ticker_rejected_not_silently_deduped():
    """Silent dedup would break A1 by shrinking the partition below `selected`."""
    assert_rejected([leg("A"), leg("A")], ("duplicate", "ticker"))


# --------------------------------------------------------------------------------------
# B. Entry timing — the original sin being fixed
# --------------------------------------------------------------------------------------


def test_B1_entry_is_the_next_session_open():
    result = run([leg("A", nxt=bar(NEXT, o=10.0, h=20.0, l=5.0, c=15.0))])
    out = next(o for o in result.outcomes)
    assert out.entry_price == 10.0, "entry must be the next-session OPEN"
    assert out.entry_price not in (15.0, 20.0, 5.0), "entry took a close/high/low"


def test_B3_bar_dated_on_or_before_signal_is_rejected():
    """A 'next session' dated at or before the signal date is lookahead by construction."""
    for day in (SIGNAL, date(2026, 1, 2)):
        assert_rejected([leg("A", nxt=flat(day, 10.0))], ("next", "signal", "date", "order"))


def test_B_exit_must_follow_entry():
    assert_rejected(
        [leg("A", nxt=flat(NEXT, 10.0), ext=flat(NEXT, 11.0))],
        ("exit", "date", "order", "after"),
    )


# --------------------------------------------------------------------------------------
# C. No-fill and the chase cap
# --------------------------------------------------------------------------------------


def test_C1_over_cap_is_no_fill_with_zero_pnl():
    result = run([leg("A", nxt=flat(NEXT, 10.5))], cap=10.0)
    out = result.outcomes[0]
    assert out.status is ReplayStatus.NO_FILL_CHASE
    assert out.gross_return in (None, 0.0) and out.net_return in (None, 0.0)
    assert result.totals.gross_return_total == 0.0


def test_C2_cap_boundary_is_documented_and_inclusive():
    """Entry exactly at the cap. Either convention is defensible, but Task 4 consumes it,
    so it must be pinned by a test rather than left implicit."""
    result = run([leg("A", nxt=flat(NEXT, 10.0))], cap=10.0)
    assert result.outcomes[0].status is ReplayStatus.FILLED, (
        "entry exactly at the cap was treated as a chase; if intentional, document it"
    )


def test_C4_no_fill_excluded_from_filled_denominator():
    result = run([leg("A", nxt=flat(NEXT, 99.0)), leg("B", nxt=bar(NEXT, o=10.0))], cap=50.0)
    assert result.totals.filled == 1
    assert result.totals.no_fill == 1


def test_C5_no_fill_carries_no_costs():
    result = run([leg("A", nxt=flat(NEXT, 99.0))], cap=50.0, cost_bps=100.0)
    assert result.totals.net_return_total == 0.0, "costs were charged on a no-fill row"


# --------------------------------------------------------------------------------------
# D. Censoring — absence recorded, never dropped
# --------------------------------------------------------------------------------------


def test_D1_D2_missing_bars_are_distinctly_censored():
    result = run([leg("A", nxt=None), leg("B", ext=None)])
    assert status_of(result, "A") is ReplayStatus.CENSORED_MISSING_ENTRY
    assert status_of(result, "B") is ReplayStatus.CENSORED_MISSING_EXIT


def test_D3_censored_rows_carry_no_pnl():
    result = run([leg("A", nxt=None), leg("B", ext=None)], cost_bps=100.0)
    for out in result.outcomes:
        assert out.gross_return in (None, 0.0)
        assert out.net_return in (None, 0.0)
    assert result.totals.gross_return_total == 0.0
    assert result.totals.net_return_total == 0.0


def test_D4_fillable_without_exit_is_censored_not_a_flat_trade():
    """The most dangerous silent-drop variant: missing data disguised as a 0% trade."""
    result = run([leg("A", nxt=flat(NEXT, 10.0), ext=None)])
    out = result.outcomes[0]
    assert out.status is ReplayStatus.CENSORED_MISSING_EXIT
    assert out.status is not ReplayStatus.FILLED
    assert out.gross_return != 0.0 or out.gross_return is None, (
        "a missing exit was recorded as a flat 0% return"
    )
    assert result.totals.filled == 0


# --------------------------------------------------------------------------------------
# E. Capacity
# --------------------------------------------------------------------------------------


def test_E1_capacity_unavailable_is_explicit_row():
    result = run([leg("A", capacity=False)])
    assert result.outcomes[0].status is ReplayStatus.CENSORED_CAPACITY
    assert result.totals.capacity_censored == 1


def test_E3_capacity_rows_carry_no_pnl():
    result = run([leg("A", capacity=False)], cost_bps=100.0)
    assert result.totals.gross_return_total == 0.0
    assert result.totals.net_return_total == 0.0


def test_E4_probe_status_precedence(capsys):
    """When several censoring conditions apply at once, precedence must be deterministic.
    The spec does not fix an order, so this reports rather than asserts - but Task 4's
    denominators depend on it, so it must be stated somewhere."""
    combos = {
        "no_capacity + missing_entry": leg("A", capacity=False, nxt=None),
        "no_capacity + missing_exit": leg("B", capacity=False, ext=None),
        "no_capacity + over_cap": leg("C", capacity=False, nxt=flat(NEXT, 99.0)),
        "missing_entry + missing_exit": leg("D", nxt=None, ext=None),
    }
    observed = {k: run([v], cap=50.0).outcomes[0].status.value for k, v in combos.items()}
    with capsys.disabled():
        print(f"\n[PROBE] status precedence: {observed}")


# --------------------------------------------------------------------------------------
# F. Cost arithmetic
# --------------------------------------------------------------------------------------


def test_F1_net_is_gross_minus_costs():
    result = run([leg("A", nxt=bar(NEXT, o=10.0), ext=flat(EXIT, 11.0))],
                 cost_bps=50.0)
    out = result.outcomes[0]
    assert out.gross_return is not None and out.net_return is not None
    assert out.net_return < out.gross_return, "costs did not reduce the return"
    assert out.net_return == pytest.approx(out.gross_return - 50.0 / 10000.0, abs=1e-9)


def test_F_cost_free_replay_is_unconstructible():
    """Restated as an invariant because it is the property that matters downstream:
    no configuration of this core can produce an uncosted return."""
    for bad in (0.0, -0.0, -1.0, float("nan")):
        assert_rejected([leg("A")], ("cost", "positive"), cost_bps=bad)


def test_F3_zero_cost_is_refused_by_design():
    """The core REFUSES total_cost_bps <= 0. That makes a cost-free performance number
    unconstructible at this layer, which is a deliberate and welcome guard rather than a
    limitation - so it is pinned here as intended behaviour."""
    assert_rejected([leg("A")], ("cost", "positive"), cost_bps=0.0)


def test_F_negative_cost_is_rejected():
    """A negative cost is a subsidy; it would silently manufacture return."""
    assert_rejected([leg("A")], ("cost", "negative", "non-negative"), cost_bps=-1.0)


# --------------------------------------------------------------------------------------
# G. Numeric robustness
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [True, False])
def test_G1_bool_price_is_rejected(value):
    """isinstance(True, int) is True in Python, so a naive numeric guard admits a bool
    and then arithmetics it as 1.0/0.0."""
    assert_rejected([leg("A", nxt=bar(NEXT, o=value))], ("positive prices", "number", "bool"))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_G2_nan_and_inf_prices_rejected(value):
    assert_rejected([leg("A", nxt=bar(NEXT, o=value))], ("positive prices", "finite", "number"))


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_G3_non_positive_prices_rejected(value):
    assert_rejected([leg("A", nxt=bar(NEXT, o=value))], ("positive prices", "positive", "number"))


@pytest.mark.parametrize("value", ["10.0", None, [10.0], {"v": 1}])
def test_G4_malformed_price_types_raise_typed_error(value):
    assert_rejected([leg("A", nxt=bar(NEXT, o=value))], ("positive prices", "number", "type"))


def test_G4_malformed_bar_object_raises_typed_error():
    """A non-Bar object must not escape as AttributeError from deep inside."""
    assert_rejected([leg("A", nxt=object())], ("must be a bar", "type", "next"))


def test_G6_impossible_ohlc_rejected():
    for label, b in {
        "high<low": bar(NEXT, o=10.0, h=5.0, l=9.0, c=10.0),
        "open>high": bar(NEXT, o=99.0, h=11.0, l=9.0, c=10.0),
        "open<low": bar(NEXT, o=1.0, h=11.0, l=9.0, c=10.0),
        "close>high": bar(NEXT, o=10.0, h=11.0, l=9.0, c=99.0),
        "close<low": bar(NEXT, o=10.0, h=11.0, l=9.0, c=1.0),
    }.items():
        assert_rejected([leg("A", nxt=b)], ("ohlc range", "valid ohlc", "range"))


def test_G7_huge_prices_do_not_overflow_to_infinity():
    huge = 1e308
    result = run([leg("A", nxt=bar(NEXT, o=huge, h=huge, l=huge, c=huge),
                      ext=bar(EXIT, o=huge, h=huge, l=huge, c=huge))])
    for out in result.outcomes:
        if out.gross_return is not None:
            assert math.isfinite(out.gross_return), "P&L overflowed to infinity"
    assert math.isfinite(result.totals.gross_return_total)
    assert math.isfinite(result.totals.net_return_total)


def test_G_negative_volume_rejected():
    assert_rejected([leg("A", nxt=bar(NEXT, v=-1.0))], ("volume", "non-negative", "negative"))


# --------------------------------------------------------------------------------------
# H. Purity and determinism
# --------------------------------------------------------------------------------------


def test_H1_deterministic_across_calls():
    legs = [leg("A"), leg("B", nxt=None), leg("C", capacity=False)]
    first, second = run(legs), run(legs)
    assert first.outcomes == second.outcomes
    assert first.totals == second.totals


def test_H2_inputs_are_not_mutated():
    legs = [leg("A"), leg("B", nxt=None)]
    payload = ReplayInput(selected=tuple(legs), max_entry_cap=1e9, total_cost_bps=10.0)
    before = copy.deepcopy(payload)
    replay_cross_section(payload)
    assert payload == before, "the replay core mutated its caller's input"


def test_H4_output_row_order_is_deterministic():
    legs = [leg("C"), leg("A"), leg("B")]
    order_1 = [o.ticker for o in run(legs).outcomes]
    order_2 = [o.ticker for o in run(legs).outcomes]
    assert order_1 == order_2


# --------------------------------------------------------------------------------------
# I. Aggregate integrity
# --------------------------------------------------------------------------------------


def test_I1_totals_only_include_filled_legs():
    result = run([
        leg("A", nxt=bar(NEXT, o=10.0), ext=flat(EXIT, 11.0)),  # +10% filled
        leg("B", nxt=flat(NEXT, 99.0)),                                  # no-fill
        leg("C", nxt=None),                                               # censored
    ], cap=50.0)
    assert result.totals.filled == 1
    assert result.totals.gross_return_total == pytest.approx(0.10, abs=1e-9)


def test_I4_totals_recompute_from_emitted_rows():
    """Totals must be derivable from the rows; disagreement means one of them lies."""
    result = run([
        leg("A", nxt=bar(NEXT, o=10.0), ext=flat(EXIT, 11.0)),
        leg("B", nxt=flat(NEXT, 20.0), ext=flat(EXIT, 19.0)),   # a LOSING leg: totals must net out
        leg("C", nxt=None),
        leg("D", capacity=False),
    ], cost_bps=25.0)
    filled = [o for o in result.outcomes if o.status is ReplayStatus.FILLED]
    assert result.totals.filled == len(filled)
    assert result.totals.gross_return_total == pytest.approx(
        sum(o.gross_return for o in filled), abs=1e-9)
    assert result.totals.net_return_total == pytest.approx(
        sum(o.net_return for o in filled), abs=1e-9)


def test_I3_probe_all_censored_aggregates(capsys):
    """Sum over zero filled legs is legitimately 0.0. Flagged for Task 4: a MEAN over
    this input must divide by `filled` (0 -> undefined), never by `selected`, or 'no
    data' silently becomes 'no edge'."""
    result = run([leg("A", nxt=None), leg("B", ext=None), leg("C", capacity=False)])
    with capsys.disabled():
        print(f"\n[PROBE] all-censored totals: filled={result.totals.filled}, "
              f"gross_total={result.totals.gross_return_total}, "
              f"net_total={result.totals.net_return_total}")


def test_B2_exit_price_is_the_exit_close_not_its_open():
    """Distinct open and close on the exit bar. Found by mutation testing: every earlier
    fixture used flat bars where open == close, so swapping exit open for exit close was
    invisible - a fixture blind spot, not an implementation fault."""
    entry, exit_open, exit_close = 10.0, 12.0, 11.0
    result = run([leg("A",
                      nxt=flat(NEXT, entry),
                      ext=Bar(day=EXIT, open=exit_open, high=12.5, low=10.5,
                              close=exit_close, volume=1000.0))])
    out = result.outcomes[0]
    assert out.exit_price == exit_close, "exit took the OPEN instead of the close"
    assert out.gross_return == pytest.approx(exit_close / entry - 1, abs=1e-9)
    assert out.gross_return != pytest.approx(exit_open / entry - 1, abs=1e-9)


def test_B1_entry_price_distinguishable_from_its_own_close():
    """Symmetric guard on the entry leg: the next-session bar must contribute its OPEN."""
    result = run([leg("A",
                      nxt=Bar(day=NEXT, open=10.0, high=13.0, low=9.0, close=12.0, volume=1000.0),
                      ext=flat(EXIT, 11.0))])
    out = result.outcomes[0]
    assert out.entry_price == 10.0
    assert out.gross_return == pytest.approx(11.0 / 10.0 - 1, abs=1e-9)
