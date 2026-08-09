from datetime import date
import sys

import pytest

from src.core.xsec_replay import (
    Bar,
    ReplayInput,
    ReplayStatus,
    TickerReplay,
    replay_cross_section,
)


SIGNAL_DATE = date(2025, 1, 2)
ENTRY_DATE = date(2025, 1, 3)
EXIT_DATE = date(2025, 1, 6)


def bar(day: date, *, open: float = 100.0, close: float = 110.0) -> Bar:
    return Bar(day=day, open=open, high=max(open, close), low=min(open, close), close=close, volume=1_000)


def filled_ticker(ticker: str = "000001.SZ", **changes: object) -> TickerReplay:
    values: dict[str, object] = {
        "ticker": ticker,
        "signal_date": SIGNAL_DATE,
        "next_session": bar(ENTRY_DATE, open=100.0, close=999.0),
        "exit_session": bar(EXIT_DATE, open=108.0, close=110.0),
        "capacity_available": True,
    }
    values.update(changes)
    return TickerReplay(**values)


def request(*tickers: TickerReplay, cap: float = 105.0, cost_bps: float = 20.0) -> ReplayInput:
    return ReplayInput(selected=tickers, max_entry_cap=cap, total_cost_bps=cost_bps)


def test_entry_uses_next_session_open_not_signal_day_close() -> None:
    result = replay_cross_section(request(filled_ticker()))

    outcome = result.outcomes[0]
    assert outcome.status is ReplayStatus.FILLED
    assert outcome.entry_price == 100.0
    assert outcome.gross_return == pytest.approx(0.10)
    assert outcome.net_return == pytest.approx(0.098)


def assert_valid_control() -> None:
    assert replay_cross_section(request(filled_ticker())).outcomes[0].status is ReplayStatus.FILLED


def test_open_above_cap_is_chase_no_fill_without_allocation() -> None:
    assert_valid_control()

    result = replay_cross_section(request(filled_ticker(next_session=bar(ENTRY_DATE, open=106.0)), cap=105.0))

    outcome = result.outcomes[0]
    assert outcome.status is ReplayStatus.NO_FILL_CHASE
    assert outcome.entry_price is None
    assert outcome.net_return is None
    assert result.totals.no_fill == 1
    assert result.totals.eligible == 1


def test_missing_next_session_is_censored_without_allocation() -> None:
    assert_valid_control()

    result = replay_cross_section(request(filled_ticker(next_session=None)))

    outcome = result.outcomes[0]
    assert outcome.status is ReplayStatus.CENSORED_MISSING_ENTRY
    assert outcome.entry_price is None
    assert outcome.gross_return is None
    assert result.totals.censored == 1
    assert result.totals.eligible == 0


def test_missing_exit_is_censored_without_return_allocation() -> None:
    assert_valid_control()

    result = replay_cross_section(request(filled_ticker(exit_session=None)))

    outcome = result.outcomes[0]
    assert outcome.status is ReplayStatus.CENSORED_MISSING_EXIT
    assert outcome.entry_price == 100.0
    assert outcome.net_return is None
    assert result.totals.censored == 1
    assert result.totals.eligible == 1


def test_capacity_censor_is_checked_before_missing_entry_data() -> None:
    assert_valid_control()

    result = replay_cross_section(request(filled_ticker(capacity_available=False, next_session=None)))

    outcome = result.outcomes[0]
    assert outcome.status is ReplayStatus.CENSORED_CAPACITY
    assert result.totals.capacity_censored == 1
    assert result.totals.censored == 1


def test_each_selected_ticker_keeps_one_outcome_including_censored_rows() -> None:
    assert_valid_control()
    result = replay_cross_section(request(
        filled_ticker("000001.SZ"),
        filled_ticker("000002.SZ", next_session=bar(ENTRY_DATE, open=106.0)),
        filled_ticker("000003.SZ", next_session=None),
        filled_ticker("000004.SZ", exit_session=None),
        filled_ticker("000005.SZ", capacity_available=False),
    ))

    assert [outcome.ticker for outcome in result.outcomes] == [
        "000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ",
    ]
    assert len(result.outcomes) == result.totals.selected == 5
    assert result.totals.filled + result.totals.no_fill + result.totals.censored == result.totals.selected
    assert result.totals.eligible == 3  # filled + chase no-fill + entry with missing exit
    assert result.totals.capacity_censored == 1


def test_replay_input_requires_immutable_selected_tuple() -> None:
    assert_valid_control()

    mutable = ReplayInput(selected=[filled_ticker()], max_entry_cap=105.0, total_cost_bps=20.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="selected must be a tuple"):
        replay_cross_section(mutable)


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"next_session": bar(SIGNAL_DATE)}, "next-session date must follow signal date"),
        ({"exit_session": bar(ENTRY_DATE)}, "exit-session date must follow entry date"),
    ],
)
def test_date_order_invalids_fail_explicitly(changes: dict[str, object], message: str) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match=message):
        replay_cross_section(request(filled_ticker(**changes)))


def test_cost_is_deducted_once_from_completed_fill_only() -> None:
    assert_valid_control()

    result = replay_cross_section(request(filled_ticker(), cost_bps=25.0))

    assert result.totals.gross_return_total == pytest.approx(0.10)
    assert result.totals.net_return_total == pytest.approx(0.0975)


@pytest.mark.parametrize(
    "replay, message",
    [
        (lambda: request(filled_ticker("000001.SZ"), filled_ticker("000001.SZ")), "duplicate selected ticker"),
        (lambda: request(filled_ticker(next_session=bar(ENTRY_DATE, open=0.0))), "positive prices"),
        (lambda: request(filled_ticker(), cap=0.0), "max_entry_cap must be positive"),
        (lambda: request(filled_ticker(), cost_bps=0.0), "total_cost_bps must be positive"),
    ],
)
def test_duplicate_and_nonpositive_inputs_fail_closed(replay, message: str) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match=message):
        replay_cross_section(replay())


@pytest.mark.parametrize(
    "replay",
    [
        lambda: request(filled_ticker(), cap=True),
        lambda: request(filled_ticker(), cost_bps=True),
        lambda: request(filled_ticker(next_session=bar(ENTRY_DATE, open=True))),
        lambda: request(filled_ticker(next_session=Bar(ENTRY_DATE, 1.0, True, 1.0, 1.0, 1_000))),
        lambda: request(filled_ticker(next_session=Bar(ENTRY_DATE, 100.0, 100.0, True, 100.0, 1_000))),
        lambda: request(filled_ticker(next_session=Bar(ENTRY_DATE, 100.0, 100.0, 100.0, True, 1_000))),
        lambda: request(filled_ticker(next_session=Bar(ENTRY_DATE, 100.0, 100.0, 100.0, 100.0, True))),
    ],
)
def test_boolean_numeric_values_fail_closed(replay) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match="positive|non-negative"):
        replay_cross_section(replay())


@pytest.mark.parametrize(
    "replay",
    [
        lambda: object(),
        lambda: ReplayInput(selected=(object(),), max_entry_cap=105.0, total_cost_bps=20.0),  # type: ignore[arg-type]
        lambda: request(filled_ticker(next_session=object())),
    ],
)
def test_malformed_replay_objects_normalize_to_value_error(replay) -> None:
    assert_valid_control()

    with pytest.raises(ValueError):
        replay_cross_section(replay())  # type: ignore[arg-type]


@pytest.mark.parametrize("exit_day", [SIGNAL_DATE, date(2025, 1, 1)])
def test_exit_date_must_follow_signal_when_entry_is_missing(exit_day: date) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match="exit-session date must follow signal date"):
        replay_cross_section(request(filled_ticker(next_session=None, exit_session=bar(exit_day))))


@pytest.mark.parametrize(
    "invalid_bar",
    [
        Bar(ENTRY_DATE, 99.0, 101.0, 100.0, 100.0, 1_000),
        Bar(ENTRY_DATE, 102.0, 101.0, 100.0, 100.0, 1_000),
        Bar(ENTRY_DATE, 100.0, 101.0, 100.0, 99.0, 1_000),
        Bar(ENTRY_DATE, 100.0, 101.0, 100.0, 102.0, 1_000),
        Bar(ENTRY_DATE, 100.0, 100.0, 101.0, 100.0, 1_000),
    ],
)
def test_ohlc_prices_must_be_inside_the_daily_range(invalid_bar: Bar) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match="OHLC range"):
        replay_cross_section(request(filled_ticker(next_session=invalid_bar)))


@pytest.mark.parametrize(
    "replay",
    [
        lambda: request(filled_ticker(
            next_session=bar(ENTRY_DATE, open=sys.float_info.min, close=sys.float_info.min),
            exit_session=bar(EXIT_DATE, open=sys.float_info.max, close=sys.float_info.max),
        )),
        lambda: request(
            filled_ticker("000001.SZ", next_session=bar(ENTRY_DATE, open=1.0, close=1.0), exit_session=bar(EXIT_DATE, open=sys.float_info.max, close=sys.float_info.max)),
            filled_ticker("000002.SZ", next_session=bar(ENTRY_DATE, open=1.0, close=1.0), exit_session=bar(EXIT_DATE, open=sys.float_info.max, close=sys.float_info.max)),
        ),
    ],
)
def test_nonfinite_derived_returns_and_totals_fail_closed(replay) -> None:
    assert_valid_control()

    with pytest.raises(ValueError, match="return total|return"):
        replay_cross_section(replay())
