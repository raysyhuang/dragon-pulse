"""Pure, research-only cross-sectional replay accounting.

This module deliberately has no selector, provider, network, or portfolio side effects.
It replays a caller-supplied fixed universe one selected ticker at a time.

Execution semantics:
* Capacity is checked first, after structural input validation.  An unavailable slot
  produces ``CENSORED_CAPACITY`` even when entry data is also unavailable.
* A valid entry is only the supplied next-session ``open``: its date must be later
  than ``signal_date``.  Signal-session closes are never inputs to execution.
* A missing next-session bar is ``CENSORED_MISSING_ENTRY``.  An open above the
  supplied cap is ``NO_FILL_CHASE``.  Neither receives P&L.
* A filled entry exits only at the supplied later exit-session ``close``.  A missing
  exit is ``CENSORED_MISSING_EXIT`` and has no return allocation.
* Completed fills use ``exit_close / entry_open - 1`` gross return; net return
  deducts ``total_cost_bps / 10_000`` exactly once.

``eligible`` counts rows with an observed, capacity-available entry opportunity:
completed fills, chase no-fills, and missing-exit censored entries.  The mutually
exclusive terminal buckets reconcile exactly as ``selected == filled + no_fill +
censored``; aggregate category counts keep censored rows visible rather than
dropping them.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
import math
from typing import Optional, Tuple


class ReplayStatus(str, Enum):
    FILLED = "FILLED"
    NO_FILL_CHASE = "NO_FILL_CHASE"
    CENSORED_MISSING_ENTRY = "CENSORED_MISSING_ENTRY"
    CENSORED_MISSING_EXIT = "CENSORED_MISSING_EXIT"
    CENSORED_CAPACITY = "CENSORED_CAPACITY"


@dataclass(frozen=True)
class Bar:
    day: date
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class TickerReplay:
    ticker: str
    signal_date: date
    next_session: Optional[Bar]
    exit_session: Optional[Bar]
    capacity_available: bool


@dataclass(frozen=True)
class ReplayInput:
    selected: Tuple[TickerReplay, ...]
    max_entry_cap: float
    total_cost_bps: float


@dataclass(frozen=True)
class Outcome:
    ticker: str
    status: ReplayStatus
    entry_price: Optional[float]
    exit_price: Optional[float]
    gross_return: Optional[float]
    net_return: Optional[float]


@dataclass(frozen=True)
class ReplayTotals:
    selected: int
    eligible: int
    filled: int
    no_fill: int
    censored: int
    capacity_censored: int
    gross_return_total: float
    net_return_total: float


@dataclass(frozen=True)
class ReplayResult:
    outcomes: Tuple[Outcome, ...]
    totals: ReplayTotals


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _validate_bar(ticker: str, label: str, value: Optional[Bar]) -> None:
    if value is None:
        return
    if not isinstance(value, Bar):
        raise ValueError(f"{ticker}: {label} must be a Bar")
    if not isinstance(value.day, date):
        raise ValueError(f"{ticker}: {label} date must be a date")
    prices = (value.open, value.high, value.low, value.close)
    if any(not _is_finite_number(price) or price <= 0 for price in prices):
        raise ValueError(f"{ticker}: {label} must have positive prices")
    if value.high < value.low or not (value.low <= value.open <= value.high) or not (value.low <= value.close <= value.high):
        raise ValueError(f"{ticker}: {label} must have a valid OHLC range")
    if not _is_finite_number(value.volume) or value.volume < 0:
        raise ValueError(f"{ticker}: {label} volume must be non-negative")


def _validate_replay(replay: ReplayInput) -> None:
    if not isinstance(replay, ReplayInput):
        raise ValueError("replay must be a ReplayInput")
    if not isinstance(replay.selected, tuple):
        raise ValueError("selected must be a tuple")
    if not _is_finite_number(replay.max_entry_cap) or replay.max_entry_cap <= 0:
        raise ValueError("max_entry_cap must be positive")
    if not _is_finite_number(replay.total_cost_bps) or replay.total_cost_bps <= 0:
        raise ValueError("total_cost_bps must be positive")

    seen: set[str] = set()
    for item in replay.selected:
        if not isinstance(item, TickerReplay):
            raise ValueError("selected item must be a TickerReplay")
        if not isinstance(item.ticker, str) or not item.ticker.strip():
            raise ValueError("selected ticker must be non-empty")
        if item.ticker in seen:
            raise ValueError(f"duplicate selected ticker: {item.ticker}")
        seen.add(item.ticker)
        if not isinstance(item.signal_date, date):
            raise ValueError(f"{item.ticker}: signal date must be a date")
        if not isinstance(item.capacity_available, bool):
            raise ValueError(f"{item.ticker}: capacity_available must be bool")
        _validate_bar(item.ticker, "next-session bar", item.next_session)
        _validate_bar(item.ticker, "exit-session bar", item.exit_session)
        if item.next_session is not None and item.next_session.day <= item.signal_date:
            raise ValueError(f"{item.ticker}: next-session date must follow signal date")
        if item.exit_session is not None and item.exit_session.day <= item.signal_date:
            raise ValueError(f"{item.ticker}: exit-session date must follow signal date")
        if (
            item.next_session is not None
            and item.exit_session is not None
            and item.exit_session.day <= item.next_session.day
        ):
            raise ValueError(f"{item.ticker}: exit-session date must follow entry date")


def replay_cross_section(replay: ReplayInput) -> ReplayResult:
    """Replay immutable supplied inputs without acquisition or strategy decisions."""
    _validate_replay(replay)

    outcomes = []
    for item in replay.selected:
        if not item.capacity_available:
            outcomes.append(Outcome(item.ticker, ReplayStatus.CENSORED_CAPACITY, None, None, None, None))
            continue
        if item.next_session is None:
            outcomes.append(Outcome(item.ticker, ReplayStatus.CENSORED_MISSING_ENTRY, None, None, None, None))
            continue
        if item.next_session.open > replay.max_entry_cap:
            outcomes.append(Outcome(item.ticker, ReplayStatus.NO_FILL_CHASE, None, None, None, None))
            continue
        if item.exit_session is None:
            outcomes.append(Outcome(item.ticker, ReplayStatus.CENSORED_MISSING_EXIT, item.next_session.open, None, None, None))
            continue
        gross_return = item.exit_session.close / item.next_session.open - 1
        net_return = gross_return - replay.total_cost_bps / 10_000
        if not math.isfinite(gross_return) or not math.isfinite(net_return):
            raise ValueError(f"{item.ticker}: derived return must be finite")
        outcomes.append(Outcome(
            item.ticker, ReplayStatus.FILLED, item.next_session.open, item.exit_session.close,
            gross_return, net_return,
        ))

    completed = [outcome for outcome in outcomes if outcome.status is ReplayStatus.FILLED]
    eligible = sum(outcome.status in {ReplayStatus.FILLED, ReplayStatus.NO_FILL_CHASE, ReplayStatus.CENSORED_MISSING_EXIT} for outcome in outcomes)
    no_fill = sum(outcome.status is ReplayStatus.NO_FILL_CHASE for outcome in outcomes)
    censored = sum(outcome.status.value.startswith("CENSORED_") for outcome in outcomes)
    capacity_censored = sum(outcome.status is ReplayStatus.CENSORED_CAPACITY for outcome in outcomes)
    gross_return_total = sum(outcome.gross_return for outcome in completed if outcome.gross_return is not None)
    net_return_total = sum(outcome.net_return for outcome in completed if outcome.net_return is not None)
    if not math.isfinite(gross_return_total) or not math.isfinite(net_return_total):
        raise ValueError("derived return total must be finite")
    return ReplayResult(
        outcomes=tuple(outcomes),
        totals=ReplayTotals(
            selected=len(outcomes), eligible=eligible, filled=len(completed), no_fill=no_fill,
            censored=censored, capacity_censored=capacity_censored,
            gross_return_total=gross_return_total,
            net_return_total=net_return_total,
        ),
    )
