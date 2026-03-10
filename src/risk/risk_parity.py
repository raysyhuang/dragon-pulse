"""
Risk Parity Position Sizing
============================

Replaces fixed-multiplier sizing with volatility-adjusted sizing so every
trade risks the same dollar amount.  Also enforces sector concentration
limits to prevent correlated blow-ups.

Core idea:
    position_shares = (equity * per_trade_risk_pct) / (entry_price * atr_pct / 100)

If a stock's ATR% is 5% and our risk budget is 1% of equity, we size it
at 1/5 the capital vs. a stock with 1% ATR.  This equalizes dollar-risk.

Sector cap: if >N picks land in the same sector, excess picks get halved
sizing — preventing a single sector crash from wiping out the portfolio.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SizingResult:
    """Output of risk-parity sizing for a single pick."""
    ticker: str
    raw_mult: float          # Pre-risk-parity multiplier (from regime * guardian)
    atr_pct: float           # Stock's ATR% (14-day)
    vol_adjusted_mult: float # After volatility normalization
    sector: str
    sector_capped: bool      # True if sector cap reduced sizing
    final_mult: float        # Final position_size_mult to use


def compute_risk_parity_sizing(
    picks: list[dict],
    equity: float = 100_000.0,
    per_trade_risk_pct: float = 1.0,
    target_atr_pct: float = 3.0,
    max_sector_positions: int = 2,
    sector_excess_penalty: float = 0.5,
) -> list[SizingResult]:
    """
    Compute volatility-adjusted position sizes for a list of picks.

    Args:
        picks: List of dicts with keys: ticker, entry_price, stop_price,
               position_size_mult, atr_pct, sector
        equity: Total portfolio equity (CNY)
        per_trade_risk_pct: Max % of equity to risk per trade (default 1%)
        target_atr_pct: "Normal" ATR% to normalize against.  Stocks with
                        higher ATR get smaller positions.
        max_sector_positions: Max picks in the same sector before penalty
        sector_excess_penalty: Multiplier for excess sector positions (0.5 = half size)

    Returns:
        List of SizingResult with final_mult for each pick.
    """
    results: list[SizingResult] = []

    # First pass: compute vol-adjusted multiplier
    for pick in picks:
        ticker = pick.get("ticker", "?")
        raw_mult = float(pick.get("position_size_mult", 1.0))
        atr_pct = float(pick.get("atr_pct", target_atr_pct))
        sector = str(pick.get("sector", ""))

        # Volatility normalization: if ATR% is double the target, size is halved
        if atr_pct > 0:
            vol_ratio = target_atr_pct / atr_pct
            # Clamp between 0.3x and 2.0x to avoid extreme sizing
            vol_ratio = max(0.3, min(2.0, vol_ratio))
        else:
            vol_ratio = 1.0

        vol_adjusted = raw_mult * vol_ratio

        results.append(SizingResult(
            ticker=ticker,
            raw_mult=raw_mult,
            atr_pct=atr_pct,
            vol_adjusted_mult=round(vol_adjusted, 3),
            sector=sector,
            sector_capped=False,
            final_mult=round(vol_adjusted, 3),
        ))

    # Second pass: sector concentration cap
    sector_counts: dict[str, int] = {}
    final_results: list[SizingResult] = []

    for r in results:
        if r.sector:
            sector_counts[r.sector] = sector_counts.get(r.sector, 0) + 1
            count = sector_counts[r.sector]
            if count > max_sector_positions:
                capped_mult = round(r.vol_adjusted_mult * sector_excess_penalty, 3)
                final_results.append(SizingResult(
                    ticker=r.ticker,
                    raw_mult=r.raw_mult,
                    atr_pct=r.atr_pct,
                    vol_adjusted_mult=r.vol_adjusted_mult,
                    sector=r.sector,
                    sector_capped=True,
                    final_mult=capped_mult,
                ))
                logger.info(
                    f"  [RISK PARITY] {r.ticker}: sector cap ({r.sector} #{count}) "
                    f"{r.vol_adjusted_mult:.2f}x -> {capped_mult:.2f}x"
                )
                continue

        final_results.append(r)

    return final_results


def apply_risk_parity_to_picks(
    picks: list,
    data_cache: Optional[dict] = None,
    ticker_sector_map: Optional[dict[str, str]] = None,
    config: Optional[dict] = None,
) -> list:
    """
    Apply risk-parity sizing to PickCandidate objects in-place.

    Reads ATR from the last row of each ticker's data if available.
    Returns the same list with updated position_size_mult.
    """
    if config is None:
        config = {}

    target_atr = config.get("target_atr_pct", 3.0)
    max_sector = config.get("max_sector_positions", 2)
    sector_penalty = config.get("sector_excess_penalty", 0.5)

    pick_dicts = []
    for p in picks:
        atr_pct = target_atr  # default
        if data_cache and p.ticker in data_cache:
            df = data_cache[p.ticker]
            if not df.empty and "atr_pct" in df.columns:
                last_atr = df["atr_pct"].iloc[-1]
                if last_atr > 0:
                    atr_pct = float(last_atr)

        sector = ""
        if ticker_sector_map and p.ticker in ticker_sector_map:
            sector = ticker_sector_map[p.ticker]
        elif hasattr(p, "sector") and p.sector:
            sector = p.sector

        pick_dicts.append({
            "ticker": p.ticker,
            "entry_price": p.entry_price,
            "stop_price": p.stop_price,
            "position_size_mult": p.position_size_mult,
            "atr_pct": atr_pct,
            "sector": sector,
        })

    sizing_results = compute_risk_parity_sizing(
        pick_dicts,
        target_atr_pct=target_atr,
        max_sector_positions=max_sector,
        sector_excess_penalty=sector_penalty,
    )

    for pick, sizing in zip(picks, sizing_results):
        if abs(pick.position_size_mult - sizing.final_mult) > 0.001:
            logger.info(
                f"  [RISK PARITY] {pick.ticker}: "
                f"ATR={sizing.atr_pct:.1f}% -> {sizing.raw_mult:.2f}x -> {sizing.final_mult:.2f}x"
                f"{' (sector capped)' if sizing.sector_capped else ''}"
            )
        pick.position_size_mult = sizing.final_mult

    return picks
