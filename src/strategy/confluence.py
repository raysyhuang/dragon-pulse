import logging
from dataclasses import dataclass, field
from typing import List, Optional
from strategy.base import StrategySignal, PickCandidate

logger = logging.getLogger(__name__)

# Sector boost/penalty applied to composite score
SECTOR_BOOST = 1.0   # +1.0 for top (hot) sectors
SECTOR_PENALTY = -1.0  # -1.0 for bottom (cold) sectors

DTL_CONFIRM_THRESHOLD = 10_000_000  # 10M CNY


@dataclass
class ConfluenceConfig:
    threshold_a: float = 50.0
    threshold_b: float = 50.0
    high_threshold: float = 70.0
    w_lens_a: float = 0.40
    w_lens_b: float = 0.35
    w_lens_c: float = 0.25
    max_daily_picks: int = 2
    min_composite_score: float = 45.0
    # Phase 2: require institutional flow for breakout-only signals
    require_dtl_for_breakout: bool = True


def _has_dtl_confirmation(signals: list[StrategySignal]) -> bool:
    """Check if any signal in the group has Dragon Tiger List institutional buy."""
    for s in signals:
        if s and s.evidence.get("dtl_net_buy", 0) > DTL_CONFIRM_THRESHOLD:
            return True
    return False


def run_confluence(
    signals_a: List[StrategySignal],
    signals_b: List[StrategySignal],
    signals_c: List[StrategySignal],
    regime: str,
    regime_sizing: float,
    config: ConfluenceConfig,
    hot_sectors: Optional[List[str]] = None,
    cold_sectors: Optional[List[str]] = None,
    ticker_sector_map: Optional[dict[str, str]] = None,
) -> List[PickCandidate]:
    """
    Combine signals from all 3 lenses into final picks.

    Confluence types (ranked by strength):
    1. "double": Lens A + B both triggered — strongest pattern
    2. "pullback_seal": Lens A + C (pullback sealed at limit-up)
    3. "breakout_seal": Lens B + C (breakout sealed at limit-up)
    4. "single_institution": One very strong lens + Dragon Tiger confirmation

    Phase 2 addition: Breakout-only signals (types involving Lens B without
    Lens A or C) now require Dragon Tiger List confirmation. This filters out
    retail-driven breakouts that are often bull traps.
    """
    signal_map = {}
    for s in signals_a + signals_b + signals_c:
        if s.ticker not in signal_map:
            signal_map[s.ticker] = {"a": None, "b": None, "c": None, "name_cn": s.name_cn}
        if s.lens == "lens_a":
            signal_map[s.ticker]["a"] = s
        elif s.lens == "lens_b":
            signal_map[s.ticker]["b"] = s
        elif s.lens == "lens_c":
            signal_map[s.ticker]["c"] = s

    picks = []
    rejections = []

    for ticker, group in signal_map.items():
        s_a = group["a"]
        s_b = group["b"]
        s_c = group["c"]

        score_a = s_a.score if s_a else 0.0
        score_b = s_b.score if s_b else 0.0
        score_c = s_c.score if s_c else 0.0

        trig_a = s_a.triggered if s_a else False
        trig_b = s_b.triggered if s_b else False
        trig_c = s_c.triggered if s_c else False

        dtl_confirm = _has_dtl_confirmation([s for s in (s_a, s_b, s_c) if s])

        confluence_type = ""
        # 1. Double (A+B) — strongest, no extra DTL requirement
        if trig_a and score_a >= config.threshold_a and trig_b and score_b >= config.threshold_b:
            confluence_type = "double"
        # 2. Pullback + Seal (A+C)
        elif trig_a and score_a >= config.threshold_a and trig_c:
            confluence_type = "pullback_seal"
        # 3. Breakout + Seal (B+C)
        elif trig_b and score_b >= config.threshold_b and trig_c:
            confluence_type = "breakout_seal"
        # 4. Single + Institution (high score + DTL required)
        elif (score_a >= config.high_threshold or score_b >= config.high_threshold) and dtl_confirm:
            confluence_type = "single_institution"

        if not confluence_type:
            continue

        # Phase 2: Dragon Tiger gating for breakout-dominant signals
        # A breakout without institutional backing is often a retail-driven bull trap
        if config.require_dtl_for_breakout:
            breakout_dominant = confluence_type in ("breakout_seal", "single_institution") and score_b > score_a
            if breakout_dominant and not dtl_confirm:
                rejections.append(
                    f"{ticker} ({confluence_type}): breakout without DTL confirmation"
                )
                continue

        comp_score = (
            config.w_lens_a * score_a
            + config.w_lens_b * score_b
            + config.w_lens_c * score_c
        )

        # Sector momentum boost/penalty
        if ticker_sector_map and ticker in ticker_sector_map:
            sector = ticker_sector_map[ticker]
            if hot_sectors and sector in hot_sectors:
                comp_score += SECTOR_BOOST
                logger.debug(f"{ticker}: +{SECTOR_BOOST} sector boost ({sector})")
            elif cold_sectors and sector in cold_sectors:
                comp_score += SECTOR_PENALTY
                logger.debug(f"{ticker}: {SECTOR_PENALTY} sector penalty ({sector})")

        if comp_score < config.min_composite_score:
            continue

        # Determine primary signal for sizing/targets
        primary = None
        if confluence_type in ("pullback_seal", "single_institution") and score_a >= score_b:
            primary = s_a
        elif s_b and s_b.triggered:
            primary = s_b
        elif confluence_type == "double":
            primary = s_a if score_a >= score_b else s_b
        if not primary and s_c:
            primary = s_c
        if primary is None:
            continue

        picks.append(PickCandidate(
            ticker=ticker,
            name_cn=group["name_cn"],
            composite_score=comp_score,
            confluence_type=confluence_type,
            signals=[s for s in (s_a, s_b, s_c) if s and s.triggered],
            entry_price=primary.entry_price,
            target_price=primary.target_price,
            stop_price=primary.stop_price,
            position_size_mult=primary.position_size_mult * regime_sizing,
            max_hold_days=primary.max_hold_days,
            regime=regime,
            sector=ticker_sector_map.get(ticker, "") if ticker_sector_map else "",
        ))

    # Log DTL rejections
    for r in rejections:
        logger.info(f"  [DTL GATE] {r}")

    # Sort and limit
    picks.sort(key=lambda x: x.composite_score, reverse=True)
    return picks[:config.max_daily_picks]
