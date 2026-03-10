import logging
import pandas as pd
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RegimeAssessment:
    label: str           # "bull", "caution", "bear"
    confidence: float    # 0.0 - 1.0
    sizing_mult: float   # 1.0, 0.6, 0.3
    csi300_above_sma20: bool
    csi300_above_sma50: bool
    breadth_score: float  # % of universe above SMA20
    northbound_flow_5d: float  # CNY billions
    details: dict


def compute_breadth(
    universe_price_data: dict[str, pd.DataFrame],
    sma_period: int = 20,
) -> float:
    """
    Compute market breadth: fraction of stocks trading above their SMA.

    This is the single most powerful regime filter for A-shares. When breadth
    is below 30%, even the best setups fail because the whole market is falling.

    Args:
        universe_price_data: Dict of ticker → OHLCV DataFrames
        sma_period: SMA period for breadth calculation

    Returns:
        Breadth ratio (0.0 to 1.0)
    """
    if not universe_price_data:
        return 0.5  # Unknown → assume neutral

    above_count = 0
    total = 0
    for ticker, df in universe_price_data.items():
        if df is None or df.empty or len(df) < sma_period:
            continue
        close = df["Close"]
        sma_val = close.rolling(sma_period).mean().iloc[-1]
        if pd.notna(sma_val) and close.iloc[-1] > sma_val:
            above_count += 1
        total += 1

    if total == 0:
        return 0.5

    return above_count / total


def classify_regime(
    csi300_df: pd.DataFrame,
    universe_price_data: Optional[dict[str, pd.DataFrame]] = None,
    config: Optional[dict] = None,
) -> RegimeAssessment:
    """
    Classify current market regime using CSI 300 + market breadth.

    Three-tier classification:
    - bull: CSI300 above both SMA20 and SMA50, breadth >= bullish threshold
    - caution: CSI300 above SMA50 only, or breadth is weak
    - bear: CSI300 below SMA50, or breadth < bearish threshold

    Breadth overrides:
    - breadth < bearish (30%): bull → caution, caution → bear
    - breadth < bullish (50%) but >= bearish: bull → caution (no further downgrade)
    """
    if config is None:
        config = {}

    sma_short = config.get("sma_short", 20)
    sma_long = config.get("sma_long", 50)
    bull_sizing = config.get("bull_sizing", 1.0)
    caution_sizing = config.get("caution_sizing", 0.6)
    bear_sizing = config.get("bear_sizing", 0.3)
    breadth_bullish = config.get("breadth_bullish", 0.50)
    breadth_bearish = config.get("breadth_bearish", 0.30)

    if csi300_df is None or csi300_df.empty or len(csi300_df) < sma_long:
        return RegimeAssessment(
            "caution", 0.5, caution_sizing, False, False, 0.5, 0,
            {"error": "No index data"},
        )

    close = csi300_df["Close"]
    sma20 = close.rolling(sma_short).mean().iloc[-1]
    sma50 = close.rolling(sma_long).mean().iloc[-1]
    last_price = close.iloc[-1]

    above_20 = last_price > sma20
    above_50 = last_price > sma50

    if above_20 and above_50:
        base_label = "bull"
        sizing = bull_sizing
    elif above_50 and not above_20:
        base_label = "caution"
        sizing = caution_sizing
    else:
        base_label = "bear"
        sizing = bear_sizing

    # Compute market breadth
    breadth = compute_breadth(universe_price_data or {}, sma_short)

    # Northbound flow placeholder
    nb_flow = 0.0

    # Breadth overrides — the key win-rate improvement
    if breadth < breadth_bearish:
        # Severe breadth collapse: force bear regardless of CSI300 level.
        # When <30% of stocks are above SMA20, even strong-looking setups fail.
        if base_label != "bear":
            logger.info(
                f"Breadth override: {base_label} → bear "
                f"(breadth {breadth:.1%} < {breadth_bearish:.0%})"
            )
            base_label = "bear"
            sizing = bear_sizing
    elif breadth < breadth_bullish and base_label == "bull":
        # Weak breadth: not enough participation for full bull
        logger.info(
            f"Breadth override: bull → caution (breadth {breadth:.1%} < {breadth_bullish:.0%})"
        )
        base_label = "caution"
        sizing = caution_sizing

    details = {
        "sma20": float(sma20),
        "sma50": float(sma50),
        "last_price": float(last_price),
        "breadth_pct": round(breadth * 100, 1),
        "breadth_bullish_threshold": breadth_bullish,
        "breadth_bearish_threshold": breadth_bearish,
    }

    return RegimeAssessment(
        label=base_label,
        confidence=1.0,
        sizing_mult=sizing,
        csi300_above_sma20=above_20,
        csi300_above_sma50=above_50,
        breadth_score=breadth,
        northbound_flow_5d=nb_flow,
        details=details,
    )
