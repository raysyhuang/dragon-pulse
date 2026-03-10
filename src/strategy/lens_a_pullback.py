from __future__ import annotations
import pandas as pd
from src.strategy.base import StrategyLens, StrategySignal

class LensAPullback(StrategyLens):
    """Lens A: Institutional Pullback Buy (机构回调买入)
    
    Buys stocks that were recently strong but pulled back on declining volume,
    with institutional confirmation from Dragon Tiger List.
    """
    
    DEFAULT_PARAMS = {
        "high_lookback": 20,           # Days to look for recent high
        "high_recency": 10,            # High must be within this many days
        "pullback_min_pct": 5.0,       # Minimum pullback from high
        "pullback_max_pct": 12.0,      # Maximum pullback (beyond = breakdown)
        "rsi2_max": 20.0,              # RSI(2) must be below this
        "sma_period": 50,              # Trend MA period
        "vol_decline_max": 0.8,        # Volume ratio must be below this
        "dtl_min_net_buy_cny": 0,      # Min institutional net buy (0 = disabled)
        "target_pct": 5.0,             # Target profit %
        "stop_pct": 3.0,               # Stop loss %
        "trailing_trigger_pct": 2.5,   # Move stop to breakeven at this profit
        "max_hold_days": 5,
        # Scoring weights (must sum to 1.0)
        "w_rsi_depth": 0.30,
        "w_pullback_mag": 0.20,
        "w_vol_decline": 0.15,
        "w_sma_margin": 0.15,
        "w_institutional": 0.20,
    }

    def scan(self, ticker: str, name_cn: str, ohlcv: pd.DataFrame, technicals: dict, context: dict) -> StrategySignal:
        """
        context keys needed:
          - dtl_net_buy_cny: float (Dragon Tiger institutional net buy, 0 if absent)
          - sector_momentum_rank: int (1=hottest, 0 if unknown)
        """
        p = self.params
        
        made_high = technicals.get("made_20d_high_in_last_10", False)
        pullback = abs(technicals.get("pullback_from_20d_high_pct", 0))
        rsi2 = technicals.get("rsi_2", 50)
        above_sma = technicals.get(f"above_ma{p['sma_period']}", False)
        vol_trend = technicals.get("volume_trend_3d_20d", 1.0)
        
        triggered = (
            made_high
            and p["pullback_min_pct"] <= pullback <= p["pullback_max_pct"]
            and rsi2 <= p["rsi2_max"]
            and above_sma
            and vol_trend <= p["vol_decline_max"]
        )
        
        score = 0.0
        if triggered:
            # RSI depth: lower = better (RSI 0 -> 100, RSI 20 -> 0)
            rsi_score = max(0, (p["rsi2_max"] - rsi2) / p["rsi2_max"]) * 100 if p["rsi2_max"] > 0 else 0
            
            # Pullback magnitude: sweet spot 7-10% (midpoint)
            mid = (p["pullback_min_pct"] + p["pullback_max_pct"]) / 2
            pb_score = max(0, 100 - abs(pullback - mid) / mid * 100) if mid > 0 else 0
            
            # Volume decline: lower ratio = better
            vol_score = max(0, (p["vol_decline_max"] - vol_trend) / p["vol_decline_max"]) * 100 if p["vol_decline_max"] > 0 else 0
            
            # SMA margin
            sma_val = technicals.get(f"ma{p['sma_period']}", 0)
            price = technicals.get("last_price", 0)
            sma_margin = ((price - sma_val) / sma_val * 100) if sma_val > 0 else 0
            sma_score = min(100, max(0, sma_margin / 5.0 * 100))  # 5% above = 100
            
            # Institutional flow
            dtl_buy = context.get("dtl_net_buy_cny", 0)
            sector_rank = context.get("sector_momentum_rank", 99)
            inst_score = 30  # baseline
            if dtl_buy > 50_000_000: inst_score = 100
            elif dtl_buy > 30_000_000: inst_score = 70
            elif dtl_buy > 10_000_000: inst_score = 50
            if sector_rank <= 5: inst_score = max(inst_score, 60)
            
            score = (
                p["w_rsi_depth"] * rsi_score +
                p["w_pullback_mag"] * pb_score +
                p["w_vol_decline"] * vol_score +
                p["w_sma_margin"] * sma_score +
                p["w_institutional"] * inst_score
            )
            
        entry = technicals.get("last_price", 0)
        return StrategySignal(
            ticker=ticker, name_cn=name_cn, lens="lens_a",
            score=round(score, 1), triggered=bool(triggered),
            evidence={"rsi2": rsi2, "pullback_pct": pullback, "vol_trend": vol_trend,
                      "dtl_net_buy": context.get("dtl_net_buy_cny", 0)},
            entry_price=entry,
            target_price=round(entry * (1 + p["target_pct"] / 100), 2),
            stop_price=round(entry * (1 - p["stop_pct"] / 100), 2),
            max_hold_days=p["max_hold_days"],
        )

    def get_param_ranges(self) -> dict[str, tuple[float, float]]:
        return {
            "pullback_min_pct": (3.0, 8.0),
            "pullback_max_pct": (8.0, 18.0),
            "rsi2_max": (10.0, 30.0),
            "vol_decline_max": (0.5, 1.0),
            "target_pct": (3.0, 8.0),
            "stop_pct": (2.0, 5.0),
            "trailing_trigger_pct": (1.5, 4.0),
            "max_hold_days": (3, 7),
            "w_rsi_depth": (0.15, 0.45),
            "w_pullback_mag": (0.10, 0.35),
            "w_vol_decline": (0.05, 0.25),
            "w_sma_margin": (0.05, 0.25),
            "w_institutional": (0.10, 0.35),
        }
