from __future__ import annotations
import pandas as pd
from strategy.base import StrategyLens, StrategySignal

class LensBBreakout(StrategyLens):
    """Lens B: Volume Breakout from Compression (量能突破)
    
    Bollinger Band squeeze followed by volume expansion.
    """
    
    DEFAULT_PARAMS = {
        "bb_period": 20,
        "bb_width_pctl_max": 20.0,     # BB width must be in bottom 20th percentile
        "volume_expansion_min": 2.0,   # Today's vol >= 2x 20d avg
        "bb_position_min": 1.0,        # Must close above upper BB (%B > 1.0)
        "rsi14_min": 50.0,
        "rsi14_max": 70.0,
        "atr_pct_min": 2.0,
        "target_pct": 5.0,
        "stop_pct": 3.5,
        "trailing_trigger_pct": 3.0,
        "max_hold_days": 5,
        "w_volume": 0.30,
        "w_squeeze": 0.20,
        "w_trend": 0.20,
        "w_rsi": 0.15,
        "w_sector": 0.15,
    }

    def scan(self, ticker: str, name_cn: str, ohlcv: pd.DataFrame, technicals: dict, context: dict) -> StrategySignal:
        p = self.params
        
        bb_pctl = technicals.get("bb_width_pctl", 100)
        vol_ratio = technicals.get("volume_ratio_3d_to_20d", 0)  # We actually want 1d to 20d, let's assume technicals has it or we compute from volume_ratio_3d_to_20d. 
        # Wait, let's compute today's vol vs 20d directly.
        if not ohlcv.empty and len(ohlcv) >= 20:
            vol_expansion = ohlcv["Volume"].iloc[-1] / ohlcv["Volume"].rolling(20).mean().iloc[-1]
        else:
            vol_expansion = 0.0
            
        bb_pos = technicals.get("bb_position", 0.0)
        rsi14 = technicals.get("rsi", 50.0)
        above_ma20 = technicals.get("above_ma20", False)
        above_ma50 = technicals.get("above_ma50", False)
        atr_pct = technicals.get("atr_pct", 0.0)
        
        triggered = (
            bb_pctl <= p["bb_width_pctl_max"]
            and vol_expansion >= p["volume_expansion_min"]
            and bb_pos >= p["bb_position_min"]
            and above_ma20 and above_ma50
            and p["rsi14_min"] <= rsi14 <= p["rsi14_max"]
            and atr_pct >= p["atr_pct_min"]
        )
        
        score = 0.0
        if triggered:
            vol_score = min(100, max(0, (vol_expansion - 1.0) / 2.0 * 100))  # 3.0x -> 100
            
            squeeze_score = max(0, (p["bb_width_pctl_max"] - bb_pctl) / p["bb_width_pctl_max"]) * 100 if p["bb_width_pctl_max"] > 0 else 0
            
            # Trend alignment (MA10 > MA20 > MA50 gives bonus)
            ma10 = technicals.get("ma10", 0)
            ma20 = technicals.get("ma20", 0)
            ma50 = technicals.get("ma50", 0)
            trend_score = 100 if (ma10 > ma20 > ma50) else 60
            
            # RSI sweet spot (60 = 100)
            rsi_score = max(0, 100 - abs(rsi14 - 60) / 10 * 100)
            
            sector_rank = context.get("sector_momentum_rank", 99)
            sector_score = 100 if sector_rank <= 3 else (60 if sector_rank <= 5 else 0)
            
            score = (
                p["w_volume"] * vol_score +
                p["w_squeeze"] * squeeze_score +
                p["w_trend"] * trend_score +
                p["w_rsi"] * rsi_score +
                p["w_sector"] * sector_score
            )
            
        entry = technicals.get("last_price", 0)
        return StrategySignal(
            ticker=ticker, name_cn=name_cn, lens="lens_b",
            score=round(score, 1), triggered=bool(triggered),
            evidence={"bb_pctl": bb_pctl, "vol_expansion": vol_expansion, "bb_pos": bb_pos},
            entry_price=entry,
            target_price=round(entry * (1 + p["target_pct"] / 100), 2),
            stop_price=round(entry * (1 - p["stop_pct"] / 100), 2),
            max_hold_days=p["max_hold_days"],
        )

    def get_param_ranges(self) -> dict[str, tuple[float, float]]:
        return {
            "bb_width_pctl_max": (10.0, 30.0),
            "volume_expansion_min": (1.5, 3.0),
            "bb_position_min": (0.8, 1.2),
            "rsi14_min": (40.0, 60.0),
            "rsi14_max": (65.0, 80.0),
            "atr_pct_min": (1.5, 3.0),
            "target_pct": (3.0, 8.0),
            "stop_pct": (2.5, 6.0),
            "trailing_trigger_pct": (2.0, 4.0),
            "max_hold_days": (3, 7),
            "w_volume": (0.15, 0.40),
            "w_squeeze": (0.10, 0.30),
            "w_trend": (0.10, 0.30),
            "w_rsi": (0.05, 0.25),
            "w_sector": (0.05, 0.25),
        }
