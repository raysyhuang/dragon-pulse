from __future__ import annotations
import pandas as pd
from strategy.base import StrategyLens, StrategySignal

class LensCLimitUp(StrategyLens):
    """Lens C: Limit-Up Institutional Seal (涨停封板机构确认)
    
    Target stocks sealed at Limit-Up with institutional confirmation.
    Uses half position size due to asymmetric risk profile.
    """
    
    DEFAULT_PARAMS = {
        "limit_pct": 9.5,              # 9.5% to catch 10% limits (main board) reliably
        "dtl_min_net_buy_cny": 10000000,
        "max_consecutive_limits": 1,     # Only first limit-up
        "min_adv_20d_cny": 50000000,
        "target_pct": 3.0,              # Conservative 
        "stop_pct": 5.0,
        "max_hold_days": 2,              # Short hold
        "position_size_mult": 0.5,       # Half size 
        "w_inst_magnitude": 0.35,
        "w_seal_quality": 0.25,
        "w_sector": 0.20,
        "w_volume": 0.20,
    }

    def scan(self, ticker: str, name_cn: str, ohlcv: pd.DataFrame, technicals: dict, context: dict) -> StrategySignal:
        p = self.params
        
        if ohlcv.empty or len(ohlcv) < 2:
            return StrategySignal(ticker, name_cn, "lens_c", 0.0, False)
            
        today = ohlcv.iloc[-1]
        yesterday = ohlcv.iloc[-2]
        
        ret_today = (today["Close"] / yesterday["Close"] - 1) * 100
        is_limit_up = ret_today >= p["limit_pct"]
        
        # Check if sealed: Close == High
        is_sealed = (today["Close"] >= today["High"] * 0.999)
        
        dtl_net_buy = context.get("dtl_net_buy_cny", 0)
        
        # Determine if it's 1st limit up (yesterday wasn't limit up)
        ret_yesterday = (yesterday["Close"] / ohlcv.iloc[-3]["Close"] - 1) * 100 if len(ohlcv) >= 3 else 0
        is_first_limit = ret_yesterday < p["limit_pct"]
        
        adv_20d = technicals.get("avg_dollar_volume_20d", 0)
        
        triggered = (
            is_limit_up
            and is_sealed
            and dtl_net_buy >= p["dtl_min_net_buy_cny"]
            and is_first_limit
            and adv_20d >= p["min_adv_20d_cny"]
        )
        
        score = 0.0
        if triggered:
            # Inst magnitude
            if dtl_net_buy > 50_000_000: inst_score = 100
            elif dtl_net_buy > 30_000_000: inst_score = 70
            else: inst_score = 40
            
            # Seal quality (approximated by turnover rate: lower is better for a sealed limit-up!)
            turnover = technicals.get("turnover_rate", 100) # (Vol / 20dVol)*100
            if turnover < 100: seal_score = 100  # Shrank volume on limit-up = incredibly strong seal
            elif turnover < 200: seal_score = 60
            else: seal_score = 30
            
            sector_rank = context.get("sector_momentum_rank", 99)
            sector_score = 100 if sector_rank <= 5 else 30
            
            # Volume quality (liquidity)
            vol_score = 100 if adv_20d > 200_000_000 else 50
            
            score = (
                p["w_inst_magnitude"] * inst_score +
                p["w_seal_quality"] * seal_score +
                p["w_sector"] * sector_score +
                p["w_volume"] * vol_score
            )
            
        entry = technicals.get("last_price", 0)
        return StrategySignal(
            ticker=ticker, name_cn=name_cn, lens="lens_c",
            score=round(score, 1), triggered=bool(triggered),
            evidence={"ret_today": ret_today, "dtl_net_buy": dtl_net_buy, "is_sealed": is_sealed},
            entry_price=entry,
            target_price=round(entry * (1 + p["target_pct"] / 100), 2),
            stop_price=round(entry * (1 - p["stop_pct"] / 100), 2),
            max_hold_days=p["max_hold_days"],
            position_size_mult=p["position_size_mult"]
        )

    def get_param_ranges(self) -> dict[str, tuple[float, float]]:
        return {
            "dtl_min_net_buy_cny": (0, 30_000_000),
            "target_pct": (2.0, 5.0),
            "stop_pct": (3.0, 8.0),
            "max_hold_days": (1, 4),
            "w_inst_magnitude": (0.20, 0.50),
            "w_seal_quality": (0.15, 0.35),
            "w_sector": (0.10, 0.30),
            "w_volume": (0.10, 0.30),
        }
