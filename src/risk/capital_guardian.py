from dataclasses import dataclass
from typing import List, Dict

@dataclass
class GuardianVerdict:
    sizing_multiplier: float    # 0.0 - 1.0 (adjusted down from 1.25)
    halt: bool                  # True = no new positions
    reasons: List[str]
    drawdown_pct: float
    consecutive_losses: int
    open_risk_pct: float

def compute_guardian_verdict(
    equity_curve: List[float],     # Daily P&L values, cumulative equity
    open_positions: List[dict],    # Current open positions
    recent_trades: List[dict],     # Closed trades to check streak
    regime_sizing: float,          # Base sizing from regime (0.3 to 1.0)
    config: dict,
) -> GuardianVerdict:
    """6-layer risk control for capital preservation."""
    reasons = []
    halt = False
    sizing_mult = 1.0
    
    # 1. Drawdown Circuit Breaker
    max_dd = 0.0
    if equity_curve:
        peak = max(equity_curve)
        current = equity_curve[-1]
        max_dd = ((peak - current) / peak * 100) if peak > 0 else 0.0
        
    dd_cap = config.get("max_drawdown_pct", 15.0)
    if max_dd >= dd_cap:
        halt = True
        reasons.append(f"Max drawdown ({max_dd:.1f}%) exceeded cap ({dd_cap}%)")
    elif max_dd > dd_cap / 2:
        # Linear decay: at 50% max_dd size is 1.0, at 99% max_dd size is ~0.25
        decay = 1.0 - (max_dd - dd_cap/2) / (dd_cap/2) * 0.75
        sizing_mult *= max(0.25, decay)
        reasons.append(f"Drawdown scaling ({max_dd:.1f}%) applied: {decay:.2f}x")

    # 2. Consecutive Loss Streak
    consecutive_losses = 0
    for trade in reversed(recent_trades):
        if trade.get('pnl_pct', 0) < 0:
            consecutive_losses += 1
        else:
            break
            
    streak_thresh = config.get("streak_reduction_after", 2)
    if consecutive_losses >= config.get("halt_after_consecutive_losses", 5):
        halt = True
        reasons.append(f"Halted due to {consecutive_losses} consecutive losses")
    elif consecutive_losses >= streak_thresh:
        penalty = 1.0 - 0.25 * (consecutive_losses - streak_thresh + 1)
        sizing_mult *= max(0.25, penalty)
        reasons.append(f"Loss streak ({consecutive_losses}) penalty: {penalty:.2f}x")

    # 3. Regime Scaling
    sizing_mult *= regime_sizing
    if regime_sizing < 1.0:
        reasons.append(f"Regime scaling applied: {regime_sizing}x")

    # 4. Portfolio Heat Cap (Total open risk)
    open_risk_pct = 0.0
    for pos in open_positions:
        risk_pct = abs(pos['entry_price'] - pos['stop_price']) / pos['entry_price'] * 100
        size_mult = pos.get('position_size_mult', 1.0)
        base_size_pct = 10.0 # Assume base size is 10% of portfolio
        open_risk_pct += (risk_pct * base_size_pct * size_mult / 100)
        
    heat_cap = config.get("max_portfolio_heat_pct", 10.0)
    halt_heat = config.get("halt_portfolio_heat_pct", 15.0)
    
    if open_risk_pct >= halt_heat:
        halt = True
        reasons.append(f"Portfolio heat ({open_risk_pct:.1f}%) exceeds halt cap ({halt_heat}%)")
    elif open_risk_pct >= heat_cap:
        sizing_mult *= 0.5
        reasons.append(f"High portfolio heat ({open_risk_pct:.1f}%), halving new size")

    # 5. Max Open Positions Cap
    max_pos = config.get("max_open_positions", 4)
    if len(open_positions) >= max_pos:
        halt = True
        reasons.append(f"Max open positions ({max_pos}) reached")

    return GuardianVerdict(
        sizing_multiplier=round(sizing_mult, 2),
        halt=halt,
        reasons=reasons,
        drawdown_pct=max_dd,
        consecutive_losses=consecutive_losses,
        open_risk_pct=open_risk_pct
    )
