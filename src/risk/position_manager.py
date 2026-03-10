import pandas as pd
from dataclasses import dataclass
from typing import Optional
from datetime import date

@dataclass
class Position:
    id: Optional[int]
    ticker: str
    name_cn: str
    entry_date: str
    entry_price: float
    target_price: float
    stop_price: float
    current_stop: float
    max_hold_days: int
    position_size_mult: float
    lens: str
    confluence_type: str
    status: str
    days_held: int = 0
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    pnl_pct: Optional[float] = None
    exit_reason: Optional[str] = None

def check_exits(position: Position, today_ohlcv: pd.Series, today_date: str, config: dict) -> Position:
    """Check if position should be exited today based on OHLCV."""
    high = today_ohlcv['High']
    low = today_ohlcv['Low']
    close = today_ohlcv['Close']
    
    # Priority 1: Stop hit (gap down or intraday)
    if low <= position.current_stop:
        position.status = "closed"
        position.exit_date = today_date
        # If open < stop, we likely gap down and get filled at open
        if today_ohlcv['Open'] < position.current_stop:
            position.exit_price = today_ohlcv['Open']
        else:
            position.exit_price = position.current_stop
            
        position.exit_reason = "stop_hit"
        position.pnl_pct = (position.exit_price / position.entry_price - 1) * 100
        return position

    # Priority 2: Target hit
    if high >= position.target_price:
        position.status = "closed"
        position.exit_date = today_date
        # If open > target, filled at open
        if today_ohlcv['Open'] > position.target_price:
            position.exit_price = today_ohlcv['Open']
        else:
            position.exit_price = position.target_price
            
        position.exit_reason = "target_hit"
        position.pnl_pct = (position.exit_price / position.entry_price - 1) * 100
        return position

    # Priority 3: Trailing stop update
    # If high reaches trailing_trigger, move stop to entry
    trailing_trigger_pct = config.get("trailing_trigger_pct", 2.5)
    trigger_price = position.entry_price * (1 + trailing_trigger_pct / 100)
    
    if high >= trigger_price and position.current_stop < position.entry_price:
        # Move stop to breakeven + tiny buffer to cover costs
        position.current_stop = position.entry_price * 1.002
    
    # Increment days held
    position.days_held += 1

    # Priority 4: Dead money rule — exit if no +2% move within 3 days
    dead_money_days = config.get("dead_money_days", 3)
    dead_money_min_pct = config.get("dead_money_min_pct", 2.0)
    if position.days_held >= dead_money_days:
        current_return_pct = (high / position.entry_price - 1) * 100
        if current_return_pct < dead_money_min_pct:
            position.status = "closed"
            position.exit_date = today_date
            position.exit_price = close
            position.exit_reason = "dead_money"
            position.pnl_pct = (close / position.entry_price - 1) * 100
            return position

    # Priority 5: Time stop
    if position.days_held >= position.max_hold_days:
        position.status = "closed"
        position.exit_date = today_date
        position.exit_price = close
        position.exit_reason = "timeout"
        position.pnl_pct = (position.exit_price / position.entry_price - 1) * 100
        return position
        
    return position
