import pandas as pd
from typing import List, Dict
from datetime import date
import copy

from strategy.confluence import ConfluenceConfig, run_confluence
from strategy.regime import classify_regime
from risk.capital_guardian import compute_guardian_verdict
from risk.position_manager import Position, check_exits
from risk.risk_parity import apply_risk_parity_to_picks
from core.technicals import compute_extended_technicals

def run_backtest(
    start_date: str,
    end_date: str,
    params_a: dict,
    params_b: dict,
    params_c: dict,
    confluence_config: ConfluenceConfig,
    guardian_config: dict,
    universe_tickers: List[str],
    data_cache: Dict[str, pd.DataFrame],
    csi300_df: pd.DataFrame,
    exit_config: dict | None = None,
    risk_parity_config: dict | None = None,
    preflight_config: dict | None = None,
):
    """Core backtest loop for Dragon Pulse strategy."""
    from strategy.lens_a_pullback import LensAPullback
    from strategy.lens_b_breakout import LensBBreakout
    from strategy.lens_c_limitup import LensCLimitUp
    from backtest.metrics import compute_metrics
    
    lens_a = LensAPullback(params_a)
    lens_b = LensBBreakout(params_b)
    lens_c = LensCLimitUp(params_c)
    
    # Align dates
    all_dates = set()
    for df in data_cache.values():
        all_dates.update(df.index)
    all_dates = sorted([d for d in all_dates if str(d)[:10] >= start_date and str(d)[:10] <= end_date])
    
    open_positions: List[Position] = []
    closed_trades: List[dict] = []
    equity_curve = [100000.0] # start with 100k
    
    # We will need historical technicals for each ticker
    print("Pre-computing technicals for universe...")
    technicals_cache = {}
    for t in universe_tickers:
        if t in data_cache:
            technicals_cache[t] = compute_extended_technicals(data_cache[t])
            
    # Iterate trading days
    for current_dt in all_dates:
        curr_date_str = str(current_dt)[:10]
        
        # 1. Update Open Positions (Check Exits)
        next_open = []
        for pos in open_positions:
            df = data_cache.get(pos.ticker)
            if df is not None and current_dt in df.index:
                today_bar = df.loc[current_dt]
                updated_pos = check_exits(pos, today_bar, curr_date_str, exit_config or {})
                
                if updated_pos.status == "closed":
                    # Record trade
                    trade = {
                        'ticker': updated_pos.ticker,
                        'name_cn': updated_pos.name_cn,
                        'entry_date': updated_pos.entry_date,
                        'exit_date': updated_pos.exit_date,
                        'entry_price': updated_pos.entry_price,
                        'exit_price': updated_pos.exit_price,
                        'pnl_pct': updated_pos.pnl_pct,
                        'exit_reason': updated_pos.exit_reason,
                        'hold_days': updated_pos.days_held,
                        'lens': updated_pos.lens,
                        'confluence': updated_pos.confluence_type
                    }
                    closed_trades.append(trade)
                    
                    # Update equity curve realistically (base size 10k per 1.0 multiplier)
                    pnl_cash = (trade['pnl_pct'] / 100) * 10000 * updated_pos.position_size_mult
                    equity_curve[-1] += pnl_cash
                else:
                    next_open.append(updated_pos)
            else:
                # Missing data, keep open
                next_open.append(pos)
                
        open_positions = next_open
        # Record daily equity
        equity_curve.append(equity_curve[-1])
        
        # 2. Slice Data up to Current Date
        csi_slice = csi300_df.loc[:current_dt] if csi300_df is not None else None
        
        # 3. Classify Regime
        regime = classify_regime(csi_slice, None, {"bear_sizing": 0.3, "caution_sizing": 0.6, "bull_sizing": 1.0})
        
        # 4. Capital Guardian Gate
        guardian = compute_guardian_verdict(
            equity_curve=[e for e in equity_curve if e > 0],
            open_positions=[{'entry_price': p.entry_price, 'stop_price': p.current_stop, 'position_size_mult': p.position_size_mult} for p in open_positions],
            recent_trades=closed_trades[-10:],
            regime_sizing=regime.sizing_mult,
            config=guardian_config
        )
        
        if guardian.halt:
            continue
            
        # 5. Scan Universe
        sig_a, sig_b, sig_c = [], [], []
        
        for ticker in universe_tickers:
            if ticker not in technicals_cache: continue
            
            tech_df = technicals_cache[ticker]
            ohlcv_df = data_cache[ticker]
            
            # Slice up to today
            if current_dt not in tech_df.index: continue
            
            tech_today = tech_df.loc[:current_dt].iloc[-1].to_dict()
            ohlcv_slice = ohlcv_df.loc[:current_dt]
            
            # Mock DTL / Context for now (since fetching historical DTL daily is extremely slow)
            context = {"dtl_net_buy_cny": 0, "sector_momentum_rank": 50} 
            
            sa = lens_a.scan(ticker, ticker, ohlcv_slice, tech_today, context)
            if sa.triggered: sig_a.append(sa)
                
            sb = lens_b.scan(ticker, ticker, ohlcv_slice, tech_today, context)
            if sb.triggered: sig_b.append(sb)
                
            sc = lens_c.scan(ticker, ticker, ohlcv_slice, tech_today, context)
            if sc.triggered: sig_c.append(sc)
                
        # 6. Confluence Engine
        picks = run_confluence(sig_a, sig_b, sig_c, regime.label, guardian.sizing_multiplier, confluence_config)

        # 6b. Risk parity sizing
        if picks and risk_parity_config:
            apply_risk_parity_to_picks(
                picks,
                data_cache={t: technicals_cache[t] for t in [p.ticker for p in picks] if t in technicals_cache},
                config=risk_parity_config,
            )

        # 6c. Pre-flight gap check (simulated: compare entry_price vs actual open)
        if picks and preflight_config:
            max_gap_up = preflight_config.get("max_gap_up_pct", 3.0)
            max_gap_down = preflight_config.get("max_gap_down_pct", 5.0)
            filtered_picks = []
            for pick in picks:
                df = data_cache.get(pick.ticker)
                if df is not None and current_dt in df.index:
                    actual_open = df.loc[current_dt, "Open"]
                    gap_pct = (actual_open / pick.entry_price - 1) * 100
                    if gap_pct > max_gap_up or gap_pct < -max_gap_down:
                        continue  # Skip — gap too large
                filtered_picks.append(pick)
            picks = filtered_picks

        # 7. Add new picks to open positions
        for pick in picks:
            if len(open_positions) >= guardian_config.get("max_open_positions", 4):
                break
                
            pos = Position(
                id=None,
                ticker=pick.ticker,
                name_cn=pick.name_cn,
                entry_date=curr_date_str,
                entry_price=pick.entry_price,
                target_price=pick.target_price,
                stop_price=pick.stop_price,
                current_stop=pick.stop_price,
                max_hold_days=pick.max_hold_days,
                position_size_mult=pick.position_size_mult,
                lens=pick.signals[0].lens if pick.signals else "multi",
                confluence_type=pick.confluence_type,
                status="open"
            )
            open_positions.append(pos)

    # Compute final metrics
    return compute_metrics(closed_trades, equity_curve[1:]) # Drop the initial 100k seed copy
