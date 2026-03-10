"""
CLI Command: Dragon Pulse Backtest
"""
import pandas as pd
from datetime import datetime, timedelta
from src.backtest.engine import run_backtest
from src.backtest.data_loader import preload_historical_data, preload_csi300_data
from src.core.universe import build_universe
from src.core.config import load_config
from src.strategy.confluence import ConfluenceConfig
from src.strategy.lens_a_pullback import LensAPullback
from src.strategy.lens_b_breakout import LensBBreakout
from src.strategy.lens_c_limitup import LensCLimitUp

def cmd_backtest(args):
    """Execute Dragon Pulse backtest."""
    config = load_config(args.config)
    dp_config = config.get("dragon_pulse", {})
    
    start_date = args.start or (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = args.end or datetime.now().strftime("%Y-%m-%d")
    
    print(f"Running Backtest from {start_date} to {end_date}...")
    
    # 1. Build Universe
    tickers = build_universe(mode="CHINA_ALL")
    if args.max_universe:
        import numpy as np
        np.random.seed(42)
        tickers = np.random.choice(tickers, min(args.max_universe, len(tickers)), replace=False).tolist()
        
    # 2. Preload Data
    data_cache = preload_historical_data(tickers, start_date, end_date)
    csi300_df = preload_csi300_data(start_date, end_date)
    
    # 3. Build exit config from lens defaults (dead money + trailing stop)
    exit_config = {
        "dead_money_days": dp_config.get("dead_money_days", 3),
        "dead_money_min_pct": dp_config.get("dead_money_min_pct", 2.0),
        "trailing_trigger_pct": dp_config.get("lens_a", {}).get("trailing_trigger_pct", 2.5),
    }

    # 4. Run Backtest
    res = run_backtest(
        start_date=start_date,
        end_date=end_date,
        params_a=dp_config.get("lens_a", LensAPullback.DEFAULT_PARAMS),
        params_b=dp_config.get("lens_b", LensBBreakout.DEFAULT_PARAMS),
        params_c=dp_config.get("lens_c", LensCLimitUp.DEFAULT_PARAMS),
        confluence_config=ConfluenceConfig(**dp_config.get("confluence", {})),
        guardian_config=dp_config.get("guardian", {}),
        universe_tickers=list(data_cache.keys()),
        data_cache=data_cache,
        csi300_df=csi300_df,
        exit_config=exit_config,
        risk_parity_config=dp_config.get("risk_parity", {}),
        preflight_config=dp_config.get("preflight", {}),
    )
    
    print("\n" + "="*40)
    print("   BACKTEST RESULTS")
    print("="*40)
    print(f"Total Trades:   {res.total_trades}")
    print(f"Win Rate:       {res.win_rate*100:.1f}%")
    print(f"Profit Factor:  {res.profit_factor:.2f}")
    print(f"Max Drawdown:   {res.max_drawdown_pct:.1f}%")
    print(f"Sharpe Ratio:   {res.sharpe:.2f}")
    print("="*40)
    
    return 0
