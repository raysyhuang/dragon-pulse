"""
CLI Command: Dragon Pulse Evolution
"""
from datetime import datetime, timedelta
from src.evolution.engine import run_evolution
from src.backtest.data_loader import preload_historical_data, preload_csi300_data
from src.core.universe import build_universe
from src.core.config import load_config

def cmd_evolve(args):
    """Execute Dragon Pulse parameter evolution."""
    config = load_config(args.config)
    ev_config = config.get("evolution", {})
    dp_config = config.get("dragon_pulse", {})
    
    train_end = datetime.now().strftime("%Y-%m-%d")
    train_start = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d") # 6 months
    
    print(f"Running Evolution optimization ({args.generations} gens)...")
    
    # 1. Universe
    tickers = build_universe(mode="CHINA_ALL")
    import numpy as np
    np.random.seed(42)
    sample_tickers = np.random.choice(tickers, min(args.population * 10, 300), replace=False).tolist()
    
    # 2. Preload
    data_cache = preload_historical_data(sample_tickers, train_start, train_end)
    csi300_df = preload_csi300_data(train_start, train_end)
    
    # 3. Run GA
    results = run_evolution(
        train_start=train_start,
        train_end=train_end,
        universe=list(data_cache.keys()),
        data_cache=data_cache,
        csi300_df=csi300_df,
        guardian_config=dp_config.get("guardian", {}),
        population_size=args.population,
        generations=args.generations
    )
    
    best = results[0]
    print("\n" + "*"*40)
    print("   EVOLUTION COMPLETE")
    print("*"*40)
    print(f"Best Fitness: {best.fitness:.4f}")
    print(f"Best Win Rate: {best.metrics['win_rate']*100:.1f}%")
    print(f"Best Profit Factor: {best.metrics['profit_factor']:.2f}")
    print(f"Best Parameters saved to DB.")
    print("*"*40)
    
    return 0
