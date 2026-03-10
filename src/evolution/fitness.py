from backtest.engine import run_backtest
from evolution.genome import Genome
from strategy.confluence import ConfluenceConfig
import math

def evaluate_fitness(genome: Genome, start_date: str, end_date: str, 
                     universe: list, data_cache: dict, csi300_df, guardian_config: dict) -> float:
    """Evaluate fitness of a genome by running a backtest."""
    
    conf_config = ConfluenceConfig(
        threshold_a=genome.confluence["threshold_a"],
        threshold_b=genome.confluence["threshold_b"],
        high_threshold=genome.confluence["high_threshold"],
        w_lens_a=genome.confluence["w_lens_a"],
        w_lens_b=genome.confluence["w_lens_b"],
        w_lens_c=genome.confluence["w_lens_c"],
        max_daily_picks=2,
        min_composite_score=45.0
    )
    
    res = run_backtest(
        start_date=start_date,
        end_date=end_date,
        params_a=genome.params_a,
        params_b=genome.params_b,
        params_c=genome.params_c,
        confluence_config=conf_config,
        guardian_config=guardian_config,
        universe_tickers=universe,
        data_cache=data_cache,
        csi300_df=csi300_df
    )
    
    # Store metrics on genome
    genome.metrics = {
        "win_rate": res.win_rate,
        "profit_factor": res.profit_factor,
        "total_trades": res.total_trades,
        "max_drawdown": res.max_drawdown_pct,
        "sharpe": res.sharpe
    }
    
    # Hard constraints
    if res.win_rate < 0.50: return 0.0
    if res.total_trades < 10: return 0.0 # lowered from 20 for faster testing
    if res.max_drawdown_pct > 25.0: return 0.0
    if res.profit_factor < 1.0: return 0.0
    
    # Fitness Function
    dd_penalty = max(0.1, 1.0 - (res.max_drawdown_pct / 30.0))
    fitness = (res.win_rate ** 2) * res.profit_factor * math.log(1 + res.total_trades) * dd_penalty
    
    return round(fitness, 4)
