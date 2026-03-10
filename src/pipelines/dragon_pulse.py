"""
Dragon Pulse (龙脉) Master Pipeline
===================================

Orchestrates the entire Dragon Pulse strategy:
1. Universe construction
2. Data download & Caching
3. Technical analysis (extended)
4. Regime classification
5. Multi-lens scanning (A, B, C)
6. Confluence & Ranking
7. Capital Guardian Risk Check
8. Persistence & Reporting
"""

from __future__ import annotations
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from core.config import load_config, get_config_value
from core.universe import build_universe
from core.cn_data import download_daily_range, get_cn_basic_info
from core.technicals import compute_extended_technicals
from core.display import format_ticker, load_name_cache
from strategy.lens_a_pullback import LensAPullback
from strategy.lens_b_breakout import LensBBreakout
from strategy.lens_c_limitup import LensCLimitUp
from strategy.confluence import ConfluenceConfig, run_confluence
from strategy.regime import classify_regime, RegimeAssessment
from risk.capital_guardian import compute_guardian_verdict, GuardianVerdict
from tracking.pick_store import init_db, save_pick, get_open_picks

logger = logging.getLogger(__name__)

def run_dragon_pulse(
    config_path: Optional[str] = None,
    asof_date: Optional[str] = None,
) -> Dict:
    """Run the master Dragon Pulse screening pipeline."""
    # 1. Load Configuration
    config = load_config(config_path)
    dp_config = config.get("dragon_pulse", {})
    
    # 2. Setup Date
    if asof_date:
        today = pd.to_datetime(asof_date)
    else:
        today = pd.Timestamp.now().normalize()
    
    date_str = today.strftime("%Y-%m-%d")
    logger.info(f"Starting Dragon Pulse Scan for {date_str}...")
    
    # 3. Build Universe
    universe_mode = dp_config.get("universe_mode", "CHINA_ALL")
    tickers = build_universe(mode=universe_mode)
    logger.info(f"Universe size: {len(tickers)} tickers")
    
    # 4. Load Name Cache for Display
    load_name_cache(tickers)
    
    # 5. Fetch Price Data (Last 60 days)
    start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    end_date = date_str
    
    # We download in chunks to be safe with AkShare
    data_map, _ = download_daily_range(tickers, start=start_date, end=end_date, threads=True)
    logger.info(f"Successfully downloaded data for {len(data_map)} tickers")
    
    # 6. Fetch CSI 300 for Regime
    from backtest.data_loader import preload_csi300_data
    csi300_df = preload_csi300_data(start_date, end_date)
    
    # 7. Classify Regime
    regime = classify_regime(csi300_df, data_map, dp_config.get("regime", {}))
    logger.info(
        f"Market Regime: {regime.label.upper()} "
        f"(Sizing: {regime.sizing_mult}x, Breadth: {regime.breadth_score:.1%}, "
        f"CSI300 vs SMA20: {'OK' if regime.csi300_above_sma20 else 'RISK-OFF'})"
    )
    
    if regime.label == "bear" and dp_config.get("regime", {}).get("bear_action") == "BLOCK":
        logger.warning("Market regime is BEAR and bear_action is BLOCK. Skipping scan.")
        return {"status": "skipped", "reason": "bear_market"}
        
    # 8. Run Lenses
    lens_a = LensAPullback(dp_config.get("lens_a", {}))
    lens_b = LensBBreakout(dp_config.get("lens_b", {}))
    lens_c = LensCLimitUp(dp_config.get("lens_c", {}))
    
    sig_a, sig_b, sig_c = [], [], []
    
    logger.info("Scanning for setup signals...")
    # Mock Dragon Tiger for now (In prod, fetch real DTL for today)
    from features.dragon_tiger.scanner import get_institutional_net_buy
    dtl_flow = get_institutional_net_buy(list(data_map.keys()))
    
    for ticker, df in data_map.items():
        if df.empty or len(df) < 30: continue
        
        # Compute technicals
        tech_df = compute_extended_technicals(df)
        tech_today = tech_df.iloc[-1].to_dict()
        
        # Build context
        context = {
            "dtl_net_buy_cny": dtl_flow.get(ticker, 0),
            "sector_momentum_rank": 50 # Placeholder
        }
        
        name = get_cn_basic_info([ticker]).get(ticker, {}).get("name_cn", ticker)
        
        sa = lens_a.scan(ticker, name, df, tech_today, context)
        if sa.triggered: sig_a.append(sa)
        
        sb = lens_b.scan(ticker, name, df, tech_today, context)
        if sb.triggered: sig_b.append(sb)
        
        sc = lens_c.scan(ticker, name, df, tech_today, context)
        if sc.triggered: sig_c.append(sc)
        
    logger.info(f"Signals found: LensA={len(sig_a)}, LensB={len(sig_b)}, LensC={len(sig_c)}")
    
    # 9. Sector momentum for confluence boost
    hot_sectors, cold_sectors, ticker_sector_map = [], [], {}
    try:
        from features.sector.rotation import calculate_sector_momentum_cn
        sector_data = calculate_sector_momentum_cn(top_n=20)
        if sector_data:
            hot_sectors = [s.sector for s in sector_data[:3] if s.momentum_score >= 6.0]
            cold_sectors = [s.sector for s in sector_data[-3:] if s.momentum_score < 4.0]
            # Build ticker→sector map from basic info (best-effort)
            try:
                for ticker in data_map:
                    info = get_cn_basic_info([ticker]).get(ticker, {})
                    sector = info.get("industry", "")
                    if sector:
                        ticker_sector_map[ticker] = sector
            except Exception:
                pass
            logger.info(f"Sector boost: hot={hot_sectors}, cold={cold_sectors}")
    except Exception as e:
        logger.debug(f"Sector momentum unavailable: {e}")

    # 10. Confluence
    conf_config = ConfluenceConfig(**dp_config.get("confluence", {}))
    picks = run_confluence(
        sig_a, sig_b, sig_c, regime.label, regime.sizing_mult, conf_config,
        hot_sectors=hot_sectors, cold_sectors=cold_sectors,
        ticker_sector_map=ticker_sector_map,
    )
    
    # 10. Capital Guardian Risk check
    db_path = dp_config.get("db_path", "data/dragon_pulse.db")
    conn = init_db(db_path)
    
    open_positions = get_open_picks(conn)
    # Note: in real use, we'd calculate equity_curve from DB
    guardian = compute_guardian_verdict(
        equity_curve=[], # simplified
        open_positions=open_positions,
        recent_trades=[], 
        regime_sizing=regime.sizing_mult,
        config=dp_config.get("guardian", {})
    )
    
    final_picks = []
    if guardian.halt:
        logger.warning(f"Capital Guardian HALT: {', '.join(guardian.reasons)}")
    else:
        # Scale picks by guardian multiplier
        for p in picks:
            p.position_size_mult *= guardian.sizing_multiplier

        # Apply risk-parity volatility-adjusted sizing
        if picks:
            try:
                from risk.risk_parity import apply_risk_parity_to_picks
                # Build technicals cache for ATR lookup
                tech_cache = {}
                for ticker in [p.ticker for p in picks]:
                    if ticker in data_map and not data_map[ticker].empty:
                        tech_cache[ticker] = compute_extended_technicals(data_map[ticker])
                apply_risk_parity_to_picks(
                    picks,
                    data_cache=tech_cache,
                    ticker_sector_map=ticker_sector_map,
                    config=dp_config.get("risk_parity", {}),
                )
            except Exception as e:
                logger.debug(f"Risk parity sizing unavailable: {e}")

        for p in picks:
            if p.position_size_mult > 0:
                final_picks.append(p)
                # Save to DB
                save_pick(conn, p.__dict__, date_str)
                
    conn.close()
    
    # 12. Build Output
    logger.info(f"Final Picks for {date_str}: {len(final_picks)}")
    for p in final_picks:
        logger.info(f"  - {p.display_ticker()} | Score: {p.composite_score:.1f} | {p.confluence_type} | Size: {p.position_size_mult:.2f}x")

    # 13. Store preflight thresholds for morning_check.py
    preflight_config = dp_config.get("preflight", {})

    return {
        "date": date_str,
        "regime": regime.label,
        "picks": final_picks,
        "signals_count": {"a": len(sig_a), "b": len(sig_b), "c": len(sig_c)},
        "preflight_config": preflight_config,
    }
