"""
Weekly Momentum Scanner Pipeline

Orchestrates the full Weekly Scanner pipeline using core modules.
"""

from __future__ import annotations
from datetime import datetime, timezone
from datetime import date as date_type
from pathlib import Path
from typing import Optional
import pandas as pd
import time

# Core imports
from ..core.config import load_config, get_config_value
from ..core.universe import build_universe
from ..core.data import get_data_functions, resolve_market_settings, get_ticker_df
from ..core.cn_data import get_cn_basic_info
from ..core.technicals import compute_technicals
from ..core.scoring import compute_technical_score_weekly
from ..core.io import get_run_dir, save_csv, save_json, save_run_metadata
from ..core.helpers import get_ny_date, get_trading_date, get_market_date

# Feature imports
try:
    from ..features.movers.daily_movers import compute_daily_movers_from_universe
    from ..features.movers.mover_filters import filter_movers, build_mover_technicals_df
    from ..features.movers.mover_queue import update_mover_queue, get_eligible_movers, load_mover_queue, save_mover_queue
except ImportError:
    # Fallback if movers not available
    compute_daily_movers_from_universe = None
    filter_movers = None
    update_mover_queue = None
    get_eligible_movers = None
    load_mover_queue = None
    save_mover_queue = None

try:
    from ..features.dragon_tiger.scanner import scan_dragon_tiger, analyze_dragon_tiger_flow
except ImportError:
    scan_dragon_tiger = None
    analyze_dragon_tiger_flow = None

try:
    from ..features.sector.rotation import calculate_sector_momentum_cn, SectorMomentumCN
except ImportError:
    calculate_sector_momentum_cn = None
    SectorMomentumCN = None

try:
    from ..core.sentiment_cn import compute_sentiment_score_cn
except ImportError:
    compute_sentiment_score_cn = None

# Helper imports
from ..core.helpers import (
    fetch_news_for_tickers,
    fetch_news_for_tickers_cn,
    get_next_earnings_date,
    load_manual_headlines,
)
from ..core.filters import apply_hard_filters
from ..core.regime import check_regime
from ..core.packets import build_weekly_scanner_packet, build_weekly_scanner_packet_cn
from ..core.data_quality import TickerDataQuality, RunDataQuality


def run_weekly(config: Optional[dict] = None, config_path: Optional[str] = None, asof_date: Optional[date_type] = None) -> dict:
    """
    Run the Weekly Momentum Scanner pipeline.
    
    Args:
        config: Optional config dict (if None, loads from config_path)
        config_path: Path to config YAML file (defaults to config/default.yaml)
    
    Returns:
        dict with keys:
          - universe_note: str
          - run_timestamp_utc: str
          - run_dir: Path
          - candidates_csv: Path
          - packets_json: Path
          - metadata_json: Path
    """
    # Load config
    if config is None:
        config = load_config(config_path)

    # Resolve market + data providers
    market_settings = resolve_market_settings(config)
    download_daily, download_daily_range, provider_config, market = get_data_functions(config)
    tz = market_settings["timezone"]
    close_hour = market_settings["close_hour_local"]
    close_minute = market_settings["close_minute_local"]
    
    # Get run directory (using trading date - excludes weekends)
    today = get_trading_date(
        asof_date,
        market=market,
        close_hour=close_hour,
        close_minute=close_minute,
        timezone=tz,
    )
    run_dir = get_run_dir(
        today,
        get_config_value(config, "outputs", "root_dir", default="outputs"),
        market=market,
        close_hour=close_hour,
        close_minute=close_minute,
        timezone=tz,
    )
    
    # Build universe
    china_sources = []
    if market == "CN" and isinstance(provider_config, dict):
        for k in ("primary", "backup"):
            v = provider_config.get(k)
            if v:
                china_sources.append(v)

    universe = build_universe(
        mode=get_config_value(config, "universe", "mode", default="SP500+NASDAQ100+R2000"),
        cache_file=get_config_value(config, "universe", "cache_file", default=None),
        cache_max_age_days=get_config_value(config, "universe", "cache_max_age_days", default=7),
        manual_include_file=get_config_value(config, "universe", "manual_include_file", default=None),
        r2000_include_file=get_config_value(config, "universe", "r2000_include_file", default=None),
        manual_include_mode=get_config_value(config, "universe", "manual_include_mode", default="ALWAYS"),
        china_board_filters=get_config_value(config, "universe", "china_board_filters", default=None),
        china_source_preference=china_sources or None,
        tushare_token_env=get_config_value(config, "data", "china", default={}).get("tushare_token_env", "TUSHARE_TOKEN"),
    )
    
    universe_note = f"Universe: {len(universe)} tickers"
    
    # Handle daily movers if enabled
    mover_source_tags = {}
    if get_config_value(config, "movers", "enabled", default=False) and compute_daily_movers_from_universe:
        movers_config = config.get("movers", {})
        
        try:
            movers_raw = compute_daily_movers_from_universe(
                universe, 
                top_n=movers_config.get("top_n", 50), 
                asof_date=get_market_date(tz)
            )
            mover_universe = []
            for k in ("gainers", "losers"):
                dfm = movers_raw.get(k)
                if isinstance(dfm, pd.DataFrame) and (not dfm.empty) and "ticker" in dfm.columns:
                    mover_universe += dfm["ticker"].astype(str).tolist()
            tech_df = build_mover_technicals_df(
                mover_universe,
                lookback_days=25,
                auto_adjust=get_config_value(config, "runtime", "yf_auto_adjust", default=False),
                threads=get_config_value(config, "runtime", "threads", default=True),
            )
            movers_filtered = filter_movers(movers_raw, technicals_df=tech_df if not tech_df.empty else None, config=movers_config)
            
            from ..features.movers.mover_queue import (
                load_mover_queue, update_mover_queue, get_eligible_movers, save_mover_queue
            )
            queue_df = load_mover_queue()
            queue_df = update_mover_queue(movers_filtered, datetime.now(timezone.utc), movers_config)
            save_mover_queue(queue_df)
            
            eligible_movers = get_eligible_movers(queue_df, datetime.now(timezone.utc))
            if eligible_movers:
                # Tag movers with source
                mover_source_tags = {t: ["DAILY_MOVER"] for t in eligible_movers}
                universe = sorted(set(universe + eligible_movers))
                universe_note += f" + {len(eligible_movers)} daily movers"
        except Exception as e:
            print(f"[WARN] Daily movers integration failed: {e}")
    
    # Download price data
    lookback_days = int(get_config_value(config, "technicals", "lookback_days", default=300))
    auto_adjust = get_config_value(config, "runtime", "yf_auto_adjust", default=False)
    threads = get_config_value(config, "runtime", "threads", default=True)

    if asof_date:
        end = today.strftime("%Y-%m-%d")
        start = (pd.Timestamp(today) - pd.Timedelta(days=lookback_days + 20)).strftime("%Y-%m-%d")
        data, report = download_daily_range(
            tickers=universe,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=auto_adjust,
            threads=threads,
            provider_config=provider_config,
        )
    else:
        data, report = download_daily(
            tickers=universe,
            period=f"{lookback_days}d",
            interval="1d",
            auto_adjust=auto_adjust,
            threads=threads,
            provider_config=provider_config,
        )
    
    # Regime gate check
    rg_config = config.get("regime_gate", {})
    if rg_config.get("enabled", True):
        asof_str = today.strftime("%Y-%m-%d") if asof_date else None
        regime_info = check_regime(
            rg_config,
            asof_date=asof_str,
            download_daily_fn=download_daily,
            download_daily_range_fn=download_daily_range,
            provider_config=provider_config,
        )
        print(f"[REGIME] {regime_info.get('message', '')}")
        if not regime_info.get("ok", True):
            action = rg_config.get("action", "WARN").upper()
            if action == "BLOCK":
                print("[REGIME] BLOCK enabled: skipping weekly scan in risk-off regime.")
                return {
                    "universe_note": universe_note,
                    "run_timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "run_dir": run_dir,
                    "candidates_csv": None,
                    "packets_json": None,
                    "metadata_json": None,
                    "html_report": None,
                    "regime_blocked": True,
                    "regime_info": regime_info,
                }
            print("[REGIME] WARN: continuing, but consider smaller size / fewer trades.")

    # Screen candidates
    print(f"\n[2/4] Screening {len(universe)} tickers...")
    candidates = []
    # Convert config dict to format expected by apply_hard_filters
    filter_params = {
        "price_min": get_config_value(config, "liquidity", "price_min", default=2.0),
        "avg_dollar_volume_20d_min": get_config_value(config, "liquidity", "min_avg_dollar_volume_20d", default=50_000_000),
        # Config uses fraction (e.g., 0.15 == 15%)
        "price_up_5d_max_pct": float(get_config_value(config, "liquidity", "max_5d_return", default=0.15)) * 100.0,
    }
    min_tech_score = float(get_config_value(config, "quality_filters_weekly", "min_technical_score", default=0.0) or 0.0)
    vol_threshold = float(get_config_value(config, "technicals", "realized_vol_threshold_ann_pct", default=20.0) or 20.0)
    
    total = len(universe)
    for idx, ticker in enumerate(universe):
        # Progress indicator
        if (idx + 1) % max(1, total // 20) == 0 or (idx + 1) == total:
            pct = ((idx + 1) / total) * 100
            print(f"  Progress: {idx + 1}/{total} ({pct:.1f}%) | Found: {len(candidates)} candidates", end="\r")
        df = get_ticker_df(data, ticker)
        if df.empty or len(df) < 20:
            continue
        
        # Apply hard filters
        passed, reasons = apply_hard_filters(df, filter_params)
        if not passed:
            continue
        
        # Compute technical score
        tech_result = compute_technical_score_weekly(df, ticker, vol_threshold=vol_threshold)
        if tech_result["score"] == 0:
            continue
        if min_tech_score > 0 and float(tech_result["score"]) < min_tech_score:
            continue
        
        # Get basic metrics
        close = df["Close"]
        volume = df["Volume"]
        last = float(close.iloc[-1])
        adv20 = float((close.tail(20) * volume.tail(20)).mean()) if len(close) >= 20 else 0.0
        
        # Get last trading day timestamp
        try:
            last_date = pd.Timestamp(df.index[-1])
            asof_price_utc = last_date.isoformat() + "Z"
        except:
            asof_price_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        # Store basic candidate data (company info fetched later for top candidates only)
        # This avoids making thousands of API calls during screening
        candidates.append({
            "ticker": ticker,
            "name": ticker,  # Will be updated later for top candidates
            "exchange": "Unknown",  # Will be updated later for top candidates
            "sector": "Unknown",  # Will be updated later for top candidates
            "technical_score": tech_result["score"],
            "technical_evidence": tech_result["evidence"],
            "current_price": last,
            "market_cap_usd": None,  # Will be updated later for top candidates
            "avg_dollar_volume_20d": adv20,
            "asof_price_utc": asof_price_utc,
        })
    
    print()  # New line after progress
    print(f"  Screening complete: {len(candidates)} candidates found")
    
    # Convert to DataFrame and sort by technical score
    candidates_df = pd.DataFrame(candidates)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values("technical_score", ascending=False).head(30)
    
    # Fetch company info for top candidates only (more efficient than during screening)
    if not candidates_df.empty:
        print(f"\n[3/4] Fetching company info for {len(candidates_df)} top candidates...")
        tickers_list = candidates_df["ticker"].tolist()

        if market == "CN":
            cn_info = get_cn_basic_info(tickers_list, provider_config)
            for idx, row in candidates_df.iterrows():
                t = str(row["ticker"]).upper()
                info = cn_info.get(t, {})
                name_cn = info.get("name_cn")
                exch = info.get("exchange")
                if name_cn:
                    candidates_df.at[idx, "name"] = name_cn
                    candidates_df.at[idx, "name_cn"] = name_cn
                if exch:
                    candidates_df.at[idx, "exchange"] = exch
                # Trading code for verification
                candidates_df.at[idx, "trading_number"] = t
                mc = info.get("market_cap")
                if mc:
                    candidates_df.at[idx, "market_cap_usd"] = mc
        else:
            import yfinance as yf
            total_info = len(candidates_df)
            for info_idx, (idx, row) in enumerate(candidates_df.iterrows()):
                # Progress for info fetching
                if (info_idx + 1) % 5 == 0 or (info_idx + 1) == total_info:
                    pct = ((info_idx + 1) / total_info) * 100
                    print(f"  Info: {info_idx + 1}/{total_info} ({pct:.1f}%)", end="\r")
                ticker = row["ticker"]
                # Skip if already has valid info
                if row.get("name") != ticker and row.get("exchange") != "Unknown":
                    continue
                    
                # Fetch company info with retry
                for attempt in range(2):
                    try:
                        if attempt > 0:
                            time.sleep(0.5)
                        tk = yf.Ticker(ticker)
                        info = tk.info
                        if info and isinstance(info, dict) and len(info) > 0:
                            name = info.get("longName", info.get("shortName", ticker))
                            sector = info.get("sector", "Unknown")
                            exchange_raw = info.get("exchange", "Unknown")
                            # Normalize exchange names
                            if exchange_raw and exchange_raw != "Unknown":
                                if "NMS" in exchange_raw or "NASDAQ" in exchange_raw.upper():
                                    exchange = "NASDAQ"
                                elif "NYQ" in exchange_raw or "NYSE" in exchange_raw.upper() or "New York" in exchange_raw:
                                    exchange = "NYSE"
                                else:
                                    exchange = exchange_raw
                            else:
                                exchange = "Unknown"
                            market_cap = info.get("marketCap", None)
                            
                            # Update DataFrame
                            candidates_df.at[idx, "name"] = name
                            candidates_df.at[idx, "sector"] = sector
                            candidates_df.at[idx, "exchange"] = exchange
                            if market_cap:
                                candidates_df.at[idx, "market_cap_usd"] = int(market_cap)
                            break
                    except Exception:
                        if attempt == 1:
                            # Final attempt failed, keep defaults
                            pass
                        continue
                # Throttle to avoid rate limiting
                if (info_idx + 1) % 10 == 0:
                    time.sleep(0.3)
            
            print()  # New line after info progress
    
    # Enrich candidates with Dragon Tiger, sector rotation, and sentiment data
    dragon_tiger_map = {}
    sector_map = {}
    sentiment_map = {}

    if market == "CN" and not candidates_df.empty:
        tickers_for_enrichment = candidates_df["ticker"].tolist()

        # Dragon Tiger List
        if scan_dragon_tiger is not None:
            try:
                print("  Fetching Dragon Tiger List data...")
                dt_candidates = scan_dragon_tiger(
                    tickers=tickers_for_enrichment,
                    min_flow_score=0.0,
                    min_net_buy_cny=0,
                    top_n=len(candidates_df),
                )
                for dtc in dt_candidates:
                    dragon_tiger_map[dtc.ticker.upper()] = {
                        "flow_score": dtc.flow_score,
                        "net_buy_amount_cny": dtc.net_buy_amount_cny,
                        "buy_amount_cny": dtc.buy_amount_cny,
                        "sell_amount_cny": dtc.sell_amount_cny,
                        "reason": dtc.reason,
                        "trade_date": dtc.trade_date,
                    }
                print(f"  Found {len(dragon_tiger_map)} Dragon Tiger matches")
            except Exception as e:
                print(f"  [WARN] Dragon Tiger List fetch failed: {e}")

        # Sector Rotation
        if calculate_sector_momentum_cn is not None:
            try:
                print("  Fetching sector rotation data...")
                sector_list = calculate_sector_momentum_cn(top_n=20)
                # Build a lookup by sector name for matching
                sector_lookup = {}
                for sm in sector_list:
                    sector_lookup[sm.sector] = {
                        "sector": sm.sector,
                        "momentum_score": sm.momentum_score,
                        "return_1d": sm.return_1d,
                        "return_5d": sm.return_5d,
                        "trend": sm.trend,
                    }
                # Match each candidate's sector to sector data
                for _, row in candidates_df.iterrows():
                    t = str(row["ticker"]).upper()
                    candidate_sector = str(row.get("sector", ""))
                    # Try exact match first, then substring
                    if candidate_sector in sector_lookup:
                        sector_map[t] = sector_lookup[candidate_sector]
                    else:
                        for sname, sdata in sector_lookup.items():
                            if sname in candidate_sector or candidate_sector in sname:
                                sector_map[t] = sdata
                                break
                print(f"  Matched {len(sector_map)} tickers to sector data ({len(sector_list)} sectors loaded)")
            except Exception as e:
                print(f"  [WARN] Sector rotation fetch failed: {e}")

        # Sentiment
        if compute_sentiment_score_cn is not None:
            try:
                print("  Fetching sentiment data...")
                for _, row in candidates_df.iterrows():
                    t = str(row["ticker"]).upper()
                    try:
                        sent = compute_sentiment_score_cn(t, headlines=None)
                        if sent.score > 0:
                            sentiment_map[t] = {
                                "score": sent.score,
                                "hot_stock_rank": sent.evidence.get("hot_stock_rank"),
                                "eastmoney_guba": sent.evidence.get("eastmoney_guba", {}),
                                "news_tone": sent.evidence.get("news_tone", "neutral"),
                            }
                    except Exception:
                        pass
                print(f"  Found sentiment data for {len(sentiment_map)} tickers")
            except Exception as e:
                print(f"  [WARN] Sentiment fetch failed: {e}")

    # Save candidates CSV
    candidates_csv = run_dir / f"weekly_scanner_candidates_{today.strftime('%Y-%m-%d')}.csv"
    if not candidates_df.empty:
        # Ensure CN outputs include trading number and Chinese names
        if market == "CN":
            if "trading_number" not in candidates_df.columns:
                candidates_df["trading_number"] = candidates_df["ticker"]
            if "name_cn" not in candidates_df.columns:
                candidates_df["name_cn"] = candidates_df["name"]
        save_csv(candidates_df, candidates_csv)
    
    # Build packets
    print(f"\n[4/4] Building LLM packets...")
    packets = []
    run_data_quality = RunDataQuality()
    if not candidates_df.empty:
        tickers_list = candidates_df["ticker"].tolist()
        print(f"  Fetching news for {len(tickers_list)} tickers...")
        max_news = get_config_value(config, "news", "max_items", default=25)
        throttle_news = get_config_value(config, "news", "throttle_sec", default=0.15)
        if market == "CN":
            news_df = fetch_news_for_tickers_cn(
                tickers_list,
                max_items=max_news,
                throttle_sec=throttle_news,
            )
        else:
            news_df = fetch_news_for_tickers(
                tickers_list,
                max_items=max_news,
                throttle_sec=throttle_news,
            )
        print(f"  Loaded {len(news_df)} news headlines")
        manual_headlines_df = load_manual_headlines("manual_headlines.csv")
        
        total_packets = len(candidates_df)
        for pkt_idx, (_, row) in enumerate(candidates_df.iterrows()):
            if (pkt_idx + 1) % 5 == 0 or (pkt_idx + 1) == total_packets:
                pct = ((pkt_idx + 1) / total_packets) * 100
                print(f"  Packets: {pkt_idx + 1}/{total_packets} ({pct:.1f}%)", end="\r")
            ticker = row["ticker"]
            earnings_date = get_next_earnings_date(ticker)
            source_tags = mover_source_tags.get(ticker, ["BASE_UNIVERSE"])
            
            # Build packet — use CN-specific builder when market is CN
            if market == "CN":
                packet = build_weekly_scanner_packet_cn(
                    ticker=ticker,
                    row=row,
                    news_df=news_df,
                    manual_headlines_df=manual_headlines_df,
                    source_tags=source_tags,
                    dragon_tiger_data=dragon_tiger_map.get(ticker.upper()),
                    sector_data=sector_map.get(ticker.upper()),
                    sentiment_data=sentiment_map.get(ticker.upper()),
                )
            else:
                packet = build_weekly_scanner_packet(
                    ticker=ticker,
                    row=row,
                    news_df=news_df,
                    earnings_date=earnings_date,
                    manual_headlines_df=manual_headlines_df,
                    source_tags=source_tags,
                )
            # Track data quality per ticker
            t_upper = ticker.upper() if isinstance(ticker, str) else str(ticker).upper()
            ticker_news_available = not news_df.empty and ticker in news_df.get("Ticker", pd.Series()).values
            tq = TickerDataQuality(
                ticker=t_upper,
                has_price=True,
                has_dragon_tiger=t_upper in dragon_tiger_map,
                has_sector=t_upper in sector_map,
                has_sentiment=t_upper in sentiment_map,
                has_news=ticker_news_available,
            )
            run_data_quality.add(tq)
            packet["data_quality"] = tq.to_dict()

            packets.append(packet)

        print()  # New line after packet progress
    
    # Save packets JSON
    packets_json = run_dir / f"weekly_scanner_packets_{today.strftime('%Y-%m-%d')}.json"
    save_json({
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "method_version": get_config_value(config, "runtime", "method_version", default="v3.0"),
        "universe_note": universe_note,
        "data_quality_summary": run_data_quality.summary(),
        "packets": packets,
    }, packets_json)
    
    # Save metadata
    metadata_json = save_run_metadata(
        run_dir=run_dir,
        method_version=get_config_value(config, "runtime", "method_version", default="v3.0"),
        config=config,
        universe_size=len(universe),
        candidates_count=len(candidates_df),
    )
    
    # Generate HTML report
    try:
        from ..core.report import generate_html_report
        html_file = generate_html_report(run_dir, today.strftime('%Y-%m-%d'))
        print(f"  ✓ HTML report: {html_file}")
    except Exception as e:
        print(f"  ⚠ HTML report generation failed: {e}")
        html_file = None
    
    return {
        "universe_note": universe_note,
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_dir": run_dir,
        "candidates_csv": candidates_csv,
        "packets_json": packets_json,
        "metadata_json": metadata_json,
        "html_report": str(html_file) if html_file else None,
    }

