"""
Complete scan command handler (all systems + hybrid analysis).

Invariant R1: Retries re-run computation but MUST NOT emit side effects.
"""

from __future__ import annotations
import json
import hashlib
import os
import pandas as pd
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime
import logging
from src.core.config import load_config, get_config_value
from src.core.helpers import get_ny_date, get_trading_date
from src.core.io import get_run_dir, save_json
from src.core.report import generate_html_report
from src.pipelines.weekly import run_weekly
from src.pipelines.pro30 import run_pro30
from src.pipelines.swing import run_swing
from src.core.universe import build_universe
from src.features.movers.daily_movers import compute_daily_movers_from_universe
from src.features.movers.mover_filters import filter_movers
from src.features.movers.mover_queue import (
    update_mover_queue, get_eligible_movers, load_mover_queue, save_mover_queue
)
from src.core.llm import rank_weekly_candidates, rank_with_debate
from src.core.decision_summary_v2 import build_decision_summary_v2
from src.core.decision_context import build_decision_context
from src.utils.time import utc_now

# Check if debate is available
try:
    from src.core.debate import DEBATE_AVAILABLE
except ImportError:
    DEBATE_AVAILABLE = False

# New modules (v3.1+)
try:
    from src.core.alerts import AlertConfig, send_overlap_alert, send_run_summary_alert
    from src.core.logging_utils import log_operation, ProgressLogger
    ALERTS_AVAILABLE = True
except ImportError:
    ALERTS_AVAILABLE = False

logger = logging.getLogger(__name__)


def _compute_config_hash(config: dict) -> str:
    """Stable hash for effective runtime configuration."""
    config_str = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(config_str.encode("utf-8")).hexdigest()


def _build_runtime_fingerprint(
    args,
    config: dict,
    output_date_str: str,
    asof_date,
    universe_size: int | None,
) -> dict:
    """Capture deterministic runtime context for forensic comparisons."""
    runtime_cfg = config.get("runtime", {})
    universe_cfg = config.get("universe", {})
    db_path = Path("data/prices.db")
    db_last_modified_utc = None
    if db_path.exists():
        db_last_modified_utc = datetime.utcfromtimestamp(db_path.stat().st_mtime).isoformat() + "Z"

    db_row_count = None
    db_ticker_count = None
    try:
        from src.core.price_db import get_price_db

        db_stats = get_price_db(str(db_path)).get_stats()
        db_row_count = int(db_stats.get("total_records", 0))
        db_ticker_count = int(db_stats.get("total_tickers", 0))
    except Exception as e:
        logger.debug(f"Runtime fingerprint could not read price DB stats: {e}")

    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    polygon_enabled = bool(runtime_cfg.get("polygon_primary", False) and polygon_api_key)

    return {
        "date": output_date_str,
        "asof": asof_date.strftime("%Y-%m-%d") if asof_date else None,
        "config_path": getattr(args, "config", "config/default.yaml"),
        "config_hash": _compute_config_hash(config),
        "workflow_name": os.environ.get("GITHUB_WORKFLOW", "LOCAL"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "prices_db_path": str(db_path),
        "prices_db_exists": db_path.exists(),
        "prices_db_last_modified_utc": db_last_modified_utc,
        "prices_db_row_count": db_row_count,
        "prices_db_ticker_count": db_ticker_count,
        "universe_mode": universe_cfg.get("mode", "SP500+NASDAQ100+R2000"),
        "universe_cache_file": universe_cfg.get("cache_file", "universe_cache.csv"),
        "universe_cache_max_age_days": universe_cfg.get("cache_max_age_days", 7),
        "universe_size": universe_size,
        "lookback_days": config.get("technicals", {}).get("lookback_days", 300),
        "polygon_enabled": polygon_enabled,
        "polygon_workers": runtime_cfg.get("polygon_max_workers", 8),
        "allow_partial_day_attention": bool(runtime_cfg.get("allow_partial_day_attention", False)),
    }


def _open_browser(file_path: Path) -> None:
    """Open file in default browser (cross-platform)."""
    import webbrowser
    try:
        url = file_path.as_uri()
        webbrowser.open(url)
        logger.info("Opened report in browser")
    except Exception as e:
        logger.warning(f"Failed to open browser: {e}")
        logger.info(f"Manually open: {file_path}")


def cmd_all(args) -> int:
    """Run all screeners and produce hybrid analysis."""
    import time as _time
    _run_t0 = _time.time()
    _stage_timings: dict[str, dict] = {}

    logger.info("=" * 60)
    logger.info("COMPLETE SCAN - All Systems")
    logger.info("=" * 60)
    
    # Output folder date = last trading day (not calendar date)
    # This ensures we don't create folders for weekends/holidays
    # - output_date_str: last trading day (or --date override)
    # - asof_date: same as output_date (last completed trading day)
    from datetime import datetime as dt
    if args.date:
        # User specified a date explicitly
        output_date = dt.strptime(args.date, "%Y-%m-%d").date()
        output_date_str = args.date
    else:
        # Use last trading day, not calendar date
        output_date = get_trading_date()
        output_date_str = output_date.strftime("%Y-%m-%d")
    asof_date = get_trading_date(output_date)
    config = load_config(args.config)
    cfg = SimpleNamespace(
        phase3=config.get("phase3", {}),
        phase4=config.get("phase4", {}),
        phase5=config.get("phase5", {}),
    )
    
    if args.no_movers:
        config["movers"]["enabled"] = False
    if getattr(args, "legacy_pro30", False):
        config = dict(config)
        config.setdefault("liquidity", {})
        config["liquidity"]["min_avg_dollar_volume_20d"] = 20_000_000
        config.setdefault("quality_filters_30d", {})
        config["quality_filters_30d"]["min_score"] = 0.0
        config.setdefault("movers", {})
        config["movers"]["enabled"] = False
    if getattr(args, "intraday_attention", False):
        config.setdefault("attention_pool", {})
        config["attention_pool"]["enable_intraday"] = True
    if getattr(args, "allow_partial_day", False):
        config.setdefault("runtime", {})
        config["runtime"]["allow_partial_day_attention"] = True
    
    results = {}

    universe_size = None

    # Determine market region early — gates US-only pipelines (movers, swing)
    market_region = get_config_value(config, "market", "region", default="US").upper()

    # Build universe (needed by all pipelines)
    ucfg = config.get("universe", {})
    quarantine_cfg = config.get("data_reliability", {}).get("quarantine", {})
    universe = build_universe(
        mode=ucfg.get("mode", "SP500+NASDAQ100+R2000"),
        cache_file=ucfg.get("cache_file"),
        cache_max_age_days=ucfg.get("cache_max_age_days", 7),
        manual_include_file=ucfg.get("manual_include_file"),
        r2000_include_file=ucfg.get("r2000_include_file"),
        manual_include_mode=ucfg.get("manual_include_mode", "ALWAYS"),
        quarantine_file=quarantine_cfg.get("file", "data/bad_tickers.json"),
        quarantine_enabled=bool(quarantine_cfg.get("enabled", True)),
        china_board_filters=ucfg.get("china_board_filters"),
        china_source_preference=ucfg.get("china_source_preference"),
    )
    universe_size = len(universe)
    logger.info(f"  Universe: {universe_size} tickers ({market_region})")

    # Step 1: Daily Movers — US-only (yfinance/Polygon), skip for CN
    _step_t0 = _time.time()
    if market_region == "CN":
        logger.info("\n[1/6] Daily Movers — skipped (CN market, US-only pipeline)")
        results["movers"] = {"count": 0, "tickers": []}
        _stage_timings["movers"] = {"status": "skipped", "reason": "CN market"}
    else:
        logger.info("\n[1/6] Daily Movers Discovery...")
        try:
            movers_config = config.get("movers", {})
            runtime_config = config.get("runtime", {})
            polygon_api_key = os.environ.get("POLYGON_API_KEY")
            reliability_cfg = config.get("data_reliability", {})
            movers_raw = compute_daily_movers_from_universe(
                universe,
                top_n=movers_config.get("top_n", 50),
                asof_date=get_trading_date(asof_date),
                polygon_api_key=polygon_api_key,
                use_polygon_primary=bool(runtime_config.get("polygon_primary", False) and polygon_api_key),
                polygon_max_workers=runtime_config.get("polygon_max_workers", 8),
                quarantine_cfg=reliability_cfg.get("quarantine", {}) if isinstance(reliability_cfg, dict) else {},
                yf_retry_cfg=reliability_cfg.get("yfinance", {}) if isinstance(reliability_cfg, dict) else {},
                polygon_retry_cfg=reliability_cfg.get("polygon", {}) if isinstance(reliability_cfg, dict) else {},
            )
            # Make movers credibility real: pass adv/avgvol so volume spike + $ADV20 checks can be enforced
            try:
                from src.features.movers.mover_filters import build_mover_technicals_df
                mover_universe = []
                for k in ("gainers", "losers"):
                    dfm = movers_raw.get(k)
                    if isinstance(dfm, pd.DataFrame) and (not dfm.empty) and "ticker" in dfm.columns:
                        mover_universe += dfm["ticker"].astype(str).tolist()
                tech_df = build_mover_technicals_df(
                    mover_universe,
                    lookback_days=25,
                    auto_adjust=bool(config.get("runtime", {}).get("yf_auto_adjust", False)),
                    threads=bool(config.get("runtime", {}).get("threads", True)),
                )
            except Exception:
                tech_df = None
            movers_filtered = filter_movers(movers_raw, technicals_df=tech_df if tech_df is not None and not tech_df.empty else None, config=movers_config)
            from src.overlays.phase3_filters import apply_phase3_filters
            movers_filtered = apply_phase3_filters(movers_filtered, {}, cfg.phase3)
            queue_df = load_mover_queue()
            queue_df = update_mover_queue(movers_filtered, utc_now(), movers_config)
            save_mover_queue(queue_df)
            eligible_movers = get_eligible_movers(queue_df, utc_now())
            results["movers"] = {"count": len(eligible_movers), "tickers": eligible_movers}
            logger.info(f"  ✓ Found {len(eligible_movers)} eligible movers")
            _stage_timings["movers"] = {"status": "ok", "duration_s": round(_time.time() - _step_t0)}
        except Exception as e:
            logger.error(f"  ✗ Movers failed: {e}", exc_info=True)
            results["movers"] = {"count": 0, "tickers": []}
            _stage_timings["movers"] = {"status": "error", "error": str(e), "duration_s": round(_time.time() - _step_t0)}

    # Root output dir (configurable)
    outputs_root = Path(config.get("outputs", {}).get("root_dir", "outputs"))
    output_dir = outputs_root / output_date_str
    output_dir.mkdir(parents=True, exist_ok=True)

    # Record deterministic runtime context once per run
    runtime_fingerprint = _build_runtime_fingerprint(
        args=args,
        config=config,
        output_date_str=output_date_str,
        asof_date=asof_date,
        universe_size=universe_size,
    )
    runtime_fingerprint_path = output_dir / f"runtime_fingerprint_{output_date_str}.json"
    save_json(runtime_fingerprint, runtime_fingerprint_path)
    logger.info(f"  ✓ Runtime fingerprint saved: {runtime_fingerprint_path.name}")

    # Step 2: Swing Strategy (Primary) — US-only, skip for CN market
    _step_t0 = _time.time()
    if market_region == "CN":
        logger.info("\n[2/7] Swing Strategy — skipped (CN market, US-only pipeline)")
        results["swing"] = None
        _stage_timings["swing"] = {"status": "skipped", "reason": "CN market"}
    else:
        logger.info("\n[2/7] Swing Strategy (Primary)...")
        try:
            swing_result = run_swing(
                config=config,
                asof_date=asof_date,
                output_date=output_date,
                run_dir=output_dir,
            )
            results["swing"] = swing_result
            logger.info("  ✓ Swing strategy complete")
            _stage_timings["swing"] = {"status": "ok", "duration_s": round(_time.time() - _step_t0)}
        except Exception as e:
            logger.error(f"  ✗ Swing strategy failed: {e}", exc_info=True)
            results["swing"] = None
            _stage_timings["swing"] = {"status": "error", "error": str(e), "duration_s": round(_time.time() - _step_t0)}
    
    # Step 3: Weekly Scanner (Secondary)
    _step_t0 = _time.time()
    logger.info("\n[3/7] Weekly Scanner (Secondary)...")
    try:
        # Pass regime from pro30 to weekly so it can widen funnel in bear markets
        _pro30_regime_info = (results.get("pro30") or {}).get("regime_info", {})
        _weekly_regime = "stress" if not _pro30_regime_info.get("spy_above_ma", True) else None
        weekly_result = run_weekly(
            config=config,
            asof_date=asof_date,
            output_date=output_date,
            run_dir=output_dir,
            regime=_weekly_regime,
        )
        results["weekly"] = weekly_result
        _weekly_candidates = 0
        if weekly_result and weekly_result.get("candidates_csv"):
            try:
                _weekly_candidates = len(pd.read_csv(weekly_result["candidates_csv"]))
            except Exception:
                pass
        _stage_timings["weekly"] = {
            "status": "ok",
            "duration_s": round(_time.time() - _step_t0),
            "candidates_found": _weekly_candidates,
            "universe_screened": universe_size,
        }
        logger.info("  ✓ Weekly scanner complete")
        
        phase4_cfg = cfg.phase4
        phase4_enabled = phase4_cfg.get("enabled", False) if isinstance(phase4_cfg, dict) else bool(getattr(phase4_cfg, "enabled", False))
        if phase4_enabled:
            trade_plan_path = output_dir / f"trade_plan_{output_date_str}.csv"
            if trade_plan_path.exists():
                try:
                    trade_plan_rows = pd.read_csv(trade_plan_path).to_dict(orient="records")
                    from src.overlays.phase4_conviction import apply_phase4_overlay
                    trade_plan_rows = apply_phase4_overlay(trade_plan_rows, results, {}, phase4_cfg)
                    pd.DataFrame(trade_plan_rows).to_csv(trade_plan_path, index=False)
                except Exception as e:
                    logger.warning(f"Phase4 overlay skipped (trade plan): {e}")
    except Exception as e:
        logger.error(f"  ✗ Weekly scanner failed: {e}", exc_info=True)
        results["weekly"] = None
        _stage_timings["weekly"] = {"status": "error", "error": str(e), "duration_s": round(_time.time() - _step_t0)}

    # Step 4: 30-Day Screener (Secondary) — US-only (yfinance/Polygon), skip for CN
    _step_t0 = _time.time()
    if market_region == "CN":
        logger.info("\n[4/7] 30-Day Screener — skipped (CN market, US-only pipeline)")
        results["pro30"] = None
        _stage_timings["pro30"] = {"status": "skipped", "reason": "CN market"}
    else:
        logger.info("\n[4/7] 30-Day Screener (Secondary)...")
        try:
            ucfg = config.get("universe", {})
            runtime_cfg = config.get("runtime", {})
            logger.info(
                "  Pro30 context: config=%s asof=%s output=%s universe_mode=%s cache=%s max_age_days=%s lookback_days=%s polygon_primary=%s polygon_workers=%s price_db=%s",
                getattr(args, "config", "config/default.yaml"),
                asof_date.strftime("%Y-%m-%d") if asof_date else "N/A",
                output_date_str,
                ucfg.get("mode", "SP500+NASDAQ100+R2000"),
                ucfg.get("cache_file", "universe_cache.csv"),
                ucfg.get("cache_max_age_days", 7),
                config.get("technicals", {}).get("lookback_days", 300),
                bool(runtime_cfg.get("polygon_primary", False)),
                runtime_cfg.get("polygon_max_workers", 8),
                "data/prices.db",
            )
            pro30_result = run_pro30(
                config=config,
                asof_date=asof_date,
                output_date=output_date,
                run_dir=output_dir,
            )
            results["pro30"] = pro30_result
            logger.info("  ✓ 30-Day screener complete")
            _stage_timings["pro30"] = {"status": "ok", "duration_s": round(_time.time() - _step_t0)}
        except Exception as e:
            logger.error(f"  ✗ 30-Day screener failed: {e}", exc_info=True)
            results["pro30"] = None
            _stage_timings["pro30"] = {"status": "error", "error": str(e), "duration_s": round(_time.time() - _step_t0)}
    
    # Step 5: LLM Ranking & Hybrid Analysis (with optional Bull/Bear Debate)
    _step_t0 = _time.time()
    logger.info("\n[5/7] LLM Ranking & Hybrid Analysis...")
    debate_analysis = {}  # Store debate results for later use
    primary_packet_lookup = {}  # ticker -> source packet with deterministic fields
    try:
        # Load primary packets (configurable; fallback to other source)
        use_swing_primary = bool(config.get("swing_strategy", {}).get("use_as_primary", True))
        primary_source = "swing" if use_swing_primary else "weekly"
        packets_file = None
        if primary_source == "swing":
            if results.get("swing") and results["swing"].get("packets_json"):
                packets_file = results["swing"]["packets_json"]
            elif results.get("weekly") and results["weekly"].get("packets_json"):
                primary_source = "weekly"
                packets_file = results["weekly"]["packets_json"]
        else:
            if results.get("weekly") and results["weekly"].get("packets_json"):
                packets_file = results["weekly"]["packets_json"]
            elif results.get("swing") and results["swing"].get("packets_json"):
                primary_source = "swing"
                packets_file = results["swing"]["packets_json"]
        
        if packets_file:
            with open(packets_file, "r") as f:
                packets_data = json.load(f)
            packets = packets_data.get("packets", [])
            if not packets and primary_source == "swing" and results.get("weekly") and results["weekly"].get("packets_json"):
                primary_source = "weekly"
                with open(results["weekly"]["packets_json"], "r") as f:
                    packets_data = json.load(f)
                packets = packets_data.get("packets", [])
            
            if packets:
                primary_packet_lookup = {
                    str(p.get("ticker", "")).upper(): p
                    for p in packets
                    if isinstance(p, dict) and p.get("ticker")
                }
                # Rank candidates
                model = args.model or "gpt-5.2"
                
                # Check if debate mode is enabled (use debate for GPT-5.2 by default)
                # --no-debate flag disables it
                no_debate = getattr(args, "no_debate", False)
                use_debate = (not no_debate) and DEBATE_AVAILABLE
                debate_rounds = getattr(args, "debate_rounds", 1)
                
                if use_debate and model in ["gpt-5.2", "gpt-4o", "gpt-4-turbo"]:
                    logger.info("  Using advanced ranking with Bull/Bear debate...")
                    llm_result = rank_with_debate(
                        packets=packets,
                        provider=args.provider,
                        model=model,
                        api_key=args.api_key,
                        debate_rounds=debate_rounds,
                        debate_top_n=10,
                        use_memory=True,
                    )
                    debate_analysis = llm_result.get("debate_analysis", {})
                else:
                    llm_result = rank_weekly_candidates(
                        packets=packets,
                        provider=args.provider,
                        model=model,
                        api_key=args.api_key,
                    )
                all_top5 = llm_result.get("top5", [])
                
                # Apply rank-based filtering (based on backtest: Rank 1=38%, Rank 2,4=19%)
                weekly_filters = config.get("quality_filters_weekly", {})
                top_ranks_only = weekly_filters.get("top_ranks_only", 5)  # Default: all 5
                if top_ranks_only < 5 and all_top5:
                    filtered_top5 = [item for item in all_top5 if item.get("rank", 99) <= top_ranks_only]
                    logger.info(f"  📊 Rank filter: Keeping ranks 1-{top_ranks_only} ({len(filtered_top5)} of {len(all_top5)} picks)")
                    results["llm_primary_top5"] = filtered_top5
                    llm_result["top5"] = filtered_top5
                    llm_result["rank_filter_applied"] = f"Top {top_ranks_only} only"
                else:
                    results["llm_primary_top5"] = all_top5
                
                # Save LLM results
                llm_result["primary_label"] = "Swing" if primary_source == "swing" else "Weekly"
                if primary_source == "swing":
                    top5_file = output_dir / f"swing_top5_{output_date_str}.json"
                else:
                    top5_file = output_dir / f"weekly_scanner_top5_{output_date_str}.json"
                save_json(llm_result, top5_file)
                
                # Save debate analysis separately if available
                if debate_analysis:
                    debate_file = output_dir / f"debate_analysis_{output_date_str}.json"
                    save_json({
                        "date": output_date_str,
                        "model": model,
                        "debate_rounds": debate_rounds,
                        "analysis": debate_analysis,
                    }, debate_file)
                    logger.info(f"  ✓ Debate analysis saved to {debate_file.name}")
                
                results["llm_primary_source"] = primary_source
                results["llm_primary_top5_file"] = str(top5_file)
                logger.info(f"  ✓ LLM ranking complete (source: {primary_source})")
                _stage_timings["llm_ranking"] = {
                    "status": "ok",
                    "duration_s": round(_time.time() - _step_t0),
                    "input_candidates": len(packets),
                    "output_picks": len(results.get("llm_primary_top5", [])),
                }
            else:
                logger.warning("  ⚠ No packets found for LLM ranking")
                results["llm_primary_top5"] = []
                _stage_timings["llm_ranking"] = {"status": "no_packets", "duration_s": round(_time.time() - _step_t0)}
        else:
            logger.warning("  ⚠ Primary packets not available")
            results["llm_primary_top5"] = []
            _stage_timings["llm_ranking"] = {"status": "no_packets", "duration_s": round(_time.time() - _step_t0)}
    except Exception as e:
        logger.error(f"  ✗ LLM ranking failed: {e}", exc_info=True)
        results["llm_primary_top5"] = []
        _stage_timings["llm_ranking"] = {"status": "error", "error": str(e), "duration_s": round(_time.time() - _step_t0)}
    
    # Generate Hybrid Analysis
    logger.info("\n" + "=" * 60)
    logger.info("HYBRID ANALYSIS - Cross-Referenced Results")
    logger.info("=" * 60)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # LOAD PRO30 WITH DETERMINISTIC RANKING
    # Previously: converted to set(), losing all ranking info → caused tie-break collapse
    # Now: preserve full DataFrame with deterministic multi-key sort
    # ═══════════════════════════════════════════════════════════════════════════════
    pro30_tickers = set()
    pro30_lookup = {}  # ticker -> {pro30_rank, Score, RSI14, ...}
    
    if results.get("pro30") and results["pro30"].get("candidates_csv"):
        try:
            pro30_df = pd.read_csv(results["pro30"]["candidates_csv"])
            if not pro30_df.empty and "Ticker" in pro30_df.columns:
                # Data-integrity guard: remove duplicated feature signatures that
                # indicate copied OHLCV rows mapped to different tickers.
                signature_cols = [
                    c for c in [
                        "Last", "RVOL", "ATR%", "RSI14", "Dist_to_52W_High%",
                        "$ADV20", "MA20", "MA50", "Ret20d%", "Ret5d%", "Setup", "Score",
                    ]
                    if c in pro30_df.columns
                ]
                if signature_cols:
                    dupe_mask = pro30_df.duplicated(subset=signature_cols, keep="first")
                    dupe_count = int(dupe_mask.sum())
                    if dupe_count > 0:
                        dropped = pro30_df.loc[dupe_mask, "Ticker"].astype(str).tolist()
                        logger.warning(
                            "Pro30 integrity: dropping %d duplicated feature rows (tickers=%s)",
                            dupe_count,
                            dropped[:10],
                        )
                        pro30_df = pro30_df.loc[~dupe_mask].reset_index(drop=True)

                # Validate required columns for deterministic ranking
                REQUIRED_PRO30_COLS = {"Ticker", "Score", "$ADV20", "RSI14"}
                missing = REQUIRED_PRO30_COLS - set(pro30_df.columns)
                if missing:
                    logger.warning(f"Pro30 CSV missing columns for ranking: {missing}")
                    # Fall back to simple set if columns missing
                    pro30_tickers = set(pro30_df["Ticker"].tolist())
                else:
                    # Derive TrendScore from MA alignment (Above_MA20 + Above_MA50)
                    # Higher = better trend confirmation
                    if "Above_MA20" in pro30_df.columns and "Above_MA50" in pro30_df.columns:
                        pro30_df["_TrendScore"] = (
                            pro30_df["Above_MA20"].astype(int) + 
                            pro30_df["Above_MA50"].astype(int)
                        )
                    else:
                        pro30_df["_TrendScore"] = 0
                    
                    # DETERMINISTIC MULTI-KEY SORT (no alphabetical bias)
                    # Priority: Score > RSI14 > TrendScore > $ADV20
                    # Ties after all 4 keys are broken by hash (deterministic but not A-Z)
                    import hashlib
                    pro30_df["_hash_key"] = pro30_df["Ticker"].apply(
                        lambda t: hashlib.md5(f"{t}{output_date_str}".encode()).hexdigest()
                    )
                    pro30_ranked = (
                        pro30_df
                        .sort_values(
                            by=["Score", "RSI14", "_TrendScore", "$ADV20", "_hash_key"],
                            ascending=[False, False, False, False, True],
                        )
                        .reset_index(drop=True)
                    )
                    pro30_ranked = pro30_ranked.drop(columns=["_hash_key"])
                    pro30_ranked["pro30_rank"] = pro30_ranked.index + 1
                    
                    # Sanity check: Score must be monotonically decreasing after sort
                    # If this fails, upstream Pro30 generation is broken
                    if not pro30_ranked["Score"].is_monotonic_decreasing:
                        logger.warning("Pro30 Score not monotonic after sort - possible tie or data issue")
                    
                    # Build lookup dictionary for O(1) access
                    pro30_lookup = (
                        pro30_ranked
                        .set_index("Ticker")
                        .to_dict(orient="index")
                    )
                    pro30_tickers = set(pro30_lookup.keys())
                    
                    logger.info(f"  Pro30 loaded with deterministic ranking: {len(pro30_tickers)} candidates")
                    # Log top 5 ranked for transparency
                    top5_ranked = pro30_ranked.head(5)
                    for _, row in top5_ranked.iterrows():
                        logger.debug(f"    r{int(row['pro30_rank'])}: {row['Ticker']} "
                                   f"(Score={row['Score']:.1f}, RSI={row['RSI14']:.1f})")
        except (FileNotFoundError, pd.errors.EmptyDataError, KeyError) as e:
            logger.debug(f"Pro30 CSV read skipped: {e}")
    
    # Get primary top 5 tickers (Swing preferred)
    primary_top5_tickers = set()
    for item in results.get("llm_primary_top5", []):
        if isinstance(item, dict) and "ticker" in item:
            primary_top5_tickers.add(item["ticker"])
    
    # Get movers
    movers_tickers = set(results.get("movers", {}).get("tickers", []))
    
    # Find overlaps
    overlap_primary_pro30 = primary_top5_tickers.intersection(pro30_tickers)
    overlap_primary_movers = primary_top5_tickers.intersection(movers_tickers)
    overlap_pro30_movers = pro30_tickers.intersection(movers_tickers)
    overlap_all_three = primary_top5_tickers.intersection(pro30_tickers).intersection(movers_tickers)
    
    # Print summary
    primary_label = "Swing" if results.get("llm_primary_source") == "swing" else "Weekly"
    primary_regime = None
    if results.get("swing") and results["swing"].get("regime"):
        primary_regime = results["swing"]["regime"]
    logger.info("\n📊 Results Summary:")
    logger.info(f"  {primary_label} Top 5: {len(primary_top5_tickers)} tickers")
    logger.info(f"  30-Day Candidates: {len(pro30_tickers)} tickers")
    logger.info(f"  Daily Movers: {len(movers_tickers)} tickers")
    
    logger.info("\n🎯 Overlap Analysis (Higher Conviction):")
    if overlap_all_three:
        logger.info(f"  ⭐ ALL THREE (Highest Conviction): {len(overlap_all_three)} tickers")
        for t in sorted(overlap_all_three):
            logger.info(f"    - {t}")
    
    if overlap_primary_pro30:
        logger.info(f"  🔥 {primary_label} + 30-Day: {len(overlap_primary_pro30)} tickers")
        for t in sorted(overlap_primary_pro30 - overlap_all_three):
            logger.info(f"    - {t}")
    
    if overlap_primary_movers:
        logger.info(f"  📈 {primary_label} + Movers: {len(overlap_primary_movers)} tickers")
        for t in sorted(overlap_primary_movers - overlap_all_three):
            logger.info(f"    - {t}")
    
    if overlap_pro30_movers:
        logger.info(f"  💎 30-Day + Movers: {len(overlap_pro30_movers)} tickers")
        for t in sorted(overlap_pro30_movers - overlap_all_three):
            logger.info(f"    - {t}")
    
    # Save hybrid results
    # output_dir already created above (output_date_str folder)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # PRO30 WEIGHTING: Based on backtest, Pro30 has 33.3% hit rate vs Weekly's 27.6%
    # Give Pro30 picks 2x weight in final scoring
    # ═══════════════════════════════════════════════════════════════════════════════
    weighted_picks = []
    all_tickers = primary_top5_tickers | pro30_tickers | movers_tickers
    
    for ticker in all_tickers:
        hybrid_score = 0.0
        sources = []
        candidate_price = 0.0
        
        # Primary Top 5 contribution (Swing preferred)
        if ticker in primary_top5_tickers:
            primary_item = next((x for x in results.get("llm_primary_top5", []) if x.get("ticker") == ticker), None)
            packet_item = primary_packet_lookup.get(str(ticker).upper(), {})
            if primary_item:
                try:
                    packet_price = float(packet_item.get("current_price") or 0)
                    llm_price = float(primary_item.get("current_price") or 0)
                    primary_price = packet_price if packet_price > 0 else llm_price
                    if primary_price > 0:
                        candidate_price = primary_price
                    if packet_price > 0 and llm_price > 0:
                        ratio = max(packet_price, llm_price) / max(min(packet_price, llm_price), 1e-9)
                        if ratio > 3:
                            logger.warning(
                                "Price mismatch for %s in primary ranking: packet=%.2f llm=%.2f. "
                                "Using packet price.",
                                ticker, packet_price, llm_price,
                            )
                except (TypeError, ValueError):
                    pass
            hw = config.get("hybrid_weighting", {})
            if primary_label == "Swing":
                base_weight = float(hw.get("swing_weight", 1.2))
                rank1_bonus = float(hw.get("swing_rank1_bonus", 0.6))
                rank2_bonus = float(hw.get("swing_rank2_bonus", 0.3))
            else:
                base_weight = float(hw.get("weekly_weight", 1.0))
                rank1_bonus = float(hw.get("weekly_rank1_bonus", 0.5))
                rank2_bonus = float(hw.get("weekly_rank2_bonus", 0.2))
            rank = primary_item.get("rank", 5) if primary_item else 5
            primary_score = base_weight
            if rank == 1:
                primary_score += rank1_bonus
            elif rank == 2:
                primary_score += rank2_bonus
            hybrid_score += primary_score
            sources.append(f"{primary_label}({rank if primary_item else '?'})")
        
        # Pro30 contribution (weight: 2.0 base + rank bonus for deterministic ordering)
        # Rank bonus: top-ranked Pro30 picks get up to +1.0 extra (decays by 0.05 per rank)
        # This eliminates alphabetical bias from tie-breaking
        if ticker in pro30_lookup:
            pro30_info = pro30_lookup[ticker]
            rank = pro30_info["pro30_rank"]
            if candidate_price <= 0:
                for price_key in ("current_price", "Last", "Close"):
                    try:
                        v = float(pro30_info.get(price_key) or 0)
                        if v > 0:
                            candidate_price = v
                            break
                    except (TypeError, ValueError):
                        continue
            base_weight = 2.0
            # Rank bonus: r1 gets +1.0, r2 gets +0.95, ... r21+ gets +0.0
            rank_bonus = max(0.0, 1.0 - 0.05 * (rank - 1))
            hybrid_score += base_weight + rank_bonus
            sources.append(f"Pro30(r{rank})")
        elif ticker in pro30_tickers:
            # Fallback if lookup failed but ticker is in set (shouldn't happen)
            hybrid_score += 2.0
            sources.append("Pro30(?)")
        
        # Movers contribution (weight: 0.5 - currently 0% hit rate in backtest)
        if ticker in movers_tickers:
            hybrid_score += 0.5
            sources.append("Movers")
        
        # Overlap bonuses
        hw = config.get("hybrid_weighting", {})
        if ticker in overlap_all_three:
            hybrid_score += float(hw.get("all_three_overlap_bonus", 3.0))
        elif ticker in overlap_primary_pro30:
            hybrid_score += float(hw.get("weekly_pro30_overlap_bonus", 1.5))
        
        weighted_picks.append({
            "ticker": ticker,
            "hybrid_score": hybrid_score,
            "sources": sources,
            "current_price": round(candidate_price, 4) if candidate_price > 0 else 0.0,
            "in_all_three": ticker in overlap_all_three,
            "in_primary_pro30": ticker in overlap_primary_pro30,
        })
    
    # Sort by hybrid score with source-count tie-break (more sources = higher conviction)
    # Avoids alphabetical bias: ties broken by number of confirming sources, then random
    import hashlib
    run_seed = output_date_str  # deterministic per-day but not alphabetical
    weighted_picks.sort(key=lambda x: (
        -x["hybrid_score"],
        -len(x["sources"]),  # more sources = higher conviction
        hashlib.md5(f"{x['ticker']}{run_seed}".encode()).hexdigest(),  # deterministic shuffle
    ))
    
    # Log top weighted picks
    if weighted_picks:
        logger.info(f"\n📊 Weighted Rankings (Pro30 + {primary_label} + Movers):")
        for i, pick in enumerate(weighted_picks[:10], 1):
            sources_str = ", ".join(pick["sources"])
            logger.info(f"  {i}. {pick['ticker']}: {pick['hybrid_score']:.1f} pts [{sources_str}]")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # HYBRID TOP 3: Best picks across ALL models (weighted by hit rate)
    # Pro30 has ~50% hit rate, Weekly ~25%, Movers ~0%
    # ═══════════════════════════════════════════════════════════════════════════════
    hybrid_top3 = []
    for pick in weighted_picks[:3]:
        ticker = pick["ticker"]
        packet_item = primary_packet_lookup.get(str(ticker).upper(), {})
        sources = pick.get("sources", [])
        src_lower = {str(s).lower() for s in sources}
        if "pro30" in src_lower:
            risk_pct, reward_pct = 0.08, 0.18
        elif "movers" in src_lower:
            risk_pct, reward_pct = 0.04, 0.08
        else:
            risk_pct, reward_pct = 0.05, 0.10

        hybrid_entry = {
            "ticker": ticker,
            "hybrid_score": pick["hybrid_score"],
            "sources": sources,
            "rank": weighted_picks.index(pick) + 1,
        }
        
        # Try to get detailed info from weekly or pro30 data
        primary_item = next((x for x in results.get("llm_primary_top5", []) if x.get("ticker") == ticker), None)
        if primary_item:
            packet_price = float(packet_item.get("current_price") or 0)
            pick_price = float(pick.get("current_price") or 0)
            llm_price = float(primary_item.get("current_price") or 0)
            current_price = packet_price if packet_price > 0 else (pick_price if pick_price > 0 else llm_price)

            if packet_price > 0 and llm_price > 0:
                ratio = max(packet_price, llm_price) / max(min(packet_price, llm_price), 1e-9)
                if ratio > 3:
                    logger.warning(
                        "Hybrid export price mismatch for %s: packet=%.2f llm=%.2f. Using packet price.",
                        ticker, packet_price, llm_price,
                    )

            target = primary_item.get("target", {})
            if not isinstance(target, dict):
                target = {}
            target_10 = float(target.get("target_price_for_10pct") or 0)
            if current_price > 0 and (target_10 <= current_price or target_10 > current_price * 2.5):
                target = {"target_price_for_10pct": round(current_price * (1 + reward_pct), 2)}

            stop = primary_item.get("stop", {})
            if not isinstance(stop, dict):
                stop = {}
            stop_3 = float(stop.get("stop_price_for_3pct") or 0)
            if current_price > 0 and (stop_3 <= 0 or stop_3 >= current_price):
                stop = {"stop_price_for_3pct": round(current_price * (1 - risk_pct), 2)}

            # Copy relevant fields from primary packet
            hybrid_entry.update({
                "name": primary_item.get("name", ""),
                "sector": primary_item.get("sector", ""),
                "current_price": current_price,
                "composite_score": primary_item.get("composite_score", 0),
                "confidence": primary_item.get("confidence", "SPECULATIVE"),
                "primary_catalyst": primary_item.get("primary_catalyst", {}),
                "scores": primary_item.get("scores", {}),
                "evidence": primary_item.get("evidence", {}),
                "target": target,
                "stop": stop,
                "risk_factors": primary_item.get("risk_factors", []),
                "data_gaps": primary_item.get("data_gaps", []),
            })
        else:
            # For Pro30/Movers only picks, get basic info from packets if available
            fallback_price = float(pick.get("current_price") or 0)
            hybrid_entry.update({
                "name": pick.get("name") or packet_item.get("name", ""),
                "sector": pick.get("sector") or packet_item.get("sector", ""),
                "current_price": fallback_price,
                "composite_score": pick["hybrid_score"],  # Use hybrid score
                "confidence": "MEDIUM" if "Pro30" in sources else "SPECULATIVE",
                "target": {"target_price_for_10pct": round(fallback_price * (1 + reward_pct), 2)} if fallback_price > 0 else {},
                "stop": {"stop_price_for_3pct": round(fallback_price * (1 - risk_pct), 2)} if fallback_price > 0 else {},
            })
        
        hybrid_top3.append(hybrid_entry)
    
    # Store hybrid top 3 in results for downstream use
    results["hybrid_top3"] = hybrid_top3
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # SANITY CHECKS: Ensure deterministic, non-duplicate results
    # ═══════════════════════════════════════════════════════════════════════════════
    if hybrid_top3:
        top3_tickers = [p["ticker"] for p in hybrid_top3]
        if len(set(top3_tickers)) != len(top3_tickers):
            logger.error(f"CRITICAL: Duplicate tickers in hybrid_top3: {top3_tickers}")
            raise RuntimeError(f"Duplicate tickers in hybrid_top3: {top3_tickers}")
        
        # Log provenance for debugging/reproducibility
        provenance = ", ".join(
            f"{p['ticker']}[{','.join(p.get('sources', ['?']))}]" 
            for p in hybrid_top3
        )
        logger.info(f"\n📋 Hybrid Top3 provenance: {provenance}")
    
    logger.info("\n🎯 HYBRID TOP 3 (Best Across All Models):")
    for item in hybrid_top3:
        sources_str = ", ".join(item.get("sources", []))
        name = item.get("name", "")[:25] or "(Pro30/Movers)"
        logger.info(f"  {item['rank']}. {item['ticker']} ({name}) — Hybrid: {item['hybrid_score']:.1f} pts [{sources_str}]")
    
    # Generate comprehensive HTML report
    try:
        html_file = generate_html_report(output_dir, output_date_str)
        html_path = html_file.resolve() if html_file else None
    except Exception as e:
        logger.warning(f"\n⚠ HTML report generation failed: {e}", exc_info=True)
        html_file = None
        html_path = None
    
    hybrid_file = output_dir / f"hybrid_analysis_{output_date_str}.json"
    hybrid_data = {
        "date": output_date_str,
        "asof_trading_date": asof_date.strftime("%Y-%m-%d") if asof_date else None,
        "summary": {
            "primary_top5_count": len(primary_top5_tickers),
            "weekly_top5_count": len(primary_top5_tickers),
            "pro30_candidates_count": len(pro30_tickers),
            "movers_count": len(movers_tickers),
            "hybrid_top3_count": len(hybrid_top3),
        },
        "overlaps": {
            "all_three": sorted(list(overlap_all_three)),
            "primary_pro30": sorted(list(overlap_primary_pro30 - overlap_all_three)),
            "primary_movers": sorted(list(overlap_primary_movers - overlap_all_three)),
            "pro30_movers": sorted(list(overlap_pro30_movers - overlap_all_three)),
            # Backward-compatible keys
            "weekly_pro30": sorted(list(overlap_primary_pro30 - overlap_all_three)),
            "weekly_movers": sorted(list(overlap_primary_movers - overlap_all_three)),
        },
        "hybrid_top3": hybrid_top3,  # NEW: Best 3 picks across all models
        "weighted_picks": weighted_picks[:20],  # Top 20 by hybrid score
        "weighting_note": f"Primary={primary_label}, Pro30, Movers, overlaps (configurable)",
        "primary_label": primary_label,
        "primary_top5": results.get("llm_primary_top5", []),
        "pro30_tickers": sorted(list(pro30_tickers)),
        "movers_tickers": sorted(list(movers_tickers)),
    }
    
    save_json(hybrid_data, hybrid_file)

    # ═══════════════════════════════════════════════════════════════════════════════
    # EXECUTION WATCHLIST ARTIFACT
    # ═══════════════════════════════════════════════════════════════════════════════
    dp_config = config.get("dragon_pulse", {})
    preflight_cfg = dp_config.get("preflight", {})
    watchlist = {
        "date": output_date_str,
        "regime": primary_regime or "unknown",
        "generated_utc": utc_now().isoformat(),
        "preflight_config": {
            "max_gap_up_pct": preflight_cfg.get("max_gap_up_pct", 3.0),
            "max_gap_down_pct": preflight_cfg.get("max_gap_down_pct", 5.0),
            "min_volume_ratio": preflight_cfg.get("min_volume_ratio", 0.10),
        },
        "picks": [],
    }
    for item in hybrid_top3:
        watchlist["picks"].append({
            "ticker": item["ticker"],
            "name": item.get("name", ""),
            "sources": item.get("sources", []),
            "entry_price": item.get("current_price", 0),
            "target_price": (item.get("target") or {}).get("target_price_for_10pct", 0),
            "stop_price": (item.get("stop") or {}).get("stop_price_for_3pct", 0),
            "hybrid_score": item.get("hybrid_score", 0),
            "confidence": item.get("confidence", ""),
        })
    watchlist_file = output_dir / f"execution_watchlist_{output_date_str}.json"
    save_json(watchlist, watchlist_file)
    logger.info(f"  ✓ Execution watchlist saved to {watchlist_file}")

    # ═══════════════════════════════════════════════════════════════════════════════
    # CONVICTION RANKER (Self-Improving Model)
    # ═══════════════════════════════════════════════════════════════════════════════
    conviction_result = None
    conviction_picks = []
    try:
        from src.pipelines.conviction_ranker import rank_candidates, format_conviction_picks
        from src.core.adaptive_scorer import get_adaptive_scorer
        
        logger.info("\n[5.5/7] Conviction Ranker (Adaptive Model)...")
        
        scorer = get_adaptive_scorer()
        model_info = scorer.get_model_info()
        
        # Run conviction ranking
        conviction_result = rank_candidates(
            weekly_picks=results.get("llm_primary_top5", []),
            pro30_picks=list(pro30_tickers),
            movers_picks=list(movers_tickers),
            scorer=scorer,
            max_picks=3,
            min_confidence="MEDIUM",
        )
        
        # Display top conviction picks
        if conviction_result.get("top_picks"):
            formatted = format_conviction_picks(conviction_result)
            logger.info("\n" + formatted)
            
            # Store conviction result in results
            results["conviction_picks"] = conviction_result
            
            # Save conviction results
            conviction_file = output_dir / f"conviction_picks_{output_date_str}.json"
            save_json(conviction_result, conviction_file)
            logger.info(f"  ✓ Conviction picks saved to {conviction_file}")
            conviction_picks = conviction_result.get("top_picks", [])
        else:
            logger.info("  ℹ No picks meet conviction threshold today")
        
        # Check if model needs retraining
        if scorer.should_retrain():
            logger.info("  ℹ Model has new outcomes - consider running 'python main.py learn'")
        
    except ImportError as e:
        logger.debug(f"Conviction ranker not available: {e}")
    except Exception as e:
        logger.warning(f"  ⚠ Conviction ranking failed: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # OBSERVABILITY METRICS: Track system health and market conditions
    # Passive observation only - no impact on picks or scoring
    # ═══════════════════════════════════════════════════════════════════════════════
    try:
        logger.info("\n[5.6/7] Generating Observability Metrics...")
        
        # 1. Technical Score Distribution (by source)
        score_distribution = {
            "pro30": {"scores": [], "count": 0, "count_gte_6": 0, "count_gte_8": 0},
            "weekly": {"scores": [], "count": 0, "count_gte_6": 0, "count_gte_8": 0},
        }
        
        # Collect Pro30 scores
        for ticker, info in pro30_lookup.items():
            score = info.get("Score")
            if score is not None:
                score_distribution["pro30"]["scores"].append(float(score))
                score_distribution["pro30"]["count"] += 1
                if score >= 6:
                    score_distribution["pro30"]["count_gte_6"] += 1
                if score >= 8:
                    score_distribution["pro30"]["count_gte_8"] += 1
        
        # Collect Weekly scores from packets
        for pick in results.get("llm_primary_top5", []):
            tech_score = pick.get("scores", {}).get("technical") if isinstance(pick.get("scores"), dict) else None
            if tech_score is None:
                tech_score = pick.get("technical_score") or pick.get("composite_score")
            if tech_score is not None:
                score_distribution["weekly"]["scores"].append(float(tech_score))
                score_distribution["weekly"]["count"] += 1
                if tech_score >= 6:
                    score_distribution["weekly"]["count_gte_6"] += 1
                if tech_score >= 8:
                    score_distribution["weekly"]["count_gte_8"] += 1
        
        # Compute percentiles for each source
        import numpy as np
        for src in ["pro30", "weekly"]:
            scores = score_distribution[src]["scores"]
            if scores:
                score_distribution[src].update({
                    "min": round(float(np.min(scores)), 2),
                    "p25": round(float(np.percentile(scores, 25)), 2),
                    "median": round(float(np.median(scores)), 2),
                    "p75": round(float(np.percentile(scores, 75)), 2),
                    "max": round(float(np.max(scores)), 2),
                })
            # Remove raw scores array to keep file small
            del score_distribution[src]["scores"]
        
        # 2. Overlap Statistics
        overlap_stats = {
            "weekly_only": len(primary_top5_tickers - pro30_tickers - movers_tickers),
            "pro30_only": len(pro30_tickers - primary_top5_tickers - movers_tickers),
            "movers_only": len(movers_tickers - primary_top5_tickers - pro30_tickers),
            "weekly_pro30": len(overlap_primary_pro30 - overlap_all_three),
            "weekly_movers": len(overlap_primary_movers - overlap_all_three),
            "pro30_movers": len(overlap_pro30_movers - overlap_all_three),
            "all_three": len(overlap_all_three),
            "any_overlap_count": len(overlap_primary_pro30 | overlap_primary_movers | overlap_pro30_movers | overlap_all_three),
            # Overlap potential (was overlap possible?)
            "overlap_potential": {
                "weekly_candidates": len(primary_top5_tickers),
                "pro30_candidates": len(pro30_tickers),
                "movers_candidates": len(movers_tickers),
                "intersection_possible": len(primary_top5_tickers) > 0 and len(pro30_tickers) > 0,
            },
        }
        
        # 3. Confidence Distribution
        confidence_distribution = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        near_misses = []
        
        if conviction_result and conviction_result.get("all_candidates"):
            for candidate in conviction_result["all_candidates"]:
                conf = candidate.get("confidence", "LOW")
                if conf in confidence_distribution:
                    confidence_distribution[conf] += 1
                
                # Track near-misses (LOW confidence with highest scores)
                if conf == "LOW" and len(near_misses) < 5:
                    near_misses.append({
                        "ticker": candidate.get("ticker"),
                        "conviction_score": candidate.get("conviction_score"),
                        "sources": candidate.get("sources", []),
                        "missing": "needs overlap or tech>=6" if candidate.get("overlap_count", 1) < 2 else "needs higher tech score",
                    })
        
        # Compute confidence rates
        total_candidates = sum(confidence_distribution.values())
        confidence_rates = {}
        for level, count in confidence_distribution.items():
            confidence_rates[f"{level.lower()}_pct"] = round(count / max(1, total_candidates) * 100, 1)
        
        # 4. Model Maturity
        model_maturity = {
            "observations": 0,
            "min_required": 50,
            "pct_complete": 0.0,
            "observations_remaining": 50,
        }
        
        if conviction_result and conviction_result.get("model_info"):
            model_info = conviction_result["model_info"]
            obs = model_info.get("observations", 0)
            min_obs = model_info.get("min_observations", 50)
            model_maturity.update({
                "observations": obs,
                "min_required": min_obs,
                "pct_complete": round(min(100, obs / max(1, min_obs) * 100), 1),
                "observations_remaining": max(0, min_obs - obs),
                "version": model_info.get("version", 1),
                "last_trained": model_info.get("last_trained"),
            })
        
        # 5. Build and save observability file
        observability_data = {
            "date": output_date_str,
            "asof_trading_date": asof_date.strftime("%Y-%m-%d") if asof_date else None,
            "technical_score_distribution": score_distribution,
            "overlap_stats": overlap_stats,
            "confidence_distribution": confidence_distribution,
            "confidence_rates": confidence_rates,
            "near_misses": near_misses[:5],  # Cap at 5
            "model_maturity": model_maturity,
            "regime": primary_regime or "unknown",
        }
        
        observability_file = output_dir / f"observability_{output_date_str}.json"
        save_json(observability_data, observability_file)
        logger.info(f"  ✓ Observability metrics saved to {observability_file.name}")
        
        # Log summary
        logger.info(f"    Score dist (Pro30): median={score_distribution['pro30'].get('median', 'N/A')}, ≥6={score_distribution['pro30']['count_gte_6']}/{score_distribution['pro30']['count']}")
        logger.info(f"    Overlap: any={overlap_stats['any_overlap_count']}, all_three={overlap_stats['all_three']}")
        logger.info(f"    Confidence: HIGH={confidence_distribution['HIGH']}, MED={confidence_distribution['MEDIUM']}, LOW={confidence_distribution['LOW']}")
        logger.info(f"    Model maturity: {model_maturity['pct_complete']:.0f}% ({model_maturity['observations']}/{model_maturity['min_required']})")
        
    except Exception as e:
        logger.warning(f"  ⚠ Observability metrics generation failed: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # PHASE-5 LEARNING: Write scan-time features (append-only, idempotent)
    # R1-safe: only writes if not a retry attempt
    # ═══════════════════════════════════════════════════════════════════════════════
    try:
        from src.learning import build_phase5_row, get_phase5_store
        from src.core.retry_guard import is_retry_attempt
        
        # Only write on first attempt (R1 invariant)
        if not is_retry_attempt():
            store = get_phase5_store()
            phase5_rows = []
            
            # Get current weights snapshot for frozen context
            hw = config.get("hybrid_weighting", {})
            weights_snapshot = {
                "swing_weight": float(hw.get("swing_weight", 1.2)),
                "weekly_weight": float(hw.get("weekly_weight", 1.0)),
                "pro30_weight": 2.0,  # Base Pro30 weight
                "movers_weight": 0.5,
                "all_three_overlap_bonus": float(hw.get("all_three_overlap_bonus", 3.0)),
                "primary_pro30_overlap_bonus": float(hw.get("weekly_pro30_overlap_bonus", 1.5)),
            }
            
            # Build set of conviction pick tickers
            conviction_tickers = set(p.get("ticker") for p in conviction_picks if p.get("ticker"))
            hybrid_top3_tickers = set(p.get("ticker") for p in hybrid_top3 if p.get("ticker"))
            
            # Determine which candidates to track (Top 20 weighted picks for learning)
            candidates_to_track = weighted_picks[:20]
            
            for idx, candidate in enumerate(candidates_to_track):
                ticker = candidate.get("ticker")
                if not ticker:
                    continue
                
                # Get Pro30 info if available
                pro30_info = pro30_lookup.get(ticker, {})
                
                # Get primary (Swing/Weekly) rank
                primary_rank = None
                for item in results.get("llm_primary_top5", []):
                    if item.get("ticker") == ticker:
                        primary_rank = item.get("rank")
                        break
                
                # Get technical data from Pro30 if available
                rsi = pro30_info.get("RSI14")
                atr_pct = pro30_info.get("ATR%")
                adv20 = pro30_info.get("$ADV20")
                dist_52w = pro30_info.get("Dist_to_52W_High%")
                above_ma20 = pro30_info.get("Above_MA20", False)
                above_ma50 = pro30_info.get("Above_MA50", False)
                ret_20d = pro30_info.get("Ret20d%")
                
                # Build Phase5Row
                row = build_phase5_row(
                    scan_date=output_date_str,
                    ticker=ticker,
                    primary_strategy=primary_label,
                    regime=primary_regime or "bull",  # Default to bull if unknown
                    # Signal flags
                    in_swing_top5=(primary_label == "Swing" and ticker in primary_top5_tickers),
                    swing_rank=primary_rank if primary_label == "Swing" else None,
                    in_weekly_top5=(primary_label == "Weekly" and ticker in primary_top5_tickers),
                    weekly_rank=primary_rank if primary_label == "Weekly" else None,
                    in_pro30=(ticker in pro30_lookup),
                    pro30_rank=pro30_info.get("pro30_rank"),
                    pro30_score=pro30_info.get("Score"),
                    in_movers=(ticker in movers_tickers),
                    in_confluence=(ticker in overlap_all_three or ticker in overlap_primary_pro30),
                    confluence_score=len([s for s in candidate.get("sources", []) if s]),
                    overlap_primary_pro30=(ticker in overlap_primary_pro30),
                    overlap_all_three=(ticker in overlap_all_three),
                    # Hybrid context
                    hybrid_score=candidate.get("hybrid_score", 0.0),
                    hybrid_rank=idx + 1,
                    hybrid_sources=candidate.get("sources", []),
                    weights_snapshot=weights_snapshot,
                    in_hybrid_top3=(ticker in hybrid_top3_tickers),
                    in_conviction_picks=(ticker in conviction_tickers),
                    # Technical buckets
                    rsi=rsi,
                    atr_pct=atr_pct,
                    adv20=adv20,
                    distance_52w_pct=dist_52w,
                    above_ma20=bool(above_ma20),
                    above_ma50=bool(above_ma50),
                    ret_20d=ret_20d,
                    # Metadata
                    run_id=f"{output_date_str}_{primary_label}",
                )
                phase5_rows.append(row)
            
            # Write rows (idempotent - skips existing keys)
            if phase5_rows:
                result = store.write_rows(phase5_rows, output_date_str)
                if result["written"] > 0:
                    logger.info(f"  ✓ Phase-5 learning: {result['written']} rows written")
                if result["skipped"] > 0:
                    logger.debug(f"  ℹ Phase-5: {result['skipped']} rows skipped (already exist)")
        else:
            logger.debug("  ℹ Phase-5 write skipped (retry attempt)")
    
    except ImportError as e:
        logger.debug(f"Phase-5 learning not available: {e}")
    except Exception as e:
        logger.warning(f"  ⚠ Phase-5 learning write failed: {e}")

    # ── Phase 6: Adaptive Model Recommendations ──
    try:
        phase6_cfg = config.get("phase6", {})
        if phase6_cfg.get("enabled", False) and phase6_cfg.get("auto_analyze", False):
            from src.overlays.phase6_adaptive import run_phase6_analysis
            phase6_result = run_phase6_analysis(
                outcomes_data=None,
                context={"regime": primary_label, "date": output_date_str},
                cfg=phase6_cfg,
            )
            if phase6_result:
                logger.info(f"  ✓ Phase-6 recommendations generated")
    except ImportError as e:
        logger.debug(f"Phase-6 analysis not available: {e}")
    except Exception as e:
        logger.warning(f"  ⚠ Phase-6 analysis failed: {e}")

    # Open browser if requested
    if hasattr(args, 'open') and args.open and html_path:
        _open_browser(html_path)
    
    # Append to model history log
    try:
        _append_model_history(
            date_str=output_date_str,
            weekly_top5=primary_top5_tickers,
            pro30_tickers=pro30_tickers,
            movers_tickers=movers_tickers,
            overlaps=hybrid_data["overlaps"],
            weekly_top5_data=hybrid_data.get("primary_top5", []),
        )
    except Exception as e:
        logger.debug(f"Could not append to model history: {e}")
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # POST-SCAN ANALYSIS (Auto-integrated)
    # ═══════════════════════════════════════════════════════════════════════════════
    
    # Step 6: Quick Model Validation (run BEFORE alerts to include in notification)
    logger.info("\n[6/7] Quick Model Validation...")
    model_health_data = None
    try:
        from src.commands.validate import run_full_backtest, generate_scorecard
        from datetime import timedelta
        
        cutoff = (utc_now() - timedelta(days=30)).strftime("%Y-%m-%d")
        val_df = run_full_backtest(
            start_date=cutoff,
            holding_periods=[5, 7],
            hit_thresholds=[5.0, 7.0, 10.0],
        )
        
        if not val_df.empty:
            scorecard = generate_scorecard(val_df, primary_period=7, primary_threshold=7)
            kpi = scorecard.get("primary_kpi", {})
            health = scorecard.get("model_health", "Unknown")
            
            logger.info(f"  ✓ Model Health: {health}")
            logger.info(f"    Hit Rate (+7%): {(kpi.get('hit_rate') or 0) * 100:.1f}%")
            logger.info(f"    Win Rate: {(kpi.get('win_rate') or 0) * 100:.1f}%")
            
            # Strategy ranking
            strategy_data = []
            for s in scorecard.get("strategy_ranking", [])[:3]:
                hr = (s.get('hit_rate') or 0) * 100
                logger.info(f"    {s['strategy']}: {hr:.1f}% hit rate (n={s['n']})")
                strategy_data.append({
                    "name": s.get('strategy'),
                    "hit_rate": s.get('hit_rate') or 0,
                    "n": s.get('n', 0)
                })
            
            # Build model health data for alerts
            model_health_data = {
                "status": health,
                "hit_rate": kpi.get('hit_rate'),
                "win_rate": kpi.get('win_rate'),
                "strategies": strategy_data,
                "regime_breakdown": scorecard.get("regime_breakdown", []),
            }
        else:
            logger.info("  ⚠ Insufficient historical data for validation")
    except Exception as e:
        logger.warning(f"  ⚠ Validation skipped: {e}")
    
    # Step 7: Confluence Analysis
    logger.info("\n[7/7] Confluence Analysis (Multi-Signal Alignment)...")
    confluence_picks = []
    try:
        from src.pipelines.confluence import run_confluence_scan, save_confluence_results
        
        confluence_picks = run_confluence_scan(
            date=output_date_str,
            min_signals=2,
            include_options=False,  # Skip for speed
            include_sector=False,   # Skip for speed
        )
        
        if confluence_picks:
            logger.info(f"  ✓ Found {len(confluence_picks)} high-conviction picks (2+ signals)")
            for c in confluence_picks[:5]:
                sources = ", ".join(s.source for s in c.signals)
                logger.info(f"    🎯 {c.ticker}: {c.confluence_score}/10 [{sources}]")
            
            save_confluence_results(confluence_picks, date=output_date_str)
        else:
            logger.info("  ℹ No confluence picks today (no multi-signal alignment)")
    except Exception as e:
        logger.warning(f"  ⚠ Confluence scan skipped: {e}")
    
    hybrid_sources = []
    for pick in results.get("hybrid_top3", []):
        hybrid_sources.extend(pick.get("sources", []) or [])
    
    ctx = build_decision_context(
        regime=primary_regime or "unknown",
        weekly_count=len(primary_top5_tickers),
        pro30_count=len(pro30_tickers),
        confluence_count=len(confluence_picks),
        conviction_count=len(conviction_picks),
        hybrid_sources=hybrid_sources,
    )
    
    decision = build_decision_summary_v2(ctx)
    logger.info(decision.render())
    
    # Auto-track positions from this scan
    # Primary: Hybrid Top 3 (best across all models by weighted scoring)
    position_alerts_summary = None
    try:
        from src.features.positions.tracker import PositionTracker, send_position_alerts
        
        tracker = PositionTracker()
        
        # Use Hybrid Top 3 as primary picks to track (best performers by hit rate)
        # Tag them with source "hybrid_top3" for tracking
        hybrid_top3_data = results.get("hybrid_top3", [])
        for pick in hybrid_top3_data:
            pick["source_type"] = "hybrid_top3"  # Mark as hybrid pick
        
        # Also track weekly picks separately (for backward compatibility)
        weekly_picks_data = results.get("llm_primary_top5", [])
        
        # Don't separately track Pro30/Movers - they're already in hybrid_top3 if ranked high enough
        # This prevents duplicate tracking
        pro30_list = []  # Skip - covered by hybrid_top3
        movers_list = []  # Skip - covered by hybrid_top3
        
        # Add conviction scores to picks if available
        if conviction_result and conviction_result.get("all_candidates"):
            conviction_map = {
                c["ticker"]: c for c in conviction_result["all_candidates"]
            }
            for pick in hybrid_top3_data:
                ticker = pick.get("ticker")
                if ticker in conviction_map:
                    pick["conviction_score"] = conviction_map[ticker].get("conviction_score")
                    pick["confidence"] = conviction_map[ticker].get("confidence")
            for pick in weekly_picks_data:
                ticker = pick.get("ticker")
                if ticker in conviction_map:
                    pick["conviction_score"] = conviction_map[ticker].get("conviction_score")
                    pick["confidence"] = conviction_map[ticker].get("confidence")
        
        # Track Hybrid Top 3 as the primary picks
        if decision.allow_new_positions:
            added = tracker.add_positions_from_scan(
                scan_date=output_date_str,
                weekly_picks=hybrid_top3_data,  # Use Hybrid Top 3 as primary
                pro30_picks=pro30_list,
                movers_picks=movers_list,
                config=config,
            )
        else:
            logger.info("🚫 New positions suppressed due to defensive posture")
            added = 0
        
        if added > 0:
            logger.info(f"  📊 Position tracker: Added {added} new positions")
        
        # Monitor existing positions for drawdown alerts
        alerts = tracker.monitor_positions()
        if alerts:
            logger.info(f"  ⚠️ Position alerts: {len(alerts)} alerts generated")
            for alert in alerts[:3]:
                logger.info(f"    {alert['message']}")
            position_alerts_summary = {
                "count": len(alerts),
                "sample": [a.get("message", "") for a in alerts[:5]],
                "high": sum(1 for a in alerts if a.get("severity") == "high"),
                "warning": sum(1 for a in alerts if a.get("severity") == "warning"),
                "info": sum(1 for a in alerts if a.get("severity") == "info"),
            }
            
            # Send alerts if enabled
            if ALERTS_AVAILABLE:
                try:
                    alerts_cfg = config.get("alerts", {})
                    single_message_only = bool(alerts_cfg.get("single_message_only", True))
                    if alerts_cfg.get("enabled") and alerts_cfg.get("position_alerts_enabled", True) and not single_message_only:
                        send_position_alerts(alerts, config)
                    elif not alerts_cfg.get("position_alerts_enabled", True):
                        logger.debug("Position alerts disabled in config (position_alerts_enabled: false)")
                    elif single_message_only:
                        logger.debug("Position alerts suppressed (single_message_only: true)")
                except Exception as e:
                    logger.debug(f"Position alert sending failed: {e}")
        
        tracker.save()
        
        phase5_cfg = cfg.phase5
        phase5_enabled = phase5_cfg.get("enabled", False) if isinstance(phase5_cfg, dict) else bool(getattr(phase5_cfg, "enabled", False))
        if phase5_enabled:
            # Retries re-run computation but MUST NOT emit side effects
            # Guard against double-counting outcomes on retry attempts
            from src.core.retry_guard import is_retry_attempt, log_retry_suppression
            if is_retry_attempt():
                log_retry_suppression("Phase 5 outcome persistence")
                phase5_enabled = False  # Skip persistence, continue with rest of flow
        
        if phase5_enabled:
            try:
                from src.core.outcome_db import get_outcome_db
                db = get_outcome_db()
                outcomes_df = db.get_training_data()
                if not outcomes_df.empty and "closed_at" in outcomes_df.columns:
                    outcomes_df["closed_at"] = outcomes_df["closed_at"].astype(str)
                    outcomes_df = outcomes_df[outcomes_df["closed_at"].str.startswith(output_date_str)]
                outcomes = outcomes_df.to_dict(orient="records") if not outcomes_df.empty else []
                
                from src.overlays.phase5_learning import record_phase5_learning
                from src.learning.phase5_store import persist_learning
                from src.learning.phase5_analyzer import summarize_learning
                
                phase5_records = record_phase5_learning(outcomes, results, {"regime": primary_regime or "unknown"}, phase5_cfg)
                
                if phase5_cfg.get("persist", False):
                    persist_learning(phase5_records)
                
                if phase5_cfg.get("summarize", False):
                    summary = summarize_learning(phase5_records)
                    logger.info(f"📘 Phase 5 Learning Summary: {summary}")
            except Exception as e:
                logger.warning(f"Phase5 learning skipped: {e}")
    except Exception as e:
        logger.debug(f"Position tracking skipped: {e}")
    
    # Build primary summary data for CLI/alerts
    primary_label = "Swing" if results.get("llm_primary_source") == "swing" else "Weekly"
    primary_top5 = results.get("llm_primary_top5", [])
    primary_candidates_count = 0
    if primary_label == "Swing" and results.get("swing") and results["swing"].get("candidates_csv"):
        if results["swing"].get("candidates_count") is not None:
            primary_candidates_count = int(results["swing"].get("candidates_count") or 0)
        else:
            try:
                swing_df = pd.read_csv(results["swing"]["candidates_csv"])
                primary_candidates_count = int(len(swing_df))
            except Exception:
                primary_candidates_count = 0
    elif primary_label == "Weekly" and results.get("weekly") and results["weekly"].get("candidates_csv"):
        try:
            weekly_df = pd.read_csv(results["weekly"]["candidates_csv"])
            primary_candidates_count = int(len(weekly_df))
        except Exception:
            primary_candidates_count = 0

    # Send single consolidated alert after all steps (if enabled)
    if ALERTS_AVAILABLE:
        try:
            alerts_raw = config.get("alerts", {})
            alert_kwargs = {
                "enabled": alerts_raw.get("enabled", False),
                "channels": alerts_raw.get("channels", []),
                "slack_webhook": alerts_raw.get("slack_webhook"),
                "discord_webhook": alerts_raw.get("discord_webhook"),
                "alert_log_path": alerts_raw.get("alert_log_path", "outputs/alerts.log"),
            }
            email_cfg = alerts_raw.get("email", {})
            if email_cfg:
                alert_kwargs["smtp_host"] = email_cfg.get("smtp_host", "smtp.gmail.com")
                alert_kwargs["smtp_port"] = email_cfg.get("smtp_port", 587)
                alert_kwargs["from_address"] = email_cfg.get("from_address")
                alert_kwargs["to_addresses"] = email_cfg.get("to_addresses", [])
            triggers_cfg = alerts_raw.get("triggers", {})
            if triggers_cfg:
                alert_kwargs["trigger_all_three_overlap"] = triggers_cfg.get("all_three_overlap", True)
                alert_kwargs["trigger_weekly_pro30_overlap"] = triggers_cfg.get("weekly_pro30_overlap", True)
                alert_kwargs["trigger_high_composite_score"] = triggers_cfg.get("high_composite_score", 7.0)
            
            alert_config = AlertConfig(**alert_kwargs)
            if alert_config.enabled:
                send_run_summary_alert(
                    date_str=output_date_str,
                    weekly_count=len(primary_top5_tickers),
                    pro30_count=len(pro30_tickers),
                    movers_count=len(movers_tickers),
                    overlaps=hybrid_data["overlaps"],
                    config=alert_config,
                    weekly_tickers=sorted(list(primary_top5_tickers)),
                    pro30_tickers=sorted(list(pro30_tickers)),
                    movers_tickers=sorted(list(movers_tickers)),
                    model_health=model_health_data,
                    weekly_top5_data=primary_top5,
                    hybrid_top3=results.get("hybrid_top3", []),
                    primary_label=primary_label,
                    primary_candidates_count=primary_candidates_count,
                    position_alerts=position_alerts_summary,
                    regime=primary_regime,
                )
                logger.info("Alerts sent successfully")
        except Exception as e:
            logger.warning(f"Failed to send alerts: {e}")

    # CLI Summary (comprehensive)
    summary_lines = [
        f"Run Date: {output_date_str} | As-of: {asof_date.strftime('%Y-%m-%d') if asof_date else 'N/A'}",
        f"Primary Strategy: {primary_label} | Candidates: {primary_candidates_count} | Top5: {len(primary_top5_tickers)}",
        f"Secondary Pools: Pro30={len(pro30_tickers)} | Movers={len(movers_tickers)}",
        f"Overlaps: AllThree={len(overlap_all_three)} | {primary_label}+Pro30={len(overlap_primary_pro30)} | {primary_label}+Movers={len(overlap_primary_movers)}",
    ]
    if primary_top5:
        summary_lines.append("Primary Top Picks:")
        for item in primary_top5[:5]:
            ticker = item.get("ticker", "?")
            score = item.get("composite_score")
            if score is None:
                score = item.get("swing_score", 0) or 0
            try:
                score = float(score)
            except Exception:
                score = 0.0
            verdict = item.get("verdict") or item.get("confidence", "")
            summary_lines.append(f"  - {ticker}: score={score:.2f} {verdict}".rstrip())
    if model_health_data:
        summary_lines.append(f"Model Health: {model_health_data.get('status', 'Unknown')}")
        hit_rate = model_health_data.get("hit_rate")
        win_rate = model_health_data.get("win_rate")
        if hit_rate is not None and win_rate is not None:
            summary_lines.append(f"  Hit={hit_rate * 100:.1f}% | Win={win_rate * 100:.1f}%")
    if position_alerts_summary:
        summary_lines.append(f"Position Alerts: {position_alerts_summary.get('count', 0)} (high={position_alerts_summary.get('high', 0)}, warning={position_alerts_summary.get('warning', 0)})")
    if html_path:
        summary_lines.append(f"Report: {html_path}")

    logger.info("\n" + "=" * 60)
    logger.info("RUN SUMMARY")
    logger.info("=" * 60)
    for line in summary_lines:
        logger.info(line)
    
    # ═══════════════════════════════════════════════════════════════════════════════
    # RUN SUMMARY ARTIFACT — structured diagnostics for CI observability
    # ═══════════════════════════════════════════════════════════════════════════════
    try:
        _regime_status = "unknown"
        _regime_benchmark = get_config_value(config, "regime_gate", "spy_symbol", default="")
        if results.get("weekly") and isinstance(results["weekly"], dict):
            _ri = results["weekly"].get("regime_info", {})
            if _ri:
                _regime_status = "ok" if _ri.get("spy_above_ma") else "below_ma"
            elif primary_regime:
                _regime_status = str(primary_regime)

        _final_picks = len(results.get("hybrid_top3", []))

        run_summary = {
            "date": output_date_str,
            "market_region": market_region,
            "universe_size": universe_size,
            "stages": _stage_timings,
            "regime": {"status": _regime_status, "benchmark": _regime_benchmark},
            "final_picks": _final_picks,
            "total_duration_s": round(_time.time() - _run_t0),
        }
        _run_summary_path = output_dir / f"run_summary_{output_date_str}.json"
        save_json(run_summary, _run_summary_path)
        logger.info(f"  ✓ Run summary saved: {_run_summary_path.name}")
    except Exception as e:
        logger.warning(f"  ⚠ Run summary save failed: {e}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ ALL ANALYSIS COMPLETE")
    logger.info("=" * 60)

    return 0


def _append_model_history(
    date_str: str,
    weekly_top5: list,
    pro30_tickers: list,
    movers_tickers: list,
    overlaps: dict,
    weekly_top5_data: list,
) -> None:
    """Append run summary to model_history.md for AI analysis."""
    history_path = Path("outputs/model_history.md")
    
    # Build the entry
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M UTC")
    
    lines = [
        f"\n### {date_str} (run: {timestamp})",
        f"",
        f"**Picks:**",
        f"- Primary Top 5: {', '.join(weekly_top5) if weekly_top5 else '(none)'}",
        f"- Pro30: {', '.join(pro30_tickers[:5]) if pro30_tickers else '(none)'}" + (f" (+{len(pro30_tickers)-5} more)" if len(pro30_tickers) > 5 else ""),
        f"- Movers: {', '.join(movers_tickers) if movers_tickers else '(none)'}",
        f"",
        f"**Overlaps:**",
        f"- All Three: {', '.join(overlaps.get('all_three', [])) or '(none)'}",
        f"- Primary+Pro30: {', '.join(overlaps.get('primary_pro30', overlaps.get('weekly_pro30', []))) or '(none)'}",
        f"",
    ]
    
    # Add top pick details
    if weekly_top5_data:
        lines.append("**Top Pick Details:**")
        for item in weekly_top5_data[:3]:
            ticker = item.get("ticker", "?")
            score = item.get("composite_score", 0)
            catalyst = item.get("primary_catalyst", {}).get("title", "N/A")[:60]
            confidence = item.get("confidence", "?")
            lines.append(f"- {ticker}: score={score:.2f}, confidence={confidence}, catalyst=\"{catalyst}\"")
        lines.append("")
    
    lines.append("---")
    lines.append("")
    
    # Append to file
    with open(history_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
