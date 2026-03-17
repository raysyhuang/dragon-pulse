from __future__ import annotations

import json
import re
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import yfinance as yf

# PR4: Execution model and metrics (legacy — graceful fallback)
try:
    from src.backtest.execution import ExecutionModel, entry_price
    from src.backtest.metrics import compute_path_metrics, compute_expectancy
except ImportError:
    ExecutionModel = None  # type: ignore[assignment,misc]
    entry_price = None  # type: ignore[assignment]
    compute_path_metrics = None  # type: ignore[assignment]
    compute_expectancy = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _dedup_keep_order(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in items:
        t = str(x).strip().upper()
        if not t or t in seen:
            continue
        out.append(t)
        seen.add(t)
    return out


def _safe_read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


@dataclass(frozen=True)
class DatePicks:
    date_str: str
    weekly_top5: list[str]
    pro30: list[str]
    movers: list[str]
    combined: list[str]

    # lightweight traceability
    sources: dict[str, str]


def iter_output_dates(outputs_root: str | Path = "outputs") -> list[str]:
    """
    Return available output dates (YYYY-MM-DD) under the outputs root.
    """
    root = Path(outputs_root)
    if not root.exists():
        return []

    dates: list[str] = []
    for p in root.iterdir():
        if p.is_dir() and _DATE_RE.match(p.name):
            dates.append(p.name)
    return sorted(dates)


def load_picks_for_date(date_str: str, outputs_root: str | Path = "outputs") -> DatePicks:
    """
    Load weekly top5 + pro30 + movers tickers for a given date from the run folder.

    Ordering:
    - weekly_top5 keeps rank order (as stored)
    - pro30 keeps CSV order (as stored)
    - movers keeps JSON order (as stored)
    - combined = weekly_top5 + pro30 + movers (de-duped, preserves first occurrence)
    """
    if not _DATE_RE.match(date_str):
        raise ValueError(f"date_str must be YYYY-MM-DD, got: {date_str!r}")

    run_dir = Path(outputs_root) / date_str
    sources: dict[str, str] = {}

    # --- Weekly Top 5 ---
    weekly: list[str] = []
    top5_json_path = run_dir / f"weekly_scanner_top5_{date_str}.json"
    if top5_json_path.exists():
        obj = _safe_read_json(top5_json_path)
        top5_list = obj.get("top5") or []
        if isinstance(top5_list, list):
            weekly = _dedup_keep_order([x.get("ticker") for x in top5_list if isinstance(x, dict) and x.get("ticker")])
            sources["weekly_top5"] = str(top5_json_path)

    if not weekly:
        # fallback: run_card has top5_tickers
        run_card_path = run_dir / f"run_card_{date_str}.json"
        if run_card_path.exists():
            obj = _safe_read_json(run_card_path)
            xs = obj.get("top5_tickers") or []
            if isinstance(xs, list):
                weekly = _dedup_keep_order(xs)
                sources["weekly_top5"] = str(run_card_path)

    if not weekly:
        # fallback: hybrid_analysis weekly_top5 list-of-dicts
        hybrid_path = run_dir / f"hybrid_analysis_{date_str}.json"
        obj = _safe_read_json(hybrid_path)
        xs = obj.get("weekly_top5") or []
        if isinstance(xs, list):
            weekly = _dedup_keep_order([x.get("ticker") for x in xs if isinstance(x, dict) and x.get("ticker")])
            if weekly:
                sources["weekly_top5"] = str(hybrid_path)

    if not weekly:
        # last fallback: top5_{date}.csv generated artifact
        top5_csv_path = run_dir / f"top5_{date_str}.csv"
        df = _safe_read_csv(top5_csv_path)
        if not df.empty and "ticker" in df.columns:
            weekly = _dedup_keep_order(df["ticker"].dropna().tolist())
            if weekly:
                sources["weekly_top5"] = str(top5_csv_path)

    # --- PRO30 ---
    pro30: list[str] = []
    pro30_paths = [
        run_dir / f"30d_momentum_candidates_{date_str}.csv",
        run_dir / f"30d_breakout_candidates_{date_str}.csv",
        run_dir / f"30d_reversal_candidates_{date_str}.csv",
    ]
    for p in pro30_paths:
        df = _safe_read_csv(p)
        if not df.empty and "Ticker" in df.columns:
            pro30 += df["Ticker"].dropna().tolist()
            sources.setdefault("pro30", str(p))
    pro30 = _dedup_keep_order(pro30)

    if not pro30:
        # fallback: hybrid_analysis pro30_tickers list
        hybrid_path = run_dir / f"hybrid_analysis_{date_str}.json"
        obj = _safe_read_json(hybrid_path)
        xs = obj.get("pro30_tickers") or []
        if isinstance(xs, list):
            pro30 = _dedup_keep_order(xs)
            if pro30:
                sources["pro30"] = str(hybrid_path)

    # --- Movers ---
    movers: list[str] = []
    hybrid_path = run_dir / f"hybrid_analysis_{date_str}.json"
    obj = _safe_read_json(hybrid_path)
    xs = obj.get("movers_tickers") or []
    if isinstance(xs, list):
        movers = _dedup_keep_order(xs)
        if movers:
            sources["movers"] = str(hybrid_path)
    if "movers" not in sources:
        sources["movers"] = str(hybrid_path)

    combined = _dedup_keep_order(list(weekly) + list(pro30) + list(movers))
    sources.setdefault("weekly_top5", str(top5_json_path))
    sources.setdefault("pro30", str(pro30_paths[0]))

    return DatePicks(
        date_str=date_str,
        weekly_top5=weekly,
        pro30=pro30,
        movers=movers,
        combined=combined,
        sources=sources,
    )


def _to_dt(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


@dataclass(frozen=True)
class PricePanel:
    """
    OHLC-like panel (Close + High) for multiple tickers, indexed by date.
    """

    close: pd.DataFrame
    high: pd.DataFrame


def download_prices_once(
    tickers: list[str],
    start_date: str,
    end_date: str,
    *,
    auto_adjust: bool = False,
    threads: bool = True,
    use_cache: bool = True,
) -> PricePanel:
    """
    Download Close + High for many tickers in one call.
    
    Uses price cache for reproducible backtests:
    - Historical data is cached for 30 days (immutable)
    - Subsequent runs are instant (no API calls)

    Notes:
    - `end_date` is inclusive in this function (we add +1 day for yfinance's `end`)
    - The returned index is sorted ascending.
    """
    tickers = _dedup_keep_order(tickers)
    if not tickers:
        return PricePanel(close=pd.DataFrame(), high=pd.DataFrame())

    # Try cached download first (for reproducible backtests)
    if use_cache:
        try:
            from src.core.yf import download_daily_range_cached
            data_dict, report = download_daily_range_cached(
                tickers=tickers,
                start=start_date,
                end=end_date,
                auto_adjust=auto_adjust,
                threads=threads,
            )
            
            cache_hits = report.get("cache_hits", 0)
            downloaded = report.get("downloaded", 0)
            if cache_hits > 0 or downloaded > 0:
                logger.info(f"Backtest prices: {cache_hits} from cache, {downloaded} downloaded, {len(report.get('bad_tickers', []))} failed")
            
            # Build Close and High DataFrames from cached data
            if data_dict:
                close_data = {}
                high_data = {}
                for ticker, df in data_dict.items():
                    if "Close" in df.columns:
                        close_data[ticker] = df["Close"]
                    if "High" in df.columns:
                        high_data[ticker] = df["High"]
                
                close = pd.DataFrame(close_data).sort_index()
                high = pd.DataFrame(high_data).sort_index()
                return PricePanel(close=close, high=high)
        except ImportError:
            logger.debug("Cache module not available, falling back to direct yfinance")
        except Exception as e:
            logger.warning(f"Cache failed, falling back to direct download: {e}")
    
    # Fallback to direct yfinance download
    start_dt = _to_dt(start_date)
    end_dt = _to_dt(end_date) + timedelta(days=1)

    data = yf.download(
        tickers=tickers,
        start=start_dt,
        end=end_dt,
        progress=False,
        auto_adjust=auto_adjust,
        threads=threads,
        group_by="column",
    )

    if data is None or getattr(data, "empty", True):
        return PricePanel(close=pd.DataFrame(), high=pd.DataFrame())

    if isinstance(data.columns, pd.MultiIndex):
        close = data.get("Close", pd.DataFrame()).copy()
        high = data.get("High", pd.DataFrame()).copy()
    else:
        # single ticker: columns like ["Open","High","Low","Close",...]
        close = data[["Close"]].copy()
        high = data[["High"]].copy()
        close.columns = [tickers[0]]
        high.columns = [tickers[0]]

    close = close.sort_index()
    high = high.sort_index()
    return PricePanel(close=close, high=high)


def _first_valid_idx_on_or_after(series: pd.Series, baseline_date: str) -> int | None:
    """
    Find the first positional index i where (date >= baseline_date) and value is finite.
    Returns None if not found.
    """
    if series is None or series.empty:
        return None
    # ensure sorted
    s = series.sort_index()
    try:
        baseline_ts = pd.Timestamp(baseline_date)
        mask = (s.index >= baseline_ts) & s.notna()
        if not mask.any():
            return None
        # idxmax returns first True when boolean
        first_label = mask.idxmax()
        return int(s.index.get_loc(first_label))
    except Exception:
        return None


def compute_forward_window_max(
    panel: PricePanel,
    ticker: str,
    baseline_date: str,
    *,
    forward_trading_days: int = 7,
    use_high: bool = True,
    exclude_entry_day: bool = True,
) -> tuple[float | None, float | None, float | None]:
    """
    For a given ticker and baseline date, compute:
    - entry_close
    - max_forward_price (High or Close) in next N trading days
    - max_return_pct

    Entry is the first valid Close on/after baseline_date.
    Because entry is at Close, we exclude entry-day High by default.
    """
    t = str(ticker).strip().upper()
    if panel.close is None or panel.close.empty or t not in panel.close.columns:
        return None, None, None

    close_s = panel.close[t].dropna()
    if close_s.empty:
        return None, None, None

    entry_pos = _first_valid_idx_on_or_after(panel.close[t], baseline_date)
    if entry_pos is None:
        return None, None, None

    # Rebuild on full index to keep positional alignment with High
    close_full = panel.close[t]
    entry_close = close_full.iloc[entry_pos]
    try:
        entry_close_f = float(entry_close)
    except Exception:
        return None, None, None

    start_pos = entry_pos + (1 if exclude_entry_day else 0)
    end_pos = start_pos + int(forward_trading_days)

    price_full = (panel.high[t] if use_high and (panel.high is not None and t in panel.high.columns) else panel.close[t])
    window = price_full.iloc[start_pos:end_pos].dropna()
    if window.empty:
        return entry_close_f, None, None

    max_price = float(window.max())
    max_return_pct = (max_price / entry_close_f - 1.0) * 100.0
    return entry_close_f, max_price, max_return_pct


def _date_in_range(date_str: str, start: str | None, end: str | None) -> bool:
    if start and date_str < start:
        return False
    if end and date_str > end:
        return False
    return True


def load_picks_in_range(
    outputs_root: str | Path = "outputs",
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[DatePicks]:
    """
    Load picks for all available output dates in [start_date, end_date].
    """
    dates = [d for d in iter_output_dates(outputs_root) if _date_in_range(d, start_date, end_date)]
    return [load_picks_for_date(d, outputs_root=outputs_root) for d in dates]


def compute_hit10_backtest(
    picks: list[DatePicks],
    *,
    outputs_root: str | Path = "outputs",
    forward_trading_days: int = 7,
    hit_threshold_pct: float = 10.0,
    use_high: bool = True,
    exclude_entry_day: bool = True,
    auto_adjust: bool = False,
    threads: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compute Hit(+10% within N trading days) using entry=baseline-day close (first valid close on/after baseline).

    Returns:
    - perf_detail: per (baseline_date, ticker) row
    - perf_by_date: aggregated hit-rates by baseline date
    - perf_by_component: aggregated hit-rates by component across all dates
    - perf_by_feature: weekly-only feature buckets (from top5_{date}.csv), if available
    """
    if not picks:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    # global download range
    all_dates = sorted({p.date_str for p in picks})
    start_date = all_dates[0]
    end_date = all_dates[-1]
    pad_days = max(21, int(forward_trading_days) * 4)
    end_dt_padded = (_to_dt(end_date) + timedelta(days=pad_days)).strftime("%Y-%m-%d")

    all_tickers: list[str] = _dedup_keep_order([t for p in picks for t in p.combined])
    panel = download_prices_once(
        all_tickers,
        start_date=start_date,
        end_date=end_dt_padded,
        auto_adjust=auto_adjust,
        threads=threads,
    )

    rows: list[dict[str, Any]] = []
    for p in picks:
        weekly_set = set(p.weekly_top5)
        pro30_set = set(p.pro30)
        movers_set = set(p.movers)
        weekly_rank = {t: i + 1 for i, t in enumerate(p.weekly_top5)}

        for t in p.combined:
            entry_close, max_price, max_ret_pct = compute_forward_window_max(
                panel,
                ticker=t,
                baseline_date=p.date_str,
                forward_trading_days=forward_trading_days,
                use_high=use_high,
                exclude_entry_day=exclude_entry_day,
            )

            hit10 = None
            if max_ret_pct is not None:
                hit10 = bool(max_ret_pct >= float(hit_threshold_pct))

            rows.append(
                {
                    "baseline_date": p.date_str,
                    "ticker": t,
                    "in_weekly_top5": t in weekly_set,
                    "weekly_rank": weekly_rank.get(t),
                    "in_pro30": t in pro30_set,
                    "in_movers": t in movers_set,
                    "entry_close": entry_close,
                    "max_forward_price": max_price,
                    "max_return_pct": max_ret_pct,
                    "hit10": hit10,
                }
            )

    perf_detail = pd.DataFrame(rows)

    # Aggregations
    def _hit_rate(df: pd.DataFrame) -> float | None:
        if df is None or df.empty or "hit10" not in df.columns:
            return None
        s = df["hit10"].dropna()
        if s.empty:
            return None
        return float(s.mean())

    by_date_rows: list[dict[str, Any]] = []
    for d, g in perf_detail.groupby("baseline_date", sort=True):
        by_date_rows.append(
            {
                "baseline_date": d,
                "n_tickers": int(len(g)),
                "hit_rate_all": _hit_rate(g),
                "n_weekly_top5": int(g["in_weekly_top5"].sum()),
                "hit_rate_weekly_top5": _hit_rate(g[g["in_weekly_top5"]]),
                "n_pro30": int(g["in_pro30"].sum()),
                "hit_rate_pro30": _hit_rate(g[g["in_pro30"]]),
                "n_movers": int(g["in_movers"].sum()),
                "hit_rate_movers": _hit_rate(g[g["in_movers"]]),
            }
        )
    perf_by_date = pd.DataFrame(by_date_rows)

    comp_rows: list[dict[str, Any]] = []
    comp_rows.append({"component": "all", "n": int(len(perf_detail)), "hit_rate": _hit_rate(perf_detail)})
    comp_rows.append(
        {
            "component": "weekly_top5",
            "n": int(perf_detail["in_weekly_top5"].sum()),
            "hit_rate": _hit_rate(perf_detail[perf_detail["in_weekly_top5"]]),
        }
    )
    comp_rows.append(
        {
            "component": "pro30",
            "n": int(perf_detail["in_pro30"].sum()),
            "hit_rate": _hit_rate(perf_detail[perf_detail["in_pro30"]]),
        }
    )
    comp_rows.append(
        {
            "component": "movers",
            "n": int(perf_detail["in_movers"].sum()),
            "hit_rate": _hit_rate(perf_detail[perf_detail["in_movers"]]),
        }
    )
    perf_by_component = pd.DataFrame(comp_rows)

    # Feature buckets (weekly top5 only) using stored top5_{date}.csv where possible
    feature_rows: list[pd.DataFrame] = []
    for d in sorted({p.date_str for p in picks}):
        top5_csv = Path(outputs_root) / d / f"top5_{d}.csv"
        df = _safe_read_csv(top5_csv)
        if df.empty or "ticker" not in df.columns:
            continue
        df = df.copy()
        df["baseline_date"] = d
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        # keep only expected score columns if present
        keep_cols = [c for c in ["baseline_date", "ticker", "composite_score", "technical_score", "catalyst_score"] if c in df.columns]
        feature_rows.append(df[keep_cols])

    perf_by_feature = pd.DataFrame()
    if feature_rows:
        features = pd.concat(feature_rows, ignore_index=True)
        merged = perf_detail.merge(features, on=["baseline_date", "ticker"], how="left")
        merged = merged[merged["in_weekly_top5"]].copy()
        if "composite_score" in merged.columns:
            merged["composite_bin"] = pd.cut(
                merged["composite_score"].astype(float),
                bins=[-1e9, 4.5, 5.0, 5.5, 1e9],
                labels=["<4.5", "4.5-5.0", "5.0-5.5", ">=5.5"],
            )
            perf_by_feature = (
                merged.groupby("composite_bin", observed=True)
                .apply(lambda x: pd.Series({"n": int(len(x)), "hit_rate": _hit_rate(x)}))
                .reset_index()
            )

    return perf_detail, perf_by_date, perf_by_component, perf_by_feature


def compute_hit10_backtest_enhanced(
    picks: list[DatePicks],
    *,
    outputs_root: str | Path = "outputs",
    forward_trading_days: int = 7,
    hit_threshold_pct: float = 10.0,
    stop_pct: Optional[float] = None,
    auto_adjust: bool = False,
    threads: bool = True,
    entry_model: str = "next_open",
    slippage_bps: float = 5.0,
    fee_bps: float = 2.0,
) -> tuple[pd.DataFrame, dict]:
    """
    Enhanced backtest with execution realism and MAE/MFE metrics.
    
    Uses next_open entry by default (no look-ahead), applies slippage/fees,
    and computes MAE/MFE using intraday High/Low.
    
    Args:
        picks: List of DatePicks from load_picks_in_range
        outputs_root: Root outputs directory
        forward_trading_days: Holding period in trading days
        hit_threshold_pct: Target return percentage
        stop_pct: Stop loss percentage (None = no stop)
        auto_adjust: Use adjusted prices
        threads: Use threading for downloads
        entry_model: "next_open" (default) or "same_close"
        slippage_bps: Slippage in basis points
        fee_bps: Fees in basis points
    
    Returns:
        (trades_df, summary_dict)
    """
    if not picks:
        return pd.DataFrame(), {}
    
    # Create execution model
    exec_model = ExecutionModel(
        entry=entry_model,
        slippage_bps=slippage_bps,
        fee_bps=fee_bps,
    )
    
    # Global download range
    all_dates = sorted({p.date_str for p in picks})
    start_date = all_dates[0]
    end_date = all_dates[-1]
    pad_days = max(21, int(forward_trading_days) * 4)
    end_dt_padded = (_to_dt(end_date) + timedelta(days=pad_days)).strftime("%Y-%m-%d")
    
    # Download all OHLCV data at once
    all_tickers: list[str] = _dedup_keep_order([t for p in picks for t in p.combined])
    
    # Get full OHLCV data for MAE/MFE computation
    logger.info(f"Downloading OHLCV for {len(all_tickers)} tickers...")
    ohlcv_data: dict[str, pd.DataFrame] = {}
    
    try:
        from src.core.yf import download_daily_range_cached
        data_dict, report = download_daily_range_cached(
            tickers=all_tickers,
            start=start_date,
            end=end_dt_padded,
            auto_adjust=auto_adjust,
            threads=threads,
        )
        ohlcv_data = data_dict
        logger.info(f"  Cache: {report.get('cache_hits', 0)} hits, {report.get('downloaded', 0)} downloaded")
    except Exception as e:
        logger.warning(f"Cache failed, using yfinance directly: {e}")
        # Fallback to direct download
        data = yf.download(
            tickers=all_tickers,
            start=start_date,
            end=end_dt_padded,
            progress=False,
            auto_adjust=auto_adjust,
            threads=threads,
            group_by="ticker",
        )
        if data is not None and not data.empty:
            for t in all_tickers:
                try:
                    if isinstance(data.columns, pd.MultiIndex):
                        ohlcv_data[t] = data[t].dropna()
                    else:
                        ohlcv_data[t] = data
                except Exception:
                    continue
    
    # Compute outcomes for each pick
    rows: list[dict[str, Any]] = []
    
    for p in picks:
        weekly_set = set(p.weekly_top5)
        pro30_set = set(p.pro30)
        movers_set = set(p.movers)
        weekly_rank = {t: i + 1 for i, t in enumerate(p.weekly_top5)}
        
        for t in p.combined:
            df = ohlcv_data.get(t, pd.DataFrame())
            
            # Compute entry price using execution model
            entry_px = entry_price(df, p.date_str, exec_model)
            
            outcome = {
                "asof_date": p.date_str,
                "ticker": t,
                "in_weekly_top5": t in weekly_set,
                "weekly_rank": weekly_rank.get(t),
                "in_pro30": t in pro30_set,
                "in_movers": t in movers_set,
                "entry_model": exec_model.entry,
                "slippage_bps": exec_model.slippage_bps,
                "fee_bps": exec_model.fee_bps,
            }
            
            if entry_px is None:
                outcome.update({
                    "entry_price": None,
                    "mfe_pct": None,
                    "mae_pct": None,
                    "days_to_hit": None,
                    "hit": None,
                    "exit_reason": "no_entry_price",
                    "max_return_pct": None,
                })
                rows.append(outcome)
                continue
            
            # Compute path metrics (MAE/MFE)
            start_dt = pd.to_datetime(p.date_str)
            pm = compute_path_metrics(
                df=df,
                entry_px=entry_px,
                start_dt=start_dt,
                horizon_days=forward_trading_days,
                target_pct=hit_threshold_pct,
                stop_pct=stop_pct,
            )
            
            outcome.update({
                "entry_price": round(entry_px, 4),
                "mfe_pct": round(pm.mfe, 2) if pm.mfe is not None else None,
                "mae_pct": round(pm.mae, 2) if pm.mae is not None else None,
                "days_to_hit": pm.days_to_hit,
                "hit": pm.hit,
                "exit_reason": pm.exit_reason,
                "max_return_pct": round(pm.mfe, 2) if pm.mfe is not None else None,
            })
            
            rows.append(outcome)
    
    trades_df = pd.DataFrame(rows)
    
    # Compute summary metrics
    valid_outcomes = [r for r in rows if r.get("hit") is not None]
    summary = compute_expectancy(valid_outcomes, target_pct=hit_threshold_pct)
    summary.update({
        "entry_model": exec_model.entry,
        "slippage_bps": exec_model.slippage_bps,
        "fee_bps": exec_model.fee_bps,
        "forward_trading_days": forward_trading_days,
        "hit_threshold_pct": hit_threshold_pct,
        "stop_pct": stop_pct,
    })
    
    return trades_df, summary


def write_backtest_outputs(
    perf_detail: pd.DataFrame,
    perf_by_date: pd.DataFrame,
    perf_by_component: pd.DataFrame,
    perf_by_feature: pd.DataFrame,
    *,
    output_dir: str | Path = "outputs/performance",
) -> dict[str, str]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths: dict[str, str] = {}
    p1 = out / "perf_detail.csv"
    perf_detail.to_csv(p1, index=False)
    paths["perf_detail_csv"] = str(p1)

    p2 = out / "perf_by_date.csv"
    perf_by_date.to_csv(p2, index=False)
    paths["perf_by_date_csv"] = str(p2)

    p3 = out / "perf_by_component.csv"
    perf_by_component.to_csv(p3, index=False)
    paths["perf_by_component_csv"] = str(p3)

    p4 = out / "perf_by_feature.csv"
    perf_by_feature.to_csv(p4, index=False)
    paths["perf_by_feature_csv"] = str(p4)

    return paths


def write_backtest_enhanced_outputs(
    trades_df: pd.DataFrame,
    summary: dict,
    *,
    output_dir: str | Path = "outputs/performance",
) -> dict[str, str]:
    """
    Write enhanced backtest outputs.
    
    Args:
        trades_df: Per-trade outcomes with MAE/MFE
        summary: Aggregate metrics dict
        output_dir: Output directory
    
    Returns:
        Dict with output file paths
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    paths: dict[str, str] = {}
    
    # Per-trade outcomes CSV
    p1 = out / "backtest_trades.csv"
    trades_df.to_csv(p1, index=False)
    paths["backtest_trades_csv"] = str(p1)
    
    # Summary JSON
    p2 = out / "backtest_summary.json"
    with open(p2, "w") as f:
        json.dump(summary, f, indent=2)
    paths["backtest_summary_json"] = str(p2)
    
    return paths


