"""
China A-share Data Download Helpers

Primary: AkShare
Backup:  Tushare (if AkShare fails or returns empty)

Returns the same shape as yfinance helpers:
- A dict of {ticker: DataFrame[Open, High, Low, Close, Volume]}
- A report dict with bad tickers and reasons
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Iterable, Tuple

logger = logging.getLogger(__name__)

import pandas as pd


class _ProviderTimeoutError(TimeoutError):
    """Raised when a provider call exceeds the configured wall-clock timeout."""


def _call_with_timeout(provider_name: str, timeout_seconds: float, func, *args, **kwargs):
    """Run a provider fetch with a hard timeout on Unix main-thread code paths."""
    if timeout_seconds <= 0:
        return func(*args, **kwargs)

    if threading.current_thread() is not threading.main_thread() or os.name == "nt":
        return func(*args, **kwargs)

    def _handle_timeout(signum, frame):
        raise _ProviderTimeoutError(
            f"{provider_name} fetch exceeded {timeout_seconds:.2f}s timeout"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return func(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _period_to_dates(period: str, end: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Convert period strings like '300d' or '1y' to start/end datetimes."""
    end_dt = pd.to_datetime(end or datetime.utcnow()).to_pydatetime()
    days = 365
    try:
        period = str(period).lower()
        if period.endswith("d"):
            days = int(period[:-1])
        elif period.endswith("y"):
            years = float(period[:-1])
            days = int(years * 365)
    except Exception:
        days = 365
    start_dt = end_dt - timedelta(days=days)
    return start_dt, end_dt


def _split_cn_symbol(ticker: str) -> Tuple[str, str]:
    """
    Split ticker into (code, exchange) parts.
    Supports formats like '600000.SH' or '000001.SZ'. If no suffix, infer by code prefix.
    """
    raw = str(ticker).strip().upper()
    if "." in raw:
        code, exch = raw.split(".", 1)
    else:
        code, exch = raw, ""
    if not exch:
        if code.startswith("6"):
            exch = "SH"
        else:
            exch = "SZ"
    return code, exch


_INDEX_CODES = {"000001", "000300", "000016", "000905", "399001", "399006"}


def _ak_fetch_index_daily(code: str, exch: str, start: Optional[datetime], end: Optional[datetime]) -> pd.DataFrame:
    """Fetch daily OHLCV for a Chinese index (e.g. CSI 300) via AkShare."""
    import akshare as ak  # pyright: ignore[reportMissingModuleSource]

    # AkShare index format: "sh000300" (Shanghai) or "sz399001" (Shenzhen)
    prefix = "sh" if exch == "SH" else "sz"
    symbol = f"{prefix}{code}"

    start_str = start.strftime("%Y%m%d") if start else None
    end_str = end.strftime("%Y%m%d") if end else None

    df = ak.stock_zh_index_daily(symbol=symbol)
    if df is None or df.empty:
        return pd.DataFrame()

    rename_map = {
        "date": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    df = df.rename(columns=rename_map)
    if "Date" not in df.columns:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    # Filter to requested date range
    if start_str:
        df = df[df["Date"] >= pd.to_datetime(start_str)]
    if end_str:
        df = df[df["Date"] <= pd.to_datetime(end_str)]

    df = df.sort_values("Date")
    df = df.set_index("Date")

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["Open", "High", "Low", "Close", "Volume"]].dropna(how="any")


def _ak_fetch_daily(code: str, exch: str, start: Optional[datetime], end: Optional[datetime], adjust: str) -> pd.DataFrame:
    import akshare as ak  # pyright: ignore[reportMissingModuleSource]

    # Route index codes to the index-specific API
    if code in _INDEX_CODES:
        return _ak_fetch_index_daily(code, exch, start, end)

    start_str = start.strftime("%Y%m%d") if start else None
    end_str = end.strftime("%Y%m%d") if end else None
    adj_map = {"none": "", "qfq": "qfq", "hfq": "hfq"}
    adj_val = adj_map.get(str(adjust).lower(), "")

    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_str,
        end_date=end_str,
        adjust=adj_val,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    rename_map = {
        "日期": "Date",
        "开盘": "Open",
        "最高": "High",
        "最低": "Low",
        "收盘": "Close",
        "成交量": "Volume",
    }
    df = df.rename(columns=rename_map)
    if "Date" not in df.columns:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    df = df.sort_values("Date")
    df = df.set_index("Date")

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["Open", "High", "Low", "Close", "Volume"]].dropna(how="any")


def _tushare_fetch_daily(
    ticker: str,
    start: Optional[datetime],
    end: Optional[datetime],
    adjust: str,
    token: Optional[str],
) -> pd.DataFrame:
    import tushare as ts  # pyright: ignore[reportMissingModuleSource]

    pro = ts.pro_api(token=token)

    ts_code = ticker.upper()
    start_str = start.strftime("%Y%m%d") if start else None
    end_str = end.strftime("%Y%m%d") if end else None
    adj_map = {"none": None, "": None, "qfq": "qfq", "hfq": "hfq"}
    adj_val = adj_map.get(str(adjust).lower(), None)

    df = ts.pro_bar(
        ts_code=ts_code,
        asset="E",
        start_date=start_str,
        end_date=end_str,
        adj=adj_val,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "vol": "Volume"})
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["Date"])
    df = df.sort_values("Date")
    df = df.set_index("Date")

    # Tushare volume is in lots; convert to shares
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce") * 100.0

    for col in ("Open", "High", "Low", "Close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["Open", "High", "Low", "Close", "Volume"]].dropna(how="any")


def _ak_fetch_basic_info() -> pd.DataFrame:
    import akshare as ak  # pyright: ignore[reportMissingModuleSource]

    df = ak.stock_info_a_code_name()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    # Ensure consistent columns
    if "code" not in df.columns or "name" not in df.columns:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["exchange"] = df["code"].str[0].map(lambda x: "SH" if x == "6" else "SZ")
    df["ticker"] = df["code"] + "." + df["exchange"]
    # AkShare basic info does not include market cap or industry; add placeholders
    df["market_cap"] = None
    df["industry"] = None
    return df[["ticker", "name", "exchange", "market_cap", "industry"]]


def _tushare_fetch_basic_info(token: Optional[str]) -> pd.DataFrame:
    import tushare as ts  # pyright: ignore[reportMissingModuleSource]

    pro = ts.pro_api(token=token)
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,market,industry")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"ts_code": "ticker"})
    df["ticker"] = df["ticker"].astype(str).str.upper()
    # Derive exchange from ticker suffix (600519.SH → SH), not from
    # Tushare's 'market' column which returns Chinese labels like 主板/创业板.
    df["exchange"] = df["ticker"].str.split(".").str[-1]
    # Try to fetch market cap (total_mv) from daily_basic for today as enrichment
    try:
        today_str = datetime.utcnow().strftime("%Y%m%d")
        caps = pro.daily_basic(ts_code=",".join(df["ticker"].tolist()), trade_date=today_str, fields="ts_code,total_mv")
        if caps is not None and not caps.empty:
            caps = caps.rename(columns={"ts_code": "ticker"})
            caps["ticker"] = caps["ticker"].astype(str).str.upper()
            df = df.merge(caps[["ticker", "total_mv"]], on="ticker", how="left")
            df = df.rename(columns={"total_mv": "market_cap"})
    except Exception:
        pass
    if "market_cap" not in df.columns:
        df["market_cap"] = None
    if "industry" not in df.columns:
        df["industry"] = None
    return df[["ticker", "name", "exchange", "market_cap", "industry"]]


def get_cn_basic_info(tickers: list[str], provider_config: Optional[dict] = None) -> dict[str, dict]:
    """
    Best-effort company info (Chinese name, exchange, industry) for CN tickers.
    Returns mapping: ticker -> {"name_cn": str, "exchange": str, "market_cap": float|None, "industry": str|None}
    """
    if not tickers:
        return {}
    
    # For basic info, we prefer Tushare because it has richer metadata (industry, market cap)
    providers = ["tushare", "akshare"]
    tushare_token_env = (provider_config or {}).get("tushare_token_env", "TUSHARE_TOKEN")
    tushare_token = (provider_config or {}).get("tushare_token") or os.environ.get(tushare_token_env)

    df = pd.DataFrame()
    for provider in providers:
        try:
            if provider.lower() == "akshare":
                df = _ak_fetch_basic_info()
            elif provider.lower() == "tushare":
                df = _tushare_fetch_basic_info(tushare_token)
            else:
                continue
            if df is not None and not df.empty:
                break
        except Exception:
            df = pd.DataFrame()
            continue

    if df is None or df.empty:
        return {}

    lookup = {}
    df = df.dropna(subset=["ticker"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df = df.drop_duplicates(subset=["ticker"])
    for _, row in df.iterrows():
        name = str(row.get("name", "")) if pd.notna(row.get("name", "")) else ""
        lookup[row["ticker"]] = {
            "name_cn": name,
            "exchange": str(row.get("exchange", "")).upper() if pd.notna(row.get("exchange", "")) else "",
            "market_cap": float(row["market_cap"]) if "market_cap" in row and pd.notna(row["market_cap"]) else None,
            "industry": str(row.get("industry", "")) if pd.notna(row.get("industry", "")) else None,
            "is_st": "ST" in name.upper() or "*ST" in name.upper(),
        }

    out: dict[str, dict] = {}
    for t in tickers:
        key = str(t).upper()
        if key in lookup:
            out[key] = lookup[key]
    return out


def fetch_cn_news_for_tickers(tickers: list[str], max_items: int = 20, throttle_sec: float = 0.1) -> pd.DataFrame:
    """
    Best-effort CN news via AkShare with multiple fallback sources.
    
    Sources tried in order:
    1. stock_news_em (东方财富个股新闻)
    2. stock_info_global_em (东方财富全球财经)
    3. stock_zh_a_alerts_cls (财联社快讯)
    
    Returns DataFrame with Ticker, title, publisher, link, published_utc.
    """
    rows = []
    if not tickers:
        return pd.DataFrame(columns=["Ticker", "title", "publisher", "link", "published_utc"])
    
    try:
        import akshare as ak  # pyright: ignore[reportMissingModuleSource]
    except Exception:
        print("[WARN] AkShare not available for news fetching")
        return pd.DataFrame(columns=["Ticker", "title", "publisher", "link", "published_utc"])

    import time as time_module
    
    for t in tickers:
        code, exch = _split_cn_symbol(t)
        df = pd.DataFrame()
        source_name = "Unknown"
        
        # Method 1: 东方财富个股新闻 (stock_news_em)
        try:
            df = ak.stock_news_em(symbol=code)
            source_name = "东方财富"
        except Exception as e:
            df = pd.DataFrame()
        
        # Method 2: Fallback to different column names in stock_news_em
        if df is None or df.empty:
            try:
                # Some versions use different parameters
                df = ak.stock_news_em(symbol=f"{code}")
                source_name = "东方财富"
            except Exception:
                df = pd.DataFrame()
        
        # Method 3: Try stock_individual_info_em for company announcements
        if df is None or df.empty:
            try:
                # Get recent announcements
                full_code = f"{code}.{exch}"
                info_df = ak.stock_notice_report(symbol=full_code)
                if info_df is not None and not info_df.empty:
                    # Convert announcements to news format
                    df = info_df.rename(columns={
                        "标题": "title",
                        "日期": "datetime",
                    })
                    df["source"] = "公司公告"
                    df["url"] = ""
                    source_name = "公司公告"
            except Exception:
                df = pd.DataFrame()
        
        if df is None or df.empty:
            continue
        
        df = df.head(max_items)
        
        # Handle various column name formats from different AkShare endpoints
        for _, r in df.iterrows():
            # Title - try multiple column names
            title = ""
            for col in ["title", "新闻标题", "标题", "content", "新闻内容"]:
                if col in r and pd.notna(r.get(col)):
                    title = str(r.get(col, "")).strip()
                    if title:
                        break
            
            if not title:
                continue
            
            # Publisher/source
            pub = source_name
            for col in ["source", "来源", "新闻来源", "媒体"]:
                if col in r and pd.notna(r.get(col)):
                    pub = str(r.get(col, "")).strip() or source_name
                    break
            
            # URL/link
            link = ""
            for col in ["url", "link", "新闻链接", "链接"]:
                if col in r and pd.notna(r.get(col)):
                    link = str(r.get(col, "")).strip()
                    break
            
            # Timestamp
            ts = None
            for col in ["datetime", "time", "pub_time", "发布时间", "日期", "时间"]:
                if col in r and pd.notna(r.get(col)):
                    ts_raw = r.get(col)
                    try:
                        ts = pd.to_datetime(ts_raw, errors="coerce")
                        if pd.notna(ts):
                            # Localize to Shanghai timezone if naive
                            if ts.tz is None:
                                ts = ts.tz_localize("Asia/Shanghai")
                            ts = ts.tz_convert("UTC")
                            break
                    except Exception:
                        continue
            
            rows.append({
                "Ticker": t,
                "title": title,
                "publisher": pub,
                "link": link,
                "published_utc": ts,
            })
        
        if throttle_sec and throttle_sec > 0:
            time_module.sleep(throttle_sec)

    if not rows:
        # Try fetching general market news as fallback
        try:
            general_news = ak.stock_info_global_em()
            if general_news is not None and not general_news.empty:
                for _, r in general_news.head(10).iterrows():
                    title = str(r.get("title", r.get("标题", ""))).strip()
                    if title:
                        rows.append({
                            "Ticker": "MARKET",
                            "title": title,
                            "publisher": "东方财富全球",
                            "link": str(r.get("url", "")).strip(),
                            "published_utc": pd.to_datetime(r.get("datetime", r.get("时间")), errors="coerce"),
                        })
        except Exception:
            pass
    
    if not rows:
        return pd.DataFrame(columns=["Ticker", "title", "publisher", "link", "published_utc"])
    
    out_df = pd.DataFrame(rows)
    out_df = out_df.dropna(subset=["title"])
    out_df = out_df[out_df["title"].str.strip().ne("")]
    return out_df.reset_index(drop=True)


def fetch_cn_sector_news(sector: str = "", max_items: int = 30) -> pd.DataFrame:
    """
    Fetch general market/sector news from various CN sources.
    
    Args:
        sector: Optional sector filter (e.g., "科技", "金融")
        max_items: Maximum items to return
    
    Returns:
        DataFrame with title, publisher, link, published_utc
    """
    rows = []
    try:
        import akshare as ak
        
        # Try financial news
        try:
            df = ak.stock_info_global_em()
            if df is not None and not df.empty:
                for _, r in df.head(max_items).iterrows():
                    title = str(r.get("title", r.get("标题", ""))).strip()
                    if title:
                        rows.append({
                            "title": title,
                            "publisher": str(r.get("source", "东方财富")).strip(),
                            "link": str(r.get("url", "")).strip(),
                            "published_utc": pd.to_datetime(r.get("datetime"), errors="coerce"),
                        })
        except Exception:
            pass
        
        # Try Cailian Press (财联社)
        try:
            cls_df = ak.stock_telegraph_cls()
            if cls_df is not None and not cls_df.empty:
                for _, r in cls_df.head(max_items // 2).iterrows():
                    title = str(r.get("content", r.get("内容", ""))).strip()
                    if title and len(title) > 10:
                        rows.append({
                            "title": title[:200],  # Truncate long content
                            "publisher": "财联社",
                            "link": "",
                            "published_utc": pd.to_datetime(r.get("time", r.get("时间")), errors="coerce"),
                        })
        except Exception:
            pass
            
    except ImportError:
        pass
    
    if not rows:
        return pd.DataFrame(columns=["title", "publisher", "link", "published_utc"])
    
    return pd.DataFrame(rows).drop_duplicates(subset=["title"]).reset_index(drop=True)


def _validate_df(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(df.columns):
        return False
    if (df["High"] < df["Low"]).any():
        return False
    if df["Volume"].isna().all():
        return False
    return True


def _provider_sequence(provider_config: Optional[dict]) -> Iterable[str]:
    cfg = provider_config or {}
    primary = cfg.get("primary", "akshare")
    backup = cfg.get("backup", "tushare")
    seq = [primary]
    if backup and backup != primary:
        seq.append(backup)
    return seq


def _is_timeout_error(message: str) -> bool:
    lowered = str(message).lower()
    return any(
        pattern in lowered
        for pattern in (
            "read timed out",
            "connect timeout",
            "timed out",
            "timeout",
        )
    )


def download_daily(
    tickers: list[str],
    period: str = "1y",
    interval: str = "1d",
    auto_adjust: bool = False,
    threads: bool = True,
    progress: bool = False,
    provider_config: Optional[dict] = None,
) -> tuple[dict, dict]:
    """
    Download daily OHLCV for A-shares.
    Mirrors yfinance helper signatures; interval must be 1d.
    """
    if not tickers:
        return {}, {"bad_tickers": [], "reasons": {}}

    start_dt, end_dt = _period_to_dates(period)
    return download_daily_range(
        tickers=tickers,
        start=start_dt,
        end=end_dt,
        interval=interval,
        auto_adjust=auto_adjust,
        threads=threads,
        progress=progress,
        provider_config=provider_config,
    )


def download_daily_range(
    tickers: list[str],
    start: str | datetime,
    end: str | datetime,
    *,
    interval: str = "1d",
    auto_adjust: bool = False,
    threads: bool = True,
    progress: bool = False,
    provider_config: Optional[dict] = None,
) -> tuple[dict, dict]:
    """
    Date-range download for A-shares with AkShare primary and Tushare backup.
    """
    if interval != "1d":
        raise ValueError("Only daily interval is supported for China A-share adapter.")

    if not tickers:
        return {}, {"bad_tickers": [], "reasons": {}}

    try:
        start_dt = pd.to_datetime(start).to_pydatetime()
        end_dt = pd.to_datetime(end).to_pydatetime()
    except Exception:
        start_dt, end_dt = _period_to_dates("1y")

    providers = list(_provider_sequence(provider_config))
    adjust = (provider_config or {}).get("adjust", "none")
    tushare_token_env = (provider_config or {}).get("tushare_token_env", "TUSHARE_TOKEN")
    tushare_token = (provider_config or {}).get("tushare_token") or os.environ.get(tushare_token_env)
    backup_timeout_trip_count = int((provider_config or {}).get("backup_timeout_trip_count", 3))
    provider_timeout_seconds = float((provider_config or {}).get("provider_timeout_seconds", 20.0))
    progress_interval = int((provider_config or {}).get("progress_interval", 50))

    data_map: dict[str, pd.DataFrame] = {}
    bad_tickers: list[str] = []
    reasons: dict[str, str] = {}
    disabled_backups: set[str] = set()
    provider_timeout_failures: dict[str, int] = {}

    # Circuit breaker: abort early on systemic data failure.
    # Checks overall failure rate every CB_INTERVAL tickers after the
    # initial CB_WINDOW. Catches both total outages and intermittent
    # failures where occasional successes reset a consecutive counter.
    _CB_WINDOW = 50       # min tickers before first check
    _CB_INTERVAL = 25     # re-check every N tickers after window
    _CB_THRESHOLD = 0.90  # abort if ≥90% failed overall
    processed = 0

    # Throttle between API calls to avoid provider rate limits.
    # AkShare (Eastmoney) aggressively rate-limits rapid-fire requests;
    # without a delay, bulk downloads trigger IP bans after ~50 calls.
    _THROTTLE_SEC = 0.15
    started_at = time.time()
    total_tickers = len(tickers)

    for i, ticker in enumerate(tickers):
        if i > 0:
            time.sleep(_THROTTLE_SEC)

        code, exch = _split_cn_symbol(ticker)
        df_out = pd.DataFrame()
        last_err = ""
        for idx, provider in enumerate(providers):
            provider_name = provider.lower()
            if idx > 0 and provider_name in disabled_backups:
                continue
            try:
                if provider_name == "akshare":
                    df_out = _call_with_timeout(
                        provider_name,
                        provider_timeout_seconds,
                        _ak_fetch_daily,
                        code,
                        exch,
                        start_dt,
                        end_dt,
                        adjust,
                    )
                elif provider_name == "tushare":
                    df_out = _call_with_timeout(
                        provider_name,
                        provider_timeout_seconds,
                        _tushare_fetch_daily,
                        f"{code}.{exch}",
                        start_dt,
                        end_dt,
                        adjust,
                        tushare_token,
                    )
                else:
                    continue
                if _validate_df(df_out):
                    provider_timeout_failures[provider_name] = 0
                    break
            except Exception as e:
                last_err = str(e)
                df_out = pd.DataFrame()
                reasons[ticker] = f"{provider} error: {last_err}"
                if idx > 0 and backup_timeout_trip_count > 0 and _is_timeout_error(last_err):
                    provider_timeout_failures[provider_name] = (
                        provider_timeout_failures.get(provider_name, 0) + 1
                    )
                    if provider_timeout_failures[provider_name] >= backup_timeout_trip_count:
                        disabled_backups.add(provider_name)
                        logger.warning(
                            "Disabled %s backup after %d consecutive timeout failures",
                            provider_name,
                            provider_timeout_failures[provider_name],
                        )
                        reasons["__disabled_backups__"] = (
                            "Disabled backup providers after repeated timeout failures: "
                            + ", ".join(sorted(disabled_backups))
                        )
                continue

        processed += 1

        if not _validate_df(df_out):
            bad_tickers.append(ticker)
            reasons.setdefault(ticker, last_err or "No valid data returned")
        else:
            data_map[ticker] = df_out

        if (
            progress_interval > 0
            and total_tickers >= progress_interval
            and (processed % progress_interval == 0 or processed == total_tickers)
        ):
            elapsed_min = (time.time() - started_at) / 60
            logger.info(
                "Download progress: %d/%d processed, %d OK, %d failed (%.1f min)",
                processed,
                total_tickers,
                len(data_map),
                len(bad_tickers),
                elapsed_min,
            )

        # Circuit breaker: check overall failure rate periodically
        if (processed >= _CB_WINDOW
                and (processed == _CB_WINDOW or processed % _CB_INTERVAL == 0)):
            fail_rate = len(bad_tickers) / processed
            if fail_rate >= _CB_THRESHOLD:
                logger.error(
                    "Circuit breaker: %d/%d failed (%.0f%%). "
                    "Aborting — systemic data failure. Last error: %s",
                    len(bad_tickers), processed, fail_rate * 100,
                    last_err or "empty response",
                )
                reasons["__circuit_breaker__"] = (
                    f"Aborted at {processed}/{len(tickers)} tickers, "
                    f"{fail_rate:.0%} failure rate"
                )
                break

    # Log summary of failures for operator visibility
    if bad_tickers:
        sample = bad_tickers[:5]
        sample_reasons = [reasons.get(t, "unknown") for t in sample]
        logger.warning(
            "Download failures: %d/%d tickers failed. Sample: %s",
            len(bad_tickers), len(tickers),
            "; ".join(f"{t}: {r}" for t, r in zip(sample, sample_reasons)),
        )

    return data_map, {"bad_tickers": bad_tickers, "reasons": reasons}
