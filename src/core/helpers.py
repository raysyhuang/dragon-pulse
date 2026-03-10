"""
Shared Helper Functions

News, earnings, and other utilities that are shared across pipelines.
These can be imported from existing scripts for backward compatibility.
"""

from __future__ import annotations
import time
import pandas as pd
import yfinance as yf
from datetime import date
from typing import Optional
from pathlib import Path


def get_ny_date() -> date:
    """
    Get the current date in New York timezone (America/New_York).
    
    This ensures that folder names and displays show the correct trading day
    based on NY time, not the local system timezone.
    
    Returns:
        date object representing today's date in NY timezone
    """
    ny_tz = pd.Timestamp.now(tz="America/New_York")
    return ny_tz.date()


def iter_weekdays(start_date: date, end_date: date) -> list[date]:
    """
    Iterate weekdays between start_date and end_date inclusive.
    Note: does not account for US market holidays.
    """
    if start_date > end_date:
        return []
    out: list[date] = []
    for ts in pd.date_range(start_date, end_date, freq="D"):
        if ts.weekday() < 5:
            out.append(ts.date())
    return out


def get_last_trading_date(check_date: Optional[date] = None) -> date:
    """
    Get the last trading day (weekday, excluding weekends and US market holidays).
    
    If the given date is a weekend or holiday, returns the previous trading day.
    If the given date is today and market hasn't closed yet (before 4 PM ET), 
    returns the previous trading day.
    If no date is provided, uses current NY date.
    
    Args:
        check_date: Optional date to check (defaults to current NY date)
    
    Returns:
        date object representing the last trading day
    """
    if check_date is None:
        check_date = get_ny_date()
    
    # US Market Holidays (fixed dates for common years, approx for floating)
    # This covers major NYSE/NASDAQ holidays
    def is_market_holiday(d: date) -> bool:
        year = d.year
        holidays = set()
        
        # Fixed holidays
        holidays.add(date(year, 1, 1))   # New Year's Day
        holidays.add(date(year, 7, 4))   # Independence Day
        holidays.add(date(year, 12, 25)) # Christmas
        
        # Juneteenth (June 19, observed since 2021)
        holidays.add(date(year, 6, 19))
        
        # MLK Day (3rd Monday of January)
        jan1 = date(year, 1, 1)
        days_to_monday = (7 - jan1.weekday()) % 7
        if jan1.weekday() == 0:
            days_to_monday = 0
        first_monday = (pd.Timestamp(jan1) + pd.Timedelta(days=days_to_monday)).date()
        mlk = (pd.Timestamp(first_monday) + pd.Timedelta(days=14)).date()
        holidays.add(mlk)
        
        # Presidents Day (3rd Monday of February)
        feb1 = date(year, 2, 1)
        days_to_monday = (7 - feb1.weekday()) % 7
        if feb1.weekday() == 0:
            days_to_monday = 0
        first_monday = (pd.Timestamp(feb1) + pd.Timedelta(days=days_to_monday)).date()
        presidents = (pd.Timestamp(first_monday) + pd.Timedelta(days=14)).date()
        holidays.add(presidents)
        
        # Good Friday (varies - approximate, 2 days before Easter)
        # Easter calculation (Computus algorithm)
        a = year % 19
        b = year // 100
        c = year % 100
        dd = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - dd - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        easter = date(year, month, day)
        good_friday = (pd.Timestamp(easter) - pd.Timedelta(days=2)).date()
        holidays.add(good_friday)
        
        # Memorial Day (last Monday of May)
        may31 = date(year, 5, 31)
        days_back = may31.weekday()  # Days back to get to Monday
        memorial = (pd.Timestamp(may31) - pd.Timedelta(days=days_back)).date()
        holidays.add(memorial)
        
        # Labor Day (1st Monday of September)
        sep1 = date(year, 9, 1)
        days_to_monday = (7 - sep1.weekday()) % 7
        if sep1.weekday() == 0:
            days_to_monday = 0
        labor = (pd.Timestamp(sep1) + pd.Timedelta(days=days_to_monday)).date()
        holidays.add(labor)
        
        # Thanksgiving (4th Thursday of November)
        nov1 = date(year, 11, 1)
        days_to_thu = (3 - nov1.weekday() + 7) % 7
        first_thu = (pd.Timestamp(nov1) + pd.Timedelta(days=days_to_thu)).date()
        thanksgiving = (pd.Timestamp(first_thu) + pd.Timedelta(days=21)).date()
        holidays.add(thanksgiving)
        
        # Check if date is holiday (or observed day if holiday falls on weekend)
        if d in holidays:
            return True
        
        # Check if Monday is observed for Sunday holiday
        if d.weekday() == 0:  # Monday
            sunday = (pd.Timestamp(d) - pd.Timedelta(days=1)).date()
            if sunday in holidays:
                return True
        
        # Check if Friday is observed for Saturday holiday
        if d.weekday() == 4:  # Friday
            saturday = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()
            if saturday in holidays:
                return True
        
        return False
    
    # Convert to pandas Timestamp for calculations
    ts = pd.Timestamp(check_date)
    
    # Helper to find previous trading day
    def prev_trading_day(d: date) -> date:
        candidate = (pd.Timestamp(d) - pd.Timedelta(days=1)).date()
        while candidate.weekday() >= 5 or is_market_holiday(candidate):
            candidate = (pd.Timestamp(candidate) - pd.Timedelta(days=1)).date()
        return candidate
    
    # If weekend or holiday, find previous trading day
    if check_date.weekday() >= 5 or is_market_holiday(check_date):
        return prev_trading_day(check_date)
    
    # Weekday and not a holiday - check if market is closed (before 4 PM ET)
    ny_now = pd.Timestamp.now(tz="America/New_York")
    today_date = ny_now.date()
    
    # If check_date is today and market hasn't closed yet, use previous trading day
    if check_date == today_date:
        market_close = ny_now.replace(hour=16, minute=0, second=0, microsecond=0)
        if ny_now < market_close:
            return prev_trading_day(check_date)
    
    # Market is closed or it's a past date, return the date
    return check_date


def get_market_date(timezone: str = "America/New_York") -> date:
    """
    Get the current date in the given market timezone.

    Args:
        timezone: IANA timezone string (e.g. ``"Asia/Shanghai"``,
                  ``"America/New_York"``).

    Returns:
        Today's date in the requested timezone.
    """
    return pd.Timestamp.now(tz=timezone).date()


def get_trading_date(
    check_date: Optional[date] = None,
    *,
    market: Optional[str] = None,
    close_hour: Optional[int] = None,
    close_minute: Optional[int] = None,
    timezone: Optional[str] = None,
) -> date:
    """
    Get the appropriate trading date for output directories.

    This ensures outputs are only created for actual trading days.
    If run on a weekend or before market close, uses the last completed trading day.

    Args:
        check_date: Optional date to check (defaults to current NY date)
        market: Market identifier (currently unused, reserved for CN holiday support)
        close_hour: Market close hour in local time (defaults to 16 for US)
        close_minute: Market close minute in local time (defaults to 0)
        timezone: IANA timezone string (defaults to America/New_York)

    Returns:
        date object representing the trading day to use for outputs
    """
    return get_last_trading_date(check_date)


def fetch_news_for_tickers(tickers: list[str], max_items: int, throttle_sec: float = 0.0) -> pd.DataFrame:
    """
    Fetch news headlines for tickers using yfinance.
    
    Args:
        tickers: List of ticker symbols
        max_items: Maximum news items per ticker
        throttle_sec: Seconds to wait between ticker requests
    
    Returns:
        DataFrame with columns: Ticker, published_utc, published_local, title, publisher, link, type
    """
    rows = []
    skipped = 0
    for t in tickers:
        fetched = False
        for attempt in range(3):
            try:
                tk = yf.Ticker(t)
                items = tk.news or []
                for it in items[:max_items]:
                    # Handle new yfinance news structure (content.title) vs old (title)
                    content = it.get("content", {}) if isinstance(it.get("content"), dict) else {}
                    title = content.get("title") or it.get("title") or ""
                    title = str(title).strip() if title else ""
                    
                    if not title:
                        continue
                    
                    # Handle timestamp - try multiple formats
                    ts = None
                    if "providerPublishTime" in it:
                        ts = pd.to_datetime(it.get("providerPublishTime"), unit="s", utc=True, errors="coerce")
                    elif "pubDate" in content:
                        ts = pd.to_datetime(content.get("pubDate"), utc=True, errors="coerce")
                    elif "displayTime" in content:
                        ts = pd.to_datetime(content.get("displayTime"), utc=True, errors="coerce")
                    
                    # Handle link
                    link = ""
                    if "canonicalUrl" in it and isinstance(it["canonicalUrl"], dict):
                        link = it["canonicalUrl"].get("url", "")
                    elif "clickThroughUrl" in it and isinstance(it["clickThroughUrl"], dict):
                        link = it["clickThroughUrl"].get("url", "")
                    else:
                        link = it.get("link", "") or ""
                    link = str(link).strip()
                    
                    # Handle publisher
                    pub = ""
                    if "provider" in it and isinstance(it["provider"], dict):
                        pub = it["provider"].get("displayName", "")
                    elif isinstance(it.get("provider"), list) and it["provider"]:
                        # Newer yfinance may return list of providers
                        first = it["provider"][0]
                        if isinstance(first, dict):
                            pub = first.get("name") or first.get("displayName") or ""
                    else:
                        pub = it.get("publisher", "") or ""
                    pub = str(pub).strip()

                    # Drop rows missing provenance or link (poor quality for LLM + audit)
                    if not title or not pub or not link:
                        skipped += 1
                        continue
                    
                    rows.append({
                        "Ticker": t,
                        "published_utc": ts,
                        "published_local": (ts.tz_convert("America/New_York").strftime("%Y-%m-%d %H:%M ET") if pd.notna(ts) else ""),
                        "title": title,
                        "publisher": pub,
                        "link": link,
                        "type": content.get("contentType", it.get("type", "")),
                    })
                fetched = True
                break
            except Exception as e:
                if attempt == 2:
                    import sys
                    print(f"  [WARN] News fetch failed for {t}: {type(e).__name__}", file=sys.stderr)
                time.sleep(0.25 * (attempt + 1))
        if throttle_sec and throttle_sec > 0:
            time.sleep(throttle_sec)
    
    if not rows:
        return pd.DataFrame(columns=["Ticker","published_utc","published_local","title","publisher","link","type"])
    
    df = pd.DataFrame(rows)
    # Dedup by (Ticker, title) keep most recent
    df = df.sort_values(["Ticker", "published_utc"], ascending=[True, False])
    df = df.drop_duplicates(subset=["Ticker", "title"], keep="first")
    df = df.reset_index(drop=True)

    # If we skipped many, surface a warning row for observability (count only)
    if skipped > 0:
        warn_row = {
            "Ticker": "__meta__",
            "published_utc": pd.Timestamp.utcnow(),
            "published_local": "",
            "title": f"Skipped {skipped} headline(s) missing publisher/link/title",
            "publisher": "data-quality",
            "link": "",
            "type": "meta"
        }
        df = pd.concat([pd.DataFrame([warn_row]), df], ignore_index=True)

    return df


def fetch_news_for_tickers_cn(
    tickers: list[str],
    max_items: int = 25,
    throttle_sec: float = 0.0,
) -> pd.DataFrame:
    """
    Fetch news headlines for China A-share tickers via AkShare (best effort).

    Falls back to an empty DataFrame if AkShare is unavailable or fails.

    Args:
        tickers: List of A-share ticker symbols (e.g. ``"600519.SH"``)
        max_items: Maximum news items per ticker
        throttle_sec: Seconds to wait between ticker requests

    Returns:
        DataFrame with columns matching ``fetch_news_for_tickers`` output.
    """
    cols = ["Ticker", "published_utc", "published_local", "title", "publisher", "link", "type"]
    rows: list[dict] = []

    try:
        import akshare as ak  # noqa: F811
    except ImportError:
        return pd.DataFrame(columns=cols)

    for t in tickers:
        code = t.split(".")[0]
        try:
            df_news = ak.stock_news_em(symbol=code)
            if df_news is None or df_news.empty:
                continue
            for _, row in df_news.head(max_items).iterrows():
                title = str(row.get("新闻标题", "") or "").strip()
                if not title:
                    continue
                pub_time = pd.to_datetime(row.get("发布时间"), errors="coerce")
                rows.append({
                    "Ticker": t,
                    "published_utc": pub_time,
                    "published_local": pub_time.strftime("%Y-%m-%d %H:%M") if pd.notna(pub_time) else "",
                    "title": title,
                    "publisher": str(row.get("新闻来源", "")) or "",
                    "link": str(row.get("新闻链接", "")) or "",
                    "type": "news",
                })
        except Exception:
            continue
        if throttle_sec and throttle_sec > 0:
            time.sleep(throttle_sec)

    if not rows:
        return pd.DataFrame(columns=cols)

    result = pd.DataFrame(rows)
    result = result.drop_duplicates(subset=["Ticker", "title"], keep="first").reset_index(drop=True)
    return result


def validate_required_columns(df: pd.DataFrame, required_cols: list[str], context: str) -> None:
    """
    Ensure required columns are present and non-null.

    Raises ValueError if any required column is missing or contains null/NaN.
    """
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"{context}: missing required columns {missing_cols}")

    null_cols = [c for c in required_cols if df[c].isna().any()]
    if null_cols:
        raise ValueError(f"{context}: null values found in required columns {null_cols}")


def get_next_earnings_date(ticker: str) -> str:
    """
    Get next earnings date for a ticker (best-effort).
    
    Args:
        ticker: Ticker symbol
    
    Returns:
        Earnings date string (or "Unknown" if unavailable)
    """
    try:
        tk = yf.Ticker(ticker)
        cal = tk.calendar
        if cal is not None and not cal.empty and "Earnings Date" in cal.index:
            dates = cal.loc["Earnings Date"].dropna()
            if not dates.empty:
                # Get the first date
                first_date = dates.iloc[0]
                if pd.notna(first_date):
                    if isinstance(first_date, pd.Timestamp):
                        return first_date.strftime("%Y-%m-%d")
                    return str(first_date)
    except Exception:
        pass
    return "Unknown"


def load_manual_headlines(filepath: str = "manual_headlines.csv") -> pd.DataFrame:
    """
    Load manual headlines from CSV file.
    
    Expected CSV format:
    Ticker,Date,Source,Headline
    
    Args:
        filepath: Path to CSV file
    
    Returns:
        DataFrame with columns: Ticker, Date, Source, Headline
    """
    if not Path(filepath).exists():
        return pd.DataFrame(columns=["Ticker", "Date", "Source", "Headline"])
    
    try:
        df = pd.read_csv(filepath)
        required_cols = ["Ticker", "Date", "Source", "Headline"]
        if not all(col in df.columns for col in required_cols):
            return pd.DataFrame(columns=required_cols)
        return df
    except Exception:
        return pd.DataFrame(columns=["Ticker", "Date", "Source", "Headline"])

