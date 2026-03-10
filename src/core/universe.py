"""
Universe Management

Functions to build, cache, and normalize ticker universes.
"""

from __future__ import annotations
import os
import time
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Callable, Optional
from io import StringIO


def normalize_ticker_for_yahoo(sym: str) -> str:
    """Normalize ticker symbol for Yahoo Finance (BRK.B -> BRK-B)."""
    return str(sym).strip().replace(".", "-")


def load_tickers_from_file(filepath: str) -> list[str]:
    """Load tickers from a single file, one per line (comments allowed with #)."""
    if not filepath or not os.path.exists(filepath):
        return []
    
    out: list[str] = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            # Allow comments: "MSTR  # bitcoin proxy"
            t = line.split("#")[0].strip()
            t = normalize_ticker_for_yahoo(t)
            if t:
                out.append(t)
    
    return out


def get_sp500_universe() -> list[str]:
    """
    Get S&P 500 ticker list from Wikipedia.
    Returns list of tickers (handles BRK.B -> BRK-B conversion for Yahoo).
    """
    import requests  # pyright: ignore[reportMissingModuleSource]
    
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Retry logic with timeout
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            table = pd.read_html(StringIO(response.text))[0]
            tickers = table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
            return tickers
        except Exception as e:
            if attempt < 2:
                time.sleep(1)  # Brief pause before retry
                continue
            # Final fallback: try without headers
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                table = pd.read_html(StringIO(response.text))[0]
                tickers = table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
                return tickers
            except Exception as e2:
                print(f"Error fetching S&P 500 universe after retries: {e2}")
                return []


def _read_wiki_table(url: str, table_pick: Callable, retries: int = 3) -> pd.DataFrame:
    """Robust read_html: sometimes table index shifts."""
    import requests  # pyright: ignore[reportMissingModuleSource]
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
            return table_pick(tables)
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            raise


def get_nasdaq100_universe() -> list[str]:
    """Get NASDAQ-100 ticker list from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    try:
        df = _read_wiki_table(url, lambda tables: next(t for t in tables if "Ticker" in t.columns))
        tickers = df["Ticker"].astype(str).map(normalize_ticker_for_yahoo).tolist()
        return sorted(set(tickers))
    except Exception:
        return []


def get_russell2000_universe() -> list[str]:
    """
    Best-effort Russell 2000 universe via iShares IWM holdings CSV.
    Robust to variable header/disclaimer blocks by auto-locating the real header row.
    """
    import requests  # pyright: ignore[reportMissingModuleSource]
    
    url = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?tab=all&fileType=csv"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,application/octet-stream,*/*",
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        text = r.text
        
        # Find the first line that looks like the true table header.
        # iShares typically uses "Ticker" as a column.
        lines = text.splitlines()
        header_idx = None
        for i, line in enumerate(lines):
            # Normalize whitespace; look for a CSV header containing Ticker/Symbol
            l = line.strip().strip("\ufeff")
            if l.startswith("Ticker,") or (("Ticker" in l.split(",")) and ("Name" in l or "Issuer" in l or "Sector" in l)):
                header_idx = i
                break
        
        if header_idx is None:
            return []
        
        # Rebuild CSV from the detected header downwards
        csv_text = "\n".join(lines[header_idx:])
        
        df = pd.read_csv(StringIO(csv_text))
        # Find ticker column robustly
        ticker_col = None
        for c in df.columns:
            if str(c).strip().lower() in {"ticker", "symbol"}:
                ticker_col = c
                break
        if ticker_col is None:
            # fallback: any column containing 'ticker' or 'symbol'
            cols = [c for c in df.columns if "ticker" in str(c).lower() or "symbol" in str(c).lower()]
            if not cols:
                return []
            ticker_col = cols[0]
        
        tickers = (
            df[ticker_col]
            .dropna()
            .astype(str)
            .map(normalize_ticker_for_yahoo)
            .tolist()
        )
        
        # Filter obvious non-tickers and skip disclaimer rows
        # Valid tickers should be short, alphanumeric (with dashes), and not contain disclaimer keywords
        disclaimer_keywords = ["content", "blackrock", "ishares", "prospectus", "copyright", "trademark"]
        tickers_filtered = []
        for t in tickers:
            t_clean = str(t).strip()
            # Skip if it's a disclaimer row (too long or contains disclaimer keywords)
            if len(t_clean) > 10 or any(keyword in t_clean.lower() for keyword in disclaimer_keywords):
                continue
            # Must be valid ticker format - relaxed validation (allow up to 10 chars, allow - and ^)
            if (t_clean.isascii() and 1 <= len(t_clean) <= 10 and 
                t_clean.replace("-", "").replace("^", "").isalnum()):
                tickers_filtered.append(t_clean)
        
        # Sanity check - should have a reasonable number of tickers (Russell 2000 has ~2000 stocks)
        if len(tickers_filtered) > 500:
            return sorted(set(tickers_filtered))
        else:
            # Log the count for debugging instead of silently returning empty
            if len(tickers_filtered) > 0:
                print(f"[WARN] Russell 2000 returned only {len(tickers_filtered)} tickers (expected >500), may be partial.")
            return []
    
    except Exception:
        return []


def get_china_a_universe(
    china_source_preference: Optional[list[str]] = None,
    tushare_token_env: str = "TUSHARE_TOKEN",
    china_board_filters: Optional[list[str]] = None,
) -> list[str]:
    """
    Best-effort China A-share universe.

    Returns SSE/SZSE symbols like ``600519.SH`` / ``000001.SZ``.
    """
    from . import cn_data

    providers = [str(p).lower() for p in (china_source_preference or ["akshare", "tushare"])]
    tickers: list[str] = []
    last_error: Optional[Exception] = None

    for provider in providers:
        try:
            if provider == "akshare":
                df = cn_data._ak_fetch_basic_info()
            elif provider == "tushare":
                token = os.environ.get(tushare_token_env)
                df = cn_data._tushare_fetch_basic_info(token)
            else:
                continue

            if df is None or df.empty or "ticker" not in df.columns:
                continue

            tickers = (
                df["ticker"]
                .dropna()
                .astype(str)
                .str.upper()
                .tolist()
            )
            if tickers:
                break
        except Exception as exc:
            last_error = exc
            continue

    if not tickers:
        if last_error:
            print(f"[WARN] China A-share universe fetch failed; last error: {last_error}")
        return []

    board_filters = {str(item).upper() for item in (china_board_filters or [])}
    if board_filters:
        filtered: list[str] = []
        for ticker in tickers:
            code = ticker.split(".", 1)[0]
            if "STAR" in board_filters and code.startswith("688"):
                continue
            if "CHINEXT" in board_filters and code.startswith(("300", "301")):
                continue
            if "BSE" in board_filters and code.startswith(("4", "8", "92")):
                continue
            filtered.append(ticker)
        tickers = filtered

    return sorted(set(tickers))


def load_universe_from_cache(cache_file: str, max_age_days: Optional[int] = 7) -> list[str]:
    """
    Load tickers from cache file if it exists and is fresh.
    
    Args:
        cache_file: Path to cache CSV file
        max_age_days: Maximum age in days for cache to be valid
    
    Returns:
        List of tickers, or empty list if cache is missing/stale
    """
    if not os.path.exists(cache_file):
        return []
    
    try:
        df = pd.read_csv(cache_file)
        if "Ticker" not in df.columns or "asof" not in df.columns:
            return []
        asof = pd.to_datetime(df["asof"].iloc[0], errors="coerce")
        if pd.isna(asof):
            return []
        if max_age_days is not None:
            age_days = (pd.Timestamp.utcnow().normalize() - asof.tz_localize(None).normalize()).days
            if age_days > max_age_days:
                return []
        tickers = df["Ticker"].dropna().astype(str).tolist()
        return sorted(set(tickers))
    except Exception:
        return []


def save_universe_to_cache(tickers: list[str], cache_file: str) -> None:
    """
    Save tickers to cache file.
    
    Args:
        tickers: List of ticker symbols
        cache_file: Path to cache CSV file
    """
    df = pd.DataFrame({"Ticker": sorted(set(tickers))})
    df["asof"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    df.to_csv(cache_file, index=False)


def build_universe(
    mode: str = "SP500+NASDAQ100+R2000",
    cache_file: Optional[str] = None,
    cache_max_age_days: int = 7,
    manual_include_file: Optional[str] = None,
    r2000_include_file: Optional[str] = None,
    manual_include_mode: str = "ALWAYS",
    quarantine_file: Optional[str] = None,
    quarantine_enabled: bool = True,
    china_board_filters: Optional[list[str]] = None,
    china_source_preference: Optional[list[str]] = None,
    tushare_token_env: str = "TUSHARE_TOKEN",
) -> list[str]:
    """
    Build ticker universe based on mode and optional manual includes.
    
    Args:
        mode: Universe mode ("SP500", "SP500+NASDAQ100", "SP500+NASDAQ100+R2000")
        cache_file: Optional cache file path (auto-generated if None based on mode)
        cache_max_age_days: Maximum age for cache to be valid
        manual_include_file: Optional path to manual include tickers file
        r2000_include_file: Optional path to R2000 tickers file
        manual_include_mode: "ALWAYS" or "ONLY_IF_IN_UNIVERSE"
    
    Returns:
        Sorted list of unique ticker symbols
    """
    mode = mode.upper()
    
    # Compute cache filename if not provided
    if cache_file is None:
        cache_file = f"universe_cache_{mode.replace('+', '_')}.csv"
    
    # Try cache first
    cached = load_universe_from_cache(cache_file, cache_max_age_days)
    if cached:
        base_tickers = cached
    else:
        # Build from sources
        if mode in {"CHINA_A", "CHINA_ALL"}:
            base_tickers = get_china_a_universe(
                china_source_preference=china_source_preference,
                tushare_token_env=tushare_token_env,
                china_board_filters=china_board_filters,
            )
        else:
            sp = get_sp500_universe()
            n100 = get_nasdaq100_universe() if "NASDAQ100" in mode else []
            r2k = get_russell2000_universe() if "R2000" in mode else []

            # Warn if R2000 fetch failed when it was requested, and fallback to local file if present
            if not r2k and "R2000" in mode:
                print("[WARN] Russell 2000 fetch failed; proceeding with SP500 + NASDAQ100 only.")
                if r2000_include_file:
                    fallback_r2k = load_tickers_from_file(r2000_include_file)
                    if fallback_r2k:
                        r2k = fallback_r2k
                        print(f"[INFO] Loaded Russell 2000 fallback from {r2000_include_file} ({len(r2k)} tickers)")
                    else:
                        print("[WARN] Russell 2000 fallback file empty or missing.")
            
            base_tickers = sorted(set(sp + n100 + r2k))
        
        # Fallback to stale cache if live fetch fails
        if not base_tickers and cache_file and os.path.exists(cache_file):
            stale = load_universe_from_cache(cache_file, max_age_days=None)
            if stale:
                print("[WARN] Universe fetch failed; using stale cache.")
                base_tickers = stale
        
        # Save to cache
        if base_tickers:
            save_universe_to_cache(base_tickers, cache_file)
    
    # Handle manual includes
    manual_tickers = []
    if manual_include_file:
        manual_tickers.extend(load_tickers_from_file(manual_include_file))
    if r2000_include_file:
        manual_tickers.extend(load_tickers_from_file(r2000_include_file))
    
    manual_tickers = sorted(set(manual_tickers))
    
    if manual_include_mode == "ALWAYS":
        # Always add manual tickers
        all_tickers = sorted(set(base_tickers + manual_tickers))
    elif manual_include_mode == "ONLY_IF_IN_UNIVERSE":
        # Only add manual tickers if they're already in base universe
        all_tickers = sorted(set([t for t in manual_tickers if t in base_tickers] + base_tickers))
    else:
        all_tickers = base_tickers
    
    # Apply quarantine (exclude known bad tickers)
    if quarantine_enabled:
        try:
            from .quarantine import get_quarantined_tickers
            quarantined = get_quarantined_tickers(quarantine_file or "data/bad_tickers.json")
            if quarantined:
                all_tickers = [t for t in all_tickers if t not in quarantined]
        except Exception:
            pass

    # Shuffle to prevent alphabetical position bias in downstream processing.
    # Uses date-based seed for reproducibility (same day = same order).
    import hashlib
    from datetime import date
    seed = int(hashlib.md5(str(date.today()).encode()).hexdigest()[:8], 16)
    import random
    rng = random.Random(seed)
    rng.shuffle(all_tickers)

    return all_tickers
