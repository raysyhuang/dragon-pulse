"""
Dragon Tiger List (龙虎榜) Scanner
==================================

Analyzes institutional trading activity from the Dragon Tiger List (龙虎榜),
which is published daily by Chinese exchanges showing unusual trading activity.

This replaces options flow analysis for China A-shares since:
1. A-share options market is very limited
2. Dragon Tiger List shows actual institutional positions
3. Research shows institutional flow can predict 55-65% of subsequent moves

Data sources via AkShare:
- stock_lhb_detail_em: 龙虎榜详情
- stock_lhb_ggtj_em: 龙虎榜个股统计
- stock_lhb_jgstatistic_em: 机构席位统计
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional, List
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class DragonTigerSignal:
    """A single Dragon Tiger List signal."""
    ticker: str
    signal_type: str  # "institutional_net_buy", "limit_up", "unusual_volume", "northbound_flow"
    strength: float  # 0-10 scale
    details: dict
    timestamp: datetime


@dataclass 
class DragonTigerCandidate:
    """A candidate identified by Dragon Tiger List analysis."""
    ticker: str
    name: str
    flow_score: float  # 0-10 composite score
    signals: List[DragonTigerSignal] = field(default_factory=list)
    net_buy_amount_cny: float = 0.0  # Net institutional buy amount in CNY
    buy_amount_cny: float = 0.0  # Total buy amount
    sell_amount_cny: float = 0.0  # Total sell amount
    institution_count: int = 0  # Number of institutional seats
    reason: str = ""  # Reason for appearing on list (e.g., "涨幅偏离值达7%")
    trade_date: Optional[str] = None
    exchange: str = ""


def fetch_dragon_tiger_daily(trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch Dragon Tiger List (龙虎榜) for a given date.
    
    Args:
        trade_date: Date string in YYYYMMDD format. If None, uses latest available.
    
    Returns:
        DataFrame with columns: ticker, name, close, change_pct, net_buy, 
                                buy_amount, sell_amount, reason, trade_date
    """
    try:
        import akshare as ak
    except ImportError:
        logger.error("AkShare not installed - Dragon Tiger List unavailable")
        return pd.DataFrame()
    
    try:
        # Fetch daily Dragon Tiger List summary
        if trade_date:
            df = ak.stock_lhb_detail_em(start_date=trade_date, end_date=trade_date)
        else:
            # Get recent data (last 5 trading days)
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            df = ak.stock_lhb_detail_em(start_date=start, end_date=end)
        
        if df is None or df.empty:
            logger.warning("No Dragon Tiger List data returned")
            return pd.DataFrame()
        
        # Standardize column names
        rename_map = {
            "代码": "ticker",
            "名称": "name", 
            "收盘价": "close",
            "涨跌幅": "change_pct",
            "龙虎榜净买额": "net_buy",
            "龙虎榜买入额": "buy_amount",
            "龙虎榜卖出额": "sell_amount",
            "上榜原因": "reason",
            "上榜日期": "trade_date",
            "解读": "interpretation",
        }
        
        df = df.rename(columns=rename_map)
        
        # Clean up ticker format
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str).str.zfill(6)
            # Add exchange suffix
            df["exchange"] = df["ticker"].apply(lambda x: "SH" if x.startswith("6") else "SZ")
            df["ticker"] = df["ticker"] + "." + df["exchange"]
        
        # Convert numeric columns
        for col in ["close", "change_pct", "net_buy", "buy_amount", "sell_amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to fetch Dragon Tiger List: {e}")
        return pd.DataFrame()


def fetch_dragon_tiger_detail(ticker: str, days: int = 30) -> pd.DataFrame:
    """
    Fetch detailed Dragon Tiger List history for a specific ticker.
    
    Args:
        ticker: Stock ticker (e.g., "600000.SH")
        days: Number of days to look back
    
    Returns:
        DataFrame with detailed institutional trading data
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()
    
    try:
        # Extract code without exchange suffix
        code = ticker.split(".")[0] if "." in ticker else ticker
        code = code.zfill(6)
        
        # Try individual stock Dragon Tiger history
        df = ak.stock_lhb_stock_statistic_em(symbol=code)
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Keep recent days only
        if "上榜日期" in df.columns:
            df["上榜日期"] = pd.to_datetime(df["上榜日期"], errors="coerce")
            cutoff = datetime.now() - timedelta(days=days)
            df = df[df["上榜日期"] >= cutoff]
        
        return df
        
    except Exception as e:
        logger.debug(f"Failed to fetch Dragon Tiger detail for {ticker}: {e}")
        return pd.DataFrame()


def fetch_institutional_seats(trade_date: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch institutional seat (机构席位) statistics from Dragon Tiger List.
    
    This shows which institutional traders were active and their net positions.
    
    Args:
        trade_date: Date in YYYYMMDD format
    
    Returns:
        DataFrame with institutional trading data
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()
    
    try:
        df = ak.stock_lhb_jgstatistic_em(symbol="近一月")
        
        if df is None or df.empty:
            return pd.DataFrame()
        
        return df
        
    except Exception as e:
        logger.debug(f"Failed to fetch institutional seats: {e}")
        return pd.DataFrame()


def fetch_northbound_flow(days: int = 5) -> pd.DataFrame:
    """
    Fetch recent Northbound (北向资金) Stock Connect flow data.
    
    This shows foreign institutional money flow into A-shares via
    Shanghai-Hong Kong and Shenzhen-Hong Kong Stock Connect.
    
    Args:
        days: Number of recent trading days to fetch
    
    Returns:
        DataFrame with northbound flow data
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()
    
    try:
        # Get Shanghai-HK Connect
        sh_df = ak.stock_hsgt_north_net_flow_in_em(symbol="沪股通")
        # Get Shenzhen-HK Connect  
        sz_df = ak.stock_hsgt_north_net_flow_in_em(symbol="深股通")
        
        # Combine flows
        combined = pd.DataFrame()
        if sh_df is not None and not sh_df.empty:
            sh_df["channel"] = "沪股通"
            combined = pd.concat([combined, sh_df])
        if sz_df is not None and not sz_df.empty:
            sz_df["channel"] = "深股通"
            combined = pd.concat([combined, sz_df])
        
        if combined.empty:
            return pd.DataFrame()
        
        # Keep recent days
        if "日期" in combined.columns:
            combined["日期"] = pd.to_datetime(combined["日期"], errors="coerce")
            combined = combined.sort_values("日期", ascending=False).head(days * 2)
        
        return combined
        
    except Exception as e:
        logger.debug(f"Failed to fetch northbound flow: {e}")
        return pd.DataFrame()


def get_institutional_net_buy(tickers: List[str], days: int = 5) -> dict:
    """
    Get net institutional buy amounts for a list of tickers from Dragon Tiger List.
    
    Args:
        tickers: List of ticker symbols
        days: Days to look back
    
    Returns:
        Dict mapping ticker -> net_buy_amount (CNY)
    """
    df = fetch_dragon_tiger_daily()
    
    if df.empty:
        return {}
    
    result = {}
    ticker_set = set(t.upper() for t in tickers)
    
    for _, row in df.iterrows():
        t = str(row.get("ticker", "")).upper()
        if t in ticker_set:
            net = float(row.get("net_buy", 0) or 0)
            # Accumulate if ticker appears multiple times
            result[t] = result.get(t, 0) + net
    
    return result


def analyze_dragon_tiger_flow(ticker: str, days: int = 10) -> Optional[DragonTigerCandidate]:
    """
    Analyze Dragon Tiger List flow for a single ticker.
    
    Returns DragonTigerCandidate if significant signals detected, None otherwise.
    
    Args:
        ticker: Stock ticker
        days: Days to analyze
    
    Returns:
        DragonTigerCandidate or None
    """
    # Fetch recent Dragon Tiger appearances
    df = fetch_dragon_tiger_daily()
    
    if df.empty:
        return None
    
    # Filter for this ticker
    ticker_upper = ticker.upper()
    ticker_df = df[df["ticker"].str.upper() == ticker_upper]
    
    if ticker_df.empty:
        return None
    
    # Aggregate signals
    signals = []
    total_buy = 0.0
    total_sell = 0.0
    institution_count = 0
    
    for _, row in ticker_df.iterrows():
        buy = float(row.get("buy_amount", 0) or 0)
        sell = float(row.get("sell_amount", 0) or 0)
        net = float(row.get("net_buy", 0) or 0)
        reason = str(row.get("reason", ""))
        
        total_buy += buy
        total_sell += sell
        
        # Signal 1: Net institutional buying
        if net > 10_000_000:  # > 10M CNY net buy
            strength = min(10, net / 50_000_000 * 10)  # Scale to 10
            signals.append(DragonTigerSignal(
                ticker=ticker,
                signal_type="institutional_net_buy",
                strength=strength,
                details={"net_buy_cny": net, "reason": reason},
                timestamp=datetime.now(),
            ))
        
        # Signal 2: Limit up appearance (涨停)
        if "涨停" in reason or "涨幅" in reason:
            signals.append(DragonTigerSignal(
                ticker=ticker,
                signal_type="limit_up_breakout",
                strength=7.0,
                details={"reason": reason},
                timestamp=datetime.now(),
            ))
        
        # Signal 3: Unusual volume
        if "换手率" in reason or "成交额" in reason:
            signals.append(DragonTigerSignal(
                ticker=ticker,
                signal_type="unusual_volume",
                strength=5.0,
                details={"reason": reason},
                timestamp=datetime.now(),
            ))
    
    if not signals:
        return None
    
    # Compute composite flow score
    flow_score = sum(s.strength for s in signals) / len(signals)
    
    # Boost for multiple signals
    if len(signals) >= 2:
        flow_score = min(10, flow_score * 1.2)
    if len(signals) >= 3:
        flow_score = min(10, flow_score * 1.3)
    
    # Get name from first row
    name = ticker_df.iloc[0].get("name", ticker) if not ticker_df.empty else ticker
    exchange = ticker_df.iloc[0].get("exchange", "") if not ticker_df.empty else ""
    reason = ticker_df.iloc[0].get("reason", "") if not ticker_df.empty else ""
    trade_date = str(ticker_df.iloc[0].get("trade_date", "")) if not ticker_df.empty else ""
    
    return DragonTigerCandidate(
        ticker=ticker,
        name=str(name),
        flow_score=round(flow_score, 2),
        signals=signals,
        net_buy_amount_cny=total_buy - total_sell,
        buy_amount_cny=total_buy,
        sell_amount_cny=total_sell,
        institution_count=len(ticker_df),
        reason=reason,
        trade_date=trade_date,
        exchange=exchange,
    )


def scan_dragon_tiger(
    tickers: Optional[List[str]] = None,
    min_flow_score: float = 5.0,
    min_net_buy_cny: float = 10_000_000,
    top_n: int = 20,
) -> List[DragonTigerCandidate]:
    """
    Scan Dragon Tiger List for candidates with strong institutional flow.
    
    Args:
        tickers: Optional list of tickers to filter. If None, scans all.
        min_flow_score: Minimum flow score (0-10) to include
        min_net_buy_cny: Minimum net buy amount in CNY
        top_n: Maximum candidates to return
    
    Returns:
        List of DragonTigerCandidate sorted by flow_score descending
    """
    df = fetch_dragon_tiger_daily()
    
    if df.empty:
        logger.warning("No Dragon Tiger List data available")
        return []
    
    # Filter by ticker list if provided
    if tickers:
        ticker_set = set(t.upper() for t in tickers)
        df = df[df["ticker"].str.upper().isin(ticker_set)]
    
    # Filter by net buy amount
    if "net_buy" in df.columns:
        df = df[pd.to_numeric(df["net_buy"], errors="coerce").fillna(0) >= min_net_buy_cny]
    
    candidates = []
    seen_tickers = set()
    
    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).upper()
        
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        
        candidate = analyze_dragon_tiger_flow(ticker)
        
        if candidate and candidate.flow_score >= min_flow_score:
            candidates.append(candidate)
    
    # Sort by flow score
    candidates.sort(key=lambda x: x.flow_score, reverse=True)
    
    logger.info(f"Found {len(candidates)} Dragon Tiger candidates with score >= {min_flow_score}")
    
    return candidates[:top_n]


def format_dragon_tiger_report(candidates: List[DragonTigerCandidate]) -> str:
    """Format Dragon Tiger List candidates as readable report."""
    if not candidates:
        return "No significant Dragon Tiger List activity detected."
    
    lines = [
        "=" * 60,
        "🐉 DRAGON TIGER LIST (龙虎榜) SCANNER RESULTS",
        "=" * 60,
        "",
    ]
    
    for i, c in enumerate(candidates, 1):
        signal_types = [s.signal_type for s in c.signals]
        lines.extend([
            f"{i}. {c.ticker} {c.name} (Flow Score: {c.flow_score}/10)",
            f"   📈 Net Buy: ¥{c.net_buy_amount_cny/1e6:.1f}M | Buy: ¥{c.buy_amount_cny/1e6:.1f}M | Sell: ¥{c.sell_amount_cny/1e6:.1f}M",
            f"   🏛️ Appearances: {c.institution_count} | Reason: {c.reason[:50]}",
            f"   ⚡ Signals: {', '.join(signal_types)}",
            "",
        ])
    
    return "\n".join(lines)
