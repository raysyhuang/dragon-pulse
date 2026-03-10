"""
Packet Building Functions

Build LLM packets for ranking and analysis.
Includes China A-share specific data (Dragon Tiger List, sector rotation, sentiment).
"""

from __future__ import annotations
import pandas as pd
from typing import Optional
from src.core.analysis import analyze_headlines


def build_weekly_scanner_packet(
    ticker: str,
    row: pd.Series,
    news_df: pd.DataFrame,
    earnings_date: str,
    manual_headlines_df: Optional[pd.DataFrame] = None,
    source_tags: Optional[list[str]] = None,
    market: str = "US",
) -> dict:
    """
    Build a packet for LLM Weekly Scanner ranking.
    
    Args:
        ticker: Ticker symbol
        row: Series with candidate data (from screening)
        news_df: DataFrame with news headlines
        earnings_date: Next earnings date string
        manual_headlines_df: Optional DataFrame with manual headlines
        source_tags: Optional list of source tags (e.g., ["BASE_UNIVERSE", "DAILY_MOVER"])
        market: Market identifier ("US" or "CN")
    
    Returns:
        Dict with all data needed for LLM scoring
    """
    # Filter news for this ticker
    ticker_news = news_df[news_df["Ticker"] == ticker].copy() if not news_df.empty else pd.DataFrame()
    if not ticker_news.empty and "published_utc" in ticker_news.columns:
        ticker_news["published_utc"] = pd.to_datetime(ticker_news["published_utc"], utc=True, errors="coerce")
        ticker_news = ticker_news.sort_values("published_utc", ascending=False)
        ticker_news = ticker_news.head(15)  # Top 15 headlines
    
    # Format headlines (manual first, then fetched news)
    headlines = []
    headline_titles = []
    
    # Add manual headlines first
    if manual_headlines_df is not None and not manual_headlines_df.empty:
        manual = manual_headlines_df[manual_headlines_df["Ticker"].astype(str).str.strip().eq(ticker)]
        for _, m in manual.iterrows():
            date_str = str(m.get("Date", "Unknown")).strip()
            source = str(m.get("Source", "Manual")).strip()
            headline = str(m.get("Headline", "")).strip()
            if headline:
                headline_titles.append(headline)
                headlines.append({
                    "title": headline,
                    "publisher": source,
                    "url": "",
                    "published_at": date_str
                })
    
    # Add fetched news headlines
    for _, n in ticker_news.iterrows():
        title = str(n.get("title", "")).strip()
        publisher = str(n.get("publisher", "")).strip()
        link = str(n.get("link", "")).strip()
        pub_time = n.get("published_utc", pd.NaT)
        pub_str = pub_time.strftime("%Y-%m-%d") if pd.notna(pub_time) else "Unknown"
        if title:
            headline_titles.append(title)
            headlines.append({
                "title": title,
                "publisher": publisher,
                "url": link,
                "published_at": pub_str
            })
    
    # Analyze headlines for flags
    flags = analyze_headlines(headline_titles)
    
    # Base packet
    packet = {
        "ticker": ticker,
        "name": row.get("name", ticker),
        "exchange": row.get("exchange", "Unknown"),
        "sector": row.get("sector", "Unknown"),
        "current_price": row.get("current_price"),
        "market_cap_usd": row.get("market_cap_usd"),
        "avg_dollar_volume_20d": row.get("avg_dollar_volume_20d"),
        "asof_price_utc": row.get("asof_price_utc"),
        
        # Technical (LOCKED - from Python)
        "technical_score": row.get("technical_score"),
        "technical_evidence": row.get("technical_evidence", {}),
        "technical_data_gaps": row.get("technical_data_gaps", []),
        
        # Catalyst data
        "earnings_date": earnings_date,
        "headlines": headlines,
        "dilution_flag": flags.get("dilution_flag", 0),
        "catalyst_tags": flags.get("catalyst_tags", ""),
        
        # Source tags
        "source_tags": source_tags if source_tags else ["BASE_UNIVERSE"],
    }
    
    # Add China-specific fields for CN market
    if market.upper() == "CN":
        packet["name_cn"] = row.get("name_cn", row.get("name", ticker))
        packet["trading_number"] = row.get("trading_number", ticker)
        
        # Market activity replaces options for China
        packet["market_activity"] = {
            "dragon_tiger_score": row.get("dragon_tiger_score"),
            "on_dragon_tiger_list": row.get("on_dragon_tiger_list", False),
            "net_institutional_buy_cny": row.get("net_institutional_buy_cny"),
            "sector_momentum_score": row.get("sector_momentum_score"),
            "hot_sector": row.get("hot_sector", False),
            "sentiment_score": row.get("sentiment_score"),
            "hot_stock_rank": row.get("hot_stock_rank"),
        }
        packet["market_activity_available"] = any([
            row.get("dragon_tiger_score"),
            row.get("on_dragon_tiger_list"),
            row.get("sector_momentum_score"),
            row.get("sentiment_score"),
        ])
        
        # Remove US-specific fields
        packet["options_data_available"] = False
        packet["sentiment_data_available"] = packet["market_activity_available"]
    else:
        # US market - original fields
        packet["options_data_available"] = False
        packet["sentiment_data_available"] = False
    
    return packet


def build_weekly_scanner_packet_cn(
    ticker: str,
    row: pd.Series,
    news_df: pd.DataFrame,
    manual_headlines_df: Optional[pd.DataFrame] = None,
    source_tags: Optional[list[str]] = None,
    dragon_tiger_data: Optional[dict] = None,
    sector_data: Optional[dict] = None,
    sentiment_data: Optional[dict] = None,
) -> dict:
    """
    Build a packet for China A-share Weekly Scanner ranking.
    
    This is an enhanced version specifically for China market with:
    - Dragon Tiger List (龙虎榜) data
    - Sector rotation data
    - Chinese sentiment data
    
    Args:
        ticker: Stock ticker (e.g., "600000.SH")
        row: Series with candidate data
        news_df: DataFrame with news headlines
        manual_headlines_df: Optional manual headlines
        source_tags: Source tags
        dragon_tiger_data: Optional Dragon Tiger List data for this ticker
        sector_data: Optional sector rotation data
        sentiment_data: Optional sentiment analysis data
    
    Returns:
        Dict with all data needed for LLM scoring
    """
    # Build base packet
    packet = build_weekly_scanner_packet(
        ticker=ticker,
        row=row,
        news_df=news_df,
        earnings_date="Unknown",  # Earnings dates not readily available for A-shares
        manual_headlines_df=manual_headlines_df,
        source_tags=source_tags,
        market="CN",
    )
    
    # Add Dragon Tiger List data
    if dragon_tiger_data:
        packet["dragon_tiger"] = {
            "on_list": True,
            "flow_score": dragon_tiger_data.get("flow_score", 0),
            "net_buy_cny": dragon_tiger_data.get("net_buy_amount_cny", 0),
            "buy_amount_cny": dragon_tiger_data.get("buy_amount_cny", 0),
            "sell_amount_cny": dragon_tiger_data.get("sell_amount_cny", 0),
            "reason": dragon_tiger_data.get("reason", ""),
            "trade_date": dragon_tiger_data.get("trade_date", ""),
        }
        packet["market_activity"]["dragon_tiger_score"] = dragon_tiger_data.get("flow_score")
        packet["market_activity"]["on_dragon_tiger_list"] = True
        packet["market_activity"]["net_institutional_buy_cny"] = dragon_tiger_data.get("net_buy_amount_cny")
    else:
        packet["dragon_tiger"] = {
            "on_list": False,
            "flow_score": None,
            "net_buy_cny": None,
        }
    
    # Add sector rotation data
    if sector_data:
        packet["sector_rotation"] = {
            "sector": sector_data.get("sector", "Unknown"),
            "sector_momentum_score": sector_data.get("momentum_score", 0),
            "sector_return_1d": sector_data.get("return_1d", 0),
            "sector_return_5d": sector_data.get("return_5d", 0),
            "sector_trend": sector_data.get("trend", "unknown"),
            "is_hot_sector": sector_data.get("momentum_score", 0) >= 6.0,
        }
        packet["market_activity"]["sector_momentum_score"] = sector_data.get("momentum_score")
        packet["market_activity"]["hot_sector"] = sector_data.get("momentum_score", 0) >= 6.0
    else:
        packet["sector_rotation"] = {
            "sector": "Unknown",
            "sector_momentum_score": None,
            "is_hot_sector": False,
        }
    
    # Add sentiment data
    if sentiment_data:
        packet["sentiment_cn"] = {
            "score": sentiment_data.get("score", 0),
            "hot_stock_rank": sentiment_data.get("hot_stock_rank"),
            "eastmoney_guba": sentiment_data.get("eastmoney_guba", {}),
            "news_tone": sentiment_data.get("news_tone", "neutral"),
        }
        packet["market_activity"]["sentiment_score"] = sentiment_data.get("score")
        packet["market_activity"]["hot_stock_rank"] = sentiment_data.get("hot_stock_rank")
    else:
        packet["sentiment_cn"] = {
            "score": None,
            "hot_stock_rank": None,
            "news_tone": "neutral",
        }
    
    # Update market activity availability flag
    packet["market_activity_available"] = any([
        packet["market_activity"].get("dragon_tiger_score"),
        packet["market_activity"].get("on_dragon_tiger_list"),
        packet["market_activity"].get("sector_momentum_score"),
        packet["market_activity"].get("sentiment_score"),
    ])
    
    return packet

