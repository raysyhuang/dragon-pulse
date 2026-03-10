"""
China A-Share Sentiment Analysis Module
=======================================

Fetches social sentiment data from Chinese platforms to enhance scoring.

Data sources via AkShare:
- 东方财富股吧 (Eastmoney Guba): Retail sentiment
- 雪球 (Xueqiu): Social mentions  
- 同花顺 (10jqka): Market sentiment
- 财联社 (Cailian Press): News sentiment
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta
from typing import Optional, TypedDict
from dataclasses import dataclass, field
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class SentimentEvidenceCN(TypedDict):
    """Sentiment evidence structure for China A-shares."""
    eastmoney_guba: dict  # 东方财富股吧
    xueqiu: dict  # 雪球
    news_tone: str
    data_source: str
    hot_stock_rank: Optional[int]  # Rank in hot stock lists


@dataclass
class SentimentScoreCN:
    """Sentiment momentum score result for China stocks."""
    score: float  # 0-10 scale
    evidence: SentimentEvidenceCN
    data_gaps: list = field(default_factory=list)
    cap_applied: Optional[float] = None


def fetch_eastmoney_guba_sentiment(ticker: str) -> Optional[dict]:
    """
    Fetch sentiment data from 东方财富股吧 (Eastmoney Guba).
    
    This is China's largest retail investor discussion forum.
    
    Args:
        ticker: Stock ticker (e.g., "600000.SH")
    
    Returns:
        Dict with post_count, read_count, comment_count, sentiment_estimate
    """
    try:
        import akshare as ak
        
        # Extract code without suffix
        code = ticker.split(".")[0] if "." in ticker else ticker
        code = code.zfill(6)
        
        # Try to get stock guba posts
        try:
            df = ak.stock_guba_em(symbol=code)
            
            if df is None or df.empty:
                return None
            
            # Analyze recent posts
            recent_posts = df.head(50)
            total_reads = 0
            total_comments = 0
            
            for _, row in recent_posts.iterrows():
                reads = int(row.get("阅读数", row.get("阅读", 0)) or 0)
                comments = int(row.get("评论数", row.get("评论", 0)) or 0)
                total_reads += reads
                total_comments += comments
            
            return {
                "post_count": len(recent_posts),
                "total_reads": total_reads,
                "total_comments": total_comments,
                "avg_reads_per_post": total_reads / len(recent_posts) if len(recent_posts) > 0 else 0,
                "source": "eastmoney_guba"
            }
            
        except Exception:
            return None
            
    except ImportError:
        return None
    except Exception as e:
        logger.debug(f"Eastmoney Guba fetch failed for {ticker}: {e}")
        return None


def fetch_hot_stock_rank(ticker: str) -> Optional[int]:
    """
    Check if stock is on hot stock lists (人气榜).
    
    Args:
        ticker: Stock ticker
    
    Returns:
        Rank if on list (1 = highest), None otherwise
    """
    try:
        import akshare as ak
        
        code = ticker.split(".")[0] if "." in ticker else ticker
        code = code.zfill(6)
        
        # Try to get hot stock rankings
        try:
            df = ak.stock_hot_rank_em()
            
            if df is not None and not df.empty:
                # Check if our stock is in the list
                if "代码" in df.columns:
                    match = df[df["代码"].astype(str).str.zfill(6) == code]
                    if not match.empty:
                        return int(match.index[0]) + 1
                        
        except Exception:
            pass
        
        # Try concept hot stocks
        try:
            df = ak.stock_hot_rank_detail_em(symbol="即时")
            
            if df is not None and not df.empty:
                if "代码" in df.columns:
                    match = df[df["代码"].astype(str).str.zfill(6) == code]
                    if not match.empty:
                        return int(match.index[0]) + 1
                        
        except Exception:
            pass
            
        return None
        
    except ImportError:
        return None
    except Exception:
        return None


def fetch_stock_comment_sentiment(ticker: str) -> Optional[dict]:
    """
    Fetch stock comment/discussion sentiment metrics.
    
    Args:
        ticker: Stock ticker
    
    Returns:
        Dict with discussion metrics
    """
    try:
        import akshare as ak
        
        code = ticker.split(".")[0] if "." in ticker else ticker
        code = code.zfill(6)
        
        # Try stock hot follow (关注度)
        try:
            df = ak.stock_hot_follow_xq(symbol=code)
            
            if df is not None and not df.empty:
                return {
                    "follow_count": int(df.get("关注人数", [0])[0] if "关注人数" in df.columns else 0),
                    "discuss_count": int(df.get("讨论次数", [0])[0] if "讨论次数" in df.columns else 0),
                    "source": "xueqiu"
                }
        except Exception:
            pass
            
        return None
        
    except ImportError:
        return None
    except Exception:
        return None


def analyze_news_tone_cn(headlines: list[str]) -> str:
    """
    Analyze tone of Chinese news headlines.
    
    Args:
        headlines: List of headline strings
    
    Returns:
        "positive", "negative", "mixed", or "neutral"
    """
    if not headlines:
        return "neutral"
    
    positive_keywords = [
        "上涨", "涨停", "大涨", "突破", "新高", "利好", "增持", "增长",
        "超预期", "盈利", "订单", "获批", "中标", "合作", "签约",
        "政策支持", "龙头", "领涨", "强势", "放量", "资金流入",
        "机构买入", "北向资金", "主力", "看多", "推荐", "买入"
    ]
    
    negative_keywords = [
        "下跌", "跌停", "大跌", "暴跌", "亏损", "利空", "减持", "下调",
        "风险", "警示", "处罚", "违规", "诉讼", "退市", "ST",
        "业绩下滑", "资金流出", "获利了结", "出货", "看空", "卖出",
        "监管", "调查", "质押", "爆仓"
    ]
    
    positive_count = 0
    negative_count = 0
    
    for headline in headlines:
        headline_str = str(headline)
        
        for kw in positive_keywords:
            if kw in headline_str:
                positive_count += 1
                break
        
        for kw in negative_keywords:
            if kw in headline_str:
                negative_count += 1
                break
    
    total = positive_count + negative_count
    
    if total == 0:
        return "neutral"
    
    positive_ratio = positive_count / total
    
    if positive_ratio >= 0.65:
        return "positive"
    elif positive_ratio <= 0.35:
        return "negative"
    else:
        return "mixed"


def compute_sentiment_score_cn(
    ticker: str,
    headlines: Optional[list[str]] = None,
) -> SentimentScoreCN:
    """
    Compute sentiment momentum score (0-10) for a China A-share.
    
    Scoring factors:
    - Hot stock ranking (人气榜): 30%
    - Guba activity (股吧热度): 30%
    - News tone (新闻情绪): 40%
    
    Args:
        ticker: Stock ticker
        headlines: Optional list of recent headlines
    
    Returns:
        SentimentScoreCN with score and evidence
    """
    data_gaps = []
    evidence: SentimentEvidenceCN = {
        "eastmoney_guba": {},
        "xueqiu": {},
        "news_tone": "neutral",
        "data_source": "none",
        "hot_stock_rank": None,
    }
    
    score = 0.0
    cap_applied = None
    sources_found = 0
    
    # Check hot stock ranking
    hot_rank = fetch_hot_stock_rank(ticker)
    if hot_rank is not None:
        sources_found += 1
        evidence["hot_stock_rank"] = hot_rank
        evidence["data_source"] = "hot_rank"
        
        # Score based on ranking (top 10 = max points)
        if hot_rank <= 10:
            score += 3.0
        elif hot_rank <= 30:
            score += 2.0
        elif hot_rank <= 50:
            score += 1.0
        else:
            score += 0.5
    
    # Eastmoney Guba sentiment
    guba_data = fetch_eastmoney_guba_sentiment(ticker)
    if guba_data:
        sources_found += 1
        evidence["eastmoney_guba"] = guba_data
        if evidence["data_source"] == "none":
            evidence["data_source"] = "eastmoney_guba"
        else:
            evidence["data_source"] += "+guba"
        
        # Score based on activity level
        avg_reads = guba_data.get("avg_reads_per_post", 0)
        comments = guba_data.get("total_comments", 0)
        
        if avg_reads >= 10000:
            score += 2.0
        elif avg_reads >= 5000:
            score += 1.5
        elif avg_reads >= 1000:
            score += 1.0
        else:
            score += 0.5
        
        if comments >= 500:
            score += 0.5
    
    # Xueqiu/discussion data
    xq_data = fetch_stock_comment_sentiment(ticker)
    if xq_data:
        sources_found += 1
        evidence["xueqiu"] = xq_data
        if evidence["data_source"] == "none":
            evidence["data_source"] = "xueqiu"
        else:
            evidence["data_source"] += "+xueqiu"
        
        follow_count = xq_data.get("follow_count", 0)
        if follow_count >= 100000:
            score += 1.5
        elif follow_count >= 50000:
            score += 1.0
        elif follow_count >= 10000:
            score += 0.5
    
    # News tone analysis
    if headlines:
        tone = analyze_news_tone_cn(headlines)
        evidence["news_tone"] = tone
        
        if tone == "positive":
            score += 2.5
        elif tone == "mixed":
            score += 1.0
        elif tone == "negative":
            score -= 0.5
        else:
            score += 0.5
    else:
        data_gaps.append("No headlines provided for sentiment analysis")
    
    # Apply cap if insufficient data
    if sources_found == 0:
        data_gaps.append("No sentiment data sources available; score capped at 4.0")
        cap_applied = 4.0
    elif sources_found == 1:
        data_gaps.append("Limited sentiment sources; score capped at 6.0")
        cap_applied = 6.0
    
    if cap_applied is not None:
        score = min(score, cap_applied)
    
    # Ensure score is within bounds
    score = max(0.0, min(10.0, score))
    
    return SentimentScoreCN(
        score=round(score, 2),
        evidence=evidence,
        data_gaps=data_gaps,
        cap_applied=cap_applied,
    )


def get_hot_stocks_cn(top_n: int = 50) -> list[str]:
    """
    Get list of hot stocks from various Chinese platforms.
    
    Args:
        top_n: Number of hot stocks to return
    
    Returns:
        List of ticker symbols
    """
    tickers = []
    
    try:
        import akshare as ak
        
        # Try hot rank
        try:
            df = ak.stock_hot_rank_em()
            if df is not None and not df.empty:
                for _, row in df.head(top_n).iterrows():
                    code = str(row.get("代码", "")).zfill(6)
                    if code.startswith("6"):
                        tickers.append(f"{code}.SH")
                    else:
                        tickers.append(f"{code}.SZ")
        except Exception:
            pass
        
        # Also try concept hot stocks
        try:
            df = ak.stock_hot_rank_detail_em(symbol="即时")
            if df is not None and not df.empty:
                for _, row in df.head(top_n // 2).iterrows():
                    code = str(row.get("代码", "")).zfill(6)
                    if code.startswith("6"):
                        ticker = f"{code}.SH"
                    else:
                        ticker = f"{code}.SZ"
                    if ticker not in tickers:
                        tickers.append(ticker)
        except Exception:
            pass
            
    except ImportError:
        pass
    
    return tickers[:top_n]
