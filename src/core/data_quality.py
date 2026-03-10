"""
Data Quality Tracking

Tracks per-ticker data completeness across all data sources
(price, dragon_tiger, sector, sentiment, news) and provides
aggregate run-level quality metrics.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TickerDataQuality:
    """Tracks which data sources were available for a single ticker."""

    ticker: str
    has_price: bool = False
    has_dragon_tiger: bool = False
    has_sector: bool = False
    has_sentiment: bool = False
    has_news: bool = False

    _SOURCES = ("has_price", "has_dragon_tiger", "has_sector", "has_sentiment", "has_news")

    @property
    def completeness_score(self) -> float:
        """0-1 fraction of data sources that were available."""
        total = len(self._SOURCES)
        found = sum(1 for s in self._SOURCES if getattr(self, s, False))
        return found / total if total > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "has_price": self.has_price,
            "has_dragon_tiger": self.has_dragon_tiger,
            "has_sector": self.has_sector,
            "has_sentiment": self.has_sentiment,
            "has_news": self.has_news,
            "completeness_score": round(self.completeness_score, 2),
        }


@dataclass
class RunDataQuality:
    """Aggregates data quality across all tickers in a run."""

    ticker_qualities: list[TickerDataQuality] = field(default_factory=list)

    def add(self, tq: TickerDataQuality) -> None:
        self.ticker_qualities.append(tq)

    @property
    def avg_completeness(self) -> float:
        if not self.ticker_qualities:
            return 0.0
        return sum(tq.completeness_score for tq in self.ticker_qualities) / len(self.ticker_qualities)

    def source_coverage(self) -> dict[str, float]:
        """Fraction of tickers that had each data source."""
        if not self.ticker_qualities:
            return {}
        n = len(self.ticker_qualities)
        return {
            "price": sum(1 for tq in self.ticker_qualities if tq.has_price) / n,
            "dragon_tiger": sum(1 for tq in self.ticker_qualities if tq.has_dragon_tiger) / n,
            "sector": sum(1 for tq in self.ticker_qualities if tq.has_sector) / n,
            "sentiment": sum(1 for tq in self.ticker_qualities if tq.has_sentiment) / n,
            "news": sum(1 for tq in self.ticker_qualities if tq.has_news) / n,
        }

    def summary(self) -> dict:
        return {
            "n_tickers": len(self.ticker_qualities),
            "avg_completeness": round(self.avg_completeness, 3),
            "source_coverage": {k: round(v, 3) for k, v in self.source_coverage().items()},
        }
