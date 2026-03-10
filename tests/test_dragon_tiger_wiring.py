"""Tests for Dragon Tiger / Sector / Sentiment wiring into packets."""

import pytest
import pandas as pd
from src.core.packets import build_weekly_scanner_packet, build_weekly_scanner_packet_cn
from src.core.data_quality import TickerDataQuality, RunDataQuality


class TestBuildWeeklyScannerPacketCN:
    """Verify that enrichment data flows correctly into CN packets."""

    def _base_row(self):
        return pd.Series({
            "ticker": "600000.SH",
            "name": "浦发银行",
            "name_cn": "浦发银行",
            "exchange": "SSE",
            "sector": "银行",
            "current_price": 12.0,
            "market_cap_usd": 100_000_000,
            "avg_dollar_volume_20d": 80_000_000,
            "asof_price_utc": "2025-12-20T00:00:00Z",
            "technical_score": 7.5,
            "technical_evidence": {"rsi14": 62},
            "trading_number": "600000.SH",
        })

    def _empty_news(self):
        return pd.DataFrame(columns=["Ticker", "title", "publisher", "link", "published_utc"])

    def test_dragon_tiger_data_populates_packet(self):
        dt_data = {
            "flow_score": 8.5,
            "net_buy_amount_cny": 50_000_000,
            "buy_amount_cny": 120_000_000,
            "sell_amount_cny": 70_000_000,
            "reason": "涨幅偏离值达7%",
            "trade_date": "2025-12-20",
        }
        packet = build_weekly_scanner_packet_cn(
            ticker="600000.SH",
            row=self._base_row(),
            news_df=self._empty_news(),
            dragon_tiger_data=dt_data,
        )
        assert packet["dragon_tiger"]["on_list"] is True
        assert packet["dragon_tiger"]["flow_score"] == 8.5
        assert packet["dragon_tiger"]["net_buy_cny"] == 50_000_000
        assert packet["market_activity"]["on_dragon_tiger_list"] is True
        assert packet["market_activity_available"] is True

    def test_sector_data_populates_packet(self):
        sector_data = {
            "sector": "银行",
            "momentum_score": 7.0,
            "return_1d": 1.2,
            "return_5d": 3.5,
            "trend": "accelerating",
        }
        packet = build_weekly_scanner_packet_cn(
            ticker="600000.SH",
            row=self._base_row(),
            news_df=self._empty_news(),
            sector_data=sector_data,
        )
        assert packet["sector_rotation"]["sector"] == "银行"
        assert packet["sector_rotation"]["sector_momentum_score"] == 7.0
        assert packet["sector_rotation"]["is_hot_sector"] is True

    def test_sentiment_data_populates_packet(self):
        sent_data = {
            "score": 6.5,
            "hot_stock_rank": 15,
            "eastmoney_guba": {"post_count": 50},
            "news_tone": "positive",
        }
        packet = build_weekly_scanner_packet_cn(
            ticker="600000.SH",
            row=self._base_row(),
            news_df=self._empty_news(),
            sentiment_data=sent_data,
        )
        assert packet["sentiment_cn"]["score"] == 6.5
        assert packet["sentiment_cn"]["hot_stock_rank"] == 15
        assert packet["market_activity"]["sentiment_score"] == 6.5

    def test_no_enrichment_data_gives_nulls(self):
        packet = build_weekly_scanner_packet_cn(
            ticker="600000.SH",
            row=self._base_row(),
            news_df=self._empty_news(),
        )
        assert packet["dragon_tiger"]["on_list"] is False
        assert packet["sector_rotation"]["sector_momentum_score"] is None
        assert packet["sentiment_cn"]["score"] is None
        assert packet["market_activity_available"] is False

    def test_all_enrichment_sources_combined(self):
        packet = build_weekly_scanner_packet_cn(
            ticker="600000.SH",
            row=self._base_row(),
            news_df=self._empty_news(),
            dragon_tiger_data={"flow_score": 7, "net_buy_amount_cny": 20_000_000, "buy_amount_cny": 50_000_000, "sell_amount_cny": 30_000_000, "reason": "test", "trade_date": "2025-12-20"},
            sector_data={"sector": "银行", "momentum_score": 6.5, "return_1d": 0.5, "return_5d": 2.0, "trend": "steady"},
            sentiment_data={"score": 5.0, "hot_stock_rank": 30, "eastmoney_guba": {}, "news_tone": "mixed"},
        )
        assert packet["market_activity_available"] is True
        assert packet["dragon_tiger"]["on_list"] is True
        assert packet["sector_rotation"]["sector_momentum_score"] == 6.5
        assert packet["sentiment_cn"]["score"] == 5.0


class TestDataQuality:
    """Tests for data quality tracking."""

    def test_completeness_all_sources(self):
        tq = TickerDataQuality(
            ticker="600000.SH",
            has_price=True,
            has_dragon_tiger=True,
            has_sector=True,
            has_sentiment=True,
            has_news=True,
        )
        assert tq.completeness_score == 1.0

    def test_completeness_partial(self):
        tq = TickerDataQuality(
            ticker="600000.SH",
            has_price=True,
            has_dragon_tiger=False,
            has_sector=False,
            has_sentiment=False,
            has_news=True,
        )
        assert tq.completeness_score == pytest.approx(0.4)

    def test_completeness_none(self):
        tq = TickerDataQuality(ticker="600000.SH")
        assert tq.completeness_score == 0.0

    def test_to_dict(self):
        tq = TickerDataQuality(ticker="600000.SH", has_price=True)
        d = tq.to_dict()
        assert d["ticker"] == "600000.SH"
        assert d["has_price"] is True
        assert d["has_dragon_tiger"] is False
        assert d["completeness_score"] == 0.2

    def test_run_data_quality_summary(self):
        rdq = RunDataQuality()
        rdq.add(TickerDataQuality(ticker="A", has_price=True, has_news=True))
        rdq.add(TickerDataQuality(ticker="B", has_price=True, has_dragon_tiger=True, has_sector=True, has_sentiment=True, has_news=True))
        summary = rdq.summary()
        assert summary["n_tickers"] == 2
        assert summary["avg_completeness"] == pytest.approx(0.7)
        assert summary["source_coverage"]["price"] == 1.0
        assert summary["source_coverage"]["dragon_tiger"] == 0.5
