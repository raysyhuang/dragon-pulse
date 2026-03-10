"""Tests for LLM output validation, post-LLM gate, and fallback."""

import pytest
from src.core.llm import validate_llm_ranking, apply_post_llm_gate, _build_fallback_top5


class TestValidateLlmRanking:
    """Tests for validate_llm_ranking()."""

    def _input_tickers(self):
        return ["600000.SH", "000001.SZ", "601398.SH", "300750.SZ", "600519.SH"]

    def test_valid_output_passes(self):
        result = {
            "top5": [
                {"rank": 1, "ticker": "600000.SH", "scores": {"technical": 8, "catalyst": 7, "market_activity": 6}, "composite_score": 7.2},
                {"rank": 2, "ticker": "000001.SZ", "scores": {"technical": 7, "catalyst": 6, "market_activity": 5}, "composite_score": 6.2},
            ]
        }
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert len(validated["top5"]) == 2
        assert len(warnings) == 0

    def test_unknown_ticker_dropped(self):
        result = {
            "top5": [
                {"rank": 1, "ticker": "FAKE.SH", "scores": {"technical": 8}, "composite_score": 8.0},
                {"rank": 2, "ticker": "600000.SH", "scores": {"technical": 7}, "composite_score": 7.0},
            ]
        }
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert len(validated["top5"]) == 1
        assert validated["top5"][0]["ticker"] == "FAKE.SH" or validated["top5"][0]["ticker"] == "600000.SH"
        # The FAKE.SH entry should be dropped
        tickers_in_result = [e["ticker"] for e in validated["top5"]]
        assert "FAKE.SH" not in tickers_in_result
        assert any("not in input universe" in w for w in warnings)

    def test_scores_clamped(self):
        result = {
            "top5": [
                {"rank": 1, "ticker": "600000.SH", "scores": {"technical": 15, "catalyst": -2, "market_activity": 6}, "composite_score": 12.0},
            ]
        }
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        entry = validated["top5"][0]
        assert entry["scores"]["technical"] == 10.0
        assert entry["scores"]["catalyst"] == 0.0
        assert entry["composite_score"] == 10.0
        assert len(warnings) >= 2  # At least tech + catalyst + composite clamped

    def test_top5_not_list(self):
        result = {"top5": "invalid"}
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert validated["top5"] == []
        assert any("not a list" in w for w in warnings)

    def test_truncates_to_5(self):
        entries = [
            {"rank": i, "ticker": t, "scores": {"technical": 5}, "composite_score": 5.0}
            for i, t in enumerate(self._input_tickers() + ["601398.SH"], 1)
        ]
        result = {"top5": entries}
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert len(validated["top5"]) <= 5

    def test_missing_ticker_skipped(self):
        result = {
            "top5": [
                {"rank": 1, "scores": {"technical": 8}},
            ]
        }
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert len(validated["top5"]) == 0
        assert any("no ticker" in w for w in warnings)

    def test_case_insensitive_ticker_match(self):
        result = {
            "top5": [
                {"rank": 1, "ticker": "600000.sh", "scores": {"technical": 8}, "composite_score": 8.0},
            ]
        }
        validated, warnings = validate_llm_ranking(result, self._input_tickers())
        assert len(validated["top5"]) == 1
        assert len(warnings) == 0


class TestPostLlmGate:
    """Tests for apply_post_llm_gate()."""

    def _make_entry(self, ticker: str, composite: float, confidence: str) -> dict:
        return {
            "rank": 1,
            "ticker": ticker,
            "composite_score": composite,
            "confidence": confidence,
            "scores": {"technical": 8, "catalyst": 7, "market_activity": 6},
        }

    def test_high_confidence_high_score_passes(self):
        result = {"top5": [self._make_entry("600000.SH", 7.5, "HIGH")]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert len(filtered["top5"]) == 1
        assert len(rejections) == 0

    def test_low_composite_rejected(self):
        result = {"top5": [self._make_entry("600000.SH", 5.0, "HIGH")]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert len(filtered["top5"]) == 0
        assert len(rejections) == 1
        assert "composite" in rejections[0]

    def test_speculative_rejected_when_min_is_medium(self):
        result = {"top5": [self._make_entry("600000.SH", 7.5, "SPECULATIVE")]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert len(filtered["top5"]) == 0
        assert "confidence" in rejections[0]

    def test_speculative_passes_when_min_is_speculative(self):
        result = {"top5": [self._make_entry("600000.SH", 7.5, "SPECULATIVE")]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="SPECULATIVE")
        assert len(filtered["top5"]) == 1

    def test_mixed_entries_partial_rejection(self):
        result = {"top5": [
            self._make_entry("600000.SH", 8.0, "HIGH"),
            self._make_entry("000001.SZ", 5.0, "MEDIUM"),
            self._make_entry("601398.SH", 7.0, "SPECULATIVE"),
            self._make_entry("300750.SZ", 7.5, "MEDIUM"),
        ]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert len(filtered["top5"]) == 2
        tickers = [e["ticker"] for e in filtered["top5"]]
        assert "600000.SH" in tickers
        assert "300750.SZ" in tickers
        assert len(rejections) == 2

    def test_reranking_after_rejection(self):
        result = {"top5": [
            self._make_entry("A.SH", 5.0, "HIGH"),   # rejected: low score
            self._make_entry("B.SH", 8.0, "HIGH"),    # kept
            self._make_entry("C.SH", 7.0, "MEDIUM"),  # kept
        ]}
        filtered, _ = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert filtered["top5"][0]["rank"] == 1
        assert filtered["top5"][0]["ticker"] == "B.SH"
        assert filtered["top5"][1]["rank"] == 2

    def test_all_rejected_returns_empty(self):
        result = {"top5": [
            self._make_entry("A.SH", 4.0, "SPECULATIVE"),
            self._make_entry("B.SH", 3.0, "SPECULATIVE"),
        ]}
        filtered, rejections = apply_post_llm_gate(result, min_composite_score=6.5, min_confidence="MEDIUM")
        assert len(filtered["top5"]) == 0
        assert len(rejections) == 2

    def test_gate_rejections_stored_in_result(self):
        result = {"top5": [self._make_entry("A.SH", 4.0, "HIGH")]}
        filtered, _ = apply_post_llm_gate(result, min_composite_score=6.5)
        assert "gate_rejections" in filtered


class TestBuildFallbackTop5:
    """Tests for _build_fallback_top5()."""

    def test_ranks_by_technical_score(self):
        packets = [
            {"ticker": "A.SH", "name": "A", "technical_score": 6},
            {"ticker": "B.SH", "name": "B", "technical_score": 9},
            {"ticker": "C.SH", "name": "C", "technical_score": 7},
        ]
        top5 = _build_fallback_top5(packets)
        assert len(top5) == 3
        assert top5[0]["ticker"] == "B.SH"
        assert top5[1]["ticker"] == "C.SH"
        assert top5[2]["ticker"] == "A.SH"
        assert all(e["confidence"] == "FALLBACK" for e in top5)

    def test_limits_to_5(self):
        packets = [{"ticker": f"{i}.SH", "name": str(i), "technical_score": i} for i in range(10)]
        top5 = _build_fallback_top5(packets)
        assert len(top5) == 5

    def test_empty_packets(self):
        assert _build_fallback_top5([]) == []
