#!/usr/bin/env python3
"""Unit tests for Telegram tracking implementation."""

import os
import sys
from pathlib import Path
from unittest.mock import patch

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.core.alerts import AlertConfig, send_run_summary_alert


def test_telegram_tracking():
    """Telegram sends create markers, skip duplicates, and suppress retries."""
    os.environ["GITHUB_WORKFLOW"] = "Test Workflow"
    os.environ["GITHUB_RUN_ID"] = "test_12345"
    os.environ["GITHUB_RUN_ATTEMPT"] = "1"
    os.environ["GITHUB_SHA"] = "abc123def456789"
    os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
    os.environ["TELEGRAM_CHAT_ID"] = "test-chat"

    config = AlertConfig(
        enabled=True,
        channels=["telegram"],
    )
    test_date = "2026-01-30"
    marker_path = Path(f"outputs/{test_date}/.telegram_sent_test_12345_1.txt")
    marker_path_attempt2 = Path(f"outputs/{test_date}/.telegram_sent_test_12345_2.txt")
    marker_path_new = Path(f"outputs/{test_date}/.telegram_sent_test_67890_1.txt")

    for marker in [marker_path, marker_path_attempt2, marker_path_new]:
        if marker.exists():
            marker.unlink()

    with patch("src.core.alerts._forward_to_mas_log"), patch(
        "src.core.alerts.AlertManager._send_telegram_message", return_value=True
    ) as mock_send:
        results = send_run_summary_alert(
            date_str=test_date,
            weekly_count=5,
            pro30_count=10,
            movers_count=8,
            overlaps={
                "all_three": ["AAPL", "MSFT"],
                "weekly_pro30": ["GOOGL"],
                "weekly_movers": ["TSLA"],
                "pro30_movers": [],
            },
            config=config,
            weekly_top5_data=[
                {
                    "ticker": "AAPL",
                    "rank": 1,
                    "composite_score": 8.5,
                    "confidence": "HIGH",
                    "name": "Apple Inc.",
                },
                {
                    "ticker": "MSFT",
                    "rank": 2,
                    "composite_score": 7.8,
                    "confidence": "MEDIUM",
                    "name": "Microsoft Corporation",
                },
            ],
            hybrid_top3=[
                {
                    "ticker": "AAPL",
                    "hybrid_score": 5.1,
                    "sources": ["Swing(1)", "Pro30"],
                    "rank": 1,
                    "confidence": "HIGH",
                },
            ],
            model_health={
                "status": "GOOD",
                "hit_rate": 0.35,
                "win_rate": 0.28,
                "strategies": [
                    {"name": "Swing", "hit_rate": 0.38, "n": 50},
                    {"name": "Pro30", "hit_rate": 0.33, "n": 45},
                ]
            },
            primary_label="Swing",
            regime="Bull",
        )
        assert results["telegram"] is True
        assert mock_send.called
        assert marker_path.exists()

        mock_send.reset_mock()
        results2 = send_run_summary_alert(
            date_str=test_date,
            weekly_count=5,
            pro30_count=10,
            movers_count=8,
            overlaps={"all_three": [], "weekly_pro30": [], "weekly_movers": [], "pro30_movers": []},
            config=config,
            primary_label="Swing",
        )
        assert results2["telegram"] is True
        assert not mock_send.called

        os.environ["GITHUB_RUN_ATTEMPT"] = "2"
        mock_send.reset_mock()
        results3 = send_run_summary_alert(
            date_str=test_date,
            weekly_count=5,
            pro30_count=10,
            movers_count=8,
            overlaps={"all_three": [], "weekly_pro30": [], "weekly_movers": [], "pro30_movers": []},
            config=config,
            primary_label="Swing",
        )
        assert results3["telegram"] is True
        assert not mock_send.called
        assert not marker_path_attempt2.exists()

        os.environ["GITHUB_RUN_ID"] = "test_67890"
        os.environ["GITHUB_RUN_ATTEMPT"] = "1"
        mock_send.reset_mock()
        results4 = send_run_summary_alert(
            date_str=test_date,
            weekly_count=5,
            pro30_count=10,
            movers_count=8,
            overlaps={"all_three": [], "weekly_pro30": [], "weekly_movers": [], "pro30_movers": []},
            config=config,
            primary_label="Swing",
        )
        assert results4["telegram"] is True
        assert mock_send.called
        assert marker_path_new.exists()

    for marker in [marker_path, marker_path_attempt2, marker_path_new]:
        if marker.exists():
            marker.unlink()
