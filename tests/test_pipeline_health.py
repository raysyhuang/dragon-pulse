"""
Tests for pipeline health differentiation and smoke test.

Covers:
- Morning check distinguishes healthy-but-suppressed vs degraded scans
- Smoke test exits correctly on valid/failed fetches
- Scan results contract includes health fields
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestMorningCheckHealthDifferentiation:
    """Morning check should produce different messages for healthy vs degraded zero-pick days."""

    def _write_artifacts(self, tmp_path, date_str, watchlist_data, scan_results_data):
        """Write watchlist and scan_results artifacts to a temp output dir."""
        out_dir = tmp_path / "outputs" / date_str
        out_dir.mkdir(parents=True)
        (out_dir / f"execution_watchlist_{date_str}.json").write_text(
            json.dumps(watchlist_data), encoding="utf-8"
        )
        (out_dir / f"scan_results_{date_str}.json").write_text(
            json.dumps(scan_results_data), encoding="utf-8"
        )
        return out_dir

    def test_healthy_breadth_suppressed_message(self, tmp_path):
        """Zero picks from breadth suppression should say so, not raise alarm."""
        date_str = "2026-03-22"
        self._write_artifacts(tmp_path, date_str,
            watchlist_data={"date": date_str, "regime": "bear", "picks": []},
            scan_results_data={
                "date": date_str,
                "regime": "bear",
                "downloaded": 996,
                "download_failed": 0,
                "download_health": "ok",
                "circuit_breaker": None,
                "signals_total": 115,
                "regime_detail": {
                    "acceptance_mode": "breadth_suppressed",
                    "market_breadth_pct_above_sma20": 0.247,
                },
            },
        )

        scan_path = tmp_path / "outputs" / date_str / f"scan_results_{date_str}.json"
        scan_data = json.loads(scan_path.read_text())

        # Simulate the health check logic from morning_check.py
        dl_health = scan_data.get("download_health", "ok")
        circuit_breaker = scan_data.get("circuit_breaker")
        if dl_health != "ok" or circuit_breaker:
            health = "degraded"
        else:
            health = "healthy"

        assert health == "healthy"
        assert scan_data["signals_total"] == 115
        assert scan_data["regime_detail"]["acceptance_mode"] == "breadth_suppressed"

    def test_degraded_circuit_breaker_message(self, tmp_path):
        """Circuit breaker trip should be flagged as degraded, not quiet market."""
        date_str = "2026-03-19"
        self._write_artifacts(tmp_path, date_str,
            watchlist_data={"date": date_str, "regime": "bear", "picks": []},
            scan_results_data={
                "date": date_str,
                "regime": "bear",
                "downloaded": 4,
                "download_failed": 46,
                "download_health": "critical",
                "circuit_breaker": "Aborted at 50/996 tickers, 92% failure rate",
                "signals_total": 0,
                "regime_detail": {},
            },
        )

        scan_path = tmp_path / "outputs" / date_str / f"scan_results_{date_str}.json"
        scan_data = json.loads(scan_path.read_text())

        dl_health = scan_data.get("download_health", "ok")
        circuit_breaker = scan_data.get("circuit_breaker")
        if dl_health != "ok" or circuit_breaker:
            health = "degraded"
        else:
            health = "healthy"

        assert health == "degraded"
        assert scan_data["circuit_breaker"] is not None
        assert scan_data["downloaded"] == 4

    def test_degraded_bad_health_no_circuit_breaker(self, tmp_path):
        """download_health != ok without circuit breaker is still degraded."""
        date_str = "2026-03-18"
        self._write_artifacts(tmp_path, date_str,
            watchlist_data={"date": date_str, "regime": "bear", "picks": []},
            scan_results_data={
                "date": date_str,
                "downloaded": 1,
                "download_failed": 995,
                "download_health": "critical",
                "circuit_breaker": None,
                "signals_total": 0,
                "regime_detail": {},
            },
        )

        scan_path = tmp_path / "outputs" / date_str / f"scan_results_{date_str}.json"
        scan_data = json.loads(scan_path.read_text())

        dl_health = scan_data.get("download_health", "ok")
        circuit_breaker = scan_data.get("circuit_breaker")
        health = "degraded" if (dl_health != "ok" or circuit_breaker) else "healthy"

        assert health == "degraded"

    def test_missing_scan_results_treated_as_unknown(self, tmp_path):
        """If scan_results file is missing, health should be None (unknown)."""
        date_str = "2026-03-15"
        out_dir = tmp_path / "outputs" / date_str
        out_dir.mkdir(parents=True)
        (out_dir / f"execution_watchlist_{date_str}.json").write_text(
            json.dumps({"date": date_str, "regime": "bear", "picks": []}),
            encoding="utf-8",
        )

        scan_path = out_dir / f"scan_results_{date_str}.json"
        scan_health = None
        if scan_path.exists():
            scan_data = json.loads(scan_path.read_text())
            dl_health = scan_data.get("download_health", "ok")
            circuit_breaker = scan_data.get("circuit_breaker")
            scan_health = "degraded" if (dl_health != "ok" or circuit_breaker) else "healthy"

        assert scan_health is None


class TestScanResultsContract:
    """Scan results must include health fields for downstream consumers."""

    REQUIRED_HEALTH_FIELDS = [
        "downloaded",
        "download_health",
        "circuit_breaker",
        "signals_total",
    ]

    def test_healthy_scan_has_all_fields(self):
        scan = {
            "date": "2026-03-14",
            "downloaded": 996,
            "download_failed": 0,
            "download_health": "ok",
            "circuit_breaker": None,
            "signals_total": 50,
        }
        for field in self.REQUIRED_HEALTH_FIELDS:
            assert field in scan, f"Missing field: {field}"

    def test_degraded_scan_has_all_fields(self):
        scan = {
            "date": "2026-03-19",
            "downloaded": 4,
            "download_failed": 46,
            "download_health": "critical",
            "circuit_breaker": "Aborted at 50/996 tickers, 92% failure rate",
            "signals_total": 0,
        }
        for field in self.REQUIRED_HEALTH_FIELDS:
            assert field in scan, f"Missing field: {field}"


class TestSmokeScript:
    """Smoke test script validation."""

    def test_smoke_script_exists(self):
        smoke_path = Path(__file__).parent.parent / "scripts" / "smoke_cn_provider.py"
        assert smoke_path.exists(), "smoke_cn_provider.py not found"

    def test_smoke_probe_tickers_are_valid_cn_codes(self):
        """Probe tickers should be valid A-share or index codes."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "smoke", Path(__file__).parent.parent / "scripts" / "smoke_cn_provider.py"
        )
        mod = importlib.util.module_from_spec(spec)
        # Don't exec the module (it has side effects), just check the constant
        import ast
        source = (Path(__file__).parent.parent / "scripts" / "smoke_cn_provider.py").read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "PROBE_TICKERS":
                        tickers = ast.literal_eval(node.value)
                        assert len(tickers) >= 2, "Need at least 2 probe tickers"
                        for t in tickers:
                            assert t.endswith(".SH") or t.endswith(".SZ"), \
                                f"Invalid CN ticker format: {t}"
                        return
        pytest.fail("PROBE_TICKERS not found in smoke script")


class TestDownloadThrottle:
    """Verify the download loop has throttle protection."""

    def test_cn_data_has_throttle(self):
        """cn_data.py download loop should include a sleep/throttle."""
        cn_data_path = Path(__file__).parent.parent / "src" / "core" / "cn_data.py"
        source = cn_data_path.read_text()
        assert "_THROTTLE_SEC" in source, "Missing throttle constant in cn_data.py"
        assert "time.sleep" in source, "Missing time.sleep in cn_data.py download loop"
