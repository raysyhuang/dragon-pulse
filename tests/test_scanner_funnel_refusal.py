"""The funnel's refusal must reach the artifact, not kill the scan.

`_sector_cap` raises when industry metadata is missing rather than guessing
(see src/pipelines/funnel.py). That is correct. But `get_cn_basic_info` is
best-effort and returns {} when the provider call fails, so live can hit it.
An unhandled raise writes no artifact and sends no Telegram alert, which looks
exactly like a quiet no-pick day — the ambiguity the refusal exists to remove.
"""
from __future__ import annotations

import pytest

from src.pipelines.funnel import _sector_cap


class _Sig:
    def __init__(self, ticker: str, score: float = 95.0) -> None:
        self.ticker = ticker
        self.score = score
        self.components = {}


def _cands() -> list[tuple[str, _Sig]]:
    return [("alpha", _Sig("600000.SH")), ("alpha", _Sig("000001.SZ"))]


def test_sector_cap_applies_with_full_metadata():
    info = {"600000.SH": {"industry": "银行"}, "000001.SZ": {"industry": "保险"}}
    assert len(_sector_cap(_cands(), info, 1)) == 2


def test_sector_cap_refuses_on_partial_metadata():
    info = {"600000.SH": {"industry": "银行"}, "000001.SZ": {"industry": None}}
    with pytest.raises(ValueError, match="cannot be applied"):
        _sector_cap(_cands(), info, 1)


def test_sector_cap_refuses_on_empty_info_map():
    """get_cn_basic_info returns {} when the provider call fails."""
    with pytest.raises(ValueError, match="cannot be applied"):
        _sector_cap(_cands(), {}, 1)


def test_sector_cap_allows_single_candidate_without_metadata():
    """One candidate cannot violate a cap of 1, so there is nothing to refuse."""
    assert len(_sector_cap([("alpha", _Sig("600000.SH"))], {}, 1)) == 1


def test_run_funnel_guarded_converts_refusal_to_zero_picks(monkeypatch):
    """A refusal degrades to zero picks + a recorded error, never propagates."""
    import src.pipelines.scanner as scanner

    def _refuse(*_a, **_k):
        raise ValueError("sector cap of 1 cannot be applied: no industry for 2 candidate(s)")

    monkeypatch.setattr(scanner, "run_selection_funnel", _refuse)

    errors: list[str] = []
    regime_detail: dict = {}
    stage = scanner.run_funnel_guarded(
        [], "bull", 0.5, {}, universe_size=10,
        data_map={}, info_map={}, errors=errors, regime_detail=regime_detail,
    )

    assert stage.final_picks == []
    assert stage.regime == "bull"
    assert any("sector cap" in e for e in errors)
    assert "selection_error" in regime_detail


def test_run_funnel_guarded_passes_through_on_success(monkeypatch):
    """A healthy funnel result is returned untouched and records no error."""
    import src.pipelines.scanner as scanner
    from src.pipelines.funnel import StageResult

    sentinel = StageResult(final_picks=[("alpha", _Sig("600000.SH"))], regime="bull")
    monkeypatch.setattr(scanner, "run_selection_funnel", lambda *a, **k: sentinel)

    errors: list[str] = []
    regime_detail: dict = {}
    stage = scanner.run_funnel_guarded(
        [], "bull", 0.5, {}, universe_size=10,
        data_map={}, info_map={}, errors=errors, regime_detail=regime_detail,
    )

    assert stage is sentinel
    assert errors == []
    assert regime_detail == {}


def test_run_funnel_guarded_does_not_swallow_unrelated_errors(monkeypatch):
    """Only the metadata refusal is handled; real bugs must still surface."""
    import src.pipelines.scanner as scanner

    def _bug(*_a, **_k):
        raise KeyError("genuine bug")

    monkeypatch.setattr(scanner, "run_selection_funnel", _bug)

    with pytest.raises(KeyError):
        scanner.run_funnel_guarded(
            [], "bull", 0.5, {}, universe_size=10,
            data_map={}, info_map={}, errors=[], regime_detail={},
        )
