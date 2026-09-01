import importlib.util
from pathlib import Path


VERIFIER = (
    Path(__file__).resolve().parents[1]
    / "outputs/backtest/pit5y_bull_choppy_evidence/verify_evidence.py"
)


def _load_verifier():
    spec = importlib.util.spec_from_file_location("dp_choppy_evidence_verifier", VERIFIER)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_evidence_verifier_detects_six_digit_star_market_symbols():
    verifier = _load_verifier()

    assert verifier._star_market_tickers(
        ["688006.SH", "600688.SH", "300688.SZ", "688009.SH"]
    ) == ["688006.SH", "688009.SH"]
