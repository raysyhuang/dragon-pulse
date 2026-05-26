import pandas as pd

from src.signals.sniper import score_sniper


def _sniper_df() -> pd.DataFrame:
    n = 70
    close = [10 + i * 0.02 for i in range(n)]
    volume = [2_000_000 - i * 5_000 for i in range(n - 1)] + [4_000_000]
    return pd.DataFrame({
        "close": close,
        "volume": volume,
        "BBB_20_2.0": [0.20] * (n - 1) + [0.01],
    })


def test_score_sniper_sets_configurable_max_entry_ceiling():
    df = _sniper_df()
    features = {
        "close": 11.38,
        "atr_14": 0.4,
        "atr_pct": 3.6,
        "vol_sma_20": 2_000_000,
        "roc_10": 4.0,
        "pct_above_sma50": 5.0,
        "pct_above_sma200": 8.0,
        "sma_50": 10.8,
        "sma_200": 9.7,
        "rsi_14": 55.0,
    }
    csi300 = pd.DataFrame({"close": [100.0] * 10 + [101.0]})

    signal = score_sniper(
        ticker="600118.SH",
        df=df,
        features=features,
        regime="bull",
        csi300_df=csi300,
        max_entry_pct=0.02,
    )

    assert signal is not None
    assert signal.entry_price == 11.38
    assert signal.max_entry_price == 11.61
