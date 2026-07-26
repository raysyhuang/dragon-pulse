#!/usr/bin/env python3
"""Purged walk-forward ML experiment — the honest test of the model-class hypothesis.

Alpha158-style daily features -> LightGBM -> rolling walk-forward with an embargo sized to
the label horizon (no leakage) -> out-of-sample rank-IC/ICIR + a cost-aware top-decile
portfolio vs CSI300. Runs on the daily cache from pull_daily.py.

If the model produces ~0 out-of-sample IC and a portfolio that doesn't beat CSI300 net of
costs across folds, the model-class hypothesis fails (consistent with the factor findings).
If it shows robust positive IC + a beating portfolio across walk-forward folds, that's the
first thing to work — and would warrant a PIT-universe, deeper-model follow-up.

Usage: python scripts/qlib_experiment/wf_ml.py [--horizon 10]
"""
from __future__ import annotations
import argparse, glob
from pathlib import Path
import numpy as np, pandas as pd
import lightgbm as lgb
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parent.parent.parent
CACHE = ROOT / "outputs" / "qlib_experiment" / "daily_cache"
COST_BPS = 15.0  # per side (commission+stamp+slippage)


def features(df: pd.DataFrame) -> pd.DataFrame:
    c, v, h, l, o = (pd.to_numeric(df[k], errors="coerce") for k in ("close", "vol", "high", "low", "open"))
    ret = c.pct_change()
    f = {}
    for w in (1, 5, 10, 20, 60):
        f[f"ret{w}"] = c.pct_change(w)
    for w in (5, 10, 20, 60):
        f[f"ma{w}"] = c / c.rolling(w).mean() - 1
        f[f"std{w}"] = ret.rolling(w).std()
        f[f"vma{w}"] = v / v.rolling(w).mean().replace(0, np.nan) - 1
    f["hl"] = (h - l) / c
    f["clpos"] = (c - l) / (h - l).replace(0, np.nan)
    f["gap"] = o / c.shift(1) - 1
    f["mom_acc"] = c.pct_change(10) - c.pct_change(20)
    out = pd.DataFrame(f, index=df.index)
    out["ticker"] = df["ticker"].values
    out["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").values
    out["close"] = c.values
    return out


def build_panel(horizon: int) -> pd.DataFrame:
    rows = []
    for fp in sorted(glob.glob(str(CACHE / "*.csv"))):
        df = pd.read_csv(fp)
        if len(df) < 300:
            continue
        df["ticker"] = Path(fp).stem
        df = df.sort_values("trade_date").reset_index(drop=True)
        ft = features(df)
        c = ft["close"]
        ft["label"] = c.shift(-1 - horizon) / c.shift(-1) - 1  # enter t+1, exit t+1+H
        rows.append(ft)
    p = pd.concat(rows, ignore_index=True).dropna(subset=["label"])
    feat_cols = [x for x in p.columns if x not in ("ticker", "date", "close", "label")]
    # cross-sectional rank-normalize features per date (qlib CSRankNorm)
    p[feat_cols] = p.groupby("date")[feat_cols].rank(pct=True)
    p = p.dropna(subset=feat_cols, how="any")
    return p, feat_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=10)
    args = ap.parse_args()
    H = args.horizon

    p, feat_cols = build_panel(H)
    dates = np.array(sorted(p["date"].unique()))
    print(f"panel: {len(p):,} rows, {p['ticker'].nunique()} tickers, {p['date'].nunique()} days "
          f"{pd.Timestamp(dates[0]).date()}..{pd.Timestamp(dates[-1]).date()}, {len(feat_cols)} features, horizon={H}\n")

    csi = pd.read_csv(ROOT / "outputs" / "paper_lab" / "index_CSI300.csv", parse_dates=["trade_date"]).set_index("trade_date")["close"]

    TRAIN_D, TEST_D, EMBARGO = 252 * 3, 126, H + 3
    all_ic, port_ret, port_dates = [], [], []
    fold = 0
    start = TRAIN_D + EMBARGO
    while start + TEST_D <= len(dates):
        tr = dates[max(0, start - EMBARGO - TRAIN_D):start - EMBARGO]
        te = dates[start:start + TEST_D]
        trd = p[p["date"].isin(tr)]
        ted = p[p["date"].isin(te)]
        if len(trd) < 5000 or len(ted) < 500:
            start += TEST_D; continue
        model = lgb.LGBMRegressor(n_estimators=200, num_leaves=31, learning_rate=0.05,
                                  subsample=0.8, colsample_bytree=0.8, min_child_samples=50,
                                  n_jobs=-1, verbosity=-1)
        model.fit(trd[feat_cols], trd["label"])
        ted = ted.copy()
        ted["pred"] = model.predict(ted[feat_cols])
        # daily rank-IC
        for d, g in ted.groupby("date"):
            if len(g) >= 20:
                ic = spearmanr(g["pred"], g["label"]).correlation
                if not np.isnan(ic):
                    all_ic.append(ic)
        # non-overlapping H-day portfolio: long top decile by pred
        te_sorted = sorted(ted["date"].unique())
        for k in range(0, len(te_sorted), H):
            d = te_sorted[k]
            g = ted[ted["date"] == d]
            n = max(5, int(len(g) * 0.10))
            top = g.nlargest(n, "pred")
            r = top["label"].mean()  # label already = forward H-day return
            if not np.isnan(r):
                port_ret.append(r - 2 * COST_BPS / 10000.0)  # round-trip cost
                port_dates.append(pd.Timestamp(d))
        fold += 1
        start += TEST_D
    print(f"walk-forward folds: {fold}, embargo={EMBARGO}d\n")

    ic = np.array(all_ic)
    print("=== out-of-sample predictive power ===")
    print(f"  mean rank-IC: {ic.mean():+.4f}   ICIR: {ic.mean()/ic.std():+.3f}   %positive: {(ic>0).mean()*100:.0f}%   (n={len(ic)} days)")
    print(f"  (interesting if IC > ~0.03 and ICIR > ~0.3, robustly)")

    pr = np.array(port_ret)
    eq = np.cumprod(1 + pr)
    yrs = (port_dates[-1] - port_dates[0]).days / 365.25
    dd = (1 - eq / np.maximum.accumulate(eq)).max()
    ppy = len(pr) / yrs
    # CSI300 over same rebalance dates
    cr = []
    for i in range(len(port_dates) - 1):
        c0 = csi[csi.index <= port_dates[i]].iloc[-1]; c1 = csi[csi.index <= port_dates[i + 1]].iloc[-1]
        cr.append(c1 / c0 - 1)
    ceq = np.cumprod(1 + np.array(cr))
    print("\n=== top-decile portfolio (net of costs) vs CSI300, walk-forward OOS ===")
    print(f"  strategy: CAGR {(eq[-1]**(1/yrs)-1)*100:+.1f}%  Sharpe {pr.mean()/pr.std()*np.sqrt(ppy):+.2f}  maxDD {dd*100:.1f}%  ({len(pr)} trades)")
    print(f"  CSI300  : CAGR {(ceq[-1]**(1/yrs)-1)*100:+.1f}%  Sharpe {np.mean(cr)/np.std(cr)*np.sqrt(ppy):+.2f}")


if __name__ == "__main__":
    main()
