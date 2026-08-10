# Is There an Edge? — Independent Re-Test of the Surviving Lead (2026-08-10)

**Status:** research / paper-only. Changes no selector, cron, execution rule, or order authority.
**Reproduce:** `python scripts/edge_test_trend_timing.py` — reads cached index CSVs, no provider calls.

## Question

The repository's alpha hunt killed every stock-selection candidate: momentum, value, low-vol, short-term reversal, multifactor, IVOL, northbound accumulation, crowdedness. One lead survived — ChiNext 50/200 trend timing — but it was measured with the harness the infrastructure track was built to replace.

So the question is not "does 50/200 look good." It is **"does it survive the specific failure modes that killed everything else."**

## Method

Long-or-cash, no leverage, no shorting. Signal from `close[t]` sets the position **held over t+1** — the position series is shifted one bar before multiplying by the forward return, so lookahead is structurally impossible rather than merely avoided. Costs 5 bps/side on every position change. Cash earns 1.8%/yr. Kill tests preregistered in the script docstring before any output was inspected.

## Results

### It survived all three kill tests

**K1 — window selection** (this is what killed low-vol: Sharpe 0.48 → 0.42 at 6y → 0.11 at 7y). 36 start quarters, 2014Q1–2022Q4, all ending 2026-07-24:

- cuts drawdown in **36/36** starts
- beats buy & hold on Sharpe in **33/36**; worst case −0.03
- delta Sharpe median **+0.15**, range −0.03 to +0.30

**K2 — parameter selection.** Full MA grid, 29 cells:

- cuts drawdown in **29/29** cells
- beats B&H Sharpe in 20/29
- 50/200 sits at the **83rd percentile — a plateau, not a peak**

The best cell is 50/250 at Sharpe 0.81. **Do not switch to it.** Selecting the grid maximum after seeing the grid is exactly the overfit this test exists to detect. The plateau is the finding; the peak is noise.

**K3 — index specificity.** Sharpe improves on **3/3** indices. Split-sample: both halves work (delta Sharpe +0.28 first half, +0.19 second).

Cost sensitivity: survives to 50 bps/side (Sharpe 0.41 vs B&H 0.38) at 7.9 side-trades/yr.

### Total-return correction

Index series are price-return, so dividends are missing and buy & hold is understated. Dividends accrue to the holder, so B&H earns the full yield while the timed arm earns it only while invested. The decisive number is the **break-even yield at which the advantage vanishes**:

| Index | CAGR break-even | Sharpe break-even | Actual yield | Verdict |
|---|---|---|---|---|
| **ChiNext** | **4.95%** | none up to 12% | ~0.3–0.6% | **survives, ~8× margin** |
| CSI300 | 0.00% | 9.25% | ~2.0–3.0% | loses return even at zero yield |
| CSI500 | 0.65% | 7.30% | ~1.0–1.5% | marginally loses return |

At a realistic 0.5% ChiNext yield, over 2014-10 → 2026-07:

| Arm | CAGR | Sharpe | maxDD |
|---|---|---|---|
| ChiNext 50/200 timed | **+11.09%** | **0.70** | **−37.3%** |
| ChiNext buy & hold | +7.82% | 0.40 | −69.2% |

## What the edge actually is

**A robust drawdown/Sharpe overlay on one index. Not stock selection, and not a broad return engine.**

Bear episodes — the regimes the frozen B1 baseline contains none of:

| Episode | Timed | B&H | Saved | Exposure |
|---|---|---|---|---|
| 2015 crash | −25.0% | −51.3% | **+26.4%** | 9% |
| 2018 bear | +0.9% | −29.6% | **+30.5%** | 7% |
| 2021–22 bear | −2.4% | −34.7% | **+32.2%** | 3% |
| 2023–24 slide | +1.4% | −31.3% | **+32.7%** | 0% |

And the price paid, which must be stated with equal prominence — it re-enters late:

| Rally | Timed | B&H | Cost |
|---|---|---|---|
| 2019 recovery | −2.9% | +33.7% | **−36.6%** |
| 2020 rally | +51.6% | +82.4% | −30.7% |
| 2024–25 rally | +30.3% | +107.6% | **−77.3%** |

You forgo 30–77% of a rally to avoid 26–33% of a crash. On ChiNext the trade has been worth it on both return and risk. On CSI300 and CSI500 it costs return and buys only drawdown reduction.

## Limits

1. **Not a test of stock selection.** Dragon Pulse's selector is untouched by this. The prior evidence against selection stands and this does not weaken it.
2. **Index-level, so it needs none of the Tasks 1–5 machinery** — no cross-section means no survivorship and no PIT-universe problem. That is a strength of the result, not a criticism of the infrastructure, which remains required for any selection claim.
3. **ETF implementation is unmodelled**: tracking error, premium/discount, and real spreads on 159915 are not in these numbers. The 50 bps/side sensitivity gives headroom but is not a substitute for measurement.
4. **One index, one country, one 11.8-year sample.** Robust within that sample; still a single history.
5. Cash rate is assumed constant at 1.8%.

## Recommendation

Paper-track ChiNext 50/200 as a labelled parallel sleeve. It is the only candidate in this repository with 11.8 years and four bear markets behind it, it needs one ETF and ~8 side-trades a year, and it survived every test that killed the alternatives.

Hold 50/200 rather than the grid-best 50/250, and report the plateau rather than the peak.

This does not authorise a production selector change, and it is not evidence that Dragon Pulse's stock selection works.

---

## Addendum — full history and a genuine out-of-sample window

The original test was capped by the cached CSVs, which begin 2014-01-02. Fetching the
index directly returns history from **2010-05-31**, so roughly three and a half further
years were available and untested. Re-running on the full series:

**Full history 2010-05 → 2026-08** (0.5% dividends, 1.8% cash, 5 bps/side):

| Arm | CAGR | Sharpe | maxDD |
|---|---|---|---|
| ChiNext 50/200 timed | **+11.64%** | **0.73** | **−37.3%** |
| Buy & hold | +9.13% | 0.44 | −69.2% |

**K1 extended to 44 start quarters (2012Q1–2022Q4):**

- cuts drawdown in **44/44**
- beats buy & hold on Sharpe in **44/44** — median +0.21, **worst case +0.02**
- beats on CAGR in 35/44 (median +1.42%, worst −4.44%)

The Sharpe result strengthened: at 36 start quarters it was 33/36 with a worst case of
−0.03; with the extra history it is 44/44 and never negative.

**Pre-cache out-of-sample (2011–2014), a window the original study never saw:**

| Arm | CAGR | Sharpe | maxDD |
|---|---|---|---|
| Timed | **+13.60%** | **0.84** | **−17.8%** |
| Buy & hold | +11.30% | 0.51 | −41.4% |

Positive on all three measures in a period that played no part in selecting the rule.
This is the closest thing to a true holdout available here, and it confirms rather than
attenuates the effect.

Nothing above changes the character of the finding: it remains a drawdown and Sharpe
overlay on one index, not stock selection, and the rally cost is unchanged.

## Paper sleeve

`scripts/chinext_timing_paper_sleeve.py` records the rule daily, append-only and
idempotent, into `outputs/paper_lab/chinext_timing_paper_ledger.jsonl`. It places no
orders, sends no alerts, and changes no selector or cron.

Rows before the inception stamp are labelled `BACKFILLED_FROM_HISTORY`; rows from
inception onward are `FORWARD_PAPER`. This repository's evidence hierarchy puts live
above replay above reconstruction, so the backfill must never read as forward evidence.

Inception 2026-08-07, 3,733 backfilled sessions. **State at inception: CASH** — close
3563.12 against SMA50 3847.54, so the rule is out of the market, consistent with the
production regime gate's BEAR call the same morning.
