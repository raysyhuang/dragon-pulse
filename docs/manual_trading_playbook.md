# Dragon Pulse — Manual / IBKR Trading Playbook

**Engine:** `alpha_rs_pullback` (bull+choppy relative-strength pullback, A-shares)
**Status:** paper/daily core as of 2026-09-01. Choppy is **paper-only**; this
document does not authorize automatic or manual live orders. The underlying rule set was
validated as of 2026-06-23 —
both alternative exits (ATR runner) and relaxed entries (chase band) were tested over
5 years and **rejected**; they only beat this config in the recent bull and lost over
the full cycle including the 2021–22 bear.

> ⚠️ **Survivorship bias is large — measured 2026-06-23.** The validation universe uses
> *current* top-1000 market cap applied to history. 19% of backtest picks were not in the
> top-1000 as of their own date. Removing them cuts 5Y return **+65% → +24%** (Sharpe
> 0.82 → 0.42, DD 19% → 25%). **Use the historical point-in-time audit as a
> conservative reference, while retaining its membership-assumption limits:**
>
> **Earlier point-in-time audit estimates** (2026-06-23, scanner re-run on as-of-date
> top-1000 schedules; separate from the current frozen adoption replay):
>
> | Window | PIT return | PIT ann. | Sharpe | **Max DD** |
> |--------|-----------|----------|--------|-----------|
> | 5Y (incl. bear) | +29% | ~5.7%/yr | 0.46 | **30%** |
> | 3Y | +35% | ~11.4%/yr | 0.72 | 25% |
> | 1Y (recent bull) | +64% | ~70%/yr | 2.34 | 10% |
>
> For reference: static-biased 5Y was +65%/0.82/19%DD; survivorship-clean audit +24%/0.42/25%.
> The true edge is ~¼–⅓ of the headline, per-trade P&L ~0.36% (vs biased 1.05%), and the
> honest full-cycle **drawdown is ~30%**. Edge is real but thin and recent-bull-flattered —
> consistent with the soft live Apr–Jun 2026 result. **Size for a thin edge with deep DD.**

> **Frozen 5Y gate replay (2026-09-01):** the current 20%-per-position,
> max-5-concurrent gross paper portfolio produced **2.482x / 21.01% DD / 0.991
> Sharpe**. Of 1,080 filled replay trades, 935 fit the portfolio and 145 were skipped
> by the concurrency cap. Both 688xxx ordinary shares and 689xxx STAR CDRs are excluded.
> Six entries opened below their planned stop; A-share T+1 was enforced by exiting at
> the next-session open rather than fabricating a same-bar stop fill.
> These figures are gross, use a trusted historical PIT-membership assumption, and
> remain non-binding. The older bull-only portfolio predates the same STAR exclusion,
> so it is not a valid like-for-like comparator and no improvement delta is claimed.

---

## 1. The rule set (what the bot/you executes)

**Signal source:** the nightly `execution_watchlist_<date>.json` (top picks, already
ranked, deduped, sector-capped).

**Regime gate (already enforced by the engine):**
- The paper scanner may emit picks in **bull or choppy**. **Bear remains a hard no-entry
  regime.** If the watchlist is empty, that's the system working — do nothing.
- Choppy picks stay paper-tracked until a separate promotion decision; do not route them
  to IBKR merely because they appear in the watchlist.
- Exclude all STAR Market names, including 688xxx ordinary shares and 689xxx CDRs.

**Selection (corrected on unbiased point-in-time data, 2026-06-23):**
- Picks already require **score ≥ 90**, **ADV ≥ ¥80M**, **max 1 per sector**.
- **Do NOT rank by score / take top-1-by-score.** On the *biased* static set that looked
  like a win, but on the unbiased point-in-time set it does not beat top-2 (5Y Sharpe 0.44
  vs 0.46; 3Y 0.62 vs 0.72). The 90+ score is saturated and does not discriminate quality.
- **The only defensible selection edge is risk-aware, for drawdown control:** top-1/day
  ranked by *lowest stop-distance* halves the 5Y max drawdown (30%→16%) and lifts full-cycle
  Sharpe (0.46→0.58) at similar return. But it's neutral-to-slightly-negative on 3Y/1Y — so
  use it to tame the deep tail, not to boost return.
- Net: **take top-2 by default; if you want to cap the ~30% drawdown, take 1/day ranked by
  lowest stop-distance.** Either way, ranking by score buys nothing.

**Entry — strict no-chase (validated as correct):**
- Place a **buy limit at `max_entry_price`** (= signal entry × 1.02) for the T+1 session.
- Fill at the open if it opens at/below `max_entry_price`.
- **If it gaps above `max_entry_price`, SKIP IT.** Do not chase. The picks that run
  away are, across regimes, worse trades — chasing them inflated drawdown from 19% → 35%
  in testing.
- T+1 only — no buying on signal day.

**Exit — fixed (validated as best, risk-adjusted, all regimes):**
- **Stop:** `stop_loss` (= entry − 1.1 × ATR). Set it immediately on fill.
- **Target:** `target_1` (= entry + 2.1 × ATR, capped at +10% A-share limit). Sell the
  full position if hit.
- **Time exit:** if neither stop nor target hits, **exit at the close on holding day 5.**
- Whichever of stop / target / day-5 comes first. **No partial scale-outs** — taking
  winners off early was the worst-performing variant in every test.
- T+1 constraint: no same-day exit after entry.
- If the entry open is already below the planned stop, the replay holds through that
  entry session and exits at the next-session open—the earliest T+1-legal exit.

> The exit is the part a manual IBKR workflow struggles with (the day-5 timed close and
> the intraday stop/target bracket). Automate this — it's the highest-value thing to bot.

---

## 2. Position sizing & risk

Size from portfolio-level evidence, not per-trade compounding. The frozen bull+choppy
paper replay produced 2.482x gross equity, 21.01% max drawdown and 0.991 Sharpe at
20%/position with max 5 concurrent; 145/1,080 trades were capacity-skipped. Costs are
not modelled and PIT membership is a trusted historical assumption, so treat this as an
optimistic paper result rather than a live sizing promise.

**Recommended sizing (conservative, for a thin edge + deep DD):**

- **Phase 1 — paper / proving (first 4–6 weeks):** paper-trade the full rule set; log fills
  vs. signal price, slippage, day-5 exits. Confirm live matches before risking capital
  (live Apr–Jun 2026 came in soft — assume the thin number, not the bull).
- **Phase 2 — small live:** **10% per position, max 4–5 concurrent** (≤ ~50% gross). At a
  Sharpe ~0.46 / 30%-DD edge, fractional sizing is mandatory — do not run near 100% gross.
- **Drawdown control:** if the ~30% DD is unacceptable, switch to **1 pick/day ranked by
  lowest stop-distance** (halves 5Y DD to ~16%) — accepting slightly lower return.
- **Only scale toward 15–20%** if live performance over a meaningful sample (not one bull
  quarter) actually matches the point-in-time numbers.

**Hard risk limits:**
- Per-trade risk ≈ position% × (1.1 × ATR / entry). Keep total open risk sane at 10%/pos.
- The honest full-cycle drawdown is ~30% (PIT) — expect it, and a real bear could be worse.
- **Benchmark check:** DP made +1% live while CSI 300 did +11% over the same span. Before
  scaling, confirm the strategy is actually beating just holding the index after costs.

---

## 3. Daily checklist

1. **Paper regime = bull or choppy?** If bear, no new entries. Choppy remains paper-only.
2. Pull the watchlist; take the top ≤2 picks (≤1/sector).
3. For each: set **buy limit @ max_entry_price** for the open.
4. On fill: immediately set **stop @ stop_loss** and **target @ target_1** (OCO bracket).
5. Unfilled (gapped above limit) → **cancel, do not chase.**
6. Track holding day; **force close at the day-5 close** if still open.
7. Respect concurrency/size caps; never average down.

---

## 4. What NOT to do (each disproven by 5Y testing)

- ❌ Don't chase entries above `max_entry_price` (worse trades, deeper DD).
- ❌ Don't use a trailing/runner exit instead of the fixed target (fragile, param-sensitive,
  worse Sharpe over the full cycle).
- ❌ Don't scale out winners early (consistently the worst variant).
- ❌ Don't trade in bear regime.
- ❌ Don't promote choppy paper picks to live orders without a separate reviewed decision.

---

## 5. Open questions worth real work (not exit/entry tinkering)

1. **Survivorship-bias-free validation** — re-run with a point-in-time universe to get
   the true return/drawdown. Likely the most important remaining task.
2. **Why did live Apr–Jun 2026 underperform the backtest?** Small sample vs. an execution
   or regime gap — investigate before trusting the rosy backtest.
3. **Concentration/sizing** is the only lever not yet falsified — but given the pattern
   (aggression flatters bulls), test it the same rigorous, multi-regime way.

*Harness for any further tests: `scripts/backtest_1yr.py --dump-picks` (pin once) →
`scripts/exit_validation.py` / `scripts/entry_sweep.py` (replay variants in minutes).*
