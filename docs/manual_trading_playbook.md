# Dragon Pulse — Manual / IBKR Trading Playbook

**Engine:** `alpha_rs_pullback` (bull-gated relative-strength pullback, A-shares)
**Status:** validated config as of 2026-06-23. This is the *current production logic* —
both alternative exits (ATR runner) and relaxed entries (chase band) were tested over
5 years and **rejected**; they only beat this config in the recent bull and lost over
the full cycle including the 2021–22 bear.

> ⚠️ **Survivorship bias is large — measured 2026-06-23.** The validation universe uses
> *current* top-1000 market cap applied to history. 19% of backtest picks were not in the
> top-1000 as of their own date. Removing them cuts 5Y return **+65% → +24%** (Sharpe
> 0.82 → 0.42, DD 19% → 25%). **Use the survivorship-clean numbers as the realistic
> expectation, and treat even those as an optimistic upper bound:**
>
> | Window | Clean return | Clean ann. | Sharpe | Max DD |
> |--------|-------------|-----------|--------|--------|
> | 5Y (incl. bear) | +24% | ~4.7%/yr | 0.42 | 25% |
> | 3Y | +26% | ~8.6%/yr | 0.65 | 24% |
> | 1Y (recent bull) | +54% | ~59%/yr | 2.13 | 13% |
>
> The edge is real but **thin outside bull markets** and the recent strong 1Y flatters it.
> This is consistent with the soft live Apr–Jun 2026 result. Size conservatively.

---

## 1. The rule set (what the bot/you executes)

**Signal source:** the nightly `execution_watchlist_<date>.json` (top picks, already
ranked, deduped, sector-capped).

**Regime gate (already enforced by the engine):**
- Trade **only in bull regime.** Zero new entries in choppy or bear. If the watchlist
  is empty, that's the system working — do nothing.

**Selection (validated improvement — take ONE/day):**
- Take the **single top-scored pick per day**, **max 1 per sector**. Picks already
  require **score ≥ 90** and **ADV ≥ ¥80M**.
- Selection sweep (2026-06-23, pinned 5Y): top-1/day by score beats top-2 on Sharpe in
  every window (5Y 0.82→0.88, 3Y 1.16→1.22, 1Y 2.58→2.87) and cuts max drawdown ~20–55%
  (1Y 16%→7%). The top pick is genuinely better than the 2nd. Mirrors MAS's "one-pick/day."
- Rank by **score**, not by risk. A stop-risk filter (drop wide-stop names) was tested and
  *hurt* (removes winners) — do not use it.
- Trade-off: fewer trades → less total compounding, but the book is under-utilized at
  top-1/day, so size up per position (below) to recover return at the better risk profile.

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

> The exit is the part a manual IBKR workflow struggles with (the day-5 timed close and
> the intraday stop/target bracket). Automate this — it's the highest-value thing to bot.

---

## 2. Position sizing & risk

Validated sim used **20% per position, max 5 concurrent** (≈ up to 100% gross):

| Window | Total ret | Sharpe | Max DD |
|--------|-----------|--------|--------|
| 5Y (incl. bear) | +65% | 0.82 | 19% |
| 3Y | +65% | 1.16 | 17% |
| 1Y | +84% | 2.58 | 16% |
*(survivorship-biased — treat as optimistic)*

**Recommended phased rollout (mirrors the MAS Sniper discipline):**

- **Phase 1 — live proving (first 4–6 weeks):**
  - **10–15% per position, max 4 concurrent** (≤ ~60% gross).
  - Full rule discipline; log fills vs. signal price, slippage, and day-5 exits.
  - Goal: confirm live fills/slippage match the backtest before adding size.
- **Phase 2 — scale (only if Phase 1 matches expectations):**
  - **20% per position, max 5 concurrent.**
  - Reserve the top of the range for A+ setups only: score ≥ 98, clean liquidity,
    no binary/earnings event, market not weak at the open.

**Hard risk limits:**
- Per-trade risk ≈ position% × (1.1 × ATR / entry). With ~5% ATR names that's ~0.8–1.1%
  of book per 15–20% position — keep total open risk sane.
- If realized drawdown exceeds the backtest's 19%, cut size; the real bear will be deeper.

---

## 3. Daily checklist

1. **Regime = bull?** If not, no trades today.
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
- ❌ Don't trade in choppy/bear regime.

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
