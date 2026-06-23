# Dragon Pulse — Manual / IBKR Trading Playbook

**Engine:** `alpha_rs_pullback` (bull-gated relative-strength pullback, A-shares)
**Status:** validated config as of 2026-06-23. This is the *current production logic* —
both alternative exits (ATR runner) and relaxed entries (chase band) were tested over
5 years and **rejected**; they only beat this config in the recent bull and lost over
the full cycle including the 2021–22 bear.

> ⚠️ **Backtest caveat:** the validation universe uses *current* top-1000 market cap
> applied to history (survivorship/look-ahead bias). Real-world returns and drawdowns
> will be **worse** than the backtest figures below. Size conservatively at first.

---

## 1. The rule set (what the bot/you executes)

**Signal source:** the nightly `execution_watchlist_<date>.json` (top picks, already
ranked, deduped, sector-capped).

**Regime gate (already enforced by the engine):**
- Trade **only in bull regime.** Zero new entries in choppy or bear. If the watchlist
  is empty, that's the system working — do nothing.

**Selection:**
- Take at most **2 picks per day** (engine cap), **max 1 per sector**.
- Picks already require **score ≥ 90** and **ADV ≥ ¥80M**.

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
