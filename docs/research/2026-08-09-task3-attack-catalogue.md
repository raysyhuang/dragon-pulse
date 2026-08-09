# Task 3 Attack Catalogue — Cross-Sectional Replay Accounting

**Written before the implementation was pushed**, from the Task 3 spec in
`docs/plans/2026-08-09-evidence-grade-research-infrastructure.md`. Binding these
invariants to the real API is a single adapter function once the SHA lands; the
invariants themselves are fixed here and will not be revised to match whatever the
implementation happens to do.

Ordering note: the catalogue is grouped by failure class, and the classes are ranked by
how badly a miss would corrupt a downstream result. Conservation errors are first
because they silently inflate every performance number computed on top of them.

---

## A. Conservation — the partition must be exact

This is the whole point of the task: *"never dropped names."*

| # | Invariant |
|---|---|
| A1 | `selected == filled + no_fill + censored + capacity_censored`, exactly, for every input |
| A2 | Every selected ticker appears **exactly once** across all outcome rows |
| A3 | No ticker appears in two outcome categories |
| A4 | Empty selection returns zeroed denominators, not a crash or an empty-dict KeyError |
| A5 | Duplicate ticker in the selection is rejected, not silently deduped (dedup would break A1) |
| A6 | Reported denominators reconcile with the actual row counts, not just with each other |

## B. Entry timing — the original sin being fixed

| # | Invariant |
|---|---|
| B1 | Entry price is the **next session open**, never the signal-date close |
| B2 | Entry is never the signal-date open either |
| B3 | No bar dated `<= signal_date` can ever supply an entry price (lookahead) |
| B4 | If the next session bar is absent, the row is censored — a **later** bar must not be silently substituted as if adjacent |
| B5 | Probe: is there a maximum gap before a later session stops counting as "next"? Unspecified in the spec; report the behaviour |

## C. No-fill and the chase cap

| # | Invariant |
|---|---|
| C1 | Entry open above `max_entry_cap` → `NO_FILL_CHASE` with **zero** allocated P&L |
| C2 | Boundary: entry open exactly equal to the cap → filled. Either convention is defensible; an **undocumented** one is a finding, since Task 4 consumes it |
| C3 | No-fill rows contribute nothing to gross/net aggregates |
| C4 | No-fill rows are excluded from the filled denominator |
| C5 | Costs are **not** charged on a no-fill row |

## D. Censoring — absence must be recorded, never dropped

| # | Invariant |
|---|---|
| D1 | Missing next-session bar → `CENSORED_MISSING_ENTRY` |
| D2 | Missing exit bar → `CENSORED_MISSING_EXIT` |
| D3 | Censored rows carry no P&L and no costs |
| D4 | A filled position with no exit bar is censored, **not** recorded as a 0% return — this is the single most dangerous silent-drop variant, because it looks like a flat trade instead of missing data |
| D5 | Censoring reason is distinguishable per row, not collapsed into one bucket |

## E. Capacity

| # | Invariant |
|---|---|
| E1 | Selections beyond available capital/slots produce explicit `CENSORED_CAPACITY` rows |
| E2 | Which names get cut is **deterministic and documented** (rank order), not arbitrary |
| E3 | Capacity-censored rows carry no P&L |
| E4 | Capacity censoring happens before or after fill checks — order must be consistent and stated, since it changes the denominators |

## F. Cost arithmetic

| # | Invariant |
|---|---|
| F1 | `net == gross - costs` exactly, no sign errors |
| F2 | Commission charged on **both** legs; stamp duty on the **sell** leg only (A-share convention) |
| F3 | Zero-cost configuration → `net == gross` identically |
| F4 | Costs never applied to no-fill or censored rows (follows C5/D3) |
| F5 | Minimum-commission handling, if present, is applied per leg not per round trip |

## G. Numeric robustness — the five holes their reviewer caught, plus the rest

| # | Invariant |
|---|---|
| G1 | **`bool` as a price is rejected.** `isinstance(True, int)` is `True` in Python, so a naive numeric check admits `True` and then arithmetics it as `1.0` |
| G2 | `NaN` and `inf` prices rejected — must never yield `inf`/`NaN` P&L |
| G3 | Zero or negative prices rejected |
| G4 | Malformed bar object (missing key, `None`, wrong type, string price) raises a **typed** error, not `KeyError`/`AttributeError`/`TypeError` escaping the module |
| G5 | `exit_date <= entry_date` rejected |
| G6 | Impossible OHLC rejected: `high < low`, `open` outside `[low, high]`, `close` outside `[low, high]` |
| G7 | Astronomically large prices do not overflow to `inf` in the P&L computation |
| G8 | Extremely small prices do not underflow or divide-by-zero in the return computation |

## H. Purity and determinism

| # | Invariant |
|---|---|
| H1 | Identical inputs → byte-identical outputs, across repeated calls |
| H2 | Input structures are **not mutated** (caller's bars/selection unchanged after the call) |
| H3 | No provider calls, no network, no filesystem access |
| H4 | Output row order is deterministic |

## I. Aggregate integrity — the classic inflation bug

| # | Invariant |
|---|---|
| I1 | Gross/net returns are computed **only** over filled legs |
| I2 | Mean return divides by the **filled** count, never the selected count — dividing by selected silently dilutes toward zero; dividing filled-sum by filled-count while *reporting* selected as the denominator inflates apparent breadth |
| I3 | Aggregates over an all-censored input are undefined/None, not `0.0` — a zero return is a claim, absence is not |
| I4 | Per-leg returns and the aggregate agree under recomputation from the emitted rows |

---

## Method

1. Bind to the real API through one adapter function.
2. Run the catalogue; every rejection asserts a **specific** message, never bare `pytest.raises`.
3. Mutation-test both suites — inject defects, measure catch rate, and for anything that survives, run a direct exploit rather than assuming it is a hole or assuming it is equivalent.

The recurring lesson from Tasks 1, 2 and 2.5: three separate tests passed **for the wrong reason** (schema mismatch, caveat masking the grade check, filename pattern short-circuiting a traversal check). Only mutation testing exposed them. Expect the same here and check every green negative test against a deliberately broken implementation.
