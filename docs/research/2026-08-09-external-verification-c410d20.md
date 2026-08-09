# External Verification — Task 3 Cross-Sectional Replay Accounting

**SHA:** `c410d206d084a78c4552cc04fe31cc600e560959`
**Method:** clean `git clone` on a separate machine from the builder.
**Suite:** `tests/test_xsec_replay_adversarial.py` — 44 cases, bound from a pre-push attack catalogue.

## Verdict: PASS

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **44 passed** |
| All verification suites vs SHA | 284 passed |
| Mutation testing — adversarial suite | **13 / 13** after closing one gap (12/13 initially) |
| Mutation testing — first-party suite | 12 / 13 |

Invariants were fixed in an attack catalogue written before the push; only the binding to the real dataclasses was added afterwards. My four earlier adversarial files are byte-identical to `50f2c59`.

## The five holes their reviewer caught are genuinely closed

Verified individually by mutation, not taken on trust:

- **bool prices** — `_is_finite_number` explicitly excludes `bool`. Removing that exclusion is caught. This is the subtle one: `isinstance(True, int)` is `True`, so a naive guard admits `True` and arithmetics it as `1.0`.
- **malformed objects** — a non-`Bar` raises a typed `ValueError`, not an escaping `AttributeError`.
- **exit ordering** — `exit <= entry` and `exit <= signal` both rejected.
- **impossible OHLC** — `high < low`, and `open`/`close` outside `[low, high]`, all rejected.
- **overflow** — derived returns and totals are both finiteness-checked; `1e308` inputs do not produce `inf` P&L.

## What else the core gets right

- **Conservation is exact.** `selected == filled + no_fill + censored`, with `capacity_censored` a proper subset of `censored` — no double counting. Every selected ticker appears exactly once. Duplicate selections are rejected rather than silently deduped, which would have shrunk the partition.
- **Entry timing.** Entry is the next-session open. A next-session bar dated at or before the signal date is rejected outright, so lookahead is unconstructible rather than merely avoided.
- **D4, the dangerous silent drop.** A fillable position with no exit bar becomes `CENSORED_MISSING_EXIT` and retains its entry price — it is *not* recorded as a flat 0% trade. Mutating it into a flat trade is caught. This is the failure mode that most easily disguises missing data as a real result.
- **Costs cannot be switched off.** `total_cost_bps <= 0` is rejected, so no configuration of this core can produce an uncosted return. That is a deliberate and valuable guard, and it is now pinned by a test.
- **Status precedence is deterministic:** capacity → missing entry → over cap → missing exit.
- **Purity holds:** frozen dataclasses, no input mutation, deterministic output order, no I/O.

## Findings

### J1 — First-party suite misses the chase-cap boundary (LOW/MEDIUM)
Mutating the cap test from `open > cap` to `open >= cap` passes all first-party tests. The implementation is correct (entry exactly at the cap fills), but the convention is unpinned there. Task 4 consumes this boundary, and an off-by-one would silently convert marginal fills into no-fills, shrinking the filled denominator and flattering per-trade averages.

### J2 — My own fixture blind spot (INFO, closed)
My initial suite missed the mutation swapping exit `close` for exit `open`, because every fixture I had repaired into a valid OHLC used flat bars where `open == close`. A fixture that cannot distinguish two fields cannot test that they are not confused. Closed with two cases using distinct open/close on both legs.

This is the fourth time in this review that a test passed for the wrong reason — and the first where the fault was mine rather than the builder's. Same lesson, same detection method.

## Scope observations, not defects

- **Costs are a single `total_cost_bps`.** The A-share split (commission both legs, stamp duty sell-side only) is not modelled here. Correct for a pure accounting core, but the asymmetry has to live somewhere before any net number is quoted as realistic.
- **`capacity_available` is a caller-supplied bool.** This core records capacity censoring; it does not decide who gets cut. Task 4 owns that, and the cut rule must be deterministic and ranked, or bundles stop being reproducible.
- **All-censored input reports totals of `0.0`.** Correct as a *sum* over an empty set. The trap is downstream: a **mean** over that input must divide by `filled` (zero → undefined), never by `selected`. Dividing by `selected` is exactly how "no data" becomes "no edge". The core exposes both counts, so Task 4 can get this right — it just has to.

## Recommendation

**Approve Task 3. Cleared for Task 4.** Carry J1 (pin the cap boundary), and carry the three scope observations into Task 4's design, where each becomes a live decision rather than a documentation note.

Standing status, unchanged: this is measurement plumbing. Tasks 1–3 make an eventual result trustworthy; none of them provides evidence of an edge, and nothing here should be read as moving that question.
