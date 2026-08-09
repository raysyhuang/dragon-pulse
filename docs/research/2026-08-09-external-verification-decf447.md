# External Re-Verification — H1 Relative-Path Repair

**SHA:** `decf44726226df91710edb50da565f8c05babc59`
**Method:** clean `git clone` on a separate machine from the builder.
**New suite:** `tests/test_capture_path_containment_adversarial.py` — 13 cases written specifically for this repair.

## Verdict: PASS — both directions verified

| Check | Result |
|---|---|
| All verification suites at SHA | **306 passed** |
| Regression (Task 1 / 2 / 2.5 adversarial) | all still pass, none weakened |
| Positive direction (symlinked ancestor accepted) | **confirmed via CLI** |
| Negative direction (containment intact) | **confirmed by direct exploit test** |

My three adversarial files are byte-identical to what I pushed at `991b00b` — the builder did not touch them.

## Positive direction — H1 is genuinely fixed

Reproduced the original failure case from the CLI: a sources directory reached through a symlinked ancestor now builds, as does the physical path. Also verified two-level indirection (`link -> link -> real`), so depth of legitimate indirection does not matter.

The design is right: `_safe_source_snapshot` walks symlinks from the snapshot **up to the sources directory only**, exempting ancestors above it, then maps the validated relative child onto the physical root.

## Negative direction — containment did not weaken

The risk in any such relaxation is that the ancestor exemption leaks downward. It does not. All 13 containment cases pass, including the ones that matter most:

- symlinked snapshot file — rejected (outward *and* inward)
- symlinked receipt — rejected
- symlinked directory *between* the sources root and the snapshot — rejected
- **legitimate symlinked ancestor + escaping child symlink** — rejected
- symlinked `raw/` payload and symlinked `raw/` directory — rejected
- relative traversal and absolute-outside-root snapshot arguments — rejected

Rather than trust test outcomes, I ran a direct exploit harness: under each mutation, can the validator be made to accept a snapshot whose bytes physically live outside the sources root? **At `decf447`, all three attack vectors (symlink-in-sources, absolute-outside, relative-traversal) are rejected.**

## Mutation analysis of the repair

Five defects injected into the new path logic. Four initially survived both suites, but survival is not the same as a hole — the exploit harness separates them:

| Mutant | Removing it breaks containment? | Status |
|---|---|---|
| Q2 `..`/absolute child guard | **Yes** — relative traversal accepted | **uniquely load-bearing; now covered** |
| Q4 `_safe_file` symlink guard | Yes | already covered |
| Q1 lexical symlink walk | No — physical `_safe_file` still blocks | redundant layer |
| Q3 physical `_safe_file` call | No — lexical walk still blocks | redundant layer |
| Q5 absolute-outside rejection | No — child guard still blocks | redundant layer |

Q1/Q3/Q5 are equivalent mutants: the repair deliberately layers a lexical walk and a physical check, and either alone stops every vector. That redundancy is a strength, not dead code.

**Q2 was a genuine coverage gap in both suites.** My own traversal test had been passing for the wrong reason — it used a non-conforming filename (`outside.csv`), so the request died on the snapshot-name pattern and never reached the containment guard. Rewritten so the escaping path is fully valid apart from the traversal (conforming filename, real file, matching receipt); Q2 is now caught.

That is the third time in this review that a test passed for the wrong reason. It is the dominant failure mode of adversarial suites, and only mutation testing surfaces it.

## Confirmed unresolved scope (unchanged, by design)

The builder's own summary is accurate, and I verified each:

- same-date capture accepted;
- year-2999 capture accepted — no upper bound policy;
- a fabricated `raw/` payload can still claim `OBSERVED_CAPTURE`, because the system binds operator-supplied evidence but does not notarize the provider.

## Recommendation

**H1 closed. Task 2.5 fully approved. Cleared for Task 3.**

Remaining optional hardening, none blocking: an upper bound on `captured_at` (capture ≤ build time), and the first-party suite gaps P8 (`schema_version`) and P14 (naive timestamp).

The standing caveat for everything downstream is unchanged: Tasks 1, 2 and 2.5 establish that supplied evidence is self-consistent, hash-bound, membership-checked, reproducible, correctly graded and path-contained — and that the strongest available grade remains operator-asserted.
