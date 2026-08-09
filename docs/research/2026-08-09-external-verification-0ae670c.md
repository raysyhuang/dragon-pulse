# External Verification — Task 2 PIT Universe Schedule Builder

**SHA:** `0ae670cc1a6daabe12d3e7b98fd21d452d2dbba8` (confirmed tip via GitHub API + `git ls-remote`)
**Method:** clean `git clone` on a separate machine from the builder, checked out at the SHA.
**Suite:** `tests/test_build_pit_universe_schedule_adversarial.py` — 55 cases, authored from the Task 2 spec.

## Verdict: PASS

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **55 passed** |
| Task 1 + Task 2 first-party suites vs SHA | 150 passed |
| Mutation testing — adversarial suite | **11 / 12 caught** (1 equivalent mutant) |
| Mutation testing — first-party Task 2 suite | 9 / 12 caught |

## F5 is resolved at the contract level

This was the carry-forward from Task 1, and it is properly closed:

- Snapshots must now carry `list_date` and `delist_date`; a missing column fails the build.
- Eligibility is `list_date <= as_of < delist_date`, with both boundaries verified correct.
- A row contradicting membership on its own source date raises `source snapshot eligibility contradiction as of <date>` — a **hard failure, not a silent filter**. Confirmed by direct probe. Silent filtering was the mechanism that would have let a current-universe snapshot be laundered into a historical bundle.
- Blank `list_date` rejected; blank `delist_date` permitted only as "still listed".
- Output is atomic (`mkdtemp` + `os.replace`), refuses to overwrite, byte-deterministic, copies sources byte-identically, and the emitted bundle passes the Task 1 validator end to end.
- The manifest records the limitation explicitly: `"historical_membership_authenticity": "not established by this builder"`.

Task 1 findings F1 and F2 are now closed in the first-party suite. F3 remains uncovered there but is guarded repo-wide by the committed adversarial test.

## Findings

### G1 — Builder's suite misses a tie-break regression (LOW/MEDIUM)
Mutation **N5** (drop the ascending-ticker tie-break, leaving `-circ_mv` only) passes the first-party Task 2 suite. Implementation is correct. With equal `circ_mv`, selection would silently fall back to file order, so two byte-identical snapshots in different row orders would produce different universes — destroying build reproducibility, which is the point of the bundle.

### G2 — Staging-directory cleanup was untested by both suites (LOW, now closed)
Mutation **N10** (remove `shutil.rmtree` from the failure path) passed both suites. Verified load-bearing: an injected write-phase failure leaks `.bundle.tmp-XXXX` into the output parent. The window is narrow — all validation completes before the temp dir exists, so only disk-full/permission/interrupt failures reach it — but it is the same untested-correct-guard class as Task 1's F3. Covered now by `test_write_phase_failure_leaves_no_temp_directory`.

### G3 — Recorded builder provenance ignores working-tree state (MEDIUM)
`_git_commit` records `git rev-parse HEAD` only. Verified: with a modified working tree (1 dirty file), the manifest still stamped the clean SHA `0ae670c`. A bundle built from edited code therefore claims provenance of a commit that did not produce it — which defeats the purpose of recording the commit at all, precisely in the case where it matters (someone testing a local change).

Suggested fix: record dirtiness explicitly, e.g. a `builder_tree_dirty: true` field or a `-dirty` suffix, sourced from `git status --porcelain`. Cheap, and it makes the field trustworthy.

### N8 — equivalent mutant, not a gap (INFO)
Removing the explicit blank-`list_date` guard still rejects, because `_parse_date("")` fails. Redundant defense in depth, correctly so. No action.

## Residual risk carried forward

Probe result: a snapshot with **coherent but invented** `list_date` values and blank `delist_date` builds and validates cleanly. Task 2 closes *self-contradictory* fabrication; it cannot close *internally consistent* fabrication. The spec now names this honestly — authenticity of capture is an upstream gate that does not yet exist.

This is the correct scope boundary, but it must stay visible: **no C2 factor result may be described as point-in-time on the strength of Tasks 1–2 alone.** They establish that supplied evidence is self-consistent, hash-bound and reproducible — not that the vendor snapshot was genuinely captured on its named date. A capture-provenance gate (dated retrieval receipts, vendor response hashes at capture time) remains unbuilt and should be an explicit precondition before any factor result is interpreted.

## Recommendation

**Approve Task 2. Proceed to Task 3**, carrying G3 (make the git provenance field honest) and G1 (close the tie-break gap). Neither blocks. Keep the capture-provenance limitation attached to every downstream claim.
