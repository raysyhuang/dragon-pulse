# External Verification — Task 1 PIT Manifest Validator

**SHA:** `104bdf56a5d3d3b2f26aee51f3be1beafedeb224` (confirmed branch tip via `git ls-remote`)
**Method:** clean `git clone` from GitHub on a separate machine from the builder, checked out at the SHA, working tree clean.
**Suite:** `tests/test_pit_bundle_adversarial.py` — 79 cases. Intent authored from the spec **before** the code was pushed; fixture builder adapted afterwards to the real schema.

## Verdict: PASS

The implementation withstood every adversarial case, including all boundary, tamper, path-escape and truthy-coercion attacks.

| Run | Result |
|---|---|
| Adversarial suite vs SHA | **79 passed** |
| Builder's own suite vs SHA | 45 passed |
| Mutation testing — adversarial suite | **10 / 10 caught** |
| Mutation testing — builder's suite | 7 / 10 caught |

Full-repo suite could not run here: `pandas_ta` absent from the bare system python. Unrelated to Task 1 — `pit_bundle.py` is stdlib-only.

## Independently confirmed correct

Both eligibility conventions matched my pre-written assumptions exactly, without coordination:

- `listed_on_or_before > as_of_date` → reject; equality is **eligible** (inclusive first session).
- `delisted_after <= as_of_date` → reject; equality is **ineligible**.

An off-by-one in either direction would silently thin or inflate every historical universe. They are right, and now pinned by tests.

Also confirmed: `pit_grade` requires literal `True` (rejects `"true"`, `"True"`, `1`, `"yes"`, `[]`, `{}`); composite hash binds the whole hash set; schedule-date set must equal `as_of_dates` exactly; unreferenced source hashes rejected; extra untracked files rejected.

## Findings

### F1 — Builder's suite misses a listing-boundary regression (MEDIUM)
Mutation **M1** (`listed_on_or_before > as_of_date` → `>=`) passes all 45 of the builder's tests. The implementation is correct; the regression guard is absent. Task 2 builds schedules against exactly this convention, so a later off-by-one would propagate into every generated bundle undetected.

### F2 — Builder's suite misses a hash-format regression (LOW/MEDIUM)
Mutation **M5** (`_SHA256_RE` made case-insensitive) passes all 45 builder tests. Accepting uppercase hex would let two spellings of one digest compare unequal, breaking hash-identity comparisons downstream.

### F3 — A correct guard had zero coverage in *both* suites (LOW, now closed)
Mutation **M7** (delete the `rglob` symlink scan) initially passed both suites. Verified it is uniquely load-bearing: a stray **symlinked directory** is invisible to the unlisted-file check (`is_file()` is False) and is not manifest-listed, so no other check sees it. A refactor could have deleted a correct guard silently. Added `test_rejects_stray_symlinked_directory_in_bundle`; M7 is now caught.

### F4 — Manifest schema deviates from the written spec (DOC)
Spec Task 1 specifies a `sources` list with per-entry hashes. The implementation uses `bundle_id` + a flat `hashes` map + `composite_sha256`. **The implementation's design is better** — the composite binds the entire set, which a per-entry list does not. But the plan document is now wrong, and Task 2's builder must emit the real contract. Update the spec, don't change the code.

### F5 — Residual survivorship hole moves to Task 2 (IMPORTANT, not a defect)
Task 1's stated objective is "make it impossible for a research run to call a current-universe snapshot PIT." Strictly, it does not achieve that alone. A schedule built from today's universe, with every name stamped an early `listed_on_or_before` and a blank `delisted_after`, **validates cleanly** (probe confirmed: `rejected: False`).

That is the correct scope boundary — a validator cannot know real listing dates — but it means the survivorship guarantee rests entirely on Task 2 sourcing genuine listing/delisting dates from a dated source, never synthesising them. This should be an explicit, tested Task 2 requirement, and the objective line in the plan should be narrowed to what Task 1 actually enforces.

## Recommendation

**Approve Task 1. Proceed to Task 2**, carrying F4 (fix the spec doc) and F5 (make real listing/delisting provenance a tested Task 2 gate). F1/F2 are suite-coverage gaps worth closing but do not block.
