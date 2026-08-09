# Dragon Pulse Evidence-Grade Research Infrastructure Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Replace the non-PIT, same-close cross-sectional research path with fail-closed provenance and executable-replay infrastructure, while freezing the existing daily-selector evidence without reconstructing missing source artifacts.

**Architecture:** Keep the current non-PIT input-bundle path explicitly non-PIT and backward compatible. Add a separate, strict PIT schedule/manifest loader which accepts only immutable historical as-of rows and raw-source hashes. Extract deterministic replay accounting into a pure module; the cross-sectional runner consumes it and emits fills/no-fills/missing/censored denominators instead of silently dropping names. The current top-1 ledger gets an inventory/attestation artifact, not retroactively invented signals.

**Tech Stack:** Python 3.11, pandas, pytest, Tushare raw JSON/CSV artifacts, Git provenance.

**Non-goals:** No change to `execution_watchlist`, 09:00 cron, selector ranking, broker integration, C2 factor formula, C3 return study, or live/paper promotion. No claim that a historical panel is PIT until it passes the new manifest contract.

---

### Task 1: Add a strict PIT manifest validator

**Objective:** Validate supplied evidence so a research run cannot call an unhashed or internally inconsistent snapshot bundle “PIT.” This validator cannot prove that supplied historical membership is authentic or complete.

**Files:**
- Create: `src/core/pit_bundle.py`
- Create: `tests/test_pit_bundle.py`
- Do not modify: `src/core/input_bundle.py` non-PIT contract

**Step 1: Write failing tests**

Create fixtures containing:
- `manifest.json` with `bundle_id`, `pit_grade: true`, `as_of_dates`, flat `hashes`, `composite_sha256`, source/snapshot metadata and `universe_schedule.csv`.
- schedule rows with `as_of_date`, `ticker`, `listed_on_or_before`, `delisted_after`, `source_file`, `source_sha256`.

Test: valid bundle loads; false pit grade is rejected; un-hashed raw source is rejected; a schedule date outside manifest dates is rejected; duplicate `(as_of_date,ticker)` is rejected; missing source hash is rejected.

Run: `.venv/bin/python -m pytest tests/test_pit_bundle.py -q`
Expected: FAIL because module is missing.

**Step 2: Minimal implementation**

Implement `PitBundleValidationError`, immutable `PitBundle`, SHA-256/composite helpers, and `validate_pit_bundle(path)`. Require raw-source files under `sources/`; require every scheduled row to refer to one manifest-listed, hash-valid source file and matching digest; return schedule sorted by date/ticker.

**Step 3: Verify**

Run focused tests, then `.venv/bin/python -m pytest tests/test_pit_bundle.py tests/test_input_bundle.py -q`.

**Step 4: Commit**

`git commit -m "feat: validate PIT research bundle manifests"`

---

### Task 2: Build deterministic PIT schedule artifacts from supplied historical snapshots

**Objective:** Act as the provenance gate: build a hash-stamped universe schedule only from dated supplied source snapshots, not a current market-cap universe. The builder validates snapshot shape, membership dates and selection before creating the immutable bundle; it still does not establish an upstream provider's historical authenticity.

**Files:**
- Create: `scripts/build_pit_universe_schedule.py`
- Create: `tests/test_build_pit_universe_schedule.py`
- Modify: `docs/research/2026-08-09-a-share-pipeline-replacement-protocol.md`

**Step 1: Write failing tests**

Use two small dated `daily_basic`-style CSV source snapshots. Test that:
- schedule membership is created only for tickers in that source date;
- ranking selects the stated `universe_n` by `circ_mv` at each date;
- missing/duplicate ticker/cap and unsupported source date fail closed;
- emitted manifest has `pit_grade: true`, source hashes and schedule hashes.

Run focused pytest and observe expected failure.

**Step 2: Minimal implementation**

CLI inputs: `--sources-dir`, `--output`, `--as-of-dates`, `--universe-n`, `--source-label`. Each source is an immutable historical snapshot named exactly `daily_basic_YYYYMMDD.csv`; it must include `ts_code,circ_mv,list_date,delist_date`. Require nonblank real listing dates, permit blank delisting dates only for still-listed names, select only `list_date <= as_of < delist_date` (when present), and rank `circ_mv` descending / ticker ascending. Emit a new output atomically: copied immutable `sources/` files, the exact Task 1 schedule schema, and a validator-compatible `bundle_id` / flat `hashes` / `composite_sha256` manifest with provenance/source label and builder Git commit. Do not fetch from providers in this task.

**Step 3: Verify**

Run unit tests. Build a tiny local fixture bundle and validate it with `validate_pit_bundle`; inspect its manifest hash.

**Step 4: Commit**

`git commit -m "feat: build hash-stamped PIT universe schedules"`

---

### Task 2.5: Capture-provenance attestation gate

**Objective:** Tasks 1–2 prove bundle self-consistency and source membership only. Before Task 3, require one immutable Tushare `daily_basic` capture receipt per supplied snapshot, without provider calls or third-party notarization.

**Files:**
- Create: `src/core/capture_provenance.py`
- Create: `tests/test_capture_provenance.py`
- Modify: `src/core/pit_bundle.py`, `scripts/build_pit_universe_schedule.py`

**Contract:** A receipt named `daily_basic_YYYYMMDD.capture.json` binds schema version 1, literal provider/endpoint, filename date, snapshot filename and SHA-256, ISO UTC capture time, and a grade. `OBSERVED_CAPTURE` additionally binds a hash-valid flat `raw/` payload. Existing historical snapshots may enter only as `TRUSTED_HISTORICAL_ASSUMPTION` with literal caveat `historical_tushare_trusted_assumption`; they are permanently caveated and must never be called `PIT_CAPTURE_VERIFIED`. The builder copies/hash-binds `sources/`, `attestations/`, and observed `raw/` payloads, and emits `OBSERVED_CAPTURE` only when every receipt is observed; any mixed set is `TRUSTED_HISTORICAL_ASSUMPTION` with the permanent statement that trusted history is not independently capture-proven.

**Verification:** Test trusted and observed positive controls; stale/tampered hashes, date/provider/endpoint/grade/caveat/path and symlink attacks; and output ancillary tampering/unlisted payload rejection. Do not change selector, cron, or cross-sectional replay.

---

### Task 3: Create a pure executable cross-sectional replay accounting core

**Objective:** Eliminate same-close fills and silent missing-name drops from the research engine.

**Files:**
- Create: `src/core/xsec_replay.py`
- Create: `tests/test_xsec_replay.py`

**Step 1: Write failing tests**

Define a pure API that accepts fixed selected tickers, known signal date, next-session OHLCV, exit-date OHLCV, max-entry cap, and cost inputs. Test:
- an entry begins at the next valid session open, never the signal-date close;
- an entry over cap becomes `NO_FILL_CHASE` with zero allocated P&L;
- no next-session bar becomes `CENSORED_MISSING_ENTRY`, not dropped;
- an exit/bar gap becomes `CENSORED_MISSING_EXIT`, not dropped;
- every selected ticker appears exactly once in accounting totals;
- no capital or slot produces an explicit `CENSORED_CAPACITY` row.

Run focused pytest and observe failure.

**Step 2: Minimal implementation**

Implement pure dataclasses/dicts and deterministic accounting. Do not make any provider call. Return outcomes plus denominator counts: selected, eligible, filled, no_fill, censored, capacity_censored and gross/net return only for filled legs.

**Step 3: Verify**

Run focused tests, then full `tests/`.

**Step 4: Commit**

`git commit -m "feat: add executable cross-sectional replay accounting"`

---

### Task 4: Route the cross-sectional runner through the replay core and label its evidence correctly

**Objective:** Ensure the existing xsec script cannot silently present its legacy close-to-close curve as execution-grade evidence.

**Files:**
- Modify: `scripts/xsec_sleeves.py`
- Create: `tests/test_xsec_sleeves_contract.py`
- Modify: `docs/alpha_hunt_conclusion.md`

**Step 1: Write failing tests**

Test that the runner/replay invocation emits a per-rebalance accounting record with selected/filled/no-fill/censored counts; that missing returns are recorded rather than `.dropna()` removed; and that output metadata marks non-PIT input as `PIT_GRADE_FALSE` and `RESEARCH_ONLY`.

**Step 2: Minimal implementation**

Keep legacy output only as a labelled compatibility view. Add a canonical per-rebalance CSV/JSONL including source date, signal timestamp convention, input grade, selected membership, outcome categories and cost assumptions. Do not run a factor result unless a valid PIT bundle is provided; live provider mode must mark output non-binding.

**Step 3: Verify**

Run targeted tests and existing paper-lab unit tests. Smoke with a tiny local/PIT fixture; do not overwrite committed performance output.

**Step 4: Commit**

`git commit -m "fix: make xsec replay fill and PIT limitations explicit"`

---

### Task 5: Freeze and inventory the incumbent daily-selector evidence without reconstruction

**Objective:** Produce a signed inventory of saved signal artifacts and their resolvability; do not pretend a missing artifact was an alert.

**Files:**
- Create: `scripts/freeze_top1_baseline.py`
- Create: `tests/test_freeze_top1_baseline.py`
- Modify: `scripts/top1_paper_track.py` only if a read-only manifest pointer is needed

**Step 1: Write failing tests**

Fixture with saved execution watchlist/artifact and a ledger row. Verify the inventory records file SHA, scan date, source path, source tier, and status. Test a ledger row with no corresponding artifact becomes `MISSING_NATIVE_ARTIFACT`, never reconstructed. Test repeat runs are byte-stable given fixed inputs.

**Step 2: Minimal implementation**

Create a read-only `outputs/top1_paper/baseline_inventory_<asof>.json` plus manifest hash. Include all-signal/filled/no-fill/censored denominators from saved ledger rows, regime counts, and source-artifact status. Do not alter `ledger.jsonl` or fabricate signals/outcomes.

**Step 3: Verify**

Run focused tests and generate an inventory on the real repository. Validate JSON and inspect its coverage/status counts.

**Step 4: Commit**

`git commit -m "feat: freeze Dragon Pulse top1 evidence inventory"`

---

### Task 6: Gates, full verification and independent review

**Objective:** Prevent outcomes from being interpreted before the infrastructure is actually evidence-grade.

**Files:**
- Modify: `docs/research/2026-08-09-a-share-pipeline-replacement-protocol.md`
- Create: `docs/research/2026-08-09-infrastructure-verification.md`

**Steps:**
1. Run all tests with the Tushare token available; run `git diff --check`.
2. Validate a generated PIT fixture and real baseline inventory.
3. Record which gates are **passed**, **blocked**, or **not attempted**. Do not label a real history PIT-grade until historical immutable source snapshots have been captured and validated.
4. Run independent Hawk/Opus review on the immutable commit and exact test outputs.
5. Address any `MODIFY`/`REJECT` finding, rerun review if code changes, then push the research branch. No merge to `main` without Ray's subsequent approval.
