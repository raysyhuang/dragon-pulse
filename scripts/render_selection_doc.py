#!/usr/bin/env python3
"""Render the selection study's evidence sections from analysis.json, or verify them.

Four consecutive audits found the research document disagreeing with its own artifact:
an obsolete hash, a stale t-statistic in prose, a p-column showing a return. Each time the
fix was applied by hand or in a throwaway shell session, so the next edit could reintroduce
the same class of error. This script is the fix: the document's evidence blocks are
GENERATED, and `--check` fails if they have drifted, so the binding can be enforced.

    python scripts/render_selection_doc.py --write    # regenerate the blocks
    python scripts/render_selection_doc.py --check    # exit 1 if the doc has drifted

Everything between a BEGIN/END marker pair is owned by this script. Prose outside the
markers is human-written and never touched.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANALYSIS = ROOT / "outputs" / "pit_selection_v2" / "analysis.json"
DOC = ROOT / "docs" / "research" / "2026-08-10-pit-selection-test-v2-corrected.md"
BEGIN, END = "<!-- BEGIN GENERATED: {} -->", "<!-- END GENERATED: {} -->"


def pct(v) -> str:
    return "—" if v is None else f"{v * 100:+.2f}%"


def num(v, n=2) -> str:
    return "—" if v is None else f"{v:.{n}f}"


def blocks(doc: dict) -> dict[str, str]:
    a = doc["analysis"]
    order = sorted(a, key=lambda k: -(a[k]["cagr"] if a[k]["cagr"] is not None else -9))

    rows = ["| Sleeve | mo | filled | cens | CAGR | beta | annual alpha | t | p | H1 (post-hoc) | H2 (post-hoc) |",
            "|---|---|---|---|---|---|---|---|---|---|---|"]
    for k in order:
        s, r = a[k], a[k]["regression"]
        name = f"**{k}**" if k == "control_spread" else k
        rows.append(f"| {name} | {s['months']} | {s['filled']} | {s['censored']} | "
                    f"{pct(s['cagr'])} | {num(r['beta'])} | {pct(r['annual_alpha'])} | "
                    f"{num(r['t_alpha'])} | {num(r['p_two_sided'])} | "
                    f"{pct(s['cagr_first_half'])} | {pct(s['cagr_second_half'])} |")

    best = max((k for k in a if a[k]["regression"]["t_alpha"] is not None),
               key=lambda k: a[k]["regression"]["t_alpha"])
    br = a[best]["regression"]
    ps = [a[k]["regression"]["p_two_sided"] for k in a
          if a[k]["regression"]["p_two_sided"] is not None]
    verdict = (f"**No factor is statistically significant.** The largest intercept "
               f"t-statistic in the study is **{br['t_alpha']:.5f}** (`{best}`, "
               f"p = {br['p_two_sided']:.5f}, df = {br['df']}); the smallest two-sided "
               f"p-value across all sleeves is **{min(ps):.5f}**. The half-sample columns "
               f"are a POST-HOC split of a single sample and are not an out-of-sample "
               f"holdout.")

    turnover = a["turnover_low"]
    tr = turnover["regression"]
    attribution = (
        f"`turnover_low` remains the only non-negative sleeve and remains a low-beta "
        f"portfolio ({num(tr['beta'])}) that earns {pct(turnover['mean_excess_up_months'])} "
        f"in up months and {pct(turnover['mean_excess_down_months'])} in down months. "
        f"Its post-hoc half-split is **positive then negative** "
        f"({pct(turnover['cagr_first_half'])} → {pct(turnover['cagr_second_half'])}).")

    sens = ["| Config | slots | turnover_low fills | control fills | cap-censored | turnover_low | control | excess |",
            "|---|---|---|---|---|---|---|---|"]
    for s in doc["execution_sensitivity"]:
        tl, cs = s["turnover_low"], s["control_spread"]
        sens.append(f"| {s['config']} | {s['slots']} | {tl['filled']} | {cs['filled']} | "
                    f"{tl['capacity_censored']} | {pct(tl['cagr'])} | {pct(cs['cagr'])} | "
                    f"{pct(s['excess'])} |")

    no_cap = next(s for s in doc["execution_sensitivity"] if s["cap_multiple"] is None)
    constrained = [s for s in doc["execution_sensitivity"] if s["cap_multiple"] is not None]
    signs = {s["excess"] > 0 for s in doc["execution_sensitivity"] if s["excess"] is not None}
    sensitivity_verdict = (
        f"Constraints do not move the estimate monotonically. Relative to the no-cap "
        f"turnover_low fill count ({no_cap['turnover_low']['filled']}), constrained "
        f"configurations fill {', '.join(str(s['turnover_low']['filled']) for s in constrained)} "
        f"positions; excess {'flips sign' if len(signs) > 1 else 'does not flip sign'} "
        f"across the persisted configurations. Under these execution constraints the "
        f"effect is not identified here; this is exploratory evidence, not a promotion basis.")

    non_control = {k: v for k, v in a.items() if k != "control_spread"}
    non_negative = [k for k, v in non_control.items() if v["cagr"] is not None and v["cagr"] >= 0]
    beat_control = [v for v in non_control.values() if v["months_beating_control"] > v["months"] / 2]
    beta_values = [v["regression"]["beta"] for v in beat_control if v["regression"]["beta"] is not None]
    evidence_summary = (
        f"**Supports:** across these {len(non_control)} sorts, on a bundle-validated "
        f"survivorship-controlled universe over {doc['rebalances']} monthly rebalances, "
        f"no factor produced significant alpha. The non-negative sleeve(s) are "
        f"{', '.join(non_negative) or 'none'}; sleeves beating the control in more than "
        f"half their observed months have beta between {min(beta_values):.2f} and "
        f"{max(beta_values):.2f}. `turnover_low` reverses in the POST-HOC second half "
        f"and is unidentified under the execution constraints above.")
    scope = doc["scope"]
    scope_statement = (
        f"**Does not support**, and the v1 claim to the contrary is withdrawn: this does "
        f"**not** close \"standard A-share factor selection.\" {scope['conclusion'].capitalize()}. "
        f"{len(non_control)} hand-chosen sorts at a single parameterisation over a "
        f"{a['control_spread']['months'] / 12:.1f}-year window cannot settle that "
        f"question. Limitations remain: {scope['neutralisation']}; {scope['half_split']}; "
        f"multiple testing: {scope['multiple_testing']}. It remains research / paper-only "
        f"and must not alter a selector, cron, execution rule, or order authority.")
    addendum_statistics = (
        f"**Corrected statistics** (OLS; the largest available regression df is "
        f"{max(v['regression']['df'] for v in a.values() if v['regression']['df'] is not None)}) "
        f"are generated from `analysis.json`, not hand-written. No two-sided p-value in "
        f"the study is below {min(ps):.2f}; the smallest is {min(ps):.5f}. "
        f"**Status is unchanged by these fixes:** exploratory; this does not close the "
        f"selection question.")

    ib = doc["input_binding"]
    prov = [
        f"- analysis self-hash `{doc['analysis_sha256']}`",
        f"- bundle `{doc['bundle_id']}` composite `{doc['bundle_composite_sha256']}`",
        f"- study script `{ib['study_script']['sha256']}`",
        f"- trade calendar `{ib['trade_calendar']['sha256']}` "
        f"({ib['trade_calendar']['source']}, schema `{ib['trade_calendar']['schema']}`)",
        f"- frozen plan `{ib['frozen_plan']['sha256']}` — {ib['frozen_plan']['rebalances']} "
        f"rebalances, {ib['frozen_plan']['first_signal']} .. {ib['frozen_plan']['last_signal']}",
        f"- raw inputs hash-bound but **not committed**: {len(ib['panels'])} daily panels, "
        f"{len(ib['daily_basic'])} daily_basic panels",
        f"- capture grade `{doc['capture_provenance_grade']}`; replayability: "
        f"{doc['scope']['replayability'].split('.')[0]}.",
    ]
    return {"results": "\n".join(rows), "verdict": verdict,
            "turnover_attribution": attribution,
            "sensitivity": "\n".join(sens) + "\n\n" + sensitivity_verdict,
            "evidence_summary": evidence_summary,
            "scope": scope_statement,
            "addendum_statistics": addendum_statistics,
            "provenance": "\n".join(prov)}


def apply(text: str, name: str, body: str) -> str:
    b, e = BEGIN.format(name), END.format(name)
    if b not in text or e not in text:
        return text + f"\n\n{b}\n{body}\n{e}\n"
    head, rest = text.split(b, 1)
    _, tail = rest.split(e, 1)
    return f"{head}{b}\n{body}\n{e}{tail}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--write", action="store_true")
    g.add_argument("--check", action="store_true")
    args = ap.parse_args()

    doc = json.loads(ANALYSIS.read_text())
    text = original = DOC.read_text()
    for name, body in blocks(doc).items():
        text = apply(text, name, body)

    if args.write:
        DOC.write_text(text)
        print(f"rendered {len(blocks(doc))} generated blocks into {DOC.relative_to(ROOT)}")
        return 0

    if text != original:
        print("DRIFT: the document's generated blocks do not match analysis.json.\n"
              "       run: python scripts/render_selection_doc.py --write", file=sys.stderr)
        return 1
    print("document matches analysis.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
