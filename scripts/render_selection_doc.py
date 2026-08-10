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

    sens = ["| Config | slots | turnover_low fills | control fills | cap-censored | turnover_low | control | excess |",
            "|---|---|---|---|---|---|---|---|"]
    for s in doc["execution_sensitivity"]:
        tl, cs = s["turnover_low"], s["control_spread"]
        sens.append(f"| {s['config']} | {s['slots']} | {tl['filled']} | {cs['filled']} | "
                    f"{tl['capacity_censored']} | {pct(tl['cagr'])} | {pct(cs['cagr'])} | "
                    f"{pct(s['excess'])} |")

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
            "sensitivity": "\n".join(sens), "provenance": "\n".join(prov)}


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
