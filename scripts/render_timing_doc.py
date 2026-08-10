#!/usr/bin/env python3
"""Render the timing study's evidence blocks from timing_study_analysis.json, or verify.

The study document previously published figures that no committed code reproduced. This
generator owns every number in it; `--check` fails if the document has drifted.

    python scripts/render_timing_doc.py --write
    python scripts/render_timing_doc.py --check
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANALYSIS = ROOT / "outputs" / "paper_lab" / "timing_study_analysis.json"
DOC = ROOT / "docs" / "research" / "2026-08-10-edge-test-trend-timing.md"
B, E = "<!-- BEGIN GENERATED: {} -->", "<!-- END GENERATED: {} -->"


def pct(v):
    return "—" if v is None else f"{v * 100:+.2f}%"


def blocks(d: dict) -> dict[str, str]:
    hl, k1, k2 = d["headline"], d["k1_window"], d["k2_grid"]
    head = [
        "| Arm | CAGR | Sharpe | maxDD |", "|---|---|---|---|",
        f"| ChiNext 50/200 timed | {pct(hl['timed']['cagr'])} | {hl['timed']['sharpe']:.2f} | {pct(hl['timed']['maxdd'])} |",
        f"| Buy & hold | {pct(hl['buy_hold']['cagr'])} | {hl['buy_hold']['sharpe']:.2f} | {pct(hl['buy_hold']['maxdd'])} |",
        "", f"Exposure {hl['exposure']:.0%}, {hl['trades_per_year']:.1f} side-trades/year, "
        f"{d['side_bps']:.0f} bps/side, cash {d['cash_annual']:.1%}, "
        f"ChiNext dividend yield {d['dividend_yield']['ChiNext']:.1%}.",
        f"Fill: {d['convention']}.",
    ]
    kills = [
        "| Kill test | Result |", "|---|---|",
        f"| K1 window selection | cuts drawdown **{k1['cuts_drawdown']}/{k1['starts']}** start quarters; "
        f"beats B&H Sharpe **{k1['beats_sharpe']}/{k1['starts']}**, worst {k1['worst_d_sharpe']:+.2f}, "
        f"median {k1['median_d_sharpe']:+.2f} |",
        f"| K2 parameter grid | cuts drawdown **{k2['cuts_drawdown']}/{k2['cells']}** cells; "
        f"50/200 at the **{k2['percentile_50_200']:.0%}** percentile "
        f"(best cell {k2['best']['fast']}/{k2['best']['slow']} at {k2['best']['sharpe']:.2f} — "
        f"a plateau, not a peak; do not switch to the maximum) |",
    ]
    for n, v in d["k3_indices"].items():
        kills.append(f"| K3 {n} | timed {pct(v['timed']['cagr'])} / {v['timed']['sharpe']:.2f} "
                     f"vs B&H {pct(v['buy_hold']['cagr'])} / {v['buy_hold']['sharpe']:.2f} |")
    sp, pc = d["split_post_hoc"], d["pre_cache_window"]
    kills.append(f"| Post-hoc split ({sp['split_at']}) | first {pct(sp['first']['cagr'])}, "
                 f"second {pct(sp['second']['cagr'])} — {sp['note']} |")
    kills.append(f"| {pc['window']} | timed {pct(pc['timed']['cagr'])} / {pc['timed']['sharpe']:.2f} "
                 f"vs B&H {pct(pc['buy_hold']['cagr'])} / {pc['buy_hold']['sharpe']:.2f} — {pc['note']} |")

    be = ["| Index | CAGR break-even yield | Assumed actual yield |", "|---|---|---|"]
    for n, v in d["dividend_break_even"].items():
        b = v["cagr_break_even_yield"]
        be.append(f"| {n} | {'none up to 12%' if b is None else f'{b:.2%}'} | "
                  f"{v['assumed_yield']:.1%} |")
    be += ["", "Assumed yields are stated inputs, not measured from the data; the break-even "
           "column is what the result is sensitive to."]

    cost = ["| bps/side | CAGR | Sharpe |", "|---|---|---|"]
    for c in d["cost_sensitivity"]:
        cost.append(f"| {c['side_bps']:.0f} | {pct(c['cagr'])} | {c['sharpe']:.2f} |")

    inp = d["inputs"]
    prov = [f"- analysis hash `{d['analysis_sha256']}`",
            f"- capture grade `{inp['provenance_grade']}` ({inp['caveat']})"]
    for n, v in inp["series"].items():
        prov.append(f"- {n} `{v['ts_code']}` {v['rows']} rows {v['first']}..{v['last']} "
                    f"sha256 `{v['sha256'][:16]}…`")
    return {"headline": "\n".join(head), "kills": "\n".join(kills),
            "breakeven": "\n".join(be), "costs": "\n".join(cost), "provenance": "\n".join(prov)}


def apply(t: str, n: str, body: str) -> str:
    b, e = B.format(n), E.format(n)
    if b not in t or e not in t:
        return t + f"\n\n{b}\n{body}\n{e}\n"
    h, rest = t.split(b, 1)
    _, tail = rest.split(e, 1)
    return f"{h}{b}\n{body}\n{e}{tail}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--write", action="store_true")
    g.add_argument("--check", action="store_true")
    a = ap.parse_args()
    d = json.loads(ANALYSIS.read_text())
    t = orig = DOC.read_text()
    for n, body in blocks(d).items():
        t = apply(t, n, body)
    if a.write:
        DOC.write_text(t)
        print(f"rendered {len(blocks(d))} blocks into {DOC.relative_to(ROOT)}")
        return 0
    if t != orig:
        print("DRIFT: run scripts/render_timing_doc.py --write", file=sys.stderr)
        return 1
    print("document matches timing_study_analysis.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
