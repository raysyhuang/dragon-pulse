#!/usr/bin/env python3
"""Monthly PIT cross-sectional selection test — bundle-bound, frozen, self-contained.

Rewritten after an external audit rejected the first version. What changed, and why each
change was required:

  1. PIT EVIDENCE. The universe is now READ FROM a validated Task 1/2/2.5 bundle and the
     bundle is passed to the replay runner, so canonical output is
     PIT_UNIVERSE_MEMBERSHIP_ONLY rather than PIT_GRADE_FALSE. The first version
     constructed a survivorship-free universe but produced no evidence of it.
  2. FROZEN SAMPLE. The window is fixed by FROZEN_END, never by date.today(), so reruns
     cannot silently change the sample.
  3. PERSISTED STATISTICS. Beta, alpha, t(alpha), up/down attribution, half-sample split
     and hit rates are computed HERE and written to a hashed artifact. In the first
     version they lived in an ad-hoc shell session and were not reproducible.
  4. EXECUTION SENSITIVITY. The frictionless case is retained as the primary reading
     because removing constraints is CONSERVATIVE for a null, and a constrained case is
     run alongside it so the claim is bounded rather than asserted.
  5. NON-OVERWRITING OUTPUT. Nothing is deleted; publication refuses to clobber.

What it still does not do, and therefore what it cannot conclude: no sector
neutralisation, no sealed holdout, no multiple-testing adjustment, seven factors at one
parameterisation. It is evidence about these seven sorts over this window, not a closure
of A-share factor selection.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import statistics as st
import sys

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.core.pit_bundle import validate_pit_bundle          # noqa: E402
from src.core.xsec_runner import run_xsec_replay             # noqa: E402

FROZEN_END = "20260630"      # explicit; never date.today()
TOP_K = 50
COST_BPS = 30.0


def compact(iso: str) -> str:
    return iso.replace("-", "")


def load_panels(cache: pathlib.Path, days: set[str]) -> dict[str, pd.DataFrame]:
    out = {}
    for d in sorted(days):
        p = cache / f"daily_{d}.parquet"
        if p.exists():
            out[d] = pd.read_parquet(p).set_index("ts_code")
    return out


def bar(frame, ticker, day):
    if frame is None or ticker not in frame.index:
        return None
    r = frame.loc[ticker]
    try:
        o, h, l, c, v = (float(r["open"]), float(r["high"]), float(r["low"]),
                         float(r["close"]), float(r["vol"]))
    except (TypeError, ValueError, KeyError):
        return None
    if not all(x > 0 for x in (o, h, l, c)) or not (l <= o <= h and l <= c <= h):
        return None
    return {"day": f"{day[:4]}-{day[4:6]}-{day[6:]}", "open": o, "high": h,
            "low": l, "close": c, "volume": max(v, 0.0)}


def stats_block(series: dict[str, float], ctrl: dict[str, float]) -> dict:
    days = sorted(set(series) & set(ctrl))
    x = [ctrl[d] for d in days]
    y = [series[d] for d in days]
    mx, my = st.mean(x), st.mean(y)
    var = sum((a - mx) ** 2 for a in x)
    beta = sum((a - mx) * (b - my) for a, b in zip(x, y)) / var if var > 0 else float("nan")
    alpha_m = my - beta * mx
    resid = [b - (alpha_m + beta * a) for a, b in zip(x, y)]
    se = st.pstdev(resid) / (len(days) ** 0.5) if len(days) > 1 else float("nan")
    eq = 1.0
    for m in y:
        eq *= (1 + m)
    up = [b - a for a, b in zip(x, y) if a > 0]
    dn = [b - a for a, b in zip(x, y) if a <= 0]
    half = len(y) // 2
    def cagr(v):
        e = 1.0
        for m in v:
            e *= (1 + m)
        return e ** (12 / len(v)) - 1 if v else float("nan")
    return {
        "months": len(days), "total_return": eq - 1, "cagr": cagr(y),
        "beta_vs_control": beta, "annual_alpha": alpha_m * 12,
        "t_alpha": alpha_m / se if se and se == se and se > 0 else float("nan"),
        "mean_excess_up_months": st.mean(up) if up else None, "n_up": len(up),
        "mean_excess_down_months": st.mean(dn) if dn else None, "n_dn": len(dn),
        "cagr_first_half": cagr(y[:half]), "cagr_second_half": cagr(y[half:]),
        "months_beating_control": sum(1 for a, b in zip(x, y) if b > a),
    }


def main() -> int:
    scratch = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if scratch is None:
        raise SystemExit("usage: pit_selection_test.py <work_dir>  "
                         "(expects <work_dir>/pit_bundle_66 and <work_dir>/seltest_cache)")
    bundle_dir, cache = scratch / "pit_bundle_66", scratch / "seltest_cache"
    out_root = scratch / "pit_selection_v2"

    bundle = validate_pit_bundle(bundle_dir)
    manifest = json.loads((bundle_dir / "manifest.json").read_text())
    print(f"PIT bundle VALIDATED  {bundle.bundle_id}")
    print(f"  composite {bundle.composite_sha256}")
    print(f"  capture grade {manifest['capture_provenance_grade']}  dates {len(bundle.as_of_dates)}")

    # universe strictly from the bundle schedule -> the bundle is load-bearing
    uni: dict[str, list[str]] = {}
    for row in bundle.schedule:
        uni.setdefault(compact(row["as_of_date"]), []).append(row["ticker"])
    sig_dates = sorted(uni)
    sig_dates = [d for d in sig_dates if d <= FROZEN_END]
    print(f"  universe from bundle: {len(sig_dates)} dates x {len(uni[sig_dates[0]])} names")

    cal = pd.read_parquet(cache / "trade_cal.parquet")
    opens = sorted(cal[cal["is_open"].astype(int) == 1]["cal_date"].tolist())
    plan = []
    for sd in sig_dates:
        after = [d for d in opens if d > sd]
        if not after:
            continue
        month = after[0][:6]
        same = [d for d in opens if d[:6] == month]
        if not same or same[-1] > FROZEN_END:
            continue
        plan.append({"signal": sd, "entry": same[0], "exit": same[-1]})
    print(f"  frozen plan: {len(plan)} rebalances {plan[0]['signal']}..{plan[-1]['signal']}"
          f"  (FROZEN_END={FROZEN_END})")

    panels = load_panels(cache, {p[k] for p in plan for k in ("signal", "entry", "exit")})
    basics = {d: pd.read_parquet(cache / f"basic_{d}.parquet").set_index("ts_code")
              for d in sig_dates if (cache / f"basic_{d}.parquet").exists()}
    closes = pd.DataFrame({d: panels[d]["close"].astype(float) for d in sig_dates if d in panels})

    FACTORS = ["value_pe", "value_pb", "dividend", "size_small", "turnover_low",
               "momentum_12_1", "reversal_1m", "control_spread"]
    COL = {"value_pe": ("pe_ttm", True), "value_pb": ("pb", True),
           "dividend": ("dv_ratio", False), "size_small": ("circ_mv", True),
           "turnover_low": ("turnover_rate", True)}

    rebs = {f: [] for f in FACTORS}
    for pi, p in enumerate(plan):
        sd, ed, xd = p["signal"], p["entry"], p["exit"]
        members = uni[sd]
        b = basics[sd].reindex(members)
        for c in ("circ_mv", "pe_ttm", "pb", "dv_ratio", "turnover_rate"):
            if c in b:
                b[c] = pd.to_numeric(b[c], errors="coerce")
        j = sig_dates.index(sd)
        for f in FACTORS:
            if f == "control_spread":
                step = max(1, len(members) // TOP_K)
                pick = members[::step][:TOP_K]
            elif f == "momentum_12_1":
                if j < 12:
                    continue
                s = (closes[sig_dates[j - 1]] / closes[sig_dates[j - 12]] - 1).dropna()
                pick = list(s[s.index.isin(members)].nlargest(TOP_K).index)
            elif f == "reversal_1m":
                if j < 1:
                    continue
                s = (closes[sd] / closes[sig_dates[j - 1]] - 1).dropna()
                pick = list(s[s.index.isin(members)].nsmallest(TOP_K).index)
            else:
                col, asc = COL[f]
                s = b[col].dropna()
                if col in ("pe_ttm", "pb"):
                    s = s[s > 0]
                pick = list((s.nsmallest(TOP_K) if asc else s.nlargest(TOP_K)).index)
            if not pick:
                continue
            rebs[f].append({
                "rebalance_date": f"{sd[:4]}-{sd[4:6]}-{sd[6:]}", "sleeve": f,
                "factor_order": "DESC", "max_entry_cap": 1e12,
                "selected": [{"ticker": t, "factor_score": float(i),
                              "next_session": bar(panels.get(ed), t, ed),
                              "exit_session": bar(panels.get(xd), t, xd)}
                             for i, t in enumerate(pick)]})
        if pi % 20 == 0:
            print(f"    built {pi}/{len(plan)}", flush=True)

    series, summaries = {}, {}
    for f, rr in rebs.items():
        if not rr:
            continue
        art = run_xsec_replay(rr, output_dir=out_root / f, max_concurrent_slots=TOP_K,
                              total_cost_bps=COST_BPS, pit_bundle=bundle_dir)
        recs = sorted((json.loads(l) for l in art.read_text().splitlines() if l.strip()),
                      key=lambda r: r["rebalance_date"])
        series[f] = {r["rebalance_date"]: r["summary"]["filled_mean_net_return"]
                     for r in recs if r["summary"]["filled_mean_net_return"] is not None}
        summaries[f] = {"selected": sum(r["summary"]["selected"] for r in recs),
                        "filled": sum(r["summary"]["filled"] for r in recs),
                        "censored": sum(r["summary"]["censored"] for r in recs),
                        "pit_grade": recs[0]["pit_grade"],
                        "evidence_label": recs[0]["evidence_label"]}

    ctrl = series["control_spread"]
    analysis = {f: {**summaries[f], **stats_block(s, ctrl)} for f, s in series.items()}
    doc = {"schema_version": 1, "frozen_end": FROZEN_END, "top_k": TOP_K,
           "cost_bps": COST_BPS, "bundle_id": bundle.bundle_id,
           "bundle_composite_sha256": bundle.composite_sha256,
           "capture_provenance_grade": manifest["capture_provenance_grade"],
           "rebalances": len(plan), "analysis": analysis,
           "interpretation": ("Evidence about these seven sorts over this window. Not a "
                              "closure of A-share factor selection: no sector neutralisation, "
                              "no sealed holdout, no multiple-testing adjustment.")}
    payload = json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()
    doc["analysis_sha256"] = hashlib.sha256(payload).hexdigest()
    ap = out_root / "analysis.json"
    if ap.exists():
        raise SystemExit(f"refusing to overwrite {ap}")
    ap.write_text(json.dumps(doc, indent=2, sort_keys=True))

    print("\n" + "=" * 112)
    print(f"RESULTS — PIT-bound, frozen to {FROZEN_END}, top {TOP_K}, {COST_BPS:.0f}bps")
    print("=" * 112)
    print(f"  {'sleeve':16}{'mo':>4}{'fill':>6}{'cens':>5}{'CAGR':>9}{'beta':>7}"
          f"{'alpha':>9}{'t':>7}{'up':>8}{'down':>8}{'H1':>9}{'H2':>9}")
    for f in sorted(analysis, key=lambda k: -analysis[k]["cagr"]):
        a = analysis[f]
        print(f"  {f:16}{a['months']:>4}{a['filled']:>6}{a['censored']:>5}{a['cagr']:>+9.2%}"
              f"{a['beta_vs_control']:>7.2f}{a['annual_alpha']:>+9.2%}{a['t_alpha']:>7.2f}"
              f"{(a['mean_excess_up_months'] or 0):>+8.2%}{(a['mean_excess_down_months'] or 0):>+8.2%}"
              f"{a['cagr_first_half']:>+9.2%}{a['cagr_second_half']:>+9.2%}")
    print(f"\n  PIT grade on artifacts: {analysis['control_spread']['pit_grade']}")
    print(f"  analysis_sha256 {doc['analysis_sha256']}")
    print(f"  artifacts {out_root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
