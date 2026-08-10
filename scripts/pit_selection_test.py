#!/usr/bin/env python3
"""Monthly PIT cross-sectional selection test — bundle-bound, frozen, self-contained.

Rewritten after an external audit rejected the first version. What changed, and why each
change was required:

  1. PIT EVIDENCE. The universe is now READ FROM a validated Task 1/2/2.5 bundle and the
     bundle is passed to the replay runner, so canonical output is
     PIT_UNIVERSE_MEMBERSHIP_ONLY rather than PIT_GRADE_FALSE. The first version
     constructed a survivorship-controlled universe but produced no evidence of it.
  2. FROZEN SAMPLE. The window is fixed by FROZEN_END, never by date.today(), so reruns
     cannot silently change the sample.
  3. PERSISTED STATISTICS. Beta, alpha, t(alpha), up/down attribution, half-sample split
     and hit rates are computed HERE and written to a hashed artifact. In the first
     version they lived in an ad-hoc shell session and were not reproducible.
  4. EXECUTION SENSITIVITY, IN-SCRIPT. An earlier draft argued the frictionless case was
     "conservative for a null" because constraints only reduce factor returns. That is
     FALSE: constraints collapse the filled sample and flip the sign of the excess across
     configurations. The sensitivity grid is therefore computed HERE and persisted, and
     the frictionless case is reported as one configuration among several rather than as
     a conservative bound.
  5. NON-OVERWRITING OUTPUT. Nothing is deleted; publication refuses to clobber.

What it still does not do, and therefore what it cannot conclude: no sector
neutralisation, no sealed holdout, no multiple-testing adjustment, seven factors at one
parameterisation. It is evidence about these seven sorts over this window, not a closure
of A-share factor selection.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import statistics as st
import sys
import tempfile

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.core.pit_bundle import validate_pit_bundle          # noqa: E402
from src.core.xsec_runner import run_xsec_replay             # noqa: E402

class AnalysisDurabilityUncertainError(OSError):
    """The analysis artifact was linked but its directory entry was not synced."""

    def __init__(self, artifact: pathlib.Path) -> None:
        self.artifact = artifact
        super().__init__(f"analysis artifact {artifact}: artifact may exist but durability "
                         "is uncertain; inspect/reconcile manually")


def publish_analysis(output: pathlib.Path, body: bytes) -> pathlib.Path:
    """Exclusively publish durable analysis bytes without deleting a linked artifact."""
    output.mkdir(parents=True, exist_ok=True)
    artifact = output / "analysis.json"
    temporary: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile("wb", dir=output, prefix=".analysis.", delete=False) as fh:
            temporary = pathlib.Path(fh.name)
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        os.link(temporary, artifact)
        directory_fd = os.open(output, os.O_RDONLY)
        try:
            try:
                os.fsync(directory_fd)
            except OSError as exc:
                raise AnalysisDurabilityUncertainError(artifact) from exc
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return artifact


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


def ols_intercept(y: list[float], x: list[float]) -> dict:
    """OLS y = alpha + beta*x. Returns the intercept t-statistic with the proper
    standard error: s * sqrt(1/n + xbar^2 / Sxx), with s^2 = SSR/(n-2).

    An earlier version divided alpha by pstdev(resid)/sqrt(n), which omits both the
    degrees-of-freedom correction and the leverage term. That is not a t-statistic.
    """
    n = len(y)
    if n < 3:
        return {"beta": None, "alpha_monthly": None, "se_alpha": None,
                "t_alpha": None, "df": None, "p_two_sided": None}
    xbar, ybar = sum(x) / n, sum(y) / n
    sxx = sum((a - xbar) ** 2 for a in x)
    if sxx <= 0:
        return {"beta": None, "alpha_monthly": None, "se_alpha": None,
                "t_alpha": None, "df": None, "p_two_sided": None}
    beta = sum((a - xbar) * (b - ybar) for a, b in zip(x, y)) / sxx
    alpha = ybar - beta * xbar
    ssr = sum((b - (alpha + beta * a)) ** 2 for a, b in zip(x, y))
    df = n - 2
    s2 = ssr / df
    se = (s2 * (1.0 / n + xbar ** 2 / sxx)) ** 0.5
    t = alpha / se if se > 0 else None
    p = None
    if t is not None:
        # two-sided p from Student-t via its incomplete-beta representation
        import math
        xb = df / (df + t * t)
        def betacf(a, b, xv):
            qab, qap, qam = a + b, a + 1.0, a - 1.0
            c, d = 1.0, 1.0 - qab * xv / qap
            d = 1.0 / (d if abs(d) > 1e-30 else 1e-30)
            h = d
            for m in range(1, 200):
                m2 = 2 * m
                aa = m * (b - m) * xv / ((qam + m2) * (a + m2))
                d = 1.0 + aa * d; d = 1.0 / (d if abs(d) > 1e-30 else 1e-30)
                c = 1.0 + aa / (c if abs(c) > 1e-30 else 1e-30); h *= d * c
                aa = -(a + m) * (qab + m) * xv / ((a + m2) * (qap + m2))
                d = 1.0 + aa * d; d = 1.0 / (d if abs(d) > 1e-30 else 1e-30)
                c = 1.0 + aa / (c if abs(c) > 1e-30 else 1e-30)
                de = d * c; h *= de
                if abs(de - 1.0) < 3e-12:
                    break
            return h
        def betai(a, b, xv):
            if xv <= 0: return 0.0
            if xv >= 1: return 1.0
            lb = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                  + a * math.log(xv) + b * math.log(1 - xv))
            return (math.exp(lb) * betacf(a, b, xv) / a if xv < (a + 1) / (a + b + 2)
                    else 1.0 - math.exp(lb) * betacf(b, a, 1 - xv) / b)
        p = betai(df / 2.0, 0.5, xb)
    return {"beta": beta, "alpha_monthly": alpha, "se_alpha": se,
            "t_alpha": t, "df": df, "p_two_sided": p}


def stats_block(series: dict[str, float], ctrl: dict[str, float]) -> dict:
    days = sorted(set(series) & set(ctrl))
    x = [ctrl[d] for d in days]
    y = [series[d] for d in days]
    reg = ols_intercept(y, x)
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
        "regression": {"specification": "y_t = alpha + beta * control_t + e_t, OLS, "
                                        "monthly net returns, no HAC correction",
                       **reg,
                       "annual_alpha": (reg["alpha_monthly"] * 12
                                        if reg["alpha_monthly"] is not None else None)},
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
    out_root = scratch / "pit_selection_v3"

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

    def replay(name, rr, slots, cap_override=None, panels_ref=None):
        if cap_override is not None:
            for r in rr:
                sd = compact(r["rebalance_date"])
                prev = panels_ref[sd]["close"].astype(float)
                med = float(prev.reindex([x["ticker"] for x in r["selected"]]).median())
                r = r
                r["max_entry_cap"] = med * cap_override
        art = run_xsec_replay(rr, output_dir=out_root / name, max_concurrent_slots=slots,
                              total_cost_bps=COST_BPS, pit_bundle=bundle_dir)
        recs = sorted((json.loads(l) for l in art.read_text().splitlines() if l.strip()),
                      key=lambda r: r["rebalance_date"])
        return recs

    series, summaries = {}, {}
    for f, rr in rebs.items():
        if not rr:
            continue
        recs = replay(f, [dict(r) for r in rr], TOP_K)
        series[f] = {r["rebalance_date"]: r["summary"]["filled_mean_net_return"]
                     for r in recs if r["summary"]["filled_mean_net_return"] is not None}
        summaries[f] = {"selected": sum(r["summary"]["selected"] for r in recs),
                        "filled": sum(r["summary"]["filled"] for r in recs),
                        "no_fill": sum(r["summary"]["no_fill"] for r in recs),
                        "censored": sum(r["summary"]["censored"] for r in recs),
                        "pit_grade": recs[0]["pit_grade"],
                        "evidence_label": recs[0]["evidence_label"]}

    ctrl = series["control_spread"]
    analysis = {f: {**summaries[f], **stats_block(s_, ctrl)} for f, s_ in series.items()}

    # ---- blocker 4: execution sensitivity, computed HERE and persisted ----------
    def cagr_of(recs):
        ms = [r["summary"]["filled_mean_net_return"] for r in recs
              if r["summary"]["filled_mean_net_return"] is not None]
        e = 1.0
        for m in ms:
            e *= (1 + m)
        return (e ** (12 / len(ms)) - 1) if ms else None
    sensitivity = []
    for cap_mult, slots, tag in ((None, TOP_K, "no_cap_50_slots"),
                                 (1.02, 20, "cap_1.02x_20_slots"),
                                 (1.00, 10, "cap_1.00x_10_slots")):
        row = {"config": tag, "cap_multiple": cap_mult, "slots": slots}
        for f in ("turnover_low", "control_spread"):
            rr = [dict(r) for r in rebs[f]]
            if cap_mult is not None:
                for r in rr:
                    sd = compact(r["rebalance_date"])
                    med = float(panels[sd]["close"].astype(float)
                                .reindex([x["ticker"] for x in r["selected"]]).median())
                    r["max_entry_cap"] = med * cap_mult
            recs = replay(f"sens_{tag}_{f}", rr, slots)
            row[f] = {"cagr": cagr_of(recs),
                      "filled": sum(r["summary"]["filled"] for r in recs),
                      "no_fill": sum(r["summary"]["no_fill"] for r in recs),
                      "capacity_censored": sum(r["summary"]["capacity_censored"] for r in recs)}
        a, b = row["turnover_low"]["cagr"], row["control_spread"]["cagr"]
        row["excess"] = (a - b) if (a is not None and b is not None) else None
        sensitivity.append(row)

    def sha(fp):
        return hashlib.sha256(pathlib.Path(fp).read_bytes()).hexdigest()
    plan_repr = json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()
    input_binding = {
        "study_script": {"path": "scripts/pit_selection_test.py", "sha256": sha(__file__)},
        "trade_calendar": {"path": "seltest_cache/trade_cal.parquet",
                           "sha256": sha(cache / "trade_cal.parquet"),
                           "source": "tushare trade_cal, exchange=SSE",
                           "schema": "cal_date,is_open",
                           "producer": "scripts/pit_selection_test.py fetch step (cached)"},
        "frozen_plan": {"rebalances": len(plan),
                        "first_signal": plan[0]["signal"], "last_signal": plan[-1]["signal"],
                        "sha256": hashlib.sha256(plan_repr).hexdigest(),
                        "definition": "signal=last session of month M; entry=first session "
                                      "of M+1; exit=last session of M+1; capped at FROZEN_END"},
        "panels": {f"{d}": sha(cache / f"daily_{d}.parquet")
                   for d in sorted({p[k] for p in plan for k in ("signal", "entry", "exit")})
                   if (cache / f"daily_{d}.parquet").exists()},
        "daily_basic": {d: sha(cache / f"basic_{d}.parquet") for d in sig_dates
                        if (cache / f"basic_{d}.parquet").exists()},
        "note": ("panel bytes are HASH-BOUND here but NOT committed; an auditor holding "
                 "the panels can verify they are the ones used, but cannot re-derive "
                 "selection from this repository alone"),
    }
    doc = {"schema_version": 3, "input_binding": input_binding, "frozen_end": FROZEN_END, "top_k": TOP_K,
           "cost_bps": COST_BPS, "bundle_id": bundle.bundle_id,
           "bundle_composite_sha256": bundle.composite_sha256,
           "capture_provenance_grade": manifest["capture_provenance_grade"],
           "rebalances": len(plan), "analysis": analysis,
           "execution_sensitivity": sensitivity,
           "scope": {
               "survivorship": ("controlled via list/delist dates from a listing table "
                                "fetched today; NOT survivorship-free in the absolute "
                                "sense, since an issuer absent from the vendor's current "
                                "records is invisible"),
               "half_split": ("POST-HOC split of one sample; NOT a sealed out-of-sample "
                              "holdout"),
               "multiple_testing": "none applied across seven sleeves",
               "neutralisation": "none: no sector, industry or size neutralisation",
               "replayability": ("ACCOUNTABLE OUTCOMES, NON-REPLAYABLE SELECTION INPUTS. "
                                 "Canonical rows carry every executed entry and exit price, "
                                 "so all accounting is auditable from this repository. "
                                 "Selection construction is not reproducible here because "
                                 "the raw provider panels are hash-bound but not committed. "
                                 "The runner's CALLER_ASSERTED_UNVERIFIED label is therefore "
                                 "correct and remains limiting."),
               "conclusion": ("exploratory evidence about these seven sorts over this "
                              "window; does NOT close A-share factor selection")}}
    # blocker 2: strict JSON, no bare NaN anywhere
    def clean(o):
        if isinstance(o, float):
            return None if o != o or o in (float("inf"), float("-inf")) else o
        if isinstance(o, dict):
            return {k: clean(v) for k, v in o.items()}
        if isinstance(o, list):
            return [clean(v) for v in o]
        return o
    doc = clean(doc)
    payload = json.dumps(doc, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    doc["analysis_sha256"] = hashlib.sha256(payload).hexdigest()
    body = json.dumps(doc, indent=2, sort_keys=True, allow_nan=False).encode()

    # blocker 3: atomic, no-replace publication via os.link (not check-then-write)
    publish_analysis(out_root, body)

    print("\n" + "=" * 116)
    print(f"RESULTS — PIT-bound, frozen to {FROZEN_END}, top {TOP_K}, {COST_BPS:.0f}bps")
    print("=" * 116)
    print(f"  {'sleeve':16}{'mo':>4}{'fill':>6}{'cens':>5}{'CAGR':>9}{'beta':>7}"
          f"{'alpha':>9}{'t':>7}{'p':>7}{'H1':>9}{'H2':>9}")
    for f in sorted(analysis, key=lambda k: -(analysis[k]["cagr"] or -9)):
        a = analysis[f]; r = a["regression"]
        fmt = lambda v, w, s="": ("{:>%d.2f}" % w).format(v) if v is not None else " " * (w - 1) + "-"
        print(f"  {f:16}{a['months']:>4}{a['filled']:>6}{a['censored']:>5}{a['cagr']:>+9.2%}"
              f"{fmt(r['beta'],7)}"
              f"{(('%+9.2f%%' % (r['annual_alpha']*100)) if r['annual_alpha'] is not None else '        -')}"
              f"{fmt(r['t_alpha'],7)}{fmt(r['p_two_sided'],7)}"
              f"{a['cagr_first_half']:>+9.2%}{a['cagr_second_half']:>+9.2%}")
    print(f"\n  EXECUTION SENSITIVITY (in-script, persisted)")
    for row in sensitivity:
        tl, cs = row["turnover_low"], row["control_spread"]
        ex = f"{row['excess']:+.2%}" if row["excess"] is not None else "-"
        print(f"    {row['config']:20} fills {tl['filled']:>5}/{cs['filled']:<5} "
              f"cap_cens {tl['capacity_censored']:>5}  turnover_low {tl['cagr']:+.2%}  "
              f"control {cs['cagr']:+.2%}  excess {ex}")
    print(f"\n  PIT grade: {analysis['control_spread']['pit_grade']}")
    print(f"  analysis_sha256 {doc['analysis_sha256']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
