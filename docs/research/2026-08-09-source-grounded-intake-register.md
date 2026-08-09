# A-Share Pipeline Source Register — Initial Scan (2026-08-09)

**Scope:** source-grounded idea intake for the preregistered Dragon Pulse reliability study. This is research/paper-only; it is not a stock recommendation, a production ranker, or a reason to change the 09:00 selection.

## Evidence tiers

- **Tier A — China-specific peer-reviewed literature:** establishes that a hypothesis has prior academic support. It does not establish a live tradable edge in the present market.
- **Tier B — structured vendor facts:** Tushare as-of fields cross-checked from stored responses. These establish data values and availability, not valuation correctness or alpha.
- **Tier C — published research/media opinion:** dated theme lead only. It must be tested mechanically and is never an execution input by itself.

## Strategy-intake register

| ID | Hypothesis | Evidence / exact source | Data available | Intake decision / primary failure mode |
|---|---|---|---|---|
| S1 | **Value research lead** (exact measure unverified) | Tier A bibliographic citation: `Size and value in China`, *Journal of Financial Economics* (2019), https://doi.org/10.1016/j.jfineco.2019.03.008 | Tushare `daily_basic`: https://tushare.pro/document/2?doc_id=32 | Not an implementation spec. The paper's full text was not accessible here, so do not infer that it validates low-PB construction. Any exact measure must be independently verified; the existing price-only value sleeve is the mandatory control. |
| S2 | **Short-horizon reversal lead** | Tier A bibliographic citation: `Profitability of reversal strategies: A modified version of the Carhart model in China`, *Economic Modelling* (2018), https://doi.org/10.1016/j.econmod.2017.09.003 | Tushare daily bars: https://tushare.pro/document/2?doc_id=27 | No build now. Exact construction/finding remains unverified; high turnover, price-limit and next-open fill bias are likely fatal. The existing repo's full-cycle replay was weak, so no parameter re-tuning from that outcome. |
| S3 | **Earnings/forecast disclosure-drift lead** | Tier A bibliographic citation: `Post-earnings announcement abnormal return in the Chinese equity market`, *J. International Financial Markets, Institutions & Money* (2011), https://doi.org/10.1016/j.intfin.2011.04.002 | Forecast: https://tushare.pro/document/2?doc_id=45; earnings express: https://tushare.pro/document/2?doc_id=46 | C3 feasibility only. Date-only `ann_date` is insufficient to prove a pre-open timestamp; no return test until availability/version evidence exists. |
| S4 | **Medium-horizon momentum lead** | Tier A bibliographic citation, aged sample: `Contrarian and momentum strategies in the China stock market: 1993–2000`, *Pacific-Basin Finance Journal* (2002), https://doi.org/10.1016/S0927-538X(02)00046-X | Tushare daily bars | Rejected as an immediate build: exact cited finding remains unverified and our full-cycle replay already finds daily momentum weak/negative. Re-open only with a materially different, preregistered data regime—not lookback shopping. |
| S5 | **Profitability / conservative-investment quality lead** | Tier A bibliographic citation: `The five-factor asset pricing model tests for the Chinese stock market`, *Pacific-Basin Finance Journal* (2017), https://doi.org/10.1016/j.pacfin.2017.02.001 | Tushare financial indicator joined with `ann_date` and daily valuation | C2 guardrail, not a standalone return claim. Full-text findings/constructs remain unverified; ROE is leverage-sensitive and financials need a separate specification. |

**Citation boundary:** DOI links resolved to the publishers, but full texts were not browser-accessible from this environment. The register uses them as durable bibliographic citations; it does **not** claim the exact factor construction or finding has been read/verified. Any implementation must be independently reproduced from accessible text or declared as a new specification before implementation.

## Current valuation-opinion register

### V1 — securities brokers / 券商: qualified low-valuation theme

**Tier C claim (not a fact of intrinsic value):** a 2026-08-03 *National Business Daily* article, syndicated by Eastmoney, reported the securities sector at **1.19× PB**, its **9.7th percentile since 2010**, and described 2025 dividends declared by 40 listed brokers as exceeding RMB70bn.

- Source: https://finance.eastmoney.com/a/202608033829716796.html
- The accessible page confirms its date/source and the quoted 1.19x / 9.7th-percentile statement. It also expressly says it is not investment advice.

**Tier C corroborating opinion:** a 2026-08-07 *Shanghai Securities News* article reported bullish research views driven by low valuation, interim earnings and policy, and named 广发证券, 中信证券, 华泰证券 and 国泰海通 among analyst recommendations.

- Source: https://finance.eastmoney.com/a/202608073835340427.html
- The accessible article confirms this is analysts'/media commentary. Its named stocks remain *their* recommendations, not Dragon Pulse selections.

**Tier B Tushare factual cross-check:** `outputs/valuation_theme_crosscheck_2026-08-07.json` stores values at the 2026-08-07 close and the latest financial indicator with `ann_date <= 2026-08-07`. Sample P/B values were: 华泰证券 **0.9772**, 国泰海通 **0.9672**, 广发证券 **1.1893**, 中信证券 **1.4981**, 东兴证券 **1.4653**, 信达证券 **2.5071**.

Interpretation: this confirms that the named list is heterogeneous and that two of the sampled names were below 1× book at the as-of close. It does **not** establish sector PB, book-quality comparability, a buying opportunity, or expected return. Broker leverage and operating-cash-flow ratios must not be compared mechanically with industrials.

### V2 — innovative drugs / CRO: earnings-and-catalyst narrative, **not** an undervaluation conclusion

- Media source: https://finance.eastmoney.com/a/202608073835202626.html (2026-08-07) reported sector moves and an industry comment framing returns around earnings/globalisation delivery.
- Issuer source example: https://www.wuxiapptec.cn/news/wuxi-news/wuxi-apptec-crdmo-model-delivers-strong-results (2026-08-03) reported unaudited H1 figures and raised guidance for 药明康德.

Intake decision: no valuation signal until a PIT screen supplies valuation, profitability, cash generation, leverage/dilution and post-disclosure return controls. Recent earnings growth is not “cheapness.”

### V3 — AI hardware / semiconductors / storage: repair/flow narrative, **not** an undervaluation conclusion

- Source: https://finance.eastmoney.com/a/202608083835609698.html (2026-08-08) reports analyst views around correction repair and earnings improvement, alongside fund-flow figures.

Intake decision: do not create a value sleeve from flow or analyst narrative. If pursued, it is a separate event/revision hypothesis, subject to C3's timestamp/control gate.

## Immediate research decisions

1. **Do not add internet opinions to the 09:00 ranker.** They remain Tier C context.
2. **Do not reopen generic momentum.** Existing full-cycle output is a rejection signal, and internet citations do not override it.
3. **Measurement before model:** build no C2 score until historical PIT inputs and a next-session, no-fill-aware replay exist. C2 is an ablation of a failed price-only value sleeve, not a fresh factor discovery.
4. **C3 remains feasibility only:** prove actual pre-open disclosure availability/revisions before writing a return test.
5. **Independent checking required:** any C2/C3 implementation must receive a separate data/PIT and statistical review before the outcome report is treated as evidence. See `2026-08-09-hawk-review.md`.
