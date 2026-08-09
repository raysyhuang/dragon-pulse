# A-Share Pipeline Source Register — Initial Scan (2026-08-09)

**Scope:** source-grounded idea intake for the preregistered Dragon Pulse reliability study. This is research/paper-only; it is not a stock recommendation, a production ranker, or a reason to change the 09:00 selection.

## Evidence tiers

- **Tier A — China-specific peer-reviewed literature:** establishes that a hypothesis has prior academic support. It does not establish a live tradable edge in the present market.
- **Tier B — structured vendor facts:** Tushare as-of fields cross-checked from stored responses. These establish data values and availability, not valuation correctness or alpha.
- **Tier C — published research/media opinion:** dated theme lead only. It must be tested mechanically and is never an execution input by itself.

## Strategy-intake register

| ID | Hypothesis | Evidence / exact source | Data available | Intake decision / primary failure mode |
|---|---|---|---|---|
| S1 | **Low-PB value**, formed within industry and size buckets | Tier A: `Size and value in China`, *Journal of Financial Economics* (2019), https://doi.org/10.1016/j.jfineco.2019.03.008 | Tushare `daily_basic`: https://tushare.pro/document/2?doc_id=32 | Candidate C2 only. Must use announcement-timed financial quality overlay; PB is not comparable across financials, asset-light growth firms or negative/weak equity. |
| S2 | **Short-horizon reversal**, fixed loser basket | Tier A: `Profitability of reversal strategies: A modified version of the Carhart model in China`, *Economic Modelling* (2018), https://doi.org/10.1016/j.econmod.2017.09.003 | Tushare daily bars: https://tushare.pro/document/2?doc_id=27 | Separate candidate only; high turnover, price-limit and next-open fill bias are likely fatal. The existing repo's full-cycle replay was weak, so no parameter re-tuning from that outcome. |
| S3 | **Earnings/forecast disclosure drift**, mechanical positive-surprise proxy | Tier A: `Post-earnings announcement abnormal return in the Chinese equity market`, *J. International Financial Markets, Institutions & Money* (2011), https://doi.org/10.1016/j.intfin.2011.04.002 | Forecast: https://tushare.pro/document/2?doc_id=45; earnings express: https://tushare.pro/document/2?doc_id=46 | Candidate C3 only. Date-only `ann_date` is insufficient to prove a pre-open timestamp; entries must follow a conservative next-session convention plus matched random-entry control. |
| S4 | **Medium-horizon momentum**, a competing hypothesis, not a blend | Tier A but aged sample: `Contrarian and momentum strategies in the China stock market: 1993–2000`, *Pacific-Basin Finance Journal* (2002), https://doi.org/10.1016/S0927-538X(02)00046-X | Tushare daily bars | Rejected as an immediate build: our full-cycle repo replay already finds daily momentum weak/negative. Re-open only with a materially different, preregistered data regime—not lookback shopping. |
| S5 | **Profitability / conservative-investment quality overlay** | Tier A: `The five-factor asset pricing model tests for the Chinese stock market`, *Pacific-Basin Finance Journal* (2017), https://doi.org/10.1016/j.pacfin.2017.02.001 | Tushare financial indicator joined with `ann_date` and daily valuation | C2 guardrail, not a standalone return claim. ROE is leverage-sensitive and financials need a separate specification. |

**Citation boundary:** DOI links resolved to the publishers, but full texts were not browser-accessible from this environment. The register uses them as durable bibliographic citations; any exact factor construction must be independently reproduced from accessible text or a declared new specification before implementation.

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
3. **Next build candidate:** a sector-aware, announcement-timed quality-value research screen (C2), excluding/segregating financials. Its percentile rules are intentionally not selected yet.
4. **Second candidate:** disclosure-reaction C3 only after the `ann_date`/availability and matched-control data gate is proven.
5. **Independent checking required:** any C2/C3 implementation must receive a separate data/PIT and statistical review before the outcome report is treated as evidence.
