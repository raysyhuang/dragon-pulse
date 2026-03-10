"""
LLM Integration for China A-Shares

Functions for calling LLM APIs and ranking candidates.
Adapted for China A-share market characteristics:
- No options market (removed from scoring)
- Different sentiment sources (东方财富/雪球 instead of StockTwits/Reddit)
- Policy-driven catalysts (国务院/证监会 announcements)
- 10% daily limit up/down consideration
- Dragon Tiger List (龙虎榜) for institutional activity
"""

from __future__ import annotations
import os
import json
from typing import Optional
from pathlib import Path


def build_weekly_scanner_prompt_cn(packets: list[dict]) -> str:
    """
    Build the LLM prompt for Weekly Momentum Scanner ranking - China A-Shares version.
    
    Uses a 3-factor model adapted for CN market:
    - Technical Momentum (40%)
    - Catalyst/News (40%)  
    - Market Activity/龙虎榜 (20%)
    
    Args:
        packets: List of candidate packets
    
    Returns:
        Complete prompt string
    """
    # Detect if any packet has market activity data
    has_market_activity = any(
        p.get("market_activity_available", False) for p in packets
    )

    if has_market_activity:
        weight_tech = 40
        weight_catalyst = 40
        weight_market = 20
        weight_formula = "0.40*technical + 0.40*catalyst + 0.20*market_activity"
    else:
        weight_tech = 50
        weight_catalyst = 50
        weight_market = 0
        weight_formula = "0.50*technical + 0.50*catalyst (market activity unavailable — set all market_activity scores to 0)"

    # Build prompt header with dynamic weights, then append static body
    prompt = (
        "# **Weekly Momentum Scanner (China A-Shares) — Top 5 Candidates for >=10% Move in Next 7 Trading Days**\n\n"
        "## **Mission**\n\n"
        "Identify **Top 5 China A-share stocks** (SSE/SZSE; any market cap) that have the "
        "**highest probability of a >=10% price move upward within the next 7 trading days**, "
        "using a **3-factor model**:\n\n"
        f"1. Technical Momentum ({weight_tech}%)\n"
        f"2. Catalyst/News ({weight_catalyst}%)\n"
        f"3. Market Activity/Institutional Flow ({weight_market}%)\n\n"
        f"**Weighting:** composite_score = {weight_formula}\n\n"
        "**Note:** China A-shares have a 10% daily limit up/down, so a 10% target is achievable "
        "but requires sustained momentum.\n\n"
    )

    prompt += """---
## **0) Execution Rules (anti-hallucination + reproducibility)**

1. **Data freshness targets** (best-effort):
   * Price/volume/technicals: EOD data
   * News: must be **source-cited** with timestamp
   * 龙虎榜 (Dragon Tiger List): if available, from recent trading days
2. **No guessing**: if any key metric is unavailable, set it to null, explain in data_gaps, and **cap** the relevant factor score.
3. **Cross-check** catalysts/news when possible. Policy announcements (国务院/证监会/发改委) are high-weight catalysts.
4. **Output must be valid JSON only** (no markdown, no commentary).
5. Include run_timestamp_utc and asof_* timestamps per data type.

---
## **1) Universe & Liquidity Gate**

Universe: **All A-shares listed on SSE (上交所) and SZSE (深交所)**.
**Hard liquidity filters (must pass):**
* avg_dollar_volume_20d >= 50,000,000 CNY
* price >= 5.00 CNY (avoid low-priced speculative names)
**Exclusions (must exclude unless user overrides):**
* ST/ST* stocks (special treatment - high delisting risk)
* Price up **> 20%** in the last **5** trading days (avoid already-exploded names near limit-up exhaustion)
* Stocks suspended or with trading halts

---
## **2) Scoring Model (3-Factor for China A-Shares)**

### **Composite Score**
"""

    prompt += f"composite_score = {weight_formula}\n"

    prompt += """
### **2A) Technical Momentum Score (0–10)**
**LOCKED - Do NOT modify. Technical scores are provided by Python and are final.**
You may only reference the technical_score and technical_evidence from the packet.

Point rubric (for reference):
* +2.0 if within 5% of 52W high OR daily close breaks above resistance
* +2.0 if 3-day avg volume ≥ 1.5× 20-day avg volume
* +2.0 if RSI(14) in [50, 70] (healthy momentum, not overbought)
* +2.0 if price > MA10, MA20, MA50 (all three)
* +2.0 if 5-day realized vol annualized ≥ 20%

### **2B) Catalyst Score (0–10)**
Catalyst must be **within the next 7 trading days** and **source-cited**.
Point rubric:
* +4.0: High-impact event (earnings release, major policy announcement affecting sector, significant contract/order)
* +3.0: Strong sector rotation signal (政策利好, 板块轮动) or institutional research coverage upgrade
* +2.0: Company-specific news (management changes, shareholder actions, product launches)
* +1.0: General sector tailwind or market sentiment driver
**Penalty rules:**
* −2.0 if catalyst is rumor-only / single-source / unverified (小道消息)
* −2.0 if event timing is unclear (no date)
* −1.0 to −3.0 if catalyst is likely already priced in
* −3.0 if catalyst/news is > 2 trading days old (stale catalyst — the move already happened)
* −1.0 if catalyst is only a general macro/sector trend without company-specific angle
**Red flag penalties (apply BEFORE composite calculation):**
* −2.0 if stock has high pledge ratio (股权质押比例 > 30%) — forced liquidation risk
* −3.0 if CSRC/exchange issued regulatory warning (监管函/问询函) in last 30 days
* −2.0 if company announced major shareholder reduction plan (大股东减持计划)
* −1.0 if recent auditor qualification or financial restatement
**Missing data rule:** if you cannot verify any catalyst with sources → cap catalyst_cap = 3 (lowered from 4).

### **2C) Market Activity Score (0–10)**
This replaces options activity (not available for A-shares) with:
* +3.0 if stock appears on 龙虎榜 (Dragon Tiger List) with net institutional buying
* +2.0 if 北向资金 (Northbound/Stock Connect) shows recent net inflows for this stock
* +2.0 if strong volume expansion (RVOL > 2.5)
* +2.0 if stock is in a hot sector (板块热点) with rotation into the sector
* +1.0 if retail sentiment is positive (东方财富股吧/雪球 trending)
**Missing data rule:** if no market activity data → cap market_activity_cap = 3 (lowered from 4).
**Important:** If BOTH dragon tiger data AND sector rotation data are missing, the stock cannot achieve HIGH confidence regardless of other scores.

---
## **3) Tie-breakers & Risk Adjustments**

If composite scores are close (±0.3), rank higher the name with:
1. clearer **dated catalyst** (政策/业绩/合同)
2. better **liquidity** (higher average turnover)
3. cleaner **technical structure** (breakout level + volume confirmation)
4. recent **龙虎榜** activity with institutional net buying

Add a **Risk Adjustment Note** (not altering score unless extreme):
* ST risk or financial irregularities
* High pledge ratio (股权质押比例高)
* Sector-wide regulatory risk (监管风险)
* Market regime risk (大盘趋势)

---
## **4) Required Output (STRICT JSON ONLY)**

Return a single JSON object with:
* run_timestamp_utc
* universe_note
* method_version
* top5 (array of 5 objects sorted by rank)

Each stock object must match:

```json
{
  "rank": 1,
  "ticker": "600000.SH",
  "name": "浦发银行",
  "name_en": "Shanghai Pudong Development Bank",
  "exchange": "SSE",
  "sector": "金融/Financials",
  "current_price": 12.34,
  "market_cap_cny": 123456789000,
  "avg_dollar_volume_20d": 150000000,
  "asof_price_utc": "2025-12-14T00:00:00Z",

  "target": {
    "horizon_trading_days": 7,
    "upside_threshold_pct": 10,
    "target_price_for_10pct": 13.57,
    "base_case_upside_pct_range": [10, 15],
    "bear_case_note": "What invalidates the setup"
  },

  "primary_catalyst": {
    "title": "政策利好/Earnings/Contract",
    "key_date_local": "2025-12-18",
    "timing": "After close",
    "why_it_matters": "1-2 sentences in Chinese or English",
    "sources": [
      { "title": "Source title", "publisher": "东方财富/新浪财经/公司公告", "url": "https://...", "published_at": "2025-12-10" }
    ]
  },

  "scores": {
    "technical": 8.5,
    "catalyst": 7.0,
    "market_activity": 6.0
  },
  "composite_score": 7.40,

  "evidence": {
    "technical": {
      "within_5pct_52w_high": true,
      "resistance_level": 13.00,
      "volume_ratio_3d_to_20d": 1.8,
      "rsi14": 63.2,
      "above_ma10_ma20_ma50": true,
      "realized_vol_5d_ann_pct": 28.0
    },
    "market_activity": {
      "on_dragon_tiger_list": false,
      "dragon_tiger_net_buy_cny": null,
      "northbound_net_flow_5d_cny": null,
      "sector_rotation_signal": "positive",
      "retail_sentiment": "neutral"
    }
  },

  "risk_factors": [
    "Top risk 1 (e.g., 大盘调整风险)",
    "Top risk 2 (e.g., 板块回调压力)"
  ],
  "confidence": "HIGH",
  "data_gaps": []
}
```

### **Confidence labels (STRICT — err on the side of lower confidence)**
* HIGH: all three factor scores ≥7, no red flags, catalyst dated within 2 trading days, dragon tiger OR northbound data present
* MEDIUM: 2 factors ≥7 and the third ≥5, no critical red flags, catalyst is source-verified
* SPECULATIVE: missing market activity data OR only 1 factor ≥7 OR any red flag present
* Note: SPECULATIVE picks will be filtered out by the post-LLM quality gate. Only assign HIGH/MEDIUM if you are genuinely confident.

---
## **5) Final instruction**

Now rank the provided packets and return **only the JSON** response with the **Top 5** ranked results.

**CRITICAL REMINDERS:**
- Technical scores are LOCKED - use them as-is from the packet
- This is for **China A-shares** (SSE/SZSE), NOT US stocks
- Consider the 10% daily limit (涨跌停) when assessing move probability
- Policy catalysts (政策利好) carry high weight in China markets
- If market activity data is missing → cap at 4.0
- Output must be valid JSON only, no markdown wrapper

---
## **Packets to Rank**

"""
    
    # Add packets
    for i, packet in enumerate(packets, 1):
        prompt += f"\n=== PACKET {i} ===\n"
        prompt += json.dumps(packet, indent=2, default=str, ensure_ascii=False)
        prompt += "\n\n"
    
    prompt += "\n\nNow return the Top 5 ranked results as JSON only (no markdown, no commentary).\n"
    
    return prompt


def build_weekly_scanner_prompt(packets: list[dict]) -> str:
    """
    Build the LLM prompt for Weekly Momentum Scanner ranking.
    Auto-detects market type from packet data.
    
    Args:
        packets: List of candidate packets
    
    Returns:
        Complete prompt string
    """
    # Detect if this is China market based on ticker format
    if packets:
        sample_ticker = packets[0].get("ticker", "")
        if ".SH" in sample_ticker or ".SZ" in sample_ticker:
            return build_weekly_scanner_prompt_cn(packets)
    
    # Fall back to US prompt (legacy)
    return _build_weekly_scanner_prompt_us(packets)


def _build_weekly_scanner_prompt_us(packets: list[dict]) -> str:
    """Original US market prompt - kept for reference."""
    prompt = """# **Weekly Momentum Scanner (US) — Top 5 Candidates**

## **Mission**
Identify **Top 5 U.S.-listed stocks** with highest probability of ≥10% move in 7 trading days.

## **Packets to Rank**

"""
    for i, packet in enumerate(packets, 1):
        prompt += f"\n=== PACKET {i} ===\n"
        prompt += json.dumps(packet, indent=2, default=str)
        prompt += "\n\n"
    
    prompt += "\nReturn Top 5 as JSON only.\n"
    return prompt


def call_openai(prompt: str, model: str = "gpt-4o", api_key: Optional[str] = None) -> str:
    """Call OpenAI API."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("openai library not installed")
    
    api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")
    
    client = OpenAI(api_key=api_key)
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a quantitative trading analyst specializing in China A-shares (A股). Return only valid JSON. Never include markdown code blocks or commentary outside the JSON. You can respond in Chinese or English as appropriate."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3,
        response_format={"type": "json_object"} if model.startswith("gpt-4") or "o1" not in model else None,
    )
    
    return response.choices[0].message.content


def call_anthropic(prompt: str, model: str = "claude-sonnet-4-20250514", api_key: Optional[str] = None) -> str:
    """Call Anthropic API."""
    try:
        import anthropic
    except ImportError:
        raise ImportError("anthropic library not installed")
    
    api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")
    
    client = anthropic.Anthropic(api_key=api_key)
    
    response = client.messages.create(
        model=model,
        max_tokens=8000,
        temperature=0.3,
        system="You are a quantitative trading analyst specializing in China A-shares (A股). Return only valid JSON. Never include markdown code blocks or commentary outside the JSON. You can respond in Chinese or English as appropriate.",
        messages=[
            {"role": "user", "content": prompt}
        ],
    )
    
    return response.content[0].text


def extract_json_from_response(text: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = text.strip()
    
    # Remove markdown code blocks
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON parsing failed: {e}\nResponse preview: {text[:500]}")


def validate_llm_ranking(result: dict, input_tickers: list[str]) -> tuple[dict, list[str]]:
    """
    Validate and sanitize LLM ranking output.

    - Ensures top5 is a list with 1-5 entries
    - Verifies tickers exist in input universe
    - Clamps out-of-range scores to [0, 10]
    - Drops entries with missing/unknown tickers

    Args:
        result: Parsed LLM JSON output
        input_tickers: List of tickers that were in the input packets

    Returns:
        (validated_result, warnings) tuple
    """
    warnings = []
    input_set = {t.upper() for t in input_tickers}

    top5 = result.get("top5")
    if not isinstance(top5, list):
        warnings.append(f"top5 is not a list (got {type(top5).__name__}), replacing with empty list")
        top5 = []

    validated_entries = []
    for i, entry in enumerate(top5):
        if not isinstance(entry, dict):
            warnings.append(f"top5[{i}] is not a dict, skipping")
            continue

        ticker = str(entry.get("ticker", "")).upper().strip()
        if not ticker:
            warnings.append(f"top5[{i}] has no ticker, skipping")
            continue

        if ticker not in input_set:
            warnings.append(f"top5[{i}] ticker '{ticker}' not in input universe, skipping")
            continue

        # Clamp scores to [0, 10]
        scores = entry.get("scores", {})
        if isinstance(scores, dict):
            for key in ("technical", "catalyst", "market_activity"):
                val = scores.get(key)
                if val is not None:
                    try:
                        clamped = max(0.0, min(10.0, float(val)))
                        if clamped != float(val):
                            warnings.append(f"top5[{i}] scores.{key} clamped from {val} to {clamped}")
                        scores[key] = clamped
                    except (TypeError, ValueError):
                        warnings.append(f"top5[{i}] scores.{key} is not numeric: {val}")
                        scores[key] = 0.0
            entry["scores"] = scores

        # Clamp composite_score
        cs = entry.get("composite_score")
        if cs is not None:
            try:
                clamped = max(0.0, min(10.0, float(cs)))
                if clamped != float(cs):
                    warnings.append(f"top5[{i}] composite_score clamped from {cs} to {clamped}")
                entry["composite_score"] = clamped
            except (TypeError, ValueError):
                entry["composite_score"] = 0.0

        validated_entries.append(entry)

    # Limit to 5
    if len(validated_entries) > 5:
        warnings.append(f"top5 had {len(validated_entries)} entries, truncating to 5")
        validated_entries = validated_entries[:5]

    result["top5"] = validated_entries
    return result, warnings


def apply_post_llm_gate(
    result: dict,
    min_composite_score: float = 6.5,
    min_confidence: str = "MEDIUM",
) -> tuple[dict, list[str]]:
    """
    Post-LLM quality gate — reject weak picks before saving.

    This is the missing validation layer: the LLM returns top5 but we never
    checked if the composite scores or confidence levels actually meet our bar.

    Args:
        result: Validated LLM result dict with top5
        min_composite_score: Minimum composite score to keep
        min_confidence: Minimum confidence level ("HIGH", "MEDIUM", "SPECULATIVE")

    Returns:
        (filtered_result, gate_rejections) tuple
    """
    CONFIDENCE_RANK = {"HIGH": 3, "MEDIUM": 2, "SPECULATIVE": 1, "FALLBACK": 0}
    min_conf_rank = CONFIDENCE_RANK.get(min_confidence.upper(), 1)

    top5 = result.get("top5", [])
    kept = []
    rejections = []

    for entry in top5:
        ticker = entry.get("ticker", "?")
        composite = float(entry.get("composite_score", 0))
        confidence = str(entry.get("confidence", "SPECULATIVE")).upper()
        conf_rank = CONFIDENCE_RANK.get(confidence, 0)

        reasons = []
        if composite < min_composite_score:
            reasons.append(f"composite {composite:.2f} < {min_composite_score}")
        if conf_rank < min_conf_rank:
            reasons.append(f"confidence {confidence} < {min_confidence}")

        if reasons:
            rejections.append(f"{ticker} rejected: {', '.join(reasons)}")
        else:
            kept.append(entry)

    # Re-rank remaining entries
    for i, entry in enumerate(kept, 1):
        entry["rank"] = i

    result["top5"] = kept
    if rejections:
        result["gate_rejections"] = rejections

    return result, rejections


def _build_fallback_top5(packets: list[dict]) -> list[dict]:
    """Build a minimal fallback top5 from packets ranked by technical_score."""
    sorted_packets = sorted(packets, key=lambda p: float(p.get("technical_score", 0)), reverse=True)
    top5 = []
    for i, p in enumerate(sorted_packets[:5], 1):
        top5.append({
            "rank": i,
            "ticker": p.get("ticker", ""),
            "name": p.get("name", p.get("ticker", "")),
            "composite_score": float(p.get("technical_score", 0)) * 0.4,
            "scores": {
                "technical": float(p.get("technical_score", 0)),
                "catalyst": 0.0,
                "market_activity": 0.0,
            },
            "confidence": "FALLBACK",
            "data_gaps": ["LLM ranking failed validation — ranked by technical score only"],
        })
    return top5


def rank_weekly_candidates(
    packets: list[dict],
    provider: str = "openai",
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    min_composite_score: float = 6.5,
    min_confidence: str = "MEDIUM",
    method_version: str = "v3.2-CN",
) -> dict:
    """
    Rank weekly scanner candidates using LLM.
    
    Args:
        packets: List of candidate packets
        provider: LLM provider ("openai" or "anthropic")
        model: Model name (defaults to provider default)
        api_key: API key (overrides env var)
    
    Returns:
        Dict with top5 ranking results
    """
    import datetime as dt
    
    # Set model defaults
    if not model:
        if provider == "openai":
            model = "gpt-4o"
        else:
            model = "claude-sonnet-4-20250514"
    
    # Build prompt (auto-detects CN vs US)
    prompt = build_weekly_scanner_prompt(packets)
    
    # Call LLM
    if provider == "openai":
        response_text = call_openai(prompt, model, api_key)
    else:
        response_text = call_anthropic(prompt, model, api_key)
    
    # Parse JSON
    result = extract_json_from_response(response_text)

    # Validate LLM output
    input_tickers = [p.get("ticker", "") for p in packets]
    result, validation_warnings = validate_llm_ranking(result, input_tickers)

    if validation_warnings:
        for w in validation_warnings:
            print(f"  [LLM VALIDATION] {w}")

    # Fallback if top5 is empty after validation
    is_fallback = False
    if not result.get("top5"):
        print("  [LLM FALLBACK] No valid top5 after validation, falling back to technical score ranking")
        result["top5"] = _build_fallback_top5(packets)
        is_fallback = True

    # Apply post-LLM quality gate (skip confidence check for fallback — those are
    # already low-confidence by definition, but still useful when LLM fails)
    if is_fallback:
        result, gate_rejections = apply_post_llm_gate(
            result,
            min_composite_score=min_composite_score,
            min_confidence="FALLBACK",  # Accept all confidence levels in fallback mode
        )
    else:
        result, gate_rejections = apply_post_llm_gate(
            result,
            min_composite_score=min_composite_score,
            min_confidence=min_confidence,
        )
    if gate_rejections:
        for r in gate_rejections:
            print(f"  [POST-LLM GATE] {r}")
        print(f"  [POST-LLM GATE] {len(result.get('top5', []))} picks survived gate")

    # Add metadata
    result["run_timestamp_utc"] = dt.datetime.utcnow().isoformat() + "Z"
    result["method_version"] = method_version
    if "universe_note" not in result:
        result["universe_note"] = "China A-shares (SSE + SZSE)"
    if validation_warnings:
        result["validation_warnings"] = validation_warnings

    return result
