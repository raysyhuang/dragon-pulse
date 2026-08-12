"""Shared layout helpers for Dragon Pulse Telegram alerts.

These messages are read on a phone minutes before the open, so the layout obeys
one rule: the tradable numbers come first, diagnostics collapse to the bottom.

Every builder emits Telegram HTML (``<b>``, ``<i>``, ``<code>``,
``<blockquote expandable>``) and the first line of every message is the plain
form of the alert title, so ``AlertManager`` does not prepend a duplicate header.
"""

from __future__ import annotations

import re

# Narrow enough that it never wraps on a phone.
RULE = "─" * 18

_MINUS = "−"  # real minus sign — reads better than a hyphen next to digits


def money(value: object, *, dash: str = "—") -> str:
    """Format a CNY price, tolerating None/garbage from upstream artifacts."""
    try:
        return f"¥{float(value):,.2f}"
    except (TypeError, ValueError):
        return dash


def signed_pct(value: object, *, dash: str = "") -> str:
    """Format a percentage with an explicit sign: ``+9.6%`` / ``−5.0%``."""
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return dash
    sign = "+" if pct >= 0 else _MINUS
    return f"{sign}{abs(pct):.1f}%"


def move_pct(entry: object, level: object) -> str:
    """Percent move from entry to a stop/target level."""
    try:
        base = float(entry)
        target = float(level)
    except (TypeError, ValueError):
        return ""
    if base <= 0:
        return ""
    return signed_pct((target - base) / base * 100)


def title_line(emoji: str, title: str) -> str:
    """Bold header whose plain text equals the alert title (avoids duplication)."""
    return f"<b>{emoji} {title}</b>" if emoji else f"<b>{title}</b>"


def meta_line(*parts: str) -> str:
    """Join short context fragments with a middot, dropping empties."""
    return " · ".join(p for p in parts if p)


def pick_block(
    *,
    index: int,
    display: str,
    entry: object,
    stop: object = None,
    target: object = None,
    max_entry: object = None,
    holding: object = None,
    score: object = None,
    icon: str = "",
    tag: str = "",
    extras: list[str] | None = None,
    notes: list[str] | None = None,
    sub: str = "",
) -> list[str]:
    """One pick rendered as a compact, scannable four-line block.

    <b>1. 药明康德</b> <code>603259.SH</code>
    买入 ¥161.34 · 追高上限 ¥164.57
    止损 ¥153.20 (−5.0%) · 目标 ¥176.89 (+9.6%)
    持仓 5天 · 评分 116 · 北向排名 #1
    """
    name, _, code = display.rpartition(" ")
    prefix = f"{icon} " if icon else ""
    label = f"{index}. {name}" if name else f"{index}."
    suffix = f" [{tag}]" if tag else ""
    lines = [f"{prefix}<b>{label}</b> <code>{code}</code>{suffix}"]

    entry_bits = [f"买入 <b>{money(entry)}</b>"]
    if max_entry is not None:
        capped = money(max_entry, dash="")
        if capped:
            entry_bits.append(f"追高上限 {capped}")
    lines.append(meta_line(*entry_bits))

    risk_bits = []
    if stop is not None:
        pct = move_pct(entry, stop)
        risk_bits.append(f"止损 {money(stop)}" + (f" ({pct})" if pct else ""))
    if target is not None:
        pct = move_pct(entry, target)
        risk_bits.append(f"目标 {money(target)}" + (f" ({pct})" if pct else ""))
    if risk_bits:
        lines.append(meta_line(*risk_bits))

    stat_bits = []
    if holding not in (None, ""):
        stat_bits.append(f"持仓 {holding}天")
    if score is not None:
        try:
            stat_bits.append(f"评分 {float(score):.0f}")
        except (TypeError, ValueError):
            pass
    stat_bits.extend(extras or [])
    if stat_bits:
        lines.append(meta_line(*stat_bits))

    for note in notes or []:
        lines.append(f"→ {note}")
    if sub:
        lines.append(f"<i>{sub}</i>")
    return lines


RECONCILIATION_CHECK_NAME = "SOURCE_RECONCILIATION_HEALTHCHECK_NON_BINDING"


def reconciliation_is_valid(recon: object, expected_date: str) -> bool:
    """Does this artifact actually attest to today's check, in full?

    Reading counts out of whatever JSON happens to sit at the expected path is
    not verification — it trusts the filename. A stale artifact from a previous
    run, an unrelated tool's output, or a hand-edited file would all render
    green while attesting to nothing. Since this line reaches a human who then
    places IBKR orders, only an artifact matching the contract exactly may show
    a pass; everything else degrades to unverified.
    """
    if not isinstance(recon, dict):
        return False
    if recon.get("check") != RECONCILIATION_CHECK_NAME:
        return False
    if recon.get("binding") is not False:          # identity, not truthiness
        return False
    if recon.get("asof") != expected_date:         # a prior run must not vouch for today
        return False

    agreed, total = recon.get("agreed"), recon.get("total")
    for value in (agreed, total):
        # bool is an int subclass; True would otherwise sail through as 1.
        if isinstance(value, bool) or not isinstance(value, int):
            return False
    if total <= 0 or not (0 <= agreed <= total):
        return False

    rows = recon.get("rows")
    if not isinstance(rows, list) or len(rows) != total:
        return False
    if sum(1 for r in rows if isinstance(r, dict) and r.get("agree") is True) != agreed:
        return False
    return True


def reconciliation_line(recon: dict | None, expected_date: str) -> str:
    """Top-of-message cross-source status for the morning alert.

    Sits above the fold rather than in the collapsed diagnostics block: a health
    check nobody reads is the failure mode this whole layer exists to remove.
    A pass is worded to claim exactly what was measured — four anchors, EOD,
    unadjusted — because "数据正常" would be a broader promise than the check makes.

    ``expected_date`` is required rather than optional: an artifact that cannot
    be tied to the session being reported on is not evidence about it.
    """
    if not reconciliation_is_valid(recon, expected_date):
        return "数据交叉核验：⚠️ <b>无法验证</b>（无有效核验记录 · 主扫描仍为 TuShare）"
    agreed, total = int(recon["agreed"]), int(recon["total"])
    if agreed == total:
        return f"数据交叉核验：✅ {agreed}/{total} 锚点一致（仅 EOD 未复权收盘）"
    return f"数据交叉核验：⚠️ <b>未通过 {agreed}/{total}</b>（主扫描仍为 TuShare）"


def append_footer(lines: list[str], rows: list[str]) -> None:
    """Close a message with a rule + de-emphasised context, trimming blank tails."""
    while lines and not lines[-1].strip():
        lines.pop()
    lines.append(RULE)
    lines.extend(r for r in rows if r)


def expandable(header: str, rows: list[str]) -> str:
    """Collapsed diagnostics block — one tap to expand, invisible until then."""
    body = "\n".join(r for r in rows if r)
    if not body:
        return f"<i>{header}</i>"
    return f"<blockquote expandable><b>{header}</b>\n{body}</blockquote>"


_STATUS_CN = (
    ("discontinued_after", "已停止披露"),
    ("parser_error", "解析失败"),
    ("missing tushare_token", "缺少 Tushare 令牌"),
    ("no rows", "无数据"),
    ("timeout", "请求超时"),
    ("unavailable", "不可用"),
)


def humanize_status(status: object, *, ok_label: str = "正常") -> str:
    """Collapse a machine status (often a raw exception) to a short Chinese phrase.

    Telegram gets the phrase; the untouched original stays in the JSON artifact.
    """
    raw = str(status or "").strip()
    if not raw or raw.lower() in {"n/a", "none"}:
        return "无"
    if raw.lower() == "ok":
        return ok_label
    low = raw.lower()
    for needle, label in _STATUS_CN:
        if needle in low:
            return label
    head = raw.split(":", 1)[0].strip()
    return head[:28] if head else "异常"


_GAP_CN = (
    (re.compile(r"older than asof"), "榜单日期滞后于今日"),
    (re.compile(r"permanently unavailable"), "增持数据已于2024-08-16停止披露（不可恢复）"),
    (re.compile(r"net_amount"), "无净买入数据，仅成交额排名"),
    (re.compile(r"holding-delta"), "增持替代源不可用"),
    (re.compile(r"unavailable/degraded"), "榜单数据源异常"),
)


def humanize_gap(gap: object) -> str:
    """Turn one English ``data_gaps`` entry into a short Chinese bullet."""
    raw = str(gap or "").strip()
    if not raw:
        return ""
    for pattern, label in _GAP_CN:
        if pattern.search(raw):
            return label
    return raw[:60]
