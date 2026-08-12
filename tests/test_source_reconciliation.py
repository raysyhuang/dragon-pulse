"""Guards for the Tushare-vs-iFinD health check.

This script exists to distinguish a quiet market from a broken pipe, so its one
unforgivable bug is reporting agreement it did not actually verify. Every test
here is about a way that could happen: a code the provider silently swapped, a
forward-filled weekend close, an undated response, an unparseable cell.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def recon():
    spec = importlib.util.spec_from_file_location(
        "source_reconciliation", PROJECT_ROOT / "scripts" / "source_reconciliation.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["source_reconciliation"] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeClient:
    """Stands in for IFindClient; returns a canned payload."""

    def __init__(self, payload):
        self.payload = payload

    def call(self, server, tool, arguments):
        return self.payload


def _payload(code="000300.SH", close="4663.7887", date="20260811", name="沪深300"):
    return {
        "answer": (
            "|证券代码|证券简称|收盘价（单位：元）|\n"
            "|---|---|---|\n"
            f"|{code}|{name}|{close}|"
        ),
        "indicators_params": {"收盘价": {"交易日期": date, "复权方式": "不复权"}},
    }


# --- number parsing -------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("4663.7887", 4663.7887),
    ("1.6832万亿", 1.6832e12),
    ("36.4005亿", 36.4005e8),
    ("1,346.50", 1346.50),
    ("¥84.82", 84.82),
])
def test_parse_cn_number_handles_prose_numerals(recon, raw, expected):
    assert recon.parse_cn_number(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", ["", "—", "\t", "abc", "12哈"])
def test_parse_cn_number_refuses_garbage(recon, raw):
    with pytest.raises(recon.ReconError) as e:
        recon.parse_cn_number(raw)
    assert e.value.kind == "PARSE_FAILURE"


def test_markdown_table_drops_the_separator_row(recon):
    rows = recon.parse_markdown_table(_payload()["answer"])

    assert len(rows) == 1
    assert rows[0]["证券代码"] == "000300.SH"


def test_markdown_table_refuses_a_non_table_answer(recon):
    with pytest.raises(recon.ReconError) as e:
        recon.parse_markdown_table("抱歉，未找到相关数据")
    assert e.value.kind == "PARSE_FAILURE"


# --- tolerance ------------------------------------------------------------

def test_rounding_scale_differences_agree(recon):
    assert recon.within_tolerance(4663.7887, 4663.79)   # index, 2dp vs 4dp
    assert recon.within_tolerance(11.26, 11.2600)


def test_real_divergence_does_not_agree(recon):
    assert not recon.within_tolerance(4663.79, 4680.00)
    assert not recon.within_tolerance(11.26, 11.90)


def test_absolute_floor_covers_cheap_instruments(recon):
    # 2dp rounding on a ~¥3 stock exceeds the relative tolerance on its own.
    assert recon.within_tolerance(3.00, 3.005)


# --- code validation ------------------------------------------------------

def test_declared_alias_is_accepted_and_recorded(recon):
    inst = recon.INSTRUMENTS[0]
    assert inst.code == "000300.SH" and "399300.SZ" in inst.accepted_codes

    close, returned = recon.ifind_close(
        FakeClient(_payload(code="399300.SZ")), inst, "20260811"
    )

    assert close == pytest.approx(4663.7887)
    assert returned == "399300.SZ"  # surfaced, not silently normalised


def test_undeclared_code_fails_even_when_the_price_matches(recon):
    inst = recon.INSTRUMENTS[0]

    with pytest.raises(recon.ReconError) as e:
        recon.ifind_close(FakeClient(_payload(code="000905.SH")), inst, "20260811")

    assert e.value.kind == "CODE_MISMATCH"


def test_equity_has_no_aliases(recon):
    equity = next(i for i in recon.INSTRUMENTS if i.kind == "equity")
    assert equity.accepted_codes == {equity.code}


# --- date validation ------------------------------------------------------

def test_forward_filled_non_trading_day_is_rejected(recon):
    """iFinD repeats the prior close on weekends; that must never count as agreement."""
    inst = recon.INSTRUMENTS[0]

    with pytest.raises(recon.ReconError) as e:
        recon.ifind_close(FakeClient(_payload(date="20260807")), inst, "20260811")

    assert e.value.kind == "IFIND_STALE"


def test_undated_response_is_rejected(recon):
    inst = recon.INSTRUMENTS[0]
    payload = _payload()
    payload["indicators_params"] = {}

    with pytest.raises(recon.ReconError) as e:
        recon.ifind_close(FakeClient(payload), inst, "20260811")

    assert e.value.kind == "IFIND_UNDATED"


# --- fail-closed ----------------------------------------------------------

def test_a_row_that_failed_never_reports_agreement(recon):
    row = recon.Row(code="000300.SH", name="沪深300", kind="index")
    row.failure = "IFIND_UNAVAILABLE"

    assert row.agree is False


def test_every_failure_kind_is_typed(recon):
    err = recon.ReconError("IFIND_STALE", "dated 20260807, asked 20260811")

    assert err.kind == "IFIND_STALE"
    assert "20260807" in err.detail


def test_ifind_date_stamp_is_an_echo_not_an_attestation(recon):
    """Documents the measured behaviour so the false guarantee is never re-added.

    Live check on 2026-08-08 (a Saturday) returned Friday's 4694.4365 stamped
    20260808 — iFinD reflects the requested date back. The stamp therefore
    cannot detect a fabricated bar; only Tushare-first ordering can.
    """
    inst = recon.INSTRUMENTS[0]
    fabricated = _payload(close="4694.4365", date="20260808")  # Friday's close, Saturday's stamp

    close, _ = recon.ifind_close(FakeClient(fabricated), inst, "20260808")

    assert close == pytest.approx(4694.4365)  # the stamp check does NOT save us here


def test_tushare_is_queried_before_ifind_so_fabrication_cannot_agree(recon, monkeypatch):
    """The actual defence: a non-trading day dies on Tushare before iFinD is reached."""
    calls = []

    def fake_tushare_close(token, inst, day):
        calls.append(("tushare", inst.code, day))
        raise recon.ReconError("TUSHARE_NO_DATA", f"{inst.code} has no bar on {day}")

    def fake_ifind_close(client, inst, day):
        calls.append(("ifind", inst.code, day))
        return 4694.4365, inst.code

    monkeypatch.setattr(recon, "tushare_close", fake_tushare_close)
    monkeypatch.setattr(recon, "ifind_close", fake_ifind_close)
    monkeypatch.setattr(recon, "resolve_session", lambda token, d: "20260808")
    monkeypatch.setattr(recon, "IFindClient", lambda *a, **k: object())

    rows, session = recon.reconcile("2026-08-08", "t", "i")

    assert all(not r.agree for r in rows)
    assert all(r.failure == "TUSHARE_NO_DATA" for r in rows)
    assert not any(c[0] == "ifind" for c in calls), "iFinD must not be reached once Tushare has no bar"


# --- alert surface --------------------------------------------------------

def _fmt():
    from src.core import message_format
    return message_format


def test_missing_artifact_reads_as_unverified_never_healthy():
    line = _fmt().reconciliation_line(None)

    assert "无法验证" in line
    assert "正常" not in line          # must never imply the data was checked
    assert "TuShare" in line           # states what the scan still ran on


def test_pass_line_claims_only_what_was_measured():
    line = _fmt().reconciliation_line({"agreed": 4, "total": 4})

    assert "✅" in line and "4/4" in line
    assert "EOD" in line and "未复权" in line   # scope, not a vendor certification
    assert "数据正常" not in line


def test_partial_agreement_is_a_visible_failure():
    line = _fmt().reconciliation_line({"agreed": 2, "total": 4})

    assert "未通过 2/4" in line
    assert "<b>" in line               # bold: above the fold, not buried
    assert "✅" not in line


def test_zero_total_is_not_reported_as_a_pass():
    line = _fmt().reconciliation_line({"agreed": 0, "total": 0})

    assert "✅" not in line


def test_read_reconciliation_returns_none_when_absent_or_corrupt(tmp_path, monkeypatch):
    from src.core.io import read_reconciliation
    monkeypatch.chdir(tmp_path)

    assert read_reconciliation("2026-08-12") is None

    day = tmp_path / "outputs" / "2026-08-12"
    day.mkdir(parents=True)
    (day / "source_reconciliation_2026-08-12.json").write_text("{broken", encoding="utf-8")

    assert read_reconciliation("2026-08-12") is None


def test_read_reconciliation_loads_a_valid_artifact(tmp_path, monkeypatch):
    import json as _json
    from src.core.io import read_reconciliation
    monkeypatch.chdir(tmp_path)
    day = tmp_path / "outputs" / "2026-08-12"
    day.mkdir(parents=True)
    (day / "source_reconciliation_2026-08-12.json").write_text(
        _json.dumps({"check": "SOURCE_RECONCILIATION_HEALTHCHECK_NON_BINDING",
                     "binding": False, "agreed": 4, "total": 4}), encoding="utf-8")

    data = read_reconciliation("2026-08-12")

    assert data["agreed"] == 4 and data["binding"] is False


def test_artifact_carries_the_non_binding_label(recon):
    assert recon.CHECK_NAME == "SOURCE_RECONCILIATION_HEALTHCHECK_NON_BINDING"


# --- rendered message regression -----------------------------------------

def test_rendered_morning_message_places_status_above_the_fold():
    """Golden render: the status line sits between the header and the rule.

    Position is the whole point. If this drifts below RULE it lands in the
    de-emphasised tail, and a health check nobody reads is the failure this
    layer was built to remove. Pinned as a full string so wording and placement
    both regress together.
    """
    fmt = _fmt()
    lines = [
        fmt.title_line("", "🐉 龙脉扫描 — 2026-08-12 开盘检查"),
        fmt.meta_line("🔴 <b>熊市</b>", "选股 <b>0</b>"),
        fmt.reconciliation_line({"agreed": 4, "total": 4}),
        fmt.RULE,
        "今日无选股 — 无信号通过筛选",
    ]

    assert "\n".join(lines) == (
        "<b>🐉 龙脉扫描 — 2026-08-12 开盘检查</b>\n"
        "🔴 <b>熊市</b> · 选股 <b>0</b>\n"
        "数据交叉核验：✅ 4/4 锚点一致（仅 EOD 未复权收盘）\n"
        "──────────────────\n"
        "今日无选股 — 无信号通过筛选"
    )


def test_all_three_morning_layouts_carry_the_status_line():
    """Guards against wiring only the layout that happened to be under test."""
    source = (PROJECT_ROOT / "scripts" / "morning_check.py").read_text(encoding="utf-8")

    assert source.count("reconciliation_line(read_reconciliation(date_str))") == 3
    # and it precedes the rule in every one of them
    for block in source.split("reconciliation_line(read_reconciliation(date_str)),")[1:]:
        assert block.lstrip().startswith("RULE,"), "status line must sit directly above RULE"
