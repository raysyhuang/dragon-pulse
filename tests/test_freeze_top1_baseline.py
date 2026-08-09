"""Contract tests for the fail-closed legacy top-1 baseline inventory."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "freeze_top1_baseline.py"
_MODULE = None


def _load_module():
    global _MODULE
    if _MODULE is None:
        spec = importlib.util.spec_from_file_location("freeze_top1_baseline", SCRIPT)
        assert spec is not None and spec.loader is not None
        _MODULE = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_MODULE)
    return _MODULE


def _output(directory: Path) -> Path:
    return directory / "baseline_inventory_2026-08-09.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _row(date: str = "2026-05-04", *, regime: str | None = "bull", evaluated: bool = True,
         ticker: object = "600498.SH", leg: dict | None = None) -> dict:
    if leg is None:
        leg = {"rank": 0, "ticker": ticker, "filled": True, "reason": "target"}
    return {
        "scan_date": date,
        "regime": regime,
        "n_live_picks": 1,
        "top1": {"ticker": ticker},
        "top2": [{"ticker": ticker}],
        "evaluated": evaluated,
        "results": ({"legs": [leg], "top1_ret_pct": 1.0, "top2_ret_pct": 1.0, "csi300_ret_pct": None,
                     "entry_date": "2026-05-05", "exit_date": "2026-05-05"} if evaluated else None),
    }


def _write_ledger(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _freeze(tmp_path: Path, ledger: Path, roots: list[Path], output: Path, **kwargs):
    return _load_module().freeze_baseline(
        ledger_path=ledger,
        artifact_roots=roots,
        output_path=output,
        as_of="2026-08-09",
        source_tiers={"top1_paper_watchlist": "NATIVE_TOP1_PAPER_WATCHLIST"},
        **kwargs,
    )


def test_inventory_records_native_artifact_and_saved_positive_accounting(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    root = tmp_path / "artifacts"
    artifact = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": "2026-05-04", "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": _row()["regime"], "top1": _row()["top1"], "top2": _row()["top2"]}), encoding="utf-8")
    output = tmp_path / "baseline_inventory_2026-08-09.json"

    inventory = _freeze(tmp_path, ledger, [root], output)

    assert output.exists()
    assert inventory["evidence_grade"] == "LEGACY_NON_PIT_BASELINE_INVENTORY"
    assert inventory["promotion_status"] == "NON_EXECUTION_NON_PROMOTABLE"
    assert "not retroactively established" in inventory["capture_provenance_caveat"]
    row = inventory["rows"][0]
    assert row["scan_date"] == "2026-05-04"
    assert row["identity"] == "2026-05-04"
    assert row["ledger_record_sha256"] == hashlib.sha256(json.dumps(_row(), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    assert row["artifact_path"] == "2026-05-04/top1_paper_watchlist_2026-05-04.json"
    assert row["artifact_sha256"] == _sha256(artifact)
    assert row["source_tier"] == "NATIVE_TOP1_PAPER_WATCHLIST"
    assert row["status"] == "RESOLVED_NATIVE_ARTIFACT"
    assert row["accounting"] == {"selected": 1, "filled": 1, "no_fill": 0, "censored": 0, "unknown": 0}


def test_missing_native_artifact_is_explicit_and_never_reconstructed(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    output = tmp_path / "baseline_inventory_2026-08-09.json"

    inventory = _freeze(tmp_path, ledger, [tmp_path / "empty"], output)

    row = inventory["rows"][0]
    assert row["status"] == "MISSING_NATIVE_ARTIFACT"
    assert row["artifact_path"] is None
    assert row["artifact_sha256"] is None
    assert row["reason"] == "no deterministic native top1_paper_watchlist artifact found"
    assert inventory["summary"]["artifact_status_counts"] == {"MISSING_NATIVE_ARTIFACT": 1}


def test_artifact_hash_change_changes_inventory(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    root = tmp_path / "artifacts"
    artifact = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": "2026-05-04", "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": _row()["regime"], "top1": _row()["top1"], "top2": _row()["top2"], "version": 1}), encoding="utf-8")
    first = _freeze(tmp_path, ledger, [root], _output(tmp_path / "one"))
    artifact.write_text(json.dumps({"date": "2026-05-04", "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": _row()["regime"], "top1": _row()["top1"], "top2": _row()["top2"], "version": 2}), encoding="utf-8")
    second = _freeze(tmp_path, ledger, [root], _output(tmp_path / "two"))

    assert first["rows"][0]["artifact_sha256"] != second["rows"][0]["artifact_sha256"]
    assert first["inputs"]["artifact_evidence"][0]["sha256"] != second["inputs"]["artifact_evidence"][0]["sha256"]


@pytest.mark.parametrize("field, value", [
    ("top1", {"ticker": "000001.SZ"}),
    ("top2", [{"ticker": "000001.SZ"}]),
    ("paper_only", False),
    ("status", "NOT_PAPER"),
])
def test_artifact_immutable_identity_mismatch_fails_before_output(tmp_path, field, value):
    ledger = tmp_path / "ledger.jsonl"
    row = _row()
    _write_ledger(ledger, [row])
    root = tmp_path / "artifacts"
    artifact = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    artifact.parent.mkdir(parents=True)
    payload = {"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
               "status": "PAPER_TRACK_ONLY", "regime": row["regime"], "top1": row["top1"], "top2": row["top2"]}
    payload[field] = value
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    output = _output(tmp_path)

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [root], output)

    assert not output.exists()


def test_artifact_regime_mismatch_fails_before_output(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    row = _row(regime="bull")
    _write_ledger(ledger, [row])
    root = tmp_path / "artifacts"
    artifact = root / row["scan_date"] / f"top1_paper_watchlist_{row['scan_date']}.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": "bear", "top1": row["top1"], "top2": row["top2"]}), encoding="utf-8")
    output = _output(tmp_path)

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [root], output)

    assert not output.exists()


@pytest.mark.parametrize("regime", [None, "UNKNOWN"])
def test_artifact_regime_exactly_matches_tracker_value(tmp_path, regime):
    ledger = tmp_path / "ledger.jsonl"
    row = _row(regime=regime)
    _write_ledger(ledger, [row])
    root = tmp_path / "artifacts"
    artifact = root / row["scan_date"] / f"top1_paper_watchlist_{row['scan_date']}.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": regime, "top1": row["top1"], "top2": row["top2"]}), encoding="utf-8")

    inventory = _freeze(tmp_path, ledger, [root], _output(tmp_path))

    assert inventory["rows"][0]["status"] == "RESOLVED_NATIVE_ARTIFACT"
    assert inventory["rows"][0]["regime"] == ("UNKNOWN" if regime is None else regime)


def test_evaluated_top1_leg_foreign_ticker_fails_before_output(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row(leg={"rank": 0, "ticker": "EVIL.SH", "filled": True, "reason": "target"})])
    output = _output(tmp_path)

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [], output)

    assert not output.exists()


@pytest.mark.parametrize("ticker", [None, "", 7])
def test_evaluated_top1_requires_nonempty_string_ledger_ticker(tmp_path, ticker):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row(ticker=ticker)])
    output = _output(tmp_path)

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [], output)

    assert not output.exists()


def test_artifact_non_identity_extra_is_allowed(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    row = _row()
    _write_ledger(ledger, [row])
    root = tmp_path / "artifacts"
    artifact = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": row["regime"], "top1": row["top1"], "top2": row["top2"],
                                    "note": "mutable descriptive metadata"}), encoding="utf-8")

    inventory = _freeze(tmp_path, ledger, [root], _output(tmp_path))

    assert inventory["rows"][0]["status"] == "RESOLVED_NATIVE_ARTIFACT"


@pytest.mark.parametrize("content", [
    "{not-json}\n",
    json.dumps({"scan_date": "2026-05-04", "top1": {}}) + "\n",
    json.dumps(_row()) + "\n" + json.dumps(_row()) + "\n",
])
def test_malformed_or_duplicate_ledger_fails_before_output(tmp_path, content):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(content, encoding="utf-8")
    output = tmp_path / "baseline_inventory_2026-08-09.json"

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [], output)

    assert not output.exists()


@pytest.mark.parametrize("mutate", [
    lambda row: row.update(n_live_picks=1, top1=None),
    lambda row: row.update(evaluated=False, results={"legs": []}),
    lambda row: row.update(evaluated=True, results=None),
    lambda row: row.pop("regime"),
])
def test_contradictory_or_incomplete_legacy_schema_fails_before_output(tmp_path, mutate):
    ledger = tmp_path / "ledger.jsonl"
    row = _row()
    mutate(row)
    _write_ledger(ledger, [row])
    output = _output(tmp_path)

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [], output)

    assert not output.exists()


@pytest.mark.parametrize("location", ["ledger", "artifact_root", "output_parent"])
def test_symlinked_declared_path_ancestor_is_rejected_before_output(tmp_path, location):
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    safe = tmp_path / "safe"
    safe.mkdir()
    ledger = (linked if location == "ledger" else safe) / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    root = (linked if location == "artifact_root" else safe) / "artifacts"
    root.mkdir()
    output = (linked if location == "output_parent" else safe) / "out" / "baseline_inventory_2026-08-09.json"

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [root], output)

    assert not output.exists()


def test_artifact_traversal_and_symlink_are_rejected_before_output(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    malicious = _row()
    malicious["native_artifact_path"] = "../outside.json"
    _write_ledger(ledger, [malicious])
    root = tmp_path / "artifacts"
    root.mkdir()
    (tmp_path / "outside.json").write_text("outside", encoding="utf-8")
    output = tmp_path / "baseline_inventory_2026-08-09.json"

    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [root], output)
    assert not output.exists()

    safe = _row()
    safe["native_artifact_path"] = "linked.json"
    _write_ledger(ledger, [safe])
    (root / "linked.json").symlink_to(tmp_path / "outside.json")
    with pytest.raises(_load_module().BaselineInventoryError):
        _freeze(tmp_path, ledger, [root], output)
    assert not output.exists()


def test_byte_stable_repeat_is_non_overwriting_and_does_not_mutate_ledger(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    ledger_before = ledger.read_bytes()
    root = tmp_path / "artifacts"
    artifact = root / "2026-05-04" / "top1_paper_watchlist_2026-05-04.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text(json.dumps({"date": "2026-05-04", "sleeve": "top1_paper", "paper_only": True,
                                    "status": "PAPER_TRACK_ONLY", "regime": _row()["regime"], "top1": _row()["top1"], "top2": _row()["top2"]}), encoding="utf-8")
    first_path, second_path = _output(tmp_path / "one"), _output(tmp_path / "two")

    _freeze(tmp_path, ledger, [root], first_path)
    _freeze(tmp_path, ledger, [root], second_path)

    assert first_path.read_bytes() == second_path.read_bytes()
    assert ledger.read_bytes() == ledger_before
    with pytest.raises(FileExistsError):
        _freeze(tmp_path, ledger, [root], first_path)


def test_all_status_and_regime_accounting_preserves_unknowns(tmp_path):
    rows = [
        _row("2026-05-04", regime="bull", ticker="a", leg={"rank": 0, "ticker": "a", "filled": True, "reason": "target"}),
        _row("2026-05-05", regime="choppy", ticker="b", leg={"rank": 0, "ticker": "b", "filled": False, "reason": "no_fill_chase"}),
        _row("2026-05-06", regime=None, ticker="c", leg={"rank": 0, "ticker": "c", "filled": None, "reason": "censored_missing_entry"}),
        _row("2026-05-07", regime="bear", evaluated=False),
    ]
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, rows)
    inventory = _freeze(tmp_path, ledger, [], _output(tmp_path / "accounting"))

    assert inventory["summary"]["accounting"] == {"selected": 4, "filled": 1, "no_fill": 1, "censored": 1, "unknown": 1}
    assert inventory["summary"]["regime_counts"] == {"UNKNOWN": 1, "bear": 1, "bull": 1, "choppy": 1}
    assert inventory["rows"][2]["accounting"] == {"selected": 1, "filled": 0, "no_fill": 0, "censored": 1, "unknown": 0}
    assert inventory["rows"][3]["accounting"] == {"selected": 1, "filled": 0, "no_fill": 0, "censored": 0, "unknown": 1}


def test_empty_ledger_emits_deterministic_header_and_summary(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text("", encoding="utf-8")
    output = tmp_path / "baseline_inventory_2026-08-09.json"

    inventory = _freeze(tmp_path, ledger, [], output)

    assert inventory["schema_version"] == 1
    assert inventory["as_of"] == "2026-08-09"
    assert inventory["rows"] == []
    assert inventory["summary"] == {
        "ledger_rows": 0,
        "artifact_status_counts": {},
        "accounting": {"selected": 0, "filled": 0, "no_fill": 0, "censored": 0, "unknown": 0},
        "regime_counts": {},
    }


def test_ledger_symlink_swap_after_lexical_validation_remains_anchored(tmp_path, monkeypatch):
    module = _load_module()
    safe = tmp_path / "safe"
    safe.mkdir()
    ledger = safe / "ledger.jsonl"
    legitimate = json.dumps(_row(), sort_keys=True).encode() + b"\n"
    ledger.write_bytes(legitimate)
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    sentinel = attacker / "ledger.jsonl"
    sentinel.write_bytes(b"ATTACKER SENTINEL MUST NEVER BE READ\n")
    output = _output(tmp_path / "out")

    def swap_ledger() -> None:
        safe.rename(tmp_path / "safe-original")
        safe.symlink_to(attacker, target_is_directory=True)

    monkeypatch.setattr(module, "_after_anchored_open", lambda kind: swap_ledger() if kind == "ledger" else None)

    inventory = _freeze(tmp_path, ledger, [], output)

    assert inventory["inputs"]["ledger"]["sha256"] == hashlib.sha256(legitimate).hexdigest()
    assert output.exists()


def test_artifact_symlink_swap_after_open_remains_anchored(tmp_path, monkeypatch):
    module = _load_module()
    ledger = tmp_path / "ledger.jsonl"
    row = _row()
    _write_ledger(ledger, [row])
    root = tmp_path / "artifacts"
    artifact = root / row["scan_date"] / f"top1_paper_watchlist_{row['scan_date']}.json"
    artifact.parent.mkdir(parents=True)
    legitimate = json.dumps({"date": row["scan_date"], "sleeve": "top1_paper", "paper_only": True,
                             "status": "PAPER_TRACK_ONLY", "regime": row["regime"], "top1": row["top1"], "top2": row["top2"]}).encode()
    artifact.write_bytes(legitimate)
    attacker = tmp_path / "attacker"
    attacker.mkdir()
    attacker_artifact = attacker / row["scan_date"] / artifact.name
    attacker_artifact.parent.mkdir()
    attacker_artifact.write_bytes(b"ATTACKER SENTINEL MUST NEVER BE HASHED OR PARSED")

    swapped = []

    def swap_artifact_root(kind: str) -> None:
        if kind == "artifact":
            root.rename(tmp_path / "artifacts-original")
            root.symlink_to(attacker, target_is_directory=True)
            swapped.append(True)

    monkeypatch.setattr(module, "_after_anchored_open", swap_artifact_root)

    inventory = _freeze(tmp_path, ledger, [root], _output(tmp_path / "out"))

    assert swapped == [True]
    assert inventory["rows"][0]["artifact_sha256"] == hashlib.sha256(legitimate).hexdigest()
    assert inventory["rows"][0]["status"] == "RESOLVED_NATIVE_ARTIFACT"


def test_output_parent_symlink_swap_after_open_never_publishes_to_attacker(tmp_path, monkeypatch):
    module = _load_module()
    ledger = tmp_path / "ledger.jsonl"
    _write_ledger(ledger, [_row()])
    output_parent = tmp_path / "safe-output"
    output_parent.mkdir()
    output = _output(output_parent)
    attacker = tmp_path / "attacker-output"
    attacker.mkdir()
    swapped = []

    def swap_output_parent(kind: str) -> None:
        if kind == "output_parent":
            output_parent.rename(tmp_path / "safe-output-original")
            output_parent.symlink_to(attacker, target_is_directory=True)
            swapped.append(True)

    monkeypatch.setattr(module, "_after_anchored_open", swap_output_parent)

    _freeze(tmp_path, ledger, [], output)

    assert swapped == [True]
    assert not (attacker / output.name).exists()
    assert (tmp_path / "safe-output-original" / output.name).exists()


def test_open_regular_closes_acquired_fd_when_fstat_fails(monkeypatch, tmp_path):
    module = _load_module()
    closed: list[int] = []
    monkeypatch.setattr(module.os, "open", lambda *args, **kwargs: 73)
    monkeypatch.setattr(module.os, "fstat", lambda fd: (_ for _ in ()).throw(OSError("fstat failed")))
    monkeypatch.setattr(module.os, "close", closed.append)

    with pytest.raises(module.BaselineInventoryError, match="cannot open regular"):
        module._open_regular(11, "ledger.jsonl", tmp_path / "ledger.jsonl")

    assert closed == [73]


def test_anchored_file_close_attempts_parent_when_file_close_fails(monkeypatch, tmp_path):
    module = _load_module()
    closed: list[int] = []

    def close(fd: int) -> None:
        closed.append(fd)
        if fd == 22:
            raise OSError("file close failed")

    monkeypatch.setattr(module.os, "close", close)

    with pytest.raises(OSError, match="file close failed"):
        module._AnchoredFile(parent_fd=11, fd=22, display=tmp_path / "ledger.jsonl").close()

    assert closed == [22, 11]


def test_anchored_file_close_attempts_file_when_parent_close_fails(monkeypatch, tmp_path):
    module = _load_module()
    closed: list[int] = []

    def close(fd: int) -> None:
        closed.append(fd)
        if fd == 11:
            raise OSError("parent close failed")

    monkeypatch.setattr(module.os, "close", close)

    with pytest.raises(OSError, match="parent close failed"):
        module._AnchoredFile(parent_fd=11, fd=22, display=tmp_path / "ledger.jsonl").close()

    assert closed == [22, 11]


def test_atomic_write_unlinks_temporary_when_close_fails(monkeypatch, tmp_path):
    module = _load_module()
    parent_fd = module.os.open(tmp_path, module._DIRECTORY_FLAGS)
    parent = module._AnchoredDirectory(fd=parent_fd, display=tmp_path)
    output = _output(tmp_path)
    temporary_name = f".{output.name}.fixed.tmp"
    temporary_fds: set[int] = set()
    unlinked: list[str] = []
    real_open, real_close, real_unlink = module.os.open, module.os.close, module.os.unlink

    def open_temporary(name, *args, **kwargs):
        fd = real_open(name, *args, **kwargs)
        if name == temporary_name:
            temporary_fds.add(fd)
        return fd

    def close_temporary(fd: int) -> None:
        real_close(fd)
        if fd in temporary_fds:
            raise OSError("temporary close failed")

    def unlink_temporary(name, *args, **kwargs) -> None:
        unlinked.append(name)
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(module.secrets, "token_hex", lambda _: "fixed")
    monkeypatch.setattr(module.os, "open", open_temporary)
    monkeypatch.setattr(module.os, "close", close_temporary)
    monkeypatch.setattr(module.os, "unlink", unlink_temporary)
    try:
        with pytest.raises(OSError, match="temporary close failed"):
            module._write_new_atomically(output, parent, b"inventory")
    finally:
        real_close(parent_fd)

    assert unlinked == [temporary_name]
    assert not (tmp_path / temporary_name).exists()


def test_atomic_write_preserves_body_error_when_cleanup_close_fails(monkeypatch, tmp_path):
    module = _load_module()
    parent_fd = module.os.open(tmp_path, module._DIRECTORY_FLAGS)
    parent = module._AnchoredDirectory(fd=parent_fd, display=tmp_path)
    output = _output(tmp_path)
    temporary_name = f".{output.name}.fixed.tmp"
    temporary_fds: set[int] = set()
    unlinked: list[str] = []
    real_open, real_close, real_unlink = module.os.open, module.os.close, module.os.unlink

    def open_temporary(name, *args, **kwargs):
        fd = real_open(name, *args, **kwargs)
        if name == temporary_name:
            temporary_fds.add(fd)
        return fd

    def close_temporary(fd: int) -> None:
        real_close(fd)
        if fd in temporary_fds:
            raise OSError("temporary close failed")

    def unlink_temporary(name, *args, **kwargs) -> None:
        unlinked.append(name)
        real_unlink(name, *args, **kwargs)

    monkeypatch.setattr(module.secrets, "token_hex", lambda _: "fixed")
    monkeypatch.setattr(module.os, "open", open_temporary)
    monkeypatch.setattr(module.os, "close", close_temporary)
    monkeypatch.setattr(module.os, "unlink", unlink_temporary)
    monkeypatch.setattr(module.os, "write", lambda *args: (_ for _ in ()).throw(RuntimeError("body failed")))
    try:
        with pytest.raises(RuntimeError, match="body failed"):
            module._write_new_atomically(output, parent, b"inventory")
    finally:
        real_close(parent_fd)

    assert unlinked == [temporary_name]
    assert not (tmp_path / temporary_name).exists()
