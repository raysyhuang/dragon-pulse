"""Regression coverage for artifact-owned selection-study evidence blocks."""
from __future__ import annotations

import importlib.util
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "render_selection_doc.py"
ANALYSIS = ROOT / "outputs" / "pit_selection_v2" / "analysis.json"
DOC = ROOT / "docs" / "research" / "2026-08-10-pit-selection-test-v2-corrected.md"


def _load_renderer():
    spec = importlib.util.spec_from_file_location("render_selection_doc", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _marker_body(text: str, name: str) -> str:
    begin = f"<!-- BEGIN GENERATED: {name} -->"
    end = f"<!-- END GENERATED: {name} -->"
    return text.split(begin, 1)[1].split(end, 1)[0]


def test_all_evidence_blocks_are_artifact_owned_and_match_analysis() -> None:
    renderer = _load_renderer()
    analysis = json.loads(ANALYSIS.read_text(encoding="utf-8"))
    text = DOC.read_text(encoding="utf-8")

    expected = renderer.blocks(analysis)
    assert set(expected) == {
        "results", "turnover_attribution", "sensitivity", "verdict",
        "evidence_summary", "scope", "addendum_statistics", "provenance",
    }
    for name, body in expected.items():
        assert _marker_body(text, name).strip() == body


def test_v1_comparative_bias_claim_is_not_in_human_written_prose() -> None:
    text = DOC.read_text(encoding="utf-8")
    human_written = re.sub(
        r"<!-- BEGIN GENERATED: [^>]+ -->.*?<!-- END GENERATED: [^>]+ -->",
        "",
        text,
        flags=re.DOTALL,
    )

    assert "apparent effect is *weaker* than in v1, consistent with v1" not in human_written
    assert "carrying mild optimistic bias" not in human_written


def test_check_cli_rejects_one_digit_drift_in_every_evidence_block(tmp_path: Path) -> None:
    sandbox = tmp_path / "study"
    (sandbox / "scripts").mkdir(parents=True)
    (sandbox / "outputs" / "pit_selection_v2").mkdir(parents=True)
    (sandbox / "docs" / "research").mkdir(parents=True)
    shutil.copy2(SCRIPT, sandbox / "scripts" / SCRIPT.name)
    shutil.copy2(ANALYSIS, sandbox / "outputs" / "pit_selection_v2" / "analysis.json")
    pristine = DOC.read_text(encoding="utf-8")
    copied_doc = sandbox / "docs" / "research" / DOC.name

    for name in _load_renderer().blocks(json.loads(ANALYSIS.read_text(encoding="utf-8"))):
        body = _marker_body(pristine, name)
        digit = next(char for char in body if char.isdigit())
        copied_doc.write_text(pristine.replace(body, body.replace(digit, str((int(digit) + 1) % 10), 1), 1), encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(sandbox / "scripts" / SCRIPT.name), "--check"],
            capture_output=True, text=True, check=False,
        )
        assert result.returncode == 1, name
        assert "DRIFT" in result.stderr
