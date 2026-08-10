"""Durability contract for selection-study analysis publication."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "pit_selection_test.py"


def _load_study():
    spec = importlib.util.spec_from_file_location("pit_selection_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_publish_analysis_refuses_overwrite_and_cleans_temp(tmp_path: Path) -> None:
    study = _load_study()
    output = tmp_path / "out"
    output.mkdir()
    artifact = output / "analysis.json"
    artifact.write_bytes(b"prior evidence")

    with pytest.raises(FileExistsError):
        study.publish_analysis(output, b"new evidence")

    assert artifact.read_bytes() == b"prior evidence"
    assert list(output.glob(".analysis.*")) == []


def test_publish_analysis_writes_exact_bytes_and_cleans_temp(tmp_path: Path) -> None:
    study = _load_study()
    payload = b'{"strict":true}\n'

    artifact = study.publish_analysis(tmp_path / "out", payload)

    assert artifact == tmp_path / "out" / "analysis.json"
    assert artifact.read_bytes() == payload
    assert list((tmp_path / "out").glob(".analysis.*")) == []


def test_publish_analysis_post_link_sync_error_preserves_artifact_and_cleans_temp(tmp_path: Path, monkeypatch) -> None:
    study = _load_study()
    output = tmp_path / "out"
    artifact = output / "analysis.json"
    real_fsync = study.os.fsync
    calls = 0

    def fail_directory_sync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        real_fsync(fd)

    monkeypatch.setattr(study.os, "fsync", fail_directory_sync)
    with pytest.raises(study.AnalysisDurabilityUncertainError) as raised:
        study.publish_analysis(output, b"linked bytes")

    assert raised.value.artifact == artifact
    assert str(artifact) in str(raised.value)
    assert "durability is uncertain" in str(raised.value)
    assert "reconcile manually" in str(raised.value)
    assert isinstance(raised.value.__cause__, OSError)
    assert artifact.read_bytes() == b"linked bytes"
    assert list(output.glob(".analysis.*")) == []


def test_publish_analysis_never_unlinks_canonical_artifact_after_post_link_failure(tmp_path: Path, monkeypatch) -> None:
    study = _load_study()
    output = tmp_path / "out"
    artifact = output / "analysis.json"
    real_fsync = study.os.fsync
    real_unlink = Path.unlink
    calls = 0
    artifact_unlinks = 0

    def fail_directory_sync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        real_fsync(fd)

    def observe_unlink(path: Path, *args: object, **kwargs: object) -> None:
        nonlocal artifact_unlinks
        if path == artifact:
            artifact_unlinks += 1
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(study.os, "fsync", fail_directory_sync)
    monkeypatch.setattr(Path, "unlink", observe_unlink)
    with pytest.raises(study.AnalysisDurabilityUncertainError):
        study.publish_analysis(output, b"preserved linked bytes")

    assert artifact_unlinks == 0
    assert artifact.read_bytes() == b"preserved linked bytes"
    assert list(output.glob(".analysis.*")) == []


def test_publish_analysis_link_failure_leaves_no_destination_or_temp(tmp_path: Path, monkeypatch) -> None:
    study = _load_study()

    def fail_link(source: str, destination: str, *args: object, **kwargs: object) -> None:
        raise OSError("injected link failure")

    monkeypatch.setattr(study.os, "link", fail_link)
    output = tmp_path / "out"
    with pytest.raises(OSError, match="injected link failure"):
        study.publish_analysis(output, b"never linked")

    assert not (output / "analysis.json").exists()
    assert list(output.glob(".analysis.*")) == []
