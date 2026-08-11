"""Origin tagging: every alert must say which host produced it.

A VPS cron and the CI pipeline send to the same Telegram chat, so when two
messages disagree the tag is what tells them apart.
"""

import pytest

from src.core.alerts import run_source_tag

_ORIGIN_ENV = ("DP_RUN_ORIGIN", "GITHUB_ACTIONS", "GITHUB_RUN_ID")


@pytest.fixture(autouse=True)
def _clear_origin_env(monkeypatch):
    for name in _ORIGIN_ENV:
        monkeypatch.delenv(name, raising=False)


def test_local_run_is_tagged_local():
    assert run_source_tag() == "DP_LOCAL"


def test_github_actions_self_identifies(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert run_source_tag() == "DP_GH"

    monkeypatch.delenv("GITHUB_ACTIONS")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    assert run_source_tag() == "DP_GH"


@pytest.mark.parametrize(
    ("origin", "expected"),
    [("vps", "DP_VPS"), ("VPS", "DP_VPS"), ("mirror", "DP_MIRROR"), ("local", "DP_LOCAL")],
)
def test_declared_origin_wins(monkeypatch, origin, expected):
    monkeypatch.setenv("DP_RUN_ORIGIN", origin)
    assert run_source_tag() == expected


def test_declared_origin_overrides_github_detection(monkeypatch):
    """A mirror that replays a workflow must not claim to be the CI pipeline."""
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("DP_RUN_ORIGIN", "mirror")
    assert run_source_tag() == "DP_MIRROR"


def test_unknown_origin_is_labelled_not_silently_local(monkeypatch):
    monkeypatch.setenv("DP_RUN_ORIGIN", "hk-node 2")
    tag = run_source_tag()
    assert tag == "DP_HK_NODE_2"
    assert tag not in {"DP_LOCAL", "DP_GH"}


def test_blank_origin_falls_back_to_detection(monkeypatch):
    monkeypatch.setenv("DP_RUN_ORIGIN", "   ")
    assert run_source_tag() == "DP_LOCAL"
