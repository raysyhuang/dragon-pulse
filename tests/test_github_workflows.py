import yaml
from pathlib import Path


REPO_ROOT = Path(__file__).parent.parent


def test_cn_morning_workflow_has_same_day_preopen_scan_and_open_check():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "cn-morning.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    schedules = [item["cron"] for item in workflow[True]["schedule"]]
    assert "15 23 * * 0-4" in schedules
    assert "35 1 * * 1-5" in schedules

    jobs = workflow["jobs"]
    assert jobs["preopen-scan"]["if"] == "github.event.schedule == '15 23 * * 0-4' || inputs.mode == 'preopen_scan'"
    assert jobs["preflight"]["if"] == "github.event.schedule == '35 1 * * 1-5' || github.event.schedule == '55 1 * * 1-5' || inputs.mode == 'open_check'"

    preopen_run = "\n".join(
        step.get("run", "") for step in jobs["preopen-scan"]["steps"]
    )
    assert 'python main.py scan --config config/default.yaml --date "${{ steps.date.outputs.trade_date }}"' in preopen_run
    assert "DRAGON_PULSE_SKIP_TELEGRAM" not in preopen_run
    assert "scripts/gha_push_with_rebase.sh main 3" in preopen_run

    preflight_run = "\n".join(
        step.get("run", "") for step in jobs["preflight"]["steps"]
    )
    assert "outputs/${TRADE_DATE}/execution_watchlist_${TRADE_DATE}.json" in preflight_run
    assert "Missing same-day watchlist" in preflight_run
    assert "sort -t_ -k3 -r | head -1" not in preflight_run


def test_cn_morning_workflow_has_no_retired_northbound_sleeve():
    """The northbound paper sleeve was retired 2026-08-11 (prereg Amendment 3).

    It must not reappear as a scheduled job, a dispatch mode, or a stray cron —
    retirement that only lives in a doc gets undone by the next workflow edit.
    """
    workflow_path = REPO_ROOT / ".github" / "workflows" / "cn-morning.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    assert not [job for job in workflow["jobs"] if "northbound" in job]
    assert "northbound" not in workflow_path.read_text(encoding="utf-8").lower()

    modes = workflow[True]["workflow_dispatch"]["inputs"]["mode"]["options"]
    assert modes == ["preopen_scan", "open_check"]

    # The three probe crons existed only to feed the sleeve.
    schedules = [item["cron"] for item in workflow[True]["schedule"]]
    for retired_cron in ("0 4 * * 1-5", "30 7 * * 1-5", "0 9 * * 1-5"):
        assert retired_cron not in schedules


def test_cn_nightly_workflow_uses_same_push_helper():
    workflow_path = REPO_ROOT / ".github" / "workflows" / "cn-nightly.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    job = workflow["jobs"]["analyze"]
    run_blocks = "\n".join(step.get("run", "") for step in job["steps"])

    assert "python main.py scan --config config/default.yaml" in run_blocks
    assert "auto: CN nightly outputs" in run_blocks
    assert "scripts/gha_push_with_rebase.sh main 3" in run_blocks
    assert "pushed=false" not in run_blocks
    assert "git stash push --include-untracked" not in run_blocks
