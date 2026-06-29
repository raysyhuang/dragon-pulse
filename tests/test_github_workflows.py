import yaml
from pathlib import Path


def test_cn_morning_workflow_has_same_day_preopen_scan_and_open_check():
    workflow_path = Path(__file__).parent.parent / ".github" / "workflows" / "cn-morning.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    schedules = [item["cron"] for item in workflow[True]["schedule"]]
    assert "15 23 * * 0-4" in schedules
    assert "35 1 * * 1-5" in schedules

    jobs = workflow["jobs"]
    assert jobs["preopen-scan"]["if"] == "github.event.schedule == '15 23 * * 0-4' || inputs.mode == 'preopen_scan'"
    assert jobs["preflight"]["if"] == "github.event.schedule == '35 1 * * 1-5' || inputs.mode == 'open_check'"
    assert jobs["northbound-paper-preopen"]["if"] == "always() && (github.event.schedule == '15 23 * * 0-4' || inputs.mode == 'northbound_paper_preopen')"
    assert jobs["northbound-paper-open-check"]["if"] == "always() && (github.event.schedule == '35 1 * * 1-5' || inputs.mode == 'northbound_paper_open_check')"
    assert jobs["northbound-source-probe"]["if"] == "github.event.schedule == '0 4 * * 1-5' || github.event.schedule == '30 7 * * 1-5' || github.event.schedule == '0 9 * * 1-5'"

    preopen_run = "\n".join(
        step.get("run", "") for step in jobs["preopen-scan"]["steps"]
    )
    assert 'python main.py scan --config config/default.yaml --date "${{ steps.date.outputs.trade_date }}"' in preopen_run
    assert "DRAGON_PULSE_SKIP_TELEGRAM" not in preopen_run

    preflight_run = "\n".join(
        step.get("run", "") for step in jobs["preflight"]["steps"]
    )
    assert "outputs/${TRADE_DATE}/execution_watchlist_${TRADE_DATE}.json" in preflight_run
    assert "Missing same-day watchlist" in preflight_run
    assert "sort -t_ -k3 -r | head -1" not in preflight_run

    nb_preopen_run = "\n".join(
        step.get("run", "") for step in jobs["northbound-paper-preopen"]["steps"]
    )
    assert "python scripts/northbound_paper_sleeve.py --mode preopen" in nb_preopen_run
    assert "northbound_paper_watchlist_${TRADE_DATE}.json" in nb_preopen_run

    nb_open_run = "\n".join(
        step.get("run", "") for step in jobs["northbound-paper-open-check"]["steps"]
    )
    assert "python scripts/northbound_paper_sleeve.py --mode open_check" in nb_open_run
    assert "northbound_paper_execution_check_${TRADE_DATE}.json" in nb_open_run

    nb_probe_run = "\n".join(
        step.get("run", "") for step in jobs["northbound-source-probe"]["steps"]
    )
    assert "python scripts/northbound_paper_sleeve.py --mode source_probe" in nb_probe_run
    assert "northbound_source_probe_${TRADE_DATE}_${PROBE_SLOT}.json" in nb_probe_run
