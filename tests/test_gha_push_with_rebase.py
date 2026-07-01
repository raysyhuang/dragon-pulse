import os
import subprocess
from pathlib import Path


def run(cmd, cwd):
    return subprocess.run(cmd, cwd=cwd, check=True, text=True, capture_output=True)


def test_gha_push_helper_rebases_committed_output_after_rejected_push(tmp_path):
    remote = tmp_path / "remote.git"
    base = tmp_path / "base"
    job = tmp_path / "job"
    other = tmp_path / "other"
    helper = Path(__file__).parent.parent / "scripts" / "gha_push_with_rebase.sh"

    run(["git", "init", "--bare", str(remote)], tmp_path)
    run(["git", "clone", str(remote), str(base)], tmp_path)
    run(["git", "config", "user.name", "Test"], base)
    run(["git", "config", "user.email", "test@example.com"], base)
    (base / "README.md").write_text("base\n")
    run(["git", "add", "README.md"], base)
    run(["git", "commit", "-m", "base"], base)
    run(["git", "branch", "-M", "main"], base)
    run(["git", "push", "origin", "main"], base)

    run(["git", "clone", str(remote), str(job)], tmp_path)
    run(["git", "clone", str(remote), str(other)], tmp_path)
    for repo in [job, other]:
        run(["git", "checkout", "main"], repo)
        run(["git", "config", "user.name", "Test"], repo)
        run(["git", "config", "user.email", "test@example.com"], repo)

    (job / "job_output.txt").write_text("job output\n")
    run(["git", "add", "job_output.txt"], job)
    run(["git", "commit", "-m", "job output"], job)

    (other / "other_output.txt").write_text("other output\n")
    run(["git", "add", "other_output.txt"], other)
    run(["git", "commit", "-m", "other output"], other)
    run(["git", "push", "origin", "HEAD:main"], other)

    env = os.environ.copy()
    env["GIT_EDITOR"] = "true"
    result = subprocess.run(
        ["bash", str(helper), "main", "3"],
        cwd=job,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    assert "rebasing committed outputs" in result.stdout

    verify = tmp_path / "verify"
    run(["git", "clone", str(remote), str(verify)], tmp_path)
    run(["git", "checkout", "main"], verify)
    assert (verify / "job_output.txt").read_text() == "job output\n"
    assert (verify / "other_output.txt").read_text() == "other output\n"
