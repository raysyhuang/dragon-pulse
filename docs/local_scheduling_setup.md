# Local scheduled jobs (launchd) — required one-time permission

Two launchd agents run this project locally. **Both are currently blocked by macOS
TCC** and need a one-time Full Disk Access grant to work.

| Agent | Script | Schedule | Purpose |
|---|---|---|---|
| `com.dragonpulse.morning-alert` | `scripts/morning_alert_local.sh` | Mon–Fri 09:28 Shanghai | Local fallback for the morning Telegram alert (primary is GitHub Actions `cn-morning.yml`) |
| `com.dragonpulse.paper-lab-refresh` | `scripts/paper_lab_refresh.sh` | Sun 20:00 Shanghai | Forward-refresh the paper lab (ChiNext-timing + IVOL vs benchmarks) |

Both run locally by design: the morning-alert agent exists precisely as a *fallback for
when Actions is unavailable*, and the paper-lab refresh is long-running with its data
cache on this machine.

> **Correction (2026-07-27):** an earlier version of this doc claimed "Tushare rejects
> GitHub/CI runner IPs". That was **wrong** — it came from a mis-diagnosis after the
> repo's `TUSHARE_TOKEN` secret was accidentally deleted. Tushare works fine from GitHub
> runners (verified: northbound probe returned `status: ok`, 20 rows, from Actions).

## The problem

macOS protects `~/Documents`, `~/Desktop`, and `~/Downloads` (TCC). A process spawned
by `launchd` has no GUI session to prompt for consent, so it is denied silently:

```
/bin/bash: .../scripts/morning_alert_local.sh: Operation not permitted
```

Symptom: `launchctl list | grep dragonpulse` shows a non-zero exit status (78 / 126),
and the `.err` log in `outputs/local_logs/` fills with "Operation not permitted".

**The morning-alert agent has been failing this way since ~2026-05-22** (last stderr
write). The GitHub Actions path still runs, so alerts were not lost — but the local
fallback has been dead.

## The fix (one-time, GUI — cannot be scripted)

1. Open **System Settings → Privacy & Security → Full Disk Access**
2. Click **+**, press **⌘⇧G**, enter `/bin/bash`, and add it
3. Ensure its toggle is **on**
4. Reload both agents:

```bash
launchctl unload ~/Library/LaunchAgents/com.dragonpulse.morning-alert.plist
launchctl load   ~/Library/LaunchAgents/com.dragonpulse.morning-alert.plist
launchctl unload ~/Library/LaunchAgents/com.dragonpulse.paper-lab-refresh.plist
launchctl load   ~/Library/LaunchAgents/com.dragonpulse.paper-lab-refresh.plist
```

## Verify

```bash
launchctl start com.dragonpulse.paper-lab-refresh
sleep 30
launchctl list | grep dragonpulse          # want exit status 0
tail outputs/local_logs/paper_lab_refresh.log
```

Exit status `0` and a leaderboard in the log = working. `126` / `78` with
"Operation not permitted" in the `.err` file = the grant did not take effect.

## Alternative if you would rather not grant Full Disk Access to /bin/bash

Run the refresh manually whenever you want the record updated — it is not
time-critical (the factor sleeve only rebalances quarterly):

```bash
./scripts/paper_lab_refresh.sh
```

Then commit the refreshed `outputs/paper_lab/*.csv` to keep the live record in git.
