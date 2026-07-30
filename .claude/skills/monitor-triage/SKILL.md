---
name: monitor-triage
description: Use when checking the health of the deployed SDMx monitor, triaging what it has detected, or when a scheduled routine fires. Reads the monitor's API, decides whether anything changed since the last look, investigates what did, records findings in docs/monitor/incidents.md, and stays silent when nothing changed.
---

# Monitor Triage

Read the deployed SDMx monitor, work out whether anything actually changed, and
investigate what did.

Runs either as a scheduled claude.ai routine (this repository cloned, results
committed and pushed) or manually in an interactive session.

## The one rule that governs everything here

**Never turn "I could not tell" into "nothing is wrong."**

This project has repeatedly had to defend that distinction. If the monitor is
unreachable, you have learned nothing about any provider, and the report must
say so rather than showing twelve quiet rows. Every report entry carries an
explicit "could not determine" line, even when it says "nothing".

## Constants

- Monitor: `https://sdmx-monitor-production.up.railway.app`
- Report: `docs/monitor/incidents.md`
- State: `docs/monitor/triage-state.json`
- Output branch: `claude/monitor-log`

Paths are relative to this repository's root.

## Network access

A cloud run reaches only the hosts its environment allows. The monitor and every
SDMx provider sit outside the default `Trusted` allowlist, so the environment
must use `Custom` network access including these hosts:

```
sdmx-monitor-production.up.railway.app
stats-sdmx-disseminate.pacificdata.org
data-sdmx-disseminate.statsfiji.gov.fj
data-sdmx-disseminate.sbs.gov.ws
data-api.ecb.europa.eu
sdmx.data.unicef.org
api.imf.org
sdmx.oecd.org
ec.europa.eu
sdmx.ilo.org
data.api.abs.gov.au
stats.bis.org
api.data.stats.govt.nz
```

A `403` with `x-deny-reason: host_not_allowed` means the environment is missing a
host, which is a configuration fault rather than a provider failure. Record it as
such and do not mark any endpoint broken because of it.

## Never call `/api/refresh`

This routine is strictly read-only against the monitor. A forced refresh would
clear a wedged or stale cycle and destroy the evidence of the one failure mode
that has already caused a real outage (a cycle wedged for 10.6 hours, silently
suppressing every later run). Staleness gets reported, never repaired. Staying
read-only also avoids the endpoint's 429 cooldown and 409 in-progress responses.

## Step 0: Get onto the log branch

All output goes to the branch `claude/monitor-log`, never to `main`. A cloud
session may only push to its current working branch, and `claude/`-prefixed
branches are the ones always accepted, so this is a hard constraint rather than
a preference.

```bash
git fetch origin claude/monitor-log 2>/dev/null \
  && git checkout claude/monitor-log \
  && git pull --ff-only \
  || git checkout -b claude/monitor-log
```

Each scheduled run is a fresh session, so the state file **on this branch** is
the only memory of what the previous run saw. Read it after checking out, not
before: `main` carries only the original baseline and will look like a first run.

## Step 1: Read

```bash
M=https://sdmx-monitor-production.up.railway.app
curl -s --max-time 30 -o /dev/null -w '%{http_code}\n' "$M/healthz"
curl -s --max-time 60 "$M/api/status"
curl -s --max-time 60 "$M/api/contracts"
```

If `/healthz` does not answer, **stop investigating providers**. That is the
finding: the monitor is down, so we are blind. Record it and state plainly that
no provider conclusions are possible.

Read `docs/monitor/triage-state.json` for `last_cycle_id`. If the file is
missing, this is a first run: see "First run" below.

## Step 2: Severe conditions

Each of these is reportable on its own, regardless of how endpoints look:

| Condition | Meaning |
| --- | --- |
| `/healthz` unreachable | the monitor itself is down; we are blind |
| `stale: true` | no cycle has completed within the expected interval |
| `gateway_up: false` | the gateway is unreachable from the monitor |

`stale` is the watchdog-of-the-watchdog. Treat it as high priority: it means
monitoring has stopped, which is worse than any single provider failing.

## Step 3: Detect change

```bash
curl -s --max-time 90 "$M/api/history?hours=48"
```

`series` maps each endpoint key to a per-cycle list of `{cycle_id, started_at,
status, failing}`.

Compare **per-endpoint status at `last_cycle_id` against the newest cycle**, and
also scan the cycles in between.

A changed cycle id means nothing by itself: the monitor produces a new cycle
every two hours in normal operation. Only a changed *status* is a change.

Three kinds of change matter:

1. An endpoint moved from `healthy` to anything else.
2. An endpoint returned to `healthy`.
3. An endpoint failed and recovered *between* runs. Report it once, marked as
   already resolved. Do not discard it: flapping is often what precedes a hard
   failure, and at a 6-hour cadence over 2-hour cycles there are always
   intermediate cycles where this can hide.

Contract changes come from the `changes` array of `/api/contracts`, which the
monitor computes server-side as was/now pairs. Also flag any assertion whose
`verdict` is `broken` or `capability_appeared`.

Contract verdict vocabulary:

| Verdict | Meaning |
| --- | --- |
| `ok` | behaviour matches what the gateway assumes |
| `broken` | behaviour no longer matches; the gateway's assumption is now wrong |
| `capability_appeared` | the provider gained something we could now use |
| `ignored` | provider returned 200 but the payload matched the `references=none` baseline byte-for-byte, so it accepted the parameter and silently dropped it |
| `skipped` | the check could not run this cycle |

`ignored` is easy to misread as a false alarm, because `expected` and `observed`
both show `200`. It is a real signal: the provider claims support it does not
deliver.

**If no severe condition holds, no endpoint changed status, and no contract
changed:** update the state file, commit and push only that file with the
message `monitor triage: no change (cycle N)`, write no report entry, and stop.
Silence is the correct and expected outcome of most runs.

## Step 4: Investigate what changed

For each endpoint whose status changed:

### 4a. Confirm it is still true, live

A cycle can be two hours old. Re-run the failing check yourself against the
provider now. Get the failing path and kind from the `failing` array in the
history series, and the endpoint's base URL from `config.py` (`SDMX_ENDPOINTS`).

Record both what the cycle saw and what your live recheck saw, **especially when
they disagree**. Disagreement is informative: it means transient.

### 4b. Adopt the monitor's classification

Do not recompute this. `monitor/derive.py` already decided, and its vocabulary
carries the distinction that matters:

| Status | Means | Whose problem |
| --- | --- | --- |
| `provider_down` | metadata failed on both the gateway and direct paths | theirs; nothing to fix in our code |
| `gateway_issue` | direct path succeeded, gateway path failed | **ours** |
| `degraded` | some checks failing, mixed | investigate which |
| `unknown` | the gateway was unreachable | nothing can be concluded about providers |

`gateway_issue` is the one that means we have a bug. Say so clearly and name the
likely code site: the failing check's `path` and `kind` identify whether it is
the metadata, data, or json channel.

### 4c. New or chronic?

From the history series, state whether this is new as of a specific cycle or has
been failing for days, and whether it is flapping.

### 4d. Do not cry wolf

Two different things get confused here, so keep them apart.

**Architectural facts.** These are configured and expected. They do not make an
endpoint red, and they are not incidents:

- **ILO**: `/availableconstraint/` returns 500; constraints come via `?references=all`
- **ESTAT**: no practical constraint support
- **BIS, ILO, IMF**: `references:contentconstraint` reads `ignored`, meaning the
  parameter is accepted and silently dropped

**Endpoints that have flapped, verified against history on 2026-07-30** (all
twelve were healthy at cycle 60). Re-derive this list from `/api/history` rather
than trusting it, since it goes stale:

- **ESTAT**: `list_dataflows` times out at 60s under load, then recovers. Seen at
  cycles 54 and 55. This is our own call deadline firing, working as designed.
- **FBOS**: cycle 50 showed `Temporary failure in name resolution`, which is DNS
  failing on the monitor's side, recorded as `provider_down`. Treat a
  name-resolution error as infrastructure, not as the provider being down.
- **SPC**: HTTP 500 on the direct path at cycle 26, recovered.
- **ILO**: HTTP 403 on direct data and json at cycle 32, recovered.
- **UNICEF**: HTTP 503 at cycle 2, recovered.
- **ABS**: `gateway metadata: Error:` at cycle 26, with an empty error body. If
  this recurs, the empty message is itself the bug to report.

**ECB is no longer a known failure.** It failed with `HTTP 406 from provider`
through cycle 43 and has been healthy since, after the gateway learned to retry
`text/csv` when an SDMx-CSV `Accept` draws a 406. Do not carry the old
"ECB is broadly not working" assumption; if ECB fails again it is news.

### 4e. Contract changes

For each entry in `changes`, report the was/now pair and name which gateway
assumption it invalidates. `monitor/contracts_config.py` mirrors `config.py`
deliberately so this mapping is available. A `capability_appeared` verdict is
not a failure: it means the provider gained something we could now use.

### 4f. Rule out the network first

Before attributing a live recheck failure to a provider, check whether *other*
providers answer. If none do, the network where this routine runs is suspect,
and the entry must say that rather than filing twelve simultaneous provider
outages.

## Step 5: Write the report

Prepend to `docs/monitor/incidents.md`, newest first. Create the file with an
`# Monitor Incidents` heading if absent.

```markdown
## 2026-07-30T14:03Z - cycle 412

**Changed:** SPC direct data healthy -> degraded

**Cycle saw:** direct data: 0 observations returned (HTTP 200)
**Live recheck:** still failing, same shape
**Classification:** provider-side (`degraded`, direct path failing, gateway path also failing)
**History:** new as of cycle 411; healthy for the preceding 23 cycles
**Recommended action:** watch one more cycle before filing; SPC has not flapped here before
**Could not determine:** whether the empty response is a provider deploy or a dataflow retirement
```

Keep entries factual. No em-dashes (repo writing rule). Use `->` for
transitions.

## Step 6: Signal through the commit message

A scheduled cloud run has no terminal and no desktop, so the commit message is
the notification channel. Make it carry the finding.

When anything is notify-worthy, **prefix the commit subject with `ALERT:`** and
state the finding in the subject itself, under 72 characters where possible:

```
ALERT: monitor stale 7h, no cycle since 03:12 (cycle 412)
ALERT: SPC healthy -> gateway_issue, direct path OK (cycle 412)
ALERT: contract ECB dialect:sdmx3 404 -> 200 (cycle 412)
```

Notify-worthy means any of:

- the monitor is unreachable
- `stale` is true
- `gateway_up` is false
- an endpoint changed status in either direction
- a contract assertion became `broken` or `capability_appeared`

Otherwise the subject is `monitor triage: no change (cycle N)` with no `ALERT:`
prefix. A routine that shouts every six hours about a chronically red endpoint
gets ignored within a week, and a genuine new failure gets ignored with it.

If running in an interactive session, also send one `PushNotification` with the
same text. It is silently skipped when there is no terminal, so it costs nothing
to attempt.

## Step 7: Update state and push

Write `docs/monitor/triage-state.json`:

```json
{
  "last_cycle_id": 412,
  "last_cycle_started_at": "2026-07-30T14:00:00Z",
  "last_run": "2026-07-30T14:03Z",
  "alerted": true,
  "endpoint_status_at_last_run": {"SPC": "gateway_issue", "ECB": "healthy"},
  "open_items": []
}
```

Record every endpoint's status, since that is what the next run compares
against. Use real UTC from `date -u`, never an estimated timestamp.

Then commit and push to the log branch:

```bash
git add docs/monitor/
git commit -m "<subject from step 6>"
git push -u origin claude/monitor-log
```

Never push to `main`. If a push is rejected, report the rejection in the next
run's entry rather than retrying with force.

Update state on every run, including silent ones and including runs where the
monitor was unreachable. When it was unreachable, leave `last_cycle_id`
unchanged so the next run still compares against the last cycle actually seen,
and record why in `open_items`.

Never commit anything outside `docs/monitor/`. If the working tree has other
changes, leave them alone.

## First run

When the state file is missing, do not alert about every pre-existing chronic
failure. Record the current state as the baseline, write one report entry titled
`baseline` listing each endpoint's current status and noting which failures are
already known, and commit without the `ALERT:` prefix. Subsequent runs compare
against that baseline.

## When invoked manually

Same routine, with one difference: report what you found to the user directly in
the conversation even when nothing changed, because they asked. Still write no
report entry when nothing changed, and do not commit a state update from an
interactive session unless the user asks, so the scheduled routine's history
stays clean.
