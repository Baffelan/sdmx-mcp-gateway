# Monitor Incidents

Written by the `monitor-triage` routine. Newest entry first.
Untracked on purpose: scheduled runs never commit.

## 2026-07-30T12:43Z - cycle 65

**Changed:** ESTAT healthy -> gateway_issue

**Cycle saw:** `gateway metadata: tool call list_dataflows timed out after
60.0s`, first at cycle 63 (2026-07-30T08:25:41Z) and still present at cycles
64 and 65 (three consecutive cycles, six hours). Direct path passing
throughout, per the monitor's own classification.

**Live recheck:** fetched `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all`
directly just now: HTTP 200 in 41.9s, 37.5 MB payload. Slow but under the 60s
deadline this time, so the live probe did not reproduce the timeout, only the
slowness that causes it.

**Classification:** `gateway_issue` by the monitor's own label, but this is
the known recurring failure mode documented for this endpoint: ESTAT's full
dataflow listing is large enough (tens of MB) that it intermittently crosses
our own 60s call deadline under load. It is our deadline firing as designed
against a slow provider response, not a gateway bug and not evidence the
provider is down. Other providers checked fine during the same window (ECB
200 in 1.3s), so this is not a network problem on the monitor's or this
routine's side.

**History:** not new. The same `list_dataflows` timeout was seen at cycles
49-50, 53-55, and now 63-65. Each prior occurrence recovered within a few
cycles. This is the longest run of consecutive gateway_issue cycles observed
for ESTAT so far (3, versus 2-3 previously), otherwise consistent with the
known pattern.

**Recommended action:** no code change indicated by this occurrence alone. If
the streak extends past 3-4 more cycles, or ESTAT stops recovering, that
would be new information worth a closer look (possible provider-side
regression in response size or latency, not just our deadline).

**Could not determine:** whether ESTAT's dataflow listing has grown larger or
slower recently, which would explain why this streak is longer than prior
ones; would need historical latency numbers this routine does not have.

### STATSNZ open item, unchanged

`auth:listing` still reads `capability_appeared` at cycle 65 (expected
"credentials required", observed 200), same as recorded at cycle 60 and
carried through cycle 62. No new information this run; not re-reported as a
fresh finding, carried forward in the state file.

## 2026-07-30T04:10Z - cycle 60 - baseline

First run, so this records the starting point rather than reporting a change.
No notification sent.

**Monitor:** `/healthz` 200, `stale: false`, `gateway_up: true`, latency 188 ms,
cycle 60 started 02:25:41Z and finished 02:26:36Z. No dataflow drift.

**All twelve endpoints healthy:** ABS, BIS, ECB, ESTAT, FBOS, ILO, IMF, OECD,
SBS, SPC, STATSNZ, UNICEF.

**Contracts:** 149 `ok`, 3 `ignored`, 1 `capability_appeared`, no `changes`
against the previous cycle.

### Open item carried from the baseline

**STATSNZ `auth:listing` reads `capability_appeared`.** Expected "credentials
required", observed 200. The contract probe deliberately withholds credentials,
so a 200 means the dataflow listing is now served without authentication. Either
Stats NZ opened the endpoint or their APIM gateway changed. Worth confirming
before acting: the gateway still holds an API key for STATSNZ, and if auth is
genuinely no longer required that key is redundant rather than wrong.

### Flap history over the preceding 7 days (59 cycles)

| Endpoint | Event | Cycle | Resolved |
| --- | --- | --- | --- |
| ECB | `HTTP 406 from provider` on gateway data | through 43 | yes, fix deployed |
| ESTAT | `list_dataflows` timed out after 60s | 54, 55 | yes |
| FBOS | `Temporary failure in name resolution` | 50 | yes |
| ILO | HTTP 403 on direct data and json | 32 | yes |
| SPC | HTTP 500 on direct data and json | 26 | yes |
| ABS | `gateway metadata: Error:` (empty message) | 26 | yes |
| UNICEF | HTTP 503 | 2 | yes |

**Could not determine:** whether FBOS cycle 50 was the provider or the monitor's
own DNS. The error text points at the monitor's resolver, but it was recorded as
`provider_down`, which would misattribute it. Flagged for the monitor rather
than for FBOS.
