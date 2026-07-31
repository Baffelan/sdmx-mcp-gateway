# Monitor Incidents

Written by the `monitor-triage` routine. Newest entry first.
Each scheduled run commits to its own branch and merges into `main`, so this
file is the canonical record and the routine's memory across runs.

## 2026-07-31T00:43Z - cycle 70

**Changed:** ILO healthy -> degraded

**Cycle saw:** direct json failing with HTTP 403. Contract assertions
`auth:listing` (200 -> 403, "provider now demands credentials"),
`references:descendants` (200 -> 403), and `references:parentsandsiblings`
(200 -> 403) all flipped to `broken`. Gateway metadata and gateway data both
passed; direct metadata and direct data both passed. Only the direct json
channel and three reference-style contract checks against ILO were affected.

**Live recheck:** re-ran all four requests directly against
`https://sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`
just now (plain listing, `?references=descendants`,
`?references=parentsandsiblings`, `?references=none`): all four returned
HTTP 200. The json data query with `Accept: application/vnd.sdmx.data+json`
returned HTTP 400, not 403 (ILO does not serve SDMX-JSON for data queries;
this is the endpoint's documented behavior, not the 403 seen in the cycle).
Cycle and live recheck disagree on every point that changed, which points to
a transient condition at ILO that has already cleared.

**Classification:** provider-side (`degraded`, failures on the direct path
only; gateway path was unaffected). Not a `gateway_issue` and not caused by
our code.

**History:** ILO has flapped exactly this way before: HTTP 403 on direct
data and json at cycle 32, recovered by the next cycle. This looks like the
same pattern recurring, 38 cycles later. Healthy for the 8 preceding cycles
(62-69) before this one.

**Recommended action:** none beyond recording it. Live recheck already
shows recovery; watch the next scheduled cycle to confirm it stays healthy.

**Could not determine:** what caused the momentary 403 on ILO's side (rate
limiting, a backend blip, or a WAF rule); ILO gives no error body to go on.

## 2026-07-31T00:43Z - cycle 70

**Changed:** ABS healthy -> gateway_issue

**Cycle saw:** gateway metadata check failed with `error: "Error: "` (empty
message body), latency 31599ms across 2 attempts. Gateway data, direct
metadata, direct data, and direct json all passed.

**Live recheck:** could not re-run the failing call itself, since it goes
through the gateway's MCP tool (`list_dataflows`) rather than a plain HTTP
endpoint reachable with curl. Rechecked the direct provider path instead
(`https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/latest`), which is
unaffected and answered normally, consistent with the cycle's own read that
the direct path was fine and only the gateway path failed.

**Classification:** `gateway_issue`, ours to fix, not ABS's. The failing
check is gateway metadata (`kind: metadata`, `path: gateway`), which is the
`list_dataflows` tool path.

**History:** this is a recurrence of a previously flagged pattern. Per prior
notes, ABS produced `gateway metadata: Error:` with an empty error body at
cycle 26 (2026-07-30), also recovering. That entry said explicitly: if this
recurs, the empty message is itself the bug to report. It has now recurred,
44 cycles later. ABS was healthy for the 21 preceding cycles (49-69).

**Recommended action:** the empty error message on this specific failure
path is worth fixing in its own right so a future occurrence carries an
actual exception message or type instead of `Error: ` with nothing after it.

**Could not determine:** whether the underlying failure is a timeout, a
connection error, or something else, because the error string carries no
detail. The 31599ms latency with 2 attempts suggests each attempt ran
roughly 15-16s before failing, well under any of this gateway's own 60s
deadlines, so it does not look like our own timeout firing the way the
ESTAT case does.

## 2026-07-30T18:46Z - cycle 68

**Changed:** contract `ABS encoding:structure_xml` was/now pair reported by
`/api/contracts`: `application/vnd.sdmx.structure+xml; charset=utf-8;
version=2.1` -> `application/vnd.sdmx.structure+xml; version=2.1;
charset=utf-8`. Verdict stayed `ok` on both sides; only the parameter order
in the `Content-Type` header differs.

**Cycle saw:** the was/now pair above, computed server-side by the monitor
against the previous contract check.

**Live recheck:** fetched `https://data.api.abs.gov.au/rest/dataflow/ABS?references=none`
directly five times in quick succession just now. Both orderings appeared
within the same short window: three responses came back
`version=2.1; charset=utf-8` and two came back `charset=utf-8; version=2.1`.
So this is not a one-off flip the monitor happened to catch; ABS's own
`Content-Type` header genuinely varies between requests, most likely because
requests land on different backend instances that serialize the header
differently.

**Classification:** provider-side, cosmetic. The media type and parameters
are semantically identical either way (`application/vnd.sdmx.structure+xml`,
version 2.1, UTF-8), so nothing consumes this ordering meaningfully and no
gateway assumption is invalidated. Recorded as `ok` throughout.

**History:** first time this specific was/now pair has appeared in the
`changes` array since the baseline at cycle 60. No endpoint status changed
this run: all twelve endpoints match the previous run's state exactly
(cycle 65 -> cycle 68), including ESTAT, which stayed `gateway_issue`.

**Recommended action:** none. Flagging this here mainly so a future run that
sees the header flip back doesn't re-report it as new; it is expected to
alternate.

**Could not determine:** how many distinct backend instances ABS is running
behind this endpoint, or whether the ordering correlates with anything
(region, load balancer, SDMX library version) beyond varying request to
request.

### ESTAT gateway_issue streak, update

Still `gateway metadata: tool call list_dataflows timed out after 60.0s`,
unchanged in shape since cycle 63. Now six consecutive cycles (63 through
68, 2026-07-30T08:25:41Z through 18:25:41+00:00), double the length reported
at cycle 65, and past the "3-4 more cycles" watch threshold set in that
entry.

**Live recheck:** fetched Eurostat's dataflow listing directly
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all`)
just now: HTTP 200 in 29.9s, 37.6 MB payload, comfortably under the 60s
deadline this time. Other providers answered normally in the same window
(ECB 200 in under 2s), so this is not a network problem on this routine's
side.

**Classification:** unchanged from cycle 65: `gateway_issue` by the
monitor's label, but this is our own 60s call deadline firing against a
slow-but-working provider response, not a provider outage or a gateway bug.
The live recheck this run came in well under the deadline (29.9s vs the
41.9s seen at cycle 65), which argues against a provider-side regression in
response size or latency; the streak length looks more like the deadline
sitting close to ESTAT's typical response time than a worsening trend.

**Recommended action:** still no code change indicated. Continue watching;
if the streak extends for multiple more days rather than resolving, that
would be the point to consider raising the deadline or paginating this
check instead of pulling the full dataflow listing.

**Could not determine:** why this streak has run twice as long as any prior
occurrence when the live-fetch timing does not show it getting worse;
would need per-cycle latency numbers this routine does not have to say
whether it is bunched near a slow hour of day or genuinely more frequent
now.

### STATSNZ open item, unchanged

`auth:listing` still reads `capability_appeared` at cycle 68 (expected
"credentials required", observed 200), same as recorded at cycles 60 through
65. No new information this run; carried forward in the state file.

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
