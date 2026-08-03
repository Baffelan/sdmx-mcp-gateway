# Monitor Incidents

Written by the `monitor-triage` routine. Newest entry first.
Each scheduled run commits to its own branch and merges into `main`, so this
file is the canonical record and the routine's memory across runs.

## 2026-08-03T18:43Z - cycle 115

**Changed:** ESTAT `healthy` -> `gateway_issue` -> `healthy`, flapped between
runs.

**Cycle saw:** at cycle 113 (2026-08-03T14:01:22+00:00), the gateway metadata
check for ESTAT hit the hard 60,000ms deadline: `tool call list_dataflows
timed out after 60.0s`, 2 attempts, direct path (metadata, data, json) stayed
healthy throughout. It recovered at cycle 114 (2026-08-03T16:01:22+00:00),
latency 46,729ms on 2 attempts, and stayed healthy at cycle 115
(2026-08-03T18:01:22+00:00), latency 50,562ms on 1 attempt (84% of the
deadline). This is the same failing check as every prior ESTAT episode and,
per the cycle 112 entry's own framing, the fourth distinct occurrence since
cycle 85. It is also the shortest: 1 cycle, versus 7, ~6 (cleared 105-106),
and 3 for the first three. No other endpoint changed status across cycles
112-115; all eleven others stayed healthy throughout. `/api/contracts`
`changes` is empty at cycle 115, and ESTAT's own contract rows stayed `ok`
even during the cycle 113 timeout (12 total, 0 broken) - the timeout only
took out the gateway metadata check, nothing else. The only non-`ok` verdict
anywhere is the same already-known `STATSNZ auth:listing
capability_appeared` (open since cycle 60, unchanged, not re-reported).

**Live recheck:** current state confirmed live via cycle 115's own gateway
metadata check (50,562ms, ok) - no separate direct-path fetch was needed
since the endpoint is presently healthy and cycle 115 already re-ran the
exact check in question 42 minutes before this triage run.

**Classification:** `gateway_issue` (ours), consistent with every prior
episode: direct path fine, only the gateway's `list_dataflows` call fails.
Likely code site unchanged from the cycle 109/112 entries:
`monitor/checks_gateway.py` (`READ_TIMEOUT_FLOOR_S = 60.0`, the 60s deadline
that fired) wrapping whatever `list_dataflows` does in `tools/sdmx_tools.py`
against the ESTAT metadata endpoint.

**History:** fourth episode, cycles 113-113 (1 cycle, resolved same day).
Prior episodes: cycles ~85-96 (first, ~7 cycles per cycle 106 entry), cycles
100-106 (second, 7-cycle streak), cycles 107-109 (third, 3 cycles). The
cycle 112 entry explicitly flagged this as the trigger point: "treat the
next occurrence as the trigger to actually apply one of the outstanding
fixes rather than watching a fourth time." That next occurrence is this one.
The margin did not widen in between: cycle 112 sat at 49,095ms/60,000ms
(82%), cycle 113 timed out at exactly 60,000ms, cycle 114 recovered at
46,729ms (78%, 2 attempts), cycle 115 sits at 50,562ms (84%, 1 attempt). The
budget is being consumed at 78-84% on every recent passing cycle, not just
the ones that fail.

**Recommended action:** this run is scoped to `docs/monitor/` only and does
not carry authorization to change gateway or monitor code, so no fix is
applied here. Recording explicitly that the fourth-occurrence trigger named
in the cycle 112 entry has now been hit, so the next session with a broader
mandate should treat applying one of the three outstanding fixes (raise the
deadline, stream the listing, cache the parsed result) as due rather than
optional.

**Could not determine:** why cycle 113 timed out on the second attempt (2
attempts recorded, same as the recovered cycle 114) when the retry mechanism
apparently gives the call a second try within the same 60s budget - whether
both attempts share the deadline or each gets its own is not observable from
the monitor's output alone, and matters for judging how close to failing the
"healthy" cycles actually are.

## 2026-08-03T12:43Z - cycle 112

**Changed:** ESTAT `gateway_issue` -> `healthy`.

**Cycle saw:** the third `gateway_issue` episode (cycles 107-109, recorded in
the cycle 109 entry) cleared at cycle 110 (2026-08-03T08:01:22Z) and has held
healthy through cycles 110, 111, and 112 (2026-08-03T12:01:22+00:00) - 3
consecutive healthy cycles. No other endpoint changed status anywhere in
cycles 109-112; all eleven others stayed healthy throughout. `/api/contracts`
`changes` is empty at cycle 112. The only non-`ok` verdicts present are the
same two already-known, unchanged items: `STATSNZ auth:listing
capability_appeared` (open since cycle 60, still present, not re-reported
because unchanged) and `BIS`/`ILO`/`IMF` `references:contentconstraint
ignored` (architectural, expected).

**Live recheck:** fetched the direct ESTAT dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT?references=none`)
just now: HTTP 200, 37MB in 28.1s, matching the transfer times seen in the
cycle 106 and cycle 109 entries (27.9s and 29.0s for the same payload). The
raw transfer time has not changed. More telling: cycle 112's own gateway
metadata check for ESTAT recorded a latency of 49,095ms against the 60,000ms
deadline - 82% of the budget consumed on a cycle the monitor still counted as
a pass. The symptom cleared, but the margin did not widen.

**Classification:** recovery from `gateway_issue` (ours). The underlying
mechanism named in the cycle 109 entry - transfer consistently fits in
~28-29s, so the remainder is gateway-side parse/processing time - is
unchanged. At 49.1s against a 60s deadline this cycle passed with roughly 11s
to spare, which is not a comfortable margin.

**History:** third episode (cycles 107-109) resolved after 3 cycles, the
shortest of the three recorded episodes (first: cycles 98-104, 7 cycles;
second: none, cleared 105-106; third: cycles 107-109, 3 cycles). No fix has
been applied yet; the cycle 109 entry's recommended actions (raise the
deadline, stream the listing, or cache the parsed result) remain outstanding.

**Recommended action:** the pattern has now recurred three times without a
code change, each time clearing on its own within a handful of cycles. Given
cycle 112's latency sat at 82% of the deadline while still passing, treat the
next occurrence as the trigger to actually apply one of the outstanding
fixes rather than watching a fourth time.

**Could not determine:** why parse/processing time varies enough to push the
same ~28s transfer over a 60s deadline on some cycles and not others (catalog
size at fetch time, host load, GC pauses are all plausible and none are
observable from outside the gateway).

## 2026-08-03T06:45Z - cycle 109

**Changed:** ESTAT `healthy` -> `gateway_issue`.

**Cycle saw:** the same failing check as every prior ESTAT episode: `gateway
metadata: tool call list_dataflows timed out after 60.0s`. Direct path (both
metadata and data) and gateway data checks kept passing throughout; only the
gateway's `list_dataflows` call failed. It started at cycle 107
(2026-08-03T02:01:22Z), right after the two healthy cycles (105, 106) the
last entry recorded, and was still failing at cycle 108 and cycle 109
(2026-08-03T06:01:22Z) - 3 consecutive cycles so far. No other endpoint
changed status across cycles 106-109; all eleven others stayed healthy. The
only `/api/contracts` `changes` entry at cycle 109 is OECD's
`encoding:structure_xml` observed value swapping the order of its
`charset`/`version` parameters (verdict stays `ok`), the same cosmetic
Content-Type reordering already known and dismissed for ABS. No assertion is
`broken` or newly `capability_appeared`; `STATSNZ auth:listing
capability_appeared` (open since cycle 60) and `BIS`/`ILO`
`references:contentconstraint ignored` remain the only non-`ok` verdicts,
both already known and architectural.

**Live recheck:** fetched the direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?references=none`,
matching the cycle 106 entry's methodology) just now: HTTP 200, 37MB in
29.0s, essentially identical to the cycle 106 recheck (27.9s for the same
size). The raw transfer is not the bottleneck now any more than it was then.
An initial recheck attempt that omitted `?references=none` downloaded 27MB
in 65s without finishing, which only shows that the unqualified URL pulls a
much larger payload (likely `references=all`-equivalent) and is not a
like-for-like comparison; it is not used as evidence here.

**Classification:** `gateway_issue` (ours). Direct metadata and data both
succeed; only the gateway's own `list_dataflows` call exceeds its 60s
deadline. This points at parse/processing time on top of a transfer that
consistently fits in ~29s, not at Eurostat being unreachable or slow to
respond.

**History:** third distinct `gateway_issue` episode for this exact check.
First: cycles 98-104 (7 cycles, longest on record). Second: none between -
it cleared at 105-106. Third: cycles 107-109, ongoing as of this run. The
cycle 106 entry explicitly deferred action with the condition "if ESTAT
returns to `gateway_issue` a third time, ... action them rather than watched
again." This is that third time.

**Recommended action:** action one of the previously recommended fixes now
rather than deferring again: raise the gateway's call deadline for this
endpoint, paginate/stream the listing instead of parsing it whole, or cache
the parsed result between cycles.

**Could not determine:** whether this episode will clear on its own like the
first one did, or reflects a durable change (catalog growth, slower parsing
under load) that will keep recurring until one of the recommended fixes is
applied. No gateway-side parse-duration metric is exposed to distinguish
these.

## 2026-08-03T00:43Z - cycle 106

**Changed:** ESTAT `gateway_issue` -> `healthy`.

**Cycle saw:** ESTAT held `gateway_issue` continuously from cycle 98 through
104 (2026-08-02T08:01Z to 2026-08-02T20:01Z, 7 consecutive cycles, 14 hours),
the same failing check throughout: `gateway metadata: tool call
list_dataflows timed out after 60.0s`. It cleared on its own at cycle 105
(2026-08-02T22:01:22Z) and stayed clear at cycle 106
(2026-08-03T00:01:22Z), both with `failing: []`. No other endpoint changed
status across cycles 103-106; `ABS, BIS, ECB, FBOS, ILO, IMF, OECD, SBS, SPC,
STATSNZ, UNICEF` stayed healthy throughout. `/api/contracts` `changes` is
empty at cycle 106; the only non-`ok` verdicts in the matrix are the
already-known `STATSNZ auth:listing capability_appeared` (open since cycle
60) and `BIS references:contentconstraint ignored` (architectural, expected).

**Live recheck:** direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?references=none`)
answered HTTP 200 just now, 37MB in 27.9s, in line with every prior live
recheck in this file (27.5-29.5s). The raw transfer has never been the
bottleneck; it is the gap between that and the gateway's 60s call deadline
that closed. Could not directly measure gateway-side XML parsing time, so
whether the recovery is a genuine speedup (less load, faster parse) or the
timeout margin narrowly clearing by chance is not established.

**Classification:** `gateway_issue` resolved. This was previously flagged
(cycle 103 entry) as a confirmed regression, not a flap, at double the
length of either prior streak (83-85, 92-94, three cycles each). It has now
cleared for two consecutive cycles, longer than the recovery window of
either prior streak, so treat it as resolved rather than a third occurrence
of the same flap. Nothing in this diagnosis pointed to a gateway code
change; the previous entry's recommended fixes (raise the deadline, stream
the listing, cache the parsed result) were not applied as far as this
routine can tell from the repository history available to it.

**History:** 7 consecutive cycles of `gateway_issue` (98-104), longest ESTAT
streak on record, now healthy at 105 and 106.

**Recommended action:** none required now that it has cleared. If ESTAT
returns to `gateway_issue` a third time, the recommended fixes from the
cycle 103 entry (raise the deadline for this endpoint, paginate/stream the
listing, or cache the parsed result) still apply and should be actioned
rather than watched again.

**Could not determine:** why the gateway-side timing recovered; no
gateway-side parse-duration metric is exposed by the monitor to confirm
whether this was a genuine speedup or a narrow margin clearing by chance.

## 2026-08-02T18:44Z - cycle 103

**Changed:** no status change since the cycle 100 entry (ESTAT was already
`gateway_issue` there), but the ESTAT streak has now crossed the threshold
that entry set up to watch for.

**Cycle saw:** ESTAT has held `gateway_issue` continuously from cycle 98
through cycle 103, six consecutive cycles (2026-08-02T08:01Z to
2026-08-02T18:01Z, 12 hours). The failing check is unchanged: `gateway
metadata: tool call list_dataflows timed out after 60.0s`. Direct path
stays healthy the whole window. No other endpoint changed status across
cycles 100-103; `ABS, BIS, ECB, FBOS, ILO, IMF, OECD, SBS, SPC, STATSNZ,
UNICEF` stayed healthy throughout. `/api/contracts` `changes` is empty at
cycle 103; the only non-`ok` verdict in the matrix is the already-known
`STATSNZ auth:listing capability_appeared` (open since cycle 60, not a new
development).

**Live recheck:** direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest`,
the same call the gateway's `list_dataflows` wraps) answered HTTP 200 just
now, 37MB in 28.1s. That is consistent with the two prior live rechecks in
this file (29.5s at cycle 100, 27.5s at cycle 97) - the raw transfer alone
stays comfortably under the 60s deadline. The gap between "raw fetch takes
~28s" and "gateway call times out at 60s" is presumably the gateway's own
XML parsing of a 37MB payload on top of the transfer, which this recheck
does not measure directly.

**Classification:** `gateway_issue` - direct path OK, gateway path failing.
Same mechanical cause as the last two occurrences (our own 60s call
deadline against a slow, large provider listing endpoint), but the
duration has changed character.

**History:** the cycle 100 entry explicitly flagged: "If ESTAT is still
`gateway_issue` at cycle 101 this streak would be longer than both prior
occurrences and should be treated as a possible regression." It is now
cycle 103 and the streak has not cleared at 101, 102, or 103 - six cycles
running, double the length of either prior streak (83-85 and 92-94, three
cycles each, both of which cleared on their own). This is no longer inside
the previously observed range.

**Recommended action:** treat as a regression rather than the same
recurring flap. Worth a real fix rather than another wait-cycle: either
raise the gateway's call deadline for this specific slow endpoint, have
`list_dataflows` stream/paginate the ESTAT listing instead of pulling the
full 37MB body, or cache the parsed listing between calls. Continue
watching status; if it clears on its own before a fix ships, note the
total duration in the next entry.

**Could not determine:** whether the gateway-side timing has actually
gotten worse (e.g. slower parsing, more dataflows in the payload) or
whether this is the same per-call timing as before just landing on the
unlucky side of the 60s boundary six times running. The recheck above
measures only the direct HTTP fetch, not the gateway's parsing step, so
the actual gateway-side duration for this run is not verified.

## 2026-08-02T12:44Z - cycle 100

**Changed:** ESTAT healthy -> gateway_issue

**Cycle saw:** ESTAT moved to `gateway_issue` at cycle 98 (2026-08-02T08:01Z)
and has held that status through cycles 99 and 100, three consecutive cycles.
The failing check each time is the same: `gateway metadata: tool call
list_dataflows timed out after 60.0s`. No other endpoint changed status
across cycles 97-100; `ABS, BIS, ECB, FBOS, ILO, IMF, OECD, SBS, SPC,
STATSNZ, UNICEF` stayed healthy the whole window. `/api/contracts` `changes`
is empty at cycle 100; the only non-`ok` verdict in the matrix is the
already-known `STATSNZ auth:listing capability_appeared`, unchanged.

**Live recheck:** direct ESTAT dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT`)
answered HTTP 200 just now, 37MB in 29.5s, well under the 60s gateway
deadline on its own. Other endpoints are unaffected (confirmed by their
status remaining healthy at cycle 100), so this is not a network problem on
this run's side.

**Classification:** `gateway_issue` - direct path OK, gateway path failing.
This is the known/expected kind: our own 60s call deadline firing on a slow
provider listing endpoint, not a code bug, and not a provider outage.

**History:** matches the documented "list_dataflows times out at 60s under
load, then recovers" pattern (see the cycle 97 entry below and the skill's
flapping list). Two prior streaks of this exact pattern (83-85, 92-94) each
ran 3 cycles and cleared on their own. This is a third streak, currently 3
cycles long (98-100) and still open as of this run - at the edge of, not yet
past, the previously observed range.

**Recommended action:** watch the next cycle. If ESTAT is still
`gateway_issue` at cycle 101 this streak would be longer than both prior
occurrences and should be treated as a possible regression rather than the
same recurring pattern.

**Could not determine:** whether cycle 101 will clear on its own like the
prior two streaks, since that cycle has not run yet.

## 2026-08-02T06:44Z - cycle 97

**Changed:** ESTAT gateway_issue -> healthy, ILO degraded -> healthy (both
resolving the open items left by the cycle 94 entry below)

**Cycle saw:** ESTAT recovered to healthy at cycle 95 (2026-08-02T02:01Z)
and has held healthy through cycles 96 and 97. ILO recovered to healthy at
the same cycle 95 and has also held through 96 and 97. No other endpoint
changed status across cycles 94-97; `ABS, BIS, ECB, FBOS, IMF, OECD, SBS,
SPC, STATSNZ, UNICEF` stayed healthy the whole window. `/api/contracts`
`changes` is empty at cycle 97; the four ILO contract assertions that
broke at cycle 94 (`references:children`, `references:none`,
`references:parents`, `references:parentsandsiblings`) all read `ok`
again.

**Live recheck:** direct ILO metadata (`/rest/dataflow/ILO/all/latest`)
answered HTTP 200 in 2.6s just now, consistent with healthy. Direct ESTAT
dataflow listing (`/dataflow/ESTAT/all/latest`) answered HTTP 200 but took
27.5s, confirming the endpoint is still slow under the hood even though it
now finishes inside the 60s gateway deadline; this matches the documented
"list_dataflows times out at 60s under load, then recovers" pattern rather
than a new problem.

**Classification:** both resolved as predicted in the cycle 94 entry.
ESTAT: `gateway_issue`, the known/expected kind (our own timeout firing on
a slow provider endpoint, not a code bug). ILO: was provider-side
(403 on direct paths only, gateway paths stayed healthy throughout), now
fully recovered.

**History:** ESTAT's 92-94 streak ran 3 cycles, matching the length of the
prior 83-85 streak, then cleared - stays inside the previously observed
range, no escalation warranted. ILO's cycle 94 occurrence was a single
cycle, same shape as the cycle-32 flap in duration (one cycle) even though
it hit more checks (metadata+data+json plus four contract assertions,
versus data+json only at cycle 32).

**Recommended action:** none. Both open items from the cycle 94 entry are
now closed. Continue watching ILO for a third, broader occurrence, which
would be the point this stops looking like a flap.

**Could not determine:** the underlying cause on either provider's side
(ESTAT's server-side listing latency, ILO's transient 403) since neither
publishes incident information the routine can read.

## 2026-08-02T00:43Z - cycle 94

**Changed:** ILO healthy -> degraded (contract broken)

**Cycle saw:** at cycle 94 (2026-08-02T00:01Z) ILO's direct metadata, direct
data, and direct json checks all failed with HTTP 403. Gateway metadata and
gateway data checks passed. Four contract assertions flipped to `broken` on
the same cause: `references:children`, `references:none`,
`references:parents`, `references:parentsandsiblings` all went from `200`
to `403`. This is the same endpoint that flapped with a direct-path 403 at
cycle 32 (data and json only, recovered by the next cycle), but this
occurrence is broader: it now includes metadata and breaks four contract
checks that held through cycle 32.

**Live recheck:** fetched the exact monitor paths directly at 00:43 UTC
(about 40 minutes after the cycle), with the monitor's own User-Agent
header, three times each for metadata and data: HTTP 200 every time, both
paths. ECB and OECD direct fetches also succeeded in the same window,
ruling out a network problem on this routine's side. The live result
disagrees with what the cycle recorded, pointing at a transient block
(rate limit or WAF hiccup on ILO's side) that has already cleared rather
than a durable change.

**Classification:** provider-side, not `gateway_issue` (theirs, not ours).
Gateway checks passed while only direct checks failed, so the gateway code
path is unaffected; this reads as ILO blocking or rate-limiting the
monitor's direct client specifically for one cycle.

**History:** ILO was healthy for all 23 preceding cycles in the 48-hour
window (cycles 71 through 93). This is the only degraded cycle so far;
next cycle (expected ~02:01 UTC) will confirm whether it recovers as the
cycle-32 flap did.

**Recommended action:** watch the next cycle before filing anything
further. If the 403 recurs, especially with metadata included again, that
would be new behavior worth investigating on ILO's side (their WAF
tightening) rather than the transient blip this looks like now.

**Could not determine:** why the block hit metadata/data/json and four
contract checks simultaneously this time when the cycle-32 flap only hit
data/json; ILO gave no error body beyond the 403 status.

## 2026-08-02T00:43Z - cycle 94

**Changed:** ESTAT healthy -> gateway_issue

**Cycle saw:** `gateway metadata: tool call list_dataflows timed out after
60.0s` at cycles 92, 93, and 94 (2026-08-01T20:01Z through
2026-08-02T00:01Z), a fresh 3-cycle streak. Direct path checks all passed;
direct json is skipped as expected (Eurostat returns 406 for SDMx-JSON,
architectural). No contract assertions affected.

**Live recheck:** not applicable in the same way as a provider check: the
failure is the gateway's own `list_dataflows` tool call exceeding its
60-second deadline, which is the documented behavior noted in the prior
baseline ("ESTAT list_dataflows times out at 60s under load, then
recovers... our own call deadline firing, working as designed").

**Classification:** `gateway_issue`, but the known/expected kind: this is
our own timeout firing under load against a slow ESTAT list endpoint, not
a code bug. Ours by label, not a defect to fix per the existing baseline
note.

**History:** this endpoint flapped the same way at cycles 83-85 (3
cycles) and once at cycle 87, then held healthy through cycles 88-91 (4
cycles) per the last recorded state. This new streak (92-94, 3 cycles and
counting, still gateway_issue at the newest cycle) is longer than the
single-cycle blip at 87 but matches the 83-85 streak length. Not yet
resolved as of cycle 94.

**Recommended action:** continue watching. If this streak extends past 4-5
cycles (>8-10 hours) it would cross into new territory and merit a
gateway-side timeout/retry investigation; at 3 cycles it is still within
the previously observed range.

**Could not determine:** whether Eurostat's `list_dataflows` endpoint is
now consistently slower (a load-time regression on their side) or this is
ordinary variance, since the routine has no visibility into ESTAT's
server-side timing.

## 2026-08-01T18:41Z - cycle 91

**Changed:** IMF provider_down -> healthy

**Cycle saw:** IMF was already healthy again at cycle 89 (2026-08-01T14:01Z),
the very next cycle after the cycle 88 401 outage, and has stayed healthy
through cycles 90 and 91 (2026-08-01T16:01Z and 18:01Z), three consecutive
cycles / 6 hours. No other endpoint changed status across cycles 89-91; all
twelve were healthy at cycle 91.

**Live recheck:** fetched `https://api.imf.org/external/sdmx/2.1/dataflow/IMF.STA/all/latest`
directly at 18:41 UTC: HTTP 200. Matches the monitor's own reading.

**Classification:** recovery confirmed. This is the outcome the cycle 88
entry predicted: "watch the next cycle; if IMF is healthy again at cycle 89,
treat this as a one-off transient 401." That is exactly what happened, so
this closes the open item from the previous run.

**History:** provider_down for a single cycle only (cycle 88), healthy for
22 cycles before it and 3 cycles after it so far. No recurrence.

**Recommended action:** none. Treat as resolved; no gateway change needed.

**Could not determine:** the root cause of the original cycle 88 401 is
still unknown (IMF gave no error detail beyond the status code), but with
the endpoint stable for 6 hours this is no longer worth pursuing unless it
recurs.

## 2026-08-01T12:43Z - cycle 88

**Changed:** IMF healthy -> provider_down

**Cycle saw:** at cycle 88 (2026-08-01T12:01Z) every IMF check failed with
HTTP 401 Unauthorized: gateway metadata (`dataflow/IMF.STA/all/latest`),
gateway data, direct metadata, and direct data. Direct json is skipped as
expected (IMF ignores the JSON `Accept` header, a known architectural
fact). Ten contract assertions flipped to `broken` on the same underlying
cause; `auth:listing` reports "provider now demands credentials" (was
`200`, now `401`).

**Live recheck:** fetched `https://api.imf.org/external/sdmx/2.1/dataflow/IMF.STA/all/latest`
directly three times at 12:43 UTC, about 40 minutes after the cycle ran:
HTTP 200 every time. ECB and OECD direct fetches also succeeded in the
same window, which rules out a network problem on this routine's side.
The live result disagrees with what the cycle recorded, which points to a
transient failure that has already cleared rather than a durable new auth
requirement.

**Classification:** `provider_down` (theirs). Gateway and direct paths
failed identically, so nothing points at gateway code.

**History:** IMF had been healthy for at least 22 consecutive cycles
before this (cycle 66 through 87, 2026-07-30T14:25Z through
2026-08-01T10:01Z). No prior IMF auth failure appears in this log.

**Recommended action:** watch the next cycle. If IMF is healthy again at
cycle 89, treat this as a one-off transient 401 and take no further
action. If it recurs, that is new: IMF has never required credentials for
this listing before, and the gateway would need to start handling IMF
auth.

**Could not determine:** why the provider returned 401 for roughly the
two-hour cycle window despite being reachable again less than an hour
later; whether this was a brief provider-side auth rollout, a
misclassified rate limit, or unrelated infrastructure noise.

## 2026-08-01T12:43Z - cycle 88

**Changed:** ESTAT gateway_issue -> healthy

**Cycle saw:** ESTAT was healthy at cycle 88, continuing the known
`list_dataflows` timeout flapping pattern. Since the last recorded run
(cycle 85, `gateway_issue` for 3 consecutive cycles 83-85), it recovered
at cycle 86, recurred for one cycle at 87, then recovered again at 88.

**Live recheck:** not needed; the endpoint is currently healthy and
matches the cycle.

**Classification:** `gateway_issue` when failing (ours, `list_dataflows`
timeout against ESTAT); currently `healthy`.

**History:** matches the documented short-blip pattern - occurrences
under 4-5 cycles have always self-recovered so far. Only two prior
streaks (11 and 14 cycles) needed the gateway-side timing investigation
that remains undone. Neither the 83-85 streak nor the single cycle-87
recurrence reached that threshold.

**Recommended action:** none. Per the open item recorded at cycle 85 ("if
it clears, no action"), this occurrence is resolved. Continue watching for
a streak that clears the 4-5 cycle mark before doing the `list_dataflows`
timing investigation.

**Could not determine:** nothing outstanding for this occurrence.

## 2026-08-01T06:42Z - cycle 85

**Changed:** ESTAT healthy -> gateway_issue

**Cycle saw:** failing check `gateway metadata: tool call list_dataflows
timed out after 60.0s` at cycles 83, 84 and 85 (2026-08-01T02:01Z through
06:01Z), 3 consecutive cycles so far. Gateway data, direct metadata and
direct data all pass; direct json is skipped as expected (ESTAT returns 406
for SDMx-JSON, a known architectural fact). Last healthy cycle was 82
(2026-08-01T00:01Z), matching the state file's last recorded run.

**Live recheck:** fetched the same ESTAT full dataflow listing directly
just now (`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest`):
HTTP 200 in 28.0s, 37.1 MB, comfortably under the 60s deadline the gateway
call is missing. Consistent with every prior recheck of this same failure
(29.4s, 32s, 41.9s in earlier incidents) - the raw fetch is never close to
timing out.

**Classification:** `gateway_issue` (ours). Direct path healthy, gateway
path failing on `list_dataflows` specifically.

**History:** this is a fresh recurrence, not a continuation. The prior
streak (cycle 63-76, 14 cycles) recovered at cycle 77 and held healthy
through cycle 82 (6 cycles, noted as an open item to watch). It broke again
at cycle 83. Every prior short occurrence (cycles 49-50, 53-55, 63-65) ran
3 cycles or less before either recovering or turning into the long streak;
this one is at 3 cycles and still open as of this run.

**Recommended action:** watch the next cycle. If it clears on its own,
this matches the short-blip pattern and needs no further action. If it
extends past 4-5 cycles, that has twice before been the leading edge of a
much longer streak (11 and 14 cycles respectively) and is worth the
gateway-side `list_dataflows` timing investigation that two prior entries
already recommended and no one has yet done.

**Could not determine:** why the gateway's `list_dataflows` call for ESTAT
is slower than the raw HTTP fetch it wraps, since this routine can only
measure from outside the gateway process.

## 2026-07-31T18:43Z - cycle 79

**Changed:** ESTAT gateway_issue -> healthy

**Cycle saw:** all checks passing at cycle 79 (gateway metadata, gateway
data, direct metadata, direct data, direct json). Recovery actually landed
at cycle 77 (2026-07-31T14:01:22Z), two cycles before this run's comparison
point, and held through cycles 78 and 79 as well.

**Live recheck:** direct dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest`)
answered HTTP 200 in 32s just now, well under the 60s deadline that was
timing out during the incident, and consistent with the prior finding that
the direct fetch was never the slow side.

**Classification:** recovery from `gateway_issue` (ours). The failing check
throughout was `gateway metadata: tool call list_dataflows timed out after
60.0s`, i.e. the gateway's own call to `list_dataflows`, not the provider.

**History:** this closes the streak that ran cycle 63 through cycle 76 (14
consecutive cycles, ~27.6 hours), the longest continuous `gateway_issue`
period recorded for ESTAT so far. No other endpoint changed status in the
same window (cycles 77-79 all healthy across the board), and no contract
assertion changed (`changes: []` from `/api/contracts`). STATSNZ
`auth:listing` remains `capability_appeared` as before, unchanged since
cycle 60, not a fresh event.

**Recommended action:** none required now that it has cleared, but the
14-cycle duration is notably longer than ESTAT's prior single-cycle
`list_dataflows` timeouts (cycles 54-55). Worth a closer look next time it
recurs at whether the gateway-side timeout or retry behavior for ESTAT
specifically needs adjusting, since the direct path was never close to the
60s limit during the incident.

**Could not determine:** what caused the `list_dataflows` call to time out
for 14 cycles straight, or what changed at cycle 77 to clear it; no error
detail beyond the timeout message is available from the monitor.

## 2026-07-31T06:42Z - cycle 73

**Changed:** ABS gateway_issue -> healthy

**Cycle saw:** all checks passing at cycle 73 (gateway metadata, gateway
data, direct metadata, direct data, direct json). Recovery actually landed
at cycle 71 (2026-07-31T02:01:22Z), two cycles before this run's comparison
point, and held through cycles 72 and 73.

**Live recheck:** direct path
(`https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/latest`) answered HTTP
200 in 1.4s just now. The gateway path goes through the `list_dataflows` MCP
tool, which is not reachable directly with curl from this session; the
monitor's own three consecutive healthy cycles are the evidence for that
side.

**Classification:** recovery from `gateway_issue` (ours). The empty-error
pattern (`gateway metadata: Error:`, no message body) first seen at cycle 26
and recurring at cycle 70 has cleared again.

**History:** matches the cycle-26 pattern: transient, self-resolving within
a cycle or two, no user-visible recurrence beyond a single blip.

**Recommended action:** none. If the empty-error pattern recurs a third
time, the empty error string itself is worth fixing in gateway code so the
next occurrence carries a diagnosable message.

**Could not determine:** what caused the cycle-70 failure or why it
cleared; the gateway gives no detail beyond the empty error string.

## 2026-07-31T06:42Z - cycle 73

**Changed:** ILO degraded -> healthy

**Cycle saw:** all checks and all twelve contract assertions passing at
cycle 73. Recovery landed at cycle 71, the cycle immediately after the
degraded one, and held through cycles 72 and 73.

**Live recheck:** attempted to re-verify directly just now and got a
confusing result: the pinned metadata path, the pinned CSV data path, and
the pinned JSON data path
(`.../data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`,
`Accept: application/vnd.sdmx.data+json;version=2.0.0`) all returned HTTP
403 "Access is denied" from ILO's Cloudflare/IIS front end on repeated
requests, including the metadata path, which had itself returned HTTP 200
on the very first request made minutes earlier. Reads like ILO's own
rate-limiting or WAF reacting to several requests in quick succession from
this session's IP, not a real outage: the monitor's own three consecutive
cycles (71, 72, 73) all recorded the same checks passing at 200, most
recently about 40 minutes before this recheck. Ruled out a broader network
problem on this session's side first: ABS and ESTAT direct requests both
answered normally in the same window.

**Classification:** recovery from `degraded` (provider-side, direct path
only; gateway path was unaffected throughout). The live-recheck 403s are
recorded but not trusted as current evidence of ILO's state, given the
likely self-inflicted rate-limit confound.

**History:** matches the cycle-32 pattern exactly: one cycle of HTTP 403 on
the direct path, then full recovery by the next cycle. Second occurrence of
this exact pattern, 41 cycles apart.

**Recommended action:** none beyond recording it. If a future run's live
recheck against ILO needs to be trusted, it should pace its requests
(single request per path, with delay) rather than firing several pinned
paths back to back, since that pattern alone was enough to draw a 403 this
time.

**Could not determine:** whether ILO's WAF has become stricter and is now
the reason this routine cannot reliably confirm ILO by direct recheck, or
whether this run's IP coincidentally tripped a threshold.

## 2026-07-31T06:42Z - cycle 73

**Changed:** none (status unchanged, still `gateway_issue`) - recorded
because the previous run set an explicit watch threshold for this endpoint
that this run crosses.

**Cycle saw:** `gateway metadata: tool call list_dataflows timed out after
60.0s`, continuously from cycle 63 (2026-07-30T08:25:41Z) through cycle 73
(2026-07-31T06:01:22Z): 11 consecutive cycles, roughly 22 hours with no
recovery cycle in between. Direct path passing throughout, per the
monitor's own classification.

**Live recheck:** fetched the same ESTAT full dataflow listing directly
just now
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all`):
HTTP 200 in 29.4s, 37.1 MB, comfortably under the 60s deadline. Similar
order of magnitude to the 41.9s seen in the cycle-65 recheck.

**Classification:** `gateway_issue` by the monitor's own label; the direct
path keeps answering within the deadline while the gateway path keeps
missing it, so on its face this is still our own call deadline firing
against a slow provider response, not a provider outage.

**History:** the cycle-65 report called this "the longest run of
consecutive gateway_issue cycles observed for ESTAT so far (3)" and said
"if the streak extends past 3-4 more cycles ... that would be new
information worth a closer look." It has now extended to 11 consecutive
cycles, well past that threshold, with no recovery in between - unlike
every prior occurrence (cycles 49-50, 53-55, 63-65 as previously recorded),
none of which ran longer than 3 cycles.

**Recommended action:** worth the closer look the prior run flagged. Two
independent direct-fetch rechecks (29.4s and 41.9s) both land well under
60s, which does not obviously explain a gateway-side timeout sustained for
22 continuous hours. Either the gateway's effective budget for this call is
tighter than the raw HTTP fetch time it wraps, or something in the
gateway's own processing around the fetch (not the fetch itself) has
slowed. Worth checking gateway logs/timing for `list_dataflows` on ESTAT
specifically; a 22-hour continuous streak is no longer well explained by
"provider slow under load," which fit the earlier 2-6 hour blips.

**Could not determine:** whether the gateway's `list_dataflows` handling of
ESTAT carries overhead beyond the raw HTTP fetch, since this routine can
only measure the direct fetch time from outside, not the gateway's own
processing time.

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
