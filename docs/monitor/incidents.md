# Monitor Incidents

Written by the `monitor-triage` routine. Newest entry first.
Each scheduled run commits to its own branch and merges into `main`, so this
file is the canonical record and the routine's memory across runs.

## 2026-08-27T18:44Z - cycle 403

**Changed:** ILO `provider_down` still open, now eight consecutive cycles
(396-403) without resolving -- this exceeds the five-cycle mark reported
last run and keeps climbing. ESTAT flapped twice since cycle 400: `healthy`
-> `gateway_issue` (401) -> `healthy` (402) -> `gateway_issue` (403, still
open at time of writing).

**Cycle saw (ILO):** same blanket shape as every prior cycle in this
episode -- gateway metadata `Client error '403 Forbidden'` on
`dataflow/ILO/all/latest`, gateway data probe HTTP 403, and all three
direct checks (metadata, data, json) also HTTP 403. Unchanged across
cycles 396 through 403.

**Live recheck (ILO):** re-ran `GET https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`
directly at 2026-08-27T18:43:24Z -- still HTTP 403. Confirmed live, not a
stale cycle artifact.

**Network sanity:** direct-hit ECB (`dataflow/ECB`) and OECD
(`dataflow/all`) at the same moment -- both HTTP 200. The routine's own
network is fine; the block is isolated to ILO.

**Cycle saw (ESTAT):** cycles 401 and 403 both show `gateway metadata: tool
call list_dataflows timed out after 60.0s`; cycle 402 was clean `healthy`
with no failing checks.

**Live recheck (ESTAT):** re-ran the direct `dataflow/ESTAT` listing just
now -- HTTP 200 in 28.4s, under the 60s deadline. Consistent with the known
pattern of the gateway's own call deadline firing under a slow response,
not a provider outage.

**Contracts:** the ten ILO assertions (`auth:listing`,
`constraint:availableconstraint`, `errors:missing_artefact`, and all eight
`references:*` variants) still read `broken` with `observed: 403`, the same
blanket-403 condition surfacing through contracts, not a separate finding.
The one `changes` entry this cycle (ABS `encoding:structure_xml`,
charset/version parameter order, verdict stays `ok`) is the known cosmetic
flap already on record -- not re-reporting. STATSNZ `auth:listing` still
reads `capability_appeared`, unchanged since cycle 60 -- not re-reporting.

**Classification:** ILO remains `provider_down` (both gateway and direct
403; provider-side, nothing to fix in our code). ESTAT is `gateway_issue`
(direct path fine, only the gateway's `list_dataflows` tool call timed out;
on our side, but the known architectural pattern, not a new bug).

**History:** ILO's blanket-403 episode (started cycle 396) has now run
eight consecutive cycles (396-403), roughly 16 hours, without a single
healthy reading in between. Every prior occurrence on record (244, 251,
260, 264, 272, 281, 321, 382) resolved within one or two cycles at most.
This is now well past all of those and is approaching the "roughly 24h/12
cycles" mark previously set as the point to treat this as a standing block
rather than a transient outage; four more cycles (24 hours from the block's
start) will cross it. ESTAT's two flaps this window (401, 403) are its
twenty-third and twenty-fourth episodes overall of the same one-cycle
`list_dataflows` timeout pattern, consistent with the long-running
architectural cause. All other ten endpoints stayed healthy through cycles
401, 402, and 403, ruling out the routine's own network for either finding.

**Recommended action:** ILO -- keep watching; if it reaches 12 consecutive
cycles (cycle 407, roughly 24h) without a healthy reading, treat it as a
standing block and flag that the gateway's ILO request pattern (headers,
rate) may need review, which is beyond what this read-only routine can act
on. No new action for ESTAT beyond the standing recommendation already on
record (raise the call deadline, stream the listing, or cache the parsed
result in `monitor/checks_gateway.py`).

**Could not determine:** why this ILO occurrence is running longer than
every prior one -- whether it is a longer provider-side outage, a rate
limit that escalated to a longer ban, or an IP/UA block on the monitor's
egress. Nothing in the response body distinguishes these; the next run
will show whether it has cleared.

## 2026-08-27T12:47Z - cycle 400

**Changed:** ILO `provider_down` still open, now five consecutive cycles
(396-400) without resolving -- this exceeds every prior occurrence on record.
ESTAT `gateway_issue` (open at cycle 397) recovered to `healthy` by cycle 398
and stayed healthy through 399 and 400.

**Cycle saw (ILO):** same blanket shape as cycle 397 -- gateway metadata
`Client error '403 Forbidden'` on `dataflow/ILO/all/latest`, gateway data
probe HTTP 403, and all three direct checks (metadata, data, json) also
HTTP 403, unchanged across cycles 396, 397, 398, 399, 400.

**Live recheck (ILO):** re-ran `GET https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`
directly just now (2026-08-27T12:46Z) -- still HTTP 403. Confirmed live,
not a stale cycle artifact.

**Network sanity:** direct-hit ECB and OECD at the same moment -- both
HTTP 200. The routine's own network is fine; this is isolated to ILO.

**Cycle saw (ESTAT):** cycle 397 showed the usual `gateway metadata: tool
call list_dataflows timed out after 60.0s`; cycles 398-400 all show clean
`healthy` with no failing checks.

**Contracts:** every ILO contract assertion (`auth:listing`,
`constraint:availableconstraint`, `errors:missing_artefact`, and all eight
`references:*` variants) now reads `broken` at cycle 400, each with
`observed: 403` in place of its normal expected response. This is the same
blanket-403 condition surfacing through the contract checks, not a separate
new finding. The one contract `changes` entry this cycle (OECD
`encoding:structure_xml`, charset/version parameter order, verdict stays
`ok`) is the known cosmetic flap already on record -- not re-reporting per
the standing note. STATSNZ `auth:listing` still reads `capability_appeared`,
unchanged since cycle 60 -- not re-reporting.

**Classification:** ILO remains `provider_down` (both gateway and direct
403; provider-side, nothing to fix in our code). ESTAT is back to `healthy`,
resolving within the known one-to-two-cycle flap pattern for its
`list_dataflows` timeout.

**History:** ILO's blanket-403 episode (started cycle 396) has now run five
consecutive cycles (396, 397, 398, 399, 400) without a single healthy
reading in between. Every prior occurrence on record (244, 251, 260, 264,
272, 281, 321, 382) resolved within one or two cycles at most (321 was the
previous worst, at two cycles). This is now three cycles past that
precedent and is new territory for this pattern -- either the provider is
in a longer-than-usual outage or something about the block has changed
(e.g. it stopped being a transient rate-limit and become a standing IP or
UA block). ESTAT's timeout was a one-cycle flap, its twenty-second episode
overall, consistent with the long-running architectural pattern (the
gateway's own 60s call deadline firing under a slow response).

**Recommended action:** ILO -- keep watching; if this reaches roughly
24h/12 cycles without a healthy reading, treat it as a standing block
rather than an outage and consider whether the gateway's ILO request
pattern (headers, rate) needs review, which is beyond what this read-only
routine can act on. No action for ESTAT.

**Could not determine:** why this ILO occurrence is running longer than
every prior one -- whether it is a longer provider-side outage, a rate
limit that escalated to a longer ban, or an IP/UA block on the monitor's
egress. Nothing in the response body distinguishes these; the next run
will show whether it has cleared.

## 2026-08-27T06:43Z - cycle 397

**Changed:** ILO `healthy` -> `provider_down` (cycle 396, still open at cycle
397). ESTAT `healthy` -> `gateway_issue` (cycle 395) -> `healthy` (396) ->
`gateway_issue` (397, still open).

**Cycle saw (ILO):** gateway metadata `Client error '403 Forbidden'` on
`dataflow/ILO/all/latest`, gateway data probe HTTP 403, and all three direct
checks (metadata, data, json) also HTTP 403. Blanket failure across gateway
and direct paths alike.

**Live recheck (ILO):** re-ran `GET https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`
directly just now -- still HTTP 403. Confirmed live, not a stale cycle
artifact.

**Cycle saw (ESTAT):** `gateway metadata: tool call list_dataflows timed out
after 60.0s`, direct path healthy throughout.

**Live recheck (ESTAT):** re-ran the direct `dataflow/ESTAT` listing just
now -- HTTP 200 in 32.9s, under the 60s deadline. Consistent with the known
pattern of this being the gateway's own call deadline firing under a slow
response, not a provider outage.

**Classification:** ILO is `provider_down` (both gateway and direct 403;
their problem, nothing to fix in our code). ESTAT is `gateway_issue` (direct
path fine, only the gateway's `list_dataflows` tool call timed out; on our
side, but the known architectural pattern, not a new bug).

**History:** ILO blanket-403 provider_down is now on its ninth on-record
occurrence (priors: 244, 251, 260, 264, 272, 281, 321, 382, all but 321
resolved within a single cycle; 321 took two cycles). This occurrence has
now spanned two cycles (396, 397) without resolving, matching the 321
precedent rather than the more common one-cycle recovery -- worth watching
for a third cycle. ESTAT's `list_dataflows` timeout is its twenty-second
episode overall (prior: cycle 395 resolved by 396), consistent with the
long-running flapping pattern already on record; all ten other endpoints
stayed healthy through both cycle 395 and 397, ruling out the routine's own
network.

**Recommended action:** no action for ILO (provider-side, nothing to fix in
our code); watch the next cycle for resolution. No new action for ESTAT
beyond the standing recommendation already on record (raise the call
deadline, stream the listing, or cache the parsed result in
`monitor/checks_gateway.py`), still a code-change-scope fix this read-only
routine cannot act on.

**Could not determine:** whether ILO's two-cycle persistence this time
signals something worse than the usual one-cycle blip, since it was still
failing live at the moment of this recheck; the next run will show whether
it clears within the 321-style two-cycle window or goes longer.

## 2026-08-26T18:43Z - cycle 391

**Changed:** ABS and ESTAT both `healthy` -> `gateway_issue` (cycle 389) ->
`healthy` (390, 391)

**Cycle saw:** at cycle 389 both ABS and ESTAT failed with the identical
failing check: `gateway metadata: tool call list_dataflows timed out after
60.0s`. All ten other endpoints (BIS, ECB, FBOS, ILO, IMF, OECD, SBS, SPC,
STATSNZ, UNICEF) stayed healthy through the same cycle, so this was not the
monitor's own network being unreachable.

**Live recheck:** not performed against the providers directly, since the
monitor's own history already shows recovery two cycles ago (390 and 391
both report all-healthy for both endpoints); no live evidence remains to
recheck against for a timeout that resolved on its own within one cycle.

**Classification:** `gateway_issue` for both -- the direct paths stayed
healthy, only the gateway's `list_dataflows` tool call timed out. This is
the gateway/monitor side, not a provider fault.

**History:** this is the second time ABS and ESTAT have shown `gateway_issue`
in the exact same cycle (first was cycle 319, see prior open item). Unlike
cycle 319, this occurrence carries new information: both endpoints failed
with the byte-identical failure message and the same 60-second deadline,
which points more toward a shared cause (the gateway or its host under load
for that cycle window) than coincidence. ESTAT alone has a long-standing
history of solo `list_dataflows` timeouts (nineteen prior episodes); this is
the first time that exact failure text has appeared on ABS, and the first
time it has appeared on two endpoints in the same cycle with matching text.

**Recommended action:** treat the standing recommendation for the ESTAT
timeout pattern (raise the call deadline, stream the listing, or cache the
parsed result in `monitor/checks_gateway.py`) as now also relevant to
whatever produced the ABS timeout at the same moment. Still a code-change,
not something this read-only routine can act on.

**Could not determine:** whether the shared cause is the gateway process
itself being under load, an upstream network blip affecting the monitor's
outbound calls to both providers' underlying `list_dataflows` handlers at
once, or purely a coincidence of two independent slow responses landing in
the same two-hour window. A third same-cycle occurrence, especially with
matching failure text again, would make the shared-cause reading much
stronger.

## 2026-08-26T06:45Z - cycle 385

**Changed:** ILO `provider_down` (cycle 382, open at last run) -> `healthy`
(383) -> `gateway_issue` (384) -> `healthy` (385)

**Cycle saw:** cycle 383 showed all checks passing (the blanket-403 episode
from cycle 382 cleared). Cycle 384 then showed gateway metadata HTTP 403
Forbidden and gateway data probe HTTP 403 from provider, with the direct
path healthy throughout -- a `gateway_issue` shape, not a repeat of the
provider_down episode. Cycle 385 shows all checks passing again.

**Live recheck:** a direct request to
`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest` at the time of this run
returned HTTP 200. Direct requests to ECB and OECD control endpoints at the
same time also answered without error, so this routine's own network is not
a factor.

**Classification:** cycle 384 is `gateway_issue` (direct path succeeded,
gateway paths failed) -- ours, not theirs, but only while it lasted.

**History:** the cycle 382 blanket-403 (`provider_down`) episode reported
last run closed within one cycle, as all but one prior occurrence has. The
cycle 384 shape is new: both gateway metadata and gateway data 403 together,
with a fully healthy cycle (383) in between it and the provider_down episode
rather than being a direct tail of it. This does not match either previously
catalogued ILO gateway pattern (the gateway-data-only 403 flap at cycles 322
and 370, which leaves metadata passing; or the gateway-metadata-500 episode
at cycles 268-269, which is HTTP 500 not 403 and leaves data passing).
Treating it as a new, third gateway-side ILO shape to watch separately.

**Recommended action:** watch for a second occurrence of this specific
combination (gateway metadata + gateway data both 403, direct healthy, with
a healthy cycle preceding it). No code action indicated from a single
occurrence that resolved within one cycle.

**Could not determine:** whether cycle 384 was a fresh provider-side event
or a delayed tail effect of the cycle 382 episode that happened to skip
cycle 383. No other endpoint or contract changed in this window (382-385);
STATSNZ `auth:listing` remains `capability_appeared`, unchanged since cycle
60, not re-reported.

## 2026-08-26T00:42Z - cycle 382

**Changed:** ILO `healthy` -> `provider_down` (new this cycle, not yet resolved
as of this run)

**Cycle saw:** gateway metadata: HTTP 403 Forbidden from provider; gateway
data: probe status error, HTTP 403 from provider; direct metadata: HTTP 403.

**Live recheck:** a direct request to
`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest` at the time of this run
returned HTTP 403, the same shape the cycle saw. Direct requests to ECB and
OECD at the same time both returned 200, ruling out this routine's own
network as the cause.

**Classification:** `provider_down` (metadata failing on both the gateway and
direct paths) -- theirs, nothing to fix in our code.

**History:** new as of cycle 382; ILO was healthy across cycles 375-381 (the
last state file, cycle 379, same branch, also recorded it healthy). This is
the eighth occurrence of the recurring ILO blanket-403 pattern (priors:
cycles 244, 251, 260, 264, 272, 281, 321; all but 321 resolved within a
single cycle, 321 took two cycles with a gateway-data-only tail at 322).

**Recommended action:** none beyond watching; every prior occurrence has
cleared on its own within one to two cycles. Re-check next run; escalate only
if this persists past cycle 383 or the recovery is staggered like cycle
321-322 was.

**Could not determine:** whether provider-side rate limiting or an IP block
is the cause; this routine has no visibility into ILO's infrastructure.

**Changed:** ABS `healthy` -> `gateway_issue` -> `healthy`, already resolved
by the time of this run

**Cycle saw (381):** gateway metadata failing with `Error:` (empty error
body), the recurring empty-error-body shape. All other ABS checks were
passing.

**Live recheck:** `/api/status` at cycle 382 shows ABS fully healthy across
all checks; the recovery had already held for one full cycle before this run
started.

**Classification:** `gateway_issue` (gateway-side metadata call failing) --
ours, matching the long-standing ABS empty-error-body flap. This is the
eleventh occurrence (the most recent prior was cycle 365, resolved by 366;
all ten prior occurrences resolved within one cycle).

**History:** resolved within a single cycle, consistent with every prior
occurrence. The underlying empty error message is still worth fixing under
code-change scope (`GatewayError`/`next_step` in `monitor/checks_gateway.py`),
not actioned by this read-only routine.

**Recommended action:** none; close this watch item. Continue watching for a
twelfth occurrence.

**Could not determine:** the underlying cause of the empty error body itself;
this remains the standing code-change-scope item, not a new investigation.

## 2026-08-25T12:44Z - cycle 376

**Changed:** ESTAT `healthy` -> `gateway_issue` -> `healthy`, already resolved
by the time of this run. Last state file (cycle 373, same branch) recorded
ESTAT healthy; history shows the change appeared at cycle 374 and had
already cleared by cycle 375, clean through cycle 376 (current). No other
endpoint changed status. `changes` array on `/api/contracts` is empty; the
only non-`ok` verdict in the current matrix is STATSNZ `auth:listing`
`capability_appeared`, which has been open and unchanged since cycle 60 and
is not re-reportable.

**Cycle saw (374):** gateway metadata failing with `tool call list_dataflows
timed out after 60.0s`. All other ESTAT checks (gateway data, direct
metadata, direct data, direct json) were not listed as failing.

**Live recheck:** cycle 376 finished at 12:02:38Z; this run started at
12:44Z, about 41 minutes later. `/api/status` shows ESTAT fully healthy
across all checks at cycle 376, and it had already recovered one full cycle
earlier (375, started 10:01:22Z). No separate direct-provider probe was
needed given two consecutive clean cycles plus this near-live status read.

**Classification:** `gateway_issue` (this routine's own 60s call deadline on
`list_dataflows` firing under provider load), matching the long-documented
ESTAT `list_dataflows` timeout pattern -- working as designed, not a gateway
bug. This is the nineteenth episode on record.

**History:** resolved within a single cycle, consistent with seventeen of
the eighteen prior episodes (the eighteenth, cycles 362-364, was the only
one to persist across three consecutive cycles before this run's last
report). No sign of multi-cycle persistence recurring this time.

**Recommended action:** none; close this watch item. Continue watching for a
twentieth episode and specifically whether multi-cycle persistence repeats.
The standing fix (raise the deadline, stream the listing, or cache the
parsed result) remains open as a code-change-scope fix, not actioned by
this read-only routine.

**Could not determine:** nothing outstanding for this entry; the resolution
is corroborated by two consecutive clean cycles and a near-live status
check.

## 2026-08-25T06:43Z - cycle 373

**Changed:** ILO `gateway_issue` -> `healthy`, resolved (already resolved by the
time of this run). Last state file (cycle 370, same branch) recorded ILO
`gateway_issue`; history shows the recovery landed at cycle 371 and has held
clean through cycles 372 and 373. No other endpoint changed status and no
non-cosmetic contract changed (`changes` array holds only the known
Content-Type parameter-order flap on ILO's `encoding:structure_xml`,
verdict stayed `ok`, not re-reportable per standing guidance).

**Cycle saw (371-373):** all five ILO checks passing (gateway metadata,
gateway data, direct metadata, direct data, direct json).

**Live recheck:** cycle 373 finished at 06:02:36Z; this run started at
06:43Z, about 40 minutes later, and `/api/status` still shows ILO fully
healthy across all checks, so the recovery has held for roughly 41 minutes
past the cycle that confirmed it. No separate direct-provider probe was
needed given three consecutive clean cycles plus this near-live status read.

**Classification:** confirms the prediction filed in the prior run's open
item -- this was the second true standalone occurrence of the
gateway-data-only 403 shape (first was cycle 370 itself; the only earlier
occurrence, cycle 322, was the tail of the cycle 321 blanket-403 episode).
It resolved within a single cycle, same as every ILO gateway_issue flap on
record. No second occurrence yet of a gateway-data-only 403 persisting past
one cycle.

**History:** ILO gateway_issue (gateway-data-only 403 flap, standalone
shape): now two occurrences, cycles 370 and (tail-end context) 322, both
resolved within one to two cycles. STATSNZ `auth:listing`
`capability_appeared` is still present at cycle 373 (first seen cycle 60);
not re-reported since it did not appear as a was/now entry in this cycle's
`changes` array.

**Recommended action:** none; close this watch item. If the gateway-data-only
403 shape recurs a third time, or persists past one cycle, that would be new
and worth deeper investigation into `probe_data_url` in
`monitor/checks_gateway.py`.

**Could not determine:** nothing outstanding for this entry; the resolution
is corroborated by three consecutive clean cycles and a near-live status
check.

## 2026-08-25T00:43Z - cycle 370

**Changed:** ILO `healthy` -> `gateway_issue`, new as of cycle 370 (current).
Last state file (cycle 367, same branch) recorded ILO healthy; history shows
it stayed clean through cycles 368 and 369, with the change appearing only at
370. No other endpoint changed status and no contract changed (`changes`
array empty at cycle 370).

**Cycle saw:** gateway data check failing with `probe status: error; HTTP 403
from provider.`. Gateway metadata (`list_dataflows`) and all three direct
checks (metadata, data, json) passed. `contracts.broken` empty; the only
`references:contentconstraint` row reads `ignored`, the known architectural
baseline for ILO, not a change.

**Live recheck:** direct `dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`
returned HTTP 200 in 0.9s. The exact URL the gateway's data probe uses,
`data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`, also
returned HTTP 200, in 1.6s. A control request to ECB's direct dataflow
endpoint returned HTTP 200 too, ruling out a network-wide issue on this
routine's side. The live recheck disagrees with the cycle's failing gateway
probe: transient.

**Classification:** `gateway_issue` (direct path OK, gateway path failing) --
nominally ours, but the live recheck already resolves clean and the failure
sits entirely inside the provider round-trip (`probe_data_url` calling the
same pinned data URL that just answered 200 directly), not in any gateway
logic that transforms the request. No code site to point at from this
evidence alone.

**History:** this specific shape (gateway data check alone failing with
HTTP 403, gateway metadata and all direct checks healthy) has one prior
occurrence on record, at cycle 322, but that one was the tail end of a
blanket-403 `provider_down` episode (cycle 321) recovering in two stages
rather than one. This is the first time it has appeared standalone, with no
preceding blanket-403 cycle. ILO has a long history of separate 403-flavored
flaps (blanket `provider_down`, direct-only 403, this gateway-data-only 403);
each has resolved within one or two cycles every time it has been seen.

**Recommended action:** watch cycle 371. If it clears, file this as an
eighth blanket-403-adjacent flap variant and keep watching for a second
standalone occurrence. If it persists past cycle 371 with direct still
healthy, that would be new: no prior gateway-data-only 403 has lasted beyond
the two-cycle tail seen at 322-323, and none has occurred without a
preceding blanket-403 cycle.

**Could not determine:** why the gateway's outbound request to the exact
same URL failed at 403 while this routine's direct request to that URL
succeeded one cycle later. Could be an IP-specific block on the gateway's
egress that this routine's network cannot reproduce, or the failure could
simply have already cleared by the time of this recheck, roughly 40 minutes
after the cycle ran. No corroborating evidence to choose between them.

## 2026-08-24T18:46Z - cycle 367

**Changed (1 of 3):** ESTAT `gateway_issue` (at cycle 364, matches last state
file) -> `healthy` at cycle 365, clean through 366 and 367 (current).
Resolved.

**Cycle saw:** `gateway metadata: tool call list_dataflows timed out after
60.0s` through cycle 364, then no failing checks from 365 onward.
**Live recheck:** not needed; the monitor's own cycle 365-367 checks already
show three consecutive clean cycles (6 hours), and this is a resolution, not
an open failure.
**Classification:** `gateway_issue` resolved back to `healthy`. Same known
`list_dataflows` deadline pattern as the prior seventeen episodes.
**History:** eighteenth episode of the ESTAT `list_dataflows` timeout
pattern, previously flagged as notable for spanning cycles 362-364 (three
cycles, ~6 hours) versus one cycle for all seventeen priors. Now resolved,
having taken longer than any prior episode but recovering on its own
without intervention. Standing recommendation (raise deadline, stream
listing, or cache parsed result) remains open as a code-change-scope fix,
not actioned by this read-only routine.
**Could not determine:** whether the longer persistence this time reflects
a genuinely slower ESTAT deploy during that window or is within normal
variance for a provider whose `list_dataflows` response is already close
to the 60s deadline.

**Changed (2 of 3):** ABS `healthy` -> `gateway_issue` at cycle 365 ->
`healthy` at cycle 366, clean through 367 (current). Flapped between runs,
already resolved.
**Cycle saw:** `gateway metadata: Error:` (empty error body), the same
shape as all nine prior occurrences.
**Live recheck:** not applicable; resolved one cycle later per the
monitor's own data.
**Classification:** `gateway_issue` per `monitor/derive.py` (direct path
healthy throughout, gateway metadata failing). Matches the known ABS
empty-error-body flap.
**History:** tenth occurrence of this pattern (ninth was cycle 321,
resolved by 322). All ten occurrences have now resolved within one cycle.
The empty error message itself is still worth fixing under code-change
scope (`GatewayError`/`next_step` in `monitor/checks_gateway.py`), not
actioned here.

**Changed (3 of 3):** SBS `healthy` -> `gateway_issue` at cycle 366 ->
`healthy` at cycle 367 (current). Flapped between runs, already resolved.
Isolated to SBS: all other eleven endpoints were healthy at cycle 366.
**Cycle saw:** `gateway metadata: Error: [Errno -3] Temporary failure in
name resolution` and `gateway data: tool call probe_data_url timed out
after 60.0s`.
**Live recheck:** a plain request to `data-sdmx-disseminate.sbs.gov.ws`
from this session resolved and connected normally, consistent with the
monitor's own recovery at cycle 367.
**Classification:** `gateway_issue` per `monitor/derive.py`, but the
failure text is a DNS resolution error on the gateway's outbound call, the
same shape previously seen for FBOS at cycle 50 and treated there as
infrastructure rather than the provider being down. Since only SBS failed
this cycle while the other eleven endpoints resolved fine, this looks like
a transient DNS blip local to that one lookup rather than a network-wide
problem on the monitor's side.
**History:** first occurrence of this specific pattern for SBS. Watch for
recurrence; a second occurrence would be worth escalating as a possible
DNS caching or retry gap in the gateway's outbound calls.

**Contract changes:** one `changes` entry this cycle (ABS,
`encoding:structure_xml`, Content-Type parameter order flip), verdict
`ok`. Known cosmetic flap already on file, not re-reported as its own
item. One `capability_appeared` row (STATSNZ `auth:listing`), matching the
standing open item since cycle 60; not a new change.

**Recommended action:** none from this read-only routine on any of the
three. All three are resolved as of the current cycle.

## 2026-08-24T12:45Z - cycle 364

**Changed:** ESTAT `healthy` (at cycle 361, matches last state file, same
branch) -> `gateway_issue` at cycle 362, still `gateway_issue` at 363 and
364 (current). Ongoing, not yet resolved.

**Cycle saw (all of 362, 363, 364):** `gateway metadata: tool call
list_dataflows timed out after 60.0s`. Gateway data, direct metadata, and
direct data all passing throughout; direct json stays skipped as usual
(ESTAT does not serve SDMx-JSON).

**Live recheck:** direct `dataflow/ESTAT/all/latest` against
`ec.europa.eu` returned `HTTP 200` in 30.9s (37 MB payload), comfortably
under the 60s gateway deadline but well above ESTAT's usual response time.
Control request to ECB's direct dataflow endpoint returned `HTTP 200` in
1.0s, ruling out a network-wide issue on this routine's side.

**Classification:** `gateway_issue` per `monitor/derive.py` (direct path
healthy, gateway path timed out) -- ours in the sense that the gateway call
deadline is our own budget, tripped by provider-side slowness. Same known
`list_dataflows` deadline pattern as the prior seventeen episodes.

**History:** eighteenth episode of the ESTAT `list_dataflows` timeout
pattern. Notable departure from all seventeen priors: every previous
episode resolved within one cycle (2 hours); this one has now held for
three consecutive cycles (362, 363, 364), roughly 6 hours as of this run.
Still active at time of writing.

**Contract changes:** one `changes` entry this cycle (ABS,
`encoding:structure_xml`, Content-Type parameter order flip), verdict
`ok`. Known cosmetic flap already on file; not re-reported as its own
item.

**Recommended action:** none from this read-only routine. Standing
recommendation to raise the `list_dataflows` deadline, stream the listing,
or cache the parsed result remains open as a code-change-scope fix. Given
this episode's unusual persistence, the next run should check whether it
has resolved or is still ongoing; if still ongoing at the next scheduled
run (6+ hours further, ~4 cycles total), that would be a further escalation
worth calling out explicitly.

**Could not determine:** whether this cycle's outcome will be recorded as
resolved by the time this run's PR merges (the routine observed the state
as of 12:01-12:03 UTC cycle 364 and the live recheck a few minutes after);
whether the extended duration reflects a genuinely slower ESTAT deploy or
is coincidental variance within the existing pattern.

## 2026-08-23T00:42Z - cycle 346

**Changed:** UNICEF `healthy` (at cycle 343, matches last state file, same
branch, clean through 345) -> `degraded` at cycle 346 (current). New this
run, not seen flapping in the intervening cycles (344, 345 both healthy).

**Cycle saw:** gateway data: `probe status: error; HTTP 429 from provider`;
direct data: `HTTP 429`; direct json: `HTTP 429`. Metadata (both paths)
still passing. All 14 contract assertions still read `ok`, no entries in
`/api/contracts` `changes`.

**Live recheck:** direct `data/UNICEF,CME,1.0/all` against
`sdmx.data.unicef.org` returned `HTTP 200` on two successive tries, about
41 minutes after the cycle ran. Control request to ECB's direct data
endpoint also returned `HTTP 200`, ruling out a network-wide issue on this
routine's side.

**Classification:** `degraded` per `monitor/derive.py` (gateway and direct
paths both hit the same HTTP 429, metadata unaffected) -- provider-side
rate limiting, not a gateway bug. Already resolved by the time of the live
recheck.

**History:** fifth occurrence of the UNICEF HTTP 429 flap (priors: cycle
126, 178, 262, 305), all four priors resolved within one cycle. This one
also shows resolved already, ahead of the next scheduled cycle.

**Recommended action:** none. Watch for a sixth occurrence.

**Could not determine:** nothing outstanding; live recheck directly
corroborates recovery.

## 2026-08-21T12:43Z - cycle 328

**Changed:** ESTAT `healthy` (at cycle 325, matches last state file, same
branch) -> `gateway_issue` at cycle 326 -> `healthy` at 327, and clean
through 328 (current). Failed and recovered between runs; reporting once,
already resolved.

**Cycle saw (ESTAT, cycle 326):** `gateway metadata: tool call
list_dataflows timed out after 60.0s`. No other checks failing.

**Live recheck:** direct `dataflow/ESTAT/all/latest` against
`ec.europa.eu` returned `HTTP 200` in 28.5s, comfortably under the 60s
gateway deadline but well above ESTAT's usual response time. Control
request to ECB's direct dataflow endpoint returned `HTTP 200` in 1.2s,
ruling out a network-wide issue on this routine's side.

**Classification:** `gateway_issue` per `monitor/derive.py` (direct path
healthy, gateway path timed out) -- ours in the sense that the gateway call
deadline is our own budget, tripped by provider-side slowness. Not a bug to
fix; this is the known `list_dataflows` deadline pattern.

**History:** seventeenth episode of the ESTAT `list_dataflows` timeout
pattern (prior sixteen all resolved within one cycle; this one also
resolved within one cycle, healthy again by 327). No other endpoint
changed status in this window, and no `severe conditions` (`stale`,
`gateway_up`) were present at any point.

**Contract changes:** one `changes` entry this cycle (ABS,
`encoding:structure_xml`, Content-Type parameter order flip between
`version=2.1; charset=utf-8` and `charset=utf-8; version=2.1`), verdict
`ok`. This is the known cosmetic flap already on file; not re-reported as
its own item per the standing note. `STATSNZ auth:listing` still reads
`capability_appeared` in the current matrix but is unchanged from the last
run and does not appear in this cycle's `changes` list, so it is not new;
already tracked as an open item.

**Recommended action:** none. Standing recommendation to raise the
`list_dataflows` deadline, stream the listing, or cache the parsed result
remains open as a code-change-scope fix, not actioned by this read-only
routine.

**Could not determine:** nothing outstanding for this episode; the
provider-side latency spike and gateway-side timeout are directly
corroborated by the live recheck.

## 2026-08-21T06:43Z - cycle 325

**Changed:** one endpoint, comparing the last state file (cycle 322, same
branch) against the current cycle (325), with cycles 323-324 checked in
between.

- ILO: `gateway_issue` (at cycle 322, matches last state, gateway data probe
  still returning HTTP 403) -> `healthy` at cycle 323, and clean through 324
  and 325 (current). This closes the open item carried in the last state
  file, which asked to watch cycle 323 specifically.

**Cycle saw (ILO, cycle 323):** all five checks passing, per `/api/history`.
No `failing` entries logged for ILO at 323, 324, or 325.

**Live recheck (ILO, now):** direct `dataflow/ILO/all/latest` against
`sdmx.ilo.org` returned `HTTP 200`. A control request to ECB's direct
dataflow endpoint also returned `HTTP 200`, ruling out a network-wide issue
on this routine's side.

**Classification:** provider-side transient block (`provider_down` at 321,
theirs), fully resolved. The gateway-only tail at cycle 322 was `gateway_issue`
in the narrow sense of "gateway path still blocked after direct recovered,"
not a bug in our code -- it cleared on its own without a code change.

**History:** this is the resolution of the seventh blanket-403
`provider_down` occurrence (cycle 321, priors: 244, 251, 260, 264, 272,
281). It took two cycles to fully clear (322 partial, 323 full) instead of
the usual one -- the first time that has happened in this log, as flagged
in the last run's open items. No recurrence since; treating the "does a
staggered recovery repeat" question as answered no for now, but keeping the
underlying blanket-403 pattern itself on watch for an eighth occurrence.

**Contract changes:** two `changes` entries this cycle (ABS and ILO,
`encoding:structure_xml`, Content-Type parameter order flip between
`version=2.1; charset=utf-8` and `charset=utf-8; version=2.1`), verdict
`ok` both times. This is the known cosmetic flap already on file; not
re-reported as its own item per the standing note (verdict unchanged).

**Recommended action:** none. Close the ILO watch item from last run.

**Could not determine:** whether the two-cycle recovery shape reflects
something specific about this occurrence (e.g. an IP-level block on the
gateway's egress that needed a second retry cycle to clear) or is
coincidental variance within the existing pattern. No corroborating
evidence either way from this vantage point.

## 2026-08-21T00:43Z - cycle 322

**Changed:** three endpoints, comparing the last state file (cycle 319, same
branch) against the current cycle (322), with cycles 320-321 checked in
between.

- ABS: `gateway_issue` (at cycle 319, matches last state) -> `healthy` at 320
  -> `gateway_issue` again at 321 -> `healthy` at 322 (current). Flapped
  twice and is healthy now.
- ESTAT: `gateway_issue` (at cycle 319, matches last state) -> `healthy` from
  cycle 320 onward. Recovered and stayed healthy; this closes the "sixteenth
  episode, open as of cycle 319" item carried in the last state file.
- ILO: `healthy` through cycle 320 -> `provider_down` (blanket 403 on every
  path) at cycle 321 -> `gateway_issue` at cycle 322 (current), with only
  the gateway data probe still failing. This is new and still open as of
  this run.

**Cycle saw (ABS, cycle 321):** `gateway metadata: Error: ` (empty error
body), same shape as the cycle 319 occurrence and all priors. Direct and
gateway-data checks passing throughout.

**Cycle saw (ILO, cycle 321):** all five checks failing --
`gateway metadata: ... 403 Forbidden ...`, `gateway data: probe status:
error; HTTP 403 from provider.`, `direct metadata: HTTP 403`, `direct data:
HTTP 403`, `direct json: HTTP 403`. Cycle 322: only `gateway data: probe
status: error; HTTP 403 from provider.` remains; gateway metadata and all
three direct checks are back to passing.

**Live recheck (ILO, now):** direct `dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`
against `sdmx.ilo.org` returned `HTTP 200` in 0.8s. Direct
`data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1` (the
same URL the gateway's data probe uses) returned `HTTP 200` in 2.0s. The
provider answers cleanly from this routine's network right now, disagreeing
with the still-failing gateway data probe as of cycle 322 -- consistent with
either a residual, gateway-IP-specific block left over from the cycle 321
outage, or the gateway probe simply not having run again since cycle 322
started.

**Classification:** ABS `gateway_issue` (ours, in the empty-error-body sense
already on file) both times. ILO `provider_down` at 321 (theirs; nothing to
fix in our code), moving to `gateway_issue` at 322 (direct path recovered,
gateway path still failing on the data check specifically -- notable
because prior blanket-403 episodes always resolved cleanly to `healthy` in
one jump, not a staggered recovery like this).

**History:** ABS empty-error-body flap, ninth occurrence (eighth was cycle
319, itself resolved by 320; this is a second flap four hours later, also
resolved within one cycle). ESTAT timeout, confirms the sixteenth episode
closed clean at cycle 320 as expected from all fifteen priors. ILO
blanket-403 `provider_down`, seventh occurrence (priors: 244, 251, 260, 264,
272, 281, all resolved within one cycle) -- but this is the first time the
very next cycle did not come back fully healthy; instead the gateway data
check alone is still open one cycle later. That partial-recovery shape has
no precedent in this log.

**Recommended action:** watch cycle 323 for the gateway data probe to clear.
If it does, this reads as a slightly longer tail on an already-known
pattern. If it is still failing at 323 while direct stays healthy, treat it
as a second occurrence of a genuine `gateway_issue` (distinct from the
cycles 268-269 metadata-500 pattern, since this one is the data check and a
403, not a 500) and escalate per that pattern's standing "spans more than
one cycle" trigger.

**Could not determine:** whether the still-failing ILO gateway data probe
reflects a block on the gateway's own outbound IP that this routine's
network cannot reproduce (this routine's direct recheck succeeded, but it
runs from a different network path than the gateway), or simply that the
gateway has not re-probed since cycle 322 began and would already show
healthy if polled again right now.

## 2026-08-20T18:43Z - cycle 319

**Changed:** ABS `healthy` -> `gateway_issue` and ESTAT `healthy` ->
`gateway_issue`, both new as of this cycle. The prior state file (cycle 316,
this same branch) recorded both as healthy; history shows both clean through
cycle 318 and both failing starting at cycle 319, the current cycle. No
intermediate cycles to check since the last run was the immediately
preceding one.

**Cycle saw (ABS):** `gateway metadata: Error: ` (empty error body), direct
metadata/data/json and gateway data all passing.

**Cycle saw (ESTAT):** `gateway metadata: tool call list_dataflows timed out
after 60.0s`, direct metadata/data/json and gateway data all passing.

**Live recheck:** direct `dataflow/ABS/all/latest` against
`data.api.abs.gov.au` returned `HTTP 200` in 35.2s. Direct
`dataflow/ESTAT/all/latest` against `ec.europa.eu` returned `HTTP 200` in
28.7s. Both comfortably above their usual latency and close to or past the
60s gateway call deadline, consistent with provider-side slowness rather
than an outage. Checked ECB and BIS directly at the same time as a network
sanity check; both answered quickly, so the slowness is specific to ABS and
ESTAT, not this routine's network path.

**Classification:** both `gateway_issue` per `monitor/derive.py` (direct
path healthy, gateway path failing -- ours in the sense that gateway call
deadlines are our own budget, tripped by provider-side slowness).

**History:** ABS empty-error-body gateway metadata flap, eighth occurrence
(prior seven: most recently cycle 295, resolved by 296; before that cycle
206 and earlier). Always resolved within one cycle. ESTAT `list_dataflows`
timeout, sixteenth episode (prior fifteenth: cycle 312, resolved by 313).
Also always resolved within one cycle. Both endpoints failing in the same
cycle is new -- no prior entry shows them coinciding -- but each pattern
individually is well established and neither implicates the other.

**Recommended action:** none beyond watching the next cycle for recovery,
consistent with sixteen and eight priors respectively. Escalate for real if
either fails to clear by cycle 320, or if this becomes a pattern of the two
coinciding.

**Could not determine:** whether the same-cycle coincidence reflects a
shared upstream cause (e.g. broad EU/AU network congestion at this hour) or
is pure chance given how frequently each pattern occurs independently.

## 2026-08-20T06:43Z - cycle 313

**Changed:** ESTAT `healthy` -> `gateway_issue` -> `healthy`, flapped and
recovered between runs. The last state file (written at cycle 310) recorded
ESTAT as healthy; history shows it dropped at cycle 312 and was back to
healthy by cycle 313, the current cycle.

**Cycle saw:** cycle 312 failing array: `gateway metadata: tool call
list_dataflows timed out after 60.0s`. Direct metadata/data/json and gateway
data were not in the failing list, so only the gateway `list_dataflows` call
tripped the 60s deadline. Cycle 313: `failing: []`, all checks passing.

**Live recheck:** performed now, direct against `ec.europa.eu`. Requested
`dataflow/ESTAT/all/latest` and got `HTTP 200` in 31.1s, comfortably inside
the 60s deadline but close enough to it to confirm the provider is still
running slow under load rather than genuinely down.

**Classification:** was `gateway_issue` per `monitor/derive.py` while open
(direct path not in the failing list, gateway `list_dataflows` timed out --
ours, in the sense that the 60s deadline is our own call budget firing under
provider-side slowness). No longer applicable; resolved.

**History:** fifteenth episode of this exact ESTAT `list_dataflows` timeout
pattern (open_items on file: fourteenth episode was cycles 291-292, resolved
by 293). Like all fourteen priors, this one resolved by the very next cycle.
The standing recommendation (raise the deadline, stream the listing, or
cache the parsed result) remains an open code-change-scope item, not
actioned by this read-only routine.

**Recommended action:** none beyond continuing to watch; still resolving
within one cycle every time. Reconsider the code-change fix if a future
episode spans more than one cycle, which none of the fifteen have yet.

**Could not determine:** the exact load conditions on Eurostat's dissemination
API that push `list_dataflows` past 60s on some cycles and not others.

No other endpoint changed status (ABS, BIS, ECB, FBOS, ILO, IMF, OECD, SBS,
SPC, STATSNZ, UNICEF all healthy at cycle 310 and remain healthy at 313). No
contract assertions changed (`/api/contracts` `changes: []`, no `broken` or
`capability_appeared` verdicts). `stale: false`, `gateway_up: true`,
`drift: []` at cycle 313.

## 2026-08-19T18:42Z - cycle 307

**Changed:** UNICEF `healthy` -> `degraded` -> `healthy`, flapped and recovered
between runs. The last state file (written at cycle 304) recorded UNICEF as
healthy; history shows it dropped at cycle 305 and was back to healthy by
cycle 306, confirmed clean through the current cycle, 307.

**Cycle saw:** cycle 305 failing array: `direct data: HTTP 429`, `direct
json: HTTP 429`. Direct metadata and both gateway checks were not in the
failing list, so only the two rate-limited paths tripped. Cycle 306 onward:
`failing: []`.

**Live recheck:** performed now, ~13.5 hours after the failing cycle. UNICEF
metadata endpoint (`dataflow/UNICEF/all/latest`) returned `200`. A control
request against a different provider (ECB) also returned `200`, ruling out
a network-side cause for the recheck itself. Consistent with a transient
provider-side rate limit that has since cleared.

**Classification:** was `degraded` while open (mixed: only the direct
data/json paths failing, HTTP 429). No longer applicable; resolved.

**History:** fourth occurrence of this exact HTTP 429 pattern on UNICEF.
Priors: cycle 126, cycle 178, cycle 262 (per `open_items` in the state
file). All four resolved within one cycle of being observed, same as this
one.

**Recommended action:** none beyond continuing to watch. Four occurrences
of a single-cycle 429 that clears on its own is not yet a pattern that
needs a code change (e.g. backoff/retry on the direct data path); reconsider
if a future occurrence spans more than one cycle.

**Could not determine:** the exact request volume or timing that triggered
UNICEF's rate limit, since the monitor does not log request counts.

## 2026-08-19T00:43Z - cycle 298

**Changed:** ABS `gateway_issue` -> `healthy`, resolved by cycle 296. Recovered
between runs: the last state file (written at cycle 295) recorded ABS as
still open, but the routine that wrote it could not yet see cycle 296.

**Cycle saw:** cycle 295 failed with `gateway metadata: Error: ` (empty error
body), direct path healthy throughout, same shape as the six prior
occurrences of this pattern (most recently cycle 206). Cycle 296 onward:
`failing: []`, all checks passing. Clean through the current cycle, 298.

**Live recheck:** not performed. The failing cycle is already ~30 hours old
and three consecutive later cycles (296, 297, 298) confirm recovery, so a
live recheck would not add information the monitor's own history doesn't
already show.

**Classification:** was `gateway_issue` per `monitor/derive.py` while open
(direct path healthy, gateway path failing -- ours). No longer applicable;
resolved.

**History:** seventh occurrence of this exact empty-error-body pattern
(priors: cycle 26 and five more through cycle 206). All seven, including
this one, resolved by the cycle immediately following the one where they
were first observed -- consistent with the established pattern, not an
escalation. The previous run's note calling this "the first time it has not
resolved within the same cycle it was observed" was a snapshot taken before
cycle 296's data existed; with cycle 296 in hand, this occurrence behaved
exactly like the prior six.

**Recommended action:** none beyond the standing code-change-scope item
already on file: the empty error message (`GatewayError`/`next_step` in
`monitor/checks_gateway.py`) is itself worth fixing so a future empty-body
failure is diagnosable without waiting a cycle.

**Could not determine:** the underlying cause of the transient gateway
metadata failure at cycle 295 (no error detail beyond the empty body).

No other endpoint changed status (BIS, ECB, ESTAT, FBOS, ILO, IMF, OECD,
SBS, SPC, STATSNZ, UNICEF all healthy at cycle 295 and remain healthy at
298). No contract assertions changed (`/api/contracts` `changes: []`, no
`broken` or `capability_appeared` verdicts). `stale: false`, `gateway_up:
true` at cycle 298.

## 2026-08-18T18:44Z - cycle 295

**Changed:** OECD `gateway_issue` -> `healthy`, resolved by cycle 293 (was
open cycles 290-292, three cycles / ~6 hours). ESTAT `gateway_issue` ->
`healthy`, resolved by cycle 293 (was open cycles 291-292, two cycles).
ABS `healthy` -> `gateway_issue`, new as of cycle 295 (`gateway metadata:
Error: `, empty error body, direct path healthy). Three separate
findings, reported together since they all landed in the same poll.

**Cycle saw:**
- OECD (293-295, all healthy): no failing checks; the cycle 290-292 403
  Forbidden on gateway `list_dataflows` and gateway data probe is gone.
- ESTAT (293-295, all healthy): no failing checks; the cycle 291-292
  `list_dataflows` 60s timeout is gone.
- ABS (295, `gateway_issue`): gateway metadata check failed with
  `error: "Error: "` (empty body), 2 attempts, latency 32.7s. Gateway
  data, direct metadata, direct data, direct json all HTTP 200 /
  succeeded normally in the same cycle. No ABS contract assertion broke.

**Live recheck (18:44Z):** ABS direct metadata `GET
https://data.api.abs.gov.au/rest/dataflow/ABS/all/latest?references=none`
-> first two attempts from this session timed out at the TLS handshake
(connection hung after `CONNECT` and `Client Hello`, no response) while
ECB and BIS answered normally through the same proxy in ~1.2s each,
ruling out a session-wide network problem; a third attempt succeeded,
HTTP 200 in 1.5s with a normal SDMx-ML structure payload. Treat the
timeouts as noise on this session's path to ABS specifically, not
evidence about the provider or the gateway. Could not recheck the
gateway path itself (`sdmx-mcp-gateway-production.up.railway.app` is
outside this session's network allowlist, as in every prior run).

**Classification:** OECD and ESTAT -- both resolved, no classification
needed going forward; while open both were `gateway_issue` per
`monitor/derive.py` (direct healthy, gateway failing, ours to explain).
ABS -- `gateway_issue`, direct path healthy throughout, so this is ours,
not ABS's.

**History:**
- OECD: this closes the first-ever multi-cycle occurrence of this
  pattern for OECD (cycles 290-292, flagged in the prior run as unusual
  because it didn't clear within one cycle like every other known
  gateway/direct flap here). It did eventually self-resolve, just slower
  than the norm. Watch for a second occurrence; if a future one also
  spans more than one cycle this stops looking like a fluke.
- ESTAT: the fourteenth episode of the known `list_dataflows` 60s-timeout
  pattern (first to span two cycles, per the prior run) is now resolved.
  Fifteenth-episode watch stands; the standing recommendation (raise the
  deadline, stream the listing, or cache the parsed result) is still
  open as a code-change-scope fix.
- ABS: this is the seventh occurrence of the known gateway-metadata
  empty-error-body flap (`Error: ` with no message), first seen cycle 26,
  sixth occurrence at cycle 206 (per prior-run `open_items`), all six
  resolved within one cycle. (Separate from the one-off 502 Bad Gateway
  variant at cycle 198, a different error shape.) Cycle 296 (due
  ~20:01Z) will show whether this seventh occurrence also clears within
  one cycle.

**Recommended action:** OECD and ESTAT -- none beyond continuing to
watch; both resolved on their own as chronic patterns have before. ABS --
none yet; this is a known chronic flap that has always cleared in one
cycle. If ABS is still `gateway_issue` at cycle 296, that would be the
first time this pattern spans more than one cycle, matching the
escalation condition already used for OECD and ESTAT, and would be worth
raising as a real incident (plus still worth fixing the empty error
message itself under code-change scope, in `monitor/checks_gateway.py`).

**Could not determine:** whether OECD's and ESTAT's resolutions happened
gradually or all at once between cycles 292 and 293, since this run
skipped straight from 292 to 295 (the routine runs every ~6 hours, cycles
are 2 hours apart, so intermediate cycle 293-294 detail beyond `series`
status is not available). Also could not determine the actual cause of
ABS's empty gateway error, same as every prior occurrence of this
pattern -- the gateway's own logs are not visible from here.

## 2026-08-18T12:45Z - cycle 292

**Changed:** OECD `healthy` -> `gateway_issue` as of cycle 290, still
`gateway_issue` at cycle 292 (three consecutive cycles, about 6 hours).
ESTAT `healthy` -> `gateway_issue` at cycle 291, still `gateway_issue` at
cycle 292 (two consecutive cycles). Reported together since both are the
same shape (gateway path failing, direct path healthy), but they are two
separate findings.

**Cycle saw (292, 2026-08-18T12:01:22Z):**
- OECD: gateway metadata `list_dataflows` -> `Error: Client error '403
  Forbidden' for url 'https://sdmx.oecd.org/public/rest/dataflow/all/all/latest'`
  (2 attempts). Gateway data probe also failed with the same HTTP 403.
  Direct metadata, direct data, and direct json all HTTP 200. No OECD
  contract assertion broke.
- ESTAT: gateway metadata `list_dataflows` -> `tool call list_dataflows
  timed out after 60.0s` (2 attempts). Gateway data, direct metadata,
  direct data all healthy; direct json is permanently skipped by design
  (Eurostat has no SDMx-JSON). No ESTAT contract assertion broke.

**Live recheck (12:45Z):**
- OECD: `GET https://sdmx.oecd.org/public/rest/dataflow/all/all/latest`
  from this session -> HTTP 200, normal SDMx-ML structure response, no
  challenge page. The exact URL the gateway's `list_dataflows` call
  failed on answers fine from here.
- ESTAT: `GET
  https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest`
  -> HTTP 200 in ~29s, well under the 60s deadline the gateway hit.
- Could not recheck the gateway path itself: the gateway's own host
  (`sdmx-mcp-gateway-production.up.railway.app`, from `monitor/main.py`
  `GATEWAY_URL`) is not on this environment's network allowlist and the
  proxy returns `403` for it. Only the monitor and the provider hosts
  listed in the skill are reachable from here.

**Classification:** `gateway_issue` per `monitor/derive.py` for both:
direct path healthy, gateway path failing, both are ours to explain, not
the providers'.

**History:**
- OECD: not in the known chronic-flap list. First occurrence found in
  `/api/history?hours=48`; no prior OECD gateway_issue in that window.
  New as of cycle 290 and has not self-resolved across cycles 290, 291,
  292 -- the known chronic gateway/direct flaps in this repo (ESTAT
  timeout, ABS empty-error) have always cleared within one cycle. This
  one has not, which is the reason to escalate rather than wait quietly.
- ESTAT: this is the fourteenth occurrence of the known `list_dataflows`
  60s-timeout pattern (prior episodes at cycles 285 and earlier per
  `triage-state.json` open items, thirteen total). Every prior episode
  resolved within one cycle. This one is still open at the next cycle
  (291 -> 292), which is new for this pattern and matches the escalation
  condition the prior run's state file called out ("escalate if a future
  episode still shows degraded/failing when the next cycle completes").

**Recommended action:** OECD -- since the direct path (same URL, from two
different networks) answers 200 with no Cloudflare challenge, the most
likely explanation is that OECD is blocking or rate-limiting the
gateway's own outbound IP (Railway) specifically, not a bug in the
request the gateway sends; worth checking the gateway's outbound
IP/User-Agent against OECD's Cloudflare rules if this persists past the
next cycle. ESTAT -- treat the same 60s-deadline fix already on record
(raise deadline, stream the listing, or cache the parsed result) as more
urgent than before, since this episode has now outlasted every prior one;
otherwise watch the next cycle before concluding it is a new, distinct
failure mode.

**Could not determine:** whether OECD's 403 is IP-based blocking, a
rate limit, or a header/User-Agent difference specific to the gateway's
MCP-driven request, since the gateway host itself is not reachable from
this session to test directly. Also could not determine whether either
episode will still be open at the next cycle, since this recheck happened
mid-episode.

## 2026-08-18T00:42Z - cycle 286

**Changed:** ILO `healthy` -> `degraded` at cycle 286 (direct metadata,
direct data both HTTP 403). ESTAT `healthy` -> `gateway_issue` at cycle
285, recovered by cycle 286. Reported together, both matching known
chronic patterns.

**Cycle saw (286, 2026-08-18T00:01:22Z):** ILO direct metadata and direct
data both HTTP 403 (2 attempts each). ILO direct json and both gateway
checks (metadata, data) stayed healthy throughout. No ILO contract
assertion broke; `references:contentconstraint` still reads `ignored` as
expected.
**Live recheck (00:42Z, ~40 min after the cycle):** direct metadata `GET
https://sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`
-> HTTP 200. direct data `GET
https://sdmx.ilo.org/rest/data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`
-> HTTP 200. Both direct paths are back up; the 403 is already gone
ahead of the monitor's next cycle (due 02:01:22Z).
**Classification:** `degraded` per `monitor/derive.py` as recorded at
cycle 286, only the direct path failing, gateway path unaffected
throughout. Live recheck shows this has already self-resolved.
**History:** this is (by count) the tenth occurrence of the ILO
direct-path 403 flap. Prior occurrences: cycle 32, 94, 159, 166, 202, 238,
244, 249, 267 (the ninth, broader, hit all three direct checks; this one
hit two). Consistent with the established pattern of ILO periodically
blocking its direct REST path (likely WAF/Cloudflare-side) and
self-clearing within a cycle or two.
**Recommended action:** none. Continue treating as chronic and
self-resolving. Escalate only if a future occurrence spans more than one
cycle when next checked, or broadens beyond the direct path to include
the gateway path.
**Could not determine:** whether this occurrence had already cleared
before cycle 286 finished recording it, since the live recheck happened
well after the cycle ran.

**ESTAT, same window:** cycle 285 (2026-08-17T22:01:22Z) showed
`gateway_issue`, `gateway metadata: tool call list_dataflows timed out
after 60.0s`. Recovered by cycle 286, healthy. This is the thirteenth
episode of the known `list_dataflows` timeout-under-load pattern (twelfth
resolved by cycle 244). No live recheck needed; the monitor's own next
cycle already shows recovery. Standing recommendation to raise the
gateway call deadline, stream the listing, or cache the parsed result
remains open as a code-change-scope fix, unchanged from prior runs.

**Also observed, not alerted:** ECB `constraint:availableconstraint` and
`references:none` contract assertions each show a `was`/`now` pair in
`/api/contracts` `changes` (504 -> 404 and 504 -> 200 respectively).
`verdict` stayed `ok` throughout for both -- this is a transient timeout
on the assertion probe itself, not a change in what the gateway can rely
on, and ECB's basic health checks stayed `healthy` across cycles 280-286
with no failing checks recorded. Not reported as an incident; noted here
only because it appeared in the same poll.

No other endpoint changed status across cycles 283-286 (the remaining ten
endpoints stayed `healthy` throughout). `STATSNZ auth:listing`
`capability_appeared` is still present (open since cycle 60, listing
served without credentials) but unchanged from the last run, so not
re-reported here.

## 2026-08-17T18:43Z - cycle 283

**Changed:** ILO `healthy` -> `provider_down` at cycle 281, recovered by
cycle 282. Reported once, already resolved.

**Cycle saw (281, 2026-08-17T14:01:22Z):** all five ILO basic checks failing
with HTTP 403 (`gateway metadata`, `gateway data`, `direct metadata`,
`direct data`, `direct json`). Classified `provider_down`. Recovered by
cycle 282 (2026-08-17T16:01:22Z), all checks healthy, and still healthy at
cycle 283 (2026-08-17T18:01:22Z).
**Live recheck (18:43Z, direct metadata `GET
https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`):** HTTP 200. Confirms
the cycle-282/283 recovery; the 403 is gone.
**Classification:** `healthy` per `monitor/derive.py`, fully recovered
after one cycle.
**History:** this is the sixth occurrence of the ILO blanket-403
`provider_down` pattern (all five checks failing with 403 simultaneously,
both gateway and direct paths). Prior occurrences: cycle 244, 251, 260,
264, 272, all resolved within one cycle. This one also resolved within one
cycle, consistent with the established pattern of ILO periodically
blocking all paths (likely WAF/Cloudflare-side) and self-clearing.
`/api/contracts` `changes` for cycle 283 is empty; no contract assertion
moved.
**Recommended action:** none. Continue treating this as a chronic,
self-resolving ILO pattern. Escalate only if a future occurrence spans
more than one cycle, or if the occurrence rate visibly increases.
**Could not determine:** nothing outstanding; the live recheck confirms
the resolution the history series already showed.

No other endpoint changed status across cycles 280-283 (all eleven other
endpoints stayed `healthy` throughout). `STATSNZ auth:listing`
`capability_appeared` is still present (open since cycle 60, listing
served without credentials) but unchanged from the last run, so not
re-reported here.

## 2026-08-17T06:52Z - cycle 277

**Changed:** ILO `degraded` -> `healthy`, `references:contentconstraint`
`broken` -> `ignored`. This closes the watch item opened at cycle 274.

**Cycle saw (277, 2026-08-17T06:01:22Z):** all five ILO basic checks
passing. `references:contentconstraint` reads `ignored` (expected 200,
observed 200, byte-identical to `references=none`), matching the
pre-existing architectural baseline. `/api/contracts` `changes` for this
cycle contains one entry, unrelated to ILO (`ABS encoding:structure_xml`
content-type parameter order, `ok` both before and after, matching the
already-documented cosmetic flap; not re-reported).
**Live recheck (06:52Z, three requests to the same
`DF_GED_XLU1_SEX_HHT_CHL_RT?references=contentconstraint&detail=allstubs`
URL used at cycle 274):** 3 of 3 returned HTTP 200, consistent with the
current `ignored` reading and with none of them reproducing the cycle-274
500.
**Classification:** `healthy` per `monitor/derive.py`. Fully recovered.
**History:** `/api/history` reports basic-check status `healthy` for
cycles 274 through 277 (the `degraded` classification at cycle 274 came
from the contract layer, which `/api/history`'s per-cycle series does not
carry). The `changes` array's silence about ILO between cycles 275-277
indicates the assertion was already back to `ignored` well before this
run, consistent with the cycle-274 report's read that this was transient
WAF/Cloudflare-side flakiness rather than a durable change. Did not persist
beyond one cycle and did not recur a third time, so per the cycle-274
watch note this does not escalate.
**Recommended action:** none. Close this watch item. Continue treating a
third occurrence of this specific assertion flapping, or a `broken` reading
that persists past one cycle, as the threshold for a real investigation.
**Could not determine:** nothing outstanding on this item.

No other endpoint changed status across cycles 275-277 (all eleven other
endpoints stayed `healthy` throughout), and no other contract assertion
changed.

## 2026-08-17T00:45Z - cycle 274

**Changed:** ILO `healthy` -> `degraded` (contract `references:contentconstraint`
`ok`/`ignored` -> `broken`). Also, between this run and the last: ILO
blanket-403 `provider_down` at cycle 272, recovered by cycle 273.

**Cycle saw (272, 2026-08-16T20:01:22Z):** all five ILO checks failing with
HTTP 403 (`gateway metadata`, `gateway data`, `direct metadata`, `direct
data`, `direct json`). Classified `provider_down`. Recovered by cycle 273
(2026-08-16T22:01:22Z), all checks healthy.
**Cycle saw (274, 2026-08-17T00:01:22Z):** all five basic checks passing.
The `references:contentconstraint` contract assertion, which has read
`ignored` (accepted 200, payload byte-identical to `references=none`) for
every prior cycle, this time got `expected 200, observed 500` and was
classified `broken`. This is the sole entry in `/api/contracts` `changes`
for this cycle; nothing else in the matrix moved.
**Live recheck (00:44Z, five requests to
`https://sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest?references=contentconstraint&detail=allstubs`):**
4 of 5 returned HTTP 200 with a 1973-byte body byte-identical (apart from
the message ID and Prepared timestamp) to a `references=none` fetch taken
in the same window, i.e. `ignored`, matching the documented baseline. 1 of 5
returned HTTP 403. None reproduced the 500 the monitor saw at cycle 274.
**Classification:** `degraded` per `monitor/derive.py` (contract broken,
basic checks all passing). The live recheck disagrees with the cycle-274
observation, which points to transient flakiness on ILO's side rather than
a durable behavior change, consistent with ILO's already-documented
Cloudflare/WAF flakiness (blanket-403 episodes, direct-path 403 flap).
**History:** this exact assertion flapped once before, cycle 199 only
(`ignored-vs-ok blip`), with no recurrence through cycle 271. This is the
second occurrence, and the first to be classified `broken` rather than a
same-cycle blip that self-resolved before being observed live. The cycle-272
blanket-403 episode is the fifth occurrence of that pattern (prior: 244,
251, 260, 264), each one-cycle and self-resolved; this is unremarkable per
prior runs' watch note.
**Recommended action:** do not file a code fix. Watch cycle 275
(due approximately 2026-08-17T02:01Z) to confirm the contract reading
returns to `ok`/`ignored`. If `broken` persists for more than one cycle, or
recurs a third time, treat it as a real change in ILO's handling of this
parameter rather than WAF noise.
**Could not determine:** whether the HTTP 500 the monitor captured at
cycle 274 came from the same Cloudflare/WAF layer producing the blanket-403
episodes, or a distinct backend fault. ILO gives no error body distinguishing
the two failure modes.

No other endpoint changed status across cycles 271-274, and no other
contract assertion changed.

## 2026-08-16T18:42Z - cycle 271

**Changed:** ILO `gateway_issue` (cycles 268-269) -> `healthy` (cycles 270-271).
Recovered before this run started; nothing was open at check time.

**Cycle saw (268, 12:01:22Z and 269, 14:01:22Z):** gateway metadata failing
with `Error: Server error '500 Internal Server Error' for url
'https://sdmx.ilo.org/rest/dataflow/ILO/all/latest'`. Direct path stayed
healthy across both cycles.
**Cycle saw (270, 16:01:22Z and 271, 18:01:22Z):** all ILO checks passing,
gateway metadata included.
**Live recheck:** `GET https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`
returns HTTP 200 now, consistent with cycles 270-271.
**Classification:** was `gateway_issue` per `monitor/derive.py` (ours, not
provider's, since direct never broke). Fully recovered; no code fix pending.
**History:** the episode spanned two consecutive cycles (268-269, about 4
hours), which meets the escalation threshold the previous run set for this
exact watch item ("escalate ... only if gateway metadata keeps failing while
direct stays healthy across more than one cycle"). It self-resolved before
any deeper investigation was needed. This was the first occurrence of this
specific HTTP-500-on-gateway-metadata pattern in ILO's catalogued history.
**Recommended action:** none required now, the episode is closed. If a
similar >1-cycle gateway-only 500 recurs on ILO's dataflow listing, it is
worth checking whether the gateway is retrying/caching a request shape that
triggers this, since the direct path never showed the same 500 in the same
window.
**Could not determine:** why the gateway path returned 500 for two cycles
while the direct path never did. The cause resolved on its own before it
could be inspected further, so it remains unexplained.

No other endpoint changed status across cycles 268-271, and no contract
assertion changed (`changes` array empty; `STATSNZ auth:listing` remains the
only `capability_appeared`, unchanged from prior runs).

## 2026-08-16T12:44Z - cycle 268 (still open at report time)

**Changed:** ILO `healthy` (cycle 265, baseline) -> `degraded` (cycle 267) ->
`gateway_issue` (cycle 268, current). Not yet resolved as of this run.

**Cycle saw (267, 2026-08-16T10:01:22Z):** all three direct-path checks
failed with HTTP 403 (direct metadata, direct data, direct json). Gateway
paths were fine that cycle.
**Cycle saw (268, 2026-08-16T12:01:22Z):** direct paths recovered (all
200), but gateway metadata now failed: `Server error '500 Internal Server
Error' for url 'https://sdmx.ilo.org/rest/dataflow/ILO/all/latest'`.
Gateway data still succeeded. Monitor classifies the endpoint
`gateway_issue` (direct path OK, gateway path failing).
**Live recheck (12:44Z, about 40 minutes after cycle 268):** a direct GET
to that same URL (`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`)
also returned HTTP 500, but with a different body than the gateway's
error: `Value cannot be null. (Parameter 'Indicator 'EIP_3EET_SEX_AGE_NB'
is not found in the indicator codelist dictionary.')`. All other providers
checked healthy in this same window, so this is not a network problem on
this run's side.
**Classification:** monitor says `gateway_issue` (ours) based on cycle
268's snapshot. The live recheck disagrees: the *direct* path, hitting the
identical URL, also failed just afterward, with a distinct server-side
error mentioning a missing indicator codelist entry. That points at ILO's
dataflow-listing endpoint being intermittently flaky server-side
(different requests hit different internal errors) rather than a stable
bug specific to our gateway's call shape. Adopting the monitor's
`gateway_issue` label for the record, but flagging the disagreement since
it changes where the likely fault lies.
**History:** the cycle 267 direct-403 episode is broader than the
previously catalogued "ILO direct-path 403 flap" pattern (open_items:
eighth occurrence at cycle 249 hit only direct metadata; this one hit all
three direct checks). The cycle 268 gateway_issue with a 500 (rather than
403) has not been seen before in this endpoint's catalogued history.
**Recommended action:** watch cycle 269/270 (14:01Z, 16:01Z) before
escalating further. If gateway metadata keeps failing while direct
succeeds across more than one cycle, that would confirm a real
gateway-side bug worth investigating in `monitor/checks_gateway.py`'s
`list_dataflows` call (likely a request-shape difference, e.g. `fresh=True`
or params, that happens to hit ILO's flaky indicator lookup more often).
If instead direct also keeps failing intermittently, that confirms
provider-side flakiness and no code change is needed.
**Could not determine:** whether the gateway's specific request
parameters make it more likely to trigger ILO's indicator-codelist error
than a plain direct GET, since only one gateway_issue cycle exists so far
to compare against.

## 2026-08-16T06:43Z - cycle 264 (surfaced at cycle 265)

**Changed:** ILO `healthy` -> `provider_down` -> `healthy`, entirely between
runs. Previous run's baseline was cycle 262 (healthy). History shows cycle
263 healthy, cycle 264 `provider_down`, cycle 265 (current) healthy again.
Reporting per the flap rule even though it already resolved.

**Cycle saw (264, 2026-08-16T04:01:22Z):** all five basic checks failed with
HTTP 403: gateway metadata, gateway data, direct metadata, direct data,
direct json.
**Live recheck (06:43Z):** direct dataflow listing returned HTTP 200. ECB
and UNICEF direct paths also returned 200 in the same recheck, so this is
not a network problem on this run's side.
**Classification:** `provider_down` (both gateway and direct paths failed
identically with 403, theirs not ours).
**History:** fourth occurrence of the ILO blanket-403 `provider_down`
pattern. First occurrence cycle 244, second cycle 251, third cycle 260, all
three resolved within the same cycle. This fourth episode also resolved
within a single cycle (264 only, healthy again by 265). Distinct from the
separate direct-metadata-only 403 flap tracked since cycle 32 (eighth
occurrence at cycle 249, clean since). Gap between occurrences is
narrowing: cycle 244 to 251 is 7 cycles, 251 to 260 is 9 cycles, 260 to 264
is only 4 cycles.
**Recommended action:** per the standing note, escalate for real only if a
future episode spans more than one cycle; that has not happened yet. Worth
flagging on its own that the interval between occurrences is shrinking
(14h, then 18h, now 8h) even though each individual episode still
self-resolves. Continue watching; a fifth occurrence on a similarly short
interval, or one that does not clear within a cycle, should be treated as
a real incident.
**Could not determine:** whether ILO is rate-limiting or blocking on a
schedule; occurrence times so far (cycle 244 ~13:00Z, cycle 251 ~02:00Z,
cycle 260 ~20:00Z, cycle 264 ~04:00Z) still show no obvious time-of-day
pattern.

## 2026-08-16T06:43Z - cycle 263 (confirmed clean through cycle 265)

**Changed:** UNICEF `degraded` -> `healthy`. This confirms the recovery the
previous run (cycle 262) was watching for.

**Cycle saw (263, 2026-08-16T02:01:22Z):** all checks passing, no failing
entries.
**Live recheck (06:43Z):** UNICEF direct path returned HTTP 200.
**Classification:** resolved; no gateway-side issue was ever indicated
(metadata paths stayed healthy throughout the cycle 262 episode).
**History:** third occurrence of the UNICEF 429 pattern (cycle 262) is now
confirmed resolved by cycle 263, and has stayed clean through cycle 265.
No fourth occurrence followed.
**Recommended action:** none. Continue watching for a fourth occurrence of
the same pattern.
**Could not determine:** nothing outstanding; this closes the item opened
at cycle 262.

## 2026-08-16T00:43Z - cycle 262

**Changed:** UNICEF `healthy` -> `degraded`. Failing: gateway data, direct
data, direct json. Metadata (both paths) still ok.

**Cycle saw (262, 2026-08-16T00:01:22Z):** gateway data probe returned
"HTTP 429 from provider" after 2 attempts; direct data returned HTTP 429;
direct json returned HTTP 429.
**Live recheck (00:42Z, ~40 min later):** direct metadata and direct data
both returned HTTP 200 with real observations (sample "2.59"-style values
came back fine on other endpoints too; UNICEF data body parsed normally).
All 11 other endpoints were healthy in this same cycle, so this is not a
network problem on this run's side.
**Classification:** provider-side (`degraded`, data/json paths rate-limited
by UNICEF, metadata unaffected, nothing pointing at the gateway).
**History:** third occurrence of this exact UNICEF 429 pattern. First
occurrence cycle 126, resolved cycle 127. Second occurrence cycle 178,
resolved cycle 179. Clean through cycle 259. This occurrence differs
slightly: previous two hit only the direct path; this one also hit
gateway data, not just direct data/json.
**Recommended action:** no code change; matches the known transient
UNICEF rate-limit flap. The live recheck suggests it has already cleared,
but the monitor has not yet completed a cycle confirming that. Watch the
next cycle for confirmation and for a fourth occurrence.
**Could not determine:** whether the next completed cycle (263) actually
shows UNICEF healthy again, since this run's live recheck cannot substitute
for the monitor's own gateway-path probe.

## 2026-08-16T00:43Z - cycle 260 (surfaced at cycle 262)

**Changed:** ILO `healthy` -> `provider_down` -> `healthy`, entirely
between runs. Previous run's baseline was cycle 259 (healthy). History
shows cycle 260 `provider_down`, cycle 261 healthy, cycle 262 (current)
still healthy. Reporting per the flap rule even though it already
resolved.

**Cycle saw (260, 2026-08-15T20:01:22Z):** all five basic checks failed
with HTTP 403: gateway metadata, gateway data, direct metadata, direct
data, direct json.
**Live recheck:** not applicable; already two cycles resolved (261, 262
both healthy) by the time this run happened.
**Classification:** `provider_down` (both gateway and direct paths failed
identically with 403, theirs not ours).
**History:** third occurrence of the ILO blanket-403 `provider_down`
pattern. First occurrence cycle 244, second cycle 251, both resolved
within the same cycle. This third episode also resolved within a single
cycle (260 only, healthy again by 261). Distinct from the separate
direct-metadata-only 403 flap tracked since cycle 32 (eighth occurrence at
cycle 249, clean since); this cycle 260 episode is the blanket kind (all
five checks, matching cycles 244 and 251).
**Recommended action:** per the standing note from the first two
occurrences, escalate for real only if metadata+data failure recurs again
in a way that spans more than one cycle. That has not happened; this is
still resolving within a single cycle each time. Continue watching; a
fourth occurrence, or one that does not clear within a cycle, should be
treated as a real incident.
**Could not determine:** whether ILO is rate-limiting or blocking on a
schedule; the three occurrences so far (cycle 244 ~13:00Z, cycle 251
~02:00Z, cycle 260 ~20:00Z) still show no obvious time-of-day pattern.

## 2026-08-15T06:43Z - cycle 253

**Changed:** ILO `healthy` -> `provider_down` -> `healthy`, entirely between runs.
Previous run's baseline was cycle 250 (healthy). History shows cycle 251
`provider_down`, cycle 252 healthy, cycle 253 (current) still healthy.
Reporting per the flap rule even though it already resolved.

**Cycle saw (251):** all five basic checks failed with HTTP 403: gateway
metadata, gateway data, direct metadata, direct data, direct json.
**Live recheck:** direct metadata now returns HTTP 200. Other providers
(OECD) answer normally, so this is not a network problem on this run's side.
ILO is currently healthy.
**Classification:** `provider_down` (both gateway and direct paths failed
identically with 403, so this is theirs, not a gateway bug).
**History:** second occurrence of the ILO blanket-403 `provider_down`
pattern. First occurrence was cycle 244 (reported 2026-08-14T13:05Z),
resolved within that cycle, clean for six cycles (245-250). This second
episode also resolved within a single cycle (251 only, healthy again by
252). Separately, ILO has flapped direct-metadata-only 403 eight times
before (most recently cycle 249); this cycle 251 episode is the blanket
kind (all five checks, not just direct metadata), matching the cycle 244
shape.
**Recommended action:** per the open item from cycle 244, escalate when
"metadata failure recurs alongside data failure" - that condition is met
here (both metadata and data failed together, on both paths). The episode
still resolved within one cycle, same as last time, so this is not yet a
standing outage. Continue watching; a third occurrence, or one that spans
more than a single cycle, should be treated as a real provider-side
incident worth raising with ILO or adding retry/backoff around.
**Could not determine:** whether ILO is rate-limiting or blocking on some
schedule (both occurrences so far happened at different times of day, cycle
244 ~13:00Z and cycle 251 ~02:00Z, so no obvious pattern yet).

## 2026-08-15T00:43Z - cycle 250

**Changed:** ILO `healthy` -> `degraded` -> `healthy`, entirely between runs.
Previous run's baseline was cycle 247 (healthy). History shows cycle 248
healthy, cycle 249 `degraded`, cycle 250 (current) back to healthy. Reporting
per the flap rule even though it already resolved.

**Cycle saw (249):** one failing check, `direct metadata: HTTP 403`. All
other basic checks passed that cycle; contracts data was not separately
retained for cycle 249, and the current `/api/contracts` `changes` array
(computed 249 -> 250) is empty, so no contract assertion is recorded as
having flipped.

**Live recheck:** fetched the configured ILO direct metadata URL just now,
`https://sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest` ->
HTTP 200. Also checked ECB's dataflow listing as a network sanity check ->
HTTP 200, ruling out a problem on this session's network path.

**Classification:** provider-side (`degraded`, single direct-path check
failing, gateway path unaffected; not a gateway bug).

**History:** this is the narrower, single-check ILO direct-path 403 flavor
tracked separately from the cycle 244 blanket-403 episode. Prior occurrences
of this specific shape: cycles 32, 94, 159, 166, 202, 238, 244. This is the
eighth, and like all seven before it, resolved within one cycle.

**Recommended action:** none. Consistent with the long-standing recurring
pattern; continue watching for a ninth occurrence or for one that spans more
than one cycle, which would be new.

**Could not determine:** whether cycle 249's contract assertions for ILO
were affected, since per-cycle contract history is not exposed by
`/api/contracts` (it only carries the latest cycle's rows and its diff
against the immediately preceding cycle).

## 2026-08-14T13:05Z - cycle 244

**Changed:** ILO `healthy` -> `provider_down`. Three consecutive healthy
cycles (239, 240, 241, the last one being the previous run's baseline) held
through cycles 242 and 243, then broke at cycle 244, the current cycle at the
time of this run.

**Cycle saw:** all five basic checks failing with HTTP 403 - gateway metadata
(`Error: Client error '403 Forbidden' for url
'https://sdmx.ilo.org/rest/dataflow/ILO/all/latest'`), gateway data
(`probe status: error; HTTP 403 from provider.`), direct metadata (HTTP 403),
direct data (HTTP 403), and direct json (HTTP 403). This is the first time
metadata has failed alongside data on both paths; every prior ILO 403 episode
on record left at least one metadata check passing. 10 of 12 contract
assertions for ILO flipped to `broken` (all `references:*`,
`constraint:availableconstraint`, `errors:missing_artefact`,
`auth:listing`), one `skipped` (`encoding:structure_xml`, judged unreadable
under a blanket 403), and one still read `ok` (`dialect:sdmx3`, which expects
a 4xx and got one). `stale: false`, `gateway_up: true`, all other 11
endpoints healthy in this cycle, no other endpoint changed status.

**Live recheck:** fetched the same three URLs just now -
`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest` (the gateway's metadata
URL), the configured direct metadata path
(`https://sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`),
and the configured direct data path
(`https://sdmx.ilo.org/rest/data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`).
All three now return HTTP 200. This disagrees with the cycle's 403 across
the board, so the failure was transient and had already cleared by the time
of this recheck. Checked OECD and ECB directly at the same time to rule out
a network-side problem on this session's end; both answered 200, so the
403s were specific to ILO.

**Classification:** provider-side (`provider_down`, metadata failing on both
gateway and direct paths is the monitor's definition of provider-down; the
gateway itself was reachable and correctly relayed ILO's 403, so this is not
a gateway bug).

**History:** related to, but broader than, the known recurring shape. ILO
direct-path 403 on data-only checks has recurred six times before (cycles
32, 94, 159, 166, 202, 238), and a combined gateway+direct 403 on data
specifically occurred at cycles 226 and 238, each resolved within one cycle.
This is the first occurrence where metadata was also blocked and the
endpoint reached `provider_down` rather than `gateway_issue` or `degraded`.
Treating this as a new, more severe variant of the same recurring pattern
rather than folding it silently into the existing watch item.

**Recommended action:** no code change; watch for a second occurrence of
this specific "blanket 403 including metadata" shape. If it recurs or lasts
more than one cycle, escalate - a metadata-inclusive block looks more like
ILO rate-limiting or blocking this monitor's traffic outright than the
narrower data-query rejection seen before.

**Could not determine:** why this episode blocked metadata in addition to
data when none of the prior six 403 episodes did; whether this reflects a
change on ILO's side (broader WAF rule, IP-range block) or coincidental
timing with unrelated ILO-side load.

**Also seen (not alerting):** ESTAT `list_dataflows` timed out at 60s on the
gateway metadata check at cycles 242 and 243 (`gateway_issue`), then
recovered by cycle 244 (current status: healthy). This is the twelfth
occurrence of the already-documented pattern (our own call deadline firing
under load, working as designed); no new information here.

## 2026-08-14T00:51Z - cycle 238

**Changed:** ILO `healthy` -> `degraded`. Twelve consecutive healthy cycles
(cycles for this endpoint through 237, since the last run at cycle 235) ended
at cycle 238, the current cycle at the time of this run.

**Cycle saw:** both data checks failing with HTTP 403 - `gateway data`
("probe status: error; HTTP 403 from provider.") and `direct data` (HTTP 403
on `https://sdmx.ilo.org/rest/data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`).
Both metadata checks (gateway and direct) and the direct json check stayed
healthy in the same cycle. All 12 contract assertions for ILO read `ok`
(11) or `ignored` (1, `references:contentconstraint`, the known accepted-
but-dropped pattern) - none `broken`. `stale: false`, `gateway_up: true`, all
other 11 endpoints healthy, no contract `changes` reported.

**Live recheck:** fetched the same direct data URL just now -
`https://sdmx.ilo.org/rest/data/ILO,DF_GED_XLU1_SEX_HHT_CHL_RT/ITA.....?firstNObservations=1`
returned HTTP 200 with a normal GenericData payload, and the direct metadata
URL also returned 200. This disagrees with the cycle's 403, so the failure
was transient. Could not independently recheck the gateway data path (the
MCP gateway is not directly callable from this session); the monitor's own
gateway-metadata check on the same cycle succeeded, so the gateway itself
was reachable.

**Classification:** provider-side (`degraded`, both gateway and direct data
checks failed identically with 403 while both metadata checks passed on both
paths - this looks like ILO transiently rejecting this specific data query
rather than a gateway bug).

**History:** matches a known recurring shape. Direct-path 403 on ILO basic
checks has recurred five times before (cycles 32, 94, 159, 166, 202), each
resolved within one cycle. A combined gateway+direct 403 on data specifically
also occurred once before at cycle 226 (reported 2026-08-13T00:44Z), which
resolved the same run. This is at minimum the sixth occurrence of the
direct-path flavor and the second of the combined gateway+direct flavor.

**Recommended action:** none needed now; live recheck confirms already
resolved. Continue watching; escalate if a future episode is still failing
at live-recheck time instead of having already cleared, or if it starts
spanning more than one cycle.

**Could not determine:** whether ILO's 403 on this cycle was scoped to the
specific dataflow/key queried (`ITA.....`) or a broader transient block,
since only this one data query is exercised per cycle.

## 2026-08-13T18:45Z - cycle 235

**Changed:** ABS `healthy` -> `degraded` -> `healthy`, already resolved before
this run started. Contract assertion `errors:missing_artefact` went `ok` ->
`broken`.

**Cycle saw:** at cycle 234 (2026-08-13T16:01:22Z), two ABS contract checks
both returned HTTP 503 instead of their expected codes: `dialect:sdmx3`
(expected 400, observed 503) and `errors:missing_artefact` (expected 404,
observed 503, `error semantics changed from HTTP 404`). The endpoint-level
status for that cycle was `degraded`, reason "API contract broken:
errors:missing_artefact". Cycle 233 was clean (no broken contracts, status
healthy) and cycle 235 (current) is back to matching expectations for both
assertions (400 and 404 respectively), status `healthy`, all 12 endpoints
healthy, `stale: false`, `gateway_up: true`.

**Live recheck:** fetched both underlying ABS URLs directly just now -
`https://data.api.abs.gov.au/rest/dataflow/ABS/NONEXISTENT_XYZ_2026/latest`
returned 404, and `https://data.api.abs.gov.au/rest/structure/dataflow/ABS/CPI/latest`
returned 400. Both match the expected codes, confirming recovery. Sanity
check against ECB (`data-api.ecb.europa.eu`) returned 200, so this run's
network path is not the explanation for the earlier 503s.

**Classification:** provider-side. Both failing checks are direct requests
against ABS's own structure/dataflow endpoints (not gateway-mediated), so
this is ABS returning 503 for one cycle, not a gateway bug.

**History:** single-cycle event, onset and resolution both at cycle 234; no
prior occurrence found in this file of a 503 on `dialect:sdmx3` or
`errors:missing_artefact` specifically (the existing tracked ABS pattern is a
`gateway_issue` / empty-error-body flap on the gateway metadata check, a
different check entirely). Treating this as a new, first-seen flap shape
rather than a recurrence of the known one.

**Recommended action:** none needed now; already resolved. Watch for a second
occurrence of 503 on these two contract checks specifically - if it recurs or
spans more than one cycle, it would suggest a genuine ABS-side issue rather
than a one-off blip.

**Could not determine:** whether the 503 was ABS-wide (e.g. a brief
maintenance window or upstream hiccup affecting all of `data.api.abs.gov.au`)
or scoped to just these two request shapes, since the other three ABS checks
in the same cycle (gateway metadata, gateway data, direct metadata/data/json)
all passed normally at cycle 234.

## 2026-08-13T00:44Z - cycle 226

**Changed:** ILO `healthy` -> `gateway_issue`. 24 consecutive healthy cycles
(203-225) ended at cycle 226, the first cycle of this run's window.

**Cycle saw:** gateway path failing on both basic checks - gateway metadata
and gateway data both returned `403 Forbidden` for
`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest`. Direct path (metadata,
data, json) stayed fully healthy (HTTP 200) in the same cycle. Eight contract
assertions also flipped to `403` in the same cycle: `auth:listing`
(200 -> 403, flagged `broken` as "provider now demands credentials"),
`constraint:availableconstraint` (500 -> 403, `broken`),
`dialect:sdmx3` (400 -> 403, stayed `ok`, uninformative fallthrough),
`encoding:structure_xml` (skipped, judged unreadable under 403),
`errors:missing_artefact` (404 -> 403, `broken`), `references:all`
(200 -> 403, `broken`), `references:contentconstraint` (200 -> 403,
`broken`), `references:parents` (200 -> 403, `broken`). Four other ILO
contract checks in the same cycle (`references:children`,
`references:descendants`, `references:none`,
`references:parentsandsiblings`) stayed `200 ok`, so the block was not a
blanket ban on the host.

**Live recheck:** all of it is gone now. Direct curl against
`https://sdmx.ilo.org/rest/dataflow/ILO/all/latest` with the gateway's actual
Accept header (`application/vnd.sdmx.structure+xml;version=2.1`) returned
`200` on four separate attempts. The `auth:listing`, `references:all`, and
`references:parents` URLs all returned `200` unauthenticated. The
`errors:missing_artefact` URL returned `404` (matches the pre-existing
expectation, not 403). The `constraint:availableconstraint` URL returned
`500` (matches the documented ILO architectural fact, not 403). Other
providers (Pacific Data, ECB, several root checks) answered normally in the
same window, so this was not a network problem on the runner's side.

**Classification:** transient. The monitor called this `gateway_issue`
("ours") because the gateway path 403'd while the direct path succeeded in
the same cycle, but the live recheck shows the gateway's own request shape
is not at fault - the identical request now succeeds cleanly. This reads as
a short-lived block or rate limit on ILO's side that had already cleared by
the time this run checked, well within the same two-hour window.

**History:** new episode, distinct from the resolved "ILO contract-layer 403
episode" noted at cycle 214 in the state file's open items (that one did not
touch the basic gateway/direct path checks; this one did). First occurrence
of this exact shape (gateway-path 403 alongside partial contract 403s) in
the run's visibility.

**Recommended action:** none from this run; treat as resolved given the
clean live recheck. Watch for a second occurrence. If it recurs and this
time overlaps a live recheck (i.e. still failing when checked), escalate:
that would move it from "provider blip" to something worth a code-side
retry/backoff change in the gateway's ILO handling.

**Could not determine:** whether the block was IP-based rate limiting, a WAF
rule, or a maintenance blip on ILO's side; the provider gives no diagnostic
detail beyond a bare `403 Forbidden`. No other endpoint or contract changed
between cycle 223 and cycle 226.

## 2026-08-12T12:49Z - cycle 220

**Changed:** ESTAT `gateway_issue` -> `healthy`. All checks and all 12
contract assertions read `ok` at cycle 220; no `broken` verdicts anywhere
across all twelve endpoints.

**Cycle saw:** cycles 218, 219, and 220 all show ESTAT fully healthy
(`failing: []` in each). The gateway `list_dataflows` timeout that opened
this episode at cycle 217 did not recur.

**Live recheck:** not performed; this is a status recovery already
confirmed clean for three consecutive cycles (218-220) by the monitor's
own checks, not a new failure needing independent verification.

**Classification:** resolved. Eleventh episode of the known
`list_dataflows` timeout pattern closed after a single cycle (onset 217,
resolved by 218), matching the typical shape of prior episodes rather than
the alternating tenth episode.

**History:** per the prior entry and the state file's open items, this was
flagged to watch for whether it "resolved within a cycle like most prior
ones, or persisted like the tenth." It resolved within a cycle. Standing
recommendation (raise the gateway deadline for `list_dataflows`, stream
the listing, or cache the parsed result) remains open as a code-change-
scope fix; no new occurrence changes that recommendation.

**Recommended action:** none from this run. Continue watching for a
twelfth episode.

**Could not determine:** nothing outstanding on ESTAT; this entry only
records the recovery for the log. No other endpoint or contract changed
between cycle 217 and cycle 220.

## 2026-08-12T06:43Z - cycle 217

**Changed:** ESTAT `healthy` -> `gateway_issue`. Gateway metadata check
(`list_dataflows`) timed out after 60.0s across 2 attempts. Direct metadata
and direct data both stayed `ok` (716ms and 308ms). Gateway data also
stayed `ok`.

**Cycle saw:** cycle 217 (started 2026-08-12T06:01:22Z). Only the gateway
`list_dataflows` metadata call failed, with `error: "tool call
list_dataflows timed out after 60.0s"`. Direct path and all 12 contract
assertions for ESTAT read `ok`.

**Live recheck:** fetched `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT`
directly just now. It returned HTTP 200 after 29.0s, with a 37MB response
body. Slow, but well under the gateway's 60s deadline taken alone - the
gateway path is presumably losing the race when Eurostat's full dataflow
list is slow to render on top of whatever the gateway's list_dataflows
tool does with it.

**Classification:** `gateway_issue` (ours), same recurring cause as prior
episodes: the ESTAT dataflow list is very large and Eurostat's render time
for it is inconsistent, so our fixed 60s deadline sometimes gets exceeded
under load. Not a provider outage; direct path answers every time, just
sometimes slowly.

**History:** eleventh episode of this pattern (per `docs/monitor/triage-
state.json` open items, most recently the tenth: onset cycle 208, resolved
by cycle 211 with an alternating fail/recover shape in between). Clean for
cycles 211-216 (6 consecutive healthy cycles). This episode's onset is
cycle 217, single cycle so far.

**Recommended action:** unchanged standing recommendation, carried forward
again: raise the gateway's deadline for this specific call, switch to
streaming the dataflow listing, or cache the parsed result. This is a
code-change-scope fix, not something this triage run makes.

**Could not determine:** whether cycle 217 alone is the whole episode or
the start of a longer run like the tenth one; next run's history check
will show whether it resolved within a cycle or persisted.

## 2026-08-12T06:43Z - cycle 217 (ILO recovery)

**Changed:** ILO `degraded` -> `healthy`. All 12 contract assertions read
`ok` or the expected architectural `ignored` (for
`references:contentconstraint`); none `broken`.

**Cycle saw:** cycle 217, all ILO checks and contracts clean.

**Live recheck:** not needed; this is a status recovery, not a new
failure, and the prior run's own live recheck (cycle 214, minutes after
the failing cycle) had already confirmed the provider was back to the
documented baseline (500 for `/availableconstraint/`, 200 for the four
`references=*` checks).

**Classification:** resolved. The prior entry's contract-layer 403 episode
did not recur in cycles 215, 216, or 217.

**History:** the cycle 214 episode was the only occurrence; it did not
span a second cycle, so per the prior entry's stated escalation threshold
("escalate if a future episode spans a second full cycle without
clearing") no escalation is warranted. Closing the open item.

**Recommended action:** none. No code change indicated by a single-cycle
blip that already cleared.

**Could not determine:** nothing outstanding; this entry only records the
recovery for the log.

## 2026-08-12T00:43Z - cycle 214

**Changed:** ILO `healthy` -> `degraded`. Five contract assertions turned
`broken` in the same cycle: `constraint:availableconstraint` (expected 500,
observed 403), `references:all` (expected 200, observed 403),
`references:contentconstraint` (expected 200, observed 403),
`references:descendants` (expected 200, observed 403), and
`references:parentsandsiblings` (expected 200, observed 403). All five
`was` values in `/api/contracts`'s `changes` array match the long-standing
architectural baseline (500 for availableconstraint, 200 for the four
references checks), so this is a fresh, single-cycle deviation, not a
config drift.

**Cycle saw:** cycle 214 (started 2026-08-12T00:01:22Z). ILO's basic health
checks (gateway metadata, gateway data, direct metadata, direct data, direct
json) all stayed `ok`, including direct metadata and direct data returning
HTTP 200 with real observation counts. Only the contract-probe layer's
`references=*` and `/availableconstraint/` requests came back 403. This is a
different shape than the previously logged "ILO direct-path 403 flap" (which
hit the basic direct-path metadata/data/json checks); this one hit five
contract assertions simultaneously while the basic checks stayed clean.

**Live recheck:** ran the same requests directly against
`https://sdmx.ilo.org/rest` just now: `/availableconstraint/DF_GED_XLU1_SEX_HHT_CHL_RT/all/all/all`
returned 500 (matches the documented baseline), and
`/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest` with `references=all`,
`references=contentconstraint`, and `references=none` all returned 200. All
four recheck calls matched the pre-existing expected behavior, not the 403
the monitor's cycle 214 saw. Disagreement between the cycle and the live
recheck, resolved already less than an hour later.

**Classification:** provider-side (`degraded`, contract-probe layer only;
basic gateway and direct paths both `ok` throughout, so nothing indicates a
gateway bug here).

**History:** new shape as of cycle 214; no prior occurrence of all five
`references:*`/`availableconstraint` assertions failing together. The
open item log has five prior ILO direct-path 403 episodes (cycles 32, 94,
159, 166, 202), each resolving within one cycle and each on the basic
direct-path checks rather than the contract layer. Plausibly the same
underlying cause (provider-side throttling under the contract probes' burst
of near-simultaneous requests) manifesting on a different set of endpoints
this time.

**Recommended action:** watch the next cycle. If a second consecutive cycle
shows the same five contracts broken, escalate; a single-cycle blip that
already cleared on live recheck does not warrant a code change.

**Could not determine:** whether this was caused by rate limiting specific
to the contract probe's request burst (as suspected for the similar IMF
references:* 401 flap and the ILO direct-path 403 flap) or something else
transient on ILO's side; the live recheck a few minutes later cannot rule
either out, only confirm the effect did not persist.

## 2026-08-11T18:43Z - cycle 211

**Changed:** ESTAT `gateway_issue` -> `healthy`, resolved, but with a relapse
in between (closes the open item from cycle 208).

**Cycle saw:** last recorded state was cycle 208, ESTAT `gateway_issue` on the
`list_dataflows` timeout. Checked cycles 209, 210, and 211 individually via
`/api/history`: cycle 209 shows `healthy` (recovered after one cycle, same as
prior episodes), but cycle 210 shows `gateway_issue` again with the identical
failure (`gateway metadata: tool call list_dataflows timed out after 60.0s`),
and cycle 211 (the newest) is `healthy` again. Gateway data, direct metadata,
and direct data checks stayed `ok` throughout; direct json stayed `skipped`
as designed. No contract assertion broken across any of these cycles; the
only entry in the `changes` array is the known cosmetic ABS
`charset`/`version` order flip on `encoding:structure_xml` (verdict stays
`ok`, not re-reported per standing instruction).

**Live recheck:** fetched ESTAT's full agency-wide dataflow listing
(`/dataflow/ESTAT?references=none`) directly just now. It completed in
28.0s at 37.2 MB, well under the gateway's 60s deadline this time. Confirms
the listing's size is the root cause and its response time varies
cycle-to-cycle under load, consistent with prior episodes and with cycle
208's live recheck (65s+, 24.8 MB partial).

**Classification:** provider-side load, not a gateway bug (matches the
skill's documented flap pattern: "ESTAT: `list_dataflows` times out at 60s
under load, then recovers. This is our own call deadline firing, working as
designed.").

**History:** tenth known episode, onset cycle 208. Unlike most prior
episodes (which failed for one or two consecutive cycles then stayed clean),
this one alternated: fail (208), recover (209), fail again (210), recover
(211). That alternating shape is new for this pattern - the ninth episode
(cycles 144-145) was two consecutive failing cycles with a clean recovery
after, not a fail/recover/fail/recover sequence. Worth a closer look if a
future episode repeats this shape, since it could mean load is now closer to
the 60s boundary more persistently rather than a single spike.

**Recommended action:** none beyond continuing to watch. Standing
recommendation to raise the gateway's deadline, stream the listing, or cache
the parsed result carries forward for a session with code-change scope; this
is a docs-only run.

**Could not determine:** whether the alternating shape (as opposed to
consecutive-then-clean) reflects a genuine change in ESTAT's load pattern or
is coincidental variance across two data points; only one episode has shown
it so far.

## 2026-08-11T12:49Z - cycle 208

**Changed:** ESTAT `healthy` -> `gateway_issue`, current as of this cycle (still
in that state as of this run, roughly 46 minutes after cycle start).

**Cycle saw:** cycle 208 (started 2026-08-11T12:01:22Z) shows ESTAT gateway
metadata check (`list_dataflows`) failing with `tool call list_dataflows
timed out after 60.0s`, 2 attempts. Gateway data check, direct metadata,
and direct data checks all `ok`; direct json is `skipped` as designed
(Eurostat returns 406 for SDMx-JSON, expected). No contract assertion
broken; `changes` array showed only the known cosmetic ABS
`charset`/`version` order flip (verdict stays `ok`, not re-reported per
standing instruction).

**Live recheck:** fetched ESTAT's full agency-wide dataflow listing
(`/dataflow/ESTAT`) directly just now. It did not finish within 65 seconds
and had already downloaded 24.8 MB when the recheck's own timeout cut it
off. This confirms the listing genuinely is large and slow right now, not
a gateway-side bug: the gateway's `list_dataflows` call is timing out
against a real large/slow response, exactly the shape of the
already-known pattern.

**Classification:** provider-side load, not a gateway bug (`gateway_issue`
status label reflects which path failed, not fault - matches this
endpoint's established pattern per the skill's flap list: "ESTAT:
`list_dataflows` times out at 60s under load, then recovers. This is our
own call deadline firing, working as designed.").

**History:** ESTAT `list_dataflows` timeout pattern, now its tenth known
episode (prior: onset then resolution across cycles up to the ninth
episode at cycles 144-145, clean for 60 consecutive cycles through cycle
205 per the last state file). Onset this time is cycle 208, the latest
cycle available at the time of this run - not yet confirmed resolved
since no cycle 209 exists yet.

**Recommended action:** none beyond watching for cycle 209 to confirm
recovery, consistent with every prior episode. Standing recommendation to
raise the gateway's deadline, stream the listing, or cache the parsed
result carries forward for a session with code-change scope; this is a
docs-only run.

**Could not determine:** whether cycle 209 will show recovery, since it
has not started yet at the time of this run.

## 2026-08-11T12:49Z - cycle 206 (resolved by 207)

**Changed:** ABS `healthy` -> `gateway_issue` -> `healthy`, resolved before
this run started (flap happened between the last two runs).

**Cycle saw:** cycle 206 (started roughly two hours before cycle 208)
shows ABS gateway metadata check failing with an empty error message
(`"error": "Error: "`), 2 attempts, latency 31.9s. Gateway data check and
all three direct-path checks (metadata, data, json) were `ok`. No contract
assertion broken in cycle 206. By cycle 207, ABS gateway metadata is `ok`
again and status reads `healthy` through cycle 208.

**Live recheck:** not applicable - the failure had already self-resolved
by the time this run started, one cycle after onset.

**Classification:** provider-side (`gateway_issue` while it lasted, direct
path never affected). Matches the pre-existing "ABS gateway metadata
empty-error-body flap" pattern exactly, including the shape of the error
(empty message after "Error: ").

**History:** sixth occurrence of this recurring pattern. Fifth occurrence
was cycles 150-152 (3 consecutive cycles, the longest yet); this one
lasted a single cycle, clean again by cycle 207. 54 consecutive healthy
cycles for this shape preceded this occurrence.

**Recommended action:** none for this occurrence beyond logging it. The
empty error message itself remains worth fixing under code-change scope
(`GatewayError`/`next_step` in `monitor/checks_gateway.py`), carried
forward from prior runs.

**Could not determine:** nothing outstanding; the flap is fully bounded
(one cycle, self-resolved, confirmed via `/api/cycle/206` and
`/api/status` for 207/208).

## 2026-08-11T06:46Z - cycle 205

**Changed:** ILO `degraded` -> `healthy`, resolved (closes the open item from
cycle 202).

**Cycle saw:** last recorded state was cycle 202 (2026-08-11T00:01:22Z), ILO
`degraded` with 9 of 12 contract assertions returning `403`, live recheck at
that time still showing mixed 200/403 and marked "not confirmed clean yet."
Checked cycles 203, 204, and 205 individually via `/api/cycle/{id}` and
`/api/status` (not just `/api/history`, which does not reflect contract-probe
degradation). ILO's `contracts.broken` list is empty in all three cycles; the
only non-`ok` row is the expected `references:contentconstraint` -> `ignored`
informational verdict. Endpoint status reads `healthy` in cycles 203, 204, and
205. Resolution happened by cycle 203, one cycle after onset - same as all
four prior occurrences (cycles 32, 94, 159, 166). This does not extend into a
second full cycle, so it stays inside the known pattern rather than becoming
new territory.

**Live recheck:** against the contract probe's actual dataflow
(`DF_GED_XLU1_SEX_HHT_CHL_RT`), `references=none/children/descendants/parents/all`
all returned `200` just now. A separate probe against the full agency-wide
listing with `references=all` (a much heavier, unrelated query) timed out
after 45s; other providers (ECB, ILO root, ABS, BIS) answered normally in the
same window, so this is that one heavy query being slow, not a provider or
network outage - it is not part of the contract check the monitor runs and is
not being carried forward as an issue.

**Classification:** provider-side, resolved. No gateway code implicated (the
prior classification for the degraded episode already ruled that out).

**History:** fifth occurrence of this recurring pattern, onset cycle 202,
resolved by cycle 203, clean through cycle 205 (3 consecutive healthy
cycles including the contract-probe layer). Confirms the pattern still
self-resolves within one cycle every time it has been observed.

**Recommended action:** none. Keep watching for a sixth occurrence and for
any future case where it does not clear within a cycle.

**Could not determine:** nothing outstanding for this item; fully confirmed
resolved by direct cycle-by-cycle inspection and a live recheck.

## 2026-08-11T00:45Z - cycle 202

**Changed:** ILO `healthy` -> `degraded`, current as of this cycle (still
partly occurring at live recheck time, roughly 40 minutes after cycle start).

**Cycle saw:** last run's state was cycle 199 (2026-08-10T18:01:22Z), ILO
`healthy`. `/api/history` shows ILO `healthy` through cycles 199, 200, 201
- but that series does not reflect contract-probe-only degradation (a known
gap, already noted from prior ILO occurrences). `/api/status` for the
current cycle 202 (started 2026-08-11T00:01:22Z) shows ILO `degraded`,
reason "API contract broken", with 9 of 12 contract assertions returning
`403` where `200`, `500`, or `404` was expected: `auth:listing`,
`constraint:availableconstraint`, `errors:missing_artefact`,
`references:all`, `references:children`, `references:contentconstraint`,
`references:descendants`, `references:none`, `references:parentsandsiblings`.
`references:parents` stayed `ok` (200). All five basic gateway/direct
checks (metadata, data, json on both paths) stayed `ok` throughout - only
contract-probe traffic drew the 403, matching the pattern already tracked
as "ILO direct-path 403 flap" in the state file's open items.

**Live recheck:** mixed and still flapping. Agency-wide listing
(`/rest/dataflow/ILO/all/latest`) and its `references=none` returned `200`;
`references=all` on the same agency-wide query timed out after 45s with no
response. Against the contract probe's specific dataflow
(`DF_GED_XLU1_SEX_HHT_CHL_RT`): `references=none` `403`, `references=children`
`200`, `references=descendants` `403`, `references=parents` `403`,
`references=parentsandsiblings` `403`, `references=all` `200`,
`availableconstraint` `500` (back to the expected shape), missing-artefact
probe `404` (also back to expected). ECB and OECD answered `200` on
unrelated checks in the same window, ruling out a network problem on this
session's side.

**Classification:** provider-side (`degraded`, contract-probe traffic only;
basic gateway and direct checks unaffected - nothing gateway-specific,
nothing to fix in our code).

**History:** fifth occurrence of the recurring ILO 403 flap. Prior
occurrences: cycle 32, cycle 94, cycle 159 (10 of 12, broadest until now),
cycle 166 (5 of 12). All four resolved within one cycle. This one hit 9 of
12 - close to the broadest occurrence yet - and unlike the prior four, it
had not fully cleared by the time of this run's live recheck (still mixed
200/403 responses on the same probe set).

**Recommended action:** none beyond watching. This is the same recognized
pattern (provider-side throttling tripped by contract-probe volume, not a
general ILO outage) as the four prior occurrences; the next run should
confirm it has cleared. If it spans into a second full cycle, that would be
new territory for this pattern and worth escalating.

**Could not determine:** whether this occurrence has fully resolved as of
this run, since the live recheck still showed intermittent `403`s roughly
40 minutes after the cycle that first saw it.

---

## 2026-08-10T18:45Z - cycle 199

**Changed:** ECB `healthy` -> `provider_down`, current as of this cycle
(not yet resolved when this run started, recovered by the time of the live
recheck ~44 minutes later).

**Cycle saw:** last run's state was cycle 196 (2026-08-10T12:01:22Z), all 12
endpoints `healthy`. `/api/history` shows ECB `healthy` through cycles 196,
197, 198, then `provider_down` at 199 (2026-08-10T18:01:22Z), reason
"metadata failing on both the gateway and the direct path". Both `gateway
metadata` and `direct metadata` returned `Error: Server error '503 Service
Temporarily Unavailable'` / `HTTP 503` for
`https://data-api.ecb.europa.eu/service/dataflow/ECB/all/latest`. `gateway
data`, `direct data`, and `direct json` all stayed `ok` - only the metadata
listing failed. `/api/contracts` `changes` confirms the same cycle: 8
`references:*` and `errors:missing_artefact` assertions flipped `200` ->
`503`, verdict `broken`; `encoding:structure_xml` flipped to `skipped`
(also 503).

**Live recheck:** an unauthenticated direct GET to
`data-api.ecb.europa.eu/service/dataflow/ECB/all/latest` returned `200` with
a normal structure payload. ILO answered `200` on the same check moments
earlier, ruling out a network problem on this session's side. ECB has
recovered.

**Classification:** provider-side (`provider_down`, both gateway and direct
paths failing identically with 503 - nothing gateway-specific, nothing to
fix in our code).

**History:** new failure mode. Not the same issue as the earlier, resolved
ECB `406` problem (fixed by retrying `text/csv` on 406, healthy since,
noted in this skill's known-flaps list). This is the first observed ECB
`503` on the metadata endpoint.

**Recommended action:** none needed now that it has recovered; watch for a
second occurrence. If ECB `503`s recur, treat it as a recognized pattern
the way the ILO 403 and IMF 401 contract-probe flaps are tracked.

**Could not determine:** whether the 503 came from an ECB-side deploy,
maintenance window, or transient overload; the outage window was too short
to tell from outside.

---

**Also seen, both already resolved between the last run and this one (found
by checking intermediate cycles individually, since `/api/history` and
`/api/contracts` `changes` only cover the two newest cycles):**

- **UNICEF** `healthy` -> `degraded` at cycle 197 (2026-08-10T14:01:22Z),
  resolved by cycle 198. `gateway data`, `direct data`, and `direct json`
  all returned `HTTP 404` ("no data found for this query"); metadata
  unaffected. The `constraint:availableconstraint` contract assertion also
  went `broken` (expected 200, observed 404) in the same cycle. This is a
  different shape than the UNICEF `429` flap already tracked in
  `open_items` (first at cycle 126, second at cycle 178) - first time a 404
  "no data" shape has been seen here. Likely a transient empty result for
  the probe's specific dataflow/period rather than an outage; resolved
  within one cycle.

- **ABS** `healthy` -> `gateway_issue` at cycle 198 (2026-08-10T16:01:22Z),
  resolved by cycle 199. Only `gateway metadata` failed, with `Error:
  Server error '502 Bad Gateway'` from `data.api.abs.gov.au`; `direct
  metadata`, `direct data`, `direct json`, and `gateway data` all stayed
  `ok` in the same cycle. This is a different shape than the tracked ABS
  "empty error body" flap in `open_items` (which has no error message at
  all) - here the 502 and message are both present, and it came from ABS's
  own server rather than a gateway-side empty response. Classified
  `gateway_issue` per the monitor's own rules (direct succeeded, gateway
  path failed), but the underlying 502 originated at ABS, propagated
  through the gateway's HTTP client. Resolved within one cycle.

**Minor, not alerted:** ILO's `references:contentconstraint` contract
verdict read `ok` at cycle 199 instead of the expected `ignored` (expected
"200", observed "200" in both cases, so it did not show up in
`/api/contracts` `changes`, which only diffs those fields). A live recheck
against `sdmx.ilo.org/rest/dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest`
just now shows `references=none` and `references=contentconstraint`
returning the same payload size (7699 bytes) with identical content once
the per-request `message:ID`/`Prepared` header fields are excluded - i.e.
the parameter is still silently dropped in substance. Reads as one-cycle
noise in the monitor's own ignored-vs-ok comparison, not a real capability
change. Not `broken` or `capability_appeared`, so not treated as
notify-worthy on its own; noting here in case it recurs.

## 2026-08-09T12:45Z - cycle 184

**Changed:** ECB `healthy` -> `degraded` (contract-probe only), already
resolved before this run started.

**Cycle saw:** last run's newest cycle was 181 (2026-08-09T06:01:22Z), all 12
endpoints `healthy`, ECB clean. `/api/history` for the intervening cycles
shows ECB `healthy` at 182, 183, and 184 - but that series does not reflect
contract-probe-only degradation, as noted in `open_items` for the ILO 403
pattern, so per-cycle detail was pulled directly. `/api/cycle/182` showed ECB
`degraded`, reason "API contract broken: references:descendants,
references:none, references:parents" - all three returned HTTP 504 where
`200` was expected, plus `encoding:structure_xml` `skipped` (also 504).
`/api/cycle/183` showed ECB `degraded` again with a different pair broken:
`references:children` and `references:contentconstraint`, again HTTP 504.
`/api/cycle/184` shows ECB fully `healthy`, all contract rows `ok`.
`/api/contracts` `changes` (which only diffs the two newest cycles) surfaced
only the 183->184 half of this: `references:children` and
`references:contentconstraint` `504` -> `200`, verdict `ok`. No other
endpoint changed status across cycles 182-184.

**Live recheck:** direct, unauthenticated requests to
`data-api.ecb.europa.eu/service/dataflow/ECB/EXR` with `references=children`
and `references=contentconstraint` both returned `200` in under 1.2s. IMF
answered `200` on the same check during this recheck, ruling out a network
problem on this session's side.

**Classification:** provider-side timeout (`degraded`, HTTP 504 on
`references=*` structure queries only; basic gateway/direct health checks -
metadata, data, json - stayed `ok` throughout both cycles). Nothing to fix in
gateway code.

**History:** first observed occurrence of an ECB `references=*` 504 flap.
Not the same failure mode as ECB's earlier 406 issue (resolved, no longer a
known failure per `open_items`). Spans two consecutive cycles (182 and 183,
4 hours), each affecting a different subset of the `references:*` contract
assertions, then fully clean by 184.

**Recommended action:** none needed; watch for a second occurrence. If
`references=*` 504s recur soon, or affect the basic metadata/data checks
rather than only contract probes, treat it as a recognized recurring pattern
like the ILO 403 flap or IMF 401 flap rather than a one-off.

**Could not determine:** whether the 504s came from ECB-side load, a CDN or
proxy timeout in front of ECB, or throttling triggered by the monitor's own
contract-probe volume (the same mechanism suspected for the ILO and IMF
flaps).

## 2026-08-09T00:43Z - cycle 178

**Changed:** UNICEF `healthy` -> `degraded`, already resolved before this run
started.

**Cycle saw:** last run's newest cycle was 175 (2026-08-08T18:01:22Z), all 12
endpoints `healthy`. Checked the intermediate cycles individually via
`/api/history`: 176 and 177 both clean. Cycle 178 (2026-08-09T00:01:22Z)
showed UNICEF `degraded`, reason "failing: gateway data, direct data, direct
json" - all three failing checks returned HTTP 429 from the provider. Direct
metadata and gateway metadata both stayed `ok`. `/api/contracts` `changes`
is empty and no contract row is `broken`; the only non-`ok` contract row
anywhere is STATSNZ `auth:listing` `capability_appeared`, open since cycle
60, unchanged, not re-reported. No other endpoint shows a status change
across cycles 176-178.

**Live recheck:** performed now, cycle 178 about 40 minutes old. Direct,
unauthenticated request to `sdmx.data.unicef.org` for
`data/UNICEF,GLOBAL_DATAFLOW/ALB.CME_MRY0T4._T?firstNObservations=1` returned
`200` in 0.75s. The `dataflow/UNICEF/all/latest?detail=allstubs` metadata
path also returned `200`. Confirms the episode has already cleared. Other
providers (ECB, IMF) answered fine during the same check, ruling out a
network problem on this session's side.

**Classification:** provider-side rate limiting (`degraded`, HTTP 429 on
data paths only, metadata unaffected on both gateway and direct). Nothing to
fix in gateway code.

**History:** second occurrence of the UNICEF direct-path 429 flap noted in
`open_items` (first: cycle 126, direct data + direct json only, resolved by
cycle 127). This time gateway data also drew a 429, since the gateway proxies
the same provider call. Both occurrences resolved within one cycle.

**Recommended action:** none needed; watch for a third occurrence. If a
third episode arrives soon or fails to clear within a cycle, treat UNICEF
429 as a recognized recurring pattern like the ILO 403 flap rather than a
one-off.

**Could not determine:** the exact provider-side trigger (rate limit window,
request volume, or scheduled maintenance) for either 429 episode; UNICEF
does not expose that in its response body.

## 2026-08-08T00:42Z - cycle 166

**Changed:** ILO `healthy` -> `degraded` (contracts broken), already resolved
before this run started.

**Cycle saw:** last run's newest cycle was 163 (18:01:22Z), all 12 endpoints
`healthy`. This run's newest cycle is 166 (2026-08-08T00:01:22Z). Pulling
`/api/cycle/164` and `/api/cycle/165` directly found both clean; `/api/cycle/166`
showed ILO `degraded`, reason `"API contract broken: references:all,
references:children, references:descendants, references:parents,
references:parentsandsiblings"`. All five assertions returned HTTP 403 where
`200` was expected. `/api/contracts` `changes` confirms the same five
was:200 -> now:403 entries and nothing else. Basic gateway/direct checks
(metadata, data, json on both paths) for ILO stayed `ok` throughout cycle 166 -
only the `references:*` contract probes drew the 403. No other endpoint
appears degraded across cycles 164-166, and STATSNZ's long-standing
`auth:listing` `capability_appeared` (open since cycle 60) is the only other
non-`ok` contract row present - unchanged, not re-reported.

**Live recheck:** performed now, cycle 166 about 40 minutes old. Direct,
unauthenticated requests to `sdmx.ilo.org` for
`dataflow/ILO/DF_GED_XLU1_SEX_HHT_CHL_RT/latest` with `references=all`,
`children`, `descendants`, `parents`, `parentsandsiblings`, and `none` all
returned `200` now - confirms the episode has already cleared, no
disagreement with what the pattern predicts.

**Classification:** provider-side. Same shape as every prior ILO
`references:*` 403 flap: probe-only traffic tripping something on ILO's side
(rate limit or transient WAF rule) while the checks the gateway's actual tool
calls exercise stayed healthy throughout.

**History:** this is the fourth occurrence of the ILO direct-path 403 flap
tracked in `open_items` (prior: cycle 32, cycle 94, cycle 159). Breadth this
time (5 of 12 assertions - all the `references:*` checks except
`contentconstraint` and `none`) is narrower than cycle 159's 10 of 12, closer
to cycle 94's shape. All four occurrences have now resolved within a single
cycle.

**Recommended action:** no code change; keep watching for a fifth occurrence
or one that fails to clear within a cycle, which would upgrade this from
"known flap" to "new problem."

**Could not determine:** whether ILO's 403 was a deliberate rate-limit
response to the contract probe's request volume/pattern, or an unrelated
provider-side blip that happened to coincide with the probe window.

## 2026-08-07T18:43Z - cycle 163

**Changed:** IMF `healthy` -> `degraded` (contracts broken) -> `healthy`, already resolved before this run started.

**Cycle saw:** last run's newest cycle was 160 (12:01:22Z), all 12 endpoints
`healthy`. This run's newest cycle is 163 (18:01:22Z), also all `healthy`, and
`/api/contracts` `changes` is empty (it only diffs the two most recent cycles).
Pulling `/api/cycle/161` and `/api/cycle/162` directly (the two intermediate
cycles) found it: at cycle 161 (14:01:22Z) IMF status was `degraded`, reason
`"API contract broken: references:all, references:children,
references:descendants, references:parentsandsiblings"`. All four assertions
returned HTTP 401 where `200` was expected. Cycle 162 (16:01:22Z) and cycle
163 are both clean - the episode is exactly one cycle wide. As with the ILO
episode logged at cycle 160, `/api/history?hours=48`'s per-endpoint
`status`/`failing` series shows `healthy`/`[]` for IMF at cycle 161 - it only
tracks the five basic gateway/direct checks, not contract assertions, so it
missed this too. `/api/cycle/{id}` is the only view that caught it. No other
endpoint appears degraded across cycles 161-163, and STATSNZ's long-standing
`auth:listing` `capability_appeared` (open since cycle 60) is the only other
non-`ok` contract row present at any of these cycles - unchanged, not
re-reported.

**Live recheck:** performed now, cycle 161 nearly 5 hours old. Direct,
unauthenticated requests to `api.imf.org` for `dataflow/IMF.STA/CPI` with
`references=all`, `children`, `descendants`, and `parentsandsiblings` all
returned `200` - confirms the monitor's own cycle-162 recovery, no
disagreement.

**Classification:** provider-side. Cycle 161's basic gateway and direct
checks for IMF (metadata, data on both paths; direct json is permanently
skipped since IMF ignores the JSON `Accept` header) all stayed `ok` - only
the four `references:*` contract probes drew `401`. IMF has no separate
listing/auth-probe endpoint the way ILO does, but the shape is the same as
the ILO cycle-159 and cycle-32/94/159 flaps: probe-only traffic tripping
something on the provider side (rate limit or transient auth check) while
the checks the gateway's actual tool calls exercise stayed healthy
throughout.

**History:** new as of cycle 161; no prior IMF flap is recorded in this log
or in the previous run's `open_items`. Not (yet) known to be chronic or
flapping - this is the first occurrence.

**Recommended action:** no code change; watch for a second IMF `references:*`
401 occurrence. If it recurs, treat it the same as the ILO pattern (probe
volume triggering provider-side throttling) rather than a gateway bug, since
the basic health checks are unaffected both times.

**Could not determine:** whether the 401 came from a real IMF auth/rate-limit
system or a transient IMF-side fault unrelated to request volume; IMF does
not expose enough detail in the response to distinguish the two.

## 2026-08-07T12:49Z - cycle 160

**Changed:** ILO `healthy` -> `degraded` (contracts broken) -> `healthy`, already resolved before this run started.

**Cycle saw:** last run's newest cycle was 157 (06:01:22Z), `healthy`. `/api/status`
at cycle 160 (12:01:22Z, this run's newest) shows all 12 endpoints `healthy`,
`gateway_up: true`, `stale: false`, `drift: []`. But `/api/contracts` `changes`
listed 12 ILO entries with `was: 403` across `auth:listing`,
`constraint:availableconstraint`, `dialect:sdmx3`, `encoding:structure_xml`,
`errors:missing_artefact`, and all seven `references:*` assertions. Pulling
`/api/cycle/159` (10:01:22Z, the cycle between 157 and 160) directly confirmed
it: ILO status was `degraded` that cycle, reason `"API contract broken:
auth:listing, constraint:availableconstraint, errors:missing_artefact,
references:all, references:children, references:contentconstraint,
references:descendants, references:none, references:parents,
references:parentsandsiblings"`. 10 of 12 contract assertions returned HTTP 403
that cycle (`dialect:sdmx3` still read `ok` since its expectation is `n/a`, and
`encoding:structure_xml` was `skipped` because the 403 pre-empted it). Cycle 158
(08:01:22Z) was clean, and cycle 160 is clean again - the episode is exactly
one cycle wide. Note: `/api/history?hours=48`'s per-endpoint `status`/`failing`
series shows `healthy`/`[]` for ILO at cycle 159 - that series tracks only the
five basic gateway/direct checks, not contract assertions, so it would have
missed this entirely. `/api/cycle/{id}` and `/api/contracts` are the only
views that caught it. All 11 other endpoints stayed `healthy` across cycles
158-160, and no other endpoint appears in the `changes` array.

**Live recheck:** performed now, cycle 160 already 48 minutes old. Direct,
unauthenticated requests to ILO's listing, `references=all`, and
missing-artefact URLs all returned normally (`200`, `200`, `404` respectively)
- confirms the monitor's own cycle-160 recovery, no disagreement.

**Classification:** provider-side. The basic gateway and direct data/metadata
checks (the ones the gateway's actual tool calls exercise) stayed `ok` through
cycle 159 - only the contract probes, which hit ILO's listing/structure
endpoints directly and more frequently than normal traffic, drew a blanket 403
for that one cycle. Reads like a transient rate-limit or WAF trip on ILO's
side against the probe traffic pattern, not a gateway bug and not a change in
what the gateway itself can reach.

**History:** this is the third occurrence of the ILO direct-path 403 flap
tracked in `open_items`, and clearly the broadest yet - 10 contract assertions
broken this time, versus 4 at cycle 94 and data+json only at cycle 32. All
three have now resolved within a single cycle. The escalating breadth (4 of 12
-> 10 of 12) is worth continuing to watch; a fourth occurrence, or one that
fails to clear within a cycle, would upgrade this from "known flap" to "new
problem."

**Recommended action:** no code change - watch for a fourth occurrence.
Because `/api/history` doesn't surface contract-driven degradation, future
runs need to keep checking `/api/contracts` `changes` and individual
`/api/cycle/{id}` calls for the cycles between runs, not just the history
series, or a repeat of this could be missed.

**Could not determine:** whether ILO's 403 was a deliberate rate-limit
response to the contract probe's request volume/pattern, or an unrelated
provider-side blip that happened to hit during that cycle's probe window.

## 2026-08-07T00:42Z - cycle 154

**Changed:** ABS `gateway_issue` -> `healthy`, already resolved before this run started.

**Cycle saw:** last run's newest cycle (151, 18:01:22Z) was `gateway_issue`.
The history series for cycles 148-154 shows the episode ran longer than
recorded last time: `gateway_issue` at 150, 151, and 152 (16:01:22Z through
20:01:22Z, three consecutive cycles, all on the same failing check `gateway
metadata: Error:` with an empty body after the colon), then `healthy` again
at 153 (22:01:22Z) and 154 (00:01:22Z, this run's newest). All other 11
endpoints stayed `healthy` across the whole window. `/api/status` at cycle
154: `gateway_up: true`, `stale: false`, `drift: []`. `/api/contracts`
`changes` is empty - no contract assertion changed. STATSNZ `auth:listing`
is still `capability_appeared`, same as every run since cycle 60, not a new
change.

**Live recheck:** not performed against the ABS failure itself - by the time
this run started the condition had already cleared for two full cycles
(153 and 154), so there was nothing live to catch. Cycle 154 (41 minutes old
at run time) already shows ABS gateway metadata passing at 404ms.

**Classification:** was `gateway_issue` (ours) while it lasted - direct path
stayed fine throughout, only the gateway path failed. Same code site as the
last two occurrences: `gateway_metadata_check` in `monitor/checks_gateway.py`
surfaces the gateway's own `next_step` field verbatim when `total_found < 1`
and the field starts with `"Error"`; here that field carried no message after
the colon, so the underlying cause on the gateway side is still not visible
from the monitor's output. The empty-message gap itself remains open and
unfixed - this run is docs-only and out of scope for a code change.

**History:** fifth occurrence of the ABS empty-error-body pattern overall
(prior: cycle 26, cycle 134, cycle 143, and the 150-151 span reported last
run), and now the longest one seen: three consecutive cycles (150-152, 4
hours from first failure to last), one cycle longer than the 150-151 span
already flagged as unusual last time. It resolved on its own by cycle 153
without any intervention (this routine never calls `/api/refresh`).

**Recommended action:** the escalating duration (1 cycle historically, then
2, now 3) is worth watching as a trend rather than dismissing as the same
old flap. If a sixth occurrence spans four or more cycles, treat it as a
genuine regression rather than a known cosmetic pattern, and prioritize the
empty-error-message fix in `checks_gateway.py` / the gateway's `next_step`
construction so a real cause is visible next time instead of a blank string.

**Could not determine:** what change on ABS's or the gateway's side made
this episode last three cycles instead of one or two, and whether it is
still the same root cause (ABS's own `references=none` listing being slow,
as the live recheck during cycle 151 suggested) or something new, since no
live recheck was possible this run once the condition had already cleared.

## 2026-08-06T18:45Z - cycle 151

**Changed:** ABS `healthy` -> `gateway_issue`, ongoing for two consecutive cycles.

**Cycle saw:** cycle 148 (this run's previous state) was healthy. Cycle 150
(16:01:22Z) flipped to `gateway_issue` on `gateway metadata: Error:` (empty
error body after the colon), `attempts: 2`, `latency_ms: 32197`. Cycle 151
(18:01:22Z, this run's newest) is still `gateway_issue`, same failing check,
same empty error text. Direct path stayed fine both cycles (metadata 200 in
246ms, data 200 with a sample observation). All other 11 endpoints are
`healthy` at cycle 151. `/api/contracts` `changes` shows one entry: FBOS
`encoding:structure_xml` Content-Type parameter order flipped (charset before
version, now version before charset), verdict stays `ok` - this is the known
cosmetic flap already covered by the standing note (previously seen on ABS,
OECD, ILO; FBOS is a new endpoint added to that list, not a new kind of
change). No `broken` verdicts on any endpoint. STATSNZ `auth:listing` is
still `capability_appeared`, unchanged since cycle 60, not re-reported.

**Live recheck:** fetched the direct ABS dataflow listing
(`https://data.api.abs.gov.au/rest/dataflow?references=none`) live during
this run. First two attempts (15s and 20s caps) timed out with no response;
a third attempt with a 40s cap succeeded, HTTP 200, 3,218,152 bytes, 12.9s.
Ruled out a network-wide problem first: ECB and OECD direct endpoints, plus
the monitor's own `/healthz`, all answered normally in the same window, so
the timeouts were specific to ABS, not this routine's network path.

**Classification:** `gateway_issue` (ours) per the monitor - direct path OK,
gateway path failing. `gateway_metadata_check` in `monitor/checks_gateway.py`
takes the `next_step` field from a `total_found < 1` response when it starts
with `"Error"`; an empty string after the colon means the gateway's own
`list_dataflows` call returned that literal placeholder with no underlying
message, not that ABS returned nothing. The live recheck suggests why: ABS's
`references=none` dataflow listing is large (3.2MB) and was slow and variable
tonight (over 20s on two attempts, 12.9s on the third), which plausibly pushes
the gateway's own internal timeout for this call past its budget on some
attempts. This is consistent with, not a repeat of, the pattern already open
for ABS (see standing item below) - same empty-error-body shape, but every
prior occurrence resolved within a single cycle, and this one has now held for
two.

**History:** fourth occurrence of the ABS empty-error-body pattern (prior:
cycle 26, cycle 134, cycle 143), and the first that has not self-resolved
within one cycle - it has now spanned cycles 150 and 151 (4 hours). The
standing recommendation already called the empty message itself worth fixing
after the third occurrence; a fourth occurrence that also broke the
one-cycle-recovery habit raises that from "worth fixing eventually" to "worth
fixing soon," for a session with code-change scope. No fix applied here
(docs-only scope).

**Recommended action:** watch cycle 152. If it recovers, note the total
duration and close as a longer but still self-resolving episode. If it is
still `gateway_issue` at cycle 152, treat this as a genuine escalation of the
known pattern and raise priority on fixing the empty-error-body message and
reviewing whether the gateway's internal timeout for `list_dataflows` needs to
account for large provider listings like ABS's, not just ESTAT's (see the
ESTAT `list_dataflows` timeout item below - the two may share a root cause:
a call-time budget that does not scale with listing size).

**Could not determine:** whether tonight's ABS slowness is itself a new,
separate condition on ABS's side (their listing endpoint under load) or pure
coincidence with the gateway's known empty-error-body bug; the live recheck
shows ABS actually is slow right now, but cannot show what the gateway's
internal call experienced on cycles 150-151 specifically.

## 2026-08-06T12:45Z - cycle 148

**Changed:** ESTAT `gateway_issue` -> `healthy`, resolved.

**Cycle saw:** cycle 145 (this run's previous state) was still `gateway_issue`
on `list_dataflows` timeout, second consecutive cycle. Cycle 146 (08:01:22Z)
recovered to `healthy`, and stayed `healthy` through cycles 147 and 148
(this run's newest, 12:01:22Z). All 12 endpoints are `healthy` at cycle 148.
`/api/contracts` `changes` shows one entry: OECD `encoding:structure_xml`
Content-Type parameter order flipped (charset before version, now version
before charset), verdict stays `ok` - this is the known cosmetic flap already
covered by the standing note, not re-reported as new. No `broken` verdicts on
any endpoint. STATSNZ `auth:listing` is still `capability_appeared` (listing
served without credentials), unchanged since first seen at cycle 60, not
re-reported per the standing open item.

**Live recheck:** not performed; the cycle 148 data already shows three
consecutive healthy cycles (146, 147, 148) after the failure, which is
sufficient confirmation of recovery without a live fetch.

**Classification:** `gateway_issue` (ours) resolved on its own, consistent
with all eight prior episodes of this pattern - the gateway's own ~60s call
deadline for `list_dataflows` firing against Eurostat's large listing, then
succeeding on a later attempt. Not a provider outage.

**History:** ninth episode on the ESTAT `list_dataflows` timeout pattern
closes out: began cycle 144, held through cycle 145 (the first time it lasted
two consecutive cycles instead of one), recovered by cycle 146. No fix
applied; this remains a docs-only-scope routine. Standing recommendation
carried forward: raise the deadline, stream the listing, or cache the parsed
result, for a session with code-change scope.

**Recommended action:** no action this run. Continue watching for a tenth
episode or one that fails to resolve within a cycle or two, which would raise
priority on the standing recommendation.

**Could not determine:** nothing outstanding; the recovery is confirmed by
three consecutive healthy cycles from the monitor's own record.

## 2026-08-06T06:43Z - cycle 145

**Changed (1 of 2):** ESTAT `healthy` -> `gateway_issue`, ongoing for two
consecutive cycles.

**Cycle saw:** cycle 143 (02:01:22Z, previous run's newest) was healthy.
Cycle 144 (04:01:22Z) flipped to `gateway_issue` on the usual failing check:
`gateway metadata: tool call list_dataflows timed out after 60.0s`. Cycle 145
(06:01:22Z, this run's newest, finished 06:03:32Z) is still `gateway_issue`,
same failing check, `attempts: 2`. Direct path stayed fine both cycles
(metadata 200, data 200 with a sample observation). All other 11 endpoints
are `healthy` at cycle 145; `/api/contracts` `changes` is empty; no
`broken` or `capability_appeared` verdicts anywhere.

**Live recheck:** fetched the direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all`)
live during this run: HTTP 200, 37,156,652 bytes, 27.8s. Consistent with
every prior recheck of this same payload; the listing itself has not grown
pathologically or gone fully unresponsive.

**Classification:** `gateway_issue` (ours) - direct path healthy, gateway
path failing. Same root cause as all eight prior episodes: the gateway's
own ~60s call deadline for `list_dataflows` occasionally fires against
Eurostat's large, unpaginated, slow listing. Not a provider outage.

**History:** ninth data point on the ESTAT `list_dataflows` timeout
pattern (prior episodes at cycles ~54-55, then six more through episode
eight at cycles 134/136, resolved by cycle 137 and confirmed clean through
143). Every prior episode resolved within one to a few cycles; this is the
first time the pattern has held for two consecutive cycles rather than
recovering after one. Worth watching whether cycle 146 recovers or this
has become a longer-lived episode.

**Recommended action:** watch cycle 146. If still `gateway_issue` there,
treat it as an escalation of the known pattern (episode lasting 3+ cycles
for the first time) rather than routine flapping. Standing recommendation
unchanged: raise the gateway's per-call deadline for `list_dataflows`,
stream the listing, or cache the parsed result, under a future session
with code-change scope.

**Could not determine:** whether the two-cycle persistence reflects a
slower listing right now (this run's live fetch, 27.8s, is well within
prior norms and doesn't show that) or purely gateway-side load/timing
variance.

**Changed (2 of 2):** ABS `healthy` -> `gateway_issue` -> `healthy`, third
occurrence of the empty-error-body flap, already resolved.

**Cycle saw:** cycle 142 (00:01:22Z, previous run) was healthy. Cycle 143
(02:01:22Z) flipped to `gateway_issue` on `gateway metadata: Error:` (empty
error body, same shape as the two prior occurrences at cycles 26 and 134).
Cycle 144 (04:01:22Z) recovered fully and stayed healthy through cycle 145.

**Live recheck:** not applicable; the flap was already resolved two cycles
before this run started, and ABS is healthy now (all checks passing,
contracts all `ok`/`ignored` as expected).

**Classification:** `gateway_issue` while it lasted - an empty error
message on the gateway metadata path, not a provider-side failure signal
by itself.

**History:** third occurrence of this exact flap (cycle 26, cycle 134, now
cycle 143). All three resolved within one cycle. The open item from the
last run said a third occurrence would make the empty error message itself
worth fixing.

**Recommended action:** the empty-body error message
(`GatewayError` stringifying an empty `isError` payload in
`monitor/checks_gateway.py`, per the standing note) has now recurred three
times at roughly the same shape and severity. Worth fixing under
code-change scope so future occurrences carry an actual message; this
docs-only run cannot make that change.

**Could not determine:** the underlying cause of the empty-body error
itself (gateway-side exception, transient network blip, or something else)
since no code-change-scope session has inspected it yet.

## 2026-08-05T18:43Z - cycle 139

**Changed:** ESTAT `gateway_issue` -> `healthy`, seventh episode resolved.

**Cycle saw:** at cycle 136 (12:01:22Z, previous run) ESTAT was `gateway_issue`
on `gateway metadata: list_dataflows` (seventh episode, ongoing at that run).
History shows it recovered at cycle 137 (14:01:22Z) and has stayed healthy
through cycle 138 (16:01:22Z) and cycle 139 (18:01:22Z, this run's newest,
finished 18:02:15Z) - three clean cycles. All 12 endpoints are `healthy` at
cycle 139; `/api/contracts` `changes` is empty; the only non-`ok` contract
verdicts are the already-known `STATSNZ auth:listing capability_appeared`
(open since cycle 60) and `ignored` on `references:contentconstraint` for
BIS/ILO/IMF (architectural, documented in the skill). None of these are new.

**Live recheck:** cycle 139's own `gateway metadata` check for ESTAT
(`list_dataflows`) took 48,437ms and passed, under the 60s deadline that has
been tripping this pattern. Independently fetched the direct ESTAT full
dataflow listing (`.../dataflow/ESTAT/all/latest`) live during this run:
HTTP 200, 37MB in 27.8s, consistent with prior reckecks of the same payload.
Recovery looks genuine, not a monitor artifact.

**Classification:** resolved. Same known pattern as episodes one through
six: the gateway's own call deadline occasionally fires against Eurostat's
slow, unpaginated dataflow listing, working as designed rather than a bug -
but the deadline keeps getting close enough to trip that a structural fix
(raise the deadline, stream the listing, or cache the parsed result) remains
worth doing under code-change scope, which this docs-only routine cannot do.

**History:** eighth data point on the ESTAT `list_dataflows` timeout pattern
(episodes now: cycles ~54-55 note in the skill, then five more before this
run's predecessor logged episodes six and seven at cycles 134 and 136).
Every episode so far has resolved within one to a few cycles. No other
endpoint changed status between cycle 136 and cycle 139.

**Recommended action:** none needed this run; all endpoints healthy. Carry
forward the standing recommendation (raise the gateway's per-call deadline
for `list_dataflows`, stream the listing, or cache the parsed result) for a
future session with code-change scope.

**Could not determine:** whether Eurostat's listing endpoint has gotten
structurally faster or this is normal variance within the known
slow/timeout pattern; only three clean cycles have been observed since the
seventh episode.

## 2026-08-05T12:47Z - cycle 136

**Changed (1 of 2):** ESTAT `healthy` -> `gateway_issue` -> `healthy` ->
`gateway_issue`, sixth and seventh episodes.

**Cycle saw:** cycle 133 (06:01:22Z, last run) was healthy. Cycle 134
(08:01:22Z) flipped to `gateway_issue` on the same failing check as every
prior episode: `gateway metadata: tool call list_dataflows timed out after
60.0s`. Cycle 135 (10:01:22Z) recovered fully (healthy, no failing checks).
Cycle 136 (12:01:22Z, this run, the newest) is `gateway_issue` again, same
failing check, latency 60,000ms, 2 attempts. Direct path (metadata, data)
and gateway data checks stayed healthy throughout all three cycles; only
`list_dataflows` fails. Counting the fifth episode (cycles 116-119, closed
at cycle 120 per the cycle-121 entry) as the last one logged, this is a
sixth episode (cycle 134 only, 1 cycle) that fully resolved, followed by a
seventh episode (cycle 136, ongoing as of this run). No other endpoint was
non-healthy at cycle 136. `/api/contracts` `changes` shows only a cosmetic
`encoding:structure_xml` Content-Type parameter-order flip on ILO (`charset`
vs `version` ordering, verdict stays `ok`) - the same flapping category
already on record for ABS/OECD, now also seen on ILO; not re-reported per
standing guidance. The only non-`ok` contract verdict is the already-known
`STATSNZ auth:listing capability_appeared` (open since cycle 60, unchanged).

**Live recheck:** fetched the direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest?references=none`)
live during this run: HTTP 200, 37MB in 30.2s, matching every prior recheck
of the same payload (27.9-30.2s across the cycle 106, 109, 112, and 118
entries). Raw transfer time is unchanged; the bottleneck remains
parse/processing time on the gateway side stacked on top of that transfer,
occasionally pushing the combined time past the 60s deadline. (A first
attempt at this recheck hit a 65s client-side cutoff before completing;
discarded as a one-off network hiccup rather than new evidence, since the
repeat came back consistent with history.)

**Classification:** `gateway_issue` (ours), consistent with every prior
episode. Likely code site unchanged: `monitor/checks_gateway.py`
(`READ_TIMEOUT_FLOOR_S = 60.0`) wrapping `list_dataflows` in
`tools/sdmx_tools.py` against the ESTAT metadata endpoint.

**History:** seventh data point on the same recurring pattern, with a sixth
episode in between that resolved within one cycle. Still no fix has been
applied (this run remains docs-only scope).

**Recommended action:** none for this run beyond recording the pattern.
Carry the standing recommendation forward again: the next session with
broader mandate should apply one of raise the deadline, stream the listing,
or cache the parsed result.

**Could not determine:** whether the seventh episode (cycle 136) will
resolve within one cycle like the sixth, or run longer; not yet known at
the time of this run since the next cycle is not due until roughly 14:01
UTC.

---

**Changed (2 of 2):** ABS `healthy` -> `gateway_issue` -> `healthy`, second
occurrence of the empty-error-body flap.

**Cycle saw:** cycle 134 (08:01:22Z) recorded ABS `gateway_issue` with
failing check `gateway metadata: Error:` - an empty error message after the
`Error:` prefix, same shape as the sole prior occurrence at cycle 26. Cycle
135 (10:01:22Z) and cycle 136 (12:01:22Z, this run) are both healthy with no
failing checks. Direct path and gateway data check were unaffected.

**Live recheck:** not attempted; the failure was already resolved by the
time this run started (two clean cycles since), and the failing check is
against the gateway's own tool call rather than the ABS provider directly,
so there is nothing live to re-verify for a closed episode.

**Classification:** `gateway_issue` (ours) while it lasted; the error
content itself, not just the failure, is the finding. `monitor/checks_gateway.py`
raises `GatewayError(str(payload)[:500])` when the MCP tool call returns
`result.isError`, and here that payload stringified to an empty body. That
the gateway (or the underlying MCP error payload) produced no message text
on this failure path is a real gap independent of whatever transient
condition triggered the error.

**History:** second occurrence of this exact empty-error-body shape. The
skill's standing note said explicitly: if this recurs, the empty message is
itself the bug to report. It has now recurred (cycle 26, then cycle 134,
108 cycles apart at 2h per cycle, roughly 9 days). The prior occurrence is
outside this run's 48-hour `/api/history` window, so that gap is derived
from the cycle numbers alone, not re-verified against the history series.

**Recommended action:** worth a small fix under a session with code-change
scope: ensure the ABS metadata tool call path always includes a non-empty
error message when `result.isError` is set, so a future occurrence is
diagnosable from the monitor alone rather than requiring log access.

**Could not determine:** the underlying transient cause of the cycle-134
ABS metadata failure itself (only that whatever it was produced no error
text); whether this is a periodic condition worth watching for a third
occurrence, or coincidence.

## 2026-08-04T18:43Z - cycle 127

**Changed:** UNICEF `healthy` -> `degraded` -> `healthy`, flapped between runs.

**Cycle saw:** cycle 126 (started 16:01:22Z) recorded UNICEF `degraded`, with
`direct data: HTTP 429` and `direct json: HTTP 429`. Cycle 125 (14:01:22Z) and
cycle 127 (18:01:22Z, the newest at this run) both show clean `healthy` with no
failing checks. Only the direct path failed; gateway checks are not listed in
`failing` for cycle 126, so the gateway path itself was not affected.

**Live recheck:** `GET https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/dataflow/UNICEF`
returned HTTP 200 in 0.84s at 2026-08-04T18:43Z, agreeing with the monitor's
own cycle 127 recovery.

**Classification:** provider-side, resolved. Direct path only failed with
HTTP 429 (rate limiting), gateway path unaffected, so this is not a
`gateway_issue`; nothing in our code needs attention.

**History:** single-cycle event at 126, the only non-healthy UNICEF cycle in
the last 48 hours (24 cycles checked). This is a new occurrence, distinct
from the cycle-2 HTTP 503 flap already on record for UNICEF; that one is old
and this one is a different failure mode (429 vs 503).

**Recommended action:** none for now. One clean cycle since the recovery.
Watch for recurrence; a second 429 episode within a short window would
suggest the monitor's own check cadence is triggering UNICEF's rate limit
rather than a one-off provider event.

**Could not determine:** whether the 429 was UNICEF-side capacity limiting or
a shared-IP rate limit that also affects other consumers from this network.

## 2026-08-04T12:44Z - cycle 124

**Changed:** ECB `degraded` -> `healthy`, resolved.

**Cycle saw:** cycle 121's entry recorded ECB as `degraded` because four
contract assertions (`errors:missing_artefact`, `references:all`,
`references:descendants`, `references:parents`) had flipped to HTTP 504,
with a live recheck the same run already showing full recovery. That entry
flagged a watch: a second occurrence of any of those four going `broken`
would be grounds to escalate past docs-only tracking.

**Live recheck:** not needed this run; checked the monitor's own record
instead via `/api/cycle/122` and `/api/cycle/123` (the two cycles between
the last run and this one), both showing ECB `status: healthy`,
`contracts.broken: []`. Cycle 124, read directly via `/api/status`, is the
same: `healthy`, no broken contracts, plain health checks all passing.

**Classification:** provider-side, resolved. Three consecutive clean cycles
(122, 123, 124) since the cycle-121 blip, no recurrence of the four broken
assertions.

**History:** single-cycle event at 121, fully resolved by 122 onward. This
closes the watch opened in the cycle-121 entry.

**Recommended action:** none. Drop the "watch for a second occurrence" item
from state; treat a future ECB contract break as a fresh event, not a
continuation of this one.

**Could not determine:** nothing outstanding; the monitor's own historical
record for cycles 122-123 confirms the recovery independently of the
live-recheck already reported last run.

## 2026-08-04T06:43Z - cycle 121

**Changed (1 of 2):** ECB `healthy` -> `degraded`, new. This is news: ECB has
been healthy since cycle 43 and the standing assumption in this log has been
that its earlier HTTP 406 problem is closed.

**Cycle saw:** at cycle 121 (2026-08-04T06:01:22+00:00) four contract
assertions flipped from their normal fast responses to HTTP 504 after
~10.2s each: `errors:missing_artefact` (404 -> 504, "error semantics changed
from HTTP 404"), `references:all` (200 -> 504), `references:descendants`
(200 -> 504), `references:parents` (200 -> 504). A fifth assertion,
`references:contentconstraint`, came back `skipped` with
`RemoteProtocolError: Server disconnected without sending a response.`
`auth:listing` also drew a 504 but its verdict stayed `ok` (it only checks
for an auth wall, not success). The plain health checks (gateway
metadata/data, direct metadata/data/json) all stayed `ok: true` on the same
cycle, and `references:none`, `references:children`,
`references:parentsandsiblings`, `dialect:sdmx3`, `encoding:structure_xml`,
and `constraint:availableconstraint` all still passed. So this was narrow:
specific reference-heavy dataflow-structure queries against `ECB/EXR`
timing out or dropping the connection, not a wholesale ECB outage.

**Live recheck:** re-ran all four broken queries directly against
`https://data-api.ecb.europa.eu/service/dataflow/ECB/EXR/latest` at
2026-08-04T06:43Z, about 40 minutes after the cycle: `references=all` HTTP
200 in 1.18s, `references=parents` HTTP 200 in 0.92s, `references=descendants`
HTTP 200 in 0.76s, and the missing-artefact probe
(`/dataflow/ECB/NONEXISTENT_XYZ_2026/latest`) HTTP 404 in 0.63s, matching the
gateway's assumption again. `references=none` (baseline) also 200 in 0.64s.
Every one of them now passes and none are slow. Cycle saw failure, live
recheck sees full recovery: this looks transient on ECB's side rather than a
lasting break.

**Classification:** provider-side (`degraded`, contract assertions `broken`
on the direct-equivalent structure endpoint, not a `gateway_issue` - nothing
here points at gateway code). Other endpoints' checks in the same cycle
(ABS, BIS, ESTAT, FBOS, ILO, IMF, OECD, SBS, SPC, STATSNZ, UNICEF) were all
healthy, so this was not the monitor's network being generally impaired.

**History:** new as of cycle 121; `/api/history` shows ECB `healthy` with no
failing checks across every cycle from 98 through 121 (48 hours), which
confirms the underlying health-check series never flagged this - the
contract layer caught something the health checks did not.

**Recommended action:** no code change needed yet given the live recheck
passed cleanly. Watch the next 1-2 cycles for a repeat; if
`errors:missing_artefact`, `references:all`, `references:descendants`, or
`references:parents` go `broken` again against ECB, that is a second
occurrence and worth escalating past docs-only tracking.

**Could not determine:** the root cause of the ~10s ECB timeouts at cycle
121 (server-side load, a transient network path between Railway and
Frankfurt, or something else) - only that it was real at the time (four
independent assertions failing the same way) and gone by the time of this
run's recheck.

---

**Changed (2 of 2):** ESTAT `gateway_issue` -> `healthy`, recovered. Closes
out the fifth episode logged in the cycle 118 entry below.

**Cycle saw:** the fifth `gateway_issue` episode (same `gateway metadata:
tool call list_dataflows timed out after 60.0s` failure documented at cycle
118) was still failing at cycle 119 (2026-08-04T02:01:22+00:00) and
recovered at cycle 120 (2026-08-04T04:01:22+00:00); cycle 121
(2026-08-04T06:01:22+00:00, this run) is healthy too, so it has now held for
2 consecutive cycles (4 hours). Final length of the fifth episode: cycles
116-119, 4 cycles (8 hours) - longer than the fourth episode (1 cycle) and
now the joint-longest on record with the first.

**Live recheck:** not repeated this run; the cycle's own health check
already shows `list_dataflows` succeeding, and the cycle 118 entry already
confirmed the underlying direct-fetch transfer time (28s) is unchanged, so
there is nothing new to verify live for a recovery.

**Classification:** `gateway_issue` while it lasted (ours, unchanged
diagnosis from every prior episode); now resolved.

**History:** sixth data point on the same recurring pattern; still no fix
has been applied (this run remains docs-only scope). The next session with
broader mandate should still apply one of raise the deadline, stream the
listing, or cache the parsed result - deferring again only grows the count
of episodes logged without a fix.

**Recommended action:** none for this run beyond recording the recovery.
Carry the standing recommendation (deadline/streaming/caching fix) forward
again.

**Could not determine:** whether the two healthy cycles reflect a genuine
change in ESTAT's or the gateway's behavior or are within the same range of
variance as the healthy gaps between the first four episodes.

## 2026-08-04T00:43Z - cycle 118

**Changed:** ESTAT `healthy` -> `gateway_issue`, ongoing since cycle 116.

**Cycle saw:** cycle 115 was healthy (recorded in the last entry). Cycle 116
(2026-08-03T20:01:22+00:00) flipped to `gateway_issue` on the same failing
check as every prior episode: `gateway metadata: tool call list_dataflows
timed out after 60.0s`. It stayed failing at cycle 117
(2026-08-03T22:01:22+00:00) and is still failing at cycle 118
(2026-08-04T00:01:22+00:00, this run) - 3 consecutive cycles (6 hours) so
far, with no recovery yet. Cycle 118's own check detail shows 60,000ms
latency on 2 attempts, same shape as every prior timeout. The direct path
(metadata, data) and gateway data check stayed healthy throughout; only
`list_dataflows` fails. This is the fifth distinct `gateway_issue` episode
tracked in this log (first: ~7 cycles, second: 3 cycles, third: 3 cycles,
fourth: 1 cycle at cycle 113). No other endpoint changed status across
cycles 115-118; all eleven others stayed healthy throughout. `/api/contracts`
`changes` is empty at cycle 118 and no assertion is `broken` or newly
`capability_appeared`. The only non-`ok` verdict present is the same
already-known `STATSNZ auth:listing capability_appeared` (open since cycle
60, unchanged, not re-reported).

**Live recheck:** fetched the direct ESTAT full dataflow listing
(`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all/latest`)
live during this run: HTTP 200, 37MB in 28.0s, matching every prior recheck
of the same payload (27.9-29.0s in the cycle 106, 109, and 112 entries). The
raw transfer time has still not changed; the bottleneck remains
parse/processing time on the gateway side stacked on top of that transfer.

**Classification:** `gateway_issue` (ours), consistent with every prior
episode. Likely code site unchanged: `monitor/checks_gateway.py`
(`READ_TIMEOUT_FLOOR_S = 60.0`) wrapping `list_dataflows` in
`tools/sdmx_tools.py` against the ESTAT metadata endpoint.

**History:** fifth episode, cycles 116-118 and still ongoing at the time of
this run (not yet resolved), already as long as the third episode (3
cycles) and longer than the fourth (1 cycle at cycle 113). The cycle 112
entry named the next occurrence after the fourth as the trigger to apply a
fix; that occurrence (the fourth, cycle 113) came and went without a fix
under docs-only scope, and this fifth episode is now running past it without
one either.

**Recommended action:** this run is scoped to `docs/monitor/` only and does
not carry authorization to change gateway or monitor code, so no fix is
applied here. The recommendation from the cycle 112 and 115 entries stands
and is now overdue by one more episode: the next session with a broader
mandate should apply one of raise the deadline, stream the listing, or cache
the parsed result, rather than let a sixth episode accumulate under
docs-only watching.

**Could not determine:** whether this episode has already recovered by the
time this entry is read, since it was still failing as of cycle 118 and the
next cycle is not due until roughly 02:01 UTC. Also could not determine
per-cycle latency for cycles 116 and 117 specifically (only the current
cycle's check detail is exposed by `/api/status`; `/api/history` gives
status and the failure message but not latency), so the closeness of each
of those two cycles to the 60s deadline is not observable after the fact.

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
