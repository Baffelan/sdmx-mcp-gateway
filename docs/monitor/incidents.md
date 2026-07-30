# Monitor Incidents

Written by the `monitor-triage` routine. Newest entry first.
Untracked on purpose: scheduled runs never commit.

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
