# SDMx MCP Gateway Monitor

A standalone health monitor for the SDMx MCP gateway and its provider
endpoints. Every two hours it checks the gateway over MCP (Streamable
HTTP) and each provider both through the gateway and directly, stores the
raw results in SQLite, and serves a status page with history.

Design spec and implementation plan live in `docs/superpowers/` at the
workspace root (not in this repo's tree).

## Run locally

    cd monitor
    uv sync --extra dev
    uv run pytest                      # unit + integration tests (no network)
    uv run python scripts/live_smoke.py   # ONE real cycle, prints a table
    DB_PATH=/tmp/monitor-dev.db uv run uvicorn main:app --port 8080

Open http://localhost:8080.

## Corporate proxies

On machines behind TLS-intercepting proxies (e.g. Zscaler), direct
provider checks and the hosted-gateway connection may fail locally with
certificate errors while curl succeeds. Point `GATEWAY_URL` at a
locally-run gateway for local verification:

    uv run python main_server.py --transport http --port 8765

(run from the repo root), then:

    GATEWAY_URL=http://localhost:8765/mcp uv run python scripts/live_smoke.py

Trust the deployed monitor for real status.

## What each check verifies

Each cycle runs up to five checks per provider: two through the gateway
(`metadata`, `data`) and three direct against the provider's own SDMx
REST API (`metadata`, `data`, `json`).

- **gateway metadata**: the gateway's `list_dataflows` tool returns at
  least one dataflow for the provider.
- **gateway data**: the gateway's `probe_data_url` tool returns a
  non-empty observation for a pinned query.
- **direct metadata**: the provider's own metadata endpoint answers HTTP
  200 with a Dataflow element in the body.
- **direct data**: the provider's own data endpoint answers HTTP 200 with
  a real observation. For SDMx-CSV responses this requires a numeric
  `OBS_VALUE`, not just a 200 and a body; empty or non-numeric values do
  not count as a pass.
- **json**: the provider's SDMx-JSON endpoint answers with the right
  media type, the requested SDMx-JSON version, and a non-empty
  `dataSets` array. IMF, Eurostat, and Stats NZ do not serve SDMx-JSON,
  so their `json` checks are recorded as skipped rather than failed.

## Status page and history drill-down

The status page displays endpoint health via uptime bars. Each cell in a bar represents one check cycle, with green indicating all checks passed and red indicating at least one failure.

Hovering over a failing cell names the checks that failed and why, for example "direct metadata: HTTP 503".

Clicking any cell opens that cycle's full detail view, showing results for every endpoint with a timestamp banner and a "Back to latest" control. While viewing history, the page stops auto-refreshing and the re-check button is disabled.

For scripted access, `GET /api/cycle/{id}` serves any completed cycle; it returns HTTP 404 for an unknown or still-running cycle ID.

## API contracts

Beyond the five checks above, each cycle also asserts a set of behavioural
contracts per provider: not "does this dataflow exist" but "does the SDMx
API still behave the way the gateway assumes it behaves". An assertion is a
single probe of one such assumption, e.g. "`references=parents` returns
HTTP 200 for this provider" or "a missing artefact returns HTTP 404". Every
probe uses `detail=allstubs` so the sweep stays small: it shrinks IMF's
`references=all` response from 2.8 MB to 18 KB while still exercising the
parameter.

Each assertion resolves to one of four verdicts:

- **ok** - the provider still behaves the way the gateway assumes.
- **broken** - the provider stopped behaving the way the gateway assumes;
  a query the gateway relies on will now fail or return nothing.
- **ignored** - the provider accepts the parameter but its answer does not
  change (e.g. `references=parents` returns exactly what `references=none`
  would); the request is not rejected, but it does not do anything either.
- **capability_appeared** - the provider now supports something the gateway
  assumed it did not; not a failure, but worth knowing so the gateway's
  fallback logic can eventually be simplified.

`broken` degrades an endpoint's status (folded into the same `degraded`
state the five basic checks use); `ignored` and `capability_appeared` are
informational and never change the endpoint's health.

Each result also carries a `spec_verdict` (`conforms` or `deviates`)
judging the observation against the SDMx REST standard itself, independent
of what the gateway expects. A provider can deviate from the standard while
still matching the gateway's (already-adjusted-for-that-deviation)
expectation; the two verdicts answer different questions: "did the gateway's
assumption survive" versus "does the standard's own rule hold".

The pinned expectations live in `contracts_config.py` and were verified live
against every provider on 2026-07-25. The `/api/contracts` endpoint and the
conformance view on the status page are the living replacement for the
February 2026 snapshot in `sdmx-api-interoperability-issues.md` at the
workspace root: instead of a one-time write-up, provider conformance is
now re-checked every cycle and any drift shows up automatically.

## Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `GATEWAY_URL` | production Railway URL | MCP gateway to monitor |
| `CHECK_INTERVAL_MIN` | `120` | minutes between scheduled cycles (default 2 hours) |
| `DB_PATH` | `./data/monitor.db` | SQLite location (put on a volume) |
| `CHECK_TIMEOUT_S` | `30` | direct (non-gateway) per-request timeout, and the gateway connect timeout |
| `CALL_TIMEOUT_S` | `120` | per-call budget for a single gateway MCP round trip (`call_tool`/`tool_count`) |
| `CYCLE_TIMEOUT_S` | `900` | seconds before a cycle gives up and closes with a note |
| `SDMX_STATSNZ_KEY` | unset | Stats NZ subscription key; without it the STATSNZ direct checks are recorded as skipped |

## Cycle timeout

When a cycle exceeds its `CYCLE_TIMEOUT_S` deadline, it closes with a drift note rather than holding its lock. This exists because a hung cycle previously stopped all monitoring for over ten hours until the service was restarted. Gateway calls are individually bounded by `CALL_TIMEOUT_S`, so a provider holding a connection open cannot stall a cycle. `CALL_TIMEOUT_S` defaults to 120s because ESTAT's dataflow listing alone takes 46-50s in production; a smaller budget timed out repeatedly. Endpoints run in parallel (`asyncio.gather` in `cycle.py`), so the larger per-call budget does not threaten `CYCLE_TIMEOUT_S`: the worst case is bounded by the slowest single endpoint's sequential work (metadata check plus data check, each retried once at up to `CALL_TIMEOUT_S` per attempt), around 500s, comfortably under the 900s cycle deadline.

## Deploy on Railway

1. Railway dashboard -> the existing project -> "New Service" -> "GitHub
   repo" -> pick `Baffelan/sdmx-mcp-gateway`.
2. Service settings -> Root Directory: `monitor` (Railway then builds
   `monitor/Dockerfile`).
3. Add a Volume mounted at `/data`, and set env var `DB_PATH=/data/monitor.db`.
4. Set `SDMX_STATSNZ_KEY` if available (same value as the gateway service).
5. Settings -> Health check path: `/healthz`.
6. Generate a public domain for the service.

The monitor is read-only toward the gateway and providers; the only write
surface is `POST /api/refresh`, which is rate-limited to one manual cycle
per 5 minutes.
