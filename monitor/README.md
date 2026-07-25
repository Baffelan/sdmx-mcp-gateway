# SDMx MCP Gateway Monitor

A standalone health monitor for the SDMx MCP gateway and its provider
endpoints. Every 30 minutes it checks the gateway over MCP (Streamable
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

## Configuration (env vars)

| Var | Default | Meaning |
|---|---|---|
| `GATEWAY_URL` | production Railway URL | MCP gateway to monitor |
| `CHECK_INTERVAL_MIN` | `30` | minutes between scheduled cycles |
| `DB_PATH` | `./data/monitor.db` | SQLite location (put on a volume) |
| `CHECK_TIMEOUT_S` | `30` | per-request timeout |
| `SDMX_STATSNZ_KEY` | unset | Stats NZ subscription key; without it the STATSNZ direct checks are recorded as skipped |

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
