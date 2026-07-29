"""FastAPI app: status page, JSON API, and the in-process scheduler."""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse

from cycle import run_cycle
from derive import derive_status
from endpoints_config import ENDPOINTS
from storage import Store

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

GATEWAY_URL = os.getenv(
    "GATEWAY_URL", "https://sdmx-mcp-gateway-production.up.railway.app/mcp"
)
CHECK_INTERVAL_MIN = int(os.getenv("CHECK_INTERVAL_MIN", "120"))
DB_PATH = os.getenv("DB_PATH", "./data/monitor.db")
CHECK_TIMEOUT_S = float(os.getenv("CHECK_TIMEOUT_S", "30"))
CYCLE_TIMEOUT_S = float(os.getenv("CYCLE_TIMEOUT_S", "900"))
REFRESH_COOLDOWN_S = 300
STATIC_DIR = Path(__file__).parent / "static"

ENDPOINTS_BY_KEY = {ep.key: ep for ep in ENDPOINTS}


def _endpoint_payloads(store: Store, cycle: dict) -> list[dict]:
    """Per-endpoint payload for one cycle: derived status plus its raw rows."""
    contracts_by_key: dict[str, list[dict]] = {}
    for row in store.contracts_for_cycle(cycle["id"]):
        contracts_by_key.setdefault(row["endpoint_key"], []).append(row)
    by_key: dict[str, list[dict]] = {}
    for row in cycle["results"]:
        by_key.setdefault(row["endpoint_key"], []).append(row)
    payloads = []
    for key, rows in sorted(by_key.items()):
        contract_rows = contracts_by_key.get(key, [])
        status, reason = derive_status(rows, cycle["gateway_up"], contract_rows)
        ep = ENDPOINTS_BY_KEY.get(key)
        payloads.append({
            "key": key,
            "name": ep.name if ep else key,
            "status": status,
            "reason": reason,
            "last_success": store.last_success(key),
            "checks": rows,
            "contracts": {
                "total": len(contract_rows),
                "broken": [c["assertion"] for c in contract_rows
                           if c["verdict"] == "broken"],
                "informational": [c["assertion"] for c in contract_rows
                                  if c["verdict"] in ("capability_appeared", "ignored")],
                "rows": contract_rows,
            },
        })
    return payloads


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _is_stale(started_at: str | None, factor: float = 2.0) -> bool:
    if started_at is None:
        return True
    age = datetime.now(timezone.utc) - _parse_ts(started_at)
    return age > timedelta(minutes=CHECK_INTERVAL_MIN * factor)


async def _locked_cycle(app: FastAPI) -> int:
    async with app.state.cycle_lock:
        return await run_cycle(
            app.state.store, ENDPOINTS, GATEWAY_URL,
            timeout_s=CHECK_TIMEOUT_S, cycle_timeout_s=CYCLE_TIMEOUT_S,
        )


def create_app(store: Store | None = None, enable_scheduler: bool = True) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store = store if store is not None else Store(DB_PATH)
        app.state.cycle_lock = asyncio.Lock()
        app.state.last_refresh: float | None = None
        scheduler = None
        if enable_scheduler:
            scheduler = AsyncIOScheduler(timezone="UTC")
            scheduler.add_job(
                _locked_cycle, "interval", minutes=CHECK_INTERVAL_MIN, args=[app],
                max_instances=1, coalesce=True,
            )
            scheduler.start()
            latest = app.state.store.latest_cycle()
            if _is_stale(latest["started_at"] if latest else None, factor=1.0):
                logger.info("no recent cycle found; scheduling catch-up run")
                scheduler.add_job(_locked_cycle, args=[app])
        try:
            yield
        finally:
            if scheduler is not None:
                scheduler.shutdown(wait=False)
            if store is None:
                app.state.store.close()

    app = FastAPI(title="SDMx MCP Gateway Monitor", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/api/status")
    def api_status(request: Request) -> dict:
        s: Store = request.app.state.store
        latest = s.latest_cycle()
        if latest is None:
            return {
                "cycle": None,
                "stale": True,
                "check_interval_min": CHECK_INTERVAL_MIN,
                "drift": [],
                "endpoints": [],
            }
        return {
            "cycle": {
                "id": latest["id"],
                "started_at": latest["started_at"],
                "finished_at": latest["finished_at"],
                "gateway_up": latest["gateway_up"],
                "gateway_latency_ms": latest["gateway_latency_ms"],
            },
            "stale": _is_stale(latest["started_at"]),
            "check_interval_min": CHECK_INTERVAL_MIN,
            "drift": [d for d in latest["drift"].split("; ") if d],
            "endpoints": _endpoint_payloads(s, latest),
        }

    @app.get("/api/cycle/{cycle_id}")
    def api_cycle(request: Request, cycle_id: int) -> dict:
        s: Store = request.app.state.store
        cycle = s.cycle_by_id(cycle_id)
        if cycle is None:
            raise HTTPException(404, "no finished cycle with id " + str(cycle_id))
        return {
            "cycle": {
                "id": cycle["id"],
                "started_at": cycle["started_at"],
                "finished_at": cycle["finished_at"],
                "gateway_up": cycle["gateway_up"],
                "gateway_latency_ms": cycle["gateway_latency_ms"],
            },
            "endpoints": _endpoint_payloads(s, cycle),
        }

    @app.get("/api/contracts")
    def api_contracts(request: Request) -> dict:
        s: Store = request.app.state.store
        latest = s.latest_cycle()
        if latest is None:
            return {"cycle": None, "changes": [], "matrix": {}}
        rows = s.contracts_for_cycle(latest["id"])
        previous = s.previous_contract_values(latest["id"])
        changes = []
        matrix: dict[str, list[dict]] = {}
        for row in rows:
            matrix.setdefault(row["endpoint_key"], []).append(row)
            was = previous.get((row["endpoint_key"], row["assertion"]))
            if was is not None and row["observed"] is not None and was != row["observed"]:
                changes.append({
                    "endpoint_key": row["endpoint_key"],
                    "assertion": row["assertion"],
                    "was": was,
                    "now": row["observed"],
                    "verdict": row["verdict"],
                    "spec_verdict": row["spec_verdict"],
                })
        return {
            "cycle": {"id": latest["id"], "started_at": latest["started_at"]},
            "changes": changes,
            "matrix": matrix,
        }

    @app.get("/api/history")
    def api_history(request: Request, hours: int = 168) -> dict:
        hours = max(1, min(hours, 24 * 90))
        s: Store = request.app.state.store
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat(
            timespec="seconds"
        )
        series: dict[str, list[dict]] = {}
        for cycle_row in s.cycles_since(since):
            by_key: dict[str, list[dict]] = {}
            for row in cycle_row["results"]:
                by_key.setdefault(row["endpoint_key"], []).append(row)
            for key, rows in by_key.items():
                status, _ = derive_status(rows, cycle_row["gateway_up"])
                failing = []
                if status != "healthy":
                    for row in rows:
                        if row["ok"] or row["skipped"]:
                            continue
                        detail = (row["error"] or "").strip()
                        if not detail:
                            detail = ("HTTP " + str(row["http_status"])
                                      if row["http_status"] is not None
                                      else "no response")
                        failing.append(row["path"] + " " + row["kind"] + ": " + detail[:120])
                series.setdefault(key, []).append(
                    {
                        "cycle_id": cycle_row["id"],
                        "started_at": cycle_row["started_at"],
                        "status": status,
                        "failing": failing,
                    }
                )
        return {"hours": hours, "interval_min": CHECK_INTERVAL_MIN, "series": series}

    @app.post("/api/refresh")
    async def api_refresh(request: Request) -> dict:
        app_ = request.app
        now = time.monotonic()
        last = app_.state.last_refresh
        if last is not None and now - last < REFRESH_COOLDOWN_S:
            wait = int(REFRESH_COOLDOWN_S - (now - last))
            raise HTTPException(429, "cooldown: retry in " + str(wait) + "s")
        if app_.state.cycle_lock.locked():
            raise HTTPException(409, "a check cycle is already running")
        app_.state.last_refresh = now
        cycle_id = await _locked_cycle(app_)
        return {"cycle_id": cycle_id}

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
