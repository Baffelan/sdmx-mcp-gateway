"""One monitoring cycle: liveness, drift, all endpoint checks, storage, pruning."""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import httpx

import checks_common  # noqa: F401  # accessed as cycle.checks_common by tests
from checks_common import with_retry
from checks_direct import run_direct_checks
from checks_gateway import (
    GatewaySession,
    gateway_data_check,
    gateway_drift,
    gateway_metadata_check,
)
from endpoints_config import Endpoint
from storage import CheckResult, Store, utcnow_iso

logger = logging.getLogger(__name__)

RETENTION_DAYS = 90
USER_AGENT = "sdmx-monitor/0.1 (+https://github.com/Baffelan/sdmx-mcp-gateway)"


async def _endpoint_bundle(
    ep: Endpoint, gw: GatewaySession | None, client: httpx.AsyncClient, timeout_s: float
) -> list[CheckResult]:
    results: list[CheckResult] = []
    if gw is None:
        reason = "skipped: gateway unreachable"
        results.append(CheckResult(ep.key, "gateway", "metadata", ok=False,
                                   skipped=True, error=reason))
        results.append(CheckResult(ep.key, "gateway", "data", ok=False,
                                   skipped=True, error=reason))
    else:
        results.append(await with_retry(lambda: gateway_metadata_check(gw, ep)))
        if ep.data_url is None:
            results.append(CheckResult(ep.key, "gateway", "data", ok=False, skipped=True,
                                       error="skipped: no pinned data query"))
        else:
            results.append(
                await with_retry(lambda: gateway_data_check(gw, ep, timeout_s))
            )
    results.extend(await run_direct_checks(client, ep))
    return results


async def run_cycle(
    store: Store,
    endpoints: list[Endpoint],
    gateway_url: str,
    *,
    timeout_s: float = 30.0,
) -> int:
    started_at = utcnow_iso()
    cycle_id = store.open_cycle(started_at)
    logger.info("cycle %s started", cycle_id)

    gw = None
    gw_cm = None
    entered = False
    gateway_latency_ms: int | None = None
    start = time.monotonic()
    try:
        gw_cm = GatewaySession(gateway_url, timeout_s)
        gw = await gw_cm.__aenter__()
        entered = True
        tool_count = await gw.tool_count()
        gateway_latency_ms = int((time.monotonic() - start) * 1000)
        if tool_count == 0:
            raise RuntimeError("gateway lists zero tools")
    except Exception as exc:
        # any failure here means "gateway down"; cancellation still propagates
        logger.warning("gateway liveness failed: %s", exc)
        gw = None

    drift = ""
    cycle_error: str | None = None
    try:
        if gw is not None:
            try:
                drift = "; ".join(
                    await gateway_drift(gw, [ep.key for ep in endpoints])
                )
            except Exception as exc:
                drift = ("drift check failed: " + str(exc))[:300]

        async with httpx.AsyncClient(
            timeout=timeout_s, follow_redirects=True, headers={"User-Agent": USER_AGENT}
        ) as client:
            bundles = await asyncio.gather(
                *(_endpoint_bundle(ep, gw, client, timeout_s) for ep in endpoints)
            )
        for bundle in bundles:
            for result in bundle:
                store.add_result(cycle_id, result)
    except Exception as exc:
        # never let a mid-cycle failure leave the cycle row open forever
        logger.exception("cycle %s aborted mid-checks", cycle_id)
        cycle_error = ("cycle error: " + str(exc))[:300]
    finally:
        if gw_cm is not None and entered:
            await gw_cm.__aexit__(None, None, None)

    if cycle_error:
        drift = (drift + "; " if drift else "") + cycle_error

    store.close_cycle(cycle_id, utcnow_iso(), gw is not None, gateway_latency_ms, drift)
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    ).isoformat(timespec="seconds")
    pruned = store.prune(cutoff)
    if pruned:
        logger.info("pruned %s cycles older than %s days", pruned, RETENTION_DAYS)
    logger.info("cycle %s finished (gateway_up=%s)", cycle_id, gw is not None)
    return cycle_id
