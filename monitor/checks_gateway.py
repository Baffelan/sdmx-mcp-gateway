"""Gateway-path checks: talk MCP to the deployed gateway like a real client.

GatewaySession wraps the official mcp client over Streamable HTTP, using our
own certifi-backed httpx.AsyncClient as the transport (the SDK's built-in
client does not reliably verify certificates in all environments). The check
functions accept any object with `async call_tool(name, args) -> dict`, so
unit tests inject a fake and CI never opens a network connection.
"""

import asyncio
import json
import time
from contextlib import AsyncExitStack

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from endpoints_config import Endpoint
from storage import CheckResult


class GatewayError(Exception):
    pass


READ_TIMEOUT_FLOOR_S = 60.0


def _read_timeout(timeout_s: float) -> float:
    """Read timeout for the MCP transport.

    Derived from the configured timeout rather than hardcoded: a fixed five
    minutes meant a 30 second configuration could still stall a cycle for far
    longer than intended. The floor keeps ordinary slow tool calls alive.
    """
    return max(float(timeout_s), READ_TIMEOUT_FLOOR_S)


async def _await_bounded(coro, budget: float, what: str):
    """Await `coro`, converting a timeout into a `GatewayError`.

    An HTTP read timeout does not fire while a server holds an SSE stream
    open without answering, so any single round trip to the gateway (an MCP
    initialize handshake, list_tools, or call_tool) can otherwise wait
    forever. Shared by every unbounded await on the MCP session.
    """
    try:
        return await asyncio.wait_for(coro, timeout=budget)
    except asyncio.TimeoutError as exc:
        raise GatewayError(what + " timed out after " + str(budget) + "s") from exc


class GatewaySession:
    def __init__(self, url: str, timeout_s: float = 30.0) -> None:
        self._url = url
        self._timeout_s = timeout_s
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "GatewaySession":
        self._stack = AsyncExitStack()
        try:
            http_client = await self._stack.enter_async_context(
                httpx.AsyncClient(
                    timeout=httpx.Timeout(self._timeout_s, read=_read_timeout(self._timeout_s)),
                    follow_redirects=True,
                )
            )
            transport = await self._stack.enter_async_context(
                streamable_http_client(self._url, http_client=http_client)
            )
            read_stream, write_stream = transport[0], transport[1]
            self._session = await self._stack.enter_async_context(
                ClientSession(read_stream, write_stream)
            )
            await self._session.initialize()
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(self, *exc_info) -> bool:
        if self._stack is not None:
            await self._stack.aclose()
        return False

    async def tool_count(self) -> int:
        assert self._session is not None
        budget = self._timeout_s * 2
        result = await _await_bounded(self._session.list_tools(), budget, "list_tools")
        return len(result.tools)

    async def call_tool(self, name: str, args: dict) -> dict:
        assert self._session is not None
        budget = self._timeout_s * 2
        result = await _await_bounded(
            self._session.call_tool(name, args), budget, "tool call " + name
        )
        payload = result.structuredContent
        if payload is None:
            text = result.content[0].text if result.content else ""
            try:
                payload = json.loads(text)
            except ValueError:
                payload = {"text": text}
        if isinstance(payload, dict) and set(payload.keys()) == {"result"}:
            payload = payload["result"]
        if result.isError:
            raise GatewayError(str(payload)[:500])
        if not isinstance(payload, dict):
            raise GatewayError("unexpected tool payload: " + str(payload)[:200])
        return payload


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def gateway_metadata_check(gw, ep: Endpoint) -> CheckResult:
    start = time.monotonic()
    try:
        payload = await gw.call_tool("list_dataflows", {"limit": 1, "endpoint": ep.key})
    except Exception as exc:
        return CheckResult(ep.key, "gateway", "metadata", ok=False,
                           latency_ms=_ms(start), error=str(exc)[:300])
    total = int(payload.get("total_found", 0))
    if total >= 1:
        return CheckResult(ep.key, "gateway", "metadata", ok=True, latency_ms=_ms(start))
    next_step = str(payload.get("next_step", ""))
    error = next_step if next_step.startswith("Error") else "gateway returned zero dataflows"
    return CheckResult(ep.key, "gateway", "metadata", ok=False,
                       latency_ms=_ms(start), error=error[:300])


async def gateway_data_check(gw, ep: Endpoint, timeout_s: float = 30.0) -> CheckResult:
    start = time.monotonic()
    try:
        payload = await gw.call_tool(
            "probe_data_url",
            {
                "data_url": ep.data_url,
                "timeout_ms": int(timeout_s * 1000),
                "sample_observations_limit": 50,
                "endpoint": ep.key,
            },
        )
    except Exception as exc:
        return CheckResult(ep.key, "gateway", "data", ok=False,
                           latency_ms=_ms(start), error=str(exc)[:300])
    status = str(payload.get("status", "missing"))
    obs = int(payload.get("observation_count", 0))
    if status == "nonempty":
        samples = payload.get("sample_observations") or []
        sample_value: str | None = None
        sample_period: str | None = None
        for sample in samples:
            if not isinstance(sample, dict) or sample.get("value") is None:
                continue
            sample_value = str(sample["value"])
            dims = sample.get("dimensions") or {}
            period = dims.get("TIME_PERIOD") if isinstance(dims, dict) else None
            sample_period = str(period) if period else None
            break
        if samples and sample_value is None and obs <= len(samples):
            return CheckResult(ep.key, "gateway", "data", ok=False, latency_ms=_ms(start),
                               obs_count=obs,
                               error="probe returned no non-null observation value")
        if sample_value is not None:
            note = None
        elif samples:
            note = "observation values not verified (sampled observations had no values)"
        else:
            note = "observation values not verified (probe returned no samples)"
        return CheckResult(ep.key, "gateway", "data", ok=True, latency_ms=_ms(start),
                           obs_count=obs, sample_value=sample_value,
                           sample_period=sample_period, error=note)
    notes = "; ".join(str(n) for n in payload.get("notes", []))
    error = ("probe status: " + status + ("; " + notes if notes else ""))[:300]
    return CheckResult(ep.key, "gateway", "data", ok=False,
                       latency_ms=_ms(start), obs_count=obs, error=error)


async def gateway_drift(gw, expected_keys: list[str]) -> list[str]:
    payload = await gw.call_tool("list_available_endpoints", {})
    gateway_keys = {
        str(e.get("key")) for e in payload.get("endpoints", []) if e.get("key")
    }
    expected = set(expected_keys)
    warnings: list[str] = []
    unmonitored = sorted(gateway_keys - expected)
    if unmonitored:
        warnings.append("gateway endpoints not monitored: " + ", ".join(unmonitored))
    missing = sorted(expected - gateway_keys)
    if missing:
        warnings.append("monitored endpoints missing from gateway: " + ", ".join(missing))
    return warnings
