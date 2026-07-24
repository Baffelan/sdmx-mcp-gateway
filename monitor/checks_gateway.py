"""Gateway-path checks: talk MCP to the deployed gateway like a real client.

GatewaySession wraps the official mcp client over Streamable HTTP. The check
functions accept any object with `async call_tool(name, args) -> dict`, so
unit tests inject a fake and CI never opens a network connection.
"""

import json
import time
from contextlib import AsyncExitStack
from datetime import timedelta

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from endpoints_config import Endpoint
from storage import CheckResult


class GatewayError(Exception):
    pass


class GatewaySession:
    def __init__(self, url: str, timeout_s: float = 30.0) -> None:
        self._url = url
        self._timeout_s = timeout_s
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    async def __aenter__(self) -> "GatewaySession":
        self._stack = AsyncExitStack()
        try:
            transport = await self._stack.enter_async_context(
                streamablehttp_client(
                    self._url, timeout=timedelta(seconds=self._timeout_s)
                )
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
        result = await self._session.list_tools()
        return len(result.tools)

    async def call_tool(self, name: str, args: dict) -> dict:
        assert self._session is not None
        result = await self._session.call_tool(name, args)
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
            {"data_url": ep.data_url, "timeout_ms": int(timeout_s * 1000)},
        )
    except Exception as exc:
        return CheckResult(ep.key, "gateway", "data", ok=False,
                           latency_ms=_ms(start), error=str(exc)[:300])
    status = str(payload.get("status", "missing"))
    obs = int(payload.get("observation_count", 0))
    if status == "nonempty":
        return CheckResult(ep.key, "gateway", "data", ok=True,
                           latency_ms=_ms(start), obs_count=obs)
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
