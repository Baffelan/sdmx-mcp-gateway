"""
End-to-end smoke tests for the per-call `endpoint=` parameter wiring.

These tests drive the real MCP tool handlers registered in main_server.py
against live SDMX providers (SPC and ECB). Unlike the integration tests,
which mock SDMXProgressiveClient, these let the real client make real
HTTP calls so we can catch end-to-end wiring bugs the mocks cannot see.

Approach: "hybrid" per the smoke-test plan. We do not spawn a subprocess
and drive it over stdio; we construct a real AppContext with SessionManager
and call the @mcp.tool() handlers in-process with a minimal FakeCtx. This
exercises the real lifespan data path (_resolve_client, client pooling,
mismatch hints, switch_endpoint pointer flip) end-to-end without the MCP
transport layer.

Marked e2e + slow; opt-in only. Run with:
    uv run pytest tests/e2e/test_multiendpoint_smoke.py -v -m e2e
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
import pytest_asyncio

from app_context import AppContext
from session_manager import SessionManager


# Skip the entire module if neither SPC nor ECB is reachable so CI runs
# without internet don't mark these as failures.
def _reachable(url: str, timeout: float = 5.0) -> bool:
    try:
        # GET with short timeout; any non-network response (even 404/405)
        # proves the host is up.
        with httpx.Client(timeout=timeout, follow_redirects=True) as c:
            c.get(url)
        return True
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError):
        return False


SPC_UP = _reachable("https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC")
ECB_UP = _reachable("https://data-api.ecb.europa.eu/service/dataflow/ECB")

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.slow,
    pytest.mark.skipif(
        not (SPC_UP and ECB_UP),
        reason="SPC or ECB unreachable; skipping live multi-endpoint smoke",
    ),
]


class _FakeCtx:
    """
    Minimal stand-in for mcp.server.fastmcp.Context.

    Mirrors the shape used in tests/integration/test_cross_endpoint_tools.py:
    request_context.lifespan_context is the AppContext; request_context.session_id
    doubles as the stdio-side session identifier. The real mcp.Context exposes
    async .info() for progress logging, so we stub that too.
    """

    def __init__(self, app_ctx: AppContext, session_id: str = "default") -> None:
        class RC:
            pass

        rc = RC()
        rc.lifespan_context = app_ctx
        rc.session_id = session_id
        rc.meta = None
        self.request_context = rc
        self.session = None
        self.meta = None

    async def info(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def report_progress(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def debug(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def warning(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def error(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@pytest_asyncio.fixture
async def app_ctx():
    """Fresh AppContext per test, cleaned up afterwards."""
    mgr = SessionManager(default_endpoint_key="SPC")
    ctx = AppContext(session_manager=mgr)
    try:
        yield ctx
    finally:
        await mgr.close_all()


# ---------------------------------------------------------------------------
# Scenario 1: Default endpoint persists across calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_1_default_endpoint_persists(app_ctx):
    """get_current_endpoint() returns SPC both times when session default is SPC."""
    from main_server import get_current_endpoint

    ctx = _FakeCtx(app_ctx)

    ep1 = await get_current_endpoint(ctx=ctx)
    ep2 = await get_current_endpoint(ctx=ctx)

    assert ep1.key == "SPC"
    assert ep2.key == "SPC"
    assert ep1.agency_id == "SPC"
    assert ep2.agency_id == "SPC"


# ---------------------------------------------------------------------------
# Scenario 2: Per-call override leaves session default alone
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_2_per_call_override_preserves_session_default(app_ctx):
    """list_dataflows(endpoint='ECB') returns ECB data but leaves session on SPC."""
    from main_server import get_current_endpoint, list_dataflows

    ctx = _FakeCtx(app_ctx)

    try:
        result = await list_dataflows(limit=5, endpoint="ECB", ctx=ctx)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("ECB unreachable mid-test: " + str(e))

    assert result.agency_id == "ECB", "ECB call should report ECB agency_id"

    # Session default pointer unchanged
    ep = await get_current_endpoint(ctx=ctx)
    assert ep.key == "SPC"
    assert ep.agency_id == "SPC"

    # And both endpoints live in the pool now
    session = app_ctx.get_session(ctx)
    assert "ECB" in session.clients


# ---------------------------------------------------------------------------
# Scenario 3: Parallel cross-endpoint hits distinct base URLs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_3_parallel_cross_endpoint(app_ctx):
    """
    Concurrent list_dataflows on SPC and ECB both resolve to the right provider.

    We use list_dataflows rather than get_dataflow_structure because the former
    is cheap and every provider exposes at least one dataflow. The agency_id
    in the returned payload proves each call hit the right base URL.
    """
    from main_server import list_dataflows

    ctx = _FakeCtx(app_ctx)

    try:
        spc_result, ecb_result = await asyncio.gather(
            list_dataflows(limit=3, endpoint="SPC", ctx=ctx),
            list_dataflows(limit=3, endpoint="ECB", ctx=ctx),
        )
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("provider unreachable mid-test: " + str(e))

    assert spc_result.agency_id == "SPC"
    assert ecb_result.agency_id == "ECB"

    # Both clients in pool, distinct base_urls
    session = app_ctx.get_session(ctx)
    assert set(session.clients.keys()) >= {"SPC", "ECB"}
    assert session.clients["SPC"].base_url != session.clients["ECB"].base_url


# ---------------------------------------------------------------------------
# Scenario 4: Pool warmth — second call to warmed endpoint reuses the client
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_4_pool_reuses_warmed_client(app_ctx):
    """Second list_dataflows(endpoint='ECB') reuses the exact same client instance."""
    from main_server import list_dataflows

    ctx = _FakeCtx(app_ctx)

    try:
        await list_dataflows(limit=3, endpoint="ECB", ctx=ctx)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("ECB unreachable on first call: " + str(e))

    session = app_ctx.get_session(ctx)
    first_client = session.clients["ECB"]

    try:
        await list_dataflows(limit=3, endpoint="ECB", ctx=ctx)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("ECB unreachable on second call: " + str(e))

    second_client = session.clients["ECB"]
    assert first_client is second_client, "Pool should reuse same client instance"


# ---------------------------------------------------------------------------
# Scenario 5: switch_endpoint flips pointer + retains pool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_5_switch_endpoint_retains_pool(app_ctx):
    """switch_endpoint flips default_endpoint_key but does not tear down pooled clients."""
    from main_server import get_current_endpoint, list_dataflows, switch_endpoint

    ctx = _FakeCtx(app_ctx)

    # Warm SPC by making a real call
    try:
        await list_dataflows(limit=3, endpoint="SPC", ctx=ctx)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("SPC unreachable: " + str(e))

    session = app_ctx.get_session(ctx)
    spc_client_before_switch = session.clients["SPC"]

    # Switch to ECB
    result = await switch_endpoint(endpoint_key="ECB", ctx=ctx)
    assert result.success
    assert result.new_endpoint is not None
    assert result.new_endpoint.key == "ECB"

    current = await get_current_endpoint(ctx=ctx)
    assert current.key == "ECB"

    # SPC client still in the pool (not closed)
    assert "SPC" in session.clients, "switch_endpoint must not evict existing pooled clients"
    assert session.clients["SPC"] is spc_client_before_switch, (
        "SPC client should be the identical instance — only the pointer flipped"
    )

    # Switch back to SPC; ECB stays warm
    # (ECB client only exists if a call was made with endpoint='ECB';
    # switch_endpoint does not warm up the new endpoint eagerly, so we
    # explicitly warm it before flipping back.)
    try:
        await list_dataflows(limit=3, ctx=ctx)  # uses session default (ECB now)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("ECB unreachable after switch: " + str(e))

    assert "ECB" in session.clients

    result2 = await switch_endpoint(endpoint_key="SPC", ctx=ctx)
    assert result2.success

    current2 = await get_current_endpoint(ctx=ctx)
    assert current2.key == "SPC"

    # Both pool entries still present after the round-trip
    assert {"SPC", "ECB"}.issubset(session.clients.keys()), (
        "Both endpoints must remain in pool after switch round-trip; got "
        + str(sorted(session.clients.keys()))
    )


# ---------------------------------------------------------------------------
# Scenario 6: Mismatch hint names the known-elsewhere endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_6_mismatch_hint_sharp_path(app_ctx):
    """
    Pick an SPC dataflow via list_dataflows, then ask for it via endpoint='ECB'.
    The returned structure error should contain a hint naming 'SPC'.
    """
    from main_server import get_dataflow_structure, list_dataflows

    ctx = _FakeCtx(app_ctx)

    try:
        listing = await list_dataflows(limit=3, endpoint="SPC", ctx=ctx)
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("SPC unreachable: " + str(e))

    if not listing.dataflows:
        pytest.skip("SPC returned no dataflows; cannot seed registry")

    # The registry should have been populated by list_dataflows
    session = app_ctx.get_session(ctx)
    assert "SPC" in session.known_dataflows and session.known_dataflows["SPC"], (
        "list_dataflows should have registered at least one dataflow on SPC"
    )

    spc_df_id = listing.dataflows[0].id

    # Now ask ECB for that SPC dataflow. ECB should 404.
    try:
        result = await get_dataflow_structure(
            dataflow_id=spc_df_id, endpoint="ECB", ctx=ctx
        )
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("ECB unreachable: " + str(e))

    # next_steps should mention SPC as the known-elsewhere endpoint
    joined = " | ".join(result.next_steps)
    assert "SPC" in joined, (
        "Mismatch hint should name SPC as the known-elsewhere endpoint. "
        "next_steps=" + repr(result.next_steps)
    )


# ---------------------------------------------------------------------------
# Scenario 7: Mismatch hint generic path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_7_mismatch_hint_generic_path(app_ctx):
    """
    Asking for a never-seen dataflow should produce the generic "Registered endpoints: [...]"
    form of the mismatch hint.
    """
    from main_server import get_dataflow_structure

    ctx = _FakeCtx(app_ctx)

    try:
        result = await get_dataflow_structure(
            dataflow_id="NONEXISTENT_DF_XYZ_2026", endpoint="SPC", ctx=ctx
        )
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.TimeoutException) as e:
        pytest.skip("SPC unreachable: " + str(e))

    joined = " | ".join(result.next_steps)
    # The generic hint text lists registered endpoints
    assert "Registered endpoints" in joined, (
        "Generic mismatch hint should list registered endpoints. "
        "next_steps=" + repr(result.next_steps)
    )


# ---------------------------------------------------------------------------
# Scenario 8: Session isolation under HTTP-style session IDs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_8_session_isolation_with_distinct_ids(app_ctx):
    """
    Two FakeCtx instances with different session_ids should maintain independent
    default_endpoint_key pointers. Session A switches to ECB; session B stays on SPC.

    This simulates the HTTP transport's per-Mcp-Session-Id model without spinning
    up the HTTP server: the SessionManager is the same piece of machinery either way.
    """
    from main_server import get_current_endpoint, switch_endpoint

    ctx_a = _FakeCtx(app_ctx, session_id="session-a")
    ctx_b = _FakeCtx(app_ctx, session_id="session-b")

    # Touch both sessions so they exist
    ep_a0 = await get_current_endpoint(ctx=ctx_a)
    ep_b0 = await get_current_endpoint(ctx=ctx_b)
    assert ep_a0.key == "SPC"
    assert ep_b0.key == "SPC"

    # Session A switches to ECB
    r = await switch_endpoint(endpoint_key="ECB", ctx=ctx_a)
    assert r.success

    # Session A sees ECB; session B stays on SPC
    ep_a = await get_current_endpoint(ctx=ctx_a)
    ep_b = await get_current_endpoint(ctx=ctx_b)
    assert ep_a.key == "ECB"
    assert ep_b.key == "SPC", (
        "Session B should not be affected by session A's switch; "
        "got " + str(ep_b.key)
    )


# ---------------------------------------------------------------------------
# Scenario 9: Strict-mode raise when endpoint= passed without AppContext
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scenario_9_resolve_client_strict_raise():
    """_resolve_client(None, endpoint='ECB') raises ValueError (no AppContext)."""
    from main_server import _resolve_client

    with pytest.raises(ValueError) as exc:
        await _resolve_client(None, endpoint="ECB")

    msg = str(exc.value)
    assert "endpoint='ECB'" in msg
    assert "AppContext" in msg
