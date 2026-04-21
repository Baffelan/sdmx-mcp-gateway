"""Reproduces the bug that motivated the per-call endpoint parameter.

Two parallel tool calls targeting different endpoints must hit the right base URLs.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from app_context import AppContext
from session_manager import SessionManager


class _FakeCtx:
    def __init__(self, app_ctx, sid="default"):
        class RC:
            pass

        rc = RC()
        rc.lifespan_context = app_ctx
        rc.session_id = sid
        rc.meta = None
        self.request_context = rc
        self.session = None
        self.meta = None


@pytest.mark.asyncio
async def test_parallel_cross_endpoint_hits_distinct_base_urls():
    """Parallel get_dataflow_structure(df, endpoint=X) and (df, endpoint=Y) resolve correctly."""
    # Use two real endpoints from the SDMX_ENDPOINTS config.
    from config import SDMX_ENDPOINTS

    available = list(SDMX_ENDPOINTS.keys())
    assert "SPC" in available, "SPC endpoint should be configured"
    # Pick a second endpoint that's configured; prefer ECB, fall back to any other.
    ep_b = "ECB" if "ECB" in available else next(k for k in available if k != "SPC")

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    seen_base_urls: list[str] = []

    async def recording_impl(client, dataflow_id, agency_id=None, ctx=None):
        seen_base_urls.append(client.base_url)
        return {
            "dataflow": {
                "id": dataflow_id,
                "agency": agency_id or client.agency_id,
                "name": dataflow_id,
                "description": "",
                "version": "1.0",
            },
            "structure": {
                "dimensions": [],
                "attributes": [],
                "measures": [],
            },
            "next_step": "",
        }

    with patch(
        "tools.sdmx_tools.get_dataflow_structure",
        side_effect=recording_impl,
    ):
        from main_server import get_dataflow_structure as handler

        await asyncio.gather(
            handler(dataflow_id="DF_A", endpoint="SPC", ctx=ctx),
            handler(dataflow_id="DF_B", endpoint=ep_b, ctx=ctx),
        )

    assert len(seen_base_urls) == 2, f"Expected 2 calls, got {len(seen_base_urls)}: {seen_base_urls}"
    assert len(set(seen_base_urls)) == 2, (
        "Both calls resolved to the same base URL! "
        f"base_urls={seen_base_urls}"
    )

    # Both endpoints are now pooled on the session
    session = app_ctx.get_session(ctx)
    assert {"SPC", ep_b}.issubset(session.clients.keys()), (
        f"Pool should contain both endpoints; got {set(session.clients.keys())}"
    )


@pytest.mark.asyncio
async def test_mismatch_hint_points_at_registered_endpoint_on_error():
    """When a dataflow is known on endpoint X, a 404 on endpoint Y returns a hint naming X."""
    from config import SDMX_ENDPOINTS

    available = list(SDMX_ENDPOINTS.keys())
    assert "SPC" in available
    ep_b = "ECB" if "ECB" in available else next(k for k in available if k != "SPC")

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    # Seed the registry: pretend we learned about DF_SHARED on SPC earlier.
    session = app_ctx.get_session(ctx)
    session.register_dataflow("SPC", "DF_SHARED")

    async def failing_impl(client, dataflow_id, agency_id=None, ctx=None):
        return {"error": "404 not found on " + client.base_url}

    with patch(
        "tools.sdmx_tools.get_dataflow_structure",
        side_effect=failing_impl,
    ):
        from main_server import get_dataflow_structure as handler

        result = await handler(dataflow_id="DF_SHARED", endpoint=ep_b, ctx=ctx)

    # next_steps should include the error AND the sharp hint pointing at SPC
    joined = "\n".join(result.next_steps)
    assert "DF_SHARED" in joined
    assert ep_b in joined or "404" in joined  # error preserved
    assert "SPC" in joined  # hint points at registered endpoint
    assert "endpoint='SPC'" in joined


@pytest.mark.asyncio
async def test_no_hint_when_error_is_not_a_404():
    """Network errors without a not-found signal should not emit a mismatch hint."""
    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def network_error_impl(client, dataflow_id, agency_id=None, ctx=None):
        return {"error": "Connection timeout after 30s"}

    with patch(
        "tools.sdmx_tools.get_dataflow_structure",
        side_effect=network_error_impl,
    ):
        from main_server import get_dataflow_structure as handler

        result = await handler(dataflow_id="DF_UNSEEN", endpoint="SPC", ctx=ctx)

    joined = "\n".join(result.next_steps)
    # Error should be preserved
    assert "Connection timeout" in joined
    # But no mismatch-hint phrasing (no "Pass endpoint=" suggestion on a network error)
    assert "Pass endpoint=" not in joined
    assert "Registered endpoints:" not in joined
