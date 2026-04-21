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
