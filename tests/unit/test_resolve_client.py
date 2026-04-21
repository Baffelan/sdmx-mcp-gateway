"""Tests for main_server._resolve_client."""

from __future__ import annotations

import asyncio

import pytest

from app_context import AppContext
from session_manager import SessionManager


@pytest.fixture
def app_ctx():
    mgr = SessionManager(default_endpoint_key="SPC")
    return AppContext(session_manager=mgr)


class _FakeCtx:
    """Minimal stand-in for mcp.Context with a lifespan_context."""

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


@pytest.mark.asyncio
async def test_resolve_client_defaults_to_session_endpoint(app_ctx):
    from main_server import _resolve_client

    ctx = _FakeCtx(app_ctx, "s1")
    client, ep_key = await _resolve_client(ctx, endpoint=None)
    assert ep_key == "SPC"
    assert client.endpoint_key == "SPC"


@pytest.mark.asyncio
async def test_resolve_client_explicit_endpoint_overrides_default(app_ctx):
    from main_server import _resolve_client

    ctx = _FakeCtx(app_ctx, "s1")
    client, ep_key = await _resolve_client(ctx, endpoint="ECB")
    assert ep_key == "ECB"
    assert client.endpoint_key == "ECB"
    # Default pointer unchanged
    session = app_ctx.get_session(ctx)
    assert session.default_endpoint_key == "SPC"


@pytest.mark.asyncio
async def test_resolve_client_unknown_endpoint_raises_with_valid_keys(app_ctx):
    from main_server import _resolve_client

    ctx = _FakeCtx(app_ctx, "s1")
    with pytest.raises(ValueError) as exc:
        await _resolve_client(ctx, endpoint="BOGUS")
    msg = str(exc.value)
    assert "BOGUS" in msg
    assert "SPC" in msg  # valid keys listed


@pytest.mark.asyncio
async def test_resolve_client_unset_default_raises_specific_message(app_ctx):
    """If default_endpoint_key is empty AND endpoint= is None, we raise a 'no default' message."""
    from main_server import _resolve_client

    ctx = _FakeCtx(app_ctx, "s1")
    session = app_ctx.get_session(ctx)
    session.default_endpoint_key = ""  # simulate a broken init
    with pytest.raises(ValueError) as exc:
        await _resolve_client(ctx, endpoint=None)
    assert "no session default" in str(exc.value).lower() or \
           "no endpoint" in str(exc.value).lower()


@pytest.mark.asyncio
async def test_resolve_client_no_app_context_falls_back_to_default_client():
    """When ctx is None or has no AppContext, fall back to the default client and 'SPC'."""
    from main_server import _resolve_client

    client, ep_key = await _resolve_client(None, endpoint=None)
    assert ep_key == "SPC"
    assert client is not None


@pytest.mark.asyncio
async def test_resolve_client_no_app_context_rejects_explicit_endpoint():
    """Explicit endpoint= with no AppContext must raise rather than silently misroute."""
    from main_server import _resolve_client

    with pytest.raises(ValueError) as exc:
        await _resolve_client(None, endpoint="ECB")
    msg = str(exc.value)
    assert "ECB" in msg
    assert "AppContext" in msg or "appcontext" in msg.lower()


@pytest.mark.asyncio
async def test_resolve_client_parallel_cross_endpoint_keeps_clients_distinct(app_ctx):
    """gather'd resolves for two endpoints produce two distinct clients, both cached."""
    from main_server import _resolve_client

    ctx = _FakeCtx(app_ctx, "s1")
    (c_spc, k_spc), (c_ecb, k_ecb) = await asyncio.gather(
        _resolve_client(ctx, endpoint="SPC"),
        _resolve_client(ctx, endpoint="ECB"),
    )
    assert (k_spc, k_ecb) == ("SPC", "ECB")
    assert c_spc is not c_ecb
    session = app_ctx.get_session(ctx)
    assert set(session.clients) == {"SPC", "ECB"}
