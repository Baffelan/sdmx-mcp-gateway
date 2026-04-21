"""Tests for SessionState client pool."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from session_manager import SessionState, _now_utc


@pytest.mark.asyncio
async def test_pool_reuse_returns_same_client():
    """Second call for the same endpoint returns the existing client."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    c1 = await state.get_or_create_client("SPC")
    c2 = await state.get_or_create_client("SPC")
    assert c1 is c2


@pytest.mark.asyncio
async def test_pool_isolation_distinct_endpoints():
    """Different endpoints yield distinct client instances."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    c_spc = await state.get_or_create_client("SPC")
    c_ecb = await state.get_or_create_client("ECB")
    assert c_spc is not c_ecb
    assert c_spc.endpoint_key == "SPC"
    assert c_ecb.endpoint_key == "ECB"


@pytest.mark.asyncio
async def test_pool_concurrent_creation_shares_client():
    """Two gather'd creations for the same endpoint share one client."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )

    from sdmx_progressive_client import SDMXProgressiveClient

    call_count = 0
    real_init = SDMXProgressiveClient.__init__

    def counting_init(self, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        real_init(self, *args, **kwargs)

    with patch.object(SDMXProgressiveClient, "__init__", counting_init):
        results = await asyncio.gather(
            state.get_or_create_client("SPC"),
            state.get_or_create_client("SPC"),
            state.get_or_create_client("SPC"),
        )

    assert results[0] is results[1] is results[2]
    assert call_count == 1


@pytest.mark.asyncio
async def test_pool_unknown_endpoint_raises():
    """get_or_create_client on an unknown key raises KeyError (from SDMX_ENDPOINTS lookup)."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    with pytest.raises(KeyError):
        await state.get_or_create_client("BOGUS")


@pytest.mark.asyncio
async def test_pool_pending_cleanup_on_failure():
    """If _build raises, pending[key] is cleared and a fresh call can succeed."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )

    from sdmx_progressive_client import SDMXProgressiveClient

    fail_first = [True]
    real_init = SDMXProgressiveClient.__init__

    def flaky_init(self, *args, **kwargs):
        if fail_first[0]:
            fail_first[0] = False
            raise RuntimeError("simulated build failure")
        real_init(self, *args, **kwargs)

    with patch.object(SDMXProgressiveClient, "__init__", flaky_init):
        with pytest.raises(RuntimeError):
            await state.get_or_create_client("SPC")
        # pending must be cleared
        assert "SPC" not in state.pending
        # Fresh call succeeds now that init no longer raises
        client = await state.get_or_create_client("SPC")
        assert client.endpoint_key == "SPC"


from session_manager import SessionManager


@pytest.mark.asyncio
async def test_switch_endpoint_is_pointer_flip():
    """switch_endpoint flips default_endpoint_key without tearing down pooled clients."""
    mgr = SessionManager(default_endpoint_key="SPC")
    # Prime the pool by touching a client on SPC
    session = mgr.get_session("s1")
    spc_client = await session.get_or_create_client("SPC")

    await mgr.switch_endpoint("ECB", session_id="s1")

    # default flipped
    assert session.default_endpoint_key == "ECB"
    # previous client still in the pool (not closed, not replaced)
    assert session.clients["SPC"] is spc_client
    # no httpx session opened yet means close() never runs; verify by
    # re-fetching the SPC client yields the same instance
    assert await session.get_or_create_client("SPC") is spc_client


@pytest.mark.asyncio
async def test_switch_endpoint_unknown_raises_with_valid_list():
    mgr = SessionManager(default_endpoint_key="SPC")
    mgr.get_session("s1")
    with pytest.raises(ValueError) as exc:
        await mgr.switch_endpoint("BOGUS", session_id="s1")
    msg = str(exc.value)
    assert "BOGUS" in msg
    assert "SPC" in msg  # valid list surfaced


@pytest.mark.asyncio
async def test_close_all_partial_failure_does_not_raise():
    """If one client close() raises, others still get closed and no exception propagates."""
    import types

    mgr = SessionManager(default_endpoint_key="SPC")
    session = mgr.get_session("s1")

    c_spc = await session.get_or_create_client("SPC")
    c_ecb = await session.get_or_create_client("ECB")

    # Fake open sessions so close() is attempted on both
    c_spc.session = object()  # type: ignore[assignment]
    c_ecb.session = object()  # type: ignore[assignment]

    closed: list[str] = []

    async def good_close(self):
        closed.append(self.endpoint_key)

    async def bad_close(self):
        closed.append(self.endpoint_key)
        raise RuntimeError("boom")

    # Patch each instance's close directly
    c_spc.close = types.MethodType(bad_close, c_spc)  # type: ignore[assignment]
    c_ecb.close = types.MethodType(good_close, c_ecb)  # type: ignore[assignment]
    await session.close()

    assert set(closed) == {"SPC", "ECB"}
    assert session.clients == {}
