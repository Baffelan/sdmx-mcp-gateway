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

    async def info(self, *args, **kwargs):
        # The real mcp.Context exposes async info() for progress logging.
        pass


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


@pytest.mark.asyncio
async def test_get_code_usage_soft_failure_emits_sharp_hint():
    """When _fetch_constraint_info returns empty but the dataflow is known on
    another endpoint, the soft-failure early-return should append the hint."""
    from config import SDMX_ENDPOINTS

    available = list(SDMX_ENDPOINTS.keys())
    ep_b = "ECB" if "ECB" in available else next(k for k in available if k != "SPC")

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    # Pretend we've seen DF_ELSEWHERE on SPC
    session = app_ctx.get_session(ctx)
    session.register_dataflow("SPC", "DF_ELSEWHERE")

    # Force _fetch_constraint_info to return an empty _ConstraintInfo
    from main_server import _ConstraintInfo

    async def empty_fetch(client, dataflow_id, agency, endpoint_key=None):
        return _ConstraintInfo(), 1

    with patch("main_server._fetch_constraint_info", side_effect=empty_fetch):
        from main_server import get_code_usage as handler

        result = await handler(
            dataflow_id="DF_ELSEWHERE",
            endpoint=ep_b,
            ctx=ctx,
        )

    joined = "\n".join(result.interpretation)
    # "No ContentConstraint found" stays so legitimate constraint-less cases are still clear
    assert "No ContentConstraint found" in joined
    # Sharp hint names the real endpoint
    assert "SPC" in joined
    assert "endpoint='SPC'" in joined


@pytest.mark.asyncio
async def test_list_available_endpoints_marks_session_default_as_current():
    """Regression: list_available_endpoints must not AttributeError on the
    removed session.endpoint_key mirror field."""
    mgr = SessionManager(default_endpoint_key="ECB")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    from main_server import list_available_endpoints as handler

    result = await handler(ctx=ctx)

    assert result.current == "ECB"
    current_flags = [ep for ep in result.endpoints if ep.is_current]
    assert len(current_flags) == 1
    assert current_flags[0].key == "ECB"


@pytest.mark.asyncio
async def test_switch_endpoint_interactive_uses_session_path_when_available():
    """switch_endpoint_interactive must flip the session pointer, not the
    process-wide global, whenever AppContext is present. Without this, one
    user's interactive switch would affect every other concurrent session."""
    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)

    # Craft a ctx that reports no elicitation support so the tool returns
    # the non-elicitation branch without issuing a real elicit request.
    class _CtxNoElicit:
        def __init__(self, app_ctx, sid="interactive-s1"):
            class RC:
                pass

            rc = RC()
            rc.lifespan_context = app_ctx
            rc.session_id = sid
            rc.meta = None
            self.request_context = rc
            # session exists but client_params exposes no elicitation capability
            class _Session:
                client_params = None

            self.session = _Session()
            self.meta = None

        async def info(self, *a, **kw):
            pass

    ctx = _CtxNoElicit(app_ctx)

    # Seed the session on SPC
    session = app_ctx.get_session(ctx)
    assert session.default_endpoint_key == "SPC"

    from main_server import switch_endpoint_interactive as handler

    # With no elicitation support we get the "please use switch_endpoint(...)"
    # guidance response. The important regression check is the hint text
    # reflects the session's current endpoint, not a global.
    result = await handler(ctx, endpoint_key="ECB")
    assert result.success is False
    assert "SPC" in (result.hint or "")  # session's current endpoint surfaced


@pytest.mark.asyncio
async def test_switch_endpoint_session_path_does_not_AttributeError():
    """Regression: switch_endpoint's session branch must not read removed
    endpoint_name / base_url mirror fields on SessionState."""
    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    from main_server import switch_endpoint as handler

    # require_confirmation=False skips the elicit() prompt that reads
    # current_name/current_url, but the preamble still constructs them.
    result = await handler(endpoint_key="ECB", require_confirmation=False, ctx=ctx)

    assert result.success is True
    assert result.new_endpoint is not None
    assert result.new_endpoint.key == "ECB"
    # Session pointer flipped on the underlying SessionState
    assert app_ctx.get_session(ctx).default_endpoint_key == "ECB"


@pytest.mark.asyncio
async def test_validate_query_populates_suggestion_with_mismatch_hint():
    """A failed validation where the dataflow is registered elsewhere should
    put the sharp hint into ValidationResult.suggestion."""
    from config import SDMX_ENDPOINTS

    available = list(SDMX_ENDPOINTS.keys())
    ep_b = "ECB" if "ECB" in available else next(k for k in available if k != "SPC")

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    session = app_ctx.get_session(ctx)
    session.register_dataflow("SPC", "DF_ELSEWHERE")

    async def failing_validate(client, dataflow_id, key=None, filters=None,
                                start_period=None, end_period=None,
                                agency_id=None, ctx=None):
        return {
            "is_valid": False,
            "errors": [
                {
                    "type": "error",
                    "field": "dataflow_id",
                    "message": "Dataflow not found on this endpoint (404)",
                }
            ],
            "warnings": [],
        }

    with patch("tools.sdmx_tools.validate_query", side_effect=failing_validate):
        from main_server import validate_query as handler

        result = await handler(
            dataflow_id="DF_ELSEWHERE",
            key="A.B.C",
            endpoint=ep_b,
            ctx=ctx,
        )

    assert result.valid is False
    assert result.suggestion is not None
    assert "SPC" in result.suggestion
    assert "endpoint='SPC'" in result.suggestion


@pytest.mark.asyncio
async def test_compare_dataflow_dimensions_pair_hints():
    """Error in compare_dataflow_dimensions emits per-dataflow hints for whichever
    side is known on a different endpoint."""
    from config import SDMX_ENDPOINTS

    available = list(SDMX_ENDPOINTS.keys())
    ep_b = "ECB" if "ECB" in available else next(k for k in available if k != "SPC")

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    session = app_ctx.get_session(ctx)
    # Only DF_A is registered on SPC; DF_B is unknown.
    session.register_dataflow("SPC", "DF_A")

    # Simulate a 404 inside the try by forcing get_structure_summary to raise.
    from sdmx_progressive_client import SDMXProgressiveClient

    async def raise_404(*args, **kwargs):
        raise RuntimeError("404 not found")

    with patch.object(SDMXProgressiveClient, "get_structure_summary", side_effect=raise_404):
        from main_server import compare_dataflow_dimensions as handler

        result = await handler(
            dataflow_id_a="DF_A",
            dataflow_id_b="DF_B",
            endpoint_a=ep_b,
            endpoint_b=ep_b,
            ctx=ctx,
        )

    interp = "\n".join(result.interpretation)
    # Side A should get the sharp hint (DF_A known on SPC)
    assert "A:" in interp
    assert "endpoint='SPC'" in interp
    # Side B should get the generic not-found hint (DF_B unseen; error text has "404")
    assert "B:" in interp


@pytest.mark.asyncio
async def test_get_code_usage_soft_failure_no_hint_when_unregistered():
    """When a dataflow has simply no constraint and isn't known elsewhere,
    the soft-failure return should NOT append a noisy generic hint."""
    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    from main_server import _ConstraintInfo

    async def empty_fetch(client, dataflow_id, agency, endpoint_key=None):
        return _ConstraintInfo(), 1

    with patch("main_server._fetch_constraint_info", side_effect=empty_fetch):
        from main_server import get_code_usage as handler

        result = await handler(
            dataflow_id="DF_LEGITIMATELY_CONSTRAINTLESS",
            endpoint="SPC",
            ctx=ctx,
        )

    joined = "\n".join(result.interpretation)
    assert "No ContentConstraint found" in joined
    # No generic noise ("Registered endpoints: [...]") for a legitimately constraint-less dataflow
    assert "Registered endpoints:" not in joined
    assert "Pass endpoint=" not in joined


@pytest.mark.asyncio
async def test_probe_data_url_registers_dataflow_on_nonempty_success():
    """Successful probe must register the dataflow for future mismatch hints.

    Regression guard for a drift where the handler checked status == "ok"
    while the impl emits status == "nonempty" (ProbeStatus Literal).
    """
    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def nonempty_probe(
        client,
        data_url=None,
        dataflow_id=None,
        filters=None,
        start_period=None,
        end_period=None,
        sample_limit=5,
        max_distinct_per_dim=10,
        timeout_ms=10000,
        **_kwargs,  # swallow session cache kwargs added for M1
    ):
        return {
            "status": "nonempty",
            "observation_count": 3,
            "series_count": 1,
            "time_period_count": 3,
            "dimensions": {},
            "has_time_dimension": True,
            "geo_dimension_id": None,
            "sample_observations": [],
            "query_fingerprint": "test-fp",
            "notes": [],
        }

    with patch("tools.probing_tools.probe_data_url", side_effect=nonempty_probe):
        from main_server import probe_data_url as handler

        result = await handler(
            dataflow_id="DF_PROBED",
            filters={"X": "A"},
            endpoint="SPC",
            ctx=ctx,
        )

    assert result.status == "nonempty"
    session = app_ctx.get_session(ctx)
    assert "DF_PROBED" in session.known_dataflows.get("SPC", set()), (
        "Expected probe_data_url to register DF_PROBED on SPC after a "
        "nonempty success; registry has: " + repr(session.known_dataflows)
    )


@pytest.mark.asyncio
async def test_probe_cache_is_session_scoped_audit_m1():
    """Audit M1: Session A's cached probe result must not surface on
    Session B's probe call for the same URL.

    Pre-fix the probe cache was module-level, keyed on URL fingerprint +
    shaping params. Two sessions hitting the same URL meant the second
    served the first's cached payload (including sample_observations).
    Now the cache lives on SessionState; sessions are independent.
    """
    from main_server import probe_data_url as handler

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx_a = _FakeCtx(app_ctx, sid="session-A")
    ctx_b = _FakeCtx(app_ctx, sid="session-B")

    impl_calls: list[str] = []

    async def recording_probe(
        client,
        data_url=None,
        dataflow_id=None,
        filters=None,
        start_period=None,
        end_period=None,
        sample_limit=5,
        max_distinct_per_dim=10,
        timeout_ms=10000,
        probe_cache=None,
        probe_cache_lock=None,
    ):
        # Side effect: record which cache instance was threaded in so we
        # can assert each session got its own.
        impl_calls.append(
            "cache_id="
            + str(id(probe_cache) if probe_cache is not None else None)
        )
        # Actually exercise the cache so the second call-for-same-URL is a hit.
        key = "test-key"
        if probe_cache is not None and key in probe_cache:
            return probe_cache[key]
        result = {
            "status": "nonempty",
            "observation_count": 7,
            "series_count": 1,
            "time_period_count": 7,
            "dimensions": {},
            "has_time_dimension": False,
            "geo_dimension_id": None,
            "sample_observations": [],
            "query_fingerprint": "fp-" + str(id(probe_cache)),
            "notes": [],
        }
        if probe_cache is not None:
            probe_cache[key] = result
        return result

    with patch("tools.probing_tools.probe_data_url", side_effect=recording_probe):
        result_a = await handler(
            data_url="https://example/data/DF/A",
            endpoint="SPC",
            ctx=ctx_a,
        )
        result_b = await handler(
            data_url="https://example/data/DF/A",  # same URL
            endpoint="SPC",
            ctx=ctx_b,
        )

    # Both handler calls fired the impl (neither was served from a
    # cross-session cache); fingerprints differ because each session
    # had a distinct cache dict used to derive the fingerprint.
    assert len(impl_calls) == 2
    assert result_a.query_fingerprint != result_b.query_fingerprint, (
        "Expected distinct query_fingerprints from two sessions; same value "
        "would indicate the impl was served a shared cache."
    )
    # Each session's probe_cache now holds its own entry; sessions are isolated.
    session_a = app_ctx.get_session(ctx_a)
    session_b = app_ctx.get_session(ctx_b)
    assert session_a.probe_cache is not session_b.probe_cache
    assert "test-key" in session_a.probe_cache
    assert "test-key" in session_b.probe_cache
    # And the stored entries are distinct objects (from distinct calls)
    assert session_a.probe_cache["test-key"] is not session_b.probe_cache["test-key"]
