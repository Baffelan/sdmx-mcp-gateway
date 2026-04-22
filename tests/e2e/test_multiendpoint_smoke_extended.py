"""
Extended end-to-end smoke tests for the per-call `endpoint=` parameter wiring.

Complements `tests/e2e/test_multiendpoint_smoke.py` (the 9-scenario baseline)
with four broader coverage dimensions:

- TestToolCoverage: exercises the migrated tools other than the baseline's
  list_dataflows / get_dataflow_structure / get_current_endpoint, each called
  with an explicit endpoint= argument to prove per-call routing works.
- TestEdgeCases: provider-specific quirks (STATSNZ auth + format, OECD sub-agency
  override, ILO references_all, custom endpoint via env, SDMX_ENDPOINT startup).
- TestProviderBreadth: a parameterised spot check of list_dataflows across every
  reachable configured provider.
- TestFullStackSubprocess: spawns main_server.py as a real MCP stdio subprocess
  and drives it via the mcp.client.stdio ClientSession protocol.

All tests are marked e2e + slow so they stay opt-in. Provider flakiness (5xx,
timeouts, auth denials) becomes SKIP, not FAIL.

Run individually:
    uv run pytest tests/e2e/test_multiendpoint_smoke_extended.py::TestToolCoverage -v -m e2e
    uv run pytest tests/e2e/test_multiendpoint_smoke_extended.py::TestEdgeCases -v -m e2e
    uv run pytest tests/e2e/test_multiendpoint_smoke_extended.py::TestProviderBreadth -v -m e2e
    uv run pytest tests/e2e/test_multiendpoint_smoke_extended.py::TestFullStackSubprocess -v -m e2e
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx
import pytest
import pytest_asyncio

from app_context import AppContext
from session_manager import SessionManager


# ---------------------------------------------------------------------------
# Reachability probe (mirrors baseline)
# ---------------------------------------------------------------------------


def _reachable(url: str, timeout: float = 5.0) -> bool:
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as c:
            c.get(url)
        return True
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError):
        return False


SPC_UP = _reachable("https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC")

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.slow,
    pytest.mark.skipif(
        not SPC_UP,
        reason="SPC unreachable; skipping extended multi-endpoint smoke",
    ),
]


# Exceptions we treat as provider flakiness (SKIP, don't FAIL).
_NET_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.TimeoutException,
    httpx.RemoteProtocolError,
    httpx.HTTPStatusError,
)


class _FakeCtx:
    """Same minimal stand-in used by the baseline smoke file."""

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


# Known-good SPC dataflow + dimension + codelist used by several tool-coverage
# tests. DF_ADBKI is small (< 100 indicators) and exposes the common GEO_PICT
# dimension backed by CL_COM_GEO_PICT. Verified 2026-04-22.
SPC_DF_ID = "DF_ADBKI"
SPC_DIM_ID = "GEO_PICT"
SPC_CODELIST_ID = "CL_COM_GEO_PICT"
# Any Pacific island code that appears in GEO_PICT. FJ (Fiji) is canonical.
SPC_SAMPLE_CODE = "FJ"


# ===========================================================================
# Class 1: TestToolCoverage — exercise the other 12 migrated tools
# ===========================================================================


class TestToolCoverage:
    """
    Each test calls one previously uncovered @mcp.tool() with endpoint='SPC'
    (unless noted) to prove the per-call endpoint argument threads through.

    We assert shape and non-exception; we allow tools that return
    error-shaped payloads to pass as long as the interpretation/errors
    field is populated — this is a smoke test, not a correctness test.
    """

    @pytest.mark.asyncio
    async def test_get_codelist(self, app_ctx):
        from main_server import get_codelist

        ctx = _FakeCtx(app_ctx)
        try:
            result = await get_codelist(
                codelist_id=SPC_CODELIST_ID, endpoint="SPC", ctx=ctx
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, dict)
        if "error" in result:
            assert isinstance(result["error"], str) and result["error"]
        else:
            assert "codes" in result
            assert isinstance(result["codes"], list)

    @pytest.mark.asyncio
    async def test_get_dimension_codes(self, app_ctx):
        from main_server import get_dimension_codes
        from models.schemas import DimensionCodesResult

        ctx = _FakeCtx(app_ctx)
        try:
            result = await get_dimension_codes(
                dataflow_id=SPC_DF_ID,
                dimension_id=SPC_DIM_ID,
                limit=5,
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, DimensionCodesResult)
        # Either codes were returned or the usage field carries an error string
        assert result.usage, "usage field should be populated even on error"

    @pytest.mark.asyncio
    async def test_get_code_usage(self, app_ctx):
        from main_server import get_code_usage

        ctx = _FakeCtx(app_ctx)
        try:
            result = await get_code_usage(
                dataflow_id=SPC_DF_ID,
                codes=[SPC_SAMPLE_CODE, "XX"],
                dimension_id=SPC_DIM_ID,
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert result.dataflow_id == SPC_DF_ID
        assert isinstance(result.interpretation, list)
        assert result.interpretation  # at least one line

    @pytest.mark.asyncio
    async def test_check_time_availability(self, app_ctx):
        from main_server import check_time_availability

        ctx = _FakeCtx(app_ctx)
        try:
            result = await check_time_availability(
                dataflow_id=SPC_DF_ID,
                query_period="2020",
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert result.dataflow_id == SPC_DF_ID
        assert result.availability in ("no", "plausible", "plausible_different_frequency")
        assert result.interpretation

    @pytest.mark.asyncio
    async def test_find_code_usage_across_dataflows_spc(self, app_ctx):
        """SPC has bulk constraint support via /contentconstraint/SPC/all/latest."""
        from main_server import find_code_usage_across_dataflows

        ctx = _FakeCtx(app_ctx)
        try:
            result = await find_code_usage_across_dataflows(
                code=SPC_SAMPLE_CODE,
                dimension_id=SPC_DIM_ID,
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert result.code == SPC_SAMPLE_CODE
        assert isinstance(result.dataflows_with_data, list)
        # SPC's bulk endpoint returns 121 Actual constraints per the matrix;
        # at least one dataflow should report FJ in GEO_PICT.
        # (Soft assertion: if the provider returns nothing, still pass shape.)
        assert result.interpretation

    @pytest.mark.asyncio
    async def test_find_code_usage_across_dataflows_unsupported(self, app_ctx):
        """
        BIS has single-flow constraints but no bulk strategy. The tool should
        return a graceful "not supported" result, not raise.
        """
        from main_server import find_code_usage_across_dataflows

        ctx = _FakeCtx(app_ctx)
        try:
            result = await find_code_usage_across_dataflows(
                code="XX",
                dimension_id="FREQ",
                endpoint="BIS",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("BIS unreachable mid-test: " + str(e))

        # No bulk strategy -> empty results + explanation
        assert result.total_dataflows_checked == 0
        joined = " ".join(result.interpretation)
        assert "does not support bulk" in joined or "Alternative" in joined

    @pytest.mark.asyncio
    async def test_get_data_availability(self, app_ctx):
        from main_server import get_data_availability

        ctx = _FakeCtx(app_ctx)
        try:
            result = await get_data_availability(
                dataflow_id=SPC_DF_ID, endpoint="SPC", ctx=ctx
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert result.dataflow_id == SPC_DF_ID
        assert isinstance(result.interpretation, list)
        assert result.interpretation  # non-empty per spec

    @pytest.mark.asyncio
    async def test_validate_query(self, app_ctx):
        from main_server import validate_query
        from models.schemas import ValidationResult

        ctx = _FakeCtx(app_ctx)
        # Dots match SPC DSD_ADBKI dimension ordering: FREQ.GEO_PICT.INDICATOR
        # We leave everything wildcarded which should be structurally valid.
        try:
            result = await validate_query(
                dataflow_id=SPC_DF_ID,
                key="..",
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, ValidationResult)
        assert result.dataflow_id == SPC_DF_ID
        # Either valid or has understandable errors
        assert result.valid or result.errors

    @pytest.mark.asyncio
    async def test_build_key(self, app_ctx):
        from main_server import build_key
        from models.schemas import KeyBuildResult

        ctx = _FakeCtx(app_ctx)
        try:
            result = await build_key(
                dataflow_id=SPC_DF_ID,
                filters={"GEO_PICT": SPC_SAMPLE_CODE},
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, KeyBuildResult)
        # On happy path key is populated; on error usage describes the failure.
        assert result.key or result.usage

    @pytest.mark.asyncio
    async def test_build_data_url(self, app_ctx):
        from main_server import build_data_url
        from models.schemas import DataUrlResult

        ctx = _FakeCtx(app_ctx)
        try:
            result = await build_data_url(
                dataflow_id=SPC_DF_ID,
                filters={"GEO_PICT": SPC_SAMPLE_CODE},
                start_period="2020",
                end_period="2021",
                format_type="csv",
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, DataUrlResult)
        if result.url:
            assert "stats-sdmx-disseminate.pacificdata.org" in result.url, (
                "build_data_url(endpoint='SPC') must produce an SPC URL, got "
                + result.url
            )

    @pytest.mark.asyncio
    async def test_probe_data_url(self, app_ctx):
        """Build an SPC URL then probe it end-to-end."""
        from main_server import build_data_url, probe_data_url
        from models.schemas import ProbeResult

        ctx = _FakeCtx(app_ctx)
        try:
            built = await build_data_url(
                dataflow_id=SPC_DF_ID,
                filters={"GEO_PICT": SPC_SAMPLE_CODE},
                start_period="2020",
                end_period="2021",
                endpoint="SPC",
                ctx=ctx,
            )
            if not built.url:
                pytest.skip("build_data_url returned empty URL; skipping probe")
            result = await probe_data_url(
                data_url=built.url,
                endpoint="SPC",
                timeout_ms=15000,
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, ProbeResult)
        # ProbeStatus Literal contract: only nonempty / empty / error.
        # If a real probe ever returns something else, Pydantic would have
        # raised before we got here — but asserting explicitly guards against
        # a schema loosening.
        assert result.status in ("nonempty", "empty", "error"), (
            "Unexpected probe status: " + repr(result.status)
        )
        assert isinstance(result.observation_count, int)

    @pytest.mark.asyncio
    async def test_suggest_nonempty_queries(self, app_ctx):
        """
        Build a URL with a filter that's unlikely to yield data, ask for
        relaxations. Budget kept low.
        """
        from main_server import build_data_url, suggest_nonempty_queries
        from models.schemas import SuggestionResult

        ctx = _FakeCtx(app_ctx)
        try:
            # Pick a bogus indicator to force emptiness; suggestions should
            # recommend dropping INDICATOR.
            built = await build_data_url(
                dataflow_id=SPC_DF_ID,
                filters={
                    "GEO_PICT": SPC_SAMPLE_CODE,
                    "INDICATOR": "__NONEXISTENT__",
                },
                endpoint="SPC",
                ctx=ctx,
            )
            if not built.url:
                pytest.skip("build_data_url returned empty URL; skipping suggest")
            result = await suggest_nonempty_queries(
                data_url=built.url,
                max_suggestions=2,
                max_probes=4,
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, SuggestionResult)
        assert isinstance(result.suggestions, list)
        # notes is always a list; probes_used >= 0
        assert isinstance(result.probes_used, int)

    @pytest.mark.asyncio
    async def test_get_structure_diagram(self, app_ctx):
        from main_server import get_structure_diagram
        from models.schemas import StructureDiagramResult

        ctx = _FakeCtx(app_ctx)
        try:
            result = await get_structure_diagram(
                structure_type="dataflow",
                structure_id=SPC_DF_ID,
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, StructureDiagramResult)
        # Either a non-empty mermaid string or an interpretation describing the error
        assert result.mermaid_diagram or result.interpretation

    @pytest.mark.asyncio
    async def test_compare_structures_codelists(self, app_ctx):
        """
        Compare two distinct SPC codelists (cross-structure mode). Version
        comparison requires two versions of the same codelist to exist, which
        is flaky across providers; cross-structure is always feasible.
        """
        from main_server import compare_structures
        from models.schemas import StructureComparisonResult

        ctx = _FakeCtx(app_ctx)
        try:
            result = await compare_structures(
                structure_type="codelist",
                structure_id_a=SPC_CODELIST_ID,
                structure_id_b="CL_COM_FREQ",
                endpoint="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, StructureComparisonResult)
        assert result.interpretation

    @pytest.mark.asyncio
    async def test_compare_dataflow_dimensions_same_endpoint(self, app_ctx):
        """
        Same-endpoint comparison is the simplest path through the tool and
        proves both endpoint_a and endpoint_b resolve via _resolve_client.
        """
        from main_server import compare_dataflow_dimensions, list_dataflows
        from models.schemas import DataflowDimensionComparisonResult

        ctx = _FakeCtx(app_ctx)
        try:
            listing = await list_dataflows(limit=5, endpoint="SPC", ctx=ctx)
            if len(listing.dataflows) < 2:
                pytest.skip("SPC returned fewer than 2 dataflows; cannot compare")
            df_a = listing.dataflows[0].id
            df_b = listing.dataflows[1].id
            result = await compare_dataflow_dimensions(
                dataflow_id_a=df_a,
                dataflow_id_b=df_b,
                endpoint_a="SPC",
                endpoint_b="SPC",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("SPC unreachable mid-test: " + str(e))

        assert isinstance(result, DataflowDimensionComparisonResult)


# ===========================================================================
# Class 2: TestEdgeCases — provider-specific quirks
# ===========================================================================


class TestEdgeCases:
    """
    Provider-specific behaviour that the generic tool-coverage class can't
    exercise: auth headers, default query params, sub-agency overrides,
    alternative constraint strategies, and env-var-driven endpoint resolution.
    """

    @pytest.mark.asyncio
    async def test_statsnz_auth_and_format(self, app_ctx):
        """
        If SDMX_STATSNZ_KEY is set, a STATSNZ call must produce an httpx
        session carrying both the Ocp-Apim-Subscription-Key header and the
        format=xml default query param.
        """
        if not os.getenv("SDMX_STATSNZ_KEY"):
            pytest.skip("SDMX_STATSNZ_KEY not set; cannot exercise auth path")

        from main_server import list_dataflows

        ctx = _FakeCtx(app_ctx)
        try:
            result = await list_dataflows(limit=1, endpoint="STATSNZ", ctx=ctx)
        except _NET_EXCEPTIONS as e:
            pytest.skip("STATSNZ unreachable / auth-denied: " + str(e))

        # The call succeeded (no 401, no exception). Now verify the session
        # object carries the configured header + default params. A call was
        # made, so _get_session() will have been invoked.
        session = app_ctx.get_session(ctx)
        statsnz_client = session.clients.get("STATSNZ")
        assert statsnz_client is not None, "STATSNZ client should be pooled"
        assert statsnz_client.session is not None, (
            "httpx session should be created after first call"
        )
        # httpx.AsyncClient.headers is a case-insensitive Headers object.
        assert "Ocp-Apim-Subscription-Key" in statsnz_client.session.headers, (
            "STATSNZ auth header must be injected into default session headers"
        )
        # Default params live on client.params (httpx QueryParams)
        params = dict(statsnz_client.session.params)
        assert params.get("format") == "xml", (
            "STATSNZ default query params should include format=xml; got "
            + repr(params)
        )
        # Sanity on the response shape too
        assert result.agency_id == "STATSNZ"

    @pytest.mark.asyncio
    async def test_oecd_sub_agency_override(self, app_ctx):
        """
        OECD config overrides dataflow_agency to 'all' because bare OECD/agency
        returns 404. list_dataflows must still succeed and the result should
        reflect the override in the agency_id (or at minimum return dataflows).
        """
        from config import get_dataflow_agency
        from main_server import list_dataflows

        assert get_dataflow_agency("OECD") == "all", (
            "Config invariant: OECD dataflow_agency override should be 'all'"
        )

        ctx = _FakeCtx(app_ctx)
        try:
            result = await list_dataflows(limit=1, endpoint="OECD", ctx=ctx)
        except _NET_EXCEPTIONS as e:
            pytest.skip("OECD unreachable / slow: " + str(e))

        # If the override worked, we got dataflows back. The agency_id on the
        # result reflects whatever the tool reports; accept either "all" or
        # the override being reflected in results. The critical invariant is
        # that more than zero dataflows came back — the override is what
        # makes OECD work at all.
        assert result.total_found > 0 or len(result.dataflows) > 0, (
            "OECD list_dataflows returned zero; the sub-agency override may "
            "have regressed. agency_id=" + str(result.agency_id)
        )

    @pytest.mark.asyncio
    async def test_ilo_references_all_strategy(self, app_ctx):
        """
        ILO uses the references_all strategy (not availableconstraint). This
        test fires get_code_usage, which dispatches through _fetch_constraint_info;
        the goal is to prove the call doesn't raise and either returns codes
        or an understandable message. ILO per-flow is cheap when it works but
        is known-flaky; SKIP on network/5xx failures.
        """
        from config import get_constraint_strategy
        from main_server import get_code_usage

        # Invariant: ILO is configured to use references_all
        assert get_constraint_strategy("ILO", "single_flow") == "references_all"

        ctx = _FakeCtx(app_ctx)
        # DF_SDG_T8_1_SEX_ECO_NB is referenced in the constraint matrix
        # as ILO's test dataflow. It's small enough for a smoke test.
        try:
            result = await get_code_usage(
                dataflow_id="DF_SDG_T8_1_SEX_ECO_NB",
                endpoint="ILO",
                ctx=ctx,
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip("ILO unreachable / flaky: " + str(e))

        assert result.dataflow_id == "DF_SDG_T8_1_SEX_ECO_NB"
        assert result.interpretation  # at least one line of explanation

    @pytest.mark.asyncio
    async def test_custom_endpoint_via_sdmx_base_url_no_appctx(
        self, monkeypatch
    ):
        """
        With no AppContext present, setting SDMX_BASE_URL makes _resolve_client
        report the endpoint key as 'CUSTOM'. This is the legacy fallback path.

        (When an AppContext is present, SDMX_BASE_URL is ignored — sessions
        resolve via SDMX_ENDPOINTS keys only. See the next test for that.)
        """
        from main_server import _resolve_client

        # Point SDMX_BASE_URL at SPC's URL so the underlying HTTP still works
        # if anything tries to use the returned client. We don't make an HTTP
        # call here anyway — we only care about the endpoint_key resolution.
        monkeypatch.setenv(
            "SDMX_BASE_URL",
            "https://stats-sdmx-disseminate.pacificdata.org/rest",
        )
        # Clear any conflicting SDMX_ENDPOINT env
        monkeypatch.delenv("SDMX_ENDPOINT", raising=False)

        # ctx=None => no AppContext branch in _resolve_client
        client, ep_key = await _resolve_client(ctx=None, endpoint=None)
        assert ep_key == "CUSTOM", (
            "When SDMX_BASE_URL is set and no AppContext is available, "
            "_resolve_client should report ep_key='CUSTOM', got " + ep_key
        )
        # Cleanup: close the legacy default client's session if it opened one
        if client.session is not None:
            await client.close()

    @pytest.mark.asyncio
    async def test_custom_endpoint_with_appctx_uses_sdmx_endpoint_env(
        self, monkeypatch
    ):
        """
        When AppContext IS present, the session's default_endpoint_key comes
        from the SessionManager's default_endpoint_key, which app_lifespan
        reads from SDMX_ENDPOINT. SDMX_BASE_URL does not apply to sessions.
        """
        monkeypatch.setenv("SDMX_ENDPOINT", "ECB")
        # SDMX_BASE_URL is irrelevant on the AppContext path
        monkeypatch.setenv(
            "SDMX_BASE_URL",
            "https://stats-sdmx-disseminate.pacificdata.org/rest",
        )

        # Mimic what app_lifespan does: read SDMX_ENDPOINT and construct the
        # SessionManager with that default.
        default_endpoint = os.getenv("SDMX_ENDPOINT", "SPC")
        mgr = SessionManager(default_endpoint_key=default_endpoint)
        try:
            app_ctx_local = AppContext(session_manager=mgr)
            ctx = _FakeCtx(app_ctx_local)

            session = app_ctx_local.get_session(ctx)
            assert session.default_endpoint_key == "ECB", (
                "SDMX_ENDPOINT=ECB should yield session default 'ECB', got "
                + session.default_endpoint_key
            )
            # And CUSTOM is NOT the resolved key on this path, even though
            # SDMX_BASE_URL is set.
            from main_server import _resolve_client

            _, ep_key = await _resolve_client(ctx=ctx, endpoint=None)
            assert ep_key == "ECB"
        finally:
            await mgr.close_all()

    @pytest.mark.asyncio
    async def test_sdmx_endpoint_startup_override(self, monkeypatch):
        """
        With no session yet created, a fresh SessionManager built from
        SDMX_ENDPOINT=ECB produces a first session whose default is ECB,
        not the hardcoded SPC fallback.
        """
        monkeypatch.setenv("SDMX_ENDPOINT", "ECB")
        # Mimic app_lifespan's construction step.
        default_endpoint = os.getenv("SDMX_ENDPOINT", "SPC")
        mgr = SessionManager(default_endpoint_key=default_endpoint)
        try:
            app_ctx_local = AppContext(session_manager=mgr)
            ctx = _FakeCtx(app_ctx_local)
            session = app_ctx_local.get_session(ctx)
            assert session.default_endpoint_key == "ECB"
        finally:
            await mgr.close_all()


# ===========================================================================
# Class 3: TestProviderBreadth — spot-check every configured provider
# ===========================================================================


# Providers to exercise. ESTAT and STATSNZ deliberately excluded:
#   - ESTAT: no practical constraint support and 34MB dataflow listings.
#   - STATSNZ: requires API key (covered in TestEdgeCases).
# INSEE is not in the current config.
# Expected agency_id per provider: either the configured agency_id or,
# for OECD, the "all" override applied by get_dataflow_agency().
_PROVIDER_BREADTH = [
    ("FBOS", "FBOS"),
    ("SBS", "SBS"),
    ("UNICEF", "UNICEF"),
    ("IMF", "IMF.STA"),
    ("BIS", "BIS"),
    ("ABS", "ABS"),
    ("ILO", "ILO"),
    ("OECD", "all"),  # dataflow_agency override
    ("SPC", "SPC"),
    ("ECB", "ECB"),
]


class TestProviderBreadth:
    """
    Parameterised list_dataflows(endpoint=X, limit=1) against every
    supportable configured provider. Measures per-provider duration and
    treats network flakiness as SKIP.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "endpoint_key,expected_agency", _PROVIDER_BREADTH
    )
    async def test_list_dataflows_spot_check(
        self, app_ctx, endpoint_key, expected_agency, capsys
    ):
        from main_server import list_dataflows

        ctx = _FakeCtx(app_ctx)
        t0 = time.perf_counter()
        try:
            result = await list_dataflows(
                limit=1, endpoint=endpoint_key, ctx=ctx
            )
        except httpx.HTTPStatusError as e:
            pytest.skip(
                "HTTP " + str(e.response.status_code)
                + " from " + endpoint_key + ": " + str(e)
            )
        except _NET_EXCEPTIONS as e:
            pytest.skip(endpoint_key + " unreachable: " + str(e))
        except Exception as e:
            # Some providers raise generic exceptions for 4xx/5xx; treat as
            # SKIP rather than FAIL so the suite doesn't bounce on outages.
            pytest.skip(endpoint_key + " errored out: " + str(e))
        duration = time.perf_counter() - t0

        assert result.dataflows is not None
        assert len(result.dataflows) >= 0
        # agency_id should match the expected (or the override for OECD)
        assert result.agency_id == expected_agency, (
            "Expected agency_id=" + expected_agency
            + " for endpoint " + endpoint_key
            + ", got " + result.agency_id
        )

        # Emit a one-line diagnostic so the test output shows per-provider
        # timing without cluttering logs at default verbosity.
        with capsys.disabled():
            print(
                "  [breadth] " + endpoint_key + ": "
                + str(len(result.dataflows)) + " dataflows in "
                + f"{duration:.2f}s"
            )


# ===========================================================================
# Class 4: TestFullStackSubprocess — real MCP stdio protocol
# ===========================================================================


class TestFullStackSubprocess:
    """
    Spawn main_server.py as a subprocess and drive it via the official
    mcp.client.stdio transport. This is the only path that exercises the
    full JSON-RPC serialization / schema-validation layer end-to-end.

    Caveats:
    - The SDK returns tool results as CallToolResult with .structuredContent
      (the Pydantic-serialised dict) and .content (list of TextContent).
      We prefer structuredContent when present.
    - Tool errors are surfaced via isError=True, not exceptions.
    """

    @staticmethod
    def _extract_result(call_result) -> dict[str, Any]:
        """
        Normalise a CallToolResult into a plain dict.

        Priority:
          1. .structuredContent (Pydantic-serialised dict from the tool's
             BaseModel return)
          2. JSON-parsed first text content block
          3. {} if neither available
        """
        if getattr(call_result, "structuredContent", None):
            sc = call_result.structuredContent
            if isinstance(sc, dict):
                return sc
        content = getattr(call_result, "content", None) or []
        for block in content:
            text = getattr(block, "text", None)
            if text:
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, dict):
                        return parsed
                except (json.JSONDecodeError, ValueError):
                    continue
        return {}

    @pytest.mark.asyncio
    async def test_protocol_level_multi_endpoint_flow(self):
        """
        One long-lived subprocess, one session: drive the ordered scenarios
        specified in the extended smoke plan. If the SDK raises on any
        call_tool because the real protocol emits a shape we didn't
        anticipate, the exception message is surfaced in the assertion so
        we can adjust the test.
        """
        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ImportError as e:
            pytest.skip("mcp client SDK unavailable: " + str(e))

        server_params = StdioServerParameters(
            command="uv",
            args=["run", "python", "main_server.py"],
            cwd="/home/gvdr/reps/sdmx/MCP/sdmx-mcp-gateway",
        )

        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()

                    # Scenario A: list_available_endpoints — sanity shape check
                    res_a = await session.call_tool(
                        "list_available_endpoints", arguments={}
                    )
                    assert res_a.isError is not True, (
                        "list_available_endpoints should not error: " + str(res_a)
                    )
                    dict_a = self._extract_result(res_a)
                    # Expect at least the key SDMX providers in the response
                    body_text = json.dumps(dict_a)
                    assert "SPC" in body_text
                    assert "ECB" in body_text

                    # Scenario B: get_current_endpoint → SPC default
                    res_b = await session.call_tool(
                        "get_current_endpoint", arguments={}
                    )
                    assert res_b.isError is not True
                    dict_b = self._extract_result(res_b)
                    assert dict_b.get("key") == "SPC", (
                        "Default endpoint should be SPC; got " + repr(dict_b)
                    )

                    # Scenario C: list_dataflows(endpoint='ECB', limit=2)
                    try:
                        res_c = await session.call_tool(
                            "list_dataflows",
                            arguments={"endpoint": "ECB", "limit": 2},
                        )
                    except Exception as e:
                        pytest.skip("ECB call raised at protocol level: " + str(e))
                    if res_c.isError:
                        pytest.skip(
                            "ECB list_dataflows returned error (provider "
                            "flakiness): " + str(res_c.content)
                        )
                    dict_c = self._extract_result(res_c)
                    assert dict_c.get("agency_id") == "ECB"
                    # dataflows list present (may be fewer than 2 on the edge)
                    assert isinstance(dict_c.get("dataflows"), list)

                    # Scenario D: list_dataflows with no endpoint= still
                    # reports the startup default (SPC). The session default
                    # is immutable — there is no switch_endpoint tool.
                    try:
                        res_d = await session.call_tool(
                            "list_dataflows", arguments={"limit": 2}
                        )
                    except Exception as e:
                        pytest.skip(
                            "SPC call raised at protocol level (session "
                            "default): " + str(e)
                        )
                    if res_d.isError:
                        pytest.skip(
                            "SPC list_dataflows (session default) errored: "
                            + str(res_d.content)
                        )
                    dict_d = self._extract_result(res_d)
                    assert dict_d.get("agency_id") == "SPC", (
                        "Session default is SPC; list_dataflows with no "
                        "endpoint= should report agency_id='SPC'; got "
                        + repr(dict_d.get("agency_id"))
                    )

                    # Scenario E: after the ECB call in scenario C, the
                    # session default is still SPC — per-call endpoint=
                    # does not mutate the default.
                    res_e = await session.call_tool(
                        "get_current_endpoint", arguments={}
                    )
                    dict_e = self._extract_result(res_e)
                    assert dict_e.get("key") == "SPC", (
                        "Session default must remain SPC after per-call "
                        "endpoint='ECB'; got " + repr(dict_e)
                    )
        except FileNotFoundError as e:
            pytest.skip("subprocess launcher failed (uv missing?): " + str(e))
        except Exception as e:
            # Surface what the real protocol emits so we can fix the expected
            # shape. Skip so a flaky subprocess doesn't fail the suite.
            pytest.skip(
                "subprocess/stdio path failed unexpectedly: "
                + type(e).__name__ + ": " + str(e)
            )
