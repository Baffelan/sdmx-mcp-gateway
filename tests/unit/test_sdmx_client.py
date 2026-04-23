"""
Unit tests for SDMXProgressiveClient.
"""

import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from sdmx_progressive_client import SDMXProgressiveClient


class TestSDMXProgressiveClient:
    """Test SDMX progressive client functionality."""

    @pytest.fixture
    def client(self):
        """Create SDMX client for testing."""
        return SDMXProgressiveClient(base_url="https://test.api.org/rest", agency_id="TEST")

    @pytest.fixture
    def mock_dataflow_response(self):
        """Mock SDMX dataflow response."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Dataflows>
            <str:Dataflow id="TEST_DF" agencyID="TEST" version="1.0" isFinal="true">
                <com:Name>Test Dataflow</com:Name>
                <com:Description>A test dataflow for unit testing</com:Description>
                <str:Structure>
                    <com:Ref id="TEST_DSD" agencyID="TEST" version="1.0"/>
                </str:Structure>
            </str:Dataflow>
            <str:Dataflow id="TRADE_DF" agencyID="TEST" version="2.0">
                <com:Name>Trade Data</com:Name>
                <com:Description>International trade statistics</com:Description>
            </str:Dataflow>
        </str:Dataflows>
    </str:Structures>
</str:Structure>"""

    @pytest.fixture
    def mock_codelist_response(self):
        """Mock SDMX codelist response."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Codelists>
            <str:Codelist id="REF_AREA" agencyID="TEST" version="1.0">
                <com:Name>Reference Area</com:Name>
                <str:Code id="TO">
                    <com:Name>Tonga</com:Name>
                    <com:Description>Kingdom of Tonga</com:Description>
                </str:Code>
                <str:Code id="FJ">
                    <com:Name>Fiji</com:Name>
                    <com:Description>Republic of Fiji</com:Description>
                </str:Code>
            </str:Codelist>
        </str:Codelists>
    </str:Structures>
</str:Structure>"""

    def test_init(self):
        """Test client initialization."""
        client = SDMXProgressiveClient("https://example.org/rest/", "AGENCY")
        assert client.base_url == "https://example.org/rest"  # Trailing slash removed
        assert client.agency_id == "AGENCY"
        assert client.session is None
        assert client.version_cache == {}
        assert client._cache == {}

    def test_init_with_defaults(self):
        """Test client initialization with default values."""
        # Will use values from config module
        client = SDMXProgressiveClient()
        assert client.base_url is not None
        assert client.agency_id is not None
        assert client.session is None

    @pytest.mark.asyncio
    async def test_get_session(self, client):
        """Test session creation and reuse."""
        # First call should create session
        session1 = await client._get_session()
        assert isinstance(session1, httpx.AsyncClient)
        assert client.session is session1

        # Second call should reuse session
        session2 = await client._get_session()
        assert session2 is session1

        # Cleanup
        await client.close()

    @pytest.mark.asyncio
    async def test_close(self, client):
        """Test session cleanup."""
        # Create session
        await client._get_session()
        assert client.session is not None

        # Close should clean up
        await client.close()
        assert client.session is None

    @pytest.mark.asyncio
    async def test_discover_dataflows_success(self, client, mock_dataflow_response):
        """Test successful dataflow discovery."""
        mock_response = Mock()
        mock_response.content = mock_dataflow_response.encode("utf-8")
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            result = await client.discover_dataflows()

            assert len(result) == 2

            # Check first dataflow
            df1 = result[0]
            assert df1["id"] == "TEST_DF"
            assert df1["agency"] == "TEST"
            assert df1["version"] == "1.0"
            assert df1["name"] == "Test Dataflow"
            assert df1["description"] == "A test dataflow for unit testing"

            # Check second dataflow
            df2 = result[1]
            assert df2["id"] == "TRADE_DF"
            assert df2["version"] == "2.0"

            # Check URL construction
            mock_client.get.assert_called_once()
            call_args = mock_client.get.call_args
            assert "/dataflow/TEST/all/latest" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_discover_dataflows_empty(self, client):
        """Test dataflow discovery with empty response."""
        mock_response = Mock()
        mock_response.content = b"""<?xml version="1.0"?>
        <str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
        </str:Structure>"""
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            result = await client.discover_dataflows()

            assert result == []

    @pytest.mark.asyncio
    async def test_discover_dataflows_http_error(self, client):
        """Test dataflow discovery with HTTP error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=Mock(), response=mock_response
        )

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            # Current implementation raises exceptions on HTTP errors
            with pytest.raises(httpx.HTTPStatusError):
                await client.discover_dataflows()

    @pytest.mark.asyncio
    async def test_browse_codelist_success(self, client, mock_codelist_response):
        """Test successful codelist browsing."""
        mock_response = Mock()
        mock_response.text = mock_codelist_response
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            result = await client.browse_codelist("REF_AREA")

            assert result["codelist_id"] == "REF_AREA"
            assert result["agency_id"] == "TEST"
            assert result["total_codes"] == 2

            codes = result["codes"]
            assert len(codes) == 2

            # Check Tonga code
            tonga = next(c for c in codes if c["id"] == "TO")
            assert tonga["name"] == "Tonga"
            assert tonga["description"] == "Kingdom of Tonga"

            # Check Fiji code
            fiji = next(c for c in codes if c["id"] == "FJ")
            assert fiji["name"] == "Fiji"
            assert fiji["description"] == "Republic of Fiji"

    @pytest.mark.asyncio
    async def test_browse_codelist_with_search(self, client, mock_codelist_response):
        """Test codelist browsing with search filter."""
        mock_response = Mock()
        mock_response.text = mock_codelist_response
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            # Search for "Tonga"
            result = await client.browse_codelist("REF_AREA", search_term="Tonga")

            # Should only return matching codes
            assert result["total_codes"] == 1
            assert result["codes"][0]["id"] == "TO"
            assert result["filtered_by"] == "Tonga"

    @pytest.mark.asyncio
    async def test_browse_codelist_error(self, client):
        """Test codelist browsing with error."""
        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.side_effect = httpx.ConnectError("Connection failed")
            mock_session.return_value = mock_client

            result = await client.browse_codelist("REF_AREA")

            assert result["codelist_id"] == "REF_AREA"
            assert "error" in result
            assert result["codes"] == []

    @pytest.mark.asyncio
    async def test_resolve_version_caching(self, client, mock_dataflow_response):
        """Test that version resolution is cached."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = mock_dataflow_response
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            # First call - should fetch from API
            version1 = await client.resolve_version("TEST_DF", "TEST")
            assert version1 == "1.0"

            # Second call - should use cache
            version2 = await client.resolve_version("TEST_DF", "TEST")
            assert version2 == "1.0"

            # Should have only called API once
            assert mock_client.get.call_count == 1

            # Check cache was populated
            assert ("TEST", "TEST_DF") in client.version_cache

    @pytest.mark.asyncio
    async def test_resolve_version_explicit(self, client):
        """Test that explicit version bypasses resolution."""
        # When version is explicitly provided, no API call needed
        version = await client.resolve_version("TEST_DF", "TEST", version="2.5")
        assert version == "2.5"

    def test_build_progressive_query_guide(self, client):
        """Test progressive query guide building."""
        # The method now expects a DataStructureSummary object
        from sdmx_progressive_client import DataStructureSummary, DimensionInfo

        dimensions = [
            DimensionInfo(id="FREQ", position=1, type="Dimension"),
            DimensionInfo(id="REF_AREA", position=2, type="Dimension"),
            DimensionInfo(id="INDICATOR", position=3, type="Dimension"),
        ]

        structure_summary = DataStructureSummary(
            id="TEST_DSD",
            agency="TEST",
            version="1.0",
            dimensions=dimensions,
            key_family=["FREQ", "REF_AREA", "INDICATOR"],
            attributes=[],
        )

        guide = client.build_progressive_query_guide(structure_summary)

        assert "key_structure" in guide
        assert "dimensions_order" in guide
        assert guide["dimensions_order"] == ["FREQ", "REF_AREA", "INDICATOR"]
        assert len(guide["steps"]) == 3
        assert "examples" in guide

    @pytest.mark.asyncio
    async def test_context_integration(self, client, mock_dataflow_response):
        """Test MCP Context integration with progress reporting."""
        mock_context = Mock()
        mock_context.info = AsyncMock()
        mock_context.report_progress = AsyncMock()

        mock_response = Mock()
        mock_response.content = mock_dataflow_response.encode("utf-8")
        mock_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client

            await client.discover_dataflows(ctx=mock_context)

            # Check that context methods were called
            assert mock_context.info.call_count > 0
            assert mock_context.report_progress.call_count > 0

            # Check progress reporting calls
            progress_calls = mock_context.report_progress.call_args_list
            assert any(call[0][0] == 100 for call in progress_calls)  # Ended at 100


class TestDataflowOverviewAgencyFallback:
    """get_dataflow_overview retries with agency='all' on 404.

    Motivation: OECD publishes flows under ~50 sub-agencies (e.g. the flow
    DSD_RDS_GERD@DF_GERD_SOF is owned by OECD.STI.STP, not bare OECD).
    Callers that don't know the owning sub-agency would otherwise need a
    pre-flight list_dataflows or codelist walk just to resolve one structure.
    SDMX 2.1 REST documents `all` as a valid wildcard for the `agencies`
    path parameter on /dataflow/ — this test pins that we actually use it.
    """

    @pytest.fixture
    def wildcard_response_xml(self) -> str:
        return """<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Dataflows>
            <str:Dataflow id="DSD_RDS_GERD@DF_GERD_SOF" agencyID="OECD.STI.STP" version="1.0">
                <com:Name>R&amp;D expenditure by source of funds</com:Name>
                <com:Description>Test flow owned by a sub-agency</com:Description>
                <str:Structure>
                    <Ref id="DSD_RDS_GERD" agencyID="OECD.STI.STP" version="1.0"/>
                </str:Structure>
            </str:Dataflow>
        </str:Dataflows>
    </str:Structures>
</str:Structure>"""

    @pytest.mark.asyncio
    async def test_retries_with_all_on_404_and_recovers_real_agency(
        self, wildcard_response_xml
    ):
        """A 404 on bare-agency triggers one retry with agency='all'; the
        real sub-agency is read from the XML response and surfaced on
        DataflowOverview.agency."""
        client = SDMXProgressiveClient(
            base_url="https://sdmx.oecd.org/public/rest", agency_id="OECD"
        )

        not_found_response = Mock()
        not_found_response.status_code = 404
        not_found_response.raise_for_status = Mock(
            side_effect=httpx.HTTPStatusError(
                "404", request=Mock(), response=Mock(status_code=404)
            )
        )

        ok_response = Mock()
        ok_response.status_code = 200
        ok_response.content = wildcard_response_xml.encode("utf-8")
        ok_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_http = AsyncMock()
            mock_http.get.side_effect = [not_found_response, ok_response]
            mock_session.return_value = mock_http

            overview = await client.get_dataflow_overview(
                "DSD_RDS_GERD@DF_GERD_SOF"
            )

        assert mock_http.get.call_count == 2, (
            "Expected exactly two HTTP calls — one to the configured agency, "
            "one to the 'all' wildcard fallback."
        )
        first_url = mock_http.get.call_args_list[0][0][0]
        second_url = mock_http.get.call_args_list[1][0][0]
        assert "/dataflow/OECD/DSD_RDS_GERD@DF_GERD_SOF/" in first_url
        assert "/dataflow/all/DSD_RDS_GERD@DF_GERD_SOF/" in second_url

        # Response carried the real sub-agency; overview must surface it
        # instead of the request-time 'all', otherwise downstream DSD lookups
        # target the wildcard and fail.
        assert overview.agency == "OECD.STI.STP"
        assert overview.dsd_ref is not None
        assert overview.dsd_ref["agency"] == "OECD.STI.STP"

    @pytest.mark.asyncio
    async def test_no_retry_when_first_attempt_succeeds(self, mock_dataflow_response):
        """Happy path: 200 on first attempt — no fallback call fired."""
        client = SDMXProgressiveClient(
            base_url="https://test.api.org/rest", agency_id="TEST"
        )

        ok_response = Mock()
        ok_response.status_code = 200
        ok_response.content = mock_dataflow_response.encode("utf-8")
        ok_response.raise_for_status = Mock()

        with patch.object(client, "_get_session") as mock_session:
            mock_http = AsyncMock()
            mock_http.get.return_value = ok_response
            mock_session.return_value = mock_http

            await client.get_dataflow_overview("TEST_DF")

        assert mock_http.get.call_count == 1

    @pytest.mark.asyncio
    async def test_no_retry_when_agency_already_all(self, mock_dataflow_response):
        """If the caller explicitly asked for agency='all' and the provider
        still 404s, we don't loop. The 404 propagates."""
        client = SDMXProgressiveClient(
            base_url="https://test.api.org/rest", agency_id="TEST"
        )

        not_found_response = Mock()
        not_found_response.status_code = 404
        not_found_response.raise_for_status = Mock(
            side_effect=httpx.HTTPStatusError(
                "404", request=Mock(), response=Mock(status_code=404)
            )
        )

        with patch.object(client, "_get_session") as mock_session:
            mock_http = AsyncMock()
            mock_http.get.return_value = not_found_response
            mock_session.return_value = mock_http

            with pytest.raises(httpx.HTTPStatusError):
                await client.get_dataflow_overview(
                    "GHOST_DF", agency_id="all"
                )

        assert mock_http.get.call_count == 1

    @pytest.fixture
    def mock_dataflow_response(self):
        return """<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Dataflows>
            <str:Dataflow id="TEST_DF" agencyID="TEST" version="1.0">
                <com:Name>Test Dataflow</com:Name>
            </str:Dataflow>
        </str:Dataflows>
    </str:Structures>
</str:Structure>"""


class TestClientConfiguration:
    """Test client configuration handling."""

    def test_url_normalization(self):
        """Test URL trailing slash handling."""
        client1 = SDMXProgressiveClient("https://api.org/rest/")
        assert client1.base_url == "https://api.org/rest"

        client2 = SDMXProgressiveClient("https://api.org/rest")
        assert client2.base_url == "https://api.org/rest"

    def test_agency_override(self):
        """Test agency ID can be overridden."""
        client = SDMXProgressiveClient(base_url="https://api.org/rest", agency_id="CUSTOM_AGENCY")
        assert client.agency_id == "CUSTOM_AGENCY"


class TestAuthHeaders:
    """Subscription-key header injection for keyed endpoints (e.g. STATSNZ)."""

    def test_no_endpoint_key_returns_empty(self):
        client = SDMXProgressiveClient(base_url="https://api.org/rest", agency_id="X")
        assert client._build_auth_headers() == {}

    def test_endpoint_without_auth_returns_empty(self):
        client = SDMXProgressiveClient(
            base_url="https://stats-sdmx-disseminate.pacificdata.org/rest",
            agency_id="SPC",
            endpoint_key="SPC",
        )
        assert client._build_auth_headers() == {}

    def test_keyed_endpoint_with_env_var_set(self, monkeypatch):
        monkeypatch.setenv("SDMX_STATSNZ_KEY", "test-key-abc123")
        client = SDMXProgressiveClient(
            base_url="https://api.data.stats.govt.nz/rest",
            agency_id="STATSNZ",
            endpoint_key="STATSNZ",
        )
        headers = client._build_auth_headers()
        assert headers == {"Ocp-Apim-Subscription-Key": "test-key-abc123"}

    def test_keyed_endpoint_without_env_var_warns_and_returns_empty(
        self, monkeypatch, caplog
    ):
        monkeypatch.delenv("SDMX_STATSNZ_KEY", raising=False)
        client = SDMXProgressiveClient(
            base_url="https://api.data.stats.govt.nz/rest",
            agency_id="STATSNZ",
            endpoint_key="STATSNZ",
        )
        with caplog.at_level("WARNING"):
            headers = client._build_auth_headers()
        assert headers == {}
        assert any(
            "SDMX_STATSNZ_KEY" in rec.message and rec.levelname == "WARNING"
            for rec in caplog.records
        )

    def test_unknown_endpoint_key_returns_empty(self):
        client = SDMXProgressiveClient(
            base_url="https://api.org/rest",
            agency_id="X",
            endpoint_key="NOT_A_REAL_ENDPOINT",
        )
        assert client._build_auth_headers() == {}

    @pytest.mark.asyncio
    async def test_session_carries_auth_header_when_env_set(self, monkeypatch):
        monkeypatch.setenv("SDMX_STATSNZ_KEY", "live-key-xyz")
        client = SDMXProgressiveClient(
            base_url="https://api.data.stats.govt.nz/rest",
            agency_id="STATSNZ",
            endpoint_key="STATSNZ",
        )
        session = await client._get_session()
        try:
            assert session.headers.get("Ocp-Apim-Subscription-Key") == "live-key-xyz"
        finally:
            await session.aclose()

    @pytest.mark.asyncio
    async def test_session_has_no_auth_header_for_non_keyed_endpoint(self):
        client = SDMXProgressiveClient(
            base_url="https://stats-sdmx-disseminate.pacificdata.org/rest",
            agency_id="SPC",
            endpoint_key="SPC",
        )
        session = await client._get_session()
        try:
            assert "Ocp-Apim-Subscription-Key" not in session.headers
        finally:
            await session.aclose()

    def test_default_query_params_empty_for_non_statsnz(self):
        client = SDMXProgressiveClient(
            base_url="https://stats-sdmx-disseminate.pacificdata.org/rest",
            agency_id="SPC",
            endpoint_key="SPC",
        )
        assert client._build_default_query_params() == {}

    def test_default_query_params_for_statsnz_forces_xml(self):
        client = SDMXProgressiveClient(
            base_url="https://api.data.stats.govt.nz/rest",
            agency_id="STATSNZ",
            endpoint_key="STATSNZ",
        )
        assert client._build_default_query_params() == {"format": "xml"}
