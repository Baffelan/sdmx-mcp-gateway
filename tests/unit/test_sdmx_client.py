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
        mock_context.info = Mock()
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
