"""
Integration tests for MCP tools.

These tests verify the SDMX tools work correctly with mocked SDMX client.
Updated to match current API signatures.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sdmx_progressive_client import (
    DataflowOverview,
    DataStructureSummary,
    DimensionInfo,
    SDMXProgressiveClient,
)
from tools.sdmx_tools import (
    build_data_url,
    build_sdmx_key,
    get_data_availability,
    get_dataflow_structure,
    get_dimension_codes,
    list_dataflows,
    validate_query,
)


class TestListDataflows:
    """Test list_dataflows tool."""

    @pytest.fixture
    def mock_dataflows(self):
        """Mock dataflow data returned by client."""
        return [
            {
                "id": "TRADE_FOOD",
                "agency": "SPC",
                "version": "1.0",
                "name": "Food Trade Statistics",
                "description": "International trade in food products",
                "is_final": True,
                "structure_reference": {"id": "TRADE_DSD", "agency": "SPC", "version": "1.0"},
            },
            {
                "id": "FISHERIES",
                "agency": "SPC",
                "version": "2.0",
                "name": "Fisheries Production",
                "description": "Commercial fishing and aquaculture data",
                "is_final": True,
                "structure_reference": {"id": "FISH_DSD", "agency": "SPC", "version": "2.0"},
            },
            {
                "id": "TOURISM_STATS",
                "agency": "SPC",
                "version": "1.5",
                "name": "Tourism Statistics",
                "description": "Visitor arrivals and tourism revenue",
                "is_final": False,
                "structure_reference": None,
            },
        ]

    @pytest.fixture
    def mock_client(self, mock_dataflows):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.discover_dataflows = AsyncMock(return_value=mock_dataflows)
        return client

    @pytest.mark.asyncio
    async def test_list_dataflows_no_keywords(self, mock_client, mock_dataflows):
        """Test listing all dataflows without keyword filtering."""
        result = await list_dataflows(client=mock_client)

        assert result["discovery_level"] == "overview"
        assert result["agency_id"] == "SPC"
        assert result["total_found"] == 3
        assert result["showing"] == 3
        assert len(result["dataflows"]) == 3
        assert "next_step" in result

    @pytest.mark.asyncio
    async def test_list_dataflows_with_keywords(self, mock_client, mock_dataflows):
        """Test listing dataflows with keyword filtering."""
        result = await list_dataflows(client=mock_client, keywords=["food", "trade"])

        # Should find TRADE_FOOD dataflow (matches both keywords)
        assert result["total_found"] == 1
        assert result["dataflows"][0]["id"] == "TRADE_FOOD"
        assert "filter_info" in result

    @pytest.mark.asyncio
    async def test_list_dataflows_no_matches(self, mock_client, mock_dataflows):
        """Test listing dataflows with keywords that don't match anything."""
        result = await list_dataflows(client=mock_client, keywords=["nonexistent"])

        assert result["total_found"] == 0
        assert len(result["dataflows"]) == 0

    @pytest.mark.asyncio
    async def test_list_dataflows_pagination(self, mock_client, mock_dataflows):
        """Test dataflow listing with pagination."""
        result = await list_dataflows(client=mock_client, limit=2, offset=0)

        assert result["showing"] == 2
        assert result["pagination"]["has_more"] is True
        assert result["pagination"]["next_offset"] == 2

    @pytest.mark.asyncio
    async def test_list_dataflows_error(self):
        """Test error handling in list_dataflows."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.discover_dataflows = AsyncMock(side_effect=Exception("Network error"))

        result = await list_dataflows(client=client)

        assert "error" in result
        assert result["dataflows"] == []
        assert "Network error" in result["error"]


class TestGetDataflowStructure:
    """Test get_dataflow_structure tool."""

    @pytest.fixture
    def mock_overview(self):
        """Mock dataflow overview."""
        return DataflowOverview(
            id="TRADE_FOOD",
            agency="SPC",
            version="1.0",
            name="Food Trade Statistics",
            description="International trade in food products",
            dsd_ref={"id": "TRADE_DSD", "agency": "SPC", "version": "1.0"},
        )

    @pytest.fixture
    def mock_structure(self):
        """Mock data structure summary."""
        return DataStructureSummary(
            id="TRADE_DSD",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(id="FREQ", position=1, type="Dimension"),
                DimensionInfo(id="REF_AREA", position=2, type="Dimension"),
                DimensionInfo(id="INDICATOR", position=3, type="Dimension"),
                DimensionInfo(id="TIME_PERIOD", position=4, type="TimeDimension"),
            ],
            key_family=["FREQ", "REF_AREA", "INDICATOR"],
            attributes=[{"id": "OBS_STATUS", "assignment_status": "Conditional"}],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_client(self, mock_overview, mock_structure):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.get_dataflow_overview = AsyncMock(return_value=mock_overview)
        client.get_structure_summary = AsyncMock(return_value=mock_structure)
        return client

    @pytest.mark.asyncio
    async def test_get_structure_success(self, mock_client):
        """Test successful dataflow structure retrieval."""
        result = await get_dataflow_structure(client=mock_client, dataflow_id="TRADE_FOOD")

        assert result["discovery_level"] == "structure"
        assert result["dataflow_id"] == "TRADE_FOOD"
        assert result["total_dimensions"] == 4
        assert len(result["structure"]["dimensions"]) == 4
        assert "next_steps" in result

    @pytest.mark.asyncio
    async def test_get_structure_invalid_dataflow_id(self, mock_client):
        """Test with invalid dataflow ID format."""
        result = await get_dataflow_structure(client=mock_client, dataflow_id="123INVALID")

        assert "error" in result
        assert "Invalid dataflow_id format" in result["error"]

    @pytest.mark.asyncio
    async def test_get_structure_not_found(self, mock_client):
        """Test error handling when structure not found."""
        mock_client.get_structure_summary = AsyncMock(return_value=None)

        result = await get_dataflow_structure(client=mock_client, dataflow_id="NONEXISTENT")

        assert "error" in result


class TestGetDimensionCodes:
    """Test get_dimension_codes tool."""

    @pytest.fixture
    def mock_codes_result(self):
        """Mock dimension codes result."""
        return {
            "dimension_id": "REF_AREA",
            "position": 2,
            "codelist": {"id": "CL_AREA", "agency": "SPC", "version": "1.0"},
            "total_codes": 3,
            "codes": [
                {"id": "TO", "name": "Tonga"},
                {"id": "FJ", "name": "Fiji"},
                {"id": "WS", "name": "Samoa"},
            ],
            "truncated": False,
            "search_term": None,
        }

    @pytest.fixture
    def mock_client(self, mock_codes_result):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.get_dimension_codes = AsyncMock(return_value=mock_codes_result)
        return client

    @pytest.mark.asyncio
    async def test_get_codes_success(self, mock_client, mock_codes_result):
        """Test successful dimension codes retrieval."""
        result = await get_dimension_codes(
            client=mock_client, dataflow_id="TRADE_FOOD", dimension_id="REF_AREA"
        )

        assert result["discovery_level"] == "codes"
        assert result["dimension_id"] == "REF_AREA"
        assert result["total_codes"] == 3
        assert len(result["codes"]) == 3
        assert "next_step" in result

    @pytest.mark.asyncio
    async def test_get_codes_pagination(self, mock_client, mock_codes_result):
        """Test dimension codes with pagination."""
        result = await get_dimension_codes(
            client=mock_client, dataflow_id="TRADE_FOOD", dimension_id="REF_AREA", limit=2
        )

        # Result comes from mock, but pagination logic is tested
        assert "pagination" in result

    @pytest.mark.asyncio
    async def test_get_codes_not_found(self, mock_client):
        """Test error handling when codes not found."""
        mock_client.get_dimension_codes = AsyncMock(return_value={"error": "Not found"})

        result = await get_dimension_codes(
            client=mock_client, dataflow_id="TEST", dimension_id="NONEXISTENT"
        )

        assert "error" in result


class TestValidateQuery:
    """Test validate_query tool."""

    @pytest.fixture
    def mock_structure(self):
        """Mock data structure summary."""
        return DataStructureSummary(
            id="TRADE_DSD",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(
                    id="FREQ",
                    position=1,
                    type="Dimension",
                    codelist_ref={"id": "CL_FREQ", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(
                    id="REF_AREA",
                    position=2,
                    type="Dimension",
                    codelist_ref={"id": "CL_AREA", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(id="TIME_PERIOD", position=3, type="TimeDimension"),
            ],
            key_family=["FREQ", "REF_AREA"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_client(self, mock_structure):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.get_structure_summary = AsyncMock(return_value=mock_structure)
        return client

    @pytest.mark.asyncio
    async def test_validate_query_valid(self, mock_client):
        """Test validation of valid query."""
        result = await validate_query(
            client=mock_client, dataflow_id="TRADE_FOOD", key="A.TO", start_period="2020"
        )

        # Should return validation result
        assert "valid" in result or "errors" in result

    @pytest.mark.asyncio
    async def test_validate_query_invalid_dataflow(self, mock_client):
        """Test validation with invalid dataflow ID."""
        result = await validate_query(client=mock_client, dataflow_id="123INVALID", key="A.TO")

        assert "error" in result or (result.get("is_valid") is False)


class TestBuildDataUrl:
    """Test build_data_url tool."""

    @pytest.fixture
    def mock_structure(self):
        """Mock data structure summary."""
        return DataStructureSummary(
            id="TRADE_DSD",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(id="FREQ", position=1, type="Dimension"),
                DimensionInfo(id="REF_AREA", position=2, type="Dimension"),
                DimensionInfo(id="TIME_PERIOD", position=3, type="TimeDimension"),
            ],
            key_family=["FREQ", "REF_AREA"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_client(self, mock_structure):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.get_structure_summary = AsyncMock(return_value=mock_structure)
        client.resolve_version = AsyncMock(return_value="1.0")
        client.base_url = "https://stats.pacificdata.org/data-nsi/rest"
        client.agency_id = "SPC"
        return client

    @pytest.mark.asyncio
    async def test_build_url_csv(self, mock_client):
        """Test building CSV data URL."""
        result = await build_data_url(
            client=mock_client,
            dataflow_id="TRADE_FOOD",
            key="A.TO",
            output_format="csv",
            start_period="2020",
            end_period="2023",
        )

        assert "url" in result
        assert "csv" in result.get("format", "") or "csv" in result.get("url", "").lower()

    @pytest.mark.asyncio
    async def test_build_url_json(self, mock_client):
        """Test building JSON data URL."""
        result = await build_data_url(
            client=mock_client, dataflow_id="TRADE_FOOD", key="A.TO", output_format="json"
        )

        assert "url" in result

    @pytest.mark.asyncio
    async def test_build_url_invalid_dataflow(self, mock_client):
        """Test URL building with invalid dataflow ID."""
        result = await build_data_url(client=mock_client, dataflow_id="123INVALID", key="A.TO")

        assert "error" in result


class TestBuildSdmxKey:
    """Test build_sdmx_key tool."""

    @pytest.fixture
    def mock_structure(self):
        """Mock data structure summary."""
        return DataStructureSummary(
            id="TRADE_DSD",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(id="FREQ", position=1, type="Dimension"),
                DimensionInfo(id="REF_AREA", position=2, type="Dimension"),
                DimensionInfo(id="INDICATOR", position=3, type="Dimension"),
            ],
            key_family=["FREQ", "REF_AREA", "INDICATOR"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_client(self, mock_structure):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.get_structure_summary = AsyncMock(return_value=mock_structure)
        client.resolve_version = AsyncMock(return_value="1.0")
        return client

    @pytest.mark.asyncio
    async def test_build_key_with_values(self, mock_client):
        """Test building SDMX key with dimension values."""
        result = await build_sdmx_key(
            client=mock_client,
            dataflow_id="TRADE_FOOD",
            filters={"FREQ": "A", "REF_AREA": "TO"},
        )

        assert "key" in result
        # Key should contain specified values
        key = result.get("key", "")
        assert "A" in key or "TO" in key

    @pytest.mark.asyncio
    async def test_build_key_all_wildcards(self, mock_client):
        """Test building SDMX key with all wildcards (empty filters)."""
        result = await build_sdmx_key(client=mock_client, dataflow_id="TRADE_FOOD", filters={})

        assert "key" in result


class TestContextIntegration:
    """Test MCP Context integration."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.discover_dataflows = AsyncMock(
            return_value=[{"id": "TEST", "name": "Test", "description": "Test dataflow"}]
        )
        return client

    @pytest.fixture
    def mock_context(self):
        """Create a mock MCP context."""
        ctx = MagicMock()
        ctx.info = AsyncMock()
        ctx.report_progress = AsyncMock()
        return ctx

    @pytest.mark.asyncio
    async def test_list_dataflows_with_context(self, mock_client, mock_context):
        """Test list_dataflows with MCP context."""
        result = await list_dataflows(client=mock_client, ctx=mock_context)

        # Should succeed regardless of context
        assert "dataflows" in result
        assert result["total_found"] == 1
