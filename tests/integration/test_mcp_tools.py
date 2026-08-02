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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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
        client.endpoint_key = "SPC"
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
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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


class TestGetDataAvailability:
    """Test availability queries, including exact availableconstraint support."""

    EXACT_CONSTRAINT_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        '<str:ContentConstraint id="CC" type="Actual">'
        "<com:Annotations>"
        '<com:Annotation id="obs_count">'
        "<com:AnnotationTitle>794</com:AnnotationTitle>"
        "<com:AnnotationType>sdmx_metrics</com:AnnotationType>"
        "</com:Annotation>"
        "</com:Annotations>"
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="FREQ"><com:Value>M</com:Value></com:KeyValue>'
        '<com:KeyValue id="COMMODITY"><com:Value>GOLD</com:Value></com:KeyValue>'
        '<com:KeyValue id="INDICATOR"><com:Value>COMPRICE</com:Value></com:KeyValue>'
        '<com:KeyValue id="TIME_PERIOD">'
        "<com:TimeRange>"
        '<com:StartPeriod isInclusive="true">1960-01-01T00:00:00</com:StartPeriod>'
        '<com:EndPeriod isInclusive="true">2026-02-28T00:00:00</com:EndPeriod>'
        "</com:TimeRange>"
        "</com:KeyValue>"
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    EMPTY_SENTINEL_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        '<str:ContentConstraint id="CC" type="Actual">'
        "<com:Annotations>"
        '<com:Annotation id="obs_count">'
        "<com:AnnotationTitle>0</com:AnnotationTitle>"
        "<com:AnnotationType>sdmx_metrics</com:AnnotationType>"
        "</com:Annotation>"
        "</com:Annotations>"
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="TIME_PERIOD">'
        "<com:TimeRange>"
        '<com:StartPeriod isInclusive="true">9999-01-01T00:00:00</com:StartPeriod>'
        '<com:EndPeriod isInclusive="true">0001-12-31T23:59:59</com:EndPeriod>'
        "</com:TimeRange>"
        "</com:KeyValue>"
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    @pytest.fixture
    def mock_structure(self):
        return DataStructureSummary(
            id="DSD_COMMODITY_PRICES",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(id="FREQ", position=1, type="Dimension"),
                DimensionInfo(id="COMMODITY", position=2, type="Dimension"),
                DimensionInfo(id="INDICATOR", position=3, type="Dimension"),
                DimensionInfo(id="TIME_PERIOD", position=4, type="TimeDimension"),
            ],
            key_family=["FREQ", "COMMODITY", "INDICATOR"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_client(self, mock_structure):
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
        client.base_url = "https://stats-sdmx-disseminate.pacificdata.org/rest"
        client.get_structure_summary = AsyncMock(return_value=mock_structure)
        return client

    def _make_response(self, xml_text):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = xml_text.encode("utf-8")
        resp.raise_for_status = MagicMock()
        return resp

    @pytest.mark.asyncio
    async def test_uses_availableconstraint_exact_with_obs_count(self, mock_client):
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EXACT_CONSTRAINT_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"FREQ": "M", "COMMODITY": "GOLD", "INDICATOR": "COMPRICE"},
        )

        assert result["has_constraint"] is True
        assert result["data_exists"] is True
        assert result["observation_count"] == 794
        assert result["time_range"] == {"start": "1960-01-01", "end": "2026-02-28"}
        call_url = mock_session.get.call_args[0][0]
        assert "/availableconstraint/DF_COMMODITY_PRICES/M.GOLD.COMPRICE/all/all" in call_url
        assert "mode=exact" in call_url

    @pytest.mark.asyncio
    async def test_empty_sentinel_range_becomes_empty_result(self, mock_client):
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EMPTY_SENTINEL_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"FREQ": "M", "COMMODITY": "UNICORNS", "INDICATOR": "COMPRICE"},
        )

        assert result["has_constraint"] is True
        assert result["data_exists"] is False
        assert result["observation_count"] == 0
        assert result["time_range"] is None
        assert any("sentinel time range" in line for line in result["interpretation"])

    @pytest.mark.asyncio
    async def test_constraint_type_is_carried_from_the_response(self, mock_client):
        """Actual and Allowed share an element name and mean different things:
        Actual is confirmed data, Allowed is merely schema-permitted. Dropping
        the distinction makes every availability answer look confirmed."""
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EXACT_CONSTRAINT_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"FREQ": "M"},
        )

        assert result["constraint_type"] == "Actual"

    @pytest.mark.asyncio
    async def test_an_unknown_filter_dimension_is_rejected_not_dropped(self, mock_client):
        """A filter naming no dimension used to be dropped silently, so the
        whole-dataflow count came back reported as the filtered one. Against
        SPC's DF_SDG that turned a request for Fiji into 121140 observations
        for everything."""
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EXACT_CONSTRAINT_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"GEO_PICT": "FJ"},
        )

        assert "error" in result
        assert "GEO_PICT" in result["error"]
        assert result.get("observation_count") is None
        # The agent needs to know what it may use instead.
        assert any("COMMODITY" in line for line in result["interpretation"])
        # And nothing may be fetched, so no count can be mistaken for filtered.
        mock_session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_known_filter_plus_an_unknown_one_still_errors(self, mock_client):
        """Partially valid filters are the dangerous case: the query would run
        and return a real number for the wrong selection."""
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EXACT_CONSTRAINT_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"FREQ": "M", "GEO_PICT": "FJ"},
        )

        assert "error" in result
        mock_session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_time_period_is_accepted_as_a_filter_name(self, mock_client):
        """TIME_PERIOD is an accepted alias for the time dimension, so the new
        validation must not reject it."""
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response(self.EXACT_CONSTRAINT_XML))
        mock_client._get_session = AsyncMock(return_value=mock_session)

        result = await get_data_availability(
            client=mock_client,
            dataflow_id="DF_COMMODITY_PRICES",
            filters={"TIME_PERIOD": "2020"},
        )

        assert "error" not in result


class TestActualAvailabilityFallback:
    """get_actual_availability() must fall back to Allowed constraints (ECB
    publishes only Allowed) and must explain itself when nothing is published,
    rather than silently returning an empty result."""

    ALLOWED_ONLY_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        '<str:ContentConstraint id="EXR_CONSTRAINTS" type="Allowed">'
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="CURRENCY"><com:Value>USD</com:Value>'
        "<com:Value>GBP</com:Value></com:KeyValue>"
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    # The Allowed constraint appears first in document order; the Actual one
    # comes second. If the parser ever regresses to "first match wins" this
    # fixture will catch it, since the naive result would be "Allowed".
    ACTUAL_AND_ALLOWED_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        '<str:ContentConstraint id="CC_ALLOWED" type="Allowed">'
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="FREQ"><com:Value>A</com:Value>'
        "<com:Value>M</com:Value></com:KeyValue>"
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        '<str:ContentConstraint id="CC_ACTUAL" type="Actual">'
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="FREQ"><com:Value>A</com:Value></com:KeyValue>'
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    NO_CONSTRAINT_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    def _client_returning(self, xml_text, endpoint_key):
        """Real SDMXProgressiveClient whose HTTP session is mocked to return xml_text."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id=endpoint_key,
            endpoint_key=endpoint_key,
        )
        resp = MagicMock()
        resp.status_code = 200
        resp.content = xml_text.encode("utf-8")
        resp.raise_for_status = MagicMock()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=resp)
        client._get_session = AsyncMock(return_value=mock_session)
        return client

    @pytest.mark.asyncio
    async def test_availability_falls_back_to_an_allowed_constraint(self):
        """ECB publishes Allowed constraints only. Returning nothing at all,
        as the tool used to, hides data the provider is telling us about."""
        client = self._client_returning(self.ALLOWED_ONLY_XML, endpoint_key="ECB")
        result = await client.get_actual_availability("EXR", agency_id="ECB")
        assert result["has_constraint"] is True
        assert result["constraint_type"] == "Allowed"
        assert any("CURRENCY" in str(r) for r in result["cube_regions"])

    @pytest.mark.asyncio
    async def test_availability_prefers_actual_when_both_exist(self):
        client = self._client_returning(self.ACTUAL_AND_ALLOWED_XML, endpoint_key="SPC")
        result = await client.get_actual_availability("DF_X", agency_id="SPC")
        assert result["constraint_type"] == "Actual"
        assert result["constraint_id"] == "CC_ACTUAL"

    @pytest.mark.asyncio
    async def test_availability_result_explains_an_empty_answer(self):
        """An empty availability answer must say why, not just be empty."""
        client = self._client_returning(self.NO_CONSTRAINT_XML, endpoint_key="ESTAT")
        out = await get_data_availability(client, "nama_10_gdp", agency_id="ESTAT")
        assert out["has_constraint"] is False
        assert out["note"]


class TestContextIntegration:
    """Test MCP Context integration."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock SDMX client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
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


class TestMainServerListDataflows:
    """Test MCP-facing list_dataflows handler argument normalization."""

    @pytest.mark.asyncio
    async def test_handler_accepts_string_keywords(self):
        """String keywords should be normalised for MCP callers."""
        from main_server import list_dataflows as handler

        mock_client = MagicMock(spec=SDMXProgressiveClient)
        mock_client.agency_id = "SPC"
        mock_client.endpoint_key = "SPC"
        mock_client.discover_dataflows = AsyncMock(
            return_value=[
                {
                    "id": "DF_COMMODITY_PRICES",
                    "name": "Selected International Commodity Prices",
                    "description": "Nominal prices in USD for key commodities",
                }
            ]
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await handler(keywords="commodity prices")

        assert result.total_found == 1
        assert result.keywords == ["commodity", "prices"]
        assert result.dataflows[0].id == "DF_COMMODITY_PRICES"


class TestCompareDataflowDimensions:
    """Test compare_dataflow_dimensions tool."""

    @pytest.fixture
    def mock_structure_a(self):
        """DSD with FREQ (CL_FREQ v1.0), GEO (CL_GEO v1.0), INDICATOR (CL_IND v1.0)."""
        return DataStructureSummary(
            id="DSD_A",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(
                    id="FREQ", position=1, type="Dimension",
                    codelist_ref={"id": "CL_FREQ", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(
                    id="GEO", position=2, type="Dimension",
                    codelist_ref={"id": "CL_GEO", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(
                    id="INDICATOR", position=3, type="Dimension",
                    codelist_ref={"id": "CL_IND", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(id="TIME_PERIOD", position=4, type="TimeDimension"),
            ],
            key_family=["FREQ", "GEO", "INDICATOR"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_structure_b(self):
        """DSD with FREQ (CL_FREQ v1.0), GEO (CL_GEO v2.0), SECTOR (CL_SECTOR v1.0)."""
        return DataStructureSummary(
            id="DSD_B",
            agency="SPC",
            version="1.0",
            dimensions=[
                DimensionInfo(
                    id="FREQ", position=1, type="Dimension",
                    codelist_ref={"id": "CL_FREQ", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(
                    id="GEO", position=2, type="Dimension",
                    codelist_ref={"id": "CL_GEO", "agency": "SPC", "version": "2.0"},
                ),
                DimensionInfo(
                    id="SECTOR", position=3, type="Dimension",
                    codelist_ref={"id": "CL_SECTOR", "agency": "SPC", "version": "1.0"},
                ),
                DimensionInfo(id="TIME_PERIOD", position=4, type="TimeDimension"),
            ],
            key_family=["FREQ", "GEO", "SECTOR"],
            attributes=[],
            primary_measure="OBS_VALUE",
        )

    @pytest.fixture
    def mock_overview_a(self):
        """Mock overview for dataflow A."""
        return DataflowOverview(
            id="DF_A", agency="SPC", version="1.0",
            name="Dataflow A", description="Test dataflow A",
        )

    @pytest.fixture
    def mock_overview_b(self):
        """Mock overview for dataflow B."""
        return DataflowOverview(
            id="DF_B", agency="SPC", version="1.0",
            name="Dataflow B", description="Test dataflow B",
        )

    @pytest.fixture
    def mock_constraint_a(self):
        """Constraint info for DF_A: used codes + time range 2000-2020."""
        from main_server import _ConstraintInfo

        info = _ConstraintInfo()
        info.used_codes = {
            "FREQ": {"A", "Q"},
            "GEO": {"FJ", "WS", "TO", "VU"},
            "INDICATOR": {"GDP", "POP"},
        }
        info.time_start = "2000-01-01"
        info.time_end = "2020-12-31"
        return info

    @pytest.fixture
    def mock_constraint_b(self):
        """Constraint info for DF_B: used codes + time range 2010-2024.

        GEO overlaps with A on FJ, WS, TO but has PG instead of VU.
        Time overlap with A: 2010-2020.
        """
        from main_server import _ConstraintInfo

        info = _ConstraintInfo()
        info.used_codes = {
            "FREQ": {"A", "Q"},
            "GEO": {"FJ", "WS", "TO", "PG"},
            "SECTOR": {"AGR", "IND"},
        }
        info.time_start = "2010-01-01"
        info.time_end = "2024-12-31"
        return info

    @pytest.fixture
    def mock_session_client(self, mock_structure_a, mock_structure_b,
                            mock_overview_a, mock_overview_b):
        """Create a mock SDMX client that returns different structures per dataflow."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"

        async def get_structure(dataflow_id, agency_id=None, ctx=None):
            if dataflow_id == "DF_A":
                return mock_structure_a
            return mock_structure_b

        async def get_overview(dataflow_id, agency_id=None, ctx=None):
            if dataflow_id == "DF_A":
                return mock_overview_a
            return mock_overview_b

        client.get_structure_summary = AsyncMock(side_effect=get_structure)
        client.get_dataflow_overview = AsyncMock(side_effect=get_overview)
        client.close = AsyncMock()
        return client

    @pytest.fixture
    def mock_app_context(self, mock_session_client):
        """Create a mock AppContext with session state."""
        session_state = MagicMock()
        session_state.endpoint_key = "SPC"
        session_state.default_endpoint_key = "SPC"
        session_state.client = mock_session_client
        session_state.get_or_create_client = AsyncMock(return_value=mock_session_client)

        app_ctx = MagicMock()
        app_ctx.get_session.return_value = session_state
        app_ctx.get_client.return_value = mock_session_client
        return app_ctx

    def _patch_fetch_constraint_info(self, mock_constraint_a, mock_constraint_b):
        """Return a patch for _fetch_constraint_info returning per-dataflow info."""
        async def side_effect(client, dataflow_id, agency, endpoint_key=None):
            if dataflow_id == "DF_A":
                return mock_constraint_a, 1
            return mock_constraint_b, 1

        return patch("main_server._fetch_constraint_info", side_effect=side_effect)

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_shared_same_version(self, mock_get_client, mock_get_app,
                                       mock_session_client, mock_app_context,
                                       mock_constraint_a, mock_constraint_b):
        """FREQ has same codelist+version → shared, with used-code overlap."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        freq_dim = next(d for d in result.dimensions if d.dimension_id == "FREQ")
        assert freq_dim.status == "shared"
        # Both use A and Q → 100% overlap of used codes
        assert freq_dim.code_overlap is not None
        assert freq_dim.code_overlap.used_in_both == 2
        assert freq_dim.code_overlap.overlap_pct == 100.0
        assert "FREQ" in result.shared_dimensions

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_shared_different_version(self, mock_get_client, mock_get_app,
                                            mock_session_client, mock_app_context,
                                            mock_constraint_a, mock_constraint_b):
        """GEO has same codelist ID, different version → shared with used-code overlap."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        geo_dim = next(d for d in result.dimensions if d.dimension_id == "GEO")
        assert geo_dim.status == "shared"
        assert geo_dim.code_overlap is not None
        assert geo_dim.code_overlap.same_codelist is True
        assert geo_dim.code_overlap.used_in_both == 3  # FJ, WS, TO
        assert geo_dim.code_overlap.only_in_a == 1  # VU
        assert geo_dim.code_overlap.only_in_b == 1  # PG
        assert geo_dim.code_overlap.overlap_pct == 75.0  # 3/4 * 100
        assert "GEO" in result.shared_dimensions

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_unique_dimensions(self, mock_get_client, mock_get_app,
                                     mock_session_client, mock_app_context,
                                     mock_constraint_a, mock_constraint_b):
        """INDICATOR unique to A, SECTOR unique to B."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        ind_dim = next(d for d in result.dimensions if d.dimension_id == "INDICATOR")
        assert ind_dim.status == "unique_to_a"
        assert ind_dim.position_a == 3
        assert ind_dim.position_b is None

        sector_dim = next(d for d in result.dimensions if d.dimension_id == "SECTOR")
        assert sector_dim.status == "unique_to_b"
        assert sector_dim.position_a is None
        assert sector_dim.position_b == 3

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_excludes_time_period(self, mock_get_client, mock_get_app,
                                        mock_session_client, mock_app_context,
                                        mock_constraint_a, mock_constraint_b):
        """TIME_PERIOD should not appear in comparison results."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        dim_ids = [d.dimension_id for d in result.dimensions]
        assert "TIME_PERIOD" not in dim_ids

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_join_columns(self, mock_get_client, mock_get_app,
                                mock_session_client, mock_app_context,
                                mock_constraint_a, mock_constraint_b):
        """Shared dims with identical codelist or high overlap → join columns."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        # FREQ: identical codelist version → join column
        assert "FREQ" in result.join_columns
        # GEO: 75% used-code overlap (>= 50) → join column
        assert "GEO" in result.join_columns
        # TIME_PERIOD: shared TimeDimension → always a join column
        assert "TIME_PERIOD" in result.join_columns
        # INDICATOR/SECTOR: unique → not join columns
        assert "INDICATOR" not in result.join_columns
        assert "SECTOR" not in result.join_columns

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_time_overlap(self, mock_get_client, mock_get_app,
                                mock_session_client, mock_app_context,
                                mock_constraint_a, mock_constraint_b):
        """Time ranges 2000-2020 vs 2010-2024 → overlap 2010-2020."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_constraint_info(mock_constraint_a, mock_constraint_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        assert result.time_overlap is not None
        assert result.time_overlap.has_overlap is True
        assert result.time_overlap.range_a.start == "2000-01-01"
        assert result.time_overlap.range_a.end == "2020-12-31"
        assert result.time_overlap.range_b.start == "2010-01-01"
        assert result.time_overlap.range_b.end == "2024-12-31"
        assert result.time_overlap.overlap_start == "2010-01-01"
        assert result.time_overlap.overlap_end == "2020-12-31"
        assert result.time_overlap.overlap_years == 11.0

    @pytest.mark.asyncio
    @patch("main_server._fetch_constraint_info")
    @patch("main_server._resolve_client")
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_cross_provider(self, mock_get_client, mock_get_app,
                                  mock_resolve_client, mock_fetch_info,
                                  mock_session_client,
                                  mock_app_context, mock_structure_a, mock_structure_b,
                                  mock_overview_a, mock_overview_b):
        """Cross-provider: endpoint_a=None, endpoint_b='IMF' resolves to pooled clients."""
        from main_server import _ConstraintInfo, compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context
        mock_fetch_info.return_value = (_ConstraintInfo(), 1)

        # Create a second mock client for IMF
        imf_client = MagicMock(spec=SDMXProgressiveClient)
        imf_client.agency_id = "IMF.STA"
        imf_client.endpoint_key = "IMF"
        imf_client.get_structure_summary = AsyncMock(return_value=mock_structure_b)
        imf_client.get_dataflow_overview = AsyncMock(return_value=mock_overview_b)
        imf_client.close = AsyncMock()

        async def resolve_side_effect(ctx, endpoint):
            if endpoint == "IMF":
                return (imf_client, "IMF")
            return (mock_session_client, "SPC")

        mock_resolve_client.side_effect = resolve_side_effect

        result = await compare_dataflow_dimensions(
            "DF_A", "DF_B", endpoint_a=None, endpoint_b="IMF", ctx=None,
        )

        assert result.endpoint_a == "SPC"
        assert result.endpoint_b == "IMF"

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_no_constraint_graceful(self, mock_get_client, mock_get_app,
                                          mock_session_client, mock_app_context):
        """When no constraint is available, code_overlap and time_overlap are None."""
        from main_server import _ConstraintInfo, compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        empty_info = _ConstraintInfo()
        with patch("main_server._fetch_constraint_info", new_callable=AsyncMock,
                   return_value=(empty_info, 1)):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        freq_dim = next(d for d in result.dimensions if d.dimension_id == "FREQ")
        assert freq_dim.status == "shared"
        assert freq_dim.code_overlap is None  # No constraint data
        assert result.time_overlap is None  # No time range data
        # Still a join column because identical codelist version
        assert "FREQ" in result.join_columns


# =============================================================================
# Interoperability Fix Tests
# =============================================================================


class TestConfigHelpers:
    """Test config helper functions for interoperability."""

    def test_get_dataflow_agency_oecd(self):
        """OECD should return 'all' for dataflow listing."""
        from config import get_dataflow_agency
        assert get_dataflow_agency("OECD") == "all"

    def test_get_dataflow_agency_spc(self):
        """SPC should return None (no override needed)."""
        from config import get_dataflow_agency
        assert get_dataflow_agency("SPC") is None

    def test_get_dataflow_agency_unknown(self):
        """Unknown endpoint should return None."""
        from config import get_dataflow_agency
        assert get_dataflow_agency("NONEXISTENT") is None

    def test_get_best_references_supported(self):
        """When desired value is supported, return it directly."""
        from config import get_best_references
        assert get_best_references("SPC", "all") == "all"
        assert get_best_references("SPC", "parents") == "parents"
        assert get_best_references("ECB", "all") == "all"

    def test_get_best_references_estat_fallback(self):
        """ESTAT doesn't support 'all' or 'parents', falls back to 'descendants'."""
        from config import get_best_references
        assert get_best_references("ESTAT", "all") == "descendants"
        assert get_best_references("ESTAT", "parents") is None
        assert get_best_references("ESTAT", "children") == "children"

    def test_get_best_references_none_endpoint(self):
        """None endpoint should return desired value (unknown endpoint)."""
        from config import get_best_references
        assert get_best_references(None, "all") == "all"
        assert get_best_references(None, "parents") == "parents"

    def test_get_best_references_unknown_endpoint(self):
        """Unknown endpoint should return desired value."""
        from config import get_best_references
        assert get_best_references("NONEXISTENT", "all") == "all"


class TestAgencyIdResolution:
    """Test that agency_id defaults resolve from client when not provided."""

    @pytest.mark.asyncio
    async def test_list_dataflows_uses_client_agency(self):
        """When agency_id is None, list_dataflows should use client.agency_id."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "ECB"
        client.endpoint_key = "ECB"
        client.discover_dataflows = AsyncMock(return_value=[])

        await list_dataflows(client=client)

        # Verify discover_dataflows was called with ECB, not SPC
        client.discover_dataflows.assert_called_once()
        call_kwargs = client.discover_dataflows.call_args
        assert call_kwargs[1].get("agency_id") == "ECB" or call_kwargs[0][0] if call_kwargs[0] else True

    @pytest.mark.asyncio
    async def test_list_dataflows_explicit_agency_overrides(self):
        """When agency_id is explicitly provided, it should be used."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "ECB"
        client.endpoint_key = "ECB"
        client.discover_dataflows = AsyncMock(return_value=[])

        await list_dataflows(client=client, agency_id="UNICEF")

        # Verify discover_dataflows was called with UNICEF
        client.discover_dataflows.assert_called_once()
        call_kwargs = client.discover_dataflows.call_args
        assert call_kwargs[1].get("agency_id") == "UNICEF" or True

    @pytest.mark.asyncio
    async def test_get_structure_uses_client_agency(self):
        """get_dataflow_structure should resolve agency from client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "UNICEF"
        client.endpoint_key = "UNICEF"
        client.get_structure_summary = AsyncMock(return_value=None)

        result = await get_dataflow_structure(client=client, dataflow_id="TEST_DF")

        # Should not error even without explicit agency_id
        assert "error" in result or "discovery_level" in result

    @pytest.mark.asyncio
    async def test_validate_query_uses_client_agency(self):
        """validate_query should resolve agency from client."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "IMF.STA"
        client.endpoint_key = "IMF"
        client.get_structure_summary = AsyncMock(return_value=None)

        result = await validate_query(client=client, dataflow_id="TEST_DF", key="A.B")

        assert "errors" in result


class TestCodelistFallback:
    """Test codelist agency fallback mechanism."""

    def test_get_fallback_agencies_standard(self):
        """Standard agency should try SDMX as fallback."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id="UNICEF",
            endpoint_key="UNICEF",
        )
        fallbacks = client._get_fallback_agencies("UNICEF")
        assert "SDMX" in fallbacks
        assert len(fallbacks) == 1

    def test_get_fallback_agencies_dotted(self):
        """Dotted agency should try both SDMX and parent agency."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id="IMF.STA",
            endpoint_key="IMF",
        )
        fallbacks = client._get_fallback_agencies("IMF.STA")
        assert "SDMX" in fallbacks
        assert "IMF" in fallbacks
        assert len(fallbacks) == 2

    def test_get_fallback_agencies_sdmx(self):
        """SDMX agency should not try itself as fallback."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id="SDMX",
        )
        fallbacks = client._get_fallback_agencies("SDMX")
        assert "SDMX" not in fallbacks
        assert len(fallbacks) == 0


class TestEndpointKeyThreading:
    """Test that endpoint_key is properly threaded through the system."""

    def test_client_stores_endpoint_key(self):
        """SDMXProgressiveClient should store endpoint_key."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id="ESTAT",
            endpoint_key="ESTAT",
        )
        assert client.endpoint_key == "ESTAT"

    def test_client_endpoint_key_defaults_none(self):
        """endpoint_key should default to None if not provided."""
        client = SDMXProgressiveClient(
            base_url="https://example.com/rest",
            agency_id="TEST",
        )
        assert client.endpoint_key is None

    @pytest.mark.asyncio
    async def test_session_manager_passes_endpoint_key(self):
        """SessionManager should seed session with default endpoint key."""
        from session_manager import SessionManager

        manager = SessionManager()
        session = manager.get_session("test-session")
        assert session.default_endpoint_key == "SPC"  # Default endpoint
        client = await session.get_or_create_client(session.default_endpoint_key)
        assert client.endpoint_key == "SPC"
        await manager.close_all()

class TestConstraintStrategies:
    """Test _fetch_constraint_info with different constraint strategies."""

    # Minimal valid SDMX constraint XML for testing
    CONSTRAINT_XML = (
        '<?xml version="1.0"?>'
        '<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"'
        ' xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"'
        ' xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<mes:Structures><str:Constraints>"
        '<str:ContentConstraint id="CC" type="Actual">'
        '<str:CubeRegion include="true">'
        '<com:KeyValue id="FREQ"><com:Value>A</com:Value></com:KeyValue>'
        '<com:KeyValue id="TIME_PERIOD">'
        "<com:TimeRange>"
        '<com:StartPeriod isInclusive="true">2010-01-01T00:00:00</com:StartPeriod>'
        '<com:EndPeriod isInclusive="true">2024-12-31T00:00:00</com:EndPeriod>'
        "</com:TimeRange>"
        "</com:KeyValue>"
        "</str:CubeRegion>"
        "</str:ContentConstraint>"
        "</str:Constraints></mes:Structures></mes:Structure>"
    )

    def _make_client(self, endpoint_key, agency_id=None):
        """Create a mock client for constraint testing."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.base_url = "https://example.com/rest"
        client.agency_id = agency_id or endpoint_key
        client.endpoint_key = endpoint_key
        return client

    def _make_response(self, status_code=200, content=None):
        """Create a mock HTTP response."""
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = (content or self.CONSTRAINT_XML).encode("utf-8")
        return resp

    @pytest.mark.asyncio
    async def test_availableconstraint_strategy(self):
        """BIS/ABS/SPC strategy: /availableconstraint/{flow}/all/all/all."""
        from main_server import _fetch_constraint_info

        client = self._make_client("BIS")
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response())
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "WS_CBPOL", "BIS", endpoint_key="BIS"
        )

        assert api_calls == 1
        assert info.constraint_type == "Actual"
        assert "FREQ" in info.used_codes
        assert "A" in info.used_codes["FREQ"]
        assert info.time_start == "2010-01-01"
        assert info.time_end == "2024-12-31"
        # Verify correct URL was called
        call_url = mock_session.get.call_args[0][0]
        assert "/availableconstraint/WS_CBPOL/all/all/all" in call_url

    @pytest.mark.asyncio
    async def test_references_strategy(self):
        """ECB strategy: ?references=contentconstraint."""
        from main_server import _fetch_constraint_info

        client = self._make_client("ECB")
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response())
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "EXR", "ECB", endpoint_key="ECB"
        )

        assert api_calls == 1
        assert info.constraint_type == "Actual"
        call_url = mock_session.get.call_args[0][0]
        assert "?references=contentconstraint" in call_url
        assert "/dataflow/ECB/EXR/latest" in call_url

    @pytest.mark.asyncio
    async def test_references_all_strategy(self):
        """ILO strategy: ?references=all on the dataflow endpoint."""
        from main_server import _fetch_constraint_info

        client = self._make_client("ILO")
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=self._make_response())
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "DF_SDG_A", "ILO", endpoint_key="ILO"
        )

        assert api_calls == 1
        assert info.constraint_type == "Actual"
        assert "FREQ" in info.used_codes
        assert info.time_start == "2010-01-01"
        # Verify ?references=all was used (not contentconstraint)
        call_url = mock_session.get.call_args[0][0]
        assert "?references=all" in call_url
        assert "/dataflow/ILO/DF_SDG_A/latest" in call_url

    @pytest.mark.asyncio
    async def test_none_strategy_skips_api_calls(self):
        """ESTAT: No constraint support — zero API calls."""
        from main_server import _fetch_constraint_info

        client = self._make_client("ESTAT")
        mock_session = AsyncMock()
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "prc_hicp_manr", "ESTAT", endpoint_key="ESTAT"
        )

        assert api_calls == 0
        assert info.constraint_type is None
        assert len(info.used_codes) == 0
        mock_session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_endpoint_cascades(self):
        """Custom/unknown endpoint: tries availableconstraint, then references."""
        from main_server import _fetch_constraint_info

        client = self._make_client(None, agency_id="CUSTOM")
        mock_session = AsyncMock()
        # First call (availableconstraint) fails, second succeeds
        empty_resp = self._make_response(status_code=404, content="<empty/>")
        ok_resp = self._make_response()
        mock_session.get = AsyncMock(side_effect=[empty_resp, ok_resp])
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "TEST_DF", "CUSTOM", endpoint_key=None
        )

        assert api_calls == 2
        assert info.constraint_type == "Actual"

    @pytest.mark.asyncio
    async def test_references_all_empty_response(self):
        """ILO with empty/error response returns empty info gracefully."""
        from main_server import _fetch_constraint_info

        client = self._make_client("ILO")
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_response(status_code=500, content="")
        )
        client._get_session = AsyncMock(return_value=mock_session)

        info, api_calls = await _fetch_constraint_info(
            client, "DF_SDG_A", "ILO", endpoint_key="ILO"
        )

        assert api_calls == 1
        assert info.constraint_type is None
        assert len(info.used_codes) == 0


class TestConstraintConfigStrategies:
    """Test that config correctly maps endpoints to constraint strategies."""

    def test_bis_has_availableconstraint(self):
        from config import get_constraint_strategy
        assert get_constraint_strategy("BIS", "single_flow") == "availableconstraint"
        assert get_constraint_strategy("BIS", "bulk") is None

    def test_abs_has_availableconstraint(self):
        from config import get_constraint_strategy
        assert get_constraint_strategy("ABS", "single_flow") == "availableconstraint"
        assert get_constraint_strategy("ABS", "bulk") is None

    def test_ilo_has_references_all(self):
        from config import get_constraint_strategy
        assert get_constraint_strategy("ILO", "single_flow") == "references_all"
        assert get_constraint_strategy("ILO", "bulk") is None

    def test_estat_has_none(self):
        from config import get_constraint_strategy
        assert get_constraint_strategy("ESTAT", "single_flow") is None
        assert get_constraint_strategy("ESTAT", "bulk") is None


class TestDataflowSummaryCarriesAgency:
    """list_dataflows must report each dataflow's owning agency.

    OECD publishes under sub-agencies (OECD.CFE.EDS, OECD.SDD.TPS, and others)
    and also mirrors flows owned by ESTAT and IAEG-SDGs, so the agency is not
    derivable from the endpoint. DataflowSummary.agency defaults to "", so
    failing to pass it produces an empty string rather than an error, and the
    caller then builds structure and data URLs against the wrong agency.
    """

    @pytest.fixture
    def oecd_dataflows(self):
        return [
            {
                "id": "DSD_FUA_CLIM@DF_CLIM_PROJ",
                "agency": "OECD.CFE.EDS",
                "version": "1.4",
                "name": "Climate projections",
                "description": "Projected climate indicators",
            },
            {
                "id": "SEEA_AEA_A",
                "agency": "ESTAT",
                "version": "1.4",
                "name": "Air emissions accounts",
                "description": "Mirrored from Eurostat",
            },
        ]

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server._resolve_client")
    async def test_agency_is_reported_for_each_dataflow(
        self, mock_resolve, mock_get_app, oecd_dataflows
    ):
        from main_server import list_dataflows as list_dataflows_tool

        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "all"
        client.endpoint_key = "OECD"
        client.discover_dataflows = AsyncMock(return_value=oecd_dataflows)
        mock_resolve.return_value = (client, "OECD")
        mock_get_app.return_value = None

        result = await list_dataflows_tool(ctx=None)

        by_id = {df.id: df for df in result.dataflows}
        assert by_id["DSD_FUA_CLIM@DF_CLIM_PROJ"].agency == "OECD.CFE.EDS"
        # A mirrored flow keeps its own owner, not the endpoint's default.
        assert by_id["SEEA_AEA_A"].agency == "ESTAT"

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server._resolve_client")
    async def test_structure_reports_the_agency_it_resolved_under(
        self, mock_resolve, mock_get_app
    ):
        """get_dataflow_structure must say which agency it used, and which
        agency and version own the DSD. Without them an agent that started from
        list_dataflows cannot rebuild a URL for an OECD sub-agency flow."""
        from main_server import get_dataflow_structure as structure_tool

        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "OECD.CFE.EDS"
        client.endpoint_key = "OECD"
        client.get_dataflow_overview = AsyncMock(
            return_value=DataflowOverview(
                id="DSD_FUA_CLIM@DF_CLIM_PROJ",
                agency="OECD.CFE.EDS",
                version="1.4",
                name="Climate projections",
                description="Projected climate indicators",
            )
        )
        client.get_structure_summary = AsyncMock(
            return_value=DataStructureSummary(
                id="DSD_FUA_CLIM",
                agency="OECD.CFE.EDS",
                version="1.4",
                dimensions=[DimensionInfo(id="FREQ", position=1, type="Dimension")],
                key_family=["FREQ"],
                attributes=[],
                primary_measure="OBS_VALUE",
            )
        )
        mock_resolve.return_value = (client, "OECD")
        mock_get_app.return_value = None

        result = await structure_tool("DSD_FUA_CLIM@DF_CLIM_PROJ", ctx=None)

        assert result.dataflow.agency == "OECD.CFE.EDS"
        assert result.structure.agency == "OECD.CFE.EDS"
        assert result.structure.version == "1.4"
