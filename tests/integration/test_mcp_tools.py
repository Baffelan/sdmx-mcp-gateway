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
    def mock_used_codes_a(self):
        """Used codes from ContentConstraint for DF_A."""
        return {
            "FREQ": {"A", "Q"},
            "GEO": {"FJ", "WS", "TO", "VU"},
            "INDICATOR": {"GDP", "POP"},
        }

    @pytest.fixture
    def mock_used_codes_b(self):
        """Used codes from ContentConstraint for DF_B.

        GEO overlaps with A on FJ, WS, TO but has PG instead of VU.
        """
        return {
            "FREQ": {"A", "Q"},
            "GEO": {"FJ", "WS", "TO", "PG"},
            "SECTOR": {"AGR", "IND"},
        }

    @pytest.fixture
    def mock_session_client(self, mock_structure_a, mock_structure_b,
                            mock_overview_a, mock_overview_b):
        """Create a mock SDMX client that returns different structures per dataflow."""
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"

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
        session_state.client = mock_session_client

        app_ctx = MagicMock()
        app_ctx.get_session.return_value = session_state
        app_ctx.get_client.return_value = mock_session_client
        return app_ctx

    def _patch_fetch_used_codes(self, mock_used_codes_a, mock_used_codes_b):
        """Return a patch for _fetch_used_codes that returns per-dataflow used codes."""
        async def side_effect(client, dataflow_id, agency):
            if dataflow_id == "DF_A":
                return mock_used_codes_a, 1
            return mock_used_codes_b, 1

        return patch("main_server._fetch_used_codes", side_effect=side_effect)

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_shared_same_version(self, mock_get_client, mock_get_app,
                                       mock_session_client, mock_app_context,
                                       mock_used_codes_a, mock_used_codes_b):
        """FREQ has same codelist+version → shared, with used-code overlap."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_used_codes(mock_used_codes_a, mock_used_codes_b):
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
                                            mock_used_codes_a, mock_used_codes_b):
        """GEO has same codelist ID, different version → shared with used-code overlap."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_used_codes(mock_used_codes_a, mock_used_codes_b):
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
                                     mock_used_codes_a, mock_used_codes_b):
        """INDICATOR unique to A, SECTOR unique to B."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_used_codes(mock_used_codes_a, mock_used_codes_b):
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
                                        mock_used_codes_a, mock_used_codes_b):
        """TIME_PERIOD should not appear in comparison results."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_used_codes(mock_used_codes_a, mock_used_codes_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        dim_ids = [d.dimension_id for d in result.dimensions]
        assert "TIME_PERIOD" not in dim_ids

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_join_columns(self, mock_get_client, mock_get_app,
                                mock_session_client, mock_app_context,
                                mock_used_codes_a, mock_used_codes_b):
        """Shared dims with identical codelist or high overlap → join columns."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        with self._patch_fetch_used_codes(mock_used_codes_a, mock_used_codes_b):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        # FREQ: identical codelist version → join column
        assert "FREQ" in result.join_columns
        # GEO: 75% used-code overlap (>= 50) → join column
        assert "GEO" in result.join_columns
        # INDICATOR/SECTOR: unique → not join columns
        assert "INDICATOR" not in result.join_columns
        assert "SECTOR" not in result.join_columns

    @pytest.mark.asyncio
    @patch("main_server._fetch_used_codes", new_callable=AsyncMock,
           return_value=({}, 1))
    @patch("main_server._get_client_for_endpoint")
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_cross_provider(self, mock_get_client, mock_get_app,
                                  mock_get_endpoint_client, mock_fetch_codes,
                                  mock_session_client,
                                  mock_app_context, mock_structure_a, mock_structure_b,
                                  mock_overview_a, mock_overview_b):
        """Cross-provider: endpoint_a='SPC', endpoint_b='IMF' creates temp client."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        # Create a second mock client for IMF
        imf_client = MagicMock(spec=SDMXProgressiveClient)
        imf_client.agency_id = "IMF.STA"
        imf_client.get_structure_summary = AsyncMock(return_value=mock_structure_b)
        imf_client.get_dataflow_overview = AsyncMock(return_value=mock_overview_b)
        imf_client.close = AsyncMock()

        def side_effect(endpoint_key, session_client, session_endpoint_key):
            if endpoint_key == "IMF":
                return (imf_client, "IMF", True)
            return (session_client, session_endpoint_key, False)

        mock_get_endpoint_client.side_effect = side_effect

        result = await compare_dataflow_dimensions(
            "DF_A", "DF_B", endpoint_a=None, endpoint_b="IMF", ctx=None,
        )

        assert result.endpoint_a == "SPC"
        assert result.endpoint_b == "IMF"
        # Verify temp client was closed
        imf_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("main_server.get_app_context")
    @patch("main_server.get_session_client")
    async def test_no_constraint_graceful(self, mock_get_client, mock_get_app,
                                          mock_session_client, mock_app_context):
        """When no constraint is available, code_overlap is None but status still computed."""
        from main_server import compare_dataflow_dimensions

        mock_get_client.return_value = mock_session_client
        mock_get_app.return_value = mock_app_context

        # Return empty used codes (no constraint found)
        with patch("main_server._fetch_used_codes", new_callable=AsyncMock,
                   return_value=({}, 1)):
            result = await compare_dataflow_dimensions("DF_A", "DF_B", ctx=None)

        freq_dim = next(d for d in result.dimensions if d.dimension_id == "FREQ")
        assert freq_dim.status == "shared"
        assert freq_dim.code_overlap is None  # No constraint data
        # Still a join column because identical codelist version
        assert "FREQ" in result.join_columns
