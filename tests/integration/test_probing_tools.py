"""Integration tests for query probing tools."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sdmx_progressive_client import DataStructureSummary, DimensionInfo, SDMXProgressiveClient

SAMPLE_CSV_NONEMPTY = (
    "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
    "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2020,1234\n"
    "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2021,1456\n"
    "SPC:DF_KAVA(3.0),A,TO,KAVA_PROD,2020,789\n"
)

SAMPLE_CSV_EMPTY = "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"

AVAILABLECONSTRAINT_NONEMPTY = (
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
    '<com:KeyValue id="FREQ"><com:Value>A</com:Value></com:KeyValue>'
    '<com:KeyValue id="GEO_PICT"><com:Value>FJ</com:Value></com:KeyValue>'
    '<com:KeyValue id="INDICATOR"><com:Value>KAVA_PROD</com:Value></com:KeyValue>'
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

AVAILABLECONSTRAINT_EMPTY = (
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

SAMPLE_URL = (
    "https://stats-sdmx-disseminate.pacificdata.org/rest"
    "/data/DF_KAVA/A.FJ.KAVA_PROD?startPeriod=2020&endPeriod=2024"
)


class TestProbeDataUrl:
    """Test probe_data_url implementation."""

    @pytest.fixture(autouse=True)
    def clear_probe_cache(self):
        """Clear the probe cache before each test to avoid inter-test pollution."""
        from tools.probing_tools import _probe_cache
        _probe_cache.clear()
        yield
        _probe_cache.clear()

    @pytest.fixture
    def mock_client(self):
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
        client.base_url = "https://stats-sdmx-disseminate.pacificdata.org/rest"
        return client

    def _make_availability_response(self, xml_text):
        response = MagicMock()
        response.status_code = 200
        response.content = xml_text.encode("utf-8")
        return response

    @pytest.mark.asyncio
    async def test_probe_nonempty_url(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_availability_response(AVAILABLECONSTRAINT_NONEMPTY)
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "nonempty"
        assert result["observation_count"] == 794
        assert result["series_count"] == 2
        assert result["time_period_count"] == 2
        assert result["has_time_dimension"] is True
        assert result["geo_dimension_id"] == "GEO_PICT"
        assert result["query_fingerprint"].startswith("sha256:")
        assert len(result["sample_observations"]) > 0
        assert mock_client.fetch_data_probe.call_count == 1

    @pytest.mark.asyncio
    async def test_probe_empty_url(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_availability_response(AVAILABLECONSTRAINT_EMPTY)
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_EMPTY))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "empty"
        assert result["observation_count"] == 0
        assert any("availableconstraint" in n.lower() for n in result["notes"])
        assert mock_client.fetch_data_probe.call_count == 0

    @pytest.mark.asyncio
    async def test_probe_http_error(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(return_value=(500, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "error"
        assert any("500" in n for n in result["notes"])

    @pytest.mark.asyncio
    async def test_probe_network_failure(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(return_value=(0, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_probe_404_empty(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(return_value=(404, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "empty"
        assert any("404" in n or "no data" in n.lower() for n in result["notes"])

    @pytest.mark.asyncio
    async def test_probe_with_structured_input(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_availability_response(AVAILABLECONSTRAINT_NONEMPTY)
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))
        mock_client.get_structure_summary = AsyncMock(return_value=_make_mock_structure())

        result = await probe_data_url(
            client=mock_client,
            dataflow_id="DF_KAVA",
            filters={"FREQ": "A", "GEO_PICT": "FJ", "INDICATOR": "KAVA_PROD"},
            start_period="2020",
            end_period="2024",
        )

        assert result["status"] == "nonempty"
        mock_client.fetch_data_probe.assert_called_once()
        called_url = mock_client.fetch_data_probe.await_args.args[0]
        assert "/data/DF_KAVA/A.FJ.KAVA_PROD" in called_url

    @pytest.mark.asyncio
    async def test_probe_structured_input_uses_dsd_dimension_order(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))
        mock_client.get_structure_summary = AsyncMock(return_value=_make_mock_structure())

        await probe_data_url(
            client=mock_client,
            dataflow_id="DF_KAVA",
            filters={"INDICATOR": "KAVA_PROD", "GEO_PICT": "FJ", "FREQ": "A"},
        )

        called_url = mock_client.fetch_data_probe.await_args.args[0]
        assert "/data/DF_KAVA/A.FJ.KAVA_PROD" in called_url

    @pytest.mark.asyncio
    async def test_probe_cache_hit(self, mock_client):
        from tools.probing_tools import _probe_cache, probe_data_url

        # Clear cache before test
        _probe_cache.clear()

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_availability_response(AVAILABLECONSTRAINT_NONEMPTY)
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result1 = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)
        result2 = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result1["query_fingerprint"] == result2["query_fingerprint"]
        assert result1["status"] == result2["status"]
        # Second call should hit cache — only 1 HTTP call
        assert mock_client.fetch_data_probe.call_count == 1

    @pytest.mark.asyncio
    async def test_probe_cache_separates_shape_parameters(self, mock_client):
        from tools.probing_tools import _probe_cache, probe_data_url

        _probe_cache.clear()
        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            return_value=self._make_availability_response(AVAILABLECONSTRAINT_NONEMPTY)
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result1 = await probe_data_url(
            client=mock_client,
            data_url=SAMPLE_URL,
            sample_limit=1,
        )
        result2 = await probe_data_url(
            client=mock_client,
            data_url=SAMPLE_URL,
            sample_limit=5,
        )

        assert len(result1["sample_observations"]) == 1
        assert len(result2["sample_observations"]) == 3
        assert mock_client.fetch_data_probe.call_count == 2


class TestProbeDataUrlHandler:
    """Test the MCP tool handler for probe_data_url."""

    @pytest.fixture(autouse=True)
    def clear_probe_cache(self):
        from tools.probing_tools import _probe_cache
        _probe_cache.clear()
        yield
        _probe_cache.clear()

    @pytest.mark.asyncio
    async def test_handler_returns_probe_result_model(self):
        from main_server import probe_data_url as handler
        from models.schemas import ProbeResult

        mock_client = MagicMock(spec=SDMXProgressiveClient)
        mock_client.agency_id = "SPC"
        mock_client.endpoint_key = "SPC"
        mock_client.base_url = "https://example.org/rest"
        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(
            return_value=(200, SAMPLE_CSV_NONEMPTY)
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await handler(data_url=SAMPLE_URL)

        assert isinstance(result, ProbeResult)
        assert result.status == "nonempty"
        assert result.observation_count == 3
        assert result.query_fingerprint.startswith("sha256:")


def _make_mock_structure():
    """Build a mock DataStructureSummary for testing."""
    return DataStructureSummary(
        id="DSD_KAVA",
        agency="SPC",
        version="3.0",
        dimensions=[
            DimensionInfo(id="FREQ", position=0, type="Dimension", codelist_ref=None),
            DimensionInfo(id="GEO_PICT", position=1, type="Dimension", codelist_ref=None),
            DimensionInfo(id="INDICATOR", position=2, type="Dimension", codelist_ref=None),
        ],
        key_family=["FREQ", "GEO_PICT", "INDICATOR"],
        attributes=[],
    )


class TestSuggestNonemptyQueries:
    """Test suggest_nonempty_queries implementation."""

    @pytest.fixture(autouse=True)
    def clear_probe_cache(self):
        from tools.probing_tools import _probe_cache
        _probe_cache.clear()
        yield
        _probe_cache.clear()

    @pytest.fixture
    def mock_client(self):
        client = MagicMock(spec=SDMXProgressiveClient)
        client.agency_id = "SPC"
        client.endpoint_key = "SPC"
        client.base_url = "https://stats-sdmx-disseminate.pacificdata.org/rest"
        client.get_structure_summary = AsyncMock(return_value=_make_mock_structure())
        client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        return client

    def _make_availability_response(self, xml_text):
        response = MagicMock()
        response.status_code = 200
        response.content = xml_text.encode("utf-8")
        return response

    @pytest.mark.asyncio
    async def test_original_nonempty_no_suggestions(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        mock_client.fetch_data_probe = AsyncMock(
            return_value=(200, SAMPLE_CSV_NONEMPTY)
        )

        result = await suggest_nonempty_queries(
            client=mock_client, data_url=SAMPLE_URL,
        )

        assert result["original_status"] == "nonempty"
        assert len(result["suggestions"]) == 0
        assert result["probes_used"] == 1

    @pytest.mark.asyncio
    async def test_one_dim_relaxation_finds_data(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        # First call returns empty (original), second returns data (relaxed)
        mock_client.fetch_data_probe = AsyncMock(
            side_effect=[
                (200, SAMPLE_CSV_EMPTY),     # original probe
                (200, SAMPLE_CSV_NONEMPTY),  # first relaxation hits
            ]
        )

        result = await suggest_nonempty_queries(
            client=mock_client, data_url=SAMPLE_URL, max_suggestions=1,
        )

        assert result["original_status"] == "empty"
        assert len(result["suggestions"]) >= 1
        assert result["suggestions"][0]["rank"] == 1
        assert len(result["suggestions"][0]["changed_dimensions"]) == 1
        assert result["probes_used"] >= 2

    @pytest.mark.asyncio
    async def test_budget_exhaustion(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        # All probes return empty
        mock_client.fetch_data_probe = AsyncMock(
            return_value=(200, SAMPLE_CSV_EMPTY)
        )

        result = await suggest_nonempty_queries(
            client=mock_client, data_url=SAMPLE_URL, max_probes=5,
        )

        assert result["original_status"] == "empty"
        assert len(result["suggestions"]) == 0
        assert result["probes_used"] <= 5
        assert any("budget" in n.lower() or "exhausted" in n.lower() for n in result["notes"])

    @pytest.mark.asyncio
    async def test_max_suggestions_limit(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        call_count = 0

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (200, SAMPLE_CSV_EMPTY)  # original
            return (200, SAMPLE_CSV_NONEMPTY)   # relaxation

        mock_client.fetch_data_probe = AsyncMock(side_effect=side_effect)

        result = await suggest_nonempty_queries(
            client=mock_client, data_url=SAMPLE_URL, max_suggestions=2,
        )

        assert len(result["suggestions"]) <= 2

    @pytest.mark.asyncio
    async def test_time_widening_finds_data(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        call_count = 0
        total_dims = 3  # FREQ, GEO_PICT, INDICATOR

        async def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # First call = original, next 3 = dim relaxations, then time widening
            if call_count <= 1 + total_dims:
                return (200, SAMPLE_CSV_EMPTY)
            return (200, SAMPLE_CSV_NONEMPTY)

        mock_client.fetch_data_probe = AsyncMock(side_effect=side_effect)

        result = await suggest_nonempty_queries(
            client=mock_client, data_url=SAMPLE_URL,
        )

        time_suggestions = [
            s for s in result["suggestions"]
            if "TIME_PERIOD" in s["changed_dimensions"]
        ]
        assert len(time_suggestions) >= 1

    @pytest.mark.asyncio
    async def test_suggestions_use_availability_without_csv_for_empty_candidate(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            side_effect=[
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
            ]
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock()

        result = await suggest_nonempty_queries(
            client=mock_client,
            data_url=SAMPLE_URL,
            max_probes=5,
        )

        assert result["original_status"] == "empty"
        assert result["suggestions"] == []
        assert mock_client.fetch_data_probe.call_count == 0

    @pytest.mark.asyncio
    async def test_suggestions_fill_series_and_time_counts_from_availability(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(
            side_effect=[
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_EMPTY),
                self._make_availability_response(AVAILABLECONSTRAINT_NONEMPTY),
            ]
        )
        mock_client._get_session = AsyncMock(return_value=mock_session)
        mock_client.fetch_data_probe = AsyncMock()

        result = await suggest_nonempty_queries(
            client=mock_client,
            data_url=SAMPLE_URL,
            max_suggestions=1,
        )

        assert len(result["suggestions"]) == 1
        suggestion = result["suggestions"][0]
        assert suggestion["probe_result"]["observation_count"] == 794
        assert suggestion["probe_result"]["series_count"] == 1
        assert suggestion["probe_result"]["time_period_count"] == 794
        assert mock_client.fetch_data_probe.call_count == 0

    @pytest.mark.asyncio
    async def test_suggestions_preserve_non_time_query_params(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        mock_client.fetch_data_probe = AsyncMock(
            side_effect=[
                (200, SAMPLE_CSV_EMPTY),
                (200, SAMPLE_CSV_NONEMPTY),
            ]
        )
        url_with_params = (
            "https://stats-sdmx-disseminate.pacificdata.org/rest"
            "/data/DF_KAVA/A.FJ.KAVA_PROD"
            "?dimensionAtObservation=AllDimensions&format=jsondata&startPeriod=2020"
        )

        result = await suggest_nonempty_queries(
            client=mock_client,
            data_url=url_with_params,
            max_suggestions=1,
        )

        assert len(result["suggestions"]) == 1
        suggested_url = result["suggestions"][0]["suggested_data_url"]
        assert "dimensionAtObservation=AllDimensions" in suggested_url
        assert "format=jsondata" in suggested_url

    @pytest.mark.asyncio
    async def test_suggestions_use_versioned_flow_ref_for_structure_lookup(self, mock_client):
        from tools.probing_tools import suggest_nonempty_queries

        mock_client.fetch_data_probe = AsyncMock(
            side_effect=[
                (200, SAMPLE_CSV_EMPTY),
                (200, SAMPLE_CSV_NONEMPTY),
            ]
        )
        versioned_url = (
            "https://stats-sdmx-disseminate.pacificdata.org/rest"
            "/data/SPC,DF_KAVA,3.0/A.FJ.KAVA_PROD?startPeriod=2020&endPeriod=2024"
        )

        await suggest_nonempty_queries(
            client=mock_client,
            data_url=versioned_url,
            max_suggestions=1,
        )

        mock_client.get_structure_summary.assert_awaited_once_with(
            dataflow_id="DF_KAVA",
            agency_id="SPC",
            version="3.0",
        )


class TestSuggestNonemptyHandler:
    """Test the MCP tool handler for suggest_nonempty_queries."""

    @pytest.fixture(autouse=True)
    def clear_probe_cache(self):
        from tools.probing_tools import _probe_cache
        _probe_cache.clear()
        yield
        _probe_cache.clear()

    @pytest.mark.asyncio
    async def test_handler_returns_suggestion_result_model(self):
        from main_server import suggest_nonempty_queries as handler
        from models.schemas import SuggestionResult

        mock_client = MagicMock(spec=SDMXProgressiveClient)
        mock_client.agency_id = "SPC"
        mock_client.endpoint_key = "SPC"
        mock_client.base_url = "https://example.org/rest"
        mock_client._get_session = AsyncMock(side_effect=Exception("skip availability"))
        mock_client.fetch_data_probe = AsyncMock(
            return_value=(200, SAMPLE_CSV_NONEMPTY)
        )
        mock_client.get_structure_summary = AsyncMock(
            return_value=_make_mock_structure()
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await handler(data_url=SAMPLE_URL)

        assert isinstance(result, SuggestionResult)
        assert result.original_status == "nonempty"
