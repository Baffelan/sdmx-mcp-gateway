"""Integration tests for query probing tools."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sdmx_progressive_client import SDMXProgressiveClient


SAMPLE_CSV_NONEMPTY = (
    "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
    "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2020,1234\n"
    "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2021,1456\n"
    "SPC:DF_KAVA(3.0),A,TO,KAVA_PROD,2020,789\n"
)

SAMPLE_CSV_EMPTY = "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"

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

    @pytest.mark.asyncio
    async def test_probe_nonempty_url(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "nonempty"
        assert result["observation_count"] == 3
        assert result["series_count"] == 2
        assert result["time_period_count"] == 2
        assert result["has_time_dimension"] is True
        assert result["geo_dimension_id"] == "GEO_PICT"
        assert result["query_fingerprint"].startswith("sha256:")
        assert len(result["sample_observations"]) > 0

    @pytest.mark.asyncio
    async def test_probe_empty_url(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_EMPTY))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "empty"
        assert result["observation_count"] == 0
        assert any("zero observations" in n.lower() for n in result["notes"])

    @pytest.mark.asyncio
    async def test_probe_http_error(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(500, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "error"
        assert any("500" in n for n in result["notes"])

    @pytest.mark.asyncio
    async def test_probe_network_failure(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(0, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_probe_404_empty(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(404, ""))

        result = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result["status"] == "empty"
        assert any("404" in n or "no data" in n.lower() for n in result["notes"])

    @pytest.mark.asyncio
    async def test_probe_with_structured_input(self, mock_client):
        from tools.probing_tools import probe_data_url

        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result = await probe_data_url(
            client=mock_client,
            dataflow_id="DF_KAVA",
            filters={"FREQ": "A", "GEO_PICT": "FJ", "INDICATOR": "KAVA_PROD"},
            start_period="2020",
            end_period="2024",
        )

        assert result["status"] == "nonempty"
        mock_client.fetch_data_probe.assert_called_once()

    @pytest.mark.asyncio
    async def test_probe_cache_hit(self, mock_client):
        from tools.probing_tools import _probe_cache, probe_data_url

        # Clear cache before test
        _probe_cache.clear()

        mock_client.fetch_data_probe = AsyncMock(return_value=(200, SAMPLE_CSV_NONEMPTY))

        result1 = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)
        result2 = await probe_data_url(client=mock_client, data_url=SAMPLE_URL)

        assert result1["query_fingerprint"] == result2["query_fingerprint"]
        assert result1["status"] == result2["status"]
        # Second call should hit cache — only 1 HTTP call
        assert mock_client.fetch_data_probe.call_count == 1


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
        mock_client.fetch_data_probe = AsyncMock(
            return_value=(200, SAMPLE_CSV_NONEMPTY)
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await handler(data_url=SAMPLE_URL)

        assert isinstance(result, ProbeResult)
        assert result.status == "nonempty"
        assert result.observation_count == 3
        assert result.query_fingerprint.startswith("sha256:")


from sdmx_progressive_client import DataStructureSummary, DimensionInfo


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
        return client

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
