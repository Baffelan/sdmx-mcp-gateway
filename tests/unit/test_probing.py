"""Unit tests for query probing models and utilities."""

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestProbeModels:
    """Test Pydantic model construction for probing tools."""

    def test_probe_result_nonempty(self):
        from models.schemas import DimensionSummary, ProbeResult, SampleObservation

        result = ProbeResult(
            status="nonempty",
            observation_count=124,
            series_count=3,
            time_period_count=12,
            dimensions={
                "GEO": DimensionSummary(distinct_count=3, sample_values=["FJ", "WS", "TO"]),
            },
            has_time_dimension=True,
            geo_dimension_id="GEO",
            sample_observations=[
                SampleObservation(
                    dimensions={"GEO": "FJ", "TIME_PERIOD": "2023"},
                    value=928784.0,
                ),
            ],
            query_fingerprint="sha256:abc123",
            notes=[],
        )
        assert result.status == "nonempty"
        assert result.observation_count == 124
        assert result.dimensions["GEO"].distinct_count == 3

    def test_probe_result_empty(self):
        from models.schemas import ProbeResult

        result = ProbeResult(
            status="empty",
            observation_count=0,
            series_count=0,
            time_period_count=0,
            dimensions={},
            has_time_dimension=False,
            geo_dimension_id=None,
            sample_observations=[],
            query_fingerprint="sha256:def456",
            notes=["Query is syntactically valid but returned zero observations."],
        )
        assert result.status == "empty"
        assert result.observation_count == 0

    def test_suggestion_result(self):
        from models.schemas import QuerySuggestion, SuggestionProbeResult, SuggestionResult

        result = SuggestionResult(
            original_status="empty",
            original_query_fingerprint="sha256:abc",
            suggestions=[
                QuerySuggestion(
                    rank=1,
                    change_summary="Relaxed SEX from M to all values",
                    changed_dimensions=["SEX"],
                    suggested_data_url="https://example.org/data/DF/...",
                    probe_result=SuggestionProbeResult(
                        status="nonempty",
                        observation_count=24,
                        series_count=1,
                        time_period_count=24,
                    ),
                ),
            ],
            probes_used=6,
            notes=[],
        )
        assert result.original_status == "empty"
        assert len(result.suggestions) == 1
        assert result.suggestions[0].rank == 1


class TestFetchDataProbe:
    """Test SDMXProgressiveClient.fetch_data_probe method."""

    @pytest.mark.asyncio
    async def test_fetch_data_probe_success(self):
        from sdmx_progressive_client import SDMXProgressiveClient

        client = SDMXProgressiveClient(
            base_url="https://example.org/rest", agency_id="TEST"
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "DATAFLOW,FREQ,OBS_VALUE\nTEST:DF(1.0),A,123\n"
        mock_response.raise_for_status = MagicMock()

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        client.session = mock_session

        status, text = await client.fetch_data_probe(
            "https://example.org/rest/data/DF/A"
        )

        assert status == 200
        assert "123" in text
        mock_session.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_data_probe_http_error(self):
        from sdmx_progressive_client import SDMXProgressiveClient

        client = SDMXProgressiveClient(
            base_url="https://example.org/rest", agency_id="TEST"
        )

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = ""

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        client.session = mock_session

        status, text = await client.fetch_data_probe(
            "https://example.org/rest/data/DF/A"
        )

        assert status == 404
        assert text == ""

    @pytest.mark.asyncio
    async def test_fetch_data_probe_network_error(self):
        import httpx

        from sdmx_progressive_client import SDMXProgressiveClient

        client = SDMXProgressiveClient(
            base_url="https://example.org/rest", agency_id="TEST"
        )

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        client.session = mock_session

        status, text = await client.fetch_data_probe(
            "https://example.org/rest/data/DF/A"
        )

        assert status == 0
        assert text == ""


class TestUrlParsing:
    """Test SDMX data URL parsing and normalization."""

    def test_parse_basic_url(self):
        from tools.probing_tools import parse_sdmx_data_url

        parsed = parse_sdmx_data_url(
            "https://stats-sdmx-disseminate.pacificdata.org/rest/data/DF_KAVA/A.FJ.KAVA_PROD"
            "?startPeriod=2020&endPeriod=2024"
        )
        assert parsed["base_url"] == "https://stats-sdmx-disseminate.pacificdata.org/rest"
        assert parsed["dataflow_id"] == "DF_KAVA"
        assert parsed["key"] == "A.FJ.KAVA_PROD"
        assert parsed["key_parts"] == ["A", "FJ", "KAVA_PROD"]
        assert parsed["start_period"] == "2020"
        assert parsed["end_period"] == "2024"

    def test_parse_url_with_agency(self):
        from tools.probing_tools import parse_sdmx_data_url

        parsed = parse_sdmx_data_url(
            "https://data.api.abs.gov.au/rest/data/ABS,CPI,1.0.0/1.10001..Q"
            "?dimensionAtObservation=AllDimensions"
        )
        assert parsed["dataflow_id"] == "ABS,CPI,1.0.0"
        assert parsed["key"] == "1.10001..Q"
        assert parsed["key_parts"] == ["1", "10001", "", "Q"]

    def test_parse_url_no_params(self):
        from tools.probing_tools import parse_sdmx_data_url

        parsed = parse_sdmx_data_url(
            "https://example.org/rest/data/DF_POP/all"
        )
        assert parsed["dataflow_id"] == "DF_POP"
        assert parsed["key"] == "all"
        assert parsed["start_period"] is None
        assert parsed["end_period"] is None

    def test_parse_url_invalid(self):
        from tools.probing_tools import parse_sdmx_data_url

        parsed = parse_sdmx_data_url("https://example.org/not-sdmx")
        assert parsed is None

    def test_fingerprint_deterministic(self):
        from tools.probing_tools import normalize_query_fingerprint

        url = "https://example.org/rest/data/DF/A.FJ?startPeriod=2020&endPeriod=2024"
        fp1 = normalize_query_fingerprint(url)
        fp2 = normalize_query_fingerprint(url)
        assert fp1 == fp2
        assert fp1.startswith("sha256:")

    def test_fingerprint_param_order_invariant(self):
        from tools.probing_tools import normalize_query_fingerprint

        url_a = "https://example.org/rest/data/DF/A?startPeriod=2020&endPeriod=2024"
        url_b = "https://example.org/rest/data/DF/A?endPeriod=2024&startPeriod=2020"
        assert normalize_query_fingerprint(url_a) == normalize_query_fingerprint(url_b)

    def test_build_probe_url_adds_first_n(self):
        from tools.probing_tools import build_probe_url

        probe = build_probe_url(
            "https://example.org/rest/data/DF/A.FJ?startPeriod=2020"
        )
        assert "firstNObservations=1" in probe

    def test_build_probe_url_preserves_existing_params(self):
        from tools.probing_tools import build_probe_url

        probe = build_probe_url(
            "https://example.org/rest/data/DF/A.FJ?startPeriod=2020&endPeriod=2024"
        )
        assert "startPeriod=2020" in probe
        assert "endPeriod=2024" in probe
        assert "firstNObservations=1" in probe


class TestCsvParsing:
    """Test CSV probe response parsing."""

    def test_parse_nonempty_csv(self):
        from tools.probing_tools import parse_csv_probe_response

        csv_text = (
            "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
            "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2020,1234\n"
            "SPC:DF_KAVA(3.0),A,FJ,KAVA_PROD,2021,1456\n"
            "SPC:DF_KAVA(3.0),A,TO,KAVA_PROD,2020,789\n"
        )
        shape = parse_csv_probe_response(csv_text)

        assert shape["observation_count"] == 3
        assert shape["series_count"] == 2  # FJ and TO
        assert shape["time_period_count"] == 2  # 2020 and 2021
        assert "GEO_PICT" in shape["dimensions"]
        assert shape["dimensions"]["GEO_PICT"]["distinct_count"] == 2
        assert set(shape["dimensions"]["GEO_PICT"]["sample_values"]) == {"FJ", "TO"}
        assert shape["has_time_dimension"] is True
        assert shape["geo_dimension_id"] == "GEO_PICT"
        assert len(shape["sample_observations"]) <= 5

    def test_parse_empty_csv(self):
        from tools.probing_tools import parse_csv_probe_response

        csv_text = "DATAFLOW,FREQ,GEO_PICT,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
        shape = parse_csv_probe_response(csv_text)

        assert shape["observation_count"] == 0
        assert shape["series_count"] == 0
        assert shape["time_period_count"] == 0

    def test_parse_csv_no_geo(self):
        from tools.probing_tools import parse_csv_probe_response

        csv_text = (
            "DATAFLOW,FREQ,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
            "SPC:DF(1.0),A,GDP,2020,100\n"
        )
        shape = parse_csv_probe_response(csv_text)

        assert shape["geo_dimension_id"] is None
        assert shape["has_time_dimension"] is True

    def test_parse_csv_no_time(self):
        from tools.probing_tools import parse_csv_probe_response

        csv_text = (
            "DATAFLOW,GEO,INDICATOR,OBS_VALUE\n"
            "SPC:DF(1.0),FJ,GDP,100\n"
        )
        shape = parse_csv_probe_response(csv_text)

        assert shape["has_time_dimension"] is False
        assert shape["time_period_count"] == 0

    def test_parse_csv_sample_limit(self):
        from tools.probing_tools import parse_csv_probe_response

        rows = ["SPC:DF(1.0),A,GDP,20{:02d},{}".format(i, i * 100) for i in range(20)]
        csv_text = "DATAFLOW,FREQ,INDICATOR,TIME_PERIOD,OBS_VALUE\n" + "\n".join(rows) + "\n"
        shape = parse_csv_probe_response(csv_text, sample_limit=5)

        assert shape["observation_count"] == 20
        assert len(shape["sample_observations"]) == 5

    def test_parse_csv_obs_value_missing(self):
        from tools.probing_tools import parse_csv_probe_response

        csv_text = (
            "DATAFLOW,FREQ,INDICATOR,TIME_PERIOD,OBS_VALUE\n"
            "SPC:DF(1.0),A,GDP,2020,\n"
        )
        shape = parse_csv_probe_response(csv_text)

        assert shape["observation_count"] == 1
        assert shape["sample_observations"][0]["value"] is None


class TestCandidateGeneration:
    """Test relaxation candidate generation."""

    def test_single_dimension_relaxations(self):
        from tools.probing_tools import generate_relaxation_candidates

        parsed = {
            "base_url": "https://example.org/rest",
            "dataflow_id": "DF_POP",
            "key": "A.FJ.POP_TOTAL",
            "key_parts": ["A", "FJ", "POP_TOTAL"],
            "start_period": "2020",
            "end_period": "2024",
            "params": {},
        }
        dim_names = ["FREQ", "GEO_PICT", "INDICATOR"]

        candidates = generate_relaxation_candidates(
            parsed, dim_names, relax_dimensions=None
        )

        # Should produce 3 single-dimension relaxations + 1 time widening
        single_dim = [c for c in candidates if "TIME_PERIOD" not in c["changed_dimensions"]]
        assert len(single_dim) == 3

        # Check that each relaxes one dimension
        for c in single_dim:
            assert len(c["changed_dimensions"]) == 1
            assert c["url"].startswith("https://example.org/rest/data/DF_POP/")

    def test_relaxation_with_restricted_dims(self):
        from tools.probing_tools import generate_relaxation_candidates

        parsed = {
            "base_url": "https://example.org/rest",
            "dataflow_id": "DF_POP",
            "key": "A.FJ.POP_TOTAL",
            "key_parts": ["A", "FJ", "POP_TOTAL"],
            "start_period": "2020",
            "end_period": "2024",
            "params": {},
        }
        dim_names = ["FREQ", "GEO_PICT", "INDICATOR"]

        candidates = generate_relaxation_candidates(
            parsed, dim_names, relax_dimensions=["GEO_PICT"]
        )

        # Should only relax GEO_PICT (no TIME_PERIOD in relax list)
        assert all("GEO_PICT" in c["changed_dimensions"] for c in candidates)

    def test_relaxation_preserves_time_params(self):
        from tools.probing_tools import generate_relaxation_candidates

        parsed = {
            "base_url": "https://example.org/rest",
            "dataflow_id": "DF_POP",
            "key": "A.FJ.POP_TOTAL",
            "key_parts": ["A", "FJ", "POP_TOTAL"],
            "start_period": "2020",
            "end_period": "2024",
            "params": {},
        }
        dim_names = ["FREQ", "GEO_PICT", "INDICATOR"]

        candidates = generate_relaxation_candidates(parsed, dim_names)

        # Non-time candidates should preserve time params
        non_time = [c for c in candidates if "TIME_PERIOD" not in c["changed_dimensions"]]
        for c in non_time:
            assert "startPeriod=2020" in c["url"]
            assert "endPeriod=2024" in c["url"]

    def test_time_widening_candidate(self):
        from tools.probing_tools import generate_relaxation_candidates

        parsed = {
            "base_url": "https://example.org/rest",
            "dataflow_id": "DF_POP",
            "key": "A.FJ.POP_TOTAL",
            "key_parts": ["A", "FJ", "POP_TOTAL"],
            "start_period": "2020",
            "end_period": "2024",
            "params": {},
        }
        dim_names = ["FREQ", "GEO_PICT", "INDICATOR"]

        candidates = generate_relaxation_candidates(parsed, dim_names)

        # Should include time-widened candidate (no time params)
        time_widened = [c for c in candidates if "TIME_PERIOD" in c["changed_dimensions"]]
        assert len(time_widened) >= 1
        for c in time_widened:
            assert "startPeriod" not in c["url"]
            assert "endPeriod" not in c["url"]

    def test_skips_already_wildcarded(self):
        from tools.probing_tools import generate_relaxation_candidates

        parsed = {
            "base_url": "https://example.org/rest",
            "dataflow_id": "DF_POP",
            "key": ".FJ.POP_TOTAL",
            "key_parts": ["", "FJ", "POP_TOTAL"],
            "start_period": None,
            "end_period": None,
            "params": {},
        }
        dim_names = ["FREQ", "GEO_PICT", "INDICATOR"]

        candidates = generate_relaxation_candidates(parsed, dim_names)

        # FREQ is already empty (wildcarded), should not appear
        freq_candidates = [c for c in candidates if "FREQ" in c["changed_dimensions"]]
        assert len(freq_candidates) == 0
        # No time params so no time widening either
        assert all("TIME_PERIOD" not in c["changed_dimensions"] for c in candidates)
