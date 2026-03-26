"""Unit tests for query probing models and utilities."""

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
