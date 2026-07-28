"""`_build_availableconstraint_key` builds the positional SDMx key segment
used in `/availableconstraint/<dataflow>/<key>/all/all` requests.

With no filters, the naive implementation appended an empty string per
non-time dimension and joined them, producing e.g. "..". Path normalisation
then collapses ".../DF_ADBKI/.." to "...", silently dropping the dataflow
from the request URL (verified live against SPC's /availableconstraint/
endpoint, which then answers 403). See docs/TODO.md and the ECB
compatibility verification notes for the incident this test guards against.
"""

import pytest

from sdmx_progressive_client import DataStructureSummary, DimensionInfo
from tools.sdmx_tools import _build_availableconstraint_key

pytestmark = pytest.mark.unit


class FakeClient:
    """Minimal stand-in exposing only what `_build_availableconstraint_key` uses."""

    async def get_structure_summary(self, dataflow_id: str, agency_id: str):
        dimensions = [
            DimensionInfo(id="FREQ", position=1, type="Dimension"),
            DimensionInfo(id="GEO_PICT", position=2, type="Dimension"),
            DimensionInfo(id="INDICATOR", position=3, type="Dimension"),
            DimensionInfo(id="TIME_PERIOD", position=4, type="TimeDimension"),
        ]
        return DataStructureSummary(
            id="DF_ADBKI",
            agency="SPC",
            version="1.0",
            dimensions=dimensions,
            key_family=["FREQ", "GEO_PICT", "INDICATOR"],
            attributes=[],
        )


@pytest.fixture
def client():
    return FakeClient()


@pytest.mark.asyncio
async def test_no_filters_yields_the_wildcard_key(client):
    """An all-empty key joins to '..', which path normalisation collapses,
    silently dropping the dataflow from the request URL."""
    key, time_period = await _build_availableconstraint_key(client, "DF_ADBKI", "SPC", None)
    assert key == "all"
    assert ".." not in key
    assert time_period is None


@pytest.mark.asyncio
async def test_empty_dict_filters_also_yield_the_wildcard_key(client):
    key, _ = await _build_availableconstraint_key(client, "DF_ADBKI", "SPC", {})
    assert key == "all"


@pytest.mark.asyncio
async def test_a_partial_key_is_preserved(client):
    """Partially-specified keys are legitimate SDMx and must survive."""
    key, _ = await _build_availableconstraint_key(client, "DF_ADBKI", "SPC", {"GEO_PICT": "FJ"})
    assert key.count(".") == 2  # three non-time dimensions
    assert "FJ" in key
    assert not key.startswith("..")


@pytest.mark.asyncio
async def test_time_filter_is_extracted_not_positional(client):
    key, time_period = await _build_availableconstraint_key(
        client, "DF_ADBKI", "SPC", {"TIME_PERIOD": "2020"}
    )
    assert time_period == "2020"
    assert "2020" not in key
