"""ECB never implemented standard SDMx-CSV: it answers 406 for
`application/vnd.sdmx.data+csv;version=1.0.0` and 200 for `text/csv`
(verified live 2026-07-27). The Accept header must therefore be per-provider."""

import pytest

from config import get_data_accept
from tools.sdmx_tools import _get_accept_header

pytestmark = pytest.mark.unit

SDMX_CSV = "application/vnd.sdmx.data+csv;version=1.0.0"


def test_ecb_csv_uses_plain_text_csv():
    assert get_data_accept("ECB", "csv") == "text/csv"


def test_providers_without_an_override_keep_the_standard_type():
    for key in ("SPC", "IMF", "UNICEF", "ILO", "ABS"):
        assert get_data_accept(key, "csv") == SDMX_CSV


def test_unknown_endpoint_falls_back_to_the_standard_type():
    assert get_data_accept(None, "csv") == SDMX_CSV
    assert get_data_accept("NOT_A_PROVIDER", "csv") == SDMX_CSV


def test_non_csv_formats_are_untouched_for_ecb():
    """Only CSV diverges; ECB serves standard SDMx XML."""
    assert get_data_accept("ECB", "xml") == "application/vnd.sdmx.genericdata+xml;version=2.1"


def test_accept_header_helper_is_unchanged_without_an_endpoint():
    assert _get_accept_header("csv") == SDMX_CSV
    assert _get_accept_header("xml") == "application/vnd.sdmx.genericdata+xml;version=2.1"


def test_accept_header_helper_honours_the_endpoint():
    assert _get_accept_header("csv", "ECB") == "text/csv"
    assert _get_accept_header("csv", "SPC") == SDMX_CSV


def test_ecb_sdmx_csv_alias_also_uses_plain_text_csv():
    """The 'sdmx-csv' alias resolves to the same standard type as 'csv'
    everywhere else, so ECB's override must cover it too."""
    assert get_data_accept("ECB", "sdmx-csv") == "text/csv"
    assert get_data_accept("SPC", "sdmx-csv") == SDMX_CSV


@pytest.mark.asyncio
async def test_build_data_url_usage_line_uses_the_endpoint_media_type():
    """The curl line the tool hands the user must actually work."""
    from tools import sdmx_tools

    class FakeClient:
        agency_id = "ECB"
        base_url = "https://data-api.ecb.europa.eu/service"
        endpoint_key = "ECB"

        async def get_structure_summary(self, *a, **k):
            return {"dimensions": []}

    result = await sdmx_tools.build_data_url(
        client=FakeClient(), dataflow_id="EXR", key="D.USD.EUR.SP00.A",
        output_format="csv",
    )
    assert "text/csv" in result["usage"]
    assert "vnd.sdmx.data+csv" not in result["usage"]
    assert result["headers"]["Accept"] == "text/csv"
