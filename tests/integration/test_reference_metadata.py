import httpx
import pytest
import respx

from tools.reference_metadata import (
    fetch_dsd_attribute_metadata,
    fetch_msd_metadata,
    parse_msd_csv,
)

pytestmark = pytest.mark.integration

# Shape of a real csvfilewithlabels response: interleaved id/label columns,
# metadata attributes identified by a dot in the column name.
MSD_CSV = (
    'STRUCTURE,STRUCTURE_ID,ACTION,FREQ,Frequency of observation,'
    'DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation,'
    'DATA_SOURCE.DATA_SOURCE_TITLE,Title of the dataset\n'
    'dataflow,SPC:DF_SDG(4.3),I,A,Annual,'
    '"en:""<p>UNSD</p>"",fr:""""","Source organisation",'
    '"en:""<p>SDG Global Database</p>""","Title of the dataset"\n'
)


def test_parse_msd_csv_extracts_attributes_with_hierarchy_and_labels():
    rows = parse_msd_csv(MSD_CSV)
    by_id = {r["id"]: r for r in rows}
    org = by_id["DATA_SOURCE_ORGANIZATION"]
    assert org["path"] == "DATA_SOURCE.DATA_SOURCE_ORGANIZATION"
    assert org["label"] == "Source organisation"
    assert org["value"] == "UNSD"
    assert org["language"] == "en"
    assert "DATA_SOURCE_TITLE" in by_id


def test_parse_msd_csv_ignores_dimension_columns():
    rows = parse_msd_csv(MSD_CSV)
    assert all("." in r["path"] for r in rows)
    assert not any(r["id"] == "FREQ" for r in rows)


def test_parse_msd_csv_on_an_empty_body():
    assert parse_msd_csv("") == []


class FakeClient:
    base_url = "https://example.org/rest"
    agency_id = "SPC"
    endpoint_key = "SPC"

    def __init__(self, version="4.3"):
        self._version = version
        self._session: httpx.AsyncClient | None = None

    async def resolve_version(self, dataflow_id, agency_id=None, version="latest", ctx=None):
        return self._version

    async def _get_session(self) -> httpx.AsyncClient:
        # respx intercepts at the transport level, so any httpx.AsyncClient
        # constructed while `@respx.mock` is active gets mocked responses;
        # this mirrors SDMXProgressiveClient._get_session lazily building one.
        if self._session is None:
            self._session = httpx.AsyncClient()
        return self._session


def _msd_url(version="4.3", key="all"):
    return ("https://example.org/rest/v2/data/dataflow/SPC/DF_SDG/"
            + version + "/" + key)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_uses_the_resolved_version_never_latest():
    """`latest` is a 2.1 keyword; the v2 endpoint answers 400 for it."""
    route = respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert status == "found"
    assert attrs
    requested = str(route.calls[0].request.url)
    assert "/4.3/" in requested
    assert "latest" not in requested
    assert "attributes=msd" in requested
    assert "measures=none" in requested


@pytest.mark.asyncio
@respx.mock
async def test_a_204_is_inconclusive_not_absence():
    """Several malformed-request cases answer 204, so it cannot be read as
    'this provider publishes no metadata'."""
    respx.get(url__startswith=_msd_url()).respond(204)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_200_with_no_metadata_columns_is_empty():
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,ACTION,FREQ,Frequency of observation\ndataflow,I,A,Annual\n")
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "empty"


@pytest.mark.asyncio
@respx.mock
async def test_a_200_carrying_an_html_error_page_is_inconclusive():
    """A maintenance page served with 200 must not read as 'no metadata'."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="<!DOCTYPE html><html><body>Service unavailable</body></html>")
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_genuine_sdmx_csv_with_no_metadata_columns_is_empty():
    """SBS's DF_CPI is a real case: the endpoint works, the dataflow has none."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,STRUCTURE_ID,ACTION,FREQ,Frequency of observation\n"
                  "dataflow,SBS:DF_CPI(1.0),I,A,Annual\n")
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "empty"


@pytest.mark.asyncio
@respx.mock
async def test_an_endpoint_without_v2_is_unsupported_without_a_request():
    class EcbClient(FakeClient):
        endpoint_key = "ECB"

    attrs, status = await fetch_msd_metadata(EcbClient(), "EXR", "ECB", "all")
    assert attrs == []
    assert status == "unsupported"


@pytest.mark.asyncio
@respx.mock
async def test_transport_failure_is_reported_not_raised():
    respx.get(url__startswith=_msd_url()).mock(side_effect=httpx.ConnectError("boom"))
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_version_resolution_failure_is_reported_not_raised():
    """resolve_version can itself hit the network (to look up 'latest') and
    raises ValueError on failure; that must not propagate out of here. No
    route is registered, so if the code skipped the guard and tried an HTTP
    call anyway, respx would fail the test rather than this assertion."""

    class FailingClient(FakeClient):
        async def resolve_version(self, dataflow_id, agency_id=None, version="latest", ctx=None):
            raise ValueError("could not resolve version")

    attrs, status = await fetch_msd_metadata(FailingClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_an_unparseable_response_is_inconclusive_not_raised():
    """A single field can exceed Python's csv field-size limit even when the
    whole response is well under UNKEYED_SIZE_CAP_BYTES -- e.g. a provider
    that concatenates many language translations into one value. That must
    surface as inconclusive, not as an unhandled exception."""
    huge_field = "x" * 200_000
    body = 'A,B.C\n1,"' + huge_field + '"'
    respx.get(url__startswith=_msd_url()).respond(200, text=body)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_huge_unkeyed_response_is_refused_with_a_usable_status():
    """SPC's DF_SDG is 5.37 MB unfiltered. Pulling that on every call would be
    rude to the provider and useless to the caller."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "STRUCTURE,A.B,label\n" + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    respx.get(url__startswith=_msd_url()).respond(200, text=huge)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", "all")
    assert attrs == []
    assert status == "too_broad"


@pytest.mark.asyncio
@respx.mock
async def test_a_keyed_response_is_never_refused_for_size():
    """With a key supplied the caller has already narrowed it; honour that."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = MSD_CSV + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    respx.get(url__startswith=_msd_url(key="A.G.SI_POV_DAY1")).respond(200, text=huge)
    attrs, status = await fetch_msd_metadata(
        FakeClient(), "DF_SDG", "SPC", "A.G.SI_POV_DAY1")
    assert status == "found"
    assert attrs


DATA_XML = (
    '<?xml version="1.0"?>'
    '<mes:StructureSpecificData '
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
    '<mes:DataSet ss:dataScope="DataStructure" '
    'FULL_DESCRIPTION="The Consumer Price Index dataset includes national indexes." '
    'LICENSE="(c) IMF. All rights reserved." CONTACT_POINT="datahelp@imf.org">'
    '<Series FREQ="A" SOURCE_AGENCY="4F0">'
    '<Obs TIME_PERIOD="2020" OBS_VALUE="1.5" NOTE_INDICATOR="Micro data processing"/>'
    '</Series></mes:DataSet></mes:StructureSpecificData>'
)


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_reads_all_three_attachment_levels():
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert status == "found"
    levels = {a["id"]: a["level"] for a in attrs}
    assert levels["FULL_DESCRIPTION"] == "dataset"
    assert levels["SOURCE_AGENCY"] == "series"
    assert levels["NOTE_INDICATOR"] == "observation"
    full = [a for a in attrs if a["id"] == "FULL_DESCRIPTION"][0]
    assert full["value"].startswith("The Consumer Price Index")


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_skips_structural_and_dimension_noise():
    """Envelope attributes and dimensions are not reference metadata."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    attrs, _status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    ids = {a["id"] for a in attrs}
    assert "dataScope" not in ids
    assert "TIME_PERIOD" not in ids
    assert "OBS_VALUE" not in ids


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_reports_empty_when_only_envelope_attributes():
    bare = ('<?xml version="1.0"?><mes:StructureSpecificData '
            'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
            'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
            '<mes:DataSet ss:dataScope="DataStructure"/></mes:StructureSpecificData>')
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=bare)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert attrs == []
    assert status == "empty"


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_reports_transport_failure():
    respx.get(url__startswith="https://example.org/rest/data/").mock(
        side_effect=httpx.ConnectError("boom"))
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert attrs == []
    assert status == "inconclusive"
