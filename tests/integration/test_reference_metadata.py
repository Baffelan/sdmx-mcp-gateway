import httpx
import pytest
import respx

from tools.reference_metadata import (
    fetch_dsd_attribute_metadata,
    fetch_msd_metadata,
    get_reference_metadata,
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
    rows = parse_msd_csv(MSD_CSV, dimension_ids={"FREQ"})
    by_id = {r["id"]: r for r in rows}
    org = by_id["DATA_SOURCE_ORGANIZATION"]
    assert org["path"] == "DATA_SOURCE.DATA_SOURCE_ORGANIZATION"
    assert org["label"] == "Source organisation"
    assert org["value"] == "UNSD"
    assert org["language"] == "en"
    assert "DATA_SOURCE_TITLE" in by_id


def test_parse_msd_csv_ignores_dimension_columns():
    rows = parse_msd_csv(MSD_CSV, dimension_ids={"FREQ"})
    assert all("." in r["path"] for r in rows)
    assert not any(r["id"] == "FREQ" for r in rows)


def test_parse_msd_csv_on_an_empty_body():
    assert parse_msd_csv("") == []


def test_flat_metadata_columns_are_found_for_providers_that_use_them():
    """OECD and FBOS use flat attribute names; only SPC uses dotted ones."""
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "QUALITY_ASSMNT,Quality management,REC_USE_LIM,Recommended uses\n"
        'dataflow,OECD:DF(1.0),I,NLD,Netherlands,'
        '"en:""<p>Assessed annually</p>""","Quality management",'
        '"en:""<p>Use with care</p>""","Recommended uses"\n'
    )
    rows = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
    ids = {r["id"] for r in rows}
    assert ids == {"QUALITY_ASSMNT", "REC_USE_LIM"}
    assert "REF_AREA" not in ids


def test_dotted_metadata_columns_still_work_and_keep_their_path():
    rows = parse_msd_csv(MSD_CSV, dimension_ids={"FREQ"})
    org = [r for r in rows if r["id"] == "DATA_SOURCE_ORGANIZATION"][0]
    assert org["path"] == "DATA_SOURCE.DATA_SOURCE_ORGANIZATION"
    assert not any(r["id"] == "FREQ" for r in rows)


def test_label_columns_are_never_treated_as_attributes():
    rows = parse_msd_csv(MSD_CSV, dimension_ids=set())
    assert all(" " not in r["path"] for r in rows)


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

    # Deliberately no get_structure_summary: its *absence* exercises
    # get_reference_metadata's fallback-to-empty-set path (see
    # test_a_broken_dimension_lookup_does_not_fail_the_whole_tool and every
    # other get_reference_metadata test above, all of which rely on that
    # fallback rather than defining a real structure summary).


class _FakeDimension:
    def __init__(self, id):
        self.id = id


class _FakeStructureSummary:
    def __init__(self, dimension_ids):
        self.dimensions = [_FakeDimension(d) for d in dimension_ids]


class DimAwareClient(FakeClient):
    """A FakeClient that can actually answer get_structure_summary, for
    tests that need to verify dimension ids reach both metadata channels."""

    async def get_structure_summary(self, dataflow_id, agency_id=None):
        return _FakeStructureSummary({"FREQ"})


def _msd_url(version="4.3", key=None):
    # The v2 endpoint has no "all" wildcard: the unkeyed form is an empty key
    # segment, i.e. the path ends with "/" and nothing after it.
    segment = key if key else ""
    return ("https://example.org/rest/v2/data/dataflow/SPC/DF_SDG/"
            + version + "/" + segment)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_uses_the_resolved_version_never_latest():
    """`latest` is a 2.1 keyword; the v2 endpoint answers 400 for it."""
    route = respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert status == "found"
    assert attrs
    requested = str(route.calls[0].request.url)
    assert "/4.3/" in requested
    assert "latest" not in requested
    assert "attributes=msd" in requested
    assert "measures=none" in requested


@pytest.mark.asyncio
@respx.mock
async def test_an_unkeyed_query_omits_the_key_segment_entirely():
    """The literal `all` is rejected: SPC answers 204 and OECD 422
    'Not enough key values'. The unkeyed form is an empty key segment."""
    route = respx.get(url__startswith="https://example.org/rest/v2/data/dataflow/SPC/DF_SDG/4.3/"
                      ).respond(200, text=MSD_CSV)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert status == "found"
    requested = str(route.calls[0].request.url)
    assert "/4.3/?" in requested
    assert "/all?" not in requested


@pytest.mark.asyncio
@respx.mock
async def test_a_204_is_inconclusive_not_absence():
    """Several malformed-request cases answer 204, so it cannot be read as
    'this provider publishes no metadata'."""
    respx.get(url__startswith=_msd_url()).respond(204)
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_200_with_no_metadata_columns_is_empty():
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,ACTION,FREQ,Frequency of observation\ndataflow,I,A,Annual\n")
    attrs, status = await fetch_msd_metadata(
        FakeClient(), "DF_SDG", "SPC", None, dimension_ids={"FREQ"})
    assert attrs == []
    assert status == "empty"


@pytest.mark.asyncio
@respx.mock
async def test_a_200_carrying_an_html_error_page_is_inconclusive():
    """A maintenance page served with 200 must not read as 'no metadata'."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="<!DOCTYPE html><html><body>Service unavailable</body></html>")
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_a_genuine_sdmx_csv_with_no_metadata_columns_is_empty():
    """SBS's DF_CPI is a real case: the endpoint works, the dataflow has none."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,STRUCTURE_ID,ACTION,FREQ,Frequency of observation\n"
                  "dataflow,SBS:DF_CPI(1.0),I,A,Annual\n")
    attrs, status = await fetch_msd_metadata(
        FakeClient(), "DF_SDG", "SPC", None, dimension_ids={"FREQ"})
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
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
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
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
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
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
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
async def test_dsd_fallback_drops_dimension_named_attributes_when_given_a_dimension_set():
    """A data message cannot tell a dimension from an attribute on its own --
    `FREQ` here is a Series-level dimension of the dataflow, not metadata --
    so the caller must supply the dataflow's dimension ids to filter it out."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all", dimension_ids={"FREQ"})
    assert status == "found"
    ids = {a["id"] for a in attrs}
    assert "FREQ" not in ids
    assert "SOURCE_AGENCY" in ids


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


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_reports_html_error_page_as_inconclusive():
    """ET.fromstring parses HTML happily, so 'it parsed' is not evidence we
    got the message we asked for."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text="<!DOCTYPE html><html><body>Service unavailable</body></html>")
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_reports_non_200_as_inconclusive():
    respx.get(url__startswith="https://example.org/rest/data/").respond(500)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert attrs == []
    assert status == "inconclusive"


@pytest.mark.asyncio
@respx.mock
async def test_assembles_the_msd_channel_and_reports_the_others():
    respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    result = await get_reference_metadata(FakeClient(), "DF_SDG", key=None)
    assert result["dataflow_id"] == "DF_SDG"
    assert result["version"] == "4.3"
    ids = {a["id"] for a in result["metadata_attributes"]}
    assert "DATA_SOURCE_ORGANIZATION" in ids
    assert result["channels"]["msd_v2"] == "found"
    sources = {a["source"] for a in result["metadata_attributes"]}
    assert "msd" in sources


@pytest.mark.asyncio
@respx.mock
async def test_the_assembly_fetches_dimensions_once_and_filters_the_msd_channel():
    """The bug this guards against: FREQ is a real dimension of DF_SDG, not
    reference metadata, and must not appear in the assembled result."""
    respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    result = await get_reference_metadata(DimAwareClient(), "DF_SDG", key=None)
    ids = {a["id"] for a in result["metadata_attributes"]}
    assert "FREQ" not in ids
    assert "DATA_SOURCE_ORGANIZATION" in ids


@pytest.mark.asyncio
@respx.mock
async def test_a_broken_dimension_lookup_does_not_fail_the_whole_tool():
    """get_structure_summary can itself fail (network, 404, ...); that must
    fall back to an empty dimension set rather than take the whole tool
    down with it."""

    class BrokenDimClient(FakeClient):
        async def get_structure_summary(self, dataflow_id, agency_id=None):
            raise RuntimeError("structure lookup failed")

    respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    result = await get_reference_metadata(BrokenDimClient(), "DF_SDG", key=None)
    assert result["channels"]["msd_v2"] == "found"
    assert any(a["id"] == "DATA_SOURCE_ORGANIZATION" for a in result["metadata_attributes"])


@pytest.mark.asyncio
@respx.mock
async def test_falls_back_when_the_provider_has_no_v2():
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    result = await get_reference_metadata(EcbClient(), "EXR", key="all")
    assert result["channels"]["msd_v2"] == "unsupported"
    assert result["channels"]["dsd_attributes"] == "found"
    assert any(a["source"] == "dsd_attribute" for a in result["metadata_attributes"])


@pytest.mark.asyncio
@respx.mock
async def test_reports_every_channel_empty_without_inventing_content():
    class EcbClient(FakeClient):
        endpoint_key = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_reference_metadata(EcbClient(), "EXR", key="all")
    assert result["metadata_attributes"] == []
    assert result["channels"]["msd_v2"] == "unsupported"
    assert result["notes"]


@pytest.mark.asyncio
@respx.mock
async def test_a_too_broad_result_does_not_claim_nothing_was_found():
    """The tool saw megabytes and refused them; saying 'none found' would be
    the opposite of true."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "STRUCTURE,A.B,label\n" + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    respx.get(url__startswith=_msd_url()).respond(200, text=huge)
    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_reference_metadata(FakeClient(), "DF_SDG", key=None)
    assert result["channels"]["msd_v2"] == "too_broad"
    joined = " ".join(result["notes"]).lower()
    assert "too large" in joined
    assert "no reference metadata was found" not in joined


class _FakeCtx:
    """Minimal MCP Context stand-in, mirroring test_cross_endpoint_tools.py."""

    def __init__(self, app_ctx, sid="default"):
        class RC:
            pass

        rc = RC()
        rc.lifespan_context = app_ctx
        rc.session_id = sid
        rc.meta = None
        self.request_context = rc
        self.session = None
        self.meta = None

    async def info(self, *args, **kwargs):
        pass


@pytest.mark.asyncio
async def test_the_wrapper_registers_the_dataflow_only_on_a_confirmed_channel():
    """A channel that actually reached the provider (found/empty/too_broad) is
    evidence the dataflow is real and worth remembering for mismatch hints."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def confirmed_impl(client, dataflow_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id or client.agency_id,
            "endpoint": "SPC",
            "version": "4.3",
            "metadata_attributes": [],
            "channels": {"msd_v2": "found", "dsd_attributes": "skipped"},
            "notes": [],
        }

    with patch("tools.reference_metadata.get_reference_metadata", side_effect=confirmed_impl):
        from main_server import get_reference_metadata as handler

        await handler(dataflow_id="DF_SDG", endpoint="SPC", ctx=ctx)

    session = app_ctx.get_session(ctx)
    assert "DF_SDG" in session.snapshot_known_dataflows().get("SPC", frozenset())


@pytest.mark.asyncio
async def test_the_wrapper_does_not_register_on_unsupported_or_inconclusive_channels():
    """Silence from every channel is not evidence the dataflow is real; it
    must not be recorded as if it were."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def unconfirmed_impl(client, dataflow_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id or client.agency_id,
            "endpoint": "ECB",
            "version": None,
            "metadata_attributes": [],
            "channels": {"msd_v2": "unsupported", "dsd_attributes": "inconclusive"},
            "notes": ["not configured for the v2 metadata endpoint"],
        }

    with patch("tools.reference_metadata.get_reference_metadata", side_effect=unconfirmed_impl):
        from main_server import get_reference_metadata as handler

        await handler(dataflow_id="EXR", endpoint="ECB", ctx=ctx)

    session = app_ctx.get_session(ctx)
    assert "EXR" not in session.snapshot_known_dataflows().get("ECB", frozenset())
