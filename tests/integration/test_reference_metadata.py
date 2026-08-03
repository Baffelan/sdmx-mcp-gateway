import httpx
import pytest
import respx

import tools.reference_metadata as reference_metadata_module
from tools.reference_metadata import (
    _row_level,
    fetch_dsd_attribute_metadata,
    fetch_msd_metadata,
    get_metadata_attribute_values,
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


def test_row_level_is_dataflow_when_every_dimension_cell_is_wildcard_or_empty():
    """A row with no concrete dimension value was not narrowed by a key, so
    whatever metadata it carries describes the whole dataflow."""
    level, context = _row_level(
        ["~", "", "~"], [(0, "FREQ"), (1, "REF_AREA"), (2, "ADJUSTMENT")]
    )
    assert level == "dataflow"
    assert context == {}


def test_row_level_is_partial_key_when_any_dimension_cell_is_concrete():
    """Row 1 of OECD's real HICP response is exactly this shape: REF_AREA=GBR
    with every other dimension wildcarded."""
    level, context = _row_level(
        ["A", "GBR", "~"], [(0, "FREQ"), (1, "REF_AREA"), (2, "ADJUSTMENT")]
    )
    assert level == "partial_key"
    assert context == {"FREQ": "A", "REF_AREA": "GBR"}


def test_parse_msd_csv_finds_a_value_that_only_appears_in_a_later_row():
    """Real MSD responses are one row per attachment target: OECD's HICP
    query returns 111 rows, and a metadata column can be empty in early rows
    and populated only in a later one (REC_USE_LIM is empty in 107 of
    OECD's 111 rows). Reading only the first data row drops it entirely."""
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "REC_USE_LIM,Recommended uses\n"
        "dataflow,OECD:DF(1.0),I,AUS,Australia,,\n"
        "dataflow,OECD:DF(1.0),I,GBR,United Kingdom,,\n"
        'dataflow,OECD:DF(1.0),I,USA,United States,'
        '"en:""<p>Use with care</p>""","Recommended uses"\n'
    )
    rows = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
    by_id = {r["id"]: r for r in rows}
    assert "REC_USE_LIM" in by_id
    assert by_id["REC_USE_LIM"]["value"] == "Use with care"
    assert by_id["REC_USE_LIM"]["scope"] == "partial_key"
    assert by_id["REC_USE_LIM"]["key_context"] == {"REF_AREA": "USA"}


def test_a_dataflow_level_value_is_the_headline_over_a_disagreeing_partial_key_one():
    """When rows disagree, a value that describes the whole dataflow is a
    more useful headline than one that only describes a slice of it -- but
    the slice-specific value must still be visible in `all_values`, not
    silently dropped, even though only the headline's own `key_context` is
    exposed structurally."""
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "COVERAGE,Coverage\n"
        'dataflow,OECD:DF(1.0),I,~,~,'
        '"en:""<p>Whole country</p>""","Coverage"\n'
        'dataflow,OECD:DF(1.0),I,GBR,United Kingdom,'
        '"en:""<p>United Kingdom only</p>""","Coverage"\n'
    )
    rows = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
    coverage = [r for r in rows if r["id"] == "COVERAGE"][0]
    assert coverage["value"] == "Whole country"
    assert coverage["scope"] == "dataflow"
    assert coverage["key_context"] is None
    assert coverage["distinct_value_count"] == 2
    values = [v["value"] for v in coverage["all_values"]]
    assert values == ["Whole country", "United Kingdom only"]


def test_msd_headline_scope_is_independent_of_row_order():
    """SDMx-CSV row order is not part of the contract, so the same value
    reappearing on a dataflow-wide row must be recognised as describing the
    whole dataflow whether that row comes before or after the partial-key
    row that first carried the identical text -- otherwise the same
    response could parse to a different headline on different calls."""
    csv_keyed_first = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "METHODOLOGY,Methodology\n"
        "dataflow,IMF:DF(1.0),I,GBR,United Kingdom,"
        "Compiled to BPM6,Methodology\n"
        "dataflow,IMF:DF(1.0),I,~,~,"
        "Compiled to BPM6,Methodology\n"
    )
    csv_dataflow_first = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "METHODOLOGY,Methodology\n"
        "dataflow,IMF:DF(1.0),I,~,~,"
        "Compiled to BPM6,Methodology\n"
        "dataflow,IMF:DF(1.0),I,GBR,United Kingdom,"
        "Compiled to BPM6,Methodology\n"
    )
    for csv_text in (csv_keyed_first, csv_dataflow_first):
        rows = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
        methodology = [r for r in rows if r["id"] == "METHODOLOGY"][0]
        assert methodology["scope"] == "dataflow"
        assert methodology["value"] == "Compiled to BPM6"
        assert methodology["key_context"] is None
        assert methodology["distinct_value_count"] == 1


def test_metadata_attribute_schema_preserves_attachment_context():
    """A partial_key value is only meaningful if the caller can see the key.

    Four disagreeing per-country values is exactly the shape the headline
    rule withholds a value for, so `value` stays null and `drill_down` is
    true even though `sample_key_context` still names one example key."""
    from models.schemas import MetadataAttribute

    attr = MetadataAttribute(
        id="REC_USE_LIM", path="REC_USE_LIM", label="Recommended uses",
        status="populated", scope="partial_key", value=None, language=None,
        sample_key_context={"REF_AREA": "GBR"}, distinct_values=4, drill_down=True,
    )
    assert attr.sample_key_context == {"REF_AREA": "GBR"}
    assert attr.distinct_values == 4
    assert attr.value is None
    assert attr.drill_down is True


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
    tests that need to verify dimension ids reach both metadata channels.

    Defaults to `{"FREQ"}`, DF_SDG's own dimension, but accepts any set so
    tests that need a different dimension excluded from the metadata columns
    (e.g. `REF_AREA`, to exercise per-country values) can supply it.
    """

    def __init__(self, version="4.3", dimension_ids=frozenset({"FREQ"})):
        super().__init__(version=version)
        self._dimension_ids = dimension_ids

    async def get_structure_summary(self, dataflow_id, agency_id=None):
        return _FakeStructureSummary(self._dimension_ids)


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
    """With a key supplied the caller has already narrowed it; honour that.

    The padding is many small rows rather than one giant field: parse_msd_csv
    now reads every row (that is the whole point of the fix it accompanies),
    so a single ~2 MB field would trip Python's csv field-size limit and be
    read as a parse failure rather than exercising the size guard at all."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = MSD_CSV + ("x\n" * (UNKEYED_SIZE_CAP_BYTES // 2 + 10))
    respx.get(url__startswith=_msd_url(key="A.G.SI_POV_DAY1")).respond(200, text=huge)
    attrs, status = await fetch_msd_metadata(
        FakeClient(), "DF_SDG", "SPC", "A.G.SI_POV_DAY1")
    assert status == "found"
    assert attrs


@pytest.mark.asyncio
@respx.mock
async def test_an_unkeyed_response_is_refused_by_content_length_before_the_body_is_read():
    """A provider that sends an honest Content-Length header lets the guard
    trip before any of the body is read at all, not just after it has all
    arrived -- the bug this fix exists for was `session.get` fully
    materialising the body before the size check ever ran."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    respx.get(url__startswith=_msd_url()).respond(
        200,
        text="STRUCTURE,A.B,label\nshort\n",
        headers={"content-length": str(UNKEYED_SIZE_CAP_BYTES + 1)},
    )
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert attrs == []
    assert status == "too_broad"


@pytest.mark.asyncio
@respx.mock
async def test_an_unkeyed_response_without_a_content_length_header_is_still_bounded():
    """Some providers use chunked transfer and send no Content-Length at
    all; the guard must still trip from what has actually been read as it
    streams in, not rely on a header that may not be there."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    async def chunks():
        sent = 0
        chunk = b"x" * 200_000
        while sent < UNKEYED_SIZE_CAP_BYTES + 500_000:
            yield chunk
            sent += len(chunk)

    respx.get(url__startswith=_msd_url()).mock(
        return_value=httpx.Response(200, stream=chunks())
    )
    attrs, status = await fetch_msd_metadata(FakeClient(), "DF_SDG", "SPC", None)
    assert attrs == []
    assert status == "too_broad"


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
    scopes = {a["id"]: a["scope"] for a in attrs}
    assert scopes["FULL_DESCRIPTION"] == "dataset"
    assert scopes["SOURCE_AGENCY"] == "series"
    assert scopes["NOTE_INDICATOR"] == "observation"
    full = [a for a in attrs if a["id"] == "FULL_DESCRIPTION"][0]
    assert full["value"].startswith("The Consumer Price Index")


IMF_DATASET_ATTRIBUTES_XML = (
    '<?xml version="1.0"?>'
    '<mes:StructureSpecificData '
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
    '<mes:DataSet ss:dataScope="DataStructure" '
    'METHODOLOGY="Consumer prices are compiled per COICOP." '
    'LICENSE="(c) IMF. All rights reserved.">'
    '<Series FREQ="A" REF_AREA="US">'
    '<Obs TIME_PERIOD="2020" OBS_VALUE="1.5"/>'
    '</Series></mes:DataSet></mes:StructureSpecificData>'
)


@pytest.mark.asyncio
@respx.mock
async def test_dsd_channel_reports_scope_and_status_like_the_msd_channel():
    """IMF, ECB and ILO have no /v2/ endpoint, so this channel is their only
    metadata. It must speak the same shape or the summary drops it."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text=IMF_DATASET_ATTRIBUTES_XML)
    attributes, _state = await fetch_dsd_attribute_metadata(
        client=FakeClient(), dataflow_id="CPI", agency_id="IMF.STA", key="A.US.PCPI_IX",
    )
    attr = next(a for a in attributes if a["id"] == "METHODOLOGY")
    assert attr["status"] == "populated"
    assert attr["scope"] == "dataset"
    assert attr["path"] == "METHODOLOGY"
    assert isinstance(attr["all_values"], list) and attr["all_values"]
    assert "level" not in attr


REPEATED_OBS_ATTRIBUTE_XML = (
    '<?xml version="1.0"?>'
    '<mes:StructureSpecificData '
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
    '<mes:DataSet ss:dataScope="DataStructure">'
    '<Series FREQ="A">'
    '<Obs TIME_PERIOD="2018" OBS_VALUE="1.1" OBS_COMMENT="Provisional"/>'
    '<Obs TIME_PERIOD="2019" OBS_VALUE="1.2" OBS_COMMENT="Revised"/>'
    '<Obs TIME_PERIOD="2020" OBS_VALUE="1.3" OBS_COMMENT="Provisional"/>'
    '</Series></mes:DataSet></mes:StructureSpecificData>'
)


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_dedupes_repeated_values_and_keeps_first_seen_order():
    """OBS_COMMENT is an observation-level attribute, so it shows up on every
    Obs element that carries it -- here three times, with the first value
    ("Provisional") repeated on the third Obs. distinct_value_count must
    count distinct values, not occurrences, all_values must not repeat the
    duplicate, and the order must be the order the values were first seen
    in, not last-write-wins or alphabetical."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text=REPEATED_OBS_ATTRIBUTE_XML)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert status == "found"
    comment = next(a for a in attrs if a["id"] == "OBS_COMMENT")
    assert comment["distinct_value_count"] == 2
    values = [v["value"] for v in comment["all_values"]]
    assert values == ["Provisional", "Revised"]
    assert values.count("Provisional") == 1


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
async def test_dsd_fallback_is_refused_for_an_oversized_unkeyed_body():
    """`firstNObservations=1` still returns one row per series: an unkeyed
    request against a large dataflow (ECB's EXR, IMF's CPI) is multi-
    megabyte, the same size problem the MSD channel guards against -- but
    the live checks that shaped this fallback never hit it, since those
    cases were all keyed."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "<x>" + ("y" * (UNKEYED_SIZE_CAP_BYTES + 1000)) + "</x>"
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=huge)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "all")
    assert attrs == []
    assert status == "too_broad"


@pytest.mark.asyncio
@respx.mock
async def test_dsd_fallback_with_a_real_key_is_never_refused_for_size():
    """With a key supplied the caller has already narrowed the request;
    honour that, the same way the MSD channel does."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    padding = "<!-- " + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1000)) + " -->"
    huge = DATA_XML.replace("</mes:DataSet>", padding + "</mes:DataSet>")
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=huge)
    attrs, status = await fetch_dsd_attribute_metadata(
        FakeClient(), "CPI", "IMF.STA", "A.USD")
    assert status == "found"
    assert any(a["id"] == "FULL_DESCRIPTION" for a in attrs)


GENERIC_DATA_XML = (
    '<?xml version="1.0"?>'
    '<mes:GenericData '
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:generic="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">'
    '<mes:DataSet>'
    '<generic:Series>'
    '<generic:Attributes>'
    '<generic:Value id="LICENSE" value="(c) IMF. All rights reserved."/>'
    '</generic:Attributes>'
    '<generic:Obs>'
    '<generic:ObsDimension value="2020"/>'
    '<generic:ObsValue value="1.5"/>'
    '</generic:Obs>'
    '</generic:Series>'
    '</mes:DataSet></mes:GenericData>'
)


@pytest.mark.asyncio
@respx.mock
async def test_a_generic_dialect_message_is_inconclusive_not_empty():
    """A provider that ignores our structure-specific Accept header can
    still answer 200 with a well-formed SDMx-ML Generic message, which
    reuses the same DataSet/Series/Obs local names but carries attribute
    values as child <generic:Value> elements rather than XML attributes.
    Zero attributes found there is not evidence the dataflow carries no
    metadata; it is evidence we got a dialect this fallback does not parse."""
    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text=GENERIC_DATA_XML)
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


@pytest.mark.asyncio
async def test_the_wrapper_registers_when_dsd_attributes_is_too_broad():
    """When dsd_attributes returns too_broad (200 response refused for size),
    that still proves the dataflow exists on the endpoint and should be
    registered as confirmed, preventing spurious mismatch hints."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def too_broad_impl(client, dataflow_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id or client.agency_id,
            "endpoint": "ECB",
            "version": None,
            "metadata_attributes": [],
            "channels": {"msd_v2": "unsupported", "dsd_attributes": "too_broad"},
            "notes": ["DSD fallback response too large to process unkeyed"],
        }

    with patch("tools.reference_metadata.get_reference_metadata", side_effect=too_broad_impl):
        from main_server import get_reference_metadata as handler

        await handler(dataflow_id="EXR", endpoint="ECB", ctx=ctx)

    session = app_ctx.get_session(ctx)
    assert "EXR" in session.snapshot_known_dataflows().get("ECB", frozenset())


@pytest.mark.asyncio
async def test_the_wrapper_carries_key_context_through_for_a_partial_key_attribute():
    """A caller reading the MCP tool's structured `ReferenceMetadataResult`,
    not the raw dict `get_reference_metadata_impl` returns, must still be
    able to see which key a partial_key value applies to -- that is the
    whole point of putting `sample_key_context` on `MetadataAttribute` at
    all. Every one of OECD's HICP attributes lands as partial_key in
    practice, so this is the common case being exercised, not an edge one.
    Four disagreeing per-country values also means the headline rule
    withholds `value`, so this pins that behaviour through the wrapper too."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def partial_key_impl(client, dataflow_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id or client.agency_id,
            "endpoint": "OECD",
            "version": "1.0",
            "metadata_attributes": [{
                "id": "REC_USE_LIM",
                "path": "REC_USE_LIM",
                "label": "Recommended uses",
                "status": "populated",
                "scope": "partial_key",
                "value_kind": "unknown",
                "distinct_values": 4,
                "value": None,
                "language": None,
                "sample_key_context": {"REF_AREA": "GBR"},
                "drill_down": True,
            }],
            "channels": {"msd_v2": "found", "dsd_attributes": "skipped"},
            "notes": [],
        }

    with patch("tools.reference_metadata.get_reference_metadata", side_effect=partial_key_impl):
        from main_server import get_reference_metadata as handler

        result = await handler(dataflow_id="DF_PRICES_HICP", endpoint="OECD", ctx=ctx)

    attr = result.metadata_attributes[0]
    assert attr.scope == "partial_key"
    assert attr.sample_key_context == {"REF_AREA": "GBR"}
    assert attr.distinct_values == 4
    assert attr.value is None
    assert attr.drill_down is True


# =============================================================================
# Headline rule and coverage (get_reference_metadata summary shape)
# =============================================================================

REC_USE_LIM_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "REC_USE_LIM,Recommended uses\n"
    "dataflow,OECD:DF(1.0),I,AUS,Australia,"
    "Interpret with caution in Australia,Recommended uses\n"
    "dataflow,OECD:DF(1.0),I,GBR,United Kingdom,"
    "Interpret with caution in the United Kingdom,Recommended uses\n"
    "dataflow,OECD:DF(1.0),I,USA,United States,"
    "Interpret with caution in the United States,Recommended uses\n"
    "dataflow,OECD:DF(1.0),I,NZL,New Zealand,"
    "Interpret with caution in New Zealand,Recommended uses\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_summary_withholds_a_headline_for_a_per_country_value():
    """OECD's REC_USE_LIM differs by country. Returning Australia's text as
    the answer means a question about Japan gets Australia's caveats."""
    respx.get(url__startswith=_msd_url()).respond(200, text=REC_USE_LIM_MSD_CSV)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    attr = next(a for a in result["metadata_attributes"] if a["id"] == "REC_USE_LIM")
    assert attr["value"] is None
    assert attr["distinct_values"] == 4
    assert attr["drill_down"] is True
    assert attr["sample_key_context"] == {"REF_AREA": "AUS"}


QUALITY_ASSMNT_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "QUALITY_ASSMNT,Quality management\n"
    "dataflow,OECD:DF(1.0),I,ITA,Italy,"
    "Assessed for Italy only,Quality management\n"
    "dataflow,OECD:DF(1.0),I,FRA,France,,\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_single_value_at_partial_key_still_withholds_the_headline():
    """QUALITY_ASSMNT has one value, describing Italy alone; France's row
    leaves it blank. Under the all_observed_rows rule the discriminator is
    whether a value appeared on every row a query read, not whether it is
    the only distinct value seen, so this needs a second, blank row to stay
    withheld. A single-row response would trivially satisfy "every row"
    and is covered separately (all_observed_rows tests below)."""
    respx.get(url__startswith=_msd_url()).respond(200, text=QUALITY_ASSMNT_MSD_CSV)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    attr = next(a for a in result["metadata_attributes"] if a["id"] == "QUALITY_ASSMNT")
    assert attr["value"] is None
    assert attr["drill_down"] is True


COMPILING_ORG_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "COMPILING_ORG,Compiling organisation\n"
    "dataflow,FBOS:DF(1.0),I,~,~,"
    "Fiji Bureau of Statistics,Compiling organisation\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_dataflow_wide_single_value_keeps_its_headline():
    """FBOS's compiling agency applies to the whole dataflow, so withholding
    it would be unhelpful in the other direction."""
    respx.get(url__startswith=_msd_url()).respond(200, text=COMPILING_ORG_MSD_CSV)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    attr = next(a for a in result["metadata_attributes"] if a["id"] == "COMPILING_ORG")
    assert attr["value"] == "Fiji Bureau of Statistics"
    assert attr["drill_down"] is False


DF_SDG_SINGLE_VALUE_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,FJI,Fiji,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,TON,Tonga,"
    "UNSD,Source organisation\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_a_value_on_every_row_gets_a_headline_but_keeps_drill_down():
    """The approved fix for SPC's real DF_SDG: it publishes the same value
    on every per-country row and has no dataflow-wide row at all, so every
    row is partial_key and every populated attribute used to come back
    value: null, drill_down: true. The summary showed no readable values
    at all on the gateway's default provider. The value now surfaces as a
    headline, but drill_down stays true since the per-row detail is still
    reachable and the value only describes the rows this query returned."""
    respx.get(url__startswith=_msd_url()).respond(200, text=DF_SDG_SINGLE_VALUE_MSD_CSV)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    attr = next(a for a in result["metadata_attributes"]
                if a["id"] == "DATA_SOURCE_ORGANIZATION")
    assert attr["scope"] == "all_observed_rows"
    assert attr["value"] == "UNSD"
    assert attr["drill_down"] is True
    assert attr["sample_key_context"] is None
    joined = " ".join(result["notes"]).lower()
    assert "identical on every" in joined
    assert "not covered" in joined or "rows outside" in joined


SINGLE_ROW_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "QUALITY_ASSMNT,Quality management\n"
    "dataflow,OECD:DF(1.0),I,ITA,Italy,"
    "Assessed for Italy only,Quality management\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_a_single_row_response_trivially_satisfies_every_row():
    """A response with exactly one data row satisfies "appeared on every
    row read" by construction: there is nothing else it could disagree
    with. This is a deliberate consequence of the all_observed_rows rule,
    pinned here so it reads as intentional rather than an untested edge
    case; test_single_value_at_partial_key_still_withholds_the_headline
    above is the contrasting case where a second, blank row keeps the
    value withheld."""
    respx.get(url__startswith=_msd_url()).respond(200, text=SINGLE_ROW_MSD_CSV)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    attr = next(a for a in result["metadata_attributes"] if a["id"] == "QUALITY_ASSMNT")
    assert attr["scope"] == "all_observed_rows"
    assert attr["value"] == "Assessed for Italy only"
    assert attr["drill_down"] is True


COVERAGE_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,Frequency of observation,"
    "DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation,"
    "DATA_SOURCE.DATA_SOURCE_TITLE,Title of the dataset,"
    "DATA_SOURCE.DATA_SOURCE_LICENSE,Licence,"
    "DATA_SOURCE.DATA_SOURCE_DATE,Date sourced,"
    "DATA_SOURCE.DATA_SOURCE_URL,Source URL,"
    "DATA_SOURCE.DATA_SOURCE_TYPE,Source type,"
    "QUALITY.QUALITY_ASSESSMENT,Quality assessment,"
    "QUALITY.QUALITY_DOC,Quality documentation,"
    "COMPILATION.COMPILATION_ORG,Compiling organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,~,~,"
    "UNSD,Source organisation,"
    "SDG Global Database,Title of the dataset,"
    ",Licence,"
    ",Date sourced,"
    "https://example.org/source,Source URL,"
    "Administrative data,Source type,"
    "Assessed annually,Quality assessment,"
    "See methodology,Quality documentation,"
    "Pacific Data Hub,Compiling organisation\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_coverage_counts_declared_populated_and_empty():
    """SPC's DF_SDG declares 9 and populates 7; the 2 blanks include the
    licence, which is exactly what a caller asks about."""
    respx.get(url__startswith=_msd_url()).respond(200, text=COVERAGE_MSD_CSV)
    result = await get_reference_metadata(DimAwareClient(), "DF_SDG", key=None)
    assert result["coverage"] == {"declared": 9, "populated": 7, "empty": 2}
    licence = next(a for a in result["metadata_attributes"]
                   if a["id"] == "DATA_SOURCE_LICENSE")
    assert licence["status"] == "declared_empty"


@pytest.mark.asyncio
@respx.mock
async def test_coverage_is_none_when_no_channel_gave_a_confirmed_answer():
    """SPC's DF_SDG unkeyed is the real case this pins: the MSD channel is
    too_broad (it saw megabytes and refused them) and the DSD fallback is
    inconclusive. Zero attributes here is not evidence of zero declared --
    it is evidence we do not know, so `coverage` must stay null rather than
    assert `{"declared": 0, ...}`, which would flatly contradict the
    too_broad note saying the metadata exists and is too large to return."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "STRUCTURE,A.B,label\n" + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    respx.get(url__startswith=_msd_url()).respond(200, text=huge)
    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_reference_metadata(FakeClient(), "DF_SDG", key=None)
    assert result["channels"]["msd_v2"] == "too_broad"
    assert result["coverage"] is None


@pytest.mark.asyncio
@respx.mock
async def test_coverage_is_none_when_only_the_dsd_channel_answered():
    """The DSD-attribute channel reads only what a data message actually
    carries, so it cannot see a declared-but-empty attribute the way the
    MSD channel's `declared_empty` status can. Reporting `declared`/`empty`
    from it would just relabel `populated` as `declared` and always claim
    zero blanks -- exactly the shape IMF, ECB, ILO and ESTAT would get on
    every call, since none of them has an MSD channel at all."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    result = await get_reference_metadata(EcbClient(), "EXR", key="all")
    assert result["channels"]["dsd_attributes"] == "found"
    assert result["coverage"] is None
    assert any("declared set is not observable" in note for note in result["notes"])


@pytest.mark.asyncio
@respx.mock
async def test_a_truncated_msd_response_notes_that_declared_empty_may_be_an_artefact(
    monkeypatch,
):
    """I1: a column populated only beyond the row cap must not silently read
    as a plain declared_empty. This lowers the cap so a 3-row fixture can
    exceed it, then pins that get_reference_metadata surfaces a note
    saying the response was cut off."""
    monkeypatch.setattr(reference_metadata_module, "_MAX_MSD_DATA_ROWS", 2)
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "COVERAGE,Coverage\n"
        "dataflow,OECD:DF(1.0),I,AUS,Australia,,\n"
        "dataflow,OECD:DF(1.0),I,GBR,United Kingdom,,\n"
        'dataflow,OECD:DF(1.0),I,USA,United States,'
        '"en:""<p>Whole thing</p>""","Coverage"\n'
    )
    respx.get(url__startswith=_msd_url()).respond(200, text=csv_text)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"REF_AREA"}), "DF_SDG", key=None)
    coverage_attr = next(a for a in result["metadata_attributes"] if a["id"] == "COVERAGE")
    assert coverage_attr["status"] == "declared_empty"
    assert any(
        "truncat" in n.lower() or "cut off" in n.lower() for n in result["notes"]
    )


@pytest.mark.asyncio
@respx.mock
async def test_coverage_is_none_when_msd_is_empty_and_dsd_is_unresolved():
    """C2: the MSD channel can legitimately confirm zero metadata columns
    (status "empty") while the DSD fallback that always runs alongside it
    fails to resolve (here, inconclusive). `coverage` must not assert
    {declared: 0, populated: 0, empty: 0} next to a note saying the DSD
    query did not produce a usable result -- that numeric zero reads as a
    confirmed fact, not as "we don't know"."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,ACTION,FREQ,Frequency of observation\ndataflow,I,A,Annual\n")
    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_reference_metadata(
        DimAwareClient(dimension_ids={"FREQ"}), "DF_SDG", key=None)
    assert result["channels"]["msd_v2"] == "empty"
    assert result["channels"]["dsd_attributes"] == "inconclusive"
    assert result["coverage"] is None


@pytest.mark.asyncio
@respx.mock
async def test_dataset_scope_keeps_its_headline_but_series_and_observation_do_not():
    """The headline rule's dataflow-wide scopes are `dataflow` (MSD channel)
    and `dataset` (DSD channel). All of the earlier headline-rule tests
    drive the MSD channel only, so narrowing `_DATAFLOW_WIDE_SCOPES` to
    just `{"dataflow"}` would silently strip every DSD-sourced headline --
    IMF's dataset-level attributes among them -- without failing anything
    else in the suite. This drives the DSD channel directly and checks all
    three of its scopes in one message: `dataset` keeps its value,
    `series` and `observation` do not, even though each is the only value
    seen for its attribute."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    result = await get_reference_metadata(EcbClient(), "EXR", key="all")
    by_id = {a["id"]: a for a in result["metadata_attributes"]}

    dataset_attr = by_id["FULL_DESCRIPTION"]
    assert dataset_attr["scope"] == "dataset"
    assert dataset_attr["value"] is not None
    assert dataset_attr["drill_down"] is False

    series_attr = by_id["SOURCE_AGENCY"]
    assert series_attr["scope"] == "series"
    assert series_attr["value"] is None
    assert series_attr["drill_down"] is True

    observation_attr = by_id["NOTE_INDICATOR"]
    assert observation_attr["scope"] == "observation"
    assert observation_attr["value"] is None
    assert observation_attr["drill_down"] is True


@pytest.mark.asyncio
async def test_the_wrapper_builds_a_populated_metadata_coverage():
    """`coverage` is built via `MetadataCoverage(**result["coverage"])` in
    the wrapper; nothing else in this suite exercises that construction
    with a non-null `coverage` dict (the other wrapper-level tests mock
    returns that omit the key entirely), so a key-name mismatch between the
    raw dict and the model would ship undetected."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def coverage_impl(client, dataflow_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id or client.agency_id,
            "endpoint": "SPC",
            "version": "4.3",
            "metadata_attributes": [],
            "coverage": {"declared": 9, "populated": 7, "empty": 2},
            "channels": {"msd_v2": "found", "dsd_attributes": "skipped"},
            "notes": [],
        }

    with patch("tools.reference_metadata.get_reference_metadata", side_effect=coverage_impl):
        from main_server import get_reference_metadata as handler

        result = await handler(dataflow_id="DF_SDG", endpoint="SPC", ctx=ctx)

    assert result.coverage is not None
    assert result.coverage.declared == 9
    assert result.coverage.populated == 7
    assert result.coverage.empty == 2


# =============================================================================
# get_metadata_attribute_values (the drill-down call)
# =============================================================================


def _drill_msd_url(dataflow_id, version="4.3", key=None):
    # Same shape as _msd_url above, generalised to a dataflow id other than
    # DF_SDG so these tests can use realistic dataflow names.
    segment = key if key else ""
    return ("https://example.org/rest/v2/data/dataflow/SPC/" + dataflow_id + "/"
            + version + "/" + segment)


HICP_REC_USE_LIM_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "REC_USE_LIM,Recommended uses\n"
    "dataflow,OECD:DF_PRICES_HICP(1.0),I,AUS,Australia,"
    "Interpret with caution in Australia,Recommended uses\n"
    "dataflow,OECD:DF_PRICES_HICP(1.0),I,CAN,Canada,"
    "Interpret with caution in Canada,Recommended uses\n"
    "dataflow,OECD:DF_PRICES_HICP(1.0),I,JPN,Japan,"
    "Interpret with caution in Japan,Recommended uses\n"
    "dataflow,OECD:DF_PRICES_HICP(1.0),I,IND,India,"
    "Interpret with caution in India,Recommended uses\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_returns_every_value_with_its_own_context():
    """Four countries, four disagreeing values: the drill-down call is what
    lets a caller actually read them, one per slice, in the order seen."""
    respx.get(url__startswith=_drill_msd_url("DF_PRICES_HICP")).respond(
        200, text=HICP_REC_USE_LIM_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_PRICES_HICP",
        attribute_id="REC_USE_LIM",
    )
    assert result["total"] == 4
    assert result["truncated"] is False
    areas = [v["key_context"]["REF_AREA"] for v in result["values"]]
    assert areas == ["AUS", "CAN", "JPN", "IND"]


DF_SDG_THREE_COUNTRY_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,FJI,Fiji,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,TON,Tonga,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,WSM,Samoa,"
    "UNSD,Source organisation\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_returns_every_context_for_a_value_repeated_on_every_row():
    """The all_observed_rows headline for a value repeated across many
    countries must not cost the drill-down its detail: each row's own
    context stays reachable through get_metadata_attribute, one entry per
    country, not just the first one seen."""
    respx.get(url__startswith=_msd_url()).respond(200, text=DF_SDG_THREE_COUNTRY_MSD_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_ORGANIZATION",
    )
    assert result["total"] == 3
    areas = [v["key_context"]["REF_AREA"] for v in result["values"]]
    assert areas == ["FJI", "TON", "WSM"]
    assert all(v["value"] == "UNSD" for v in result["values"])


BOP_TABLE1_MSD_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "COMPILING_ORG,Compiling organisation,"
    "COVERAGE,Coverage\n"
    "dataflow,IMF:DF_BOP_TABLE1(1.0),I,~,~,"
    "International Monetary Fund,Compiling organisation,"
    ",Coverage\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_on_an_unknown_attribute_lists_the_declared_ids():
    """Same treatment as an unknown availability filter: say what may be used
    rather than returning an empty list that reads as 'no values'."""
    respx.get(url__startswith=_drill_msd_url("DF_BOP_TABLE1")).respond(
        200, text=BOP_TABLE1_MSD_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_BOP_TABLE1",
        attribute_id="NOT_AN_ATTRIBUTE",
    )
    assert "error" in result
    assert "COMPILING_ORG" in result["error"] or any(
        "COMPILING_ORG" in n for n in result["notes"]
    )
    assert result["values"] == []


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_on_a_declared_empty_attribute_says_so():
    """Zero values because the provider left it blank is not the same as an
    attribute that does not exist."""
    respx.get(url__startswith=_drill_msd_url("DF_BOP_TABLE1")).respond(
        200, text=BOP_TABLE1_MSD_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_BOP_TABLE1",
        attribute_id="COVERAGE",
    )
    assert result["total"] == 0
    assert "error" not in result
    assert any("declared" in n.lower() for n in result["notes"])


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_reaches_a_dotted_attribute_by_its_id():
    """SPC nests MSD attributes under a dotted hierarchy
    (DATA_SOURCE.DATA_SOURCE_ORGANIZATION); `id` is the part after the last
    dot, and that is what a caller supplies here (matching what
    get_reference_metadata()'s summary reports back as `id`). Matching on
    `path` instead would make every SPC attribute unreachable through this
    call while every flat-column provider kept working, so this must be
    pinned with a dotted fixture, not the flat ones the other tests use."""
    respx.get(url__startswith=_msd_url()).respond(200, text=MSD_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(), dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_ORGANIZATION",
    )
    assert "error" not in result
    assert result["total"] == 1
    assert result["values"][0]["value"] == "UNSD"


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_caps_values_but_reports_the_true_total(monkeypatch):
    """Rewriting `total` as `len(values)` after slicing would keep this
    green even if values beyond the cap silently vanished, so `total` and
    `len(values)` are asserted separately against the same four-country
    fixture used above, with the cap lowered to 2 rather than requiring a
    204-row fixture to exceed the real cap of 200."""
    monkeypatch.setattr(reference_metadata_module, "_MAX_ATTRIBUTE_VALUES", 2)
    respx.get(url__startswith=_drill_msd_url("DF_PRICES_HICP")).respond(
        200, text=HICP_REC_USE_LIM_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_PRICES_HICP",
        attribute_id="REC_USE_LIM",
    )
    assert result["total"] == 4
    assert len(result["values"]) == 2
    assert result["truncated"] is True


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_does_not_claim_unknown_when_no_channel_confirmed():
    """SPC's DF_SDG unkeyed is the real case this pins: the MSD channel is
    too_broad (it saw megabytes and refused them) and the DSD fallback is
    inconclusive. Neither channel ever produced a declared set, so the
    answer must not be phrased as "declared attributes are" (which would
    read as an empty declared set, i.e. the attribute does not exist) --
    that would misreport a fetch that never resolved as a caller mistake."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "STRUCTURE,A.B,label\n" + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    respx.get(url__startswith=_msd_url()).respond(200, text=huge)
    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_metadata_attribute_values(
        client=FakeClient(), dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_LICENSE",
    )
    assert "error" not in result
    assert result["total"] == 0
    assert result["values"] == []
    joined = " ".join(result["notes"]).lower()
    assert "too large" in joined
    assert "declared attributes are" not in joined


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_does_not_claim_unknown_when_msd_empty_and_dsd_unresolved():
    """C1: the MSD channel can legitimately answer "empty" (zero metadata
    columns in this dataflow's response) while the DSD fallback that always
    runs alongside it fails to resolve (here, inconclusive). Treating the
    whole lookup as "confirmed" from the MSD half alone let an attribute
    lookup fall through to the unknown-attribute branch with an empty
    declared list -- "declared attributes are " with nothing after the
    colon -- misreporting a DSD-side fetch failure as a caller typo."""
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,ACTION,FREQ,Frequency of observation\ndataflow,I,A,Annual\n")
    respx.get(url__startswith="https://example.org/rest/data/").respond(204)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"FREQ"}),
        dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_LICENSE",
    )
    assert "error" not in result
    assert result["total"] == 0
    assert result["values"] == []
    joined = " ".join(result["notes"]).lower()
    assert "declared attributes are" not in joined
    assert "unknown" in joined or "not understood" in joined


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_on_msd_empty_and_dsd_empty_is_unresolved_not_confirmed():
    """C1's second half, revisited: an MSD "empty" plus a DSD "empty" used
    to be read as both channels agreeing on zero declared attributes. But
    the DSD-attribute channel only ever sees what a message actually
    populates (fetch_dsd_attribute_metadata's own docstring says so), so
    its "empty" cannot vouch for the full declared set any more than a DSD
    "found" can -- only the MSD channel's own `found` answer can license
    that claim. This must read as unresolved, the same "we do not know"
    answer a too_broad or unsupported MSD channel already gives."""
    bare = ('<?xml version="1.0"?><mes:StructureSpecificData '
            'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
            'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
            '<mes:DataSet ss:dataScope="DataStructure"/></mes:StructureSpecificData>')
    respx.get(url__startswith=_msd_url()).respond(
        200, text="STRUCTURE,ACTION,FREQ,Frequency of observation\ndataflow,I,A,Annual\n")
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=bare)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"FREQ"}),
        dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_LICENSE",
    )
    assert result["total"] == 0
    assert result["values"] == []
    assert "error" not in result
    joined = " ".join(result["notes"]).lower()
    assert "declared attributes are" not in joined
    assert "could not be established" in joined


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_does_not_claim_confirmed_empty_when_msd_is_too_broad():
    """N1: the MSD channel saw megabytes of data and refused to return it
    unfiltered, which is positive evidence metadata exists, while the DSD
    fallback's own message carried no attributes at all. The DSD channel
    cannot confirm a declared set on its own (fetch_dsd_attribute_metadata
    only ever sees what is populated), so this must not be worded as
    "confirmed empty"; it is the same "we do not know" answer the too_broad
    channel-status note already gives."""
    from tools.reference_metadata import UNKEYED_SIZE_CAP_BYTES

    huge = "STRUCTURE,A.B,label\n" + ("x" * (UNKEYED_SIZE_CAP_BYTES + 1))
    bare = ('<?xml version="1.0"?><mes:StructureSpecificData '
            'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
            'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
            '<mes:DataSet ss:dataScope="DataStructure"/></mes:StructureSpecificData>')
    respx.get(url__startswith=_msd_url()).respond(200, text=huge)
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=bare)
    result = await get_metadata_attribute_values(
        client=FakeClient(), dataflow_id="DF_SDG", attribute_id="DATA_SOURCE_LICENSE",
    )
    assert "error" not in result
    assert result["total"] == 0
    assert result["values"] == []
    joined = " ".join(result["notes"]).lower()
    assert "too large" in joined
    assert "confirmed empty" not in joined


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_does_not_claim_confirmed_empty_when_msd_is_unsupported():
    """N1: a provider without a /v2/ endpoint has no MSD channel to confirm
    anything with. The DSD fallback answering "empty" only means the one
    message it read carried no attributes; by its own documented contract
    it never sees a declared-but-blank attribute, so it cannot confirm the
    declared set on its own either. This must not be worded as "confirmed
    empty"."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    bare = ('<?xml version="1.0"?><mes:StructureSpecificData '
            'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
            'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
            '<mes:DataSet ss:dataScope="DataStructure"/></mes:StructureSpecificData>')
    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=bare)
    result = await get_metadata_attribute_values(
        client=EcbClient(), dataflow_id="EXR", attribute_id="LICENSE", key="all",
    )
    assert "error" not in result
    assert result["total"] == 0
    assert result["values"] == []
    joined = " ".join(result["notes"]).lower()
    assert "confirmed empty" not in joined
    assert "not configured for the v2 metadata endpoint" in joined


DSD_MULTI_VALUE_XML = (
    '<?xml version="1.0"?>'
    '<mes:StructureSpecificData '
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific">'
    '<mes:DataSet ss:dataScope="DataStructure">'
    '<Series FREQ="A" SOURCE_AGENCY="4F0"><Obs TIME_PERIOD="2020" OBS_VALUE="1.5"/></Series>'
    '<Series FREQ="A" SOURCE_AGENCY="5B0"><Obs TIME_PERIOD="2021" OBS_VALUE="1.6"/></Series>'
    '</mes:DataSet></mes:StructureSpecificData>'
)


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_through_the_dsd_channel_notes_the_lost_slice():
    """fetch_dsd_attribute_metadata sets key_context=None on every value by
    construction (it has no per-value key to report), so two disagreeing
    SOURCE_AGENCY values coming back with key_context null would otherwise
    read as two contradictory dataflow-wide statements rather than two
    different series' values. A note must say the slice is not observable
    through this channel."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text=DSD_MULTI_VALUE_XML)
    result = await get_metadata_attribute_values(
        client=EcbClient(), dataflow_id="EXR", attribute_id="SOURCE_AGENCY", key="all",
    )
    assert result["total"] == 2
    assert all(v["key_context"] is None for v in result["values"])
    assert any("DSD-attribute channel" in n for n in result["notes"])


@pytest.mark.asyncio
@respx.mock
async def test_a_dsd_found_channel_does_not_license_an_unknown_attribute_claim():
    """The real bug this pins: ECB has no MSD channel at all, so a data
    message carrying SOURCE_AGENCY but never mentioning LICENSE (blank and
    declared, or simply not attached at this level) must not read as
    "Unknown attribute 'LICENSE': declared attributes are SOURCE_AGENCY" --
    the DSD-attribute channel only ever sees what a message populates, so
    SOURCE_AGENCY being present says nothing about whether LICENSE is
    declared. Only the MSD channel's own `found` answer can license that
    claim, and ECB has none."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(
        200, text=DSD_MULTI_VALUE_XML)
    result = await get_metadata_attribute_values(
        client=EcbClient(), dataflow_id="EXR", attribute_id="LICENSE", key="all",
    )
    assert "error" not in result
    assert result["total"] == 0
    assert result["values"] == []
    joined = " ".join(result["notes"]).lower()
    assert "declared attributes are" not in joined
    assert "not configured for the v2 metadata endpoint" in joined


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_does_not_contradict_the_headline_for_a_dataset_scope_attribute():
    """I2: `_DATAFLOW_WIDE_SCOPES` treats `dataset` as dataflow-wide in the
    summary's headline rule (get_reference_metadata keeps FULL_DESCRIPTION's
    headline for exactly this reason), so the drill-down must not then say
    the same value "attach[es] at dataset level, not to the whole
    dataflow" -- that flatly contradicts the summary a caller may have just
    read for the same attribute."""
    class EcbClient(FakeClient):
        endpoint_key = "ECB"
        agency_id = "ECB"

    respx.get(url__startswith="https://example.org/rest/data/").respond(200, text=DATA_XML)
    result = await get_metadata_attribute_values(
        client=EcbClient(), dataflow_id="EXR", attribute_id="FULL_DESCRIPTION", key="all",
    )
    assert result["total"] == 1
    assert not any("not to the whole dataflow" in n for n in result["notes"])


@pytest.mark.asyncio
@respx.mock
async def test_drill_down_on_a_declared_empty_attribute_says_slice_when_keyed():
    """I3: `declared_empty` means blank across the response actually read.
    With a key supplied, that response is one slice, not the whole
    dataflow -- SPC's DF_SDG can only be queried keyed, so this is the
    normal path for its flagship dataflow -- and the wording must not claim
    the provider published nothing for the dataflow as a whole."""
    respx.get(url__startswith=_drill_msd_url("DF_BOP_TABLE1", key="A.FJI")).respond(
        200, text=BOP_TABLE1_MSD_CSV)
    result = await get_metadata_attribute_values(
        client=DimAwareClient(dimension_ids={"REF_AREA"}),
        dataflow_id="DF_BOP_TABLE1", attribute_id="COVERAGE", key="A.FJI",
    )
    assert result["total"] == 0
    joined = " ".join(result["notes"])
    assert "slice queried" in joined
    assert "for this dataflow and the provider has published no value" not in joined


@pytest.mark.asyncio
async def test_the_attribute_wrapper_marks_an_unknown_attribute_as_an_error():
    """MetadataAttributeValuesResult has no `error` field, so the "Error: "
    prefix main_server.py inserts into `notes` is the only thing at the tool
    boundary telling this apart from the declared-empty answer below --
    without this test, deleting that insertion would leave the two
    indistinguishable and nothing would fail."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def unknown_impl(client, dataflow_id, attribute_id, key=None, agency_id=None, ctx=None):
        return {
            "dataflow_id": dataflow_id,
            "attribute_id": attribute_id,
            "label": None,
            "value_kind": "unknown",
            "values": [],
            "total": 0,
            "truncated": False,
            "notes": [],
            "error": (
                "Unknown attribute 'NOT_AN_ATTRIBUTE' for DF_BOP_TABLE1: "
                "declared attributes are COMPILING_ORG, COVERAGE"
            ),
        }

    with patch(
        "tools.reference_metadata.get_metadata_attribute_values", side_effect=unknown_impl
    ):
        from main_server import get_metadata_attribute as handler

        result = await handler(
            dataflow_id="DF_BOP_TABLE1", attribute_id="NOT_AN_ATTRIBUTE",
            endpoint="SPC", ctx=ctx,
        )

    assert result.total == 0
    assert result.values == []
    assert result.notes[0].startswith("Error: Unknown attribute")


@pytest.mark.asyncio
async def test_the_attribute_wrapper_does_not_mark_a_declared_empty_attribute_as_an_error():
    """The counterpart to the test above: a declared-but-empty attribute
    must reach the caller without the "Error: " prefix, or the two answers
    this whole task exists to keep apart would look identical again at the
    tool boundary."""
    from unittest.mock import patch

    from app_context import AppContext
    from session_manager import SessionManager

    mgr = SessionManager(default_endpoint_key="SPC")
    app_ctx = AppContext(session_manager=mgr)
    ctx = _FakeCtx(app_ctx)

    async def declared_empty_impl(
        client, dataflow_id, attribute_id, key=None, agency_id=None, ctx=None
    ):
        return {
            "dataflow_id": dataflow_id,
            "attribute_id": attribute_id,
            "label": "Coverage",
            "value_kind": "unknown",
            "values": [],
            "total": 0,
            "truncated": False,
            "notes": [
                "'COVERAGE' is declared for this dataflow and the provider "
                "has published no value for it."
            ],
        }

    with patch(
        "tools.reference_metadata.get_metadata_attribute_values",
        side_effect=declared_empty_impl,
    ):
        from main_server import get_metadata_attribute as handler

        result = await handler(
            dataflow_id="DF_BOP_TABLE1", attribute_id="COVERAGE", endpoint="SPC", ctx=ctx,
        )

    assert result.total == 0
    assert result.values == []
    assert not result.notes[0].startswith("Error:")
    assert any("declared" in n.lower() for n in result.notes)
