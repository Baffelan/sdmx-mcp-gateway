"""Reference metadata values arrive multilingual with embedded HTML, e.g.
`en:"<p>text</p>",fr:""`. Returning that raw to an agent would be useless."""

import pytest

import tools.reference_metadata as reference_metadata_module
from config import get_metadata_support
from tools.reference_metadata import parse_localised_value, parse_msd_csv, strip_markup

pytestmark = pytest.mark.unit

REAL = ('en:"<p>United Nations Statistics Division (UNSD) in collaboration with '
        'UN custodian agencies for each SDG indicator.</p>",fr:""')


def test_parses_a_real_spc_value():
    text, lang = parse_localised_value(REAL)
    assert lang == "en"
    assert text.startswith("United Nations Statistics Division")
    assert "<p>" not in text
    assert text.endswith("indicator.")


def test_empty_translations_are_ignored():
    text, lang = parse_localised_value('en:"",fr:"<p>Bonjour</p>"')
    assert lang == "fr"
    assert text == "Bonjour"


def test_preferred_language_wins_when_several_are_present():
    text, lang = parse_localised_value('fr:"<p>Bonjour</p>",en:"<p>Hello</p>"')
    assert (text, lang) == ("Hello", "en")


def test_preference_can_be_overridden():
    text, lang = parse_localised_value('fr:"<p>Bonjour</p>",en:"<p>Hello</p>"', prefer="fr")
    assert (text, lang) == ("Bonjour", "fr")


def test_value_without_a_language_prefix_is_returned_as_is():
    text, lang = parse_localised_value("<p>Plain value</p>")
    assert text == "Plain value"
    assert lang is None


def test_all_empty_yields_nothing():
    assert parse_localised_value('en:"",fr:""') == (None, None)
    assert parse_localised_value("") == (None, None)


def test_links_keep_their_text_and_href():
    raw = 'en:"<p><a href=\\"https://unstats.un.org/sdgs\\">SDG portal</a></p>"'
    text, _lang = parse_localised_value(raw)
    assert "SDG portal" in text
    assert "https://unstats.un.org/sdgs" in text


def test_strip_markup_collapses_whitespace_and_entities():
    assert strip_markup("<p>a&nbsp;b</p>\n<p>c</p>") == "a b c"


def test_strip_markup_keeps_the_y_x_form_when_text_and_href_differ():
    """The URL is the useful part when the link text is genuine prose, so
    both must survive, in the "text (url)" order."""
    raw = '<a href="https://unstats.un.org/sdgs">SDG portal</a>'
    assert strip_markup(raw) == "SDG portal (https://unstats.un.org/sdgs)"


def test_strip_markup_collapses_a_self_referential_link_to_one_occurrence():
    """SPC's DATA_SOURCE_LINK anchor text is literally the href. The naive
    "text (url)" substitution then prints the same URL twice:
    'https://...SI_POV_DAY1 (https://...SI_POV_DAY1)'. There is nothing the
    second copy adds, so it must collapse to a single occurrence."""
    url = "https://unstats.un.org/sdgs/dataportal/SDMXMetadataPage?1.1.1-SI_POV_DAY1"
    raw = '<a href="' + url + '">' + url + "</a>"
    assert strip_markup(raw) == url


def test_strip_markup_collapses_self_referential_link_despite_surrounding_whitespace():
    """The comparison must ignore surrounding whitespace in the anchor text,
    since that whitespace is markup formatting, not part of the answer."""
    url = "https://example.org/x"
    raw = '<a href="' + url + '">\n  ' + url + "  \n</a>"
    assert strip_markup(raw) == url


def test_providers_verified_serving_msd_metadata():
    for key in ("SPC", "OECD", "FBOS", "SBS"):
        support = get_metadata_support(key)
        assert support is not None, key
        assert support["status"] == "supported", key
        assert support["v2_path"] == "/v2", key


def test_providers_that_route_v2_but_fail_are_marked_unsupported():
    """ABS 404s and ILO 500s on the metadata query (verified 2026-07-29)."""
    for key in ("ABS", "ILO"):
        support = get_metadata_support(key)
        assert support is not None, key
        assert support["status"] == "unsupported", key
        assert support["reason"], key


def test_providers_without_a_v2_endpoint_return_none():
    for key in ("ECB", "UNICEF", "BIS", "ESTAT"):
        assert get_metadata_support(key) is None, key


def test_unknown_endpoint_returns_none():
    assert get_metadata_support(None) is None
    assert get_metadata_support("NOT_A_PROVIDER") is None


def test_every_endpoint_is_classified_exactly_once():
    """Assert the whole partition, so a newly added provider cannot quietly
    default into 'no metadata support' without someone deciding that."""
    from config import SDMX_ENDPOINTS

    supported = {"SPC", "OECD", "FBOS", "SBS"}
    routed_but_failing = {"ABS", "ILO"}
    no_v2 = {"ECB", "UNICEF", "IMF", "ESTAT", "BIS", "STATSNZ","TNSO"}

    assert supported | routed_but_failing | no_v2 == set(SDMX_ENDPOINTS)
    assert not (supported & routed_but_failing)
    assert not (supported & no_v2)
    assert not (routed_but_failing & no_v2)

    for key in supported:
        assert get_metadata_support(key) == {"v2_path": "/v2", "status": "supported"}, key
    for key in routed_but_failing:
        assert get_metadata_support(key)["status"] == "unsupported", key
    for key in no_v2:
        assert get_metadata_support(key) is None, key


def test_unsupported_reasons_name_the_observed_failure():
    assert "404" in get_metadata_support("ABS")["reason"]
    assert "500" in get_metadata_support("ILO")["reason"]


FBOS_CSV = (
    "STRUCTURE,STRUCTURE_ID,STRUCTURE_NAME,ACTION,"
    "FREQ,Frequency of observation,REF_AREA,Reference area,"
    "COMPILING_ORG,Compiling agency,UNIT,Note on unit,COVERAGE,Note on coverage\n"
    "DATAFLOW,FBOS:DF_BOP_TABLE1(1.0),Balance of Payments,I,"
    "~,~,~,~,Fiji Bureau of Statistics,,,,,\n"
)


def test_declared_but_empty_columns_are_reported():
    """FBOS declares three metadata attributes and populates one. Dropping
    the two empty ones makes a blank licence or coverage field
    indistinguishable from a provider that has no such concept."""
    out = parse_msd_csv(FBOS_CSV, dimension_ids={"FREQ", "REF_AREA"})
    by_id = {a["id"]: a for a in out}

    assert by_id["COMPILING_ORG"]["status"] == "populated"
    assert by_id["COMPILING_ORG"]["value"] == "Fiji Bureau of Statistics"
    assert by_id["COMPILING_ORG"]["scope"] == "dataflow"

    for empty in ("UNIT", "COVERAGE"):
        assert by_id[empty]["status"] == "declared_empty"
        assert by_id[empty]["value"] is None
        assert by_id[empty]["distinct_value_count"] == 0
        assert by_id[empty]["scope"] is None
        # The label still tells the caller what the provider left blank.
        assert by_id[empty]["label"]


def test_parse_msd_csv_signals_truncation_when_the_row_cap_is_hit(monkeypatch):
    """I1: at the row cap the parser used to stop reading and only log it,
    so a column populated only beyond that point read as a plain
    declared_empty -- indistinguishable from a column the provider never
    fills. The cap is lowered here so a 3-row fixture can exceed it instead
    of needing a 5000-row one."""
    monkeypatch.setattr(reference_metadata_module, "_MAX_MSD_DATA_ROWS", 2)
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
        "COVERAGE,Coverage\n"
        "dataflow,OECD:DF(1.0),I,AUS,Australia,,\n"
        "dataflow,OECD:DF(1.0),I,GBR,United Kingdom,,\n"
        'dataflow,OECD:DF(1.0),I,USA,United States,'
        '"en:""<p>Whole thing</p>""","Coverage"\n'
    )
    out = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
    coverage = next(a for a in out if a["id"] == "COVERAGE")
    # The only value is on the row beyond the (lowered) cap, so it still
    # reads as declared_empty -- but the parser must say the read stopped
    # early, so a caller can tell this apart from a genuinely blank column.
    assert coverage["status"] == "declared_empty"
    assert out.truncated is True


def test_parse_msd_csv_does_not_report_truncated_when_every_row_is_read():
    out = parse_msd_csv(FBOS_CSV, dimension_ids={"FREQ", "REF_AREA"})
    assert out.truncated is False


def test_all_values_are_kept_uncapped_for_drill_down():
    """The summary caps what it shows; the parser must not lose the rest."""
    csv_text = (
        "STRUCTURE,STRUCTURE_ID,STRUCTURE_NAME,ACTION,"
        "REF_AREA,Reference area,REC_USE_LIM,Recommended uses\n"
        "DATAFLOW,X:Y(1.0),Y,I,AUS,Australia,limits for AUS,\n"
        "DATAFLOW,X:Y(1.0),Y,I,CAN,Canada,limits for CAN,\n"
        "DATAFLOW,X:Y(1.0),Y,I,JPN,Japan,limits for JPN,\n"
        "DATAFLOW,X:Y(1.0),Y,I,IND,India,limits for IND,\n"
    )
    out = parse_msd_csv(csv_text, dimension_ids={"REF_AREA"})
    attr = next(a for a in out if a["id"] == "REC_USE_LIM")

    assert attr["distinct_value_count"] == 4
    assert len(attr["all_values"]) == 4
    contexts = [v["key_context"]["REF_AREA"] for v in attr["all_values"]]
    assert contexts == ["AUS", "CAN", "JPN", "IND"]
    assert all(v["scope"] == "partial_key" for v in attr["all_values"])


DF_SDG_LIKE_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,FJI,Fiji,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,TON,Tonga,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,VUT,Vanuatu,"
    "UNSD,Source organisation\n"
)


def test_a_value_on_every_row_is_scoped_all_observed_rows():
    """SPC's DF_SDG publishes the same value on every per-country row and
    has no dataflow-wide row at all, so every row is partial_key. The value
    still describes everything this query returned, even though no row
    ever appeared unqualified."""
    out = parse_msd_csv(DF_SDG_LIKE_CSV, dimension_ids={"REF_AREA"})
    org = next(a for a in out if a["id"] == "DATA_SOURCE_ORGANIZATION")
    assert org["scope"] == "all_observed_rows"
    assert org["value"] == "UNSD"
    assert org["distinct_value_count"] == 1
    assert org["key_context"] is None
    # The headline is a weaker claim than "dataflow", so the per-row detail
    # behind it must still be reachable -- one entry per country, not just
    # the first one seen.
    assert len(org["all_values"]) == 3


FJI_TON_WSM_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "DATA_SOURCE.DATA_SOURCE_ORGANIZATION,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,FJI,Fiji,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,TON,Tonga,"
    "UNSD,Source organisation\n"
    "dataflow,SPC:DF_SDG(4.3),I,WSM,Samoa,"
    "UNSD,Source organisation\n"
)


def test_repeated_values_keep_every_row_context_instead_of_only_the_first():
    """Deduping on the value text alone (the pre-fix behaviour) kept only
    the FJI row's context and silently dropped TON and WSM. That is worse
    than data loss: the surviving entry then misattributes a Pacific-wide
    value to Fiji alone. Each row's own context must survive as its own
    entry in all_values, keyed on the (value, key_context) pair rather than
    the value alone."""
    out = parse_msd_csv(FJI_TON_WSM_CSV, dimension_ids={"REF_AREA"})
    org = next(a for a in out if a["id"] == "DATA_SOURCE_ORGANIZATION")
    # distinct_value_count still counts distinct values, not pairs: one
    # value ("UNSD") drives the headline rule regardless of how many
    # countries published it.
    assert org["distinct_value_count"] == 1
    assert org["scope"] == "all_observed_rows"
    assert org["key_context"] is None
    assert len(org["all_values"]) == 3
    contexts = [v["key_context"] for v in org["all_values"]]
    assert contexts == [
        {"REF_AREA": "FJI"}, {"REF_AREA": "TON"}, {"REF_AREA": "WSM"},
    ]
    assert all(v["value"] == "UNSD" for v in org["all_values"])


PARTIAL_COVERAGE_CSV = (
    "STRUCTURE,STRUCTURE_ID,ACTION,REF_AREA,Reference area,"
    "COVERAGE,Coverage\n"
    "dataflow,OECD:DF(1.0),I,AUS,Australia,"
    "Whole country,Coverage\n"
    "dataflow,OECD:DF(1.0),I,GBR,United Kingdom,,\n"
    "dataflow,OECD:DF(1.0),I,USA,United States,"
    "Whole country,Coverage\n"
)


def test_a_value_on_only_some_rows_keeps_partial_key_scope():
    """Two of three rows carry the identical text; the third leaves the
    column blank. Only one distinct value was ever seen, but it did not
    appear on every row this query read, so it must not be promoted to
    all_observed_rows: that would claim more than was actually true."""
    out = parse_msd_csv(PARTIAL_COVERAGE_CSV, dimension_ids={"REF_AREA"})
    coverage = next(a for a in out if a["id"] == "COVERAGE")
    assert coverage["distinct_value_count"] == 1
    assert coverage["scope"] == "partial_key"
    assert coverage["key_context"] is not None


def test_truncation_blocks_the_all_observed_rows_scope(monkeypatch):
    """Truncation makes "every row" unknowable: a value identical on every
    row actually read might still disagree with a row past the cap, so a
    truncated response must not claim it covers all observed rows."""
    monkeypatch.setattr(reference_metadata_module, "_MAX_MSD_DATA_ROWS", 2)
    out = parse_msd_csv(DF_SDG_LIKE_CSV, dimension_ids={"REF_AREA"})
    org = next(a for a in out if a["id"] == "DATA_SOURCE_ORGANIZATION")
    assert out.truncated is True
    assert org["scope"] == "partial_key"
    assert org["value"] == "UNSD"


def test_value_kind_is_unknown_rather_than_guessed():
    """Defaulting to prose would imply the value was examined and found to be
    text. Where nothing can be established, say so."""
    from tools.reference_metadata import classify_value_kind

    assert classify_value_kind("https://unstats.un.org/sdgs") == "url"
    assert classify_value_kind("2026-05-26") == "date"
    assert classify_value_kind("2026-05-26T06:11:07Z") == "date"
    # A coded value such as "I15" is labelled prose, not "code": neither
    # metadata channel carries the attribute's codelist reference, so there
    # is no way to tell a code apart from ordinary short text.
    assert classify_value_kind("I15") == "prose"
    assert classify_value_kind("Data are compiled by UNSD.") == "prose"
    assert classify_value_kind(None) == "unknown"
    assert classify_value_kind("") == "unknown"


def test_classify_value_kind_no_longer_accepts_has_codelist():
    """has_codelist advertised a "code" kind that neither production call
    site could ever trigger (both hard-code has_codelist=False, since
    neither channel carries a codelist reference), so a caller branching on
    "code" was writing dead code. Removed from the contract entirely rather
    than left as an unreachable parameter."""
    import inspect

    from tools.reference_metadata import classify_value_kind

    assert "has_codelist" not in inspect.signature(classify_value_kind).parameters
    with pytest.raises(TypeError):
        classify_value_kind("I15", has_codelist=True)


def test_localised_value_with_space_after_colon():
    """Some dataflows emit a space between the language tag and the opening
    quote: en: "value" instead of en:"value". The parser must handle both."""
    text, lang = parse_localised_value('en: "https://www.statsfiji.gov.fj/index.php/census-2017"')
    assert lang == "en"
    assert text == "https://www.statsfiji.gov.fj/index.php/census-2017"


def test_localised_value_without_space_still_works():
    """Regression test: the original no-space form must still work after
    adding support for spaces."""
    text, lang = parse_localised_value('en:"https://example.org"')
    assert lang == "en"
    assert text == "https://example.org"


def test_localised_value_with_tab_after_colon():
    """A tab is also horizontal whitespace and must be handled like a space."""
    text, lang = parse_localised_value('en:\t"value"')
    assert lang == "en"
    assert text == "value"


def test_localised_value_multiple_languages_with_spaces():
    """Two languages where both have spaces after the colon; English preference
    must still win."""
    text, lang = parse_localised_value('en: "Hello",fr: "Bonjour"')
    assert lang == "en"
    assert text == "Hello"


def test_localised_value_does_not_cross_newlines():
    """A newline must NOT allow the regex to match across lines. If en: sits
    at the end of one line and "unrelated" starts on the next line, they must
    not be paired together. This test pins the choice of [ \\t]* over \\s*.

    With [ \\t]*, the pattern does not match (no newline in the whitespace
    set), so it falls through to strip_markup which collapses the newline to
    a space. If someone later changes [ \\t]* to \\s*, the regex WOULD match
    and incorrectly parse this as language='en', value='unrelated'. This test
    documents that is wrong and must never happen."""
    text, lang = parse_localised_value('en:\n"unrelated"')
    # The regex does not match because [ \t]* does not match \n, so this
    # falls through to strip_markup(raw), which collapses whitespace and
    # returns it with language=None (not as a matched localised value).
    # The critical assertion: language is None, NOT 'en'.
    assert lang is None
    # And the value is NOT the string from inside the quotes.
    assert text != "unrelated"


def test_real_spc_hhcounts_data_source_link():
    """Regression test using the actual reported value from SPC DF_HHCOUNTS
    attribute DATA_SOURCE_LINK with space after colon."""
    raw = 'en: "https://www.statsfiji.gov.fj/index.php/census-2017"'
    text, lang = parse_localised_value(raw)
    assert lang == "en"
    assert text == "https://www.statsfiji.gov.fj/index.php/census-2017"
