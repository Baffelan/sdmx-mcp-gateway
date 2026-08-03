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
    no_v2 = {"ECB", "UNICEF", "IMF", "ESTAT", "BIS", "STATSNZ"}

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
    assert classify_value_kind("I15", has_codelist=True) == "code"
    assert classify_value_kind("Data are compiled by UNSD.") == "prose"
    assert classify_value_kind(None) == "unknown"
    assert classify_value_kind("") == "unknown"
