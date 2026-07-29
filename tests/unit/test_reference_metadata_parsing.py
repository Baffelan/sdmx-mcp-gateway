"""Reference metadata values arrive multilingual with embedded HTML, e.g.
`en:"<p>text</p>",fr:""`. Returning that raw to an agent would be useless."""

import pytest

from tools.reference_metadata import parse_localised_value, strip_markup

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
