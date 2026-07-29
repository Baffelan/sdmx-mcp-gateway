"""Reference metadata retrieval.

Reference metadata is the descriptive material about a dataflow: who compiled
it, from what source, under what licence, with what caveats. It is what turns
a retrieved number into something citable.

There is no single place it lives. .Stat Suite deployments serve it through an
SDMx 3.0 data query asking for MSD attributes (`attributes=msd`), while other
providers put descriptive text in ordinary DSD attributes on the data message.
This module reads both and reports which channels a provider left empty.
"""

import html
import logging
import re

logger = logging.getLogger(__name__)

# `en:"<p>text</p>",fr:""` — a language tag, a colon, then a quoted value.
_LOCALISED = re.compile(r'([A-Za-z]{2,3}(?:-[A-Za-z]{2,4})?):"((?:[^"\\]|\\.)*)"')
_TAG = re.compile(r"<[^>]+>")
_HREF = re.compile(r'<a\s[^>]*href=\\?"([^"\\]+)\\?"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)


def strip_markup(raw: str) -> str:
    """Plain text from the HTML fragments providers embed in metadata values."""
    if not raw:
        return ""
    # Keep a link's text and its target, since the URL is often the useful part.
    text = _HREF.sub(lambda m: m.group(2) + " (" + m.group(1) + ")", raw)
    text = _TAG.sub(" ", text)
    text = html.unescape(text).replace("\xa0", " ")
    return " ".join(text.split())


def parse_localised_value(raw: str, prefer: str = "en") -> tuple[str | None, str | None]:
    """Pull one readable string out of a multilingual metadata value.

    Returns (text, language). Language is None when the value carried no
    language tags at all. Empty translations are discarded rather than
    returned as blank strings, because a provider filling only `en` should
    not look like it filled `fr` too.
    """
    if not raw or not raw.strip():
        return None, None

    matches = _LOCALISED.findall(raw)
    if not matches:
        stripped = strip_markup(raw)
        return (stripped or None), None

    by_lang: dict[str, str] = {}
    for lang, value in matches:
        text = strip_markup(value.replace('\\"', '"'))
        if text:
            by_lang.setdefault(lang.lower(), text)

    if not by_lang:
        return None, None
    if prefer.lower() in by_lang:
        return by_lang[prefer.lower()], prefer.lower()
    first = next(iter(by_lang))
    return by_lang[first], first
