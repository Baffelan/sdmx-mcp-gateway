"""Reference metadata retrieval.

Reference metadata is the descriptive material about a dataflow: who compiled
it, from what source, under what licence, with what caveats. It is what turns
a retrieved number into something citable.

There is no single place it lives. .Stat Suite deployments serve it through an
SDMx 3.0 data query asking for MSD attributes (`attributes=msd`), while other
providers put descriptive text in ordinary DSD attributes on the data message.
This module reads both and reports which channels a provider left empty.
"""

import csv
import html
import io
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

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


MSD_QUERY = "attributes=msd&measures=none&format=csvfilewithlabels"

# Above this, an unkeyed response is refused and the caller is asked for a key.
UNKEYED_SIZE_CAP_BYTES = 2_000_000

# A header column that looks like an SDMx identifier: all caps, digits,
# underscores and dots, no spaces or lowercase. This is what separates the id
# columns from the human-readable label columns that `format=csvfilewithlabels`
# interleaves (e.g. "Note on coverage", "Frequency of observation").
_SDMX_IDENTIFIER = re.compile(r"[A-Z0-9_.]+")

# Columns that are structural plumbing in every SDMx-CSV response -- never
# reference metadata, regardless of which provider or dataflow produced them.
_STRUCTURAL_COLUMNS = frozenset({
    "STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION",
    "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS",
})


def parse_msd_csv(
    text: str,
    dimension_ids: set[str] | frozenset[str] = frozenset(),
    prefer: str = "en",
) -> list[dict[str, Any]]:
    """Pull metadata attributes out of a csvfilewithlabels response.

    Metadata columns are told apart from dimension and structural columns by
    shape, not by provider convention. SPC nests its MSD attributes under a
    dotted hierarchy (`DATA_SOURCE.DATA_SOURCE_ORGANIZATION`), but OECD and
    FBOS publish flat attribute names (`QUALITY_ASSMNT`, `COVERAGE`) that look
    exactly like a dimension column such as `REF_AREA`. A column counts as
    metadata when its header looks like an SDMx identifier rather than a
    human label (see `_SDMX_IDENTIFIER`), is not one of the structural
    columns every SDMx-CSV response carries, and is not one of the
    dataflow's own dimension ids -- supplied by the caller, since a data
    message alone cannot tell a dimension from an attribute.

    Every metadata column is followed by its human-readable label column,
    which is what `format=csvfilewithlabels` adds. `path` keeps the full
    column name so SPC's hierarchy is preserved; `id` is the part after the
    last dot.

    Only the header and the first data row are read, and lazily: a keyed
    request is exempt from the size cap in `fetch_msd_metadata`, so a
    provider could in principle still return a very large body, and any rows
    beyond the first carry nothing this function uses. Materialising the
    whole body into `csv.reader` would both waste the parse and risk
    Python's default 131072-byte csv field-size limit on an oversized row
    this function was never going to read anyway.
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
        first = next(reader)
    except StopIteration:
        return []
    out: list[dict[str, Any]] = []
    for index, column in enumerate(header):
        if not _SDMX_IDENTIFIER.fullmatch(column):
            continue
        if column in _STRUCTURAL_COLUMNS:
            continue
        attr_id = column.rsplit(".", 1)[-1]
        if attr_id in dimension_ids:
            continue
        raw = first[index] if index < len(first) else ""
        value, language = parse_localised_value(raw, prefer=prefer)
        if value is None:
            continue
        label = header[index + 1] if index + 1 < len(header) else None
        out.append({
            "id": attr_id,
            "path": column,
            "label": label,
            "value": value,
            "language": language,
        })
    return out


def _looks_like_sdmx_csv(text: str) -> bool:
    """Cheap discriminator between a genuine SDMx-CSV body and an HTML error
    page or JSON fault served with HTTP 200. SDMx-CSV bodies from this
    endpoint begin with a `STRUCTURE` column; check only the first line
    rather than re-parsing the whole body.
    """
    first_line = text.split("\n", 1)[0]
    first_column = first_line.split(",", 1)[0].strip()
    return first_column == "STRUCTURE"


# Distinguishes "no version supplied, resolve it yourself" (the default,
# used by direct/unit-test callers of fetch_msd_metadata) from "a caller
# already resolved this and got None back" (used by get_reference_metadata,
# which resolves the version once for the whole result and must not repeat
# a resolution that already failed). Not `None` itself, since `None` is the
# second, meaningful case.
_UNRESOLVED = object()


async def fetch_msd_metadata(
    client: Any,
    dataflow_id: str,
    agency_id: str,
    key: str | None,
    ctx: Any | None = None,
    version: Any = _UNRESOLVED,
    dimension_ids: set[str] | frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], str]:
    """Read reference metadata through the .Stat Suite v2 MSD query.

    Returns (attributes, status) where status is one of `found`, `empty`,
    `inconclusive`, `too_broad` or `unsupported`. A 204 is deliberately
    `inconclusive` rather than `empty`: several malformed-request shapes
    answer 204, so it cannot be distinguished from genuine absence without
    more information.

    `version`, if supplied, is used as-is instead of resolving "latest"
    again -- pass a resolved version string to skip the lookup, or `None`
    explicitly to mean "a caller already tried to resolve this and failed,"
    which is reported as inconclusive without a second attempt. Leaving the
    default lets this function resolve the version itself, as it always did.

    `dimension_ids`, if supplied, is passed straight through to
    `parse_msd_csv` so the dataflow's own dimension columns (e.g. `FREQ`,
    `REF_AREA`) are not mistaken for metadata attributes.
    """
    from config import get_metadata_support

    support = get_metadata_support(getattr(client, "endpoint_key", None))
    if not support or support.get("status") != "supported":
        return [], "unsupported"

    # `latest` is a 2.1 keyword the v2 endpoint rejects with HTTP 400, and the
    # wildcards misbehave here, so resolve the published version and use it.
    # resolve_version can itself hit the network (to look up "latest") and
    # raises ValueError on a transport or parse failure, so it gets the same
    # inconclusive treatment as the metadata request below.
    if version is _UNRESOLVED:
        try:
            version = await client.resolve_version(
                dataflow_id=dataflow_id, agency_id=agency_id, ctx=ctx
            )
        except Exception as exc:
            logger.info("could not resolve version for %s: %s", dataflow_id, exc)
            return [], "inconclusive"
    elif version is None:
        # Already tried elsewhere and failed; retrying the identical call
        # here would just fail again.
        return [], "inconclusive"
    # The v2 endpoint has no "all" wildcard keyword: SPC answers 204 (which
    # reads as absence, the exact trap the 204 handling below exists to
    # avoid) and OECD/FBOS hard-fail with 422 "Not enough key values in
    # query". The unkeyed form is an empty key segment -- the path just ends
    # with a trailing "/" before the query string.
    key_segment = "/" + key if key else "/"
    url = (client.base_url + support.get("v2_path", "/v2") + "/data/dataflow/" + agency_id
           + "/" + dataflow_id + "/" + version + key_segment
           + "?" + MSD_QUERY)

    try:
        session = await client._get_session()
        response = await session.get(url, headers={"Accept-Language": "en"})
    except Exception as exc:
        logger.info("reference metadata request failed for %s: %s", dataflow_id, exc)
        return [], "inconclusive"

    if response.status_code == 204:
        return [], "inconclusive"
    if response.status_code != 200:
        logger.info(
            "reference metadata query returned HTTP %s for %s",
            response.status_code, dataflow_id,
        )
        return [], "inconclusive"

    # An unkeyed query against a large dataflow is the one way this tool can
    # pull megabytes: SPC's DF_SDG is 5.37 MB unfiltered and 5.6 KB with a
    # partial key. Refuse rather than punish the provider, and tell the caller
    # what to do about it.
    if len(response.content) > UNKEYED_SIZE_CAP_BYTES and not key:
        logger.info(
            "reference metadata response for %s was %s bytes; asking for a key",
            dataflow_id, len(response.content),
        )
        return [], "too_broad"

    # A single oversized field (not just the trailing padding the size guard
    # above catches) can still exceed Python's csv field-size limit, e.g. a
    # provider that concatenates many language translations into one value.
    # That is a malformed/unparseable response, not proof of absence.
    try:
        attributes = parse_msd_csv(response.text, dimension_ids=dimension_ids)
    except Exception as exc:
        logger.info(
            "could not parse reference metadata response for %s: %s", dataflow_id, exc
        )
        return [], "inconclusive"
    if attributes:
        return attributes, "found"
    # A 200 carrying an HTML error page or JSON fault parses as "no dot
    # columns", which is indistinguishable from genuine emptiness unless we
    # check the body really is the SDMx-CSV we asked for. This is the same
    # mistake the 204 handling above exists to prevent, one level down.
    if not _looks_like_sdmx_csv(response.text):
        logger.info("reference metadata body for %s was not SDMx-CSV", dataflow_id)
        return [], "inconclusive"
    return [], "empty"


STRUCTURE_SPECIFIC_DATA = "application/vnd.sdmx.structurespecificdata+xml;version=2.1"

# The measure and the SDMx envelope attributes that every message carries are
# never reference metadata. Dimensions are not listed here: a data message
# cannot tell a dimension from an attribute on its own (both are just XML
# attributes on Series/Obs elements), so the caller must supply the
# dataflow's dimension ids via `dimension_ids` for those to be excluded.
_NOT_METADATA = frozenset({"TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "action"})
_LEVEL_BY_TAG = {"DataSet": "dataset", "Series": "series", "Obs": "observation"}


async def fetch_dsd_attribute_metadata(
    client: Any,
    dataflow_id: str,
    agency_id: str,
    key: str,
    ctx: Any | None = None,
    dimension_ids: set[str] | frozenset[str] = frozenset(),
) -> tuple[list[dict[str, Any]], str]:
    """Read descriptive material carried as ordinary DSD attributes.

    Providers without a `/v2/` endpoint still publish useful metadata this
    way, at whichever level they chose: IMF at dataset level, ECB at series
    level, ILO at observation level. One tiny keyed slice captures all three.

    `dimension_ids`, if supplied, excludes columns that are actually
    dimensions of this dataflow (e.g. `FREQ`, `REF_AREA`) rather than
    reference metadata -- a data message alone cannot make that distinction.
    """
    url = (client.base_url + "/data/" + agency_id + "," + dataflow_id + "/"
           + (key or "all") + "?firstNObservations=1")
    try:
        session = await client._get_session()
        response = await session.get(
            url, headers={"Accept": STRUCTURE_SPECIFIC_DATA, "Accept-Language": "en"}
        )
    except Exception as exc:
        logger.info("attribute metadata request failed for %s: %s", dataflow_id, exc)
        return [], "inconclusive"

    if response.status_code != 200:
        return [], "inconclusive"

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError:
        return [], "inconclusive"

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    saw_data_message = False
    for element in root.iter():
        level = _LEVEL_BY_TAG.get(element.tag.rsplit("}", 1)[-1])
        if level is None:
            continue
        saw_data_message = True
        for name, raw in element.attrib.items():
            # Namespaced attributes are envelope plumbing, not metadata.
            if "}" in name or name in _NOT_METADATA or name in seen or name in dimension_ids:
                continue
            value, language = parse_localised_value(raw)
            if value is None:
                continue
            seen.add(name)
            out.append({
                "id": name,
                "path": name,
                "label": None,
                "value": value,
                "language": language,
                "level": level,
            })
    if out:
        return out, "found"
    # A well-formed XML document that never mentions DataSet/Series/Obs is not
    # a structure-specific data message at all -- an HTML error page or a
    # wrong-schema body parses fine here, since ET.fromstring only rejects
    # syntactically invalid XML. That is not evidence the provider carries no
    # metadata, so it must not be reported as `empty`.
    if not saw_data_message:
        logger.info("attribute metadata body for %s was not a data message", dataflow_id)
        return [], "inconclusive"
    return [], "empty"


async def get_reference_metadata(
    client: Any,
    dataflow_id: str,
    key: str | None = None,
    agency_id: str | None = None,
    ctx: Any | None = None,
) -> dict[str, Any]:
    """Assemble whatever reference metadata this provider actually publishes.

    Reads the MSD channel where the provider supports it, falls back to
    descriptive DSD attributes otherwise, and reports the state of every
    channel so a caller can tell "this provider publishes nothing" from
    "we could not find out".
    """
    agency = agency_id or client.agency_id
    channels: dict[str, str] = {}
    notes: list[str] = []
    attributes: list[dict[str, Any]] = []

    # Both metadata channels below read a data message (CSV or XML) that
    # cannot, on its own, tell a dimension apart from a genuine attribute --
    # they are just columns/XML-attributes either way. The dataflow's
    # dimension ids are fetched once here, from the DSD, and used to filter
    # both channels. This is best-effort: a provider hiccup here must not
    # fail the whole tool, it just means dimension columns may leak into the
    # result as if they were metadata.
    try:
        structure = await client.get_structure_summary(
            dataflow_id=dataflow_id, agency_id=agency
        )
        dimension_ids = {dim.id for dim in structure.dimensions}
    except Exception as exc:
        logger.info("could not resolve dimensions for %s: %s", dataflow_id, exc)
        dimension_ids = set()

    # Resolved once, here, and reused both for the MSD lookup below and for
    # the `version` field in the result. fetch_msd_metadata resolves
    # "latest" itself when nothing is passed in (e.g. called directly from a
    # unit test), but a second, independent resolve_version call from this
    # function would repeat the same network round trip on every call -- and
    # on a provider where resolution fails, repeat the same failure. This
    # tolerates that failure (falls back to None) rather than letting it
    # propagate out of the tool.
    try:
        version = await client.resolve_version(
            dataflow_id=dataflow_id, agency_id=agency, ctx=ctx
        )
    except Exception as exc:
        logger.info("could not resolve version for %s: %s", dataflow_id, exc)
        version = None

    # Unlike the DSD-attribute fallback below, the v2 endpoint has no "all"
    # wildcard keyword -- passing key straight through (rather than key or
    # "all") lets fetch_msd_metadata build the correct empty-key-segment URL
    # when no key was supplied.
    msd_attrs, msd_status = await fetch_msd_metadata(
        client, dataflow_id, agency, key, ctx=ctx, version=version, dimension_ids=dimension_ids
    )
    channels["msd_v2"] = msd_status
    for attr in msd_attrs:
        attributes.append({**attr, "level": "dataflow", "source": "msd"})

    if msd_status != "found":
        dsd_attrs, dsd_status = await fetch_dsd_attribute_metadata(
            client, dataflow_id, agency, key or "all", ctx=ctx, dimension_ids=dimension_ids
        )
        channels["dsd_attributes"] = dsd_status
        for attr in dsd_attrs:
            attributes.append({**attr, "source": "dsd_attribute"})
    else:
        channels["dsd_attributes"] = "skipped"

    if msd_status == "unsupported":
        notes.append(
            "This provider is not configured for the v2 metadata endpoint; "
            "any attributes shown come from the data message."
        )
    if msd_status == "inconclusive":
        notes.append(
            "The metadata query did not produce a usable result, which can "
            "mean either that this dataflow has no metadata or that the "
            "request failed or was not understood. Treat it as unknown "
            "rather than as absence."
        )
    if msd_status == "too_broad":
        notes.append(
            "This dataflow's metadata is too large to return unfiltered. "
            "Supply a dimension key to narrow it, for example a single "
            "indicator or reference area."
        )
    if channels.get("dsd_attributes") == "inconclusive":
        notes.append(
            "The DSD-attribute fallback query did not return a usable "
            "response, which can mean either that this dataflow carries no "
            "descriptive attributes or that the request failed. Treat it as "
            "unknown rather than as absence."
        )
    # `too_broad` means the tool positively observed metadata and refused to
    # return it unkeyed -- the opposite of "nothing was found". Saying so
    # here would contradict the too_broad note above, so only claim absence
    # when nothing was found *and* nothing was refused for size.
    if not attributes and msd_status != "too_broad":
        notes.append("No reference metadata was found for this dataflow.")

    return {
        "dataflow_id": dataflow_id,
        "agency_id": agency,
        "endpoint": getattr(client, "endpoint_key", None),
        "version": version,
        "metadata_attributes": attributes,
        "channels": channels,
        "notes": notes,
    }
