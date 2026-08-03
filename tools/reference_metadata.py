"""Reference metadata retrieval.

Reference metadata is the descriptive material about a dataflow: who compiled
it, from what source, under what licence, with what caveats. It is what turns
a retrieved number into something citable.

There is no single place it lives. .Stat Suite deployments serve it through a
data query on their `/v2/` API surface asking for MSD attributes
(`attributes=msd`), while other providers put descriptive text in ordinary DSD
attributes on the data message. This module reads both and reports which
channels a provider left empty.

That `/v2/` surface is a newer query and serialization layer over artefacts
that remain modelled and versioned per SDMx 2.1, so it is not an SDMx 3.0
implementation. The practical consequence is version resolution: artefacts
carry two-part versions like `4.3`, the 2.1 `latest` keyword is rejected by the
v2 query parser, and the 3.0 `+` operator matches only three-part semver, so no
wildcard reliably resolves. Callers must resolve the exact published version
first, which is what `get_reference_metadata` does.
"""

import csv
import html
import io
import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

logger = logging.getLogger(__name__)

# `en:"<p>text</p>",fr:""`: a language tag, a colon, then a quoted value.
_LOCALISED = re.compile(r'([A-Za-z]{2,3}(?:-[A-Za-z]{2,4})?):"((?:[^"\\]|\\.)*)"')
_TAG = re.compile(r"<[^>]+>")
_HREF = re.compile(r'<a\s[^>]*href=\\?"([^"\\]+)\\?"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?Z?)?$")


def strip_markup(raw: str) -> str:
    """Plain text from the HTML fragments providers embed in metadata values."""
    if not raw:
        return ""
    # Keep a link's text and its target, since the URL is often the useful part.
    text = _HREF.sub(lambda m: m.group(2) + " (" + m.group(1) + ")", raw)
    text = _TAG.sub(" ", text)
    text = html.unescape(text).replace("\xa0", " ")
    return " ".join(text.split())


def classify_value_kind(value: str | None, has_codelist: bool = False) -> str:
    """Say what kind of thing a metadata value is, so a caller knows whether
    it can be expanded (a code), followed (a url), or read as final (prose).

    Returns "unknown" rather than falling back to "prose" when there is
    nothing to inspect, so an empty answer is not dressed up as examined text.
    """
    if not value or not value.strip():
        return "unknown"
    text = value.strip()
    if text.startswith(("http://", "https://")):
        return "url"
    if _ISO_DATE.match(text):
        return "date"
    if has_codelist:
        return "code"
    return "prose"


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


# Above this many data rows an MSD response is truncated rather than read in
# full, so a pathological or misbehaving response cannot spin forever. Real
# responses are nowhere near this: OECD's HICP metadata query, the largest
# seen in practice, is 111 rows. This is not a size guard: a keyed request
# is exempt from `UNKEYED_SIZE_CAP_BYTES` (see `_bounded_get`) but not from
# this row cap, so a broad-but-legal key can still hit it.
_MAX_MSD_DATA_ROWS = 5000


class _ParsedAttributes(list):
    """The list `parse_msd_csv` returns, plus whether the row scan stopped
    early at `_MAX_MSD_DATA_ROWS`.

    A plain `list` subclass rather than a separate return value, so every
    existing caller that treats the result as a list of attribute dicts
    (`for attr in parse_msd_csv(...)`, `parse_msd_csv(...) == []`) keeps
    working unchanged; only a caller that wants to know about truncation
    reads `.truncated`, via `getattr(result, "truncated", False)` so a
    plain list (e.g. one built directly in a test) is treated as untruncated
    rather than raising.

    Truncation matters because a column whose only value lives in a row
    past the cap reads as `declared_empty` -- indistinguishable, without
    this flag, from a column the provider never fills at all.
    """

    def __init__(self, *args: Any, truncated: bool = False) -> None:
        super().__init__(*args)
        self.truncated = truncated


def _row_level(
    row: list[str], dimension_columns: list[tuple[int, str]]
) -> tuple[str, dict[str, str]]:
    """Classify one MSD data row from its own dimension cells.

    A row where every dimension cell is a wildcard (`~`) or empty was not
    narrowed by a key, so whatever metadata it carries describes the
    dataflow as a whole. A row with even one concrete dimension value only
    describes that slice -- e.g. row 1 of OECD's HICP response is
    REF_AREA=GBR with every other dimension wildcarded, so its COVERAGE
    value is the United Kingdom's, not the dataflow's.

    Returns (level, key_context): level is `"dataflow"` or `"partial_key"`;
    key_context holds the row's concrete dimension values, empty for a
    dataflow-level row.
    """
    key_context: dict[str, str] = {}
    for index, dim_id in dimension_columns:
        cell = row[index].strip() if index < len(row) else ""
        if cell and cell != "~":
            key_context[dim_id] = cell
    level = "partial_key" if key_context else "dataflow"
    return level, key_context


def parse_msd_csv(
    text: str,
    dimension_ids: set[str] | frozenset[str] = frozenset(),
    prefer: str = "en",
) -> _ParsedAttributes:
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

    An unkeyed MSD response is one row per attachment target, not one row
    for the whole dataflow. OECD's HICP metadata query returns 111 rows, and
    a metadata column can be populated in some rows and empty in others: of
    the 8 columns that carry a value somewhere in that response, row 1 alone
    carries only 5, missing DATA_COMP, QUALITY_ASSMNT and REC_USE_LIM (the
    dataflow's recommended-uses-and-limitations text) entirely. Every row is
    read here, up to `_MAX_MSD_DATA_ROWS`, and each metadata column's
    distinct non-empty values are collected across all of them. The returned
    `_ParsedAttributes` is a `list` in every respect except one: its
    `.truncated` flag is `True` when the row cap was hit, so a caller can
    tell a column that reports `declared_empty` because a later, unread row
    was the only one that populated it apart from a column the provider
    genuinely never fills. `scope` and
    `key_context` come from each row's own dimension cells (`_row_level`): a
    value attaches to the whole dataflow only when the row that carried it
    had every dimension wildcarded, otherwise it attaches to that row's
    partial key. Entries are deduped on the `(value, key_context)` pair, not
    on the value text alone: SPC's `DF_SDG` publishes the same "UNSD" text
    for FJI, TON and WSM on three separate rows, each with its own
    key_context, and every one of those pairs survives as its own entry in
    `all_values` -- deduping on the text alone would keep only the first
    row's context and silently misattribute a Pacific-wide value to a
    single country. A dataflow-level row always produces `key_context=None`,
    so its pair can never collide with a partial-key row's pair for the same
    value; this is what makes the same response parse to the same headline
    regardless of which row came first, without needing to mutate an
    already-recorded entry in place. When a column carries several distinct
    values, the headline `value` (and its `scope`/`key_context`) prefers a
    dataflow-level entry over a partial-key one, found by scanning
    `all_values` for one; `all_values` keeps every distinct pair in
    first-seen order uncapped for drill-down use, and `distinct_value_count`
    counts distinct *values* (not pairs), since that is what the headline
    rule and the `all_observed_rows` promotion below both key off. Declared-
    but-empty columns are returned with `status="declared_empty"` so callers
    can distinguish a blank licence field from a provider that defines no
    licence concept.

    A column whose single distinct value never appears on an unqualified
    row still gets a `scope` of `"all_observed_rows"`, rather than staying
    `partial_key`, when that value was present on every data row this call
    actually read and the response was not truncated. SPC's `DF_SDG`
    publishes the same value on every per-country row and has no
    dataflow-wide row at all, so every row is `partial_key` on its own; the
    value nonetheless describes everything this query returned. This is a
    weaker claim than `"dataflow"`: it says the value was identical on every
    row the query happened to return, not that the provider declared it
    unqualified, so it never overrides an already-promoted `"dataflow"`
    scope. Truncation makes "every row" unknowable, so a truncated response
    never uses it even when the rows actually read all agreed.
    """
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return _ParsedAttributes([])

    dimension_columns: list[tuple[int, str]] = []
    meta_columns: list[tuple[int, str, str, str | None]] = []
    for index, column in enumerate(header):
        if not _SDMX_IDENTIFIER.fullmatch(column):
            continue
        if column in _STRUCTURAL_COLUMNS:
            continue
        attr_id = column.rsplit(".", 1)[-1]
        if attr_id in dimension_ids:
            dimension_columns.append((index, attr_id))
            continue
        label = header[index + 1] if index + 1 < len(header) else None
        meta_columns.append((index, column, attr_id, label))

    if not meta_columns:
        return _ParsedAttributes([])

    # Per metadata column index: every distinct (value, key_context) pair
    # seen, in first-seen order, uncapped -- these pairs are kept in full in
    # the assembled result below (not capped), since callers need the
    # complete dataset for drill-down purposes. `entry_by_value` indexes the
    # same entries by their pair, keyed on the value text plus a hashable
    # form of key_context, so a repeat of an already-seen pair is recognised
    # without a linear scan.
    collected: dict[int, list[dict[str, Any]]] = {index: [] for index, *_ in meta_columns}
    entry_by_value: dict[int, dict[tuple[Any, ...], dict[str, Any]]] = {
        index: {} for index, *_ in meta_columns
    }
    # Per metadata column index: how many of the data rows actually read
    # carried any value at all for it, regardless of whether that value was
    # distinct from others seen. Compared against `total_rows_read` below to
    # tell "the same value on every row" apart from "the same value on some
    # of them, blank on the rest"; only the former earns the
    # `"all_observed_rows"` scope.
    rows_with_value: dict[int, int] = {index: 0 for index, *_ in meta_columns}

    truncated = False
    total_rows_read = 0
    for row_number, row in enumerate(reader):
        if row_number >= _MAX_MSD_DATA_ROWS:
            logger.info(
                "reference metadata response had more than %s data rows; truncating",
                _MAX_MSD_DATA_ROWS,
            )
            truncated = True
            break
        total_rows_read += 1
        level, key_context = _row_level(row, dimension_columns)
        for index, _column, _attr_id, _label in meta_columns:
            raw = row[index] if index < len(row) else ""
            value, language = parse_localised_value(raw, prefer=prefer)
            if value is None:
                continue
            rows_with_value[index] += 1
            # A dataflow-level row always has an empty key_context, so its
            # pair key can never collide with a partial-key row's pair for
            # the same value -- a later dataflow-scope row confirming a
            # value already seen at partial-key naturally becomes its own,
            # additional entry rather than needing to mutate the earlier one
            # in place. That is what keeps the headline scan below
            # independent of row order without any promotion step here.
            pair_key = (value, tuple(sorted(key_context.items())) if key_context else None)
            if pair_key in entry_by_value[index]:
                continue
            entry = {
                "value": value,
                "language": language,
                "scope": level,
                "key_context": key_context or None,
            }
            entry_by_value[index][pair_key] = entry
            collected[index].append(entry)

    out: list[dict[str, Any]] = []
    for index, column, attr_id, label in meta_columns:
        values = collected[index]
        if not values:
            # Declared by the provider's MSD and left blank. Reporting this
            # is the point: a blank licence field is a different answer from
            # a provider that defines no licence concept.
            out.append({
                "id": attr_id,
                "path": column,
                "label": label,
                "status": "declared_empty",
                "scope": None,
                "value": None,
                "language": None,
                "key_context": None,
                "distinct_value_count": 0,
                "all_values": [],
            })
            continue
        # `values` now holds distinct (value, key_context) pairs, so several
        # entries can share the same value text -- distinct_value_count
        # counts distinct values, since that is what the headline rule and
        # the all_observed_rows promotion below both key off, and "one
        # value published for twelve countries" must stay
        # distinct_value_count == 1.
        distinct_value_count = len({v["value"] for v in values})
        headline = next((v for v in values if v["scope"] == "dataflow"), values[0])
        scope = headline["scope"]
        key_context = headline["key_context"]
        # A value that never appeared on an unqualified row is still worth
        # surfacing as a headline when it was the only value seen and it
        # was present on literally every row this call read: that is a
        # weaker claim than `"dataflow"` (it says nothing about rows this
        # query did not return, e.g. a different key or rows past the row
        # cap), so it never overrides an already-promoted `"dataflow"`
        # scope, and a truncated read can never earn it either. The
        # headline entry itself keeps key_context=None here even though
        # all_values may hold several per-country entries behind it: this
        # is a sample-of-one headline, not a claim about a single slice.
        if (
            scope == "partial_key"
            and distinct_value_count == 1
            and not truncated
            and total_rows_read > 0
            and rows_with_value[index] == total_rows_read
        ):
            scope = "all_observed_rows"
            key_context = None
        out.append({
            "id": attr_id,
            "path": column,
            "label": label,
            "status": "populated",
            "scope": scope,
            "value": headline["value"],
            "language": headline["language"],
            "key_context": key_context,
            "distinct_value_count": distinct_value_count,
            "all_values": values,
        })
    return _ParsedAttributes(out, truncated=truncated)


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


async def _bounded_get(
    session: Any, url: str, headers: dict[str, str], unkeyed: bool
) -> tuple[int, bytes | None, str | None]:
    """GET a URL without ever materialising an unbounded unkeyed body.

    A keyed request has already been narrowed by the caller, so it is read
    in full and never refused for size -- this only applies to unkeyed
    requests, the one way an oversized response reaches this tool (SPC's
    DF_SDG is 5.37 MB unfiltered; ECB's EXR and IMF's CPI are multi-megabyte
    too, through the DSD-attribute fallback). A plain `session.get` would
    materialise the whole body before any size check could run, which is
    exactly the bug this exists to avoid, so an unkeyed request is streamed
    instead and aborted once more than `UNKEYED_SIZE_CAP_BYTES` has actually
    been read, or immediately when a `Content-Length` header already says as
    much.

    Returns (status_code, body, encoding). `body` is `None` when an unkeyed
    response was refused for size before being fully read; the caller must
    report that as `too_broad` rather than try to parse it.
    """
    if not unkeyed:
        response = await session.get(url, headers=headers)
        return response.status_code, response.content, response.encoding

    async with session.stream("GET", url, headers=headers) as response:
        if response.status_code != 200:
            return response.status_code, b"", None

        content_length = response.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > UNKEYED_SIZE_CAP_BYTES:
                    return response.status_code, None, None
            except ValueError:
                pass

        body = bytearray()
        async for chunk in response.aiter_bytes():
            body.extend(chunk)
            if len(body) > UNKEYED_SIZE_CAP_BYTES:
                return response.status_code, None, None
        return response.status_code, bytes(body), response.encoding


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
        status_code, body, encoding = await _bounded_get(
            session, url, {"Accept-Language": "en"}, unkeyed=not key
        )
    except Exception as exc:
        logger.info("reference metadata request failed for %s: %s", dataflow_id, exc)
        return [], "inconclusive"

    if status_code == 204:
        return [], "inconclusive"
    if status_code != 200:
        logger.info(
            "reference metadata query returned HTTP %s for %s",
            status_code, dataflow_id,
        )
        return [], "inconclusive"

    # An unkeyed query against a large dataflow is the one way this tool can
    # pull megabytes: SPC's DF_SDG is 5.37 MB unfiltered and 5.6 KB with a
    # partial key. `_bounded_get` aborts the read partway through rather than
    # materialising the whole body first, so `body` is `None` here, not a
    # size measured after the fact.
    if body is None:
        logger.info(
            "reference metadata response for %s exceeded %s bytes; asking for a key",
            dataflow_id, UNKEYED_SIZE_CAP_BYTES,
        )
        return [], "too_broad"

    text = body.decode(encoding or "utf-8", errors="replace")

    # A single oversized field (not just the trailing padding the size guard
    # above catches) can still exceed Python's csv field-size limit, e.g. a
    # provider that concatenates many language translations into one value.
    # That is a malformed/unparseable response, not proof of absence.
    try:
        attributes = parse_msd_csv(text, dimension_ids=dimension_ids)
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
    if not _looks_like_sdmx_csv(text):
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

    Every attribute this channel returns carries `status="populated"`: it
    reads only what the message itself carries, so there is no way to see an
    attribute the DSD declares but this response leaves empty, unlike the
    MSD channel's `declared_empty` case. Recognising that would mean
    consulting the DSD's declared attribute list, which is out of scope here.

    Returns (attributes, status) where status can also be `too_broad`:
    `firstNObservations=1` still returns one row per series, so an unkeyed
    request against a large dataflow (ECB's EXR, IMF's CPI) is a multi-
    megabyte body that `root.iter()` would then have to walk in full -- the
    same size guard used for the MSD channel applies here too.
    """
    unkeyed = not key or key == "all"
    url = (client.base_url + "/data/" + agency_id + "," + dataflow_id + "/"
           + (key or "all") + "?firstNObservations=1")
    try:
        session = await client._get_session()
        status_code, body, _encoding = await _bounded_get(
            session, url, {"Accept": STRUCTURE_SPECIFIC_DATA, "Accept-Language": "en"},
            unkeyed=unkeyed,
        )
    except Exception as exc:
        logger.info("attribute metadata request failed for %s: %s", dataflow_id, exc)
        return [], "inconclusive"

    if status_code != 200:
        return [], "inconclusive"

    if body is None:
        logger.info(
            "attribute metadata response for %s exceeded %s bytes; asking for a key",
            dataflow_id, UNKEYED_SIZE_CAP_BYTES,
        )
        return [], "too_broad"

    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return [], "inconclusive"

    # A provider that ignores our structure-specific Accept header can still
    # answer 200 with a well-formed SDMx-ML *Generic* message, which reuses
    # the same DataSet/Series/Obs local names but carries attribute values
    # as child <generic:Value> elements rather than XML attributes -- so the
    # loop below would find zero attributes and this would misread as
    # `empty` ("carries no descriptive attributes") when the truth is
    # "answered in a dialect we do not parse". Only the structure-specific
    # dialect's root is accepted as evidence either way.
    root_tag = root.tag.rsplit("}", 1)[-1]
    if root_tag != "StructureSpecificData":
        logger.info(
            "attribute metadata body for %s was not structure-specific data (root was %s)",
            dataflow_id, root_tag,
        )
        return [], "inconclusive"

    # Per attribute name: every distinct value seen, in first-seen order,
    # uncapped -- mirrors parse_msd_csv's `collected`/`entry_by_value` pair
    # above. An attribute is declared at exactly one attachment level in the
    # DSD, so `scope` is fixed the first time a name is seen (unlike
    # parse_msd_csv's per-row `scope`, there is no later row that could
    # promote it); only its values can repeat, e.g. the same
    # observation-level attribute on many Obs elements.
    order: list[str] = []
    scope_by_id: dict[str, str] = {}
    values_by_id: dict[str, list[dict[str, Any]]] = {}
    seen_values: dict[str, set[str]] = {}
    saw_data_message = False
    for element in root.iter():
        level = _LEVEL_BY_TAG.get(element.tag.rsplit("}", 1)[-1])
        if level is None:
            continue
        saw_data_message = True
        for name, raw in element.attrib.items():
            # Namespaced attributes are envelope plumbing, not metadata.
            if "}" in name or name in _NOT_METADATA or name in dimension_ids:
                continue
            value, language = parse_localised_value(raw)
            if value is None:
                continue
            if name not in values_by_id:
                order.append(name)
                scope_by_id[name] = level
                values_by_id[name] = []
                seen_values[name] = set()
            if value in seen_values[name]:
                continue
            seen_values[name].add(value)
            values_by_id[name].append({
                "value": value,
                "language": language,
                "scope": level,
                "key_context": None,
            })

    out: list[dict[str, Any]] = []
    for name in order:
        values = values_by_id[name]
        headline = values[0]
        out.append({
            "id": name,
            "path": name,
            "label": None,
            "status": "populated",
            "scope": scope_by_id[name],
            "value": headline["value"],
            "language": headline["language"],
            "key_context": None,
            "distinct_value_count": len(values),
            "all_values": values,
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


def _channel_status_notes(msd_status: str, dsd_status: str | None) -> list[str]:
    """Plain-language notes for a channel status that did not deliver a
    confirmed declared set (unsupported, inconclusive or too_broad).

    Shared between get_reference_metadata and get_metadata_attribute_values
    so the two report the same channel failure the same way rather than
    drifting into two different descriptions of the same fact. `dsd_status`
    is `None` when the DSD fallback never ran (the MSD channel answered
    `found`), which matches every condition below being skipped.
    """
    notes: list[str] = []
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
    if dsd_status == "inconclusive":
        notes.append(
            "The DSD-attribute fallback query did not return a usable "
            "response, which can mean either that this dataflow carries no "
            "descriptive attributes or that the request failed. Treat it as "
            "unknown rather than as absence."
        )
    if dsd_status == "too_broad":
        notes.append(
            "This dataflow's attribute data is too large to return "
            "unfiltered. Supply a dimension key to narrow it, for example a "
            "single indicator or reference area."
        )
    return notes


def _unresolved_attribute_result(
    dataflow_id: str, attribute_id: str, msd_status: str, dsd_status: str | None
) -> dict[str, Any]:
    """The shared "we do not know" shape get_metadata_attribute_values
    returns whenever no channel resolved to a declared set it can speak
    for, whether that is discovered before any attribute lookup was even
    attempted or only once the lookup found nothing to report.
    """
    return {
        "dataflow_id": dataflow_id,
        "attribute_id": attribute_id,
        "label": None,
        "value_kind": "unknown",
        "values": [],
        "total": 0,
        "truncated": False,
        "notes": _channel_status_notes(msd_status, dsd_status),
    }


# Scopes eligible for a headline value: a provider-marked dataflow-wide
# value (`dataflow`, `dataset`), or a value that was identical on every
# data row a query actually returned (`all_observed_rows`, see
# parse_msd_csv). `_DATAFLOW_WIDE_SCOPES` below is a strict subset of this
# set: it also retires the drill-down, which only the provider-marked
# scopes earn, since only they say anything about rows the query did not
# return.
_HEADLINE_SCOPES = frozenset({"dataflow", "dataset", "all_observed_rows"})

# Scopes where no further per-slice detail remains to drill into: a strict
# subset of `_HEADLINE_SCOPES`. `"all_observed_rows"` (see parse_msd_csv)
# earns a headline value because it was identical on every row a query
# returned, but says nothing about rows that query did not return, so
# drill_down must stay true for it even though a headline is shown.
_DATAFLOW_WIDE_SCOPES = frozenset({"dataflow", "dataset"})


def _to_summary_attribute(attr: dict[str, Any]) -> dict[str, Any]:
    """Apply the headline rule: a value is only offered as the dataflow's
    answer when exactly one distinct value exists and it either describes
    the whole dataflow (`scope` is `dataflow` or `dataset`) or was identical
    on every row a query actually returned (`all_observed_rows`). A value
    that describes one slice, such as a single country's caveats or a
    single series' source, is still that slice's answer, so it stays behind
    the drill-down even when it is the only value seen: one value is not
    the same as one that describes the whole dataflow. `all_observed_rows`
    keeps the drill-down available too, even though it earns a headline:
    unlike `dataflow`/`dataset`, it makes no claim about rows outside the
    query, so the per-row detail behind it still matters.
    """
    if attr["status"] == "declared_empty":
        return {
            "id": attr["id"],
            "path": attr["path"],
            "label": attr["label"],
            "status": "declared_empty",
            "scope": None,
            "value_kind": "unknown",
            "distinct_values": 0,
            "value": None,
            "language": None,
            "sample_key_context": None,
            "drill_down": False,
            "source": attr.get("source"),
        }
    has_headline = (
        attr["distinct_value_count"] == 1 and attr["scope"] in _HEADLINE_SCOPES
    )
    fully_resolved = (
        attr["distinct_value_count"] == 1 and attr["scope"] in _DATAFLOW_WIDE_SCOPES
    )
    return {
        "id": attr["id"],
        "path": attr["path"],
        "label": attr["label"],
        "status": "populated",
        "scope": attr["scope"],
        "value_kind": classify_value_kind(attr["value"], has_codelist=False),
        "distinct_values": attr["distinct_value_count"],
        "value": attr["value"] if has_headline else None,
        "language": attr["language"] if has_headline else None,
        "sample_key_context": attr["key_context"] if not has_headline else None,
        "drill_down": not fully_resolved,
        "source": attr.get("source"),
    }


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
    "we could not find out". Each attribute found is reduced to a summary
    shape by `_to_summary_attribute`, which decides whether its value may be
    offered as the dataflow's headline answer, and `coverage` counts how
    many of the provider's declared attributes were actually populated --
    reported only when a channel capable of seeing the declared set actually
    answered, `None` otherwise.
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
        # `scope` (and, for a partial-key value, `key_context`) already come
        # from parse_msd_csv, which derives them per row rather than
        # assuming every value describes the whole dataflow.
        attributes.append({**attr, "source": "msd"})
    if getattr(msd_attrs, "truncated", False):
        notes.append(
            "This dataflow's metadata response was cut off after "
            + str(_MAX_MSD_DATA_ROWS) + " rows; a declared_empty attribute "
            "here may only appear in a row that was not read, not "
            "genuinely absent."
        )

    if msd_status != "found":
        dsd_attrs, dsd_status = await fetch_dsd_attribute_metadata(
            client, dataflow_id, agency, key or "all", ctx=ctx, dimension_ids=dimension_ids
        )
        channels["dsd_attributes"] = dsd_status
        for attr in dsd_attrs:
            attributes.append({**attr, "source": "dsd_attribute"})
    else:
        channels["dsd_attributes"] = "skipped"

    notes.extend(_channel_status_notes(msd_status, channels.get("dsd_attributes")))
    # `too_broad` on either channel means the tool positively observed
    # metadata and refused to return it unkeyed -- the opposite of "nothing
    # was found". Saying so here would contradict the too_broad notes above,
    # so only claim absence when nothing was found *and* nothing was refused
    # for size on either channel.
    if (
        not attributes
        and msd_status != "too_broad"
        and channels.get("dsd_attributes") != "too_broad"
    ):
        notes.append("No reference metadata was found for this dataflow.")

    summary_attributes = [_to_summary_attribute(attr) for attr in attributes]

    if any(attr["scope"] == "all_observed_rows" for attr in summary_attributes):
        notes.append(
            "One or more attribute values were identical on every row this "
            "query returned, so they are shown as a headline value. Rows "
            "outside this query, such as a different key or rows beyond "
            "the response's row limit, are not covered by that value."
        )

    # `coverage` counts the provider's declared attributes against how many
    # are actually populated. Only the MSD channel can see a declared-but-
    # empty attribute -- parse_msd_csv reports "declared_empty" for a column
    # every row leaves blank -- so when the DSD-attribute fallback answered,
    # `declared` would just be `populated` relabelled and `empty` would
    # always read zero, which is not what those numbers claim to mean.
    # And when neither channel gave a confirmed answer (inconclusive,
    # too_broad, unsupported), zero attributes is not evidence of zero
    # declared; it is evidence we do not know, so `coverage` must not
    # assert absence there either. In both cases the honest answer is
    # "unknown", which is what leaving `coverage` as `None` says.
    # An MSD "empty" on its own is not the whole picture: the DSD fallback
    # always runs whenever the MSD channel did not answer "found" (that
    # includes a legitimate MSD "empty"), and its attributes are folded into
    # `summary_attributes` alongside whatever the MSD channel contributed.
    # So an MSD "empty" only makes the combined declared set known once the
    # DSD fallback has also resolved (found or empty) -- if it is instead
    # too_broad or inconclusive, an empty `summary_attributes` reflects a
    # fetch that did not resolve, not a confirmed absence, and coverage must
    # stay unknown there too.
    coverage: dict[str, int] | None = None
    if channels.get("dsd_attributes") == "found":
        notes.append(
            "This dataflow's metadata came from the DSD-attribute channel, "
            "which shows only populated attributes. The provider's full "
            "declared set is not observable through this channel, so "
            "coverage counts are not reported."
        )
    elif channels.get("msd_v2") == "found" or (
        channels.get("msd_v2") == "empty"
        and channels.get("dsd_attributes") == "empty"
    ):
        coverage = {
            "declared": len(summary_attributes),
            "populated": sum(1 for a in summary_attributes if a["status"] == "populated"),
            "empty": sum(1 for a in summary_attributes if a["status"] == "declared_empty"),
        }

    return {
        "dataflow_id": dataflow_id,
        "agency_id": agency,
        "endpoint": getattr(client, "endpoint_key", None),
        "version": version,
        "metadata_attributes": summary_attributes,
        "coverage": coverage,
        "channels": channels,
        "notes": notes,
    }


# Above this many values, get_metadata_attribute_values caps what it returns
# and reports the true count separately via `total`/`truncated`, the same
# shape UNKEYED_SIZE_CAP_BYTES protects the channel fetch itself against.
_MAX_ATTRIBUTE_VALUES = 200


async def get_metadata_attribute_values(
    client: Any,
    dataflow_id: str,
    attribute_id: str,
    key: str | None = None,
    agency_id: str | None = None,
    ctx: Any | None = None,
) -> dict[str, Any]:
    """Get every value of one reference metadata attribute, with the slice
    each applies to.

    This is the drill-down get_reference_metadata() points to whenever a
    summary attribute reports `drill_down=true`: more detail remains than the
    summary shows, either because values differ across rows or because a
    single value was identical only on the rows this query returned.
    This call lets a caller read all distinct values and their slices.

    Fetches exactly as get_reference_metadata does -- fetch_msd_metadata,
    falling back to fetch_dsd_attribute_metadata when the MSD channel does
    not answer -- so the two calls can never disagree about what a provider
    published; only the attribute selected and how much of it is returned
    differ. `attribute_id` is matched against each attribute's `id` (the
    part of `path` after the last dot), since that is what a caller reads
    back from get_reference_metadata()'s summary output, not the full
    hierarchical path.

    Four distinct answers matter here and must not be collapsed into one
    another:

    - Neither channel resolving to a declared set it can speak for is
      reported as that channel state, using the same wording
      get_reference_metadata uses. This includes an MSD channel that
      answered `too_broad`, `inconclusive` or `unsupported` even when the
      DSD fallback on its own resolved to `empty`, since that fallback
      cannot see a declared-but-blank attribute and so cannot confirm the
      declared set by itself; only the MSD channel's own `found` or
      `empty` answer can. It is not evidence the attribute does not exist,
      so it must not be phrased as the unknown-attribute error below.
    - `attribute_id` absent from a declared set a channel *did* confirm is
      an error naming the declared ids, never an empty value list -- an
      empty list reads as "this attribute has no values" when the true
      answer is "you asked for something that does not exist".
    - A declared-but-empty attribute (the MSD channel's `declared_empty`
      status: the provider defines it for this dataflow and every row
      leaves it blank) returns `total: 0` with no error, only a note --
      that is a real, observed answer, not a failure.
    - A populated attribute returns its values, capped at
      `_MAX_ATTRIBUTE_VALUES` with `truncated` set and `total` left as the
      true, uncapped count. Values read through the DSD-attribute fallback
      carry `key_context: null` for every entry regardless of how many
      distinct values there are, because that channel has no per-value key
      to report (see fetch_dsd_attribute_metadata) -- a note says so rather
      than letting several disagreeing values look like several
      dataflow-wide statements.
    """
    agency = agency_id or client.agency_id

    try:
        structure = await client.get_structure_summary(
            dataflow_id=dataflow_id, agency_id=agency
        )
        dimension_ids = {dim.id for dim in structure.dimensions}
    except Exception as exc:
        logger.info("could not resolve dimensions for %s: %s", dataflow_id, exc)
        dimension_ids = set()

    try:
        version = await client.resolve_version(
            dataflow_id=dataflow_id, agency_id=agency, ctx=ctx
        )
    except Exception as exc:
        logger.info("could not resolve version for %s: %s", dataflow_id, exc)
        version = None

    msd_attrs, msd_status = await fetch_msd_metadata(
        client, dataflow_id, agency, key, ctx=ctx, version=version, dimension_ids=dimension_ids
    )
    from_dsd = msd_status != "found"
    dsd_status: str | None = None
    if not from_dsd:
        attributes = msd_attrs
    else:
        dsd_attrs, dsd_status = await fetch_dsd_attribute_metadata(
            client, dataflow_id, agency, key or "all", ctx=ctx, dimension_ids=dimension_ids
        )
        attributes = dsd_attrs

    # `attributes` above came from whichever channel actually supplied it:
    # msd_attrs when the MSD channel found something (the DSD fallback never
    # runs then), otherwise dsd_attrs -- and the DSD fallback always runs
    # whenever the MSD channel did not answer "found", which includes a
    # legitimate MSD "empty" (zero metadata columns), not just a failure.
    # So the declared set is known only when the channel that actually
    # produced `attributes` resolved: MSD found (dsd_status is irrelevant,
    # it was never queried), or the DSD fallback answered found or empty.
    # An MSD "empty" on its own is not enough -- if the DSD fallback then
    # failed to resolve, `attributes` is empty for a reason that has
    # nothing to do with the declared set, and treating that as "confirmed
    # empty" would misreport a DSD-side fetch failure as a caller mistake.
    channel_confirmed = msd_status == "found" or dsd_status in ("found", "empty")
    if not channel_confirmed:
        return _unresolved_attribute_result(dataflow_id, attribute_id, msd_status, dsd_status)

    match = next((attr for attr in attributes if attr["id"] == attribute_id), None)
    if match is None:
        if attributes:
            declared = ", ".join(sorted(attr["id"] for attr in attributes))
            error = (
                "Unknown attribute '" + attribute_id + "' for " + dataflow_id
                + ": declared attributes are " + declared
            )
        elif msd_status not in ("found", "empty"):
            # `attributes` is empty here only because dsd_status == "empty":
            # that is the one way channel_confirmed can be true without
            # msd_status itself being found or empty (see the note above).
            # The DSD fallback saw a message with no attributes at all, but
            # by its own documented contract (fetch_dsd_attribute_metadata)
            # it never sees a declared-but-blank attribute, so it cannot
            # confirm the declared set on its own; only the MSD channel's
            # own found/empty answer can, the same rule
            # get_reference_metadata's coverage count uses. Reporting
            # "confirmed empty" here would misread a too_broad, inconclusive
            # or unsupported MSD channel as a genuine declared-empty answer.
            return _unresolved_attribute_result(
                dataflow_id, attribute_id, msd_status, dsd_status
            )
        else:
            # A confirmed channel that found nothing at all is a different
            # answer from "some other attribute exists but not this one":
            # naming an empty list after a colon reads as a formatting bug,
            # not as "this dataflow declares no reference metadata."
            error = (
                "Unknown attribute '" + attribute_id + "' for " + dataflow_id
                + ": this dataflow's declared metadata attributes are confirmed empty."
            )
        return {
            "dataflow_id": dataflow_id,
            "attribute_id": attribute_id,
            "label": None,
            "value_kind": "unknown",
            "values": [],
            "total": 0,
            "truncated": False,
            "notes": [],
            "error": error,
        }

    if match["status"] == "declared_empty":
        # `declared_empty` only describes the response actually read: the
        # whole dataflow when no key was supplied, or the one slice queried
        # when it was -- a keyed request never sees the other slices, so it
        # cannot speak for them. SPC's DF_SDG can only be queried keyed, so
        # this is the ordinary path for its flagship dataflow, not an edge
        # case.
        if key is not None:
            declared_empty_note = (
                "'" + attribute_id + "' is declared for this dataflow and the "
                "provider published no value for it in the slice queried."
            )
        else:
            declared_empty_note = (
                "'" + attribute_id + "' is declared for this dataflow and the "
                "provider has published no value for it."
            )
        empty_notes = [declared_empty_note]
        if not from_dsd and getattr(attributes, "truncated", False):
            empty_notes.append(
                "This response was cut off after " + str(_MAX_MSD_DATA_ROWS)
                + " rows; a value beyond that point would not have been read, "
                "so this may be an artefact of truncation rather than the "
                "provider's doing."
            )
        return {
            "dataflow_id": dataflow_id,
            "attribute_id": attribute_id,
            "label": match["label"],
            "value_kind": "unknown",
            "values": [],
            "total": 0,
            "truncated": False,
            "notes": empty_notes,
        }

    all_values = match["all_values"]
    total = len(all_values)
    capped = all_values[:_MAX_ATTRIBUTE_VALUES]
    truncated = total > len(capped)
    values = [
        {
            "value": v["value"],
            "key_context": v.get("key_context"),
            "language": v.get("language"),
        }
        for v in capped
    ]
    notes: list[str] = []
    # `_DATAFLOW_WIDE_SCOPES` treats `dataset` scope as dataflow-wide in the
    # summary's headline rule (`_to_summary_attribute`), so for a
    # dataset-scope value that null key_context is the expected,
    # dataflow-wide meaning and needs no caveat -- only `series` and
    # `observation` scope, which the headline rule does NOT treat as
    # dataflow-wide, would otherwise have a null key_context misread as
    # "dataflow-wide" when it actually means "this channel cannot say".
    if from_dsd and match["scope"] not in _DATAFLOW_WIDE_SCOPES:
        notes.append(
            "This attribute came from the DSD-attribute channel, which has "
            "no per-value key to report: key_context is null for every "
            "value here even though they attach at " + match["scope"]
            + " level, not to the whole dataflow."
        )
    if truncated:
        notes.append(
            "Showing the first " + str(_MAX_ATTRIBUTE_VALUES) + " of " + str(total)
            + " values."
        )
    return {
        "dataflow_id": dataflow_id,
        "attribute_id": attribute_id,
        "label": match["label"],
        "value_kind": classify_value_kind(match["value"], has_codelist=False),
        "values": values,
        "total": total,
        "truncated": truncated,
        "notes": notes,
    }
