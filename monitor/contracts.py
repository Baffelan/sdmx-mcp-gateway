"""Assert the API behaviours the gateway depends on, per provider.

These probes are deliberately about behaviour rather than content. Whether a
dataflow gained a dimension is not our business; whether `references=parents`
is still accepted is, because the gateway builds that query and a change
turns working features into silent empty answers.

Every probe uses `detail=allstubs` and a single artefact so a full sweep stays
small: it shrinks IMF's `references=all` response from 2.8 MB to 18 KB while
still exercising the parameter.
"""

import time
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import httpx

from contracts_config import REFERENCE_PROBES, ContractExpectation
from endpoints_config import Endpoint
from sdmx_spec import classify_status, is_legal_reference
from storage import ContractResult

SDMX_STRUCTURE_XML = "application/vnd.sdmx.structure+xml;version=2.1"


def structure_url(ep: Endpoint, exp: ContractExpectation, **params: str) -> str:
    """A single-dataflow structure query, with the provider's own quirks applied."""
    base = (ep.base_url + "/dataflow/" + exp.flow_agency + "/" + exp.flow_id
            + "/latest")
    query = dict(params)
    if ep.key == "STATSNZ":
        # Stats NZ's APIM gateway has historically ignored the Accept header
        # for structural metadata; the gateway forces format=xml for this.
        query["format"] = "xml"
    return base + ("?" + urlencode(query) if query else "")


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def _get(client: httpx.AsyncClient, ep: Endpoint, url: str
               ) -> tuple[httpx.Response | None, str | None]:
    headers = {"Accept": SDMX_STRUCTURE_XML, **ep.auth_headers()}
    try:
        return await client.get(url, headers=headers), None
    except Exception as exc:
        return None, (type(exc).__name__ + ": " + str(exc))[:300]


async def _get_unauthenticated(
    client: httpx.AsyncClient, url: str
) -> tuple[httpx.Response | None, str | None]:
    """Fetch without credentials, to observe whether the provider demands them."""
    try:
        return await client.get(url, headers={"Accept": SDMX_STRUCTURE_XML}), None
    except Exception as exc:
        return None, (type(exc).__name__ + ": " + str(exc))[:300]


def _append_note(existing: str | None, addition: str) -> str:
    return existing + "; " + addition if existing else addition


_BASELINE_UNAVAILABLE = "ignored-detection unavailable: no references=none baseline this cycle"

# --- Unobservable-evidence policy -----------------------------------------
# A transport error (resp is None) or an HTTP 429 means the probe could not
# observe the provider's behaviour at all: a timeout, reset, or rate limit
# tells us nothing about whether the assumption still holds. With ~120
# unretried GETs per cycle, treating either as "broken" would yellow a status
# card until the next cycle (up to two hours later) over pure noise, and
# writing a fake "observed" value would make the changes panel flip-flop
# (e.g. "404 -> transport error -> 404"). Every check function below must
# therefore map both cases to verdict "skipped" with observed=None and
# spec_verdict="n/a" -- never "broken", never "ok". Real outages are already
# caught by the retried direct checks, and provider_down outranks contract
# breakage in derive_status, so nothing is lost by staying quiet here.
_RATE_LIMITED_NOTE = "provider rate-limited the probe (HTTP 429)"


def _skip_reason(resp: httpx.Response | None, error: str | None) -> str | None:
    """The reason this probe could not observe its target behaviour, or None
    if it did observe something and the caller should judge it normally."""
    if resp is None:
        return error
    if resp.status_code == 429:
        return _RATE_LIMITED_NOTE
    return None


async def check_references(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> list[ContractResult]:
    """Probe every `references` value the standard defines.

    The `none` response doubles as the baseline for detecting a parameter that
    is accepted and then silently ignored: if `references=X` returns exactly
    as many bytes as `references=none`, no referenced artefacts came back.
    Comparing live rather than against a pinned size keeps this honest as
    provider payloads grow.
    """
    results: list[ContractResult] = []
    baseline_size: int | None = None

    # `none` must be the first value probed: every later probe's
    # ignored-detection compares its payload size against `baseline_size`,
    # which only exists once `none` has been fetched. Building the probe order
    # explicitly (rather than relying on REFERENCE_PROBES already starting
    # with "none") keeps that guarantee independent of tuple order, while
    # still covering every value in REFERENCE_PROBES exactly once.
    probe_order = ("none", *(v for v in REFERENCE_PROBES if v != "none"))

    for value in probe_order:
        assertion = "references:" + value
        expected_ok = exp.references[value]
        url = structure_url(ep, exp, references=value, detail="allstubs")
        start = time.monotonic()
        resp, error = await _get(client, ep, url)
        latency = _ms(start)

        skip_reason = _skip_reason(resp, error)
        if skip_reason is not None:
            note = skip_reason
            if value != "none" and baseline_size is None:
                note = _append_note(note, _BASELINE_UNAVAILABLE)
            results.append(ContractResult(
                ep.key, assertion, verdict="skipped", spec_verdict="n/a",
                expected="200" if expected_ok else "non-200",
                observed=None, latency_ms=latency,
                http_status=(resp.status_code if resp is not None else None),
                error=note))
            continue

        observed = str(resp.status_code)
        spec_verdict, spec_note = classify_status(
            resp.status_code, legal_request=is_legal_reference(value))
        size = len(resp.content)
        if value == "none" and resp.status_code == 200:
            baseline_size = size

        if resp.status_code == 200 and not expected_ok:
            verdict, note = "capability_appeared", (
                "now accepted; the gateway still assumes it is rejected")
        elif resp.status_code != 200 and expected_ok:
            verdict, note = "broken", "expected 200"
        elif (resp.status_code == 200 and value != "none"
                and baseline_size is not None and size == baseline_size):
            verdict, note = "ignored", (
                "accepted but ignored: same payload size as references=none")
        else:
            verdict, note = "ok", None

        note = note or spec_note
        if value != "none" and baseline_size is None:
            note = _append_note(note, _BASELINE_UNAVAILABLE)

        results.append(ContractResult(
            ep.key, assertion, verdict=verdict, spec_verdict=spec_verdict,
            expected="200" if expected_ok else "non-200", observed=observed,
            latency_ms=latency, http_status=resp.status_code,
            error=note))
    return results


def availableconstraint_url(ep: Endpoint, exp: ContractExpectation) -> str:
    url = ep.base_url + "/availableconstraint/" + exp.flow_id + "/all/all/all"
    return url + ("?format=xml" if ep.key == "STATSNZ" else "")


def sdmx3_url(ep: Endpoint, exp: ContractExpectation) -> str:
    """The SDMx 3.0 structure path shape, per sdmx-3.0-rest.yaml."""
    url = (ep.base_url + "/structure/dataflow/" + exp.flow_agency + "/"
           + exp.flow_id + "/latest")
    return url + ("?format=xml" if ep.key == "STATSNZ" else "")


def missing_artefact_url(ep: Endpoint, exp: ContractExpectation) -> str:
    url = (ep.base_url + "/dataflow/" + exp.flow_agency
           + "/NONEXISTENT_XYZ_2026/latest")
    return url + ("?format=xml" if ep.key == "STATSNZ" else "")


def listing_url(ep: Endpoint) -> str:
    """The bulk dataflow listing the gateway's list_dataflows tool uses."""
    return ep.base_url + ep.metadata_path


def _constraint_key_values(text: str) -> tuple[int, str | None]:
    """Count KeyValue elements and read the constraint type, namespace-agnostically."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return 0, None
    count = 0
    ctype: str | None = None
    for element in root.iter():
        tag = element.tag.rsplit("}", 1)[-1]
        if tag == "KeyValue":
            count += 1
        elif tag == "ContentConstraint" and ctype is None:
            ctype = element.get("type")
    return count, ctype


async def check_constraint(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> list[ContractResult]:
    """Assert the constraint mechanism the gateway relies on still yields codes.

    Also record whether the constraint is Actual or Allowed: they share an
    element name but mean different things, and a silent switch to Allowed
    would make every availability answer over-optimistic.
    """
    url = availableconstraint_url(ep, exp)
    start = time.monotonic()
    resp, error = await _get(client, ep, url)
    latency = _ms(start)
    expected_status = exp.availableconstraint_status
    results: list[ContractResult] = []

    skip_reason = _skip_reason(resp, error)
    if skip_reason is not None:
        return [ContractResult(
            ep.key, "constraint:availableconstraint", verdict="skipped",
            spec_verdict="n/a", expected=str(expected_status), observed=None,
            latency_ms=latency,
            http_status=(resp.status_code if resp is not None else None),
            error=skip_reason)]

    observed = str(resp.status_code)
    spec_verdict, spec_note = classify_status(resp.status_code, legal_request=True)
    count, ctype = (_constraint_key_values(resp.text)
                    if resp.status_code == 200 else (0, None))

    if resp.status_code == 200 and expected_status != 200:
        verdict, note = "capability_appeared", (
            "now supported; the gateway assumes HTTP " + str(expected_status))
    elif resp.status_code == 200 and count == 0:
        verdict, note = "broken", "returned 200 but no KeyValue entries"
    elif resp.status_code == 200:
        verdict, note = "ok", None
    elif resp.status_code == expected_status:
        verdict, note = "ok", "unsupported, as the gateway already assumes"
    else:
        verdict, note = "broken", "expected HTTP " + str(expected_status)

    results.append(ContractResult(
        ep.key, "constraint:availableconstraint", verdict=verdict,
        spec_verdict=spec_verdict, expected=str(expected_status),
        observed=observed, latency_ms=latency, http_status=resp.status_code,
        error=note or spec_note))

    # constraint:type judges the mechanism this expectation's pinned type
    # describes. ECB's pinned type ("Allowed") describes what its configured
    # single-flow strategy (?references=contentconstraint) yields, not what
    # /availableconstraint/ returns; gating on availableconstraint_status==200
    # keeps this probe from judging a mechanism its pinned type never spoke to
    # (e.g. ECB gaining /availableconstraint/ support, which is informational).
    if (exp.availableconstraint_status == 200
            and exp.constraint_type is not None and ctype is not None):
        matches = ctype == exp.constraint_type
        results.append(ContractResult(
            ep.key, "constraint:type", verdict="ok" if matches else "broken",
            expected=exp.constraint_type, observed=ctype,
            error=None if matches else (
                "constraint semantics changed from " + exp.constraint_type
                + " to " + ctype)))
    return results


async def check_dialect(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> ContractResult:
    """Detect a provider beginning to answer SDMx 3.0 structure paths."""
    start = time.monotonic()
    resp, error = await _get(client, ep, sdmx3_url(ep, exp))
    latency = _ms(start)
    skip_reason = _skip_reason(resp, error)
    if skip_reason is not None:
        return ContractResult(ep.key, "dialect:sdmx3", verdict="skipped",
                              spec_verdict="n/a", expected=str(exp.sdmx3_status),
                              observed=None, latency_ms=latency,
                              http_status=(resp.status_code if resp is not None else None),
                              error=skip_reason)
    if resp.status_code == 200:
        return ContractResult(
            ep.key, "dialect:sdmx3", verdict="capability_appeared",
            expected=str(exp.sdmx3_status), observed="200", latency_ms=latency,
            http_status=200,
            error="now answers SDMx 3.0 structure queries; the gateway speaks 2.1")
    return ContractResult(ep.key, "dialect:sdmx3", verdict="ok",
                          expected=str(exp.sdmx3_status),
                          observed=str(resp.status_code), latency_ms=latency,
                          http_status=resp.status_code)


async def check_error_semantics(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> ContractResult:
    """A missing artefact must keep answering what the gateway expects.

    The standard documents 404. IMF answers 204, which is not a documented
    response code at all and leaves a client unable to distinguish a missing
    artefact from an empty one.
    """
    start = time.monotonic()
    resp, error = await _get(client, ep, missing_artefact_url(ep, exp))
    latency = _ms(start)
    skip_reason = _skip_reason(resp, error)
    if skip_reason is not None:
        return ContractResult(
            ep.key, "errors:missing_artefact", verdict="skipped", spec_verdict="n/a",
            expected=str(exp.missing_artefact_status), observed=None,
            latency_ms=latency,
            http_status=(resp.status_code if resp is not None else None),
            error=skip_reason)
    observed = resp.status_code
    # This probe deliberately asks for an artefact that does not exist, so a
    # rejection is the conforming outcome (legal_request=False) rather than a
    # deviation; the undocumented-status check inside classify_status still
    # runs first, so an undocumented code like IMF's 204 is correctly flagged
    # as a deviation regardless.
    spec_verdict, spec_note = classify_status(observed, legal_request=False)
    matches = observed == exp.missing_artefact_status
    return ContractResult(
        ep.key, "errors:missing_artefact", verdict="ok" if matches else "broken",
        spec_verdict=spec_verdict, expected=str(exp.missing_artefact_status),
        observed=str(observed), latency_ms=latency, http_status=observed,
        error=spec_note if matches else (
            "error semantics changed from HTTP "
            + str(exp.missing_artefact_status)))


async def check_auth(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> ContractResult:
    """Detect a provider newly demanding credentials, or dropping the demand.

    Credentials are deliberately withheld here (via `_get_unauthenticated`,
    not `_get`): the question this probe answers is "does this endpoint
    demand credentials?", which a request that already carries them cannot
    answer -- it would report 200 whether or not auth is actually required.
    """
    start = time.monotonic()
    resp, error = await _get_unauthenticated(client, listing_url(ep))
    latency = _ms(start)
    expected = "credentials required" if exp.auth_required_for_listing else "open"
    skip_reason = _skip_reason(resp, error)
    if skip_reason is not None:
        return ContractResult(ep.key, "auth:listing", verdict="skipped",
                              spec_verdict="n/a", expected=expected, observed=None,
                              latency_ms=latency,
                              http_status=(resp.status_code if resp is not None else None),
                              error=skip_reason)
    denied = resp.status_code in (401, 403)
    observed = str(resp.status_code)
    if denied and not exp.auth_required_for_listing:
        verdict, note = "broken", "provider now demands credentials"
    elif not denied and exp.auth_required_for_listing:
        if resp.status_code == 200:
            verdict, note = "capability_appeared", (
                "listing succeeded without credentials; the gateway still sends a key")
        else:
            # Neither denied nor confirmed successful: inconclusive, not a
            # capability claim (Fix 4 -- a 503 or similar is not a success).
            verdict, note = "skipped", (
                "listing neither denied nor confirmed successful (HTTP "
                + observed + ")")
    else:
        verdict, note = "ok", None
    return ContractResult(ep.key, "auth:listing", verdict=verdict,
                          expected=expected, observed=observed,
                          latency_ms=latency, http_status=resp.status_code,
                          error=note)


async def check_encoding(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> ContractResult:
    """Structural metadata must still arrive as XML, which the client parses."""
    start = time.monotonic()
    resp, error = await _get(client, ep, structure_url(ep, exp, detail="allstubs"))
    latency = _ms(start)
    skip_reason = _skip_reason(resp, error)
    if skip_reason is not None:
        return ContractResult(ep.key, "encoding:structure_xml", verdict="skipped",
                              spec_verdict="n/a", expected="xml", observed=None,
                              latency_ms=latency,
                              http_status=(resp.status_code if resp is not None else None),
                              error=skip_reason)
    if resp.status_code != 200:
        # Availability is already owned by the retried direct metadata check;
        # a non-200 here (e.g. a transient 503 maintenance page) is not
        # evidence about the encoding of structural metadata (Fix 3).
        return ContractResult(
            ep.key, "encoding:structure_xml", verdict="skipped", expected="xml",
            observed=str(resp.status_code), latency_ms=latency,
            http_status=resp.status_code,
            error="HTTP " + str(resp.status_code) + "; encoding not judged")
    content_type = resp.headers.get("content-type", "")
    is_xml = "xml" in content_type.lower()
    return ContractResult(
        ep.key, "encoding:structure_xml", verdict="ok" if is_xml else "broken",
        expected="xml", observed=content_type or "none", latency_ms=latency,
        http_status=resp.status_code,
        error=None if is_xml else "structural metadata is no longer XML")


async def run_contracts(
    client: httpx.AsyncClient, ep: Endpoint, exp: ContractExpectation
) -> list[ContractResult]:
    results = await check_references(client, ep, exp)
    results.extend(await check_constraint(client, ep, exp))
    results.append(await check_dialect(client, ep, exp))
    results.append(await check_error_semantics(client, ep, exp))
    results.append(await check_auth(client, ep, exp))
    results.append(await check_encoding(client, ep, exp))
    return results
