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


def _append_note(existing: str | None, addition: str) -> str:
    return existing + "; " + addition if existing else addition


_BASELINE_UNAVAILABLE = "ignored-detection unavailable: no references=none baseline this cycle"


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

        if resp is None:
            note = error
            if value != "none" and baseline_size is None:
                note = _append_note(note, _BASELINE_UNAVAILABLE)
            results.append(ContractResult(
                ep.key, assertion, verdict="broken" if expected_ok else "ok",
                expected="200" if expected_ok else "non-200",
                observed="transport error", latency_ms=latency, error=note))
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
