import httpx
import respx

from contracts import (
    availableconstraint_url,
    check_auth,
    check_constraint,
    check_dialect,
    check_encoding,
    check_error_semantics,
    check_references,
    listing_url,
    missing_artefact_url,
    sdmx3_url,
    structure_url,
)
from contracts_config import EXPECTATIONS
from endpoints_config import ENDPOINTS

BASELINE = "<Structure><Dataflow id='DF'/></Structure>"
RICHER = BASELINE + "<Codelist id='CL_GEO'/>" * 20


def _ep(key: str):
    (ep,) = [e for e in ENDPOINTS if e.key == key]
    return ep


def _mock_references(key: str, bodies: dict[str, tuple[int, str]]):
    ep, exp = _ep(key), EXPECTATIONS[key]
    for value, (status, body) in bodies.items():
        url = structure_url(ep, exp, references=value, detail="allstubs")
        respx.get(url).respond(status, text=body)
    return ep, exp


def test_structure_url_builds_the_expected_path_and_query():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    url = structure_url(ep, exp, references="none", detail="allstubs")
    assert url == (
        "https://stats-sdmx-disseminate.pacificdata.org/rest/dataflow/SPC/DF_ADBKI/latest"
        "?references=none&detail=allstubs"
    )


def test_structure_url_forces_format_xml_for_statsnz():
    """Stats NZ's APIM gateway ignores the Accept header for structural
    metadata, so the gateway forces format=xml for this provider only."""
    ep, exp = _ep("STATSNZ"), EXPECTATIONS["STATSNZ"]
    url = structure_url(ep, exp, references="none", detail="allstubs")
    assert url.startswith(
        "https://api.data.stats.govt.nz/rest/dataflow/STATSNZ/AGR_AGR_001/latest?"
    )
    assert "format=xml" in url
    other = structure_url(_ep("SPC"), EXPECTATIONS["SPC"], references="none", detail="allstubs")
    assert "format=xml" not in other


@respx.mock
async def test_declared_references_passing_are_ok():
    ep, exp = _mock_references("SPC", {
        v: (200, RICHER if v != "none" else BASELINE) for v in exp_values("SPC")})
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    assert {r.assertion for r in results} == {
        "references:" + v for v in exp_values("SPC")}
    assert all(r.verdict == "ok" for r in results)


@respx.mock
async def test_declared_reference_that_stops_working_is_broken():
    values = exp_values("SPC")
    bodies = {v: (200, RICHER if v != "none" else BASELINE) for v in values}
    bodies["parents"] = (400, "nope")
    ep, exp = _mock_references("SPC", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    parents = _by_assertion(results, "references:parents")
    assert parents.verdict == "broken"
    assert parents.expected == "200"
    assert parents.observed == "400"
    # the standard defines `parents`, so rejecting it also deviates
    assert parents.spec_verdict == "deviates"


@respx.mock
async def test_undeclared_reference_that_starts_working_is_a_capability():
    """ESTAT rejects `all` today; a 200 means the gateway could drop a workaround."""
    values = exp_values("ESTAT")
    bodies = {v: (400, "nope") for v in values}
    bodies["none"] = (200, BASELINE)
    bodies["children"] = (200, RICHER)
    bodies["descendants"] = (200, RICHER)
    bodies["all"] = (200, RICHER)  # was 400 at baseline
    ep, exp = _mock_references("ESTAT", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    result = _by_assertion(results, "references:all")
    assert result.verdict == "capability_appeared"
    assert result.observed == "200"


@respx.mock
async def test_reference_accepted_but_ignored_is_detected():
    """IMF, ILO and BIS answer 200 for contentconstraint with a payload
    identical to references=none: accepted and silently ignored."""
    values = exp_values("BIS")
    bodies = {v: (200, RICHER) for v in values}
    bodies["none"] = (200, BASELINE)
    bodies["contentconstraint"] = (200, BASELINE)  # same as the none baseline
    ep, exp = _mock_references("BIS", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    result = _by_assertion(results, "references:contentconstraint")
    assert result.verdict == "ignored"
    assert "ignored" in (result.error or "")


@respx.mock
async def test_none_itself_is_never_flagged_as_ignored():
    values = exp_values("SPC")
    bodies = {v: (200, BASELINE) for v in values}
    ep, exp = _mock_references("SPC", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    assert _by_assertion(results, "references:none").verdict == "ok"


@respx.mock
async def test_transport_error_is_captured_not_raised():
    values = exp_values("SPC")
    bodies = {v: (200, RICHER) for v in values}
    ep, exp = _mock_references("SPC", bodies)
    respx.get(structure_url(_ep("SPC"), EXPECTATIONS["SPC"],
                            references="parents", detail="allstubs")).mock(
        side_effect=httpx.ConnectError("boom"))
    async with httpx.AsyncClient() as client:
        results = await check_references(client, _ep("SPC"), EXPECTATIONS["SPC"])
    parents = _by_assertion(results, "references:parents")
    assert parents.verdict == "broken"
    assert "boom" in (parents.error or "")


@respx.mock
async def test_expected_rejection_of_a_spec_legal_value_is_ok_but_deviates():
    """ESTAT rejects `parents`, which the standard defines. The gateway already
    assumes that, so it is not broken, but it is still a deviation."""
    values = exp_values("ESTAT")
    bodies = {v: (400, "nope") for v in values}
    bodies["none"] = (200, BASELINE)
    bodies["children"] = (200, RICHER)
    bodies["descendants"] = (200, RICHER)
    ep, exp = _mock_references("ESTAT", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    parents = _by_assertion(results, "references:parents")
    assert parents.verdict == "ok"
    assert parents.spec_verdict == "deviates"


@respx.mock
async def test_missing_baseline_is_reported_on_later_probes():
    values = exp_values("SPC")
    bodies = {v: (200, RICHER) for v in values}
    bodies["none"] = (500, "server error")
    ep, exp = _mock_references("SPC", bodies)
    async with httpx.AsyncClient() as client:
        results = await check_references(client, ep, exp)
    later = _by_assertion(results, "references:children")
    assert "ignored-detection unavailable" in (later.error or "")


@respx.mock
async def test_transport_error_on_an_undeclared_value_is_not_a_capability():
    values = exp_values("ESTAT")
    bodies = {v: (200, RICHER) for v in values}
    bodies["none"] = (200, BASELINE)
    ep, exp = _mock_references("ESTAT", bodies)
    respx.get(structure_url(_ep("ESTAT"), EXPECTATIONS["ESTAT"],
                            references="all", detail="allstubs")).mock(
        side_effect=httpx.ConnectError("boom"))
    async with httpx.AsyncClient() as client:
        results = await check_references(client, _ep("ESTAT"), EXPECTATIONS["ESTAT"])
    result = _by_assertion(results, "references:all")
    assert result.verdict == "ok"          # a failure is not a new capability
    assert "boom" in (result.error or "")


def exp_values(key: str) -> tuple[str, ...]:
    from contracts_config import REFERENCE_PROBES
    return REFERENCE_PROBES


def _by_assertion(results, assertion):
    (match,) = [r for r in results if r.assertion == assertion]
    return match


CONSTRAINT_ACTUAL = (
    "<Structure><ContentConstraint type='Actual'>"
    "<CubeRegion><KeyValue id='GEO_PICT'><Value>FJ</Value></KeyValue></CubeRegion>"
    "</ContentConstraint></Structure>"
)
CONSTRAINT_ALLOWED = CONSTRAINT_ACTUAL.replace("Actual", "Allowed")
CONSTRAINT_EMPTY = "<Structure><ContentConstraint type='Actual'/></Structure>"


@respx.mock
async def test_constraint_ok_when_the_configured_mechanism_works():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(availableconstraint_url(ep, exp)).respond(200, text=CONSTRAINT_ACTUAL)
    async with httpx.AsyncClient() as client:
        results = await check_constraint(client, ep, exp)
    status = _by_assertion(results, "constraint:availableconstraint")
    assert status.verdict == "ok"
    assert _by_assertion(results, "constraint:type").observed == "Actual"


@respx.mock
async def test_constraint_broken_when_it_returns_no_key_values():
    """A 200 carrying an empty constraint is not usable data."""
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(availableconstraint_url(ep, exp)).respond(200, text=CONSTRAINT_EMPTY)
    async with httpx.AsyncClient() as client:
        results = await check_constraint(client, ep, exp)
    assert _by_assertion(results, "constraint:availableconstraint").verdict == "broken"


@respx.mock
async def test_constraint_type_change_is_reported():
    """A silent switch from Actual to Allowed would make every availability
    answer over-optimistic without any error surfacing."""
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(availableconstraint_url(ep, exp)).respond(200, text=CONSTRAINT_ALLOWED)
    async with httpx.AsyncClient() as client:
        results = await check_constraint(client, ep, exp)
    type_result = _by_assertion(results, "constraint:type")
    assert type_result.verdict == "broken"
    assert type_result.expected == "Actual"
    assert type_result.observed == "Allowed"


@respx.mock
async def test_constraint_capability_appears_for_an_unsupported_provider():
    """ILO's /availableconstraint/ returns 500 today; a working one would let
    the gateway drop its references=all workaround."""
    ep, exp = _ep("ILO"), EXPECTATIONS["ILO"]
    respx.get(availableconstraint_url(ep, exp)).respond(200, text=CONSTRAINT_ACTUAL)
    async with httpx.AsyncClient() as client:
        results = await check_constraint(client, ep, exp)
    assert _by_assertion(
        results, "constraint:availableconstraint").verdict == "capability_appeared"


@respx.mock
async def test_dialect_reports_sdmx3_appearing():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(sdmx3_url(ep, exp)).respond(200, text="<Structures/>")
    async with httpx.AsyncClient() as client:
        result = await check_dialect(client, ep, exp)
    assert result.verdict == "capability_appeared"
    assert "3.0" in (result.error or "")


@respx.mock
async def test_dialect_ok_when_still_refused():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(sdmx3_url(ep, exp)).respond(404, text="no")
    async with httpx.AsyncClient() as client:
        result = await check_dialect(client, ep, exp)
    assert result.verdict == "ok"


@respx.mock
async def test_error_semantics_match_baseline_and_flag_spec_deviation():
    ep, exp = _ep("IMF"), EXPECTATIONS["IMF"]
    respx.get(missing_artefact_url(ep, exp)).respond(204)
    async with httpx.AsyncClient() as client:
        result = await check_error_semantics(client, ep, exp)
    assert result.verdict == "ok"          # matches IMF's recorded baseline
    assert result.spec_verdict == "deviates"  # 204 is undocumented in the standard


@respx.mock
async def test_error_semantics_change_is_broken():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(missing_artefact_url(ep, exp)).respond(200, text="<Structure/>")
    async with httpx.AsyncClient() as client:
        result = await check_error_semantics(client, ep, exp)
    assert result.verdict == "broken"
    assert result.observed == "200"


@respx.mock
async def test_auth_broken_when_a_provider_starts_demanding_a_key():
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(listing_url(ep)).respond(401, text="denied")
    async with httpx.AsyncClient() as client:
        result = await check_auth(client, ep, exp)
    assert result.verdict == "broken"
    assert result.observed == "401"


@respx.mock
async def test_encoding_broken_when_structure_comes_back_as_json():
    """Stats NZ has historically served SDMx-JSON for structural metadata
    unless format=xml is forced."""
    ep, exp = _ep("SPC"), EXPECTATIONS["SPC"]
    respx.get(structure_url(ep, exp, detail="allstubs")).respond(
        200, text='{"data":{}}', headers={"content-type": "application/json"})
    async with httpx.AsyncClient() as client:
        result = await check_encoding(client, ep, exp)
    assert result.verdict == "broken"
    assert "json" in (result.observed or "").lower()
