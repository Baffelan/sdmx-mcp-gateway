import httpx
import respx

from contracts import check_references, structure_url
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


def exp_values(key: str) -> tuple[str, ...]:
    from contracts_config import REFERENCE_PROBES
    return REFERENCE_PROBES


def _by_assertion(results, assertion):
    (match,) = [r for r in results if r.assertion == assertion]
    return match
