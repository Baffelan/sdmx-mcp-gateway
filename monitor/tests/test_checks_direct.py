import httpx
import pytest
import respx

import checks_common
from checks_direct import count_csv_observations, looks_like_xml_data, run_direct_checks
from endpoints_config import ENDPOINTS


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    monkeypatch.setattr(checks_common, "RETRY_DELAY_S", 0.0)


def _ep(key: str):
    (ep,) = [e for e in ENDPOINTS if e.key == key]
    return ep


CSV_BODY = "DATAFLOW,FREQ,TIME_PERIOD,OBS_VALUE\nSPC:DF_ADBKI(1.0),A,2020,1.5\n"
XML_META = '<Structure><Dataflow id="DF_ADBKI"/></Structure>'


def test_count_csv_observations():
    assert count_csv_observations(CSV_BODY) == 1
    assert count_csv_observations("HEADER\n") == 0
    assert count_csv_observations("") == 0


def test_looks_like_xml_data():
    assert looks_like_xml_data("<gen:Obs ...>") is True
    assert looks_like_xml_data("<Obs value='1'/>") is True
    assert looks_like_xml_data("<Structure/>") is False


@respx.mock
async def test_happy_path_metadata_and_data():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text=CSV_BODY, headers={"content-type": "text/csv"})
    async with httpx.AsyncClient() as client:
        meta, data = await run_direct_checks(client, ep)
    assert (meta.path, meta.kind, meta.ok) == ("direct", "metadata", True)
    assert meta.http_status == 200 and meta.latency_ms is not None
    assert (data.kind, data.ok, data.obs_count) == ("data", True, 1)


@respx.mock
async def test_http_500_fails_and_retries_once():
    ep = _ep("SPC")
    meta_route = respx.get(ep.metadata_url).respond(500, text="err")
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, _data = await run_direct_checks(client, ep)
    assert meta.ok is False
    assert meta.attempts == 2
    assert meta.http_status == 500
    assert meta_route.call_count == 2


@respx.mock
async def test_retry_recovers_on_second_attempt():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).mock(
        side_effect=[httpx.Response(500), httpx.Response(200, text=XML_META)]
    )
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, _data = await run_direct_checks(client, ep)
    assert meta.ok is True
    assert meta.attempts == 2


@respx.mock
async def test_empty_csv_data_fails():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text="ONLY,A,HEADER\n")
    async with httpx.AsyncClient() as client:
        _meta, data = await run_direct_checks(client, ep)
    assert data.ok is False
    assert "no observations" in (data.error or "")


@respx.mock
async def test_connection_error_is_captured_not_raised():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).mock(side_effect=httpx.ConnectError("boom"))
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, _data = await run_direct_checks(client, ep)
    assert meta.ok is False
    assert "boom" in (meta.error or "")


async def test_statsnz_without_key_skips_both(monkeypatch):
    monkeypatch.delenv("SDMX_STATSNZ_KEY", raising=False)
    ep = _ep("STATSNZ")
    async with httpx.AsyncClient() as client:
        meta, data = await run_direct_checks(client, ep)
    assert meta.skipped is True and data.skipped is True
    assert "SDMX_STATSNZ_KEY" in (meta.error or "")


@respx.mock
async def test_no_pinned_data_query_skips_data_only(monkeypatch):
    monkeypatch.setenv("SDMX_STATSNZ_KEY", "k")
    ep = _ep("STATSNZ")
    route = respx.get(ep.metadata_url).respond(200, text=XML_META)
    async with httpx.AsyncClient() as client:
        meta, data = await run_direct_checks(client, ep)
    assert meta.ok is True
    assert route.calls[0].request.headers["Ocp-Apim-Subscription-Key"] == "k"
    assert data.skipped is True
    assert "no pinned data query" in (data.error or "")


@respx.mock
async def test_non_httpx_exception_is_captured_not_raised():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).mock(side_effect=RuntimeError("surprise"))
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, data = await run_direct_checks(client, ep)
    assert meta.ok is False
    assert "RuntimeError" in (meta.error or "")
    assert data.ok is True  # contract held: both results returned


@respx.mock
async def test_xml_data_response_counts_as_observations():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(
        200,
        text="<GenericData><gen:Obs value='1'/></GenericData>",
        headers={"content-type": "application/xml"},
    )
    async with httpx.AsyncClient() as client:
        _meta, data = await run_direct_checks(client, ep)
    assert data.ok is True
    assert data.obs_count is None
