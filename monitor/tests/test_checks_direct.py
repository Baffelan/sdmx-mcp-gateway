import dataclasses

import httpx
import pytest
import respx

import checks_common
from checks_direct import (
    looks_like_xml_data,
    parse_csv_observations,
    run_direct_checks,
    verify_json_payload,
)
from endpoints_config import ENDPOINTS


@pytest.fixture(autouse=True)
def _no_retry_delay(monkeypatch):
    monkeypatch.setattr(checks_common, "RETRY_DELAY_S", 0.0)


def _ep(key: str):
    (ep,) = [e for e in ENDPOINTS if e.key == key]
    return ep


CSV_BODY = "DATAFLOW,FREQ,TIME_PERIOD,OBS_VALUE\nSPC:DF_ADBKI(1.0),A,2020,1.5\n"
XML_META = '<Structure><Dataflow id="DF_ADBKI"/></Structure>'

GOOD_CSV = (
    "DATAFLOW,FREQ,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n"
    "SPC:DF_ADBKI(1.0),A,2000,,\n"  # legitimately empty value, must not fail the check
    "SPC:DF_ADBKI(1.0),A,2001,1345.2,A\n"
)


def test_parse_csv_observations_accepts_first_numeric_after_blanks():
    count, value, period, error = parse_csv_observations(GOOD_CSV)
    assert error is None
    assert count == 1
    assert value == "1345.2"
    assert period == "2001"


def test_parse_csv_observations_rejects_missing_obs_value_column():
    _c, _v, _p, error = parse_csv_observations("SOME,OTHER,COLUMNS\n1,2,3\n")
    assert error is not None and "OBS_VALUE" in error


def test_parse_csv_observations_rejects_header_only():
    _c, _v, _p, error = parse_csv_observations("DATAFLOW,TIME_PERIOD,OBS_VALUE\n")
    assert error is not None and "no observation rows" in error


def test_parse_csv_observations_rejects_all_empty_or_non_numeric_values():
    body = "DATAFLOW,TIME_PERIOD,OBS_VALUE\nX,2020,\nX,2021,n/a\n"
    _c, _v, _p, error = parse_csv_observations(body)
    assert error is not None and "numeric" in error


def test_parse_csv_observations_rejects_non_csv_text():
    _c, _v, _p, error = parse_csv_observations("Service temporarily unavailable\nplease retry\n")
    assert error is not None and "OBS_VALUE" in error


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
        meta, data, _json = await run_direct_checks(client, ep)
    assert (meta.path, meta.kind, meta.ok) == ("direct", "metadata", True)
    assert meta.http_status == 200 and meta.latency_ms is not None
    assert (data.kind, data.ok, data.obs_count) == ("data", True, 1)


@respx.mock
async def test_http_500_fails_and_retries_once():
    ep = _ep("SPC")
    meta_route = respx.get(ep.metadata_url).respond(500, text="err")
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, _data, _json = await run_direct_checks(client, ep)
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
        meta, _data, _json = await run_direct_checks(client, ep)
    assert meta.ok is True
    assert meta.attempts == 2


@respx.mock
async def test_empty_csv_data_fails():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text="DATAFLOW,TIME_PERIOD,OBS_VALUE\n")
    async with httpx.AsyncClient() as client:
        _meta, data, _json = await run_direct_checks(client, ep)
    assert data.ok is False
    assert "no observation rows" in (data.error or "")


@respx.mock
async def test_connection_error_is_captured_not_raised():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).mock(side_effect=httpx.ConnectError("boom"))
    respx.get(ep.data_url).respond(200, text=CSV_BODY)
    async with httpx.AsyncClient() as client:
        meta, _data, _json = await run_direct_checks(client, ep)
    assert meta.ok is False
    assert "boom" in (meta.error or "")


async def test_statsnz_without_key_skips_all_checks(monkeypatch):
    monkeypatch.delenv("SDMX_STATSNZ_KEY", raising=False)
    ep = _ep("STATSNZ")
    async with httpx.AsyncClient() as client:
        meta, data, json_result = await run_direct_checks(client, ep)
    assert meta.skipped is True and data.skipped is True and json_result.skipped is True
    assert "SDMX_STATSNZ_KEY" in (meta.error or "")


@respx.mock
async def test_no_pinned_data_query_skips_data_only(monkeypatch):
    monkeypatch.setenv("SDMX_STATSNZ_KEY", "k")
    # STATSNZ itself now has a pinned data query; use a synthetic copy without
    # one to keep exercising the "no pinned data query" skip branch.
    ep = dataclasses.replace(_ep("STATSNZ"), data_path=None)
    route = respx.get(ep.metadata_url).respond(200, text=XML_META)
    async with httpx.AsyncClient() as client:
        meta, data, _json = await run_direct_checks(client, ep)
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
        meta, data, _json = await run_direct_checks(client, ep)
    assert meta.ok is False
    assert "RuntimeError" in (meta.error or "")
    assert data.ok is True  # contract held: all results returned


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
        _meta, data, _json = await run_direct_checks(client, ep)
    assert data.ok is True
    assert data.obs_count is None


@respx.mock
async def test_data_check_records_sample_value():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text=GOOD_CSV, headers={"content-type": "text/csv"})
    async with httpx.AsyncClient() as client:
        _meta, data, _json = await run_direct_checks(client, ep)
    assert data.ok is True
    assert data.obs_count == 1
    assert data.sample_value == "1345.2"
    assert data.sample_period == "2001"


@respx.mock
async def test_data_check_fails_on_200_with_plain_text_body():
    """A 200 carrying a maintenance notice must not count as data."""
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text="Scheduled maintenance\nback at 09:00\n")
    async with httpx.AsyncClient() as client:
        _meta, data, _json = await run_direct_checks(client, ep)
    assert data.ok is False
    assert "OBS_VALUE" in (data.error or "")


SDMX_JSON_2 = '{"meta":{"schema":"https://example/2.0.0/sdmx-json-data-schema.json"},"dataSets":[{"action":"Information"}]}'


def test_verify_json_payload_accepts_matching_version():
    assert verify_json_payload(
        SDMX_JSON_2,
        "application/vnd.sdmx.data+json; version=2.0.0; charset=utf-8",
        "application/vnd.sdmx.data+json;version=2.0.0",
    ) is None


def test_verify_json_payload_rejects_xml_body():
    """IMF answers a JSON request with HTTP 200 and XML; that must fail."""
    error = verify_json_payload(
        "<?xml version='1.0'?><message:StructureSpecificData/>",
        "application/xml",
        "application/vnd.sdmx.data+json;version=1.0.0",
    )
    assert error is not None and "not JSON" in error


def test_verify_json_payload_rejects_csv_body():
    """STATSNZ answers a JSON request with HTTP 200 and CSV."""
    error = verify_json_payload(
        "DATAFLOW,TIME_PERIOD,OBS_VALUE\nX,2020,1\n",
        "application/vnd.sdmx.data+csv; charset=utf-8",
        "application/vnd.sdmx.data+json;version=2.0.0",
    )
    assert error is not None and "not JSON" in error


def test_verify_json_payload_rejects_version_mismatch():
    error = verify_json_payload(
        SDMX_JSON_2,
        "application/vnd.sdmx.data+json; version=1.0.0",
        "application/vnd.sdmx.data+json;version=2.0.0",
    )
    assert error is not None and "version" in error


def test_verify_json_payload_rejects_missing_datasets():
    error = verify_json_payload(
        '{"meta":{"id":"x"}}',
        "application/vnd.sdmx.data+json; version=2.0.0",
        "application/vnd.sdmx.data+json;version=2.0.0",
    )
    assert error is not None and "dataSets" in error


def test_verify_json_payload_accepts_untyped_json_when_shape_is_right():
    """UNICEF and BIS return content-type application/json with no version."""
    assert verify_json_payload(
        '{"meta":{"id":"x"},"dataSets":[{"action":"Information"}]}',
        "application/json;charset=UTF-8",
        "application/vnd.sdmx.data+json;version=1.0.0",
    ) is None


SDMX_JSON_2_NESTED = (
    '{"meta":{"schema":"https://example/2.0.0/x.json"},'
    '"data":{"dataSets":[{"action":"Information"}],"structures":[]},"errors":[]}'
)


def test_verify_json_payload_accepts_datasets_nested_under_data():
    """SDMx-JSON 2.0.0 and most 1.0.0 responses nest dataSets under `data`."""
    assert verify_json_payload(
        SDMX_JSON_2_NESTED,
        "application/vnd.sdmx.data+json; version=2.0.0",
        "application/vnd.sdmx.data+json;version=2.0.0",
    ) is None


def test_verify_json_payload_rejects_empty_nested_datasets():
    error = verify_json_payload(
        '{"meta":{},"data":{"dataSets":[]}}',
        "application/vnd.sdmx.data+json; version=2.0.0",
        "application/vnd.sdmx.data+json;version=2.0.0",
    )
    assert error is not None and "dataSets" in error


@respx.mock
async def test_run_direct_checks_returns_three_results_with_json_ok():
    ep = _ep("SPC")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    route = respx.get(ep.data_url)
    route.side_effect = [
        httpx.Response(200, text=CSV_BODY, headers={"content-type": "text/csv"}),
        httpx.Response(200, text=SDMX_JSON_2,
                       headers={"content-type": "application/vnd.sdmx.data+json; version=2.0.0"}),
    ]
    async with httpx.AsyncClient() as client:
        results = await run_direct_checks(client, ep)
    assert [r.kind for r in results] == ["metadata", "data", "json"]
    assert results[2].ok is True
    assert route.calls[1].request.headers["accept"] == ep.json_accept


@respx.mock
async def test_json_check_skipped_when_unsupported():
    ep = _ep("IMF")
    respx.get(ep.metadata_url).respond(200, text=XML_META)
    respx.get(ep.data_url).respond(200, text=CSV_BODY, headers={"content-type": "text/csv"})
    async with httpx.AsyncClient() as client:
        results = await run_direct_checks(client, ep)
    json_result = results[2]
    assert json_result.skipped is True
    assert "does not serve" in (json_result.error or "")
