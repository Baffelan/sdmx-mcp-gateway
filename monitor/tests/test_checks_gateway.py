"""Gateway checks are tested against a fake session object; the real
GatewaySession is exercised by scripts/live_smoke.py, not in CI."""

import pytest

from checks_gateway import (
    GatewayError,
    gateway_data_check,
    gateway_drift,
    gateway_metadata_check,
)
from endpoints_config import ENDPOINTS


class FakeGateway:
    def __init__(self, responses: dict):
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, name: str, args: dict) -> dict:
        self.calls.append((name, args))
        value = self.responses[name]
        if isinstance(value, Exception):
            raise value
        return value


def _ep(key: str):
    (ep,) = [e for e in ENDPOINTS if e.key == key]
    return ep


async def test_metadata_ok_when_dataflows_found():
    gw = FakeGateway({"list_dataflows": {"total_found": 25, "dataflows": [{"id": "X"}]}})
    result = await gateway_metadata_check(gw, _ep("SPC"))
    assert result.ok is True
    assert (result.path, result.kind) == ("gateway", "metadata")
    assert gw.calls == [("list_dataflows", {"limit": 1, "endpoint": "SPC", "fresh": True})]


async def test_metadata_check_bypasses_the_dataflow_cache():
    """This call is a liveness check: it must prove the gateway can reach
    the provider right now. Served from cache it would report a provider
    healthy while that provider is unreachable, for as long as the cache
    TTL lasts."""
    gw = FakeGateway({"list_dataflows": {"total_found": 1, "dataflows": [{"id": "X"}]}})
    await gateway_metadata_check(gw, _ep("SPC"))
    name, args = gw.calls[0]
    assert name == "list_dataflows"
    assert args["fresh"] is True


async def test_metadata_fails_on_zero_dataflows_with_tool_error_text():
    gw = FakeGateway({"list_dataflows": {
        "total_found": 0, "dataflows": [], "next_step": "Error: 401 Unauthorized"}})
    result = await gateway_metadata_check(gw, _ep("SPC"))
    assert result.ok is False
    assert "401" in (result.error or "")


async def test_metadata_captures_exceptions():
    gw = FakeGateway({"list_dataflows": GatewayError("tool blew up")})
    result = await gateway_metadata_check(gw, _ep("SPC"))
    assert result.ok is False
    assert "tool blew up" in (result.error or "")


async def test_data_ok_on_nonempty_probe():
    gw = FakeGateway({"probe_data_url": {"status": "nonempty", "observation_count": 42}})
    ep = _ep("SPC")
    result = await gateway_data_check(gw, ep)
    assert result.ok is True
    assert result.obs_count == 42
    name, args = gw.calls[0]
    assert name == "probe_data_url"
    assert args["data_url"] == ep.data_url
    assert args["timeout_ms"] == 30000
    assert args["sample_observations_limit"] == 50
    assert args["endpoint"] == ep.key


async def test_data_fails_on_empty_or_error_probe():
    for status in ("empty", "error"):
        gw = FakeGateway({"probe_data_url": {
            "status": status, "observation_count": 0, "notes": ["HTTP 406 from provider."]}})
        result = await gateway_data_check(gw, _ep("ECB"))
        assert result.ok is False
        assert status in (result.error or "")
        assert result.obs_count == 0


async def test_drift_reports_both_directions():
    gw = FakeGateway({"list_available_endpoints": {
        "endpoints": [{"key": "SPC"}, {"key": "ECB"}, {"key": "NEWONE"}]}})
    warnings = await gateway_drift(gw, ["SPC", "ECB", "GONE"])
    joined = " ".join(warnings)
    assert "NEWONE" in joined
    assert "GONE" in joined


async def test_drift_empty_when_lists_match():
    gw = FakeGateway({"list_available_endpoints": {
        "endpoints": [{"key": "SPC"}, {"key": "ECB"}]}})
    assert await gateway_drift(gw, ["SPC", "ECB"]) == []


async def test_data_records_sample_value_from_probe():
    gw = FakeGateway({"probe_data_url": {
        "status": "nonempty", "observation_count": 5,
        "sample_observations": [
            {"dimensions": {"TIME_PERIOD": "2000"}, "value": None},
            {"dimensions": {"TIME_PERIOD": "2001"}, "value": 1345.2},
        ]}})
    result = await gateway_data_check(gw, _ep("SPC"))
    assert result.ok is True
    assert result.sample_value == "1345.2"
    assert result.sample_period == "2001"


async def test_data_fails_when_all_probe_samples_are_null():
    """A probe that misparses a provider's error envelope reports nonempty with
    null-valued samples; that is not data."""
    gw = FakeGateway({"probe_data_url": {
        "status": "nonempty", "observation_count": 1,
        "sample_observations": [{"dimensions": {}, "value": None}]}})
    result = await gateway_data_check(gw, _ep("ESTAT"))
    assert result.ok is False
    assert "no non-null" in (result.error or "")


async def test_data_passes_without_samples_but_notes_it():
    gw = FakeGateway({"probe_data_url": {
        "status": "nonempty", "observation_count": 42, "sample_observations": []}})
    result = await gateway_data_check(gw, _ep("SPC"))
    assert result.ok is True
    assert result.sample_value is None
    assert "not verified" in (result.error or "")


async def test_data_passes_when_null_samples_do_not_cover_the_result():
    """Sparse slices are normal: several providers return empty values for part
    of a slice, and the probe only samples the first rows."""
    gw = FakeGateway({"probe_data_url": {
        "status": "nonempty", "observation_count": 70,
        "sample_observations": [{"dimensions": {}, "value": None} for _ in range(5)]}})
    result = await gateway_data_check(gw, _ep("FBOS"))
    assert result.ok is True
    assert result.sample_value is None
    assert "not verified" in (result.error or "")


async def test_data_fails_when_null_samples_cover_the_whole_result():
    """Every observation was sampled and none had a value: that is not data."""
    gw = FakeGateway({"probe_data_url": {
        "status": "nonempty", "observation_count": 1,
        "sample_observations": [{"dimensions": {}, "value": None}]}})
    result = await gateway_data_check(gw, _ep("ESTAT"))
    assert result.ok is False
    assert "no non-null" in (result.error or "")


@pytest.mark.asyncio
async def test_call_tool_gives_up_rather_than_hanging(monkeypatch):
    """An SSE stream held open without an answer must not stall a cycle."""
    import asyncio

    import checks_gateway

    class HangingSession:
        async def call_tool(self, name, args):
            await asyncio.sleep(3600)

    gw = checks_gateway.GatewaySession("http://gw.example/mcp", timeout_s=0.01)
    gw._session = HangingSession()
    with pytest.raises(checks_gateway.GatewayError) as excinfo:
        await gw.call_tool("list_dataflows", {})
    assert "timed out" in str(excinfo.value).lower()


@pytest.mark.asyncio
async def test_tool_count_gives_up_rather_than_hanging():
    import asyncio

    import checks_gateway

    class HangingSession:
        async def list_tools(self):
            await asyncio.sleep(3600)

    gw = checks_gateway.GatewaySession("http://gw.example/mcp", timeout_s=0.01)
    gw._session = HangingSession()
    with pytest.raises(checks_gateway.GatewayError) as excinfo:
        await gw.tool_count()
    assert "timed out" in str(excinfo.value).lower()


def test_read_timeout_scales_with_the_configured_timeout():
    """The read timeout used to be hardcoded at 300s regardless of config."""
    import checks_gateway

    assert checks_gateway._read_timeout(30.0) == 60.0    # floor applies
    assert checks_gateway._read_timeout(120.0) == 120.0  # scales above the floor
