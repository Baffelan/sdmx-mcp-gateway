"""Gateway checks are tested against a fake session object; the real
GatewaySession is exercised by scripts/live_smoke.py, not in CI."""

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
    assert gw.calls == [("list_dataflows", {"limit": 1, "endpoint": "SPC"})]


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


async def test_data_fails_on_empty_or_error_probe():
    for status in ("empty", "error"):
        gw = FakeGateway({"probe_data_url": {
            "status": status, "observation_count": 0, "notes": ["HTTP 406 from provider."]}})
        result = await gateway_data_check(gw, _ep("ECB"))
        assert result.ok is False
        assert status in (result.error or "")


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
