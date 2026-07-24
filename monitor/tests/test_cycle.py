from pathlib import Path

import pytest

import cycle
from endpoints_config import ENDPOINTS
from storage import CheckResult, Store


@pytest.fixture()
def store(tmp_path: Path):
    s = Store(tmp_path / "t.db")
    yield s
    s.close()


class FakeGatewaySession:
    """Stands in for cycle.GatewaySession; also the object handed to checks."""

    fail_connect = False

    def __init__(self, url: str, timeout_s: float = 30.0):
        self.url = url

    async def __aenter__(self):
        if self.fail_connect:
            raise ConnectionError("gateway down")
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def tool_count(self) -> int:
        return 18

    async def call_tool(self, name: str, args: dict) -> dict:
        raise AssertionError("checks are patched; call_tool must not be hit")


@pytest.fixture(autouse=True)
def _patch_checks(monkeypatch):
    FakeGatewaySession.fail_connect = False
    monkeypatch.setattr(cycle, "GatewaySession", FakeGatewaySession)

    async def fake_gw_meta(gw, ep):
        return CheckResult(ep.key, "gateway", "metadata", ok=True, latency_ms=10)

    async def fake_gw_data(gw, ep, timeout_s=30.0):
        return CheckResult(ep.key, "gateway", "data", ok=True, latency_ms=20)

    async def fake_drift(gw, keys):
        return ["gateway endpoints not monitored: NEWONE"]

    async def fake_direct(client, ep):
        return [
            CheckResult(ep.key, "direct", "metadata", ok=True, latency_ms=5),
            CheckResult(ep.key, "direct", "data", ok=True, latency_ms=6),
        ]

    monkeypatch.setattr(cycle, "gateway_metadata_check", fake_gw_meta)
    monkeypatch.setattr(cycle, "gateway_data_check", fake_gw_data)
    monkeypatch.setattr(cycle, "gateway_drift", fake_drift)
    monkeypatch.setattr(cycle, "run_direct_checks", fake_direct)
    monkeypatch.setattr(cycle.checks_common, "RETRY_DELAY_S", 0.0)


async def test_cycle_records_all_checks(store: Store):
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest is not None and latest["id"] == cycle_id
    assert latest["gateway_up"] is True
    assert latest["gateway_latency_ms"] is not None
    assert "NEWONE" in latest["drift"]
    # 12 endpoints x 4 checks, minus STATSNZ data via gateway (no pinned query)
    per_endpoint = {}
    for row in latest["results"]:
        per_endpoint.setdefault(row["endpoint_key"], []).append(row)
    assert set(per_endpoint) == {ep.key for ep in ENDPOINTS}
    assert all(len(rows) == 4 for rows in per_endpoint.values())
    statsnz_gw_data = [
        r for r in per_endpoint["STATSNZ"]
        if r["path"] == "gateway" and r["kind"] == "data"
    ]
    assert statsnz_gw_data[0]["skipped"] is True


async def test_gateway_unreachable_marks_cycle_down_and_skips_gateway_rows(store: Store):
    FakeGatewaySession.fail_connect = True
    await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest["gateway_up"] is False
    gw_rows = [r for r in latest["results"] if r["path"] == "gateway"]
    assert gw_rows and all(r["skipped"] for r in gw_rows)
    direct_rows = [r for r in latest["results"] if r["path"] == "direct"]
    assert direct_rows and all(not r["skipped"] for r in direct_rows)


async def test_cycle_prunes_old_cycles(store: Store):
    old = store.open_cycle("2020-01-01T00:00:00+00:00")
    store.close_cycle(old, "2020-01-01T00:01:00+00:00", True, 1, "")
    await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    ids = [c["id"] for c in store.cycles_since("2000-01-01T00:00:00+00:00")]
    assert old not in ids
