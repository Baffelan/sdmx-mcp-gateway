import asyncio
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

    def __init__(self, url: str, timeout_s: float = 30.0, call_timeout_s: float = 120.0):
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
            CheckResult(ep.key, "direct", "json", ok=True, latency_ms=7),
        ]

    async def fake_contracts(client, ep, exp):
        return []

    monkeypatch.setattr(cycle, "gateway_metadata_check", fake_gw_meta)
    monkeypatch.setattr(cycle, "gateway_data_check", fake_gw_data)
    monkeypatch.setattr(cycle, "gateway_drift", fake_drift)
    monkeypatch.setattr(cycle, "run_direct_checks", fake_direct)
    monkeypatch.setattr(cycle, "run_contracts", fake_contracts)
    monkeypatch.setattr(cycle.checks_common, "RETRY_DELAY_S", 0.0)


async def test_cycle_records_all_checks(store: Store):
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest is not None and latest["id"] == cycle_id
    assert latest["gateway_up"] is True
    assert latest["gateway_latency_ms"] is not None
    assert "NEWONE" in latest["drift"]
    # 12 endpoints x 5 checks (gateway metadata/data, direct metadata/data/json)
    per_endpoint = {}
    for row in latest["results"]:
        per_endpoint.setdefault(row["endpoint_key"], []).append(row)
    assert set(per_endpoint) == {ep.key for ep in ENDPOINTS}
    assert all(len(rows) == 5 for rows in per_endpoint.values())
    assert not [
        r for r in latest["results"]
        if r["path"] == "gateway" and r["kind"] == "data" and r["skipped"]
    ]


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


async def test_tool_count_failure_closes_session_and_marks_gateway_down(store, monkeypatch):
    class ExplodingSession(FakeGatewaySession):
        exited = False

        async def tool_count(self) -> int:
            raise RuntimeError("boom")

        async def __aexit__(self, *exc_info):
            ExplodingSession.exited = True
            return False

    ExplodingSession.exited = False
    monkeypatch.setattr(cycle, "GatewaySession", ExplodingSession)
    await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    assert ExplodingSession.exited is True
    latest = store.latest_cycle()
    assert latest["gateway_up"] is False
    gw_rows = [r for r in latest["results"] if r["path"] == "gateway"]
    assert gw_rows and all(r["skipped"] for r in gw_rows)


async def test_escaping_check_exception_still_closes_cycle(store, monkeypatch):
    async def exploding_direct(client, ep):
        raise RuntimeError("bundle bug")

    monkeypatch.setattr(cycle, "run_direct_checks", exploding_direct)
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest["id"] == cycle_id
    assert latest["finished_at"] is not None
    assert "cycle error" in latest["drift"]


async def test_cycle_records_contract_results(store: Store, monkeypatch):
    async def fake_run_contracts(client, ep, exp):
        from storage import ContractResult
        return [ContractResult(ep.key, "references:none", verdict="ok",
                               observed="200")]

    monkeypatch.setattr(cycle, "run_contracts", fake_run_contracts)
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    contracts = store.contracts_for_cycle(cycle_id)
    assert {c["endpoint_key"] for c in contracts} == {ep.key for ep in ENDPOINTS}
    assert all(c["assertion"] == "references:none" for c in contracts)


async def test_contract_failure_does_not_abort_the_cycle(store: Store, monkeypatch):
    async def exploding_contracts(client, ep, exp):
        raise RuntimeError("contract bug")

    monkeypatch.setattr(cycle, "run_contracts", exploding_contracts)
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest["id"] == cycle_id
    assert latest["finished_at"] is not None
    # the ordinary checks still recorded
    assert latest["results"]


async def test_cycle_deadline_closes_the_cycle_instead_of_hanging(store: Store, monkeypatch):
    """A hung check must not hold the lock forever and silence the monitor."""
    async def hanging_direct(client, ep):
        await asyncio.sleep(3600)

    monkeypatch.setattr(cycle, "run_direct_checks", hanging_direct)
    cycle_id = await cycle.run_cycle(
        store, ENDPOINTS[:2], "http://gw.example/mcp", cycle_timeout_s=0.05
    )
    latest = store.latest_cycle()
    assert latest["id"] == cycle_id
    assert latest["finished_at"] is not None      # the cycle closed
    assert "deadline" in latest["drift"].lower()  # and said why


async def test_a_normal_cycle_is_unaffected_by_the_deadline(store: Store):
    """The deadline must not change anything about a healthy run."""
    cycle_id = await cycle.run_cycle(store, ENDPOINTS, "http://gw.example/mcp")
    latest = store.latest_cycle()
    assert latest["id"] == cycle_id
    assert "deadline" not in (latest["drift"] or "").lower()
    assert latest["results"]


async def test_call_timeout_s_is_threaded_to_the_gateway_session(store: Store, monkeypatch):
    """The per-call budget must reach GatewaySession, not stop at run_cycle's
    own signature."""
    captured = {}

    class SpyGatewaySession(FakeGatewaySession):
        def __init__(self, url, timeout_s=30.0, call_timeout_s=120.0):
            super().__init__(url, timeout_s, call_timeout_s)
            captured["call_timeout_s"] = call_timeout_s

    monkeypatch.setattr(cycle, "GatewaySession", SpyGatewaySession)
    await cycle.run_cycle(
        store, ENDPOINTS[:1], "http://gw.example/mcp", call_timeout_s=55.0
    )
    assert captured["call_timeout_s"] == 55.0


async def test_call_timeout_s_defaults_to_120(store: Store, monkeypatch):
    """run_cycle must pass its own 120.0 default explicitly, not just leave
    the argument unset and rely on GatewaySession's default (a spy default
    of 999.0 here would otherwise mask that)."""
    captured = {}

    class SpyGatewaySession(FakeGatewaySession):
        def __init__(self, url, timeout_s=30.0, call_timeout_s=999.0):
            super().__init__(url, timeout_s, call_timeout_s)
            captured["call_timeout_s"] = call_timeout_s

    monkeypatch.setattr(cycle, "GatewaySession", SpyGatewaySession)
    await cycle.run_cycle(store, ENDPOINTS[:1], "http://gw.example/mcp")
    assert captured["call_timeout_s"] == 120.0


async def test_a_hanging_liveness_check_does_not_stall_the_cycle(store: Store, monkeypatch):
    """The liveness probe runs before the deadline-protected block, so it needs
    its own bound or a hang there stops all monitoring."""

    class HangingSession:
        def __init__(self, url, timeout_s=30.0, call_timeout_s=120.0):
            pass

        async def __aenter__(self):
            await asyncio.sleep(3600)

        async def __aexit__(self, *exc_info):
            return False

    monkeypatch.setattr(cycle, "GatewaySession", HangingSession)
    cycle_id = await cycle.run_cycle(
        store, ENDPOINTS[:2], "http://gw.example/mcp", timeout_s=0.01
    )
    latest = store.latest_cycle()
    assert latest["id"] == cycle_id
    assert latest["finished_at"] is not None   # the cycle still closed
    assert latest["gateway_up"] is False       # and reported the gateway as down
