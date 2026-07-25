from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main
from storage import CheckResult, Store, utcnow_iso


@pytest.fixture()
def store(tmp_path: Path):
    s = Store(tmp_path / "t.db")
    yield s
    s.close()


@pytest.fixture()
def client(store: Store):
    app = main.create_app(store=store, enable_scheduler=False)
    with TestClient(app) as c:
        yield c


def _seed_cycle(store: Store, started_at: str | None = None, ecb_gateway_ok: bool = True):
    cid = store.open_cycle(started_at or utcnow_iso())
    for key in ("SPC", "ECB"):
        gw_data_ok = ecb_gateway_ok if key == "ECB" else True
        store.add_result(cid, CheckResult(key, "gateway", "metadata", ok=True, latency_ms=10))
        store.add_result(cid, CheckResult(key, "gateway", "data", ok=gw_data_ok,
                                          latency_ms=20,
                                          error=None if gw_data_ok else "probe status: error"))
        store.add_result(cid, CheckResult(key, "direct", "metadata", ok=True, latency_ms=5))
        store.add_result(cid, CheckResult(key, "direct", "data", ok=True, latency_ms=6))
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=99, drift="")
    return cid


def test_healthz(client: TestClient):
    assert client.get("/healthz").json() == {"ok": True}


def test_status_empty_db(client: TestClient):
    body = client.get("/api/status").json()
    assert body["cycle"] is None
    assert body["stale"] is True
    assert body["endpoints"] == []


def test_status_reports_derived_statuses(store: Store, client: TestClient):
    _seed_cycle(store, ecb_gateway_ok=False)
    body = client.get("/api/status").json()
    assert body["cycle"]["gateway_up"] is True
    assert body["stale"] is False
    by_key = {e["key"]: e for e in body["endpoints"]}
    # only seeded endpoints appear; unseeded configured endpoints are omitted
    assert set(by_key) == {"SPC", "ECB"}
    assert by_key["SPC"]["status"] == "healthy"
    assert by_key["ECB"]["status"] == "gateway_issue"
    assert by_key["SPC"]["last_success"] is not None
    assert len(by_key["SPC"]["checks"]) == 4
    assert by_key["SPC"]["name"] == "Pacific Data Hub"


def test_status_stale_when_cycle_old(store: Store, client: TestClient):
    _seed_cycle(store, started_at="2026-01-01T00:00:00+00:00")
    body = client.get("/api/status").json()
    assert body["stale"] is True


def test_history_series(store: Store, client: TestClient):
    _seed_cycle(store)
    _seed_cycle(store, ecb_gateway_ok=False)
    body = client.get("/api/history?hours=24").json()
    assert body["hours"] == 24
    assert [p["status"] for p in body["series"]["ECB"]] == ["healthy", "gateway_issue"]
    assert len(body["series"]["SPC"]) == 2
    assert {"cycle_id", "started_at", "status"} <= set(body["series"]["SPC"][0])


def test_history_hours_clamped(store: Store, client: TestClient):
    assert client.get("/api/history?hours=999999").json()["hours"] == 2160
    assert client.get("/api/history?hours=0").json()["hours"] == 1


def test_refresh_runs_cycle_and_cooldown(store: Store, client: TestClient, monkeypatch):
    calls = []

    async def fake_run_cycle(store_arg, endpoints, url, *, timeout_s=30.0):
        calls.append(url)
        return 41

    monkeypatch.setattr(main, "run_cycle", fake_run_cycle)
    first = client.post("/api/refresh")
    assert first.status_code == 200
    assert first.json() == {"cycle_id": 41}
    second = client.post("/api/refresh")
    assert second.status_code == 429
    assert len(calls) == 1


def test_status_splits_nonempty_drift(store: Store, client: TestClient):
    cid = store.open_cycle(utcnow_iso())
    store.add_result(cid, CheckResult("SPC", "gateway", "metadata", ok=True))
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=1,
                      drift="gateway endpoints not monitored: NEWONE; drift check failed: boom")
    body = client.get("/api/status").json()
    assert body["drift"] == [
        "gateway endpoints not monitored: NEWONE",
        "drift check failed: boom",
    ]


def test_refresh_409_when_cycle_running(store: Store, client: TestClient):
    class HeldLock:
        def locked(self) -> bool:
            return True

    client.app.state.cycle_lock = HeldLock()
    resp = client.post("/api/refresh")
    assert resp.status_code == 409


def test_scheduler_catchup_runs_on_stale_store(store: Store, monkeypatch):
    import threading

    ran = threading.Event()

    async def fake_run_cycle(store_arg, endpoints, url, *, timeout_s=30.0):
        ran.set()
        return 1

    monkeypatch.setattr(main, "run_cycle", fake_run_cycle)
    app = main.create_app(store=store, enable_scheduler=True)
    with TestClient(app):
        assert ran.wait(timeout=5.0), "catch-up cycle did not run within 5s"


def test_index_serves_status_page(client: TestClient):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "SDMx MCP Gateway Status" in resp.text


def test_default_check_interval_is_two_hours():
    assert main.CHECK_INTERVAL_MIN == 120


def test_status_exposes_sample_values(store: Store, client: TestClient):
    cid = store.open_cycle(utcnow_iso())
    store.add_result(cid, CheckResult("SPC", "direct", "data", ok=True, obs_count=1,
                                      sample_value="1345.2", sample_period="2000"))
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=1, drift="")
    body = client.get("/api/status").json()
    (endpoint,) = body["endpoints"]
    (check,) = endpoint["checks"]
    assert check["sample_value"] == "1345.2"
    assert check["sample_period"] == "2000"


def _seed_contract(store: Store, cid: int, **kw):
    from storage import ContractResult
    defaults = dict(endpoint_key="SPC", assertion="references:parents",
                    verdict="ok", observed="200")
    defaults.update(kw)
    store.add_contract(cid, ContractResult(**defaults))


def test_status_summarises_contracts(store: Store, client: TestClient):
    cid = store.open_cycle(utcnow_iso())
    store.add_result(cid, CheckResult("SPC", "direct", "metadata", ok=True))
    _seed_contract(store, cid)
    _seed_contract(store, cid, assertion="references:all", verdict="broken",
                   observed="400")
    _seed_contract(store, cid, assertion="dialect:sdmx3",
                   verdict="capability_appeared", observed="200")
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=1, drift="")
    body = client.get("/api/status").json()
    (endpoint,) = body["endpoints"]
    assert endpoint["contracts"]["total"] == 3
    assert endpoint["contracts"]["broken"] == ["references:all"]
    assert "dialect:sdmx3" in endpoint["contracts"]["informational"]
    # a broken contract degrades the endpoint
    assert endpoint["status"] == "degraded"


def test_contracts_endpoint_reports_changes_since_the_previous_cycle(
    store: Store, client: TestClient
):
    first = store.open_cycle("2026-07-25T08:00:00+00:00")
    _seed_contract(store, first, assertion="constraint:availableconstraint",
                   observed="500")
    store.close_cycle(first, "2026-07-25T08:01:00+00:00", True, 1, "")
    second = store.open_cycle(utcnow_iso())
    _seed_contract(store, second, assertion="constraint:availableconstraint",
                   observed="200", verdict="capability_appeared")
    store.close_cycle(second, utcnow_iso(), True, 1, "")

    body = client.get("/api/contracts").json()
    (change,) = body["changes"]
    assert change["endpoint_key"] == "SPC"
    assert change["assertion"] == "constraint:availableconstraint"
    assert change["was"] == "500"
    assert change["now"] == "200"
    assert "SPC" in body["matrix"]


def test_contracts_endpoint_is_empty_on_a_fresh_store(client: TestClient):
    body = client.get("/api/contracts").json()
    assert body["changes"] == []
    assert body["matrix"] == {}


def _seed_unicef_outage(store: Store, started_at: str) -> int:
    """The real cycle 2: UNICEF answered 503 on every check, so both metadata
    checks failed and the endpoint derived as provider_down."""
    cid = store.open_cycle(started_at)
    for path, kind in (("gateway", "metadata"), ("gateway", "data"),
                       ("direct", "metadata"), ("direct", "data")):
        store.add_result(cid, CheckResult("UNICEF", path, kind, ok=False,
                                          http_status=503, error="HTTP 503",
                                          attempts=2))
    store.close_cycle(cid, started_at, gateway_up=True, gateway_latency_ms=40, drift="")
    return cid


def test_history_summarises_why_a_point_is_red(store: Store, client: TestClient):
    _seed_unicef_outage(store, utcnow_iso())
    body = client.get("/api/history?hours=24").json()
    (point,) = body["series"]["UNICEF"]
    assert point["status"] == "provider_down"
    assert len(point["failing"]) == 4
    assert "direct metadata: HTTP 503" in point["failing"]


def test_history_omits_failure_detail_for_healthy_points(store: Store, client: TestClient):
    cid = store.open_cycle(utcnow_iso())
    store.add_result(cid, CheckResult("SPC", "direct", "metadata", ok=True))
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=5, drift="")
    (point,) = client.get("/api/history?hours=24").json()["series"]["SPC"]
    assert point["status"] == "healthy"
    assert point["failing"] == []


def test_history_skips_skipped_checks_in_the_summary(store: Store, client: TestClient):
    cid = store.open_cycle(utcnow_iso())
    store.add_result(cid, CheckResult("IMF", "direct", "metadata", ok=False,
                                      error="HTTP 500"))
    store.add_result(cid, CheckResult("IMF", "direct", "json", ok=False, skipped=True,
                                      error="skipped: provider does not serve SDMx-JSON"))
    store.close_cycle(cid, utcnow_iso(), gateway_up=True, gateway_latency_ms=5, drift="")
    (point,) = client.get("/api/history?hours=24").json()["series"]["IMF"]
    assert point["failing"] == ["direct metadata: HTTP 500"]


def test_cycle_endpoint_returns_a_past_cycle(store: Store, client: TestClient):
    cid = _seed_unicef_outage(store, "2026-07-25T05:52:47+00:00")
    newer = store.open_cycle(utcnow_iso())
    store.add_result(newer, CheckResult("UNICEF", "direct", "metadata", ok=True))
    store.close_cycle(newer, utcnow_iso(), gateway_up=True, gateway_latency_ms=5, drift="")

    body = client.get("/api/cycle/" + str(cid)).json()
    assert body["cycle"]["id"] == cid
    assert body["cycle"]["started_at"] == "2026-07-25T05:52:47+00:00"
    (endpoint,) = body["endpoints"]
    assert endpoint["key"] == "UNICEF"
    assert endpoint["status"] == "provider_down"
    assert len(endpoint["checks"]) == 4
    # the latest cycle is unaffected by asking for an old one
    assert client.get("/api/status").json()["cycle"]["id"] == newer


def test_cycle_endpoint_404s_for_unknown_cycle(client: TestClient):
    assert client.get("/api/cycle/4242").status_code == 404


def test_cycle_endpoint_handles_a_cycle_with_no_contracts(store: Store, client: TestClient):
    """Cycles recorded before the contract layer must still render."""
    cid = _seed_unicef_outage(store, utcnow_iso())
    (endpoint,) = client.get("/api/cycle/" + str(cid)).json()["endpoints"]
    assert endpoint["contracts"]["total"] == 0
