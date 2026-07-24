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
