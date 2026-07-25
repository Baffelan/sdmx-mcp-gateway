from pathlib import Path

import pytest

from storage import CheckResult, Store, utcnow_iso


@pytest.fixture()
def store(tmp_path: Path):
    s = Store(tmp_path / "data" / "test.db")
    yield s
    s.close()


def _result(key="SPC", path="direct", kind="metadata", ok=True, **kw) -> CheckResult:
    return CheckResult(endpoint_key=key, path=path, kind=kind, ok=ok, **kw)


def test_utcnow_iso_is_utc_seconds():
    ts = utcnow_iso()
    assert ts.endswith("+00:00")
    assert "." not in ts  # timespec="seconds"


def test_cycle_roundtrip(store: Store):
    cid = store.open_cycle("2026-07-24T10:00:00+00:00")
    store.add_result(cid, _result(ok=True, latency_ms=120, http_status=200))
    store.add_result(cid, _result(kind="data", ok=False, error="boom", attempts=2))
    store.close_cycle(cid, "2026-07-24T10:01:00+00:00", gateway_up=True,
                      gateway_latency_ms=333, drift="")
    latest = store.latest_cycle()
    assert latest is not None
    assert latest["id"] == cid
    assert latest["gateway_up"] is True
    assert latest["gateway_latency_ms"] == 333
    assert len(latest["results"]) == 2
    failed = [r for r in latest["results"] if not r["ok"]]
    assert failed[0]["error"] == "boom"
    assert failed[0]["attempts"] == 2
    assert failed[0]["skipped"] is False


def test_latest_cycle_none_on_empty(store: Store):
    assert store.latest_cycle() is None


def test_cycles_since_filters_by_start(store: Store):
    old = store.open_cycle("2026-07-01T00:00:00+00:00")
    store.close_cycle(old, "2026-07-01T00:01:00+00:00", True, 1, "")
    new = store.open_cycle("2026-07-24T00:00:00+00:00")
    store.close_cycle(new, "2026-07-24T00:01:00+00:00", True, 1, "")
    got = store.cycles_since("2026-07-20T00:00:00+00:00")
    assert [c["id"] for c in got] == [new]


def test_last_success_ignores_failing_and_skipped_cycles(store: Store):
    good = store.open_cycle("2026-07-24T10:00:00+00:00")
    store.add_result(good, _result(ok=True))
    store.close_cycle(good, "2026-07-24T10:01:00+00:00", True, 1, "")
    bad = store.open_cycle("2026-07-24T11:00:00+00:00")
    store.add_result(bad, _result(ok=False, error="down"))
    store.close_cycle(bad, "2026-07-24T11:01:00+00:00", True, 1, "")
    assert store.last_success("SPC") == "2026-07-24T10:00:00+00:00"
    assert store.last_success("NEVER_SEEN") is None
    # a skipped row does not count as a failure
    skip = store.open_cycle("2026-07-24T12:00:00+00:00")
    store.add_result(skip, _result(ok=True))
    store.add_result(skip, _result(kind="data", ok=False, skipped=True))
    store.close_cycle(skip, "2026-07-24T12:01:00+00:00", True, 1, "")
    assert store.last_success("SPC") == "2026-07-24T12:00:00+00:00"


def test_unfinished_cycles_are_invisible_to_readers(store: Store):
    done = store.open_cycle("2026-07-24T10:00:00+00:00")
    store.add_result(done, _result())
    store.close_cycle(done, "2026-07-24T10:01:00+00:00", True, 5, "")
    store.open_cycle("2026-07-24T10:30:00+00:00")  # in progress, never closed
    latest = store.latest_cycle()
    assert latest is not None and latest["id"] == done
    since = store.cycles_since("2026-07-24T00:00:00+00:00")
    assert [c["id"] for c in since] == [done]


def test_prune_removes_old_cycles_and_results(store: Store):
    old = store.open_cycle("2026-01-01T00:00:00+00:00")
    store.add_result(old, _result())
    store.close_cycle(old, "2026-01-01T00:01:00+00:00", True, 1, "")
    keep = store.open_cycle("2026-07-24T00:00:00+00:00")
    store.close_cycle(keep, "2026-07-24T00:01:00+00:00", True, 1, "")
    removed = store.prune("2026-06-01T00:00:00+00:00")
    assert removed == 1
    assert [c["id"] for c in store.cycles_since("2020-01-01T00:00:00+00:00")] == [keep]
