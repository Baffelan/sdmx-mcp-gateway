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


def test_sample_value_roundtrip(store: Store):
    cid = store.open_cycle("2026-07-25T10:00:00+00:00")
    store.add_result(cid, CheckResult("SPC", "direct", "data", ok=True, obs_count=483,
                                      sample_value="1345.2", sample_period="2000"))
    store.close_cycle(cid, "2026-07-25T10:01:00+00:00", True, 10, "")
    (row,) = store.latest_cycle()["results"]
    assert row["sample_value"] == "1345.2"
    assert row["sample_period"] == "2000"


def test_migration_adds_columns_to_preexisting_db(tmp_path):
    """A database created before this change must gain the new columns in place,
    keeping its existing rows (the production DB lives on a Railway volume)."""
    import sqlite3

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            gateway_up INTEGER NOT NULL DEFAULT 0,
            gateway_latency_ms INTEGER,
            drift TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE results (
            cycle_id INTEGER NOT NULL,
            endpoint_key TEXT NOT NULL,
            path TEXT NOT NULL,
            kind TEXT NOT NULL,
            ok INTEGER NOT NULL,
            skipped INTEGER NOT NULL DEFAULT 0,
            latency_ms INTEGER,
            http_status INTEGER,
            obs_count INTEGER,
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 1
        );
        INSERT INTO cycles (id, started_at, finished_at, gateway_up, gateway_latency_ms, drift)
        VALUES (1, '2026-07-25T09:00:00+00:00', '2026-07-25T09:01:00+00:00', 1, 42, '');
        INSERT INTO results (cycle_id, endpoint_key, path, kind, ok)
        VALUES (1, 'SPC', 'direct', 'data', 1);
        """
    )
    conn.commit()
    conn.close()

    store = Store(db)  # must migrate, not crash
    try:
        latest = store.latest_cycle()
        assert latest["id"] == 1
        (row,) = latest["results"]
        assert row["endpoint_key"] == "SPC"
        assert row["sample_value"] is None
        assert row["sample_period"] is None
        # and new writes work against the migrated table
        cid = store.open_cycle("2026-07-25T10:00:00+00:00")
        store.add_result(cid, CheckResult("ECB", "direct", "data", ok=True,
                                          sample_value="1.1377", sample_period="2026-07-24"))
        store.close_cycle(cid, "2026-07-25T10:01:00+00:00", True, 5, "")
        assert store.latest_cycle()["results"][0]["sample_value"] == "1.1377"
    finally:
        store.close()


def test_migration_rebuilds_table_with_stale_kind_constraint(tmp_path):
    """The deployed database was created before the json check existed; its
    CHECK constraint would reject the new rows, and SQLite cannot widen a
    CHECK in place, so the table must be rebuilt with its rows preserved."""
    import sqlite3

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            gateway_up INTEGER NOT NULL DEFAULT 0,
            gateway_latency_ms INTEGER,
            drift TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE results (
            cycle_id INTEGER NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
            endpoint_key TEXT NOT NULL,
            path TEXT NOT NULL CHECK (path IN ('gateway', 'direct')),
            kind TEXT NOT NULL CHECK (kind IN ('metadata', 'data')),
            ok INTEGER NOT NULL,
            skipped INTEGER NOT NULL DEFAULT 0,
            latency_ms INTEGER,
            http_status INTEGER,
            obs_count INTEGER,
            error TEXT,
            attempts INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX idx_results_cycle ON results(cycle_id);
        INSERT INTO cycles (id, started_at, finished_at, gateway_up, gateway_latency_ms, drift)
        VALUES (1, '2026-07-25T09:00:00+00:00', '2026-07-25T09:01:00+00:00', 1, 42, '');
        INSERT INTO results (cycle_id, endpoint_key, path, kind, ok, obs_count)
        VALUES (1, 'SPC', 'direct', 'data', 1, 483);
        """
    )
    conn.commit()
    conn.close()

    store = Store(db)
    try:
        # the pre-existing row survived the rebuild, with its values intact
        (row,) = store.latest_cycle()["results"]
        assert row["endpoint_key"] == "SPC"
        assert row["obs_count"] == 483
        assert row["sample_value"] is None
        # and the new kind now inserts, which the old constraint forbade
        cid = store.open_cycle("2026-07-25T10:00:00+00:00")
        store.add_result(cid, CheckResult("SPC", "direct", "json", ok=True, latency_ms=12))
        store.close_cycle(cid, "2026-07-25T10:01:00+00:00", True, 5, "")
        kinds = [r["kind"] for r in store.latest_cycle()["results"]]
        assert kinds == ["json"]
    finally:
        store.close()


def test_migration_leaves_foreign_keys_enforced(tmp_path):
    store = Store(tmp_path / "fk.db")
    try:
        row = store._conn.execute("PRAGMA foreign_keys").fetchone()
        assert row[0] == 1
    finally:
        store.close()


def test_migration_is_idempotent_across_reopens(tmp_path):
    db = tmp_path / "idem.db"
    first = Store(db)
    cid = first.open_cycle("2026-07-25T10:00:00+00:00")
    first.add_result(cid, CheckResult("SPC", "direct", "json", ok=True))
    first.close_cycle(cid, "2026-07-25T10:01:00+00:00", True, 1, "")
    first.close()
    second = Store(db)  # reopening must not rebuild or lose anything
    try:
        assert [r["kind"] for r in second.latest_cycle()["results"]] == ["json"]
    finally:
        second.close()
