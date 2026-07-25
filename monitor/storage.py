"""SQLite storage for check cycles and raw per-check results.

One writer (the scheduler or a manual refresh) plus read-only API requests.
A single connection guarded by a lock is enough at this scale; ISO-8601 UTC
strings sort correctly, so time filters are plain string comparisons.
"""

import sqlite3
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    gateway_up INTEGER NOT NULL DEFAULT 0,
    gateway_latency_ms INTEGER,
    drift TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS results (
    cycle_id INTEGER NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
    endpoint_key TEXT NOT NULL,
    path TEXT NOT NULL CHECK (path IN ('gateway', 'direct')),
    kind TEXT NOT NULL CHECK (kind IN ('metadata', 'data')),
    ok INTEGER NOT NULL,
    skipped INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER,
    http_status INTEGER,
    obs_count INTEGER,
    sample_value TEXT,
    sample_period TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_results_cycle ON results(cycle_id);
CREATE INDEX IF NOT EXISTS idx_results_endpoint ON results(endpoint_key, cycle_id);
CREATE INDEX IF NOT EXISTS idx_cycles_started ON cycles(started_at);
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class CheckResult:
    endpoint_key: str
    path: str  # "gateway" | "direct"
    kind: str  # "metadata" | "data"
    ok: bool
    skipped: bool = False
    latency_ms: int | None = None
    http_status: int | None = None
    obs_count: int | None = None
    sample_value: str | None = None
    sample_period: str | None = None
    error: str | None = None
    attempts: int = 1


class Store:
    def __init__(self, db_path: str | Path) -> None:
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """Add columns introduced after a database was first created.

        CREATE TABLE IF NOT EXISTS leaves an existing table untouched, so a
        database created by an earlier version (the deployed one lives on a
        Railway volume) needs its new columns added in place.
        """
        existing = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(results)").fetchall()
        }
        for column in ("sample_value", "sample_period"):
            if column not in existing:
                self._conn.execute("ALTER TABLE results ADD COLUMN " + column + " TEXT")
        self._conn.commit()

    def open_cycle(self, started_at: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO cycles (started_at) VALUES (?)", (started_at,)
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def close_cycle(
        self,
        cycle_id: int,
        finished_at: str,
        gateway_up: bool,
        gateway_latency_ms: int | None,
        drift: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE cycles SET finished_at = ?, gateway_up = ?, "
                "gateway_latency_ms = ?, drift = ? WHERE id = ?",
                (finished_at, int(gateway_up), gateway_latency_ms, drift, cycle_id),
            )
            self._conn.commit()

    def add_result(self, cycle_id: int, result: CheckResult) -> None:
        fields = asdict(result)
        with self._lock:
            self._conn.execute(
                "INSERT INTO results (cycle_id, endpoint_key, path, kind, ok, skipped, "
                "latency_ms, http_status, obs_count, sample_value, sample_period, "
                "error, attempts) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cycle_id,
                    fields["endpoint_key"],
                    fields["path"],
                    fields["kind"],
                    int(fields["ok"]),
                    int(fields["skipped"]),
                    fields["latency_ms"],
                    fields["http_status"],
                    fields["obs_count"],
                    fields["sample_value"],
                    fields["sample_period"],
                    fields["error"],
                    fields["attempts"],
                ),
            )
            self._conn.commit()

    def _cycle_dict(self, row: sqlite3.Row) -> dict:
        cycle = dict(row)
        cycle["gateway_up"] = bool(cycle["gateway_up"])
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM results WHERE cycle_id = ?", (cycle["id"],)
            ).fetchall()
        results = []
        for r in rows:
            item = dict(r)
            item["ok"] = bool(item["ok"])
            item["skipped"] = bool(item["skipped"])
            results.append(item)
        cycle["results"] = results
        return cycle

    def latest_cycle(self) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM cycles WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return self._cycle_dict(row)

    def cycles_since(self, since_iso: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM cycles WHERE started_at >= ? AND finished_at IS NOT NULL "
                "ORDER BY id ASC",
                (since_iso,),
            ).fetchall()
        return [self._cycle_dict(r) for r in rows]

    def last_success(self, endpoint_key: str) -> str | None:
        query = """
            SELECT MAX(c.started_at) AS ts FROM cycles c
            WHERE EXISTS (
                SELECT 1 FROM results r
                WHERE r.cycle_id = c.id AND r.endpoint_key = ? AND r.skipped = 0
            )
            AND NOT EXISTS (
                SELECT 1 FROM results r
                WHERE r.cycle_id = c.id AND r.endpoint_key = ?
                  AND r.ok = 0 AND r.skipped = 0
            )
        """
        with self._lock:
            row = self._conn.execute(query, (endpoint_key, endpoint_key)).fetchone()
        return row["ts"] if row and row["ts"] else None

    def prune(self, before_iso: str) -> int:
        with self._lock:
            ids = [
                r["id"]
                for r in self._conn.execute(
                    "SELECT id FROM cycles WHERE started_at < ?", (before_iso,)
                ).fetchall()
            ]
            if ids:
                marks = ",".join("?" for _ in ids)
                self._conn.execute(f"DELETE FROM results WHERE cycle_id IN ({marks})", ids)
                self._conn.execute(f"DELETE FROM cycles WHERE id IN ({marks})", ids)
                self._conn.commit()
        return len(ids)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
