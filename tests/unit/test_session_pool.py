"""Tests for SessionState client pool."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from session_manager import SessionState, _now_utc


@pytest.mark.asyncio
async def test_pool_reuse_returns_same_client():
    """Second call for the same endpoint returns the existing client."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    c1 = await state.get_or_create_client("SPC")
    c2 = await state.get_or_create_client("SPC")
    assert c1 is c2


@pytest.mark.asyncio
async def test_pool_isolation_distinct_endpoints():
    """Different endpoints yield distinct client instances."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    c_spc = await state.get_or_create_client("SPC")
    c_ecb = await state.get_or_create_client("ECB")
    assert c_spc is not c_ecb
    assert c_spc.endpoint_key == "SPC"
    assert c_ecb.endpoint_key == "ECB"


@pytest.mark.asyncio
async def test_pool_concurrent_creation_shares_client():
    """Two gather'd creations for the same endpoint share one client."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )

    from sdmx_progressive_client import SDMXProgressiveClient

    call_count = 0
    real_init = SDMXProgressiveClient.__init__

    def counting_init(self, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        real_init(self, *args, **kwargs)

    with patch.object(SDMXProgressiveClient, "__init__", counting_init):
        results = await asyncio.gather(
            state.get_or_create_client("SPC"),
            state.get_or_create_client("SPC"),
            state.get_or_create_client("SPC"),
        )

    assert results[0] is results[1] is results[2]
    assert call_count == 1


@pytest.mark.asyncio
async def test_pool_unknown_endpoint_raises():
    """get_or_create_client on an unknown key raises KeyError (from SDMX_ENDPOINTS lookup)."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )
    with pytest.raises(KeyError):
        await state.get_or_create_client("BOGUS")


@pytest.mark.asyncio
async def test_pool_pending_cleanup_on_failure():
    """If _build raises, pending[key] is cleared and a fresh call can succeed."""
    state = SessionState(
        session_id="s1",
        default_endpoint_key="SPC",
    )

    from sdmx_progressive_client import SDMXProgressiveClient

    fail_first = [True]
    real_init = SDMXProgressiveClient.__init__

    def flaky_init(self, *args, **kwargs):
        if fail_first[0]:
            fail_first[0] = False
            raise RuntimeError("simulated build failure")
        real_init(self, *args, **kwargs)

    with patch.object(SDMXProgressiveClient, "__init__", flaky_init):
        with pytest.raises(RuntimeError):
            await state.get_or_create_client("SPC")
        # pending must be cleared
        assert "SPC" not in state.pending
        # Fresh call succeeds now that init no longer raises
        client = await state.get_or_create_client("SPC")
        assert client.endpoint_key == "SPC"


from session_manager import SessionManager


@pytest.mark.asyncio
async def test_close_all_partial_failure_does_not_raise():
    """If one client close() raises, others still get closed and no exception propagates."""
    import types

    mgr = SessionManager(default_endpoint_key="SPC")
    session = mgr.get_session("s1")

    c_spc = await session.get_or_create_client("SPC")
    c_ecb = await session.get_or_create_client("ECB")

    # Fake open sessions so close() is attempted on both
    c_spc.session = object()  # type: ignore[assignment]
    c_ecb.session = object()  # type: ignore[assignment]

    closed: list[str] = []

    async def good_close(self):
        closed.append(self.endpoint_key)

    async def bad_close(self):
        closed.append(self.endpoint_key)
        raise RuntimeError("boom")

    # Patch each instance's close directly
    c_spc.close = types.MethodType(bad_close, c_spc)  # type: ignore[assignment]
    c_ecb.close = types.MethodType(good_close, c_ecb)  # type: ignore[assignment]
    await session.close()

    assert set(closed) == {"SPC", "ECB"}
    assert session.clients == {}


def test_get_session_is_race_safe_against_injected_create_delay():
    """Concurrent first-touch requests for the same session_id must all
    resolve to the same SessionState instance; no double-create.

    The audit's H1 failure mode: two threads both see `sid not in _sessions`,
    both call `_create_session`, and the second insert overwrites the first.
    The losing SessionState's future register_dataflow / get_or_create_client
    writes leak into an orphaned object.

    We expose the race deterministically by wrapping `_create_session` with a
    sleep that's longer than the context switch, so threads reliably interleave
    between the membership check and the insert. Without a lock on that
    critical section the test fails; with one, all callers see the same
    SessionState.
    """
    import threading
    import time

    mgr = SessionManager(default_endpoint_key="SPC")

    # Wrap _create_session with a sleep long enough to guarantee interleaving.
    original_create = mgr._create_session

    def slow_create(sid: str, endpoint_key: str | None = None):
        time.sleep(0.05)
        return original_create(sid, endpoint_key)

    mgr._create_session = slow_create  # type: ignore[method-assign]

    n_threads = 6
    barrier = threading.Barrier(n_threads)
    results: list[SessionState] = []
    results_lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        session = mgr.get_session("race-session")
        with results_lock:
            results.append(session)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n_threads
    winner = mgr._sessions["race-session"]
    assert all(r is winner for r in results), (
        "Expected all threads to observe the same SessionState instance; "
        "got " + str(len({id(r) for r in results})) + " distinct instances. "
        "This is the H1 race condition: concurrent check-then-insert."
    )


def test_session_id_fallback_silent_in_stdio_mode(caplog):
    """Under STDIO (default), the session-id extraction fallback is normal
    and should NOT log a WARNING. Every request legitimately uses
    DEFAULT_SESSION_ID in STDIO."""
    import logging
    import session_manager

    # Ensure we're in STDIO mode for this test
    original = session_manager._HTTP_TRANSPORT_ACTIVE
    session_manager._HTTP_TRANSPORT_ACTIVE = False
    try:
        caplog.set_level(logging.WARNING, logger="session_manager")
        sid = session_manager.get_session_id_from_context(None)
        assert sid == session_manager.DEFAULT_SESSION_ID
        assert not any(
            "DEFAULT_SESSION_ID" in r.message for r in caplog.records
        ), "STDIO should not warn on fallback; got: " + str([r.message for r in caplog.records])
    finally:
        session_manager._HTTP_TRANSPORT_ACTIVE = original


def test_session_id_fallback_warns_in_http_mode(caplog):
    """Under HTTP transport, missing Mcp-Session-Id is a bug signal and must
    log a WARNING on first occurrence so operators notice the isolation break."""
    import logging
    import session_manager

    original_flag = session_manager._HTTP_TRANSPORT_ACTIVE
    original_count = session_manager._fallback_count
    session_manager._HTTP_TRANSPORT_ACTIVE = True
    session_manager._fallback_count = 0
    try:
        caplog.set_level(logging.WARNING, logger="session_manager")
        sid = session_manager.get_session_id_from_context(None)
        assert sid == session_manager.DEFAULT_SESSION_ID
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert warnings, "Expected a WARNING on first HTTP fallback"
        assert "DEFAULT_SESSION_ID" in warnings[0].message
        assert "isolation" in warnings[0].message.lower()
    finally:
        session_manager._HTTP_TRANSPORT_ACTIVE = original_flag
        session_manager._fallback_count = original_count


def test_session_id_fallback_rate_limited_to_powers_of_ten(caplog):
    """The warning must fire at counts 1, 10, 100 — not on every single call.
    A persistent misconfiguration should be noticed, not flood the log."""
    import logging
    import session_manager

    original_flag = session_manager._HTTP_TRANSPORT_ACTIVE
    original_count = session_manager._fallback_count
    session_manager._HTTP_TRANSPORT_ACTIVE = True
    session_manager._fallback_count = 0
    try:
        caplog.set_level(logging.WARNING, logger="session_manager")
        # Call 100 times; expect warnings only at counts 1, 10, 100 = 3 total.
        for _ in range(100):
            session_manager.get_session_id_from_context(None)
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 3, (
            "Expected 3 warnings (at counts 1, 10, 100); got "
            + str(len(warnings)) + ": " + str([r.message for r in warnings])
        )
    finally:
        session_manager._HTTP_TRANSPORT_ACTIVE = original_flag
        session_manager._fallback_count = original_count


def test_get_session_concurrent_with_iteration_does_not_raise():
    """Audit H1 follow-up: the _sessions dict is accessed from multiple
    paths (get_session, list_sessions, cleanup_expired_sessions,
    get_session_info). The H1 fix only locked get_session's check-and-insert;
    a concurrent iterator in another thread still raises
    `RuntimeError: dictionary changed size during iteration`.

    This test reproduces the reviewer's failure mode: one thread calls
    get_session() repeatedly to grow the dict, another iterates it via
    list_sessions(). Under unified locking, neither raises.
    """
    import threading
    import time

    mgr = SessionManager(default_endpoint_key="SPC")
    # Pre-seed so the iterator has something to see immediately.
    for i in range(50):
        mgr.get_session("pre-" + str(i))

    stop = threading.Event()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def creator() -> None:
        i = 0
        while not stop.is_set():
            try:
                mgr.get_session("live-" + str(i))
            except BaseException as e:
                with errors_lock:
                    errors.append(e)
                return
            i += 1

    def iterator() -> None:
        while not stop.is_set():
            try:
                mgr.list_sessions()
            except BaseException as e:
                with errors_lock:
                    errors.append(e)
                return

    workers = [
        threading.Thread(target=creator),
        threading.Thread(target=creator),
        threading.Thread(target=iterator),
        threading.Thread(target=iterator),
    ]
    for w in workers:
        w.start()
    time.sleep(0.3)
    stop.set()
    for w in workers:
        w.join(timeout=2.0)

    assert not errors, (
        "Expected zero exceptions under concurrent get_session / "
        "list_sessions. Got " + str(len(errors)) + ": "
        + repr([type(e).__name__ + ": " + str(e) for e in errors[:3]])
    )


def test_register_dataflow_concurrent_with_snapshot_is_atomic():
    """Audit M3: register_dataflow's setdefault+add is two operations
    with a yield window between them. Concurrent writers for a previously-
    unseen endpoint key could each create a fresh set and one's addition
    gets lost. Concurrent readers iterating known_dataflows.items() could
    also raise "dictionary changed size during iteration".

    The _state_lock + snapshot_known_dataflows helper close both windows.
    This stress test races 4 writers on distinct (previously-missing)
    endpoint keys against 4 snapshot readers and asserts:
      a. every written dataflow id ends up in the registry
      b. readers never raise
    """
    import threading
    import time

    from session_manager import SessionState

    state = SessionState(session_id="m3", default_endpoint_key="SPC")

    n_writers = 4
    n_readers = 4
    per_writer = 200
    stop = threading.Event()
    reader_errors: list[BaseException] = []
    reader_errors_lock = threading.Lock()

    def writer(prefix: str) -> None:
        # Each writer uses its own endpoint key so the setdefault race
        # on missing-key is triggered repeatedly across writers.
        for i in range(per_writer):
            state.register_dataflow("EP_" + prefix, "DF_" + prefix + "_" + str(i))

    def reader() -> None:
        while not stop.is_set():
            try:
                snap = state.snapshot_known_dataflows()
                # Consume to expose any laziness
                sum(len(v) for v in snap.values())
            except BaseException as e:
                with reader_errors_lock:
                    reader_errors.append(e)
                return

    writers = [
        threading.Thread(target=writer, args=(chr(ord("A") + i),))
        for i in range(n_writers)
    ]
    readers = [threading.Thread(target=reader) for _ in range(n_readers)]

    for t in readers:
        t.start()
    for t in writers:
        t.start()
    for t in writers:
        t.join()
    stop.set()
    for t in readers:
        t.join(timeout=2.0)

    assert not reader_errors, (
        "Expected zero reader exceptions; got " + str(len(reader_errors))
        + ": " + repr([type(e).__name__ + ": " + str(e) for e in reader_errors[:3]])
    )
    # Every written entry must be present. No race-swallowed additions.
    for i in range(n_writers):
        prefix = chr(ord("A") + i)
        got = state.known_dataflows["EP_" + prefix]
        expected = {"DF_" + prefix + "_" + str(j) for j in range(per_writer)}
        assert got == expected, (
            "EP_" + prefix + ": expected " + str(len(expected))
            + " entries, got " + str(len(got))
        )


