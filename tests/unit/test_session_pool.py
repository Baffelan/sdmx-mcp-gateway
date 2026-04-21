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
async def test_switch_endpoint_is_pointer_flip():
    """switch_endpoint flips default_endpoint_key without tearing down pooled clients."""
    mgr = SessionManager(default_endpoint_key="SPC")
    # Prime the pool by touching a client on SPC
    session = mgr.get_session("s1")
    spc_client = await session.get_or_create_client("SPC")

    await mgr.switch_endpoint("ECB", session_id="s1")

    # default flipped
    assert session.default_endpoint_key == "ECB"
    # previous client still in the pool (not closed, not replaced)
    assert session.clients["SPC"] is spc_client
    # no httpx session opened yet means close() never runs; verify by
    # re-fetching the SPC client yields the same instance
    assert await session.get_or_create_client("SPC") is spc_client


@pytest.mark.asyncio
async def test_switch_endpoint_unknown_raises_with_valid_list():
    mgr = SessionManager(default_endpoint_key="SPC")
    mgr.get_session("s1")
    with pytest.raises(ValueError) as exc:
        await mgr.switch_endpoint("BOGUS", session_id="s1")
    msg = str(exc.value)
    assert "BOGUS" in msg
    assert "SPC" in msg  # valid list surfaced


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


def test_config_set_endpoint_emits_deprecation_warning():
    """Audit H3: set_endpoint mutates process-wide globals and is not
    multi-user safe. A DeprecationWarning must fire so callers notice."""
    import warnings as _warnings
    import config

    with _warnings.catch_warnings(record=True) as captured:
        _warnings.simplefilter("always")
        try:
            config.set_endpoint("ECB")
        finally:
            # Restore the original endpoint so the process-wide mutation
            # doesn't leak into other tests.
            config.set_endpoint("SPC")

    dep = [w for w in captured if issubclass(w.category, DeprecationWarning)]
    assert dep, "Expected a DeprecationWarning from config.set_endpoint()"
    assert "multi-user" in str(dep[0].message).lower() \
        or "AppContext" in str(dep[0].message)


def test_client_construction_without_kwargs_warns_once(caplog):
    """Audit H3: constructing SDMXProgressiveClient without explicit
    base_url/agency_id silently uses startup-time module globals that
    do NOT track config.set_endpoint() calls at runtime. Log a WARNING
    on first occurrence per process."""
    import logging

    import sdmx_progressive_client as spc_mod
    from sdmx_progressive_client import SDMXProgressiveClient

    original = spc_mod._warned_client_global_default
    spc_mod._warned_client_global_default = False
    try:
        caplog.set_level(logging.WARNING, logger="sdmx_progressive_client")
        SDMXProgressiveClient()  # no kwargs — fallback path
        SDMXProgressiveClient()  # second call: warning must NOT fire again

        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING" and "SDMX_BASE_URL" in r.message
        ]
        assert len(warnings) == 1, (
            "Expected exactly one warning across two constructions; got "
            + str(len(warnings))
        )
    finally:
        spc_mod._warned_client_global_default = original


def test_client_construction_with_kwargs_does_not_warn(caplog):
    """When constructed with explicit kwargs (the pooled path), no warning
    fires — this is the safe path."""
    import logging

    import sdmx_progressive_client as spc_mod
    from sdmx_progressive_client import SDMXProgressiveClient

    original = spc_mod._warned_client_global_default
    spc_mod._warned_client_global_default = False
    try:
        caplog.set_level(logging.WARNING, logger="sdmx_progressive_client")
        SDMXProgressiveClient(base_url="https://x/rest", agency_id="Y")
        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING" and "SDMX_BASE_URL" in r.message
        ]
        assert not warnings, (
            "Explicit-kwargs path must not warn; got: "
            + str([r.message for r in warnings])
        )
    finally:
        spc_mod._warned_client_global_default = original


def test_client_default_does_not_follow_set_endpoint():
    """Regression for review of H3: `from config import SDMX_BASE_URL`
    captures the value at import time, so config.set_endpoint() later does
    NOT update what SDMXProgressiveClient() reads by default. The H3
    warning text must describe this correctly (startup-time pinned, not
    mutated at runtime)."""
    import warnings as _warnings

    import config
    import sdmx_progressive_client as spc_mod
    from sdmx_progressive_client import SDMXProgressiveClient

    original_key = config._current_endpoint_key
    try:
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore")
            config.set_endpoint("ECB")
            # The client-side module's imported value did NOT track the switch.
            assert spc_mod.SDMX_BASE_URL != config.SDMX_BASE_URL, (
                "Expected spc_mod.SDMX_BASE_URL to stay on the import-time "
                "value while config.SDMX_BASE_URL follows set_endpoint — "
                "this divergence is why the H3 warning text was reframed."
            )
            bare = SDMXProgressiveClient()
            # Bare client reads the frozen spc_mod default, not the new config value.
            assert bare.base_url == spc_mod.SDMX_BASE_URL.rstrip("/")
            assert bare.base_url != config.SDMX_BASE_URL.rstrip("/")
    finally:
        with _warnings.catch_warnings():
            _warnings.simplefilter("ignore")
            config.set_endpoint(original_key)


def test_legacy_singleton_import_does_not_emit_h3_warning(caplog):
    """Review of H3: `tools.sdmx_tools` eagerly constructs `sdmx_client`
    at module import time. Before the fix, that construction fired the H3
    no-kwargs warning on every normal import — healthy AppContext
    deployments saw it too, which made the warning noise. The singleton
    now passes explicit kwargs so the warning stays quiet for the
    deliberate process-wide default."""
    import importlib
    import logging

    import sdmx_progressive_client as spc_mod

    original = spc_mod._warned_client_global_default
    spc_mod._warned_client_global_default = False
    try:
        caplog.set_level(logging.WARNING, logger="sdmx_progressive_client")
        import tools.sdmx_tools as sdmx_tools_mod
        importlib.reload(sdmx_tools_mod)
        warnings = [
            r for r in caplog.records
            if r.levelname == "WARNING" and "SDMX_BASE_URL" in r.message
        ]
        assert not warnings, (
            "Legacy singleton import must not fire H3 warning; got: "
            + str([r.message for r in warnings])
        )
    finally:
        spc_mod._warned_client_global_default = original


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
