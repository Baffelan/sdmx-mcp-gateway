"""
Session Manager for SDMX MCP Gateway.

This module provides per-session endpoint tracking to support multi-user
deployments. Each session maintains its own endpoint configuration,
preventing interference between users.

Architecture:
    - STDIO transport: Single session, session_id = "default"
    - HTTP transport: Multiple sessions via Mcp-Session-Id header

Usage:
    manager = SessionManager()

    # Get or create session state
    state = manager.get_session("session-123")

    # Switch endpoint for a specific session
    await manager.switch_session_endpoint("session-123", "ECB")

    # Get or create a pooled client for the session's default endpoint
    client = await state.get_or_create_client(state.default_endpoint_key)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sdmx_progressive_client import SDMXProgressiveClient

if TYPE_CHECKING:
    from mcp.server.fastmcp import Context

logger = logging.getLogger(__name__)

# Default session ID for STDIO transport (single-user)
DEFAULT_SESSION_ID = "default"

# Session timeout for cleanup (30 minutes of inactivity)
SESSION_TIMEOUT_MINUTES = 30

# HTTP-transport marker. Set by main_server when starting streamable-http.
# When True, silent fallback to DEFAULT_SESSION_ID in get_session_id_from_context
# emits a rate-limited WARNING because it breaks per-user isolation.
# STDIO legitimately uses DEFAULT_SESSION_ID; no warning there.
_HTTP_TRANSPORT_ACTIVE: bool = False
# Count of fallbacks; warnings fire at 1, 10, 100, 1000, ... (powers of 10).
_fallback_count: int = 0


def mark_http_transport_active() -> None:
    """Called by main_server when serving streamable-http so the
    session-id-fallback warning fires. Calling this is a no-op for STDIO."""
    global _HTTP_TRANSPORT_ACTIVE
    _HTTP_TRANSPORT_ACTIVE = True


def _now_utc() -> datetime:
    """Get current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


@dataclass
class SessionState:
    """
    State for a single user session.

    Holds a pool of SDMX clients keyed by endpoint and a default endpoint
    pointer. Clients are lazily created on first use per endpoint.
    """

    session_id: str
    default_endpoint_key: str
    clients: dict[str, SDMXProgressiveClient] = field(default_factory=dict)
    pending: dict[str, asyncio.Task[SDMXProgressiveClient]] = field(default_factory=dict)
    known_dataflows: dict[str, set[str]] = field(default_factory=dict)
    cache: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_now_utc)
    last_accessed: datetime = field(default_factory=_now_utc)

    def touch(self) -> None:
        """Update last accessed time."""
        self.last_accessed = _now_utc()

    def is_expired(self, timeout_minutes: int = SESSION_TIMEOUT_MINUTES) -> bool:
        """Check if session has expired due to inactivity."""
        cutoff = _now_utc() - timedelta(minutes=timeout_minutes)
        return self.last_accessed < cutoff

    def clear_cache(self) -> None:
        """Clear session-specific cache and every pool client's caches."""
        self.cache.clear()
        for client in self.clients.values():
            client_cache = getattr(client, "_cache", None)
            if isinstance(client_cache, dict):
                client_cache.clear()
            version_cache = getattr(client, "version_cache", None)
            if isinstance(version_cache, dict):
                version_cache.clear()

    async def get_or_create_client(
        self, endpoint_key: str
    ) -> SDMXProgressiveClient:
        """
        Return the pool client for endpoint_key, creating one if needed.

        Safe under concurrent callers: the first one starts creation,
        subsequent concurrent callers await the same Task.
        """
        if endpoint_key in self.clients:
            return self.clients[endpoint_key]
        if endpoint_key in self.pending:
            return await self.pending[endpoint_key]

        from config import SDMX_ENDPOINTS

        cfg = SDMX_ENDPOINTS[endpoint_key]

        async def _build() -> SDMXProgressiveClient:
            return SDMXProgressiveClient(
                base_url=cfg["base_url"],
                agency_id=cfg["agency_id"],
                endpoint_key=endpoint_key,
            )

        task = asyncio.create_task(_build())
        self.pending[endpoint_key] = task
        try:
            client = await task
            self.clients[endpoint_key] = client
            return client
        finally:
            self.pending.pop(endpoint_key, None)

    def register_dataflow(self, endpoint_key: str, dataflow_id: str) -> None:
        """Record that dataflow_id is known to exist on endpoint_key."""
        self.known_dataflows.setdefault(endpoint_key, set()).add(dataflow_id)

    async def close(self) -> None:
        """Close every client whose httpx session was opened."""
        targets = [c for c in self.clients.values() if c.session is not None]
        if not targets:
            self.clients.clear()
            return
        results = await asyncio.gather(
            *(c.close() for c in targets),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                logger.warning("client.close() failed: %s", r)
        self.clients.clear()


class SessionManager:
    """
    Manages multiple concurrent user sessions.

    This class enables multi-user support by tracking endpoint configuration
    per session. Each session has its own SDMX client instance.

    Thread Safety:
        Async-path mutations (switch_endpoint, close_session, close_all,
        cleanup_expired_sessions) are serialised by an asyncio.Lock.
        Sync-path mutations to the _sessions dict (get_session's
        check-and-insert) use a threading.Lock so the race is safe under
        both asyncio cooperative scheduling and real thread concurrency
        (ASGI workers, background threads, etc.).

    Memory Management:
        Sessions are automatically cleaned up after SESSION_TIMEOUT_MINUTES
        of inactivity when cleanup_expired_sessions() is called.
    """

    def __init__(self, default_endpoint_key: str = "SPC") -> None:
        """
        Initialize the session manager.

        Args:
            default_endpoint_key: Default endpoint for new sessions
        """
        import threading

        self._sessions: dict[str, SessionState] = {}
        self._default_endpoint_key: str = default_endpoint_key
        self._lock: asyncio.Lock = asyncio.Lock()
        # Guards the sync-path check-and-insert in get_session. Acquired for
        # microseconds only; contention is vanishing.
        self._sessions_lock: threading.Lock = threading.Lock()

    @property
    def active_session_count(self) -> int:
        """Number of active sessions."""
        return len(self._sessions)

    @property
    def session_ids(self) -> list[str]:
        """List of active session IDs."""
        return list(self._sessions.keys())

    def _get_endpoint_config(self, endpoint_key: str) -> dict[str, str]:
        """Get endpoint configuration by key."""
        # Import here to avoid circular imports
        from config import SDMX_ENDPOINTS

        if endpoint_key not in SDMX_ENDPOINTS:
            raise ValueError(
                f"Unknown endpoint: {endpoint_key}. Available: {', '.join(SDMX_ENDPOINTS.keys())}"
            )

        config: dict[str, str] = SDMX_ENDPOINTS[endpoint_key]
        return config

    def _create_session(self, session_id: str, endpoint_key: str | None = None) -> SessionState:
        """Create a new session with the given default endpoint key."""
        key = endpoint_key or self._default_endpoint_key
        # Validate up front so a bad default fails loudly at session creation,
        # not on first tool call.
        self._get_endpoint_config(key)
        return SessionState(
            session_id=session_id,
            default_endpoint_key=key,
        )

    def get_session(self, session_id: str | None = None) -> SessionState:
        """
        Get or create a session by ID.

        For STDIO transport, use session_id=None or DEFAULT_SESSION_ID.
        For HTTP transport, pass the Mcp-Session-Id header value.

        Args:
            session_id: Session identifier (None uses default)

        Returns:
            SessionState for the specified session
        """
        sid = session_id or DEFAULT_SESSION_ID

        # Race-safe check-and-insert. Under contention, at most one caller
        # invokes _create_session; the rest observe the winner. _create_session
        # is cheap (no HTTP, no pool construction) so holding the lock across
        # it is fine.
        with self._sessions_lock:
            session = self._sessions.get(sid)
            if session is None:
                session = self._create_session(sid)
                self._sessions[sid] = session
                logger.debug("Created new session: %s", sid)

        session.touch()
        return session

    def has_session(self, session_id: str | None = None) -> bool:
        """Check if a session exists."""
        sid = session_id or DEFAULT_SESSION_ID
        return sid in self._sessions

    async def switch_endpoint(
        self,
        endpoint_key: str,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Flip the session's default endpoint pointer.

        Does not tear down pooled clients; they stay warm for future calls.
        Only mutates the `default_endpoint_key` scalar.
        """
        sid = session_id or DEFAULT_SESSION_ID
        cfg = self._get_endpoint_config(endpoint_key)

        async with self._lock:
            session = self._sessions.get(sid)
            if session is None:
                session = self._create_session(sid, endpoint_key)
                self._sessions[sid] = session
                old_endpoint = None
            else:
                old_endpoint = session.default_endpoint_key
                session.default_endpoint_key = endpoint_key
                session.touch()

        logger.info("Session %s: switched from %s to %s", sid, old_endpoint, endpoint_key)

        return {
            "success": True,
            "session_id": sid,
            "previous_endpoint": old_endpoint,
            "new_endpoint": {
                "key": endpoint_key,
                "name": cfg["name"],
                "base_url": cfg["base_url"],
                "agency_id": cfg["agency_id"],
                "description": cfg.get("description", ""),
            },
        }

    async def close_session(self, session_id: str | None = None) -> bool:
        """
        Close and remove a session.

        Args:
            session_id: Session to close (None uses default)

        Returns:
            True if session was closed, False if it didn't exist
        """
        sid = session_id or DEFAULT_SESSION_ID

        async with self._lock:
            if sid in self._sessions:
                await self._sessions[sid].close()
                del self._sessions[sid]
                logger.debug("Closed session: %s", sid)
                return True

        return False

    async def cleanup_expired_sessions(self) -> int:
        """
        Remove expired sessions to free memory.

        Call this periodically (e.g., every few minutes) in long-running
        HTTP deployments.

        Returns:
            Number of sessions cleaned up
        """
        expired_ids = [sid for sid, session in self._sessions.items() if session.is_expired()]

        count = 0
        async with self._lock:
            for sid in expired_ids:
                if sid in self._sessions:
                    await self._sessions[sid].close()
                    del self._sessions[sid]
                    count += 1

        if count > 0:
            logger.info("Cleaned up %d expired sessions", count)

        return count

    async def close_all(self) -> None:
        """
        Close all sessions.

        Call this during server shutdown.
        """
        async with self._lock:
            for session in self._sessions.values():
                await session.close()
            self._sessions.clear()

        logger.debug("Closed all sessions")

    def get_session_info(self, session_id: str | None = None) -> dict[str, Any] | None:
        """Get information about a session."""
        sid = session_id or DEFAULT_SESSION_ID
        if sid not in self._sessions:
            return None

        session = self._sessions[sid]
        cfg = self._get_endpoint_config(session.default_endpoint_key)
        return {
            "session_id": session.session_id,
            "endpoint_key": session.default_endpoint_key,
            "endpoint_name": cfg["name"],
            "base_url": cfg["base_url"],
            "agency_id": cfg["agency_id"],
            "created_at": session.created_at.isoformat(),
            "last_accessed": session.last_accessed.isoformat(),
            "is_expired": session.is_expired(),
            "pool_endpoints": sorted(session.clients.keys()),
        }

    def list_sessions(self) -> list[dict[str, Any]]:
        """
        List all active sessions with their info.

        Returns:
            List of session information dicts
        """
        result: list[dict[str, Any]] = []
        for sid in self._sessions:
            info = self.get_session_info(sid)
            if info is not None:
                result.append(info)
        return result


def _fallback_to_default_session_id(reason: str) -> str:
    """
    Return DEFAULT_SESSION_ID and, under HTTP transport, emit a rate-limited
    WARNING because falling back collapses every caller into a shared session.

    Rate-limit strategy: warn at powers of 10 (1, 10, 100, 1000, ...) so first
    occurrences are loud but a persistent misconfiguration doesn't flood the log.
    Called from multiple sites so operators see one signal regardless of which
    extraction path gave up.
    """
    if _HTTP_TRANSPORT_ACTIVE:
        global _fallback_count
        _fallback_count += 1
        count = _fallback_count
        if count == 10 ** (len(str(count)) - 1):
            logger.warning(
                "MCP session id could not be extracted from ctx (%s); falling back "
                "to DEFAULT_SESSION_ID=%r. Under HTTP transport this collapses "
                "every caller into a shared session and breaks per-user "
                "isolation. Seen %d time(s). Check the transport layer's "
                "Mcp-Session-Id handling.",
                reason, DEFAULT_SESSION_ID, count,
            )
    return DEFAULT_SESSION_ID


def get_session_id_from_context(ctx: Context[Any, Any, Any] | None) -> str:
    """
    Extract session ID from MCP context.

    For HTTP transport, this attempts to get the Mcp-Session-Id.
    For STDIO transport, returns the default session ID.

    Args:
        ctx: MCP Context object

    Returns:
        Session ID string
    """
    if ctx is None:
        return _fallback_to_default_session_id("ctx is None")

    # Try various ways to get session ID
    # The exact method depends on SDK version and transport

    # Method 1: Direct session attribute
    try:
        session_obj = ctx.session
        if session_obj is not None and hasattr(session_obj, "id"):
            session_id = getattr(session_obj, "id", None)
            if session_id:
                return str(session_id)
    except (AttributeError, TypeError):
        pass

    # Method 2: Request context
    try:
        request_context = ctx.request_context
        if request_context is not None:
            # Try session_id attribute
            if hasattr(request_context, "session_id"):
                session_id = getattr(request_context, "session_id", None)
                if session_id:
                    return str(session_id)
            # Try meta dict
            if hasattr(request_context, "meta"):
                meta = getattr(request_context, "meta", None)
                if isinstance(meta, dict):
                    session_id = meta.get("session_id")
                    if session_id:
                        return str(session_id)
    except (AttributeError, TypeError):
        pass

    # Method 3: Meta attribute on context itself
    try:
        if hasattr(ctx, "meta"):
            meta = getattr(ctx, "meta", None)
            if isinstance(meta, dict):
                session_id = meta.get("session_id")
                if session_id:
                    return str(session_id)
    except (AttributeError, TypeError):
        pass

    return _fallback_to_default_session_id("no session-id found in ctx")
