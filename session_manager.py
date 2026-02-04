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

    # Get session's current client
    client = state.client
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


def _now_utc() -> datetime:
    """Get current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


@dataclass
class SessionState:
    """
    State for a single user session.

    Each session maintains its own:
    - SDMX client instance
    - Endpoint configuration
    - Cache for expensive operations
    """

    session_id: str
    endpoint_key: str
    endpoint_name: str
    base_url: str
    agency_id: str
    description: str
    client: SDMXProgressiveClient
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
        """Clear session-specific cache."""
        self.cache.clear()
        # Also clear the client's internal caches if they exist
        client_cache = getattr(self.client, "_cache", None)
        if isinstance(client_cache, dict):
            client_cache.clear()
        version_cache = getattr(self.client, "version_cache", None)
        if isinstance(version_cache, dict):
            version_cache.clear()

    async def close(self) -> None:
        """Close the session's SDMX client."""
        if self.client and self.client.session is not None:
            await self.client.close()


class SessionManager:
    """
    Manages multiple concurrent user sessions.

    This class enables multi-user support by tracking endpoint configuration
    per session. Each session has its own SDMX client instance.

    Thread Safety:
        This implementation uses asyncio.Lock for critical sections.

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
        self._sessions: dict[str, SessionState] = {}
        self._default_endpoint_key: str = default_endpoint_key
        self._lock: asyncio.Lock = asyncio.Lock()

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
        """
        Create a new session with the specified or default endpoint.

        Args:
            session_id: Unique session identifier
            endpoint_key: Optional endpoint key (uses default if not specified)

        Returns:
            New SessionState instance
        """
        key = endpoint_key or self._default_endpoint_key
        config = self._get_endpoint_config(key)

        client = SDMXProgressiveClient(
            base_url=config["base_url"],
            agency_id=config["agency_id"],
        )

        return SessionState(
            session_id=session_id,
            endpoint_key=key,
            endpoint_name=config["name"],
            base_url=config["base_url"],
            agency_id=config["agency_id"],
            description=config.get("description", ""),
            client=client,
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

        if sid not in self._sessions:
            self._sessions[sid] = self._create_session(sid)
            logger.debug("Created new session: %s", sid)

        session = self._sessions[sid]
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
        Switch endpoint for a specific session.

        This closes the old client and creates a new one configured
        for the new endpoint. Only affects the specified session.

        Args:
            endpoint_key: Target endpoint key (e.g., "ECB", "UNICEF")
            session_id: Session to switch (None uses default)

        Returns:
            Dictionary with switch result information

        Raises:
            ValueError: If endpoint_key is not recognized
        """
        sid = session_id or DEFAULT_SESSION_ID

        # Validate endpoint first
        config = self._get_endpoint_config(endpoint_key)

        async with self._lock:
            # Get or create session
            old_endpoint: str | None = None
            if sid in self._sessions:
                old_session = self._sessions[sid]
                old_endpoint = old_session.endpoint_key

                # Close old client
                await old_session.close()

            # Create new session with new endpoint
            self._sessions[sid] = self._create_session(sid, endpoint_key)

        logger.info("Session %s: switched from %s to %s", sid, old_endpoint, endpoint_key)

        return {
            "success": True,
            "session_id": sid,
            "previous_endpoint": old_endpoint,
            "new_endpoint": {
                "key": endpoint_key,
                "name": config["name"],
                "base_url": config["base_url"],
                "agency_id": config["agency_id"],
                "description": config.get("description", ""),
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
        """
        Get information about a session.

        Args:
            session_id: Session to query (None uses default)

        Returns:
            Session information dict, or None if session doesn't exist
        """
        sid = session_id or DEFAULT_SESSION_ID

        if sid not in self._sessions:
            return None

        session = self._sessions[sid]
        return {
            "session_id": session.session_id,
            "endpoint_key": session.endpoint_key,
            "endpoint_name": session.endpoint_name,
            "base_url": session.base_url,
            "agency_id": session.agency_id,
            "created_at": session.created_at.isoformat(),
            "last_accessed": session.last_accessed.isoformat(),
            "is_expired": session.is_expired(),
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
        return DEFAULT_SESSION_ID

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

    # Fallback to default
    return DEFAULT_SESSION_ID
