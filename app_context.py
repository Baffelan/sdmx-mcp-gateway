"""
Application Context and Lifespan Management for SDMX MCP Gateway.

This module implements the lifespan pattern recommended by MCP SDK v2,
providing proper resource initialization and cleanup.

Updated to support multi-user deployments via SessionManager.

Usage:
    mcp = MCPServer("SDMX Gateway", lifespan=app_lifespan)

    @mcp.tool()
    async def my_tool(ctx: Context) -> str:
        # Get session-specific client
        app_ctx = get_app_context_from_ctx(ctx)
        if app_ctx:
            session = app_ctx.get_session(ctx)
            client = session.client
        # Use client...

Note: Logging is intentionally minimal in this module to avoid
interfering with STDIO transport JSON-RPC communication.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from session_manager import (
    DEFAULT_SESSION_ID,
    SessionManager,
    SessionState,
    get_session_id_from_context,
)

if TYPE_CHECKING:
    from mcp.server.fastmcp import Context, FastMCP


@dataclass
class AppContext:
    """
    Application context holding shared resources.

    This context is created during server lifespan initialization
    and is accessible in all tool/resource handlers via:
        ctx.request_context.lifespan_context

    Multi-User Support:
        The session_manager tracks per-session endpoint configuration.
        Use get_session(ctx) to get the current session's state.

    Attributes:
        session_manager: Manager for per-session endpoint tracking
        global_config: Server-wide configuration settings
        cache: Shared cache for expensive operations (use sparingly)
    """

    session_manager: SessionManager
    global_config: dict[str, Any] = field(default_factory=dict)
    cache: dict[str, Any] = field(default_factory=dict)

    def get_session(self, ctx: Context[Any, Any, Any] | None = None) -> SessionState:
        """
        Get the session state for the current request context.

        Args:
            ctx: MCP Context (if None, returns default session)

        Returns:
            SessionState with session-specific client and config
        """
        session_id = get_session_id_from_context(ctx)
        return self.session_manager.get_session(session_id)

    def get_client(self, ctx: Context[Any, Any, Any] | None = None):
        """
        Get the SDMX client for the current session.

        Args:
            ctx: MCP Context (if None, returns default session's client)

        Returns:
            SDMXProgressiveClient for the current session
        """
        return self.get_session(ctx).client

    def get_endpoint_info(self, ctx: Context[Any, Any, Any] | None = None) -> dict[str, Any]:
        """
        Get current endpoint information for the session.

        Args:
            ctx: MCP Context

        Returns:
            Dictionary with endpoint details
        """
        session = self.get_session(ctx)
        return {
            "key": session.endpoint_key,
            "name": session.endpoint_name,
            "base_url": session.base_url,
            "agency_id": session.agency_id,
            "description": session.description,
        }

    async def switch_endpoint(
        self, endpoint_key: str, ctx: Context[Any, Any, Any] | None = None
    ) -> dict[str, Any]:
        """
        Switch endpoint for the current session.

        Args:
            endpoint_key: Target endpoint key (e.g., "ECB", "UNICEF")
            ctx: MCP Context

        Returns:
            Dictionary with switch result information
        """
        session_id = get_session_id_from_context(ctx)
        return await self.session_manager.switch_endpoint(endpoint_key, session_id)

    def clear_cache(self, ctx: Context[Any, Any, Any] | None = None) -> None:
        """
        Clear cache for the current session.

        Args:
            ctx: MCP Context (if None, clears default session's cache)
        """
        session = self.get_session(ctx)
        session.clear_cache()


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """
    Manage application lifecycle with proper resource initialization and cleanup.

    This is the recommended pattern for MCP SDK v2. Resources are initialized
    when the server starts and cleaned up when it stops.

    Multi-User Support:
        The SessionManager is initialized here and handles per-session
        endpoint tracking. Sessions are created on-demand when tools
        access them.

    Args:
        server: The MCP server instance (FastMCP or MCPServer)

    Yields:
        AppContext: The application context with initialized resources

    Example:
        from mcp.server.fastmcp import FastMCP
        from app_context import app_lifespan, AppContext

        mcp = FastMCP("My Server", lifespan=app_lifespan)

        @mcp.tool()
        async def my_tool(ctx: Context) -> str:
            # Get session-specific client
            app_ctx = ctx.request_context.lifespan_context
            client = app_ctx.get_client(ctx)
            return await client.some_method()
    """
    # Suppress unused variable warning - server is required by the lifespan protocol
    _ = server

    # Get default endpoint from environment or use SPC
    default_endpoint = os.getenv("SDMX_ENDPOINT", "SPC")

    # Initialize the session manager
    session_manager = SessionManager(default_endpoint_key=default_endpoint)

    # Create the application context
    context = AppContext(
        session_manager=session_manager,
        global_config={
            "default_endpoint": default_endpoint,
            "server_name": "SDMX Data Gateway",
        },
    )

    try:
        yield context
    finally:
        # Cleanup all sessions on shutdown - no logging to avoid STDIO interference
        await session_manager.close_all()


async def switch_endpoint_context(context: AppContext, endpoint_key: str) -> dict[str, Any]:
    """
    Switch the SDMX endpoint in the application context (default session).

    This is a convenience function for backward compatibility.
    For multi-user support, use context.switch_endpoint(endpoint_key, ctx) instead.

    Args:
        context: The current application context
        endpoint_key: Key of the endpoint to switch to

    Returns:
        Dictionary with switch result information

    Raises:
        ValueError: If the endpoint_key is not recognized
    """
    return await context.switch_endpoint(endpoint_key, ctx=None)


# Backward compatibility: expose SessionManager types
__all__ = [
    "AppContext",
    "app_lifespan",
    "switch_endpoint_context",
    "DEFAULT_SESSION_ID",
    "SessionState",
    "get_session_id_from_context",
]
