"""
SDMX MCP Gateway Server

A Model Context Protocol server for progressive SDMX data discovery.
Provides tools, resources, and prompts for exploring statistical data.

Supports both STDIO (development) and Streamable HTTP (production) transports.

Usage:
    # Development (STDIO)
    python main_server.py

    # Production (Streamable HTTP)
    python main_server.py --transport http

    # With MCP Inspector
    mcp dev ./main_server.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from mcp.server.fastmcp import Context, FastMCP

# Import lifespan and context
from app_context import AppContext, app_lifespan

# Import structured output models
from models.schemas import (
    CodeInfo,
    DataAvailabilityResult,
    DataflowInfo,
    DataflowListResult,
    DataflowStructureResult,
    DataflowSummary,
    DataUrlResult,
    DimensionCodesResult,
    DimensionInfo,
    EndpointInfo,
    EndpointListResult,
    EndpointSwitchConfirmation,
    EndpointSwitchResult,
    FilterInfo,
    KeyBuildResult,
    PaginationInfo,
    StructureInfo,
    TimeRange,
    ValidationResult,
)

# Import resources and prompts
from prompts.sdmx_prompts import (
    sdmx_best_practices,
    sdmx_discovery_guide,
    sdmx_query_builder,
    sdmx_troubleshooting_guide,
)
from resources.sdmx_resources import (
    get_agency_info,
    get_sdmx_format_guide,
    get_sdmx_query_syntax_guide,
    list_known_agencies,
)
from sdmx_progressive_client import SDMXProgressiveClient

# Logger - configured lazily in main() to avoid early writes
logger = logging.getLogger(__name__)

# Initialize FastMCP server with lifespan
mcp = FastMCP(
    "SDMX Data Gateway",
    lifespan=app_lifespan,
)


# =============================================================================
# Helper Functions for Session Management
# =============================================================================


def get_session_client(ctx: Context[Any, Any, Any] | None) -> SDMXProgressiveClient:
    """
    Get the SDMX client for the current session from lifespan context.

    This ensures each session uses its own endpoint configuration,
    preventing interference between users in multi-user deployments.

    Args:
        ctx: MCP Context object

    Returns:
        SDMXProgressiveClient configured for the current session's endpoint
    """
    if ctx is None:
        # Fallback to default session
        from tools.sdmx_tools import get_default_client

        return get_default_client()

    try:
        # Get the lifespan context (AppContext)
        lifespan_ctx = ctx.request_context.lifespan_context
        if isinstance(lifespan_ctx, AppContext):
            return lifespan_ctx.get_client(ctx)
        # Fallback to default
        from tools.sdmx_tools import get_default_client

        return get_default_client()
    except (AttributeError, TypeError):
        # Lifespan context not available, use default
        from tools.sdmx_tools import get_default_client

        return get_default_client()


def get_app_context(ctx: Context[Any, Any, Any] | None) -> AppContext | None:
    """
    Get the AppContext from the lifespan context.

    Args:
        ctx: MCP Context object

    Returns:
        AppContext or None if not available
    """
    if ctx is None:
        return None

    try:
        lifespan_ctx = ctx.request_context.lifespan_context
        if isinstance(lifespan_ctx, AppContext):
            return lifespan_ctx
        return None
    except (AttributeError, TypeError):
        return None


# =============================================================================
# Discovery Tools
# =============================================================================


@mcp.tool()
async def list_dataflows(
    keywords: list[str] | None = None,
    agency_id: str = "SPC",
    limit: int = 10,
    offset: int = 0,
    ctx: Context[Any, Any, Any] | None = None,
) -> DataflowListResult:
    """
    List available SDMX dataflows, optionally filtered by keywords.

    This is typically the first step in SDMX data discovery. Returns a list of
    statistical domains (dataflows) available from the specified agency.

    Args:
        keywords: Optional list of keywords to filter dataflows
        agency_id: The agency to query (default: "SPC")
        limit: Number of results to return (default: 10)
        offset: Number of results to skip for pagination (default: 0)

    Returns:
        Structured result with dataflows, pagination info, and navigation hints
    """
    from tools.sdmx_tools import list_dataflows as list_dataflows_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await list_dataflows_impl(client, keywords, agency_id, limit, offset, ctx)

    # Convert to structured output
    if "error" in result:
        # Return minimal result on error
        return DataflowListResult(
            discovery_level="overview",
            agency_id=agency_id,
            total_found=0,
            showing=0,
            offset=offset,
            limit=limit,
            keywords=keywords,
            dataflows=[],
            pagination=PaginationInfo(
                has_more=False, next_offset=None, total_pages=0, current_page=1
            ),
            next_step=f"Error: {result['error']}",
        )

    # Build structured result
    dataflows = [
        DataflowSummary(
            id=df["id"],
            name=df["name"],
            description=df.get("description", ""),
        )
        for df in result.get("dataflows", [])
    ]

    pagination = PaginationInfo(
        has_more=result.get("pagination", {}).get("has_more", False),
        next_offset=result.get("pagination", {}).get("next_offset"),
        total_pages=result.get("pagination", {}).get("total_pages", 0),
        current_page=result.get("pagination", {}).get("current_page", 1),
    )

    filter_info = None
    if result.get("filter_info"):
        fi = result["filter_info"]
        filter_info = FilterInfo(
            keywords_used=fi.get("keywords_used", []),
            total_before_filter=fi.get("total_before_filter", 0),
            total_after_filter=fi.get("total_after_filter", 0),
            filter_reduced_by=fi.get("filter_reduced_by", 0),
        )

    return DataflowListResult(
        discovery_level=result.get("discovery_level", "overview"),
        agency_id=result.get("agency_id", agency_id),
        total_found=result.get("total_found", len(dataflows)),
        showing=result.get("showing", len(dataflows)),
        offset=result.get("offset", offset),
        limit=result.get("limit", limit),
        keywords=result.get("keywords"),
        dataflows=dataflows,
        pagination=pagination,
        filter_info=filter_info,
        next_step=result.get("next_step", "Use get_dataflow_structure() to explore a dataflow"),
    )


@mcp.tool()
async def get_dataflow_structure(
    dataflow_id: str,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> DataflowStructureResult:
    """
    Get detailed structure information for a specific dataflow.

    Returns dimensions, attributes, measures, and codelist references.
    Use this after list_dataflows() to understand data organization.

    Args:
        dataflow_id: The dataflow identifier
        agency_id: The agency (default: "SPC")

    Returns:
        Structured result with dataflow metadata and structure definition
    """
    from tools.sdmx_tools import get_dataflow_structure as get_structure_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await get_structure_impl(client, dataflow_id, agency_id, ctx)

    if "error" in result:
        # Return minimal structure on error
        return DataflowStructureResult(
            discovery_level="structure",
            dataflow=DataflowInfo(
                id=dataflow_id,
                name=f"Error loading {dataflow_id}",
                description=result["error"],
                version="latest",
            ),
            structure=StructureInfo(
                id="unknown",
                key_template="",
                key_example="",
                dimensions=[],
                attributes=[],
                measure=None,
            ),
            next_steps=[f"Error: {result['error']}"],
        )

    # Build structured result from simplified response
    dataflow = DataflowInfo(
        id=dataflow_id,
        name=result.get("dataflow_name", ""),
        description="",
        version="latest",
    )

    struct_data = result.get("structure", {})
    dimensions = [
        DimensionInfo(
            id=dim.get("id", ""),
            position=dim.get("position", 0),
            type=dim.get("type", "Dimension"),
            codelist=dim.get("codelist"),
        )
        for dim in struct_data.get("dimensions", [])
    ]

    structure = StructureInfo(
        id=struct_data.get("id", ""),
        key_template=struct_data.get("key_template", ""),
        key_example=struct_data.get("key_example", ""),
        dimensions=dimensions,
        attributes=struct_data.get("attributes", []),
        measure=struct_data.get("measure"),
    )

    return DataflowStructureResult(
        discovery_level=result.get("discovery_level", "structure"),
        dataflow=dataflow,
        structure=structure,
        next_steps=result.get("next_steps", []),
    )


@mcp.tool()
async def get_codelist(
    codelist_id: str,
    agency_id: str = "SPC",
    version: str = "latest",
    search_term: str | None = None,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Get codes and values for a specific codelist.

    Codelists define the allowed values for dimensions (e.g., country codes, commodity codes).
    Use this to find the exact codes needed for your data query.

    Args:
        codelist_id: The codelist identifier
        agency_id: The agency (default: "SPC")
        version: Version (default: "latest")
        search_term: Optional search term to filter codes

    Returns:
        Dictionary with codelist information and codes
    """
    # Get session-specific client for multi-user support
    client = get_session_client(ctx)
    result = await client.browse_codelist(codelist_id, agency_id, version, search_term)
    return result


@mcp.tool()
async def get_dimension_codes(
    dataflow_id: str,
    dimension_id: str,
    limit: int = 50,
    offset: int = 0,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> DimensionCodesResult:
    """
    Get codes for a specific dimension of a dataflow.

    This allows drilling down into specific dimensions without loading all codelists at once.
    Useful for finding valid values for a particular dimension in your data query.

    Args:
        dataflow_id: The dataflow identifier
        dimension_id: The dimension identifier
        limit: Maximum codes to return (default: 50)
        offset: Number of codes to skip for pagination (default: 0)
        agency_id: The agency (default: "SPC")

    Returns:
        Structured result with codes for the dimension
    """
    from tools.sdmx_tools import get_dimension_codes as get_codes_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await get_codes_impl(client, dataflow_id, dimension_id, agency_id, limit, offset, ctx)

    if "error" in result:
        return DimensionCodesResult(
            discovery_level="codes",
            dataflow_id=dataflow_id,
            dimension_id=dimension_id,
            position=0,
            codelist_id=None,
            total_codes=0,
            showing=0,
            search_term=None,
            codes=[],
            usage=f"Error: {result['error']}",
            example_keys=[],
        )

    codes = [
        CodeInfo(
            id=code.get("id", ""),
            name=code.get("name", ""),
            description=code.get("description"),
        )
        for code in result.get("codes", [])
    ]

    return DimensionCodesResult(
        discovery_level=result.get("discovery_level", "codes"),
        dataflow_id=result.get("dataflow_id", dataflow_id),
        dimension_id=result.get("dimension_id", dimension_id),
        position=result.get("position", 0),
        codelist_id=result.get("codelist_id"),
        total_codes=result.get("total_codes", len(codes)),
        showing=result.get("showing", len(codes)),
        search_term=None,
        codes=codes,
        usage=result.get("usage", result.get("next_step", "")),
        example_keys=result.get("example_keys", []),
    )


@mcp.tool()
async def get_data_availability(
    dataflow_id: str,
    filters: dict[str, str] | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> DataAvailabilityResult:
    """
    Get actual data availability for a dataflow or specific dimension combinations.

    This tool is critical for avoiding empty query results. Use it to check
    if data exists before building the final data URL.

    Args:
        dataflow_id: The dataflow to check
        filters: Optional dict of dimension=value pairs to check
        agency_id: The agency ID

    Returns:
        Information about what data exists, including time ranges and suggestions
    """
    from tools.sdmx_tools import get_data_availability as get_availability_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await get_availability_impl(
        client=client,
        dataflow_id=dataflow_id,
        filters=filters,
        agency_id=agency_id,
        ctx=ctx,
    )

    # Build time range if present
    time_range = None
    if result.get("time_range"):
        tr = result["time_range"]
        time_range = TimeRange(start=tr.get("start"), end=tr.get("end"))

    return DataAvailabilityResult(
        discovery_level=result.get("discovery_level", "availability"),
        dataflow_id=result.get("dataflow_id", dataflow_id),
        has_constraint=result.get("has_constraint", False),
        constraint_id=result.get("constraint_id"),
        time_range=time_range,
        cube_regions=result.get("cube_regions", []),
        interpretation=result.get("interpretation", []),
        dimension_values_checked=result.get("dimension_values_checked"),
        data_exists=result.get("data_exists"),
        recommendation=result.get("recommendation"),
    )


@mcp.tool()
async def validate_query(
    dataflow_id: str,
    key: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> ValidationResult:
    """
    Validate SDMX query parameters before building the final URL.

    Checks syntax according to SDMX 2.1 REST API specification.
    Validates that dimension codes actually exist in the dataflow.

    Args:
        dataflow_id: The dataflow to validate against
        key: The data key (dimensions separated by dots)
        filters: Dictionary of dimension_id -> code (alternative to key)
        start_period: Start of time range
        end_period: End of time range
        agency_id: The agency

    Returns:
        Validation results including any errors, warnings, and validated parameters
    """
    from tools.sdmx_tools import validate_query as validate_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await validate_impl(
        client=client,
        dataflow_id=dataflow_id,
        key=key,
        filters=filters,
        start_period=start_period,
        end_period=end_period,
        agency_id=agency_id,
        ctx=ctx,
    )

    return ValidationResult(
        valid=result.get("is_valid", False),
        dataflow_id=dataflow_id,
        key=key or "",
        errors=result.get("errors", []),
        warnings=result.get("warnings", []),
        invalid_codes=[],
        suggestion=None,
    )


@mcp.tool()
async def build_key(
    dataflow_id: str,
    filters: dict[str, str] | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> KeyBuildResult:
    """
    Build a properly formatted SDMX key from dimension values.

    This helper tool constructs the key string with dimensions in the correct order
    according to the dataflow structure. Unspecified dimensions are left empty
    (meaning "all values").

    Use this before build_data_url() to ensure your key has the correct format.

    Args:
        dataflow_id: The dataflow identifier
        filters: Optional dict mapping dimension IDs to values
        agency_id: The agency (default: "SPC")

    Returns:
        Structured result with the constructed key and usage information
    """
    from tools.sdmx_tools import build_sdmx_key

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await build_sdmx_key(client, dataflow_id, filters or {}, agency_id, ctx)

    if "error" in result:
        return KeyBuildResult(
            dataflow_id=dataflow_id,
            version="latest",
            key="",
            dimensions_used=filters or {},
            dimensions_wildcard=[],
            key_template="",
            usage=f"Error: {result['error']}",
        )

    return KeyBuildResult(
        dataflow_id=result.get("dataflow_id", dataflow_id),
        version="latest",
        key=result.get("key", ""),
        dimensions_used=result.get("filters_applied", {}),
        dimensions_wildcard=[],
        key_template="",
        usage=result.get("usage", "Use this key in build_data_url()"),
    )


@mcp.tool()
async def build_data_url(
    dataflow_id: str,
    key: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    format_type: str = "csv",
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> DataUrlResult:
    """
    Generate final SDMX REST API URLs for data retrieval.

    Creates URLs that can be used directly to download data in various formats.
    This is the final step in the SDMX query construction process.

    Args:
        dataflow_id: The dataflow to query
        key: The data key (use build_key() to construct), or use filters instead
        filters: Dictionary of dimension_id -> code (alternative to key)
        start_period: Start of time range (optional)
        end_period: End of time range (optional)
        format_type: Output format (csv, json, xml)
        agency_id: The agency (default: "SPC")

    Returns:
        Structured result with the complete data URL and usage information
    """
    from tools.sdmx_tools import build_data_url as build_url_impl

    # Get session-specific client for multi-user support
    client = get_session_client(ctx)

    result = await build_url_impl(
        client=client,
        dataflow_id=dataflow_id,
        key=key,
        filters=filters,
        start_period=start_period,
        end_period=end_period,
        agency_id=agency_id,
        output_format=format_type,
        include_headers=True,
        ctx=ctx,
    )

    if "error" in result:
        return DataUrlResult(
            dataflow_id=dataflow_id,
            version="latest",
            key=key or "",
            format=format_type,
            url="",
            dimension_at_observation="AllDimensions",
            time_range=None,
            usage=f"Error: {result['error']}",
            formats_available=["csv", "json", "xml"],
            note=result.get("hint"),
        )

    # Build time range
    time_range = None
    if result.get("start_period") or result.get("end_period"):
        time_range = TimeRange(start=result.get("start_period"), end=result.get("end_period"))

    return DataUrlResult(
        dataflow_id=dataflow_id,
        version="latest",
        key=result.get("key", ""),
        format=format_type,
        url=result.get("url", ""),
        dimension_at_observation="AllDimensions",
        time_range=time_range,
        usage=result.get("usage", "Use this URL to retrieve the actual statistical data"),
        formats_available=["csv", "json", "xml"],
        note=None,
    )


# =============================================================================
# Endpoint Management Tools
# =============================================================================


@mcp.tool()
async def get_current_endpoint(ctx: Context[Any, Any, Any] | None = None) -> EndpointInfo:
    """
    Get information about the currently active SDMX data source.

    Shows which statistical organization's API is being used (e.g., Pacific Data,
    European Central Bank, UNICEF).

    In multi-user deployments, this returns the endpoint for the current session.

    Returns:
        Current endpoint name, URL, agency ID, and description
    """
    # Get session-specific endpoint info
    app_ctx = get_app_context(ctx)

    if app_ctx is not None:
        # Use session-specific endpoint
        endpoint_info = app_ctx.get_endpoint_info(ctx)
        return EndpointInfo(
            key=endpoint_info.get("key"),
            name=endpoint_info.get("name", "Unknown"),
            base_url=endpoint_info.get("base_url", ""),
            agency_id=endpoint_info.get("agency_id", ""),
            description=endpoint_info.get("description", ""),
            status="Active",
            is_current=True,
        )

    # Fallback to global config
    from config import get_current_config

    current = get_current_config()

    return EndpointInfo(
        key=None,
        name=current["name"],
        base_url=current["base_url"],
        agency_id=current["agency_id"],
        description=current["description"],
        status=current.get("status", "Active"),
        is_current=True,
    )


@mcp.tool()
async def list_available_endpoints(ctx: Context[Any, Any, Any] | None = None) -> EndpointListResult:
    """
    List all available SDMX data sources that can be switched to.

    Shows all configured statistical data providers (e.g., SPC, ECB, UNICEF)
    and indicates which one is currently active for your session.

    In multi-user deployments, the current endpoint is session-specific.

    Returns:
        List of available endpoints with their descriptions and status
    """
    from config import SDMX_ENDPOINTS

    # Get session-specific current endpoint
    app_ctx = get_app_context(ctx)
    current_key = None

    if app_ctx is not None:
        session = app_ctx.get_session(ctx)
        current_key = session.endpoint_key
    else:
        # Fallback to global config
        from config import get_current_config

        current_config = get_current_config()
        for key, cfg in SDMX_ENDPOINTS.items():
            if cfg["base_url"] == current_config["base_url"]:
                current_key = key
                break

    # Build endpoint list
    endpoints = []
    for key, cfg in SDMX_ENDPOINTS.items():
        endpoints.append(
            EndpointInfo(
                key=key,
                name=cfg["name"],
                base_url=cfg["base_url"],
                agency_id=cfg["agency_id"],
                description=cfg["description"],
                status=cfg.get("status", "Available"),
                is_current=(key == current_key),
            )
        )

    return EndpointListResult(
        current=current_key or "custom",
        endpoints=endpoints,
        note="Use switch_endpoint() to change the active endpoint for your session",
    )


@mcp.tool()
async def switch_endpoint(
    endpoint_key: str,
    require_confirmation: bool = False,
    ctx: Context[Any, Any, Any] | None = None,
) -> EndpointSwitchResult:
    """
    Switch to a different SDMX data source for your session.

    Changes the active statistical data provider. All subsequent queries in your
    session will use the new endpoint until switched again.

    In multi-user deployments, this only affects your session - other users
    are not impacted.

    Args:
        endpoint_key: The endpoint to switch to (e.g., "SPC", "ECB", "UNICEF", "IMF")
        require_confirmation: If True, asks user for confirmation before switching
        ctx: MCP context (injected automatically)

    Returns:
        Confirmation of the switch with new endpoint details

    Examples:
        - switch_endpoint("ECB") - Switch to European Central Bank
        - switch_endpoint("UNICEF") - Switch to UNICEF data
        - switch_endpoint("SPC") - Switch to Pacific Data Hub
    """
    from config import SDMX_ENDPOINTS

    # Validate endpoint exists first
    if endpoint_key not in SDMX_ENDPOINTS:
        available = list(SDMX_ENDPOINTS.keys())
        return EndpointSwitchResult(
            success=False,
            message="Failed to switch endpoint",
            new_endpoint=None,
            error=f"Unknown endpoint: {endpoint_key}",
            available_endpoints=available,
            hint=f"Use one of: {', '.join(available)}",
        )

    # Get current endpoint info for this session
    app_ctx = get_app_context(ctx)
    target_config = SDMX_ENDPOINTS[endpoint_key]

    if app_ctx is not None:
        # Use session-based endpoint switching (multi-user safe)
        current_session = app_ctx.get_session(ctx)
        current_name = current_session.endpoint_name
        current_url = current_session.base_url
    else:
        # Fallback to global config
        from config import get_current_config

        current_config = get_current_config()
        current_name = current_config["name"]
        current_url = current_config["base_url"]

    # If confirmation required and context available, use elicitation
    if require_confirmation and ctx is not None:
        try:
            result = await ctx.elicit(
                message=(
                    f"Switch from **{current_name}** to **{target_config['name']}**?\n\n"
                    f"This will change the data source for your session.\n\n"
                    f"- Current: {current_url}\n"
                    f"- New: {target_config['base_url']}"
                ),
                schema=EndpointSwitchConfirmation,
            )

            if result.action != "accept" or not result.data.confirm:
                return EndpointSwitchResult(
                    success=False,
                    message="Endpoint switch cancelled by user",
                    new_endpoint=None,
                    error=None,
                    available_endpoints=None,
                    hint="Use switch_endpoint() again if you change your mind",
                )
        except Exception:
            # Elicitation not supported by client, proceed without confirmation
            pass

    try:
        # Use session-based switching if available
        if app_ctx is not None:
            switch_result = await app_ctx.switch_endpoint(endpoint_key, ctx)
            new_config = switch_result["new_endpoint"]

            return EndpointSwitchResult(
                success=True,
                message=f"Switched to {new_config['name']} (session-specific)",
                new_endpoint=EndpointInfo(
                    key=endpoint_key,
                    name=new_config["name"],
                    base_url=new_config["base_url"],
                    agency_id=new_config["agency_id"],
                    description=new_config.get("description", ""),
                    status="Active",
                    is_current=True,
                ),
                error=None,
                available_endpoints=None,
                hint="This change only affects your session",
            )
        else:
            # Fallback to global switching (legacy behavior)
            from config import set_endpoint
            from sdmx_progressive_client import SDMXProgressiveClient
            from tools import sdmx_tools

            new_config = set_endpoint(endpoint_key)

            # Close existing client if it has a session
            if sdmx_tools.sdmx_client.session:
                await sdmx_tools.sdmx_client.close()

            # Create new client with updated endpoint
            sdmx_tools.sdmx_client = SDMXProgressiveClient(
                base_url=new_config["base_url"], agency_id=new_config["agency_id"]
            )

            return EndpointSwitchResult(
                success=True,
                message=f"Switched to {new_config['name']} (global)",
                new_endpoint=EndpointInfo(
                    key=endpoint_key,
                    name=new_config["name"],
                    base_url=new_config["base_url"],
                    agency_id=new_config["agency_id"],
                    description=new_config["description"],
                    status="Active",
                    is_current=True,
                ),
                error=None,
                available_endpoints=None,
                hint="Warning: Session management not available, this affects all users",
            )

    except ValueError as e:
        available = list(SDMX_ENDPOINTS.keys())
        return EndpointSwitchResult(
            success=False,
            message="Failed to switch endpoint",
            new_endpoint=None,
            error=str(e),
            available_endpoints=available,
            hint=f"Use one of: {', '.join(available)}",
        )


@mcp.tool()
async def switch_endpoint_interactive(
    ctx: Context[Any, Any, Any],
    endpoint_key: str | None = None,
) -> EndpointSwitchResult:
    """
    Interactively switch to a different SDMX data source with user confirmation.

    This tool uses elicitation to show available endpoints and ask the user
    to confirm their selection. If the client doesn't support elicitation,
    it will return a list of available endpoints for manual selection.

    Args:
        ctx: MCP context (injected automatically)
        endpoint_key: Optional - if provided, skips selection and just confirms

    Returns:
        Confirmation of the switch with new endpoint details, or list of
        available endpoints if elicitation is not supported
    """
    from config import SDMX_ENDPOINTS, get_current_config, set_endpoint
    from sdmx_progressive_client import SDMXProgressiveClient
    from tools import sdmx_tools

    current_config = get_current_config()

    # Find current endpoint key
    current_key = None
    for key, cfg in SDMX_ENDPOINTS.items():
        if cfg["base_url"] == current_config["base_url"]:
            current_key = key
            break

    # Check if client supports elicitation by checking client_params capabilities
    client_supports_elicitation = False
    try:
        if ctx.session and hasattr(ctx.session, "client_params"):
            client_params = ctx.session.client_params
            if client_params and hasattr(client_params, "capabilities"):
                caps = client_params.capabilities
                # Check if elicitation capability exists and is not None
                if caps and hasattr(caps, "elicitation") and caps.elicitation is not None:
                    client_supports_elicitation = True
    except Exception:
        pass

    # If client doesn't support elicitation, return endpoint list for manual selection
    if not client_supports_elicitation:
        _ = [
            EndpointInfo(
                key=key,
                name=cfg["name"],
                base_url=cfg["base_url"],
                agency_id=cfg["agency_id"],
                description=cfg["description"],
                status=cfg.get("status", "Available"),
                is_current=(key == current_key),
            )
            for key, cfg in SDMX_ENDPOINTS.items()
        ]

        return EndpointSwitchResult(
            success=False,
            message=(
                "Interactive selection requires elicitation support. "
                "Please use switch_endpoint(endpoint_key) with one of the available endpoints."
            ),
            new_endpoint=None,
            error="Client does not support elicitation",
            available_endpoints=list(SDMX_ENDPOINTS.keys()),
            hint=f"Current: {current_key}. Use: switch_endpoint('ECB') or switch_endpoint('UNICEF')",
        )

    # Build endpoint list for display
    endpoint_list = "\n".join(
        f"- **{key}**: {cfg['name']} - {cfg['description']}" for key, cfg in SDMX_ENDPOINTS.items()
    )

    # Create a dynamic schema for endpoint selection
    from pydantic import BaseModel, Field

    class EndpointSelection(BaseModel):
        endpoint: str = Field(
            default=endpoint_key or current_key or "SPC",
            description=f"Select endpoint: {', '.join(SDMX_ENDPOINTS.keys())}",
        )
        confirm: bool = Field(default=False, description="Confirm the switch")

    try:
        from datetime import timedelta

        from mcp import types as mcp_types
        from mcp.shared.message import ServerMessageMetadata

        # Build the elicitation request with a long timeout for human interaction
        # Default MCP timeout is too short for humans to read and respond
        message_text = (
            f"## Select SDMX Data Source\n\n"
            f"**Current endpoint**: {current_config['name']}\n\n"
            f"### Available endpoints:\n{endpoint_list}\n\n"
            f"Enter the endpoint key and confirm to switch."
        )

        # Convert Pydantic schema to JSON schema for MCP
        json_schema = EndpointSelection.model_json_schema()

        # Use session.send_request directly with a 5-minute timeout
        raw_result = await ctx.session.send_request(
            mcp_types.ServerRequest(
                mcp_types.ElicitRequest(
                    params=mcp_types.ElicitRequestFormParams(
                        message=message_text,
                        requestedSchema=mcp_types.ElicitRequestedSchema(
                            type="object",
                            properties=json_schema.get("properties", {}),
                            required=json_schema.get("required", []),
                        ),
                    ),
                )
            ),
            mcp_types.ElicitResult,
            request_read_timeout_seconds=timedelta(minutes=5),  # 5 minute timeout
            metadata=ServerMessageMetadata(related_request_id=None),
        )

        # Parse the result
        result_action = raw_result.action if hasattr(raw_result, "action") else "cancel"
        result_data: EndpointSelection | None = None

        if result_action == "accept" and hasattr(raw_result, "content") and raw_result.content:
            try:
                result_data = EndpointSelection.model_validate(raw_result.content)
            except Exception:
                result_data = None

        if result_action != "accept":
            return EndpointSwitchResult(
                success=False,
                message="Endpoint selection cancelled",
                new_endpoint=None,
                error=None,
                available_endpoints=list(SDMX_ENDPOINTS.keys()),
                hint=None,
            )

        if result_data is None or not result_data.confirm:
            return EndpointSwitchResult(
                success=False,
                message="Endpoint switch not confirmed",
                new_endpoint=None,
                error=None,
                available_endpoints=list(SDMX_ENDPOINTS.keys()),
                hint="Set confirm=True to proceed with the switch",
            )

        selected_endpoint = result_data.endpoint.upper()

        if selected_endpoint not in SDMX_ENDPOINTS:
            return EndpointSwitchResult(
                success=False,
                message=f"Invalid endpoint: {selected_endpoint}",
                new_endpoint=None,
                error=f"Unknown endpoint: {selected_endpoint}",
                available_endpoints=list(SDMX_ENDPOINTS.keys()),
                hint=f"Use one of: {', '.join(SDMX_ENDPOINTS.keys())}",
            )

        # Perform the switch
        new_config = set_endpoint(selected_endpoint)

        if sdmx_tools.sdmx_client.session:
            await sdmx_tools.sdmx_client.close()

        sdmx_tools.sdmx_client = SDMXProgressiveClient(
            base_url=new_config["base_url"], agency_id=new_config["agency_id"]
        )

        return EndpointSwitchResult(
            success=True,
            message=f"Switched to {new_config['name']}",
            new_endpoint=EndpointInfo(
                key=selected_endpoint,
                name=new_config["name"],
                base_url=new_config["base_url"],
                agency_id=new_config["agency_id"],
                description=new_config["description"],
                status="Active",
                is_current=True,
            ),
            error=None,
            available_endpoints=None,
            hint=None,
        )

    except (TimeoutError, asyncio.TimeoutError):
        return EndpointSwitchResult(
            success=False,
            message="Elicitation timed out waiting for user response",
            new_endpoint=None,
            error="Timeout: no response received within 5 minutes",
            available_endpoints=list(SDMX_ENDPOINTS.keys()),
            hint="Use switch_endpoint(endpoint_key) directly instead",
        )
    except Exception as e:
        return EndpointSwitchResult(
            success=False,
            message="Elicitation failed - client may not support interactive selection",
            new_endpoint=None,
            error=str(e),
            available_endpoints=list(SDMX_ENDPOINTS.keys()),
            hint="Use switch_endpoint(endpoint_key) directly instead",
        )


# =============================================================================
# Resources
# =============================================================================


@mcp.resource("sdmx://agencies")
def agencies_list():
    """List of well-known SDMX data agencies and their endpoints."""
    return list_known_agencies()


@mcp.resource("sdmx://agency/{agency_id}/info")
def agency_info(agency_id: str):
    """Get information about a specific SDMX data agency."""
    return get_agency_info(agency_id)


@mcp.resource("sdmx://formats/guide")
def formats_guide():
    """Guide to SDMX data formats and their use cases."""
    return get_sdmx_format_guide()


@mcp.resource("sdmx://syntax/guide")
def syntax_guide():
    """Guide to SDMX query syntax and key construction."""
    return get_sdmx_query_syntax_guide()


# =============================================================================
# Prompts
# =============================================================================


@mcp.prompt()
def discovery_guide(query_description: str):
    """
    Guide for discovering SDMX data step-by-step.

    Provides a structured approach to finding and accessing SDMX statistical data.
    """
    return sdmx_discovery_guide(query_description)


@mcp.prompt()
def troubleshooting_guide(error_type: str, error_details: str = ""):
    """
    Troubleshooting guide for common SDMX issues.
    """
    return sdmx_troubleshooting_guide(error_type, error_details)


@mcp.prompt()
def best_practices(use_case: str):
    """
    Best practices guide for different SDMX use cases.
    Available use cases: research, dashboard, automation
    """
    return sdmx_best_practices(use_case)


@mcp.prompt()
def query_builder(dataflow_info: dict[str, str], user_requirements: str):
    """
    Interactive query builder prompt based on dataflow structure.
    """
    return sdmx_query_builder(dataflow_info, user_requirements)


# =============================================================================
# Server Entry Point
# =============================================================================


def main():
    """Main entry point for the SDMX MCP Gateway server."""
    # Configure logging here (not at module level) to avoid early stderr writes
    # that can interfere with MCP Inspector's JSON-RPC parsing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(
        description="SDMX MCP Gateway - Progressive discovery tools for SDMX statistical data"
    )
    parser.add_argument(
        "--transport",
        "-t",
        choices=["stdio", "http", "streamable-http"],
        default="stdio",
        help="Transport type (default: stdio)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for HTTP transport (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=8000,
        help="Port for HTTP transport (default: 8000)",
    )
    parser.add_argument(
        "--stateless",
        action="store_true",
        help="Run in stateless mode (for HTTP transport)",
    )
    parser.add_argument(
        "--json-response",
        action="store_true",
        help="Use JSON responses instead of SSE (for HTTP transport)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Configure logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Run with appropriate transport (no startup logging - interferes with STDIO JSON-RPC)
    if args.transport == "stdio":
        mcp.run(transport="stdio")
    elif args.transport in ("http", "streamable-http"):
        logger.info("HTTP server listening on %s:%d", args.host, args.port)
        # Note: HTTP transport options depend on MCP SDK version
        # Some options may not be available in all versions
        try:
            mcp.run(
                transport="streamable-http",
            )
        except TypeError:
            # Fallback if transport options not supported
            mcp.run()
    else:
        # Default to stdio
        mcp.run()


if __name__ == "__main__":
    main()
