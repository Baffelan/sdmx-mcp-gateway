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
from pydantic import BaseModel, Field

# Import lifespan and context
from app_context import AppContext, app_lifespan

# Import structured output models
from models.schemas import (
    CodeChange,
    CodeInfo,
    ComparisonSummary,
    ComponentInfo,
    ConceptChange,
    ConceptRef,
    DataAvailabilityResult,
    DataflowDiagramResult,
    DataflowInfo,
    DataflowListResult,
    DataflowStructureResult,
    DataflowSummary,
    DataUrlResult,
    DimensionChange,
    DimensionCodesResult,
    DimensionInfo,
    EndpointInfo,
    EndpointListResult,
    EndpointSwitchConfirmation,
    EndpointSwitchResult,
    FilterInfo,
    KeyBuildResult,
    PaginationInfo,
    ReferenceChange,
    RepresentationInfo,
    StructureComparisonResult,
    StructureDiagramResult,
    StructureEdge,
    StructureInfo,
    StructureNode,
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


# =============================================================================
# Code Usage Discovery Tools
# =============================================================================


class CodeUsageInfo(BaseModel):
    """Information about a single code's usage."""

    code: str = Field(description="The code being checked")
    is_used: bool = Field(description="Whether this code has actual data")
    dimension_id: str | None = Field(default=None, description="Dimension where code is used")


class CodeUsageResult(BaseModel):
    """Result from get_code_usage() tool."""

    discovery_level: str = Field(default="code_usage", description="Discovery workflow level")
    dataflow_id: str = Field(description="Dataflow checked")
    dimension_id: str | None = Field(default=None, description="Dimension checked (if specific)")
    constraint_id: str | None = Field(default=None, description="Actual constraint used")
    codes_checked: list[CodeUsageInfo] = Field(description="Usage status for each code")
    summary: dict[str, int] = Field(description="Summary counts: total_checked, used, unused")
    all_used_codes: dict[str, list[str]] | None = Field(
        default=None,
        description="All codes with actual data per dimension (if no specific codes requested)",
    )
    interpretation: list[str] = Field(description="Human-readable explanation")
    api_calls_made: int = Field(default=1, description="Number of API calls made")


class CrossDataflowUsageInfo(BaseModel):
    """Information about code usage across dataflows."""

    dataflow_id: str = Field(description="Dataflow ID")
    dataflow_version: str | None = Field(
        default=None, description="Dataflow version from ConstraintAttachment"
    )
    dataflow_name: str | None = Field(default=None, description="Dataflow name (if available)")
    dimension_id: str = Field(description="Dimension where code is used")
    is_used: bool = Field(description="Whether code has actual data in this dataflow")


class CrossDataflowCodeUsageResult(BaseModel):
    """Result from find_code_usage_across_dataflows() tool."""

    discovery_level: str = Field(default="cross_dataflow_usage", description="Discovery level")
    codelist_id: str = Field(description="Codelist checked")
    code: str = Field(description="Code checked")
    total_dsds_using_codelist: int = Field(description="DSDs that reference this codelist")
    total_dataflows_checked: int = Field(description="Dataflows checked for actual usage")
    dataflows_with_data: list[CrossDataflowUsageInfo] = Field(
        description="Dataflows where code has actual data"
    )
    dataflows_without_data: list[str] = Field(
        description="Dataflow IDs where code is allowed but has no data"
    )
    summary: dict[str, int] = Field(
        description="Summary: dsds, dataflows_checked, with_data, without_data"
    )
    interpretation: list[str] = Field(description="Human-readable explanation")
    api_calls_made: int = Field(description="Number of API calls made")


@mcp.tool()
async def get_code_usage(
    dataflow_id: str,
    codes: list[str] | None = None,
    dimension_id: str | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> CodeUsageResult:
    """
    Efficiently check if specific codes are actually used in a dataflow's data.

    This uses the Actual ContentConstraint (if available) to determine which
    codes have real data, WITHOUT iterating through data queries. This is
    much faster than trial-and-error data requests.

    Use cases:
    - "Is country code 'FJ' actually used in DF_SDG?"
    - "Which indicator codes have data?" (leave codes empty)
    - "Are these 5 codes I want to use valid AND have data?"

    Args:
        dataflow_id: The dataflow to check
        codes: Optional list of specific codes to check. If empty, returns all used codes.
        dimension_id: Optional dimension to check. If empty, checks all dimensions.
        agency_id: The agency (default: "SPC")

    Returns:
        CodeUsageResult with:
            - codes_checked: List of codes with their usage status
            - all_used_codes: All codes that have data (by dimension)
            - summary: Counts of used/unused codes

    Examples:
        >>> get_code_usage("DF_SDG", codes=["FJ", "WS", "XX"], dimension_id="GEO_PICT")
        # Checks if Fiji, Samoa, and "XX" have SDG data

        >>> get_code_usage("DF_SDG", dimension_id="INDICATOR")
        # Returns all indicator codes that actually have data
    """
    import xml.etree.ElementTree as ET

    from utils import SDMX_NAMESPACES

    client = get_session_client(ctx)
    agency = agency_id or client.agency_id
    ns = SDMX_NAMESPACES
    api_calls = 0

    if ctx:
        await ctx.info(f"Checking code usage for {dataflow_id}...")

    url = f"{client.base_url}/dataflow/{agency}/{dataflow_id}/latest?references=all&detail=full"
    headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

    try:
        session = await client._get_session()
        resp = await session.get(url, headers=headers)
        resp.raise_for_status()
        api_calls += 1

        root = ET.fromstring(resp.content)

        # Find the Actual ContentConstraint
        actual_constraint = None
        for constraint in root.findall(".//str:ContentConstraint", ns):
            if constraint.get("type") == "Actual":
                actual_constraint = constraint
                break

        if actual_constraint is None:
            return CodeUsageResult(
                dataflow_id=dataflow_id,
                dimension_id=dimension_id,
                constraint_id=None,
                codes_checked=[],
                summary={"total_checked": 0, "used": 0, "unused": 0},
                all_used_codes=None,
                interpretation=[
                    f"No Actual ContentConstraint found for {dataflow_id}.",
                    "Cannot efficiently determine code usage.",
                ],
                api_calls_made=api_calls,
            )

        constraint_id = actual_constraint.get("id", "")

        # Parse CubeRegion to get all codes with actual data
        all_used_codes: dict[str, list[str]] = {}

        for cube_region in actual_constraint.findall(".//str:CubeRegion", ns):
            include = cube_region.get("include", "true") == "true"
            if not include:
                continue

            for key_value in cube_region.findall(".//com:KeyValue", ns):
                dim_id = key_value.get("id", "")
                values: list[str] = []
                for value in key_value.findall("./com:Value", ns):
                    if value.text:
                        values.append(value.text)

                if dim_id not in all_used_codes:
                    all_used_codes[dim_id] = []
                all_used_codes[dim_id].extend(values)

        # Deduplicate
        for dim_id in all_used_codes:
            all_used_codes[dim_id] = sorted(set(all_used_codes[dim_id]))

        # Check specific codes if provided
        codes_checked: list[CodeUsageInfo] = []

        if codes:
            if dimension_id:
                used_in_dim = set(all_used_codes.get(dimension_id, []))
                for code in codes:
                    codes_checked.append(
                        CodeUsageInfo(
                            code=code, is_used=code in used_in_dim, dimension_id=dimension_id
                        )
                    )
            else:
                for code in codes:
                    found_in = None
                    for dim_id, dim_codes in all_used_codes.items():
                        if code in dim_codes:
                            found_in = dim_id
                            break
                    codes_checked.append(
                        CodeUsageInfo(
                            code=code, is_used=found_in is not None, dimension_id=found_in
                        )
                    )

        used_count = sum(1 for c in codes_checked if c.is_used)
        summary = {
            "total_checked": len(codes_checked),
            "used": used_count,
            "unused": len(codes_checked) - used_count,
        }

        interpretation = [
            f"**Dataflow:** {dataflow_id}",
            f"**Constraint:** {constraint_id} (Actual)",
        ]
        if codes:
            interpretation.append(
                f"**Codes checked:** {len(codes)} - {used_count} used, {len(codes) - used_count} unused"
            )
        else:
            interpretation.append("**Codes with data by dimension:**")
            for dim_id, dim_codes in sorted(all_used_codes.items()):
                interpretation.append(f"  - {dim_id}: {len(dim_codes)} codes")

        return CodeUsageResult(
            dataflow_id=dataflow_id,
            dimension_id=dimension_id,
            constraint_id=constraint_id,
            codes_checked=codes_checked,
            summary=summary,
            all_used_codes=all_used_codes if not codes else None,
            interpretation=interpretation,
            api_calls_made=api_calls,
        )

    except Exception as e:
        logger.exception("Failed to check code usage")
        return CodeUsageResult(
            dataflow_id=dataflow_id,
            dimension_id=dimension_id,
            constraint_id=None,
            codes_checked=[],
            summary={"total_checked": 0, "used": 0, "unused": 0},
            all_used_codes=None,
            interpretation=[f"Error: {str(e)}"],
            api_calls_made=api_calls,
        )


@mcp.tool()
async def find_code_usage_across_dataflows(
    codelist_id: str,
    code: str,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> CrossDataflowCodeUsageResult:
    """
    Find if a specific code from a codelist is actually used in ANY dataflow.

    This is a SMART search that fetches ALL Actual constraints in a single API call
    and searches through them, instead of iterating through dataflows one by one.
    This makes it O(1) API calls instead of O(N).

    Use cases:
    - "Is code 'FJ' from CL_COM_GEO_PICT used anywhere?"
    - "If I remove indicator 'AG_LND_FRST', what dataflows will break?"
    - "Which dataflows actually have data for this code?"

    Args:
        codelist_id: The codelist ID (e.g., "CL_COM_GEO_PICT") - used for context only
        code: The specific code to check (e.g., "FJ")
        agency_id: The agency (default: "SPC")

    Returns:
        CrossDataflowCodeUsageResult with:
            - dataflows_with_data: Dataflows where code is actually used
            - summary: Counts of usage

    Example:
        >>> find_code_usage_across_dataflows("CL_COM_GEO_PICT", "FJ")
        # Finds all dataflows where Fiji (FJ) has actual data (1 API call!)
    """
    import xml.etree.ElementTree as ET

    from utils import SDMX_NAMESPACES

    client = get_session_client(ctx)
    agency = agency_id or client.agency_id
    ns = SDMX_NAMESPACES
    api_calls = 0

    if ctx:
        await ctx.info(f"Fetching ALL Actual constraints to find usage of code '{code}'...")

    headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

    try:
        session = await client._get_session()

        # SMART: Get ALL constraints in ONE API call
        constraints_url = f"{client.base_url}/contentconstraint/{agency}/all/latest?detail=full"
        resp = await session.get(constraints_url, headers=headers, timeout=120)
        resp.raise_for_status()
        api_calls += 1

        root = ET.fromstring(resp.content)

        # Find all Actual constraints that contain this code
        dataflows_with_data: list[CrossDataflowUsageInfo] = []
        constraints_checked = 0
        actual_constraints = 0

        for constraint in root.findall(".//str:ContentConstraint", ns):
            if constraint.get("type") != "Actual":
                continue

            actual_constraints += 1
            constraint_id = constraint.get("id", "")

            # Extract dataflow reference from ConstraintAttachment (the proper SDMX way)
            # The Ref element contains: id, version, agencyID, package, class
            dataflow_id = None
            dataflow_version = None
            dataflow_name = None

            # Primary method: Get dataflow from ConstraintAttachment/Dataflow/Ref
            # The Ref element may be namespace-less, so we also iterate children
            for df_elem in constraint.findall(".//str:ConstraintAttachment/str:Dataflow", ns):
                # Try XPath first (works when Ref has no namespace)
                for df_ref in df_elem.findall("./Ref", ns):
                    dataflow_id = df_ref.get("id")
                    dataflow_version = df_ref.get("version")
                    break
                # Fallback: iterate children to find Ref element
                if not dataflow_id:
                    for child in df_elem:
                        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                        if tag == "Ref":
                            dataflow_id = child.get("id")
                            dataflow_version = child.get("version")
                            break
                if dataflow_id:
                    break

            # Fallback: extract from constraint ID pattern (CON_XXX or CR_A_XXX)
            if not dataflow_id:
                if constraint_id.startswith("CR_A_"):
                    dataflow_id = constraint_id[5:]  # CR_A_DF_SDG -> DF_SDG
                elif constraint_id.startswith("CON_"):
                    dataflow_id = "DF_" + constraint_id[4:]  # CON_BOP -> DF_BOP

            if not dataflow_id:
                continue

            # Search CubeRegions for the code
            found_in_dim = None
            for cube_region in constraint.findall(".//str:CubeRegion", ns):
                if cube_region.get("include", "true") != "true":
                    continue

                for key_value in cube_region.findall(".//com:KeyValue", ns):
                    dim_id = key_value.get("id", "")
                    for value in key_value.findall("./com:Value", ns):
                        if value.text == code:
                            found_in_dim = dim_id
                            break
                    if found_in_dim:
                        break
                if found_in_dim:
                    break

            if found_in_dim:
                # Get constraint name as proxy for dataflow name (if not already set)
                if not dataflow_name:
                    name_elem = constraint.find("./com:Name", ns)
                    if name_elem is not None and name_elem.text:
                        dataflow_name = name_elem.text

                dataflows_with_data.append(
                    CrossDataflowUsageInfo(
                        dataflow_id=dataflow_id,
                        dataflow_version=dataflow_version,
                        dataflow_name=dataflow_name,
                        dimension_id=found_in_dim,
                        is_used=True,
                    )
                )

            constraints_checked += 1

        summary = {
            "dsds_using_codelist": 0,  # Not checked in this approach
            "dataflows_checked": actual_constraints,
            "with_data": len(dataflows_with_data),
            "without_data": actual_constraints - len(dataflows_with_data),
        }

        interpretation = [
            f"**Codelist:** {codelist_id}",
            f"**Code:** {code}",
            "",
            f"**Search method:** Single API call for all {actual_constraints} Actual constraints",
            f"**API calls made:** {api_calls} (efficient!)",
            "",
        ]

        if dataflows_with_data:
            interpretation.append(f"**✓ Code HAS DATA in {len(dataflows_with_data)} dataflow(s):**")
            for df in dataflows_with_data[:15]:
                version_str = f" v{df.dataflow_version}" if df.dataflow_version else ""
                interpretation.append(f"  - {df.dataflow_id}{version_str} (dim: {df.dimension_id})")
            if len(dataflows_with_data) > 15:
                interpretation.append(f"  ... and {len(dataflows_with_data) - 15} more")
        else:
            interpretation.append(
                f"**✗ Code '{code}' has NO DATA in any of the {actual_constraints} dataflows**"
            )

        return CrossDataflowCodeUsageResult(
            codelist_id=codelist_id,
            code=code,
            total_dsds_using_codelist=0,
            total_dataflows_checked=actual_constraints,
            dataflows_with_data=dataflows_with_data,
            dataflows_without_data=[],  # Not tracked in efficient approach
            summary=summary,
            interpretation=interpretation,
            api_calls_made=api_calls,
        )

    except Exception as e:
        logger.exception("Failed to find code usage across dataflows")
        return CrossDataflowCodeUsageResult(
            codelist_id=codelist_id,
            code=code,
            total_dsds_using_codelist=0,
            total_dataflows_checked=0,
            dataflows_with_data=[],
            dataflows_without_data=[],
            summary={
                "dsds_using_codelist": 0,
                "dataflows_checked": 0,
                "with_data": 0,
                "without_data": 0,
            },
            interpretation=[f"Error: {str(e)}"],
            api_calls_made=api_calls,
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
# Structure Relationship Tools
# =============================================================================


def _generate_mermaid_diagram(
    target: StructureNode,
    nodes: list[StructureNode],
    edges: list[StructureEdge],
    show_versions: bool = False,
) -> str:
    """Generate a Mermaid diagram from nodes and edges.

    Args:
        target: The target structure node
        nodes: All nodes in the graph
        edges: All edges (relationships) in the graph
        show_versions: If True, display version numbers on each node
    """
    # Icon mapping for structure types
    icons = {
        "dataflow": "📊",
        "datastructure": "🏗️",
        "dsd": "🏗️",
        "codelist": "📋",
        "conceptscheme": "💡",
        "categoryscheme": "📁",
        "constraint": "🔒",
        "contentconstraint": "🔒",
        "categorisation": "🏷️",
        "agencyscheme": "🏛️",
        "dataproviderscheme": "🏢",
    }

    lines = ["graph TD"]

    # Group nodes by type for subgraphs
    node_groups: dict[str, list[StructureNode]] = {}
    for node in nodes:
        group_key = node.structure_type
        if group_key not in node_groups:
            node_groups[group_key] = []
        node_groups[group_key].append(node)

    # Subgraph labels
    subgraph_labels = {
        "dataflow": "Dataflows",
        "datastructure": "Data Structures",
        "dsd": "Data Structures",
        "codelist": "Codelists",
        "conceptscheme": "Concept Schemes",
        "categoryscheme": "Category Schemes",
        "constraint": "Constraints",
        "contentconstraint": "Constraints",
        "categorisation": "Categorisations",
    }

    # Generate subgraphs
    for group_type, group_nodes in node_groups.items():
        icon = icons.get(group_type, "📦")
        label = subgraph_labels.get(group_type, group_type.title())

        # Highlight target's group
        if target.structure_type == group_type:
            lines.append(f'    subgraph {group_type}["{label} ⭐"]')
        else:
            lines.append(f'    subgraph {group_type}["{label}"]')

        for node in group_nodes:
            # Escape special characters in names
            safe_name = node.name.replace('"', "'").replace("\n", " ")[:40]
            # Build version suffix if requested
            version_suffix = f" v{node.version}" if show_versions and node.version else ""
            if node.is_target:
                # Highlight target node
                lines.append(
                    f'        {node.node_id}["{icon} <b>{node.id}</b>{version_suffix}<br/>{safe_name}"]'
                )
            else:
                lines.append(
                    f'        {node.node_id}["{icon} {node.id}{version_suffix}<br/>{safe_name}"]'
                )

        lines.append("    end")

    # Generate edges
    for edge in edges:
        label = edge.label or edge.relationship
        lines.append(f'    {edge.source} -->|"{label}"| {edge.target}')

    # Add styling for target node
    lines.append(f"    style {target.node_id} fill:#e1f5fe,stroke:#01579b,stroke-width:3px")

    return "\n".join(lines)


@mcp.tool()
async def get_structure_diagram(
    structure_type: str,
    structure_id: str,
    agency_id: str | None = None,
    version: str = "latest",
    direction: str = "both",
    show_versions: bool = False,
    ctx: Context[Any, Any, Any] | None = None,
) -> StructureDiagramResult:
    """
    Generate an SDMX-aware Mermaid diagram for any structural artifact.

    The visualization adapts based on the artifact type to show the most
    relevant information following the SDMX information model:

    **Dataflow**: Shows full SDMX hierarchy (entry point view)
        - Dataflow → DSD → Components (Dimensions, Attributes, Measure)
        - Components → Concepts (semantic meaning from ConceptSchemes)
        - Components → Representations (Codelists or free text)

    **DSD/DataStructure**: Shows component structure + relationships
        - Parent dataflows that use this DSD
        - Child codelists and concept schemes referenced

    **Codelist**: Shows impact/usage view (building block)
        - Parent DSDs/dimensions that reference this codelist
        - Useful for impact analysis (what breaks if I change this?)

    **ConceptScheme**: Shows usage across structures
        - Parent DSDs/components that use these concepts

    Args:
        structure_type: Type of structure - one of:
            - "dataflow": Statistical data publication (shows full hierarchy)
            - "datastructure" or "dsd": Data Structure Definition
            - "codelist": Code list (enumeration of valid values)
            - "conceptscheme": Concept scheme (definitions)
            - "categoryscheme": Category scheme (classification)
        structure_id: The structure identifier
        agency_id: Agency ID (uses current endpoint's default if not specified)
        version: Version string (default "latest") - query a specific version
        direction: Relationship direction to explore (ignored for dataflow):
            - "parents": Show structures that USE this one
            - "children": Show structures this one REFERENCES
            - "both": Show both directions (default)
        show_versions: If True, display version numbers on each node

    Returns:
        StructureDiagramResult with:
            - mermaid_diagram: Ready-to-render Mermaid code
            - nodes: All structures in the relationship graph
            - edges: Relationships between structures
            - interpretation: Human-readable explanation

    Examples:
        >>> get_structure_diagram("dataflow", "DF_SDG")
        # Shows complete SDG dataflow structure with SDMX hierarchy

        >>> get_structure_diagram("codelist", "CL_FREQ", show_versions=True)
        # Shows what structures use CL_FREQ (impact analysis)

        >>> get_structure_diagram("dsd", "DSD_POP", direction="children")
        # Shows codelists and concept schemes used by DSD_POP
    """
    import xml.etree.ElementTree as ET

    import httpx

    from utils import SDMX_NAMESPACES

    client = get_session_client(ctx)
    agency = agency_id or client.agency_id
    ns = SDMX_NAMESPACES

    # ==========================================================================
    # DATAFLOW: Generate full SDMX hierarchy diagram
    # ==========================================================================
    if structure_type.lower() == "dataflow":
        return await _generate_dataflow_hierarchy_diagram(
            client, structure_id, agency, show_versions, ctx
        )

    # ==========================================================================
    # OTHER ARTIFACTS: Generate relationship diagram
    # ==========================================================================
    # ==========================================================================
    # DSD/DATASTRUCTURE: Generate component-focused diagram with parent dataflows
    # ==========================================================================
    if structure_type.lower() in ("dsd", "datastructure"):
        return await _generate_dsd_hierarchy_diagram(
            client, structure_id, agency, version, show_versions, ctx
        )

    if ctx:
        await ctx.info(f"Fetching {direction} references for {structure_type}/{structure_id}...")

    # Fetch structure references from client
    result = await client.get_structure_references(
        structure_type=structure_type,
        structure_id=structure_id,
        agency_id=agency,
        version=version,
        direction=direction,
        ctx=ctx,
    )

    if "error" in result:
        # Return error result
        error_node = StructureNode(
            node_id="error",
            structure_type=structure_type,
            id=structure_id,
            agency=agency,
            version=version,
            name=f"Error: {result['error']}",
            is_target=True,
        )
        return StructureDiagramResult(
            discovery_level="structure_relationships",
            target=error_node,
            direction=direction,
            depth=1,
            nodes=[error_node],
            edges=[],
            mermaid_diagram=f'graph TD\n    error["❌ Error: {result["error"]}"]',
            interpretation=[f"Error: {result['error']}"],
            api_calls_made=1,
            note=result.get("details"),
        )

    # Build target node
    target_info = result.get("target", {})
    target_node = StructureNode(
        node_id=f"{structure_type}_{structure_id}".replace("-", "_").replace(".", "_"),
        structure_type=target_info.get("type", structure_type),
        id=target_info.get("id", structure_id),
        agency=target_info.get("agency", agency),
        version=target_info.get("version", version),
        name=target_info.get("name", structure_id),
        is_target=True,
    )

    # Build all nodes and edges
    nodes: list[StructureNode] = [target_node]
    edges: list[StructureEdge] = []
    interpretation: list[str] = []

    # Process parents
    parents = result.get("parents", [])
    if parents:
        interpretation.append(f"**{len(parents)} parent(s)** use this {structure_type}:")
        for parent in parents:
            node_id = f"{parent['type']}_{parent['id']}".replace("-", "_").replace(".", "_")
            parent_version = parent.get("version", "1.0")
            nodes.append(
                StructureNode(
                    node_id=node_id,
                    structure_type=parent["type"],
                    id=parent["id"],
                    agency=parent.get("agency", ""),
                    version=parent_version,
                    name=parent.get("name", parent["id"]),
                    is_target=False,
                )
            )
            edges.append(
                StructureEdge(
                    source=node_id,
                    target=target_node.node_id,
                    relationship=parent.get("relationship", "uses"),
                    label=parent.get("relationship", "uses"),
                )
            )
            # Include version in interpretation if show_versions is enabled
            version_info = f" v{parent_version}" if show_versions else ""
            interpretation.append(
                f"  - {parent['type']}: **{parent['id']}**{version_info} ({parent.get('name', '')})"
            )

    # Process children
    children = result.get("children", [])
    if children:
        interpretation.append(
            f"**{len(children)} child(ren)** referenced by this {structure_type}:"
        )
        for child in children:
            node_id = f"{child['type']}_{child['id']}".replace("-", "_").replace(".", "_")
            child_version = child.get("version", "1.0")
            # Avoid duplicate nodes
            if not any(n.node_id == node_id for n in nodes):
                nodes.append(
                    StructureNode(
                        node_id=node_id,
                        structure_type=child["type"],
                        id=child["id"],
                        agency=child.get("agency", ""),
                        version=child_version,
                        name=child.get("name", child["id"]),
                        is_target=False,
                    )
                )
            edges.append(
                StructureEdge(
                    source=target_node.node_id,
                    target=node_id,
                    relationship=child.get("relationship", "references"),
                    label=child.get("relationship", "references"),
                )
            )
            # Include version in interpretation if show_versions is enabled
            version_info = f" v{child_version}" if show_versions else ""
            interpretation.append(
                f"  - {child['type']}: **{child['id']}**{version_info} ({child.get('name', '')})"
            )

    if not parents and not children:
        interpretation.append(f"No {direction} relationships found for this {structure_type}.")
        interpretation.append("This might mean:")
        interpretation.append("  - The structure is a leaf node (codelists have no children)")
        interpretation.append("  - The structure is a root node (no parents)")
        interpretation.append("  - The API didn't return reference information")

    # Generate Mermaid diagram
    mermaid_diagram = _generate_mermaid_diagram(target_node, nodes, edges, show_versions)

    return StructureDiagramResult(
        discovery_level="structure_relationships",
        target=target_node,
        direction=direction,
        depth=1,
        nodes=nodes,
        edges=edges,
        mermaid_diagram=mermaid_diagram,
        interpretation=interpretation,
        api_calls_made=result.get("api_calls", 1),
        note=None,
    )


async def _generate_dsd_hierarchy_diagram(
    client: SDMXProgressiveClient,
    dsd_id: str,
    agency: str,
    version: str,
    show_versions: bool,
    ctx: Context[Any, Any, Any] | None,
) -> StructureDiagramResult:
    """
    Generate full SDMX hierarchy diagram for a DSD.

    Shows: Parent Dataflows → DSD → Components → Concepts/Codelists
    """
    import xml.etree.ElementTree as ET

    from utils import SDMX_NAMESPACES

    ns = SDMX_NAMESPACES
    api_calls = 0

    if ctx:
        await ctx.info(f"Fetching SDMX hierarchy for DSD {dsd_id}...")

    headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

    try:
        session = await client._get_session()

        # Get DSD with all references (includes parent dataflows and child codelists)
        dsd_url = f"{client.base_url}/datastructure/{agency}/{dsd_id}/{version}?references=all&detail=full"
        resp = await session.get(dsd_url, headers=headers)
        resp.raise_for_status()
        api_calls += 1

        root = ET.fromstring(resp.content)
        dsd_elem = root.find(".//str:DataStructure", ns)

        if dsd_elem is None:
            error_node = StructureNode(
                node_id="error",
                structure_type="datastructure",
                id=dsd_id,
                agency=agency,
                version=version,
                name=f"DSD {dsd_id} not found",
                is_target=True,
            )
            return StructureDiagramResult(
                discovery_level="dsd_hierarchy",
                target=error_node,
                direction="both",
                depth=3,
                nodes=[error_node],
                edges=[],
                mermaid_diagram=f'graph TD\n    error["❌ DSD {dsd_id} not found"]',
                interpretation=[f"Error: DSD {dsd_id} not found"],
                api_calls_made=api_calls,
            )

        dsd_name_elem = dsd_elem.find("./com:Name", ns)
        dsd_name = (
            dsd_name_elem.text if dsd_name_elem is not None and dsd_name_elem.text else dsd_id
        )
        dsd_version = dsd_elem.get("version") or version

        # Find parent dataflows that use this DSD
        parent_dataflows: list[dict[str, str]] = []
        for df_elem in root.findall(".//str:Dataflow", ns):
            df_id = df_elem.get("id", "")
            df_name_elem = df_elem.find("./com:Name", ns)
            df_name = df_name_elem.text if df_name_elem is not None and df_name_elem.text else df_id
            df_version = df_elem.get("version", "1.0")
            parent_dataflows.append(
                {
                    "id": df_id,
                    "name": df_name,
                    "version": df_version,
                }
            )

        # Collect concept schemes and codelists
        concept_schemes_map: dict[str, dict[str, str]] = {}
        codelists_map: dict[str, dict[str, str]] = {}

        for cs_elem in root.findall(".//str:ConceptScheme", ns):
            cs_id = cs_elem.get("id", "")
            cs_agency = cs_elem.get("agencyID", agency)
            cs_version = cs_elem.get("version", "1.0")
            cs_name_elem = cs_elem.find("./com:Name", ns)
            cs_name = cs_name_elem.text if cs_name_elem is not None and cs_name_elem.text else cs_id
            concept_schemes_map[cs_id] = {
                "id": cs_id,
                "agency": cs_agency or agency,
                "version": cs_version or "1.0",
                "name": cs_name,
            }

        for cl_elem in root.findall(".//str:Codelist", ns):
            cl_id = cl_elem.get("id", "")
            cl_agency = cl_elem.get("agencyID", agency)
            cl_version = cl_elem.get("version", "1.0")
            cl_name_elem = cl_elem.find("./com:Name", ns)
            cl_name = cl_name_elem.text if cl_name_elem is not None and cl_name_elem.text else cl_id
            codelists_map[cl_id] = {
                "id": cl_id,
                "agency": cl_agency or agency,
                "version": cl_version or "1.0",
                "name": cl_name,
            }

        # Helper functions (same as dataflow)
        def get_concept_ref(elem: ET.Element) -> ConceptRef:
            concept_ref = elem.find(".//str:ConceptIdentity/Ref", ns)
            if concept_ref is None:
                concept_ref = elem.find(".//str:ConceptIdentity/com:Ref", ns)
            if concept_ref is not None:
                scheme_id = concept_ref.get("maintainableParentID") or "CS_COMMON"
                if scheme_id not in concept_schemes_map:
                    concept_schemes_map[scheme_id] = {
                        "id": scheme_id,
                        "agency": concept_ref.get("agencyID") or agency,
                        "version": concept_ref.get("maintainableParentVersion") or "1.0",
                        "name": scheme_id,
                    }
                return ConceptRef(
                    id=concept_ref.get("id") or "",
                    scheme_id=scheme_id,
                    scheme_agency=concept_ref.get("agencyID") or agency,
                    scheme_version=concept_ref.get("maintainableParentVersion") or "1.0",
                )
            return ConceptRef(id="UNKNOWN", scheme_id="CS_UNKNOWN")

        def get_representation(elem: ET.Element) -> RepresentationInfo:
            cl_ref = elem.find(".//str:LocalRepresentation/str:Enumeration/Ref", ns)
            if cl_ref is None:
                cl_ref = elem.find(".//str:LocalRepresentation/str:Enumeration/com:Ref", ns)
            if cl_ref is not None:
                cl_id = cl_ref.get("id") or ""
                cl_agency = cl_ref.get("agencyID") or agency
                cl_version = cl_ref.get("version") or "1.0"
                if cl_id not in codelists_map:
                    codelists_map[cl_id] = {
                        "id": cl_id,
                        "agency": cl_agency,
                        "version": cl_version,
                        "name": cl_id,
                    }
                return RepresentationInfo(
                    is_enumerated=True,
                    codelist_id=cl_id,
                    codelist_agency=cl_agency,
                    codelist_version=cl_version,
                )
            text_format = elem.find(".//str:LocalRepresentation/str:TextFormat", ns)
            format_type = text_format.get("textType") if text_format is not None else "String"
            return RepresentationInfo(is_enumerated=False, text_format=format_type)

        # Parse components
        dimensions: list[ComponentInfo] = []
        dim_list = dsd_elem.find(".//str:DimensionList", ns)
        if dim_list is not None:
            for dim in dim_list.findall(".//str:Dimension", ns):
                dimensions.append(
                    ComponentInfo(
                        id=dim.get("id") or "",
                        component_type="Dimension",
                        position=int(dim.get("position") or 0),
                        concept=get_concept_ref(dim),
                        representation=get_representation(dim),
                    )
                )
            time_dim = dim_list.find(".//str:TimeDimension", ns)
            if time_dim is not None:
                dimensions.append(
                    ComponentInfo(
                        id=time_dim.get("id") or "TIME_PERIOD",
                        component_type="TimeDimension",
                        position=int(time_dim.get("position") or 999),
                        concept=get_concept_ref(time_dim),
                        representation=RepresentationInfo(
                            is_enumerated=False, text_format="ObservationalTimePeriod"
                        ),
                    )
                )
        dimensions.sort(key=lambda d: d.position or 0)

        attributes: list[ComponentInfo] = []
        attr_list = dsd_elem.find(".//str:AttributeList", ns)
        if attr_list is not None:
            for attr in attr_list.findall(".//str:Attribute", ns):
                attributes.append(
                    ComponentInfo(
                        id=attr.get("id") or "",
                        component_type="Attribute",
                        assignment_status=attr.get("assignmentStatus") or None,
                        concept=get_concept_ref(attr),
                        representation=get_representation(attr),
                    )
                )

        measure: ComponentInfo | None = None
        measure_list = dsd_elem.find(".//str:MeasureList", ns)
        if measure_list is not None:
            primary = measure_list.find(".//str:PrimaryMeasure", ns)
            if primary is not None:
                measure = ComponentInfo(
                    id=primary.get("id") or "OBS_VALUE",
                    component_type="PrimaryMeasure",
                    concept=get_concept_ref(primary),
                    representation=RepresentationInfo(is_enumerated=False, text_format="Numeric"),
                )

        # Build nodes
        nodes: list[StructureNode] = []
        edges: list[StructureEdge] = []

        target_node = StructureNode(
            node_id=f"dsd_{dsd_id}".replace("-", "_"),
            structure_type="datastructure",
            id=dsd_id,
            agency=agency,
            version=dsd_version,
            name=dsd_name,
            is_target=True,
        )
        nodes.append(target_node)

        # Add parent dataflow nodes
        for df in parent_dataflows:
            df_node = StructureNode(
                node_id=f"df_{df['id']}".replace("-", "_"),
                structure_type="dataflow",
                id=df["id"],
                agency=agency,
                version=df["version"],
                name=df["name"],
                is_target=False,
            )
            nodes.append(df_node)
            edges.append(
                StructureEdge(
                    source=df_node.node_id,
                    target=target_node.node_id,
                    relationship="based on",
                    label="based on",
                )
            )

        # Generate interpretation
        interpretation = [
            f"**DSD:** {dsd_id} v{dsd_version} - {dsd_name}",
        ]
        if parent_dataflows:
            interpretation.append("")
            interpretation.append(f"**Used by {len(parent_dataflows)} dataflow(s):**")
            for df in parent_dataflows:
                interpretation.append(f"  - {df['id']}: {df['name']}")

        interpretation.append("")
        interpretation.append(f"**Dimensions ({len(dimensions)}):**")
        for dim in dimensions:
            rep = (
                f"→ {dim.representation.codelist_id}"
                if dim.representation.is_enumerated
                else f"→ [{dim.representation.text_format}]"
            )
            interpretation.append(f"  {dim.position}. {dim.id} (concept: {dim.concept.id}) {rep}")

        interpretation.append("")
        interpretation.append(f"**Attributes ({len(attributes)}):**")
        for attr in attributes:
            status = f"[{attr.assignment_status}]" if attr.assignment_status else ""
            rep = (
                f"→ {attr.representation.codelist_id}"
                if attr.representation.is_enumerated
                else "→ [Free text]"
            )
            interpretation.append(f"  - {attr.id} {status} (concept: {attr.concept.id}) {rep}")

        if measure:
            interpretation.append("")
            interpretation.append(f"**Measure:** {measure.id} (concept: {measure.concept.id})")

        interpretation.append("")
        interpretation.append(
            f"**Concept Schemes ({len(concept_schemes_map)}):** {', '.join(concept_schemes_map.keys())}"
        )
        interpretation.append(
            f"**Codelists ({len(codelists_map)}):** {', '.join(codelists_map.keys())}"
        )

        # Generate diagram
        mermaid_diagram = _generate_sdmx_dsd_diagram(
            dsd_id=dsd_id,
            dsd_name=dsd_name,
            dsd_version=dsd_version,
            parent_dataflows=parent_dataflows,
            dimensions=dimensions,
            attributes=attributes,
            measure=measure,
            concept_schemes=list(concept_schemes_map.values()),
            codelists=list(codelists_map.values()),
            show_versions=show_versions,
        )

        return StructureDiagramResult(
            discovery_level="dsd_hierarchy",
            target=target_node,
            direction="both",
            depth=3,
            nodes=nodes,
            edges=edges,
            mermaid_diagram=mermaid_diagram,
            interpretation=interpretation,
            api_calls_made=api_calls,
        )

    except Exception as e:
        logger.exception("Failed to generate DSD hierarchy diagram")
        error_node = StructureNode(
            node_id="error",
            structure_type="datastructure",
            id=dsd_id,
            agency=agency,
            version=version,
            name=f"Error: {str(e)}",
            is_target=True,
        )
        return StructureDiagramResult(
            discovery_level="dsd_hierarchy",
            target=error_node,
            direction="both",
            depth=1,
            nodes=[error_node],
            edges=[],
            mermaid_diagram=f'graph TD\n    error["❌ Error: {str(e)}"]',
            interpretation=[f"Error: {str(e)}"],
            api_calls_made=api_calls,
        )


def _generate_sdmx_dsd_diagram(
    dsd_id: str,
    dsd_name: str,
    dsd_version: str,
    parent_dataflows: list[dict[str, str]],
    dimensions: list[ComponentInfo],
    attributes: list[ComponentInfo],
    measure: ComponentInfo | None,
    concept_schemes: list[dict[str, str]],
    codelists: list[dict[str, str]],
    show_versions: bool = False,
) -> str:
    """Generate Mermaid diagram for a DSD with parent dataflows."""
    lines = ["graph TB"]

    # Styling
    lines.append("    %% Styling")
    lines.append("    classDef dataflow fill:#e3f2fd,stroke:#1565c0,stroke-width:2px")
    lines.append("    classDef dsd fill:#e8f5e9,stroke:#2e7d32,stroke-width:3px")
    lines.append("    classDef dimension fill:#fff3e0,stroke:#ef6c00,stroke-width:1px")
    lines.append("    classDef attribute fill:#fce4ec,stroke:#c2185b,stroke-width:1px")
    lines.append("    classDef measure fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px")
    lines.append("    classDef concept fill:#e0f7fa,stroke:#00838f,stroke-width:1px")
    lines.append("    classDef codelist fill:#fff8e1,stroke:#ff8f00,stroke-width:1px")
    lines.append(
        "    classDef freetext fill:#eceff1,stroke:#546e7a,stroke-width:1px,stroke-dasharray: 5 5"
    )
    lines.append("")

    def v(version: str) -> str:
        return f" v{version}" if show_versions and version else ""

    # Parent dataflows (if any)
    if parent_dataflows:
        lines.append('    subgraph DFS["📊 Dataflows using this DSD"]')
        lines.append("        direction LR")
        for df in parent_dataflows:
            df_node = f"df_{df['id']}".replace("-", "_").replace(".", "_")
            df_display = df.get("name", df["id"])[:30]
            lines.append(
                f'        {df_node}[/"📊 {df["id"]}{v(df.get("version", ""))}<br/>{df_display}"/]'
            )
            lines.append(f"        class {df_node} dataflow")
        lines.append("    end")
        lines.append("")

    # DSD node (target)
    safe_name = dsd_name.replace('"', "'")[:40]
    lines.append(f'    DSD["🏗️ <b>{dsd_id}</b>{v(dsd_version)}<br/>{safe_name}"]')
    lines.append("    class DSD dsd")

    # Connect dataflows to DSD
    for df in parent_dataflows:
        df_node = f"df_{df['id']}".replace("-", "_").replace(".", "_")
        lines.append(f"    {df_node} -->|based on| DSD")
    lines.append("")

    # Components - Dimensions
    lines.append('    subgraph DIMS["📐 Dimensions"]')
    lines.append("        direction TB")
    for dim in dimensions:
        dim_id = f"dim_{dim.id}"
        pos_str = f"[{dim.position}]" if dim.position is not None else ""
        lines.append(f'        {dim_id}["{pos_str} {dim.id}"]')
        lines.append(f"        class {dim_id} dimension")
    lines.append("    end")
    lines.append("    DSD --> DIMS")
    lines.append("")

    # Attributes
    if attributes:
        lines.append('    subgraph ATTRS["📎 Attributes"]')
        lines.append("        direction TB")
        for attr in attributes:
            attr_id = f"attr_{attr.id}"
            status = f"[{attr.assignment_status[0]}]" if attr.assignment_status else ""
            lines.append(f'        {attr_id}["{status} {attr.id}"]')
            lines.append(f"        class {attr_id} attribute")
        lines.append("    end")
        lines.append("    DSD --> ATTRS")
        lines.append("")

    # Measure
    if measure:
        lines.append('    subgraph MEAS["📏 Measure"]')
        lines.append(f'        meas_{measure.id}["{measure.id}"]')
        lines.append(f"        class meas_{measure.id} measure")
        lines.append("    end")
        lines.append("    DSD --> MEAS")
        lines.append("")

    # Concept Schemes
    if concept_schemes:
        lines.append('    subgraph CS["💡 Concept Schemes"]')
        lines.append("        direction TB")
        for cs in concept_schemes:
            cs_id = f"cs_{cs['id']}".replace("-", "_")
            cs_name = cs.get("name", cs["id"])[:30]
            lines.append(f'        {cs_id}["{cs["id"]}{v(cs.get("version", ""))}<br/>{cs_name}"]')
            lines.append(f"        class {cs_id} concept")
        lines.append("    end")
        lines.append("")

    # Codelists
    if codelists:
        lines.append('    subgraph CL["📋 Codelists"]')
        lines.append("        direction TB")
        for cl in codelists:
            cl_id = f"cl_{cl['id']}".replace("-", "_")
            cl_name = cl.get("name", cl["id"])[:25]
            lines.append(f'        {cl_id}["{cl["id"]}{v(cl.get("version", ""))}<br/>{cl_name}"]')
            lines.append(f"        class {cl_id} codelist")
        lines.append("    end")
        lines.append("")

    # Free text placeholder
    has_freetext = any(not attr.representation.is_enumerated for attr in attributes)
    if has_freetext:
        lines.append('    FREETEXT["📝 Free Text"]')
        lines.append("    class FREETEXT freetext")
        lines.append("")

    # Connect components to concepts and codelists
    lines.append("    %% Component relationships")
    for dim in dimensions:
        dim_node = f"dim_{dim.id}"
        cs_node = f"cs_{dim.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {dim_node} -.->|concept| {cs_node}")
        if dim.representation.is_enumerated and dim.representation.codelist_id:
            cl_node = f"cl_{dim.representation.codelist_id}".replace("-", "_")
            lines.append(f"    {dim_node} -->|coded by| {cl_node}")

    for attr in attributes:
        attr_node = f"attr_{attr.id}"
        cs_node = f"cs_{attr.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {attr_node} -.->|concept| {cs_node}")
        if attr.representation.is_enumerated and attr.representation.codelist_id:
            cl_node = f"cl_{attr.representation.codelist_id}".replace("-", "_")
            lines.append(f"    {attr_node} -->|coded by| {cl_node}")
        else:
            lines.append(f"    {attr_node} -->|free text| FREETEXT")

    if measure:
        meas_node = f"meas_{measure.id}"
        cs_node = f"cs_{measure.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {meas_node} -.->|concept| {cs_node}")

    return "\n".join(lines)


async def _generate_dataflow_hierarchy_diagram(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    agency: str,
    show_versions: bool,
    ctx: Context[Any, Any, Any] | None,
) -> StructureDiagramResult:
    """
    Generate full SDMX hierarchy diagram for a dataflow.

    Shows: Categories → Dataflow → DSD → Components → Concepts/Codelists + Constraints
    """
    import xml.etree.ElementTree as ET

    from utils import SDMX_NAMESPACES

    ns = SDMX_NAMESPACES
    api_calls = 0

    if ctx:
        await ctx.info(f"Fetching SDMX hierarchy for dataflow {dataflow_id}...")

    # Step 1: Get dataflow with ALL references (includes categorisations, constraints)
    dataflow_url = (
        f"{client.base_url}/dataflow/{agency}/{dataflow_id}/latest?references=all&detail=full"
    )
    headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

    try:
        session = await client._get_session()
        resp = await session.get(dataflow_url, headers=headers)
        resp.raise_for_status()
        api_calls += 1

        root = ET.fromstring(resp.content)

        # Extract dataflow info
        df_elem = root.find(".//str:Dataflow", ns)
        if df_elem is None:
            error_node = StructureNode(
                node_id="error",
                structure_type="dataflow",
                id=dataflow_id,
                agency=agency,
                version="latest",
                name=f"Dataflow {dataflow_id} not found",
                is_target=True,
            )
            return StructureDiagramResult(
                discovery_level="dataflow_hierarchy",
                target=error_node,
                direction="children",
                depth=3,
                nodes=[error_node],
                edges=[],
                mermaid_diagram=f'graph TD\n    error["❌ Dataflow {dataflow_id} not found"]',
                interpretation=[f"Error: Dataflow {dataflow_id} not found"],
                api_calls_made=api_calls,
            )

        df_name_elem = df_elem.find("./com:Name", ns)
        df_name = (
            df_name_elem.text if df_name_elem is not None and df_name_elem.text else dataflow_id
        )
        df_version = df_elem.get("version") or "1.0"

        # Get DSD reference
        struct_ref = df_elem.find(".//str:Structure/com:Ref", ns)
        if struct_ref is None:
            struct_ref = df_elem.find(".//str:Structure/Ref", ns)

        dsd_id = struct_ref.get("id", "") if struct_ref is not None else ""
        dsd_agency = struct_ref.get("agencyID") or agency if struct_ref is not None else agency
        dsd_version = struct_ref.get("version") or "1.0" if struct_ref is not None else "1.0"

        # Extract categorisations (what category this dataflow belongs to)
        categorisations: list[dict[str, str]] = []
        for cat_elem in root.findall(".//str:Categorisation", ns):
            cat_id = cat_elem.get("id", "")
            cat_name_elem = cat_elem.find("./com:Name", ns)
            cat_name = (
                cat_name_elem.text if cat_name_elem is not None and cat_name_elem.text else cat_id
            )

            # Find the target category (Target is the category, Source is the dataflow)
            target_ref = cat_elem.find(".//str:Target/Ref", ns)
            if target_ref is None:
                target_ref = cat_elem.find(".//str:Target/com:Ref", ns)

            category_id = target_ref.get("id", "") if target_ref is not None else ""
            category_scheme = (
                target_ref.get("maintainableParentID", "") if target_ref is not None else ""
            )

            categorisations.append(
                {
                    "id": cat_id,
                    "name": cat_name,
                    "category_id": category_id,
                    "category_scheme": category_scheme,
                }
            )

        # Look up category names from CategorySchemes in the response
        category_names: dict[str, str] = {}
        for cs_elem in root.findall(".//str:CategoryScheme", ns):
            for cat in cs_elem.findall(".//str:Category", ns):
                cat_id = cat.get("id", "")
                name_elem = cat.find("./com:Name", ns)
                if name_elem is not None and name_elem.text:
                    category_names[cat_id] = name_elem.text

        # Update categorisations with actual category names
        for cat in categorisations:
            if cat["category_id"] in category_names:
                cat["category_name"] = category_names[cat["category_id"]]
            else:
                cat["category_name"] = cat["category_id"]

        # Extract constraints
        constraints: list[dict[str, str]] = []
        for con_elem in root.findall(".//str:ContentConstraint", ns):
            con_id = con_elem.get("id", "")
            con_name_elem = con_elem.find("./com:Name", ns)
            con_name = (
                con_name_elem.text if con_name_elem is not None and con_name_elem.text else con_id
            )
            con_type = con_elem.get("type", "Unknown")

            constraints.append(
                {
                    "id": con_id,
                    "name": con_name,
                    "type": con_type,  # "Allowed" or "Actual"
                }
            )

        if ctx:
            await ctx.info(
                f"Found DSD: {dsd_id} v{dsd_version}, {len(categorisations)} categorisation(s), {len(constraints)} constraint(s)"
            )

        # Step 2: Get DSD with full details and children
        dsd_url = f"{client.base_url}/datastructure/{dsd_agency}/{dsd_id}/{dsd_version}?references=children&detail=full"
        resp = await session.get(dsd_url, headers=headers)
        resp.raise_for_status()
        api_calls += 1

        root = ET.fromstring(resp.content)
        dsd_elem = root.find(".//str:DataStructure", ns)

        if dsd_elem is None:
            error_node = StructureNode(
                node_id="error",
                structure_type="dataflow",
                id=dataflow_id,
                agency=agency,
                version=df_version,
                name=df_name,
                is_target=True,
            )
            return StructureDiagramResult(
                discovery_level="dataflow_hierarchy",
                target=error_node,
                direction="children",
                depth=3,
                nodes=[error_node],
                edges=[],
                mermaid_diagram=f'graph TD\n    error["❌ DSD {dsd_id} not found"]',
                interpretation=[f"Error: DSD {dsd_id} not found"],
                api_calls_made=api_calls,
            )

        # Collect referenced structures
        concept_schemes_map: dict[str, dict[str, str]] = {}
        codelists_map: dict[str, dict[str, str]] = {}

        # Parse ConceptSchemes from response
        for cs_elem in root.findall(".//str:ConceptScheme", ns):
            cs_id = cs_elem.get("id", "")
            cs_agency = cs_elem.get("agencyID", agency)
            cs_version = cs_elem.get("version", "1.0")
            cs_name_elem = cs_elem.find("./com:Name", ns)
            cs_name = cs_name_elem.text if cs_name_elem is not None and cs_name_elem.text else cs_id
            concept_schemes_map[cs_id] = {
                "id": cs_id,
                "agency": cs_agency or agency,
                "version": cs_version or "1.0",
                "name": cs_name,
            }

        # Parse Codelists from response
        for cl_elem in root.findall(".//str:Codelist", ns):
            cl_id = cl_elem.get("id", "")
            cl_agency = cl_elem.get("agencyID", agency)
            cl_version = cl_elem.get("version", "1.0")
            cl_name_elem = cl_elem.find("./com:Name", ns)
            cl_name = cl_name_elem.text if cl_name_elem is not None and cl_name_elem.text else cl_id
            codelists_map[cl_id] = {
                "id": cl_id,
                "agency": cl_agency or agency,
                "version": cl_version or "1.0",
                "name": cl_name,
            }

        # Helper to extract concept reference
        def get_concept_ref(elem: ET.Element) -> ConceptRef:
            concept_ref = elem.find(".//str:ConceptIdentity/Ref", ns)
            if concept_ref is None:
                concept_ref = elem.find(".//str:ConceptIdentity/com:Ref", ns)
            if concept_ref is not None:
                scheme_id = concept_ref.get("maintainableParentID") or "CS_COMMON"
                if scheme_id not in concept_schemes_map:
                    concept_schemes_map[scheme_id] = {
                        "id": scheme_id,
                        "agency": concept_ref.get("agencyID") or agency,
                        "version": concept_ref.get("maintainableParentVersion") or "1.0",
                        "name": scheme_id,
                    }
                return ConceptRef(
                    id=concept_ref.get("id") or "",
                    scheme_id=scheme_id,
                    scheme_agency=concept_ref.get("agencyID") or agency,
                    scheme_version=concept_ref.get("maintainableParentVersion") or "1.0",
                )
            return ConceptRef(id="UNKNOWN", scheme_id="CS_UNKNOWN")

        # Helper to extract representation
        def get_representation(elem: ET.Element) -> RepresentationInfo:
            cl_ref = elem.find(".//str:LocalRepresentation/str:Enumeration/Ref", ns)
            if cl_ref is None:
                cl_ref = elem.find(".//str:LocalRepresentation/str:Enumeration/com:Ref", ns)

            if cl_ref is not None:
                cl_id = cl_ref.get("id") or ""
                cl_agency = cl_ref.get("agencyID") or agency
                cl_version = cl_ref.get("version") or "1.0"
                if cl_id not in codelists_map:
                    codelists_map[cl_id] = {
                        "id": cl_id,
                        "agency": cl_agency,
                        "version": cl_version,
                        "name": cl_id,
                    }
                return RepresentationInfo(
                    is_enumerated=True,
                    codelist_id=cl_id,
                    codelist_agency=cl_agency,
                    codelist_version=cl_version,
                )
            else:
                text_format = elem.find(".//str:LocalRepresentation/str:TextFormat", ns)
                format_type = text_format.get("textType") if text_format is not None else "String"
                return RepresentationInfo(is_enumerated=False, text_format=format_type)

        # Parse dimensions
        dimensions: list[ComponentInfo] = []
        dim_list = dsd_elem.find(".//str:DimensionList", ns)
        if dim_list is not None:
            for dim in dim_list.findall(".//str:Dimension", ns):
                dimensions.append(
                    ComponentInfo(
                        id=dim.get("id") or "",
                        component_type="Dimension",
                        position=int(dim.get("position") or 0),
                        concept=get_concept_ref(dim),
                        representation=get_representation(dim),
                    )
                )

            time_dim = dim_list.find(".//str:TimeDimension", ns)
            if time_dim is not None:
                dimensions.append(
                    ComponentInfo(
                        id=time_dim.get("id") or "TIME_PERIOD",
                        component_type="TimeDimension",
                        position=int(time_dim.get("position") or 999),
                        concept=get_concept_ref(time_dim),
                        representation=RepresentationInfo(
                            is_enumerated=False, text_format="ObservationalTimePeriod"
                        ),
                    )
                )

        dimensions.sort(key=lambda d: d.position or 0)

        # Parse attributes
        attributes: list[ComponentInfo] = []
        attr_list = dsd_elem.find(".//str:AttributeList", ns)
        if attr_list is not None:
            for attr in attr_list.findall(".//str:Attribute", ns):
                attributes.append(
                    ComponentInfo(
                        id=attr.get("id") or "",
                        component_type="Attribute",
                        assignment_status=attr.get("assignmentStatus") or None,
                        concept=get_concept_ref(attr),
                        representation=get_representation(attr),
                    )
                )

        # Parse measure
        measure: ComponentInfo | None = None
        measure_list = dsd_elem.find(".//str:MeasureList", ns)
        if measure_list is not None:
            primary = measure_list.find(".//str:PrimaryMeasure", ns)
            if primary is not None:
                measure = ComponentInfo(
                    id=primary.get("id") or "OBS_VALUE",
                    component_type="PrimaryMeasure",
                    concept=get_concept_ref(primary),
                    representation=RepresentationInfo(is_enumerated=False, text_format="Numeric"),
                )

        # Build nodes and edges for StructureDiagramResult
        nodes: list[StructureNode] = []
        edges: list[StructureEdge] = []

        # Target node (dataflow)
        target_node = StructureNode(
            node_id=f"df_{dataflow_id}".replace("-", "_"),
            structure_type="dataflow",
            id=dataflow_id,
            agency=agency,
            version=df_version,
            name=df_name,
            is_target=True,
        )
        nodes.append(target_node)

        # DSD node
        dsd_node = StructureNode(
            node_id=f"dsd_{dsd_id}".replace("-", "_"),
            structure_type="datastructure",
            id=dsd_id,
            agency=dsd_agency,
            version=dsd_version,
            name="Data Structure Definition",
            is_target=False,
        )
        nodes.append(dsd_node)
        edges.append(
            StructureEdge(
                source=target_node.node_id,
                target=dsd_node.node_id,
                relationship="based on",
                label="based on",
            )
        )

        # Concept scheme nodes
        for cs_id, cs_info in concept_schemes_map.items():
            cs_node = StructureNode(
                node_id=f"cs_{cs_id}".replace("-", "_"),
                structure_type="conceptscheme",
                id=cs_id,
                agency=cs_info["agency"],
                version=cs_info["version"],
                name=cs_info["name"],
                is_target=False,
            )
            nodes.append(cs_node)

        # Codelist nodes
        for cl_id, cl_info in codelists_map.items():
            cl_node = StructureNode(
                node_id=f"cl_{cl_id}".replace("-", "_"),
                structure_type="codelist",
                id=cl_id,
                agency=cl_info["agency"],
                version=cl_info["version"],
                name=cl_info["name"],
                is_target=False,
            )
            nodes.append(cl_node)

        # Generate interpretation
        interpretation = [
            f"**Dataflow:** {dataflow_id} - {df_name}",
            f"**DSD:** {dsd_id} v{dsd_version}",
        ]

        # Add categorisation info
        if categorisations:
            interpretation.append("")
            interpretation.append(f"**Categorised under:**")
            for cat in categorisations:
                interpretation.append(f"  - {cat['category_name']} (from {cat['category_scheme']})")

        # Add constraint info
        if constraints:
            interpretation.append("")
            interpretation.append(f"**Constraints ({len(constraints)}):**")
            for con in constraints:
                interpretation.append(f"  - {con['name']} [{con['type']}]")

        interpretation.append("")
        interpretation.append(f"**Dimensions ({len(dimensions)}):** Define the key structure")
        for dim in dimensions:
            rep = (
                f"→ {dim.representation.codelist_id}"
                if dim.representation.is_enumerated
                else f"→ [{dim.representation.text_format}]"
            )
            interpretation.append(f"  {dim.position}. {dim.id} (concept: {dim.concept.id}) {rep}")

        interpretation.append("")
        interpretation.append(f"**Attributes ({len(attributes)}):** Metadata about observations")
        for attr in attributes:
            status = f"[{attr.assignment_status}]" if attr.assignment_status else ""
            rep = (
                f"→ {attr.representation.codelist_id}"
                if attr.representation.is_enumerated
                else "→ [Free text]"
            )
            interpretation.append(f"  - {attr.id} {status} (concept: {attr.concept.id}) {rep}")

        if measure:
            interpretation.append("")
            interpretation.append(f"**Measure:** {measure.id} (concept: {measure.concept.id})")

        interpretation.append("")
        interpretation.append(
            f"**Concept Schemes ({len(concept_schemes_map)}):** {', '.join(concept_schemes_map.keys())}"
        )
        interpretation.append(
            f"**Codelists ({len(codelists_map)}):** {', '.join(codelists_map.keys())}"
        )

        # Generate Mermaid diagram
        mermaid_diagram = _generate_sdmx_dataflow_diagram(
            dataflow_id=dataflow_id,
            dataflow_name=df_name,
            dsd_id=dsd_id,
            dsd_version=dsd_version,
            dimensions=dimensions,
            attributes=attributes,
            measure=measure,
            concept_schemes=list(concept_schemes_map.values()),
            codelists=list(codelists_map.values()),
            categorisations=categorisations,
            constraints=constraints,
            show_versions=show_versions,
        )

        return StructureDiagramResult(
            discovery_level="dataflow_hierarchy",
            target=target_node,
            direction="children",
            depth=3,
            nodes=nodes,
            edges=edges,
            mermaid_diagram=mermaid_diagram,
            interpretation=interpretation,
            api_calls_made=api_calls,
        )

    except Exception as e:
        logger.exception("Failed to generate dataflow hierarchy diagram")
        error_node = StructureNode(
            node_id="error",
            structure_type="dataflow",
            id=dataflow_id,
            agency=agency,
            version="latest",
            name=f"Error: {str(e)}",
            is_target=True,
        )
        return StructureDiagramResult(
            discovery_level="dataflow_hierarchy",
            target=error_node,
            direction="children",
            depth=1,
            nodes=[error_node],
            edges=[],
            mermaid_diagram=f'graph TD\n    error["❌ Error: {str(e)}"]',
            interpretation=[f"Error: {str(e)}"],
            api_calls_made=api_calls,
        )


def _generate_sdmx_dataflow_diagram(
    dataflow_id: str,
    dataflow_name: str,
    dsd_id: str,
    dsd_version: str,
    dimensions: list[ComponentInfo],
    attributes: list[ComponentInfo],
    measure: ComponentInfo | None,
    concept_schemes: list[dict[str, str]],
    codelists: list[dict[str, str]],
    categorisations: list[dict[str, str]] | None = None,
    constraints: list[dict[str, str]] | None = None,
    show_versions: bool = False,
) -> str:
    """
    Generate a Mermaid diagram following the SDMX information model hierarchy.

    Structure:
    - Categories (classification)
    - Dataflow → DSD (based on)
    - DSD → Components (dimensions, attributes, measure)
    - Components → Concepts (semantic meaning)
    - Components → Representations (codelists or free text)
    - Constraints (what data is allowed/available)
    """
    categorisations = categorisations or []
    constraints = constraints or []

    lines = ["graph TB"]

    # Styling definitions
    lines.append("    %% Styling")
    lines.append("    classDef dataflow fill:#e3f2fd,stroke:#1565c0,stroke-width:2px")
    lines.append("    classDef dsd fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px")
    lines.append("    classDef dimension fill:#fff3e0,stroke:#ef6c00,stroke-width:1px")
    lines.append("    classDef attribute fill:#fce4ec,stroke:#c2185b,stroke-width:1px")
    lines.append("    classDef measure fill:#f3e5f5,stroke:#7b1fa2,stroke-width:1px")
    lines.append("    classDef concept fill:#e0f7fa,stroke:#00838f,stroke-width:1px")
    lines.append("    classDef codelist fill:#fff8e1,stroke:#ff8f00,stroke-width:1px")
    lines.append(
        "    classDef freetext fill:#eceff1,stroke:#546e7a,stroke-width:1px,stroke-dasharray: 5 5"
    )
    lines.append("")

    # Version suffix helper
    def v(version: str) -> str:
        return f" v{version}" if show_versions and version else ""

    # Category nodes (if any)
    if categorisations:
        lines.append('    subgraph CAT["🏷️ Categories"]')
        lines.append("        direction LR")
        for cat in categorisations:
            cat_node_id = f"cat_{cat['category_id']}".replace("-", "_").replace(".", "_")
            cat_display = cat.get("category_name", cat["category_id"])[:30]
            lines.append(f'        {cat_node_id}["{cat_display}"]')
            lines.append(f"        style {cat_node_id} fill:#e8eaf6,stroke:#3f51b5")
        lines.append("    end")
        lines.append("")

    # Dataflow node
    safe_name = dataflow_name.replace('"', "'")[:50]
    lines.append(f'    DF[/"📊 <b>{dataflow_id}</b><br/>{safe_name}"/]')
    lines.append("    class DF dataflow")

    # Connect categories to dataflow
    for cat in categorisations:
        cat_node_id = f"cat_{cat['category_id']}".replace("-", "_").replace(".", "_")
        lines.append(f"    {cat_node_id} -->|classifies| DF")
    lines.append("")

    # DSD node
    lines.append(f'    DSD["🏗️ <b>{dsd_id}</b>{v(dsd_version)}<br/>Data Structure Definition"]')
    lines.append("    class DSD dsd")
    lines.append("    DF -->|based on| DSD")
    lines.append("")

    # Component containers (subgraphs)
    lines.append('    subgraph DIMS["📐 Dimensions"]')
    lines.append("        direction TB")
    for dim in dimensions:
        dim_id = f"dim_{dim.id}"
        pos_str = f"[{dim.position}]" if dim.position is not None else ""
        lines.append(f'        {dim_id}["{pos_str} {dim.id}"]')
        lines.append(f"        class {dim_id} dimension")
    lines.append("    end")
    lines.append("    DSD --> DIMS")
    lines.append("")

    if attributes:
        lines.append('    subgraph ATTRS["📎 Attributes"]')
        lines.append("        direction TB")
        for attr in attributes:
            attr_id = f"attr_{attr.id}"
            status = f"[{attr.assignment_status[0]}]" if attr.assignment_status else ""
            lines.append(f'        {attr_id}["{status} {attr.id}"]')
            lines.append(f"        class {attr_id} attribute")
        lines.append("    end")
        lines.append("    DSD --> ATTRS")
        lines.append("")

    if measure:
        lines.append('    subgraph MEAS["📏 Measure"]')
        lines.append(f'        meas_{measure.id}["{measure.id}"]')
        lines.append(f"        class meas_{measure.id} measure")
        lines.append("    end")
        lines.append("    DSD --> MEAS")
        lines.append("")

    # Concept Schemes
    if concept_schemes:
        lines.append('    subgraph CS["💡 Concept Schemes"]')
        lines.append("        direction TB")
        for cs in concept_schemes:
            cs_id = f"cs_{cs['id']}".replace("-", "_")
            cs_name = cs.get("name", cs["id"])[:30]
            lines.append(f'        {cs_id}["{cs["id"]}{v(cs.get("version", ""))}<br/>{cs_name}"]')
            lines.append(f"        class {cs_id} concept")
        lines.append("    end")
        lines.append("")

    # Codelists
    if codelists:
        lines.append('    subgraph CL["📋 Codelists"]')
        lines.append("        direction TB")
        for cl in codelists:
            cl_id = f"cl_{cl['id']}".replace("-", "_")
            cl_name = cl.get("name", cl["id"])[:25]
            lines.append(f'        {cl_id}["{cl["id"]}{v(cl.get("version", ""))}<br/>{cl_name}"]')
            lines.append(f"        class {cl_id} codelist")
        lines.append("    end")
        lines.append("")

    # Free text placeholder for non-enumerated
    has_freetext = any(not attr.representation.is_enumerated for attr in attributes)
    if has_freetext:
        lines.append('    FREETEXT["📝 Free Text"]')
        lines.append("    class FREETEXT freetext")
        lines.append("")

    # Connect components to concepts and representations
    lines.append("    %% Component → Concept → Representation relationships")

    # Dimensions
    for dim in dimensions:
        dim_node = f"dim_{dim.id}"
        cs_node = f"cs_{dim.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {dim_node} -.->|concept| {cs_node}")

        if dim.representation.is_enumerated and dim.representation.codelist_id:
            cl_node = f"cl_{dim.representation.codelist_id}".replace("-", "_")
            lines.append(f"    {dim_node} -->|coded by| {cl_node}")

    # Attributes
    for attr in attributes:
        attr_node = f"attr_{attr.id}"
        cs_node = f"cs_{attr.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {attr_node} -.->|concept| {cs_node}")

        if attr.representation.is_enumerated and attr.representation.codelist_id:
            cl_node = f"cl_{attr.representation.codelist_id}".replace("-", "_")
            lines.append(f"    {attr_node} -->|coded by| {cl_node}")
        else:
            lines.append(f"    {attr_node} -->|free text| FREETEXT")

    # Measure
    if measure:
        meas_node = f"meas_{measure.id}"
        cs_node = f"cs_{measure.concept.scheme_id}".replace("-", "_")
        lines.append(f"    {meas_node} -.->|concept| {cs_node}")

    # Constraints (if any)
    if constraints:
        lines.append("")
        lines.append('    subgraph CONS["🔒 Constraints"]')
        lines.append("        direction TB")
        for con in constraints:
            con_node_id = f"con_{con['id']}".replace("-", "_").replace(".", "_")
            con_type = con.get("type", "Unknown")
            con_name = con.get("name", con["id"])[:25]
            icon = "✓" if con_type == "Actual" else "⚡"
            lines.append(f'        {con_node_id}["{icon} {con_name}<br/>[{con_type}]"]')
            if con_type == "Actual":
                lines.append(f"        style {con_node_id} fill:#e8f5e9,stroke:#4caf50")
            else:
                lines.append(f"        style {con_node_id} fill:#fff3e0,stroke:#ff9800")
        lines.append("    end")
        lines.append("    DF --> CONS")

    return "\n".join(lines)


def _generate_diff_diagram(
    structure_a: StructureNode,
    structure_b: StructureNode,
    changes: list[ReferenceChange],
) -> str:
    """Generate a Mermaid diagram highlighting differences between two structures.

    Color coding:
    - Green (#c8e6c9): Added in B
    - Red (#ffcdd2): Removed from A
    - Yellow (#fff9c4): Version changed
    - Default: Unchanged
    """
    # Icon mapping
    icons = {
        "dataflow": "📊",
        "datastructure": "🏗️",
        "dsd": "🏗️",
        "codelist": "📋",
        "conceptscheme": "💡",
        "categoryscheme": "📁",
        "constraint": "🔒",
    }

    lines = ["graph LR"]

    # Add structure A and B nodes
    icon_a = icons.get(structure_a.structure_type, "📦")
    icon_b = icons.get(structure_b.structure_type, "📦")

    lines.append('    subgraph comparison["Structure Comparison"]')
    lines.append(f'        A["{icon_a} {structure_a.id}<br/>v{structure_a.version}"]')
    lines.append(f'        B["{icon_b} {structure_b.id}<br/>v{structure_b.version}"]')
    lines.append("    end")

    # Group changes by type
    added = [c for c in changes if c.change_type == "added"]
    removed = [c for c in changes if c.change_type == "removed"]
    version_changed = [c for c in changes if c.change_type == "version_changed"]
    unchanged = [c for c in changes if c.change_type == "unchanged"]

    # Add subgraphs for each change type
    if added:
        lines.append('    subgraph added_group["➕ Added"]')
        for c in added:
            icon = icons.get(c.structure_type, "📦")
            node_id = f"add_{c.id}".replace("-", "_").replace(".", "_")
            lines.append(f'        {node_id}["{icon} {c.id}<br/>v{c.version_b}"]')
        lines.append("    end")

    if removed:
        lines.append('    subgraph removed_group["➖ Removed"]')
        for c in removed:
            icon = icons.get(c.structure_type, "📦")
            node_id = f"rem_{c.id}".replace("-", "_").replace(".", "_")
            lines.append(f'        {node_id}["{icon} {c.id}<br/>v{c.version_a}"]')
        lines.append("    end")

    if version_changed:
        lines.append('    subgraph changed_group["🔄 Version Changed"]')
        for c in version_changed:
            icon = icons.get(c.structure_type, "📦")
            node_id = f"chg_{c.id}".replace("-", "_").replace(".", "_")
            lines.append(f'        {node_id}["{icon} {c.id}<br/>v{c.version_a} → v{c.version_b}"]')
        lines.append("    end")

    if unchanged and len(unchanged) <= 5:
        # Only show unchanged if there are few of them
        lines.append('    subgraph unchanged_group["✓ Unchanged"]')
        for c in unchanged:
            icon = icons.get(c.structure_type, "📦")
            node_id = f"unc_{c.id}".replace("-", "_").replace(".", "_")
            lines.append(f'        {node_id}["{icon} {c.id}<br/>v{c.version_a}"]')
        lines.append("    end")
    elif unchanged:
        # Summarize if too many
        lines.append('    subgraph unchanged_group["✓ Unchanged"]')
        lines.append(f'        unc_summary["{len(unchanged)} references unchanged"]')
        lines.append("    end")

    # Add edges from A to removed, from B to added
    for c in removed:
        node_id = f"rem_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    A -.->|removed| {node_id}")

    for c in added:
        node_id = f"add_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    B -->|added| {node_id}")

    for c in version_changed:
        node_id = f"chg_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    A -.->|was| {node_id}")
        lines.append(f"    B -->|now| {node_id}")

    # Add styling
    lines.append("    style A fill:#e3f2fd,stroke:#1976d2,stroke-width:2px")
    lines.append("    style B fill:#e3f2fd,stroke:#1976d2,stroke-width:2px")

    for c in added:
        node_id = f"add_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    style {node_id} fill:#c8e6c9,stroke:#388e3c")

    for c in removed:
        node_id = f"rem_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    style {node_id} fill:#ffcdd2,stroke:#d32f2f")

    for c in version_changed:
        node_id = f"chg_{c.id}".replace("-", "_").replace(".", "_")
        lines.append(f"    style {node_id} fill:#fff9c4,stroke:#fbc02d")

    return "\n".join(lines)


def _generate_codelist_diff_diagram(
    structure_a: StructureNode,
    structure_b: StructureNode,
    code_changes: list[CodeChange],
) -> str:
    """Generate a Mermaid diagram for codelist comparison showing code differences."""
    lines = ["graph LR"]

    # Add codelist nodes
    lines.append('    subgraph comparison["Codelist Comparison"]')
    lines.append(f'        A["📋 {structure_a.id}<br/>v{structure_a.version}"]')
    lines.append(f'        B["📋 {structure_b.id}<br/>v{structure_b.version}"]')
    lines.append("    end")

    # Group changes
    added = [c for c in code_changes if c.change_type == "added"]
    removed = [c for c in code_changes if c.change_type == "removed"]
    name_changed = [c for c in code_changes if c.change_type == "name_changed"]
    unchanged = [c for c in code_changes if c.change_type == "unchanged"]

    # Show added codes (limit to 10)
    if added:
        lines.append('    subgraph added_group["➕ Added Codes"]')
        for c in added[:10]:
            node_id = f"add_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
            safe_name = (c.name_b or c.code_id)[:25].replace('"', "'")
            lines.append(f'        {node_id}["{c.code_id}<br/>{safe_name}"]')
        if len(added) > 10:
            lines.append(f'        add_more["... +{len(added) - 10} more"]')
        lines.append("    end")

    # Show removed codes (limit to 10)
    if removed:
        lines.append('    subgraph removed_group["➖ Removed Codes"]')
        for c in removed[:10]:
            node_id = f"rem_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
            safe_name = (c.name_a or c.code_id)[:25].replace('"', "'")
            lines.append(f'        {node_id}["{c.code_id}<br/>{safe_name}"]')
        if len(removed) > 10:
            lines.append(f'        rem_more["... +{len(removed) - 10} more"]')
        lines.append("    end")

    # Show name changes (limit to 5)
    if name_changed:
        lines.append('    subgraph changed_group["🔄 Name Changed"]')
        for c in name_changed[:5]:
            node_id = f"chg_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
            lines.append(f'        {node_id}["{c.code_id}"]')
        if len(name_changed) > 5:
            lines.append(f'        chg_more["... +{len(name_changed) - 5} more"]')
        lines.append("    end")

    # Summarize unchanged
    if unchanged:
        lines.append('    subgraph unchanged_group["✓ Unchanged"]')
        lines.append(f'        unc_summary["{len(unchanged)} codes unchanged"]')
        lines.append("    end")

    # Add styling
    lines.append("    style A fill:#e3f2fd,stroke:#1976d2,stroke-width:2px")
    lines.append("    style B fill:#e3f2fd,stroke:#1976d2,stroke-width:2px")

    for c in added[:10]:
        node_id = f"add_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
        lines.append(f"    style {node_id} fill:#c8e6c9,stroke:#388e3c")

    for c in removed[:10]:
        node_id = f"rem_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
        lines.append(f"    style {node_id} fill:#ffcdd2,stroke:#d32f2f")

    for c in name_changed[:5]:
        node_id = f"chg_{c.code_id}".replace("-", "_").replace(".", "_").replace(" ", "_")
        lines.append(f"    style {node_id} fill:#fff9c4,stroke:#fbc02d")

    return "\n".join(lines)


async def _compare_codelists(
    client: "SDMXProgressiveClient",
    codelist_id_a: str,
    codelist_id_b: str,
    version_a: str,
    version_b: str,
    agency: str,
    show_diagram: bool,
    ctx: Context[Any, Any, Any] | None,
) -> StructureComparisonResult:
    """Compare two codelists by their codes."""
    api_calls = 0

    # Fetch codelist A
    result_a = await client.browse_codelist(
        codelist_id=codelist_id_a,
        agency_id=agency,
        version=version_a,
        ctx=ctx,
    )
    api_calls += 1

    if "error" in result_a:
        error_node = StructureNode(
            node_id="error_a",
            structure_type="codelist",
            id=codelist_id_a,
            agency=agency,
            version=version_a,
            name=f"Error: {result_a['error']}",
            is_target=True,
        )
        return StructureComparisonResult(
            structure_a=error_node,
            structure_b=error_node,
            comparison_type="version_comparison"
            if codelist_id_a == codelist_id_b
            else "cross_structure",
            structure_type="codelist",
            summary=ComparisonSummary(),
            interpretation=[f"Error fetching codelist A: {result_a['error']}"],
            api_calls_made=api_calls,
            note="Comparison failed",
        )

    # Fetch codelist B
    result_b = await client.browse_codelist(
        codelist_id=codelist_id_b,
        agency_id=agency,
        version=version_b,
        ctx=ctx,
    )
    api_calls += 1

    if "error" in result_b:
        node_a = StructureNode(
            node_id="codelist_a",
            structure_type="codelist",
            id=result_a.get("codelist_id", codelist_id_a),
            agency=result_a.get("agency_id", agency),
            version=result_a.get("version", version_a),
            name=result_a.get("name", codelist_id_a),
            is_target=True,
        )
        error_node = StructureNode(
            node_id="error_b",
            structure_type="codelist",
            id=codelist_id_b,
            agency=agency,
            version=version_b,
            name=f"Error: {result_b['error']}",
            is_target=False,
        )
        return StructureComparisonResult(
            structure_a=node_a,
            structure_b=error_node,
            comparison_type="version_comparison"
            if codelist_id_a == codelist_id_b
            else "cross_structure",
            structure_type="codelist",
            summary=ComparisonSummary(),
            interpretation=[f"Error fetching codelist B: {result_b['error']}"],
            api_calls_made=api_calls,
            note="Comparison failed",
        )

    # Build structure nodes
    node_a = StructureNode(
        node_id="codelist_a",
        structure_type="codelist",
        id=result_a.get("codelist_id", codelist_id_a),
        agency=result_a.get("agency_id", agency),
        version=result_a.get("version", version_a),
        name=result_a.get("name", codelist_id_a),
        is_target=True,
    )

    node_b = StructureNode(
        node_id="codelist_b",
        structure_type="codelist",
        id=result_b.get("codelist_id", codelist_id_b),
        agency=result_b.get("agency_id", agency),
        version=result_b.get("version", version_b),
        name=result_b.get("name", codelist_id_b),
        is_target=False,
    )

    # Build code maps: {code_id: {name, description}}
    codes_a: dict[str, dict] = {c["id"]: c for c in result_a.get("codes", [])}
    codes_b: dict[str, dict] = {c["id"]: c for c in result_b.get("codes", [])}

    # Compare codes
    code_changes: list[CodeChange] = []
    all_code_ids = set(codes_a.keys()) | set(codes_b.keys())

    for code_id in sorted(all_code_ids):
        in_a = code_id in codes_a
        in_b = code_id in codes_b

        if in_a and in_b:
            name_a = codes_a[code_id].get("name", "")
            name_b = codes_b[code_id].get("name", "")
            if name_a != name_b:
                code_changes.append(
                    CodeChange(
                        code_id=code_id,
                        name_a=name_a,
                        name_b=name_b,
                        change_type="name_changed",
                    )
                )
            else:
                code_changes.append(
                    CodeChange(
                        code_id=code_id,
                        name_a=name_a,
                        name_b=name_b,
                        change_type="unchanged",
                    )
                )
        elif in_a:
            code_changes.append(
                CodeChange(
                    code_id=code_id,
                    name_a=codes_a[code_id].get("name", ""),
                    name_b=None,
                    change_type="removed",
                )
            )
        else:
            code_changes.append(
                CodeChange(
                    code_id=code_id,
                    name_a=None,
                    name_b=codes_b[code_id].get("name", ""),
                    change_type="added",
                )
            )

    # Build summary
    summary = ComparisonSummary(
        added=sum(1 for c in code_changes if c.change_type == "added"),
        removed=sum(1 for c in code_changes if c.change_type == "removed"),
        modified=sum(1 for c in code_changes if c.change_type == "name_changed"),
        unchanged=sum(1 for c in code_changes if c.change_type == "unchanged"),
    )
    summary.total_changes = summary.added + summary.removed + summary.modified

    # Build interpretation
    comparison_type = "version_comparison" if codelist_id_a == codelist_id_b else "cross_structure"
    interpretation: list[str] = []

    if comparison_type == "version_comparison":
        interpretation.append(
            f"**Comparing codelist {codelist_id_a}**: v{node_a.version} → v{node_b.version}"
        )
    else:
        interpretation.append(
            f"**Comparing codelists**: {codelist_id_a} v{node_a.version} vs {codelist_id_b} v{node_b.version}"
        )

    interpretation.append(f"Total codes: A has {len(codes_a)}, B has {len(codes_b)}")
    interpretation.append("")

    if summary.total_changes == 0:
        interpretation.append("✅ **No changes detected** - codelists have identical codes.")
    else:
        interpretation.append(f"📊 **Summary**: {summary.total_changes} change(s) detected")
        interpretation.append(f"   - ➕ Added codes: {summary.added}")
        interpretation.append(f"   - ➖ Removed codes: {summary.removed}")
        interpretation.append(f"   - 🔄 Name changed: {summary.modified}")
        interpretation.append(f"   - ✓ Unchanged: {summary.unchanged}")

    # Detail added codes (limit to 10)
    added_codes = [c for c in code_changes if c.change_type == "added"]
    if added_codes:
        interpretation.append("")
        interpretation.append("**➕ Added codes:**")
        for c in added_codes[:10]:
            interpretation.append(f"   - `{c.code_id}`: {c.name_b}")
        if len(added_codes) > 10:
            interpretation.append(f"   ... and {len(added_codes) - 10} more")

    # Detail removed codes (limit to 10)
    removed_codes = [c for c in code_changes if c.change_type == "removed"]
    if removed_codes:
        interpretation.append("")
        interpretation.append("**➖ Removed codes:**")
        for c in removed_codes[:10]:
            interpretation.append(f"   - `{c.code_id}`: {c.name_a}")
        if len(removed_codes) > 10:
            interpretation.append(f"   ... and {len(removed_codes) - 10} more")

    # Detail name changes (limit to 5)
    name_changed = [c for c in code_changes if c.change_type == "name_changed"]
    if name_changed:
        interpretation.append("")
        interpretation.append("**🔄 Name changes:**")
        for c in name_changed[:5]:
            interpretation.append(f'   - `{c.code_id}`: "{c.name_a}" → "{c.name_b}"')
        if len(name_changed) > 5:
            interpretation.append(f"   ... and {len(name_changed) - 5} more")

    # Generate diagram
    mermaid_diagram = None
    if show_diagram and summary.total_changes > 0:
        mermaid_diagram = _generate_codelist_diff_diagram(node_a, node_b, code_changes)

    return StructureComparisonResult(
        structure_a=node_a,
        structure_b=node_b,
        comparison_type=comparison_type,
        structure_type="codelist",
        code_changes=code_changes,
        summary=summary,
        mermaid_diff_diagram=mermaid_diagram,
        interpretation=interpretation,
        api_calls_made=api_calls,
        note=None,
    )


@mcp.tool()
async def compare_structures(
    structure_type: str,
    structure_id_a: str,
    structure_id_b: str | None = None,
    version_a: str = "latest",
    version_b: str = "latest",
    agency_id: str | None = None,
    show_diagram: bool = True,
    ctx: Context[Any, Any, Any] | None = None,
) -> StructureComparisonResult:
    """
    Compare two SDMX structures to identify differences.

    Supports comparing different structure types with specialized logic:

    **Codelists** (`structure_type="codelist"`):
    - Compares actual codes (code IDs and names)
    - Shows added/removed/renamed codes
    - Perfect for: "What codes changed between CL_GEO v1.0 and v2.0?"

    **Data Structure Definitions** (`structure_type="datastructure"`):
    - Compares codelist/concept scheme references
    - Shows version changes in referenced codelists
    - Perfect for: "What codelists were updated in DSD v3.0?"

    **Dataflows** (`structure_type="dataflow"`):
    - Compares structural references (DSD, constraints)
    - Perfect for: "What structures do these dataflows share?"

    Args:
        structure_type: Type of structure to compare:
            - "codelist": Compare codes within codelists
            - "datastructure" or "dsd": Compare DSD references
            - "dataflow": Compare dataflow references
            - "conceptscheme": Compare concept schemes
        structure_id_a: First structure identifier
        structure_id_b: Second structure identifier (defaults to same as A for version comparison)
        version_a: Version of first structure (default "latest")
        version_b: Version of second structure (default "latest")
        agency_id: Agency ID (uses current endpoint's default if not specified)
        show_diagram: Generate a Mermaid diff diagram (default True)

    Returns:
        StructureComparisonResult with type-specific changes:
            - code_changes: For codelist comparisons
            - reference_changes: For DSD/dataflow comparisons
            - summary: Counts of added/removed/modified/unchanged
            - mermaid_diff_diagram: Visual diff diagram
            - interpretation: Human-readable explanation

    Examples:
        # Compare two versions of a codelist - see what codes changed
        >>> compare_structures("codelist", "CL_GEO", version_a="1.0", version_b="2.0")

        # Compare two different codelists - find intersection/differences
        >>> compare_structures("codelist", "CL_FREQ", "CL_TIME_FREQ")

        # Compare DSD versions - see what codelist references changed
        >>> compare_structures("datastructure", "DSD_SDG", version_a="2.0", version_b="3.0")

        # Compare two different DSDs
        >>> compare_structures("datastructure", "DSD_SDG", "DSD_EDUCATION")
    """
    client = get_session_client(ctx)
    agency = agency_id or client.agency_id

    # If structure_id_b is not provided, compare versions of the same structure
    if structure_id_b is None:
        structure_id_b = structure_id_a
        comparison_type = "version_comparison"
    else:
        comparison_type = "cross_structure"

    if ctx:
        if comparison_type == "version_comparison":
            ctx.info(f"Comparing {structure_type}/{structure_id_a} v{version_a} vs v{version_b}...")
        else:
            ctx.info(f"Comparing {structure_type}/{structure_id_a} vs {structure_id_b}...")

    # Dispatch to specialized comparison based on structure type
    if structure_type.lower() == "codelist":
        return await _compare_codelists(
            client=client,
            codelist_id_a=structure_id_a,
            codelist_id_b=structure_id_b,
            version_a=version_a,
            version_b=version_b,
            agency=agency,
            show_diagram=show_diagram,
            ctx=ctx,
        )

    # For other structure types, use reference-based comparison (existing logic)

    api_calls = 0

    # Fetch structure A with children
    result_a = await client.get_structure_references(
        structure_type=structure_type,
        structure_id=structure_id_a,
        agency_id=agency,
        version=version_a,
        direction="children",
        ctx=ctx,
    )
    api_calls += 1

    if "error" in result_a:
        error_node = StructureNode(
            node_id="error_a",
            structure_type=structure_type,
            id=structure_id_a,
            agency=agency,
            version=version_a,
            name=f"Error: {result_a['error']}",
            is_target=True,
        )
        return StructureComparisonResult(
            structure_a=error_node,
            structure_b=error_node,
            comparison_type=comparison_type,
            changes=[],
            summary=ComparisonSummary(),
            mermaid_diff_diagram=None,
            interpretation=[f"Error fetching structure A: {result_a['error']}"],
            api_calls_made=api_calls,
            note="Comparison failed due to error fetching first structure",
        )

    # Fetch structure B with children
    result_b = await client.get_structure_references(
        structure_type=structure_type,
        structure_id=structure_id_b,
        agency_id=agency,
        version=version_b,
        direction="children",
        ctx=ctx,
    )
    api_calls += 1

    if "error" in result_b:
        target_a = result_a.get("target", {})
        node_a = StructureNode(
            node_id=f"{structure_type}_{structure_id_a}".replace("-", "_").replace(".", "_"),
            structure_type=target_a.get("type", structure_type),
            id=target_a.get("id", structure_id_a),
            agency=target_a.get("agency", agency),
            version=target_a.get("version", version_a),
            name=target_a.get("name", structure_id_a),
            is_target=True,
        )
        error_node = StructureNode(
            node_id="error_b",
            structure_type=structure_type,
            id=structure_id_b,
            agency=agency,
            version=version_b,
            name=f"Error: {result_b['error']}",
            is_target=False,
        )
        return StructureComparisonResult(
            structure_a=node_a,
            structure_b=error_node,
            comparison_type=comparison_type,
            changes=[],
            summary=ComparisonSummary(),
            mermaid_diff_diagram=None,
            interpretation=[f"Error fetching structure B: {result_b['error']}"],
            api_calls_made=api_calls,
            note="Comparison failed due to error fetching second structure",
        )

    # Build structure nodes
    target_a = result_a.get("target", {})
    target_b = result_b.get("target", {})

    node_a = StructureNode(
        node_id=f"{structure_type}_{structure_id_a}_a".replace("-", "_").replace(".", "_"),
        structure_type=target_a.get("type", structure_type),
        id=target_a.get("id", structure_id_a),
        agency=target_a.get("agency", agency),
        version=target_a.get("version", version_a),
        name=target_a.get("name", structure_id_a),
        is_target=True,
    )

    node_b = StructureNode(
        node_id=f"{structure_type}_{structure_id_b}_b".replace("-", "_").replace(".", "_"),
        structure_type=target_b.get("type", structure_type),
        id=target_b.get("id", structure_id_b),
        agency=target_b.get("agency", agency),
        version=target_b.get("version", version_b),
        name=target_b.get("name", structure_id_b),
        is_target=False,
    )

    # Build reference maps: {(type, id): version}
    children_a = result_a.get("children", [])
    children_b = result_b.get("children", [])

    refs_a: dict[tuple[str, str], dict] = {}
    for child in children_a:
        key = (child["type"], child["id"])
        refs_a[key] = child

    refs_b: dict[tuple[str, str], dict] = {}
    for child in children_b:
        key = (child["type"], child["id"])
        refs_b[key] = child

    # Compare references
    changes: list[ReferenceChange] = []
    all_keys = set(refs_a.keys()) | set(refs_b.keys())

    for key in sorted(all_keys):
        struct_type, struct_id = key
        in_a = key in refs_a
        in_b = key in refs_b

        if in_a and in_b:
            # Both have it - check if version changed
            ver_a = refs_a[key].get("version", "1.0")
            ver_b = refs_b[key].get("version", "1.0")
            name = refs_b[key].get("name", struct_id)

            if ver_a != ver_b:
                changes.append(
                    ReferenceChange(
                        structure_type=struct_type,
                        id=struct_id,
                        name=name,
                        version_a=ver_a,
                        version_b=ver_b,
                        change_type="version_changed",
                    )
                )
            else:
                changes.append(
                    ReferenceChange(
                        structure_type=struct_type,
                        id=struct_id,
                        name=name,
                        version_a=ver_a,
                        version_b=ver_b,
                        change_type="unchanged",
                    )
                )
        elif in_a and not in_b:
            # Removed in B
            ver_a = refs_a[key].get("version", "1.0")
            name = refs_a[key].get("name", struct_id)
            changes.append(
                ReferenceChange(
                    structure_type=struct_type,
                    id=struct_id,
                    name=name,
                    version_a=ver_a,
                    version_b=None,
                    change_type="removed",
                )
            )
        else:
            # Added in B
            ver_b = refs_b[key].get("version", "1.0")
            name = refs_b[key].get("name", struct_id)
            changes.append(
                ReferenceChange(
                    structure_type=struct_type,
                    id=struct_id,
                    name=name,
                    version_a=None,
                    version_b=ver_b,
                    change_type="added",
                )
            )

    # Build summary
    summary = ComparisonSummary(
        added=sum(1 for c in changes if c.change_type == "added"),
        removed=sum(1 for c in changes if c.change_type == "removed"),
        modified=sum(1 for c in changes if c.change_type == "version_changed"),
        unchanged=sum(1 for c in changes if c.change_type == "unchanged"),
    )
    summary.total_changes = summary.added + summary.removed + summary.modified

    # Build interpretation
    interpretation: list[str] = []

    if comparison_type == "version_comparison":
        interpretation.append(
            f"**Comparing {structure_type} {structure_id_a}**: v{node_a.version} → v{node_b.version}"
        )
    else:
        interpretation.append(
            f"**Comparing**: {structure_id_a} v{node_a.version} vs {structure_id_b} v{node_b.version}"
        )

    interpretation.append("")

    if summary.total_changes == 0:
        interpretation.append("✅ **No changes detected** - structures have identical references.")
    else:
        interpretation.append(f"📊 **Summary**: {summary.total_changes} change(s) detected")
        interpretation.append(f"   - ➕ Added: {summary.added}")
        interpretation.append(f"   - ➖ Removed: {summary.removed}")
        interpretation.append(f"   - 🔄 Version changed: {summary.modified}")
        interpretation.append(f"   - ✓ Unchanged: {summary.unchanged}")

    # Detail the changes
    if summary.added > 0:
        interpretation.append("")
        interpretation.append("**➕ Added references:**")
        for c in changes:
            if c.change_type == "added":
                interpretation.append(f"   - {c.structure_type}: **{c.id}** v{c.version_b}")

    if summary.removed > 0:
        interpretation.append("")
        interpretation.append("**➖ Removed references:**")
        for c in changes:
            if c.change_type == "removed":
                interpretation.append(f"   - {c.structure_type}: **{c.id}** v{c.version_a}")

    if summary.version_changed > 0:
        interpretation.append("")
        interpretation.append("**🔄 Version changes:**")
        for c in changes:
            if c.change_type == "version_changed":
                interpretation.append(
                    f"   - {c.structure_type}: **{c.id}** v{c.version_a} → v{c.version_b}"
                )

    # Generate diff diagram
    mermaid_diff_diagram = None
    if show_diagram and summary.total_changes > 0:
        mermaid_diff_diagram = _generate_diff_diagram(node_a, node_b, changes)

    return StructureComparisonResult(
        structure_a=node_a,
        structure_b=node_b,
        comparison_type=comparison_type,
        structure_type=structure_type,
        reference_changes=changes,
        summary=summary,
        mermaid_diff_diagram=mermaid_diff_diagram,
        interpretation=interpretation,
        api_calls_made=api_calls,
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
