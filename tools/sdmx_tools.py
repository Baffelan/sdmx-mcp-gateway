"""
Enhanced SDMX MCP tools with progressive discovery capabilities.

These tools implement a layered approach to SDMX metadata discovery,
allowing LLMs to efficiently explore data without overwhelming context windows.

Updated to support multi-user deployments:
- All functions now accept a `client` parameter
- No global state - client is provided per-request from session
"""

from __future__ import annotations

import logging
import os
import sys
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sdmx_progressive_client import SDMXProgressiveClient
from utils import (
    filter_dataflows_by_keywords,
    validate_dataflow_id,
    validate_period,
    validate_sdmx_key,
)

if TYPE_CHECKING:
    from mcp.server.fastmcp import Context

logger = logging.getLogger(__name__)


async def list_dataflows(
    client: SDMXProgressiveClient,
    keywords: list[str] | None = None,
    agency_id: str = "SPC",
    limit: int = 10,
    offset: int = 0,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Step 1: Discover available dataflows with minimal metadata.

    This provides a high-level overview without overwhelming detail.
    Use this to identify dataflows of interest before drilling down.

    Args:
        client: SDMX client instance (from session)
        keywords: Optional list of keywords to filter dataflows
        agency_id: The agency to query (default: "SPC")
        limit: Number of results to return (default: 10)
        offset: Number of results to skip for pagination (default: 0)
        ctx: MCP context for progress reporting
    """
    try:
        if ctx:
            await ctx.info("Discovering dataflows (overview mode)...")

        all_dataflows = await client.discover_dataflows(
            agency_id=agency_id,
            references="none",
            ctx=ctx,
        )

        # Filter by keywords if provided
        if keywords:
            filtered_dataflows = filter_dataflows_by_keywords(all_dataflows, keywords)
        else:
            filtered_dataflows = all_dataflows

        # Apply pagination
        total_count = len(filtered_dataflows)
        start_idx = offset
        end_idx = min(offset + limit, total_count)
        dataflows = filtered_dataflows[start_idx:end_idx]

        # Create lightweight summaries
        summaries: list[dict[str, str]] = []
        for df in dataflows:
            desc = str(df.get("description", ""))
            if len(desc) > 100:
                desc = desc[:100] + "..."
            summaries.append(
                {
                    "id": str(df.get("id", "")),
                    "name": str(df.get("name", "")),
                    "description": desc,
                }
            )

        # Calculate pagination info
        has_more = end_idx < total_count
        next_offset = end_idx if has_more else None

        result: dict[str, Any] = {
            "discovery_level": "overview",
            "agency_id": agency_id,
            "total_found": total_count,
            "showing": len(summaries),
            "offset": offset,
            "limit": limit,
            "keywords": keywords,
            "dataflows": summaries,
            "pagination": {
                "has_more": has_more,
                "next_offset": next_offset,
                "total_pages": (total_count + limit - 1) // limit if limit > 0 else 0,
                "current_page": (offset // limit) + 1 if limit > 0 else 1,
            },
        }

        # Add filtering information if keywords were used
        if keywords:
            result["filter_info"] = {
                "keywords_used": keywords,
                "total_before_filter": len(all_dataflows),
                "total_after_filter": total_count,
                "filter_reduced_by": len(all_dataflows) - total_count,
            }
            result["total_before_filtering"] = len(all_dataflows)
            result["filtering_info"] = (
                f"Found {total_count} dataflows matching keywords out of {len(all_dataflows)} total"
            )

        # Add navigation hints
        if has_more:
            result["next_step"] = (
                f"To see more dataflows, call list_dataflows with offset={next_offset}, "
                "or use get_dataflow_structure() to explore a specific dataflow's dimensions"
            )
        else:
            result["next_step"] = (
                "Use get_dataflow_structure() to explore a specific dataflow's dimensions"
            )

        return result

    except Exception as e:
        logger.exception("Failed to discover dataflows")
        return {"error": str(e), "discovery_level": "overview", "dataflows": []}


def _extract_dict(obj: Any) -> dict[str, Any]:
    """Safely extract a dict from an object that might have to_dict() or already be a dict."""
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict", None)):
        result = obj.to_dict()
        if isinstance(result, dict):
            return result
    return {}


async def get_dataflow_structure(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Step 2: Get the structure of a specific dataflow.

    Returns dimension names and their positions, but NOT all the codes.
    This is much smaller than getting full structure details.

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow ID to get structure for
        agency_id: The agency that owns the dataflow
        ctx: MCP context for progress reporting
    """
    try:
        # Validate input
        if not validate_dataflow_id(dataflow_id):
            return {
                "error": f"Invalid dataflow_id format: {dataflow_id}",
                "hint": "Dataflow IDs should contain only letters, numbers, and underscores",
            }

        if ctx:
            await ctx.info(f"Getting structure for dataflow: {dataflow_id}")

        # Get structure summary using correct method
        structure = await client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            ctx=ctx,
        )

        if not structure:
            return {
                "error": f"No structure found for dataflow: {dataflow_id}",
                "hint": "Use list_dataflows() to discover available dataflows",
            }

        # Extract dimension overview from DataStructureSummary
        dimensions_summary: list[dict[str, Any]] = []
        structure_dict = _extract_dict(structure)
        dims = structure_dict.get("dimensions", [])

        for dim in dims:
            dim_dict = _extract_dict(dim)
            # Extract codelist reference if available
            codelist_ref = dim_dict.get("codelist_ref")
            codelist_str = None
            if codelist_ref:
                cl_id = codelist_ref.get("id", "")
                cl_agency = codelist_ref.get("agency", agency_id)
                cl_version = codelist_ref.get("version", "1.0")
                codelist_str = f"{cl_agency}:{cl_id}({cl_version})"

            dimensions_summary.append(
                {
                    "id": dim_dict.get("id", ""),
                    "name": dim_dict.get("concept", dim_dict.get("id", "")),
                    "position": dim_dict.get("position", 0),
                    "type": dim_dict.get("type", "Dimension"),
                    "codelist": codelist_str,
                    "codelist_ref": codelist_ref,
                }
            )

        # Get dataflow name from overview
        dataflow_name = ""
        try:
            overview = await client.get_dataflow_overview(
                dataflow_id=dataflow_id,
                agency_id=agency_id,
                ctx=ctx,
            )
            if hasattr(overview, "name"):
                dataflow_name = overview.name
        except Exception:
            pass

        # Extract attributes
        attributes = structure_dict.get("attributes", [])

        # Build key template and example
        key_family = structure_dict.get("key_family", [])
        key_template = ".".join([f"{{{dim}}}" for dim in key_family])
        key_example = ".".join(["*" for dim in key_family])

        return {
            "discovery_level": "structure",
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "dataflow_name": dataflow_name,
            "total_dimensions": len(dimensions_summary),
            "structure": {
                "id": structure_dict.get("id", ""),
                "agency": structure_dict.get("agency", agency_id),
                "version": structure_dict.get("version", "latest"),
                "key_template": key_template,
                "key_example": key_example,
                "dimensions": dimensions_summary,
                "attributes": attributes,
                "measure": structure_dict.get("primary_measure"),
            },
            "next_steps": [
                "Use get_dimension_codes(dataflow_id, dimension_id) to see codes for a specific dimension",
                "Use get_data_availability(dataflow_id) to check what data exists",
                "Use build_data_url(dataflow_id, filters) to construct a data query URL",
            ],
        }

    except Exception as e:
        logger.exception("Failed to get structure for %s", dataflow_id)
        return {"error": str(e), "dataflow_id": dataflow_id}


async def get_dimension_codes(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    dimension_id: str,
    agency_id: str = "SPC",
    limit: int = 50,
    offset: int = 0,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Step 3: Get codes for a specific dimension.

    Returns paginated codes for one dimension at a time.

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow containing the dimension
        dimension_id: The dimension to get codes for
        agency_id: The agency that owns the dataflow
        limit: Number of codes to return
        offset: Starting position for pagination
        ctx: MCP context for progress reporting
    """
    try:
        if ctx:
            await ctx.info(f"Getting codes for dimension: {dimension_id}")

        codes_result = await client.get_dimension_codes(
            dataflow_id=dataflow_id,
            dimension_id=dimension_id,
            agency_id=agency_id,
            ctx=ctx,
        )

        if not codes_result or "codes" not in codes_result:
            return {
                "error": f"No codes found for dimension: {dimension_id}",
                "hint": "Use get_dataflow_structure() to see available dimensions",
            }

        codes = codes_result.get("codes", [])
        if not isinstance(codes, list):
            codes = []

        # Apply pagination
        total_count = len(codes)
        start_idx = offset
        end_idx = min(offset + limit, total_count)
        paginated_codes = codes[start_idx:end_idx]

        has_more = end_idx < total_count
        next_offset = end_idx if has_more else None

        return {
            "discovery_level": "codes",
            "dataflow_id": dataflow_id,
            "dimension_id": dimension_id,
            "total_codes": total_count,
            "showing": len(paginated_codes),
            "offset": offset,
            "limit": limit,
            "codes": paginated_codes,
            "pagination": {"has_more": has_more, "next_offset": next_offset},
            "next_step": "Use get_data_availability() to check data existence for specific code combinations",
        }

    except Exception as e:
        logger.exception("Failed to get codes for %s", dimension_id)
        return {"error": str(e), "dimension_id": dimension_id}


async def get_data_availability(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    filters: dict[str, str] | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Step 4: Check data availability before querying.

    This is a lightweight check to see if data exists for given filter criteria.
    Much faster than actually fetching data, and helps refine queries.

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow to check
        filters: Dictionary of dimension_id -> code to filter by
        agency_id: The agency that owns the dataflow
        ctx: MCP context for progress reporting
    """
    try:
        # Validate input
        if not validate_dataflow_id(dataflow_id):
            return {
                "error": f"Invalid dataflow_id format: {dataflow_id}",
                "hint": "Dataflow IDs should contain only letters, numbers, and underscores",
            }

        if ctx:
            await ctx.info(f"Checking data availability for: {dataflow_id}")

        availability = await client.get_actual_availability(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            ctx=ctx,
        )

        # The availability check returns constraint info
        has_data = availability.get("has_constraint", False)
        time_range = availability.get("time_range", {})

        result: dict[str, Any] = {
            "discovery_level": "availability",
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "filters_applied": filters,
            "has_data": has_data,
            "observation_count": None,
            "time_period": time_range if time_range else None,
            "series_count": None,
        }

        if has_data:
            result["next_step"] = "Use build_data_url() to get the full data URL for retrieval"
        else:
            result["next_step"] = (
                "Try different filters - use get_dimension_codes() to see available codes"
            )
            result["suggestions"] = []

        return result

    except Exception as e:
        logger.exception("Failed to check availability for %s", dataflow_id)
        return {
            "error": str(e),
            "dataflow_id": dataflow_id,
            "hint": "This endpoint might not support availability queries. Try build_data_url() directly.",
        }


async def validate_query(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    key: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Validate SDMX query parameters before building URL.

    Checks that the dataflow exists, dimensions are valid, and codes exist.
    Returns detailed validation results and suggestions.

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow to query
        key: SDMX key string (dimension values separated by dots)
        filters: Dictionary of dimension_id -> code (alternative to key)
        start_period: Start time period (e.g., "2020")
        end_period: End time period (e.g., "2023")
        agency_id: The agency that owns the dataflow
        ctx: MCP context for progress reporting
    """
    validation_results: dict[str, Any] = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
        "validated_params": {},
    }

    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    try:
        # Validate dataflow_id format
        if not validate_dataflow_id(dataflow_id):
            validation_results["is_valid"] = False
            errors.append(
                {
                    "field": "dataflow_id",
                    "message": f"Invalid dataflow_id format: {dataflow_id}",
                    "hint": "Dataflow IDs should contain only letters, numbers, and underscores",
                }
            )
            validation_results["errors"] = errors
            return validation_results

        # Validate periods if provided
        if start_period and not validate_period(start_period):
            warnings.append(
                {
                    "field": "start_period",
                    "message": f"Unusual period format: {start_period}",
                    "hint": "Common formats: YYYY, YYYY-MM, YYYY-Q1, YYYY-W01",
                }
            )

        if end_period and not validate_period(end_period):
            warnings.append(
                {
                    "field": "end_period",
                    "message": f"Unusual period format: {end_period}",
                    "hint": "Common formats: YYYY, YYYY-MM, YYYY-Q1, YYYY-W01",
                }
            )

        # Get dataflow structure to validate dimensions
        if ctx:
            await ctx.info(f"Validating query parameters for: {dataflow_id}")

        structure = await client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            ctx=ctx,
        )

        if not structure:
            validation_results["is_valid"] = False
            errors.append(
                {
                    "field": "dataflow_id",
                    "message": f"Dataflow not found: {dataflow_id}",
                    "hint": "Use list_dataflows() to discover available dataflows",
                }
            )
            validation_results["errors"] = errors
            validation_results["warnings"] = warnings
            return validation_results

        # Build dimension lookup from structure summary
        structure_dict = _extract_dict(structure)
        dims_list = structure_dict.get("dimensions", [])

        dimensions: dict[str, dict[str, Any]] = {}
        for d in dims_list:
            d_dict = _extract_dict(d)
            dim_id = d_dict.get("id", "")
            if dim_id:
                dimensions[dim_id] = d_dict

        # Validate filters if provided
        if filters:
            for dim_id, _ in filters.items():
                if dim_id == "TIME_PERIOD":
                    warnings.append(
                        {
                            "field": "filters.TIME_PERIOD",
                            "message": "TIME_PERIOD should use startPeriod/endPeriod parameters, not filters",
                            "hint": "Pass start_period/end_period instead",
                        }
                    )
                    continue
                if dim_id not in dimensions:
                    errors.append(
                        {
                            "field": f"filters.{dim_id}",
                            "message": f"Unknown dimension: {dim_id}",
                            "available_dimensions": list(dimensions.keys()),
                        }
                    )
                    validation_results["is_valid"] = False

        # Validate key if provided
        if key and not validate_sdmx_key(key):
            warnings.append(
                {
                    "field": "key",
                    "message": f"Key format may be invalid: {key}",
                    "hint": "Keys should be dot-separated dimension values",
                }
            )

        validation_results["errors"] = errors
        validation_results["warnings"] = warnings
        validation_results["validated_params"] = {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "dimension_count": len(dimensions),
            "dimensions": list(dimensions.keys()),
        }

        if start_period:
            validation_results["validated_params"]["start_period"] = start_period
        if end_period:
            validation_results["validated_params"]["end_period"] = end_period

        return validation_results

    except Exception as e:
        logger.exception("Failed to validate query for %s", dataflow_id)
        validation_results["is_valid"] = False
        errors.append({"field": "general", "message": str(e)})
        validation_results["errors"] = errors
        return validation_results


def _get_accept_header(output_format: str) -> str:
    """
    Get the Accept header value for a given output format.

    Args:
        output_format: One of 'csv', 'json', 'xml', 'generic', 'structurespecific'

    Returns:
        MIME type string for Accept header
    """
    format_map = {
        "csv": "application/vnd.sdmx.data+csv;version=1.0.0",
        "json": "application/vnd.sdmx.data+json;version=1.0.0",
        "xml": "application/vnd.sdmx.genericdata+xml;version=2.1",
        "generic": "application/vnd.sdmx.genericdata+xml;version=2.1",
        "structurespecific": "application/vnd.sdmx.structurespecificdata+xml;version=2.1",
        "sdmx-json": "application/vnd.sdmx.data+json;version=1.0.0",
        "sdmx-csv": "application/vnd.sdmx.data+csv;version=1.0.0",
        "sdmx-xml": "application/vnd.sdmx.genericdata+xml;version=2.1",
    }
    return format_map.get(output_format.lower(), format_map["csv"])


async def build_data_url(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    key: str | None = None,
    filters: dict[str, str] | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    agency_id: str = "SPC",
    output_format: str = "csv",
    include_headers: bool = True,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Build a complete SDMX data URL for fetching actual data.

    This is the final step in the progressive discovery workflow.
    The URL can be used directly to fetch data via HTTP.

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow to query
        key: SDMX key string (dimension values separated by dots)
        filters: Dictionary of dimension_id -> code (alternative to key)
        start_period: Start time period
        end_period: End time period
        agency_id: The agency that owns the dataflow
        output_format: Desired output format ('csv', 'json', 'xml')
        include_headers: Whether to include HTTP headers in response
        ctx: MCP context for progress reporting
    """
    try:
        # Validate first
        validation = await validate_query(
            client=client,
            dataflow_id=dataflow_id,
            key=key,
            filters=filters,
            start_period=start_period,
            end_period=end_period,
            agency_id=agency_id,
            ctx=ctx,
        )

        if not validation.get("is_valid", False):
            return {
                "error": "Validation failed",
                "validation_errors": validation.get("errors", []),
                "hint": "Fix the validation errors and try again",
            }

        # Build the data key
        data_key: str
        if key:
            data_key = key
        elif filters:
            # Get structure to build key from filters
            structure = await client.get_structure_summary(
                dataflow_id=dataflow_id,
                agency_id=agency_id,
                ctx=ctx,
            )

            if structure:
                structure_dict = _extract_dict(structure)
                dims_list = structure_dict.get("dimensions", [])

                dimensions_sorted: list[dict[str, Any]] = []
                for d in dims_list:
                    d_dict = _extract_dict(d)
                    dimensions_sorted.append(d_dict)

                dimensions_sorted.sort(key=lambda x: x.get("position", 0))

                key_parts: list[str] = []
                for dim in dimensions_sorted:
                    dim_id = str(dim.get("id", ""))
                    if dim.get("type") == "TimeDimension":
                        continue
                    code = filters.get(dim_id, "") if dim_id else ""
                    key_parts.append(code)
                data_key = ".".join(key_parts)
            else:
                data_key = "all"
        else:
            data_key = "all"

        # Build URL using client's base_url
        base_url = client.base_url.rstrip("/")

        # Construct the data URL
        url = f"{base_url}/data/{dataflow_id}/{data_key}"

        # Add query parameters
        params: list[str] = []
        if start_period:
            params.append(f"startPeriod={quote(start_period)}")
        if end_period:
            params.append(f"endPeriod={quote(end_period)}")

        if params:
            url += "?" + "&".join(params)

        # Build result
        result: dict[str, Any] = {
            "url": url,
            "method": "GET",
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "key": data_key,
            "format": output_format,
        }

        if include_headers:
            result["headers"] = {
                "Accept": _get_accept_header(output_format),
                "Accept-Language": "en",
            }

        if start_period:
            result["start_period"] = start_period
        if end_period:
            result["end_period"] = end_period

        validation_warnings = validation.get("warnings", [])
        if validation_warnings:
            result["warnings"] = validation_warnings

        result["usage"] = f"curl -H 'Accept: {_get_accept_header(output_format)}' '{url}'"

        return result

    except Exception as e:
        logger.exception("Failed to build URL for %s", dataflow_id)
        return {"error": str(e), "dataflow_id": dataflow_id}


async def get_discovery_guide(
    client: SDMXProgressiveClient,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Get a guide on how to use the progressive discovery workflow.

    Args:
        client: SDMX client instance (from session)
        ctx: MCP context

    Returns:
        Dictionary with workflow steps and examples
    """
    # ctx is unused but kept for API consistency
    _ = ctx
    return {
        "title": "SDMX Progressive Discovery Workflow",
        "description": "A step-by-step approach to finding and querying SDMX data",
        "current_endpoint": {
            "base_url": client.base_url,
            "agency_id": client.agency_id,
        },
        "steps": [
            {
                "step": 1,
                "name": "Discover Dataflows",
                "tool": "list_dataflows",
                "description": "Find available statistical domains",
                "example": "list_dataflows(keywords=['population', 'census'])",
            },
            {
                "step": 2,
                "name": "Get Structure",
                "tool": "get_dataflow_structure",
                "description": "Understand the dimensions of a dataflow",
                "example": "get_dataflow_structure('DF_POP')",
            },
            {
                "step": 3,
                "name": "Explore Codes",
                "tool": "get_dimension_codes",
                "description": "See available values for each dimension",
                "example": "get_dimension_codes('DF_POP', 'GEO')",
            },
            {
                "step": 4,
                "name": "Check Availability",
                "tool": "get_data_availability",
                "description": "Verify data exists for your query",
                "example": "get_data_availability('DF_POP', filters={'GEO': 'FJ'})",
            },
            {
                "step": 5,
                "name": "Build URL",
                "tool": "build_data_url",
                "description": "Generate the final data retrieval URL",
                "example": "build_data_url('DF_POP', filters={'GEO': 'FJ'})",
            },
        ],
        "tips": [
            "Use keywords to filter dataflows by topic",
            "Start with overview, then drill down progressively",
            "Check availability before building final URLs",
            "Use pagination for large result sets",
        ],
    }


async def build_sdmx_key(
    client: SDMXProgressiveClient,
    dataflow_id: str,
    filters: dict[str, str],
    agency_id: str = "SPC",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Build an SDMX key string from dimension filters.

    The key is used in SDMX URLs to filter data.
    Format: value1.value2.value3 (one value per dimension in order)

    Args:
        client: SDMX client instance (from session)
        dataflow_id: The dataflow to build key for
        filters: Dictionary of dimension_id -> code
        agency_id: The agency that owns the dataflow
        ctx: MCP context for progress reporting

    Returns:
        Dictionary with the built key and explanation
    """
    try:
        # Get structure to understand dimension order
        structure = await client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            ctx=ctx,
        )

        if not structure:
            return {
                "error": f"Could not get structure for dataflow: {dataflow_id}",
                "hint": "Use list_dataflows() to find valid dataflow IDs",
            }

        # Sort dimensions by position
        structure_dict = _extract_dict(structure)
        dims_list = structure_dict.get("dimensions", [])

        dimensions_sorted: list[dict[str, Any]] = []
        for d in dims_list:
            d_dict = _extract_dict(d)
            dimensions_sorted.append(d_dict)

        dimensions_sorted.sort(key=lambda x: x.get("position", 0))

        # Build key parts
        key_parts: list[str] = []
        dimension_mapping: list[dict[str, Any]] = []

        for dim in dimensions_sorted:
            dim_id = str(dim.get("id", ""))
            if dim.get("type") == "TimeDimension":
                continue
            code = filters.get(dim_id, "")  # Empty string = all values
            key_parts.append(code)
            dimension_mapping.append(
                {
                    "position": dim.get("position", len(dimension_mapping)),
                    "dimension": dim_id,
                    "value": code if code else "(all)",
                }
            )

        key = ".".join(key_parts)

        return {
            "key": key,
            "dataflow_id": dataflow_id,
            "dimension_count": len(dimensions_sorted),
            "dimension_mapping": dimension_mapping,
            "filters_applied": {k: v for k, v in filters.items() if v},
            "usage": f"Use this key in data URLs: /data/{dataflow_id}/{key}",
        }

    except Exception as e:
        logger.exception("Failed to build key for %s", dataflow_id)
        return {"error": str(e), "dataflow_id": dataflow_id}


def get_default_client() -> SDMXProgressiveClient:
    """
    Get a default SDMX client for backward compatibility.

    DEPRECATED: Use session-based clients instead.
    """
    return SDMXProgressiveClient()


# Legacy global client for backward compatibility during migration
# Will be removed in future versions
sdmx_client = get_default_client()


async def cleanup_sdmx_client() -> None:
    """Clean up the legacy SDMX client session."""
    global sdmx_client
    if sdmx_client and sdmx_client.session:
        await sdmx_client.close()
