"""
Enhanced SDMX MCP tools with progressive discovery capabilities.

These tools implement a layered approach to SDMX metadata discovery,
allowing LLMs to efficiently explore data without overwhelming context windows.
"""

import logging
from typing import Any, Dict, List, Optional
import json
from urllib.parse import quote

from mcp.server.fastmcp import Context

# Import the progressive client instead of the basic one
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sdmx_progressive_client import SDMXProgressiveClient
from utils import (
    validate_dataflow_id, validate_sdmx_key, validate_provider, validate_period,
    filter_dataflows_by_keywords, SDMX_FORMATS, SDMX_NAMESPACES
)

logger = logging.getLogger(__name__)

# Global progressive SDMX client instance
sdmx_client = SDMXProgressiveClient()


async def list_dataflows(
    keywords: Optional[List[str]] = None,
    agency_id: str = "SPC",
    limit: int = 10,
    offset: int = 0,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 1: Discover available dataflows with minimal metadata.
    
    This provides a high-level overview without overwhelming detail.
    Use this to identify dataflows of interest before drilling down.
    
    Args:
        keywords: Optional list of keywords to filter dataflows
        agency_id: The agency to query (default: "SPC")
        limit: Number of results to return (default: 10)
        offset: Number of results to skip for pagination (default: 0)
        ctx: MCP context for progress reporting
    """
    try:
        if ctx:
            ctx.info("Discovering dataflows (overview mode)...")
        
        # For discovery, we still need to get the list of all dataflows
        # But we'll return minimal info
        # Note: SDMXProgressiveClient already has this functionality
        basic_client = sdmx_client  # Use the existing progressive client instance
        
        all_dataflows = await basic_client.discover_dataflows(
            agency_id=agency_id,
            references="none",  # Minimal references
            ctx=ctx
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
        summaries = []
        for df in dataflows:
            summaries.append({
                "id": df["id"],
                "name": df["name"],
                "description": df["description"][:100] + "..." if len(df["description"]) > 100 else df["description"]
            })
        
        await basic_client.close()
        
        # Calculate pagination info
        has_more = end_idx < total_count
        next_offset = end_idx if has_more else None
        
        result = {
            "discovery_level": "overview",
            "agency_id": agency_id,
            "total_found": total_count,  # This is now the filtered count
            "showing": len(summaries),
            "offset": offset,
            "limit": limit,
            "keywords": keywords,
            "dataflows": summaries,
            "pagination": {
                "has_more": has_more,
                "next_offset": next_offset,
                "total_pages": (total_count + limit - 1) // limit if limit > 0 else 0,
                "current_page": (offset // limit) + 1 if limit > 0 else 1
            }
        }
        
        # Add filtering information if keywords were used
        if keywords:
            result["filter_info"] = {
                "keywords_used": keywords,
                "total_before_filter": len(all_dataflows),
                "total_after_filter": total_count,
                "filter_reduced_by": len(all_dataflows) - total_count
            }
        
        # Add unfiltered total if keywords were used
        if keywords:
            result["total_before_filtering"] = len(all_dataflows)
            result["filtering_info"] = f"Found {total_count} dataflows matching keywords out of {len(all_dataflows)} total"
        
        # Add navigation hints
        if has_more:
            result["next_step"] = (
                "To see more dataflows, call discover_dataflows_overview() with " +
                "offset=" + str(next_offset) + " to get the next page, or " +
                "use get_dataflow_structure() to explore a specific dataflow's dimensions"
            )
        else:
            result["next_step"] = "Use get_dataflow_structure() to explore a specific dataflow's dimensions"
        
        return result
        
    except Exception as e:
        logger.exception("Failed to discover dataflows")
        return {
            "error": str(e),
            "discovery_level": "overview",
            "dataflows": []
        }


async def get_dataflow_structure(
    dataflow_id: str,
    agency_id: str = "SPC",
    version: str = "latest",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 2: Get the structure of a specific dataflow.
    
    Returns dimension order and codelist references without actual codes.
    This helps understand how to construct queries.
    """
    try:
        if ctx:
            ctx.info(f"Getting structure for dataflow {dataflow_id}...")
        
        # Resolve version first and cache it
        resolved_version = await sdmx_client.resolve_version(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Get overview first
        overview = await sdmx_client.get_dataflow_overview(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=resolved_version,
            ctx=ctx
        )
        
        # Get structure summary
        summary = await sdmx_client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=resolved_version,
            ctx=ctx
        )
        
        # Build response
        return {
            "discovery_level": "structure",
            "dataflow": {
                "id": overview.id,
                "name": overview.name,
                "description": overview.description,
                "version": resolved_version  # Include resolved version
            },
            "structure": {
                "id": summary.id,
                "key_template": summary.to_dict()["key_template"],
                "key_example": summary.to_dict()["example_key"],
                "dimensions": [
                    {
                        "id": dim.id,
                        "position": dim.position,
                        "type": dim.type,
                        "codelist": dim.codelist_ref["id"] if dim.codelist_ref else None
                    }
                    for dim in summary.dimensions
                ],
                "attributes": summary.attributes,
                "measure": summary.primary_measure
            },
            "next_steps": [
                "Use get_dimension_codes() to see available codes for a specific dimension",
                "Use get_data_availability() to see what data actually exists",
                "Use validate_query() to check your query parameters",
                "Use build_data_url() to construct a data retrieval URL"
            ]
        }
        
    except Exception as e:
        logger.exception(f"Failed to get structure for {dataflow_id}")
        return {
            "error": str(e),
            "discovery_level": "structure",
            "dataflow_id": dataflow_id
        }


async def get_dimension_codes(
    dataflow_id: str,
    dimension_id: str,
    search_term: Optional[str] = None,
    limit: int = 20,
    agency_id: str = "SPC",
    version: str = "latest",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 3: Explore codes for a specific dimension.
    
    This allows drilling down into specific dimensions without
    loading all codelists at once.
    """
    try:
        if ctx:
            ctx.info(f"Exploring codes for dimension {dimension_id}...")
        
        result = await sdmx_client.get_dimension_codes(
            dataflow_id=dataflow_id,
            dimension_id=dimension_id,
            agency_id=agency_id,
            version=version,
            search_term=search_term,
            limit=limit,
            ctx=ctx
        )
        
        # Add usage guidance
        if "error" not in result:
            result["discovery_level"] = "dimension_codes"
            result["usage"] = f"Use these codes in position {result.get('position', '?')} of the data key"
            result["example_keys"] = []
            
            # Generate example keys if we have codes
            if result.get("codes"):
                first_code = result["codes"][0]["id"]
                result["example_keys"] = [
                    f"Using '{first_code}': Place in position {result.get('position', '?')} of the key",
                    f"For all values: Leave position {result.get('position', '?')} empty (just dots)"
                ]
        
        return result
        
    except Exception as e:
        logger.exception(f"Failed to explore dimension {dimension_id}")
        return {
            "error": str(e),
            "discovery_level": "dimension_codes",
            "dataflow_id": dataflow_id,
            "dimension_id": dimension_id
        }


async def get_data_availability(
    dataflow_id: str,
    dimension_values: Optional[Dict[str, str]] = None,
    agency_id: str = "SPC",
    version: str = "latest",
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    progressive_check: Optional[List[Dict[str, str]]] = None,
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 4: Check actual data availability for the entire dataflow or specific dimension combinations.
    
    This tool is critical for avoiding empty query results. It can:
    1. Check overall dataflow availability (when dimension_values is None)
    2. Check specific dimension combinations (when dimension_values is provided)
    3. Perform progressive checking (when progressive_check is provided)
    
    Args:
        dataflow_id: The dataflow to check
        dimension_values: Optional dict of dimension=value pairs to check
                         e.g., {"GEO": "VU", "INDICATOR": "GDP"}
                         If None, checks overall dataflow availability
        agency_id: The agency ID
        version: Dataflow version
        start_period: Optional start period filter
        end_period: Optional end period filter
        progressive_check: List of dimension combinations to check progressively
                          e.g., [{"GEO": "VU"}, {"GEO": "VU", "INDICATOR": "GDP"}]
        ctx: MCP context
        
    Returns:
        Information about what data exists, including time ranges and suggestions
        
    Examples:
        1. Check overall: get_data_availability("DF_GDP")
        2. Check combination: get_data_availability("DF_GDP", {"GEO": "VU", "YEAR": "2024"})
        3. Progressive: get_data_availability("DF_GDP", progressive_check=[...])
    """
    try:
        # If progressive_check is provided, do progressive checking
        if progressive_check:
            return await _progressive_availability_check(
                dataflow_id=dataflow_id,
                dimensions_to_check=progressive_check,
                agency_id=agency_id,
                version=version,
                ctx=ctx
            )
        
        # If dimension_values is provided, check specific combination
        if dimension_values:
            return await _check_dimension_combination(
                dataflow_id=dataflow_id,
                dimension_values=dimension_values,
                agency_id=agency_id,
                version=version,
                start_period=start_period,
                end_period=end_period,
                ctx=ctx
            )
        
        # Otherwise, check overall dataflow availability
        if ctx:
            ctx.info(f"Checking overall data availability for {dataflow_id}...")
        
        availability = await sdmx_client.get_actual_availability(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Enhance response with interpretation
        if availability.get("has_constraint"):
            availability["discovery_level"] = "availability"
            availability["interpretation"] = []
            
            if availability.get("time_range"):
                availability["interpretation"].append(
                    f"Data available from {availability['time_range']['start']} to {availability['time_range']['end']}"
                )
            
            if availability.get("cube_regions"):
                availability["interpretation"].append(
                    f"Data exists for {len(availability['cube_regions'])} specific dimension combinations"
                )
                
                # Show first region as example
                if availability["cube_regions"]:
                    region = availability["cube_regions"][0]
                    if region.get("keys"):
                        example_dims = list(region["keys"].keys())[:3]
                        availability["interpretation"].append(
                            f"Example dimensions with data: {', '.join(example_dims)}"
                        )
        
        return availability
        
    except Exception as e:
        logger.exception(f"Failed to check availability for {dataflow_id}")
        return {
            "error": str(e),
            "discovery_level": "availability",
            "dataflow_id": dataflow_id
        }


async def validate_query(dataflow_id: str,
                         key: str = "all",
                         provider: str = "all", 
                         start_period: str = None,
                         end_period: str = None,
                         validate_codes: bool = False,
                         agency_id: str = "SPC",
                         version: str = "latest",
                         ctx: Context = None) -> Dict[str, Any]:
    """
    Validate SDMX query parameters before building the final URL.
    
    Checks syntax according to SDMX 2.1 REST API specification.
    Optionally validates that dimension codes actually exist.
    
    Args:
        dataflow_id: The dataflow ID
        key: The data key (dimensions separated by dots)
        provider: Provider specification
        start_period: Start of time range
        end_period: End of time range
        validate_codes: If True, check that dimension codes actually exist (slower)
        agency_id: The agency
        version: Version
        ctx: MCP context
    """
    try:
        validation_results = {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "version": version,
            "parameters": {
                "key": key,
                "provider": provider,
                "start_period": start_period,
                "end_period": end_period
            },
            "validation": {
                "is_valid": True,
                "errors": [],
                "warnings": []
            }
        }
        
        # Validate dataflow ID
        if not validate_dataflow_id(dataflow_id):
            validation_results["validation"]["errors"].append(
                "Dataflow ID must start with letter and contain only letters, digits, underscores, hyphens"
            )
            validation_results["validation"]["is_valid"] = False
        
        # Validate key syntax
        if not validate_sdmx_key(key):
            validation_results["validation"]["errors"].append(
                "Key syntax invalid. Use format like 'M.DE.000000.ANR' or 'A+M..000000.ANR' or 'all'"
            )
            validation_results["validation"]["is_valid"] = False
        
        # Validate provider syntax
        if not validate_provider(provider):
            validation_results["validation"]["errors"].append(
                "Provider syntax invalid. Use format like 'ECB' or 'CH2+NO2' or 'all'"
            )
            validation_results["validation"]["is_valid"] = False
        
        # Validate period patterns if provided
        if start_period and not validate_period(start_period):
            validation_results["validation"]["errors"].append(
                "Start period format invalid. Use ISO 8601 (2000, 2000-01, 2000-01-01) or SDMX (2000-Q1, 2000-M01)"
            )
            validation_results["validation"]["is_valid"] = False
        
        if end_period and not validate_period(end_period):
            validation_results["validation"]["errors"].append(
                "End period format invalid. Use ISO 8601 (2000, 2000-01, 2000-01-01) or SDMX (2000-Q1, 2000-M01)"
            )
            validation_results["validation"]["is_valid"] = False
        
        # Add helpful warnings
        if key == "all" and not start_period:
            validation_results["validation"]["warnings"].append(
                "Requesting all data without time constraints may return very large datasets"
            )
        
        # Validate dimension codes if requested
        if validate_codes and key != "all" and validation_results["validation"]["is_valid"]:
            if ctx:
                ctx.info("Validating dimension codes against dataflow structure...")
            
            # Get the dataflow structure
            try:
                summary = await sdmx_client.get_structure_summary(
                    dataflow_id=dataflow_id,
                    agency_id=agency_id,
                    version=version,
                    ctx=ctx
                )
                
                # Parse the key
                key_parts = key.split(".")
                
                # Check each dimension
                invalid_codes = []
                for i, (dim, key_part) in enumerate(zip(summary.dimensions, key_parts)):
                    if dim.id == "TIME_PERIOD":
                        continue
                    
                    if key_part and key_part != "":  # Not empty dimension
                        # Split multiple values (A+M)
                        values = key_part.split("+")
                        
                        # Get valid codes for this dimension
                        if dim.codelist_ref:
                            result = await sdmx_client.get_dimension_codes(
                                dataflow_id=dataflow_id,
                                dimension_id=dim.id,
                                agency_id=agency_id,
                                version=version,
                                limit=1000,  # Get more codes for validation
                                ctx=ctx
                            )
                            
                            if "codes" in result:
                                valid_codes = {code["id"] for code in result["codes"]}
                                
                                for value in values:
                                    if value not in valid_codes:
                                        invalid_codes.append({
                                            "dimension": dim.id,
                                            "position": i + 1,
                                            "invalid_code": value,
                                            "suggestion": f"Use get_dimension_codes('{dataflow_id}', '{dim.id}') to see valid codes"
                                        })
                
                if invalid_codes:
                    validation_results["validation"]["errors"].append("Some dimension codes do not exist")
                    validation_results["validation"]["invalid_codes"] = invalid_codes
                    validation_results["validation"]["is_valid"] = False
                else:
                    validation_results["validation"]["code_validation"] = "All dimension codes are valid"
                    
            except Exception as e:
                validation_results["validation"]["warnings"].append(
                    f"Could not validate codes: {str(e)}"
                )
        
        if validation_results["validation"]["is_valid"]:
            validation_results["next_steps"] = [
                "Parameters are valid - use build_data_url() to generate URLs",
                "Consider adding time constraints to limit data size",
                "Use different dimensionAtObservation values for different data views"
            ]
        
        return validation_results
        
    except Exception as e:
        logger.exception("Failed to validate query syntax")
        return {
            "error": str(e),
            "dataflow_id": dataflow_id,
            "validation": {"is_valid": False, "errors": [str(e)]}
        }


async def build_data_url(
    dataflow_id: str,
    key: str = None,
    dimensions: Dict[str, str] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    format_type: str = "csv",
    agency_id: str = "SPC",
    version: str = "latest",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 5: Build a data query URL with correct dimension handling.
    
    Can accept either a pre-formed key or a dictionary of dimensions.
    Properly handles:
    - Dimension ordering from DSD
    - Time dimensions via startPeriod/endPeriod
    - Empty strings for non-filtered dimensions
    """
    try:
        if ctx:
            ctx.info("Building data query URL with correct dimension handling...")
        
        # Import the query builder
        from sdmx_query_builder import SDMXQueryBuilder, SDMXQuerySpec
        builder = SDMXQueryBuilder(base_url=sdmx_client.base_url)
        
        # If dimensions provided instead of key, build the key properly
        if dimensions and not key:
            # Get structure to know dimension order
            summary = await sdmx_client.get_structure_summary(
                dataflow_id=dataflow_id,
                agency_id=agency_id,
                version=version,
                ctx=ctx
            )
            
            # Build query spec
            spec = SDMXQuerySpec(
                dataflow_id=dataflow_id,
                agency_id=agency_id,
                version=version if version != "latest" else "1.0",  # Use actual version
                dimension_values=dimensions,
                start_period=start_period,
                end_period=end_period,
                format_type=format_type
            )
            
            # Build the query URL
            result = builder.build_query_url(spec, summary.key_family)
            
            if "error" in result:
                return result
            
            # Add explanation of key structure
            key_explanation = builder.explain_key_structure(
                summary.key_family,
                result["key"]
            )
            
            return {
                "discovery_level": "query",
                "dataflow_id": dataflow_id,
                "key": result["key"],
                "format": format_type,
                "url": result["url"],
                "dimension_order": summary.key_family,
                "dimension_values": dimensions,
                "key_breakdown": key_explanation.get("breakdown", []),
                "time_range": result.get("time_range"),
                "usage": "Use this URL to retrieve the actual statistical data",
                "formats_available": ["csv", "json", "xml"],
                "note": "Key maintains proper dimension order with empty strings for unfiltered dimensions"
            }
        
        # If key provided directly, validate and use it
        elif key:
            # Validate the key format
            if key != "all" and not validate_sdmx_key(key):
                return {
                    "error": "Invalid SDMX key format",
                    "key": key,
                    "valid_format": "Use periods to separate dimensions, maintain all positions",
                    "special_values": "Use empty string for all values, '+' for multiple values"
                }
            
            # Build the URL with provided key
            flow = f"{agency_id},{dataflow_id},{version}"
            url = f"{sdmx_client.base_url}/data/{flow}/{key}/all"
            
            # Add query parameters
            params = []
            if start_period:
                params.append(f"startPeriod={start_period}")
            if end_period:
                params.append(f"endPeriod={end_period}")
            
            # Add format parameter
            if format_type.lower() == "csv":
                params.append("format=csv")
            elif format_type.lower() == "json":
                params.append("format=jsondata")
            
            if params:
                url += "?" + "&".join(params)
            
            return {
                "discovery_level": "query",
                "dataflow_id": dataflow_id,
                "key": key,
                "format": format_type,
                "url": url,
                "time_range": {
                    "start": start_period,
                    "end": end_period
                } if start_period or end_period else None,
                "usage": "Use this URL to retrieve the actual statistical data",
                "formats_available": ["csv", "json", "xml"]
            }
        
        # No key or dimensions provided - return all data
        else:
            flow = f"{agency_id},{dataflow_id},{version}"
            url = f"{sdmx_client.base_url}/data/{flow}/all/all"
            
            # Add query parameters
            params = []
            if start_period:
                params.append(f"startPeriod={start_period}")
            if end_period:
                params.append(f"endPeriod={end_period}")
            
            if format_type.lower() == "csv":
                params.append("format=csv")
            elif format_type.lower() == "json":
                params.append("format=jsondata")
            
            if params:
                url += "?" + "&".join(params)
            
            return {
                "discovery_level": "query",
                "dataflow_id": dataflow_id,
                "key": "all",
                "format": format_type,
                "url": url,
                "time_range": {
                    "start": start_period,
                    "end": end_period
                } if start_period or end_period else None,
                "usage": "Use this URL to retrieve ALL statistical data (no filters)",
                "formats_available": ["csv", "json", "xml"]
            }
        
    except Exception as e:
        logger.exception("Failed to build data query")
        return {
            "error": str(e),
            "discovery_level": "query",
            "dataflow_id": dataflow_id
        }


async def get_discovery_guide(ctx: Context = None) -> Dict[str, Any]:
    """
    Get a guide for using the progressive discovery tools.
    
    This helps users understand the discovery workflow.
    """
    return {
        "title": "SDMX Progressive Discovery Guide",
        "description": "Follow these steps to efficiently explore SDMX data without overwhelming the context",
        "workflow": [
            {
                "step": 1,
                "tool": "list_dataflows",
                "purpose": "Find relevant dataflows by keyword",
                "output": "List of dataflow IDs and names",
                "data_size": "~300 bytes per dataflow"
            },
            {
                "step": 2,
                "tool": "get_dataflow_structure",
                "purpose": "Understand dimensions and their order",
                "output": "Dimension list with positions and codelist references",
                "data_size": "~1-2 KB"
            },
            {
                "step": 3,
                "tool": "get_dimension_codes",
                "purpose": "Get specific codes for a dimension",
                "output": "List of valid codes with descriptions",
                "data_size": "~200-500 bytes for limited results"
            },
            {
                "step": 4,
                "tool": "get_data_availability",
                "purpose": "See what data actually exists",
                "output": "Time ranges and dimension combinations with data",
                "data_size": "~500-1000 bytes"
            },
            {
                "step": 5,
                "tool": "build_data_url",
                "purpose": "Construct the final data URL",
                "output": "Ready-to-use URL for data retrieval",
                "data_size": "~200 bytes"
            }
        ],
        "benefits": [
            "Total data: ~2-3 KB vs 100+ KB for full metadata",
            "Focused exploration: Only load what you need",
            "Better for LLMs: Fits within context windows",
            "Faster responses: Less data to parse"
        ],
        "example_workflow": {
            "goal": "Get Tonga's digital development indicators for 2020",
            "steps": [
                "1. list_dataflows(['digital', 'development'])",
                "2. get_dataflow_structure('DF_DIGITAL_DEVELOPMENT')",
                "3. get_dimension_codes('DF_DIGITAL_DEVELOPMENT', 'GEO_PICT', 'tonga')",
                "4. get_dimension_codes('DF_DIGITAL_DEVELOPMENT', 'INDICATOR')",
                "5. build_data_url('DF_DIGITAL_DEVELOPMENT', dimensions={'FREQ': 'A', 'GEO_PICT': 'TO', 'TIME_PERIOD': '2020'})"
            ]
        }
    }


async def build_sdmx_key(
    dataflow_id: str,
    dimensions: Dict[str, str] = None,
    agency_id: str = "SPC",
    version: str = "latest",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Helper tool to build a properly formatted SDMX key from dimension values.
    
    This tool helps construct the key string with dimensions in the correct order
    according to the dataflow structure. Unspecified dimensions are left empty
    (meaning "all values").
    
    Args:
        dataflow_id: The dataflow to build a key for
        dimensions: Dictionary of dimension IDs and their values. 
                   Values can be strings or lists of strings for multiple selections.
        agency_id: The agency (default: "SPC")
        version: Version (default: "latest")
        ctx: MCP context
    
    Returns:
        Dictionary with the constructed key and examples of how to use it
    """
    try:
        if ctx:
            ctx.info(f"Building SDMX key for dataflow {dataflow_id}...")
        
        # Get the structure to know dimension order
        summary = await sdmx_client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Build the key with proper dimension ordering
        key_parts = []
        dimension_breakdown = []
        
        for dim in summary.dimensions:
            if dim.id == "TIME_PERIOD":
                # TIME_PERIOD is handled via startPeriod/endPeriod parameters
                continue
                
            value = ""
            if dimensions and dim.id in dimensions:
                dim_value = dimensions[dim.id]
                # Handle lists of values - join with +
                if isinstance(dim_value, list):
                    value = "+".join(dim_value)
                else:
                    value = dim_value
            
            key_parts.append(value)
            dimension_breakdown.append({
                "position": dim.position,
                "dimension_id": dim.id,
                "value": value if value else "(all values)",
                "meaning": f"Position {dim.position}: {dim.id} = {value if value else 'all values'}"
            })
        
        # Join with dots
        key = ".".join(key_parts)
        
        # Handle special cases
        if not key or all(part == "" for part in key_parts):
            key = "all"
            key_explanation = "Using 'all' - retrieves all data without dimension filters"
        else:
            key_explanation = "Key uses dots to separate dimensions, empty positions mean 'all values'"
        
        return {
            "dataflow_id": dataflow_id,
            "dimensions_provided": dimensions or {},
            "key": key,
            "key_explanation": key_explanation,
            "dimension_breakdown": dimension_breakdown,
            "usage_examples": [
                f"build_data_url(dataflow_id='{dataflow_id}', key='{key}')",
                f"build_data_url(dataflow_id='{dataflow_id}', key='{key}', start_period='2020', end_period='2023')"
            ],
            "notes": [
                "Empty positions (consecutive dots) mean 'all values' for that dimension",
                "Pass lists for multiple values: {'GEO_PICT': ['TO', 'FJ']} becomes 'TO+FJ'",
                "Or use '+' directly in strings: {'FREQ': 'A+M'} for Annual and Monthly",
                "TIME_PERIOD is typically specified via start_period/end_period parameters"
            ]
        }
        
    except Exception as e:
        logger.exception(f"Failed to build SDMX key for {dataflow_id}")
        return {
            "error": str(e),
            "dataflow_id": dataflow_id,
            "dimensions_provided": dimensions or {}
        }


async def _check_dimension_combination(
    dataflow_id: str,
    dimension_values: Dict[str, str],
    agency_id: str,
    version: str,
    start_period: Optional[str],
    end_period: Optional[str],
    ctx: Context
) -> Dict[str, Any]:
    """Check availability for a specific dimension combination."""
    from urllib.parse import quote
    import xml.etree.ElementTree as ET
    
    if ctx:
        ctx.info(f"Checking availability for dimensions: {dimension_values}")
    
    try:
        # Get structure to know dimension order
        structure = await sdmx_client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Check if structure is a DataStructureSummary object or dict
        if hasattr(structure, 'dimensions'):
            dimensions_list = structure.dimensions
        elif isinstance(structure, dict) and "dimensions" in structure:
            dimensions_list = structure["dimensions"]
        else:
            return {
                "error": "Could not retrieve dataflow structure",
                "has_data": False
            }
        
        # Build SDMX key from dimension values
        # Note: TIME_PERIOD is handled separately via query params, not in the key
        key_parts = []
        
        for dim in dimensions_list:
            # Handle both dict and object access
            if isinstance(dim, dict):
                dim_id = dim["id"]
                dim_type = dim.get("type", "Dimension")
            else:
                dim_id = dim.id
                dim_type = getattr(dim, "type", "Dimension")
            
            # Skip TIME_PERIOD as it's a TimeDimension handled via query params
            if dim_id == "TIME_PERIOD" or dim_type == "TimeDimension":
                continue
                
            if dim_id in dimension_values:
                key_parts.append(dimension_values[dim_id])
            else:
                key_parts.append("")  # Empty means all values
        
        sdmx_key = ".".join(key_parts) if key_parts else "all"
        
        # Use availableconstraint endpoint per SDMX 2.1 REST spec
        # Format: /availableconstraint/{flow}/{key}/{provider}/{componentID}
        from config import SDMX_BASE_URL
        
        # Resolve version using the client's cached method
        try:
            actual_version = await sdmx_client.resolve_version(
                dataflow_id=dataflow_id,
                agency_id=agency_id,
                version=version,
                ctx=ctx
            )
        except ValueError as e:
            return {
                "error": f"Version resolution failed: {e}",
                "has_data": False
            }
        
        flow = f"{agency_id},{dataflow_id},{actual_version}"
        
        # Build URL: {flow}/{key}/{provider}/{componentID}
        url = f"{SDMX_BASE_URL}/availableconstraint/{flow}/{sdmx_key}/all/all"
        
        params = []
        
        # Handle TIME_PERIOD from dimension_values if present
        if "TIME_PERIOD" in dimension_values:
            time_value = dimension_values["TIME_PERIOD"]
            params.append(f"startPeriod={quote(time_value)}")
            params.append(f"endPeriod={quote(time_value)}")
        elif start_period or end_period:
            if start_period:
                params.append(f"startPeriod={quote(start_period)}")
            if end_period:
                params.append(f"endPeriod={quote(end_period)}")
        
        # Use mode=exact (default) to check what data actually exists for this key
        # params.append("mode=exact")  # Not needed as exact is the default
        
        if params:
            url += "?" + "&".join(params)
        
        # Make request
        session = await sdmx_client._get_session()
        response = await session.get(url)
        
        if response.status_code == 404:
            return {
                "has_data": False,
                "dimension_combination": dimension_values,
                "message": "No data exists for this dimension combination",
                "suggestions": [
                    "Try removing the most specific dimension",
                    "Check each dimension individually first"
                ]
            }
        elif response.status_code != 200:
            return {
                "error": f"Availability check failed: {response.status_code}",
                "has_data": False
            }
        
        # Parse response to check if data exists
        root = ET.fromstring(response.content)
        
        # Look for cube regions (in structure namespace, not common)
        cube_regions = root.findall('.//str:CubeRegion', SDMX_NAMESPACES)
        time_ranges = []
        available_dims = {}
        
        # Validate that the response actually confirms our requested dimensions
        has_valid_data = False
        
        for region in cube_regions:
            # Check if this region actually validates our requested dimensions
            region_matches = True
            for req_dim, req_value in dimension_values.items():
                # Find the KeyValue for this dimension
                dim_found = False
                for key_value in region.findall(f'.//com:KeyValue[@id="{req_dim}"]', SDMX_NAMESPACES):
                    # Check if it's a time dimension with TimeRange
                    time_range = key_value.find('.//com:TimeRange', SDMX_NAMESPACES)
                    if time_range is not None:
                        # For time dimensions, check if the requested value falls within the range
                        # For now, consider it valid if a TimeRange exists
                        # TODO: Implement proper date range checking
                        dim_found = True
                        break
                    
                    # Regular dimension with Value elements
                    values = key_value.findall('.//com:Value', SDMX_NAMESPACES)
                    if values:
                        # Check if any value matches or is empty (wildcard)
                        for v in values:
                            if v.text == req_value or not v.text:  # Match or wildcard
                                dim_found = True
                                break
                    else:
                        # No values means wildcard (all values)
                        dim_found = True
                    break
                    
                if not dim_found:
                    region_matches = False
                    break
            
            if region_matches:
                has_valid_data = True
            # Check for time periods in any KeyValue that has a TimeRange
            # (time dimension can have different names, not always TIME_PERIOD)
            for key_value in region.findall('.//com:KeyValue', SDMX_NAMESPACES):
                # Check if this KeyValue contains a TimeRange (indicates it's a time dimension)
                for time_range in key_value.findall('.//com:TimeRange', SDMX_NAMESPACES):
                    start = time_range.find('.//com:StartPeriod', SDMX_NAMESPACES)
                    end = time_range.find('.//com:EndPeriod', SDMX_NAMESPACES)
                    if start is not None and start.text:
                        # Extract just the date part (YYYY-MM-DD) from datetime
                        start_date = start.text.split('T')[0]
                        time_ranges.append(start_date)
                    if end is not None and end.text:
                        # Extract just the date part (YYYY-MM-DD) from datetime
                        end_date = end.text.split('T')[0]
                        if end_date not in time_ranges:
                            time_ranges.append(end_date)
            
            # Collect available dimension values
            for key_value in region.findall('.//com:KeyValue', SDMX_NAMESPACES):
                dim_id = key_value.get('id')
                if dim_id and dim_id not in dimension_values:
                    # Skip if this KeyValue has a TimeRange (it's a time dimension)
                    if key_value.find('.//com:TimeRange', SDMX_NAMESPACES) is not None:
                        continue
                    if dim_id not in available_dims:
                        available_dims[dim_id] = set()
                    for value in key_value.findall('.//com:Value', SDMX_NAMESPACES):
                        if value.text:
                            available_dims[dim_id].add(value.text)
            
        # Convert sets to lists
        for dim_id in available_dims:
            available_dims[dim_id] = sorted(list(available_dims[dim_id]))[:10]  # Limit to 10 examples
        
        # Use our validation result instead of just checking if regions exist
        has_data = has_valid_data
        
        result = {
            "has_data": has_data,
            "dimension_combination": dimension_values,
            "dataflow_id": dataflow_id
        }
        
        if has_data:
            if time_ranges:
                result["time_range"] = {
                    "earliest": min(time_ranges),
                    "latest": max(time_ranges),
                    "periods_available": len(set(time_ranges))
                }
            
            if available_dims:
                result["other_available_dimensions"] = available_dims
                
            result["suggestions"] = [
                "Data exists for this combination",
                f"You can query this data using the key: {sdmx_key}"
            ]
        else:
            result["suggestions"] = [
                "No data found for this exact combination",
                "Try broader criteria by removing specific dimensions"
            ]
        
        return result
            
    except Exception as e:
        logger.exception(f"Failed to check dimension combination")
        return {
            "error": str(e),
            "has_data": False,
            "dimension_combination": dimension_values
        }


async def _progressive_availability_check(
    dataflow_id: str,
    dimensions_to_check: List[Dict[str, str]],
    agency_id: str,
    version: str,
    ctx: Context
) -> Dict[str, Any]:
    """Perform progressive availability checking."""
    results = []
    
    for i, dimension_combo in enumerate(dimensions_to_check, 1):
        if ctx:
            ctx.info(f"Progressive check {i}/{len(dimensions_to_check)}: {dimension_combo}")
        
        availability = await _check_dimension_combination(
            dataflow_id=dataflow_id,
            dimension_values=dimension_combo,
            agency_id=agency_id,
            version=version,
            start_period=None,
            end_period=None,
            ctx=ctx
        )
        
        results.append({
            "step": i,
            "dimensions": dimension_combo,
            "has_data": availability.get("has_data", False),
            "details": availability
        })
        
        # Stop if no data found
        if not availability.get("has_data", False):
            break
    
    # Find last valid combination
    last_valid = None
    for r in results:
        if r["has_data"]:
            last_valid = r
    
    return {
        "dataflow_id": dataflow_id,
        "progressive_results": results,
        "last_valid_combination": last_valid["dimensions"] if last_valid else None,
        "recommendation": _generate_recommendation(results)
    }


def _generate_recommendation(results: List[Dict]) -> str:
    """Generate recommendation based on progressive results."""
    if not results:
        return "No checks performed"
    
    last_valid_idx = -1
    for i, r in enumerate(results):
        if r["has_data"]:
            last_valid_idx = i
    
    if last_valid_idx == -1:
        return "No data available even at the broadest level. Check the dataflow ID."
    
    if last_valid_idx == len(results) - 1:
        return "All dimension combinations have data. Query is valid."
    
    # Data stops at some point
    failed = results[last_valid_idx + 1]
    valid = results[last_valid_idx]
    
    # Find what dimension was added
    added_dims = {k: v for k, v in failed["dimensions"].items() 
                  if k not in valid["dimensions"]}
    
    if added_dims:
        dim_str = ", ".join(f"{k}={v}" for k, v in added_dims.items())
        return f"Data exists up to {valid['dimensions']} but not when adding {dim_str}. Use the last valid combination."
    
    return "Use the last valid dimension combination for your query."


async def cleanup_sdmx_client():
    """Clean up the SDMX client session."""
    await sdmx_client.close()


# No backward compatibility needed - this is a new project