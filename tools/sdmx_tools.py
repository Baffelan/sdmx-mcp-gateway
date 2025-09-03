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
    filter_dataflows_by_keywords, SDMX_FORMATS
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
        from sdmx_client import SDMXClient
        basic_client = SDMXClient()
        
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
        
        # Get overview first
        overview = await sdmx_client.get_dataflow_overview(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Get structure summary
        summary = await sdmx_client.get_structure_summary(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        # Build response
        return {
            "discovery_level": "structure",
            "dataflow": {
                "id": overview.id,
                "name": overview.name,
                "description": overview.description
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
    agency_id: str = "SPC",
    version: str = "latest",
    ctx: Context = None
) -> Dict[str, Any]:
    """
    Step 4: Check actual data availability.
    
    Returns information about what data actually exists,
    including time ranges and dimension combinations.
    """
    try:
        if ctx:
            ctx.info(f"Checking data availability for {dataflow_id}...")
        
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


async def cleanup_sdmx_client():
    """Clean up the SDMX client session."""
    await sdmx_client.close()


# No backward compatibility needed - this is a new project