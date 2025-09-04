"""
SDMX MCP Gateway Server

A Model Context Protocol server for progressive SDMX data discovery.
Provides tools, resources, and prompts for exploring statistical data.
"""

import logging
import asyncio
from typing import Optional, List, Dict
from mcp.server.fastmcp import FastMCP

# Import our modular components
from tools.sdmx_tools import (
    list_dataflows as list_dataflows_impl,
    get_dataflow_structure as get_dataflow_structure_impl,
    get_dimension_codes as get_dimension_codes_impl,
    get_data_availability as get_data_availability_impl,
    validate_query as validate_query_impl,
    build_data_url as build_data_url_impl,
    build_sdmx_key as build_sdmx_key_impl,
    cleanup_sdmx_client
)
from tools.endpoint_tools import (
    get_current_endpoint as get_current_endpoint_impl,
    list_available_endpoints as list_available_endpoints_impl,
    switch_endpoint as switch_endpoint_impl
)
from resources.sdmx_resources import (
    list_known_agencies, get_agency_info, get_sdmx_format_guide,
    get_sdmx_query_syntax_guide
)
from prompts.sdmx_prompts import (
    sdmx_discovery_guide, sdmx_troubleshooting_guide, 
    sdmx_best_practices, sdmx_query_builder
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("SDMX Data Gateway")

# Register tools with consistent naming
@mcp.tool()
async def list_dataflows(keywords: Optional[List[str]] = None, 
                        agency_id: str = "SPC",
                        limit: int = 10,
                        offset: int = 0):
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
        Dictionary with dataflows, pagination info, and navigation hints
    """
    # Returns minimal metadata by design for efficiency
    return await list_dataflows_impl(keywords, agency_id, limit, offset)

@mcp.tool()
async def get_dataflow_structure(dataflow_id: str,
                                agency_id: str = "SPC", 
                                version: str = "latest"):
    """
    Get detailed structure information for a specific dataflow.
    
    Returns dimensions, attributes, measures, and codelist references.
    Use this after list_dataflows() to understand data organization.
    """
    # Always returns structure with codelist references
    return await get_dataflow_structure_impl(dataflow_id, agency_id, version)

@mcp.tool()
async def get_codelist(codelist_id: str,
                      agency_id: str = "SPC",
                      version: str = "latest", 
                      search_term: Optional[str] = None):
    """
    Get codes and values for a specific codelist.
    
    Codelists define the allowed values for dimensions (e.g., country codes, commodity codes).
    Use this to find the exact codes needed for your data query.
    """
    # Use the proper SDMX codelist endpoint
    from sdmx_client import SDMXClient
    client = SDMXClient()
    try:
        result = await client.browse_codelist(codelist_id, agency_id, version, search_term)
        return result
    finally:
        await client.close()

@mcp.tool()
async def get_dimension_codes(dataflow_id: str,
                             dimension_id: str,
                             search_term: Optional[str] = None,
                             limit: int = 20,
                             agency_id: str = "SPC",
                             version: str = "latest"):
    """
    Get codes for a specific dimension of a dataflow.
    
    This allows drilling down into specific dimensions without loading all codelists at once.
    Useful for finding valid values for a particular dimension in your data query.
    """
    return await get_dimension_codes_impl(
        dataflow_id, dimension_id, search_term, limit, agency_id, version
    )

@mcp.tool()
async def get_data_availability(
    dataflow_id: str,
    dimension_values: Optional[Dict[str, str]] = None,
    agency_id: str = "SPC",
    version: str = "latest",
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    progressive_check: Optional[List[Dict[str, str]]] = None
):
    """
    Get actual data availability for a dataflow or specific dimension combinations.
    
    This tool is critical for avoiding empty query results. It can:
    1. Check overall dataflow availability (when dimension_values is None)
    2. Check specific dimension combinations (when dimension_values is provided)
    3. Perform progressive checking (when progressive_check is provided)
    
    Args:
        dataflow_id: The dataflow to check
        dimension_values: Optional dict of dimension=value pairs to check
                         e.g., {"GEO": "VU", "INDICATOR": "GDP"}
        agency_id: The agency ID
        version: Dataflow version
        start_period: Optional start period filter
        end_period: Optional end period filter
        progressive_check: List of dimension combinations to check progressively
                          e.g., [{"GEO": "VU"}, {"GEO": "VU", "INDICATOR": "GDP"}]
        
    Returns:
        Information about what data exists, including time ranges and suggestions
    """
    return await get_data_availability_impl(
        dataflow_id=dataflow_id,
        dimension_values=dimension_values,
        agency_id=agency_id,
        version=version,
        start_period=start_period,
        end_period=end_period,
        progressive_check=progressive_check
    )

@mcp.tool()
async def validate_query(dataflow_id: str,
                        key: str = "all",
                        provider: str = "all", 
                        start_period: Optional[str] = None,
                        end_period: Optional[str] = None,
                        validate_codes: bool = False,
                        agency_id: str = "SPC",
                        version: str = "latest"):
    """
    Validate SDMX query parameters before building the final URL.
    
    Checks syntax according to SDMX 2.1 REST API specification.
    Optionally validates that dimension codes actually exist in the dataflow.
    
    Args:
        dataflow_id: The dataflow to validate against
        key: The data key (dimensions separated by dots)
        provider: Provider specification
        start_period: Start of time range
        end_period: End of time range
        validate_codes: If True, check dimension codes exist (slower but thorough)
        agency_id: The agency
        version: Version
    
    Returns:
        Validation results including any errors, warnings, and invalid codes
    """
    return await validate_query_impl(
        dataflow_id, key, provider, start_period, end_period,
        validate_codes, agency_id, version
    )

@mcp.tool()
async def build_key(dataflow_id: str,
                   dimensions: Optional[dict] = None,
                   agency_id: str = "SPC",
                   version: str = "latest"):
    """
    Build a properly formatted SDMX key from dimension values.
    
    This helper tool constructs the key string with dimensions in the correct order
    according to the dataflow structure. Unspecified dimensions are left empty
    (meaning "all values").
    
    Use this before build_data_url() to ensure your key has the correct format.
    """
    return await build_sdmx_key_impl(dataflow_id, dimensions, agency_id, version)

@mcp.tool()
async def build_data_url(dataflow_id: str,
                        key: str = "all",
                        start_period: Optional[str] = None,
                        end_period: Optional[str] = None,
                        dimension_at_observation: str = "AllDimensions",
                        format_type: str = "csv",
                        agency_id: str = "SPC",
                        version: str = "latest"):
    """
    Generate final SDMX REST API URLs for data retrieval.
    
    Creates URLs that can be used directly to download data in various formats.
    This is the final step in the SDMX query construction process.
    
    Args:
        dataflow_id: The dataflow to query
        key: The data key (use build_key() to construct)
        start_period: Start of time range (optional)
        end_period: End of time range (optional)
        dimension_at_observation: Data structure (default: "AllDimensions" for flat structure)
        format_type: Output format (csv, json, xml)
        agency_id: The agency (default: "SPC")
        version: Version (default: "latest")
    """
    from urllib.parse import quote
    from config import SDMX_BASE_URL
    
    # Resolve version using the cached method
    from sdmx_client import SDMXClient
    client = SDMXClient()
    try:
        actual_version = await client.resolve_version(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version
        )
    except ValueError as e:
        # Return error if version cannot be resolved
        return {
            "error": f"Failed to resolve version: {e}",
            "dataflow_id": dataflow_id,
            "version": version
        }
    finally:
        await client.close()
    
    # Build base URL with resolved version
    flow_spec = f"{agency_id},{dataflow_id},{actual_version}"
    data_url = f"{SDMX_BASE_URL}/data/{flow_spec}/{quote(key)}/all"
    
    # Build query parameters
    params = []
    if start_period:
        params.append(f"startPeriod={quote(start_period)}")
    if end_period:
        params.append(f"endPeriod={quote(end_period)}")
    if dimension_at_observation != "TIME_PERIOD":  # Only add if not default SDMX value
        params.append(f"dimensionAtObservation={quote(dimension_at_observation)}")
    
    # Add format parameter
    if format_type.lower() == "csv":
        params.append("format=csv")
    elif format_type.lower() == "json":
        params.append("format=jsondata")
    
    if params:
        data_url += "?" + "&".join(params)
    
    return {
        "dataflow_id": dataflow_id,
        "version": actual_version,  # Show the resolved version
        "key": key,
        "format": format_type,
        "url": data_url,
        "dimension_at_observation": dimension_at_observation,
        "time_range": {
            "start": start_period,
            "end": end_period
        } if start_period or end_period else None,
        "usage": "Use this URL to retrieve the actual statistical data",
        "formats_available": ["csv", "json", "xml"],
        "note": f"Version 'latest' resolved to '{actual_version}'" if version == "latest" else None
    }

# Register endpoint management tools
@mcp.tool()
async def get_current_endpoint():
    """
    Get information about the currently active SDMX data source.
    
    Shows which statistical organization's API is being used (e.g., Pacific Data, 
    European Central Bank, UNICEF).
    
    Returns:
        Current endpoint name, URL, agency ID, and description
    """
    return await get_current_endpoint_impl()

@mcp.tool()
async def list_available_endpoints():
    """
    List all available SDMX data sources that can be switched to.
    
    Shows all configured statistical data providers (e.g., SPC, ECB, UNICEF)
    and indicates which one is currently active.
    
    Returns:
        List of available endpoints with their descriptions and status
    """
    return await list_available_endpoints_impl()

@mcp.tool()
async def switch_endpoint(endpoint_key: str):
    """
    Switch to a different SDMX data source.
    
    Changes the active statistical data provider. All subsequent queries will
    use the new endpoint until switched again.
    
    Args:
        endpoint_key: The endpoint to switch to (e.g., "SPC", "ECB", "UNICEF")
        
    Returns:
        Confirmation of the switch with new endpoint details
        
    Examples:
        - switch_endpoint("ECB") - Switch to European Central Bank
        - switch_endpoint("UNICEF") - Switch to UNICEF data
        - switch_endpoint("SPC") - Switch to Pacific Data Hub
    """
    return await switch_endpoint_impl(endpoint_key)

# Register resources
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

# Register prompts
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
def query_builder(dataflow_info: dict, user_requirements: str):
    """
    Interactive query builder prompt based on dataflow structure.
    """
    return sdmx_query_builder(dataflow_info, user_requirements)

# Note: Cleanup will be handled automatically by the SDMX client's context manager
# If FastMCP adds cleanup support in the future, we can add:
# @mcp.cleanup()
# async def cleanup():
#     """Cleanup resources on shutdown."""
#     await cleanup_sdmx_client()

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()