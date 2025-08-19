"""
SDMX MCP Gateway Server

A Model Context Protocol server for progressive SDMX data discovery.
Provides tools, resources, and prompts for exploring statistical data.
"""

import logging
import asyncio
from mcp.server.fastmcp import FastMCP

# Import our modular components
from tools.sdmx_tools import (
    list_dataflows, get_dataflow_structure, explore_codelist,
    validate_query_syntax, build_data_query, cleanup_sdmx_client
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

# Register tools
@mcp.tool()
async def discover_dataflows(keywords: list[str] = None, 
                           agency_id: str = "SPC",
                           include_references: bool = False):
    """
    Discover available SDMX dataflows, optionally filtered by keywords.
    
    This is typically the first step in SDMX data discovery. Returns a list of
    statistical domains (dataflows) available from the specified agency.
    """
    return await list_dataflows(keywords, agency_id, include_references)

@mcp.tool()
async def get_structure(dataflow_id: str,
                       agency_id: str = "SPC", 
                       version: str = "latest",
                       include_references: bool = True):
    """
    Get detailed structure information for a specific dataflow.
    
    Returns dimensions, attributes, measures, and codelist references.
    Use this after discover_dataflows() to understand data organization.
    """
    return await get_dataflow_structure(dataflow_id, agency_id, version, include_references)

@mcp.tool()
async def browse_codelist(codelist_id: str,
                         agency_id: str = "SPC",
                         version: str = "latest", 
                         search_term: str = None):
    """
    Browse codes and values for a specific codelist.
    
    Codelists define the allowed values for dimensions (e.g., country codes, commodity codes).
    Use this to find the exact codes needed for your data query.
    """
    return await explore_codelist(codelist_id, agency_id, version, search_term)

@mcp.tool()
def validate_syntax(dataflow_id: str,
                   key: str = "all",
                   provider: str = "all", 
                   start_period: str = None,
                   end_period: str = None,
                   agency_id: str = "SPC",
                   version: str = "latest"):
    """
    Validate SDMX query parameters before building the final URL.
    
    Checks syntax according to SDMX 2.1 REST API specification.
    """
    return validate_query_syntax(dataflow_id, key, provider, start_period, end_period, agency_id, version)

@mcp.tool()
def build_query(dataflow_id: str,
               key: str = "all",
               provider: str = "all",
               start_period: str = None,
               end_period: str = None,
               dimension_at_observation: str = "TIME_PERIOD",
               detail: str = "full",
               agency_id: str = "SPC",
               version: str = "latest",
               format_type: str = "csv"):
    """
    Generate final SDMX REST API URLs for data retrieval.
    
    Creates URLs that can be used directly to download data in various formats.
    This is the final step in the SDMX query construction process.
    """
    return build_data_query(
        dataflow_id, key, provider, start_period, end_period,
        dimension_at_observation, detail, agency_id, version, format_type
    )

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

# Cleanup handler
@mcp.cleanup()
async def cleanup():
    """Cleanup resources on shutdown."""
    await cleanup_sdmx_client()

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()