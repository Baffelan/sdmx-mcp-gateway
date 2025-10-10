"""
Configuration for SDMX MCP Gateway

This module handles configuration for different SDMX endpoints.
The base URL can be set via environment variable or changed in code.
"""

import os
from typing import Dict, Any

# Current active configuration (can be changed at runtime)
_current_endpoint_key = os.getenv("SDMX_ENDPOINT", "SPC")

# Allow direct URL override via environment variable
_env_base_url = os.getenv("SDMX_BASE_URL")
_env_agency_id = os.getenv("SDMX_AGENCY_ID")

# Common SDMX endpoints (verified January 2025)
SDMX_ENDPOINTS = {
    "SPC": {
        "name": "Pacific Data Hub", 
        "base_url": "https://stats-sdmx-disseminate.pacificdata.org/rest",
        "agency_id": "SPC",
        "description": "Pacific regional statistics",
        "status": "✅ Fully working"
    },
    "ECB": {
        "name": "European Central Bank",
        "base_url": "https://data-api.ecb.europa.eu/service",
        "agency_id": "ECB",
        "description": "European financial and economic statistics",
        "status": "✅ Fully working"
    },
    "UNICEF": {
        "name": "UNICEF",
        "base_url": "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest",
        "agency_id": "UNICEF",
        "description": "Children and youth statistics",
        "status": "✅ Fully working"
    },
    "IMF": {
        "name": "International Monetary Fund",
        "base_url": "https://api.imf.org/external/sdmx/2.1",
        "agency_id": "IMF.STA",
        "description": "Global financial statistics",
        "status": "✅ Fully working"
    },
    # These endpoints have limitations:
    # "OECD": {
    #     "name": "OECD",
    #     "base_url": "https://sdmx.oecd.org/public/rest",
    #     "agency_id": "OECD",
    #     "description": "OECD countries economic and social statistics",
    #     "status": "⚠️ Only works with root /dataflow, returns 8MB of data"
    # },
    # "EUROSTAT": {
    #     "name": "Eurostat",
    #     "base_url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
    #     "agency_id": "ESTAT",
    #     "description": "European Union official statistics",
    #     "status": "❌ Timeouts - may be blocking automated requests"
    # },
}

def get_current_config() -> Dict[str, Any]:
    """
    Get current SDMX endpoint configuration.
    
    Returns:
        Dict with base_url, agency_id, name, description
    """
    global _current_endpoint_key
    
    # If environment variables are set, use custom endpoint
    if _env_base_url:
        return {
            "name": "Custom SDMX Endpoint",
            "base_url": _env_base_url,
            "agency_id": _env_agency_id or "CUSTOM",
            "description": "Custom SDMX endpoint from environment"
        }
    
    # Otherwise use configured endpoint
    if _current_endpoint_key in SDMX_ENDPOINTS:
        return SDMX_ENDPOINTS[_current_endpoint_key]
    
    # Fallback to SPC
    return SDMX_ENDPOINTS["SPC"]

def set_endpoint(endpoint_key: str) -> Dict[str, Any]:
    """
    Switch to a different SDMX endpoint.
    
    Args:
        endpoint_key: Key from SDMX_ENDPOINTS dict
        
    Returns:
        The new configuration dict
    """
    global _current_endpoint_key, SDMX_BASE_URL, SDMX_AGENCY_ID
    
    if endpoint_key not in SDMX_ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_key}. Available: {list(SDMX_ENDPOINTS.keys())}")
    
    _current_endpoint_key = endpoint_key
    config = get_current_config()
    # Update the module-level variables
    SDMX_BASE_URL = config["base_url"]
    SDMX_AGENCY_ID = config["agency_id"]
    return config

# Initialize module-level variables
SDMX_BASE_URL = get_current_config()["base_url"]
SDMX_AGENCY_ID = get_current_config()["agency_id"]