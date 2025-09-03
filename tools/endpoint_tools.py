"""
SDMX Endpoint Management Tools

Tools for discovering, switching, and managing SDMX data source endpoints.
"""

from typing import Dict, Any, List
import config


async def get_current_endpoint() -> Dict[str, Any]:
    """
    Get information about the currently active SDMX endpoint.
    
    Returns:
        Dictionary with current endpoint configuration including:
        - name: Human-readable name of the endpoint
        - base_url: The API base URL
        - agency_id: The default agency ID
        - description: What data this endpoint provides
    """
    current = config.get_current_config()
    return {
        "name": current["name"],
        "base_url": current["base_url"],
        "agency_id": current["agency_id"],
        "description": current["description"],
        "status": current.get("status", "Active")
    }


async def list_available_endpoints() -> Dict[str, Any]:
    """
    List all available SDMX endpoints that can be switched to.
    
    Returns:
        Dictionary with:
        - current: The currently active endpoint key
        - endpoints: List of available endpoints with their details
    """
    current_config = config.get_current_config()
    current_key = None
    
    # Find which endpoint is currently active
    for key, cfg in config.SDMX_ENDPOINTS.items():
        if cfg["base_url"] == current_config["base_url"]:
            current_key = key
            break
    
    # Build endpoint list
    endpoints = []
    for key, cfg in config.SDMX_ENDPOINTS.items():
        endpoints.append({
            "key": key,
            "name": cfg["name"],
            "agency_id": cfg["agency_id"],
            "description": cfg["description"],
            "status": cfg.get("status", "Available"),
            "is_current": key == current_key
        })
    
    return {
        "current": current_key or "custom",
        "endpoints": endpoints,
        "note": "Use switch_endpoint() to change the active endpoint"
    }


async def switch_endpoint(endpoint_key: str) -> Dict[str, Any]:
    """
    Switch to a different SDMX endpoint.
    
    Args:
        endpoint_key: The key of the endpoint to switch to (e.g., "SPC", "ECB", "UNICEF")
        
    Returns:
        Dictionary with the new endpoint configuration
        
    Raises:
        ValueError: If the endpoint_key is not recognized
    """
    try:
        new_config = config.set_endpoint(endpoint_key)
        return {
            "success": True,
            "message": f"Switched to {new_config['name']}",
            "new_endpoint": {
                "key": endpoint_key,
                "name": new_config["name"],
                "base_url": new_config["base_url"],
                "agency_id": new_config["agency_id"],
                "description": new_config["description"]
            }
        }
    except ValueError as e:
        # Get available endpoints for helpful error message
        available = list(config.SDMX_ENDPOINTS.keys())
        return {
            "success": False,
            "error": str(e),
            "available_endpoints": available,
            "hint": f"Use one of: {', '.join(available)}"
        }