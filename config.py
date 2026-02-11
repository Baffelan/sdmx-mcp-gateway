"""
Configuration for SDMX MCP Gateway

This module handles configuration for different SDMX endpoints.
The base URL can be set via environment variable or changed in code.

Constraint strategies (per endpoint):
    single_flow: How to fetch constraints for a single dataflow.
        - "availableconstraint"  /availableconstraint/{flow}/all/all/all
          Dynamic query returning Actual constraint with all dims + time range.
        - "references"           /dataflow/{agency}/{flow}/latest?references=contentconstraint
          Static constraints attached to the dataflow metadata. May return
          Actual or Allowed; may only cover a subset of dimensions.
        - None                   No single-flow constraint support.

    bulk: How to search constraints across all dataflows at once.
        - "contentconstraint"    /contentconstraint/{agency}/all/latest?detail=full
          All ContentConstraints in one call. Only SPC has Actual here; ECB has
          Allowed only.
        - "availableconstraint"  /availableconstraint/all/all/all/all
          Dynamic query for all flows. Only UNICEF supports this.
        - None                   No bulk support. Cross-dataflow search must
          iterate per-flow (slow) or is unavailable.
"""

import os
from typing import Any

# Current active configuration (can be changed at runtime)
_current_endpoint_key = os.getenv("SDMX_ENDPOINT", "SPC")

# Allow direct URL override via environment variable
_env_base_url = os.getenv("SDMX_BASE_URL")
_env_agency_id = os.getenv("SDMX_AGENCY_ID")

# SDMX endpoints with constraint strategy metadata (verified February 2026)
#
# Constraint strategies are derived from live testing documented in
# sdmx-endpoint-constraint-matrix.md at the repository root.
SDMX_ENDPOINTS: dict[str, dict[str, Any]] = {
    "SPC": {
        "name": "Pacific Data Hub",
        "base_url": "https://stats-sdmx-disseminate.pacificdata.org/rest",
        "agency_id": "SPC",
        "description": "Pacific regional statistics",
        "constraints": {
            "single_flow": "availableconstraint",
            "bulk": "contentconstraint",
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "ECB": {
        "name": "European Central Bank",
        "base_url": "https://data-api.ecb.europa.eu/service",
        "agency_id": "ECB",
        "description": "European financial and economic statistics",
        "constraints": {
            # ECB does not support /availableconstraint/ (404).
            # ?references=contentconstraint returns Allowed constraints only.
            "single_flow": "references",
            # Bulk returns Allowed constraints (no Actual).
            "bulk": "contentconstraint",
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "UNICEF": {
        "name": "UNICEF",
        "base_url": "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest",
        "agency_id": "UNICEF",
        "description": "Children and youth statistics",
        "constraints": {
            "single_flow": "availableconstraint",
            # UNICEF is the only provider where wildcard availableconstraint works.
            "bulk": "availableconstraint",
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "IMF": {
        "name": "International Monetary Fund",
        "base_url": "https://api.imf.org/external/sdmx/2.1",
        "agency_id": "IMF.STA",
        "description": "Global financial statistics",
        "constraints": {
            "single_flow": "availableconstraint",
            # No bulk endpoint. /contentconstraint returns 204 for all agencies.
            "bulk": None,
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "OECD": {
        "name": "OECD",
        "base_url": "https://sdmx.oecd.org/public/rest",
        "agency_id": "OECD",
        "description": "OECD countries economic and social statistics",
        "constraints": {
            # Note: OECD dataflow IDs use DSD@DF format (e.g. DSD_PRICES@DF_PRICES_ALL).
            "single_flow": "availableconstraint",
            "bulk": None,
        },
        # OECD publishes dataflows under ~50 sub-agencies (OECD.CTP.TPS, etc.),
        # not under bare "OECD". Use "all" to list dataflows across sub-agencies.
        "dataflow_agency": "all",
        "references_support": ["none", "children", "parents", "all"],
    },
    "ESTAT": {
        "name": "Eurostat",
        "base_url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
        "agency_id": "ESTAT",
        "description": "European Union official statistics",
        "constraints": {
            # ESTAT returns 405 for /availableconstraint/ and 400/404 for references.
            "single_flow": None,
            "bulk": None,
        },
        # ESTAT rejects ?references=all and ?references=parents (400).
        "references_support": ["none", "children", "descendants"],
    },
    "ILO": {
        "name": "International Labour Organization",
        "base_url": "https://sdmx.ilo.org/rest",
        "agency_id": "ILO",
        "description": "Labour and employment statistics",
        "constraints": {
            # /availableconstraint/ returns 500 but ?references=all includes
            # Actual constraints with full dimension coverage. Bulk detail=full
            # returns 413 (1095 constraints).
            "single_flow": "references_all",
            "bulk": None,
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "ABS": {
        "name": "Australian Bureau of Statistics",
        "base_url": "https://data.api.abs.gov.au/rest",
        "agency_id": "ABS",
        "description": "Australian official statistics",
        "constraints": {
            # /availableconstraint/ works for most dataflows (Actual).
            # Some census dataflows return 500 — handled by error recovery.
            "single_flow": "availableconstraint",
            "bulk": None,
        },
        "references_support": ["none", "children", "parents", "all"],
    },
    "BIS": {
        "name": "Bank for International Settlements",
        "base_url": "https://stats.bis.org/api/v1",
        "agency_id": "BIS",
        "description": "International financial statistics",
        "constraints": {
            # /availableconstraint/ works per-flow (Actual).
            "single_flow": "availableconstraint",
            "bulk": None,
        },
        "references_support": ["none", "children", "parents", "all"],
    },
}


def get_constraint_strategy(endpoint_key: str, kind: str = "single_flow") -> str | None:
    """
    Get the constraint-fetching strategy for an endpoint.

    Args:
        endpoint_key: Key in SDMX_ENDPOINTS (e.g. "SPC", "ECB").
        kind: "single_flow" or "bulk".

    Returns:
        Strategy string ("availableconstraint", "references", "contentconstraint")
        or None if the endpoint does not support this kind of constraint query.
    """
    ep = SDMX_ENDPOINTS.get(endpoint_key)
    if ep is None:
        return None
    constraints = ep.get("constraints")
    if constraints is None:
        return None
    return constraints.get(kind)


def get_dataflow_agency(endpoint_key: str) -> str | None:
    """
    Get the dataflow listing agency override for an endpoint.

    Some providers (e.g. OECD) publish dataflows under sub-agencies,
    requiring "all" instead of the bare agency_id for listing.

    Args:
        endpoint_key: Key in SDMX_ENDPOINTS (e.g. "OECD").

    Returns:
        Override agency string (e.g. "all") or None if no override needed.
    """
    ep = SDMX_ENDPOINTS.get(endpoint_key)
    if ep is None:
        return None
    return ep.get("dataflow_agency")


def get_best_references(endpoint_key: str | None, desired: str) -> str | None:
    """
    Return desired ?references= value if supported, or best fallback.

    Args:
        endpoint_key: Key in SDMX_ENDPOINTS, or None for unknown endpoints.
        desired: The desired references parameter (e.g. "all", "parents").

    Returns:
        The best supported references value, or None if no useful fallback exists.
    """
    if endpoint_key is None:
        return desired
    ep = SDMX_ENDPOINTS.get(endpoint_key)
    if ep is None:
        return desired
    supported = ep.get("references_support")
    if supported is None or desired in supported:
        return desired
    # "all" can fall back to "descendants" (includes children + constraints)
    if desired == "all" and "descendants" in supported:
        return "descendants"
    return None


def get_current_config() -> dict[str, Any]:
    """
    Get current SDMX endpoint configuration.

    Returns:
        Dict with base_url, agency_id, name, description, constraints
    """
    global _current_endpoint_key

    # If environment variables are set, use custom endpoint
    if _env_base_url:
        return {
            "name": "Custom SDMX Endpoint",
            "base_url": _env_base_url,
            "agency_id": _env_agency_id or "CUSTOM",
            "description": "Custom SDMX endpoint from environment",
            "constraints": {"single_flow": None, "bulk": None},
        }

    # Otherwise use configured endpoint
    if _current_endpoint_key in SDMX_ENDPOINTS:
        return SDMX_ENDPOINTS[_current_endpoint_key]

    # Fallback to SPC
    return SDMX_ENDPOINTS["SPC"]


def set_endpoint(endpoint_key: str) -> dict[str, Any]:
    """
    Switch to a different SDMX endpoint.

    Args:
        endpoint_key: Key from SDMX_ENDPOINTS dict

    Returns:
        The new configuration dict
    """
    global _current_endpoint_key, SDMX_BASE_URL, SDMX_AGENCY_ID

    if endpoint_key not in SDMX_ENDPOINTS:
        available = ", ".join(SDMX_ENDPOINTS.keys())
        raise ValueError(
            "Unknown endpoint: " + endpoint_key + ". Available: " + available
        )

    _current_endpoint_key = endpoint_key
    config = get_current_config()
    # Update the module-level variables
    SDMX_BASE_URL = config["base_url"]
    SDMX_AGENCY_ID = config["agency_id"]
    return config


# Initialize module-level variables
SDMX_BASE_URL = get_current_config()["base_url"]
SDMX_AGENCY_ID = get_current_config()["agency_id"]
