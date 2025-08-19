"""
SDMX-specific MCP tools for progressive data discovery.
"""

import logging
from typing import Any, Dict, List, Optional
import json
from urllib.parse import quote

from mcp.server.fastmcp import Context

from sdmx_client import SDMXClient
from utils import (
    validate_dataflow_id, validate_sdmx_key, validate_provider, validate_period,
    filter_dataflows_by_keywords, SDMX_FORMATS
)

logger = logging.getLogger(__name__)

# Global SDMX client instance
sdmx_client = SDMXClient()


async def list_dataflows(keywords: List[str] = None, 
                        agency_id: str = "SPC",
                        include_references: bool = False,
                        ctx: Context = None) -> Dict[str, Any]:
    """
    Discover available SDMX dataflows, optionally filtered by keywords.
    
    This is typically the first step in SDMX data discovery.
    """
    references = "all" if include_references else "none"
    
    try:
        all_dataflows = await sdmx_client.discover_dataflows(
            agency_id=agency_id,
            references=references,
            ctx=ctx
        )
        
        # Filter by keywords if provided
        if keywords:
            if ctx:
                ctx.info(f"Filtering dataflows by keywords: {keywords}")
            
            dataflows = filter_dataflows_by_keywords(all_dataflows, keywords)[:10]  # Top 10
        else:
            dataflows = all_dataflows
        
        return {
            "agency_id": agency_id,
            "total_dataflows": len(all_dataflows),
            "filtered_dataflows": len(dataflows),
            "keywords": keywords,
            "dataflows": dataflows,
            "next_steps": [
                "Use get_dataflow_structure() to explore dimensions and structure",
                "Use explore_codelist() to browse available codes for dimensions",
                "Use build_data_query() to construct data retrieval URLs"
            ]
        }
        
    except Exception as e:
        logger.exception("Failed to list dataflows")
        return {
            "error": str(e),
            "agency_id": agency_id,
            "keywords": keywords,
            "dataflows": []
        }


async def get_dataflow_structure(dataflow_id: str,
                                agency_id: str = "SPC", 
                                version: str = "latest",
                                include_references: bool = True,
                                ctx: Context = None) -> Dict[str, Any]:
    """
    Get detailed structure information for a specific dataflow.
    
    Returns dimensions, attributes, measures, and codelist references.
    Use this after list_dataflows() to understand data organization.
    """
    references = "all" if include_references else "none"
    
    try:
        structure_info = await sdmx_client.get_datastructure(
            dataflow_id=dataflow_id,
            agency_id=agency_id,
            version=version,
            references=references,
            ctx=ctx
        )
        
        return {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "version": version,
            "structure": structure_info,
            "next_steps": [
                "Use explore_codelist() to browse codes for specific dimensions",
                "Use validate_query_syntax() to check query parameters",
                "Use build_data_query() to construct data URLs"
            ]
        }
        
    except Exception as e:
        logger.exception(f"Failed to get structure for {dataflow_id}")
        return {
            "error": str(e),
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "structure": None
        }


async def explore_codelist(codelist_id: str,
                          agency_id: str = "SPC",
                          version: str = "latest", 
                          search_term: str = None,
                          ctx: Context = None) -> Dict[str, Any]:
    """
    Browse codes and values for a specific codelist.
    
    Codelists define the allowed values for dimensions (e.g., country codes, commodity codes).
    Use this to find the exact codes needed for your data query.
    """
    try:
        codelist_info = await sdmx_client.get_codelist(
            codelist_id=codelist_id,
            agency_id=agency_id,
            version=version,
            ctx=ctx
        )
        
        if not codelist_info or codelist_info.get("error"):
            return codelist_info
        
        codes = codelist_info.get("codes", [])
        
        # Filter by search term if provided
        if search_term:
            if ctx:
                ctx.info(f"Filtering codes by search term: {search_term}")
            
            search_lower = search_term.lower()
            filtered_codes = []
            
            for code in codes:
                if (search_lower in code["id"].lower() or 
                    search_lower in code["name"].lower() or 
                    search_lower in code["description"].lower()):
                    filtered_codes.append(code)
            
            codes = filtered_codes
        
        return {
            "codelist_id": codelist_id,
            "agency_id": agency_id,
            "version": version,
            "search_term": search_term,
            "total_codes": codelist_info.get("total_codes", 0),
            "filtered_codes": len(codes),
            "codes": codes[:50],  # Limit to first 50 for readability
            "next_steps": [
                "Use these codes in validate_query_syntax()",
                "Use these codes in build_data_query() key parameter"
            ]
        }
        
    except Exception as e:
        logger.exception(f"Failed to explore codelist {codelist_id}")
        return {
            "error": str(e),
            "codelist_id": codelist_id,
            "agency_id": agency_id,
            "codes": []
        }


def validate_query_syntax(dataflow_id: str,
                         key: str = "all",
                         provider: str = "all", 
                         start_period: str = None,
                         end_period: str = None,
                         agency_id: str = "SPC",
                         version: str = "latest") -> Dict[str, Any]:
    """
    Validate SDMX query parameters before building the final URL.
    
    Checks syntax according to SDMX 2.1 REST API specification.
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
        
        if validation_results["validation"]["is_valid"]:
            validation_results["next_steps"] = [
                "Parameters are valid - use build_data_query() to generate URLs",
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


def build_data_query(dataflow_id: str,
                    key: str = "all",
                    provider: str = "all",
                    start_period: str = None,
                    end_period: str = None,
                    dimension_at_observation: str = "TIME_PERIOD",
                    detail: str = "full",
                    agency_id: str = "SPC",
                    version: str = "latest",
                    format_type: str = "csv") -> Dict[str, Any]:
    """
    Generate final SDMX REST API URLs for data retrieval.
    
    Creates URLs that can be used directly to download data in various formats.
    This is the final step in the SDMX query construction process.
    """
    try:
        # Build base data URL according to SDMX 2.1 spec
        base_url = sdmx_client.base_url
        flow_spec = f"{agency_id},{dataflow_id},{version}"
        data_url = f"{base_url}/data/{flow_spec}/{quote(key)}/{quote(provider)}"
        
        # Build query parameters
        params = []
        
        if start_period:
            params.append(f"startPeriod={quote(start_period)}")
        
        if end_period:
            params.append(f"endPeriod={quote(end_period)}")
        
        if dimension_at_observation != "TIME_PERIOD":
            params.append(f"dimensionAtObservation={quote(dimension_at_observation)}")
        
        if detail != "full":
            params.append(f"detail={quote(detail)}")
        
        # Add format-specific parameters
        if params:
            data_url += "?" + "&".join(params)
        
        # Get format definitions
        formats = {}
        for fmt_name, fmt_info in SDMX_FORMATS.items():
            formats[fmt_name] = {
                "url": data_url,
                "headers": fmt_info["headers"],
                "description": fmt_info["description"]
            }
        
        result = {
            "dataflow_id": dataflow_id,
            "agency_id": agency_id,
            "version": version,
            "query_parameters": {
                "key": key,
                "provider": provider,
                "start_period": start_period,
                "end_period": end_period,
                "dimension_at_observation": dimension_at_observation,
                "detail": detail
            },
            "primary_format": format_type,
            "primary_url": formats[format_type]["url"],
            "primary_headers": formats[format_type]["headers"],
            "all_formats": formats,
            "example_usage": {
                "python": f'''
import requests
import pandas as pd

url = "{formats[format_type]["url"]}"
headers = {json.dumps(formats[format_type]["headers"], indent=2)}

response = requests.get(url, headers=headers)
response.raise_for_status()

{"df = pd.read_csv(StringIO(response.text))" if format_type == "csv" else "data = response.json()" if format_type == "json" else "# Parse XML response"}
print({"df.head()" if format_type == "csv" else "data" if format_type == "json" else "response.text[:1000]"})
''',
                "curl": f'''curl -H "{list(formats[format_type]["headers"].items())[0][0]}: {list(formats[format_type]["headers"].items())[0][1]}" \\
     "{formats[format_type]["url"]}"'''
            }
        }
        
        return result
        
    except Exception as e:
        logger.exception("Failed to build data query")
        return {
            "error": str(e),
            "dataflow_id": dataflow_id,
            "agency_id": agency_id
        }


async def cleanup_sdmx_client():
    """Cleanup SDMX client resources."""
    await sdmx_client.close()