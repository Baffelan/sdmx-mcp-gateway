"""
SDMX-specific MCP resources for metadata browsing.
"""

import json
from utils import KNOWN_AGENCIES


def list_known_agencies() -> str:
    """List of well-known SDMX data agencies and their endpoints."""
    return json.dumps(KNOWN_AGENCIES, indent=2)


def get_agency_info(agency_id: str) -> str:
    """Get information about a specific SDMX data agency."""
    if agency_id.upper() in KNOWN_AGENCIES:
        return json.dumps(KNOWN_AGENCIES[agency_id.upper()], indent=2)
    else:
        return json.dumps({
            "error": f"Unknown agency: {agency_id}",
            "available_agencies": list(KNOWN_AGENCIES.keys())
        }, indent=2)


def get_sdmx_format_guide() -> str:
    """Guide to SDMX data formats and their use cases."""
    guide = {
        "sdmx_formats": {
            "csv": {
                "mime_type": "application/vnd.sdmx.data+csv;version=2.0.0",
                "description": "Comma-separated values format",
                "use_cases": [
                    "Analysis in spreadsheet applications",
                    "Data processing with pandas/R",
                    "Simple data visualization"
                ],
                "pros": ["Human readable", "Widely supported", "Small file size"],
                "cons": ["Limited metadata", "No structure information"]
            },
            "json": {
                "mime_type": "application/vnd.sdmx.data+json;version=1.0.0", 
                "description": "JavaScript Object Notation format",
                "use_cases": [
                    "Web applications and APIs",
                    "JavaScript-based analysis",
                    "REST API integration"
                ],
                "pros": ["Web-friendly", "Good metadata support", "Easy parsing"],
                "cons": ["Larger than CSV", "Less tool support than CSV"]
            },
            "xml": {
                "mime_type": "application/vnd.sdmx.structurespecificdata+xml;version=2.1",
                "description": "SDMX-ML XML format",
                "use_cases": [
                    "Full metadata preservation", 
                    "Statistical software integration",
                    "Official data exchange"
                ],
                "pros": ["Complete metadata", "Official SDMX standard", "Validation support"],
                "cons": ["Complex structure", "Large file size", "Requires SDMX knowledge"]
            }
        },
        "choosing_format": {
            "for_analysis": "Use CSV - easiest to work with in most tools",
            "for_web_apps": "Use JSON - native web format with good metadata",
            "for_official_use": "Use XML - preserves all SDMX metadata and structure",
            "for_large_datasets": "Use CSV - most compact format"
        }
    }
    
    return json.dumps(guide, indent=2)


def get_sdmx_query_syntax_guide() -> str:
    """Guide to SDMX query syntax and key construction."""
    guide = {
        "sdmx_key_syntax": {
            "description": "SDMX keys identify specific data series using dimension values",
            "format": "dimension1_value.dimension2_value.dimension3_value",
            "examples": {
                "full_key": {
                    "syntax": "M.DE.000000.ANR",
                    "meaning": "Monthly (M) data for Germany (DE), overall inflation (000000), annual rate (ANR)"
                },
                "partial_key": {
                    "syntax": "A+M..000000.ANR",
                    "meaning": "Annual OR Monthly (A+M) data, any country (.), overall inflation (000000), annual rate (ANR)"
                },
                "wildcard": {
                    "syntax": "A+M..000000.",
                    "meaning": "Annual OR Monthly data, any country, overall inflation, any unit"
                },
                "all_data": {
                    "syntax": "all",
                    "meaning": "All available data (use with caution - can be very large)"
                }
            }
        },
        "dimension_operators": {
            "dot": {
                "symbol": ".",
                "description": "Separates dimension values",
                "example": "M.DE.FOOD"
            },
            "plus": {
                "symbol": "+", 
                "description": "OR operator - includes multiple values",
                "example": "A+M (annual OR monthly)"
            },
            "empty": {
                "symbol": "",
                "description": "Wildcard - includes all values for this dimension",
                "example": "M..FOOD (monthly data, any country, food category)"
            }
        },
        "provider_syntax": {
            "description": "Specifies which organization provides the data",
            "examples": {
                "single": "ECB (European Central Bank data only)",
                "multiple": "ECB+OECD (data from either ECB or OECD)",
                "all": "all (data from any provider)"
            }
        },
        "period_formats": {
            "iso_8601": {
                "year": "2023",
                "month": "2023-01", 
                "date": "2023-01-15"
            },
            "sdmx_reporting": {
                "quarter": "2023-Q1",
                "semester": "2023-S1",
                "annual": "2023-A1"
            }
        }
    }
    
    return json.dumps(guide, indent=2)