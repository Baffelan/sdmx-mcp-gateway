"""
Shared utilities and constants for SDMX MCP Gateway.
"""

import re
from typing import Any, Dict

# SDMX 2.1 XML namespaces
SDMX_NAMESPACES = {
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# Known SDMX agencies and their endpoints
KNOWN_AGENCIES = {
    "SPC": {
        "name": "Secretariat of the Pacific Community",
        "base_url": "https://stats-sdmx-disseminate.pacificdata.org/rest",
        "description": "Pacific regional statistics",
    },
    "ECB": {
        "name": "European Central Bank",
        "base_url": "https://sdw-wsrest.ecb.europa.eu/service",
        "description": "European financial and economic statistics",
    },
    "OECD": {
        "name": "Organisation for Economic Co-operation and Development",
        "base_url": "http://stats.oecd.org/sdmx-json",
        "description": "OECD countries economic and social statistics",
    },
    "EUROSTAT": {
        "name": "Statistical Office of the European Union",
        "base_url": "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/",
        "description": "European Union official statistics",
    },
    "ILO": {
        "name": "International Labour Organization",
        "base_url": "https://www.ilo.org/sdmx/rest",
        "description": "Labour and employment statistics worldwide",
    },
}

# SDMX data formats and their specifications
SDMX_FORMATS = {
    "csv": {
        "headers": {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"},
        "description": "CSV format - good for analysis in spreadsheets or pandas",
    },
    "json": {
        "headers": {"Accept": "application/vnd.sdmx.data+json;version=1.0.0"},
        "description": "JSON format - good for web applications and APIs",
    },
    "xml": {
        "headers": {"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"},
        "description": "SDMX-ML format - preserves all metadata and structure",
    },
}


def validate_dataflow_id(dataflow_id: str) -> bool:
    """Validate dataflow ID according to SDMX conventions."""
    return bool(re.match(r"^[a-zA-Z][a-zA-Z\d_-]*$", dataflow_id))


def validate_sdmx_key(key: str) -> bool:
    """Validate SDMX key syntax according to SDMX 2.1 specification.

    Valid patterns:
    - "all" - special keyword for all data
    - "A.TO.ICT" - single values per dimension
    - "A+M.TO+FJ.ICT" - multiple values with +
    - "..TO.." - empty dimensions (dots only)
    - "A..." - mix of specified and empty dimensions
    """
    if key == "all":
        return True

    # Each dimension position can be:
    # - Empty (will appear as nothing between dots)
    # - Single value: [A-Za-z\d_@$-]+
    # - Multiple values: value+value+value
    # Dimensions are separated by dots

    # Pattern for a single dimension value
    value_pattern = r"[A-Za-z\d_@$-]+"
    # Pattern for a dimension (can be empty, single, or multiple values with +)
    dimension_pattern = f"({value_pattern}(\\+{value_pattern})*)?"
    # Full key is dimensions separated by dots
    # We need at least one dot or one value to be valid
    key_pattern = f"^{dimension_pattern}(\\.{dimension_pattern})*$"

    return bool(re.match(key_pattern, key))


def validate_provider(provider: str) -> bool:
    """Validate provider syntax according to SDMX 2.1 specification.

    Valid providers:
    - "all" - special keyword
    - "ECB" - simple agency ID (must start with letter)
    - "ECB+OECD" - multiple agencies
    - "SPC.STAT" - dotted agency ID
    """
    if not provider:
        return False
    if provider == "all":
        return True
    # Provider ID must start with a letter, can contain letters, digits, underscore, hyphen, dot
    # Multiple providers can be joined with +
    return bool(re.match(r"^[A-Za-z][A-Za-z\d_.-]*(\+[A-Za-z][A-Za-z\d_.-]*)*$", provider))


def validate_period(period: str) -> bool:
    """Validate period format (ISO 8601 or SDMX reporting periods).

    Valid formats:
    - Year: 2023
    - Year-Month: 2023-01 (month must be 01-12)
    - Year-Month-Day: 2023-01-15 (basic day validation)
    - SDMX reporting: 2023-Q1, 2023-S1, 2023-M01, 2023-W01, 2023-A1
    """
    if not period:
        return False

    # Year only
    if re.match(r"^\d{4}$", period):
        return True

    # Year-Month (with proper month validation 01-12)
    if re.match(r"^\d{4}-(0[1-9]|1[0-2])$", period):
        return True

    # Year-Month-Day (with basic day validation 01-31)
    if re.match(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$", period):
        return True

    # SDMX reporting periods
    sdmx_pattern = r"^\d{4}-(A1|S[12]|Q[1-4]|T[1-3]|M(0[1-9]|1[0-2])|W(0[1-9]|[1-4]\d|5[0-3])|D(00[1-9]|0[1-9]\d|[12]\d{2}|3[0-5]\d|36[0-6]))$"
    return bool(re.match(sdmx_pattern, period))


def filter_dataflows_by_keywords(dataflows: list, keywords: list) -> list:
    """Filter dataflows by keyword relevance and return sorted by score."""
    if not keywords:
        return dataflows

    scored_dataflows = []
    for df in dataflows:
        searchable_text = f"{df['name']} {df['description']} {df['id']}".lower()

        score = 0
        for keyword in keywords:
            if keyword.lower() in searchable_text:
                score += 1

        if score > 0:
            df["relevance_score"] = score
            scored_dataflows.append(df)

    return sorted(scored_dataflows, key=lambda x: x.get("relevance_score", 0), reverse=True)
