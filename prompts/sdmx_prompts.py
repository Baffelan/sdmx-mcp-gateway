"""
SDMX-specific MCP prompts for guided query construction.
"""


def sdmx_discovery_guide(query_description: str) -> str:
    """
    Guide for discovering SDMX data step-by-step.
    
    Provides a structured approach to finding and accessing SDMX statistical data.
    """
    return f"""
# SDMX Data Discovery Guide

Based on your query: "{query_description}"

Follow these steps to discover and access SDMX data:

## Step 1: Discover Available Dataflows
```
list_dataflows(keywords=["keyword1", "keyword2"])
```
This shows you what statistical domains (dataflows) are available that match your interests.

## Step 2: Explore Data Structure  
```
get_dataflow_structure(dataflow_id="SELECTED_DATAFLOW_ID")
```
This reveals the dimensions, measures, and organization of the data.

## Step 3: Browse Dimension Codes
```
explore_codelist(codelist_id="DIMENSION_CODELIST", search_term="search_term")
```
This helps you find the exact codes for countries, indicators, time periods, etc.

## Step 4: Validate Your Query
```
validate_query_syntax(
    dataflow_id="DATAFLOW_ID",
    key="dimension1_code.dimension2_code.dimension3_code", 
    start_period="2020",
    end_period="2023"
)
```

## Step 5: Build Data URLs
```
build_data_query(
    dataflow_id="DATAFLOW_ID",
    key="your_validated_key",
    format_type="csv"
)
```

## Tips:
- Start broad, then narrow down your search
- Use the "all" keyword for dimensions you want to include everything
- Consider time constraints to limit data size
- CSV format is often easiest for analysis

Would you like help with any specific step?
"""


def sdmx_troubleshooting_guide(error_type: str, error_details: str = "") -> str:
    """
    Troubleshooting guide for common SDMX issues.
    """
    base_guide = f"""
# SDMX Troubleshooting Guide

Issue: {error_type}
Details: {error_details}

## Common Solutions:

### 1. HTTP 404 - Not Found
- Check dataflow ID spelling and case sensitivity
- Verify agency ID is correct (SPC, ECB, OECD, etc.)
- Try version "latest" instead of specific version
- Use list_dataflows() to see available dataflows

### 2. HTTP 400 - Bad Request  
- Validate key syntax with validate_query_syntax()
- Check period format (use YYYY, YYYY-MM, or YYYY-Q1)
- Ensure dimension codes exist in codelists
- Use explore_codelist() to find valid codes

### 3. Empty Results
- Try broader key (use "all" or wildcards)
- Check time period constraints
- Verify data availability with different providers
- Use different dimensionAtObservation values

### 4. Large Dataset Issues
- Add time constraints (startPeriod, endPeriod)
- Use more specific key instead of "all"
- Try detail="serieskeysonly" first to see series
- Consider pagination or streaming

### 5. Authentication/Access Issues
- Check if agency requires authentication
- Try different base URL endpoints
- Some agencies have rate limiting

## Debugging Steps:
1. Start with list_dataflows() to verify dataflow exists
2. Use get_dataflow_structure() to understand dimensions
3. Use explore_codelist() to find valid codes
4. Use validate_query_syntax() before building final query
5. Test with small time periods first

## Need Help?
- Check agency documentation
- Try simpler queries first
- Use different data formats (CSV vs JSON vs XML)
"""

    return base_guide


def sdmx_best_practices(use_case: str) -> str:
    """
    Best practices guide for different SDMX use cases.
    """
    guides = {
        "research": """
# SDMX Best Practices for Research

## Data Discovery Strategy:
1. Start with broad keyword searches
2. Explore multiple agencies (SPC, ECB, OECD, EUROSTAT)
3. Check data availability and coverage before detailed analysis
4. Document your data sources and query parameters

## Query Construction:
- Use specific time periods to avoid large downloads
- Start with annual data, then get more frequent if needed
- Use "serieskeysonly" detail to explore series structure
- Test queries with small datasets first

## Data Quality:
- Check metadata for data definitions and methodology
- Be aware of seasonal adjustments and revisions
- Note break in series or methodology changes
- Cross-reference with official publications

## Performance:
- Cache frequently used dataflows locally
- Use appropriate data formats (CSV for analysis, JSON for web)
- Consider data update frequencies for automation
""",
        
        "dashboard": """
# SDMX Best Practices for Dashboards

## Real-time Data:
- Check data update schedules at agencies
- Use latest available periods, not fixed dates
- Implement error handling for data unavailability
- Cache data with appropriate TTL

## User Experience:
- Show progress indicators for long-running queries
- Provide data source attribution and links
- Allow users to customize time periods and geographies
- Offer multiple visualization formats

## Technical Implementation:
- Use JSON format for web applications
- Implement client-side caching
- Handle different time period formats gracefully
- Provide fallback data sources
""",

        "automation": """
# SDMX Best Practices for Automation

## Reliable Data Pipeline:
- Implement robust error handling and retries
- Monitor data availability and quality
- Log all data requests for debugging
- Use specific versions when possible for reproducibility

## Efficiency:
- Schedule downloads during off-peak hours
- Use incremental updates when possible
- Implement rate limiting to respect agency resources
- Cache metadata to reduce API calls

## Data Management:
- Version your data extracts
- Maintain data lineage and provenance
- Implement data validation checks
- Document any data transformations
"""
    }
    
    return guides.get(use_case.lower(), f"Best practices guide for '{use_case}' not available. Available guides: research, dashboard, automation")


def sdmx_query_builder(dataflow_info: dict, user_requirements: str) -> str:
    """
    Interactive query builder prompt based on dataflow structure.
    """
    dataflow_id = dataflow_info.get('id', 'UNKNOWN')
    dataflow_name = dataflow_info.get('name', 'Unknown Dataflow')
    
    return f"""
# SDMX Query Builder

## Target Dataflow: {dataflow_id}
**Name:** {dataflow_name}

## Your Requirements: {user_requirements}

## Query Construction Steps:

### 1. Define Your Key
Based on the dataflow structure, construct your key by specifying values for each dimension:

```
# Key format: dimension1.dimension2.dimension3...
# Use explore_codelist() to find valid codes for each dimension

key = "FREQ.REF_AREA.INDICATOR.UNIT"
```

### 2. Set Time Constraints
```
start_period = "2020"     # Start year
end_period = "2023"       # End year
```

### 3. Choose Data View
```
dimension_at_observation = "TIME_PERIOD"  # Time series view
# OR
dimension_at_observation = "AllDimensions"  # Flat table view
```

### 4. Select Detail Level
```
detail = "full"           # All data and metadata
detail = "dataonly"       # Just the numbers
detail = "serieskeysonly" # Just series identifiers
```

### 5. Validate and Build
```
# First validate your parameters
validate_query_syntax(
    dataflow_id="{dataflow_id}",
    key=your_key,
    start_period=your_start,
    end_period=your_end
)

# Then build the final query
build_data_query(
    dataflow_id="{dataflow_id}",
    key=your_key,
    start_period=your_start,
    end_period=your_end,
    format_type="csv"
)
```

## Tips for This Dataflow:
- Use explore_codelist() to see available countries, indicators, etc.
- Start with a specific country/indicator to test
- Consider using "all" for dimensions you want all values
- CSV format is recommended for analysis

Ready to start building your query?
"""