"""
Pydantic schemas for SDMX MCP Gateway structured tool outputs.

These schemas define the structured output format for all MCP tools,
enabling automatic validation and JSON Schema generation for the MCP protocol.

Following MCP SDK v2 best practices for structured output support.
"""

from typing import Optional

from pydantic import BaseModel, Field

# =============================================================================
# Common/Shared Schemas
# =============================================================================


class PaginationInfo(BaseModel):
    """Pagination metadata for list responses."""

    has_more: bool = Field(description="Whether there are more results available")
    next_offset: Optional[int] = Field(
        default=None, description="Offset for the next page, if available"
    )
    total_pages: int = Field(description="Total number of pages")
    current_page: int = Field(description="Current page number (1-based)")


class FilterInfo(BaseModel):
    """Information about applied filters."""

    keywords_used: list[str] = Field(description="Keywords used for filtering")
    total_before_filter: int = Field(description="Total count before filtering")
    total_after_filter: int = Field(description="Total count after filtering")
    filter_reduced_by: int = Field(description="Number of items filtered out")


class TimeRange(BaseModel):
    """Time period range."""

    start: Optional[str] = Field(default=None, description="Start period")
    end: Optional[str] = Field(default=None, description="End period")


class ErrorResult(BaseModel):
    """Standard error response."""

    error: str = Field(description="Error message")
    details: Optional[str] = Field(default=None, description="Additional error details")


# =============================================================================
# Dataflow Schemas
# =============================================================================


class DataflowSummary(BaseModel):
    """Lightweight dataflow summary for list responses."""

    id: str = Field(description="Dataflow identifier")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="Brief description (may be truncated)")


class DataflowListResult(BaseModel):
    """Result from list_dataflows() tool."""

    discovery_level: str = Field(default="overview", description="Discovery workflow level")
    agency_id: str = Field(description="Agency identifier queried")
    total_found: int = Field(description="Total dataflows found (after filtering)")
    showing: int = Field(description="Number of dataflows in this response")
    offset: int = Field(description="Current offset for pagination")
    limit: int = Field(description="Maximum results per page")
    keywords: Optional[list[str]] = Field(default=None, description="Keywords used for filtering")
    dataflows: list[DataflowSummary] = Field(description="List of dataflow summaries")
    pagination: PaginationInfo = Field(description="Pagination information")
    filter_info: Optional[FilterInfo] = Field(
        default=None, description="Filter statistics if keywords were used"
    )
    next_step: str = Field(description="Suggested next action in the discovery workflow")


# =============================================================================
# Structure Schemas
# =============================================================================


class DimensionInfo(BaseModel):
    """Information about a dataflow dimension."""

    id: str = Field(description="Dimension identifier")
    position: int = Field(description="Position in the SDMX key (0-based)")
    type: str = Field(description="Dimension type (e.g., 'Dimension', 'TimeDimension')")
    codelist: Optional[str] = Field(default=None, description="Associated codelist ID")


class DataflowInfo(BaseModel):
    """Basic dataflow information."""

    id: str = Field(description="Dataflow identifier")
    name: str = Field(description="Human-readable name")
    description: str = Field(description="Description")
    version: str = Field(description="Resolved version number")


class StructureInfo(BaseModel):
    """Data structure definition information."""

    id: str = Field(description="Structure identifier")
    key_template: str = Field(
        description="Template showing dimension order (e.g., '{FREQ}.{GEO}.{INDICATOR}')"
    )
    key_example: str = Field(description="Example key with placeholders")
    dimensions: list[DimensionInfo] = Field(description="List of dimensions in order")
    attributes: list[str] = Field(description="List of attribute identifiers")
    measure: Optional[str] = Field(default=None, description="Primary measure identifier")


class DataflowStructureResult(BaseModel):
    """Result from get_dataflow_structure() tool."""

    discovery_level: str = Field(default="structure", description="Discovery workflow level")
    dataflow: DataflowInfo = Field(description="Dataflow metadata")
    structure: StructureInfo = Field(description="Data structure definition")
    next_steps: list[str] = Field(description="Suggested next actions")


# =============================================================================
# Dimension Codes Schemas
# =============================================================================


class CodeInfo(BaseModel):
    """Information about a single code value."""

    id: str = Field(description="Code identifier (use this in queries)")
    name: str = Field(description="Human-readable name")
    description: Optional[str] = Field(default=None, description="Additional description")


class DimensionCodesResult(BaseModel):
    """Result from get_dimension_codes() tool."""

    discovery_level: str = Field(default="dimension_codes", description="Discovery workflow level")
    dataflow_id: str = Field(description="Parent dataflow identifier")
    dimension_id: str = Field(description="Dimension identifier")
    position: int = Field(description="Position in the SDMX key")
    codelist_id: Optional[str] = Field(default=None, description="Source codelist identifier")
    total_codes: int = Field(description="Total codes available")
    showing: int = Field(description="Number of codes in this response")
    search_term: Optional[str] = Field(default=None, description="Search term used for filtering")
    codes: list[CodeInfo] = Field(description="List of code values")
    usage: str = Field(description="How to use these codes in queries")
    example_keys: list[str] = Field(description="Example key construction hints")


# =============================================================================
# Data Availability Schemas
# =============================================================================


class CubeRegion(BaseModel):
    """Represents a region of available data in the data cube."""

    keys: dict[str, list[str]] = Field(description="Dimension values with available data")
    included: bool = Field(default=True, description="Whether this region is included or excluded")


class DataAvailabilityResult(BaseModel):
    """Result from get_data_availability() tool."""

    discovery_level: str = Field(default="availability", description="Discovery workflow level")
    dataflow_id: str = Field(description="Dataflow identifier")
    has_constraint: bool = Field(description="Whether availability constraints exist")
    constraint_id: Optional[str] = Field(
        default=None, description="Constraint identifier if available"
    )
    time_range: Optional[TimeRange] = Field(default=None, description="Available time period range")
    cube_regions: list[CubeRegion] = Field(
        default_factory=list, description="Specific data regions available"
    )
    interpretation: list[str] = Field(
        default_factory=list, description="Human-readable interpretation"
    )
    dimension_values_checked: Optional[dict[str, str]] = Field(
        default=None, description="Dimension values that were checked"
    )
    data_exists: Optional[bool] = Field(
        default=None, description="Whether data exists for checked combination"
    )
    recommendation: Optional[str] = Field(
        default=None, description="Recommendation based on availability"
    )


class ProgressiveCheckResult(BaseModel):
    """Result from progressive availability checking."""

    discovery_level: str = Field(
        default="progressive_availability", description="Discovery workflow level"
    )
    dataflow_id: str = Field(description="Dataflow identifier")
    checks: list[dict[str, str | int | bool | None]] = Field(
        description="Results of each progressive check"
    )
    summary: str = Field(description="Summary of availability findings")
    recommendation: str = Field(description="Recommended next action")


# =============================================================================
# Validation Schemas
# =============================================================================


class ValidationIssue(BaseModel):
    """A validation error or warning."""

    type: str = Field(description="Issue type: 'error' or 'warning'")
    field: str = Field(description="Field that has the issue")
    message: str = Field(description="Description of the issue")


class InvalidCode(BaseModel):
    """Information about an invalid dimension code."""

    dimension: str = Field(description="Dimension identifier")
    code: str = Field(description="Invalid code value")
    valid_codes_sample: list[str] = Field(description="Sample of valid codes for this dimension")


class ValidationResult(BaseModel):
    """Result from validate_query() tool."""

    valid: bool = Field(description="Whether the query is valid")
    dataflow_id: str = Field(description="Dataflow being validated against")
    key: str = Field(description="Key that was validated")
    errors: list[ValidationIssue] = Field(default_factory=list, description="Validation errors")
    warnings: list[ValidationIssue] = Field(default_factory=list, description="Validation warnings")
    invalid_codes: list[InvalidCode] = Field(
        default_factory=list,
        description="Invalid dimension codes if code validation was performed",
    )
    suggestion: Optional[str] = Field(default=None, description="Suggestion for fixing issues")


# =============================================================================
# Query Building Schemas
# =============================================================================


class KeyBuildResult(BaseModel):
    """Result from build_key() tool."""

    dataflow_id: str = Field(description="Dataflow identifier")
    version: str = Field(description="Resolved dataflow version")
    key: str = Field(description="Constructed SDMX key")
    dimensions_used: dict[str, str] = Field(description="Dimension values that were specified")
    dimensions_wildcard: list[str] = Field(description="Dimensions left as wildcard (all values)")
    key_template: str = Field(description="Template showing dimension positions")
    usage: str = Field(description="How to use this key")


class DataUrlResult(BaseModel):
    """Result from build_data_url() tool."""

    dataflow_id: str = Field(description="Dataflow identifier")
    version: str = Field(description="Resolved dataflow version")
    key: str = Field(description="SDMX key used")
    format: str = Field(description="Output format (csv, json, xml)")
    url: str = Field(description="Complete data retrieval URL")
    dimension_at_observation: str = Field(description="Observation dimension setting")
    time_range: Optional[TimeRange] = Field(
        default=None, description="Time period filter if specified"
    )
    usage: str = Field(description="Instructions for using the URL")
    formats_available: list[str] = Field(
        default=["csv", "json", "xml"], description="Available output formats"
    )
    note: Optional[str] = Field(
        default=None, description="Additional notes (e.g., version resolution)"
    )


# =============================================================================
# Endpoint Management Schemas
# =============================================================================


class EndpointInfo(BaseModel):
    """Information about an SDMX endpoint."""

    key: Optional[str] = Field(default=None, description="Endpoint key (e.g., 'SPC', 'ECB')")
    name: str = Field(description="Human-readable endpoint name")
    base_url: str = Field(description="API base URL")
    agency_id: str = Field(description="Default agency identifier")
    description: str = Field(description="What data this endpoint provides")
    status: str = Field(default="Active", description="Endpoint status")
    is_current: bool = Field(
        default=False, description="Whether this is the currently active endpoint"
    )


class EndpointListResult(BaseModel):
    """Result from list_available_endpoints() tool."""

    current: str = Field(description="Currently active endpoint key")
    endpoints: list[EndpointInfo] = Field(description="List of available endpoints")
    note: str = Field(description="Usage hint")


class EndpointSwitchResult(BaseModel):
    """Result from switch_endpoint() tool."""

    success: bool = Field(description="Whether the switch was successful")
    message: str = Field(description="Status message")
    new_endpoint: Optional[EndpointInfo] = Field(
        default=None, description="New endpoint info if successful"
    )
    error: Optional[str] = Field(default=None, description="Error message if failed")
    available_endpoints: Optional[list[str]] = Field(
        default=None, description="Available endpoint keys if switch failed"
    )
    hint: Optional[str] = Field(default=None, description="Hint for fixing the error")


# =============================================================================
# Elicitation Schemas
# =============================================================================


class EndpointSwitchConfirmation(BaseModel):
    """Schema for endpoint switch confirmation elicitation."""

    confirm: bool = Field(default=False, description="Confirm switching to the new endpoint")
    reason: Optional[str] = Field(
        default=None, description="Optional reason for switching (for logging)"
    )


class DataQueryConfirmation(BaseModel):
    """Schema for confirming a potentially large data query."""

    proceed: bool = Field(default=False, description="Proceed with the query")
    limit_results: bool = Field(default=True, description="Limit results to avoid large downloads")
    max_observations: int = Field(
        default=10000, description="Maximum number of observations to retrieve"
    )


class DimensionSelectionForm(BaseModel):
    """Schema for selecting dimension values interactively."""

    selected_values: list[str] = Field(
        default_factory=list, description="Selected dimension values"
    )
    include_all: bool = Field(default=False, description="Include all values (wildcard)")


class ElicitationResult(BaseModel):
    """Generic result from an elicitation request."""

    action: str = Field(description="User action: 'accept', 'decline', or 'cancel'")
    data: Optional[dict[str, str | int | bool | None]] = Field(
        default=None, description="User-provided data if accepted"
    )
    message: Optional[str] = Field(default=None, description="Additional message or context")


# =============================================================================
# Discovery Guide Schema
# =============================================================================


class DiscoveryGuideResult(BaseModel):
    """Result from get_discovery_guide() tool."""

    title: str = Field(description="Guide title")
    current_step: int = Field(description="Current step in the workflow")
    total_steps: int = Field(description="Total steps in the workflow")
    steps: list[dict[str, str | int]] = Field(
        description="List of workflow steps with descriptions"
    )
    tips: list[str] = Field(description="Helpful tips for the discovery process")
