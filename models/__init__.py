"""
SDMX MCP Gateway - Pydantic Models Package

This package contains Pydantic schemas for structured tool outputs,
following MCP SDK v2 best practices.
"""

from models.schemas import (
    # Dimension codes schemas
    CodeInfo,
    # Availability schemas
    CubeRegion,
    DataAvailabilityResult,
    # Dataflow schemas
    DataflowInfo,
    DataflowListResult,
    DataflowStructureResult,
    DataflowSummary,
    # Query building schemas
    DataQueryConfirmation,
    DataUrlResult,
    DimensionCodesResult,
    # Structure schemas
    AttributeDetail,
    DimensionInfo,
    DimensionSelectionForm,
    # Guide schemas
    DiscoveryGuideResult,
    # Elicitation schemas
    ElicitationResult,
    # Endpoint schemas
    EndpointInfo,
    EndpointListResult,
    EndpointSwitchConfirmation,
    EndpointSwitchResult,
    # Common schemas
    ErrorResult,
    FilterInfo,
    # Validation schemas
    InvalidCode,
    KeyBuildResult,
    PaginationInfo,
    ProgressiveCheckResult,
    StructureInfo,
    TimeRange,
    ValidationIssue,
    ValidationResult,
)

__all__ = [
    # Common
    "PaginationInfo",
    "FilterInfo",
    "TimeRange",
    "ErrorResult",
    # Dataflow
    "DataflowSummary",
    "DataflowListResult",
    "DataflowInfo",
    "DataflowStructureResult",
    # Structure
    "AttributeDetail",
    "DimensionInfo",
    "StructureInfo",
    # Dimension codes
    "CodeInfo",
    "DimensionCodesResult",
    # Availability
    "CubeRegion",
    "DataAvailabilityResult",
    "ProgressiveCheckResult",
    # Validation
    "ValidationIssue",
    "InvalidCode",
    "ValidationResult",
    # Query building
    "KeyBuildResult",
    "DataUrlResult",
    # Endpoints
    "EndpointInfo",
    "EndpointListResult",
    "EndpointSwitchResult",
    # Elicitation
    "EndpointSwitchConfirmation",
    "DataQueryConfirmation",
    "DimensionSelectionForm",
    "ElicitationResult",
    # Guide
    "DiscoveryGuideResult",
]
