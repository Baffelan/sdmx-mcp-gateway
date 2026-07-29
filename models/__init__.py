"""
SDMX MCP Gateway - Pydantic Models Package

This package contains Pydantic schemas for structured tool outputs,
following MCP SDK v2 best practices.
"""

from models.schemas import (
    # Structure schemas
    AttributeDetail,
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
    DimensionInfo,
    DimensionSelectionForm,
    # Guide schemas
    DiscoveryGuideResult,
    # Elicitation schemas
    ElicitationResult,
    # Endpoint schemas
    EndpointInfo,
    EndpointListResult,
    # Common schemas
    ErrorResult,
    FilterInfo,
    # Validation schemas
    InvalidCode,
    KeyBuildResult,
    MetadataAttribute,
    PaginationInfo,
    ProgressiveCheckResult,
    ReferenceMetadataResult,
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
    # Elicitation
    "DataQueryConfirmation",
    "DimensionSelectionForm",
    "ElicitationResult",
    # Guide
    "DiscoveryGuideResult",
    # Reference metadata
    "MetadataAttribute",
    "ReferenceMetadataResult",
]
