"""
SDMX-specific type definitions for client responses.

This module defines strongly-typed structures for data returned by the SDMX API,
replacing generic `Dict[str, Any]` with specific TypedDicts and Pydantic models.

The SDMX REST API returns XML (SDMX-ML) which is parsed into these structures.
All fields are based on SDMX 2.1 standard elements and attributes.

Reference: https://github.com/sdmx-twg/sdmx-rest/blob/master/doc/rest-cheat-sheet.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypedDict

# =============================================================================
# Base Reference Types (common patterns in SDMX)
# =============================================================================


class MaintainableRef(TypedDict):
    """Reference to a maintainable SDMX artefact (dataflow, DSD, codelist, etc.)."""

    id: str
    agency: str
    version: str


class MaintainableRefOptional(TypedDict, total=False):
    """Optional reference to a maintainable artefact."""

    id: str
    agency: str
    version: str


# =============================================================================
# Code and Codelist Types
# =============================================================================


class CodeValue(TypedDict):
    """A single code value from an SDMX codelist."""

    id: str
    name: str


class CodeValueWithDescription(TypedDict):
    """A code value with optional description."""

    id: str
    name: str
    description: str


class CodelistResult(TypedDict):
    """Result from fetching a codelist."""

    codelist_id: str
    agency_id: str
    version: str
    name: str
    codes: list[CodeValueWithDescription]
    total_codes: int
    filtered_by: str | None


class CodelistError(TypedDict):
    """Error response from codelist fetch."""

    codelist_id: str
    error: str
    codes: list[CodeValueWithDescription]  # Empty list


# =============================================================================
# Dimension Types
# =============================================================================


class DimensionCodesResult(TypedDict):
    """Result from get_dimension_codes()."""

    dimension_id: str
    position: int
    codelist: MaintainableRef
    total_codes: int
    codes: list[CodeValue]
    truncated: bool
    search_term: str | None


class DimensionNotFound(TypedDict):
    """Error when dimension is not found in dataflow."""

    dimension_id: str
    error: str
    available_dimensions: list[str]


class TimeDimensionInfo(TypedDict):
    """Information about a time dimension."""

    dimension_id: str
    type: Literal["TimeDimension"]
    format: str
    examples: list[str]
    note: str


class DimensionCodeError(TypedDict):
    """Error when fetching dimension codes fails."""

    dimension_id: str
    error: str


class NoCodlistDimension(TypedDict):
    """Response when dimension has no associated codelist."""

    dimension_id: str
    type: str
    error: str


DimensionCodesResponse = (
    DimensionCodesResult
    | DimensionNotFound
    | TimeDimensionInfo
    | DimensionCodeError
    | NoCodlistDimension
)


# =============================================================================
# Dataflow Types
# =============================================================================


class DataflowMetadata(TypedDict):
    """Full metadata for a single dataflow from discover_dataflows()."""

    id: str
    agency: str
    version: str
    name: str
    description: str
    is_final: bool
    structure_reference: MaintainableRef | None
    data_url_template: str
    metadata_url: str


class DataflowListItem(TypedDict):
    """Lightweight dataflow info for list responses."""

    id: str
    name: str
    description: str


# =============================================================================
# Data Availability Types
# =============================================================================


class TimeRangeInfo(TypedDict):
    """Time range for available data."""

    start: str
    end: str


class CubeRegionInfo(TypedDict):
    """Information about an available data region."""

    included: bool
    keys: dict[str, list[str]]  # dimension_id -> list of available values


class DataAvailability(TypedDict):
    """Result from get_actual_availability() when constraint found."""

    dataflow_id: str
    has_constraint: Literal[True]
    constraint_id: str
    cube_regions: list[CubeRegionInfo]
    key_sets: list[dict[str, str]]
    time_range: TimeRangeInfo | None


class DataAvailabilityNotFound(TypedDict):
    """Result when no availability constraint exists."""

    dataflow_id: str
    has_constraint: Literal[False]
    note: str


class DataAvailabilityError(TypedDict):
    """Error fetching availability information."""

    dataflow_id: str
    error: str


DataAvailabilityResponse = DataAvailability | DataAvailabilityNotFound | DataAvailabilityError


# =============================================================================
# Query Guide Types
# =============================================================================


class QueryStep(TypedDict):
    """A step in the progressive query guide."""

    position: int
    dimension: str
    type: str
    required: bool
    instruction: str


class QueryExample(TypedDict):
    """An example SDMX query key."""

    description: str
    key: str


class ProgressiveQueryGuide(TypedDict):
    """Guide for constructing SDMX queries."""

    key_structure: str
    dimensions_order: list[str]
    steps: list[QueryStep]
    examples: list[QueryExample]


# =============================================================================
# Structure Summary Types (for intermediate parsing)
# =============================================================================


class AttributeInfo(TypedDict):
    """Attribute metadata from data structure."""

    id: str
    assignment_status: str | None


# =============================================================================
# XML Parsing Result Types
# =============================================================================


class ParsedDataflow(TypedDict):
    """Dataflow parsed from SDMX-ML response."""

    id: str
    agency_id: str
    version: str
    name: str
    description: str
    is_final: bool
    dsd_ref: MaintainableRef | None


class ParsedCode(TypedDict):
    """Code parsed from SDMX-ML codelist."""

    id: str
    name: str
    description: str | None


class ParsedDimension(TypedDict):
    """Dimension parsed from SDMX-ML DSD."""

    id: str
    position: int
    type: Literal["Dimension", "TimeDimension", "MeasureDimension"]
    concept_id: str | None
    codelist_ref: MaintainableRef | None


# =============================================================================
# Dataclass types (for internal use with methods)
# =============================================================================


@dataclass
class SDMXCodelistRef:
    """Reference to an SDMX codelist with helper methods."""

    id: str
    agency: str
    version: str = "1.0"

    def to_dict(self) -> MaintainableRef:
        """Convert to TypedDict for serialization."""
        return {"id": self.id, "agency": self.agency, "version": self.version}

    @classmethod
    def from_dict(cls, data: MaintainableRef) -> SDMXCodelistRef:
        """Create from TypedDict."""
        return cls(id=data["id"], agency=data["agency"], version=data["version"])


# Type alias for dimension types
DimensionType = Literal["Dimension", "TimeDimension", "MeasureDimension"]


@dataclass
class SDMXDimension:
    """Parsed dimension with helper methods."""

    id: str
    position: int
    dim_type: DimensionType
    concept_id: str | None = None
    codelist_ref: SDMXCodelistRef | None = None
    required: bool = True

    def to_parsed_dict(self) -> ParsedDimension:
        """Convert to ParsedDimension TypedDict."""
        return {
            "id": self.id,
            "position": self.position,
            "type": self.dim_type,
            "concept_id": self.concept_id,
            "codelist_ref": self.codelist_ref.to_dict() if self.codelist_ref else None,
        }


@dataclass
class SDMXDataflow:
    """Parsed dataflow with helper methods."""

    id: str
    agency_id: str
    version: str
    name: str
    description: str = ""
    is_final: bool = False
    dsd_ref: SDMXCodelistRef | None = None

    def to_dict(self) -> ParsedDataflow:
        """Convert to ParsedDataflow TypedDict."""
        return {
            "id": self.id,
            "agency_id": self.agency_id,
            "version": self.version,
            "name": self.name,
            "description": self.description,
            "is_final": self.is_final,
            "dsd_ref": self.dsd_ref.to_dict() if self.dsd_ref else None,
        }


@dataclass
class SDMXAvailabilityResult:
    """Parsed availability constraint with helper methods."""

    dataflow_id: str
    has_constraint: bool
    constraint_id: str | None = None
    cube_regions: list[CubeRegionInfo] = field(default_factory=list)
    key_sets: list[dict[str, str]] = field(default_factory=list)
    time_range: TimeRangeInfo | None = None
    note: str | None = None
    error: str | None = None

    def to_dict(self) -> DataAvailabilityResponse:
        """Convert to appropriate response TypedDict."""
        if self.error:
            return {"dataflow_id": self.dataflow_id, "error": self.error}
        if not self.has_constraint:
            return {
                "dataflow_id": self.dataflow_id,
                "has_constraint": False,
                "note": self.note or "No availability constraint found",
            }
        return {
            "dataflow_id": self.dataflow_id,
            "has_constraint": True,
            "constraint_id": self.constraint_id or "",
            "cube_regions": self.cube_regions,
            "key_sets": self.key_sets,
            "time_range": self.time_range,
        }


# =============================================================================
# Type Aliases for clarity
# =============================================================================

# XML element text content is always str or None
XMLText = str | None

# SDMX dimension position (1-based in SDMX spec, but often 0-based in practice)
DimensionPosition = int

# SDMX key segment (a single dimension value, or '*' for wildcard)
KeySegment = str

# Full SDMX key (dimension values joined by '.')
SDMXKey = str

# SDMX time period format (ISO 8601 variants)
TimePeriod = str

# Agency identifier (e.g., "SPC", "ECB", "UNICEF")
AgencyID = str

# Artefact version (e.g., "1.0", "2.1", "latest")
ArtefactVersion = str
