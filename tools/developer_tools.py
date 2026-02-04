"""
SDMX Developer Tools - Tools for data developers preparing, validating, and monitoring SDMX data.

These tools support workflows such as:
- Validating codes before SDMX-CSV generation
- Finding where codes/indicators are used across dataflows
- Understanding semantic meanings of dimensions (concept schemes)
- Comparing structures for data migration
- Checking content constraints (allowed vs actual values)

See DEVELOPER_TOOLS_PROPOSAL.md for detailed specifications.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

import httpx
from mcp.server.fastmcp import Context

from utils import SDMX_NAMESPACES

logger = logging.getLogger(__name__)


# =============================================================================
# Type Definitions
# =============================================================================


@dataclass
class CodeValidationResult:
    """Result of validating a single code."""

    valid: bool
    codelist_id: str
    code_id: str
    code_name: str | None = None
    code_description: str | None = None
    parent_code: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, str | bool | None]:
        result: dict[str, str | bool | None] = {
            "valid": self.valid,
            "codelist_id": self.codelist_id,
            "code_id": self.code_id,
        }
        if self.valid:
            result["code_name"] = self.code_name
            result["code_description"] = self.code_description
            if self.parent_code:
                result["parent_code"] = self.parent_code
        if self.error:
            result["error"] = self.error
        return result


@dataclass
class ConceptInfo:
    """Information about a concept."""

    id: str
    name: str
    description: str | None = None
    core_representation: dict[str, str] | None = None

    def to_dict(self) -> dict[str, str | dict[str, str] | None]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "core_representation": self.core_representation,
        }


# =============================================================================
# Single Code Validation
# =============================================================================


async def validate_single_code(
    base_url: str,
    agency_id: str,
    codelist_id: str,
    code_id: str,
    version: str = "latest",
    ctx: Context[Any, Any, Any] | None = None,
) -> CodeValidationResult:
    """
    Validate that a single code exists in a codelist.

    Uses the SDMX 2.1 item-level query endpoint:
    GET /codelist/{agencyID}/{resourceID}/{version}/{itemID}

    This is more efficient than fetching the entire codelist when you just
    need to validate one code.

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID maintaining the codelist
        codelist_id: Codelist identifier
        code_id: Code to validate
        version: Codelist version (default: "latest")
        ctx: Optional MCP context for logging

    Returns:
        CodeValidationResult with validation status and code details if valid
    """
    if ctx:
        logger.info(f"Validating code '{code_id}' in codelist '{codelist_id}'...")

    # Build item-level query URL
    url = f"{base_url}/codelist/{agency_id}/{codelist_id}/{version}/{code_id}"

    try:
        async with httpx.AsyncClient(verify=True, timeout=30.0) as client:
            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )

            if response.status_code == 404:
                # Code doesn't exist - try to provide suggestions
                return CodeValidationResult(
                    valid=False,
                    codelist_id=codelist_id,
                    code_id=code_id,
                    error=f"Code '{code_id}' not found in codelist '{codelist_id}'",
                )

            if response.status_code == 501:
                # Item-level query not supported by this endpoint
                # Fall back to full codelist fetch
                if ctx:
                    logger.info("Item-level query not supported, falling back to full codelist...")
                return await _validate_code_via_full_codelist(
                    base_url, agency_id, codelist_id, code_id, version, ctx
                )

            _ = response.raise_for_status()

            # Parse the response
            root = ET.fromstring(response.text)

            # Find the code element
            code_elem = root.find(".//str:Code", SDMX_NAMESPACES)
            if code_elem is None:
                # Try without namespace prefix
                for elem in root.iter():
                    if elem.tag.endswith("Code"):
                        code_elem = elem
                        break

            if code_elem is None:
                return CodeValidationResult(
                    valid=False,
                    codelist_id=codelist_id,
                    code_id=code_id,
                    error="Unexpected response format - no Code element found",
                )

            # Extract code details
            code_name: str | None = None
            code_desc: str | None = None
            parent_code: str | None = None

            # Get name
            name_elem = code_elem.find(".//com:Name", SDMX_NAMESPACES)
            if name_elem is not None and name_elem.text:
                code_name = name_elem.text

            # Get description
            desc_elem = code_elem.find(".//com:Description", SDMX_NAMESPACES)
            if desc_elem is not None and desc_elem.text:
                code_desc = desc_elem.text

            # Get parent (if hierarchical)
            parent_elem = code_elem.find(".//str:Parent", SDMX_NAMESPACES)
            if parent_elem is not None:
                parent_ref = parent_elem.find(".//Ref", SDMX_NAMESPACES)
                if parent_ref is not None:
                    parent_code = parent_ref.get("id")

            return CodeValidationResult(
                valid=True,
                codelist_id=codelist_id,
                code_id=code_id,
                code_name=code_name,
                code_description=code_desc,
                parent_code=parent_code,
            )

    except httpx.HTTPStatusError as e:
        return CodeValidationResult(
            valid=False,
            codelist_id=codelist_id,
            code_id=code_id,
            error=f"HTTP error {e.response.status_code}: {str(e)[:200]}",
        )
    except Exception as e:
        logger.exception(f"Error validating code {code_id}")
        return CodeValidationResult(
            valid=False,
            codelist_id=codelist_id,
            code_id=code_id,
            error=f"Validation error: {str(e)}",
        )


async def _validate_code_via_full_codelist(
    base_url: str,
    agency_id: str,
    codelist_id: str,
    code_id: str,
    version: str,
    ctx: Context[Any, Any, Any] | None = None,
) -> CodeValidationResult:
    """
    Fallback validation by fetching the full codelist.

    Used when the endpoint doesn't support item-level queries (returns 501).
    """
    url = f"{base_url}/codelist/{agency_id}/{codelist_id}/{version}"

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )
            _ = response.raise_for_status()

            root = ET.fromstring(response.text)

            # Search for the specific code
            for code_elem in root.iter():
                if code_elem.tag.endswith("Code") and code_elem.get("id") == code_id:
                    code_name: str | None = None
                    code_desc: str | None = None

                    name_elem = code_elem.find(".//com:Name", SDMX_NAMESPACES)
                    if name_elem is not None and name_elem.text:
                        code_name = name_elem.text

                    desc_elem = code_elem.find(".//com:Description", SDMX_NAMESPACES)
                    if desc_elem is not None and desc_elem.text:
                        code_desc = desc_elem.text

                    return CodeValidationResult(
                        valid=True,
                        codelist_id=codelist_id,
                        code_id=code_id,
                        code_name=code_name,
                        code_description=code_desc,
                    )

            # Code not found
            return CodeValidationResult(
                valid=False,
                codelist_id=codelist_id,
                code_id=code_id,
                error=f"Code '{code_id}' not found in codelist '{codelist_id}'",
            )

    except Exception as e:
        return CodeValidationResult(
            valid=False,
            codelist_id=codelist_id,
            code_id=code_id,
            error=f"Fallback validation error: {str(e)}",
        )


# =============================================================================
# Concept Scheme Browser
# =============================================================================


async def get_concept_scheme(
    base_url: str,
    agency_id: str,
    scheme_id: str = "all",
    version: str = "latest",
    search_term: str | None = None,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Retrieve a concept scheme to understand semantic definitions.

    Concepts define the meaning of dimensions and attributes in SDMX.
    This helps data developers understand what each field represents.

    Endpoint: GET /conceptscheme/{agencyID}/{resourceID}/{version}

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        scheme_id: Concept scheme ID (or "all" for all schemes)
        version: Version (default: "latest")
        search_term: Optional filter for concept names/descriptions
        ctx: Optional MCP context

    Returns:
        Dict with concept scheme information and concepts list
    """
    if ctx:
        logger.info(f"Retrieving concept scheme '{scheme_id}'...")

    url = f"{base_url}/conceptscheme/{agency_id}/{scheme_id}/{version}"

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )
            _ = response.raise_for_status()

            root = ET.fromstring(response.text)

            schemes: list[dict[str, Any]] = []

            # Find all concept schemes
            for scheme_elem in root.iter():
                if not scheme_elem.tag.endswith("ConceptScheme"):
                    continue

                scheme_info: dict[str, Any] = {
                    "id": scheme_elem.get("id", ""),
                    "agency_id": scheme_elem.get("agencyID", agency_id),
                    "version": scheme_elem.get("version", "1.0"),
                    "name": "",
                    "description": "",
                    "concepts": [],
                }

                # Get scheme name
                name_elem = scheme_elem.find(".//com:Name", SDMX_NAMESPACES)
                if name_elem is not None and name_elem.text:
                    scheme_info["name"] = name_elem.text

                # Get scheme description
                desc_elem = scheme_elem.find(".//com:Description", SDMX_NAMESPACES)
                if desc_elem is not None and desc_elem.text:
                    scheme_info["description"] = desc_elem.text

                # Extract concepts
                concepts: list[dict[str, Any]] = []
                for concept_elem in scheme_elem.iter():
                    if not concept_elem.tag.endswith("Concept"):
                        continue

                    concept_id = concept_elem.get("id", "")
                    concept_name = ""
                    concept_desc = ""
                    core_rep: dict[str, str] | None = None

                    # Get concept name
                    c_name_elem = concept_elem.find(".//com:Name", SDMX_NAMESPACES)
                    if c_name_elem is not None and c_name_elem.text:
                        concept_name = c_name_elem.text

                    # Get concept description
                    c_desc_elem = concept_elem.find(".//com:Description", SDMX_NAMESPACES)
                    if c_desc_elem is not None and c_desc_elem.text:
                        concept_desc = c_desc_elem.text

                    # Get core representation (if any)
                    core_elem = concept_elem.find(".//str:CoreRepresentation", SDMX_NAMESPACES)
                    if core_elem is not None:
                        core_rep = {}
                        # Check for codelist reference
                        enum_elem = core_elem.find(".//str:Enumeration", SDMX_NAMESPACES)
                        if enum_elem is not None:
                            ref = enum_elem.find(".//Ref", SDMX_NAMESPACES)
                            if ref is not None:
                                core_rep["codelist_id"] = ref.get("id", "")
                                core_rep["codelist_agency"] = ref.get("agencyID", agency_id)

                        # Check for text format
                        text_elem = core_elem.find(".//str:TextFormat", SDMX_NAMESPACES)
                        if text_elem is not None:
                            text_type = text_elem.get("textType", "")
                            if text_type:
                                core_rep["text_type"] = text_type

                    # Apply search filter if provided
                    if search_term:
                        search_lower = search_term.lower()
                        if (
                            search_lower not in concept_id.lower()
                            and search_lower not in concept_name.lower()
                            and search_lower not in concept_desc.lower()
                        ):
                            continue

                    concepts.append(
                        {
                            "id": concept_id,
                            "name": concept_name,
                            "description": concept_desc,
                            "core_representation": core_rep,
                        }
                    )

                scheme_info["concepts"] = concepts
                scheme_info["total_concepts"] = len(concepts)
                schemes.append(scheme_info)

            return {
                "request": {
                    "scheme_id": scheme_id,
                    "agency_id": agency_id,
                    "search_term": search_term,
                },
                "schemes": schemes,
                "total_schemes": len(schemes),
            }

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error getting concept scheme: {e}")
        return {
            "error": f"HTTP error {e.response.status_code}: {str(e)[:200]}",
            "schemes": [],
        }
    except Exception as e:
        logger.exception("Error getting concept scheme")
        return {"error": str(e), "schemes": []}


# =============================================================================
# Content Constraints (Allowed vs Actual)
# =============================================================================


async def get_content_constraints(
    base_url: str,
    agency_id: str,
    dataflow_id: str,
    constraint_type: str = "both",
    version: str = "latest",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Get content constraints for a dataflow.

    Shows what values are ALLOWED (permitted by the constraint) vs
    what values are ACTUAL (actually have data).

    This is essential for data developers to know:
    - What codes they CAN submit
    - What codes already have data
    - Gaps in coverage

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        dataflow_id: Dataflow identifier
        constraint_type: "allowed" | "actual" | "both"
        version: Version (default: "latest")
        ctx: Optional MCP context

    Returns:
        Dict with constraint information
    """
    if ctx:
        logger.info(f"Getting {constraint_type} constraints for dataflow '{dataflow_id}'...")

    result: dict[str, Any] = {
        "dataflow_id": dataflow_id,
        "agency_id": agency_id,
        "constraint_type": constraint_type,
    }

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            # First, get the dataflow with references to find constraints
            df_url = f"{base_url}/dataflow/{agency_id}/{dataflow_id}/{version}?references=all"
            response = await client.get(
                df_url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )
            _ = response.raise_for_status()

            root = ET.fromstring(response.text)

            # Parse constraints
            allowed_constraint: dict[str, Any] | None = None
            actual_constraint: dict[str, Any] | None = None

            for constraint in root.iter():
                if not constraint.tag.endswith("ContentConstraint"):
                    continue

                constraint_type_attr = constraint.get("type", "").lower()
                constraint_info = _parse_constraint(constraint)

                if constraint_type_attr == "allowed":
                    allowed_constraint = constraint_info
                elif constraint_type_attr == "actual":
                    actual_constraint = constraint_info
                else:
                    # Default behavior - check include attribute
                    # If it defines what's included, treat as actual
                    if constraint_info.get("cube_regions"):
                        actual_constraint = constraint_info

            if constraint_type in ("allowed", "both") and allowed_constraint:
                result["allowed_constraint"] = allowed_constraint

            if constraint_type in ("actual", "both") and actual_constraint:
                result["actual_constraint"] = actual_constraint

            # Calculate gaps if we have both
            if constraint_type == "both" and allowed_constraint and actual_constraint:
                result["gaps"] = _calculate_constraint_gaps(allowed_constraint, actual_constraint)

            if not allowed_constraint and not actual_constraint:
                result["note"] = "No content constraints found for this dataflow"

            return result

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error getting constraints: {e}")
        return {
            **result,
            "error": f"HTTP error {e.response.status_code}: {str(e)[:200]}",
        }
    except Exception as e:
        logger.exception("Error getting constraints")
        return {**result, "error": str(e)}


def _parse_constraint(constraint_elem: ET.Element) -> dict[str, Any]:
    """Parse a ContentConstraint element into a dict."""
    result: dict[str, Any] = {
        "constraint_id": constraint_elem.get("id", ""),
        "dimensions": {},
        "time_range": None,
        "cube_regions": [],
    }

    # Parse CubeRegions
    for cube_region in constraint_elem.iter():
        if not cube_region.tag.endswith("CubeRegion"):
            continue

        region_info: dict[str, Any] = {
            "included": cube_region.get("include", "true").lower() == "true",
            "keys": {},
        }

        for key_value in cube_region.iter():
            if not key_value.tag.endswith("KeyValue"):
                continue

            dim_id = key_value.get("id", "")
            values: list[str] = []

            for value in key_value.iter():
                if value.tag.endswith("Value") and value.text:
                    values.append(value.text)

            if dim_id and values:
                region_info["keys"][dim_id] = values
                # Also aggregate to top-level dimensions
                if dim_id not in result["dimensions"]:
                    result["dimensions"][dim_id] = set()
                result["dimensions"][dim_id].update(values)

        # Check for time range
        for attr_value in cube_region.iter():
            if not attr_value.tag.endswith("AttributeValue"):
                continue
            if attr_value.get("id") == "TIME_PERIOD":
                time_values: list[str] = []
                for value in attr_value.iter():
                    if value.tag.endswith("Value") and value.text:
                        time_values.append(value.text)
                if time_values:
                    result["time_range"] = {
                        "start": min(time_values),
                        "end": max(time_values),
                    }

        result["cube_regions"].append(region_info)

    # Convert sets to lists for JSON serialization
    dims = result.get("dimensions", {})
    result["dimensions"] = {k: list(v) for k, v in dims.items()}

    return result


def _calculate_constraint_gaps(allowed: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    """Calculate the gaps between allowed and actual constraints."""
    gaps: dict[str, Any] = {"unused_codes": {}, "dimension_coverage": {}}

    allowed_dims = allowed.get("dimensions", {})
    actual_dims = actual.get("dimensions", {})

    for dim_id, allowed_codes in allowed_dims.items():
        actual_codes = set(actual_dims.get(dim_id, []))
        allowed_set = set(allowed_codes)

        unused = allowed_set - actual_codes
        if unused:
            gaps["unused_codes"][dim_id] = list(unused)

        # Calculate coverage percentage
        if allowed_set:
            coverage = len(actual_codes & allowed_set) / len(allowed_set) * 100
            gaps["dimension_coverage"][dim_id] = round(coverage, 1)

    return gaps


# =============================================================================
# Structure References (Find Related Structures)
# =============================================================================


async def get_structure_references(
    base_url: str,
    agency_id: str,
    structure_type: str,
    structure_id: str,
    direction: str = "both",
    version: str = "latest",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Find structures that reference or are referenced by a given structure.

    Uses the powerful `references` parameter to discover:
    - Parents: What structures USE this one
    - Children: What structures this one USES
    - Both: Complete picture of relationships

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        structure_type: Type of structure ("dataflow", "dsd", "codelist", "conceptscheme")
        structure_id: Structure identifier
        direction: "parents" | "children" | "both"
        version: Version (default: "latest")
        ctx: Optional MCP context

    Returns:
        Dict with parent and/or child structures
    """
    if ctx:
        logger.info(f"Finding {direction} references for {structure_type} '{structure_id}'...")

    # Map structure types to endpoint paths
    endpoint_map = {
        "dataflow": "dataflow",
        "dsd": "datastructure",
        "datastructure": "datastructure",
        "codelist": "codelist",
        "conceptscheme": "conceptscheme",
        "categoryscheme": "categoryscheme",
    }

    endpoint = endpoint_map.get(structure_type.lower())
    if not endpoint:
        return {
            "error": f"Unknown structure type: {structure_type}. "
            f"Supported: {list(endpoint_map.keys())}"
        }

    result: dict[str, Any] = {
        "structure": {
            "type": structure_type,
            "id": structure_id,
            "agency_id": agency_id,
        },
    }

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            # Get parents (what uses this structure)
            if direction in ("parents", "both"):
                parents_url = (
                    f"{base_url}/{endpoint}/{agency_id}/{structure_id}/{version}"
                    + "?references=parents&detail=allstubs"
                )
                try:
                    response = await client.get(
                        parents_url,
                        headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
                    )
                    _ = response.raise_for_status()
                    result["parents"] = _parse_structure_references(
                        response.text, structure_id, "parents"
                    )
                except httpx.HTTPStatusError as e:
                    result["parents"] = {"error": f"HTTP {e.response.status_code}"}

            # Get children (what this structure uses)
            if direction in ("children", "both"):
                children_url = (
                    f"{base_url}/{endpoint}/{agency_id}/{structure_id}/{version}"
                    + "?references=children&detail=allstubs"
                )
                try:
                    response = await client.get(
                        children_url,
                        headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
                    )
                    _ = response.raise_for_status()
                    result["children"] = _parse_structure_references(
                        response.text, structure_id, "children"
                    )
                except httpx.HTTPStatusError as e:
                    result["children"] = {"error": f"HTTP {e.response.status_code}"}

            return result

    except Exception as e:
        logger.exception("Error getting structure references")
        return {**result, "error": str(e)}


def _parse_structure_references(
    xml_text: str, exclude_id: str, _direction: str
) -> list[dict[str, str]]:
    """Parse structure references from XML response."""
    root = ET.fromstring(xml_text)
    references: list[dict[str, str]] = []

    # Structure type mappings for human-readable output
    type_names = {
        "Dataflow": "dataflow",
        "DataStructure": "dsd",
        "Codelist": "codelist",
        "ConceptScheme": "conceptscheme",
        "CategoryScheme": "categoryscheme",
        "Categorisation": "categorisation",
        "ContentConstraint": "constraint",
    }

    for elem in root.iter():
        # Skip the structure we queried for
        if elem.get("id") == exclude_id:
            continue

        # Check if this is a maintainable artefact
        tag_local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if tag_local not in type_names:
            continue

        ref_info: dict[str, str] = {
            "type": type_names[tag_local],
            "id": elem.get("id", ""),
            "agency_id": elem.get("agencyID", ""),
            "version": elem.get("version", ""),
        }

        # Get name if available
        name_elem = elem.find(".//com:Name", SDMX_NAMESPACES)
        if name_elem is not None and name_elem.text:
            ref_info["name"] = name_elem.text

        if ref_info["id"]:  # Only add if we have an ID
            references.append(ref_info)

    return references


# =============================================================================
# Category Scheme Browser
# =============================================================================


async def browse_category_scheme(
    base_url: str,
    agency_id: str,
    scheme_id: str = "all",
    version: str = "latest",
    include_dataflows: bool = False,
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Browse category schemes to discover dataflows by topic.

    Category schemes provide a hierarchical taxonomy for organizing
    dataflows by subject area.

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        scheme_id: Category scheme ID (or "all")
        version: Version (default: "latest")
        include_dataflows: Whether to fetch categorisations linking to dataflows
        ctx: Optional MCP context

    Returns:
        Dict with category hierarchy
    """
    if ctx:
        logger.info(f"Browsing category scheme '{scheme_id}'...")

    result: dict[str, Any] = {
        "request": {
            "scheme_id": scheme_id,
            "agency_id": agency_id,
            "include_dataflows": include_dataflows,
        },
        "schemes": [],
    }

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            # Get category scheme
            url = f"{base_url}/categoryscheme/{agency_id}/{scheme_id}/{version}"
            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )
            _ = response.raise_for_status()

            root = ET.fromstring(response.text)

            # Parse category schemes
            for scheme_elem in root.iter():
                if not scheme_elem.tag.endswith("CategoryScheme"):
                    continue

                scheme_info: dict[str, Any] = {
                    "id": scheme_elem.get("id", ""),
                    "agency_id": scheme_elem.get("agencyID", agency_id),
                    "name": "",
                    "categories": [],
                }

                # Get scheme name
                name_elem = scheme_elem.find(".//com:Name", SDMX_NAMESPACES)
                if name_elem is not None and name_elem.text:
                    scheme_info["name"] = name_elem.text

                # Parse categories (can be nested)
                scheme_info["categories"] = _parse_categories(scheme_elem)
                result["schemes"].append(scheme_info)

            result["total_schemes"] = len(result["schemes"])

            # Optionally get categorisations
            if include_dataflows:
                cat_url = f"{base_url}/categorisation/{agency_id}/all/{version}"
                try:
                    cat_response = await client.get(
                        cat_url,
                        headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
                    )
                    cat_response.raise_for_status()
                    categorisations = _parse_categorisations(cat_response.text)
                    result["categorisations"] = categorisations
                except httpx.HTTPStatusError:
                    result["categorisations_note"] = "Could not fetch categorisations"

            return result

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error browsing categories: {e}")
        return {**result, "error": f"HTTP error {e.response.status_code}"}
    except Exception as e:
        logger.exception("Error browsing categories")
        return {**result, "error": str(e)}


def _parse_categories(parent_elem: ET.Element, depth: int = 0) -> list[dict[str, Any]]:
    """Recursively parse categories from a parent element."""
    categories: list[dict[str, Any]] = []

    for elem in parent_elem:
        if not elem.tag.endswith("Category"):
            continue

        cat_info: dict[str, Any] = {
            "id": elem.get("id", ""),
            "level": depth,
            "name": "",
            "description": "",
            "children": [],
        }

        # Get name
        name_elem = elem.find(".//com:Name", SDMX_NAMESPACES)
        if name_elem is not None and name_elem.text:
            cat_info["name"] = name_elem.text

        # Get description
        desc_elem = elem.find(".//com:Description", SDMX_NAMESPACES)
        if desc_elem is not None and desc_elem.text:
            cat_info["description"] = desc_elem.text

        # Recursively parse children
        cat_info["children"] = _parse_categories(elem, depth + 1)
        cat_info["has_children"] = len(cat_info["children"]) > 0

        categories.append(cat_info)

    return categories


def _parse_categorisations(xml_text: str) -> list[dict[str, str]]:
    """Parse categorisation elements linking categories to dataflows."""
    root = ET.fromstring(xml_text)
    categorisations: list[dict[str, str]] = []

    for elem in root.iter():
        if not elem.tag.endswith("Categorisation"):
            continue

        cat_info: dict[str, str] = {
            "id": elem.get("id", ""),
            "category_id": "",
            "category_scheme": "",
            "dataflow_id": "",
            "dataflow_agency": "",
        }

        # Get source (dataflow reference)
        source = elem.find(".//str:Source", SDMX_NAMESPACES)
        if source is not None:
            ref = source.find(".//Ref", SDMX_NAMESPACES)
            if ref is not None:
                cat_info["dataflow_id"] = ref.get("id", "")
                cat_info["dataflow_agency"] = ref.get("agencyID", "")

        # Get target (category reference)
        target = elem.find(".//str:Target", SDMX_NAMESPACES)
        if target is not None:
            ref = target.find(".//Ref", SDMX_NAMESPACES)
            if ref is not None:
                cat_info["category_id"] = ref.get("id", "")
                cat_info["category_scheme"] = ref.get("maintainableParentID", "")

        if cat_info["dataflow_id"] and cat_info["category_id"]:
            categorisations.append(cat_info)

    return categorisations


# =============================================================================
# Data Update Tracker
# =============================================================================


async def check_data_updates(
    base_url: str,
    agency_id: str,
    dataflow_id: str,
    since: str,
    key: str = "all",
    _version: str = "latest",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Check if data has been updated since a specific timestamp.

    Uses the `updatedAfter` parameter to efficiently check for changes
    without downloading all data.

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        dataflow_id: Dataflow identifier
        since: ISO 8601 timestamp (e.g., "2024-01-01T00:00:00Z")
        key: Optional key filter (default: "all")
        version: Version (default: "latest")
        ctx: Optional MCP context

    Returns:
        Dict indicating whether updates exist and summary info
    """
    if ctx:
        logger.info(f"Checking for updates to '{dataflow_id}' since {since}...")

    result: dict[str, Any] = {
        "dataflow_id": dataflow_id,
        "since": since,
        "key": key,
        "has_updates": False,
    }

    try:
        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            # Use serieskeysonly to minimize data transfer
            url = (
                f"{base_url}/data/{dataflow_id}/{key}/{agency_id}"
                f"?updatedAfter={since}&detail=serieskeysonly"
            )

            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"},
            )

            if response.status_code == 304:
                # No changes since timestamp
                result["has_updates"] = False
                result["note"] = "No changes since specified timestamp"
                return result

            if response.status_code == 404:
                result["error"] = "Dataflow not found or no data available"
                return result

            _ = response.raise_for_status()

            # Parse response to count updated series
            root = ET.fromstring(response.text)

            series_count = 0
            updated_keys: list[str] = []

            for elem in root.iter():
                if elem.tag.endswith("Series"):
                    series_count += 1
                    # Try to extract the series key
                    key_values: list[str] = []
                    for key_elem in elem.iter():
                        if key_elem.tag.endswith("Value"):
                            key_values.append(key_elem.get("value", ""))
                    if key_values:
                        updated_keys.append(".".join(key_values))

            result["has_updates"] = series_count > 0
            result["updated_series_count"] = series_count
            if updated_keys and len(updated_keys) <= 20:
                result["updated_keys"] = updated_keys
            elif updated_keys:
                result["updated_keys_sample"] = updated_keys[:20]
                result["note"] = f"Showing first 20 of {len(updated_keys)} updated series"

            return result

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 501:
            result["error"] = "updatedAfter parameter not supported by this endpoint"
        else:
            result["error"] = f"HTTP error {e.response.status_code}"
        return result
    except Exception as e:
        logger.exception("Error checking for updates")
        return {**result, "error": str(e)}


# =============================================================================
# Utility: Batch Code Validation
# =============================================================================


async def validate_codes_batch(
    base_url: str,
    agency_id: str,
    codelist_id: str,
    codes: list[str],
    version: str = "latest",
    ctx: Context[Any, Any, Any] | None = None,
) -> dict[str, Any]:
    """
    Validate multiple codes against a codelist in one operation.

    More efficient than validating one at a time when preparing
    SDMX-CSV files.

    Args:
        base_url: SDMX endpoint base URL
        agency_id: Agency ID
        codelist_id: Codelist identifier
        codes: List of codes to validate
        version: Version (default: "latest")
        ctx: Optional MCP context

    Returns:
        Dict with valid and invalid codes
    """
    if ctx:
        logger.info(f"Validating {len(codes)} codes against '{codelist_id}'...")

    valid_codes: list[dict[str, str]] = []
    invalid_codes: list[str] = []

    result: dict[str, Any] = {
        "codelist_id": codelist_id,
        "total_checked": len(codes),
        "valid_codes": valid_codes,
        "invalid_codes": invalid_codes,
    }

    try:
        # Fetch the full codelist once
        url = f"{base_url}/codelist/{agency_id}/{codelist_id}/{version}"

        async with httpx.AsyncClient(verify=True, timeout=60.0) as client:
            response = await client.get(
                url,
                headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
            )
            _ = response.raise_for_status()

            root = ET.fromstring(response.text)

            # Build set of valid codes
            valid_code_set: set[str] = set()
            code_names: dict[str, str] = {}

            for elem in root.iter():
                if elem.tag.endswith("Code"):
                    code_id = elem.get("id", "")
                    if code_id:
                        valid_code_set.add(code_id)
                        # Get name
                        name_elem = elem.find(".//com:Name", SDMX_NAMESPACES)
                        if name_elem is not None and name_elem.text:
                            code_names[code_id] = name_elem.text

            # Check each code
            for code in codes:
                if code in valid_code_set:
                    valid_codes.append(
                        {
                            "code": code,
                            "name": code_names.get(code, ""),
                        }
                    )
                else:
                    invalid_codes.append(code)

            result["valid_count"] = len(valid_codes)
            result["invalid_count"] = len(invalid_codes)

            return result

    except httpx.HTTPStatusError as e:
        return {**result, "error": f"HTTP error {e.response.status_code}"}
    except Exception as e:
        logger.exception("Error validating codes batch")
        return {**result, "error": str(e)}
