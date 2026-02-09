"""
Enhanced SDMX REST API client with progressive discovery capabilities.

This client provides a layered approach to SDMX metadata discovery:
1. High-level overview (minimal data)
2. Structure summary (dimensions and their order)
3. Detailed drill-down (specific codelists, constraints)
"""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from enum import Enum
from typing import Any

import certifi
import httpx
from mcp.server.fastmcp import Context

from config import SDMX_AGENCY_ID, SDMX_BASE_URL
from models.sdmx_types import (
    AttributeInfo,
    MaintainableRef,
)
from utils import SDMX_NAMESPACES

logger = logging.getLogger(__name__)


class DetailLevel(Enum):
    """Level of detail for metadata retrieval."""

    OVERVIEW = "overview"  # Just names and descriptions
    STRUCTURE = "structure"  # Add dimensions and their order
    FULL = "full"  # Include all codes and constraints


@dataclass
class DataflowOverview:
    """Lightweight dataflow information."""

    id: str
    agency: str
    version: str
    name: str
    description: str
    dsd_ref: MaintainableRef | None = None

    def to_dict(self) -> dict[str, str | MaintainableRef | None]:
        return {
            "id": self.id,
            "agency": self.agency,
            "version": self.version,
            "name": self.name,
            "description": self.description,
            "dsd_reference": self.dsd_ref,
        }


@dataclass
class DimensionInfo:
    """Information about a dimension."""

    id: str
    position: int
    type: str  # "Dimension", "TimeDimension", "MeasureDimension"
    concept: str | None = None
    codelist_ref: MaintainableRef | None = None
    required: bool = True

    def to_dict(self) -> dict[str, str | int | bool | MaintainableRef | None]:
        return {
            "id": self.id,
            "position": self.position,
            "type": self.type,
            "concept": self.concept,
            "codelist_ref": self.codelist_ref,
            "required": self.required,
        }


@dataclass
class DataStructureSummary:
    """Summary of data structure without full codelist details."""

    id: str
    agency: str
    version: str
    dimensions: list[DimensionInfo]
    key_family: list[str]  # Ordered list of dimension IDs for key construction
    attributes: list[AttributeInfo]
    primary_measure: str | None = None

    def to_dict(
        self,
    ) -> dict[str, Any]:
        return {
            "id": self.id,
            "agency": self.agency,
            "version": self.version,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "key_family": self.key_family,
            "attributes": self.attributes,
            "primary_measure": self.primary_measure,
            "key_template": ".".join([f"{{{d}}}" for d in self.key_family]),
            "example_key": ".".join(["" for _ in self.key_family]),
        }


class SDMXProgressiveClient:
    """SDMX client with progressive discovery capabilities."""

    base_url: str
    agency_id: str
    session: httpx.AsyncClient | None
    _cache: dict[str, Any]
    version_cache: dict[tuple[str, str], str]

    def __init__(self, base_url: str | None = None, agency_id: str | None = None):
        self.base_url = (base_url or SDMX_BASE_URL).rstrip("/")
        self.agency_id = agency_id or SDMX_AGENCY_ID
        self.session = None
        self._cache = {}  # Simple cache for repeated requests
        # Cache for dataflow versions to avoid repeated lookups
        # Format: {(agency_id, dataflow_id): version}
        self.version_cache = {}

    async def _get_session(self) -> httpx.AsyncClient:
        """Get or create HTTP session with proper SSL certificate verification."""
        if self.session is None:
            # Use certifi's certificate bundle directly
            # This works better cross-platform (Windows/Linux/macOS)
            self.session = httpx.AsyncClient(
                timeout=30.0,
                verify=certifi.where(),  # Pass cert file path directly
            )
        return self.session

    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.aclose()
            self.session = None

    async def resolve_version(
        self,
        dataflow_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> str:
        """
        Resolve a version string to actual version number.
        Returns the version as-is if not "latest", otherwise fetches and caches the actual version.

        Args:
            dataflow_id: The dataflow ID
            agency_id: The agency ID
            version: The version string (could be "latest" or a specific version)
            ctx: Optional MCP context

        Returns:
            The resolved version number

        Raises:
            ValueError: If version cannot be resolved
        """
        # If not "latest", return as-is
        if version != "latest":
            return version

        agency_id = agency_id or self.agency_id
        cache_key = (agency_id, dataflow_id)

        # Check cache first
        if cache_key in self.version_cache:
            if ctx:
                ctx.info(f"Using cached version for {dataflow_id}: {self.version_cache[cache_key]}")
            return self.version_cache[cache_key]

        # Fetch the actual version
        if ctx:
            ctx.info(f"Resolving 'latest' version for {dataflow_id}...")

        session = await self._get_session()
        url = f"{self.base_url}/dataflow/{agency_id}/{dataflow_id}/latest"

        try:
            response = await session.get(url)
            if response.status_code != 200:
                raise ValueError(f"Failed to fetch dataflow metadata: HTTP {response.status_code}")

            # Parse to get the actual version
            root = ET.fromstring(response.text)
            actual_version = None

            # Find the dataflow element and extract version
            for df_elem in root.findall(".//str:Dataflow", SDMX_NAMESPACES):
                actual_version = df_elem.get("version")
                if actual_version:
                    break

            if not actual_version:
                raise ValueError(f"Could not extract version from dataflow response")

            # Cache the result
            self.version_cache[cache_key] = actual_version

            if ctx:
                ctx.info(f"Resolved 'latest' to version {actual_version}")

            return actual_version

        except httpx.RequestError as e:
            raise ValueError(f"Network error resolving version: {e}")
        except ET.ParseError as e:
            raise ValueError(f"Error parsing dataflow response: {e}")

    async def get_dataflow_overview(
        self,
        dataflow_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> DataflowOverview:
        """
        Get lightweight dataflow overview without references.
        This is the first level of discovery.
        """
        agency = agency_id or self.agency_id
        cache_key = f"df_overview_{agency}_{dataflow_id}_{version}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        url = f"{self.base_url}/dataflow/{agency}/{dataflow_id}/{version}?references=none"

        if ctx:
            ctx.info(f"Getting dataflow overview for {dataflow_id}...")

        try:
            session = await self._get_session()
            response = await session.get(
                url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}
            )
            response.raise_for_status()

            root = ET.fromstring(response.content)
            df_elem = root.find(f'.//str:Dataflow[@id="{dataflow_id}"]', SDMX_NAMESPACES)

            if df_elem is None:
                df_elem = root.find(".//str:Dataflow", SDMX_NAMESPACES)

            if df_elem is not None:
                name_elem = df_elem.find("./com:Name", SDMX_NAMESPACES)
                desc_elem = df_elem.find("./com:Description", SDMX_NAMESPACES)

                # Get DSD reference
                dsd_ref: MaintainableRef | None = None
                struct_ref = df_elem.find(".//str:Structure/Ref", SDMX_NAMESPACES)
                if struct_ref is not None:
                    dsd_ref = {
                        "id": struct_ref.get("id", ""),
                        "agency": struct_ref.get("agencyID", agency),
                        "version": struct_ref.get("version", version),
                    }

                overview = DataflowOverview(
                    id=df_elem.get("id", dataflow_id),
                    agency=df_elem.get("agencyID", agency),
                    version=df_elem.get("version", version),
                    name=name_elem.text
                    if name_elem is not None and name_elem.text
                    else df_elem.get("id", dataflow_id),
                    description=desc_elem.text if desc_elem is not None and desc_elem.text else "",
                    dsd_ref=dsd_ref,
                )

                self._cache[cache_key] = overview
                return overview

            raise ValueError(f"No dataflow element found for {dataflow_id}")

        except Exception as e:
            logger.error(f"Failed to get dataflow overview: {e}")
            raise

    async def get_structure_summary(
        self,
        dataflow_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> DataStructureSummary:
        """
        Get data structure summary with dimensions and their order.
        This is the second level of discovery.
        """
        agency = agency_id or self.agency_id

        # First get the dataflow overview to find DSD reference
        overview = await self.get_dataflow_overview(dataflow_id, agency, version, ctx)

        if not overview.dsd_ref:
            raise ValueError(f"No DSD reference found for dataflow {dataflow_id}")

        cache_key = f"dsd_summary_{overview.dsd_ref['agency']}_{overview.dsd_ref['id']}_{overview.dsd_ref['version']}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        # Fetch DSD with codelist references
        # Use references=children to get codelists referenced by the DSD
        # Also use detail=full to get concept scheme information for IMF-style references
        dsd_url = f"{self.base_url}/datastructure/{overview.dsd_ref['agency']}/{overview.dsd_ref['id']}/{overview.dsd_ref['version']}?references=children&detail=full"

        if ctx:
            ctx.info(f"Getting structure summary for {overview.dsd_ref['id']}...")

        try:
            session = await self._get_session()
            response = await session.get(
                dsd_url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}
            )
            response.raise_for_status()

            root = ET.fromstring(response.content)
            dsd_elem = root.find(".//str:DataStructure", SDMX_NAMESPACES)

            if not dsd_elem:
                raise ValueError("No DataStructure found in response")

            # Build a concept->codelist mapping from ConceptSchemes in the response
            # This handles IMF-style references where dimensions use ConceptIdentity
            concept_to_codelist = {}
            for concept_scheme in root.findall(".//str:ConceptScheme", SDMX_NAMESPACES):
                for concept in concept_scheme.findall(".//str:Concept", SDMX_NAMESPACES):
                    concept_id = concept.get("id")
                    # Look for CoreRepresentation/Enumeration/Ref
                    # Try with namespace prefix first
                    cl_ref = concept.find(
                        ".//str:CoreRepresentation/str:Enumeration/Ref", SDMX_NAMESPACES
                    )
                    if not cl_ref:
                        cl_ref = concept.find(
                            ".//str:CoreRepresentation/str:Enumeration/com:Ref", SDMX_NAMESPACES
                        )
                    # IMF uses unprefixed Ref elements, so search for any Ref element
                    if not cl_ref:
                        # Search for unprefixed Ref that is a Codelist
                        for elem in concept.iter():
                            if elem.tag.endswith("Ref") and elem.get("class") == "Codelist":
                                cl_ref = elem
                                break

                    if cl_ref is not None and concept_id:
                        concept_to_codelist[concept_id] = {
                            "id": cl_ref.get("id"),
                            "agency": cl_ref.get("agencyID"),
                            "version": cl_ref.get("version", "1.0"),
                        }

            dimensions: list[DimensionInfo] = []
            key_family: list[str] = []

            # Parse dimensions in order
            dim_list = dsd_elem.find(".//str:DimensionList", SDMX_NAMESPACES)
            if dim_list:
                # Regular dimensions
                for dim in dim_list.findall(".//str:Dimension", SDMX_NAMESPACES):
                    position = int(dim.get("position", "0"))
                    dim_id = dim.get("id", "")
                    concept_id: str | None = None

                    # Get codelist reference - support two SDMX patterns:
                    # Pattern 1: Direct LocalRepresentation/Enumeration/Ref (SPC, ECB, UNICEF)
                    codelist_ref: MaintainableRef | None = None
                    cl_ref = dim.find(
                        ".//str:LocalRepresentation/str:Enumeration/Ref", SDMX_NAMESPACES
                    )
                    if cl_ref is None:
                        cl_ref = dim.find(
                            ".//str:LocalRepresentation/str:Enumeration/com:Ref", SDMX_NAMESPACES
                        )

                    if cl_ref is not None:
                        # Found direct enumeration reference
                        codelist_ref = {
                            "id": cl_ref.get("id", ""),
                            "agency": cl_ref.get("agencyID", agency),
                            "version": cl_ref.get("version", "1.0"),
                        }
                    else:
                        # Pattern 2: ConceptIdentity reference (IMF style)
                        # Look up the concept in our concept->codelist mapping
                        concept_ref = dim.find(".//str:ConceptIdentity/Ref", SDMX_NAMESPACES)
                        if concept_ref is not None:
                            concept_id = concept_ref.get("id")
                            if concept_id in concept_to_codelist:
                                codelist_ref = concept_to_codelist[concept_id]

                    dim_info = DimensionInfo(
                        id=dim_id,
                        position=position,
                        type="Dimension",
                        concept=concept_id,
                        codelist_ref=codelist_ref,
                    )
                    dimensions.append(dim_info)

                # Time dimension (usually last)
                time_dim = dim_list.find(".//str:TimeDimension", SDMX_NAMESPACES)
                if time_dim is not None:
                    dim_info = DimensionInfo(
                        id=time_dim.get("id", "TIME_PERIOD"),
                        position=int(time_dim.get("position", "999")),
                        type="TimeDimension",
                    )
                    dimensions.append(dim_info)

            # Sort dimensions by position to get correct key order
            dimensions.sort(key=lambda d: d.position)
            # key_family contains only regular dimensions for key construction;
            # TIME_PERIOD is filtered via startPeriod/endPeriod query parameters
            key_family = [d.id for d in dimensions if d.type != "TimeDimension"]

            # Parse attributes (lightweight)
            attributes: list[AttributeInfo] = []
            attr_list = dsd_elem.find(".//str:AttributeList", SDMX_NAMESPACES)
            if attr_list:
                for attr in attr_list.findall(".//str:Attribute", SDMX_NAMESPACES):
                    attr_info: AttributeInfo = {
                        "id": attr.get("id", ""),
                        "assignment_status": attr.get("assignmentStatus"),
                    }
                    attributes.append(attr_info)

            # Get primary measure
            primary_measure = None
            measure = dsd_elem.find(".//str:MeasureList/str:PrimaryMeasure", SDMX_NAMESPACES)
            if measure:
                primary_measure = measure.get("id")

            summary = DataStructureSummary(
                id=overview.dsd_ref["id"],
                agency=overview.dsd_ref["agency"],
                version=overview.dsd_ref["version"],
                dimensions=dimensions,
                key_family=key_family,
                attributes=attributes,
                primary_measure=primary_measure,
            )

            self._cache[cache_key] = summary
            return summary

        except Exception as e:
            logger.error(f"Failed to get structure summary: {e}")
            raise

    async def get_dimension_codes(
        self,
        dataflow_id: str,
        dimension_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        search_term: str | None = None,
        limit: int = 50,
        ctx: Context[Any, Any, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Get codes for a specific dimension.
        This is the third level - drilling down into specific dimensions.
        """
        # Get structure summary to find codelist reference
        summary = await self.get_structure_summary(dataflow_id, agency_id, version, ctx)

        # Find the dimension
        dimension = None
        for dim in summary.dimensions:
            if dim.id == dimension_id:
                dimension = dim
                break

        if not dimension:
            return {
                "dimension_id": dimension_id,
                "error": f"Dimension {dimension_id} not found in dataflow",
                "available_dimensions": summary.key_family,
            }

        if dimension.type == "TimeDimension":
            return {
                "dimension_id": dimension_id,
                "type": "TimeDimension",
                "format": "ObservationalTimePeriod",
                "examples": ["2020", "2020-Q1", "2020-01", "2020-W01"],
                "note": "Use standard SDMX time period formats",
            }

        if not dimension.codelist_ref:
            return {
                "dimension_id": dimension_id,
                "type": dimension.type,
                "error": "No codelist associated with this dimension",
            }

        # Fetch the codelist
        cl_ref = dimension.codelist_ref
        cl_url = f"{self.base_url}/codelist/{cl_ref['agency']}/{cl_ref['id']}/{cl_ref['version']}"

        if ctx:
            ctx.info(f"Fetching codes for dimension {dimension_id} from codelist {cl_ref['id']}...")

        try:
            session = await self._get_session()
            response = await session.get(
                cl_url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}
            )
            response.raise_for_status()

            root = ET.fromstring(response.content)
            codes: list[dict[str, str]] = []

            for code_elem in root.findall(".//str:Code", SDMX_NAMESPACES):
                code_id = code_elem.get("id", "")
                name_elem = code_elem.find("./com:Name", SDMX_NAMESPACES)
                name = name_elem.text if name_elem is not None and name_elem.text else code_id

                # Apply search filter if provided
                if search_term:
                    if (
                        search_term.lower() not in code_id.lower()
                        and search_term.lower() not in name.lower()
                    ):
                        continue

                codes.append({"id": code_id, "name": name})

                if len(codes) >= limit:
                    break

            return {
                "dimension_id": dimension_id,
                "position": dimension.position,
                "codelist": cl_ref,
                "total_codes": len(codes),
                "codes": codes,
                "truncated": len(codes) == limit,
                "search_term": search_term,
            }

        except Exception as e:
            logger.error(f"Failed to get dimension codes: {e}")
            return {"dimension_id": dimension_id, "error": str(e)}

    async def discover_dataflows(
        self,
        agency_id: str | None = None,
        resource_id: str = "all",
        version: str = "latest",
        references: str = "none",
        detail: str = "full",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Discover available dataflows using SDMX 2.1 REST API.

        Endpoint: GET /dataflow/{agencyID}/{resourceID}/{version}
        """
        if ctx:
            ctx.info("Starting dataflow discovery...")
            await ctx.report_progress(0, 100)

        agency = agency_id or self.agency_id
        url = f"{self.base_url}/dataflow/{agency}/{resource_id}/{version}"

        headers = {"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}

        params: list[str] = []
        if references != "none":
            params.append(f"references={references}")
        if detail != "full":
            params.append(f"detail={detail}")

        if params:
            url += "?" + "&".join(params)

        try:
            if ctx:
                ctx.info(f"Fetching dataflows from: {url}")
                await ctx.report_progress(25, 100)

            session = await self._get_session()
            response = await session.get(url, headers=headers)
            response.raise_for_status()

            if ctx:
                ctx.info("Parsing SDMX-ML response...")
                await ctx.report_progress(50, 100)

            # Parse SDMX-ML response
            root = ET.fromstring(response.content)

            dataflows: list[dict[str, Any]] = []
            df_elements = root.findall(".//str:Dataflow", SDMX_NAMESPACES)

            if ctx and df_elements:
                ctx.info(f"Processing {len(df_elements)} dataflows...")

            for i, df in enumerate(df_elements):
                df_id = df.get("id")
                df_agency = df.get("agencyID", agency)
                df_version = df.get("version", "latest")
                is_final = df.get("isFinal", "false").lower() == "true"

                # Extract name and description
                name_elem = df.find("./com:Name", SDMX_NAMESPACES)
                desc_elem = df.find("./com:Description", SDMX_NAMESPACES)

                name = name_elem.text if name_elem is not None else df_id
                description = desc_elem.text if desc_elem is not None else ""

                # Extract structure reference if available
                structure_ref = None
                struct_elem = df.find("./str:Structure", SDMX_NAMESPACES)
                if struct_elem is not None:
                    struct_ref = struct_elem.find("./com:Ref", SDMX_NAMESPACES)
                    if struct_ref is not None:
                        structure_ref = {
                            "id": struct_ref.get("id"),
                            "agency": struct_ref.get("agencyID", df_agency),
                            "version": struct_ref.get("version", df_version),
                        }

                dataflow_info = {
                    "id": df_id,
                    "agency": df_agency,
                    "version": df_version,
                    "name": name,
                    "description": description,
                    "is_final": is_final,
                    "structure_reference": structure_ref,
                    "data_url_template": f"{self.base_url}/data/{df_agency},{df_id},{df_version}/{{key}}/{{provider}}",
                    "metadata_url": f"{self.base_url}/dataflow/{df_agency}/{df_id}/{df_version}",
                }

                dataflows.append(dataflow_info)

                if ctx:
                    progress = 50 + ((i + 1) * 40 // len(df_elements))
                    await ctx.report_progress(progress, 100)

            if ctx:
                ctx.info(f"Successfully discovered {len(dataflows)} dataflows")
                await ctx.report_progress(100, 100)

            return dataflows

        except Exception as e:
            if ctx:
                ctx.info(f"Error discovering dataflows: {str(e)}")
            logger.exception("Failed to discover dataflows")
            raise

    async def browse_codelist(
        self,
        codelist_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        search_term: str | None = None,
        ctx: Context[Any, Any, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Browse codes in a specific codelist using the SDMX codelist endpoint.

        This is used for standalone codelist browsing (not in dataflow context).

        Endpoint: GET /codelist/{agencyID}/{resourceID}/{version}

        Args:
            codelist_id: The codelist identifier
            agency_id: The agency ID (defaults to configured agency)
            version: The version (default: "latest")
            search_term: Optional search filter for codes
            ctx: Optional MCP context

        Returns:
            Dict containing codelist metadata and codes
        """
        if ctx:
            ctx.info(f"Retrieving codelist {codelist_id}...")

        agency = agency_id or self.agency_id

        # Build the codelist URL according to SDMX 2.1 spec
        url = f"{self.base_url}/codelist/{agency}/{codelist_id}/{version}"

        try:
            session = await self._get_session()

            if ctx:
                await ctx.report_progress(25, 100)

            # Request the codelist
            response = await session.get(url)
            response.raise_for_status()

            if ctx:
                await ctx.report_progress(50, 100)

            # Parse the XML response
            root = ET.fromstring(response.text)

            codes: list[dict[str, str]] = []

            # Find the codelist element
            codelist_elem = root.find(".//str:Codelist", SDMX_NAMESPACES)
            if codelist_elem is None:
                # Try without namespace prefix
                codelist_elem = root.find(".//Codelist", SDMX_NAMESPACES)

            # Initialize with defaults in case codelist_elem is None
            cl_id = codelist_id
            cl_agency = agency
            cl_version = version
            cl_name = codelist_id

            if codelist_elem is not None:
                # Get codelist metadata
                cl_id = codelist_elem.get("id", codelist_id)
                cl_agency = codelist_elem.get("agencyID", agency)
                cl_version = codelist_elem.get("version", "1.0")

                # Get name
                name_elem = codelist_elem.find(".//com:Name", SDMX_NAMESPACES)
                cl_name = name_elem.text if name_elem is not None and name_elem.text else cl_id

                # Extract codes
                for code_elem in codelist_elem.findall(".//str:Code", SDMX_NAMESPACES):
                    code_id = code_elem.get("id", "")

                    # Get code name/description
                    code_name_elem = code_elem.find(".//com:Name", SDMX_NAMESPACES)
                    code_name = (
                        code_name_elem.text
                        if code_name_elem is not None and code_name_elem.text
                        else code_id
                    )

                    # Get description if available
                    desc_elem = code_elem.find(".//com:Description", SDMX_NAMESPACES)
                    code_desc = desc_elem.text if desc_elem is not None and desc_elem.text else ""

                    # Apply search filter if provided
                    if search_term:
                        search_lower = search_term.lower()
                        if (
                            search_lower not in code_id.lower()
                            and search_lower not in code_name.lower()
                            and search_lower not in code_desc.lower()
                        ):
                            continue

                    codes.append({"id": code_id, "name": code_name, "description": code_desc})

            if ctx:
                await ctx.report_progress(100, 100)
                ctx.info(f"Retrieved {len(codes)} codes from codelist")

            return {
                "codelist_id": cl_id,
                "agency_id": cl_agency,
                "version": cl_version,
                "name": cl_name,
                "codes": codes,
                "total_codes": len(codes),
                "filtered_by": search_term if search_term else None,
            }

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text[:200]}"
            if ctx:
                ctx.info(f"Error retrieving codelist: {error_msg}")
            logger.error(f"Failed to get codelist {codelist_id}: {error_msg}")
            return {"codelist_id": codelist_id, "error": error_msg, "codes": []}
        except Exception as e:
            if ctx:
                ctx.info(f"Error retrieving codelist: {str(e)}")
            logger.exception(f"Failed to get codelist {codelist_id}")
            return {"codelist_id": codelist_id, "error": str(e), "codes": []}

    async def get_actual_availability(
        self,
        dataflow_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Get actual data availability from ContentConstraint.
        This shows what data actually exists vs what's theoretically possible.
        """
        agency = agency_id or self.agency_id

        # Fetch dataflow with references to get constraints
        url = f"{self.base_url}/dataflow/{agency}/{dataflow_id}/{version}?references=all"

        if ctx:
            ctx.info(f"Checking actual data availability for {dataflow_id}...")

        try:
            session = await self._get_session()
            response = await session.get(
                url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}
            )
            response.raise_for_status()

            root = ET.fromstring(response.content)

            # Find ContentConstraint with type="Actual"
            actual_constraint = None
            for constraint in root.findall(
                './/str:ContentConstraint[@type="Actual"]', SDMX_NAMESPACES
            ):
                actual_constraint = constraint
                break

            if not actual_constraint:
                return {
                    "dataflow_id": dataflow_id,
                    "has_constraint": False,
                    "note": "No actual data availability constraint found",
                }

            cube_regions: list[dict[str, Any]] = []
            key_sets: list[dict[str, str]] = []
            time_range: dict[str, str] | None = None

            # Parse CubeRegions (shows available dimension combinations)
            for cube_region in actual_constraint.findall(".//str:CubeRegion", SDMX_NAMESPACES):
                region_keys: dict[str, list[str]] = {}

                for key_value in cube_region.findall(".//com:KeyValue", SDMX_NAMESPACES):
                    dim_id = key_value.get("id", "")
                    values: list[str] = []
                    for value in key_value.findall("./com:Value", SDMX_NAMESPACES):
                        if value.text:
                            values.append(value.text)
                    region_keys[dim_id] = values

                region_info: dict[str, Any] = {
                    "included": cube_region.get("include", "true") == "true",
                    "keys": region_keys,
                }

                # Check for time period
                for attr_value in cube_region.findall(".//com:AttributeValue", SDMX_NAMESPACES):
                    if attr_value.get("id") == "TIME_PERIOD":
                        time_values: list[str] = []
                        for value in attr_value.findall("./com:Value", SDMX_NAMESPACES):
                            if value.text:
                                time_values.append(value.text)
                        if time_values:
                            time_range = {
                                "start": min(time_values),
                                "end": max(time_values),
                            }

                cube_regions.append(region_info)

            # Parse KeySets (alternative representation)
            for key_set in actual_constraint.findall(".//str:KeySet", SDMX_NAMESPACES):
                for key in key_set.findall(".//str:Key", SDMX_NAMESPACES):
                    key_values: dict[str, str] = {}
                    for key_value in key.findall(".//com:KeyValue", SDMX_NAMESPACES):
                        value_elem = key_value.find("./com:Value", SDMX_NAMESPACES)
                        if value_elem is not None and value_elem.text:
                            key_values[key_value.get("id", "")] = value_elem.text
                    key_sets.append(key_values)

            return {
                "dataflow_id": dataflow_id,
                "has_constraint": True,
                "constraint_id": actual_constraint.get("id"),
                "cube_regions": cube_regions,
                "key_sets": key_sets,
                "time_range": time_range,
            }

        except Exception as e:
            logger.error(f"Failed to get actual availability: {e}")
            return {"dataflow_id": dataflow_id, "error": str(e)}

    async def get_structure_references(
        self,
        structure_type: str,
        structure_id: str,
        agency_id: str | None = None,
        version: str = "latest",
        direction: str = "both",
        ctx: Context[Any, Any, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Fetch parent and/or child structure references for an SDMX artifact.

        Uses the SDMX REST API `references` parameter to discover structural relationships:
        - parents: Structures that USE this artifact
        - children: Structures that this artifact REFERENCES
        - both: Combines parents and children

        Args:
            structure_type: Type of structure ('dataflow', 'datastructure', 'codelist',
                           'conceptscheme', 'categoryscheme')
            structure_id: The structure identifier
            agency_id: Agency ID (defaults to client's agency)
            version: Version string (default 'latest')
            direction: 'parents', 'children', or 'both'
            ctx: Optional MCP context for logging

        Returns:
            Dictionary with:
                - target: Info about the queried structure
                - parents: List of structures that use this one (if direction includes parents)
                - children: List of structures this one references (if direction includes children)
                - error: Error message if failed
        """
        agency = agency_id or self.agency_id

        # Map structure_type to SDMX REST endpoint
        endpoint_map = {
            "dataflow": "dataflow",
            "datastructure": "datastructure",
            "dsd": "datastructure",
            "codelist": "codelist",
            "conceptscheme": "conceptscheme",
            "categoryscheme": "categoryscheme",
            "constraint": "contentconstraint",
            "contentconstraint": "contentconstraint",
        }

        endpoint = endpoint_map.get(structure_type.lower())
        if not endpoint:
            return {
                "error": f"Unsupported structure type: {structure_type}",
                "supported_types": list(endpoint_map.keys()),
            }

        # Determine which references parameter to use
        if direction == "parents":
            references_param = "parents"
        elif direction == "children":
            references_param = "children"
        else:  # both
            references_param = "all"

        url = f"{self.base_url}/{endpoint}/{agency}/{structure_id}/{version}?references={references_param}&detail=referencestubs"

        if ctx:
            ctx.info(f"Fetching {direction} references for {structure_type}/{structure_id}...")

        try:
            session = await self._get_session()
            response = await session.get(
                url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"}
            )

            if response.status_code == 404:
                return {
                    "error": f"Structure not found: {structure_type}/{agency}/{structure_id}/{version}",
                    "status_code": 404,
                }

            response.raise_for_status()
            root = ET.fromstring(response.content)

            # Extract the target structure info
            target_info = self._extract_target_structure(root, structure_type, structure_id)

            # Extract all referenced structures
            parents: list[dict[str, str]] = []
            children: list[dict[str, str]] = []

            # Parse all structures in the response
            all_structures = self._extract_all_structures(root)

            # Classify as parent or child based on relationship to target
            for struct in all_structures:
                # Skip the target itself
                if (
                    struct["id"] == structure_id
                    and struct["type"].lower() == structure_type.lower()
                ):
                    continue

                # Determine relationship direction based on structure types
                # Parents: structures that typically USE others (dataflows use DSDs, DSDs use codelists)
                # Children: structures that are typically USED BY others
                relationship = self._classify_relationship(structure_type, struct["type"])

                if relationship == "parent":
                    struct["relationship"] = self._get_relationship_label(
                        struct["type"], structure_type
                    )
                    parents.append(struct)
                elif relationship == "child":
                    struct["relationship"] = self._get_relationship_label(
                        structure_type, struct["type"]
                    )
                    children.append(struct)

            result: dict[str, Any] = {
                "target": target_info,
                "direction": direction,
                "api_calls": 1,
            }

            if direction in ("parents", "both"):
                result["parents"] = parents
            if direction in ("children", "both"):
                result["children"] = children

            if ctx:
                ctx.info(f"Found {len(parents)} parents, {len(children)} children")

            return result

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching structure references: {e}")
            return {"error": f"HTTP error: {e.response.status_code}", "details": str(e)}
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return {"error": f"Failed to parse response: {e}"}
        except Exception as e:
            logger.error(f"Error fetching structure references: {e}")
            return {"error": str(e)}

    def _extract_target_structure(
        self, root: ET.Element, structure_type: str, structure_id: str
    ) -> dict[str, str]:
        """Extract information about the target structure from the response."""
        # Map structure_type to XML element names
        element_map = {
            "dataflow": ".//str:Dataflow",
            "datastructure": ".//str:DataStructure",
            "dsd": ".//str:DataStructure",
            "codelist": ".//str:Codelist",
            "conceptscheme": ".//str:ConceptScheme",
            "categoryscheme": ".//str:CategoryScheme",
            "contentconstraint": ".//str:ContentConstraint",
            "constraint": ".//str:ContentConstraint",
        }

        xpath = element_map.get(structure_type.lower(), f".//str:{structure_type}")

        for elem in root.findall(xpath, SDMX_NAMESPACES):
            if elem.get("id") == structure_id:
                name_elem = elem.find(".//com:Name", SDMX_NAMESPACES)
                name = name_elem.text if name_elem is not None and name_elem.text else structure_id

                return {
                    "type": structure_type,
                    "id": elem.get("id", structure_id),
                    "agency": elem.get("agencyID", ""),
                    "version": elem.get("version", "1.0"),
                    "name": name,
                }

        # Fallback if not found
        return {
            "type": structure_type,
            "id": structure_id,
            "agency": "",
            "version": "",
            "name": structure_id,
        }

    def _extract_all_structures(self, root: ET.Element) -> list[dict[str, str]]:
        """Extract all SDMX structures from the response."""
        structures: list[dict[str, str]] = []

        # Map of element paths to structure types
        structure_elements = [
            (".//str:Dataflow", "dataflow"),
            (".//str:DataStructure", "datastructure"),
            (".//str:Codelist", "codelist"),
            (".//str:ConceptScheme", "conceptscheme"),
            (".//str:CategoryScheme", "categoryscheme"),
            (".//str:ContentConstraint", "constraint"),
            (".//str:Categorisation", "categorisation"),
            (".//str:AgencyScheme", "agencyscheme"),
            (".//str:DataProviderScheme", "dataproviderscheme"),
        ]

        for xpath, struct_type in structure_elements:
            for elem in root.findall(xpath, SDMX_NAMESPACES):
                name_elem = elem.find(".//com:Name", SDMX_NAMESPACES)
                name = (
                    name_elem.text
                    if name_elem is not None and name_elem.text
                    else elem.get("id", "")
                )

                structures.append(
                    {
                        "type": struct_type,
                        "id": elem.get("id", ""),
                        "agency": elem.get("agencyID", ""),
                        "version": elem.get("version", "1.0"),
                        "name": name,
                    }
                )

        return structures

    def _classify_relationship(self, target_type: str, other_type: str) -> str:
        """
        Classify the relationship direction between two structure types.

        In SDMX, the dependency hierarchy is generally:
        Dataflow -> DataStructure -> ConceptScheme -> Codelist
                                  -> Codelist (directly)
        CategoryScheme -> Categorisation -> Dataflow
        Constraint -> Dataflow/DataStructure
        """
        target = target_type.lower()
        other = other_type.lower()

        # Define what each structure type typically references (children)
        children_of = {
            "dataflow": ["datastructure", "dsd"],
            "datastructure": ["conceptscheme", "codelist"],
            "dsd": ["conceptscheme", "codelist"],
            "conceptscheme": ["codelist"],
            "constraint": ["dataflow", "datastructure", "dsd"],
            "contentconstraint": ["dataflow", "datastructure", "dsd"],
            "categorisation": ["dataflow", "categoryscheme"],
        }

        # Define what typically references each structure type (parents)
        parents_of = {
            "codelist": ["conceptscheme", "datastructure", "dsd"],
            "conceptscheme": ["datastructure", "dsd"],
            "datastructure": ["dataflow", "constraint", "contentconstraint"],
            "dsd": ["dataflow", "constraint", "contentconstraint"],
            "dataflow": ["categorisation", "constraint", "contentconstraint"],
            "categoryscheme": ["categorisation"],
        }

        if other in children_of.get(target, []):
            return "child"
        elif other in parents_of.get(target, []):
            return "parent"
        else:
            # Default heuristic: if we got it from 'children' query, treat as child
            return "child"

    def _get_relationship_label(self, from_type: str, to_type: str) -> str:
        """Get a human-readable label for the relationship between two types."""
        from_t = from_type.lower()
        to_t = to_type.lower()

        labels = {
            ("dataflow", "datastructure"): "based on",
            ("dataflow", "dsd"): "based on",
            ("datastructure", "codelist"): "uses codelist",
            ("dsd", "codelist"): "uses codelist",
            ("datastructure", "conceptscheme"): "uses concepts",
            ("dsd", "conceptscheme"): "uses concepts",
            ("conceptscheme", "codelist"): "enumerates with",
            ("constraint", "dataflow"): "constrains",
            ("contentconstraint", "dataflow"): "constrains",
            ("categorisation", "dataflow"): "categorizes",
            ("categorisation", "categoryscheme"): "uses category",
        }

        return labels.get((from_t, to_t), "references")

    def build_progressive_query_guide(
        self, structure_summary: DataStructureSummary
    ) -> dict[str, Any]:
        """
        Build a guide for constructing SDMX queries based on structure.
        """
        summary_dict = structure_summary.to_dict()
        steps: list[dict[str, Any]] = []

        for i, dim in enumerate(structure_summary.dimensions):
            instruction = ""
            if dim.type == "TimeDimension":
                instruction = "Specify time period (e.g., 2020, 2020-Q1, 2020-01)"
            elif dim.codelist_ref:
                instruction = f"Select code from codelist {dim.codelist_ref['id']} or use * for all"
            else:
                instruction = "Specify value or use * for all"

            step: dict[str, Any] = {
                "position": i + 1,
                "dimension": dim.id,
                "type": dim.type,
                "required": dim.required,
                "instruction": instruction,
            }
            steps.append(step)

        examples: list[dict[str, str]] = [
            {
                "description": "All data",
                "key": ".".join(["*" for _ in structure_summary.key_family]),
            },
            {
                "description": "Specific first dimension only",
                "key": "SPECIFIC_CODE"
                + "."
                + ".".join(["*" for _ in structure_summary.key_family[1:]]),
            },
        ]

        guide: dict[str, Any] = {
            "key_structure": summary_dict["key_template"],
            "dimensions_order": structure_summary.key_family,
            "steps": steps,
            "examples": examples,
        }

        return guide
