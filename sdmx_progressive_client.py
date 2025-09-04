"""
Enhanced SDMX REST API client with progressive discovery capabilities.

This client provides a layered approach to SDMX metadata discovery:
1. High-level overview (minimal data)
2. Structure summary (dimensions and their order)
3. Detailed drill-down (specific codelists, constraints)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET
from urllib.parse import quote
from dataclasses import dataclass
from enum import Enum

import httpx
from mcp.server.fastmcp import Context

from utils import SDMX_NAMESPACES
from config import SDMX_BASE_URL, SDMX_AGENCY_ID

logger = logging.getLogger(__name__)


class DetailLevel(Enum):
    """Level of detail for metadata retrieval."""
    OVERVIEW = "overview"      # Just names and descriptions
    STRUCTURE = "structure"     # Add dimensions and their order
    FULL = "full"              # Include all codes and constraints


@dataclass
class DataflowOverview:
    """Lightweight dataflow information."""
    id: str
    agency: str
    version: str
    name: str
    description: str
    dsd_ref: Optional[Dict[str, str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "agency": self.agency,
            "version": self.version,
            "name": self.name,
            "description": self.description,
            "dsd_reference": self.dsd_ref
        }


@dataclass
class DimensionInfo:
    """Information about a dimension."""
    id: str
    position: int
    type: str  # "Dimension", "TimeDimension", "MeasureDimension"
    concept: Optional[str] = None
    codelist_ref: Optional[Dict[str, str]] = None
    required: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "position": self.position,
            "type": self.type,
            "concept": self.concept,
            "codelist_ref": self.codelist_ref,
            "required": self.required
        }


@dataclass 
class DataStructureSummary:
    """Summary of data structure without full codelist details."""
    id: str
    agency: str
    version: str
    dimensions: List[DimensionInfo]
    key_family: List[str]  # Ordered list of dimension IDs for key construction
    attributes: List[Dict[str, Any]]
    primary_measure: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "agency": self.agency,
            "version": self.version,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "key_family": self.key_family,
            "attributes": self.attributes,
            "primary_measure": self.primary_measure,
            "key_template": ".".join([f"{{{d}}}" for d in self.key_family]),
            "example_key": ".".join(["" for _ in self.key_family])
        }


class SDMXProgressiveClient:
    """SDMX client with progressive discovery capabilities."""
    
    def __init__(self, base_url: str = None, 
                 agency_id: str = None):
        self.base_url = (base_url or SDMX_BASE_URL).rstrip('/')
        self.agency_id = agency_id or SDMX_AGENCY_ID
        self.session = None
        self._cache = {}  # Simple cache for repeated requests
        # Cache for dataflow versions to avoid repeated lookups
        # Format: {(agency_id, dataflow_id): version}
        self.version_cache = {}
        
    async def _get_session(self) -> httpx.AsyncClient:
        """Get or create HTTP session."""
        if self.session is None:
            self.session = httpx.AsyncClient(timeout=30.0)
        return self.session
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.aclose()
            self.session = None
    
    async def resolve_version(self, 
                             dataflow_id: str,
                             agency_id: str = None,
                             version: str = "latest",
                             ctx: Optional[Context] = None) -> str:
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
            for df_elem in root.findall('.//str:Dataflow', SDMX_NAMESPACES):
                actual_version = df_elem.get('version')
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
    
    async def get_dataflow_overview(self, 
                                   dataflow_id: str,
                                   agency_id: Optional[str] = None,
                                   version: str = "latest",
                                   ctx: Optional[Context] = None) -> DataflowOverview:
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
            response = await session.get(url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"})
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            df_elem = root.find(f'.//str:Dataflow[@id="{dataflow_id}"]', SDMX_NAMESPACES)
            
            if df_elem is None:
                df_elem = root.find('.//str:Dataflow', SDMX_NAMESPACES)
            
            if df_elem:
                name_elem = df_elem.find('./com:Name', SDMX_NAMESPACES)
                desc_elem = df_elem.find('./com:Description', SDMX_NAMESPACES)
                
                # Get DSD reference
                dsd_ref = None
                struct_ref = df_elem.find('.//str:Structure/Ref', SDMX_NAMESPACES)
                if struct_ref is not None:
                    dsd_ref = {
                        'id': struct_ref.get('id'),
                        'agency': struct_ref.get('agencyID', agency),
                        'version': struct_ref.get('version', version)
                    }
                
                overview = DataflowOverview(
                    id=df_elem.get('id'),
                    agency=df_elem.get('agencyID', agency),
                    version=df_elem.get('version', version),
                    name=name_elem.text if name_elem is not None else df_elem.get('id'),
                    description=desc_elem.text if desc_elem is not None else "",
                    dsd_ref=dsd_ref
                )
                
                self._cache[cache_key] = overview
                return overview
                
        except Exception as e:
            logger.error(f"Failed to get dataflow overview: {e}")
            raise
    
    async def get_structure_summary(self,
                                   dataflow_id: str,
                                   agency_id: Optional[str] = None,
                                   version: str = "latest",
                                   ctx: Optional[Context] = None) -> DataStructureSummary:
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
        
        # Fetch DSD with minimal references
        dsd_url = f"{self.base_url}/datastructure/{overview.dsd_ref['agency']}/{overview.dsd_ref['id']}/{overview.dsd_ref['version']}?references=none"
        
        if ctx:
            ctx.info(f"Getting structure summary for {overview.dsd_ref['id']}...")
        
        try:
            session = await self._get_session()
            response = await session.get(dsd_url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"})
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            dsd_elem = root.find('.//str:DataStructure', SDMX_NAMESPACES)
            
            if not dsd_elem:
                raise ValueError("No DataStructure found in response")
            
            dimensions = []
            key_family = []
            
            # Parse dimensions in order
            dim_list = dsd_elem.find('.//str:DimensionList', SDMX_NAMESPACES)
            if dim_list:
                # Regular dimensions
                for dim in dim_list.findall('.//str:Dimension', SDMX_NAMESPACES):
                    position = int(dim.get('position', '0'))
                    dim_id = dim.get('id')
                    
                    # Get codelist reference
                    codelist_ref = None
                    cl_ref = dim.find('.//str:LocalRepresentation/str:Enumeration/Ref', SDMX_NAMESPACES)
                    if cl_ref is None:
                        cl_ref = dim.find('.//str:LocalRepresentation/str:Enumeration/com:Ref', SDMX_NAMESPACES)
                    
                    if cl_ref is not None:
                        codelist_ref = {
                            'id': cl_ref.get('id'),
                            'agency': cl_ref.get('agencyID', agency),
                            'version': cl_ref.get('version', '1.0')
                        }
                    
                    dim_info = DimensionInfo(
                        id=dim_id,
                        position=position,
                        type="Dimension",
                        codelist_ref=codelist_ref
                    )
                    dimensions.append(dim_info)
                
                # Time dimension (usually last)
                time_dim = dim_list.find('.//str:TimeDimension', SDMX_NAMESPACES)
                if time_dim:
                    dim_info = DimensionInfo(
                        id=time_dim.get('id'),
                        position=int(time_dim.get('position', '999')),
                        type="TimeDimension"
                    )
                    dimensions.append(dim_info)
            
            # Sort dimensions by position to get correct key order
            dimensions.sort(key=lambda d: d.position)
            # Include all dimensions in key_family (including TIME_PERIOD) to show complete structure
            # When building queries, TIME_PERIOD can be handled via startPeriod/endPeriod params
            key_family = [d.id for d in dimensions]
            
            # Parse attributes (lightweight)
            attributes = []
            attr_list = dsd_elem.find('.//str:AttributeList', SDMX_NAMESPACES)
            if attr_list:
                for attr in attr_list.findall('.//str:Attribute', SDMX_NAMESPACES):
                    attributes.append({
                        'id': attr.get('id'),
                        'assignment_status': attr.get('assignmentStatus')
                    })
            
            # Get primary measure
            primary_measure = None
            measure = dsd_elem.find('.//str:MeasureList/str:PrimaryMeasure', SDMX_NAMESPACES)
            if measure:
                primary_measure = measure.get('id')
            
            summary = DataStructureSummary(
                id=overview.dsd_ref['id'],
                agency=overview.dsd_ref['agency'],
                version=overview.dsd_ref['version'],
                dimensions=dimensions,
                key_family=key_family,
                attributes=attributes,
                primary_measure=primary_measure
            )
            
            self._cache[cache_key] = summary
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get structure summary: {e}")
            raise
    
    async def get_dimension_codes(self,
                                 dataflow_id: str,
                                 dimension_id: str,
                                 agency_id: Optional[str] = None,
                                 version: str = "latest",
                                 search_term: Optional[str] = None,
                                 limit: int = 50,
                                 ctx: Optional[Context] = None) -> Dict[str, Any]:
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
                "available_dimensions": summary.key_family
            }
        
        if dimension.type == "TimeDimension":
            return {
                "dimension_id": dimension_id,
                "type": "TimeDimension",
                "format": "ObservationalTimePeriod",
                "examples": ["2020", "2020-Q1", "2020-01", "2020-W01"],
                "note": "Use standard SDMX time period formats"
            }
        
        if not dimension.codelist_ref:
            return {
                "dimension_id": dimension_id,
                "type": dimension.type,
                "error": "No codelist associated with this dimension"
            }
        
        # Fetch the codelist
        cl_ref = dimension.codelist_ref
        cl_url = f"{self.base_url}/codelist/{cl_ref['agency']}/{cl_ref['id']}/{cl_ref['version']}"
        
        if ctx:
            ctx.info(f"Fetching codes for dimension {dimension_id} from codelist {cl_ref['id']}...")
        
        try:
            session = await self._get_session()
            response = await session.get(cl_url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"})
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            codes = []
            
            for code_elem in root.findall('.//str:Code', SDMX_NAMESPACES):
                code_id = code_elem.get('id')
                name_elem = code_elem.find('./com:Name', SDMX_NAMESPACES)
                name = name_elem.text if name_elem is not None else code_id
                
                # Apply search filter if provided
                if search_term:
                    if search_term.lower() not in code_id.lower() and search_term.lower() not in name.lower():
                        continue
                
                codes.append({
                    'id': code_id,
                    'name': name
                })
                
                if len(codes) >= limit:
                    break
            
            return {
                "dimension_id": dimension_id,
                "position": dimension.position,
                "codelist": cl_ref,
                "total_codes": len(codes),
                "codes": codes,
                "truncated": len(codes) == limit,
                "search_term": search_term
            }
            
        except Exception as e:
            logger.error(f"Failed to get dimension codes: {e}")
            return {
                "dimension_id": dimension_id,
                "error": str(e)
            }
    
    async def get_actual_availability(self,
                                     dataflow_id: str,
                                     agency_id: Optional[str] = None,
                                     version: str = "latest",
                                     ctx: Optional[Context] = None) -> Dict[str, Any]:
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
            response = await session.get(url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"})
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            # Find ContentConstraint with type="Actual"
            actual_constraint = None
            for constraint in root.findall('.//str:ContentConstraint[@type="Actual"]', SDMX_NAMESPACES):
                actual_constraint = constraint
                break
            
            if not actual_constraint:
                return {
                    "dataflow_id": dataflow_id,
                    "has_constraint": False,
                    "note": "No actual data availability constraint found"
                }
            
            availability = {
                "dataflow_id": dataflow_id,
                "has_constraint": True,
                "constraint_id": actual_constraint.get('id'),
                "cube_regions": [],
                "key_sets": [],
                "time_range": None
            }
            
            # Parse CubeRegions (shows available dimension combinations)
            for cube_region in actual_constraint.findall('.//str:CubeRegion', SDMX_NAMESPACES):
                region_info = {"included": cube_region.get('include', 'true') == 'true', "keys": {}}
                
                for key_value in cube_region.findall('.//com:KeyValue', SDMX_NAMESPACES):
                    dim_id = key_value.get('id')
                    values = []
                    for value in key_value.findall('./com:Value', SDMX_NAMESPACES):
                        values.append(value.text)
                    region_info["keys"][dim_id] = values
                
                # Check for time period
                for attr_value in cube_region.findall('.//com:AttributeValue', SDMX_NAMESPACES):
                    if attr_value.get('id') == 'TIME_PERIOD':
                        time_values = []
                        for value in attr_value.findall('./com:Value', SDMX_NAMESPACES):
                            time_values.append(value.text)
                        if time_values:
                            availability["time_range"] = {
                                "start": min(time_values),
                                "end": max(time_values)
                            }
                
                availability["cube_regions"].append(region_info)
            
            # Parse KeySets (alternative representation)
            for key_set in actual_constraint.findall('.//str:KeySet', SDMX_NAMESPACES):
                keys = []
                for key in key_set.findall('.//str:Key', SDMX_NAMESPACES):
                    key_values = {}
                    for key_value in key.findall('.//com:KeyValue', SDMX_NAMESPACES):
                        key_values[key_value.get('id')] = key_value.find('./com:Value', SDMX_NAMESPACES).text
                    keys.append(key_values)
                availability["key_sets"].extend(keys)
            
            return availability
            
        except Exception as e:
            logger.error(f"Failed to get actual availability: {e}")
            return {
                "dataflow_id": dataflow_id,
                "error": str(e)
            }
    
    def build_progressive_query_guide(self, structure_summary: DataStructureSummary) -> Dict[str, Any]:
        """
        Build a guide for constructing SDMX queries based on structure.
        """
        summary_dict = structure_summary.to_dict()
        guide = {
            "key_structure": summary_dict['key_template'],
            "dimensions_order": structure_summary.key_family,
            "steps": []
        }
        
        for i, dim in enumerate(structure_summary.dimensions):
            step = {
                "position": i + 1,
                "dimension": dim.id,
                "type": dim.type,
                "required": dim.required,
                "instruction": ""
            }
            
            if dim.type == "TimeDimension":
                step["instruction"] = "Specify time period (e.g., 2020, 2020-Q1, 2020-01)"
            elif dim.codelist_ref:
                step["instruction"] = f"Select code from codelist {dim.codelist_ref['id']} or use * for all"
            else:
                step["instruction"] = "Specify value or use * for all"
            
            guide["steps"].append(step)
        
        guide["examples"] = [
            {
                "description": "All data",
                "key": ".".join(["*" for _ in structure_summary.key_family])
            },
            {
                "description": "Specific first dimension only",
                "key": "SPECIFIC_CODE" + "." + ".".join(["*" for _ in structure_summary.key_family[1:]])
            }
        ]
        
        return guide