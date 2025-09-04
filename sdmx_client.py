"""
SDMX REST API client with full SDMX 2.1 support.
"""

import logging
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import Context

from utils import SDMX_NAMESPACES
from config import SDMX_BASE_URL, SDMX_AGENCY_ID

logger = logging.getLogger(__name__)


class SDMXClient:
    """SDMX REST API client with full SDMX 2.1 support."""
    
    def __init__(self, base_url: str = None, agency_id: str = None):
        self.base_url = (base_url or SDMX_BASE_URL).rstrip('/')
        self.agency_id = agency_id or SDMX_AGENCY_ID
        self.session = None
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
    
    async def discover_dataflows(self, 
                                agency_id: Optional[str] = None,
                                resource_id: str = "all",
                                version: str = "latest",
                                references: str = "none",
                                detail: str = "full",
                                ctx: Optional[Context] = None) -> List[Dict[str, Any]]:
        """
        Discover available dataflows using SDMX 2.1 REST API.
        
        Endpoint: GET /dataflow/{agencyID}/{resourceID}/{version}
        """
        if ctx:
            ctx.info("Starting dataflow discovery...")
            await ctx.report_progress(0, 100)
        
        agency = agency_id or self.agency_id
        url = f"{self.base_url}/dataflow/{agency}/{resource_id}/{version}"
        
        headers = {
            "Accept": "application/vnd.sdmx.structure+xml;version=2.1"
        }
        
        params = []
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
            
            dataflows = []
            df_elements = root.findall('.//str:Dataflow', SDMX_NAMESPACES)
            
            if ctx and df_elements:
                ctx.info(f"Processing {len(df_elements)} dataflows...")
            
            for i, df in enumerate(df_elements):
                df_id = df.get('id')
                df_agency = df.get('agencyID', agency)
                df_version = df.get('version', 'latest')
                is_final = df.get('isFinal', 'false').lower() == 'true'
                
                # Extract name and description
                name_elem = df.find('./com:Name', SDMX_NAMESPACES)
                desc_elem = df.find('./com:Description', SDMX_NAMESPACES)
                
                name = name_elem.text if name_elem is not None else df_id
                description = desc_elem.text if desc_elem is not None else ""
                
                # Extract structure reference if available
                structure_ref = None
                struct_elem = df.find('./str:Structure', SDMX_NAMESPACES)
                if struct_elem is not None:
                    struct_ref = struct_elem.find('./com:Ref', SDMX_NAMESPACES)
                    if struct_ref is not None:
                        structure_ref = {
                            'id': struct_ref.get('id'),
                            'agency': struct_ref.get('agencyID', df_agency),
                            'version': struct_ref.get('version', df_version)
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
                    "metadata_url": f"{self.base_url}/dataflow/{df_agency}/{df_id}/{df_version}"
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
                ctx.info(f"Error during dataflow discovery: {str(e)}")
            logger.exception(f"Failed to discover dataflows from {url}")
            return []
    
    async def get_datastructure(self, 
                               dataflow_id: str,
                               agency_id: Optional[str] = None,
                               version: str = "latest",
                               references: str = "all",
                               detail: str = "full",
                               ctx: Optional[Context] = None) -> Optional[Dict[str, Any]]:
        """
        Get detailed data structure definition (DSD) for a dataflow.
        
        First fetches the dataflow metadata to get the actual DSD ID,
        then retrieves the data structure.
        
        Endpoint: GET /datastructure/{agencyID}/{resourceID}/{version}
        """
        if ctx:
            ctx.info(f"Retrieving structure for dataflow: {dataflow_id}")
            await ctx.report_progress(0, 100)
        
        agency = agency_id or self.agency_id
        
        # First, get the dataflow metadata to find the actual DSD ID
        df_url = f"{self.base_url}/dataflow/{agency}/{dataflow_id}/{version}?references=none"
        dsd_id = dataflow_id  # Default to dataflow_id if we can't find the structure reference
        
        try:
            if ctx:
                ctx.info(f"Fetching dataflow metadata to get DSD reference...")
            
            session = await self._get_session()
            df_response = await session.get(df_url, headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"})
            
            if df_response.status_code == 200:
                # Parse the dataflow response to get the DSD reference
                root = ET.fromstring(df_response.content)
                df_elem = root.find('.//str:Dataflow[@id="' + dataflow_id + '"]', SDMX_NAMESPACES)
                
                if df_elem is not None:
                    # Try with namespace prefix first
                    struct_ref = df_elem.find('.//str:Structure/com:Ref', SDMX_NAMESPACES)
                    if struct_ref is None:
                        # Try without namespace prefix (some implementations don't use it)
                        struct_ref = df_elem.find('.//str:Structure/Ref', SDMX_NAMESPACES)
                    
                    if struct_ref is not None:
                        dsd_id = struct_ref.get('id', dataflow_id)
                        dsd_agency = struct_ref.get('agencyID', agency)
                        dsd_version = struct_ref.get('version', version)
                        
                        if ctx:
                            ctx.info(f"Found DSD reference: {dsd_id} (agency: {dsd_agency}, version: {dsd_version})")
                        
                        # Update agency and version if they were specified in the structure reference
                        agency = dsd_agency
                        version = dsd_version
        
        except Exception as e:
            if ctx:
                ctx.info(f"Warning: Could not fetch dataflow metadata: {str(e)}. Using dataflow ID as DSD ID.")
        
        # Now fetch the actual data structure using the correct DSD ID
        url = f"{self.base_url}/datastructure/{agency}/{dsd_id}/{version}"
        
        headers = {
            "Accept": "application/vnd.sdmx.structure+xml;version=2.1"
        }
        
        params = []
        if references != "none":
            params.append(f"references={references}")
        if detail != "full":
            params.append(f"detail={detail}")
        
        if params:
            url += "?" + "&".join(params)
        
        try:
            if ctx:
                ctx.info(f"Fetching DSD from: {url}")
                await ctx.report_progress(25, 100)
            
            session = await self._get_session()
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            
            if ctx:
                ctx.info("Parsing DSD structure...")
                await ctx.report_progress(50, 100)
            
            # Parse the DSD response
            root = ET.fromstring(response.content)
            
            # Find the DataStructure element
            dsd_elem = root.find('.//str:DataStructure[@id="' + dsd_id + '"]', SDMX_NAMESPACES)
            if dsd_elem is None:
                # Try without ID filter
                dsd_elem = root.find('.//str:DataStructure', SDMX_NAMESPACES)
            
            structure_info = {
                "id": dsd_id,
                "dataflow_id": dataflow_id,
                "agency": agency,
                "version": version,
                "structure_url": url,
                "has_structure": True,
                "dimensions": [],
                "attributes": [],
                "measures": [],
                "primary_measure": None
            }
            
            if dsd_elem is not None:
                # Extract dimensions
                dim_list = dsd_elem.find('.//str:DimensionList', SDMX_NAMESPACES)
                if dim_list is not None:
                    # Regular dimensions
                    for dim in dim_list.findall('.//str:Dimension', SDMX_NAMESPACES):
                        dim_info = {
                            "id": dim.get('id'),
                            "position": dim.get('position'),
                            "type": "Dimension",
                            "concept": None,
                            "codelist": None
                        }
                        
                        # Get concept reference
                        concept_ref = dim.find('.//str:ConceptIdentity/com:Ref', SDMX_NAMESPACES)
                        if concept_ref is not None:
                            dim_info["concept"] = concept_ref.get('id')
                        
                        # Get codelist reference
                        codelist_ref = dim.find('.//str:LocalRepresentation/str:Enumeration/com:Ref', SDMX_NAMESPACES)
                        if codelist_ref is not None:
                            dim_info["codelist"] = {
                                "id": codelist_ref.get('id'),
                                "agency": codelist_ref.get('agencyID', agency),
                                "version": codelist_ref.get('version', '1.0')
                            }
                        
                        structure_info["dimensions"].append(dim_info)
                    
                    # Time dimension
                    time_dim = dim_list.find('.//str:TimeDimension', SDMX_NAMESPACES)
                    if time_dim is not None:
                        time_info = {
                            "id": time_dim.get('id'),
                            "position": time_dim.get('position'),
                            "type": "TimeDimension",
                            "concept": None,
                            "format": None
                        }
                        
                        concept_ref = time_dim.find('.//str:ConceptIdentity/com:Ref', SDMX_NAMESPACES)
                        if concept_ref is not None:
                            time_info["concept"] = concept_ref.get('id')
                        
                        # Check for time format
                        text_format = time_dim.find('.//str:LocalRepresentation/str:TextFormat', SDMX_NAMESPACES)
                        if text_format is not None:
                            time_info["format"] = text_format.get('textType')
                        
                        structure_info["dimensions"].append(time_info)
                
                # Extract attributes
                attr_list = dsd_elem.find('.//str:AttributeList', SDMX_NAMESPACES)
                if attr_list is not None:
                    for attr in attr_list.findall('.//str:Attribute', SDMX_NAMESPACES):
                        attr_info = {
                            "id": attr.get('id'),
                            "assignment_status": attr.get('assignmentStatus'),
                            "concept": None,
                            "codelist": None
                        }
                        
                        # Get concept reference
                        concept_ref = attr.find('.//str:ConceptIdentity/com:Ref', SDMX_NAMESPACES)
                        if concept_ref is not None:
                            attr_info["concept"] = concept_ref.get('id')
                        
                        # Get codelist reference if exists
                        codelist_ref = attr.find('.//str:LocalRepresentation/str:Enumeration/com:Ref', SDMX_NAMESPACES)
                        if codelist_ref is not None:
                            attr_info["codelist"] = {
                                "id": codelist_ref.get('id'),
                                "agency": codelist_ref.get('agencyID', agency),
                                "version": codelist_ref.get('version', '1.0')
                            }
                        
                        structure_info["attributes"].append(attr_info)
                
                # Extract measures
                measure_list = dsd_elem.find('.//str:MeasureList', SDMX_NAMESPACES)
                if measure_list is not None:
                    primary_measure = measure_list.find('.//str:PrimaryMeasure', SDMX_NAMESPACES)
                    if primary_measure is not None:
                        measure_info = {
                            "id": primary_measure.get('id'),
                            "concept": None
                        }
                        
                        concept_ref = primary_measure.find('.//str:ConceptIdentity/com:Ref', SDMX_NAMESPACES)
                        if concept_ref is not None:
                            measure_info["concept"] = concept_ref.get('id')
                        
                        structure_info["primary_measure"] = measure_info
                        structure_info["measures"].append(measure_info)
            
            if ctx:
                ctx.info("DSD retrieved successfully")
                await ctx.report_progress(100, 100)
            
            return structure_info
            
        except Exception as e:
            if ctx:
                ctx.info(f"Error retrieving DSD: {str(e)}")
            logger.exception(f"Failed to get DSD for {dataflow_id}")
            return {
                "dataflow_id": dataflow_id,
                "agency": agency,
                "version": version,
                "structure_url": url,
                "has_structure": False,
                "error": str(e)
            }
    
    async def get_codelist(self,
                          codelist_id: str,
                          agency_id: Optional[str] = None,
                          version: str = "latest",
                          item_id: Optional[str] = None,
                          ctx: Optional[Context] = None) -> Optional[Dict[str, Any]]:
        """
        Get codelist with all codes and descriptions.
        
        Endpoint: GET /codelist/{agencyID}/{resourceID}/{version}[/{itemID}]
        """
        if ctx:
            ctx.info(f"Retrieving codelist: {codelist_id}")
            await ctx.report_progress(0, 100)
        
        agency = agency_id or self.agency_id
        url = f"{self.base_url}/codelist/{agency}/{codelist_id}/{version}"
        
        if item_id:
            url += f"/{item_id}"
        
        headers = {
            "Accept": "application/vnd.sdmx.structure+xml;version=2.1"
        }
        
        try:
            if ctx:
                ctx.info(f"Fetching codelist from: {url}")
                await ctx.report_progress(25, 100)
            
            session = await self._get_session()
            response = await session.get(url, headers=headers)
            response.raise_for_status()
            
            if ctx:
                ctx.info("Parsing codelist...")
                await ctx.report_progress(50, 100)
            
            # Parse SDMX-ML codelist response
            root = ET.fromstring(response.content)
            
            codes = []
            code_elements = root.findall('.//str:Code', SDMX_NAMESPACES)
            
            for code_elem in code_elements:
                code_id = code_elem.get('id')
                
                name_elem = code_elem.find('./com:Name', SDMX_NAMESPACES)
                desc_elem = code_elem.find('./com:Description', SDMX_NAMESPACES)
                
                name = name_elem.text if name_elem is not None else code_id
                description = desc_elem.text if desc_elem is not None else ""
                
                codes.append({
                    "id": code_id,
                    "name": name,
                    "description": description
                })
            
            codelist_info = {
                "codelist_id": codelist_id,
                "agency": agency,
                "version": version,
                "item_id": item_id,
                "total_codes": len(codes),
                "codes": codes,
                "url": url
            }
            
            if ctx:
                ctx.info(f"Retrieved {len(codes)} codes from codelist")
                await ctx.report_progress(100, 100)
            
            return codelist_info
            
        except Exception as e:
            if ctx:
                ctx.info(f"Error retrieving codelist: {str(e)}")
            logger.exception(f"Failed to get codelist {codelist_id}")
            return {
                "codelist_id": codelist_id,
                "agency": agency,
                "version": version,
                "url": url,
                "error": str(e),
                "codes": []
            }
    
    async def browse_codelist(self,
                            codelist_id: str,
                            agency_id: Optional[str] = None,
                            version: str = "latest",
                            search_term: Optional[str] = None,
                            ctx: Optional[Context] = None) -> Dict[str, Any]:
        """
        Browse codes in a specific codelist using the SDMX codelist endpoint.
        
        Endpoint: GET /codelist/{agencyID}/{resourceID}/{version}
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
            
            codes = []
            
            # Find the codelist element
            codelist_elem = root.find('.//str:Codelist', SDMX_NAMESPACES)
            if codelist_elem is None:
                # Try without namespace prefix
                codelist_elem = root.find('.//Codelist', SDMX_NAMESPACES)
            
            if codelist_elem:
                # Get codelist metadata
                cl_id = codelist_elem.get('id')
                cl_agency = codelist_elem.get('agencyID', agency)
                cl_version = codelist_elem.get('version', '1.0')
                
                # Get name
                name_elem = codelist_elem.find('.//com:Name', SDMX_NAMESPACES)
                cl_name = name_elem.text if name_elem is not None else cl_id
                
                # Extract codes
                for code_elem in codelist_elem.findall('.//str:Code', SDMX_NAMESPACES):
                    code_id = code_elem.get('id')
                    
                    # Get code name/description
                    name_elem = code_elem.find('.//com:Name', SDMX_NAMESPACES)
                    code_name = name_elem.text if name_elem is not None else code_id
                    
                    # Get description if available
                    desc_elem = code_elem.find('.//com:Description', SDMX_NAMESPACES)
                    code_desc = desc_elem.text if desc_elem is not None else ""
                    
                    # Apply search filter if provided
                    if search_term:
                        search_lower = search_term.lower()
                        if (search_lower not in code_id.lower() and 
                            search_lower not in code_name.lower() and
                            search_lower not in code_desc.lower()):
                            continue
                    
                    codes.append({
                        "id": code_id,
                        "name": code_name,
                        "description": code_desc
                    })
            
            if ctx:
                await ctx.report_progress(100, 100)
                ctx.info(f"Retrieved {len(codes)} codes from codelist")
            
            return {
                "codelist_id": cl_id if codelist_elem else codelist_id,
                "agency_id": cl_agency if codelist_elem else agency,
                "version": cl_version if codelist_elem else version,
                "name": cl_name if codelist_elem else codelist_id,
                "codes": codes,
                "total_codes": len(codes),
                "filtered_by": search_term if search_term else None
            }
            
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text[:200]}"
            if ctx:
                ctx.info(f"Error retrieving codelist: {error_msg}")
            logger.error(f"Failed to get codelist {codelist_id}: {error_msg}")
            return {
                "codelist_id": codelist_id,
                "agency_id": agency,
                "version": version,
                "error": error_msg,
                "codes": []
            }
        except Exception as e:
            if ctx:
                ctx.info(f"Error retrieving codelist: {str(e)}")
            logger.exception(f"Failed to get codelist {codelist_id}")
            return {
                "codelist_id": codelist_id,
                "agency_id": agency,
                "version": version,
                "error": str(e),
                "codes": []
            }
    
    def build_data_url(self,
                      dataflow_id: str,
                      agency_id: Optional[str] = None,
                      version: str = "latest",
                      key: str = "all",
                      provider: str = "all",
                      start_period: Optional[str] = None,
                      end_period: Optional[str] = None,
                      format_type: str = "structurespecificdata") -> str:
        """
        Build a data query URL for SDMX REST API.
        
        Format: /data/{flow}/{key}/{provider}
        where flow = {agencyID},{dataflowID},{version}
        """
        agency = agency_id or self.agency_id
        
        # Build the flow parameter
        flow = f"{agency},{dataflow_id},{version}"
        
        # Build base URL
        url = f"{self.base_url}/data/{flow}/{key}/{provider}"
        
        # Add query parameters
        params = []
        
        if start_period:
            params.append(f"startPeriod={start_period}")
        if end_period:
            params.append(f"endPeriod={end_period}")
        
        # Add format parameter based on type
        if format_type.lower() == "csv":
            params.append("format=csv")
        elif format_type.lower() == "json":
            params.append("format=jsondata")
        
        if params:
            url += "?" + "&".join(params)
        
        return url