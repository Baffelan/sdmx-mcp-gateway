#!/usr/bin/env python3
"""
SDMX MCP Gateway Server

An MCP server that translates natural language queries into structured SDMX query information.
The server discovers available dataflows, analyzes queries, and returns structured data that
AI clients can use to generate scripts in any language.
"""

import logging
from typing import Any, Dict, List, Optional
import re
import json
import xml.etree.ElementTree as ET

import anyio
import click
import mcp.types as types
from mcp.server.lowlevel import Server
import httpx

logger = logging.getLogger(__name__)


class SDMXQueryEngine:
    """Engine for translating natural language queries into SDMX query parameters."""
    
    def __init__(self, base_url: str = "https://stats-sdmx-disseminate.pacificdata.org/rest", agency_id: str = "SPC"):
        self.base_url = base_url
        self.agency_id = agency_id
        
    async def discover_dataflows(self) -> List[Dict[str, Any]]:
        """Discover all available dataflows from the SDMX API."""
        url = f"{self.base_url}/dataflow/{self.agency_id}/"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers={
                    "Accept": "application/vnd.sdmx.structure+xml;version=2.1"
                })
                response.raise_for_status()
                
                # Parse XML response
                root = ET.fromstring(response.content)
                
                # Define namespaces for SDMX XML
                namespaces = {
                    'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                    'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
                }
                
                dataflows = []
                for df in root.findall('.//str:Dataflow', namespaces):
                    df_id = df.get('id')
                    version = df.get('version', 'latest')
                    
                    # Extract name and description
                    name_elem = df.find('./com:Name', namespaces)
                    desc_elem = df.find('./com:Description', namespaces)
                    
                    name = name_elem.text if name_elem is not None else df_id
                    description = desc_elem.text if desc_elem is not None else ""
                    
                    dataflows.append({
                        "id": df_id,
                        "version": version,
                        "name": name,
                        "description": description,
                        "agency": self.agency_id
                    })
                
                return dataflows
                
        except Exception as e:
            logger.exception(f"Failed to discover dataflows from {url}")
            return []
    
    async def get_dataflow_structure(self, dataflow_id: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get detailed structure information for a specific dataflow."""
        url = f"{self.base_url}/dataflow/{self.agency_id}/{dataflow_id}/{version}?references=all"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers={
                    "Accept": "application/vnd.sdmx.structure+xml;version=2.1"
                })
                response.raise_for_status()
                
                return {
                    "dataflow_id": dataflow_id,
                    "version": version,
                    "structure_url": url,
                    "has_structure": True,
                    "notes": "Full structure available at structure_url - contains dimensions, codelists, and constraints"
                }
                
        except Exception as e:
            logger.exception(f"Failed to get structure for dataflow {dataflow_id}")
            return {
                "dataflow_id": dataflow_id,
                "version": version,
                "structure_url": url,
                "has_structure": False,
                "error": str(e)
            }
    
    def analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze natural language query to extract key components."""
        query_lower = query.lower()
        
        # Extract words for keyword matching
        words = re.findall(r'\b\w+\b', query_lower)
        
        # Look for year patterns
        years = re.findall(r'\b(20\d{2})\b', query)
        
        # Look for potential country codes (2-3 letter uppercase codes)
        potential_countries = re.findall(r'\b[A-Z]{2,3}\b', query.upper())
        
        # Look for frequency indicators
        frequency_indicators = []
        if any(word in query_lower for word in ['monthly', 'month']):
            frequency_indicators.append('M')
        if any(word in query_lower for word in ['quarterly', 'quarter']):
            frequency_indicators.append('Q')
        if any(word in query_lower for word in ['annual', 'yearly', 'year']):
            frequency_indicators.append('A')
        
        return {
            "keywords": words,
            "years": years,
            "potential_countries": potential_countries,
            "frequency_indicators": frequency_indicators,
            "original_query": query
        }
    
    def score_dataflow_relevance(self, dataflow: Dict[str, Any], analysis: Dict[str, Any]) -> int:
        """Score how relevant a dataflow is to the query."""
        score = 0
        
        # Combine searchable text
        searchable_text = f"{dataflow['name']} {dataflow['description']} {dataflow['id']}".lower()
        
        # Score based on keyword matches
        for keyword in analysis["keywords"]:
            if len(keyword) > 2 and keyword in searchable_text:  # Ignore very short words
                score += 1
        
        return score
    
    async def translate_query(self, query: str) -> Dict[str, Any]:
        """Translate natural language query into structured SDMX query information."""
        
        # Analyze the query
        analysis = self.analyze_query(query)
        
        # Discover available dataflows
        all_dataflows = await self.discover_dataflows()
        
        # Score and filter relevant dataflows
        scored_dataflows = []
        for df in all_dataflows:
            score = self.score_dataflow_relevance(df, analysis)
            if score > 0:
                df["relevance_score"] = score
                scored_dataflows.append(df)
        
        # Sort by relevance
        scored_dataflows.sort(key=lambda x: x["relevance_score"], reverse=True)
        relevant_dataflows = scored_dataflows[:5]  # Top 5 most relevant
        
        # If no relevant dataflows found, include some examples
        if not relevant_dataflows:
            relevant_dataflows = all_dataflows[:3]  # First 3 as examples
        
        # Build structured query information
        query_info = {
            "base_url": self.base_url,
            "agency_id": self.agency_id,
            "query_analysis": analysis,
            "relevant_dataflows": relevant_dataflows,
            "total_available_dataflows": len(all_dataflows),
            "suggested_queries": []
        }
        
        # Generate suggested SDMX queries for each relevant dataflow
        for df in relevant_dataflows[:3]:  # Top 3
            query_suggestions = await self._generate_query_suggestions(df, analysis)
            query_info["suggested_queries"].extend(query_suggestions)
        
        return query_info
    
    async def _generate_query_suggestions(self, dataflow: Dict[str, Any], analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate specific SDMX query suggestions for a dataflow."""
        
        suggestions = []
        df_id = dataflow["id"]
        version = dataflow.get("version", "latest")
        
        # Get structure information
        structure_info = await self.get_dataflow_structure(df_id, version)
        
        # Base data URL
        base_data_url = f"{self.base_url}/data/{self.agency_id},{df_id},{version}"
        
        # Generate different query variations
        
        # 1. Basic query - all data with time constraints
        basic_query = {
            "dataflow_id": df_id,
            "dataflow_name": dataflow["name"],
            "query_type": "basic",
            "data_url": f"{base_data_url}/all",
            "parameters": {
                "dimensionAtObservation": "AllDimensions"
            },
            "description": f"All data from {dataflow['name']}"
        }
        
        # Add time constraints if years were mentioned
        if analysis["years"]:
            basic_query["parameters"]["startPeriod"] = min(analysis["years"])
            basic_query["parameters"]["endPeriod"] = max(analysis["years"])
            basic_query["description"] += f" for {min(analysis['years'])}-{max(analysis['years'])}"
        
        suggestions.append(basic_query)
        
        # 2. Frequency-specific query if frequency was detected
        if analysis["frequency_indicators"]:
            for freq in analysis["frequency_indicators"]:
                freq_query = {
                    "dataflow_id": df_id,
                    "dataflow_name": dataflow["name"],
                    "query_type": "frequency_filtered",
                    "data_url": f"{base_data_url}/{freq}.all.all",
                    "parameters": {
                        "dimensionAtObservation": "AllDimensions"
                    },
                    "description": f"{'Monthly' if freq == 'M' else 'Quarterly' if freq == 'Q' else 'Annual'} data from {dataflow['name']}",
                    "dimension_filter": f"FREQ={freq}"
                }
                
                if analysis["years"]:
                    freq_query["parameters"]["startPeriod"] = min(analysis["years"])
                    freq_query["parameters"]["endPeriod"] = max(analysis["years"])
                
                suggestions.append(freq_query)
        
        # 3. Country-specific query if potential countries were detected
        if analysis["potential_countries"]:
            for country in analysis["potential_countries"][:2]:  # Max 2 countries
                country_query = {
                    "dataflow_id": df_id,
                    "dataflow_name": dataflow["name"],
                    "query_type": "country_filtered",
                    "data_url": f"{base_data_url}/all.{country}.all",
                    "parameters": {
                        "dimensionAtObservation": "AllDimensions"
                    },
                    "description": f"Data from {dataflow['name']} for {country}",
                    "dimension_filter": f"REF_AREA={country}"
                }
                
                if analysis["years"]:
                    country_query["parameters"]["startPeriod"] = min(analysis["years"])
                    country_query["parameters"]["endPeriod"] = max(analysis["years"])
                
                suggestions.append(country_query)
        
        # Add structure information to all suggestions
        for suggestion in suggestions:
            suggestion["structure_info"] = structure_info
            suggestion["csv_headers"] = {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"}
            suggestion["xml_headers"] = {"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"}
        
        return suggestions


async def query_sdmx_data(query: str) -> Dict[str, Any]:
    """Main function to translate natural language query into SDMX query information."""
    
    # Initialize the query engine (could be made configurable for other providers)
    engine = SDMXQueryEngine()
    
    # Translate the query
    query_info = await engine.translate_query(query)
    
    return query_info


@click.command()
@click.option("--port", default=8000, help="Port to listen on for SSE")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport type",
)
def main(port: int, transport: str) -> int:
    """Run the SDMX MCP Gateway server."""
    
    app = Server("sdmx-query-translator")

    @app.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.ContentBlock]:
        if name != "translate_sdmx_query":
            raise ValueError(f"Unknown tool: {name}")
        
        query = arguments.get("query", "")
        
        if not query:
            raise ValueError("Missing required argument 'query'")
        
        try:
            query_info = await query_sdmx_data(query)
            
            # Return structured JSON that the AI client can use
            return [types.TextContent(
                type="text", 
                text=json.dumps(query_info, indent=2)
            )]
            
        except Exception as e:
            logger.exception("Failed to translate SDMX query")
            return [types.TextContent(
                type="text", 
                text=json.dumps({
                    "error": str(e),
                    "query": query,
                    "status": "failed"
                }, indent=2)
            )]

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="translate_sdmx_query",
                title="SDMX Query Translator",
                description="Translates natural language queries into structured SDMX query information. Discovers available dataflows, analyzes the query, and returns JSON with specific SDMX REST API URLs, parameters, and metadata that can be used to generate scripts in any programming language.",
                inputSchema={
                    "type": "object",
                    "required": ["query"],
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language description of the statistical data you want to query (e.g., 'trade data for Tonga', 'monthly tourism statistics for Pacific countries', 'currency exchange rates for 2023')",
                        }
                    },
                },
            )
        ]

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.responses import Response
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await app.run(streams[0], streams[1], app.create_initialization_options())
            return Response()

        starlette_app = Starlette(
            debug=True,
            routes=[
                Route("/sse", endpoint=handle_sse, methods=["GET"]),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )

        import uvicorn
        uvicorn.run(starlette_app, host="127.0.0.1", port=port)
    else:
        from mcp.server.stdio import stdio_server

        async def arun():
            async with stdio_server() as streams:
                await app.run(streams[0], streams[1], app.create_initialization_options())

        anyio.run(arun)

    return 0


if __name__ == "__main__":
    main()