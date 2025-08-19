#!/usr/bin/env python3
"""
Simple test script to verify the SDMX MCP Gateway server functionality.
"""

import asyncio
import json
from src.sdmx_mcp_gateway.server import query_sdmx_data

async def test_queries():
    """Test the SDMX query translation with various queries."""
    
    test_queries = [
        "trade data for Tonga",
        "monthly tourism statistics for Pacific countries",
        "currency exchange rates",
        "GDP data for Fiji",
        "fisheries statistics"
    ]
    
    print("=== SDMX MCP Gateway Test ===")
    print("Testing query translation functionality...")
    print()
    
    for i, query in enumerate(test_queries, 1):
        print(f"Test {i}: '{query}'")
        print("-" * 50)
        
        try:
            result = await query_sdmx_data(query)
            
            print("SUCCESS: Successfully translated query")
            print(f"  Found {len(result.get('relevant_dataflows', []))} relevant dataflows")
            print(f"  Generated {len(result.get('suggested_queries', []))} query suggestions")
            
            # Show first relevant dataflow
            if result.get('relevant_dataflows'):
                df = result['relevant_dataflows'][0]
                print(f"  Top match: {df['id']} - {df['name']}")
            
            # Show first suggested query
            if result.get('suggested_queries'):
                query_suggestion = result['suggested_queries'][0]
                print(f"  Sample URL: {query_suggestion['data_url']}")
            
        except Exception as e:
            print(f"ERROR: {e}")
        
        print()

if __name__ == "__main__":
    asyncio.run(test_queries())