#!/usr/bin/env python3
"""
Test the MCP server's get_data_availability tool after fixing the wrapper.
"""

import asyncio
from main_server import mcp, get_data_availability

async def test_mcp_availability():
    """Test the MCP wrapper for get_data_availability."""
    print("Testing MCP get_data_availability Tool\n")
    print("=" * 60)
    
    # List tools (FastMCP list_tools is async)
    tools = await mcp.list_tools()
    
    # Find the tool
    tool = None
    for t in tools:
        if t.name == "get_data_availability":
            tool = t
            break
    
    if not tool:
        print("ERROR: get_data_availability tool not found!")
        return
    
    print(f"Found tool: {tool.name}")
    print(f"Description: {tool.description[:100]}...")
    
    # Test 1: Basic availability (no dimension values)
    print("\n1. Testing basic availability check...")
    try:
        result = await get_data_availability(
            dataflow_id="DF_COMMODITY_PRICES",
            agency_id="SPC"
        )
        print(f"   Success! Has constraint: {result.get('has_constraint')}")
        if result.get('error'):
            print(f"   Error: {result['error']}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    # Test 2: With dimension values
    print("\n2. Testing with specific dimension values...")
    try:
        result = await get_data_availability(
            dataflow_id="DF_COMMODITY_PRICES",
            dimension_values={"COMMODITY": "GOLD", "INDICATOR": "COMPRICE", "FREQ": "M"},
            agency_id="SPC",
            version="1.0"
        )
        print(f"   Success! Has data: {result.get('has_data')}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    # Test 3: Progressive check
    print("\n3. Testing progressive check...")
    try:
        result = await get_data_availability(
            dataflow_id="DF_COMMODITY_PRICES",
            progressive_check=[
                {"COMMODITY": "GOLD"},
                {"COMMODITY": "GOLD", "INDICATOR": "COMPRICE"}
            ],
            agency_id="SPC",
            version="1.0"
        )
        print(f"   Success! Progressive results: {len(result.get('progressive_results', []))} steps")
    except Exception as e:
        print(f"   Failed: {e}")
    
    # Test 4: Invalid commodity
    print("\n4. Testing invalid commodity...")
    try:
        result = await get_data_availability(
            dataflow_id="DF_COMMODITY_PRICES",
            dimension_values={"COMMODITY": "UNICORNS"},
            agency_id="SPC",
            version="1.0"
        )
        print(f"   Success! Has data (should be False): {result.get('has_data')}")
    except Exception as e:
        print(f"   Failed: {e}")
    
    print("\n" + "=" * 60)
    print("All tests completed!")

if __name__ == "__main__":
    asyncio.run(test_mcp_availability())