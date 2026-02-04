#!/usr/bin/env python3
"""
Test version resolution caching to verify it only fetches once.
"""

import asyncio
import time
from tools.sdmx_tools import get_dataflow_structure, get_data_availability
from sdmx_progressive_client import SDMXProgressiveClient


async def test_version_caching():
    """Test that version resolution is cached and reused."""
    print("Testing Version Resolution Caching\n")
    print("=" * 60)
    
    # Create a client instance
    client = SDMXProgressiveClient()
    
    try:
        # Test 1: First call - should fetch and cache
        print("\n1. First call to get_dataflow_structure...")
        start = time.time()
        result1 = await get_dataflow_structure("DF_COMMODITY_PRICES", "SPC")
        duration1 = time.time() - start
        version1 = result1["dataflow"].get("version")
        print(f"   Version resolved: {version1}")
        print(f"   Time taken: {duration1:.2f} seconds")
        
        # Test 2: Second call - should use cached version
        print("\n2. Second call to get_dataflow_structure...")
        start = time.time()
        result2 = await get_dataflow_structure("DF_COMMODITY_PRICES", "SPC")
        duration2 = time.time() - start
        version2 = result2["dataflow"].get("version")
        print(f"   Version resolved: {version2}")
        print(f"   Time taken: {duration2:.2f} seconds")
        
        # Test 3: Call with availability check - should also use cache
        print("\n3. Call to get_data_availability...")
        start = time.time()
        result3 = await get_data_availability(
            "DF_COMMODITY_PRICES",
            dimension_values={"COMMODITY": "GOLD", "INDICATOR": "COMPRICE", "FREQ": "M"},
            agency_id="SPC"
        )
        duration3 = time.time() - start
        print(f"   Has data: {result3.get('has_data')}")
        print(f"   Time taken: {duration3:.2f} seconds")
        
        # Check cache contents
        print("\n4. Cache status:")
        print(f"   Cached versions: {client.version_cache}")
        
        # Verify performance improvement
        print("\n5. Performance Analysis:")
        print(f"   First call (with fetch): {duration1:.2f}s")
        print(f"   Second call (cached): {duration2:.2f}s")
        print(f"   Speed improvement: {duration1/duration2:.1f}x faster")
        
        if duration2 < duration1 * 0.5:
            print("   ✅ Caching is working! Second call was significantly faster.")
        else:
            print("   ⚠️ Caching might not be working as expected.")
            
    finally:
        await client.close()
    
    print("\n" + "=" * 60)
    print("Test completed!")


async def test_multiple_dataflows():
    """Test caching across multiple dataflows."""
    print("\n\nTesting Multiple Dataflows Caching")
    print("=" * 60)
    
    client = SDMXProgressiveClient()
    
    try:
        dataflows = ["DF_COMMODITY_PRICES", "DF_NATIONAL_ACCOUNTS", "DF_BOP"]
        
        print("\n1. First pass - fetching versions...")
        for df_id in dataflows:
            start = time.time()
            version = await client.resolve_version(df_id, "SPC", "latest")
            duration = time.time() - start
            print(f"   {df_id}: v{version} ({duration:.2f}s)")
        
        print("\n2. Second pass - using cached versions...")
        for df_id in dataflows:
            start = time.time()
            version = await client.resolve_version(df_id, "SPC", "latest")
            duration = time.time() - start
            print(f"   {df_id}: v{version} ({duration:.3f}s)")
        
        print(f"\n3. Total cached entries: {len(client.version_cache)}")
        
    finally:
        await client.close()
    
    print("\n" + "=" * 60)


async def main():
    """Run all caching tests."""
    await test_version_caching()
    await test_multiple_dataflows()
    

if __name__ == "__main__":
    asyncio.run(main())