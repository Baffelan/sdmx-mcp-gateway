#!/usr/bin/env python3
"""
Test that TIME_PERIOD (or other time dimensions) are parsed correctly.
"""

import asyncio
from tools.sdmx_tools import get_data_availability

async def test_time_period():
    print("Testing TIME_PERIOD Parsing Fix\n")
    print("=" * 60)
    
    # Test 1: DF_COMMODITY_PRICES (has TIME_PERIOD)
    print("\n1. Testing DF_COMMODITY_PRICES (TIME_PERIOD dimension)...")
    result = await get_data_availability(
        'DF_COMMODITY_PRICES',
        dimension_values={'COMMODITY': 'GOLD', 'INDICATOR': 'COMPRICE', 'FREQ': 'M'},
        agency_id='SPC',
        version='1.0'
    )
    
    print(f"   Has data: {result.get('has_data')}")
    if result.get('time_range'):
        tr = result['time_range']
        print(f"   ✅ Time range found:")
        print(f"      Earliest: {tr.get('earliest')}")
        print(f"      Latest: {tr.get('latest')}")
        print(f"      Periods: {tr.get('periods_available')}")
    else:
        print("   ❌ No time range found")
    
    # Test 2: Try another dataflow
    print("\n2. Testing DF_NATIONAL_ACCOUNTS...")
    result2 = await get_data_availability(
        'DF_NATIONAL_ACCOUNTS',
        dimension_values={'FREQ': 'A', 'GEO_PICT': 'FJ'},
        agency_id='SPC',
        version='1.0'
    )
    
    print(f"   Has data: {result2.get('has_data')}")
    if result2.get('time_range'):
        tr = result2['time_range']
        print(f"   ✅ Time range found:")
        print(f"      Earliest: {tr.get('earliest')}")
        print(f"      Latest: {tr.get('latest')}")
    else:
        print("   ⚠️ No time range (might not have data for this combination)")
    
    # Test 3: Check that time dimension is not in available_dims
    print("\n3. Checking available dimensions (should not include time)...")
    if result.get('other_available_dimensions'):
        dims = list(result['other_available_dimensions'].keys())
        print(f"   Other dimensions: {dims}")
        
        # Check if TIME_PERIOD or any time dimension is incorrectly included
        time_dims = [d for d in dims if 'TIME' in d or 'PERIOD' in d]
        if time_dims:
            print(f"   ❌ Time dimensions incorrectly in available_dims: {time_dims}")
        else:
            print("   ✅ No time dimensions in available_dims (correct)")
    
    print("\n" + "=" * 60)
    print("Test completed!")

if __name__ == "__main__":
    asyncio.run(test_time_period())