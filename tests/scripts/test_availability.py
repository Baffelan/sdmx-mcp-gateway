#!/usr/bin/env python3
"""
Test script for the enhanced data availability checking.
"""

import asyncio
import json
from tools.sdmx_tools import get_data_availability


async def test_basic_availability():
    """Test basic availability check (original functionality)."""
    print("\n" + "="*60)
    print("TEST 1: Basic Availability Check")
    print("="*60)
    
    result = await get_data_availability(
        dataflow_id="DF_EMPLOYED",
        agency_id="SPC"
    )
    
    print(f"Has constraint: {result.get('has_constraint')}")
    if result.get('time_range'):
        print(f"Time range: {result['time_range']['start']} to {result['time_range']['end']}")
    if result.get('interpretation'):
        print("Interpretation:")
        for interp in result['interpretation']:
            print(f"  - {interp}")
    
    return result


async def test_dimension_combination():
    """Test checking specific dimension combinations."""
    print("\n" + "="*60)
    print("TEST 2: Specific Dimension Combination")
    print("="*60)
    
    # Test 1: Check if Vanuatu has any data
    print("\n2a. Checking if Vanuatu (VU) has any population data...")
    result1 = await get_data_availability(
        dataflow_id="DF_EMPLOYED",
        dimension_values={"GEO_PICT": "VU"},
        agency_id="SPC"
    )
    
    print(f"Has data for Vanuatu: {result1.get('has_data')}")
    if result1.get('time_range'):
        print(f"Time available: {result1['time_range']['earliest']} to {result1['time_range']['latest']}")
    if result1.get('other_available_dimensions'):
        print(f"Other dimensions available: {list(result1['other_available_dimensions'].keys())[:3]}")
    
    # Test 2: Check if Vanuatu has data for 2024
    print("\n2b. Checking if Vanuatu has data for 2024...")
    result2 = await get_data_availability(
        dataflow_id="DF_EMPLOYED",
        dimension_values={"GEO_PICT": "VU", "TIME_PERIOD": "2024"},
        agency_id="SPC"
    )
    
    print(f"Has data for Vanuatu in 2024: {result2.get('has_data')}")
    if result2.get('suggestions'):
        print("Suggestions:")
        for suggestion in result2['suggestions']:
            print(f"  - {suggestion}")
    
    # Test 3: Check if Vanuatu has data for 2020
    print("\n2c. Checking if Vanuatu has data for 2020...")
    result3 = await get_data_availability(
        dataflow_id="DF_EMPLOYED",
        dimension_values={"GEO_PICT": "VU", "TIME_PERIOD": "2020"},
        agency_id="SPC"
    )
    
    print(f"Has data for Vanuatu in 2020: {result3.get('has_data')}")
    
    return result1, result2, result3


async def test_progressive_check():
    """Test progressive availability checking."""
    print("\n" + "="*60)
    print("TEST 3: Progressive Availability Check")
    print("="*60)
    
    print("\nProgressively checking data availability:")
    print("  Step 1: Fiji (FJ)")
    print("  Step 2: Fiji + Population indicator")
    print("  Step 3: Fiji + Population + Year 2024")
    
    result = await get_data_availability(
        dataflow_id="DF_EMPLOYED",
        progressive_check=[
            {"GEO_PICT": "FJ"},
            {"GEO_PICT": "FJ", "INDICATOR": "MIDYEARPOPEST"},
            {"GEO_PICT": "FJ", "INDICATOR": "MIDYEARPOPEST", "TIME_PERIOD": "2024"}
        ],
        agency_id="SPC"
    )
    
    print("\nProgressive Results:")
    for step_result in result.get('progressive_results', []):
        print(f"\nStep {step_result['step']}: {step_result['dimensions']}")
        print(f"  Has data: {step_result['has_data']}")
        if step_result.get('details', {}).get('time_range'):
            time_range = step_result['details']['time_range']
            print(f"  Time range: {time_range.get('earliest')} to {time_range.get('latest')}")
    
    print(f"\nLast valid combination: {result.get('last_valid_combination')}")
    print(f"Recommendation: {result.get('recommendation')}")
    
    return result


async def test_with_ecb_data():
    """Test with ECB endpoint."""
    print("\n" + "="*60)
    print("TEST 4: ECB Data Availability")
    print("="*60)
    
    # First switch to ECB endpoint
    import config
    config.set_endpoint('ECB')
    print(f"Switched to ECB endpoint: {config.SDMX_BASE_URL}")
    
    # Check exchange rates availability
    print("\nChecking EUR/USD exchange rate data availability...")
    
    result = await get_data_availability(
        dataflow_id="EXR",
        progressive_check=[
            {"CURRENCY": "USD"},
            {"CURRENCY": "USD", "FREQ": "D"},
            {"CURRENCY": "USD", "FREQ": "D", "TIME_PERIOD": "2024"}
        ],
        agency_id="ECB"
    )
    
    for step_result in result.get('progressive_results', []):
        print(f"\nStep {step_result['step']}: {step_result['dimensions']}")
        print(f"  Has data: {step_result['has_data']}")
    
    # Switch back to SPC
    config.set_endpoint('SPC')
    
    return result


async def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("ENHANCED DATA AVAILABILITY TESTING")
    print("="*60)
    
    try:
        # Test 1: Basic availability
        await test_basic_availability()
        
        # Test 2: Dimension combinations
        await test_dimension_combination()
        
        # Test 3: Progressive checking
        await test_progressive_check()
        
        # Test 4: Different endpoint
        # await test_with_ecb_data()  # Uncomment to test ECB
        
        print("\n" + "="*60)
        print("ALL TESTS COMPLETED")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())