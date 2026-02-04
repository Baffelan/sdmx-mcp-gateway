#!/usr/bin/env python3
"""
Simple test of availability checking with real SPC data.
"""

import asyncio
from tools.sdmx_tools import get_data_availability, list_dataflows


async def main():
    print("Testing Enhanced Data Availability\n")
    print("="*60)
    
    # 1. Use a dataflow we know has data (from the agent's findings)
    print("\n1. Using DF_COMMODITY_PRICES dataflow (known to have data)...")
    dataflow_id = "DF_COMMODITY_PRICES"
    print(f"   Dataflow: {dataflow_id}")
    print("   Description: International commodity prices")
    
    # Test 1: Overall availability
    print("\n2. Checking overall data availability...")
    overall = await get_data_availability(dataflow_id, agency_id="SPC")
    
    print(f"   Has constraint: {overall.get('has_constraint')}")
    if overall.get('interpretation'):
        for msg in overall['interpretation'][:2]:
            print(f"   - {msg}")
    
    # Test 2: Check with a dimension combination
    print("\n3. Testing dimension combination checking...")
    print("   Checking for Monthly Gold commodity price (FREQ=M, COMMODITY=GOLD, INDICATOR=COMPRICE)...")
    
    combo_result = await get_data_availability(
        dataflow_id=dataflow_id,
        dimension_values={"FREQ": "M", "COMMODITY": "GOLD", "INDICATOR": "COMPRICE"},
        agency_id="SPC"
    )
    
    print(f"   Has data for Monthly Gold: {combo_result.get('has_data')}")
    if combo_result.get('time_range'):
        tr = combo_result['time_range']
        print(f"   Time range: {tr.get('earliest')} to {tr.get('latest')}")
    if combo_result.get('other_available_dimensions'):
        dims = list(combo_result['other_available_dimensions'].keys())[:3]
        print(f"   Other dimensions available: {dims}")
    if combo_result.get('suggestions'):
        print(f"   Suggestion: {combo_result['suggestions'][0]}")
    
    # Test 3: Progressive check
    print("\n4. Testing progressive availability check...")
    print("   Progressive path: COMMODITY -> COMMODITY+INDICATOR -> COMMODITY+INDICATOR+FREQ")
    
    progressive_result = await get_data_availability(
        dataflow_id=dataflow_id,
        progressive_check=[
            {"COMMODITY": "GOLD"},
            {"COMMODITY": "GOLD", "INDICATOR": "COMPRICE"},
            {"COMMODITY": "GOLD", "INDICATOR": "COMPRICE", "FREQ": "M"}
        ],
        agency_id="SPC"
    )
    
    print("\n   Progressive results:")
    for step in progressive_result.get('progressive_results', []):
        print(f"   Step {step['step']}: {step['dimensions']}")
        print(f"      Has data: {step['has_data']}")
    
    if progressive_result.get('last_valid_combination'):
        print(f"\n   Last valid combination: {progressive_result['last_valid_combination']}")
    print(f"   Recommendation: {progressive_result.get('recommendation')}")
    
    # Test 4: Check non-existing combination
    print("\n5. Testing non-existing combination...")
    print("   Checking for COMMODITY=UNICORNS (doesn't exist)...")
    
    invalid_result = await get_data_availability(
        dataflow_id=dataflow_id,
        dimension_values={"COMMODITY": "UNICORNS"},
        agency_id="SPC"
    )
    
    print(f"   Has data for UNICORNS: {invalid_result.get('has_data')}")
    if invalid_result.get('suggestions'):
        print(f"   Suggestion: {invalid_result['suggestions'][0]}")
    
    print("\n" + "="*60)
    print("Test completed!")


if __name__ == "__main__":
    asyncio.run(main())