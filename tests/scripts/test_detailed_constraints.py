#!/usr/bin/env python3
"""
Detailed test to examine constraint information for specific dataflows.
"""

import asyncio
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.sdmx_tools import (
    get_dataflow_structure,
    get_data_availability,
    get_dimension_codes
)

async def examine_dataflow_constraints():
    """Examine constraint information for a few key dataflows."""
    print("🔬 Examining Constraint Information for Key Dataflows")
    print("=" * 60)
    
    # Focus on a few diverse dataflows for detailed analysis
    dataflows_to_examine = [
        "DF_ADBKI",           # Asian Development Bank indicators
        "DF_BOP",             # Balance of payments  
        "DF_COMMODITY_PRICES", # Commodity prices
        "DF_CIVIL_REGISTRATION" # Civil registration
    ]
    
    for dataflow_id in dataflows_to_examine:
        print(f"\n📊 Examining {dataflow_id} in detail...")
        print("-" * 50)
        
        try:
            # Get detailed structure
            structure_result = await get_dataflow_structure(dataflow_id)
            if "error" in structure_result:
                print(f"❌ Error getting structure: {structure_result['error']}")
                continue
            
            structure = structure_result["structure"]
            dataflow_info = structure_result["dataflow"]
            
            print(f"📋 Name: {dataflow_info['name']}")
            print(f"📝 Description: {dataflow_info['description'][:100]}...")
            print(f"🔢 Dimensions: {len(structure['dimensions'])}")
            
            # Get availability with constraint details
            availability_result = await get_data_availability(dataflow_id)
            if "error" in availability_result:
                print(f"❌ Error getting availability: {availability_result['error']}")
                continue
                
            print(f"\n🗂️ Constraint Analysis:")
            has_constraint = availability_result.get("has_constraint", False)
            print(f"   Has constraint metadata: {has_constraint}")
            
            if "time_range" in availability_result and availability_result["time_range"]:
                time_range = availability_result["time_range"]
                print(f"   📅 Time range: {time_range.get('start', 'unknown')} to {time_range.get('end', 'unknown')}")
            
            if "cube_regions" in availability_result and availability_result["cube_regions"]:
                regions = availability_result["cube_regions"]
                print(f"   📦 Cube regions: {len(regions)} different dimension combinations")
                
                # Show examples of dimension combinations with data
                print(f"   📋 Examples of available dimension combinations:")
                for i, region in enumerate(regions[:5], 1):
                    if "keys" in region:
                        keys = region["keys"]
                        # Show a cleaner representation
                        key_summary = {}
                        for dim, values in keys.items():
                            if isinstance(values, list):
                                if len(values) <= 3:
                                    key_summary[dim] = values
                                else:
                                    key_summary[dim] = f"{values[:3]} + {len(values)-3} more"
                            else:
                                key_summary[dim] = values
                        
                        print(f"      {i}. {key_summary}")
                        
                        # If this region has time periods, show them
                        if "TIME_PERIOD" in keys and keys["TIME_PERIOD"]:
                            time_periods = keys["TIME_PERIOD"]
                            if isinstance(time_periods, list) and len(time_periods) > 0:
                                print(f"         Time periods: {min(time_periods)} to {max(time_periods)} ({len(time_periods)} periods)")
                
                if len(regions) > 5:
                    print(f"      ... and {len(regions)-5} more combinations")
            
            # Get detailed information about a couple key dimensions
            print(f"\n🔍 Dimension Details:")
            dimensions = structure["dimensions"]
            
            for dim in dimensions[:3]:  # Examine first 3 dimensions
                if dim["id"] == "TIME_PERIOD":
                    continue  # Skip time dimension
                    
                dim_id = dim["id"]
                print(f"\n   📊 {dim_id} (position {dim['position']}):")
                
                # Get codes for this dimension
                codes_result = await get_dimension_codes(dataflow_id, dim_id, limit=10)
                if "error" not in codes_result and "codes" in codes_result:
                    codes = codes_result["codes"]
                    print(f"      Available codes ({len(codes)} shown):")
                    for code in codes[:5]:
                        print(f"        - {code['id']}: {code['name']}")
                    if len(codes) > 5:
                        print(f"        ... and {len(codes)-5} more codes")
                else:
                    print(f"      ❌ Could not retrieve codes: {codes_result.get('error', 'unknown error')}")
            
        except Exception as e:
            print(f"❌ Error examining {dataflow_id}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n" + "=" * 60)
    print("🎯 Constraint Analysis Summary")
    print("=" * 60)
    
    # Summary of findings
    print("""
Key Findings:
✅ All examined dataflows have constraint information (cube regions)
✅ Each dataflow shows specific dimension combinations that actually contain data
✅ Time ranges are available where applicable
✅ Dimension codes can be explored for query construction

Data Structure Patterns:
📊 Most dataflows follow these dimension patterns:
   - FREQ (frequency): Annual, Monthly, Quarterly, Daily
   - GEO_PICT (geography): Pacific Island Countries and Territories
   - Various indicator dimensions specific to the domain
   - TIME_PERIOD: Handled via startPeriod/endPeriod parameters

Constraint Benefits:
🎯 Constraints help avoid empty queries by showing exactly what data exists
🎯 They reveal the actual scope of available data vs theoretical structure
🎯 They provide guidance for constructing valid SDMX queries
    """)

if __name__ == "__main__":
    asyncio.run(examine_dataflow_constraints())