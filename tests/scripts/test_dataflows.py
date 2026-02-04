#!/usr/bin/env python3
"""
Test script to find dataflows with actual data using SDMX tools.
"""

import asyncio
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.sdmx_tools import (
    list_dataflows,
    get_dataflow_structure,
    get_data_availability
)

async def test_dataflow_discovery():
    """Test the SDMX discovery workflow to find dataflows with actual data."""
    print("🔍 Starting SDMX Dataflow Discovery...")
    print("=" * 60)
    
    try:
        # Step 1: List available dataflows
        print("\n1. Listing available dataflows from SPC...")
        dataflows_result = await list_dataflows(limit=15)
        
        if "error" in dataflows_result:
            print(f"❌ Error listing dataflows: {dataflows_result['error']}")
            return
        
        print(f"✅ Found {dataflows_result['total_found']} total dataflows")
        print(f"   Showing first {len(dataflows_result['dataflows'])}")
        
        # Display the dataflows
        for i, df in enumerate(dataflows_result['dataflows'], 1):
            print(f"   {i}. {df['id']}: {df['name']}")
        
        # Step 2: Check data availability for each dataflow
        print(f"\n2. Checking data availability for these {len(dataflows_result['dataflows'])} dataflows...")
        
        dataflows_with_data = []
        
        for df in dataflows_result['dataflows']:
            dataflow_id = df['id']
            print(f"\n   Checking {dataflow_id}...")
            
            # Get data availability
            availability_result = await get_data_availability(dataflow_id)
            
            if "error" in availability_result:
                print(f"   ❌ Error checking availability: {availability_result['error']}")
                continue
            
            has_data = availability_result.get("has_constraint", False) or availability_result.get("has_data", False)
            
            if has_data:
                print(f"   ✅ {dataflow_id} HAS DATA!")
                dataflows_with_data.append({
                    "dataflow": df,
                    "availability": availability_result
                })
                
                # Show time range if available
                if "time_range" in availability_result and availability_result["time_range"]:
                    time_range = availability_result["time_range"]
                    print(f"      📅 Time range: {time_range.get('start', 'unknown')} to {time_range.get('end', 'unknown')}")
                
                # Show cube regions if available
                if "cube_regions" in availability_result:
                    regions_count = len(availability_result["cube_regions"])
                    print(f"      🗂️  Available dimension combinations: {regions_count}")
                
            else:
                print(f"   ⚠️  {dataflow_id} appears to have no data")
        
        # Step 3: Detailed analysis of dataflows with data
        print(f"\n3. Detailed analysis of dataflows with data...")
        print("=" * 60)
        
        if not dataflows_with_data:
            print("❌ No dataflows found with actual data")
            return
        
        print(f"✅ Found {len(dataflows_with_data)} dataflows with data:")
        
        for i, item in enumerate(dataflows_with_data, 1):
            df = item["dataflow"]
            availability = item["availability"]
            
            print(f"\n{i}. Dataflow: {df['id']}")
            print(f"   Name: {df['name']}")
            print(f"   Description: {df['description'][:100]}...")
            
            # Get structure information
            print("   Getting structure information...")
            structure_result = await get_dataflow_structure(df['id'])
            
            if "error" not in structure_result and "structure" in structure_result:
                structure = structure_result["structure"]
                dimensions = structure.get("dimensions", [])
                
                print(f"   📊 Dimensions ({len(dimensions)}):")
                for dim in dimensions:
                    print(f"      - {dim['id']} (position {dim['position']}): {dim.get('type', 'unknown')} -> {dim.get('codelist', 'no codelist')}")
                
                if "key_template" in structure:
                    print(f"   🔑 Key template: {structure['key_template']}")
                
                if "key_example" in structure:
                    print(f"   📝 Example key: {structure['key_example']}")
            
            # Show availability details
            if "time_range" in availability and availability["time_range"]:
                time_range = availability["time_range"]
                print(f"   📅 Data available from {time_range.get('start', 'unknown')} to {time_range.get('end', 'unknown')}")
            
            if "cube_regions" in availability and availability["cube_regions"]:
                regions = availability["cube_regions"]
                print(f"   🗂️  {len(regions)} dimension combinations with data")
                
                # Show first few combinations as examples
                for j, region in enumerate(regions[:3]):
                    if "keys" in region:
                        keys = region["keys"]
                        key_str = ", ".join([f"{k}={v}" for k, v in keys.items()][:4])
                        if len(keys) > 4:
                            key_str += f" (and {len(keys)-4} more)"
                        print(f"      Example {j+1}: {key_str}")
                
                if len(regions) > 3:
                    print(f"      ... and {len(regions)-3} more combinations")
        
        print("\n" + "=" * 60)
        print(f"🎉 Discovery complete! Found {len(dataflows_with_data)} dataflows with actual data.")
        
        return dataflows_with_data
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Run the discovery
    dataflows_with_data = asyncio.run(test_dataflow_discovery())