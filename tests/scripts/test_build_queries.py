#!/usr/bin/env python3
"""
Test building actual data query URLs for dataflows with data.
"""

import asyncio
import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tools.sdmx_tools import (
    build_data_url,
    build_sdmx_key
)

async def build_example_queries():
    """Build example data query URLs for some of the dataflows with actual data."""
    print("🔗 Building Example Data Query URLs")
    print("=" * 60)
    
    examples = [
        {
            "dataflow": "DF_ADBKI",
            "description": "Asian Development Bank indicators for Fiji, Annual frequency",
            "dimensions": {
                "FREQ": "A",
                "GEO_PICT": "FJ",
                "INDICATOR": "NGDP_R_PTX_PS"  # GDP growth rate
            },
            "time_range": ("2015", "2023")
        },
        {
            "dataflow": "DF_BOP", 
            "description": "Balance of payments for Tonga, quarterly data",
            "dimensions": {
                "FREQ": "Q",
                "GEO_PICT": "TO",
                "INDICATOR": "AMTUSD",
                "ACCOUNT": "BAL_CCA"  # Current account balance
            },
            "time_range": ("2020", "2023")
        },
        {
            "dataflow": "DF_COMMODITY_PRICES",
            "description": "Monthly gold and aluminum prices",
            "dimensions": {
                "FREQ": "M",
                "COMMODITY": "GOLD",
                "INDICATOR": "COMPRICE"
            },
            "time_range": ("2023-01", "2023-12")
        },
        {
            "dataflow": "DF_CIVIL_REGISTRATION",
            "description": "Birth registration completeness for Pacific region",
            "dimensions": {
                "FREQ": "A",
                "GEO_PICT": "_T",  # Total Pacific region
                "INDICATOR": "CBR12M"  # Birth registration within 12 months
            },
            "time_range": ("2015", "2022")
        }
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"\n{i}. {example['description']}")
        print("-" * 50)
        
        try:
            # First, build the SDMX key
            print("🔑 Building SDMX key...")
            key_result = await build_sdmx_key(
                dataflow_id=example["dataflow"],
                dimensions=example["dimensions"]
            )
            
            if "error" in key_result:
                print(f"❌ Error building key: {key_result['error']}")
                continue
                
            key = key_result["key"]
            print(f"   Key: {key}")
            print(f"   Key explanation: {key_result['key_explanation']}")
            
            # Show dimension breakdown
            if "dimension_breakdown" in key_result:
                print("   Dimension breakdown:")
                for breakdown in key_result["dimension_breakdown"]:
                    print(f"     {breakdown['meaning']}")
            
            # Then build the data URL
            print("\n🔗 Building data URL...")
            start_period, end_period = example["time_range"]
            
            url_result = await build_data_url(
                dataflow_id=example["dataflow"],
                key=key,
                start_period=start_period,
                end_period=end_period,
                format_type="csv"
            )
            
            if "error" in url_result:
                print(f"❌ Error building URL: {url_result['error']}")
                continue
            
            print(f"   📊 Dataflow: {url_result['dataflow_id']}")
            print(f"   🗓️  Time range: {start_period} to {end_period}")
            print(f"   📄 Format: {url_result['format']}")
            print(f"   🌐 URL: {url_result['url']}")
            print(f"   💡 Usage: {url_result['usage']}")
            
            # Test JSON format as well
            json_result = await build_data_url(
                dataflow_id=example["dataflow"],
                key=key,
                start_period=start_period,
                end_period=end_period,
                format_type="json"
            )
            
            if "error" not in json_result:
                print(f"   🌐 JSON URL: {json_result['url']}")
            
        except Exception as e:
            print(f"❌ Error with example {i}: {e}")
            import traceback
            traceback.print_exc()
    
    # Show how to query all data for a dataflow
    print(f"\n" + "=" * 60)
    print("🌍 Example: Getting ALL data from a dataflow")
    print("=" * 60)
    
    try:
        all_data_result = await build_data_url(
            dataflow_id="DF_COMMODITY_PRICES",
            key="all",  # All dimensions
            format_type="csv"
        )
        
        if "error" not in all_data_result:
            print(f"All commodity prices (WARNING: Large dataset):")
            print(f"URL: {all_data_result['url']}")
            print(f"This would return data for all commodities, all indicators, all frequencies")
        
    except Exception as e:
        print(f"❌ Error building all-data example: {e}")
    
    print(f"\n" + "=" * 60)
    print("✅ Query Building Summary")
    print("=" * 60)
    
    print("""
Key Steps for SDMX Query Construction:
1️⃣  Use list_dataflows() to find dataflows of interest
2️⃣  Use get_dataflow_structure() to understand dimensions 
3️⃣  Use get_data_availability() to see what data actually exists
4️⃣  Use get_dimension_codes() to find valid codes for dimensions
5️⃣  Use build_sdmx_key() to construct properly ordered keys
6️⃣  Use build_data_url() to generate the final data retrieval URL

The resulting URLs can be used directly with:
- curl for command line data retrieval
- HTTP requests from any programming language
- Browser download for small datasets
- Data analysis tools that support SDMX REST APIs

All URLs support multiple formats:
📄 CSV: ?format=csv (default, easiest for analysis)
📊 JSON: ?format=jsondata (structured data)
📋 XML: Standard SDMX format (most complete metadata)
    """)

if __name__ == "__main__":
    asyncio.run(build_example_queries())