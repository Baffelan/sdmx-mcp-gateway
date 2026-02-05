"""
Live test script for get_structure_diagram functionality.

Tests the structure diagram tool against real SDMX endpoints to verify
that the Mermaid diagram generation works correctly with actual data.

Usage:
    cd sdmx-mcp-gateway
    uv run python tests/scripts/test_structure_diagram.py
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sdmx_progressive_client import SDMXProgressiveClient


async def test_dataflow_children():
    """Test getting children of a dataflow (should show DSD)."""
    print("\n" + "=" * 70)
    print("TEST 1: Get children of a dataflow (DF_SDG)")
    print("Expected: Should show the DSD that defines this dataflow's structure")
    print("=" * 70)

    client = SDMXProgressiveClient()
    try:
        result = await client.get_structure_references(
            structure_type="dataflow",
            structure_id="DF_SDG",
            agency_id="SPC",
            direction="children",
        )

        if "error" in result:
            print(f"❌ Error: {result['error']}")
            return False

        print(f"\n✅ Target: {result['target']['type']} / {result['target']['id']}")
        print(f"   Name: {result['target'].get('name', 'N/A')}")

        children = result.get("children", [])
        print(f"\n📊 Found {len(children)} children:")
        for child in children:
            print(f"   - {child['type']}: {child['id']} ({child.get('name', 'N/A')})")
            print(f"     Relationship: {child.get('relationship', 'N/A')}")

        return len(children) > 0

    finally:
        await client.close()


async def test_dsd_children():
    """Test getting children of a DSD (should show codelists)."""
    print("\n" + "=" * 70)
    print("TEST 2: Get children of a Data Structure Definition")
    print("Expected: Should show codelists and concept schemes used by this DSD")
    print("=" * 70)

    client = SDMXProgressiveClient()
    try:
        # First, get a DSD ID from a dataflow
        result = await client.get_structure_references(
            structure_type="dataflow",
            structure_id="DF_SDG",
            agency_id="SPC",
            direction="children",
        )

        if "error" in result:
            print(f"❌ Error getting dataflow: {result['error']}")
            return False

        # Find the DSD in children
        dsd_id = None
        for child in result.get("children", []):
            if child["type"] in ("datastructure", "dsd"):
                dsd_id = child["id"]
                break

        if not dsd_id:
            print("❌ No DSD found in dataflow children")
            # Try a known DSD directly
            dsd_id = "DSD_SDG"
            print(f"   Trying known DSD: {dsd_id}")

        print(f"\n🔍 Querying DSD: {dsd_id}")

        result = await client.get_structure_references(
            structure_type="datastructure",
            structure_id=dsd_id,
            agency_id="SPC",
            direction="children",
        )

        if "error" in result:
            print(f"❌ Error: {result['error']}")
            return False

        print(f"\n✅ Target: {result['target']['type']} / {result['target']['id']}")

        children = result.get("children", [])
        print(f"\n📋 Found {len(children)} children:")

        # Group by type
        by_type: dict[str, list] = {}
        for child in children:
            ctype = child["type"]
            if ctype not in by_type:
                by_type[ctype] = []
            by_type[ctype].append(child)

        for ctype, items in by_type.items():
            print(f"\n   {ctype.upper()} ({len(items)}):")
            for item in items[:5]:  # Show first 5
                print(f"      - {item['id']}: {item.get('name', 'N/A')[:40]}")
            if len(items) > 5:
                print(f"      ... and {len(items) - 5} more")

        return len(children) > 0

    finally:
        await client.close()


async def test_codelist_parents():
    """Test getting parents of a codelist (should show what uses it)."""
    print("\n" + "=" * 70)
    print("TEST 3: Get parents of a codelist (CL_FREQ)")
    print("Expected: Should show DSDs and concept schemes that use this codelist")
    print("=" * 70)

    client = SDMXProgressiveClient()
    try:
        result = await client.get_structure_references(
            structure_type="codelist",
            structure_id="CL_FREQ",
            agency_id="SPC",
            direction="parents",
        )

        if "error" in result:
            print(f"❌ Error: {result['error']}")
            # Try with a different codelist
            print("\n🔄 Trying alternate codelist: CL_UNIT_MEASURE")
            result = await client.get_structure_references(
                structure_type="codelist",
                structure_id="CL_UNIT_MEASURE",
                agency_id="SPC",
                direction="parents",
            )

        if "error" in result:
            print(f"❌ Error: {result['error']}")
            return False

        print(f"\n✅ Target: {result['target']['type']} / {result['target']['id']}")

        parents = result.get("parents", [])
        print(f"\n⬆️ Found {len(parents)} parents:")
        for parent in parents[:10]:
            print(f"   - {parent['type']}: {parent['id']}")
            print(f"     Relationship: {parent.get('relationship', 'N/A')}")

        if len(parents) > 10:
            print(f"   ... and {len(parents) - 10} more")

        return True  # Even 0 parents is valid for some codelists

    finally:
        await client.close()


async def test_full_diagram_generation():
    """Test the full diagram generation with the MCP tool."""
    print("\n" + "=" * 70)
    print("TEST 4: Full Mermaid diagram generation")
    print("Expected: Should produce a valid Mermaid diagram")
    print("=" * 70)

    # Import the tool function
    try:
        from main_server import _generate_mermaid_diagram, get_structure_diagram
        from models.schemas import StructureEdge, StructureNode
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

    # Create mock data to test diagram generation
    target = StructureNode(
        node_id="dataflow_DF_TEST",
        structure_type="dataflow",
        id="DF_TEST",
        agency="SPC",
        version="1.0",
        name="Test Dataflow",
        is_target=True,
    )

    nodes = [
        target,
        StructureNode(
            node_id="datastructure_DSD_TEST",
            structure_type="datastructure",
            id="DSD_TEST",
            agency="SPC",
            version="1.0",
            name="Test DSD",
            is_target=False,
        ),
        StructureNode(
            node_id="codelist_CL_FREQ",
            structure_type="codelist",
            id="CL_FREQ",
            agency="SPC",
            version="1.0",
            name="Frequency",
            is_target=False,
        ),
        StructureNode(
            node_id="codelist_CL_GEO",
            structure_type="codelist",
            id="CL_GEO",
            agency="SPC",
            version="1.0",
            name="Geography",
            is_target=False,
        ),
    ]

    edges = [
        StructureEdge(
            source="dataflow_DF_TEST",
            target="datastructure_DSD_TEST",
            relationship="defines",
            label="defines structure",
        ),
        StructureEdge(
            source="datastructure_DSD_TEST",
            target="codelist_CL_FREQ",
            relationship="uses",
            label="FREQ uses",
        ),
        StructureEdge(
            source="datastructure_DSD_TEST",
            target="codelist_CL_GEO",
            relationship="uses",
            label="GEO uses",
        ),
    ]

    diagram = _generate_mermaid_diagram(target, nodes, edges)

    print("\n📊 Generated Mermaid Diagram:")
    print("-" * 50)
    print(diagram)
    print("-" * 50)

    # Validate diagram structure
    checks = [
        ("graph TD" in diagram, "Has graph declaration"),
        ("subgraph" in diagram, "Has subgraphs"),
        ("dataflow_DF_TEST" in diagram, "Has target node"),
        ("codelist_CL_FREQ" in diagram, "Has codelist nodes"),
        ("-->" in diagram, "Has edges"),
        ("style dataflow_DF_TEST" in diagram, "Has target styling"),
        ("📊" in diagram, "Has dataflow icon"),
        ("📋" in diagram, "Has codelist icon"),
    ]

    print("\n✅ Diagram validation:")
    all_passed = True
    for check, desc in checks:
        status = "✓" if check else "✗"
        print(f"   {status} {desc}")
        if not check:
            all_passed = False

    return all_passed


async def test_ecb_endpoint():
    """Test with ECB endpoint to verify cross-endpoint compatibility."""
    print("\n" + "=" * 70)
    print("TEST 5: Test with ECB endpoint")
    print("Expected: Should work with different SDMX providers")
    print("=" * 70)

    client = SDMXProgressiveClient(
        base_url="https://data-api.ecb.europa.eu/service",
        agency_id="ECB",
    )
    try:
        result = await client.get_structure_references(
            structure_type="dataflow",
            structure_id="EXR",  # Exchange rates
            agency_id="ECB",
            direction="children",
        )

        if "error" in result:
            print(f"⚠️ ECB returned error (may be expected): {result['error']}")
            # ECB might not support all reference queries
            return True  # Not a failure, just different behavior

        print(f"\n✅ Target: {result['target']['type']} / {result['target']['id']}")

        children = result.get("children", [])
        print(f"\n📊 Found {len(children)} children:")
        for child in children[:5]:
            print(f"   - {child['type']}: {child['id']}")

        return True

    finally:
        await client.close()


async def test_live_tool_call():
    """Test calling the actual MCP tool with live data."""
    print("\n" + "=" * 70)
    print("TEST 6: Live MCP tool call")
    print("Expected: Should return a complete StructureDiagramResult")
    print("=" * 70)

    try:
        from unittest.mock import MagicMock, patch

        from main_server import get_structure_diagram
        from sdmx_progressive_client import SDMXProgressiveClient

        # Create a real client
        real_client = SDMXProgressiveClient()

        # Patch get_session_client to return our real client
        with patch("main_server.get_session_client", return_value=real_client):
            result = await get_structure_diagram(
                structure_type="dataflow",
                structure_id="DF_SDG",
                agency_id="SPC",
                direction="children",
            )

        await real_client.close()

        print(f"\n✅ Result type: {type(result).__name__}")
        print(f"   Target: {result.target.id}")
        print(f"   Direction: {result.direction}")
        print(f"   Nodes: {len(result.nodes)}")
        print(f"   Edges: {len(result.edges)}")
        print(f"   API calls: {result.api_calls_made}")

        print("\n📝 Interpretation:")
        for line in result.interpretation[:5]:
            print(f"   {line}")

        print("\n📊 Mermaid diagram preview (first 500 chars):")
        print("-" * 50)
        print(result.mermaid_diagram[:500])
        if len(result.mermaid_diagram) > 500:
            print("...")
        print("-" * 50)

        return len(result.nodes) > 0

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Run all tests."""
    print("🧪 SDMX Structure Diagram - Live Integration Tests")
    print("=" * 70)

    tests = [
        ("Dataflow children", test_dataflow_children),
        ("DSD children", test_dsd_children),
        ("Codelist parents", test_codelist_parents),
        ("Mermaid generation", test_full_diagram_generation),
        ("ECB endpoint", test_ecb_endpoint),
        ("Live tool call", test_live_tool_call),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = await test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n❌ Exception in {name}: {e}")
            import traceback

            traceback.print_exc()
            results.append((name, False))

    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)

    passed = 0
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {status}: {name}")
        if success:
            passed += 1

    print(f"\n   Total: {passed}/{len(results)} tests passed")

    return passed == len(results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
