"""
Unit tests for structure diagram functionality.

Tests the get_structure_diagram tool and related helper functions
that generate Mermaid diagrams showing SDMX structure relationships.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models.schemas import StructureDiagramResult, StructureEdge, StructureNode


class TestStructureNodeSchema:
    """Tests for StructureNode Pydantic model."""

    def test_structure_node_creation(self):
        """Test creating a valid StructureNode."""
        node = StructureNode(
            node_id="dataflow_DF_POP",
            structure_type="dataflow",
            id="DF_POP",
            agency="SPC",
            version="1.0",
            name="Population Statistics",
            is_target=True,
        )
        assert node.node_id == "dataflow_DF_POP"
        assert node.structure_type == "dataflow"
        assert node.id == "DF_POP"
        assert node.agency == "SPC"
        assert node.version == "1.0"
        assert node.name == "Population Statistics"
        assert node.is_target is True

    def test_structure_node_defaults(self):
        """Test StructureNode default values."""
        node = StructureNode(
            node_id="codelist_CL_FREQ",
            structure_type="codelist",
            id="CL_FREQ",
            agency="SPC",
            version="1.0",
            name="Frequency",
        )
        assert node.is_target is False


class TestStructureEdgeSchema:
    """Tests for StructureEdge Pydantic model."""

    def test_structure_edge_creation(self):
        """Test creating a valid StructureEdge."""
        edge = StructureEdge(
            source="dataflow_DF_POP",
            target="datastructure_DSD_POP",
            relationship="defines",
            label="defines structure",
        )
        assert edge.source == "dataflow_DF_POP"
        assert edge.target == "datastructure_DSD_POP"
        assert edge.relationship == "defines"
        assert edge.label == "defines structure"

    def test_structure_edge_optional_label(self):
        """Test StructureEdge with no label."""
        edge = StructureEdge(
            source="dsd_DSD_POP",
            target="codelist_CL_FREQ",
            relationship="uses",
        )
        assert edge.label is None


class TestStructureDiagramResultSchema:
    """Tests for StructureDiagramResult Pydantic model."""

    def test_structure_diagram_result_creation(self):
        """Test creating a valid StructureDiagramResult."""
        target = StructureNode(
            node_id="datastructure_DSD_POP",
            structure_type="datastructure",
            id="DSD_POP",
            agency="SPC",
            version="1.0",
            name="Population DSD",
            is_target=True,
        )
        nodes = [target]
        edges = []

        result = StructureDiagramResult(
            target=target,
            direction="both",
            depth=1,
            nodes=nodes,
            edges=edges,
            mermaid_diagram="graph TD\n    DSD_POP[DSD_POP]",
            interpretation=["No relationships found"],
            api_calls_made=1,
        )

        assert result.discovery_level == "structure_relationships"
        assert result.target.id == "DSD_POP"
        assert result.direction == "both"
        assert result.depth == 1
        assert len(result.nodes) == 1
        assert len(result.edges) == 0
        assert "graph TD" in result.mermaid_diagram
        assert result.api_calls_made == 1


class TestMermaidDiagramGeneration:
    """Tests for Mermaid diagram generation."""

    def test_generate_mermaid_diagram_basic(self):
        """Test basic Mermaid diagram generation."""
        from main_server import _generate_mermaid_diagram

        target = StructureNode(
            node_id="dataflow_DF_POP",
            structure_type="dataflow",
            id="DF_POP",
            agency="SPC",
            version="1.0",
            name="Population",
            is_target=True,
        )

        child = StructureNode(
            node_id="datastructure_DSD_POP",
            structure_type="datastructure",
            id="DSD_POP",
            agency="SPC",
            version="1.0",
            name="Population Structure",
            is_target=False,
        )

        nodes = [target, child]
        edges = [
            StructureEdge(
                source="dataflow_DF_POP",
                target="datastructure_DSD_POP",
                relationship="defines",
                label="defines structure",
            )
        ]

        diagram = _generate_mermaid_diagram(target, nodes, edges)

        assert "graph TD" in diagram
        assert "dataflow_DF_POP" in diagram
        assert "datastructure_DSD_POP" in diagram
        assert "defines structure" in diagram
        assert "📊" in diagram  # dataflow icon
        assert "🏗️" in diagram  # datastructure icon

    def test_generate_mermaid_diagram_styling(self):
        """Test that target node gets special styling."""
        from main_server import _generate_mermaid_diagram

        target = StructureNode(
            node_id="codelist_CL_FREQ",
            structure_type="codelist",
            id="CL_FREQ",
            agency="SPC",
            version="1.0",
            name="Frequency",
            is_target=True,
        )

        diagram = _generate_mermaid_diagram(target, [target], [])

        assert "style codelist_CL_FREQ" in diagram
        assert "fill:#e1f5fe" in diagram
        assert "stroke-width:3px" in diagram

    def test_generate_mermaid_diagram_multiple_codelists(self):
        """Test diagram with multiple codelists (common pattern)."""
        from main_server import _generate_mermaid_diagram

        target = StructureNode(
            node_id="datastructure_DSD_POP",
            structure_type="datastructure",
            id="DSD_POP",
            agency="SPC",
            version="1.0",
            name="Population DSD",
            is_target=True,
        )

        codelists = [
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
                source="datastructure_DSD_POP",
                target="codelist_CL_FREQ",
                relationship="uses",
                label="FREQ uses",
            ),
            StructureEdge(
                source="datastructure_DSD_POP",
                target="codelist_CL_GEO",
                relationship="uses",
                label="GEO uses",
            ),
        ]

        nodes = [target] + codelists
        diagram = _generate_mermaid_diagram(target, nodes, edges)

        assert "CL_FREQ" in diagram
        assert "CL_GEO" in diagram
        assert "Codelists" in diagram  # subgraph label

    def test_generate_mermaid_diagram_with_versions(self):
        """Test diagram with show_versions=True displays version numbers."""
        from main_server import _generate_mermaid_diagram

        target = StructureNode(
            node_id="datastructure_DSD_SDG",
            structure_type="datastructure",
            id="DSD_SDG",
            agency="SPC",
            version="3.0",
            name="SDG DSD",
            is_target=True,
        )

        codelists = [
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
                version="2.0",
                name="Geography",
                is_target=False,
            ),
        ]

        nodes = [target] + codelists
        edges = []

        # Test with show_versions=True
        diagram_with_versions = _generate_mermaid_diagram(target, nodes, edges, show_versions=True)

        assert "v3.0" in diagram_with_versions  # target version
        assert "v1.0" in diagram_with_versions  # CL_FREQ version
        assert "v2.0" in diagram_with_versions  # CL_GEO version

        # Test with show_versions=False (default)
        diagram_without_versions = _generate_mermaid_diagram(
            target, nodes, edges, show_versions=False
        )

        # Version numbers should not appear in node labels
        assert "v3.0" not in diagram_without_versions
        assert "v1.0" not in diagram_without_versions
        assert "v2.0" not in diagram_without_versions

    def test_generate_mermaid_diagram_versions_different_values(self):
        """Test that different versions are correctly displayed for each node."""
        from main_server import _generate_mermaid_diagram

        target = StructureNode(
            node_id="dsd_TEST",
            structure_type="datastructure",
            id="DSD_TEST",
            agency="SPC",
            version="2.5",
            name="Test DSD",
            is_target=True,
        )

        nodes = [
            target,
            StructureNode(
                node_id="codelist_CL_A",
                structure_type="codelist",
                id="CL_A",
                agency="SPC",
                version="1.0",
                name="Codelist A",
                is_target=False,
            ),
            StructureNode(
                node_id="codelist_CL_B",
                structure_type="codelist",
                id="CL_B",
                agency="SPC",
                version="3.1",
                name="Codelist B",
                is_target=False,
            ),
            StructureNode(
                node_id="conceptscheme_CS_X",
                structure_type="conceptscheme",
                id="CS_X",
                agency="SPC",
                version="1.2",
                name="Concept Scheme X",
                is_target=False,
            ),
        ]

        diagram = _generate_mermaid_diagram(target, nodes, [], show_versions=True)

        # Each version should appear exactly associated with its node
        assert "DSD_TEST</b> v2.5" in diagram
        assert "CL_A v1.0" in diagram
        assert "CL_B v3.1" in diagram
        assert "CS_X v1.2" in diagram


class TestSDMXClientStructureReferences:
    """Tests for SDMXProgressiveClient.get_structure_references method."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock SDMX client."""
        from sdmx_progressive_client import SDMXProgressiveClient

        client = SDMXProgressiveClient(
            base_url="https://test.example.org/rest",
            agency_id="TEST",
        )
        return client

    def test_classify_relationship_dataflow_to_dsd(self, mock_client):
        """Test relationship classification: dataflow -> datastructure."""
        result = mock_client._classify_relationship("dataflow", "datastructure")
        assert result == "child"

    def test_classify_relationship_dsd_to_codelist(self, mock_client):
        """Test relationship classification: datastructure -> codelist."""
        result = mock_client._classify_relationship("datastructure", "codelist")
        assert result == "child"

    def test_classify_relationship_codelist_to_dsd(self, mock_client):
        """Test relationship classification: codelist <- datastructure (parent)."""
        result = mock_client._classify_relationship("codelist", "datastructure")
        assert result == "parent"

    def test_get_relationship_label_dataflow_dsd(self, mock_client):
        """Test relationship label generation."""
        label = mock_client._get_relationship_label("dataflow", "datastructure")
        assert label == "defines structure"

    def test_get_relationship_label_dsd_codelist(self, mock_client):
        """Test relationship label for DSD -> codelist."""
        label = mock_client._get_relationship_label("datastructure", "codelist")
        assert label == "uses codelist"

    def test_get_relationship_label_unknown(self, mock_client):
        """Test relationship label for unknown relationship."""
        label = mock_client._get_relationship_label("unknown", "other")
        assert label == "references"

    @pytest.mark.asyncio
    async def test_get_structure_references_unsupported_type(self, mock_client):
        """Test error handling for unsupported structure types."""
        result = await mock_client.get_structure_references(
            structure_type="unsupported_type",
            structure_id="TEST_ID",
        )

        assert "error" in result
        assert "unsupported_type" in result["error"].lower()
        assert "supported_types" in result

    @pytest.mark.asyncio
    async def test_get_structure_references_success(self, mock_client):
        """Test successful structure references fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"""<?xml version="1.0" encoding="UTF-8"?>
        <message:Structure xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                           xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                           xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
            <message:Structures>
                <str:Dataflows>
                    <str:Dataflow id="DF_TEST" agencyID="TEST" version="1.0">
                        <com:Name>Test Dataflow</com:Name>
                    </str:Dataflow>
                </str:Dataflows>
                <str:DataStructures>
                    <str:DataStructure id="DSD_TEST" agencyID="TEST" version="1.0">
                        <com:Name>Test DSD</com:Name>
                    </str:DataStructure>
                </str:DataStructures>
            </message:Structures>
        </message:Structure>
        """
        mock_response.raise_for_status = MagicMock()

        with patch.object(mock_client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            result = await mock_client.get_structure_references(
                structure_type="dataflow",
                structure_id="DF_TEST",
                direction="children",
            )

            assert "error" not in result
            assert "target" in result
            assert result["direction"] == "children"

    @pytest.mark.asyncio
    async def test_get_structure_references_not_found(self, mock_client):
        """Test handling of 404 response."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch.object(mock_client, "_get_session") as mock_get_session:
            mock_session = AsyncMock()
            mock_session.get = AsyncMock(return_value=mock_response)
            mock_get_session.return_value = mock_session

            result = await mock_client.get_structure_references(
                structure_type="dataflow",
                structure_id="NONEXISTENT",
            )

            assert "error" in result
            assert result["status_code"] == 404


class TestGetStructureDiagramTool:
    """Tests for the get_structure_diagram MCP tool."""

    @pytest.mark.asyncio
    async def test_get_structure_diagram_error_handling(self):
        """Test error handling in get_structure_diagram tool."""
        from main_server import get_structure_diagram

        mock_client = MagicMock()
        mock_client.agency_id = "TEST"
        mock_client.get_structure_references = AsyncMock(
            return_value={"error": "Test error message"}
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await get_structure_diagram(
                structure_type="dataflow",
                structure_id="DF_TEST",
            )

            assert isinstance(result, StructureDiagramResult)
            assert "Error" in result.mermaid_diagram
            assert "Test error message" in result.interpretation[0]

    @pytest.mark.asyncio
    async def test_get_structure_diagram_success(self):
        """Test successful structure diagram generation."""
        from main_server import get_structure_diagram

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={
                "target": {
                    "type": "dataflow",
                    "id": "DF_POP",
                    "agency": "SPC",
                    "version": "1.0",
                    "name": "Population Statistics",
                },
                "direction": "children",
                "api_calls": 1,
                "parents": [],
                "children": [
                    {
                        "type": "datastructure",
                        "id": "DSD_POP",
                        "agency": "SPC",
                        "version": "1.0",
                        "name": "Population DSD",
                        "relationship": "defines structure",
                    }
                ],
            }
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await get_structure_diagram(
                structure_type="dataflow",
                structure_id="DF_POP",
                direction="children",
            )

            assert isinstance(result, StructureDiagramResult)
            assert result.target.id == "DF_POP"
            assert result.direction == "children"
            assert len(result.nodes) == 2  # target + 1 child
            assert len(result.edges) == 1
            assert "graph TD" in result.mermaid_diagram
            assert "DSD_POP" in result.mermaid_diagram
            assert len(result.interpretation) > 0

    @pytest.mark.asyncio
    async def test_get_structure_diagram_no_relationships(self):
        """Test diagram when no relationships found."""
        from main_server import get_structure_diagram

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={
                "target": {
                    "type": "codelist",
                    "id": "CL_FREQ",
                    "agency": "SPC",
                    "version": "1.0",
                    "name": "Frequency",
                },
                "direction": "both",
                "api_calls": 1,
                "parents": [],
                "children": [],
            }
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await get_structure_diagram(
                structure_type="codelist",
                structure_id="CL_FREQ",
                direction="both",
            )

            assert isinstance(result, StructureDiagramResult)
            assert result.target.id == "CL_FREQ"
            assert len(result.nodes) == 1  # Only target
            assert len(result.edges) == 0
            # Should have interpretation about no relationships
            assert any("No" in interp for interp in result.interpretation)


class TestCompareStructures:
    """Tests for the compare_structures MCP tool."""

    @pytest.mark.asyncio
    async def test_compare_structures_cross_structure(self):
        """Test comparing two different DSD structures."""
        from main_server import compare_structures
        from models.schemas import StructureComparisonResult

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"

        # Mock responses for two different DSDs
        async def mock_get_refs(structure_type, structure_id, **kwargs):
            if structure_id == "DSD_A":
                return {
                    "target": {
                        "type": "datastructure",
                        "id": "DSD_A",
                        "agency": "SPC",
                        "version": "1.0",
                        "name": "DSD A",
                    },
                    "direction": "children",
                    "api_calls": 1,
                    "children": [
                        {
                            "type": "codelist",
                            "id": "CL_FREQ",
                            "version": "1.0",
                            "name": "Frequency",
                        },
                        {"type": "codelist", "id": "CL_GEO", "version": "1.0", "name": "Geography"},
                        {
                            "type": "codelist",
                            "id": "CL_ONLY_A",
                            "version": "1.0",
                            "name": "Only in A",
                        },
                    ],
                }
            else:  # DSD_B
                return {
                    "target": {
                        "type": "datastructure",
                        "id": "DSD_B",
                        "agency": "SPC",
                        "version": "2.0",
                        "name": "DSD B",
                    },
                    "direction": "children",
                    "api_calls": 1,
                    "children": [
                        {
                            "type": "codelist",
                            "id": "CL_FREQ",
                            "version": "1.0",
                            "name": "Frequency",
                        },
                        {
                            "type": "codelist",
                            "id": "CL_GEO",
                            "version": "2.0",
                            "name": "Geography",
                        },  # Version changed!
                        {
                            "type": "codelist",
                            "id": "CL_ONLY_B",
                            "version": "1.0",
                            "name": "Only in B",
                        },
                    ],
                }

        mock_client.get_structure_references = AsyncMock(side_effect=mock_get_refs)

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="datastructure",
                structure_id_a="DSD_A",
                structure_id_b="DSD_B",
            )

            assert isinstance(result, StructureComparisonResult)
            assert result.comparison_type == "cross_structure"
            assert result.structure_type == "datastructure"
            assert result.structure_a.id == "DSD_A"
            assert result.structure_b.id == "DSD_B"

            # Check summary (modified replaces version_changed)
            assert result.summary.added == 1  # CL_ONLY_B
            assert result.summary.removed == 1  # CL_ONLY_A
            assert result.summary.modified == 1  # CL_GEO (version changed)
            assert result.summary.unchanged == 1  # CL_FREQ

            # Check reference_changes (new name) and changes property (backward compat)
            assert len(result.reference_changes) == 4
            assert len(result.changes) == 4  # property alias

    @pytest.mark.asyncio
    async def test_compare_structures_version_comparison(self):
        """Test comparing two versions of the same DSD structure."""
        from main_server import compare_structures
        from models.schemas import StructureComparisonResult

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"

        async def mock_get_refs(structure_type, structure_id, agency_id, version, **kwargs):
            if version == "1.0":
                return {
                    "target": {
                        "type": "datastructure",
                        "id": "DSD_TEST",
                        "agency": "SPC",
                        "version": "1.0",
                        "name": "Test DSD v1",
                    },
                    "direction": "children",
                    "api_calls": 1,
                    "children": [
                        {
                            "type": "codelist",
                            "id": "CL_FREQ",
                            "version": "1.0",
                            "name": "Frequency",
                        },
                    ],
                }
            else:  # version 2.0
                return {
                    "target": {
                        "type": "datastructure",
                        "id": "DSD_TEST",
                        "agency": "SPC",
                        "version": "2.0",
                        "name": "Test DSD v2",
                    },
                    "direction": "children",
                    "api_calls": 1,
                    "children": [
                        {
                            "type": "codelist",
                            "id": "CL_FREQ",
                            "version": "2.0",
                            "name": "Frequency",
                        },  # Upgraded!
                        {
                            "type": "codelist",
                            "id": "CL_NEW",
                            "version": "1.0",
                            "name": "New Codelist",
                        },
                    ],
                }

        mock_client.get_structure_references = AsyncMock(side_effect=mock_get_refs)

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="datastructure",
                structure_id_a="DSD_TEST",
                version_a="1.0",
                version_b="2.0",
            )

            assert result.comparison_type == "version_comparison"
            assert result.structure_a.version == "1.0"
            assert result.structure_b.version == "2.0"
            assert result.summary.added == 1  # CL_NEW
            assert result.summary.modified == 1  # CL_FREQ 1.0 -> 2.0
            assert result.summary.unchanged == 0
            assert result.summary.removed == 0

    @pytest.mark.asyncio
    async def test_compare_structures_no_changes(self):
        """Test comparing identical DSD structures."""
        from main_server import compare_structures

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={
                "target": {
                    "type": "datastructure",
                    "id": "DSD_TEST",
                    "agency": "SPC",
                    "version": "1.0",
                    "name": "Test DSD",
                },
                "direction": "children",
                "api_calls": 1,
                "children": [
                    {"type": "codelist", "id": "CL_FREQ", "version": "1.0", "name": "Frequency"},
                ],
            }
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="datastructure",
                structure_id_a="DSD_TEST",
                structure_id_b="DSD_TEST",
            )

            assert result.summary.total_changes == 0
            assert result.summary.unchanged == 1
            assert "No changes detected" in " ".join(result.interpretation)
            # No diagram when no changes
            assert result.mermaid_diff_diagram is None

    @pytest.mark.asyncio
    async def test_compare_codelists_cross_comparison(self):
        """Test comparing two different codelists by their codes."""
        from main_server import compare_structures

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"

        async def mock_browse_codelist(codelist_id, **kwargs):
            if codelist_id == "CL_A":
                return {
                    "codelist_id": "CL_A",
                    "agency_id": "SPC",
                    "version": "1.0",
                    "name": "Codelist A",
                    "codes": [
                        {"id": "CODE1", "name": "Code One", "description": ""},
                        {"id": "CODE2", "name": "Code Two", "description": ""},
                        {"id": "SHARED", "name": "Shared Code", "description": ""},
                    ],
                }
            else:  # CL_B
                return {
                    "codelist_id": "CL_B",
                    "agency_id": "SPC",
                    "version": "1.0",
                    "name": "Codelist B",
                    "codes": [
                        {"id": "CODE3", "name": "Code Three", "description": ""},
                        {"id": "SHARED", "name": "Shared Code Different Name", "description": ""},
                    ],
                }

        mock_client.browse_codelist = AsyncMock(side_effect=mock_browse_codelist)

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="codelist",
                structure_id_a="CL_A",
                structure_id_b="CL_B",
            )

            assert result.structure_type == "codelist"
            assert result.comparison_type == "cross_structure"
            assert result.structure_a.id == "CL_A"
            assert result.structure_b.id == "CL_B"

            # Check code_changes (not reference_changes for codelists)
            assert len(result.code_changes) == 4
            assert result.summary.added == 1  # CODE3
            assert result.summary.removed == 2  # CODE1, CODE2
            assert result.summary.modified == 1  # SHARED (name changed)
            assert result.summary.unchanged == 0

    @pytest.mark.asyncio
    async def test_compare_codelists_version_comparison(self):
        """Test comparing two versions of the same codelist."""
        from main_server import compare_structures

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"

        async def mock_browse_codelist(codelist_id, version, **kwargs):
            if version == "1.0":
                return {
                    "codelist_id": "CL_TEST",
                    "agency_id": "SPC",
                    "version": "1.0",
                    "name": "Test Codelist v1",
                    "codes": [
                        {"id": "A", "name": "Alpha", "description": ""},
                        {"id": "B", "name": "Beta", "description": ""},
                    ],
                }
            else:  # v2.0
                return {
                    "codelist_id": "CL_TEST",
                    "agency_id": "SPC",
                    "version": "2.0",
                    "name": "Test Codelist v2",
                    "codes": [
                        {"id": "A", "name": "Alpha", "description": ""},  # unchanged
                        {"id": "B", "name": "Bravo", "description": ""},  # name changed
                        {"id": "C", "name": "Charlie", "description": ""},  # added
                    ],
                }

        mock_client.browse_codelist = AsyncMock(side_effect=mock_browse_codelist)

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="codelist",
                structure_id_a="CL_TEST",
                version_a="1.0",
                version_b="2.0",
            )

            assert result.structure_type == "codelist"
            assert result.comparison_type == "version_comparison"
            assert result.structure_a.version == "1.0"
            assert result.structure_b.version == "2.0"

            assert result.summary.added == 1  # C
            assert result.summary.removed == 0
            assert result.summary.modified == 1  # B name changed
            assert result.summary.unchanged == 1  # A

    @pytest.mark.asyncio
    async def test_compare_structures_error_handling(self):
        """Test error handling when DSD structure fetch fails."""
        from main_server import compare_structures

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={"error": "Structure not found"}
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="datastructure",
                structure_id_a="NONEXISTENT",
            )

            assert "Error" in " ".join(result.interpretation)

    @pytest.mark.asyncio
    async def test_compare_codelists_error_handling(self):
        """Test error handling when codelist fetch fails."""
        from main_server import compare_structures

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.browse_codelist = AsyncMock(return_value={"error": "Codelist not found"})

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await compare_structures(
                structure_type="codelist",
                structure_id_a="NONEXISTENT",
            )

            assert result.structure_type == "codelist"
            assert "Error" in " ".join(result.interpretation)


class TestDiffDiagramGeneration:
    """Tests for diff diagram generation."""

    def test_generate_diff_diagram_with_changes(self):
        """Test diff diagram for DSD comparison includes all change types with correct styling."""
        from main_server import _generate_diff_diagram
        from models.schemas import ReferenceChange

        structure_a = StructureNode(
            node_id="dsd_a",
            structure_type="datastructure",
            id="DSD_A",
            agency="SPC",
            version="1.0",
            name="DSD A",
            is_target=True,
        )

        structure_b = StructureNode(
            node_id="dsd_b",
            structure_type="datastructure",
            id="DSD_B",
            agency="SPC",
            version="2.0",
            name="DSD B",
            is_target=False,
        )

        changes = [
            ReferenceChange(
                structure_type="codelist",
                id="CL_ADDED",
                name="Added Codelist",
                version_a=None,
                version_b="1.0",
                change_type="added",
            ),
            ReferenceChange(
                structure_type="codelist",
                id="CL_REMOVED",
                name="Removed Codelist",
                version_a="1.0",
                version_b=None,
                change_type="removed",
            ),
            ReferenceChange(
                structure_type="codelist",
                id="CL_CHANGED",
                name="Changed Codelist",
                version_a="1.0",
                version_b="2.0",
                change_type="version_changed",
            ),
        ]

        diagram = _generate_diff_diagram(structure_a, structure_b, changes)

        # Check structure
        assert "graph LR" in diagram
        assert "DSD_A" in diagram
        assert "DSD_B" in diagram

        # Check change groups
        assert "➕ Added" in diagram
        assert "➖ Removed" in diagram
        assert "🔄 Version Changed" in diagram

        # Check nodes
        assert "CL_ADDED" in diagram
        assert "CL_REMOVED" in diagram
        assert "CL_CHANGED" in diagram

        # Check version transition in changed node
        assert "v1.0 → v2.0" in diagram

        # Check styling (colors)
        assert "#c8e6c9" in diagram  # Green for added
        assert "#ffcdd2" in diagram  # Red for removed
        assert "#fff9c4" in diagram  # Yellow for changed

    def test_generate_diff_diagram_unchanged_summary(self):
        """Test that many unchanged items are summarized in DSD diff diagram."""
        from main_server import _generate_diff_diagram
        from models.schemas import ReferenceChange

        structure_a = StructureNode(
            node_id="dsd_a",
            structure_type="datastructure",
            id="DSD_A",
            agency="SPC",
            version="1.0",
            name="DSD A",
            is_target=True,
        )

        structure_b = StructureNode(
            node_id="dsd_b",
            structure_type="datastructure",
            id="DSD_B",
            agency="SPC",
            version="1.0",
            name="DSD B",
            is_target=False,
        )

        # Create many unchanged references
        changes = [
            ReferenceChange(
                structure_type="codelist",
                id=f"CL_{i}",
                name=f"Codelist {i}",
                version_a="1.0",
                version_b="1.0",
                change_type="unchanged",
            )
            for i in range(10)
        ]

        diagram = _generate_diff_diagram(structure_a, structure_b, changes)

        # Should show summary instead of individual nodes
        assert "10 references unchanged" in diagram

    def test_generate_codelist_diff_diagram(self):
        """Test diff diagram for codelist comparison."""
        from main_server import _generate_codelist_diff_diagram
        from models.schemas import CodeChange

        structure_a = StructureNode(
            node_id="cl_a",
            structure_type="codelist",
            id="CL_A",
            agency="SPC",
            version="1.0",
            name="Codelist A",
            is_target=True,
        )

        structure_b = StructureNode(
            node_id="cl_b",
            structure_type="codelist",
            id="CL_B",
            agency="SPC",
            version="2.0",
            name="Codelist B",
            is_target=False,
        )

        code_changes = [
            CodeChange(
                code_id="NEW_CODE",
                name_a=None,
                name_b="New Code",
                change_type="added",
            ),
            CodeChange(
                code_id="OLD_CODE",
                name_a="Old Code",
                name_b=None,
                change_type="removed",
            ),
            CodeChange(
                code_id="RENAMED",
                name_a="Old Name",
                name_b="New Name",
                change_type="name_changed",
            ),
        ]

        diagram = _generate_codelist_diff_diagram(structure_a, structure_b, code_changes)

        # Check structure
        assert "graph LR" in diagram
        assert "CL_A" in diagram
        assert "CL_B" in diagram

        # Check change groups
        assert "➕ Added Codes" in diagram
        assert "➖ Removed Codes" in diagram
        assert "🔄 Name Changed" in diagram

        # Check nodes
        assert "NEW_CODE" in diagram
        assert "OLD_CODE" in diagram
        assert "RENAMED" in diagram

        # Check styling (colors)
        assert "#c8e6c9" in diagram  # Green for added
        assert "#ffcdd2" in diagram  # Red for removed
        assert "#fff9c4" in diagram  # Yellow for changed

    @pytest.mark.asyncio
    async def test_get_structure_diagram_with_show_versions(self):
        """Test that show_versions=True includes version info in diagram and interpretation."""
        from main_server import get_structure_diagram

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={
                "target": {
                    "type": "datastructure",
                    "id": "DSD_TEST",
                    "agency": "SPC",
                    "version": "2.0",
                    "name": "Test DSD",
                },
                "direction": "children",
                "api_calls": 1,
                "parents": [],
                "children": [
                    {
                        "type": "codelist",
                        "id": "CL_FREQ",
                        "agency": "SPC",
                        "version": "1.0",
                        "name": "Frequency",
                        "relationship": "uses codelist",
                    },
                    {
                        "type": "codelist",
                        "id": "CL_GEO",
                        "agency": "SPC",
                        "version": "3.0",
                        "name": "Geography",
                        "relationship": "uses codelist",
                    },
                ],
            }
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await get_structure_diagram(
                structure_type="datastructure",
                structure_id="DSD_TEST",
                direction="children",
                show_versions=True,
            )

            assert isinstance(result, StructureDiagramResult)

            # Check versions are in Mermaid diagram
            assert "v2.0" in result.mermaid_diagram  # target version
            assert "v1.0" in result.mermaid_diagram  # CL_FREQ version
            assert "v3.0" in result.mermaid_diagram  # CL_GEO version

            # Check versions are in interpretation
            interpretation_text = " ".join(result.interpretation)
            assert "v1.0" in interpretation_text
            assert "v3.0" in interpretation_text

            # Check node objects have correct versions
            freq_node = next((n for n in result.nodes if n.id == "CL_FREQ"), None)
            geo_node = next((n for n in result.nodes if n.id == "CL_GEO"), None)
            assert freq_node is not None and freq_node.version == "1.0"
            assert geo_node is not None and geo_node.version == "3.0"

    @pytest.mark.asyncio
    async def test_get_structure_diagram_without_show_versions(self):
        """Test that show_versions=False (default) does not include version info."""
        from main_server import get_structure_diagram

        mock_client = MagicMock()
        mock_client.agency_id = "SPC"
        mock_client.get_structure_references = AsyncMock(
            return_value={
                "target": {
                    "type": "datastructure",
                    "id": "DSD_TEST",
                    "agency": "SPC",
                    "version": "2.0",
                    "name": "Test DSD",
                },
                "direction": "children",
                "api_calls": 1,
                "parents": [],
                "children": [
                    {
                        "type": "codelist",
                        "id": "CL_FREQ",
                        "agency": "SPC",
                        "version": "1.0",
                        "name": "Frequency",
                        "relationship": "uses codelist",
                    },
                ],
            }
        )

        with patch("main_server.get_session_client", return_value=mock_client):
            result = await get_structure_diagram(
                structure_type="datastructure",
                structure_id="DSD_TEST",
                direction="children",
                show_versions=False,  # Explicitly false
            )

            assert isinstance(result, StructureDiagramResult)

            # Versions should NOT appear in diagram labels
            # (but the nodes still store version info)
            assert "v2.0" not in result.mermaid_diagram
            assert "v1.0" not in result.mermaid_diagram

            # Node objects should still have versions (just not displayed)
            assert result.target.version == "2.0"
            freq_node = next((n for n in result.nodes if n.id == "CL_FREQ"), None)
            assert freq_node is not None and freq_node.version == "1.0"
