"""
Integration tests for MCP tools.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
import json

from tools.sdmx_tools import (
    list_dataflows, get_dataflow_structure, explore_codelist,
    validate_query_syntax, build_data_query
)


class TestMCPToolsIntegration:
    """Test MCP tools with mocked SDMX client."""
    
    @pytest.fixture
    def mock_dataflows(self):
        """Mock dataflow data."""
        return [
            {
                "id": "TRADE_FOOD",
                "agency": "SPC", 
                "version": "1.0",
                "name": "Food Trade Statistics",
                "description": "International trade in food products",
                "is_final": True,
                "structure_reference": {"id": "TRADE_DSD", "agency": "SPC", "version": "1.0"}
            },
            {
                "id": "FISHERIES",
                "agency": "SPC",
                "version": "2.0", 
                "name": "Fisheries Production",
                "description": "Commercial fishing and aquaculture data",
                "is_final": True,
                "structure_reference": {"id": "FISH_DSD", "agency": "SPC", "version": "2.0"}
            },
            {
                "id": "TOURISM_STATS",
                "agency": "SPC",
                "version": "1.5",
                "name": "Tourism Statistics", 
                "description": "Visitor arrivals and tourism revenue",
                "is_final": False,
                "structure_reference": None
            }
        ]
    
    @pytest.fixture
    def mock_codelist_data(self):
        """Mock codelist data."""
        return {
            "codelist_id": "REF_AREA",
            "agency": "SPC",
            "version": "latest",
            "total_codes": 3,
            "codes": [
                {"id": "TO", "name": "Tonga", "description": "Kingdom of Tonga"},
                {"id": "FJ", "name": "Fiji", "description": "Republic of Fiji"},
                {"id": "WS", "name": "Samoa", "description": "Independent State of Samoa"}
            ],
            "url": "https://test.api/codelist/SPC/REF_AREA/latest"
        }
    
    @pytest.mark.asyncio
    async def test_list_dataflows_no_keywords(self, mock_dataflows):
        """Test listing all dataflows without keyword filtering."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.discover_dataflows.return_value = mock_dataflows
            
            result = await list_dataflows()
            
            assert result["agency_id"] == "SPC"
            assert result["total_dataflows"] == 3
            assert result["filtered_dataflows"] == 3
            assert result["keywords"] is None
            assert len(result["dataflows"]) == 3
            assert "next_steps" in result
    
    @pytest.mark.asyncio
    async def test_list_dataflows_with_keywords(self, mock_dataflows):
        """Test listing dataflows with keyword filtering."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.discover_dataflows.return_value = mock_dataflows
            
            result = await list_dataflows(keywords=["food", "trade"])
            
            # Should find TRADE_FOOD dataflow (matches both keywords)
            assert result["filtered_dataflows"] == 1
            assert result["dataflows"][0]["id"] == "TRADE_FOOD"
            assert result["dataflows"][0]["relevance_score"] == 2
    
    @pytest.mark.asyncio 
    async def test_list_dataflows_no_matches(self, mock_dataflows):
        """Test listing dataflows with keywords that don't match anything."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.discover_dataflows.return_value = mock_dataflows
            
            result = await list_dataflows(keywords=["nonexistent"])
            
            assert result["filtered_dataflows"] == 0
            assert len(result["dataflows"]) == 0
    
    @pytest.mark.asyncio
    async def test_list_dataflows_error(self):
        """Test error handling in list_dataflows."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.discover_dataflows.side_effect = Exception("Network error")
            
            result = await list_dataflows()
            
            assert "error" in result
            assert result["dataflows"] == []
            assert "Network error" in result["error"]
    
    @pytest.mark.asyncio
    async def test_get_dataflow_structure_success(self):
        """Test successful dataflow structure retrieval."""
        mock_structure = {
            "dataflow_id": "TRADE_FOOD",
            "agency": "SPC", 
            "version": "1.0",
            "has_structure": True,
            "structure_url": "https://test.api/datastructure/SPC/TRADE_FOOD/1.0"
        }
        
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.get_datastructure.return_value = mock_structure
            
            result = await get_dataflow_structure("TRADE_FOOD")
            
            assert result["dataflow_id"] == "TRADE_FOOD" 
            assert result["agency_id"] == "SPC"
            assert result["structure"]["has_structure"] is True
            assert "next_steps" in result
    
    @pytest.mark.asyncio
    async def test_get_dataflow_structure_error(self):
        """Test error handling in get_dataflow_structure."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.get_datastructure.side_effect = Exception("Structure not found")
            
            result = await get_dataflow_structure("NONEXISTENT")
            
            assert "error" in result
            assert result["structure"] is None
    
    @pytest.mark.asyncio
    async def test_explore_codelist_success(self, mock_codelist_data):
        """Test successful codelist exploration."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.get_codelist.return_value = mock_codelist_data
            
            result = await explore_codelist("REF_AREA")
            
            assert result["codelist_id"] == "REF_AREA"
            assert result["total_codes"] == 3
            assert result["filtered_codes"] == 3
            assert len(result["codes"]) == 3
            assert "next_steps" in result
    
    @pytest.mark.asyncio
    async def test_explore_codelist_with_search(self, mock_codelist_data):
        """Test codelist exploration with search term."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.get_codelist.return_value = mock_codelist_data
            
            result = await explore_codelist("REF_AREA", search_term="tonga")
            
            # Should find only Tonga
            assert result["filtered_codes"] == 1
            assert result["codes"][0]["id"] == "TO"
            assert result["search_term"] == "tonga"
    
    @pytest.mark.asyncio
    async def test_explore_codelist_error(self):
        """Test error handling in explore_codelist.""" 
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.get_codelist.return_value = {"error": "Codelist not found", "codes": []}
            
            result = await explore_codelist("NONEXISTENT")
            
            assert "error" in result
            assert result["codes"] == []
    
    def test_validate_query_syntax_valid(self):
        """Test validation of valid query syntax."""
        result = validate_query_syntax(
            dataflow_id="TRADE_FOOD",
            key="A.TO.FISH",
            provider="SPC",
            start_period="2020",
            end_period="2023"
        )
        
        assert result["validation"]["is_valid"] is True
        assert len(result["validation"]["errors"]) == 0
        assert "next_steps" in result
    
    def test_validate_query_syntax_invalid_dataflow(self):
        """Test validation with invalid dataflow ID."""
        result = validate_query_syntax(
            dataflow_id="123INVALID",  # Starts with number
            key="A.TO.FISH"
        )
        
        assert result["validation"]["is_valid"] is False
        assert len(result["validation"]["errors"]) > 0
        assert "must start with letter" in result["validation"]["errors"][0]
    
    def test_validate_query_syntax_invalid_period(self):
        """Test validation with invalid period format."""
        result = validate_query_syntax(
            dataflow_id="TRADE_FOOD",
            start_period="invalid-date"
        )
        
        assert result["validation"]["is_valid"] is False
        assert any("period format invalid" in error.lower() for error in result["validation"]["errors"])
    
    def test_validate_query_syntax_warnings(self):
        """Test validation warnings for potentially problematic queries."""
        result = validate_query_syntax(
            dataflow_id="TRADE_FOOD",
            key="all"  # No time constraints
        )
        
        assert result["validation"]["is_valid"] is True
        assert len(result["validation"]["warnings"]) > 0
        assert "large datasets" in result["validation"]["warnings"][0]
    
    def test_build_data_query_basic(self):
        """Test basic data query building."""
        result = build_data_query(
            dataflow_id="TRADE_FOOD",
            key="A.TO.FISH", 
            start_period="2020",
            end_period="2023"
        )
        
        assert result["dataflow_id"] == "TRADE_FOOD"
        assert result["primary_format"] == "csv"
        assert "primary_url" in result
        assert "primary_headers" in result
        assert "all_formats" in result
        assert "example_usage" in result
        
        # Check URL construction
        url = result["primary_url"]
        assert "/data/SPC,TRADE_FOOD,latest/A.TO.FISH/all" in url
        assert "startPeriod=2020" in url
        assert "endPeriod=2023" in url
    
    def test_build_data_query_different_formats(self):
        """Test data query building with different formats."""
        for format_type in ["csv", "json", "xml"]:
            result = build_data_query(
                dataflow_id="TEST_DF",
                format_type=format_type
            )
            
            assert result["primary_format"] == format_type
            assert format_type in result["all_formats"]
            
            # Check headers are format-specific
            headers = result["primary_headers"]
            assert "Accept" in headers
            if format_type == "csv":
                assert "csv" in headers["Accept"]
            elif format_type == "json":
                assert "json" in headers["Accept"]
            elif format_type == "xml":
                assert "xml" in headers["Accept"]
    
    def test_build_data_query_with_parameters(self):
        """Test data query building with various parameters."""
        result = build_data_query(
            dataflow_id="TEST_DF",
            key="M.TO+FJ.TRADE",
            provider="SPC+ECB",
            dimension_at_observation="AllDimensions",
            detail="dataonly",
            agency_id="CUSTOM",
            version="2.0"
        )
        
        # Check parameters are included
        params = result["query_parameters"]
        assert params["key"] == "M.TO+FJ.TRADE"
        assert params["provider"] == "SPC+ECB"
        assert params["dimension_at_observation"] == "AllDimensions"
        assert params["detail"] == "dataonly"
        
        # Check URL includes custom agency and version
        url = result["primary_url"]
        assert "/data/CUSTOM,TEST_DF,2.0/" in url
        assert "dimensionAtObservation=AllDimensions" in url
        assert "detail=dataonly" in url
    
    def test_build_data_query_error_handling(self):
        """Test error handling in build_data_query."""
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            # Mock client to raise an exception
            mock_client.base_url = None  # This should cause an error
            
            result = build_data_query("TEST_DF")
            
            assert "error" in result
    
    @pytest.mark.asyncio
    async def test_context_integration(self, mock_dataflows):
        """Test Context integration across tools."""
        mock_context = Mock()
        mock_context.info = Mock()
        mock_context.report_progress = AsyncMock()
        
        with patch('tools.sdmx_tools.sdmx_client') as mock_client:
            mock_client.discover_dataflows.return_value = mock_dataflows
            
            # Test with context parameter (note: current tools don't accept ctx parameter directly)
            # This would require modifying the tool signatures to accept Context
            result = await list_dataflows(keywords=["food"])
            
            # For now, just verify the result is correct
            assert result["filtered_dataflows"] == 1