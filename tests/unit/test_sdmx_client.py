"""
Unit tests for SDMX client.
"""

import pytest
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, Mock, patch
import httpx

from sdmx_client import SDMXClient


class TestSDMXClient:
    """Test SDMX client functionality."""
    
    @pytest.fixture
    def client(self):
        """Create SDMX client for testing."""
        return SDMXClient(base_url="https://test.api.org/rest", agency_id="TEST")
    
    @pytest.fixture
    def mock_dataflow_response(self):
        """Mock SDMX dataflow response."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Dataflows>
            <str:Dataflow id="TEST_DF" agencyID="TEST" version="1.0" isFinal="true">
                <com:Name>Test Dataflow</com:Name>
                <com:Description>A test dataflow for unit testing</com:Description>
                <str:Structure>
                    <com:Ref id="TEST_DSD" agencyID="TEST" version="1.0"/>
                </str:Structure>
            </str:Dataflow>
            <str:Dataflow id="TRADE_DF" agencyID="TEST" version="2.0">
                <com:Name>Trade Data</com:Name>
                <com:Description>International trade statistics</com:Description>
            </str:Dataflow>
        </str:Dataflows>
    </str:Structures>
</str:Structure>'''
    
    @pytest.fixture 
    def mock_codelist_response(self):
        """Mock SDMX codelist response."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
               xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <str:Structures>
        <str:Codelists>
            <str:Codelist id="REF_AREA" agencyID="TEST" version="1.0">
                <com:Name>Reference Area</com:Name>
                <str:Code id="TO">
                    <com:Name>Tonga</com:Name>
                    <com:Description>Kingdom of Tonga</com:Description>
                </str:Code>
                <str:Code id="FJ">
                    <com:Name>Fiji</com:Name>
                    <com:Description>Republic of Fiji</com:Description>
                </str:Code>
            </str:Codelist>
        </str:Codelists>
    </str:Structures>
</str:Structure>'''
    
    def test_init(self):
        """Test client initialization."""
        client = SDMXClient("https://example.org/rest/", "AGENCY")
        assert client.base_url == "https://example.org/rest"  # Trailing slash removed
        assert client.agency_id == "AGENCY"
        assert client.session is None
    
    @pytest.mark.asyncio
    async def test_get_session(self, client):
        """Test session creation and reuse."""
        # First call should create session
        session1 = await client._get_session()
        assert isinstance(session1, httpx.AsyncClient)
        assert client.session is session1
        
        # Second call should reuse session
        session2 = await client._get_session()
        assert session2 is session1
    
    @pytest.mark.asyncio
    async def test_close(self, client):
        """Test session cleanup."""
        # Create session
        await client._get_session()
        assert client.session is not None
        
        # Close should clean up
        await client.close()
        assert client.session is None
    
    @pytest.mark.asyncio 
    async def test_discover_dataflows_success(self, client, mock_dataflow_response):
        """Test successful dataflow discovery."""
        mock_response = Mock()
        mock_response.content = mock_dataflow_response.encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            result = await client.discover_dataflows()
            
            assert len(result) == 2
            
            # Check first dataflow
            df1 = result[0]
            assert df1["id"] == "TEST_DF"
            assert df1["agency"] == "TEST"
            assert df1["version"] == "1.0"
            assert df1["name"] == "Test Dataflow"
            assert df1["description"] == "A test dataflow for unit testing"
            assert df1["is_final"] is True
            assert df1["structure_reference"]["id"] == "TEST_DSD"
            
            # Check second dataflow
            df2 = result[1]
            assert df2["id"] == "TRADE_DF"
            assert df2["version"] == "2.0"
            assert df2["is_final"] is False
            
            # Check URL construction
            mock_client.get.assert_called_once()
            call_args = mock_client.get.call_args
            assert "/dataflow/TEST/all/latest" in call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_discover_dataflows_with_parameters(self, client):
        """Test dataflow discovery with custom parameters."""
        mock_response = Mock()
        mock_response.content = b'<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"></str:Structure>'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            await client.discover_dataflows(
                agency_id="CUSTOM",
                resource_id="SPECIFIC",
                version="2.0",
                references="all",
                detail="referencestubs"
            )
            
            # Check URL construction with parameters
            call_args = mock_client.get.call_args
            url = call_args[0][0]
            assert "/dataflow/CUSTOM/SPECIFIC/2.0" in url
            assert "references=all" in url
            assert "detail=referencestubs" in url
    
    @pytest.mark.asyncio
    async def test_discover_dataflows_http_error(self, client):
        """Test dataflow discovery with HTTP error."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404 Not Found", request=Mock(), response=Mock()
        )
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            result = await client.discover_dataflows()
            
            # Should return empty list on error
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_datastructure_success(self, client):
        """Test successful datastructure retrieval."""
        mock_response = Mock()
        mock_response.content = b'<test>content</test>'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            result = await client.get_datastructure("TEST_DF")
            
            assert result["dataflow_id"] == "TEST_DF"
            assert result["agency"] == "TEST"
            assert result["has_structure"] is True
            assert result["raw_size_bytes"] == len(mock_response.content)
            assert "structure_url" in result
    
    @pytest.mark.asyncio
    async def test_get_datastructure_error(self, client):
        """Test datastructure retrieval with error."""
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.side_effect = httpx.ConnectError("Connection failed")
            mock_session.return_value = mock_client
            
            result = await client.get_datastructure("TEST_DF")
            
            assert result["dataflow_id"] == "TEST_DF"
            assert result["has_structure"] is False
            assert "error" in result
    
    @pytest.mark.asyncio
    async def test_get_codelist_success(self, client, mock_codelist_response):
        """Test successful codelist retrieval."""
        mock_response = Mock()
        mock_response.content = mock_codelist_response.encode('utf-8')
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            result = await client.get_codelist("REF_AREA")
            
            assert result["codelist_id"] == "REF_AREA"
            assert result["agency"] == "TEST"
            assert result["total_codes"] == 2
            
            codes = result["codes"]
            assert len(codes) == 2
            
            # Check Tonga code
            tonga = next(c for c in codes if c["id"] == "TO")
            assert tonga["name"] == "Tonga"
            assert tonga["description"] == "Kingdom of Tonga"
            
            # Check Fiji code
            fiji = next(c for c in codes if c["id"] == "FJ")
            assert fiji["name"] == "Fiji"
            assert fiji["description"] == "Republic of Fiji"
    
    @pytest.mark.asyncio
    async def test_get_codelist_with_item_id(self, client):
        """Test codelist retrieval with specific item ID."""
        mock_response = Mock()
        mock_response.content = b'<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"></str:Structure>'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            await client.get_codelist("REF_AREA", item_id="TO")
            
            # Check URL includes item ID
            call_args = mock_client.get.call_args
            url = call_args[0][0]
            assert "/codelist/TEST/REF_AREA/latest/TO" in url
    
    @pytest.mark.asyncio
    async def test_context_integration(self, client):
        """Test Context integration with progress reporting."""
        mock_context = Mock()
        mock_context.info = Mock()
        mock_context.report_progress = AsyncMock()
        
        mock_response = Mock()
        mock_response.content = b'<str:Structure xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"></str:Structure>'
        mock_response.raise_for_status = Mock()
        
        with patch.object(client, '_get_session') as mock_session:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_response
            mock_session.return_value = mock_client
            
            await client.discover_dataflows(ctx=mock_context)
            
            # Check that context methods were called
            assert mock_context.info.call_count > 0
            assert mock_context.report_progress.call_count > 0
            
            # Check progress reporting calls
            progress_calls = mock_context.report_progress.call_args_list
            assert any(call[0][0] == 0 for call in progress_calls)  # Started at 0
            assert any(call[0][0] == 100 for call in progress_calls)  # Ended at 100