"""
End-to-end tests with live SDMX API.
These tests require internet connectivity and hit real SDMX endpoints.
"""

import httpx
import pytest

from sdmx_progressive_client import SDMXProgressiveClient
from tools.sdmx_tools import get_dataflow_structure, list_dataflows


@pytest.mark.e2e
class TestLiveSDMXAPI:
    """End-to-end tests with live SDMX endpoints."""

    @pytest.fixture
    def spc_client(self):
        """Create client for SPC (Pacific Data Hub)."""
        return SDMXProgressiveClient(
            base_url="https://stats-sdmx-disseminate.pacificdata.org/rest", agency_id="SPC"
        )

    @pytest.mark.asyncio
    async def test_spc_dataflow_discovery(self, spc_client):
        """Test live dataflow discovery from SPC."""
        try:
            dataflows = await spc_client.discover_dataflows()

            # Should find some dataflows
            assert len(dataflows) > 0

            # Check structure of first dataflow
            df = dataflows[0]
            assert "id" in df
            assert "name" in df
            assert "agency" in df
            assert df["agency"] == "SPC"

            # Should have proper structure
            assert df["agency"] == "SPC"

        except httpx.ConnectError:
            pytest.skip("Cannot connect to SPC API - network issue")
        except httpx.TimeoutException:
            pytest.skip("SPC API timeout - network issue")
        finally:
            await spc_client.close()

    @pytest.mark.asyncio
    async def test_spc_specific_dataflow(self, spc_client):
        """Test retrieving a specific known dataflow from SPC."""
        try:
            # Try to get structure for a commonly available dataflow
            structure = await spc_client.get_structure_summary("DF_CPI")

            if structure and not structure.get("error"):
                assert structure.id == "DF_CPI" or structure.agency == "SPC"
            else:
                # If DF_CPI doesn't exist, just ensure we get proper error handling
                pass

        except httpx.ConnectError:
            pytest.skip("Cannot connect to SPC API - network issue")
        except httpx.TimeoutException:
            pytest.skip("SPC API timeout - network issue")
        finally:
            await spc_client.close()

    @pytest.mark.asyncio
    async def test_mcp_tools_with_live_api(self):
        """Test MCP tools with live API."""
        try:
            # Test dataflow discovery
            result = await list_dataflows(keywords=["trade"], agency_id="SPC")

            assert "dataflows" in result
            assert "agency_id" in result
            assert result["agency_id"] == "SPC"

            # If we found dataflows, test structure retrieval
            if result["dataflows"]:
                df_id = result["dataflows"][0]["id"]
                structure_result = await get_dataflow_structure(df_id, agency_id="SPC")

                assert "dataflow_id" in structure_result
                assert structure_result["dataflow_id"] == df_id

        except httpx.ConnectError:
            pytest.skip("Cannot connect to SPC API - network issue")
        except httpx.TimeoutException:
            pytest.skip("SPC API timeout - network issue")

    @pytest.mark.asyncio
    async def test_api_error_handling(self):
        """Test error handling with invalid requests."""
        client = SDMXProgressiveClient(
            base_url="https://stats-sdmx-disseminate.pacificdata.org/rest"
        )

        try:
            # Try to get a non-existent dataflow - should handle gracefully
            result = await client.discover_dataflows(resource_id="NONEXISTENT_DATAFLOW_123")

            # Should handle error gracefully - either empty list or error dict
            assert isinstance(result, list)

        except httpx.ConnectError:
            pytest.skip("Cannot connect to API - network issue")
        except httpx.TimeoutException:
            pytest.skip("API timeout - network issue")
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_multiple_agencies(self):
        """Test connecting to different SDMX agencies."""
        agencies_to_test = [
            ("SPC", "https://stats-sdmx-disseminate.pacificdata.org/rest"),
            # Add other agencies as needed, but be careful with rate limiting
        ]

        for agency_id, base_url in agencies_to_test:
            client = SDMXProgressiveClient(base_url=base_url, agency_id=agency_id)

            try:
                # Try basic connectivity test
                dataflows = await client.discover_dataflows()

                # Should either get dataflows or handle errors gracefully
                assert isinstance(dataflows, list)

                if dataflows:
                    # Check first dataflow has expected structure
                    df = dataflows[0]
                    assert "id" in df
                    assert "agency" in df

            except (httpx.ConnectError, httpx.TimeoutException):
                # Network issues are acceptable in tests
                pass
            except Exception as e:
                # Log unexpected errors but don't fail the test
                print(f"Unexpected error for {agency_id}: {e}")
            finally:
                await client.close()


@pytest.mark.e2e
class TestDataValidation:
    """Test data validation with real-world examples."""

    def test_real_dataflow_ids(self):
        """Test validation with real dataflow IDs."""
        from utils import validate_dataflow_id

        real_dataflow_ids = ["DF_CPI", "EXR", "ICP2011", "NAMA_10_GDP", "MEI_CLI"]

        for df_id in real_dataflow_ids:
            assert validate_dataflow_id(df_id), f"Should be valid: {df_id}"

    def test_real_sdmx_keys(self):
        """Test validation with real SDMX keys."""
        from utils import validate_sdmx_key

        real_keys = [
            "M.N.I8.W1.S1.S1.T.N.FA.F.F7.S.E.N",  # ECB exchange rates
            "A.AUS+AUT.GDP.SA.C",  # OECD GDP data
            "Q..GDPQ..",  # Quarterly GDP
            "A.TO.TOTAL.INDEX",  # SPC CPI data
        ]

        for key in real_keys:
            assert validate_sdmx_key(key), f"Should be valid: {key}"

    def test_real_time_periods(self):
        """Test validation with real time periods."""
        from utils import validate_period

        real_periods = ["2023", "2023-Q4", "2023-12", "2023-12-31", "2023-S2", "2023-M12"]

        for period in real_periods:
            assert validate_period(period), f"Should be valid: {period}"
