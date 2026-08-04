"""
Pytest configuration and shared fixtures.
"""

import asyncio
from unittest.mock import Mock

import pytest

from sdmx_progressive_client import clear_dataflow_cache


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def _clear_dataflow_cache():
    """The dataflow-listing cache in sdmx_progressive_client is module-level,
    shared across every client instance and every test in this run. Without
    this, whichever test populates a given cache key first decides what
    every later test with the same key sees, making results order-dependent.
    """
    clear_dataflow_cache()
    yield
    clear_dataflow_cache()


@pytest.fixture
def mock_context():
    """Create a mock Context object for testing."""
    context = Mock()
    context.info = Mock()
    context.report_progress = Mock()

    # Make report_progress async
    async def async_report_progress(current, total):
        return None

    context.report_progress = async_report_progress
    return context


@pytest.fixture
def sample_dataflow():
    """Sample dataflow data for testing."""
    return {
        "id": "TEST_DF",
        "agency": "TEST",
        "version": "1.0",
        "name": "Test Dataflow",
        "description": "A sample dataflow for testing",
        "is_final": True,
        "structure_reference": {
            "id": "TEST_DSD",
            "agency": "TEST",
            "version": "1.0"
        },
        "data_url_template": "https://test.api/data/TEST,TEST_DF,1.0/{key}/{provider}",
        "metadata_url": "https://test.api/dataflow/TEST/TEST_DF/1.0"
    }


@pytest.fixture
def sample_codelist():
    """Sample codelist data for testing."""
    return {
        "codelist_id": "TEST_CL",
        "agency": "TEST",
        "version": "1.0",
        "total_codes": 2,
        "codes": [
            {"id": "CODE1", "name": "First Code", "description": "Description of first code"},
            {"id": "CODE2", "name": "Second Code", "description": "Description of second code"}
        ],
        "url": "https://test.api/codelist/TEST/TEST_CL/1.0"
    }


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "e2e: End-to-end tests that require live API connectivity"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take a long time to run"
    )
    config.addinivalue_line(
        "markers", "unit: Fast unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests with mocked dependencies"
    )
