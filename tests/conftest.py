"""Shared test fixtures for databricks-debug-mcp tests."""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_workspace_client():
    """Provide a mocked WorkspaceClient and patch get_workspace_client."""
    mock_client = MagicMock()
    with patch("databricks_debug_mcp.client._client", None):
        with patch("databricks_debug_mcp.client.WorkspaceClient", return_value=mock_client):
            # Also patch get_workspace_client in tool modules that import it
            with patch("databricks_debug_mcp.client.get_config") as mock_cfg:
                mock_cfg.return_value = MagicMock(profile="TEST")
                yield mock_client


@pytest.fixture
def mock_sql():
    """Patch execute_sql to return controlled results."""
    with patch("databricks_debug_mcp.sql.execute_sql") as mock:
        yield mock


@pytest.fixture
def mcp_server():
    """Return the FastMCP instance with all tools registered."""
    from databricks_debug_mcp.server import mcp

    return mcp
