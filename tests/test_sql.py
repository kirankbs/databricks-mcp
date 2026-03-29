"""Tests for SQL execution client."""

from unittest.mock import MagicMock, patch

import pytest

from databricks_debug_mcp.sql import execute_sql


class TestExecuteSql:
    @patch("databricks_debug_mcp.sql.get_workspace_client")
    @patch("databricks_debug_mcp.sql.get_config")
    def test_no_warehouse_id_raises(self, mock_config, mock_client):
        mock_config.return_value = MagicMock(warehouse_id=None)
        with pytest.raises(ValueError, match="No SQL warehouse configured"):
            execute_sql("SELECT 1")

    @patch("databricks_debug_mcp.sql.get_workspace_client")
    @patch("databricks_debug_mcp.sql.get_config")
    def test_successful_query(self, mock_config, mock_client):
        mock_config.return_value = MagicMock(warehouse_id="wh-123")
        w = mock_client.return_value

        # Mock response
        col1 = MagicMock()
        col1.name = "id"
        col2 = MagicMock()
        col2.name = "name"

        resp = MagicMock()
        resp.status.state.value = "SUCCEEDED"
        resp.manifest.schema.columns = [col1, col2]
        resp.result.data_array = [["1", "test"], ["2", "other"]]
        w.statement_execution.execute_statement.return_value = resp

        rows = execute_sql("SELECT * FROM t")
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "test"}
        assert rows[1] == {"id": "2", "name": "other"}

    @patch("databricks_debug_mcp.sql.get_workspace_client")
    @patch("databricks_debug_mcp.sql.get_config")
    def test_failed_query_raises(self, mock_config, mock_client):
        mock_config.return_value = MagicMock(warehouse_id="wh-123")
        w = mock_client.return_value

        resp = MagicMock()
        resp.status.state.value = "FAILED"
        resp.status.error.message = "Table not found"
        w.statement_execution.execute_statement.return_value = resp

        with pytest.raises(RuntimeError, match="Table not found"):
            execute_sql("SELECT * FROM missing")

    @patch("databricks_debug_mcp.sql.get_workspace_client")
    @patch("databricks_debug_mcp.sql.get_config")
    def test_empty_result(self, mock_config, mock_client):
        mock_config.return_value = MagicMock(warehouse_id="wh-123")
        w = mock_client.return_value

        resp = MagicMock()
        resp.status.state.value = "SUCCEEDED"
        resp.manifest.schema.columns = []
        resp.result.data_array = []
        w.statement_execution.execute_statement.return_value = resp

        rows = execute_sql("SELECT * FROM empty")
        assert rows == []

    @patch("databricks_debug_mcp.sql.get_workspace_client")
    @patch("databricks_debug_mcp.sql.get_config")
    def test_explicit_warehouse_id_overrides_config(self, mock_config, mock_client):
        mock_config.return_value = MagicMock(warehouse_id="default-wh")
        w = mock_client.return_value

        resp = MagicMock()
        resp.status.state.value = "SUCCEEDED"
        resp.manifest.schema.columns = []
        resp.result.data_array = []
        w.statement_execution.execute_statement.return_value = resp

        execute_sql("SELECT 1", warehouse_id="override-wh")
        w.statement_execution.execute_statement.assert_called_once()
        call_kwargs = w.statement_execution.execute_statement.call_args
        assert call_kwargs.kwargs["warehouse_id"] == "override-wh"
