"""Tests for lineage tools."""

from unittest.mock import patch


class TestGetTableLineage:
    @patch("databricks_debug_mcp.tools.lineage.execute_sql")
    def test_both_directions(self, mock_sql):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.lineage import register

        mock_sql.side_effect = [
            # upstream
            [
                {
                    "source_table_full_name": "raw.events.clicks",
                    "source_type": "TABLE",
                    "entity_type": "JOB",
                    "event_time": "2024-01-15",
                }
            ],
            # downstream
            [
                {
                    "target_table_full_name": "gold.analytics.daily_stats",
                    "target_type": "TABLE",
                    "entity_type": "NOTEBOOK",
                    "event_time": "2024-01-15",
                }
            ],
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_lineage"]
        result = tool.fn(table_name="silver.events.processed")
        assert "UPSTREAM" in result
        assert "raw.events.clicks" in result
        assert "DOWNSTREAM" in result
        assert "gold.analytics.daily_stats" in result

    @patch("databricks_debug_mcp.tools.lineage.execute_sql")
    def test_upstream_only(self, mock_sql):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.lineage import register

        mock_sql.return_value = [
            {
                "source_table_full_name": "raw.data.source",
                "source_type": "TABLE",
                "entity_type": "PIPELINE",
                "event_time": "2024-01-15",
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_lineage"]
        result = tool.fn(table_name="silver.table", direction="upstream")
        assert "UPSTREAM" in result
        assert "DOWNSTREAM" not in result

    @patch("databricks_debug_mcp.tools.lineage.execute_sql")
    def test_no_lineage(self, mock_sql):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.lineage import register

        mock_sql.return_value = []
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_lineage"]
        result = tool.fn(table_name="isolated.table", direction="upstream")
        assert "No upstream" in result


class TestGetColumnLineage:
    @patch("databricks_debug_mcp.tools.lineage.execute_sql")
    def test_basic(self, mock_sql):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.lineage import register

        mock_sql.return_value = [
            {
                "source_table_full_name": "raw.events.clicks",
                "source_column_name": "user_id",
                "target_column_name": "customer_id",
                "entity_type": "JOB",
                "event_time": "2024-01-15",
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_column_lineage"]
        result = tool.fn(table_name="gold.customers", column_name="customer_id")
        assert "customer_id" in result
        assert "user_id" in result
        assert "raw.events.clicks" in result

    @patch("databricks_debug_mcp.tools.lineage.execute_sql")
    def test_no_lineage(self, mock_sql):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.lineage import register

        mock_sql.return_value = []
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_column_lineage"]
        result = tool.fn(table_name="t")
        assert "No column lineage" in result
