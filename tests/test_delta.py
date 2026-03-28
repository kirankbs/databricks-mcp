"""Tests for Delta table health and history tools."""

from unittest.mock import patch


class TestGetTableHealth:
    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_healthy_table(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.side_effect = [
            # DESCRIBE DETAIL
            [{
                "format": "delta",
                "location": "abfss://container@storage.dfs.core.windows.net/table",
                "numFiles": "100",
                "sizeInBytes": str(100 * 128 * 1024 * 1024),  # 100 files * 128MB = healthy
                "partitionColumns": "['date']",
                "properties": "{'delta.autoOptimize.optimizeWrite': 'true'}",
                "minReaderVersion": "1",
                "minWriterVersion": "7",
            }],
            # DESCRIBE HISTORY
            [
                {"operation": "OPTIMIZE", "timestamp": "2024-01-14", "userName": "system", "operationMetrics": {}},
                {"operation": "VACUUM END", "timestamp": "2024-01-13", "userName": "system", "operationMetrics": {}},
                {"operation": "WRITE", "timestamp": "2024-01-12", "userName": "pipeline", "operationMetrics": {}},
            ],
            # Predictive optimization - empty
            [],
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_health"]
        result = tool.fn(table_name="catalog.schema.table")
        assert "128.0 MB" in result
        assert "OPTIMIZE" in result
        assert "VACUUM" in result
        # No health issues for a well-maintained table
        assert "SMALL FILES" not in result

    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_small_files_detected(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.side_effect = [
            # DESCRIBE DETAIL - many small files
            [{
                "format": "delta",
                "location": "abfss://container@storage/table",
                "numFiles": "15000",
                "sizeInBytes": str(15000 * 1024 * 1024),  # 15000 * 1MB = small files
                "partitionColumns": "[]",
                "properties": "{}",
                "minReaderVersion": "1",
                "minWriterVersion": "2",
            }],
            # DESCRIBE HISTORY - no optimize
            [{"operation": "WRITE", "timestamp": "2024-01-15", "userName": "etl", "operationMetrics": {}}],
            # Predictive optimization
            Exception("System table not available"),
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_health"]
        result = tool.fn(table_name="catalog.schema.tiny_files")
        assert "SMALL FILES" in result
        assert "HIGH FILE COUNT" in result
        assert "AUTO-OPTIMIZE not set" in result
        assert "NO RECENT OPTIMIZE" in result

    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_table_not_found(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.side_effect = RuntimeError("Table not found")
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_health"]
        result = tool.fn(table_name="bad.table.name")
        assert "Failed" in result


class TestGetTableHistory:
    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_basic_history(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "version": "5",
                "timestamp": "2024-01-15 10:00:00",
                "operation": "MERGE",
                "userName": "pipeline@svc",
                "operationMetrics": {
                    "numTargetRowsInserted": "500",
                    "numTargetRowsUpdated": "100",
                    "numTargetRowsDeleted": "10",
                },
            },
            {
                "version": "4",
                "timestamp": "2024-01-14 10:00:00",
                "operation": "WRITE",
                "userName": "etl@svc",
                "operationMetrics": {
                    "numOutputRows": "10000",
                    "numOutputBytes": "52428800",
                    "numAddedFiles": "5",
                },
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_history"]
        result = tool.fn(table_name="catalog.schema.events")
        assert "MERGE" in result
        assert "WRITE" in result
        assert "ins: 500" in result
        assert "upd: 100" in result

    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_operation_filter(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {"version": "3", "timestamp": "2024-01-15", "operation": "OPTIMIZE", "userName": "sys", "operationMetrics": {"numFilesAdded": "1", "numFilesRemoved": "50"}},
            {"version": "2", "timestamp": "2024-01-14", "operation": "WRITE", "userName": "etl", "operationMetrics": {}},
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_history"]
        result = tool.fn(table_name="catalog.schema.t", operation_filter="OPTIMIZE")
        assert "OPTIMIZE" in result
        assert "WRITE" not in result

    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_schema_change_detection(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {"version": "6", "timestamp": "2024-01-16", "operation": "SET TBLPROPERTIES", "userName": "admin", "operationMetrics": {}},
            {"version": "5", "timestamp": "2024-01-15", "operation": "WRITE", "userName": "etl", "operationMetrics": {}},
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_table_history"]
        result = tool.fn(table_name="catalog.schema.t")
        assert "Schema/property changes detected" in result


class TestGetPredictiveOptimization:
    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_with_results(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "catalog_name": "main",
                "schema_name": "default",
                "table_name": "events",
                "operation_type": "COMPACTION",
                "start_time": "2024-01-15 02:00:00",
                "end_time": "2024-01-15 02:05:00",
                "usage_quantity": "0.35",
                "usage_unit": "DBU",
                "metrics": "{'numFilesCompacted': 50}",
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_predictive_optimization"]
        result = tool.fn(table_name="main.default.events")
        assert "COMPACTION" in result
        assert "0.35" in result

    @patch("databricks_debug_mcp.tools.delta.execute_sql")
    def test_no_results(self, mock_sql):
        from databricks_debug_mcp.tools.delta import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = []
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_predictive_optimization"]
        result = tool.fn()
        assert "No predictive optimization" in result
