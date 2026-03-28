"""Tests for system tables tools."""

from unittest.mock import patch
import pytest


class TestGetJobCost:
    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_no_criteria(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_job_cost"]
        result = tool.fn(job_id=None, job_name=None, run_id=None)
        assert "Provide" in result
        mock_sql.assert_not_called()

    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_by_run_id(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {"job_id": "123", "run_id": "456", "sku_name": "JOBS_COMPUTE", "usage_date": "2024-01-15", "total_dbu": "10.5", "estimated_cost_usd": "5.25"}
        ]
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_job_cost"]
        result = tool.fn(run_id=456)
        assert "456" in result
        assert "10.50" in result
        assert "$5.25" in result

    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_no_billing_data(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = []
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_job_cost"]
        result = tool.fn(job_id=123)
        assert "No billing data" in result

    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_sql_error(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.side_effect = RuntimeError("Access denied")
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_job_cost"]
        result = tool.fn(job_id=123)
        assert "Failed" in result


class TestGetQueryHistory:
    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_basic_query(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "statement_id": "s1",
                "executed_by": "user@company.com",
                "statement_text": "SELECT * FROM big_table",
                "statement_type": "SELECT",
                "total_duration_ms": "45000",
                "rows_produced": "1000000",
                "produced_rows_byte_count": "500000000",
                "read_bytes": "2000000000",
                "error_message": None,
                "execution_start_time_utc": "2024-01-15 10:30:00",
                "warehouse_id": "wh-1",
            }
        ]
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_query_history"]
        result = tool.fn()
        assert "user@company.com" in result
        assert "SELECT * FROM big_table" in result
        assert "45.0s" in result

    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_error_only(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "statement_id": "s2",
                "executed_by": "user@co.com",
                "statement_text": "SELECT bad_col FROM t",
                "statement_type": "SELECT",
                "total_duration_ms": "100",
                "rows_produced": "0",
                "produced_rows_byte_count": "0",
                "read_bytes": "0",
                "error_message": "Column not found: bad_col",
                "execution_start_time_utc": "2024-01-15 11:00:00",
                "warehouse_id": "wh-1",
            }
        ]
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_query_history"]
        result = tool.fn(error_only=True)
        assert "ERROR" in result
        assert "bad_col" in result


class TestGetAuditEvents:
    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_basic(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "event_time": "2024-01-15 09:00:00",
                "user_email": "admin@company.com",
                "service_name": "clusters",
                "action_name": "delete",
                "table_name": None,
                "status_code": "200",
                "error_message": None,
                "source_ip_address": "10.0.0.1",
            }
        ]
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_audit_events"]
        result = tool.fn()
        assert "clusters" in result
        assert "delete" in result
        assert "200" in result


class TestGetClusterUtilization:
    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_basic(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = [
            {
                "node_id": "node-001",
                "instance_id": "i-abc",
                "private_ip": "10.0.0.1",
                "node_type": "DRIVER",
                "start_time": "2024-01-15 08:00:00",
                "end_time": "2024-01-15 10:00:00",
                "duration_sec": "7200",
            },
            {
                "node_id": "node-002",
                "instance_id": "i-def",
                "private_ip": "10.0.0.2",
                "node_type": "WORKER",
                "start_time": "2024-01-15 08:05:00",
                "end_time": "2024-01-15 09:30:00",
                "duration_sec": "5100",
            },
        ]
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_utilization"]
        result = tool.fn(cluster_id="cluster-abc")
        assert "cluster-abc" in result
        assert "Driver nodes" in result
        assert "Unique workers" in result

    @patch("databricks_debug_mcp.tools.system_tables.execute_sql")
    def test_no_data(self, mock_sql):
        from databricks_debug_mcp.tools.system_tables import register
        from mcp.server.fastmcp import FastMCP

        mock_sql.return_value = []
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_utilization"]
        result = tool.fn(cluster_id="cluster-xyz")
        assert "No node timeline" in result
