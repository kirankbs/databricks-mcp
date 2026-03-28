"""Tests for DLT pipeline tools."""

from unittest.mock import MagicMock, patch


class TestGetPipelineStatus:
    @patch("databricks_debug_mcp.tools.pipelines.get_workspace_client")
    def test_by_id(self, mock_get_client):
        from databricks_debug_mcp.tools.pipelines import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        pipeline = MagicMock()
        pipeline.pipeline_id = "p-123"
        pipeline.state = MagicMock(value="RUNNING")
        pipeline.creator_user_name = "admin@co.com"
        pipeline.spec.name = "ETL Pipeline"
        pipeline.spec.catalog = "main"
        pipeline.spec.target = "silver"
        pipeline.spec.channel = "CURRENT"
        pipeline.spec.continuous = False
        pipeline.spec.clusters = []
        mock_client.pipelines.get.return_value = pipeline

        updates = MagicMock()
        updates.updates = [
            MagicMock(update_id="u-1", state=MagicMock(value="COMPLETED"), cause=MagicMock(value="USER_ACTION"), creation_time="2024-01-15"),
        ]
        mock_client.pipelines.list_updates.return_value = updates

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_pipeline_status"]
        result = tool.fn(pipeline_id="p-123")
        assert "ETL Pipeline" in result
        assert "main" in result
        assert "silver" in result

    @patch("databricks_debug_mcp.tools.pipelines.get_workspace_client")
    def test_no_args(self, mock_get_client):
        from databricks_debug_mcp.tools.pipelines import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_pipeline_status"]
        result = tool.fn()
        assert "Provide" in result


class TestGetPipelineErrors:
    @patch("databricks_debug_mcp.tools.pipelines.get_workspace_client")
    def test_with_errors(self, mock_get_client):
        from databricks_debug_mcp.tools.pipelines import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        error_event = MagicMock()
        error_event.level = "ERROR"
        error_event.timestamp = "2024-01-15 10:00:00"
        error_event.event_type = "flow_progress"
        error_event.message = "Schema mismatch in silver_events"
        error_event.error = None

        mock_client.pipelines.list_pipeline_events.return_value = iter([error_event])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_pipeline_errors"]
        result = tool.fn(pipeline_id="p-123")
        assert "Schema mismatch" in result
        assert "ERROR" in result

    @patch("databricks_debug_mcp.tools.pipelines.get_workspace_client")
    def test_no_errors(self, mock_get_client):
        from databricks_debug_mcp.tools.pipelines import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.pipelines.list_pipeline_events.return_value = iter([])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_pipeline_errors"]
        result = tool.fn(pipeline_id="p-123")
        assert "No error" in result
