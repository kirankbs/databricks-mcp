"""Tests for init script tools."""

from unittest.mock import MagicMock, patch


class TestGetInitScriptLogs:
    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_no_init_scripts(self, mock_get_client):
        from databricks_debug_mcp.tools.init_scripts import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_name = "test-cluster"
        cluster.init_scripts = []
        cluster.cluster_log_conf = None
        mock_client.clusters.get.return_value = cluster
        mock_client.clusters.events.return_value = iter([])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="c-1")
        assert "No init scripts configured" in result
        assert "Log delivery not configured" in result

    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_with_scripts(self, mock_get_client):
        from databricks_debug_mcp.tools.init_scripts import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        script = MagicMock()
        script.workspace = MagicMock(destination="/Shared/init/setup.sh")
        script.volumes = None
        script.dbfs = None
        script.abfss = None
        script.gcs = None
        script.s3 = None

        cluster = MagicMock()
        cluster.cluster_name = "my-cluster"
        cluster.init_scripts = [script]
        cluster.cluster_log_conf = None
        mock_client.clusters.get.return_value = cluster
        mock_client.clusters.events.return_value = iter([])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="c-1")
        assert "/Shared/init/setup.sh" in result

    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_cluster_not_found(self, mock_get_client):
        from databricks_debug_mcp.tools.init_scripts import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.clusters.get.side_effect = Exception("Cluster not found")

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="bad-cluster")
        assert "Failed" in result
