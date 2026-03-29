"""Tests for init script tools."""

from unittest.mock import MagicMock, patch


class TestGetInitScriptLogs:
    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_no_init_scripts(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

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
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

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
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.clusters.get.side_effect = Exception("Cluster not found")

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="bad-cluster")
        assert "Failed" in result

    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_with_init_script_events(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_name = "test-cluster"
        cluster.init_scripts = []
        cluster.cluster_log_conf = None

        ev = MagicMock()
        ev.type = MagicMock(value="INIT_SCRIPTS_FINISHED")
        ev.timestamp = 1700000000000
        ev.details.init_scripts = None

        mock_client.clusters.get.return_value = cluster
        mock_client.clusters.events.return_value = iter([ev])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="c-1")
        assert "Init script events" in result

    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_with_dbfs_log_delivery(self, mock_get_client):
        import base64

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_name = "log-cluster"
        cluster.init_scripts = []
        cluster.cluster_log_conf.dbfs.destination = "dbfs:/cluster-logs"
        cluster.cluster_log_conf.s3 = None

        mock_client.clusters.get.return_value = cluster
        mock_client.clusters.events.return_value = iter([])

        container = MagicMock()
        container.path = "dbfs:/cluster-logs/c-1/init_scripts/container1"

        log_file = MagicMock()
        log_file.path = "dbfs:/cluster-logs/c-1/init_scripts/container1/setup.sh.log"
        log_file.file_size = 100

        log_content = "Installing pandas...\nSUCCESS\n"
        read_resp = MagicMock()
        read_resp.data = base64.b64encode(log_content.encode()).decode()

        mock_client.dbfs.list.side_effect = [[container], [log_file]]
        mock_client.dbfs.read.return_value = read_resp

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="c-1")
        assert "Init script log files" in result
        assert "setup.sh.log" in result
        assert "Installing pandas" in result

    @patch("databricks_debug_mcp.tools.init_scripts.get_workspace_client")
    def test_multiple_script_types(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.init_scripts import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        s1 = MagicMock()
        s1.workspace = None
        s1.volumes = MagicMock(destination="/Volumes/init/setup.sh")
        s1.dbfs = None
        s1.abfss = None
        s1.gcs = None
        s1.s3 = None

        s2 = MagicMock()
        s2.workspace = None
        s2.volumes = None
        s2.dbfs = MagicMock(destination="dbfs:/init/install.sh")
        s2.abfss = None
        s2.gcs = None
        s2.s3 = None

        cluster = MagicMock()
        cluster.cluster_name = "multi-init"
        cluster.init_scripts = [s1, s2]
        cluster.cluster_log_conf = None
        mock_client.clusters.get.return_value = cluster
        mock_client.clusters.events.return_value = iter([])

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_init_script_logs"]
        result = tool.fn(cluster_id="c-1")
        assert "volumes:" in result
        assert "dbfs:" in result
