"""Tests for cluster investigation tools."""

from unittest.mock import MagicMock, patch


class TestGetClusterInfo:
    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_full_config_autoscale(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        c = MagicMock()
        c.cluster_name = "etl-cluster"
        c.cluster_id = "c-1"
        c.state = MagicMock(value="RUNNING")
        c.spark_version = "14.3.x-scala2.12"
        c.driver_node_type_id = "Standard_DS3_v2"
        c.node_type_id = "Standard_DS4_v2"
        c.autoscale.min_workers = 2
        c.autoscale.max_workers = 10
        c.num_workers = None
        c.spark_conf = {
            "spark.executor.memory": "8g",
            "spark.sql.adaptive.enabled": "true",
            "some.random.setting": "value",
        }
        c.cluster_log_conf.dbfs.destination = "dbfs:/cluster-logs"
        c.cluster_log_conf.s3 = None
        c.termination_reason = None
        mock_client.clusters.get.return_value = c

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_info"]
        result = tool.fn(cluster_id="c-1")

        assert "etl-cluster" in result
        assert "14.3.x" in result
        assert "2–10 (autoscale)" in result
        assert "executor.memory" in result
        assert "DBFS" in result
        # Random setting not in interesting keys
        assert "random.setting" not in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_fixed_workers(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        c = MagicMock()
        c.cluster_name = "fixed-cluster"
        c.cluster_id = "c-2"
        c.state = MagicMock(value="RUNNING")
        c.spark_version = "14.3"
        c.driver_node_type_id = "i3.xlarge"
        c.node_type_id = "i3.xlarge"
        c.autoscale = None
        c.num_workers = 4
        c.spark_conf = {}
        c.cluster_log_conf = None
        c.termination_reason = None
        mock_client.clusters.get.return_value = c

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_info"]
        result = tool.fn(cluster_id="c-2")

        assert "4 (fixed)" in result
        assert "NOT configured" in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_terminated_with_reason(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        c = MagicMock()
        c.cluster_name = "dead-cluster"
        c.cluster_id = "c-3"
        c.state = MagicMock(value="TERMINATED")
        c.spark_version = "14.3"
        c.driver_node_type_id = None
        c.node_type_id = None
        c.autoscale = None
        c.num_workers = 2
        c.spark_conf = {}
        c.cluster_log_conf = None
        c.termination_reason.type = MagicMock(value="USER_REQUEST")
        c.termination_reason.code = MagicMock(value="USER_REQUEST")
        c.termination_reason.parameters = {"username": "admin@example.com"}
        mock_client.clusters.get.return_value = c

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_info"]
        result = tool.fn(cluster_id="c-3")

        assert "Termination" in result
        assert "USER_REQUEST" in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_s3_log_delivery(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        c = MagicMock()
        c.cluster_name = "aws-cluster"
        c.cluster_id = "c-4"
        c.state = MagicMock(value="RUNNING")
        c.spark_version = "14.3"
        c.driver_node_type_id = None
        c.node_type_id = None
        c.autoscale = None
        c.num_workers = 1
        c.spark_conf = {}
        c.cluster_log_conf.dbfs = None
        c.cluster_log_conf.s3.destination = "s3://my-bucket/logs"
        c.termination_reason = None
        mock_client.clusters.get.return_value = c

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_info"]
        result = tool.fn(cluster_id="c-4")

        assert "S3" in result


class TestGetClusterEvents:
    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_events_returned(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        ev1 = MagicMock()
        ev1.type = MagicMock(value="RUNNING")
        ev1.timestamp = 1700000000000
        ev1.details.current_num_workers = 4
        ev1.details.previous_num_workers = 2
        ev1.details.target_num_workers = None
        ev1.details.reason = None

        ev2 = MagicMock()
        ev2.type = MagicMock(value="DRIVER_NOT_RESPONDING")
        ev2.timestamp = 1700000100000
        ev2.details.current_num_workers = None
        ev2.details.target_num_workers = None
        ev2.details.reason = None

        mock_client.clusters.events.return_value = [ev1, ev2]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_events"]
        result = tool.fn(cluster_id="c-1")

        assert "RUNNING" in result
        assert "DRIVER_NOT_RESPONDING" in result
        assert "2 raw" in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_event_type_filter(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        ev1 = MagicMock()
        ev1.type = MagicMock(value="RUNNING")
        ev1.timestamp = 1700000000000
        ev1.details.current_num_workers = None
        ev1.details.target_num_workers = None
        ev1.details.reason = None

        ev2 = MagicMock()
        ev2.type = MagicMock(value="DRIVER_NOT_RESPONDING")
        ev2.timestamp = 1700000100000
        ev2.details = None

        # Only ev2 matches the filter; ev1 is filtered out in the function
        mock_client.clusters.events.return_value = [ev1, ev2]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_events"]
        result = tool.fn(cluster_id="c-1", event_types=["DRIVER_NOT_RESPONDING"])

        assert "DRIVER_NOT_RESPONDING" in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_no_events(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.clusters.events.return_value = []

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_events"]
        result = tool.fn(cluster_id="c-1")

        assert "No cluster events" in result

    @patch("databricks_debug_mcp.tools.clusters.get_workspace_client")
    def test_event_details_workers(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.clusters import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        ev = MagicMock()
        ev.type = MagicMock(value="AUTOSCALING")
        ev.timestamp = 1700000000000
        ev.details.current_num_workers = 8
        ev.details.previous_num_workers = 4
        ev.details.target_num_workers = 8
        ev.details.reason = MagicMock(value="AUTOSCALING_UP")

        mock_client.clusters.events.return_value = [ev]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_events"]
        result = tool.fn(cluster_id="c-1")

        assert "workers:" in result
        assert "4" in result
        assert "8" in result
