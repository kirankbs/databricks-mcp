"""Tests for Spark analysis tools."""

from unittest.mock import MagicMock, patch


class TestAnalyzeStageSkew:
    @patch("databricks_debug_mcp.tools.spark_analysis.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_analysis.get_spark_app_id", return_value="app-123")
    @patch("databricks_debug_mcp.tools.spark_analysis.assert_cluster_running", return_value=None)
    def test_skewed_stage(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        # First call: get stages
        stages = [
            {
                "stageId": 5,
                "attemptId": 0,
                "numTasks": 200,
                "name": "HashAggregate at MyJob.scala:42",
                "status": "COMPLETE",
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
                "shuffleReadBytes": 5_000_000_000,
                "executorRunTime": 300000,
            }
        ]
        # Second call: task summary with heavy skew
        task_summary = {
            "executorRunTime": [100, 500, 1500, 3000, 8000, 12000, 60000],  # max 60x median
            "memoryBytesSpilled": [0, 0, 0, 0, 0, 0, 0],
            "diskBytesSpilled": [0, 0, 0, 0, 0, 0, 0],
            "shuffleReadBytes": [0, 0, 100000, 500000, 1000000, 2000000, 5000000000],
        }
        mock_ui.side_effect = [stages, task_summary]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_stage_skew"]
        result = tool.fn(cluster_id="cluster-1")
        assert "SKEWED STAGES" in result
        assert "Stage 5" in result
        assert "40.0x" in result  # 60000/1500

    @patch("databricks_debug_mcp.tools.spark_analysis.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_analysis.get_spark_app_id", return_value="app-123")
    @patch("databricks_debug_mcp.tools.spark_analysis.assert_cluster_running", return_value=None)
    def test_spill_detected(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        stages = [
            {
                "stageId": 2,
                "attemptId": 0,
                "numTasks": 50,
                "name": "SortMergeJoin",
                "status": "COMPLETE",
                "memoryBytesSpilled": 2_000_000_000,
                "diskBytesSpilled": 500_000_000,
                "shuffleReadBytes": 1_000_000_000,
                "executorRunTime": 120000,
            }
        ]
        task_summary = {
            "executorRunTime": [1000, 2000, 2500, 3000, 3500, 4000, 5000],
            "memoryBytesSpilled": [0, 0, 100000, 500000, 1000000, 2000000, 50000000],
            "diskBytesSpilled": [0, 0, 0, 100000, 500000, 1000000, 10000000],
            "shuffleReadBytes": [0, 100000, 500000, 1000000, 5000000, 10000000, 50000000],
        }
        mock_ui.side_effect = [stages, task_summary]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_stage_skew"]
        result = tool.fn(cluster_id="cluster-2")
        assert "STAGES WITH SPILL" in result
        assert "Stage 2" in result

    @patch("databricks_debug_mcp.tools.spark_analysis.assert_cluster_running")
    def test_terminated_cluster(self, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        mock_assert.return_value = "Cluster not RUNNING"
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_stage_skew"]
        result = tool.fn(cluster_id="dead-cluster")
        assert "not RUNNING" in result


class TestGetSparkConfig:
    @patch("databricks_debug_mcp.tools.spark_analysis.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_analysis.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_analysis.assert_cluster_running", return_value=None)
    def test_config_with_antipatterns(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        mock_ui.return_value = {
            "sparkProperties": [
                ["spark.sql.adaptive.enabled", "false"],
                ["spark.sql.shuffle.partitions", "5"],
                ["spark.executor.memory", "4g"],
                ["spark.driver.memory", "8g"],
                ["spark.dynamicAllocation.enabled", "false"],
            ],
            "systemProperties": [],
            "classpathEntries": [],
        }

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_config"]
        result = tool.fn(cluster_id="cluster-1")
        assert "AQE" in result and "DISABLED" in result
        assert "shuffle.partitions=5" in result
        assert "CONFIGURATION CONCERNS" in result


class TestGetLibraryStatus:
    @patch("databricks_debug_mcp.tools.spark_analysis.get_workspace_client")
    def test_libraries_with_failure(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        lib1 = MagicMock()
        lib1.library.pypi.package = "pandas==2.0.0"
        lib1.library.maven = None
        lib1.library.jar = None
        lib1.library.whl = None
        lib1.library.egg = None
        lib1.status = MagicMock()
        lib1.status.value = "INSTALLED"
        lib1.messages = []

        lib2 = MagicMock()
        lib2.library.pypi.package = "broken-lib==1.0"
        lib2.library.maven = None
        lib2.library.jar = None
        lib2.library.whl = None
        lib2.library.egg = None
        lib2.status = MagicMock()
        lib2.status.value = "FAILED"
        lib2.messages = ["Installation failed: package not found"]

        statuses = MagicMock()
        statuses.library_statuses = [lib1, lib2]
        mock_client.libraries.cluster_status.return_value = statuses

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_library_status"]
        result = tool.fn(cluster_id="cluster-1")
        assert "pandas" in result
        assert "FAILED LIBRARIES" in result
        assert "broken-lib" in result

    @patch("databricks_debug_mcp.tools.spark_analysis.get_workspace_client")
    def test_no_libraries(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_analysis import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        statuses = MagicMock()
        statuses.library_statuses = []
        mock_client.libraries.cluster_status.return_value = statuses

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_library_status"]
        result = tool.fn(cluster_id="cluster-1")
        assert "No libraries" in result
