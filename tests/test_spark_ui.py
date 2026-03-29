"""Tests for Spark UI tools (live cluster metrics)."""

from unittest.mock import patch


class TestGetSparkStages:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_sorted_by_duration(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "stageId": 0,
                "status": "COMPLETE",
                "numCompleteTasks": 50,
                "numTasks": 50,
                "executorRunTime": 10000,
                "shuffleReadBytes": 100,
                "shuffleWriteBytes": 200,
                "name": "short stage",
            },
            {
                "stageId": 1,
                "status": "COMPLETE",
                "numCompleteTasks": 200,
                "numTasks": 200,
                "executorRunTime": 300000,
                "shuffleReadBytes": 5000000,
                "shuffleWriteBytes": 3000000,
                "name": "long stage",
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_stages"]
        result = tool.fn(cluster_id="c-1")

        # Stage 1 (300s) should appear before stage 0 (10s)
        assert "long stage" in result
        assert "short stage" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_sort_by_shuffle_read(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "stageId": 0,
                "executorRunTime": 100000,
                "shuffleReadBytes": 5_000_000_000,
                "status": "COMPLETE",
                "numCompleteTasks": 10,
                "numTasks": 10,
                "name": "big shuffle",
            },
            {
                "stageId": 1,
                "executorRunTime": 200000,
                "shuffleReadBytes": 100,
                "status": "COMPLETE",
                "numCompleteTasks": 10,
                "numTasks": 10,
                "name": "small shuffle",
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_stages"]
        result = tool.fn(cluster_id="c-1", sort_by="shuffleRead")
        assert "sorted by shuffleRead" in result

    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    def test_invalid_status(self, mock_ui, mock_app, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_stages"]
        result = tool.fn(cluster_id="c-1", status="INVALID")
        assert "Invalid status" in result

    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running")
    def test_cluster_not_running(self, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_assert.return_value = "Cluster not RUNNING"
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_stages"]
        result = tool.fn(cluster_id="dead")
        assert "not RUNNING" in result


class TestGetExecutorMemory:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_healthy_executors(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "id": "0",
                "memoryUsed": 100_000_000,
                "maxMemory": 1_000_000_000,
                "totalGCTime": 5000,
                "totalTasks": 100,
                "totalDuration": 60000,
                "totalInputBytes": 500_000_000,
                "memoryMetrics": {
                    "usedOnHeapStorageMemory": 50_000_000,
                    "totalOnHeapStorageMemory": 500_000_000,
                    "usedOffHeapStorageMemory": 0,
                    "totalOffHeapStorageMemory": 100_000_000,
                },
                "peakMemoryMetrics": {},
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_executor_memory"]
        result = tool.fn(cluster_id="c-1")
        assert "Executor 0" in result
        assert "HIGH" not in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_high_memory_warning(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "id": "1",
                "memoryUsed": 900_000_000,
                "maxMemory": 1_000_000_000,
                "totalGCTime": 30000,
                "totalTasks": 50,
                "totalDuration": 120000,
                "totalInputBytes": 200_000_000,
                "memoryMetrics": {},
                "peakMemoryMetrics": {},
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_executor_memory"]
        result = tool.fn(cluster_id="c-1")
        assert "HIGH" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_critical_peak_memory(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "id": "2",
                "memoryUsed": 500_000_000,
                "maxMemory": 1_000_000_000,
                "totalGCTime": 50000,
                "totalTasks": 200,
                "totalDuration": 300000,
                "totalInputBytes": 1_000_000_000,
                "memoryMetrics": {},
                "peakMemoryMetrics": {
                    "JVMHeapMemory": 950_000_000,
                    "JVMOffHeapMemory": 50_000_000,
                    "PythonWorkerUsedMemory": 200_000_000,
                    "DirectPoolMemory": 10_000_000,
                    "MappedPoolMemory": 0,
                },
            }
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_executor_memory"]
        result = tool.fn(cluster_id="c-1")
        assert "CRITICAL" in result

    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running")
    def test_cluster_not_running(self, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_assert.return_value = "Cluster not RUNNING"
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_executor_memory"]
        result = tool.fn(cluster_id="dead")
        assert "not RUNNING" in result


class TestGetSparkExecutors:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_executor_list_with_totals(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "id": "driver",
                "hostPort": "10.0.0.1:5000",
                "totalCores": 4,
                "memoryUsed": 100000,
                "diskUsed": 0,
                "activeTasks": 0,
                "completedTasks": 10,
                "failedTasks": 0,
                "totalShuffleRead": 0,
                "totalShuffleWrite": 0,
            },
            {
                "id": "0",
                "hostPort": "10.0.0.2:5000",
                "totalCores": 8,
                "memoryUsed": 500000,
                "diskUsed": 1000,
                "activeTasks": 2,
                "completedTasks": 50,
                "failedTasks": 1,
                "totalShuffleRead": 1000000,
                "totalShuffleWrite": 500000,
            },
            {
                "id": "1",
                "hostPort": "10.0.0.3:5000",
                "totalCores": 8,
                "memoryUsed": 400000,
                "diskUsed": 500,
                "activeTasks": 1,
                "completedTasks": 45,
                "failedTasks": 0,
                "totalShuffleRead": 900000,
                "totalShuffleWrite": 400000,
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_executors"]
        result = tool.fn(cluster_id="c-1")
        assert "3 total" in result
        assert "TOTAL" in result
        assert "driver" in result

    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running")
    def test_cluster_not_running(self, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_assert.return_value = "Cluster not RUNNING"
        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_executors"]
        result = tool.fn(cluster_id="dead")
        assert "not RUNNING" in result


class TestGetSparkSqlQueries:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_top_queries(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {"id": 1, "status": "COMPLETED", "duration": 30000, "description": "SELECT * FROM t1"},
            {"id": 2, "status": "COMPLETED", "duration": 120000, "description": "INSERT INTO t2"},
            {"id": 3, "status": "RUNNING", "duration": 5000, "description": "MERGE INTO t3"},
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_sql_queries"]
        result = tool.fn(cluster_id="c-1")
        assert "Top 3" in result
        assert "by duration" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_specific_query_with_plan(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "id": 5,
                "status": "COMPLETED",
                "duration": 60000,
                "description": "SELECT count(*) FROM events",
                "physicalPlanDescription": "HashAggregate(keys=[], output=[count#1])",
                "runningJobIds": [],
                "successJobIds": [10, 11],
                "failedJobIds": [],
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_sql_queries"]
        result = tool.fn(cluster_id="c-1", query_id=5)
        assert "Query 5" in result
        assert "Physical plan" in result
        assert "HashAggregate" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_query_not_found(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [{"id": 1, "status": "COMPLETED", "duration": 1000}]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_sql_queries"]
        result = tool.fn(cluster_id="c-1", query_id=999)
        assert "not found" in result


class TestGetSparkJobs:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_jobs_list(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {
                "jobId": 0,
                "status": "SUCCEEDED",
                "numActiveStages": 0,
                "numCompletedStages": 3,
                "numFailedStages": 0,
                "numCompletedTasks": 200,
                "numTasks": 200,
                "submissionTime": "2024-01-15T10:00:00Z",
                "name": "count at MyJob:42",
            },
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_jobs"]
        result = tool.fn(cluster_id="c-1")
        assert "SUCCEEDED" in result
        assert "200/200" in result

    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    def test_invalid_status(self, mock_ui, mock_app, mock_assert):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_spark_jobs"]
        result = tool.fn(cluster_id="c-1", status="INVALID")
        assert "Invalid status" in result


class TestGetStreamingQueries:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_html")
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_rest_api_success(self, mock_assert, mock_app, mock_ui, mock_html):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = [
            {"name": "my-stream", "isActive": True, "id": "uuid-1"},
        ]
        mock_html.return_value = "<html></html>"

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_streaming_queries"]
        result = tool.fn(cluster_id="c-1")
        assert "my-stream" in result
        assert "ACTIVE" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_html")
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_no_queries(self, mock_assert, mock_app, mock_ui, mock_html):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = []
        mock_html.return_value = "<html></html>"

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_streaming_queries"]
        result = tool.fn(cluster_id="c-1")
        assert "No streaming queries" in result


class TestGetStreamingQueryProgress:
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_rest_response(self, mock_assert, mock_app, mock_ui):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.return_value = {
            "name": "events-stream",
            "id": "uuid-1",
            "runId": "run-abc",
            "isActive": True,
            "timestamp": "2024-01-15T10:00:00Z",
            "recentProgress": [
                {
                    "inputRowsPerSecond": 1500.0,
                    "processedRowsPerSecond": 2000.0,
                    "batchId": 42,
                    "durationMs": {"addBatch": 500, "queryPlanning": 100},
                },
            ],
        }

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_streaming_query_progress"]
        result = tool.fn(cluster_id="c-1", run_id="run-abc")
        assert "run-abc" in result
        assert "1500" in result

    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_html")
    @patch("databricks_debug_mcp.tools.spark_ui.spark_ui_request")
    @patch("databricks_debug_mcp.tools.spark_ui.get_spark_app_id", return_value="app-1")
    @patch("databricks_debug_mcp.tools.spark_ui.assert_cluster_running", return_value=None)
    def test_both_fail(self, mock_assert, mock_app, mock_ui, mock_html):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.spark_ui import register

        mock_ui.side_effect = Exception("REST failed")
        mock_html.side_effect = Exception("HTML failed")

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_streaming_query_progress"]
        result = tool.fn(cluster_id="c-1", run_id="run-abc")
        assert "Failed" in result


class TestParseStreamingHtml:
    def test_active_and_completed(self):
        from databricks_debug_mcp.tools.spark_ui import _parse_streaming_queries_html_parts

        html = """
        <tr>
            <td>my-stream</td><td>ACTIVE</td><td>uuid-1</td>
            <td><a href="?id=run-1">run-1</a></td>
            <td>2024-01-15</td><td>1h 30m</td><td>500/s</td><td>600/s</td><td>42</td>
        </tr>
        <tr>
            <td>dead-stream</td><td>FAILED</td><td>uuid-2</td>
            <td><a href="?id=run-2">run-2</a></td>
            <td>2024-01-14</td><td>0h 5m</td><td>0/s</td><td>0/s</td><td>3</td>
            <td><pre>java.lang.OOM</pre></td>
        </tr>
        """

        active, completed = _parse_streaming_queries_html_parts(html)
        assert "my-stream" in active
        assert "dead-stream" in completed
        assert "OOM" in completed

    def test_empty_html(self):
        from databricks_debug_mcp.tools.spark_ui import _parse_streaming_queries_html_parts

        active, completed = _parse_streaming_queries_html_parts("<html></html>")
        assert active == ""
        assert completed == ""


class TestCleanHtml:
    def test_strips_tags(self):
        from databricks_debug_mcp.tools.spark_ui import _clean_html

        result = _clean_html("<b>hello</b> <i>world</i>")
        assert "hello" in result
        assert "world" in result
        assert "<b>" not in result

    def test_decodes_entities(self):
        from databricks_debug_mcp.tools.spark_ui import _clean_html

        assert ">" in _clean_html("a &gt; b")
