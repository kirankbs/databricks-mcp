"""Tests for event log tools (post-mortem debugging)."""

from unittest.mock import patch

from databricks_debug_mcp.event_log import EventLogConfig


def _make_stage_completed(stage_id, name, num_tasks, sub_time, comp_time, accums=None):
    return {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": stage_id,
            "Stage Attempt ID": 0,
            "Stage Name": name,
            "Number of Tasks": num_tasks,
            "Submission Time": sub_time,
            "Completion Time": comp_time,
            "Accumulables": accums or [],
        },
    }


def _make_task_end(
    stage_id,
    task_id,
    executor_id,
    launch,
    finish,
    failed=False,
    reason="Success",
    description="",
    gc_time=0,
    spill_mem=0,
    spill_disk=0,
    shuffle_read=0,
    shuffle_write=0,
    input_bytes=0,
):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": 0,
        "Task End Reason": {"Reason": reason, "Description": description},
        "Task Info": {
            "Task ID": task_id,
            "Executor ID": executor_id,
            "Host": f"10.0.0.{executor_id}",
            "Launch Time": launch,
            "Finish Time": finish,
            "Failed": failed,
        },
        "Task Metrics": {
            "Executor Run Time": finish - launch,
            "JVM GC Time": gc_time,
            "Memory Bytes Spilled": spill_mem,
            "Disk Bytes Spilled": spill_disk,
            "Shuffle Read Metrics": {"Remote Bytes Read": shuffle_read, "Local Bytes Read": 0},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": shuffle_write},
            "Input Metrics": {"Bytes Read": input_bytes},
        },
    }


def _make_executor_removed(executor_id, reason="Container killed by YARN"):
    return {
        "Event": "SparkListenerExecutorRemoved",
        "Executor ID": str(executor_id),
        "Removed Reason": reason,
        "Timestamp": 1700000000000,
    }


_MOCK_CONFIG = EventLogConfig(
    cluster_id="cluster-test",
    log_base_path="dbfs:/logs/cluster-test/eventlog",
    files=["dbfs:/logs/cluster-test/eventlog/file1"],
)


class TestGetEventLogStages:
    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_stages_sorted_by_duration(self, mock_read):
        events = [
            _make_stage_completed(0, "count at App:10", 200, 1000, 11000),
            _make_stage_completed(1, "join at App:20", 100, 2000, 62000),
            _make_stage_completed(2, "write at App:30", 50, 3000, 8000),
        ]
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_stages"]
        result = tool.fn(cluster_id="cluster-test")
        assert "3 total" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_spill_warning(self, mock_read):
        events = [
            _make_stage_completed(
                0,
                "spilly stage",
                100,
                1000,
                11000,
                accums=[
                    {"Name": "internal.metrics.diskBytesSpilled", "Value": 5_000_000_000},
                    {"Name": "internal.metrics.memoryBytesSpilled", "Value": 10_000_000_000},
                ],
            ),
        ]
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_stages"]
        result = tool.fn(cluster_id="cluster-test")
        assert "spilled to disk" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_no_log_delivery(self, mock_read):
        mock_read.side_effect = ValueError("no log delivery configured")

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_stages"]
        result = tool.fn(cluster_id="cluster-nologs")
        assert "log delivery" in result.lower()

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_no_completed_stages(self, mock_read):
        mock_read.return_value = ([], _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_stages"]
        result = tool.fn(cluster_id="cluster-test")
        assert "No completed stages" in result


class TestGetEventLogFailures:
    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_failed_tasks_grouped(self, mock_read):
        events = [
            _make_task_end(
                0, 1, "1", 1000, 5000, failed=True, reason="FetchFailed", description="shuffle fetch failed"
            ),
            _make_task_end(
                0, 2, "2", 1000, 5000, failed=True, reason="FetchFailed", description="shuffle fetch failed"
            ),
            _make_task_end(
                1,
                3,
                "1",
                2000,
                6000,
                failed=True,
                reason="ExceptionFailure",
                description="java.lang.OutOfMemoryError\n  at Something.run()",
            ),
            _make_task_end(0, 4, "1", 1000, 2000),  # success
        ]
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_failures"]
        result = tool.fn(cluster_id="cluster-test")
        assert "3 total" in result
        assert "FetchFailed: 2" in result
        assert "ExceptionFailure: 1" in result
        assert "Stage 0: 2 failures" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_executor_removals(self, mock_read):
        events = [
            _make_executor_removed("3", "Container killed by YARN for exceeding memory limits"),
        ]
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_failures"]
        result = tool.fn(cluster_id="cluster-test")
        assert "EXECUTOR REMOVALS" in result
        assert "exceeding memory" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_no_failures(self, mock_read):
        events = [_make_task_end(0, 1, "1", 1000, 2000)]
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_event_log_failures"]
        result = tool.fn(cluster_id="cluster-test")
        assert "No task failures" in result


class TestAnalyzeEventLogSkew:
    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_detects_skew(self, mock_read):
        stage_event = _make_stage_completed(0, "HashAggregate at App:42", 5, 1000, 61000)
        tasks = [_make_task_end(0, i, str(i % 3), 1000, 1000 + 1000, gc_time=50) for i in range(4)]
        tasks.append(_make_task_end(0, 4, "0", 1000, 11000, gc_time=200))
        events = [stage_event] + tasks
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_event_log_skew"]
        result = tool.fn(cluster_id="cluster-test")
        assert "SKEWED STAGES" in result
        assert "Stage 0" in result
        assert "task 4" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_detects_spill(self, mock_read):
        stage_event = _make_stage_completed(0, "SortMergeJoin", 3, 1000, 11000)
        tasks = [_make_task_end(0, i, "1", 1000, 2000, spill_mem=1_000_000, spill_disk=500_000) for i in range(3)]
        events = [stage_event] + tasks
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_event_log_skew"]
        result = tool.fn(cluster_id="cluster-test")
        assert "STAGES WITH SPILL" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_filter_by_stage_id(self, mock_read):
        stage0 = _make_stage_completed(0, "Stage A", 3, 1000, 2000)
        stage1 = _make_stage_completed(1, "Stage B", 3, 2000, 3000)
        tasks = [_make_task_end(0, i, "1", 1000, 2000) for i in range(3)] + [
            _make_task_end(1, i + 3, "1", 2000, 3000) for i in range(3)
        ]
        events = [stage0, stage1] + tasks
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_event_log_skew"]
        result = tool.fn(cluster_id="cluster-test", stage_id=1)
        assert "1 stages analyzed" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_no_skew(self, mock_read):
        stage_event = _make_stage_completed(0, "Uniform stage", 4, 1000, 5000)
        tasks = [_make_task_end(0, i, "1", 1000, 2000 + (i * 10)) for i in range(4)]
        events = [stage_event] + tasks
        mock_read.return_value = (events, _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_event_log_skew"]
        result = tool.fn(cluster_id="cluster-test")
        assert "No skewed stages" in result
        assert "No spill detected" in result

    @patch("databricks_debug_mcp.tools.event_logs.read_and_parse_event_log")
    def test_no_task_data(self, mock_read):
        mock_read.return_value = ([], _MOCK_CONFIG)

        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.event_logs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["analyze_event_log_skew"]
        result = tool.fn(cluster_id="cluster-test")
        assert "No task data found" in result
