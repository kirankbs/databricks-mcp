"""Tests for job investigation tools."""

from unittest.mock import MagicMock, patch


def _make_run(
    run_id,
    job_id=1,
    start=1700000000000,
    end=1700000060000,
    life_cycle="TERMINATED",
    result_state="SUCCESS",
    trigger="PERIODIC",
):
    run = MagicMock()
    run.run_id = run_id
    run.job_id = job_id
    run.start_time = start
    run.end_time = end
    run.state.life_cycle_state = MagicMock(value=life_cycle)
    run.state.result_state = MagicMock(value=result_state)
    run.trigger = MagicMock(value=trigger)
    return run


def _make_task(
    task_key,
    run_id=100,
    start=1700000000000,
    end=1700000030000,
    life_cycle="TERMINATED",
    result_state="SUCCESS",
    cluster_id="c-1",
    state_message=None,
    new_cluster=None,
    attempt=0,
):
    task = MagicMock()
    task.task_key = task_key
    task.run_id = run_id
    task.start_time = start
    task.end_time = end
    task.state.life_cycle_state = MagicMock(value=life_cycle)
    task.state.result_state = MagicMock(value=result_state)
    task.state.state_message = state_message
    task.cluster_instance.cluster_id = cluster_id
    task.new_cluster = new_cluster
    task.job_cluster_key = None
    task.attempt_number = attempt
    return task


class TestListJobRuns:
    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_by_job_id(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.jobs.list_runs.return_value = [
            _make_run(100, result_state="SUCCESS"),
            _make_run(101, result_state="FAILED"),
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn(job_id=1)
        assert "100" in result
        assert "101" in result
        assert "2 run(s)" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_by_job_name(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        job = MagicMock()
        job.job_id = 42
        mock_client.jobs.list.return_value = [job]
        mock_client.jobs.list_runs.return_value = [_make_run(200, job_id=42)]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn(job_name="my-etl-job")
        assert "200" in result
        assert "42" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_job_name_not_found(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.jobs.list.return_value = []

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn(job_name="nonexistent")
        assert "No job found" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_no_params(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn()
        assert "Provide either" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_status_filter(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.jobs.list_runs.return_value = [
            _make_run(100, result_state="SUCCESS"),
            _make_run(101, result_state="FAILED"),
            _make_run(102, result_state="FAILED"),
        ]

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn(job_id=1, status_filter="FAILED")
        assert "101" in result
        assert "102" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_no_runs(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.jobs.list_runs.return_value = []

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["list_job_runs"]
        result = tool.fn(job_id=1)
        assert "No runs found" in result


class TestGetRunDetails:
    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_run_with_tasks(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = _make_run(500)
        run.tasks = [
            _make_task("extract", result_state="SUCCESS"),
            _make_task("transform", result_state="FAILED", state_message="NullPointerException"),
        ]
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_run_details"]
        result = tool.fn(run_id=500)
        assert "Run 500" in result
        assert "extract" in result
        assert "transform" in result
        assert "Failure messages" in result
        assert "NullPointerException" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_run_no_tasks(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = _make_run(501)
        run.tasks = []
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_run_details"]
        result = tool.fn(run_id=501)
        assert "Run 501" in result
        assert "Task Key" not in result


class TestGetTaskOutput:
    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_auto_selects_failed(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        t1 = _make_task("ok_task", run_id=10, result_state="SUCCESS")
        t2 = _make_task("bad_task", run_id=11, result_state="FAILED")
        run.tasks = [t1, t2]
        mock_client.jobs.get_run.return_value = run

        output = MagicMock()
        output.error = "java.lang.OutOfMemoryError"
        output.error_trace = "at line 1\nat line 2"
        output.logs = "some logs"
        output.notebook_output = None
        mock_client.jobs.get_run_output.return_value = output

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_task_output"]
        result = tool.fn(run_id=500)
        assert "bad_task" in result
        assert "OutOfMemoryError" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_specific_task_key(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        t1 = _make_task("etl_step", run_id=10, result_state="SUCCESS")
        run.tasks = [t1]
        mock_client.jobs.get_run.return_value = run

        output = MagicMock()
        output.error = None
        output.error_trace = None
        output.logs = "all good"
        output.notebook_output = None
        mock_client.jobs.get_run_output.return_value = output

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_task_output"]
        result = tool.fn(run_id=500, task_key="etl_step")
        assert "etl_step" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_task_key_not_found(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        t1 = _make_task("actual_task", run_id=10)
        run.tasks = [t1]
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_task_output"]
        result = tool.fn(run_id=500, task_key="nonexistent")
        assert "not found" in result
        assert "actual_task" in result


class TestGetClusterForRun:
    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_job_and_shared_clusters(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        # Job cluster task
        t1 = _make_task("etl", cluster_id="c-job-1", new_cluster={"spark_version": "14.3"})
        # Shared cluster task
        t2 = _make_task("notify", cluster_id="c-shared-1", new_cluster=None)
        run.tasks = [t1, t2]
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_for_run"]
        result = tool.fn(run_id=500)
        assert "Job cluster" in result
        assert "Shared cluster" in result
        assert "c-job-1" in result
        assert "c-shared-1" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_no_cluster_assignments(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        task = MagicMock()
        task.task_key = "pending"
        task.cluster_instance = None
        run.tasks = [task]
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_for_run"]
        result = tool.fn(run_id=500)
        assert "No cluster assignments" in result

    @patch("databricks_debug_mcp.tools.jobs.get_workspace_client")
    def test_no_tasks(self, mock_get_client):
        from mcp.server.fastmcp import FastMCP

        from databricks_debug_mcp.tools.jobs import register

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        run = MagicMock()
        run.tasks = []
        mock_client.jobs.get_run.return_value = run

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["get_cluster_for_run"]
        result = tool.fn(run_id=500)
        assert "no tasks" in result
