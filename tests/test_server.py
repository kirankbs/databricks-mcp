"""Tests for server registration and tool inventory."""


def test_all_tools_registered():
    from databricks_debug_mcp.server import mcp

    tools = mcp._tool_manager._tools
    expected = {
        # Core Investigation
        "list_job_runs",
        "get_run_details",
        "get_task_output",
        "get_cluster_for_run",
        # Clusters
        "get_cluster_info",
        "get_cluster_events",
        # Logs
        "get_driver_logs",
        "search_logs",
        # Spark UI
        "get_spark_stages",
        "get_spark_executors",
        "get_spark_sql_queries",
        "get_spark_jobs",
        "get_streaming_queries",
        "get_streaming_query_progress",
        "get_executor_memory",
        # Spark Analysis
        "analyze_stage_skew",
        "get_spark_config",
        "get_library_status",
        # Delta Lake
        "get_table_health",
        "get_table_history",
        "get_predictive_optimization",
        # System Tables
        "get_job_cost",
        "get_query_history",
        "get_cluster_utilization",
        "get_audit_events",
        "get_workspace_failures",
        # Lineage
        "get_table_lineage",
        "get_column_lineage",
        # DLT Pipelines
        "get_pipeline_status",
        "get_pipeline_errors",
        "get_pipeline_data_quality",
        # Init Scripts
        "get_init_script_logs",
    }

    assert set(tools.keys()) == expected


def test_tool_count():
    from databricks_debug_mcp.server import mcp
    assert len(mcp._tool_manager._tools) == 32


def test_all_tools_have_descriptions():
    from databricks_debug_mcp.server import mcp
    for name, tool in mcp._tool_manager._tools.items():
        assert tool.description, f"Tool {name} has no description"
