from mcp.server.fastmcp import FastMCP

from .tools.jobs import register as register_jobs
from .tools.clusters import register as register_clusters
from .tools.logs import register as register_logs
from .tools.spark_ui import register as register_spark_ui
from .tools.spark_analysis import register as register_spark_analysis
from .tools.delta import register as register_delta
from .tools.system_tables import register as register_system_tables
from .tools.lineage import register as register_lineage
from .tools.pipelines import register as register_pipelines
from .tools.init_scripts import register as register_init_scripts

mcp = FastMCP(
    "databricks-debug",
    instructions=(
        "Databricks debugging server. Raw data access for Spark job failures, "
        "cluster events, driver logs, Spark UI metrics, Delta table health, "
        "cost attribution, query history, audit forensics, table lineage, "
        "DLT pipeline errors, and init script debugging. "
        "Tools: list_job_runs, get_run_details, get_task_output, get_cluster_for_run, "
        "get_cluster_info, get_cluster_events, get_driver_logs, search_logs, "
        "get_spark_stages, get_spark_executors, get_spark_sql_queries, get_spark_jobs, "
        "get_streaming_queries, get_streaming_query_progress, "
        "get_executor_memory, "
        "analyze_stage_skew, get_spark_config, get_library_status, "
        "get_table_health, get_table_history, get_predictive_optimization, "
        "get_job_cost, get_query_history, get_cluster_utilization, get_audit_events, "
        "get_workspace_failures, "
        "get_table_lineage, get_column_lineage, "
        "get_pipeline_status, get_pipeline_errors, get_pipeline_data_quality, "
        "get_init_script_logs."
    ),
)

register_jobs(mcp)
register_clusters(mcp)
register_logs(mcp)
register_spark_ui(mcp)
register_spark_analysis(mcp)
register_delta(mcp)
register_system_tables(mcp)
register_lineage(mcp)
register_pipelines(mcp)
register_init_scripts(mcp)


def main() -> None:
    mcp.run()
