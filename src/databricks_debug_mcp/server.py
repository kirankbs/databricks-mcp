from mcp.server.fastmcp import FastMCP

from .tools.jobs import register as register_jobs
from .tools.clusters import register as register_clusters
from .tools.logs import register as register_logs
from .tools.spark_ui import register as register_spark_ui

mcp = FastMCP(
    "databricks-debug",
    instructions=(
        "Databricks debugging server. Raw data access for Spark job failures, "
        "cluster events, driver logs, and Spark UI metrics. "
        "Tools: list_job_runs, get_run_details, get_task_output, get_cluster_for_run, "
        "get_cluster_info, get_cluster_events, get_driver_logs, search_logs, "
        "get_spark_stages, get_spark_executors, get_spark_sql_queries, get_spark_jobs, "
        "get_streaming_queries, get_streaming_query_progress."
    ),
)

register_jobs(mcp)
register_clusters(mcp)
register_logs(mcp)
register_spark_ui(mcp)


def main() -> None:
    mcp.run()
