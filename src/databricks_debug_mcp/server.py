from mcp.server.fastmcp import FastMCP

from .tools.jobs import register as register_jobs
from .tools.clusters import register as register_clusters
from .tools.logs import register as register_logs
from .tools.spark_ui import register as register_spark_ui
from .tools.diagnostics import register as register_diagnostics

mcp = FastMCP(
    "databricks-debug",
    instructions=(
        "Databricks debugging server. Use these tools to investigate Spark job failures, "
        "cluster issues, driver log analysis, Spark UI metrics, and structured diagnostics. "
        "Start with list_job_runs or get_run_details, then drill into get_task_output, "
        "get_cluster_events, and get_driver_logs. For a one-shot investigation, use investigate_failure."
    ),
)

register_jobs(mcp)
register_clusters(mcp)
register_logs(mcp)
register_spark_ui(mcp)
register_diagnostics(mcp)


def main() -> None:
    mcp.run()
