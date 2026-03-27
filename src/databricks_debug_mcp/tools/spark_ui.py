from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client, spark_ui_request, get_spark_app_id
from ..formatting import format_duration, format_bytes, enum_val

_CLUSTER_TERMINATED_MSG = (
    "Cluster {cluster_id} is not RUNNING — Spark UI data is unavailable for terminated clusters.\n"
    "Alternatives:\n"
    "  - get_run_details: task timings and statuses\n"
    "  - get_driver_logs: driver log content\n"
    "  - get_cluster_events: infrastructure events (DRIVER_NOT_RESPONDING, etc.)"
)


def _assert_running(cluster_id: str) -> str | None:
    """Return an error string if the cluster is not RUNNING, else None."""
    w = get_workspace_client()
    cluster = w.clusters.get(cluster_id=cluster_id)
    state = enum_val(cluster.state)
    if state != "RUNNING":
        return _CLUSTER_TERMINATED_MSG.format(cluster_id=cluster_id)
    return None


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_spark_stages(
        cluster_id: str,
        sort_by: str = "duration",
        limit: int = 20,
        status: str | None = None,
    ) -> str:
        """Top Spark stages by duration (or shuffle/IO bytes). Only works on RUNNING clusters.

        sort_by: duration | shuffleRead | shuffleWrite | input | output
        status: ACTIVE | COMPLETE | FAILED (omit for all)
        """
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            path = f"applications/{app_id}/stages"
            if status:
                path += f"?status={status}"
            stages = spark_ui_request(cluster_id, path)
        except Exception as e:
            return f"Failed to fetch Spark stages: {e}"

        sort_map = {
            "duration": lambda s: s.get("executorRunTime", 0),
            "shuffleRead": lambda s: s.get("shuffleReadBytes", 0),
            "shuffleWrite": lambda s: s.get("shuffleWriteBytes", 0),
            "input": lambda s: s.get("inputBytes", 0),
            "output": lambda s: s.get("outputBytes", 0),
        }
        key_fn = sort_map.get(sort_by, sort_map["duration"])
        stages.sort(key=key_fn, reverse=True)
        stages = stages[:limit]

        lines = [f"Top {len(stages)} Spark stages (sorted by {sort_by}):\n"]
        lines.append(f"{'Stage':<8} {'Status':<12} {'Tasks':<12} {'Duration':<12} {'Shuffle R':<14} {'Shuffle W':<14} {'Description'}")
        lines.append("-" * 100)
        for s in stages:
            stage_id = s.get("stageId", "?")
            st = s.get("status", "?")
            tasks = f"{s.get('numCompleteTasks', 0)}/{s.get('numTasks', 0)}"
            dur = format_duration(s.get("executorRunTime"))
            sr = format_bytes(s.get("shuffleReadBytes"))
            sw = format_bytes(s.get("shuffleWriteBytes"))
            desc = s.get("name", "")[:60]
            lines.append(f"{stage_id:<8} {st:<12} {tasks:<12} {dur:<12} {sr:<14} {sw:<14} {desc}")

        return "\n".join(lines)

    @mcp.tool()
    def get_spark_executors(cluster_id: str) -> str:
        """Executor resource stats: memory, disk, task counts, shuffle totals. Only works on RUNNING clusters."""
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            executors = spark_ui_request(cluster_id, f"applications/{app_id}/allexecutors")
        except Exception as e:
            return f"Failed to fetch executor stats: {e}"

        lines = [f"Executors for cluster {cluster_id} ({len(executors)} total):\n"]
        lines.append(
            f"{'ID':<6} {'Host':<30} {'Cores':<6} {'Mem Used':<12} {'Disk Used':<12} "
            f"{'Active':<8} {'Done':<8} {'Failed':<8} {'Shuffle R':<14} {'Shuffle W'}"
        )
        lines.append("-" * 120)

        totals = {"active": 0, "done": 0, "failed": 0, "sr": 0, "sw": 0}
        for ex in sorted(executors, key=lambda x: x.get("id", "")):
            eid = ex.get("id", "?")
            host = ex.get("hostPort", "?")[:29]
            cores = ex.get("totalCores", "?")
            mem = format_bytes(ex.get("memoryUsed"))
            disk = format_bytes(ex.get("diskUsed"))
            active = ex.get("activeTasks", 0)
            done = ex.get("completedTasks", 0)
            failed = ex.get("failedTasks", 0)
            sr = format_bytes(ex.get("totalShuffleRead"))
            sw = format_bytes(ex.get("totalShuffleWrite"))
            lines.append(f"{eid:<6} {host:<30} {cores:<6} {mem:<12} {disk:<12} {active:<8} {done:<8} {failed:<8} {sr:<14} {sw}")
            if eid != "driver":
                totals["active"] += active
                totals["done"] += done
                totals["failed"] += failed
                totals["sr"] += ex.get("totalShuffleRead") or 0
                totals["sw"] += ex.get("totalShuffleWrite") or 0

        lines.append("-" * 120)
        lines.append(
            f"{'TOTAL':<6} {'':<30} {'':<6} {'':<12} {'':<12} "
            f"{totals['active']:<8} {totals['done']:<8} {totals['failed']:<8} "
            f"{format_bytes(totals['sr']):<14} {format_bytes(totals['sw'])}"
        )
        return "\n".join(lines)

    @mcp.tool()
    def get_spark_sql_queries(
        cluster_id: str,
        query_id: int | None = None,
        limit: int = 10,
    ) -> str:
        """SQL/DataFrame query executions with metrics. Only works on RUNNING clusters.

        If query_id is provided, includes the full physical plan.
        """
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            data = spark_ui_request(cluster_id, f"applications/{app_id}/sql")
        except Exception as e:
            return f"Failed to fetch SQL queries: {e}"

        queries = data.get("executions", data) if isinstance(data, dict) else data

        if query_id is not None:
            queries = [q for q in queries if q.get("id") == query_id]
            if not queries:
                return f"Query {query_id} not found."
            q = queries[0]
            lines = [
                f"Query {query_id}: {q.get('description', '')[:200]}",
                f"Status:  {q.get('status', '?')}",
                f"Duration: {format_duration(q.get('duration'))}",
                f"Running jobs: {q.get('runningJobIds', [])}",
                f"Succeeded jobs: {q.get('successJobIds', [])}",
                f"Failed jobs: {q.get('failedJobIds', [])}",
            ]
            plan = q.get("physicalPlanDescription") or q.get("planDescription", "")
            if plan:
                lines.append(f"\nPhysical plan:\n{plan}")
            return "\n".join(lines)

        queries = sorted(queries, key=lambda q: q.get("duration", 0), reverse=True)[:limit]
        lines = [f"Top {len(queries)} SQL queries (by duration):\n"]
        lines.append(f"{'ID':<6} {'Status':<12} {'Duration':<12} {'Description'}")
        lines.append("-" * 80)
        for q in queries:
            desc = q.get("description", "")[:60]
            lines.append(f"{q.get('id', '?'):<6} {q.get('status', '?'):<12} {format_duration(q.get('duration')):<12} {desc}")
        return "\n".join(lines)

    @mcp.tool()
    def get_spark_jobs(
        cluster_id: str,
        status: str | None = None,
        limit: int = 20,
    ) -> str:
        """Spark-level jobs (actions like count(), save()) with stage/task counts. Only works on RUNNING clusters.

        status: RUNNING | SUCCEEDED | FAILED (omit for all)
        """
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            path = f"applications/{app_id}/jobs"
            if status:
                path += f"?status={status}"
            jobs = spark_ui_request(cluster_id, path)
        except Exception as e:
            return f"Failed to fetch Spark jobs: {e}"

        jobs = sorted(jobs, key=lambda j: j.get("submissionTime", ""), reverse=True)[:limit]
        lines = [f"Spark jobs for cluster {cluster_id} ({len(jobs)} shown):\n"]
        lines.append(f"{'Job ID':<8} {'Status':<12} {'Stages':<8} {'Tasks Done/Total':<20} {'Duration':<12} {'Name'}")
        lines.append("-" * 90)
        for j in jobs:
            jid = j.get("jobId", "?")
            st = j.get("status", "?")
            stages = j.get("numActiveStages", 0) + j.get("numCompletedStages", 0) + j.get("numFailedStages", 0)
            tasks = f"{j.get('numCompletedTasks', 0)}/{j.get('numTasks', 0)}"
            # Duration: submissionTime/completionTime are ISO strings in Spark API
            dur = "—"
            name = j.get("name", "")[:50]
            lines.append(f"{jid:<8} {st:<12} {stages:<8} {tasks:<20} {dur:<12} {name}")
        return "\n".join(lines)

    @mcp.tool()
    def get_streaming_queries(cluster_id: str) -> str:
        """List active streaming queries with latest progress. Only works on RUNNING clusters."""
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            data = spark_ui_request(cluster_id, f"applications/{app_id}/streaming/statistics")
        except Exception as e:
            return (
                f"Streaming statistics endpoint unavailable: {e}\n"
                "For persistent streaming metrics, implement a StreamingQueryListener that writes "
                "progress events to a Delta table."
            )

        return str(data)

    @mcp.tool()
    def get_streaming_query_progress(cluster_id: str, query_id: str) -> str:
        """Detailed batch-level progress for a specific streaming query. Only works on RUNNING clusters."""
        err = _assert_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            data = spark_ui_request(cluster_id, f"applications/{app_id}/streaming/statistics/{query_id}")
        except Exception as e:
            return f"Failed to fetch streaming query progress: {e}"

        return str(data)
