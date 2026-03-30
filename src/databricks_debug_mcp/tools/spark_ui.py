import html as html_mod
import json
import re
from urllib.parse import quote

from mcp.server.fastmcp import FastMCP

from ..client import assert_cluster_running, get_spark_app_id, spark_ui_html, spark_ui_request
from ..formatting import format_bytes, format_duration, truncate_stacktrace

_VALID_STAGE_STATUS = {"ACTIVE", "COMPLETE", "FAILED"}
_VALID_JOB_STATUS = {"RUNNING", "SUCCEEDED", "FAILED"}


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
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            path = f"applications/{app_id}/stages"
            if status:
                if status.upper() not in _VALID_STAGE_STATUS:
                    return f"Invalid status '{status}'. Valid: {', '.join(sorted(_VALID_STAGE_STATUS))}"
                path += f"?status={quote(status)}"
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
        lines.append(
            f"{'Stage':<8} {'Status':<12} {'Tasks':<12} {'Duration':<12} {'Shuffle R':<14} {'Shuffle W':<14} {'Description'}"
        )
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
    def get_executor_memory(cluster_id: str) -> str:
        """Memory health report for OOM diagnosis: JVM heap peak vs allocated,
        GC time, Python worker RSS, off-heap, direct memory, total completed tasks,
        and uptime. Flags when heap usage exceeds 80%.

        This is the first tool to use when investigating OOM (exit code 137) or
        slow GC pauses on long-running clusters. Only works on RUNNING clusters.
        """
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            executors = spark_ui_request(cluster_id, f"applications/{app_id}/executors")
        except Exception as e:
            return f"Failed to fetch executor memory: {e}"

        lines = [f"Memory Health Report — cluster {cluster_id}\n"]

        for ex in executors:
            eid = ex.get("id", "?")
            mem_used = ex.get("memoryUsed", 0)
            mem_max = ex.get("maxMemory", 0)
            mem_pct = (mem_used / mem_max * 100) if mem_max > 0 else 0
            gc_ms = ex.get("totalGCTime", 0)
            total_tasks = ex.get("totalTasks", 0)
            total_dur_ms = ex.get("totalDuration", 0)
            total_input = ex.get("totalInputBytes", 0)

            flag = " ⚠ HIGH" if mem_pct > 80 else ""
            lines.append(f"Executor {eid}:{flag}")
            lines.append(f"  Spark storage:    {format_bytes(mem_used)} / {format_bytes(mem_max)} ({mem_pct:.1f}%)")
            lines.append(f"  GC time:          {format_duration(gc_ms)}")
            lines.append(f"  Tasks completed:  {total_tasks:,}")
            lines.append(f"  Task time:        {format_duration(total_dur_ms)}")
            lines.append(f"  Total input:      {format_bytes(total_input)}")

            mm = ex.get("memoryMetrics", {})
            if mm:
                on_heap = mm.get("usedOnHeapStorageMemory", 0)
                on_heap_total = mm.get("totalOnHeapStorageMemory", 0)
                off_heap = mm.get("usedOffHeapStorageMemory", 0)
                off_heap_total = mm.get("totalOffHeapStorageMemory", 0)
                lines.append(f"  On-heap storage:  {format_bytes(on_heap)} / {format_bytes(on_heap_total)}")
                lines.append(f"  Off-heap storage: {format_bytes(off_heap)} / {format_bytes(off_heap_total)}")

            peak = ex.get("peakMemoryMetrics", {})
            if peak:
                lines.append("  --- Peak Memory ---")
                jvm_heap = peak.get("JVMHeapMemory", 0)
                jvm_offheap = peak.get("JVMOffHeapMemory", 0)
                python_rss = peak.get("PythonWorkerUsedMemory", 0)
                direct = peak.get("DirectPoolMemory", 0)
                mapped = peak.get("MappedPoolMemory", 0)
                jvm_rss = peak.get("ProcessTreeJVMRSSMemory", 0)
                py_rss = peak.get("ProcessTreePythonRSSMemory", 0)
                other_rss = peak.get("ProcessTreeOtherRSSMemory", 0)

                lines.append(f"  JVM heap peak:    {format_bytes(jvm_heap)}")
                lines.append(f"  JVM off-heap:     {format_bytes(jvm_offheap)}")
                lines.append(f"  Python worker:    {format_bytes(python_rss)}")
                lines.append(f"  Direct memory:    {format_bytes(direct)}")
                lines.append(f"  Mapped pool:      {format_bytes(mapped)}")
                if jvm_rss or py_rss or other_rss:
                    lines.append(
                        f"  Process tree RSS: JVM={format_bytes(jvm_rss)} Python={format_bytes(py_rss)} Other={format_bytes(other_rss)}"
                    )

                # OOM risk assessment
                # mem_max is Spark storage memory, not JVM -Xmx
                if mem_max > 0 and jvm_heap > 0:
                    heap_vs_alloc = jvm_heap / mem_max * 100
                    if heap_vs_alloc > 90:
                        lines.append(
                            f"  🔴 CRITICAL: JVM heap peak is {heap_vs_alloc:.0f}% of allocated — OOM imminent"
                        )
                    elif heap_vs_alloc > 75:
                        lines.append(
                            f"  🟡 WARNING: JVM heap peak is {heap_vs_alloc:.0f}% of allocated — OOM risk on long-running jobs"
                        )

            lines.append("")

        return "\n".join(lines)

    @mcp.tool()
    def get_spark_executors(cluster_id: str) -> str:
        """Executor resource stats: memory, disk, task counts, shuffle totals. Only works on RUNNING clusters."""
        err = assert_cluster_running(cluster_id)
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
            lines.append(
                f"{eid:<6} {host:<30} {cores:<6} {mem:<12} {disk:<12} {active:<8} {done:<8} {failed:<8} {sr:<14} {sw}"
            )
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
        err = assert_cluster_running(cluster_id)
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
            lines.append(
                f"{q.get('id', '?'):<6} {q.get('status', '?'):<12} {format_duration(q.get('duration')):<12} {desc}"
            )
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
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            path = f"applications/{app_id}/jobs"
            if status:
                if status.upper() not in _VALID_JOB_STATUS:
                    return f"Invalid status '{status}'. Valid: {', '.join(sorted(_VALID_JOB_STATUS))}"
                path += f"?status={quote(status)}"
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
            dur = "—"
            name = j.get("name", "")[:50]
            lines.append(f"{jid:<8} {st:<12} {stages:<8} {tasks:<20} {dur:<12} {name}")
        return "\n".join(lines)

    @mcp.tool()
    def get_streaming_queries(cluster_id: str) -> str:
        """All structured streaming queries — active AND completed/failed — with status,
        duration, throughput, batch counts, and full error traces for failures.

        Active queries: fetched from REST API when available, HTML fallback otherwise.
        Completed/failed queries: always from HTML (no REST API exists for these).
        Only works on RUNNING clusters.
        """
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        lines = []

        # --- Active queries: prefer REST API ---
        rest_ok = False
        try:
            app_id = get_spark_app_id(cluster_id)
            data = spark_ui_request(cluster_id, f"applications/{app_id}/streaming/statistics")
            if isinstance(data, list) and data:
                rest_ok = True
                lines.append(f"Active Streaming Queries ({len(data)}):\n")
                lines.append(f"{'Name':<42} {'Status':<10} {'ID':<40} {'isActive'}")
                lines.append("-" * 100)
                for q in data:
                    name = q.get("name", "?")
                    status = "ACTIVE" if q.get("isActive") else "INACTIVE"
                    qid = q.get("id", "?")
                    lines.append(f"{name:<42} {status:<10} {qid:<40}")
        except Exception:
            pass

        # --- Always scrape HTML for completed/failed queries + richer active data ---
        try:
            page = spark_ui_html(cluster_id, "StreamingQuery/")
            active_html, completed_html = _parse_streaming_queries_html_parts(page)

            if not rest_ok and active_html:
                lines.append(active_html)

            if completed_html:
                lines.append(completed_html)
        except Exception as e:
            if not rest_ok:
                return f"Failed to fetch streaming queries: {e}"
            lines.append(f"\n(Could not fetch completed/failed queries: {e})")

        return "\n".join(lines) if lines else "No streaming queries found."

    @mcp.tool()
    def get_streaming_query_progress(cluster_id: str, run_id: str) -> str:
        """Batch-level metrics for a streaming query: input rate, processing rate,
        batch duration breakdown (addBatch, queryPlanning, walCommit, etc.).

        Use the run_id from get_streaming_queries output.
        Tries REST API first, falls back to HTML stats page.
        Only works on RUNNING clusters.
        """
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        # --- Try REST API first ---
        try:
            app_id = get_spark_app_id(cluster_id)
            data = spark_ui_request(
                cluster_id,
                f"applications/{app_id}/streaming/statistics/{quote(run_id)}",
            )
            if data:
                return _format_streaming_progress_rest(data, run_id)
        except Exception:
            pass

        # --- Fall back to HTML ---
        try:
            page = spark_ui_html(cluster_id, f"StreamingQuery/statistics?id={quote(run_id)}")
        except Exception as e:
            return f"Failed to fetch streaming query progress: {e}"

        return _parse_streaming_query_stats_html(page, run_id)


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    return html_mod.unescape(re.sub(r"<[^>]+>", " ", text)).strip()


def _parse_streaming_queries_html_parts(page: str) -> tuple[str, str]:
    """Parse /StreamingQuery/ HTML into (active_text, completed_text).

    Returns two strings so the caller can decide whether to use the active
    section (only when REST API failed) or just the completed section.
    """
    # Expects 9+ column rows: name, status, queryId, runId, startTime,
    # duration, avgInput, avgProcess, latestBatch — matches Spark 3.3–3.5
    rows = re.findall(r"<tr>\s*((?:<td.*?</td>\s*)+)</tr>", page, re.DOTALL)

    active: list[dict] = []
    completed: list[dict] = []

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 9:
            continue

        name = _clean_html(cells[0])
        status = _clean_html(cells[1])
        query_id = _clean_html(cells[2])
        run_id_match = re.search(r'href="[^"]*id=([^"&]+)"', cells[3])
        run_id = run_id_match.group(1) if run_id_match else _clean_html(cells[3])
        start_time = _clean_html(cells[4])
        duration = _clean_html(cells[5])
        avg_input = _clean_html(cells[6])
        avg_process = _clean_html(cells[7])
        latest_batch = _clean_html(cells[8])

        error = ""
        if len(cells) > 9:
            error_cell = cells[9]
            pre_match = re.search(r"<pre>(.*?)</pre>", error_cell, re.DOTALL)
            if pre_match:
                error = _clean_html(pre_match.group(1))
            else:
                error = _clean_html(error_cell)
            error = re.sub(r"\s*\+details\s*", "", error).strip()

        entry = {
            "name": name,
            "status": status,
            "query_id": query_id,
            "run_id": run_id,
            "start_time": start_time,
            "duration": duration,
            "avg_input_sec": avg_input,
            "avg_process_sec": avg_process,
            "latest_batch": latest_batch,
            "error": error,
        }

        if status == "FAILED":
            completed.append(entry)
        else:
            active.append(entry)

    # Format active section
    active_lines: list[str] = []
    if active:
        active_lines.append(f"Active Streaming Queries ({len(active)}):\n")
        active_lines.append(
            f"{'Name':<42} {'Status':<10} {'Duration':<28} {'In/s':<8} {'Proc/s':<8} {'Batch':<8} Run ID"
        )
        active_lines.append("-" * 130)
        for q in active:
            active_lines.append(
                f"{q['name']:<42} {q['status']:<10} {q['duration']:<28} "
                f"{q['avg_input_sec']:<8} {q['avg_process_sec']:<8} "
                f"{q['latest_batch']:<8} {q['run_id']}"
            )

    # Format completed/failed section
    completed_lines: list[str] = []
    if completed:
        completed_lines.append(f"\nCompleted/Failed Streaming Queries ({len(completed)}):\n")
        for q in completed:
            completed_lines.append(f"  {q['name']} [{q['status']}]")
            completed_lines.append(f"    Run ID:       {q['run_id']}")
            completed_lines.append(f"    Duration:     {q['duration']}")
            completed_lines.append(f"    Latest Batch: {q['latest_batch']}")
            completed_lines.append(f"    Throughput:   {q['avg_input_sec']} in/s, {q['avg_process_sec']} proc/s")
            if q["error"]:
                truncated = truncate_stacktrace(q["error"], max_lines=30)
                completed_lines.append("    Error:")
                for el in truncated.splitlines():
                    completed_lines.append(f"      {el}")
            completed_lines.append("")

    return "\n".join(active_lines), "\n".join(completed_lines)


def _format_streaming_progress_rest(data: dict, run_id: str) -> str:
    """Format streaming query progress from the REST API response."""
    lines = [f"Streaming Query Progress: {run_id}\n"]

    if isinstance(data, dict):
        for key in ("name", "id", "runId", "isActive", "timestamp"):
            if key in data:
                lines.append(f"  {key}: {data[key]}")

        recent = data.get("recentProgress", [])
        if recent:
            latest = recent[-1]
            lines.append("\nLatest batch:")
            lines.append(f"  Input rows/s:   {latest.get('inputRowsPerSecond', '?')}")
            lines.append(f"  Process rows/s: {latest.get('processedRowsPerSecond', '?')}")
            lines.append(f"  Batch ID:       {latest.get('batchId', '?')}")
            dur = latest.get("durationMs", {})
            if dur:
                lines.append(f"  Duration (ms):  {dur}")
    else:
        lines.append(str(data))

    return "\n".join(lines)


def _parse_streaming_query_stats_html(page: str, run_id: str) -> str:
    """Parse the /StreamingQuery/statistics?id=... HTML page for batch metrics.

    Extracts the embedded JavaScript time-series data (input rate, processing rate,
    input rows, batch duration breakdown).
    """
    lines = [f"Streaming Query Progress: {run_id}\n"]

    # Extract JS variables with time-series data
    # v1=input rate, v2=processing rate, v3=input rows, plus batch duration breakdown
    # Variable names v1/v2/v3 are Spark UI implementation details —
    # ordering has changed across Spark versions and may need updating
    js_vars = re.findall(r"var\s+(v\d+)\s*=\s*(\[.*?\]);", page, re.DOTALL)

    label_map = {"v1": "Input Rate (rows/s)", "v2": "Processing Rate (rows/s)", "v3": "Input Rows"}

    for var_name, var_data in js_vars:
        label = label_map.get(var_name)
        if not label:
            continue
        try:
            data = json.loads(var_data)
            if data:
                values = [d["y"] for d in data if "y" in d]
                if values:
                    lines.append(f"{label}:")
                    lines.append(f"  Latest:  {values[-1]:.2f}")
                    lines.append(f"  Average: {sum(values) / len(values):.2f}")
                    lines.append(f"  Min:     {min(values):.2f}")
                    lines.append(f"  Max:     {max(values):.2f}")
                    lines.append(f"  Samples: {len(values)}")
                    lines.append("")
        except Exception:
            pass

    # Extract batch duration breakdown from the JSON blocks
    batch_data = re.findall(
        r'\{x:\s*"([^"]+)"[^}]*"addBatch":\s*"([^"]+)"[^}]*"queryPlanning":\s*"([^"]+)"[^}]*"walCommit":\s*"([^"]+)"',
        page,
    )
    if batch_data:
        recent = batch_data[-5:]
        lines.append("Recent Batch Duration Breakdown (ms):")
        lines.append(f"  {'Time':<16} {'addBatch':<14} {'queryPlanning':<16} {'walCommit':<12}")
        lines.append("  " + "-" * 56)
        for time_str, add_batch, query_plan, wal in recent:
            try:
                lines.append(
                    f"  {time_str:<16} {float(add_batch):>10.0f}ms   {float(query_plan):>10.0f}ms     {float(wal):>8.0f}ms"
                )
            except ValueError:
                lines.append(f"  {time_str:<16} {add_batch:<14} {query_plan:<16} {wal:<12}")

    if len(lines) <= 2:
        lines.append("No batch metrics available.")

    return "\n".join(lines)
