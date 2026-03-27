from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import format_duration, ms_to_str, enum_val


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def compare_runs(run_ids: list[int]) -> str:
        """Side-by-side comparison of 2–5 job runs: task timing deltas, status changes, cluster config differences."""
        if len(run_ids) < 2:
            return "Provide at least 2 run_ids to compare."
        if len(run_ids) > 5:
            return "Maximum 5 runs can be compared at once."

        w = get_workspace_client()
        runs = {}
        for rid in run_ids:
            try:
                runs[rid] = w.jobs.get_run(run_id=rid)
            except Exception as e:
                return f"Failed to fetch run {rid}: {e}"

        # Collect all task keys across all runs
        all_task_keys: set[str] = set()
        for run in runs.values():
            for task in (run.tasks or []):
                all_task_keys.add(task.task_key)

        lines = [f"Comparison of runs: {run_ids}\n"]

        # Header row
        header = f"{'Task Key':<35}" + "".join(f" {rid:<20}" for rid in run_ids)
        lines.append(header)
        lines.append("-" * (35 + 21 * len(run_ids)))

        for task_key in sorted(all_task_keys):
            row = f"{task_key:<35}"
            durations = []
            for rid in run_ids:
                run = runs[rid]
                task = next((t for t in (run.tasks or []) if t.task_key == task_key), None)
                if task is None:
                    row += f" {'—':<20}"
                else:
                    dur_ms = (task.end_time - task.start_time) if task.end_time and task.start_time else None
                    result = enum_val(task.state.result_state if task.state else None, fallback="?")
                    cell = f"{format_duration(dur_ms)} ({result})"
                    row += f" {cell:<20}"
                    if dur_ms is not None:
                        durations.append(dur_ms)
            lines.append(row)

        # Run-level summary
        lines.append("")
        lines.append("Run summary:")
        for rid in run_ids:
            run = runs[rid]
            dur_ms = (run.end_time - run.start_time) if run.end_time and run.start_time else None
            result = enum_val(run.state.result_state if run.state else None, fallback="?")
            lines.append(f"  {rid}: {format_duration(dur_ms)} — {result}")

        return "\n".join(lines)

    @mcp.tool()
    def get_run_timeline(run_id: int) -> str:
        """Unified chronological timeline merging task lifecycle events with cluster events for a job run."""
        w = get_workspace_client()

        try:
            run = w.jobs.get_run(run_id=run_id)
        except Exception as e:
            return f"Failed to fetch run {run_id}: {e}"

        events: list[tuple[int, str, str]] = []  # (timestamp_ms, category, description)

        # Task events
        for task in (run.tasks or []):
            if task.start_time:
                events.append((task.start_time, "TASK", f"{task.task_key}: started"))
            if task.end_time:
                result = enum_val(task.state.result_state if task.state else None, fallback="?")
                dur_ms = task.end_time - task.start_time if task.start_time else None
                events.append((
                    task.end_time,
                    "TASK",
                    f"{task.task_key}: {result} (took {format_duration(dur_ms)})",
                ))

        # Cluster events — collect unique cluster IDs
        cluster_ids: set[str] = set()
        for task in (run.tasks or []):
            if task.cluster_instance and task.cluster_instance.cluster_id:
                cluster_ids.add(task.cluster_instance.cluster_id)

        if run.start_time and run.end_time:
            window_start = run.start_time - 300_000  # 5 min before
            window_end = run.end_time + 300_000

            import time
            for cluster_id in cluster_ids:
                try:
                    for ev in w.clusters.events(
                        cluster_id=cluster_id,
                        start_time=window_start,
                        end_time=window_end,
                        order="ASC",
                    ):
                        if ev.timestamp:
                            et = enum_val(ev.type)
                            events.append((ev.timestamp, "CLUSTER", f"[{cluster_id[:15]}] {et}"))
                except Exception:
                    pass

        events.sort(key=lambda x: x[0])

        lines = [f"Timeline for run {run_id}\n"]
        lines.append(f"{'Timestamp (UTC)':<22} {'Category':<10} {'Event'}")
        lines.append("-" * 80)

        for ts_ms, category, desc in events:
            lines.append(f"{ms_to_str(ts_ms):<22} {category:<10} {desc}")

        return "\n".join(lines)

    @mcp.tool()
    def investigate_failure(run_id: int) -> str:
        """One-shot failure investigation: gathers task output, cluster events, and log excerpts, then classifies the failure.

        Returns: failure_category, root_cause_hypothesis, evidence, affected_tasks, suggested_next_steps.
        """
        w = get_workspace_client()

        try:
            run = w.jobs.get_run(run_id=run_id)
        except Exception as e:
            return f"Failed to fetch run {run_id}: {e}"

        result_state = enum_val(run.state.result_state if run.state else None)
        if result_state == "SUCCESS":
            return f"Run {run_id} succeeded. Nothing to investigate."

        tasks = run.tasks or []
        failed_tasks = [
            t for t in tasks
            if t.state and enum_val(t.state.result_state) in ("FAILED", "TIMED_OUT")
        ]

        # Gather task output for failed tasks
        error_texts: list[str] = []
        for task in failed_tasks[:3]:  # cap at 3 to avoid excessive API calls
            try:
                out = w.jobs.get_run_output(run_id=task.run_id)
                if out.error:
                    error_texts.append(f"[{task.task_key}] {out.error}")
                if out.error_trace:
                    # Take first 10 lines of trace
                    trace_lines = out.error_trace.strip().splitlines()[:10]
                    error_texts.append("[{}] trace: {}".format(task.task_key, "\n".join(trace_lines)))
            except Exception:
                pass

        # Gather cluster events
        cluster_ids: set[str] = set()
        for task in tasks:
            if task.cluster_instance and task.cluster_instance.cluster_id:
                cluster_ids.add(task.cluster_instance.cluster_id)

        cluster_events: list[str] = []
        if run.start_time and run.end_time:
            window_start = run.start_time - 300_000
            window_end = run.end_time + 300_000
            for cid in cluster_ids:
                try:
                    for ev in w.clusters.events(
                        cluster_id=cid,
                        start_time=window_start,
                        end_time=window_end,
                        order="DESC",
                    ):
                        et = enum_val(ev.type)
                        if et in ("DRIVER_NOT_RESPONDING", "METASTORE_DOWN", "NODES_LOST",
                                  "NODE_BLACKLISTED", "CLUSTER_TERMINATED"):
                            cluster_events.append(f"{ms_to_str(ev.timestamp)} {et} [{cid[:15]}]")
                        if len(cluster_events) >= 10:
                            break
                except Exception:
                    pass

        # Heuristic classification
        category, hypothesis = _classify_failure(error_texts, cluster_events, result_state)

        # Build output
        lines = [
            f"=== Failure Investigation: Run {run_id} ===\n",
            f"Status:              {result_state}",
            f"Affected tasks:      {[t.task_key for t in failed_tasks] or ['(none identified)']}",
            f"Failure category:    {category}",
            f"Root cause:          {hypothesis}",
        ]

        if error_texts:
            lines.append("\nError evidence:")
            for e in error_texts[:5]:
                lines.append(f"  {e[:200]}")

        if cluster_events:
            lines.append("\nCluster events during run:")
            for e in cluster_events:
                lines.append(f"  {e}")

        lines.append("\nSuggested next steps:")
        for step in _next_steps(category, cluster_ids):
            lines.append(f"  • {step}")

        return "\n".join(lines)


def _classify_failure(error_texts: list[str], cluster_events: list[str], result_state: str) -> tuple[str, str]:
    combined = "\n".join(error_texts + cluster_events).lower()

    if result_state == "TIMED_OUT":
        return "TIMEOUT", "Job exceeded the configured timeout. Check task durations for the slowest bottleneck."
    if "outofmemoryerror" in combined or "gc overhead limit" in combined:
        return "OOM", "Driver or executor ran out of heap memory. Check executor memory settings and data skew."
    if "driver_not_responding" in combined or "driverstoppedexception" in combined:
        return "DRIVER_CRASH", "Spark driver crashed or became unresponsive. Check driver memory and driver logs."
    if "metastore_down" in combined or "metastore" in combined:
        return "METASTORE", "HMS metastore connection failure during the run. Check metastore availability."
    if "nodes_lost" in combined or "executorlossfailure" in combined:
        return "NODE_LOSS", "Worker node(s) lost during execution. Check cluster events for spot preemptions or node health."
    if error_texts:
        return "APPLICATION_ERROR", f"Application-level error: {error_texts[0][:150]}"

    return "UNKNOWN", "Could not classify from available data. Try get_driver_logs for more detail."


def _next_steps(category: str, cluster_ids: set[str]) -> list[str]:
    cid_str = next(iter(cluster_ids), "<cluster_id>")
    steps = {
        "OOM": [
            f"get_driver_logs(cluster_id='{cid_str}', search_pattern='OutOfMemoryError')",
            "Check spark.executor.memory and spark.driver.memory settings",
            "Use get_spark_executors to check per-executor memory usage (if cluster still running)",
        ],
        "DRIVER_CRASH": [
            f"get_driver_logs(cluster_id='{cid_str}', log_type='stderr', tail_lines=300)",
            f"get_cluster_events(cluster_id='{cid_str}', event_types=['DRIVER_NOT_RESPONDING'])",
        ],
        "METASTORE": [
            f"get_cluster_events(cluster_id='{cid_str}', event_types=['METASTORE_DOWN'])",
            "Check if HMS was available during the run window",
        ],
        "NODE_LOSS": [
            f"get_cluster_events(cluster_id='{cid_str}', event_types=['NODES_LOST', 'NODE_BLACKLISTED'])",
            "Check for spot instance preemptions in cluster events",
        ],
        "TIMEOUT": [
            "get_run_timeline to identify which tasks were running at timeout",
            "Use get_spark_stages on a re-run to identify the slowest stage",
        ],
    }
    return steps.get(category, [
        f"get_driver_logs(cluster_id='{cid_str}', log_type='stderr')",
        "get_task_output to review full stack traces",
    ])
