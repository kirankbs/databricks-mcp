"""Post-mortem Spark debugging tools — work on TERMINATED clusters via event logs.

These tools read Spark event logs from cluster log delivery (DBFS) to provide
the same stage/task analysis that get_spark_stages and analyze_stage_skew provide
for running clusters. The key difference: these work after the cluster is gone.

Requires cluster log delivery to be configured on the cluster.
"""

from mcp.server.fastmcp import FastMCP

from ..event_log import read_and_parse_event_log
from ..formatting import format_bytes, format_duration, truncate_stacktrace

_STAGE_EVENTS = {
    "SparkListenerStageCompleted",
    "SparkListenerStageSubmitted",
}

_TASK_EVENTS = {
    "SparkListenerTaskEnd",
}

_ALL_DEBUG_EVENTS = (
    _STAGE_EVENTS
    | _TASK_EVENTS
    | {
        "SparkListenerJobStart",
        "SparkListenerJobEnd",
        "SparkListenerExecutorAdded",
        "SparkListenerExecutorRemoved",
    }
)

_NO_LOG_DELIVERY = (
    "Cannot read event logs: {err}\n\n"
    "Event log tools require cluster log delivery to be configured.\n"
    "To enable: Cluster config → Advanced Options → Logging → set a DBFS destination.\n"
    "Without log delivery, Spark event logs are ephemeral and lost when the cluster terminates."
)


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_event_log_stages(
        cluster_id: str,
        top_n: int = 20,
    ) -> str:
        """Post-mortem stage analysis for TERMINATED clusters — the event-log equivalent
        of get_spark_stages.

        Reads Spark event logs from cluster log delivery (DBFS) and extracts completed
        stage metrics: duration, task count, shuffle read/write, input/output bytes,
        spill. Sorted by duration descending.

        Requires cluster log delivery to be configured on the cluster.
        cluster_id: the Databricks cluster ID (not the run ID — use get_cluster_for_run first).
        top_n: number of slowest stages to return (default 20).
        """
        try:
            events, config = read_and_parse_event_log(
                cluster_id,
                event_types=_STAGE_EVENTS,
            )
        except (ValueError, ImportError) as e:
            return _NO_LOG_DELIVERY.format(err=e)

        completed = [e for e in events if e.get("Event") == "SparkListenerStageCompleted"]
        if not completed:
            return f"No completed stages found in event logs for cluster {cluster_id}."

        stages = []
        for event in completed:
            info = event.get("Stage Info", {})
            stage_id = info.get("Stage ID", "?")
            name = info.get("Stage Name", "")[:60]
            num_tasks = info.get("Number of Tasks", 0)

            sub_time = info.get("Submission Time")
            comp_time = info.get("Completion Time")
            duration_ms = (comp_time - sub_time) if (sub_time and comp_time) else None

            # Extract accumulated metrics (filter to numeric values only)
            accums = {}
            for a in info.get("Accumulables", []):
                val = a.get("Value", 0)
                if isinstance(val, (int, float)):
                    accums[a.get("Name", "")] = val
            shuffle_read = accums.get("internal.metrics.shuffle.read.remoteBytesRead", 0)
            shuffle_write = accums.get("internal.metrics.shuffle.write.bytesWritten", 0)
            input_bytes = accums.get("internal.metrics.input.bytesRead", 0)
            output_bytes = accums.get("internal.metrics.output.bytesWritten", 0)
            spill_memory = accums.get("internal.metrics.memoryBytesSpilled", 0)
            spill_disk = accums.get("internal.metrics.diskBytesSpilled", 0)

            stages.append(
                {
                    "stage_id": stage_id,
                    "name": name,
                    "num_tasks": num_tasks,
                    "duration_ms": duration_ms,
                    "shuffle_read": shuffle_read,
                    "shuffle_write": shuffle_write,
                    "input_bytes": input_bytes,
                    "output_bytes": output_bytes,
                    "spill_memory": spill_memory,
                    "spill_disk": spill_disk,
                }
            )

        stages.sort(key=lambda s: s["duration_ms"] or 0, reverse=True)
        stages = stages[:top_n]

        lines = [
            f"Event log stages — cluster {cluster_id} ({len(completed)} total, showing top {len(stages)}):",
            f"Source: {config.log_base_path}\n",
            f"{'Stage':<8} {'Tasks':<8} {'Duration':<12} {'Shuffle R':<12} {'Shuffle W':<12} {'Input':<12} {'Spill Disk':<12} {'Name'}",
            "-" * 110,
        ]

        for s in stages:
            lines.append(
                f"{s['stage_id']:<8} {s['num_tasks']:<8} "
                f"{format_duration(s['duration_ms']):<12} "
                f"{format_bytes(s['shuffle_read']):<12} "
                f"{format_bytes(s['shuffle_write']):<12} "
                f"{format_bytes(s['input_bytes']):<12} "
                f"{format_bytes(s['spill_disk']):<12} "
                f"{s['name']}"
            )

        spill_stages = [s for s in stages if s["spill_disk"] > 0]
        if spill_stages:
            lines.append(f"\n⚠ {len(spill_stages)} stage(s) spilled to disk — possible memory pressure or data skew.")

        return "\n".join(lines)

    @mcp.tool()
    def get_event_log_failures(
        cluster_id: str,
        max_failures: int = 30,
    ) -> str:
        """Failed tasks from event logs — exception messages, failure reasons, and which
        executors/stages were affected. Works on TERMINATED clusters.

        Shows: stage ID, task ID, executor, failure reason, and truncated stack trace.
        Use this to understand WHY a job failed after the cluster is gone.

        Requires cluster log delivery to be configured on the cluster.
        cluster_id: the Databricks cluster ID.
        max_failures: maximum number of failed tasks to return (default 30).
        """
        try:
            events, config = read_and_parse_event_log(
                cluster_id,
                event_types=_TASK_EVENTS | {"SparkListenerExecutorRemoved"},
            )
        except (ValueError, ImportError) as e:
            return _NO_LOG_DELIVERY.format(err=e)

        task_events = [e for e in events if e.get("Event") == "SparkListenerTaskEnd"]
        executor_removals = [e for e in events if e.get("Event") == "SparkListenerExecutorRemoved"]

        failed_tasks = []
        for event in task_events:
            reason = event.get("Task End Reason", {})
            if reason.get("Reason") == "Success":
                continue

            task_info = event.get("Task Info", {})
            if not task_info.get("Failed", False):
                continue

            stage_id = event.get("Stage ID", "?")
            task_id = task_info.get("Task ID", "?")
            executor_id = task_info.get("Executor ID", "?")
            host = task_info.get("Host", "?")
            reason_type = reason.get("Reason", "Unknown")

            description = truncate_stacktrace(reason.get("Description", ""), max_lines=15)

            failed_tasks.append(
                {
                    "stage_id": stage_id,
                    "task_id": task_id,
                    "executor_id": executor_id,
                    "host": host,
                    "reason": reason_type,
                    "description": description,
                }
            )

        if not failed_tasks and not executor_removals:
            return f"No task failures or executor losses found in event logs for cluster {cluster_id}."

        lines = [
            f"Event log failures — cluster {cluster_id}",
            f"Source: {config.log_base_path}\n",
        ]

        if failed_tasks:
            shown = failed_tasks[:max_failures]
            lines.append(f"FAILED TASKS ({len(failed_tasks)} total, showing {len(shown)}):\n")

            # Group by failure reason for summary
            reason_counts: dict[str, int] = {}
            for t in failed_tasks:
                reason_counts[t["reason"]] = reason_counts.get(t["reason"], 0) + 1
            lines.append("Failure breakdown:")
            for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                lines.append(f"  {reason}: {count}")
            lines.append("")

            # Group by stage for summary
            stage_counts: dict[str, int] = {}
            for t in failed_tasks:
                key = str(t["stage_id"])
                stage_counts[key] = stage_counts.get(key, 0) + 1
            lines.append("Affected stages:")
            for stage, count in sorted(stage_counts.items(), key=lambda x: -x[1]):
                lines.append(f"  Stage {stage}: {count} failures")
            lines.append("")

            lines.append(f"{'Stage':<8} {'Task':<8} {'Executor':<12} {'Host':<16} {'Reason'}")
            lines.append("-" * 80)
            for t in shown:
                lines.append(
                    f"{t['stage_id']:<8} {t['task_id']:<8} {t['executor_id']:<12} {t['host']:<16} {t['reason']}"
                )
                if t["description"]:
                    for desc_line in t["description"].splitlines()[:8]:
                        lines.append(f"    {desc_line}")
                    lines.append("")

        if executor_removals:
            lines.append(f"\nEXECUTOR REMOVALS ({len(executor_removals)}):\n")
            for ev in executor_removals:
                eid = ev.get("Executor ID", "?")
                reason = ev.get("Removed Reason", "Unknown")
                lines.append(f"  Executor {eid}: {reason}")

        return "\n".join(lines)

    @mcp.tool()
    def analyze_event_log_skew(
        cluster_id: str,
        stage_id: int | None = None,
        skew_threshold: float = 3.0,
    ) -> str:
        """Post-mortem skew analysis for TERMINATED clusters — the event-log equivalent
        of analyze_stage_skew.

        Reads individual task metrics from event logs to compute per-stage skew ratios
        (max task duration / median task duration). Flags stages with data skew and spill.

        Requires cluster log delivery to be configured on the cluster.
        cluster_id: the Databricks cluster ID.
        stage_id: analyze only this stage (default: all stages).
        skew_threshold: flag stages where max/median exceeds this ratio (default 3.0).
        """
        try:
            events, config = read_and_parse_event_log(
                cluster_id,
                event_types=_TASK_EVENTS | _STAGE_EVENTS,
            )
        except (ValueError, ImportError) as e:
            return _NO_LOG_DELIVERY.format(err=e)

        # Collect task durations per stage
        stage_tasks: dict[int, list[dict]] = {}
        for event in events:
            if event.get("Event") != "SparkListenerTaskEnd":
                continue

            sid = event.get("Stage ID")
            if sid is None:
                continue
            if stage_id is not None and sid != stage_id:
                continue

            task_info = event.get("Task Info", {})
            metrics = event.get("Task Metrics", {})

            launch = task_info.get("Launch Time", 0)
            finish = task_info.get("Finish Time", 0)
            duration_ms = finish - launch if (launch and finish) else 0

            shuffle_read = metrics.get("Shuffle Read Metrics", {})
            shuffle_write = metrics.get("Shuffle Write Metrics", {})

            task_data = {
                "task_id": task_info.get("Task ID"),
                "executor_id": task_info.get("Executor ID", "?"),
                "duration_ms": duration_ms,
                "executor_run_time": metrics.get("Executor Run Time", 0),
                "gc_time": metrics.get("JVM GC Time", 0),
                "spill_memory": metrics.get("Memory Bytes Spilled", 0),
                "spill_disk": metrics.get("Disk Bytes Spilled", 0),
                "shuffle_read_bytes": shuffle_read.get("Remote Bytes Read", 0)
                + shuffle_read.get("Local Bytes Read", 0),
                "shuffle_write_bytes": shuffle_write.get("Shuffle Bytes Written", 0),
                "input_bytes": metrics.get("Input Metrics", {}).get("Bytes Read", 0),
            }

            stage_tasks.setdefault(sid, []).append(task_data)

        # Get stage names from completed stage events
        stage_names: dict[int, str] = {}
        for event in events:
            if event.get("Event") == "SparkListenerStageCompleted":
                info = event.get("Stage Info", {})
                sid = info.get("Stage ID")
                if sid is not None:
                    stage_names[sid] = (info.get("Stage Name") or "")[:60]

        if not stage_tasks:
            if stage_id is not None:
                return f"No task data for stage {stage_id} in event logs for cluster {cluster_id}."
            return f"No task data found in event logs for cluster {cluster_id}."

        analyses = []
        for sid, tasks in stage_tasks.items():
            if len(tasks) < 2:
                continue

            durations = sorted(t["executor_run_time"] for t in tasks)
            median_ms = durations[len(durations) // 2]
            p95_idx = int(len(durations) * 0.95)
            p95_ms = durations[min(p95_idx, len(durations) - 1)]
            max_ms = durations[-1]

            skew_ratio = max_ms / median_ms if median_ms > 0 else 0.0

            total_spill_mem = sum(t["spill_memory"] for t in tasks)
            total_spill_disk = sum(t["spill_disk"] for t in tasks)
            total_shuffle_read = sum(t["shuffle_read_bytes"] for t in tasks)
            total_gc = sum(t["gc_time"] for t in tasks)
            total_runtime = sum(t["executor_run_time"] for t in tasks)
            gc_pct = (total_gc / total_runtime * 100) if total_runtime > 0 else 0

            # Find straggler task
            straggler = max(tasks, key=lambda t: t["executor_run_time"])

            analyses.append(
                {
                    "stage_id": sid,
                    "name": stage_names.get(sid, ""),
                    "num_tasks": len(tasks),
                    "median_ms": median_ms,
                    "p95_ms": p95_ms,
                    "max_ms": max_ms,
                    "skew_ratio": skew_ratio,
                    "spill_memory": total_spill_mem,
                    "spill_disk": total_spill_disk,
                    "shuffle_read": total_shuffle_read,
                    "gc_pct": gc_pct,
                    "straggler_executor": straggler["executor_id"],
                    "straggler_task": straggler["task_id"],
                }
            )

        if not analyses:
            return f"No stages with enough tasks to analyze in event logs for cluster {cluster_id}."

        skewed = [a for a in analyses if a["skew_ratio"] > skew_threshold]
        spill = [a for a in analyses if a["spill_disk"] > 0]

        lines = [
            f"Event log skew analysis — cluster {cluster_id} ({len(analyses)} stages analyzed)",
            f"Source: {config.log_base_path}\n",
        ]

        if skewed:
            lines.append(f"SKEWED STAGES ({len(skewed)}):")
            for a in sorted(skewed, key=lambda x: x["skew_ratio"], reverse=True):
                lines.append(
                    f"  Stage {a['stage_id']}: skew ratio {a['skew_ratio']:.1f}x "
                    f"(median: {format_duration(a['median_ms'])}, max: {format_duration(a['max_ms'])}, "
                    f"tasks: {a['num_tasks']})"
                )
                lines.append(f"    Straggler: task {a['straggler_task']} on executor {a['straggler_executor']}")
                if a["name"]:
                    lines.append(f"    Description: {a['name']}")
        else:
            lines.append("No skewed stages detected.")

        lines.append("")

        if spill:
            lines.append(f"STAGES WITH SPILL ({len(spill)}):")
            for a in sorted(spill, key=lambda x: x["spill_disk"], reverse=True):
                lines.append(
                    f"  Stage {a['stage_id']}: "
                    f"spill memory={format_bytes(a['spill_memory'])}, "
                    f"spill disk={format_bytes(a['spill_disk'])}"
                )
        else:
            lines.append("No spill detected.")

        lines.append("")
        lines.append(
            f"{'Stage':<8} {'Tasks':<8} {'Median':<10} {'P95':<10} {'Max':<10} {'Skew':<8} {'GC%':<8} {'Spill Disk':<12} {'Shuffle R':<12}"
        )
        lines.append("-" * 100)

        top = sorted(analyses, key=lambda x: x["skew_ratio"], reverse=True)[:20]
        for a in top:
            lines.append(
                f"{a['stage_id']:<8} {a['num_tasks']:<8} "
                f"{format_duration(a['median_ms']):<10} {format_duration(a['p95_ms']):<10} "
                f"{format_duration(a['max_ms']):<10} {a['skew_ratio']:<8.1f} "
                f"{a['gc_pct']:<8.1f} "
                f"{format_bytes(a['spill_disk']):<12} "
                f"{format_bytes(a['shuffle_read']):<12}"
            )

        return "\n".join(lines)
