from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import format_duration, ms_to_str, truncate_stacktrace, enum_val


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def list_job_runs(
        job_name: str | None = None,
        job_id: int | None = None,
        limit: int = 10,
        status_filter: str | None = None,
    ) -> str:
        """List recent runs for a Databricks job.

        Provide either job_name or job_id. status_filter accepts: SUCCESS, FAILED,
        TIMED_OUT, CANCELLED. Returns run IDs, times, durations, statuses, and trigger types.
        """
        w = get_workspace_client()

        if job_id is None and job_name is not None:
            for job in w.jobs.list(name=job_name):
                job_id = job.job_id
                break
            if job_id is None:
                return f"No job found with name '{job_name}'"

        if job_id is None:
            return "Provide either job_name or job_id."

        runs = []
        # Fetch more than needed to allow client-side status filtering
        fetch_limit = limit * 5 if status_filter else limit
        for run in w.jobs.list_runs(job_id=job_id, expand_tasks=True, completed_only=True if status_filter else False):
            if status_filter:
                result_state = enum_val(run.state.result_state if run.state else None)
                if result_state != status_filter:
                    continue
            runs.append(run)
            if len(runs) >= limit:
                break

        if not runs:
            suffix = f" with status {status_filter}" if status_filter else ""
            return f"No runs found for job {job_id}{suffix}."

        lines = [f"Job {job_id} — {len(runs)} run(s)\n"]
        header = f"{'Run ID':<16} {'Start (UTC)':<22} {'Duration':<12} {'Status':<20} {'Trigger'}"
        lines.append(header)
        lines.append("-" * 82)

        for run in runs:
            duration_ms = (run.end_time - run.start_time) if run.end_time and run.start_time else None
            state = run.state
            life = enum_val(state.life_cycle_state if state else None)
            result = enum_val(state.result_state if state else None, fallback="")
            status = f"{life}/{result}" if result else life
            trigger = enum_val(run.trigger, fallback="—")
            lines.append(
                f"{run.run_id:<16} {ms_to_str(run.start_time):<22} "
                f"{format_duration(duration_ms):<12} {status:<20} {trigger}"
            )

        return "\n".join(lines)

    @mcp.tool()
    def get_run_details(run_id: int) -> str:
        """Detailed breakdown of a single job run — all task statuses, durations, cluster IDs, and error messages."""
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)

        state = run.state
        life = enum_val(state.life_cycle_state if state else None)
        result = enum_val(state.result_state if state else None, fallback="—")
        duration_ms = (run.end_time - run.start_time) if run.end_time and run.start_time else None

        lines = [
            f"Run {run_id}",
            f"Job ID:    {run.job_id}",
            f"Start:     {ms_to_str(run.start_time)}",
            f"Duration:  {format_duration(duration_ms)}",
            f"Status:    {life} / {result}",
            f"Trigger:   {enum_val(run.trigger, fallback='—')}",
        ]

        tasks = run.tasks or []
        if tasks:
            lines.append("")
            lines.append(f"{'Task Key':<35} {'Status':<22} {'Duration':<12} {'Cluster ID':<25} {'Attempt'}")
            lines.append("-" * 100)
            failed_messages = []

            for task in tasks:
                ts = task.state
                t_life = enum_val(ts.life_cycle_state if ts else None)
                t_result = enum_val(ts.result_state if ts else None, fallback="")
                t_status = f"{t_life}/{t_result}" if t_result else t_life
                t_dur = (task.end_time - task.start_time) if task.end_time and task.start_time else None
                cluster_id = task.cluster_instance.cluster_id if task.cluster_instance else "—"
                attempt = task.attempt_number if task.attempt_number is not None else 0
                lines.append(
                    f"{task.task_key:<35} {t_status:<22} {format_duration(t_dur):<12} {cluster_id:<25} {attempt}"
                )

                if ts and ts.state_message and t_result in ("FAILED", "TIMED_OUT"):
                    failed_messages.append((task.task_key, ts.state_message))

            if failed_messages:
                lines.append("")
                lines.append("Failure messages:")
                for key, msg in failed_messages:
                    lines.append(f"  {key}: {msg}")

        return "\n".join(lines)

    @mcp.tool()
    def get_task_output(run_id: int, task_key: str | None = None) -> str:
        """Error output and stack trace for a task within a job run.

        If task_key is omitted, auto-selects failed tasks. Returns error_message,
        stack trace (truncated to 30 lines), notebook output, and task logs.
        """
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)
        tasks = run.tasks or []

        if task_key:
            targets = [t for t in tasks if t.task_key == task_key]
            if not targets:
                return f"Task '{task_key}' not found in run {run_id}. Available: {[t.task_key for t in tasks]}"
        else:
            failed = [
                t for t in tasks
                if t.state and enum_val(t.state.result_state) in ("FAILED", "TIMED_OUT")
            ]
            targets = failed if failed else (tasks[:1] if tasks else [])

        if not targets:
            return f"No tasks found in run {run_id}."

        outputs = []
        for task in targets:
            task_run_id = task.run_id
            try:
                out = w.jobs.get_run_output(run_id=task_run_id)
            except Exception as e:
                outputs.append(f"=== {task.task_key} (run_id {task_run_id}) ===\nFailed to fetch output: {e}")
                continue

            block = [f"=== {task.task_key} (run_id: {task_run_id}) ==="]
            if out.error:
                block.append(f"\nError:\n  {out.error}")
            if out.error_trace:
                block.append(f"\nStack trace:\n{truncate_stacktrace(out.error_trace)}")
            if out.logs:
                log_excerpt = out.logs[-2000:] if len(out.logs) > 2000 else out.logs
                block.append(f"\nTask logs:\n{log_excerpt}")
            if out.notebook_output and out.notebook_output.result:
                result = out.notebook_output.result[:1000]
                block.append(f"\nNotebook output:\n{result}")
            if len(block) == 1:
                block.append("(No output available for this task)")

            outputs.append("\n".join(block))

        return "\n\n".join(outputs)

    @mcp.tool()
    def get_cluster_for_run(run_id: int) -> str:
        """Resolve which cluster(s) a job run used, mapped by task key.

        Distinguishes ephemeral job clusters from shared existing clusters.
        Returns cluster IDs needed to call get_cluster_info or get_cluster_events.
        """
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)
        tasks = run.tasks or []

        if not tasks:
            return f"Run {run_id} has no tasks."

        cluster_map: dict[str, dict] = {}
        for task in tasks:
            if task.cluster_instance and task.cluster_instance.cluster_id:
                cluster_id = task.cluster_instance.cluster_id
                # new_cluster on task means it's a task-level ephemeral cluster
                is_job_cluster = task.new_cluster is not None
                cluster_map[task.task_key] = {
                    "cluster_id": cluster_id,
                    "is_job_cluster": is_job_cluster,
                }

        if not cluster_map:
            return f"No cluster assignments found in run {run_id} (run may still be pending)."

        lines = [f"Cluster assignments for run {run_id}:\n"]
        lines.append(f"{'Task Key':<35} {'Cluster ID':<30} {'Type'}")
        lines.append("-" * 75)
        for task_key, info in cluster_map.items():
            ctype = "Job cluster (ephemeral)" if info["is_job_cluster"] else "Shared cluster"
            lines.append(f"{task_key:<35} {info['cluster_id']:<30} {ctype}")

        unique = sorted(set(v["cluster_id"] for v in cluster_map.values()))
        lines.append(f"\nUnique cluster IDs: {', '.join(unique)}")
        return "\n".join(lines)
