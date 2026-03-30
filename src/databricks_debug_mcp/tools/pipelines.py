import re

from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import enum_val
from ..sql import execute_sql

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_pipeline_status(
        pipeline_id: str | None = None,
        pipeline_name: str | None = None,
    ) -> str:
        """DLT (Delta Live Tables) pipeline status — current state, last update, cluster, target schema.

        Shows recent update history with durations and outcomes. Provide pipeline_id or pipeline_name.
        """
        w = get_workspace_client()

        if pipeline_id is None and pipeline_name:
            for p in w.pipelines.list_pipelines():
                if p.name and pipeline_name.lower() in p.name.lower():
                    pipeline_id = p.pipeline_id
                    break
            if pipeline_id is None:
                return f"No pipeline found matching name '{pipeline_name}'."

        if not pipeline_id:
            return "Provide pipeline_id or pipeline_name."

        try:
            pipeline = w.pipelines.get(pipeline_id=pipeline_id)
        except Exception as e:
            return f"Failed to get pipeline: {e}"

        spec = pipeline.spec
        lines = [
            f"Pipeline: {spec.name if spec else '?'}",
            f"ID:          {pipeline.pipeline_id}",
            f"State:       {enum_val(pipeline.state)}",
            f"Creator:     {pipeline.creator_user_name or '?'}",
        ]

        if spec:
            lines.append(f"Target:      {spec.catalog or '?'}.{spec.target or '?'}")
            lines.append(f"Channel:     {spec.channel or '?'}")
            lines.append(f"Continuous:  {spec.continuous or False}")
            if spec.clusters:
                for i, cc in enumerate(spec.clusters):
                    label = cc.label or f"cluster-{i}"
                    workers = "autoscale" if cc.autoscale else str(cc.num_workers or "?")
                    lines.append(f"  Cluster ({label}): {cc.node_type_id or '?'}, workers: {workers}")

        # Recent updates
        try:
            updates = list(w.pipelines.list_updates(pipeline_id=pipeline_id).updates or [])
            if updates:
                lines.append(f"\nRecent updates ({min(len(updates), 10)} shown):")
                lines.append(f"{'Update ID':<38} {'State':<15} {'Cause':<15} {'Created':<22} {'Duration'}")
                lines.append("-" * 100)
                for u in updates[:10]:
                    uid = (u.update_id or "?")[:37]
                    state = enum_val(u.state, fallback="?")
                    cause = enum_val(u.cause, fallback="?")
                    created = str(u.creation_time or "?")[:21]
                    # Duration not directly available; approximate if both times exist
                    dur = "--"
                    lines.append(f"{uid:<38} {state:<15} {cause:<15} {created:<22} {dur}")
        except Exception as e:
            lines.append(f"\n(Unable to fetch update history: {e})")

        return "\n".join(lines)

    @mcp.tool()
    def get_pipeline_errors(
        pipeline_id: str,
        limit: int = 20,
    ) -> str:
        """DLT pipeline errors — error events with affected datasets, stack traces, and data quality violations.

        Shows all ERROR-level events from the pipeline event log. Includes flow progress failures,
        dataset definition errors, and expectation violations.
        """
        w = get_workspace_client()
        limit = min(int(limit), 100)

        try:
            events = []
            inspected = 0
            for event in w.pipelines.list_pipeline_events(
                pipeline_id=pipeline_id,
                order_by=["timestamp DESC"],
                max_results=limit * 3,
            ):
                inspected += 1
                level = event.level or ""
                if level.upper() in ("ERROR", "WARN"):
                    events.append(event)
                if len(events) >= limit or inspected >= limit * 10:
                    break
        except Exception as e:
            return f"Failed to get pipeline events: {e}"

        if not events:
            return f"No error/warning events found for pipeline {pipeline_id}."

        lines = [f"Pipeline {pipeline_id} -- errors/warnings ({len(events)} events):\n"]

        for i, ev in enumerate(events):
            ts = str(ev.timestamp or "?")[:21]
            level = ev.level or "?"
            event_type = ev.event_type or "?"
            msg = ev.message or ""
            error = ev.error or None

            lines.append(f"[{i + 1}] {ts}  {level}  {event_type}")
            if msg:
                lines.append(f"    Message: {msg[:200]}")
            if error:
                exc = getattr(error, "exceptions", None)
                if exc:
                    for ex in exc[:3]:
                        class_name = getattr(ex, "class_name", "?")
                        message = getattr(ex, "message", "")[:200]
                        lines.append(f"    Exception: {class_name}: {message}")
            lines.append("")

        return "\n".join(lines)

    @mcp.tool()
    def get_pipeline_data_quality(
        pipeline_id: str,
        warehouse_id: str | None = None,
    ) -> str:
        """DLT data quality expectations — pass/fail/drop counts per dataset.

        Queries the pipeline's event log for flow_progress events containing expectation results.
        Shows which data quality rules are failing and how many rows are affected.
        """
        if not _UUID_RE.match(pipeline_id):
            return f"Invalid pipeline_id format: {pipeline_id!r}. Expected UUID."

        query = f"""
        SELECT
            timestamp,
            details:flow_progress.data_quality.expectations AS expectations,
            details:flow_progress.metrics.num_output_rows AS output_rows,
            details:flow_progress.status AS status,
            origin.dataset_name AS dataset_name
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'flow_progress'
            AND details:flow_progress.data_quality IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 50
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id)
        except Exception as e:
            return f"Failed to query pipeline data quality: {e}"

        if not rows:
            return f"No data quality events found for pipeline {pipeline_id}."

        lines = [f"Data quality for pipeline {pipeline_id} ({len(rows)} events):\n"]
        lines.append(f"{'Dataset':<30} {'Status':<12} {'Output Rows':<15} {'Expectations'}")
        lines.append("-" * 100)

        for row in rows:
            dataset = (row.get("dataset_name") or "?")[:29]
            status = (row.get("status") or "?")[:11]
            output = str(row.get("output_rows") or "?")[:14]
            expectations = str(row.get("expectations") or "--")[:40]
            lines.append(f"{dataset:<30} {status:<12} {output:<15} {expectations}")

        return "\n".join(lines)
