import time

from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import deduplicate_events, enum_val, ms_to_str


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_cluster_info(cluster_id: str) -> str:
        """Cluster config and current state: node types, Spark version, spark_conf, autoscaling, log delivery path, and termination reason."""
        w = get_workspace_client()
        c = w.clusters.get(cluster_id=cluster_id)

        lines = [
            f"Cluster: {c.cluster_name or cluster_id}",
            f"ID:              {c.cluster_id}",
            f"State:           {enum_val(c.state)}",
            f"Spark version:   {c.spark_version}",
            f"Driver type:     {c.driver_node_type_id or '—'}",
            f"Worker type:     {c.node_type_id or '—'}",
        ]

        if c.autoscale:
            lines.append(f"Workers:         {c.autoscale.min_workers}–{c.autoscale.max_workers} (autoscale)")
        elif c.num_workers is not None:
            lines.append(f"Workers:         {c.num_workers} (fixed)")

        if c.spark_conf:
            interesting = {
                k: v
                for k, v in c.spark_conf.items()
                if any(x in k.lower() for x in ["memory", "cores", "executor", "driver", "shuffle", "sql", "gc"])
            }
            if interesting:
                lines.append("\nKey Spark conf:")
                for k in sorted(interesting):
                    lines.append(f"  {k}: {interesting[k]}")

        if c.cluster_log_conf:
            if c.cluster_log_conf.dbfs:
                lines.append(f"\nLog delivery:    DBFS — {c.cluster_log_conf.dbfs.destination}")
            elif c.cluster_log_conf.s3:
                lines.append(f"\nLog delivery:    S3 — {c.cluster_log_conf.s3.destination}")
            else:
                lines.append("\nLog delivery:    Configured (non-DBFS/S3)")
        else:
            lines.append("\nLog delivery:    NOT configured — get_driver_logs will not work")

        if c.termination_reason:
            tr = c.termination_reason
            reason_type = enum_val(tr.type)
            reason_code = enum_val(tr.code)
            lines.append(f"\nTermination:     {reason_type} / {reason_code}")
            if tr.parameters:
                for k, v in tr.parameters.items():
                    lines.append(f"  {k}: {v}")

        return "\n".join(lines)

    @mcp.tool()
    def get_cluster_events(
        cluster_id: str,
        event_types: list[str] | None = None,
        hours_back: float = 24.0,
        limit: int = 50,
    ) -> str:
        """Cluster event log: DRIVER_NOT_RESPONDING, METASTORE_DOWN, NODES_LOST, autoscaling, init script failures, etc.

        event_types filters to specific types, e.g. ["DRIVER_NOT_RESPONDING", "METASTORE_DOWN"].
        hours_back controls the lookback window. Consecutive identical events are collapsed into summaries.
        """
        w = get_workspace_client()

        now_ms = int(time.time() * 1000)
        start_ms = int(now_ms - hours_back * 3600 * 1000)

        raw_events = []
        for event in w.clusters.events(
            cluster_id=cluster_id,
            start_time=start_ms,
            end_time=now_ms,
            order="DESC",
        ):
            et = enum_val(event.type)
            if event_types and et not in event_types:
                continue
            raw_events.append(event)
            if len(raw_events) >= limit:
                break

        if not raw_events:
            suffix = f" of type {event_types}" if event_types else ""
            return f"No cluster events{suffix} for {cluster_id} in the last {hours_back}h."

        chronological = list(reversed(raw_events))
        dicts = []
        for ev in chronological:
            et = enum_val(ev.type)
            details = _event_details(ev)
            dicts.append({"timestamp": ev.timestamp, "type": et, "_details": details})
        deduped = deduplicate_events(dicts, type_key="type")

        lines = [f"Cluster {cluster_id} — events (last {hours_back}h, {len(raw_events)} raw → {len(deduped)} shown):\n"]
        lines.append(f"{'Timestamp (UTC)':<22} {'Type':<32} {'Details'}")
        lines.append("-" * 85)

        for ev in deduped:
            ts = ms_to_str(ev.get("timestamp"))
            et = ev.get("type", "?")
            details = ev.get("_details", "")

            if "_repeated" in ev:
                count = ev["_repeated"]
                first_ts = ms_to_str(ev.get("_first_timestamp"))
                last_ts = ms_to_str(ev.get("_last_timestamp"))
                lines.append(f"{first_ts:<22} {et:<32} ×{count} between {first_ts} and {last_ts}")
            else:
                lines.append(f"{ts:<22} {et:<32} {details}")

        return "\n".join(lines)


def _event_details(ev) -> str:
    det = ev.details
    if not det:
        return ""
    parts = []
    if det.current_num_workers is not None:
        parts.append(f"workers: {det.previous_num_workers or '?'}→{det.current_num_workers}")
    if det.target_num_workers is not None:
        parts.append(f"target: {det.target_num_workers}")
    if det.reason:
        parts.append(enum_val(det.reason))
    return ", ".join(parts)
