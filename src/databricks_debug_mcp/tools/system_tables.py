from databricks.sdk.service.sql import StatementParameterListItem
from mcp.server.fastmcp import FastMCP

from ..formatting import format_bytes, format_duration
from ..sql import execute_sql


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_job_cost(
        job_id: int | None = None,
        job_name: str | None = None,
        run_id: int | None = None,
        days_back: int = 30,
        warehouse_id: str | None = None,
    ) -> str:
        """Cost attribution for Databricks job runs using system.billing tables.

        Shows DBU consumption and estimated dollar cost per run. Provide job_id, job_name,
        or run_id to filter. Joins billing.usage with billing.list_prices for dollar amounts.
        Requires system tables access and a SQL warehouse.
        """
        days_back = int(days_back)
        params: list[StatementParameterListItem] = []

        conditions = [
            "u.usage_metadata.job_id IS NOT NULL",
            f"u.usage_date >= current_date() - INTERVAL {days_back} DAY",
        ]

        if run_id:
            conditions.append("u.usage_metadata.job_run_id = :run_id")
            params.append(StatementParameterListItem(name="run_id", value=str(int(run_id))))
        elif job_id:
            conditions.append("u.usage_metadata.job_id = :job_id")
            params.append(StatementParameterListItem(name="job_id", value=str(int(job_id))))
        elif job_name:
            conditions.append("u.usage_metadata.job_id IN (SELECT CAST(job_id AS STRING) FROM system.lakeflow.jobs WHERE name ILIKE :job_name)")
            params.append(StatementParameterListItem(name="job_name", value=f"%{job_name}%"))
        else:
            return "Provide job_id, job_name, or run_id."

        where = " AND ".join(conditions)

        query = f"""
        SELECT
            u.usage_metadata.job_id AS job_id,
            u.usage_metadata.job_run_id AS run_id,
            u.sku_name,
            u.usage_date,
            SUM(u.usage_quantity) AS total_dbu,
            SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS estimated_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name
            AND u.usage_date BETWEEN lp.price_start_time AND COALESCE(lp.price_end_time, current_date())
        WHERE {where}
        GROUP BY 1, 2, 3, 4
        ORDER BY usage_date DESC, total_dbu DESC
        LIMIT 100
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query job costs: {e}"

        if not rows:
            return "No billing data found for the specified criteria."

        run_totals: dict[str, dict] = {}
        for row in rows:
            rid = row.get("run_id", "?")
            key = str(rid)
            if key not in run_totals:
                run_totals[key] = {"job_id": row.get("job_id"), "dbu": 0.0, "cost": 0.0, "skus": set(), "date": row.get("usage_date")}
            run_totals[key]["dbu"] += float(row.get("total_dbu") or 0)
            run_totals[key]["cost"] += float(row.get("estimated_cost_usd") or 0)
            run_totals[key]["skus"].add(row.get("sku_name", ""))

        lines = [f"Job cost attribution ({len(run_totals)} runs, last {days_back} days):\n"]
        lines.append(f"{'Run ID':<16} {'Job ID':<12} {'Date':<12} {'DBUs':<12} {'Est. Cost':<12} {'SKUs'}")
        lines.append("-" * 85)

        total_dbu = 0.0
        total_cost = 0.0
        for rid, info in sorted(run_totals.items(), key=lambda x: x[1]["cost"], reverse=True):
            dbu = info["dbu"]
            cost = info["cost"]
            total_dbu += dbu
            total_cost += cost
            skus = ", ".join(sorted(info["skus"]))
            lines.append(f"{rid:<16} {str(info['job_id']):<12} {str(info['date']):<12} {dbu:<12.2f} ${cost:<11.2f} {skus}")

        lines.append("-" * 85)
        lines.append(f"{'TOTAL':<16} {'':<12} {'':<12} {total_dbu:<12.2f} ${total_cost:<11.2f}")

        return "\n".join(lines)

    @mcp.tool()
    def get_query_history(
        user: str | None = None,
        duration_ms_gt: int | None = None,
        error_only: bool = False,
        hours_back: float = 24.0,
        limit: int = 25,
        warehouse_id: str | None = None,
    ) -> str:
        """Query execution history from system.query.history — find slow, failing, or expensive queries.

        Filters by user, minimum duration, or error status. Shows statement text, duration,
        rows produced, and bytes scanned. Requires system tables access.
        """
        hours_back = min(float(hours_back), 720.0)
        limit = min(int(limit), 100)
        params: list[StatementParameterListItem] = []

        conditions = [f"execution_start_time_utc >= current_timestamp() - INTERVAL {int(hours_back)} HOUR"]
        if user:
            conditions.append("executed_by ILIKE :user_filter")
            params.append(StatementParameterListItem(name="user_filter", value=f"%{user}%"))
        if duration_ms_gt:
            conditions.append(f"total_duration_ms > {int(duration_ms_gt)}")
        if error_only:
            conditions.append("error_message IS NOT NULL AND error_message != ''")

        where = " AND ".join(conditions)

        query = f"""
        SELECT
            statement_id,
            executed_by,
            statement_text,
            statement_type,
            total_duration_ms,
            rows_produced,
            produced_rows_byte_count,
            read_bytes,
            error_message,
            execution_start_time_utc,
            warehouse_id
        FROM system.query.history
        WHERE {where}
        ORDER BY total_duration_ms DESC
        LIMIT {limit}
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query history: {e}"

        if not rows:
            return "No queries found matching criteria."

        lines = [f"Query history ({len(rows)} results, last {hours_back}h):\n"]

        for i, row in enumerate(rows):
            stmt = (row.get("statement_text") or "")[:120]
            dur_ms = int(row.get("total_duration_ms") or 0)
            read_b = int(row.get("read_bytes") or 0)
            rows_out = row.get("rows_produced", "?")
            error = row.get("error_message") or ""

            lines.append(f"[{i+1}] {row.get('executed_by', '?')} @ {row.get('execution_start_time_utc', '?')}")
            lines.append(f"    Duration: {format_duration(dur_ms)}  |  Rows: {rows_out}  |  Scanned: {format_bytes(read_b)}")
            lines.append(f"    SQL: {stmt}")
            if error:
                lines.append(f"    ERROR: {error[:200]}")
            lines.append("")

        return "\n".join(lines)

    @mcp.tool()
    def get_cluster_utilization(
        cluster_id: str,
        hours_back: float = 24.0,
        warehouse_id: str | None = None,
    ) -> str:
        """Cluster node utilization from system.compute.node_timeline — shows when nodes joined/left,
        actual vs. requested workers, and idle time between tasks.

        Useful for right-sizing clusters and debugging autoscaling behavior.
        """
        hours_back = min(float(hours_back), 720.0)
        params = [StatementParameterListItem(name="cluster_id", value=str(cluster_id))]

        query = f"""
        SELECT
            node_id,
            instance_id,
            private_ip,
            node_type,
            start_time,
            end_time,
            TIMESTAMPDIFF(SECOND, start_time, COALESCE(end_time, current_timestamp())) AS duration_sec
        FROM system.compute.node_timeline
        WHERE cluster_id = :cluster_id
            AND start_time >= current_timestamp() - INTERVAL {int(hours_back)} HOUR
        ORDER BY start_time
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query node timeline: {e}"

        if not rows:
            return f"No node timeline data for cluster {cluster_id} in the last {hours_back}h."

        driver_nodes = [r for r in rows if r.get("node_type") == "DRIVER"]
        worker_nodes = [r for r in rows if r.get("node_type") != "DRIVER"]

        total_worker_seconds = sum(int(r.get("duration_sec") or 0) for r in worker_nodes)
        unique_workers = len(set(r.get("node_id") for r in worker_nodes))
        max_concurrent = _max_concurrent_nodes(worker_nodes)

        lines = [f"Cluster {cluster_id} -- node utilization (last {hours_back}h):\n"]
        lines.append(f"Driver nodes:     {len(driver_nodes)}")
        lines.append(f"Unique workers:   {unique_workers}")
        lines.append(f"Peak concurrent:  {max_concurrent}")
        lines.append(f"Total worker-hrs: {total_worker_seconds / 3600:.1f}")
        lines.append("")

        lines.append(f"{'Node ID':<20} {'Type':<10} {'Start':<22} {'End':<22} {'Duration'}")
        lines.append("-" * 90)
        for r in rows[:50]:
            nid = str(r.get("node_id", "?"))[:19]
            ntype = r.get("node_type", "?")
            start = str(r.get("start_time", "?"))[:21]
            end = str(r.get("end_time") or "running")[:21]
            dur = format_duration(int(r.get("duration_sec") or 0) * 1000)
            lines.append(f"{nid:<20} {ntype:<10} {start:<22} {end:<22} {dur}")

        return "\n".join(lines)

    @mcp.tool()
    def get_audit_events(
        user: str | None = None,
        action: str | None = None,
        service: str | None = None,
        hours_back: float = 24.0,
        limit: int = 50,
        warehouse_id: str | None = None,
    ) -> str:
        """Security and access audit log from system.access.audit — who did what, when.

        Filter by user, action (e.g. 'changePermissions', 'deleteCluster'), or service
        (e.g. 'clusters', 'jobs', 'unityCatalog'). Useful for forensics: "who changed
        this cluster config?" or "what API calls preceded the failure?"
        """
        hours_back = min(float(hours_back), 720.0)
        limit = min(int(limit), 200)
        params: list[StatementParameterListItem] = []

        conditions = [f"event_time >= current_timestamp() - INTERVAL {int(hours_back)} HOUR"]
        if user:
            conditions.append("user_identity.email ILIKE :user_filter")
            params.append(StatementParameterListItem(name="user_filter", value=f"%{user}%"))
        if action:
            conditions.append("action_name ILIKE :action_filter")
            params.append(StatementParameterListItem(name="action_filter", value=f"%{action}%"))
        if service:
            conditions.append("service_name ILIKE :service_filter")
            params.append(StatementParameterListItem(name="service_filter", value=f"%{service}%"))

        where = " AND ".join(conditions)

        query = f"""
        SELECT
            event_time,
            user_identity.email AS user_email,
            service_name,
            action_name,
            request_params,
            response.status_code AS status_code,
            source_ip_address
        FROM system.access.audit
        WHERE {where}
        ORDER BY event_time DESC
        LIMIT {limit}
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query audit log: {e}"

        if not rows:
            return "No audit events found matching criteria."

        lines = [f"Audit log ({len(rows)} events, last {hours_back}h):\n"]
        lines.append(f"{'Time':<22} {'User':<30} {'Service':<18} {'Action':<30} {'Status'}")
        lines.append("-" * 110)

        for row in rows:
            ts = str(row.get("event_time", "?"))[:21]
            email = (row.get("user_email") or "?")[:29]
            svc = (row.get("service_name") or "?")[:17]
            act = (row.get("action_name") or "?")[:29]
            status = str(row.get("status_code") or "?")
            lines.append(f"{ts:<22} {email:<30} {svc:<18} {act:<30} {status}")

        return "\n".join(lines)


def _max_concurrent_nodes(nodes: list[dict]) -> int:
    """Calculate peak concurrent worker count from node timeline data."""
    events = []
    for n in nodes:
        start = n.get("start_time")
        end = n.get("end_time")
        if start:
            events.append((str(start), 1))
        if end:
            events.append((str(end), -1))

    events.sort()
    current = 0
    peak = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak
