from databricks.sdk.service.sql import StatementParameterListItem
from mcp.server.fastmcp import FastMCP

from ..formatting import format_bytes
from ..sql import execute_sql, validate_identifier


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_table_health(
        table_name: str,
        warehouse_id: str | None = None,
    ) -> str:
        """Delta table health check — file count, size distribution, small file ratio, last optimize/vacuum times.

        table_name should be fully qualified: catalog.schema.table.
        Runs DESCRIBE DETAIL and checks predictive optimization history.
        Flags common issues: too many small files, stale optimize, missing auto-optimize settings.
        """
        try:
            validate_identifier(table_name)
        except ValueError as e:
            return str(e)

        detail_query = f"DESCRIBE DETAIL {table_name}"
        try:
            detail_rows = execute_sql(detail_query, warehouse_id=warehouse_id)
        except Exception as e:
            return f"Failed to describe table: {e}"

        if not detail_rows:
            return f"No detail found for {table_name}. Verify the table name is fully qualified (catalog.schema.table)."

        d = detail_rows[0]
        num_files = int(d.get("numFiles") or 0)
        size_bytes = int(d.get("sizeInBytes") or 0)
        avg_file_size = size_bytes / num_files if num_files > 0 else 0
        partitions = d.get("partitionColumns") or "[]"
        table_props = d.get("properties") or "{}"

        lines = [f"Table: {table_name}\n"]
        lines.append(f"Format:             {d.get('format', '?')}")
        lines.append(f"Location:           {d.get('location', '?')}")
        lines.append(f"Total size:         {format_bytes(size_bytes)}")
        lines.append(f"Number of files:    {num_files}")
        lines.append(f"Avg file size:      {format_bytes(int(avg_file_size))}")
        lines.append(f"Partition columns:  {partitions}")
        lines.append(f"Min reader version: {d.get('minReaderVersion', '?')}")
        lines.append(f"Min writer version: {d.get('minWriterVersion', '?')}")
        lines.append(f"Table properties:   {table_props}")

        # Health checks
        issues = []
        if num_files > 0 and avg_file_size < 32 * 1024 * 1024:
            issues.append(
                f"SMALL FILES: avg file size is {format_bytes(int(avg_file_size))} (target: 128MB-1GB). Run OPTIMIZE."
            )
        if num_files > 10000:
            issues.append(f"HIGH FILE COUNT: {num_files} files. May cause slow planning. Run OPTIMIZE.")
        if "delta.autoOptimize.optimizeWrite" not in str(table_props):
            issues.append(
                "AUTO-OPTIMIZE not set: consider ALTER TABLE SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"
            )

        # Get last optimize/vacuum from history
        try:
            history_rows = execute_sql(
                f"DESCRIBE HISTORY {table_name} LIMIT 50",
                warehouse_id=warehouse_id,
            )
            last_optimize = None
            last_vacuum = None
            for h in history_rows:
                op = (h.get("operation") or "").upper()
                ts = h.get("timestamp")
                if "OPTIMIZE" in op and not last_optimize:
                    last_optimize = ts
                if "VACUUM" in op and not last_vacuum:
                    last_vacuum = ts

            lines.append(f"\nLast OPTIMIZE:      {last_optimize or 'Not found in recent history'}")
            lines.append(f"Last VACUUM:        {last_vacuum or 'Not found in recent history'}")

            if not last_optimize:
                issues.append("NO RECENT OPTIMIZE: table may have accumulated small files")
            if not last_vacuum:
                issues.append("NO RECENT VACUUM: old files may be consuming storage")

        except Exception:
            lines.append("\nLast OPTIMIZE:      (unable to read history)")
            lines.append("Last VACUUM:        (unable to read history)")

        # Try predictive optimization with proper filtering
        parts = table_name.split(".")
        if len(parts) == 3:
            try:
                pred_rows = execute_sql(
                    """
                    SELECT operation_type, start_time, end_time, usage_quantity, metrics
                    FROM system.storage.predictive_optimization_operations_history
                    WHERE catalog_name = :catalog AND schema_name = :schema AND table_name = :tbl
                    ORDER BY start_time DESC
                    LIMIT 10
                    """,
                    warehouse_id=warehouse_id,
                    parameters=[
                        StatementParameterListItem(name="catalog", value=parts[0]),
                        StatementParameterListItem(name="schema", value=parts[1]),
                        StatementParameterListItem(name="tbl", value=parts[2]),
                    ],
                )
                if pred_rows:
                    lines.append(f"\nPredictive optimization ({len(pred_rows)} recent operations):")
                    for p in pred_rows:
                        lines.append(
                            f"  {p.get('operation_type')} @ {p.get('start_time')} -- {p.get('usage_quantity', '?')} DBUs"
                        )
            except Exception:
                lines.append("\nPredictive optimization: (system table not available)")

        if issues:
            lines.append(f"\n{'=' * 60}")
            lines.append("HEALTH ISSUES DETECTED:")
            for issue in issues:
                lines.append(f"  [!] {issue}")

        return "\n".join(lines)

    @mcp.tool()
    def get_table_history(
        table_name: str,
        limit: int = 30,
        operation_filter: str | None = None,
        warehouse_id: str | None = None,
    ) -> str:
        """Delta table transaction log history — shows every write, merge, optimize, vacuum, schema change.

        table_name should be fully qualified (catalog.schema.table).
        operation_filter narrows to specific operations: WRITE, MERGE, DELETE, UPDATE, OPTIMIZE,
        VACUUM START, VACUUM END, SET TBLPROPERTIES, ALTER TABLE.
        Detects schema changes by comparing operation metrics across versions.
        """
        try:
            validate_identifier(table_name)
        except ValueError as e:
            return str(e)

        limit = min(int(limit), 200)
        query = f"DESCRIBE HISTORY {table_name} LIMIT {limit}"
        try:
            rows = execute_sql(query, warehouse_id=warehouse_id)
        except Exception as e:
            return f"Failed to get table history: {e}"

        if not rows:
            return f"No history found for {table_name}."

        if operation_filter:
            rows = [r for r in rows if operation_filter.upper() in (r.get("operation") or "").upper()]

        lines = [f"Transaction history for {table_name} ({len(rows)} entries):\n"]
        lines.append(f"{'Version':<10} {'Timestamp':<22} {'Operation':<20} {'User':<25} {'Metrics'}")
        lines.append("-" * 110)

        schema_changes = []
        for row in rows:
            ver = str(row.get("version", "?"))
            ts = str(row.get("timestamp", "?"))[:21]
            op = row.get("operation", "?")
            user = (row.get("userName") or "?")[:24]
            metrics = row.get("operationMetrics") or {}

            metric_parts = []
            if metrics.get("numOutputRows"):
                metric_parts.append(f"rows: {metrics['numOutputRows']}")
            if metrics.get("numOutputBytes"):
                metric_parts.append(f"bytes: {format_bytes(int(metrics['numOutputBytes']))}")
            if metrics.get("numAddedFiles"):
                metric_parts.append(f"+{metrics['numAddedFiles']} files")
            if metrics.get("numRemovedFiles"):
                metric_parts.append(f"-{metrics['numRemovedFiles']} files")
            if metrics.get("numTargetRowsInserted"):
                metric_parts.append(f"ins: {metrics['numTargetRowsInserted']}")
            if metrics.get("numTargetRowsUpdated"):
                metric_parts.append(f"upd: {metrics['numTargetRowsUpdated']}")
            if metrics.get("numTargetRowsDeleted"):
                metric_parts.append(f"del: {metrics['numTargetRowsDeleted']}")
            if metrics.get("numFilesAdded"):
                metric_parts.append(f"+{metrics['numFilesAdded']} files")
            if metrics.get("numFilesRemoved"):
                metric_parts.append(f"-{metrics['numFilesRemoved']} files")

            metric_str = ", ".join(metric_parts)[:50] if metric_parts else "--"
            lines.append(f"{ver:<10} {ts:<22} {op:<20} {user:<25} {metric_str}")

            if op in ("SET TBLPROPERTIES", "ALTER TABLE") or "schema" in str(metrics).lower():
                schema_changes.append((ver, ts, op))

        if schema_changes:
            lines.append("\nSchema/property changes detected:")
            for ver, ts, op in schema_changes:
                lines.append(f"  Version {ver} @ {ts}: {op}")

        return "\n".join(lines)

    @mcp.tool()
    def get_predictive_optimization(
        table_name: str | None = None,
        schema_name: str | None = None,
        days_back: int = 7,
        warehouse_id: str | None = None,
    ) -> str:
        """What Databricks predictive optimization actually did — OPTIMIZE, VACUUM, ZORDER runs
        with file counts and DBU costs.

        Filter by table_name (fully qualified: catalog.schema.table) or schema_name.
        Shows what was optimized, when, and how much it cost in DBUs.
        """
        days_back = int(days_back)
        params: list[StatementParameterListItem] = []
        conditions = [f"start_time >= current_timestamp() - INTERVAL {days_back} DAY"]

        if table_name:
            parts = table_name.split(".")
            if len(parts) == 3:
                conditions.append("catalog_name = :catalog")
                conditions.append("schema_name = :schema")
                conditions.append("table_name = :tbl")
                params.extend(
                    [
                        StatementParameterListItem(name="catalog", value=parts[0]),
                        StatementParameterListItem(name="schema", value=parts[1]),
                        StatementParameterListItem(name="tbl", value=parts[2]),
                    ]
                )
            else:
                conditions.append("table_name = :tbl")
                params.append(StatementParameterListItem(name="tbl", value=parts[-1]))
        if schema_name:
            conditions.append("schema_name = :schema_filter")
            params.append(StatementParameterListItem(name="schema_filter", value=schema_name))

        where = " AND ".join(conditions)

        query = f"""
        SELECT
            catalog_name,
            schema_name,
            table_name,
            operation_type,
            start_time,
            end_time,
            usage_quantity,
            usage_unit,
            metrics
        FROM system.storage.predictive_optimization_operations_history
        WHERE {where}
        ORDER BY start_time DESC
        LIMIT 100
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query predictive optimization: {e}"

        if not rows:
            return "No predictive optimization operations found for the specified criteria."

        lines = [f"Predictive optimization operations (last {days_back} days, {len(rows)} ops):\n"]
        lines.append(f"{'Table':<40} {'Operation':<15} {'Start':<22} {'DBUs':<10} {'Metrics'}")
        lines.append("-" * 105)

        total_dbu = 0.0
        for row in rows:
            full_name = f"{row.get('catalog_name', '?')}.{row.get('schema_name', '?')}.{row.get('table_name', '?')}"
            full_name = full_name[:39]
            op = (row.get("operation_type") or "?")[:14]
            start = str(row.get("start_time", "?"))[:21]
            dbu = float(row.get("usage_quantity") or 0)
            total_dbu += dbu
            metrics = str(row.get("metrics") or "--")[:30]
            lines.append(f"{full_name:<40} {op:<15} {start:<22} {dbu:<10.2f} {metrics}")

        lines.append("-" * 105)
        lines.append(f"Total DBU cost: {total_dbu:.2f}")

        return "\n".join(lines)
