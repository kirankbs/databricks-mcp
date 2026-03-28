from databricks.sdk.service.sql import StatementParameterListItem
from mcp.server.fastmcp import FastMCP

from ..sql import execute_sql


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_table_lineage(
        table_name: str,
        direction: str = "both",
        days_back: int = 30,
        warehouse_id: str | None = None,
    ) -> str:
        """Table-level lineage from Unity Catalog — trace upstream sources and downstream dependents.

        table_name: fully qualified (catalog.schema.table).
        direction: 'upstream' (what feeds this table), 'downstream' (what reads from it), or 'both'.
        Shows which jobs, notebooks, or pipelines created the lineage.
        Essential for blast-radius analysis: "if this table breaks, what else is affected?"
        """
        days_back = int(days_back)
        params = [StatementParameterListItem(name="table_name", value=table_name)]
        sections = []

        if direction in ("upstream", "both"):
            upstream_query = f"""
            SELECT DISTINCT
                source_table_full_name,
                source_type,
                entity_type,
                event_time
            FROM system.access.table_lineage
            WHERE target_table_full_name = :table_name
                AND event_time >= current_timestamp() - INTERVAL {days_back} DAY
            ORDER BY event_time DESC
            LIMIT 50
            """
            try:
                upstream = execute_sql(upstream_query, warehouse_id=warehouse_id, parameters=params)
                if upstream:
                    lines = [f"UPSTREAM sources ({len(upstream)} tables feed into {table_name}):\n"]
                    lines.append(f"{'Source Table':<55} {'Type':<15} {'Via':<15} {'Last Seen'}")
                    lines.append("-" * 100)
                    for row in upstream:
                        src = (row.get("source_table_full_name") or "?")[:54]
                        stype = (row.get("source_type") or "?")[:14]
                        entity = (row.get("entity_type") or "?")[:14]
                        ts = str(row.get("event_time", "?"))[:21]
                        lines.append(f"{src:<55} {stype:<15} {entity:<15} {ts}")
                    sections.append("\n".join(lines))
                else:
                    sections.append(f"No upstream sources found for {table_name} in the last {days_back} days.")
            except Exception as e:
                sections.append(f"Failed to query upstream lineage: {e}")

        if direction in ("downstream", "both"):
            downstream_query = f"""
            SELECT DISTINCT
                target_table_full_name,
                target_type,
                entity_type,
                event_time
            FROM system.access.table_lineage
            WHERE source_table_full_name = :table_name
                AND event_time >= current_timestamp() - INTERVAL {days_back} DAY
            ORDER BY event_time DESC
            LIMIT 50
            """
            try:
                downstream = execute_sql(downstream_query, warehouse_id=warehouse_id, parameters=params)
                if downstream:
                    lines = [f"\nDOWNSTREAM dependents ({len(downstream)} tables read from {table_name}):\n"]
                    lines.append(f"{'Target Table':<55} {'Type':<15} {'Via':<15} {'Last Seen'}")
                    lines.append("-" * 100)
                    for row in downstream:
                        tgt = (row.get("target_table_full_name") or "?")[:54]
                        ttype = (row.get("target_type") or "?")[:14]
                        entity = (row.get("entity_type") or "?")[:14]
                        ts = str(row.get("event_time", "?"))[:21]
                        lines.append(f"{tgt:<55} {ttype:<15} {entity:<15} {ts}")
                    sections.append("\n".join(lines))
                else:
                    sections.append(f"No downstream dependents found for {table_name} in the last {days_back} days.")
            except Exception as e:
                sections.append(f"Failed to query downstream lineage: {e}")

        return "\n\n".join(sections)

    @mcp.tool()
    def get_column_lineage(
        table_name: str,
        column_name: str | None = None,
        days_back: int = 30,
        warehouse_id: str | None = None,
    ) -> str:
        """Column-level lineage from Unity Catalog — trace which source columns feed target columns.

        Useful for data quality investigations: "where does this ML feature column come from?"
        or "which upstream column change could have caused this data drift?"
        """
        days_back = int(days_back)
        params = [StatementParameterListItem(name="table_name", value=table_name)]
        conditions = [
            "target_table_full_name = :table_name",
            f"event_time >= current_timestamp() - INTERVAL {days_back} DAY",
        ]

        if column_name:
            conditions.append("target_column_name = :col_name")
            params.append(StatementParameterListItem(name="col_name", value=column_name))

        where = " AND ".join(conditions)

        query = f"""
        SELECT DISTINCT
            source_table_full_name,
            source_column_name,
            target_column_name,
            entity_type,
            event_time
        FROM system.access.column_lineage
        WHERE {where}
        ORDER BY target_column_name, event_time DESC
        LIMIT 100
        """

        try:
            rows = execute_sql(query, warehouse_id=warehouse_id, parameters=params)
        except Exception as e:
            return f"Failed to query column lineage: {e}"

        if not rows:
            col_suffix = f" column {column_name}" if column_name else ""
            return f"No column lineage found for {table_name}{col_suffix} in the last {days_back} days."

        lines = [f"Column lineage for {table_name} ({len(rows)} mappings):\n"]
        lines.append(f"{'Target Column':<25} {'Source Table':<45} {'Source Column':<25} {'Via'}")
        lines.append("-" * 110)

        for row in rows:
            tcol = (row.get("target_column_name") or "?")[:24]
            src_table = (row.get("source_table_full_name") or "?")[:44]
            scol = (row.get("source_column_name") or "?")[:24]
            entity = (row.get("entity_type") or "?")[:14]
            lines.append(f"{tcol:<25} {src_table:<45} {scol:<25} {entity}")

        return "\n".join(lines)
