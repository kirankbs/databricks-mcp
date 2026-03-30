"""SQL Statement Execution API client for system tables queries."""

import re
import time

from databricks.sdk.service.sql import StatementParameterListItem

from .client import get_workspace_client
from .config import get_config
from .formatting import enum_val

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier (table name, schema name) to prevent injection."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def execute_sql(
    statement: str,
    warehouse_id: str | None = None,
    parameters: list[StatementParameterListItem] | None = None,
) -> list[dict]:
    """Execute a SQL statement via the Statement Execution API and return rows as dicts.

    Uses the warehouse_id from config if not provided.
    Polls until the statement completes (up to 120s).
    """
    w = get_workspace_client()
    wh_id = warehouse_id or get_config().warehouse_id

    if not wh_id:
        raise ValueError(
            "No SQL warehouse configured. Set DATABRICKS_WAREHOUSE_ID environment variable "
            "or pass warehouse_id directly."
        )

    kwargs = dict(
        warehouse_id=wh_id,
        statement=statement,
        wait_timeout="30s",
    )
    if parameters:
        kwargs["parameters"] = parameters

    resp = w.statement_execution.execute_statement(**kwargs)

    deadline = time.time() + 120
    while resp.status and resp.status.state:
        state_val = enum_val(resp.status.state)
        if state_val not in ("PENDING", "RUNNING"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"SQL statement timed out after 120s: {statement[:100]}...")
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id=resp.statement_id)

    if resp.status and resp.status.state:
        state_val = enum_val(resp.status.state)
        if state_val == "FAILED":
            error_msg = ""
            if resp.status.error:
                error_msg = resp.status.error.message or str(resp.status.error)
            raise RuntimeError(f"SQL statement failed: {error_msg}")

    columns = (
        [col.name for col in (resp.manifest.schema.columns or [])] if resp.manifest and resp.manifest.schema else []
    )
    rows = []
    if resp.result and resp.result.data_array:
        for row_data in resp.result.data_array:
            rows.append(dict(zip(columns, row_data)))

    return rows
